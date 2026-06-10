"""
GitHub REST client for the repo-CI dashboard aggregator.

aiohttp + GH_PAT from Secret Manager (via the unified-cloud-interface secret facade —
no direct env-var reads), short TTL cache per URL, HONEST rate-limit handling: an
exhausted limit raises 503 with retry_after — never silently-stale data.

Plan: ci_dashboard_deployment_ui_2026_06_10.md Phase 1.
"""

from __future__ import annotations

import asyncio
import datetime as dt
import logging
import time
from collections.abc import Mapping
from typing import cast

import aiohttp
from fastapi import HTTPException
from unified_trading_library import get_secret_client

logger = logging.getLogger(__name__)

_API_BASE = "https://api.github.com"
_CACHE_TTL_SECONDS = 90.0
_REQUEST_TIMEOUT = aiohttp.ClientTimeout(total=20)

# url -> (monotonic_ts, parsed json | raw text, etag | None)
# The ETag lets us send a conditional `If-None-Match` once the short TTL lapses:
# GitHub answers a matching ETag with `304 Not Modified` which is FREE — it does
# NOT decrement the shared per-user REST budget. On 304 we serve the cached body;
# on 200 we refresh both the body and the ETag.
_response_cache: dict[str, tuple[float, object, str | None]] = {}
_token_cache: str | None = None


def _utcnow() -> dt.datetime:
    return dt.datetime.now(dt.UTC)


def parse_github_ts(value: str) -> dt.datetime:
    """GitHub timestamps are RFC3339 with a trailing Z."""
    return dt.datetime.fromisoformat(value.replace("Z", "+00:00"))


def age_minutes(value: str, now: dt.datetime | None = None) -> int:
    """Whole minutes elapsed since a GitHub RFC3339 timestamp."""
    reference = now if now is not None else _utcnow()
    return int((reference - parse_github_ts(value)).total_seconds() / 60.0)


async def resolve_gh_token(project_id: str) -> str:
    """GH_PAT from Secret Manager (cached for process lifetime; rotation = restart)."""
    global _token_cache
    if _token_cache:
        return _token_cache

    def _fetch() -> str | None:
        return get_secret_client(project_id=project_id).get_secret("GH_PAT")

    token = await asyncio.to_thread(_fetch)
    if not token:
        raise HTTPException(status_code=503, detail="GH_PAT secret unavailable — repo-ci cannot reach GitHub")
    _token_cache = token
    return token


def _raise_if_rate_limited(status: int, headers: Mapping[str, str], url: str) -> None:
    """Honest rate-limit handling: surface 503 + retry_after, never stale data."""
    if status not in (403, 429):
        return
    remaining = headers.get("X-RateLimit-Remaining")
    reset = headers.get("X-RateLimit-Reset")
    if status == 429 or remaining == "0":
        retry_after = max(0, int(float(reset or "0")) - int(time.time())) if reset else 60
        raise HTTPException(
            status_code=503,
            detail={"message": "GitHub rate limit exhausted", "retry_after": retry_after, "url": url},
        )


def _resource_view(resource: object) -> dict[str, object]:
    """Project one /rate_limit resource block to {limit, remaining, used, reset}."""
    block = _as_dict(resource)
    return {
        "limit": int(cast(int, block.get("limit") or 0)),
        "remaining": int(cast(int, block.get("remaining") or 0)),
        "used": int(cast(int, block.get("used") or 0)),
        "reset": int(cast(int, block.get("reset") or 0)),
    }


async def gh_rate_limit(session: aiohttp.ClientSession, token: str) -> dict[str, object]:
    """Parse `GET /rate_limit` — the shared per-user REST budget for the UI.

    The `/rate_limit` endpoint is itself FREE (it never counts against the budget),
    so this is NEVER cached and NEVER sends `If-None-Match`. Surfaces the core /
    graphql / search resource blocks plus the fetch minute (UTC) for the dashboard.
    """
    url = f"{_API_BASE}/rate_limit"
    headers = {
        "Authorization": f"Bearer {token}",
        "Accept": "application/vnd.github+json",
        "X-GitHub-Api-Version": "2022-11-28",
    }
    async with session.get(url, headers=headers, timeout=_REQUEST_TIMEOUT) as resp:
        if resp.status >= 400:
            body = (await resp.text())[:200]
            raise HTTPException(
                status_code=502, detail={"message": f"GitHub {resp.status} for /rate_limit", "body": body}
            )
        payload = cast(object, await resp.json())  # noqa: qg-raw-json
    resources = _as_dict(_as_dict(payload).get("resources"))
    return {
        "fetched_at": _utcnow().strftime("%Y-%m-%dT%H:%MZ"),
        "resources": {
            "core": _resource_view(resources.get("core")),
            "graphql": _resource_view(resources.get("graphql")),
            "search": _resource_view(resources.get("search")),
        },
    }


async def gh_get_json(session: aiohttp.ClientSession, token: str, path: str) -> object:
    """GET an api.github.com path, parsed JSON, behind the TTL + ETag cache. 404 -> None.

    Two layers of rate-cost avoidance:
      1. Fresh TTL hit -> serve the cached body with ZERO network call.
      2. TTL lapsed but ETag known -> send `If-None-Match`; GitHub's `304 Not Modified`
         is FREE (no REST-budget decrement) and we serve the cached body.
    Only a 200 spends from the shared per-user budget — and refreshes body + ETag.
    """
    url = f"{_API_BASE}{path}"
    now = time.monotonic()
    cached = _response_cache.get(url)
    if cached is not None and now - cached[0] < _CACHE_TTL_SECONDS:
        return cached[1]
    headers = {
        "Authorization": f"Bearer {token}",
        "Accept": "application/vnd.github+json",
        "X-GitHub-Api-Version": "2022-11-28",
    }
    cached_etag = cached[2] if cached is not None else None
    if cached_etag:
        headers["If-None-Match"] = cached_etag
    async with session.get(url, headers=headers, timeout=_REQUEST_TIMEOUT) as resp:
        if resp.status == 304 and cached is not None:
            # FREE conditional hit — content unchanged. Re-stamp the TTL, keep body + ETag.
            _response_cache[url] = (now, cached[1], cached_etag)
            return cached[1]
        if resp.status == 404:
            _response_cache[url] = (now, None, resp.headers.get("ETag"))
            return None
        _raise_if_rate_limited(resp.status, resp.headers, url)
        if resp.status >= 400:
            body = (await resp.text())[:200]
            raise HTTPException(status_code=502, detail={"message": f"GitHub {resp.status} for {path}", "body": body})
        # GitHub responses are heterogeneous per endpoint; shapes are narrowed by the typed
        # helpers downstream (branch_head/compare/pulls), not a single Pydantic model.
        payload = cast(object, await resp.json())  # noqa: qg-raw-json
        etag = resp.headers.get("ETag")
    _response_cache[url] = (now, payload, etag)
    return payload


async def gh_raw_file(session: aiohttp.ClientSession, token: str, org: str, repo: str, path: str, ref: str) -> str:
    """Fetch a file's raw content via the contents API (works on private repos)."""
    url = f"{_API_BASE}/repos/{org}/{repo}/contents/{path}?ref={ref}"
    now = time.monotonic()
    cached = _response_cache.get(url)
    if cached is not None and now - cached[0] < _CACHE_TTL_SECONDS:
        return str(cached[1])
    headers = {
        "Authorization": f"Bearer {token}",
        "Accept": "application/vnd.github.raw+json",
        "X-GitHub-Api-Version": "2022-11-28",
    }
    cached_etag = cached[2] if cached is not None else None
    if cached_etag:
        headers["If-None-Match"] = cached_etag
    async with session.get(url, headers=headers, timeout=_REQUEST_TIMEOUT) as resp:
        if resp.status == 304 and cached is not None:
            _response_cache[url] = (now, cached[1], cached_etag)
            return str(cached[1])
        _raise_if_rate_limited(resp.status, resp.headers, url)
        if resp.status >= 400:
            body = (await resp.text())[:200]
            raise HTTPException(status_code=502, detail={"message": f"GitHub {resp.status} for {path}", "body": body})
        text = await resp.text()
        etag = resp.headers.get("ETag")
    _response_cache[url] = (now, text, etag)
    return text


def _as_dict(value: object) -> dict[str, object]:
    return cast(dict[str, object], value) if isinstance(value, dict) else {}


def _as_list(value: object) -> list[object]:
    return cast(list[object], value) if isinstance(value, list) else []


async def branch_head(
    session: aiohttp.ClientSession, token: str, org: str, repo: str, branch: str
) -> tuple[str | None, str | None]:
    """(sha, committed_at) of a branch head; (None, None) when the branch is absent."""
    payload = await gh_get_json(session, token, f"/repos/{org}/{repo}/branches/{branch}")
    data = _as_dict(payload)
    commit = _as_dict(data.get("commit"))
    sha = str(commit.get("sha")) if commit.get("sha") else None
    inner = _as_dict(commit.get("commit"))
    committer = _as_dict(inner.get("committer"))
    committed_at = str(committer.get("date")) if committer.get("date") else None
    return sha, committed_at


async def compare_branches(
    session: aiohttp.ClientSession, token: str, org: str, repo: str, base: str, head: str
) -> tuple[int, int, int] | None:
    """(ahead_by, behind_by, files_changed) for base...head; None when either ref is absent.

    files_changed is the CONTENT delta (capped at 300 by the API page — fine as a signal;
    squash-skewed commit counts alone lie, CLAUDE.md § "LDR is the SSOT").
    """
    payload = await gh_get_json(session, token, f"/repos/{org}/{repo}/compare/{base}...{head}?per_page=1")
    if payload is None:
        return None
    data = _as_dict(payload)
    return (
        int(cast(int, data.get("ahead_by") or 0)),
        int(cast(int, data.get("behind_by") or 0)),
        int(cast(int, data.get("total_files_changed") or 0) or len(_as_list(data.get("files")))),
    )


async def list_branch_commits(
    session: aiohttp.ClientSession, token: str, org: str, repo: str, branch: str, limit: int
) -> list[dict[str, object]]:
    """Recent commits on a branch: [{sha, message, author, committed_at}]."""
    payload = await gh_get_json(session, token, f"/repos/{org}/{repo}/commits?sha={branch}&per_page={limit}")
    out: list[dict[str, object]] = []
    for item in _as_list(payload):
        entry = _as_dict(item)
        inner = _as_dict(entry.get("commit"))
        author = _as_dict(inner.get("author"))
        message = str(inner.get("message") or "").split("\n", 1)[0]
        out.append(
            {
                "sha": str(entry.get("sha") or ""),
                "message": message,
                "author": str(author.get("name") or ""),
                "committed_at": str(author.get("date")) if author.get("date") else None,
            }
        )
    return out


async def v2_conclusion_for_sha(
    session: aiohttp.ClientSession, token: str, org: str, repo: str, sha: str
) -> str | None:
    """quality-gates-v2 check-run conclusion for one commit (None = never reported)."""
    payload = await gh_get_json(
        session, token, f"/repos/{org}/{repo}/commits/{sha}/check-runs?check_name=quality-gates-v2&per_page=5"
    )
    data = _as_dict(payload)
    for run in _as_list(data.get("check_runs")):
        run_dict = _as_dict(run)
        conclusion = run_dict.get("conclusion")
        status = run_dict.get("status")
        if conclusion:
            return str(conclusion)
        if status:
            return str(status)  # queued / in_progress — still informative
    return None


async def list_open_promotion_prs(
    session: aiohttp.ClientSession, token: str, org: str, repo: str
) -> list[dict[str, object]]:
    """Open PRs into staging/main (+ LDR-headed), enriched with per-PR mergeable_state.

    The list endpoint omits mergeable_state, so each candidate PR gets one detail GET —
    promotion PRs are rare (0-2 per repo) so this stays cheap under the TTL cache.
    """
    payload = await gh_get_json(session, token, f"/repos/{org}/{repo}/pulls?state=open&per_page=30")
    candidates: list[dict[str, object]] = []
    for item in _as_list(payload):
        pr = _as_dict(item)
        base = _as_dict(pr.get("base"))
        head = _as_dict(pr.get("head"))
        base_ref = str(base.get("ref") or "")
        head_ref = str(head.get("ref") or "")
        if base_ref not in ("staging", "main") and head_ref != "live-defi-rollout":
            continue
        if bool(pr.get("draft")):
            continue
        candidates.append(pr)

    enriched: list[dict[str, object]] = []
    for pr in candidates:
        number = int(cast(int, pr.get("number") or 0))
        detail_payload = await gh_get_json(session, token, f"/repos/{org}/{repo}/pulls/{number}")
        detail = _as_dict(detail_payload)
        head = _as_dict(detail.get("head") or pr.get("head"))
        base = _as_dict(detail.get("base") or pr.get("base"))
        head_sha = str(head.get("sha") or "")
        enriched.append(
            {
                "number": number,
                "title": str(detail.get("title") or pr.get("title") or ""),
                "url": str(detail.get("html_url") or pr.get("html_url") or ""),
                "base": str(base.get("ref") or ""),
                "head": str(head.get("ref") or ""),
                "head_sha": head_sha,
                "created_at": str(detail.get("created_at") or pr.get("created_at") or ""),
                "auto_merge": (detail.get("auto_merge") or pr.get("auto_merge")) is not None,
                "mergeable_state": str(detail.get("mergeable_state") or "unknown"),
            }
        )
    return enriched


async def head_commit_message(session: aiohttp.ClientSession, token: str, org: str, repo: str, sha: str) -> str:
    """Full message of one commit (drives the [skip ci] jam classification)."""
    payload = await gh_get_json(session, token, f"/repos/{org}/{repo}/commits/{sha}")
    data = _as_dict(payload)
    inner = _as_dict(data.get("commit"))
    return str(inner.get("message") or "")


async def head_check_rollup(
    session: aiohttp.ClientSession, token: str, org: str, repo: str, sha: str
) -> tuple[bool, bool]:
    """(failed_check, v2_present) over ALL check runs on a PR head sha."""
    payload = await gh_get_json(session, token, f"/repos/{org}/{repo}/commits/{sha}/check-runs?per_page=50")
    data = _as_dict(payload)
    failed = False
    v2_present = False
    for run in _as_list(data.get("check_runs")):
        run_dict = _as_dict(run)
        name = str(run_dict.get("name") or "")
        conclusion = str(run_dict.get("conclusion") or "")
        if "quality-gates-v2" in name:
            v2_present = True
        if conclusion in ("failure", "timed_out", "startup_failure"):
            failed = True
    return failed, v2_present


async def last_workflow_run(
    session: aiohttp.ClientSession, token: str, org: str, repo: str, workflow_file: str
) -> tuple[str | None, int | None]:
    """(conclusion-or-status, age_min) of the newest run of one workflow file."""
    payload = await gh_get_json(
        session, token, f"/repos/{org}/{repo}/actions/workflows/{workflow_file}/runs?per_page=1"
    )
    data = _as_dict(payload)
    runs = _as_list(data.get("workflow_runs"))
    if not runs:
        return None, None
    run = _as_dict(runs[0])
    conclusion = str(run.get("conclusion") or run.get("status") or "")
    created_at = str(run.get("created_at") or "")
    return (conclusion or None), (age_minutes(created_at) if created_at else None)


async def latest_workflow_run_with_jobs(
    session: aiohttp.ClientSession, token: str, org: str, repo: str, workflow_file: str
) -> dict[str, object] | None:
    """The newest run of one workflow file + its per-job breakdown.

    Powers the live SIT-run panel (alert-parity, operator add 2026-06-10): the dashboard
    always shows which repos/jobs were in the last cascade run, pass/fail/in-progress —
    not just a Slack page when it fails. Returns
    {url, status, conclusion, age_min, jobs: [{name, status, conclusion}]} or None.
    """
    payload = await gh_get_json(
        session, token, f"/repos/{org}/{repo}/actions/workflows/{workflow_file}/runs?per_page=1"
    )
    data = _as_dict(payload)
    runs = _as_list(data.get("workflow_runs"))
    if not runs:
        return None
    run = _as_dict(runs[0])
    run_id = run.get("id")
    created_at = str(run.get("created_at") or "")
    jobs: list[dict[str, object]] = []
    if run_id:
        jobs_payload = await gh_get_json(session, token, f"/repos/{org}/{repo}/actions/runs/{run_id}/jobs?per_page=100")
        for job_obj in _as_list(_as_dict(jobs_payload).get("jobs")):
            job = _as_dict(job_obj)
            conclusion_value = job.get("conclusion")
            jobs.append(
                {
                    "name": str(job.get("name") or ""),
                    "status": str(job.get("status") or ""),
                    "conclusion": str(conclusion_value) if conclusion_value else None,
                }
            )
    run_conclusion = run.get("conclusion")
    return {
        "url": str(run.get("html_url") or ""),
        "status": str(run.get("status") or ""),
        "conclusion": str(run_conclusion) if run_conclusion else None,
        "age_min": age_minutes(created_at) if created_at else None,
        "jobs": jobs,
    }

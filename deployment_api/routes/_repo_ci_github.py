"""
GitHub REST client for the repo-CI dashboard aggregator.

aiohttp + GH_PAT from Secret Manager (via the unified-cloud-interface secret facade —
no direct env-var reads), short TTL cache per URL, HONEST rate-limit handling: an
exhausted limit raises 503 with retry_after — never silently-stale data.

Plan: ci_dashboard_deployment_ui_2026_06_10.md Phase 1.
"""

from __future__ import annotations

import asyncio
import contextlib
import datetime as dt
import logging
import time
from collections.abc import Mapping
from typing import cast
from urllib.parse import quote

import aiohttp
import jwt
from fastapi import HTTPException
from unified_trading_library import get_secret_client

logger = logging.getLogger(__name__)

_API_BASE = "https://api.github.com"
_CACHE_TTL_SECONDS = 90.0
_REQUEST_TIMEOUT = aiohttp.ClientTimeout(total=20)

# GitHub App ("uts-ci-poller") installation-token pool — a SEPARATE 5000/hr REST +
# 5000-pt/hr GraphQL budget from the shared user PAT. Secret Manager names (same
# project the PAT comes from); any absent → App measurement is skipped gracefully.
_APP_ID_SECRET = "gh-app-ci-poller-app-id"
_APP_PRIVATE_KEY_SECRET = "gh-app-ci-poller-private-key"
_APP_INSTALLATION_ID_SECRET = "gh-app-ci-poller-installation-id"
# Installation tokens last 1 hour; refresh a margin before expiry. Cached in-process.
_APP_TOKEN_REFRESH_MARGIN_SECONDS = 5 * 60
_app_token_cache: tuple[str, float] | None = None  # (token, expiry_epoch)

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


async def resolve_app_credentials(project_id: str) -> tuple[str, str, str] | None:
    """Read the GitHub App creds (id / private-key PEM / installation-id) from Secret Manager.

    Returns ``(app_id, private_key_pem, installation_id)`` or ``None`` when ANY is
    absent — best-effort: a missing App config simply means no App pool is surfaced.
    Never raises; never logs a secret value.
    """

    def _fetch() -> tuple[str, str, str] | None:
        client = get_secret_client(project_id=project_id)
        app_id = client.get_secret(_APP_ID_SECRET)
        private_key = client.get_secret(_APP_PRIVATE_KEY_SECRET)
        installation_id = client.get_secret(_APP_INSTALLATION_ID_SECRET)
        if not app_id or not private_key or not installation_id:
            return None
        return app_id, private_key, installation_id

    try:
        return await asyncio.to_thread(_fetch)
    except Exception:  # best-effort: a SM transient/permission error must not break the PAT path
        logger.debug("repo-ci: GitHub App creds unavailable (continuing with PAT-only rate view)", exc_info=True)
        return None


def _make_app_jwt(app_id: str, private_key_pem: str) -> str:
    """Mint a 9-minute GitHub App JWT (RS256), used to exchange for an installation token."""
    now = int(time.time())
    payload: dict[str, object] = {
        "iat": now - 60,  # 60 s clock-skew buffer
        "exp": now + 9 * 60,
        "iss": app_id,
    }
    return jwt.encode(payload, private_key_pem, algorithm="RS256")


async def _app_installation_token(
    session: aiohttp.ClientSession, app_id: str, private_key_pem: str, installation_id: str
) -> str | None:
    """Mint (or reuse a cached) App installation token via POST /app/installations/{id}/access_tokens.

    The token is minted in-memory and cached for its lifetime (refreshed a margin
    before expiry). Returns ``None`` on any failure — best-effort. Never logs the token.
    """
    global _app_token_cache
    if _app_token_cache is not None and time.time() < _app_token_cache[1] - _APP_TOKEN_REFRESH_MARGIN_SECONDS:
        return _app_token_cache[0]
    app_jwt = _make_app_jwt(app_id, private_key_pem)
    url = f"{_API_BASE}/app/installations/{installation_id}/access_tokens"
    headers = {
        "Authorization": f"Bearer {app_jwt}",
        "Accept": "application/vnd.github+json",
        "X-GitHub-Api-Version": "2022-11-28",
    }
    async with session.post(url, headers=headers, timeout=_REQUEST_TIMEOUT) as resp:
        if resp.status >= 400:
            logger.debug("repo-ci: App installation-token mint returned %s (continuing PAT-only)", resp.status)
            return None
        payload = cast(object, await resp.json())  # noqa: qg-raw-json
    data = _as_dict(payload)
    token = data.get("token")
    if not isinstance(token, str) or not token:
        return None
    expires_at = data.get("expires_at")
    expiry = time.time() + 3600  # fallback if parse fails
    if isinstance(expires_at, str):
        with contextlib.suppress(ValueError):
            expiry = parse_github_ts(expires_at).timestamp()
    _app_token_cache = (token, expiry)
    return token


async def gh_app_rate_limit(
    session: aiohttp.ClientSession, app_id: str, private_key_pem: str, installation_id: str
) -> dict[str, object] | None:
    """Measure the GitHub App installation-token REST budget — its OWN 5000/hr pool.

    Mints an installation token (RS256 JWT → access_tokens) then calls the FREE
    `GET /rate_limit` with it. Returns ``{resources: {core, graphql}}`` or ``None``
    on any failure (best-effort: never breaks the PAT measurement). Never logs a secret.
    """
    token = await _app_installation_token(session, app_id, private_key_pem, installation_id)
    if token is None:
        return None
    url = f"{_API_BASE}/rate_limit"
    headers = {
        "Authorization": f"Bearer {token}",
        "Accept": "application/vnd.github+json",
        "X-GitHub-Api-Version": "2022-11-28",
    }
    async with session.get(url, headers=headers, timeout=_REQUEST_TIMEOUT) as resp:
        if resp.status >= 400:
            logger.debug("repo-ci: App /rate_limit returned %s (continuing PAT-only)", resp.status)
            return None
        payload = cast(object, await resp.json())  # noqa: qg-raw-json
    resources = _as_dict(_as_dict(payload).get("resources"))
    return {
        "resources": {
            "core": _resource_view(resources.get("core")),
            "graphql": _resource_view(resources.get("graphql")),
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


async def gh_graphql(
    session: aiohttp.ClientSession, token: str, query: str, variables: dict[str, object]
) -> dict[str, object]:
    """POST /graphql — ONE request batching what would be many REST calls.

    GraphQL spends from its OWN 5,000-point/hr budget (separate from the REST core
    budget the rest of the dashboard uses), and a tree-with-blob-text query costs ~1
    point — so the epics tab's cold load drops from ~92 REST requests to one cheap
    query (the 2026-06-11 rate-limit-exhaustion fix). Honest rate-limit handling
    mirrors gh_get_json: 503 + retry_after on exhaustion, 502 on other errors.
    Returns the `data` object."""
    url = f"{_API_BASE}/graphql"
    headers = {
        "Authorization": f"Bearer {token}",
        "Accept": "application/vnd.github+json",
        "X-GitHub-Api-Version": "2022-11-28",
    }
    async with session.post(
        url, headers=headers, json={"query": query, "variables": variables}, timeout=_REQUEST_TIMEOUT
    ) as resp:
        _raise_if_rate_limited(resp.status, resp.headers, url)
        if resp.status >= 400:
            body = (await resp.text())[:200]
            raise HTTPException(status_code=502, detail={"message": f"GitHub {resp.status} for /graphql", "body": body})
        payload = cast(object, await resp.json())  # noqa: qg-raw-json
    data = _as_dict(payload)
    errors = _as_list(data.get("errors"))
    if errors:
        first = _as_dict(errors[0])
        if str(first.get("type") or "") == "RATE_LIMITED":
            raise HTTPException(status_code=503, detail={"message": "GitHub GraphQL rate limit exhausted", "url": url})
        raise HTTPException(
            status_code=502,
            detail={
                "message": "GitHub GraphQL errors",
                "errors": [str(_as_dict(e).get("message") or "") for e in errors[:3]],
            },
        )
    return _as_dict(data.get("data"))


def _as_dict(value: object) -> dict[str, object]:
    return cast(dict[str, object], value) if isinstance(value, dict) else {}


def _as_list(value: object) -> list[object]:
    return cast(list[object], value) if isinstance(value, list) else []


async def branch_head(
    session: aiohttp.ClientSession, token: str, org: str, repo: str, branch: str
) -> tuple[str | None, str | None, str | None]:
    """(sha, committed_at, tree_sha) of a branch head; (None, None, None) when absent.

    tree_sha (`commit.commit.tree.sha`) is the CONTENT fingerprint — base.tree_sha ==
    head.tree_sha ⟺ a promote PR has nothing to promote (squash-accounting noise).
    """
    payload = await gh_get_json(session, token, f"/repos/{org}/{repo}/branches/{branch}")
    data = _as_dict(payload)
    commit = _as_dict(data.get("commit"))
    sha = str(commit.get("sha")) if commit.get("sha") else None
    inner = _as_dict(commit.get("commit"))
    committer = _as_dict(inner.get("committer"))
    committed_at = str(committer.get("date")) if committer.get("date") else None
    tree = _as_dict(inner.get("tree"))
    tree_sha = str(tree.get("sha")) if tree.get("sha") else None
    return sha, committed_at, tree_sha


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


async def diverged_content_lag(
    session: aiohttp.ClientSession,
    token: str,
    org: str,
    repo: str,
    base: str,
    head: str,
    *,
    max_files: int = 40,
) -> tuple[str | None, int]:
    """(oldest_iso, distinct_commit_count) for the CONTENT currently diverged between `base` and
    `head`, computed from the changed FILES — NOT the commit graph.

    The commit-graph `compare` is polluted by squash-originals (content already promoted, the SHA
    lingers "ahead") and `main-backmerge-to-ldr` merge nodes (LDR-only by construction), so its
    oldest commit phantom-ages to days (incident 2026-06-22: ui read 7d3h on a repo whose real
    un-promoted content was ~2.5h old — the oldest "ahead" commit was a June-15 backmerge node).
    Instead, for each currently-diverged file ask "when was its head-side version last set?"
    (`GET /commits?sha={head}&path={file}&per_page=1`); the OLDEST across the diverged files is the
    true content lag, and the count of DISTINCT last-setting commits is the real (squash-free) commit
    count. Bounded to `max_files` per repo (a deeply-drifted repo's exact age matters less than the
    >threshold signal). Returns (None, 0) when nothing is diverged."""
    payload = await gh_get_json(session, token, f"/repos/{org}/{repo}/compare/{base}...{head}?per_page=1")
    files = [str(_as_dict(f).get("filename") or "") for f in _as_list(_as_dict(payload).get("files"))]
    files = [f for f in files if f][:max_files]
    if not files:
        return None, 0

    async def _last_set(path: str) -> tuple[str | None, str | None]:
        rows = _as_list(
            await gh_get_json(
                session, token, f"/repos/{org}/{repo}/commits?sha={head}&path={quote(path, safe='/')}&per_page=1"
            )
        )
        if not rows:
            return None, None
        entry = _as_dict(rows[0])
        committer = _as_dict(_as_dict(entry.get("commit")).get("committer"))
        when = committer.get("date")
        sha = entry.get("sha")
        return (when if isinstance(when, str) else None), (str(sha) if sha else None)

    results = await asyncio.gather(*[_last_set(f) for f in files])
    dates = [d for d, _ in results if d]
    shas = {s for _, s in results if s}
    return (min(dates) if dates else None), len(shas)


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
    """quality-gates-v2 conclusion for ONE commit, via the Actions runs API.

    Uses `/actions/workflows/quality-gates-v2.yml/runs?head_sha=` (Actions:read — which the
    GH_PAT carries) rather than `/commits/{sha}/check-runs` (Checks:read — which fine-grained
    PATs CANNOT be granted: GitHub offers no Checks permission for fine-grained tokens at all,
    so that endpoint 403s permanently — community#129512). For a single-required-check workflow
    the workflow-run conclusion equals the check-run conclusion, so this lights up the per-SHA
    verdict today with no extra scope (mirrors `v2_conclusion_for_branch`). `per_page=1` returns
    the most-recent run for the SHA (the current verdict even after a re-run). Returns the
    conclusion (`success`/`failure`/…), the status while still running (`in_progress`/`queued`),
    or None when v2 never ran on that commit."""
    payload = await gh_get_json(
        session,
        token,
        f"/repos/{org}/{repo}/actions/workflows/quality-gates-v2.yml/runs?head_sha={sha}&per_page=1",
    )
    data = _as_dict(payload)
    for run in _as_list(data.get("workflow_runs")):
        run_dict = _as_dict(run)
        conclusion = run_dict.get("conclusion")
        if conclusion:
            return str(conclusion)
        status = run_dict.get("status")
        if status:
            return str(status)  # in_progress / queued — still informative
        return None
    return None


async def v2_conclusion_for_branch(
    session: aiohttp.ClientSession, token: str, org: str, repo: str, branch: str
) -> str | None:
    """Latest quality-gates-v2 conclusion for a BRANCH head, via the Actions runs API.

    Uses `/actions/workflows/quality-gates-v2.yml/runs?branch=` (Actions:read — which the
    GH_PAT carries) rather than `/commits/{sha}/check-runs` (Checks:read — which fine-grained
    PATs CANNOT be granted at all; GitHub offers no Checks permission for them, so that endpoint
    403s permanently — community#129512). So this lights up per-branch CI without the extra
    scope. Returns the conclusion (`success`/`failure`/…), the status when still running
    (`in_progress`/`queued`), or None when the workflow never ran on that branch."""
    payload = await gh_get_json(
        session,
        token,
        f"/repos/{org}/{repo}/actions/workflows/quality-gates-v2.yml/runs?branch={branch}&per_page=1",
    )
    data = _as_dict(payload)
    for run in _as_list(data.get("workflow_runs")):
        run_dict = _as_dict(run)
        conclusion = run_dict.get("conclusion")
        if conclusion:
            return str(conclusion)
        status = run_dict.get("status")
        if status:
            return str(status)  # in_progress / queued — still informative
        return None
    return None


async def last_green_for_branch(
    session: aiohttp.ClientSession, token: str, org: str, repo: str, branch: str
) -> tuple[str, str] | None:
    """The most-recent SHA on `branch` whose quality-gates-v2 concluded success, plus the run's
    completion time (N2 — "green as of <sha> · <age>"). Distinct from the branch HEAD, which may
    be red or pending. Uses `?branch=&status=success&per_page=1` (Actions:read — which the GH_PAT
    carries) so a single API call returns the latest green run for the branch. Returns
    (head_sha, completed_at_iso) or None when no successful v2 run exists for the branch."""
    payload = await gh_get_json(
        session,
        token,
        f"/repos/{org}/{repo}/actions/workflows/quality-gates-v2.yml/runs?branch={branch}&status=success&per_page=1",
    )
    data = _as_dict(payload)
    for run in _as_list(data.get("workflow_runs")):
        run_dict = _as_dict(run)
        sha = run_dict.get("head_sha")
        when = run_dict.get("updated_at") or run_dict.get("run_started_at") or run_dict.get("created_at")
        if isinstance(sha, str) and isinstance(when, str):
            return (sha, when)
        return None
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
    """(failed_check, v2_present) over ALL Actions runs on a PR head sha.

    Via `/actions/runs?head_sha=` (Actions:read — granted) rather than
    `/commits/{sha}/check-runs` (Checks:read — ungrantable for fine-grained PATs, 403s
    permanently; see `v2_conclusion_for_sha`). All this repo's required checks ARE Actions
    workflows, so the workflow-run rollup carries the same (failed, v2_present) signal the
    check-run rollup did — and without the 403 this feeds the v2-never-reported deadlock
    classifier honestly (the old 403 path defaulted `v2_present=True`, silently masking the
    deadlock signature)."""
    payload = await gh_get_json(session, token, f"/repos/{org}/{repo}/actions/runs?head_sha={sha}&per_page=100")
    data = _as_dict(payload)
    failed = False
    v2_present = False
    for run in _as_list(data.get("workflow_runs")):
        run_dict = _as_dict(run)
        name = str(run_dict.get("name") or "")
        path = str(run_dict.get("path") or "")
        conclusion = str(run_dict.get("conclusion") or "")
        if "quality-gates-v2" in path or "quality-gates-v2" in name:
            v2_present = True
        if conclusion in ("failure", "timed_out", "startup_failure"):
            failed = True
    return failed, v2_present


async def head_blocking_status_contexts(
    session: aiohttp.ClientSession, token: str, org: str, repo: str, sha: str
) -> list[dict[str, str]]:
    """Non-success CLASSIC commit-status contexts on a PR head — the human blocker reason.

    ``head_check_rollup`` only sees Actions WORKFLOW runs; a required check posted as a
    classic STATUS CONTEXT (AWS CodeBuild, any external CI) is invisible to it — so a PR
    BLOCKED purely on a failing CodeBuild status read as a clean "draining" PR on the
    dashboard, and the operator had to escalate to find out why (2026-06-15: two LDR→staging
    drain PRs stuck on CodeBuild's "Pull request approval required for starting a build" with
    no on-screen reason). This reads ``/commits/{sha}/status`` (Statuses:read — granted to the
    GH_PAT) and returns the failure/error/pending contexts + their description (the exact
    reason string GitHub shows), worst-first (failure/error before pending)."""
    payload = await gh_get_json(session, token, f"/repos/{org}/{repo}/commits/{sha}/status")
    data = _as_dict(payload)
    out: list[dict[str, str]] = []
    for st in _as_list(data.get("statuses")):
        st_d = _as_dict(st)
        state = str(st_d.get("state") or "")
        if state in ("failure", "error", "pending"):
            out.append(
                {
                    "name": str(st_d.get("context") or ""),
                    "state": state,
                    "description": str(st_d.get("description") or ""),
                }
            )
    out.sort(key=lambda c: c["state"] == "pending")  # failure/error first, pending last
    return out


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

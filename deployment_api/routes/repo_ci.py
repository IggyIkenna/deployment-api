"""
Repo-CI dashboard aggregator — GET /api/repo-ci/overview + /api/repo-ci/{repo}/detail.

One screen instead of 25 GitHub-UI visits: per-repo branch heads + content deltas across
live-defi-rollout/staging/main, quality-gates-v2 status, open promotion PRs with stuck
classification, SIT state, and the image-level deploy signal. Read-only v1.

Plan: unified-trading-pm/plans/active/ci_dashboard_deployment_ui_2026_06_10.md.
Master: monitoring_control_plane_master_2026_06_10.md (division-of-surfaces contract).
"""

from __future__ import annotations

import asyncio
import datetime as dt
import logging
from typing import cast

import aiohttp
from fastapi import APIRouter, HTTPException

from deployment_api.deployment_api_config import DeploymentApiConfig
from deployment_api.settings import CLOUD_PROVIDER, GITHUB_ORG
from deployment_api.settings import gcp_project_id as default_project_id

from ._cloud_builds_history import (
    _get_recent_builds_for_triggers,  # pyright: ignore[reportPrivateUsage]
)
from ._cloud_builds_trigger import (
    _build_trigger_list_sync,  # pyright: ignore[reportPrivateUsage]
)
from ._code_builds_aws import (
    get_recent_builds_for_projects_sync,
    is_aws_provider,
    list_codebuild_projects_sync,
)
from ._repo_ci_alerts import AlertEntryDict, AlertsPayloadDict, derive_streams, load_alerts_payload
from ._repo_ci_github import (
    age_minutes,
    branch_head,
    compare_branches,
    gh_get_json,
    head_check_rollup,
    head_commit_message,
    latest_workflow_run_with_jobs,
    list_branch_commits,
    list_open_promotion_prs,
    resolve_gh_token,
    v2_conclusion_for_sha,
)
from ._repo_ci_manifest import ManifestView, RepoMeta, load_manifest_view
from ._repo_ci_stuck import classify_stuck_pr, derive_sit_state, is_promotion_contract_pr
from ._repo_ci_types import (
    PROMOTION_BRANCHES,
    BranchCommitsDict,
    BranchDeltaDict,
    BranchHeadDict,
    CommitEntryDict,
    ImageSignalDict,
    OverviewResponseDict,
    RepoDetailResponseDict,
    RepoErrorDict,
    RepoOverviewDict,
    RepoPrDict,
    SitJobDict,
    SitLastRunDict,
    SitStateDict,
)

logger = logging.getLogger(__name__)
router = APIRouter(prefix="/api/repo-ci", tags=["Repo CI"])

# The cascade/SIT workflow lives in the PM repo (CLAUDE.md § "Breaking-detection").
_PM_REPO = "unified-trading-pm"
_SIT_WORKFLOW_FILE = "cascade-qg-ordering.yml"
_DETAIL_COMMITS_PER_BRANCH = 8
_DETAIL_V2_LOOKUPS_PER_BRANCH = 5
_REPO_CONCURRENCY = 8


def _now_iso() -> str:
    return dt.datetime.now(dt.UTC).isoformat()


def _sit_run_tuple(sit_last_run: dict[str, object] | None) -> tuple[str | None, int | None]:
    """(conclusion-or-status, age_min) view of the last SIT run for derive_sit_state."""
    if sit_last_run is None:
        return None, None
    conclusion = sit_last_run.get("conclusion") or sit_last_run.get("status")
    age = sit_last_run.get("age_min")
    return (str(conclusion) if conclusion else None), (int(age) if isinstance(age, int) else None)


def _to_sit_last_run(raw: dict[str, object] | None) -> SitLastRunDict | None:
    """Shape the raw run+jobs dict into the typed live SIT-run panel payload."""
    if raw is None:
        return None
    jobs_raw = raw.get("jobs")
    jobs: list[SitJobDict] = []
    if isinstance(jobs_raw, list):
        for job_obj in jobs_raw:  # pyright: ignore[reportUnknownVariableType]
            if not isinstance(job_obj, dict):
                continue
            job = cast("dict[str, object]", job_obj)
            conclusion = job.get("conclusion")
            jobs.append(
                SitJobDict(
                    name=str(job.get("name") or ""),
                    status=str(job.get("status") or ""),
                    conclusion=str(conclusion) if conclusion else None,
                )
            )
    conclusion_value = raw.get("conclusion")
    age_value = raw.get("age_min")
    return SitLastRunDict(
        url=str(raw.get("url") or ""),
        status=str(raw.get("status") or ""),
        conclusion=str(conclusion_value) if conclusion_value else None,
        age_min=int(age_value) if isinstance(age_value, int) else None,
        jobs=jobs,
    )


# ---------------------------------------------------------------------------
# Mock fixtures — drive UI dev + the playwright mock-mode suite. Cover EVERY
# stuck class + stuck-in-SIT (the regression spec asserts all of them).
# ---------------------------------------------------------------------------


def _mock_sit(in_pending: bool, stuck: bool) -> SitStateDict:
    return SitStateDict(
        in_breaking_pending=in_pending,
        staging_locked=in_pending,
        staging_locked_reason="breaking cascade in flight" if in_pending else None,
        last_sit_run_status="failure" if stuck else "success",
        last_sit_run_age_min=240 if stuck else 12,
        stuck_in_sit=stuck,
    )


def _mock_image(version: str | None, stale: bool | None) -> ImageSignalDict:
    return ImageSignalDict(
        last_build_status="SUCCESS",
        last_build_sha="aaa1111",
        deployed_version=version,
        image_stale=stale,
    )


def _mock_pr(repo: str, number: int, base: str, stuck_class: str | None, state: str) -> RepoPrDict:
    return RepoPrDict(
        repo=repo,
        number=number,
        title=f"promote live-defi-rollout -> {base}",
        base=base,
        head="live-defi-rollout",
        url=f"https://github.com/{GITHUB_ORG}/{repo}/pull/{number}",
        age_min=95,
        auto_merge=True,
        merge_state=state,
        failed_check=stuck_class == "failing_check",
        v2_present=stuck_class in (None, "failing_check", "automerge_stuck"),
        stuck_class=stuck_class,  # pyright: ignore[reportArgumentType]  # fixture literal is within StuckClass
    )


def _mock_row(
    repo: str,
    repo_type: str,
    ci_status: str,
    prs: list[RepoPrDict],
    sit: SitStateDict,
) -> RepoOverviewDict:
    branches = [
        BranchHeadDict(branch="live-defi-rollout", sha="abc1234", committed_at="2026-06-10T08:00:00Z"),
        BranchHeadDict(branch="staging", sha="abc1200", committed_at="2026-06-10T07:30:00Z"),
        BranchHeadDict(branch="main", sha="abc1100", committed_at="2026-06-10T06:00:00Z"),
    ]
    deltas = [
        BranchDeltaDict(base="staging", head="live-defi-rollout", ahead_by=2, behind_by=0, files_changed=3),
        BranchDeltaDict(base="main", head="staging", ahead_by=1, behind_by=0, files_changed=1),
        BranchDeltaDict(base="main", head="live-defi-rollout", ahead_by=3, behind_by=0, files_changed=4),
    ]
    return RepoOverviewDict(
        repo=repo,
        repo_type=repo_type,
        ci_status=ci_status,
        branches=branches,
        deltas=deltas,
        open_prs=prs,
        sit=sit,
        image=_mock_image("1.2.0", stale=ci_status == "FAILING"),
    )


def _mock_overview() -> OverviewResponseDict:
    rows = [
        _mock_row("unified-trading-library", "library", "MAIN_GREEN", [], _mock_sit(False, False)),
        _mock_row(
            "market-tick-data-service",
            "service",
            "STAGING_GREEN",
            [_mock_pr("market-tick-data-service", 41, "main", "conflicting", "dirty")],
            _mock_sit(False, False),
        ),
        _mock_row(
            "instruments-service",
            "service",
            "STAGING_PENDING",
            [_mock_pr("instruments-service", 17, "staging", "v2_never_reported", "blocked")],
            _mock_sit(False, False),
        ),
        _mock_row(
            "execution-service",
            "service",
            "FAILING",
            [
                _mock_pr("execution-service", 88, "main", "skip_ci_jammed", "blocked"),
                _mock_pr("execution-service", 89, "staging", "failing_check", "blocked"),
            ],
            _mock_sit(False, False),
        ),
        _mock_row(
            "strategy-service",
            "service",
            "STAGING_GREEN",
            [_mock_pr("strategy-service", 52, "main", "automerge_stuck", "blocked")],
            _mock_sit(False, False),
        ),
        _mock_row("greeks-service", "service", "STAGING_GREEN", [], _mock_sit(True, True)),
    ]
    stuck_prs = [pr for row in rows for pr in row["open_prs"] if pr.get("stuck_class")]
    stuck_in_sit = [row["repo"] for row in rows if row["sit"]["stuck_in_sit"]]
    sit_last_run = SitLastRunDict(
        url=f"https://github.com/{GITHUB_ORG}/unified-trading-pm/actions/runs/12345",
        status="in_progress",
        conclusion=None,
        age_min=18,
        jobs=[
            SitJobDict(name="sit / unified-trading-library", status="completed", conclusion="success"),
            SitJobDict(name="sit / greeks-service", status="completed", conclusion="failure"),
            SitJobDict(name="sit / execution-service", status="in_progress", conclusion=None),
        ],
    )
    return OverviewResponseDict(
        generated_at=_now_iso(),
        source="mock",
        repos=rows,
        stuck_prs=stuck_prs,
        stuck_in_sit=stuck_in_sit,
        sit_last_run=sit_last_run,
        # One sample degraded repo so the UI errors[] panel + its regression spec have data.
        errors=[RepoErrorDict(repo="alerting-service", error="HTTP 502 from GitHub compare")],
    )


def _mock_detail(repo: str) -> RepoDetailResponseDict:
    overview = _mock_overview()
    row = next((r for r in overview["repos"] if r["repo"] == repo), overview["repos"][0])
    history = [
        BranchCommitsDict(
            branch=branch["branch"],
            commits=[
                CommitEntryDict(
                    sha=f"{branch['sha'] or 'abc0000'}{i}",
                    message=f"feat: change {i} on {branch['branch']}",
                    author="ikennaigboaka [slot-3·laptop]",
                    committed_at="2026-06-10T07:00:00Z",
                    v2_conclusion="success" if i % 3 else "failure",
                )
                for i in range(5)
            ],
        )
        for branch in row["branches"]
    ]
    return RepoDetailResponseDict(
        repo=row["repo"],
        repo_type=row["repo_type"],
        ci_status=row["ci_status"],
        generated_at=_now_iso(),
        source="mock",
        branches=row["branches"],
        deltas=row["deltas"],
        history=history,
        open_prs=row["open_prs"],
        sit=row["sit"],
        image=row["image"],
    )


# ---------------------------------------------------------------------------
# Live aggregation
# ---------------------------------------------------------------------------


async def _repo_branches_and_deltas(
    session: aiohttp.ClientSession, token: str, repo: str
) -> tuple[list[BranchHeadDict], list[BranchDeltaDict]]:
    heads = await asyncio.gather(*[branch_head(session, token, GITHUB_ORG, repo, b) for b in PROMOTION_BRANCHES])
    branches = [
        BranchHeadDict(branch=b, sha=sha, committed_at=committed_at)
        for b, (sha, committed_at) in zip(PROMOTION_BRANCHES, heads, strict=True)
    ]
    pairs = [("staging", "live-defi-rollout"), ("main", "staging"), ("main", "live-defi-rollout")]
    compares = await asyncio.gather(
        *[compare_branches(session, token, GITHUB_ORG, repo, base, head) for base, head in pairs]
    )
    deltas: list[BranchDeltaDict] = []
    for (base, head), result in zip(pairs, compares, strict=True):
        if result is None:
            continue  # one of the refs is absent (e.g. no staging branch yet)
        ahead, behind, files = result
        deltas.append(BranchDeltaDict(base=base, head=head, ahead_by=ahead, behind_by=behind, files_changed=files))
    return branches, deltas


async def _repo_open_prs(session: aiohttp.ClientSession, token: str, repo: str) -> list[RepoPrDict]:
    raw_prs = await list_open_promotion_prs(session, token, GITHUB_ORG, repo)
    out: list[RepoPrDict] = []
    for pr in raw_prs:
        head_ref = str(pr.get("head") or "")
        auto_merge = bool(pr.get("auto_merge"))
        if not is_promotion_contract_pr(head_ref, auto_merge):
            continue
        head_sha = str(pr.get("head_sha") or "")
        created_at = str(pr.get("created_at") or "")
        merge_state = str(pr.get("mergeable_state") or "unknown")
        age_min = age_minutes(created_at) if created_at else 0
        failed_check = False
        v2_present = True
        head_message = ""
        if merge_state.lower() in ("blocked", "dirty", "conflicting") and head_sha:
            # Shard-level isolation: a checks-API 403 (fine-grained PAT missing Checks:read
            # on one repo) degrades THIS PR's classification to conservative defaults —
            # it must never kill the whole overview. Rate-limit 503 still propagates.
            try:
                failed_check, v2_present = await head_check_rollup(session, token, GITHUB_ORG, repo, head_sha)
                if not v2_present:
                    head_message = await head_commit_message(session, token, GITHUB_ORG, repo, head_sha)
            except HTTPException as exc:
                if exc.status_code == 503:
                    raise
                logger.warning("[REPO-CI] %s PR #%s check rollup unavailable: %s", repo, pr.get("number"), exc.detail)
        stuck = classify_stuck_pr(
            merge_state=merge_state,
            age_min=age_min,
            v2_present=v2_present,
            failed_check=failed_check,
            head_message=head_message,
        )
        out.append(
            RepoPrDict(
                repo=repo,
                number=int(pr.get("number") or 0),  # pyright: ignore[reportArgumentType]  # enriched dict carries int
                title=str(pr.get("title") or ""),
                base=str(pr.get("base") or ""),
                head=head_ref,
                url=str(pr.get("url") or ""),
                age_min=age_min,
                auto_merge=auto_merge,
                merge_state=merge_state,
                failed_check=failed_check,
                v2_present=v2_present,
                stuck_class=stuck,
            )
        )
    return out


_builds_cache: tuple[float, dict[str, tuple[str | None, str | None]]] | None = None
_BUILDS_CACHE_TTL = 300.0  # mirrors the cloud-builds TTL pattern


async def _gcp_builds_by_repo() -> dict[str, tuple[str | None, str | None]]:
    """GCP Cloud Build half — repo -> (status, sha) via the trigger plumbing."""
    triggers = await asyncio.to_thread(_build_trigger_list_sync)
    trigger_to_repo = {t["trigger_id"]: str(t.get("service") or "") for t in triggers}
    builds = await _get_recent_builds_for_triggers(list(trigger_to_repo.keys()))
    result: dict[str, tuple[str | None, str | None]] = {}
    for trigger_id, info in builds.items():
        repo = trigger_to_repo.get(trigger_id)
        if repo:
            result[repo] = (info.get("status"), info.get("commit_sha"))
    return result


async def _aws_builds_by_repo() -> dict[str, tuple[str | None, str | None]]:
    """AWS CodeBuild half — repo -> (status, sha) via the CodeBuild project plumbing.

    Parallel to the GCP half: CodeBuild projects are the AWS equivalent of triggers,
    named `{service}-build`. `get_recent_builds_for_projects_sync` yields None for a
    project with no builds — skipped so that repo stays honestly-unknown.
    """
    projects = await asyncio.to_thread(list_codebuild_projects_sync)
    project_to_repo = {p["trigger_id"]: str(p.get("service") or "") for p in projects}
    builds = await asyncio.to_thread(get_recent_builds_for_projects_sync, list(project_to_repo.keys()))
    result: dict[str, tuple[str | None, str | None]] = {}
    for project_name, info in builds.items():
        repo = project_to_repo.get(project_name)
        if repo and info is not None:
            result[repo] = (info.get("status"), info.get("commit_sha"))
    return result


async def _latest_builds_by_repo() -> dict[str, tuple[str | None, str | None]]:
    """repo -> (last_build_status, last_build_sha) via the cloud-builds plumbing,
    dispatched on the active cloud provider (parity with the Cloud Builds tab).

    GCP reuses the Cloud Build trigger plumbing; AWS reuses the CodeBuild project
    plumbing. The GitHub/manifest half of the aggregator is cloud-agnostic — only the
    image/build signal follows the toggle. Best-effort: any cloud failure (missing
    perms, inactive/unavailable provider) yields {} — the image signal then reads
    honestly-unknown (None) for that repo, never fabricated.
    """
    global _builds_cache
    now = asyncio.get_running_loop().time()
    if _builds_cache is not None and now - _builds_cache[0] < _BUILDS_CACHE_TTL:
        return _builds_cache[1]
    try:
        result = await (_aws_builds_by_repo() if is_aws_provider() else _gcp_builds_by_repo())
    except Exception as exc:
        # boto3 / google.api_core exceptions (e.g. InvalidArgument on a region/project mismatch,
        # NoCredentialsError) outside the OSError/ValueError family; ANY failure here must degrade
        # to honest-unknown, never kill the overview (live 500, 2026-06-10). Rate limits don't
        # apply (cloud-build APIs, not GitHub).
        logger.warning("[REPO-CI] cloud-builds image signal unavailable (provider=%s): %s", CLOUD_PROVIDER, exc)
        result = {}
    _builds_cache = (now, result)
    return result


def _image_signal(
    view: ManifestView,
    repo: str,
    main_sha: str | None,
    builds: dict[str, tuple[str | None, str | None]],
) -> ImageSignalDict:
    """Image-level deploy signal (operator decision: v1 image-level, not runtime-level).

    image_stale = main HEAD sha differs from the last SUCCESSFUL build's sha — "is main's
    code built into the latest image". None = honestly unknown (no build data)."""
    build_status, build_sha = builds.get(repo, (None, None))
    stale: bool | None = None
    if main_sha and build_sha and build_status == "SUCCESS":
        stale = not main_sha.startswith(build_sha) and not build_sha.startswith(main_sha)
    return ImageSignalDict(
        last_build_status=build_status,
        last_build_sha=build_sha,
        deployed_version=view.deployed_version_for(repo),
        image_stale=stale,
    )


async def _overview_row(
    session: aiohttp.ClientSession,
    token: str,
    view: ManifestView,
    meta: RepoMeta,
    sit_run: tuple[str | None, int | None],
    builds: dict[str, tuple[str | None, str | None]],
    semaphore: asyncio.Semaphore,
) -> RepoOverviewDict | RepoErrorDict:
    """Aggregate one repo's row. On a per-repo (non-rate-limit) failure return a typed
    RepoErrorDict instead of dropping the repo silently (operator add 2026-06-10) — the
    overview endpoint splits rows from errors so a degraded repo stays VISIBLE."""
    async with semaphore:
        try:
            branches, deltas = await _repo_branches_and_deltas(session, token, meta.name)
            prs = await _repo_open_prs(session, token, meta.name)
        except HTTPException as exc:
            if exc.status_code == 503:
                raise  # rate-limit is global — surface it honestly
            logger.warning("[REPO-CI] %s aggregation degraded (errors[] entry): %s", meta.name, exc.detail)
            return RepoErrorDict(repo=meta.name, error=str(exc.detail))
        except (TimeoutError, aiohttp.ClientError, ValueError) as exc:
            logger.warning("[REPO-CI] %s aggregation failed (errors[] entry): %s", meta.name, exc)
            return RepoErrorDict(repo=meta.name, error=f"{type(exc).__name__}: {exc}")
    sit = derive_sit_state(
        repo=meta.name,
        breaking_pending=view.breaking_pending,
        staging_locked=view.staging_locked,
        staging_locked_reason=view.staging_locked_reason,
        last_sit_run_status=sit_run[0],
        last_sit_run_age_min=sit_run[1],
    )
    main_sha = next((b["sha"] for b in branches if b["branch"] == "main"), None)
    return RepoOverviewDict(
        repo=meta.name,
        repo_type=meta.repo_type,
        ci_status=view.ci_status_for(meta.name),
        branches=branches,
        deltas=deltas,
        open_prs=prs,
        sit=sit,
        image=_image_signal(view, meta.name, main_sha, builds),
    )


@router.get("/overview")
async def get_overview() -> OverviewResponseDict:
    """Fleet matrix: every repo's branch heads, deltas, CI status, PRs, SIT + deploy state."""
    cfg = DeploymentApiConfig()
    if cfg.is_mock_mode():
        return _mock_overview()

    project_id = default_project_id or ""
    token = await resolve_gh_token(project_id)
    async with aiohttp.ClientSession() as session:
        view = await load_manifest_view(session, token)
        sit_last_run = await latest_workflow_run_with_jobs(session, token, GITHUB_ORG, _PM_REPO, _SIT_WORKFLOW_FILE)
        sit_run = _sit_run_tuple(sit_last_run)
        builds = await _latest_builds_by_repo()
        semaphore = asyncio.Semaphore(_REPO_CONCURRENCY)
        rows_raw = await asyncio.gather(
            *[_overview_row(session, token, view, meta, sit_run, builds, semaphore) for meta in view.repos]
        )
    rows: list[RepoOverviewDict] = []
    errors: list[RepoErrorDict] = []
    for result in rows_raw:
        if "error" in result:  # RepoErrorDict carries an "error" key; RepoOverviewDict does not
            errors.append(result)
        else:
            rows.append(result)
    stuck_prs = [pr for row in rows for pr in row["open_prs"] if pr.get("stuck_class")]
    stuck_in_sit = [row["repo"] for row in rows if row["sit"]["stuck_in_sit"]]
    return OverviewResponseDict(
        generated_at=_now_iso(),
        source="live",
        repos=rows,
        stuck_prs=stuck_prs,
        stuck_in_sit=stuck_in_sit,
        sit_last_run=_to_sit_last_run(sit_last_run),
        errors=errors,
    )


@router.get("/{repo}/detail")
async def get_repo_detail(repo: str) -> RepoDetailResponseDict:
    """Drill-down: per-branch SHA history with v2 conclusions, PRs, SIT, image signal."""
    cfg = DeploymentApiConfig()
    if cfg.is_mock_mode():
        return _mock_detail(repo)

    project_id = default_project_id or ""
    token = await resolve_gh_token(project_id)
    async with aiohttp.ClientSession() as session:
        view = await load_manifest_view(session, token)
        # Accept deployment-SERVICE names too — resolve via manifest `consolidates[]`
        # (e.g. features-delta-one-service → features-service), so the per-service CI tab
        # works for every entry in the service list (operator 2026-06-10).
        resolved = view.resolve_repo(repo)
        if resolved is None:
            raise HTTPException(status_code=404, detail=f"unknown repo/service: {repo}")
        repo = resolved
        sit_run = _sit_run_tuple(
            await latest_workflow_run_with_jobs(session, token, GITHUB_ORG, _PM_REPO, _SIT_WORKFLOW_FILE)
        )
        branches, deltas = await _repo_branches_and_deltas(session, token, repo)
        prs = await _repo_open_prs(session, token, repo)

        history: list[BranchCommitsDict] = []
        for branch in branches:
            if branch["sha"] is None:
                continue
            commits_raw = await list_branch_commits(
                session, token, GITHUB_ORG, repo, branch["branch"], _DETAIL_COMMITS_PER_BRANCH
            )
            entries: list[CommitEntryDict] = []
            for index, commit in enumerate(commits_raw):
                sha = str(commit.get("sha") or "")
                v2: str | None = None
                if sha and index < _DETAIL_V2_LOOKUPS_PER_BRANCH:
                    # Checks-API 403 (PAT missing Checks:read) degrades to unknown, never fatal.
                    try:
                        v2 = await v2_conclusion_for_sha(session, token, GITHUB_ORG, repo, sha)
                    except HTTPException as exc:
                        if exc.status_code == 503:
                            raise
                        logger.warning("[REPO-CI] %s v2 conclusion unavailable for %s: %s", repo, sha[:7], exc.detail)
                committed_at = commit.get("committed_at")
                entries.append(
                    CommitEntryDict(
                        sha=sha,
                        message=str(commit.get("message") or ""),
                        author=str(commit.get("author") or ""),
                        committed_at=str(committed_at) if committed_at else None,
                        v2_conclusion=v2,
                    )
                )
            history.append(BranchCommitsDict(branch=branch["branch"], commits=entries))

    sit = derive_sit_state(
        repo=repo,
        breaking_pending=view.breaking_pending,
        staging_locked=view.staging_locked,
        staging_locked_reason=view.staging_locked_reason,
        last_sit_run_status=sit_run[0],
        last_sit_run_age_min=sit_run[1],
    )
    main_sha = next((b["sha"] for b in branches if b["branch"] == "main"), None)
    return RepoDetailResponseDict(
        repo=repo,
        repo_type=next((m.repo_type for m in view.repos if m.name == repo), "unknown"),
        ci_status=view.ci_status_for(repo),
        generated_at=_now_iso(),
        source="live",
        branches=branches,
        deltas=deltas,
        history=history,
        open_prs=prs,
        sit=sit,
        image=_image_signal(view, repo, main_sha, await _latest_builds_by_repo()),
    )



def _mock_alerts() -> AlertsPayloadDict:
    """Mock alert ledger — a lifecycle pair (FAILED -> RESOLVED) + a live CRITICAL, so the
    UI/playwright can assert current-vs-previous traceability."""
    entries: list[AlertEntryDict] = [
        AlertEntryDict(
            kind="alert", timestamp="2026-06-10T12:10:00Z", repo="unified-trading-pm",
            workflow_name="ci-status-update", severity="CRITICAL", conclusion="failure",
            message="CI REGRESSION: deployment-api is now FAILING (was MAIN_GREEN)",
            run_url="https://github.com/IggyIkenna/unified-trading-pm/actions/runs/1",
        ),
        AlertEntryDict(
            kind="alert", timestamp="2026-06-10T12:50:00Z", repo="unified-trading-pm",
            workflow_name="ci-status-update", severity="INFO", conclusion="success",
            message="RESOLVED: deployment-api recovered (FAILING -> MAIN_GREEN)",
            run_url="https://github.com/IggyIkenna/unified-trading-pm/actions/runs/2",
        ),
        AlertEntryDict(
            kind="alert", timestamp="2026-06-10T13:00:00Z", repo="unified-trading-pm",
            workflow_name="sit-unlock", severity="INFO", conclusion="success",
            message="SIT PASSED — staging UNLOCKED, breaking_pending cleared. Promotion queue flowing.",
            run_url="https://github.com/IggyIkenna/unified-trading-pm/actions/runs/3",
        ),
        AlertEntryDict(
            kind="alert", timestamp="2026-06-10T13:05:00Z", repo="execution-service",
            workflow_name="quality-gates-v2", severity="CRITICAL", conclusion="failure",
            message="quality-gates-v2 FAILED on main",
            run_url="https://github.com/IggyIkenna/execution-service/actions/runs/4",
        ),
    ]
    ordered = sorted(entries, key=lambda e: e["timestamp"], reverse=True)
    return AlertsPayloadDict(
        generated_at=_now_iso(), source="mock", alerts=ordered, streams=derive_streams(entries)
    )


@router.get("/alerts")
async def get_alerts() -> AlertsPayloadDict:
    """Alert-ledger traceability: every Slack alert + workflow state event, grouped into
    (repo, workflow) lifecycle streams with current vs previous state (operator 2026-06-10)."""
    cfg = DeploymentApiConfig()
    if cfg.is_mock_mode():
        return _mock_alerts()
    return await load_alerts_payload()


# Re-export for tests that patch the shared JSON getter.
__all__ = ["gh_get_json", "router"]

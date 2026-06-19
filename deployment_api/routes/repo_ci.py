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
import logging
from dataclasses import dataclass
from typing import Literal, cast

import aiohttp
from fastapi import APIRouter, HTTPException, Query

from deployment_api.deployment_api_config import DeploymentApiConfig
from deployment_api.settings import GITHUB_ORG
from deployment_api.settings import gcp_project_id as default_project_id

from ._cloud_builds_history import (
    _recent_builds_by_repo_name,  # pyright: ignore[reportPrivateUsage]
)
from ._code_builds_aws import (
    get_recent_builds_for_projects_sync,
    is_aws_provider,
    list_codebuild_projects_sync,
)
from ._repo_ci_alerts import AlertsPayloadDict, load_alerts_payload
from ._repo_ci_fleet import fetch_fleet_git_health
from ._repo_ci_github import (
    age_minutes,
    branch_head,
    compare_branches,
    gh_get_json,
    head_blocking_status_contexts,
    head_check_rollup,
    head_commit_message,
    last_green_for_branch,
    latest_workflow_run_with_jobs,
    list_branch_commits,
    list_open_promotion_prs,
    oldest_unpromoted_commit_at,
    resolve_gh_token,
    v2_conclusion_for_branch,
    v2_conclusion_for_sha,
)
from ._repo_ci_manifest import (  # pyright: ignore[reportPrivateUsage]
    ManifestView,
    RepoMeta,
    _compute_dep_order,
    load_manifest_view,
)
from ._repo_ci_mocks import (  # pyright: ignore[reportPrivateUsage]
    _mock_alerts,
    _mock_detail,
    _mock_fleet_git_health,
    _mock_overview,
)
from ._repo_ci_stuck import classify_stuck_pr, derive_sit_state, is_promotion_contract_pr
from ._repo_ci_types import (  # pyright: ignore[reportPrivateUsage]
    PROMOTION_BRANCHES,
    BlockingCheckDict,
    BranchCommitsDict,
    BranchDeltaDict,
    BranchHeadDict,
    CommitEntryDict,
    FleetGitHealthProxyDict,
    ImageSignalDict,
    LastGreenDict,
    OverviewResponseDict,
    PromoteRunDict,
    PromotionBlockedDict,
    PromotionDrainDict,
    RepoDetailResponseDict,
    RepoErrorDict,
    RepoOverviewDict,
    RepoPrDict,
    SemverHealthDict,
    SitJobDict,
    SitLastRunDict,
    _now_iso,
)

logger = logging.getLogger(__name__)
router = APIRouter(prefix="/api/repo-ci", tags=["Repo CI"])

# The cascade/SIT workflow lives in the PM repo (CLAUDE.md § "Breaking-detection").
_PM_REPO = "unified-trading-pm"
_SIT_WORKFLOW_FILE = "cascade-qg-ordering.yml"
# Routine promotion-drain workflows (PM-central, every 15 min) — distinct from the breaking
# cascade above. These answer "is LDR draining to staging/main", not "did a breaking change run".
_LDR_TO_STAGING_WORKFLOW = "ldr-to-staging-promote.yml"
_LDR_TO_MAIN_WORKFLOW = "ldr-to-main-promote.yml"
# Semver-agent standing health (G2). The bump-rate circuit-breaker arms at ≥3 pending staging
# bumps (CLAUDE.md § "Manifest version-surface semantics"); the panel mirrors that threshold.
_SEMVER_WORKFLOW = "semver-agent.yml"
_SEMVER_BREAKER_THRESHOLD = 3
# Drain-stall (per repo) keys off the repo's OWN standing promotion PR being stuck on a BLOCKING
# class (needs a human/worker), NOT the PM-central drain-leg health — PM's ldr-to-main is a
# PM-only Option-B run on an hourly-ish cadence, so gating per-repo "content ahead of main" on it
# false-flagged the entire fleet. The auto-recoverable classes (v2_never_reported / automerge_stuck
# self-heal in-band) are deliberately EXCLUDED so the signal doesn't cry wolf.
_BLOCKING_STUCK_CLASSES = frozenset({"conflicting", "failing_check", "skip_ci_jammed"})

_DETAIL_COMMITS_PER_BRANCH = 8
_DETAIL_V2_LOOKUPS_PER_BRANCH = 5
_REPO_CONCURRENCY = 8


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


def _to_semver_health(raw: dict[str, object] | None, view: ManifestView) -> SemverHealthDict | None:
    """Shape the last semver-agent run + the manifest pending-bump surface into the G2 panel.

    Returns None only when the run can't be fetched AND there is no pending-bump signal — so a
    breaker-armed state is still surfaced even if the workflow query degrades.
    """
    pending = view.pending_version_bumps()
    if raw is None and not pending:
        return None
    conclusion_value = raw.get("conclusion") if raw else None
    age_value = raw.get("age_min") if raw else None
    return SemverHealthDict(
        last_run_status=str(raw.get("status") or "") if raw else "",
        last_run_conclusion=str(conclusion_value) if conclusion_value else None,
        last_run_age_min=int(age_value) if isinstance(age_value, int) else None,
        last_run_url=str(raw.get("url") or "") if raw else "",
        pending_bump_count=len(pending),
        pending_bump_repos=pending,
        breaker_armed=len(pending) >= _SEMVER_BREAKER_THRESHOLD,
        breaker_threshold=_SEMVER_BREAKER_THRESHOLD,
    )


def _to_promote_run(raw: dict[str, object] | None) -> PromoteRunDict | None:
    """Shape a raw workflow-run dict into the typed promote-drain payload (no jobs)."""
    if raw is None:
        return None
    conclusion_value = raw.get("conclusion")
    age_value = raw.get("age_min")
    return PromoteRunDict(
        status=str(raw.get("status") or ""),
        conclusion=str(conclusion_value) if conclusion_value else None,
        age_min=int(age_value) if isinstance(age_value, int) else None,
        url=str(raw.get("url") or ""),
    )


# ---------------------------------------------------------------------------
# Live aggregation
# ---------------------------------------------------------------------------


async def _repo_branches_and_deltas(
    session: aiohttp.ClientSession, token: str, repo: str
) -> tuple[list[BranchHeadDict], list[BranchDeltaDict]]:
    heads = await asyncio.gather(*[branch_head(session, token, GITHUB_ORG, repo, b) for b in PROMOTION_BRANCHES])
    branches = [
        BranchHeadDict(branch=b, sha=sha, committed_at=committed_at, tree_sha=tree_sha)
        for b, (sha, committed_at, tree_sha) in zip(PROMOTION_BRANCHES, heads, strict=True)
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


async def _repo_open_prs(
    session: aiohttp.ClientSession, token: str, repo: str, branch_trees: dict[str, str | None]
) -> list[RepoPrDict]:
    raw_prs = await list_open_promotion_prs(session, token, GITHUB_ORG, repo)
    out: list[RepoPrDict] = []
    for pr in raw_prs:
        head_ref = str(pr.get("head") or "")
        base_ref = str(pr.get("base") or "")
        auto_merge = bool(pr.get("auto_merge"))
        if not is_promotion_contract_pr(head_ref, auto_merge):
            continue
        head_sha = str(pr.get("head_sha") or "")
        created_at = str(pr.get("created_at") or "")
        merge_state = str(pr.get("mergeable_state") or "unknown")
        age_min = age_minutes(created_at) if created_at else 0
        # Content-identity: base TREE == head TREE → nothing to promote (squash-accounting noise),
        # even when GitHub reports CONFLICTING/BLOCKED off a stale squash merge-base. Short-circuits
        # the stuck classification so the triage queue never shows a phantom-stuck promote PR.
        base_tree = branch_trees.get(base_ref)
        head_tree = branch_trees.get(head_ref)
        content_identical = base_tree is not None and base_tree == head_tree
        failed_check = False
        v2_present = True
        head_message = ""
        blocking_checks: list[BlockingCheckDict] = []
        if merge_state.lower() in ("blocked", "dirty", "conflicting") and head_sha:
            # Shard-level isolation: any per-PR rollup error (a transient 4xx/5xx on one
            # repo) degrades THIS PR's classification to conservative defaults — it must
            # never kill the whole overview. Rate-limit 503 still propagates. (The rollup
            # now reads the Actions API, which the GH_PAT can access — no Checks:read 403.)
            try:
                failed_check, v2_present = await head_check_rollup(session, token, GITHUB_ORG, repo, head_sha)
                # Classic status contexts (AWS CodeBuild etc.) the Actions rollup can't see —
                # the on-screen "why is this stuck" (operator escalation 2026-06-15).
                blocking_checks = [
                    BlockingCheckDict(name=c["name"], state=c["state"], description=c["description"])
                    for c in await head_blocking_status_contexts(session, token, GITHUB_ORG, repo, head_sha)
                ]
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
            content_identical=content_identical,
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
                content_identical=content_identical,
                stuck_class=stuck,
                blocking_checks=blocking_checks,
            )
        )
    return out


@dataclass(frozen=True)
class BuildSignal:
    """The build signal for one repo (B1 — operator add 2026-06-11). Carries the LATEST build
    (status + time + sha + log) so the Image column is a full deploy signal, AND the LAST
    SUCCESSFUL build (operator add 2026-06-11) so a red latest build doesn't hide the last good
    image — "the current build failed, what's the last sha that succeeded?". success_* fields are
    None when no successful build is found in the scanned window (honestly absent, never faked).
    All fields None when the provider doesn't report them."""

    status: str | None
    sha: str | None
    finish_time: str | None  # ISO-8601 of the build's finish_time (create_time fallback upstream)
    log_url: str | None  # GCP Cloud Build / AWS CodeBuild console URL for this build
    success_sha: str | None = (
        None  # sha of the most recent SUCCESSFUL build (may equal sha, or differ when latest failed)
    )
    success_time: str | None = None  # finish_time of that successful build
    success_log_url: str | None = None  # console log URL of that successful build


_BuildProvider = Literal["gcp", "aws"]

# Build-signal cache keyed by RESOLVED provider, so a ?provider= toggle never returns the other
# cloud's stale data (gcp + aws are cached independently). Mirrors the cloud-builds TTL pattern.
_builds_cache: dict[str, tuple[float, dict[str, BuildSignal]]] = {}
_BUILDS_CACHE_TTL = 300.0


async def _gcp_builds_by_repo() -> dict[str, BuildSignal]:
    """GCP Cloud Build half — repo -> BuildSignal, matched on each build's REPO_NAME
    substitution (1:1 with the repo). Robust to trigger recreation (a per-trigger
    `trigger_id` filter goes stale when triggers are recreated)."""
    builds = await _recent_builds_by_repo_name()
    out: dict[str, BuildSignal] = {}
    for repo, (latest, success) in builds.items():
        out[repo] = BuildSignal(
            status=latest.get("status"),
            sha=latest.get("commit_sha"),
            finish_time=latest.get("finish_time"),
            log_url=latest.get("log_url"),
            success_sha=success.get("commit_sha") if success else None,
            success_time=success.get("finish_time") if success else None,
            success_log_url=success.get("log_url") if success else None,
        )
    return out


async def _aws_builds_by_repo() -> dict[str, BuildSignal]:
    """AWS CodeBuild half — repo -> BuildSignal via the CodeBuild project plumbing.

    Parallel to the GCP half: CodeBuild projects are the AWS equivalent of triggers,
    named `{service}-build`. `get_recent_builds_for_projects_sync` yields None for a
    project with no builds — skipped so that repo stays honestly-unknown.
    """
    projects = await asyncio.to_thread(list_codebuild_projects_sync)
    project_to_repo = {p["trigger_id"]: str(p.get("service") or "") for p in projects}
    builds = await asyncio.to_thread(get_recent_builds_for_projects_sync, list(project_to_repo.keys()))
    result: dict[str, BuildSignal] = {}
    for project_name, info in builds.items():
        repo = project_to_repo.get(project_name)
        if repo and info is not None:
            # Best-effort last-success on AWS: the project plumbing returns only the latest
            # build, so success_* is that build when it's green, else None (a deeper CodeBuild
            # history scan for the last green is a follow-up; GCP carries the full last-success).
            is_success = info.get("status") == "SUCCESS"
            result[repo] = BuildSignal(
                status=info.get("status"),
                sha=info.get("commit_sha"),
                finish_time=info.get("finish_time"),
                log_url=info.get("log_url"),
                success_sha=info.get("commit_sha") if is_success else None,
                success_time=info.get("finish_time") if is_success else None,
                success_log_url=info.get("log_url") if is_success else None,
            )
    return result


async def _latest_builds_by_repo(provider: _BuildProvider | None = None) -> dict[str, BuildSignal]:
    """repo -> BuildSignal via the cloud-builds plumbing, dispatched on the REQUESTED provider.

    ``provider`` selects whose build status to read (the repo-CI GCP/AWS toggle, ?provider=). When
    None it falls back to the server's own provider (``is_aws_provider()``) so a single-cloud
    deployment keeps its default view. GCP reuses the Cloud Build trigger plumbing; AWS reuses the
    CodeBuild project plumbing (keyless GCP->AWS WIF). The GitHub/manifest half of the aggregator is
    cloud-agnostic — only the image/build signal follows the toggle. Best-effort: any cloud failure
    (missing perms, inactive/unavailable provider) yields {} — the image signal then reads
    honestly-unknown (None) for that repo, never fabricated. Cached per resolved provider.
    """
    resolved: _BuildProvider = provider or ("aws" if is_aws_provider() else "gcp")
    now = asyncio.get_running_loop().time()
    cached = _builds_cache.get(resolved)
    if cached is not None and now - cached[0] < _BUILDS_CACHE_TTL:
        return cached[1]
    try:
        result = await (_aws_builds_by_repo() if resolved == "aws" else _gcp_builds_by_repo())
    except Exception as exc:
        # boto3 / google.api_core exceptions (e.g. InvalidArgument on a region/project mismatch,
        # NoCredentialsError) outside the OSError/ValueError family; ANY failure here must degrade
        # to honest-unknown, never kill the overview (live 500, 2026-06-10). Rate limits don't
        # apply (cloud-build APIs, not GitHub).
        logger.warning("[REPO-CI] cloud-builds image signal unavailable (provider=%s): %s", resolved, exc)
        result = {}
    _builds_cache[resolved] = (now, result)
    return result


def _image_signal(
    view: ManifestView,
    repo: str,
    main_sha: str | None,
    builds: dict[str, BuildSignal],
) -> ImageSignalDict:
    """Image-level deploy signal (operator decision: v1 image-level, not runtime-level).

    image_stale = main HEAD sha differs from the last SUCCESSFUL build's sha — "is main's
    code built into the latest image". The comparison uses the last SUCCESS sha (not the latest
    build's), because a failed latest build produced no new image — the running image is still
    from the last green build. None = honestly unknown (no successful build data)."""
    sig = builds.get(repo)
    build_status = sig.status if sig else None
    build_sha = sig.sha if sig else None
    success_sha = sig.success_sha if sig else None
    stale: bool | None = None
    if main_sha and success_sha:
        stale = not main_sha.startswith(success_sha) and not success_sha.startswith(main_sha)
    return ImageSignalDict(
        last_build_status=build_status,
        last_build_sha=build_sha,
        last_build_time=sig.finish_time if sig else None,
        last_build_log_url=sig.log_url if sig else None,
        last_success_sha=success_sha,
        last_success_time=sig.success_time if sig else None,
        last_success_log_url=sig.success_log_url if sig else None,
        deployed_version=view.deployed_version_for(repo),
        image_stale=stale,
    )


def _has_unpromoted_content(ldr_main: BranchDeltaDict | None) -> bool:
    """True only when LDR has REAL file content not yet on main — gate the promotion-lag age on
    this, NOT on ahead_by. Squash-merges keep LDR perpetually ahead-by-commit-count even when the
    tree content is byte-identical to main (`ahead_by>0, files_changed=0` = "squash skew"), so an
    ahead_by gate phantom-ages the oldest squashed commit (e.g. a 4-day-old commit whose content
    already promoted) and reddens the lag chip on a fully-drained repo. files_changed>0 is the same
    real-content signal `drain_stalled` already uses — keep the two consistent."""
    return ldr_main is not None and ldr_main["files_changed"] > 0


async def _overview_row(
    session: aiohttp.ClientSession,
    token: str,
    view: ManifestView,
    meta: RepoMeta,
    sit_run: tuple[str | None, int | None],
    builds: dict[str, BuildSignal],
    semaphore: asyncio.Semaphore,
) -> RepoOverviewDict | RepoErrorDict:
    """Aggregate one repo's row. On a per-repo (non-rate-limit) failure return a typed
    RepoErrorDict instead of dropping the repo silently (operator add 2026-06-10) — the
    overview endpoint splits rows from errors so a degraded repo stays VISIBLE."""
    async with semaphore:
        try:
            branches, deltas = await _repo_branches_and_deltas(session, token, meta.name)
            branch_trees = {b["branch"]: b["tree_sha"] for b in branches}
            prs = await _repo_open_prs(session, token, meta.name, branch_trees)
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
    ci_status = view.ci_status_for(meta.name)
    # Per-branch v2 conclusion so the UI can annotate WHICH branch is red. ONLY fetched for
    # non-MAIN_GREEN repos: a fully-green repo needs no branch annotation, and skipping it keeps
    # the overview's GitHub-API budget bounded (3 Actions-API calls for only the handful of red
    # repos, not for all 25 — the cold-overview rate-limit guard). Degrades to None per-branch on a
    # fetch failure; never fails the row.
    branch_ci: dict[str, str | None] = {}
    if ci_status != "MAIN_GREEN":
        try:
            conclusions = await asyncio.gather(
                *[v2_conclusion_for_branch(session, token, GITHUB_ORG, meta.name, b) for b in PROMOTION_BRANCHES]
            )
            branch_ci = dict(zip(PROMOTION_BRANCHES, conclusions, strict=True))
        except (TimeoutError, aiohttp.ClientError, ValueError, HTTPException) as exc:
            logger.warning("[REPO-CI] %s per-branch v2 fetch degraded: %s", meta.name, exc)
            branch_ci = dict.fromkeys(PROMOTION_BRANCHES, None)
    # N2: the most-recent GREEN main sha + time ("green as of <sha> · <age>") — distinct from the
    # main HEAD, which may be red/pending. For a MAIN_GREEN repo the head IS the last green (no
    # extra API call); only a non-green repo needs the runs-API lookup (same budget profile as
    # branch_ci above — bounded to the handful of red repos).
    last_green_main: LastGreenDict | None = None
    main_committed = next((b["committed_at"] for b in branches if b["branch"] == "main"), None)
    if ci_status == "MAIN_GREEN" and main_sha is not None and main_committed is not None:
        last_green_main = LastGreenDict(sha=main_sha, at=main_committed)
    elif ci_status != "MAIN_GREEN":
        try:
            lg = await last_green_for_branch(session, token, GITHUB_ORG, meta.name, "main")
        except (TimeoutError, aiohttp.ClientError, ValueError, HTTPException) as exc:
            logger.warning("[REPO-CI] %s last-green(main) fetch degraded: %s", meta.name, exc)
            lg = None
        if lg is not None:
            last_green_main = LastGreenDict(sha=lg[0], at=lg[1])
    # G6: promotion-lag age — the age of the OLDEST LDR commit not yet on main (the lag the
    # promotion-lag-monitor pages on at >60min). Gated on REAL content delta (files_changed>0, via
    # _has_unpromoted_content), NOT ahead_by: a squash-skew repo (ahead_by>0, files_changed=0) has
    # already promoted its content and must show NO lag (else a 4-day-old squashed commit reddens a
    # fully-drained row). A repo in sync has no lag → no extra API call.
    main_lag_age_min: int | None = None
    ldr_main = next((d for d in deltas if d["base"] == "main" and d["head"] == "live-defi-rollout"), None)
    if _has_unpromoted_content(ldr_main):
        try:
            oldest_at = await oldest_unpromoted_commit_at(
                session, token, GITHUB_ORG, meta.name, "main", "live-defi-rollout"
            )
        except (TimeoutError, aiohttp.ClientError, ValueError, HTTPException) as exc:
            logger.warning("[REPO-CI] %s lag-age fetch degraded: %s", meta.name, exc)
            oldest_at = None
        if oldest_at is not None:
            main_lag_age_min = age_minutes(oldest_at)
    # promotion-drain follow-up: drain-stalled = real content ahead of staging/main (files_changed,
    # NOT ahead_by — squash-merges keep LDR perpetually ahead-by-commit-count even when content
    # matches) AND this repo's own standing promotion PR is stuck on a BLOCKING class. A fleet-wide
    # drain-leg outage shows in the PromotionDrainPanel's leg rows; it is NOT a per-repo flag.
    ldr_staging = next((d for d in deltas if d["base"] == "staging" and d["head"] == "live-defi-rollout"), None)
    content_ahead = (ldr_staging is not None and ldr_staging["files_changed"] > 0) or (
        ldr_main is not None and ldr_main["files_changed"] > 0
    )
    has_blocking_pr = any(pr.get("stuck_class") in _BLOCKING_STUCK_CLASSES for pr in prs)
    drain_stalled = content_ahead and has_blocking_pr
    return RepoOverviewDict(
        repo=meta.name,
        repo_type=meta.repo_type,
        ci_status=ci_status,
        branch_ci=branch_ci,
        branches=branches,
        deltas=deltas,
        open_prs=prs,
        sit=sit,
        image=_image_signal(view, meta.name, main_sha, builds),
        last_green_main=last_green_main,
        main_lag_age_min=main_lag_age_min,
        drain_stalled=drain_stalled,
        # tier from the manifest now; blocked_by/blocking are the CROSS-repo dep-order fields filled
        # by _compute_dep_order in get_overview once every row exists (they need the whole fleet's
        # ci_status). Seed empty here so the row shape is always complete.
        tier=view.tier_for(meta.name),
        blocked_by=[],
        blocking=[],
    )


def _build_promotion_blocked(view: ManifestView) -> list[PromotionBlockedDict]:
    """Repos parked out of staging→main (G1) — union of promotion_failures + promotion_quarantine.

    Alert-parity for the staging-to-main genuine-failure CRITICAL page (failures) + the
    newly-quarantined WARNING (quarantine). Sorted: quarantined first, then by fail count desc.
    """
    failures = view.promotion_failures()
    quarantine = view.promotion_quarantine()
    blocked: list[PromotionBlockedDict] = []
    for repo in sorted(set(failures) | set(quarantine)):
        q = quarantine.get(repo)
        entry = PromotionBlockedDict(
            repo=repo,
            failures=failures.get(repo, 0),
            quarantined=repo in quarantine,
        )
        if isinstance(q, dict):
            since = q.get("since")
            attempts = q.get("attempts")
            escalated = q.get("escalated")
            if isinstance(since, str):
                entry["since"] = since
            if isinstance(attempts, int) and not isinstance(attempts, bool):
                entry["attempts"] = attempts
            if isinstance(escalated, bool):
                entry["escalated"] = escalated
        blocked.append(entry)
    blocked.sort(key=lambda e: (not e.get("quarantined", False), -e.get("failures", 0)))
    return blocked


@router.get("/overview")
async def get_overview(
    provider: _BuildProvider | None = Query(
        default=None,
        description=(
            "Cloud whose build status to read for the Image column (repo-CI GCP/AWS toggle). "
            "Omitted = the server's own provider. 'aws' reads CodeBuild via keyless WIF."
        ),
    ),
) -> OverviewResponseDict:
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
        # Routine promote-drain (PM-central, every 15 min) — distinct from the breaking cascade
        # above. Two GLOBAL queries (not per-repo), so the GitHub-API budget is unchanged.
        staging_drain_raw = await latest_workflow_run_with_jobs(
            session, token, GITHUB_ORG, _PM_REPO, _LDR_TO_STAGING_WORKFLOW
        )
        main_drain_raw = await latest_workflow_run_with_jobs(
            session, token, GITHUB_ORG, _PM_REPO, _LDR_TO_MAIN_WORKFLOW
        )
        # Semver-agent standing health (G2) — one more global query (not per-repo).
        semver_raw = await latest_workflow_run_with_jobs(session, token, GITHUB_ORG, _PM_REPO, _SEMVER_WORKFLOW)
        builds = await _latest_builds_by_repo(provider)
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
    promotion_drain = PromotionDrainDict(
        ldr_to_staging=_to_promote_run(staging_drain_raw),
        ldr_to_main=_to_promote_run(main_drain_raw),
    )
    # Dep-order HOLD (STAGE 1.8 mirror) — computed AFTER all rows exist (it needs the whole fleet's
    # ci_status). Patches each row's blocked_by/blocking in place, then yields the top-level aggregate.
    blocked_by_map, blocking_map, promotion_held = _compute_dep_order(rows, view)
    for row in rows:
        row["blocked_by"] = blocked_by_map.get(row["repo"], [])
        row["blocking"] = blocking_map.get(row["repo"], [])
    return OverviewResponseDict(
        generated_at=_now_iso(),
        source="live",
        repos=rows,
        stuck_prs=stuck_prs,
        stuck_in_sit=stuck_in_sit,
        sit_last_run=_to_sit_last_run(sit_last_run),
        errors=errors,
        promotion_blocked=_build_promotion_blocked(view),
        promotion_drain=promotion_drain,
        semver_health=_to_semver_health(semver_raw, view),
        promotion_held=promotion_held,
    )


@router.get("/{repo}/detail")
async def get_repo_detail(
    repo: str,
    provider: _BuildProvider | None = Query(
        default=None,
        description="Cloud whose build status to read for the Image signal (repo-CI GCP/AWS toggle).",
    ),
) -> RepoDetailResponseDict:
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
        branch_trees = {b["branch"]: b["tree_sha"] for b in branches}
        prs = await _repo_open_prs(session, token, repo, branch_trees)

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
                    # Any transient lookup error degrades this commit's v2 to unknown, never
                    # fatal (now reads the Actions API — no Checks:read 403). 503 propagates.
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

        # N2-followup: per-branch last-green. A branch whose HEAD's v2 is success IS its own last
        # green (no extra call — the head v2 is history[0]); otherwise one runs-API lookup. Cheap:
        # this is a single-repo drilldown, not the fleet overview.
        last_green: dict[str, LastGreenDict | None] = {}
        for branch in branches:
            branch_name = branch["branch"]
            head_sha = branch["sha"]
            if head_sha is None:
                last_green[branch_name] = None
                continue
            branch_hist = next((h for h in history if h["branch"] == branch_name), None)
            head_v2 = branch_hist["commits"][0]["v2_conclusion"] if branch_hist and branch_hist["commits"] else None
            head_committed = branch["committed_at"]
            if head_v2 == "success" and head_committed is not None:
                last_green[branch_name] = LastGreenDict(sha=head_sha, at=head_committed)
                continue
            try:
                lg = await last_green_for_branch(session, token, GITHUB_ORG, repo, branch_name)
            except (TimeoutError, aiohttp.ClientError, ValueError, HTTPException) as exc:
                logger.warning("[REPO-CI] %s last-green(%s) fetch degraded: %s", repo, branch_name, exc)
                lg = None
            last_green[branch_name] = LastGreenDict(sha=lg[0], at=lg[1]) if lg else None

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
        image=_image_signal(view, repo, main_sha, await _latest_builds_by_repo(provider)),
        last_green=last_green,
    )


@router.get("/alerts")
async def get_alerts() -> AlertsPayloadDict:
    """Alert-ledger traceability: every Slack alert + workflow state event, grouped into
    (repo, workflow) lifecycle streams with current vs previous state (operator 2026-06-10)."""
    cfg = DeploymentApiConfig()
    if cfg.is_mock_mode():
        return _mock_alerts()
    return await load_alerts_payload()


@router.get("/fleet-git-health")
async def get_fleet_git_health() -> FleetGitHealthProxyDict:
    """Proxy the agent-orchestrator's fleet git-health into the single devops pane
    (operator decision v2, 2026-06-10). Degrades honestly + always returns the
    orchestrator deep-link URL (git-health click-through goes to the AO UI)."""
    cfg = DeploymentApiConfig()
    if cfg.is_mock_mode():
        return _mock_fleet_git_health()
    return await fetch_fleet_git_health(default_project_id or "")


# Re-export for tests that patch the shared JSON getter.
__all__ = ["gh_get_json", "router"]

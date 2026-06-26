"""
TypedDict shapes + constants for the repo-CI dashboard aggregator.

Plan: unified-trading-pm/plans/active/ci_dashboard_deployment_ui_2026_06_10.md (Phase 1).
Data architecture (operator decision 2026-06-10 — HYBRID): GitHub API live state behind a
short TTL cache + workspace-manifest.json for the repo registry / ci_status / staging_status.
"""

from __future__ import annotations

import datetime as dt
from typing import Literal, NotRequired, Required, TypedDict

# The three branches of the promotion contract (LDR -> staging -> SIT -> main).
INTEGRATION_BRANCH = "live-defi-rollout"
STAGING_BRANCH = "staging"
MAIN_BRANCH = "main"
PROMOTION_BRANCHES: tuple[str, str, str] = (INTEGRATION_BRANCH, STAGING_BRANCH, MAIN_BRANCH)

# Stuck-PR classification — the closed set (mirrors ci_failure_watcher.py signatures;
# do NOT re-derive conditions, see _repo_ci_stuck.py).
StuckClass = Literal[
    "conflicting",  # CONFLICTING/DIRTY — merge-conflict wall, needs a rebase
    "skip_ci_jammed",  # head commit carries a [skip ci] marker + required v2 never reported
    "v2_never_reported",  # BLOCKED + no failed check + v2 absent from rollup (auto-recoverable)
    "failing_check",  # BLOCKED with a genuinely FAILED required check
    "automerge_stuck",  # auto-merge armed, mergeable, but unmerged past threshold
]


class BranchHeadDict(TypedDict):  # CORRECT-LOCAL: deployment-api response shape, not a domain contract
    """Head of one promotion branch."""

    branch: str
    sha: str | None  # None = branch absent (e.g. agent-orchestrator has no staging yet)
    committed_at: str | None
    # Git TREE sha of the head commit — the CONTENT fingerprint. Two commits with the same
    # tree_sha are byte-identical in content regardless of commit history, so base.tree_sha ==
    # head.tree_sha ⟺ a promote PR has nothing to promote (squash-accounting noise). None =
    # branch absent. This is the ONLY reliable content-identity signal: the compare API's
    # three-dot `files` is inflated by a stale squash merge-base (37 "files" for an identical tree).
    tree_sha: str | None


class BranchDeltaDict(TypedDict):  # CORRECT-LOCAL: deployment-api response shape, not a domain contract
    """Content-aware compare between two promotion branches.

    files_changed is the honest signal — squash-merges keep LDR perpetually ahead by
    COMMIT count even when CONTENT matches (CLAUDE.md § "LDR is the SSOT").
    """

    base: str
    head: str
    ahead_by: int
    behind_by: int
    files_changed: int


class CommitEntryDict(TypedDict):  # CORRECT-LOCAL: deployment-api response shape, not a domain contract
    """One commit in a branch's SHA history."""

    sha: str
    message: str
    author: str
    committed_at: str | None
    v2_conclusion: str | None  # quality-gates-v2 check conclusion for this sha (None = not run)


class BlockingCheckDict(TypedDict):  # CORRECT-LOCAL: deployment-api response shape, not a domain contract
    """A non-success required check on a stuck PR head — the on-screen blocker reason."""

    name: str  # the check/status context, e.g. "AWS CodeBuild ap-northeast-1 (deployment-service)"
    state: str  # failure | error | pending
    description: str  # GitHub's reason string, e.g. "Pull request approval required for starting a build"


class RepoPrDict(TypedDict, total=False):  # CORRECT-LOCAL: deployment-api response shape, not a domain contract
    """An open PR into a promotion base, with stuck classification."""

    repo: Required[str]
    number: Required[int]
    title: str
    base: str
    head: str
    url: str
    age_min: int
    auto_merge: bool
    merge_state: str  # GitHub mergeable_state (clean/blocked/dirty/behind/unstable/unknown)
    failed_check: bool
    v2_present: bool
    # base tree == head tree → nothing to promote (squash-accounting noise; safe to CLOSE).
    # When True, stuck_class is forced None so the PR never enters the triage queue.
    content_identical: bool
    stuck_class: StuckClass | None
    # Non-success required checks blocking the merge (classic status contexts like AWS
    # CodeBuild that the Actions-run rollup is blind to) — the on-screen "why is this stuck".
    blocking_checks: list[BlockingCheckDict]


class SitStateDict(TypedDict):  # CORRECT-LOCAL: deployment-api response shape, not a domain contract
    """Per-repo SIT visibility (operator add 2026-06-10 — stuck-in-SIT must be visible)."""

    in_breaking_pending: bool
    staging_locked: bool
    staging_locked_reason: str | None
    last_sit_run_status: str | None  # conclusion of the last cascade-qg-ordering run (PM repo)
    last_sit_run_age_min: int | None
    stuck_in_sit: bool


class ImageSignalDict(TypedDict):  # CORRECT-LOCAL: deployment-api response shape, not a domain contract
    """Image-level deploy signal (operator decision: v1 is image-level, not runtime-level)."""

    last_build_status: str | None
    last_build_sha: str | None
    last_build_time: str | None  # ISO-8601 of the last build's finish_time (B1 — when it built)
    last_build_log_url: str | None  # GCP Cloud Build / AWS CodeBuild console URL (B1 — click-through)
    # Last SUCCESSFUL build (operator add 2026-06-11) — so a red latest build doesn't hide the last
    # good image: "current build failed, what's the last sha that succeeded?". None = no success in window.
    last_success_sha: str | None
    last_success_time: str | None
    last_success_log_url: str | None
    deployed_version: str | None  # workspace-manifest.json deployed_versions[repo]
    image_stale: bool | None  # main HEAD sha != last successful build sha (None = unknown)


class SitJobDict(TypedDict):  # CORRECT-LOCAL: deployment-api response shape, not a domain contract
    """One job of the last cascade/SIT run (job name carries the repo)."""

    name: str
    status: str  # queued | in_progress | completed
    conclusion: str | None


class SitLastRunDict(TypedDict):  # CORRECT-LOCAL: deployment-api response shape, not a domain contract
    """The last cascade/SIT run, always visible (alert-parity principle, operator add
    2026-06-10): which repos were in it, which passed/failed, what's in progress."""

    url: str
    status: str
    conclusion: str | None
    age_min: int | None
    jobs: list[SitJobDict]


class LastGreenDict(TypedDict):  # CORRECT-LOCAL: deployment-api response shape, not a domain contract
    """The most-recent SHA on a branch whose quality-gates-v2 concluded success, plus when (N2 —
    "green as of <sha> · <age>"). Distinct from the branch HEAD, which may be red/pending. `at`
    is the successful run's completion time (ISO-8601), or the head's commit time when the head
    itself is green."""

    sha: str
    at: str


class CodebaseHealthDict(TypedDict, total=False):  # CORRECT-LOCAL: deployment-api response shape, not a domain contract
    """Per-repo QG health snapshot: coverage, failing gate, file-debt (Cov%/QG-reason/File-debt columns).

    Sourced from Firestore ``ci_status/{repo}.codebase_health`` (authoritative) with manifest
    ``repositories[repo].codebase_health`` as the offline fallback.  All fields are optional so
    absent / partial data still serialises cleanly — the frontend renders "—" for None.
    """

    coverage_pct: float | None  # pytest coverage percentage (0.0-100.0); None = not yet captured
    qg_red_reason: str | None  # e.g. "pytest" / "basedpyright" / "ruff" / "bandit"; None = passed
    large_file_count: int | None  # files >= 900 lines (hard QG limit)
    warn_file_count: int | None  # files 700..899 lines (warn zone)


class RepoOverviewDict(TypedDict):  # CORRECT-LOCAL: deployment-api response shape, not a domain contract
    """One row of the fleet overview matrix."""

    repo: str
    repo_type: str
    ci_status: str
    # Per-branch quality-gates-v2 conclusion (live-defi-rollout / staging / main) so the UI can
    # show WHICH branch is red — `ci_status` alone (a single manifest value) can't distinguish
    # "main red, LDR recovered (draining)" from "LDR actively broken". Keyed by branch name;
    # value is success/failure/in_progress/… or None when v2 never ran on that branch.
    branch_ci: dict[str, str | None]
    branches: list[BranchHeadDict]
    deltas: list[BranchDeltaDict]
    open_prs: list[RepoPrDict]
    sit: SitStateDict
    image: ImageSignalDict
    # Dual-cloud image status (side-by-side, no provider switch): per-cloud build signal so the UI
    # shows GCP + AWS at once. `image` stays the active-provider one (deployed_version / image_stale).
    # Omitted/None for a cloud whose build API isn't reachable (creds/role) → UI renders "—".
    image_gcp: NotRequired[ImageSignalDict | None]
    image_aws: NotRequired[ImageSignalDict | None]
    # N2: most-recent GREEN main sha + time ("green as of <sha> · <age>"), distinct from the head.
    last_green_main: LastGreenDict | None
    # G6: age (minutes) of the oldest DIVERGED FILE's last LDR change — the true promotion lag
    # (>60min pages). Files-based, NOT the commit graph (squash-originals + backmerge merge-nodes
    # phantom-age the oldest "ahead" commit to days). None when LDR is in sync with main (no lag).
    main_lag_age_min: int | None
    # Real (squash-free) commit count carrying the LDR→main delta — distinct last-setting commits of
    # the diverged files; the trustworthy replacement for the squash-inflated ahead_by in the UI.
    main_unpromoted_commits: int | None
    # promotion-drain follow-up: True when this repo has REAL file-content ahead of staging or main
    # (files_changed > 0, not squash skew) AND the corresponding global drain leg is failing/stale
    # (>45min = 3 missed 15-min ticks) — the bug-#11 class where content piles on LDR with a dead
    # drain, invisible today. False when content is draining healthily or there's no real delta.
    drain_stalled: bool
    # Slack↔/repos parity: True when this repo has an OPEN promotion PR (LDR→staging / LDR→main /
    # staging→main) stuck on a human-actionable BLOCKING class (conflicting / failing_check /
    # skip_ci_jammed) — the repo-level mirror of the Slack `quality-gates-v2 ... PR #N FAILED`
    # CRITICAL page. Distinct from drain_stalled (which ALSO requires real content ahead) and from
    # ci_status (the branch-push value, which stays MAIN_GREEN off the last green main push while a
    # promotion PR fails). promotion_blocked reads GitHub ground truth, so it can never go
    # stale-green and never disagrees with Slack. SSOT: ci_status_repos_promotion_failure_parity.
    promotion_blocked: bool
    # Dep-order promotion HOLD (mirrors staging-to-main.yml STAGE 1.8) — distinct from drain_stalled
    # (a stuck PR) and promotion_blocked (a failure quarantine). A tiered promotion holds this repo
    # until every dep is on main; blocked_by non-empty ⟺ HELD.
    tier: str  # dependency layer from manifest (stringified)
    blocked_by: list[DepBlockerDict]  # deps NOT on main holding THIS repo's promotion (empty = clear)
    blocking: list[str]  # repos held because THIS repo isn't on main yet (empty = not a blocker)
    # QG health snapshot for the Cov%/QG-reason/File-debt columns — None when not yet captured.
    codebase_health: CodebaseHealthDict | None
    # WS-L: promotion path from the manifest (e.g. "ldr_main" = promotes LDR→main directly,
    # skipping staging→main). None = default path (staging→main). The UI suppresses the
    # staging→main stall classification for "ldr_main" repos because staging being ahead of
    # main is the expected steady state, not a stall.
    promotion_model: NotRequired[str | None]


class RepoErrorDict(TypedDict):  # CORRECT-LOCAL: deployment-api response shape, not a domain contract
    """A repo whose row was dropped from the overview because its aggregation failed.

    Surfaced (operator add 2026-06-10) so a degraded row is VISIBLE, not silently dropped —
    a per-repo GitHub 5xx / timeout / client error degrades that one repo to an errors[]
    entry while the rest of the fleet renders (shard-level isolation)."""

    repo: str
    error: str


class DepBlockerDict(TypedDict):  # CORRECT-LOCAL: deployment-api response shape
    """A dependency not yet on main that holds a repo's staging→main promotion (dep-order)."""

    name: str  # dep repo, e.g. "unified-api-contracts"
    tier: str  # the dep's tier (manifest tier, stringified)
    ci_status: str  # the dep's current ci_status, e.g. "STAGING_GREEN"


class RootBlockerDict(TypedDict):  # CORRECT-LOCAL: deployment-api response shape
    """A not-on-main repo holding >=1 other repo from main — the cause of the dep-order hold."""

    repo: str
    tier: str
    ci_status: str
    blocking_count: int  # how many repos are held waiting on this one
    main_files_behind: int  # this repo's own staging-ahead-of-main files_changed (content stuck)


class PromotionHeldDict(TypedDict):  # CORRECT-LOCAL: deployment-api response shape
    """Aggregate for the 'Promotion held - dependency order' card + the stalled banner.
    Dep-order HOLDS (a clean wait), distinct from promotion_blocked (failure-quarantine)."""

    held_repos: list[str]  # repos currently held by dep-order
    root_blockers: list[RootBlockerDict]  # the not-on-main repos causing the holds, lowest tier first


class PromotionBlockedDict(TypedDict, total=False):  # CORRECT-LOCAL: deployment-api response shape
    """A repo parked out of the staging→main promotion (G1 — alert-parity for the
    staging-to-main genuine-failure CRITICAL page + the newly-quarantined WARNING).

    Sourced from manifest `promotion_failures` (`{repo: consecutive-fail count}`) and
    `promotion_quarantine` (`{repo: {since, attempts, escalated}}`). `quarantined` is True
    when the repo is in `promotion_quarantine`; `failures` is its consecutive-fail count."""

    repo: Required[str]
    failures: int
    quarantined: bool
    since: str
    attempts: int
    escalated: bool


class PromoteRunDict(TypedDict):  # CORRECT-LOCAL: deployment-api response shape, not a domain contract
    """One promote-drain workflow's most-recent run (LDR→staging or LDR→main). Distinct from the
    breaking cascade/SIT — this is the ROUTINE every-15-min drain (operator ask 2026-06-11: "when
    did we last pull LDR→staging via auto-merge + QG branch-protection, and did it pass")."""

    status: str  # queued | in_progress | completed
    conclusion: str | None  # success | failure | … | None while running
    age_min: int | None
    url: str


class PromotionDrainDict(TypedDict):  # CORRECT-LOCAL: deployment-api response shape, not a domain contract
    """The fleet-wide routine promotion drain (PM-central, every 15 min) — distinct from the
    breaking cascade/SIT panel. Both legs are global (one PM-central workflow each)."""

    ldr_to_staging: PromoteRunDict | None
    ldr_to_main: PromoteRunDict | None


class SemverHealthDict(TypedDict):  # CORRECT-LOCAL: deployment-api response shape, not a domain contract
    """Semver-agent standing health (G2 — the bump-rate circuit-breaker + dispatch-failure are
    CRITICAL pages with no UI element today). `last_run_*` is the most-recent `semver-agent.yml`
    run (one global GitHub-runs query); `pending_bump_*` + `breaker_armed` derive from the manifest
    version-surface (`staging_versions` ahead of `versions`). `breaker_armed` mirrors the agent's
    ≥3-pending-staging-bumps circuit-breaker threshold."""

    last_run_status: str  # queued | in_progress | completed
    last_run_conclusion: str | None  # success | failure | … | None while running
    last_run_age_min: int | None
    last_run_url: str
    pending_bump_count: int  # repos whose staging version is ahead of main (pending promotion)
    pending_bump_repos: list[str]
    breaker_armed: bool  # pending_bump_count >= breaker_threshold (the semver-agent breaker condition)
    breaker_threshold: int  # the ≥N-pending-bumps threshold the breaker pages on (3)


class OverviewResponseDict(TypedDict):  # CORRECT-LOCAL: deployment-api response shape, not a domain contract
    """GET /api/repo-ci/overview response."""

    generated_at: str
    source: str  # "live" | "mock"
    repos: list[RepoOverviewDict]
    stuck_prs: list[RepoPrDict]  # fleet-wide triage queue (operator add)
    stuck_in_sit: list[str]  # repos currently stuck in SIT (operator add)
    sit_last_run: SitLastRunDict | None  # live SIT run panel (alert-parity, operator add)
    errors: list[RepoErrorDict]  # repos dropped on aggregation failure (operator add — never silent)
    promotion_blocked: list[PromotionBlockedDict]  # repos parked out of staging→main (G1)
    # Routine LDR→staging / LDR→main promote drain (PM-central, every 15 min) — distinct from the
    # breaking cascade/SIT above (operator gap 2026-06-11). None when the drain runs can't be fetched.
    promotion_drain: PromotionDrainDict | None
    # Semver-agent standing health (G2) — last bump run + pending-bump count + breaker-armed flag.
    # None when the semver-agent run can't be fetched.
    semver_health: SemverHealthDict | None
    # Dep-order promotion holds (mirrors STAGE 1.8): held repos + the root not-on-main blockers
    # causing them. Distinct from promotion_blocked (failure-quarantine). None only if uncomputable.
    promotion_held: PromotionHeldDict | None


class BranchCommitsDict(TypedDict):  # CORRECT-LOCAL: deployment-api response shape, not a domain contract
    """SHA history for one branch in the drill-down."""

    branch: str
    commits: list[CommitEntryDict]


class RepoDetailResponseDict(TypedDict):  # CORRECT-LOCAL: deployment-api response shape, not a domain contract
    """GET /api/repo-ci/{repo}/detail response."""

    repo: str
    repo_type: str
    ci_status: str
    generated_at: str
    source: str
    branches: list[BranchHeadDict]
    deltas: list[BranchDeltaDict]
    history: list[BranchCommitsDict]
    open_prs: list[RepoPrDict]
    sit: SitStateDict
    image: ImageSignalDict
    # N2-followup: per-branch last-green (LDR / staging / main) — keyed by branch name, the
    # most-recent SHA on that branch whose quality-gates-v2 concluded success, or None when no
    # green run exists. The overview surfaces only the MAIN axis; the drilldown carries all three
    # (a green head uses the head, else one runs-API lookup — same budget-gated pattern).
    last_green: dict[str, LastGreenDict | None]


class FleetGitHealthProxyDict(TypedDict):  # CORRECT-LOCAL: deployment-api response shape, not a domain contract
    """GET /api/repo-ci/fleet-git-health — proxied agent-orchestrator fleet git-health.

    Operator decision v2 (2026-06-10): deployment-ui is the single devops pane, so
    deployment-api proxies the orchestrator's /api/fleet/git-health server-side. Honest
    degradation: when the orchestrator is unreachable or no token is configured,
    available=False + a reason; `orchestrator_url` always lets the UI deep-link to the
    orchestrator's own Fleet Git-Health page (the authoritative git-health surface).
    """

    available: bool
    reason: str  # "" when available; failure reason otherwise (honest, never fabricated)
    orchestrator_url: str  # always present → UI deep-link target (the AO UI)
    data: dict[str, object] | None  # the orchestrator FleetGitHealthResponse payload, or None


def _now_iso() -> str:
    """UTC now in ISO-8601 (shared by the live aggregator + mock fixtures)."""
    return dt.datetime.now(dt.UTC).isoformat()

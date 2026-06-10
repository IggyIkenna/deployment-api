"""
TypedDict shapes + constants for the repo-CI dashboard aggregator.

Plan: unified-trading-pm/plans/active/ci_dashboard_deployment_ui_2026_06_10.md (Phase 1).
Data architecture (operator decision 2026-06-10 — HYBRID): GitHub API live state behind a
short TTL cache + workspace-manifest.json for the repo registry / ci_status / staging_status.
"""

from __future__ import annotations

from typing import Literal, Required, TypedDict

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
    stuck_class: StuckClass | None


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


class RepoOverviewDict(TypedDict):  # CORRECT-LOCAL: deployment-api response shape, not a domain contract
    """One row of the fleet overview matrix."""

    repo: str
    repo_type: str
    ci_status: str
    branches: list[BranchHeadDict]
    deltas: list[BranchDeltaDict]
    open_prs: list[RepoPrDict]
    sit: SitStateDict
    image: ImageSignalDict


class RepoErrorDict(TypedDict):  # CORRECT-LOCAL: deployment-api response shape, not a domain contract
    """A repo whose row was dropped from the overview because its aggregation failed.

    Surfaced (operator add 2026-06-10) so a degraded row is VISIBLE, not silently dropped —
    a per-repo GitHub 5xx / timeout / client error degrades that one repo to an errors[]
    entry while the rest of the fleet renders (shard-level isolation)."""

    repo: str
    error: str


class OverviewResponseDict(TypedDict):  # CORRECT-LOCAL: deployment-api response shape, not a domain contract
    """GET /api/repo-ci/overview response."""

    generated_at: str
    source: str  # "live" | "mock"
    repos: list[RepoOverviewDict]
    stuck_prs: list[RepoPrDict]  # fleet-wide triage queue (operator add)
    stuck_in_sit: list[str]  # repos currently stuck in SIT (operator add)
    sit_last_run: SitLastRunDict | None  # live SIT run panel (alert-parity, operator add)
    errors: list[RepoErrorDict]  # repos dropped on aggregation failure (operator add — never silent)


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

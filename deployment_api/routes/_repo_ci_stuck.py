"""
Pure stuck-PR + stuck-in-SIT classification for the repo-CI dashboard.

The conditions are PORTED from unified-trading-pm/scripts/repo-management/
ci_failure_watcher.py (detect_stuck_prs + auto_recover_stuck_prs) — the watcher is the
SSOT for these signatures; this module mirrors them for the read surface and must not
fork the semantics. Network-free so every branch is unit-testable.

Plan: ci_dashboard_deployment_ui_2026_06_10.md Phase 1 (operator adds 2026-06-10:
stuck PRs first-class; stuck-in-SIT visible).
"""

from __future__ import annotations

from ._repo_ci_types import SitStateDict, StuckClass

# Mirrors ci_failure_watcher.py — REST `mergeable_state` is lowercase; the watcher's
# GraphQL mergeStateStatus is uppercase. Normalise to lowercase at the call edge.
STUCK_MERGE_STATES = frozenset({"conflicting", "dirty", "blocked"})
CONFLICT_MERGE_STATES = frozenset({"conflicting", "dirty"})

# Heads that are part of the promotion contract (LDR -> staging -> main). A stuck PR off
# one of these is a wedged promotion even without auto-merge (watcher _PROMOTION_HEADS).
PROMOTION_HEADS = frozenset({"live-defi-rollout", "staging"})

# GitHub's full CI-suppression token set — substring match ANYWHERE in the head message
# (watcher _SKIP_CI_MARKERS; even a descriptive mention suppresses, incident 2026-06-10).
SKIP_CI_MARKERS = ("[skip ci]", "[ci skip]", "[no ci]", "[skip actions]", "[actions skip]")

# Required check name (all repos) — CLAUDE.md § "CI Verification After Every Push".
V2_CHECK_NAME = "quality-gates-v2"

# Default thresholds.
DEFAULT_STUCK_MINUTES = 30
DEFAULT_SIT_STUCK_MINUTES = 120


def head_message_suppresses_ci(head_message: str) -> bool:
    """True if the PR head commit message carries any GitHub CI-suppression marker."""
    lowered = head_message.lower()
    return any(marker in lowered for marker in SKIP_CI_MARKERS)


def is_promotion_contract_pr(head_ref: str, auto_merge: bool) -> bool:
    """A PR counts for stuck-tracking iff auto-merge is armed OR its head is a promotion branch.

    Mirrors the watcher gate — otherwise the surface fills with abandoned feature PRs.
    """
    return auto_merge or head_ref in PROMOTION_HEADS


def classify_stuck_pr(
    *,
    merge_state: str,
    age_min: float,
    v2_present: bool,
    failed_check: bool,
    head_message: str,
    content_identical: bool = False,
    stuck_minutes: int = DEFAULT_STUCK_MINUTES,
) -> StuckClass | None:
    """Classify one promotion-contract PR into the closed stuck set (None = not stuck).

    Precedence (most-actionable first, mirroring the watcher's recovery mechanisms):
    0. content_identical— base tree == head tree: NOTHING to promote. The ahead_by /
                          mergeable_state (even CONFLICTING/DIRTY from a stale squash
                          merge-base) is history divergence, not content — squash-accounting
                          noise to CLOSE, not a wall. Short-circuits ALL stuck classes so the
                          triage queue never shows a phantom-stuck promote PR (the operator
                          "doing nothing faster than we clear it" illusion). SSOT:
                          promotion_queue_conflict_wall_pileup_2026_06_17.md § Class D.
    1. conflicting      — CONFLICTING/DIRTY: a worker must rebase (escalation class).
    2. failing_check    — BLOCKED with a genuinely FAILED required check: fix the content.
    3. skip_ci_jammed   — BLOCKED + v2 absent + [skip ci] head: close+reopen CANNOT re-fire
                          v2 (push AND pull_request suppressed); needs a superseding commit.
    4. v2_never_reported— BLOCKED + no failed check + v2 absent: deterministically
                          auto-recoverable (close+reopen re-fires pull_request).
    5. automerge_stuck  — in a stuck state past threshold with none of the above.
    """
    if content_identical:
        return None
    state = merge_state.lower()
    if state not in STUCK_MERGE_STATES:
        return None
    if age_min < stuck_minutes:
        return None
    if state in CONFLICT_MERGE_STATES:
        return "conflicting"
    # state == "blocked" from here.
    if failed_check:
        return "failing_check"
    if not v2_present:
        if head_message_suppresses_ci(head_message):
            return "skip_ci_jammed"
        return "v2_never_reported"
    return "automerge_stuck"


def derive_sit_state(
    *,
    repo: str,
    breaking_pending: list[str],
    staging_locked: bool,
    staging_locked_reason: str | None,
    last_sit_run_status: str | None,
    last_sit_run_age_min: int | None,
    sit_stuck_minutes: int = DEFAULT_SIT_STUCK_MINUTES,
) -> SitStateDict:
    """Derive per-repo SIT visibility (operator add 2026-06-10).

    stuck_in_sit fires when the repo is queued for SIT (`staging_status.breaking_pending`)
    but the SIT/cascade machinery is not visibly progressing: no cascade run at all, the
    last run is older than the threshold, or the last run FAILED. This makes the
    cascade-evicted / jammed-cascade failure class visible instead of inferable.
    """
    in_pending = repo in breaking_pending
    stale_run = last_sit_run_age_min is None or last_sit_run_age_min > sit_stuck_minutes
    failed_run = last_sit_run_status in {"failure", "startup_failure", "timed_out", "cancelled"}
    return SitStateDict(
        in_breaking_pending=in_pending,
        staging_locked=staging_locked,
        staging_locked_reason=staging_locked_reason if staging_locked else None,
        last_sit_run_status=last_sit_run_status,
        last_sit_run_age_min=last_sit_run_age_min,
        stuck_in_sit=in_pending and (stale_run or failed_run),
    )

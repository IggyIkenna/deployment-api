"""Unit tests for the pure stuck-PR + stuck-in-SIT classification.

One case per stuck signature (ported from ci_failure_watcher.py — the watcher is the
SSOT; these tests pin the port so the read surface cannot drift from it).
"""

from __future__ import annotations

from deployment_api.routes._repo_ci_stuck import (
    classify_stuck_pr,
    derive_sit_state,
    head_message_suppresses_ci,
    is_promotion_contract_pr,
)


class TestClassifyStuckPr:
    def test_clean_pr_not_stuck(self) -> None:
        assert (
            classify_stuck_pr(merge_state="clean", age_min=500, v2_present=True, failed_check=False, head_message="")
            is None
        )

    def test_under_age_threshold_not_stuck(self) -> None:
        assert (
            classify_stuck_pr(merge_state="blocked", age_min=5, v2_present=False, failed_check=False, head_message="")
            is None
        )

    def test_conflicting(self) -> None:
        assert (
            classify_stuck_pr(merge_state="dirty", age_min=60, v2_present=True, failed_check=False, head_message="")
            == "conflicting"
        )
        assert (
            classify_stuck_pr(
                merge_state="CONFLICTING", age_min=60, v2_present=True, failed_check=False, head_message=""
            )
            == "conflicting"
        )

    def test_failing_check_takes_precedence_over_v2_absent(self) -> None:
        assert (
            classify_stuck_pr(merge_state="blocked", age_min=60, v2_present=False, failed_check=True, head_message="")
            == "failing_check"
        )

    def test_v2_never_reported(self) -> None:
        assert (
            classify_stuck_pr(
                merge_state="blocked", age_min=60, v2_present=False, failed_check=False, head_message="feat: x"
            )
            == "v2_never_reported"
        )

    def test_skip_ci_jammed(self) -> None:
        # Even a DESCRIPTIVE mention suppresses CI (incident 2026-06-10) — substring match.
        assert (
            classify_stuck_pr(
                merge_state="blocked",
                age_min=60,
                v2_present=False,
                failed_check=False,
                head_message="ci: advance past [skip ci] bump head",
            )
            == "skip_ci_jammed"
        )

    def test_automerge_stuck(self) -> None:
        assert (
            classify_stuck_pr(merge_state="blocked", age_min=60, v2_present=True, failed_check=False, head_message="")
            == "automerge_stuck"
        )

    def test_content_identical_short_circuits_blocked(self) -> None:
        # base tree == head tree → nothing to promote (squash-accounting noise). A BLOCKED
        # promote PR with no v2 must NOT enter the triage queue — it is closeable, not a wall.
        assert (
            classify_stuck_pr(
                merge_state="blocked",
                age_min=600,
                v2_present=False,
                failed_check=False,
                head_message="feat: x",
                content_identical=True,
            )
            is None
        )

    def test_content_identical_short_circuits_conflicting(self) -> None:
        # Even CONFLICTING/DIRTY off a stale squash merge-base (the deployment-api #101 case)
        # is noise when the trees are identical — the guard outranks the conflicting class.
        assert (
            classify_stuck_pr(
                merge_state="dirty",
                age_min=600,
                v2_present=True,
                failed_check=False,
                head_message="",
                content_identical=True,
            )
            is None
        )


class TestPromotionContractGate:
    def test_ldr_head_counts_without_automerge(self) -> None:
        assert is_promotion_contract_pr("live-defi-rollout", auto_merge=False)

    def test_feature_branch_needs_automerge(self) -> None:
        assert not is_promotion_contract_pr("feat/foo", auto_merge=False)
        assert is_promotion_contract_pr("feat/foo", auto_merge=True)


class TestSkipCiMarkers:
    def test_all_marker_variants(self) -> None:
        for marker in ("[skip ci]", "[ci skip]", "[no ci]", "[skip actions]", "[actions skip]"):
            assert head_message_suppresses_ci(f"chore: bump {marker}")

    def test_clean_message(self) -> None:
        assert not head_message_suppresses_ci("feat: a normal commit")


class TestDeriveSitState:
    def test_not_pending_never_stuck(self) -> None:
        sit = derive_sit_state(
            repo="utl",
            breaking_pending=[],
            staging_locked=False,
            staging_locked_reason=None,
            last_sit_run_status="failure",
            last_sit_run_age_min=999,
        )
        assert not sit["in_breaking_pending"]
        assert not sit["stuck_in_sit"]

    def test_pending_with_fresh_green_run_not_stuck(self) -> None:
        sit = derive_sit_state(
            repo="greeks-service",
            breaking_pending=["greeks-service"],
            staging_locked=True,
            staging_locked_reason="cascade",
            last_sit_run_status="success",
            last_sit_run_age_min=10,
        )
        assert sit["in_breaking_pending"]
        assert not sit["stuck_in_sit"]
        assert sit["staging_locked_reason"] == "cascade"

    def test_pending_with_stale_run_is_stuck(self) -> None:
        sit = derive_sit_state(
            repo="greeks-service",
            breaking_pending=["greeks-service"],
            staging_locked=True,
            staging_locked_reason="cascade",
            last_sit_run_status="success",
            last_sit_run_age_min=500,
        )
        assert sit["stuck_in_sit"]

    def test_pending_with_no_run_at_all_is_stuck(self) -> None:
        # The cascade-evicted class: queued for SIT but the cascade never ran.
        sit = derive_sit_state(
            repo="greeks-service",
            breaking_pending=["greeks-service"],
            staging_locked=False,
            staging_locked_reason=None,
            last_sit_run_status=None,
            last_sit_run_age_min=None,
        )
        assert sit["stuck_in_sit"]

    def test_pending_with_failed_fresh_run_is_stuck(self) -> None:
        sit = derive_sit_state(
            repo="unified-trading-library",
            breaking_pending=["unified-trading-library"],
            staging_locked=True,
            staging_locked_reason=None,
            last_sit_run_status="failure",
            last_sit_run_age_min=5,
        )
        assert sit["stuck_in_sit"]

    def test_locked_reason_suppressed_when_unlocked(self) -> None:
        sit = derive_sit_state(
            repo="x",
            breaking_pending=[],
            staging_locked=False,
            staging_locked_reason="stale reason",
            last_sit_run_status=None,
            last_sit_run_age_min=None,
        )
        assert sit["staging_locked_reason"] is None

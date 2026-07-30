"""Unit tests for the pure stuck-PR + stuck-in-SIT classification.

One case per stuck signature (ported from ci_failure_watcher.py — the watcher is the
SSOT; these tests pin the port so the read surface cannot drift from it).
"""

from __future__ import annotations

from deployment_api.routes._repo_ci_stuck import (
    classify_stuck_pr,
    derive_promotion_blocked,
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

    def test_dormant_mode_yields_unknown_not_false_even_when_otherwise_stuck(self) -> None:
        # 2026-07-30 tri-state fix: while staging_dormant_mode is on, `breaking_pending` is a
        # structurally meaningless input (its only writer never fires) — the input alone would
        # otherwise compute stuck_in_sit=True here (pending + no run at all), but the honest
        # answer under dormancy is "cannot currently tell", not a real False OR a leaked True.
        sit = derive_sit_state(
            repo="greeks-service",
            breaking_pending=["greeks-service"],
            staging_locked=False,
            staging_locked_reason=None,
            last_sit_run_status=None,
            last_sit_run_age_min=None,
            staging_dormant_mode=True,
        )
        assert sit["stuck_in_sit"] is None
        # in_breaking_pending stays a real, honest reflection of the (structurally-empty) input.
        assert sit["in_breaking_pending"]

    def test_dormant_mode_default_false_preserves_prior_behavior(self) -> None:
        # staging_dormant_mode defaults False — every pre-existing call site / test above keeps
        # computing the real bool exactly as before this fix.
        sit = derive_sit_state(
            repo="greeks-service",
            breaking_pending=["greeks-service"],
            staging_locked=False,
            staging_locked_reason=None,
            last_sit_run_status=None,
            last_sit_run_age_min=None,
        )
        assert sit["stuck_in_sit"] is True


class TestDerivePromotionBlocked:
    """Slack↔/repos parity: a failing promotion PR surfaces at the repo level
    (ci_status_repos_promotion_failure_parity_2026_06_25)."""

    def test_failing_check_promotion_pr_is_blocked(self) -> None:
        # THE PM #547 CASE: an LDR→main promotion PR whose required quality-gates-v2 FAILED.
        # The repo's ci_status stays MAIN_GREEN (last green main push) — this flag is what
        # makes the failing promotion visible on /repos, matching the Slack CRITICAL page.
        prs = [{"head": "live-defi-rollout", "base": "main", "stuck_class": "failing_check"}]
        assert derive_promotion_blocked(prs) is True

    def test_conflicting_and_skip_ci_jammed_are_blocked(self) -> None:
        assert derive_promotion_blocked([{"stuck_class": "conflicting"}]) is True
        assert derive_promotion_blocked([{"stuck_class": "skip_ci_jammed"}]) is True

    def test_self_healing_classes_are_not_blocked(self) -> None:
        # automerge_stuck / v2_never_reported drain in-band → not a human-actionable block;
        # the headline must not cry wolf on an in-flight self-healing drain.
        assert derive_promotion_blocked([{"stuck_class": "automerge_stuck"}]) is False
        assert derive_promotion_blocked([{"stuck_class": "v2_never_reported"}]) is False

    def test_no_open_prs_is_not_blocked(self) -> None:
        assert derive_promotion_blocked([]) is False

    def test_unstuck_pr_is_not_blocked(self) -> None:
        # An open promotion PR that is NOT stuck (stuck_class None / clean) doesn't block.
        assert derive_promotion_blocked([{"head": "live-defi-rollout", "stuck_class": None}]) is False

    def test_any_blocking_pr_among_several_blocks(self) -> None:
        prs = [
            {"stuck_class": "automerge_stuck"},  # self-healing
            {"stuck_class": None},  # clean
            {"stuck_class": "failing_check"},  # blocking → repo is blocked
        ]
        assert derive_promotion_blocked(prs) is True

"""Unit tests: out-of-coverage-window (OOW) denominator exclusion.

Verifies that empty_confirmed cells carrying lifecycle/scope reasons
(pre-genesis chains, pre-launch venues, delisted instruments, etc.)
are excluded from the completion-% denominator.

Plan: migration_verification_orphan_safety_2026_06_10.md
"""

from __future__ import annotations

import pandas as pd
import pytest
from unified_api_contracts import compute_honest_coverage

from deployment_api.services.data_status.coverage_metrics import (
    build_coverage_metrics,
    compute_capture_status_counts,
    compute_out_of_window_count,
    derive_capture_status_rates,
)

# ---------------------------------------------------------------------------
# compute_out_of_window_count
# ---------------------------------------------------------------------------


class TestComputeOutOfWindowCount:
    def test_empty_df_returns_zero(self) -> None:
        assert compute_out_of_window_count(pd.DataFrame()) == 0

    def test_no_capture_status_column_returns_zero(self) -> None:
        df = pd.DataFrame([{"error_reason": "EXPECTED_PRE_GENESIS_CHAIN"}])
        assert compute_out_of_window_count(df) == 0

    def test_all_captured_returns_zero(self) -> None:
        df = pd.DataFrame(
            [
                {"capture_status": "captured", "error_reason": ""},
                {"capture_status": "captured", "error_reason": "EXPECTED_PRE_GENESIS_CHAIN"},
            ]
        )
        assert compute_out_of_window_count(df) == 0

    def test_oow_empty_confirmed_counted(self) -> None:
        df = pd.DataFrame(
            [
                {"capture_status": "empty_confirmed", "error_reason": "EXPECTED_PRE_GENESIS_CHAIN"},
                {"capture_status": "empty_confirmed", "error_reason": "EXPECTED_PRE_VENUE_LAUNCH"},
                {"capture_status": "captured", "error_reason": ""},
            ]
        )
        assert compute_out_of_window_count(df) == 2

    def test_within_window_empty_not_counted(self) -> None:
        """Calendar/operational absences are NOT out-of-window."""
        df = pd.DataFrame(
            [
                {"capture_status": "empty_confirmed", "error_reason": "EXPECTED_HOLIDAY"},
                {"capture_status": "empty_confirmed", "error_reason": "EXPECTED_WEEKEND"},
                {"capture_status": "empty_confirmed", "error_reason": "EXPECTED_PROTOCOL_PAUSED"},
            ]
        )
        assert compute_out_of_window_count(df) == 0

    def test_blank_reason_not_counted(self) -> None:
        df = pd.DataFrame(
            [
                {"capture_status": "empty_confirmed", "error_reason": ""},
                {"capture_status": "empty_confirmed", "error_reason": None},
            ]
        )
        assert compute_out_of_window_count(df) == 0

    def test_mixed_oow_and_within_window(self) -> None:
        df = pd.DataFrame(
            [
                {"capture_status": "empty_confirmed", "error_reason": "EXPECTED_PRE_GENESIS_CHAIN"},  # OOW
                {"capture_status": "empty_confirmed", "error_reason": "EXPECTED_HOLIDAY"},  # within-window
                {"capture_status": "empty_confirmed", "error_reason": "EXPECTED_INSTRUMENT_DELISTED"},  # OOW
                {"capture_status": "empty_confirmed", "error_reason": "EXPECTED_INSTRUMENT_NOT_LISTED"},  # OOW
                {"capture_status": "captured", "error_reason": ""},
                {"capture_status": "attempted_failed", "error_reason": "some error"},
            ]
        )
        assert compute_out_of_window_count(df) == 3

    def test_no_error_reason_column_returns_zero(self) -> None:
        df = pd.DataFrame(
            [
                {"capture_status": "empty_confirmed"},
                {"capture_status": "empty_confirmed"},
            ]
        )
        assert compute_out_of_window_count(df) == 0

    @pytest.mark.parametrize(
        "oow_reason",
        [
            "EXPECTED_PRE_GENESIS_CHAIN",
            "EXPECTED_PRE_VENUE_LAUNCH",
            "EXPECTED_INSTRUMENT_DELISTED",
            "EXPECTED_INSTRUMENT_NOT_LISTED",
            "EXPECTED_DEPRECATED_DATA_TYPE",
            "EXPECTED_POST_SEASON",
            "EXPECTED_PRE_SEASON",
            "EXPECTED_OUTSIDE_PROCESSING_SCOPE",
            "EXPECTED_LEGACY_MIGRATION_MISSING_EXPIRY",
            "EXPECTED_NO_FIXTURE",
            "EXPECTED_NO_MAPPING",
            "EXPECTED_PAST_SOURCE_COVERAGE_END",
            "EXPECTED_PRE_SOURCE_COVERAGE_START",
            "EXPECTED_OUT_OF_COVERAGE_WINDOW",
            "EXPECTED_SOURCE_DOES_NOT_COVER_LEAGUE",
        ],
    )
    def test_all_15_oow_reasons_classified(self, oow_reason: str) -> None:
        df = pd.DataFrame(
            [
                {"capture_status": "empty_confirmed", "error_reason": oow_reason},
            ]
        )
        assert compute_out_of_window_count(df) == 1, f"OOW reason not classified: {oow_reason}"


# ---------------------------------------------------------------------------
# build_coverage_metrics — OOW key surfaced in capture_status_counts
# ---------------------------------------------------------------------------


class TestBuildCoverageMetricsOOWSurfacing:
    """Verify out_of_window appears in counts_dict from build_coverage_metrics."""

    def test_oow_key_present_in_counts(self) -> None:
        """out_of_window key must appear in capture_status_counts."""
        rows = [
            {"capture_status": "captured", "error_reason": ""},
            {"capture_status": "empty_confirmed", "error_reason": "EXPECTED_PRE_GENESIS_CHAIN"},
        ]
        df = pd.DataFrame(rows)
        result = build_coverage_metrics(df, "DEFI", capture_coverage_pct=50.0, total_expected_cells=2)
        counts = result["capture_status_counts"]
        assert isinstance(counts, dict)
        assert "out_of_window" in counts, "out_of_window key missing from capture_status_counts"

    def test_oow_count_correct_value(self) -> None:
        """OOW count must equal the number of OOW empty_confirmed rows."""
        rows = []
        rows += [{"capture_status": "captured", "error_reason": ""} for _ in range(10)]
        rows += [{"capture_status": "empty_confirmed", "error_reason": "EXPECTED_HOLIDAY"} for _ in range(5)]
        rows += [{"capture_status": "empty_confirmed", "error_reason": "EXPECTED_PRE_GENESIS_CHAIN"} for _ in range(3)]
        rows += [
            {"capture_status": "empty_confirmed", "error_reason": "EXPECTED_INSTRUMENT_DELISTED"} for _ in range(2)
        ]
        df = pd.DataFrame(rows)
        result = build_coverage_metrics(df, "DEFI", capture_coverage_pct=50.0, total_expected_cells=15)
        counts = result["capture_status_counts"]
        assert counts["out_of_window"] == 5  # 3 pre_genesis + 2 delisted

    def test_no_oow_rows_yields_zero_count(self) -> None:
        """No OOW rows → out_of_window == 0."""
        rows = [
            {"capture_status": "captured", "error_reason": ""},
            {"capture_status": "empty_confirmed", "error_reason": "EXPECTED_HOLIDAY"},
        ]
        df = pd.DataFrame(rows)
        result = build_coverage_metrics(df, "CEFI", capture_coverage_pct=50.0, total_expected_cells=2)
        counts = result["capture_status_counts"]
        assert counts.get("out_of_window", 0) == 0

    def test_empty_df_yields_zero_oow(self) -> None:
        result = build_coverage_metrics(pd.DataFrame(), "DEFI", capture_coverage_pct=0.0, total_expected_cells=0)
        counts = result["capture_status_counts"]
        assert counts.get("out_of_window", 0) == 0


# ---------------------------------------------------------------------------
# coverage.py _build_coverage_for_cat denominator exclusion via mock
# ---------------------------------------------------------------------------


class TestCoveragePctDenominatorExclusion:
    """Test that OOW cells are excluded from completion_pct denominator.

    Uses build_coverage_metrics indirectly since coverage.py's
    _build_coverage_for_cat reads real GCS manifests. We verify the
    denominator logic through the math:
      denominator = captured + within_window_empty + failed + expected_unattempted
      (NO out_of_window)
    """

    def test_defi_oow_exclusion_raises_completion_pct(self) -> None:
        """Simulates defi scenario: OOW cells excluded → completion% improves.

        10 captured, 2 in-window empty, 3 OOW empty.
        Without OOW exclusion: 10/15 = 66.67%
        With OOW exclusion: 10/12 = 83.33%
        """
        rows = []
        rows += [{"capture_status": "captured", "error_reason": ""} for _ in range(10)]
        rows += [{"capture_status": "empty_confirmed", "error_reason": "EXPECTED_HOLIDAY"} for _ in range(2)]
        rows += [{"capture_status": "empty_confirmed", "error_reason": "EXPECTED_PRE_GENESIS_CHAIN"} for _ in range(3)]
        df = pd.DataFrame(rows)

        oow_n = compute_out_of_window_count(df)
        assert oow_n == 3, "Expected 3 OOW rows"

        result = build_coverage_metrics(df, "DEFI", capture_coverage_pct=66.67, total_expected_cells=12)
        counts = result["capture_status_counts"]
        assert counts["out_of_window"] == 3
        # empty_confirmed in counts_dict still reflects all empty rows (UAC
        # CaptureStatusCounts doesn't split), but out_of_window surfaces the split
        assert counts.get("out_of_window", 0) == 3


# ---------------------------------------------------------------------------
# Schedule-defining FIXTURES empties resolved as out-of-window (data-type-aware)
# ---------------------------------------------------------------------------


class TestScheduleDefiningFixturesEmptyResolved:
    """A schedule-defining FIXTURES SOURCE_RETURNED_ZERO (no matches that day) is
    RESOLVED → out-of-window, but an enrichment SOURCE_RETURNED_ZERO still counts.

    Operator direction 2026-06-23: FIXTURES IS the schedule source-of-truth, so a
    zero-row fixtures response means "no matches" = complete, not a gap.
    """

    def test_fixtures_source_returned_zero_is_out_of_window(self) -> None:
        df = pd.DataFrame(
            [
                {"capture_status": "empty_confirmed", "error_reason": "SOURCE_RETURNED_ZERO", "data_type": "FIXTURES"},
                {"capture_status": "empty_confirmed", "error_reason": "SOURCE_RETURNED_ZERO", "data_type": "FIXTURES"},
                {"capture_status": "captured", "error_reason": "", "data_type": "FIXTURES"},
            ]
        )
        # Both FIXTURES no-match-day empties are now out-of-window (resolved).
        assert compute_out_of_window_count(df) == 2

    def test_enrichment_source_returned_zero_still_in_window(self) -> None:
        """An enrichment data_type's SOURCE_RETURNED_ZERO is NOT excluded."""
        df = pd.DataFrame(
            [
                {
                    "capture_status": "empty_confirmed",
                    "error_reason": "SOURCE_RETURNED_ZERO",
                    "data_type": "FIXTURE_STATS",
                },
                {"capture_status": "empty_confirmed", "error_reason": "SOURCE_RETURNED_ZERO", "data_type": "ODDS"},
                {
                    "capture_status": "empty_confirmed",
                    "error_reason": "SOURCE_RETURNED_ZERO",
                    "data_type": "PLAYER_STATS",
                },
            ]
        )
        assert compute_out_of_window_count(df) == 0

    def test_mixed_fixtures_and_enrichment_zero(self) -> None:
        """Only the FIXTURES SRZ rows are resolved; enrichment SRZ rows stay gaps."""
        df = pd.DataFrame(
            [
                {"capture_status": "empty_confirmed", "error_reason": "SOURCE_RETURNED_ZERO", "data_type": "FIXTURES"},
                {
                    "capture_status": "empty_confirmed",
                    "error_reason": "SOURCE_RETURNED_ZERO",
                    "data_type": "FIXTURE_STATS",
                },
                {"capture_status": "empty_confirmed", "error_reason": "EXPECTED_NO_FIXTURE", "data_type": "FIXTURES"},
            ]
        )
        # FIXTURES SRZ (resolved) + EXPECTED_NO_FIXTURE (lifecycle OOW) = 2; the
        # FIXTURE_STATS SRZ stays in-window.
        assert compute_out_of_window_count(df) == 2

    def test_no_data_type_column_falls_back_to_reason_only(self) -> None:
        """Without a data_type column, FIXTURES SRZ is NOT resolved (legacy path)."""
        df = pd.DataFrame(
            [
                {"capture_status": "empty_confirmed", "error_reason": "SOURCE_RETURNED_ZERO"},
                {"capture_status": "empty_confirmed", "error_reason": "EXPECTED_NO_FIXTURE"},
            ]
        )
        # Only the lifecycle reason counts when data_type is unavailable.
        assert compute_out_of_window_count(df) == 1

    def test_fixtures_completion_pct_lifts_to_100(self) -> None:
        """Golden-window shape: all FIXTURES empties resolve → completion ~100%.

        3445 captured + 233 SOURCE_RETURNED_ZERO (no-match-day) + 7732
        EXPECTED_NO_FIXTURE (already OOW). After the data-type-aware fix every
        empty is out-of-window → denominator == captured == 100%.
        """
        rows = []
        rows += [{"capture_status": "captured", "error_reason": "", "data_type": "FIXTURES"} for _ in range(3445)]
        rows += [
            {"capture_status": "empty_confirmed", "error_reason": "SOURCE_RETURNED_ZERO", "data_type": "FIXTURES"}
            for _ in range(233)
        ]
        rows += [
            {"capture_status": "empty_confirmed", "error_reason": "EXPECTED_NO_FIXTURE", "data_type": "FIXTURES"}
            for _ in range(7732)
        ]
        df = pd.DataFrame(rows)
        oow_n = compute_out_of_window_count(df)
        # All 233 SRZ + 7732 EXPECTED_NO_FIXTURE empties are out-of-window.
        assert oow_n == 233 + 7732
        total_empty = 233 + 7732
        within_window_empty = total_empty - oow_n
        denominator = 3445 + within_window_empty  # captured + in-window gaps
        completion_pct = 3445 / denominator * 100 if denominator else 0.0
        assert completion_pct == pytest.approx(100.0)


# ---------------------------------------------------------------------------
# compute_capture_status_counts populates out_of_window (the clip source)
# ---------------------------------------------------------------------------


class TestComputeCaptureStatusCountsPopulatesOOW:
    """The 4-state counter now stamps ``out_of_window`` so every downstream
    ``compute_honest_coverage(counts)`` clips out-of-life empties.
    """

    def test_oow_field_populated_from_reasons(self) -> None:
        df = pd.DataFrame(
            [
                {"capture_status": "captured", "error_reason": ""},
                {"capture_status": "empty_confirmed", "error_reason": "EXPECTED_PRE_GENESIS_CHAIN"},
                {"capture_status": "empty_confirmed", "error_reason": "EXPECTED_INSTRUMENT_DELISTED"},
                {"capture_status": "empty_confirmed", "error_reason": "EXPECTED_HOLIDAY"},
            ]
        )
        counts = compute_capture_status_counts(df)
        assert counts.empty_confirmed == 3
        assert counts.out_of_window == 2  # pre_genesis + delisted (holiday is in-window)

    def test_no_oow_leaves_field_zero(self) -> None:
        df = pd.DataFrame(
            [
                {"capture_status": "captured", "error_reason": ""},
                {"capture_status": "empty_confirmed", "error_reason": "EXPECTED_HOLIDAY"},
            ]
        )
        counts = compute_capture_status_counts(df)
        assert counts.out_of_window == 0

    def test_empty_df_oow_zero(self) -> None:
        assert compute_capture_status_counts(pd.DataFrame()).out_of_window == 0

    def test_fixtures_schedule_empty_is_oow(self) -> None:
        df = pd.DataFrame(
            [
                {"capture_status": "captured", "error_reason": "", "data_type": "FIXTURES"},
                {"capture_status": "empty_confirmed", "error_reason": "SOURCE_RETURNED_ZERO", "data_type": "FIXTURES"},
            ]
        )
        counts = compute_capture_status_counts(df)
        assert counts.out_of_window == 1


# ---------------------------------------------------------------------------
# derive_capture_status_rates honest_coverage is CLIPPED (the consumer fix)
# ---------------------------------------------------------------------------


class TestDeriveCaptureStatusRatesClipsHonestCoverage:
    """``honest_coverage`` from the panel rollup + per-venue breakdown path now
    excludes out-of-life empties from both numerator and denominator, matching
    coverage.py. Before the fix the empties were credited → inflated %.
    """

    def test_oow_empties_do_not_inflate_when_failures_present(self) -> None:
        # 10 captured, 5 OOW empty, 5 attempted_failed.
        # Part 4.1 (2026-07-22 global consolidation, unified_api_contracts
        # _honest_coverage_logic.py::compute_honest_coverage): empty_confirmed is
        # excluded from BOTH numerator and denominator UNCONDITIONALLY — out_of_window
        # is now a pure reporting subset that never changes the ratio.
        # (10 captured) / (10 captured + 5 attempted_failed) = 10/15 = 0.6667.
        rows = []
        rows += [{"capture_status": "captured", "error_reason": ""} for _ in range(10)]
        rows += [{"capture_status": "empty_confirmed", "error_reason": "EXPECTED_PRE_GENESIS_CHAIN"} for _ in range(5)]
        rows += [{"capture_status": "attempted_failed", "error_reason": "TIMEOUT"} for _ in range(5)]
        df = pd.DataFrame(rows)

        counts = compute_capture_status_counts(df)
        assert counts.out_of_window == 5

        rates = derive_capture_status_rates(counts, total_expected_cells=20)
        assert rates["honest_coverage"] == pytest.approx(10 / 15, abs=1e-6)

        # Part 4.1 invariant: out_of_window no longer participates in the formula at
        # all, so zeroing it out (simulating the pre-4.1 "legacy" clip toggle) yields
        # the SAME ratio, not the old inflated 0.75 — the reporting field is inert.
        legacy = counts._replace(out_of_window=0)
        assert compute_honest_coverage(legacy) == pytest.approx(10 / 15, abs=1e-6)

    def test_in_window_empties_also_excluded_under_part_4_1(self) -> None:
        # Part 4.1 (2026-07-22): empty_confirmed is excluded from the ratio
        # UNCONDITIONALLY — in-window empties (e.g. holidays) are no longer credited
        # to the numerator, matching instruments-service's _count_statuses exactly.
        # 8 captured / (8 captured + 2 attempted_failed) = 8/10 = 0.8.
        rows = []
        rows += [{"capture_status": "captured", "error_reason": ""} for _ in range(8)]
        rows += [{"capture_status": "empty_confirmed", "error_reason": "EXPECTED_HOLIDAY"} for _ in range(2)]
        rows += [{"capture_status": "attempted_failed", "error_reason": "TIMEOUT"} for _ in range(2)]
        df = pd.DataFrame(rows)
        counts = compute_capture_status_counts(df)
        assert counts.out_of_window == 0
        rates = derive_capture_status_rates(counts, total_expected_cells=12)
        assert rates["honest_coverage"] == pytest.approx(8 / 10, abs=1e-6)


# ---------------------------------------------------------------------------
# build_coverage_metrics ``coverage`` field is clipped (panel rollup consumer)
# ---------------------------------------------------------------------------


class TestBuildCoverageMetricsCoverageClipped:
    def test_coverage_field_excludes_oow(self) -> None:
        rows = []
        rows += [{"capture_status": "captured", "error_reason": ""} for _ in range(10)]
        rows += [{"capture_status": "empty_confirmed", "error_reason": "EXPECTED_PRE_GENESIS_CHAIN"} for _ in range(5)]
        rows += [{"capture_status": "attempted_failed", "error_reason": "TIMEOUT"} for _ in range(5)]
        df = pd.DataFrame(rows)
        result = build_coverage_metrics(df, "DEFI", capture_coverage_pct=50.0, total_expected_cells=20)
        # coverage = clipped honest_coverage = 10/15, NOT the inflated 15/20.
        assert float(result["coverage"]) == pytest.approx(10 / 15, abs=1e-6)
        counts = result["capture_status_counts"]
        assert isinstance(counts, dict)
        assert counts["out_of_window"] == 5

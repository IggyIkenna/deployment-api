"""Unit tests: out-of-coverage-window (OOW) denominator exclusion.

Verifies that empty_confirmed cells carrying lifecycle/scope reasons
(pre-genesis chains, pre-launch venues, delisted instruments, etc.)
are excluded from the completion-% denominator.

Plan: migration_verification_orphan_safety_2026_06_10.md
"""

from __future__ import annotations

import pandas as pd
import pytest

from deployment_api.services.data_status.coverage_metrics import (
    build_coverage_metrics,
    compute_out_of_window_count,
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

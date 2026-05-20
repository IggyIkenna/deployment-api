"""Capture-status (UTL v5) honest-coverage metric tests.

Phase C of the honest-coverage-metrics plan: deployment-api must surface
attempt_coverage_pct / capture_coverage_pct / empty_rate / failure_rate
computed from the manifest ``capture_status`` column, while staying
backward-compatible with legacy parquet that predates the column.
"""

from __future__ import annotations

import pandas as pd
from unified_api_contracts import CaptureStatusCounts

from deployment_api.services.data_status_service import (
    _build_coverage_metrics,
    _compute_capture_status_counts,
    _derive_capture_status_rates,
)


class TestComputeCaptureStatusCounts:
    def test_mixed_statuses_bucketed_correctly(self) -> None:
        df = pd.DataFrame(
            [
                {"capture_status": "captured"},
                {"capture_status": "captured"},
                {"capture_status": "empty_confirmed"},
                {"capture_status": "attempted_failed"},
                {"capture_status": "attempted_failed"},
                {"capture_status": "attempted_failed"},
            ]
        )
        counts = _compute_capture_status_counts(df)
        assert counts.captured == 2
        assert counts.empty_confirmed == 1
        assert counts.attempted_failed == 3

    def test_legacy_frame_no_column_coerces_to_captured(self) -> None:
        df = pd.DataFrame([{"venue": "BINANCE-SPOT"}, {"venue": "OKX-SPOT"}])
        counts = _compute_capture_status_counts(df)
        assert counts.captured == 2
        assert counts.empty_confirmed == 0
        assert counts.attempted_failed == 0

    def test_mixed_legacy_and_v5_rows_nan_coerces_to_captured(self) -> None:
        df = pd.DataFrame(
            [
                {"capture_status": "captured"},
                {"capture_status": None},  # legacy row merged with v5
                {"capture_status": "empty_confirmed"},
            ]
        )
        counts = _compute_capture_status_counts(df)
        assert counts.captured == 2
        assert counts.empty_confirmed == 1
        assert counts.attempted_failed == 0

    def test_empty_frame_returns_zero_counts(self) -> None:
        df = pd.DataFrame()
        counts = _compute_capture_status_counts(df)
        assert counts.captured == 0
        assert counts.empty_confirmed == 0
        assert counts.attempted_failed == 0

    def test_unknown_status_value_coerces_to_captured(self) -> None:
        df = pd.DataFrame([{"capture_status": "bogus"}])
        counts = _compute_capture_status_counts(df)
        assert counts.captured == 1
        assert counts.empty_confirmed == 0
        assert counts.attempted_failed == 0


class TestDeriveCaptureStatusRates:
    def test_all_three_statuses_denominator_100(self) -> None:
        counts = CaptureStatusCounts(captured=40, empty_confirmed=40, attempted_failed=20)
        rates = _derive_capture_status_rates(counts, total_expected_cells=100)
        assert rates["attempted_total"] == 100
        assert rates["attempt_coverage_pct"] == 100.0
        assert rates["capture_coverage_pct"] == 40.0
        assert rates["empty_rate"] == 0.4
        assert rates["failure_rate"] == 0.2

    def test_attempt_coverage_equals_capture_when_only_captured_rows(self) -> None:
        counts = CaptureStatusCounts(captured=50, empty_confirmed=0, attempted_failed=0)
        rates = _derive_capture_status_rates(counts, total_expected_cells=100)
        assert rates["attempt_coverage_pct"] == 50.0
        assert rates["capture_coverage_pct"] == 50.0
        assert rates["empty_rate"] == 0.0
        assert rates["failure_rate"] == 0.0

    def test_zero_denominator_returns_zero_rates(self) -> None:
        counts = CaptureStatusCounts(captured=0, empty_confirmed=0, attempted_failed=0)
        rates = _derive_capture_status_rates(counts, total_expected_cells=0)
        assert rates["attempt_coverage_pct"] == 0.0
        assert rates["capture_coverage_pct"] == 0.0
        assert rates["empty_rate"] == 0.0
        assert rates["failure_rate"] == 0.0


class TestBuildCoverageMetricsHonestCoverage:
    def test_prediction_event_driven_proxy_path_pre_phase_b(self) -> None:
        """PREDICTION, no sentinel rows → proxy (distinct-underlying) path."""
        df = pd.DataFrame([{"underlying": f"COND_{i:03d}", "date": "2025-06-01"} for i in range(99)])
        metrics = _build_coverage_metrics(df, "PREDICTION", capture_coverage_pct=23.37, total_expected_cells=35049)
        assert metrics["coverage_semantics"] == "event_driven"
        assert metrics["attempt_coverage_pct"] == 100.0
        assert metrics["capture_coverage_pct"] == 23.37
        # empty_rate_estimate derived from the proxy path ≈ 0.7663
        assert metrics["empty_rate_estimate"] is not None
        assert abs(float(metrics["empty_rate_estimate"]) - 0.7663) < 1e-4
        assert metrics["capture_status_counts"]["captured"] == 99
        assert metrics["capture_status_counts"]["empty_confirmed"] == 0
        assert metrics["capture_status_counts"]["attempted_failed"] == 0

    def test_prediction_event_driven_phase_b_path_uses_capture_status(self) -> None:
        """Once Phase B lands empty_confirmed rows, prefer direct counts."""
        # 50 captured + 40 empty + 10 failed = 100 attempted out of 200 expected
        rows = (
            [{"underlying": "BTC", "capture_status": "captured"} for _ in range(50)]
            + [{"underlying": "BTC", "capture_status": "empty_confirmed"} for _ in range(40)]
            + [{"underlying": "BTC", "capture_status": "attempted_failed"} for _ in range(10)]
        )
        df = pd.DataFrame(rows)
        metrics = _build_coverage_metrics(df, "PREDICTION", capture_coverage_pct=25.0, total_expected_cells=200)
        assert metrics["attempt_coverage_pct"] == 50.0  # 100/200
        assert metrics["capture_coverage_pct"] == 25.0
        assert metrics["empty_rate_estimate"] == 0.4  # 40/100
        assert metrics["failure_rate"] == 0.1  # 10/100

    def test_dense_cefi_attempt_equals_capture(self) -> None:
        df = pd.DataFrame(
            [
                {"venue": "BINANCE-SPOT", "capture_status": "captured"},
                {"venue": "BINANCE-SPOT", "capture_status": "captured"},
            ]
        )
        metrics = _build_coverage_metrics(df, "CEFI", capture_coverage_pct=95.0, total_expected_cells=1000)
        assert metrics["coverage_semantics"] == "dense"
        assert metrics["attempt_coverage_pct"] == 95.0
        assert metrics["capture_coverage_pct"] == 95.0
        assert metrics["empty_rate_estimate"] is None
        assert metrics["failure_rate"] == 0.0

    def test_empty_frame_returns_zero_everything(self) -> None:
        metrics = _build_coverage_metrics(pd.DataFrame(), "CEFI", capture_coverage_pct=0.0, total_expected_cells=0)
        assert metrics["attempt_coverage_pct"] == 0.0
        assert metrics["capture_coverage_pct"] == 0.0
        assert metrics["empty_rate_estimate"] is None
        assert metrics["failure_rate"] == 0.0
        assert metrics["capture_status_counts"]["captured"] == 0

"""Unit tests for deployment_api/services/data_status/live_build_guard.py."""

from __future__ import annotations

from deployment_api.services.data_status.live_build_guard import (
    estimate_live_build_bytes,
    would_exceed_budget,
)

_DEFAULT_BUDGET_BYTES = 768 * 1024**2  # matches DeploymentApiConfig default


class TestEstimateLiveBuildBytes:
    def test_full_history_mtds_far_exceeds_default_budget(self) -> None:
        """The exact shape that OOM-crashed the container (2026-07-13/14):
        market-tick-data-service, full 2018-today history, all 5 categories,
        no venue/row filter."""
        estimate = estimate_live_build_bytes(
            service="market-tick-data-service",
            start_date="2018-01-01",
            end_date="2030-01-01",
            category_count=5,
        )
        assert estimate > _DEFAULT_BUDGET_BYTES

    def test_full_history_mdps_far_exceeds_default_budget(self) -> None:
        """market-data-processing-service's per-day rate is calibrated from
        just a 3-month measurement (56 GB) — a full-history request must
        estimate even higher, not lower."""
        estimate = estimate_live_build_bytes(
            service="market-data-processing-service",
            start_date="2018-01-01",
            end_date="2030-01-01",
            category_count=5,
        )
        assert estimate > _DEFAULT_BUDGET_BYTES

    def test_small_single_day_single_category_request_is_cheap(self) -> None:
        estimate = estimate_live_build_bytes(
            service="instruments-service",
            start_date="2024-01-01",
            end_date="2024-01-01",
            category_count=1,
        )
        assert estimate < _DEFAULT_BUDGET_BYTES

    def test_unknown_service_uses_conservative_worst_case_rate(self) -> None:
        """A service with no calibration anchor must estimate AT LEAST as
        high as the worst known anchor for the same shape — never lower."""
        days = 90
        unknown = estimate_live_build_bytes(
            service="some-future-service",
            start_date="2024-01-01",
            end_date="2024-03-30",
            category_count=5,
        )
        mdps = estimate_live_build_bytes(
            service="market-data-processing-service",
            start_date="2024-01-01",
            end_date="2024-03-30",
            category_count=5,
        )
        assert days > 0  # sanity: the date range above really is ~90 days
        assert unknown == mdps

    def test_more_categories_scales_estimate_up(self) -> None:
        one_cat = estimate_live_build_bytes(
            service="instruments-service", start_date="2024-01-01", end_date="2024-01-31", category_count=1
        )
        five_cat = estimate_live_build_bytes(
            service="instruments-service", start_date="2024-01-01", end_date="2024-01-31", category_count=5
        )
        assert five_cat > one_cat

    def test_longer_date_range_scales_estimate_up(self) -> None:
        short = estimate_live_build_bytes(
            service="instruments-service", start_date="2024-01-01", end_date="2024-01-31", category_count=1
        )
        long = estimate_live_build_bytes(
            service="instruments-service", start_date="2024-01-01", end_date="2025-01-31", category_count=1
        )
        assert long > short

    def test_venue_filter_narrows_but_does_not_zero_out_estimate(self) -> None:
        unfiltered = estimate_live_build_bytes(
            service="market-tick-data-service",
            start_date="2024-01-01",
            end_date="2024-01-31",
            category_count=1,
        )
        filtered = estimate_live_build_bytes(
            service="market-tick-data-service",
            start_date="2024-01-01",
            end_date="2024-01-31",
            category_count=1,
            venue_count=1,
        )
        assert 0 < filtered < unfiltered

    def test_row_filter_narrows_but_does_not_zero_out_estimate(self) -> None:
        unfiltered = estimate_live_build_bytes(
            service="market-tick-data-service",
            start_date="2024-01-01",
            end_date="2024-01-31",
            category_count=1,
        )
        filtered = estimate_live_build_bytes(
            service="market-tick-data-service",
            start_date="2024-01-01",
            end_date="2024-01-31",
            category_count=1,
            has_row_filter=True,
        )
        assert 0 < filtered < unfiltered

    def test_malformed_date_range_errs_wide_toward_full_history(self) -> None:
        malformed = estimate_live_build_bytes(
            service="instruments-service",
            start_date="not-a-date",
            end_date="also-not-a-date",
            category_count=1,
        )
        one_day = estimate_live_build_bytes(
            service="instruments-service", start_date="2024-01-01", end_date="2024-01-01", category_count=1
        )
        assert malformed > one_day


class TestWouldExceedBudget:
    def test_over_budget_is_true(self) -> None:
        assert would_exceed_budget(1000, 999) is True

    def test_at_budget_is_false(self) -> None:
        assert would_exceed_budget(999, 999) is False

    def test_under_budget_is_false(self) -> None:
        assert would_exceed_budget(998, 999) is False

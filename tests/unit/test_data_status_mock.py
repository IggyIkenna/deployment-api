"""Unit tests for services/data_status_mock.py — pure helper coverage."""

from __future__ import annotations

from unified_trading_library import setup_events

setup_events("deployment-api", "test")


# ── _mock_venue_entry ─────────────────────────────────────────────────────────


class TestMockVenueEntry:
    def test_basic_fields_populated(self) -> None:
        from deployment_api.services.data_status_mock import _mock_venue_entry

        result = _mock_venue_entry(
            venue="BINANCE-SPOT",
            dates_expected=10,
            captured=8,
            empty_confirmed=1,
            attempted_failed=1,
        )
        assert result["dates_found"] == 8
        assert result["dates_expected"] == 10
        assert result["dates_missing"] == 2

    def test_completion_pct_capped_at_100(self) -> None:
        from deployment_api.services.data_status_mock import _mock_venue_entry

        result = _mock_venue_entry(
            venue="X",
            dates_expected=5,
            captured=10,
            empty_confirmed=0,
            attempted_failed=0,
        )
        assert result["completion_pct"] <= 100.0

    def test_capture_status_counts_present(self) -> None:
        from deployment_api.services.data_status_mock import _mock_venue_entry

        result = _mock_venue_entry(
            venue="V",
            dates_expected=10,
            captured=7,
            empty_confirmed=2,
            attempted_failed=1,
        )
        counts = result["capture_status_counts"]
        assert isinstance(counts, dict)
        assert counts["captured"] == 7
        assert counts["empty_confirmed"] == 2
        assert counts["attempted_failed"] == 1

    def test_zero_expected_no_division_error(self) -> None:
        from deployment_api.services.data_status_mock import _mock_venue_entry

        result = _mock_venue_entry(
            venue="V",
            dates_expected=0,
            captured=0,
            empty_confirmed=0,
            attempted_failed=0,
        )
        assert isinstance(result["completion_pct"], float)

    def test_failure_rate_computed(self) -> None:
        from deployment_api.services.data_status_mock import _mock_venue_entry

        result = _mock_venue_entry(
            venue="V",
            dates_expected=10,
            captured=8,
            empty_confirmed=0,
            attempted_failed=2,
        )
        assert result["failure_rate"] > 0


# ── _mock_category_entry ──────────────────────────────────────────────────────


class TestMockCategoryEntry:
    def test_cefi_dense_semantics(self) -> None:
        from deployment_api.services.data_status_mock import _mock_category_entry

        result = _mock_category_entry(
            category="CEFI",
            dates_expected=100,
            captured_total=90,
            empty_total=5,
            failed_total=5,
        )
        assert result["asset_group"] == "CEFI"
        assert result["coverage_semantics"] == "dense"
        assert "venues" in result

    def test_sports_event_driven_semantics(self) -> None:
        from deployment_api.services.data_status_mock import _mock_category_entry

        result = _mock_category_entry(
            category="SPORTS",
            dates_expected=200,
            captured_total=150,
            empty_total=40,
            failed_total=10,
        )
        assert result["coverage_semantics"] == "event_driven"

    def test_prediction_semantics(self) -> None:
        from deployment_api.services.data_status_mock import _mock_category_entry

        result = _mock_category_entry(
            category="PREDICTION",
            dates_expected=800,
            captured_total=200,
            empty_total=500,
            failed_total=100,
        )
        assert result["coverage_semantics"] == "event_driven"
        assert result["unit"] == "dates"

    def test_failure_rate_by_dimension_populated_when_failures(self) -> None:
        from deployment_api.services.data_status_mock import _mock_category_entry

        result = _mock_category_entry(
            category="CEFI",
            dates_expected=10,
            captured_total=8,
            empty_total=0,
            failed_total=2,
        )
        fbyd = result["failure_rate_by_dimension"]
        assert isinstance(fbyd, dict)
        assert len(fbyd) > 0

    def test_no_failures_empty_failure_rate_by_dimension(self) -> None:
        from deployment_api.services.data_status_mock import _mock_category_entry

        result = _mock_category_entry(
            category="CEFI",
            dates_expected=10,
            captured_total=10,
            empty_total=0,
            failed_total=0,
        )
        assert result["failure_rate_by_dimension"] == {}

    def test_venues_dict_has_expected_venues(self) -> None:
        from deployment_api.services.data_status_mock import _mock_category_entry

        result = _mock_category_entry(
            category="CEFI",
            dates_expected=30,
            captured_total=27,
            empty_total=2,
            failed_total=1,
        )
        venues = result["venues"]
        assert isinstance(venues, dict)
        assert "BINANCE-SPOT" in venues

    def test_capture_status_counts_sum_correct(self) -> None:
        from deployment_api.services.data_status_mock import _mock_category_entry

        result = _mock_category_entry(
            category="TRADFI",
            dates_expected=100,
            captured_total=90,
            empty_total=5,
            failed_total=5,
        )
        counts = result["capture_status_counts"]
        assert isinstance(counts, dict)
        assert counts["captured"] == 90
        assert counts["empty_confirmed"] == 5
        assert counts["attempted_failed"] == 5

    def test_unknown_category_uses_dense_fallback(self) -> None:
        from deployment_api.services.data_status_mock import _mock_category_entry

        result = _mock_category_entry(
            category="UNKNOWN",
            dates_expected=10,
            captured_total=8,
            empty_total=1,
            failed_total=1,
        )
        assert result["coverage_semantics"] == "dense"


# ── build_mock_turbo_response ─────────────────────────────────────────────────


class TestBuildMockTurboResponse:
    def test_instruments_service_has_all_categories(self) -> None:
        from deployment_api.services.data_status_mock import build_mock_turbo_response

        result = build_mock_turbo_response("instruments-service", "2024-01-01", "2024-01-30")
        asset_groups = result["asset_groups"]
        assert isinstance(asset_groups, dict)
        assert len(asset_groups) == 5

    def test_mock_flag_set(self) -> None:
        from deployment_api.services.data_status_mock import build_mock_turbo_response

        result = build_mock_turbo_response("instruments-service", "2024-01-01", "2024-01-30")
        assert result["mock"] is True

    def test_restricted_service_only_allowed_categories(self) -> None:
        from deployment_api.services.data_status_mock import build_mock_turbo_response

        result = build_mock_turbo_response("features-onchain-service", "2024-01-01", "2024-01-30")
        asset_groups = result["asset_groups"]
        assert isinstance(asset_groups, dict)
        assert "DEFI" in asset_groups
        assert "CEFI" not in asset_groups

    def test_asset_groups_filter_applied(self) -> None:
        from deployment_api.services.data_status_mock import build_mock_turbo_response

        result = build_mock_turbo_response(
            "instruments-service",
            "2024-01-01",
            "2024-01-30",
            asset_groups=["cefi"],
        )
        asset_groups = result["asset_groups"]
        assert isinstance(asset_groups, dict)
        assert "CEFI" in asset_groups
        assert "DEFI" not in asset_groups

    def test_overall_completion_pct_present(self) -> None:
        from deployment_api.services.data_status_mock import build_mock_turbo_response

        result = build_mock_turbo_response("instruments-service", "2024-01-01", "2024-01-30")
        assert "overall_completion_pct" in result
        assert isinstance(result["overall_completion_pct"], float)

    def test_date_range_echoed(self) -> None:
        from deployment_api.services.data_status_mock import build_mock_turbo_response

        result = build_mock_turbo_response("instruments-service", "2024-01-01", "2024-01-30")
        date_range = result["date_range"]
        assert isinstance(date_range, dict)
        assert date_range["start"] == "2024-01-01"
        assert date_range["end"] == "2024-01-30"

    def test_unknown_service_returns_all_categories(self) -> None:
        from deployment_api.services.data_status_mock import build_mock_turbo_response

        result = build_mock_turbo_response("new-unknown-service", "2024-01-01", "2024-01-30")
        asset_groups = result["asset_groups"]
        assert isinstance(asset_groups, dict)
        assert len(asset_groups) == 5

    def test_sports_service_only_sports(self) -> None:
        from deployment_api.services.data_status_mock import build_mock_turbo_response

        result = build_mock_turbo_response("features-sports-service", "2024-01-01", "2024-01-30")
        asset_groups = result["asset_groups"]
        assert isinstance(asset_groups, dict)
        assert "SPORTS" in asset_groups
        assert "CEFI" not in asset_groups

    def test_mdps_service_restricted(self) -> None:
        from deployment_api.services.data_status_mock import build_mock_turbo_response

        result = build_mock_turbo_response(
            "market-data-processing-service",
            "2024-01-01",
            "2024-01-30",
        )
        asset_groups = result["asset_groups"]
        assert isinstance(asset_groups, dict)
        assert "CEFI" in asset_groups
        assert "SPORTS" not in asset_groups


# ── build_mock_shard_instruments ──────────────────────────────────────────────


class TestBuildMockShardInstruments:
    def test_returns_three_instruments(self) -> None:
        from deployment_api.services.data_status_mock import build_mock_shard_instruments

        result = build_mock_shard_instruments(
            service="instruments-service",
            category="CEFI",
            venue="BINANCE",
            day="2024-01-01",
            instrument_type="spot",
            data_type="ohlcv",
        )
        instruments = result["instruments"]
        assert isinstance(instruments, list)
        assert len(instruments) == 3

    def test_has_all_capture_statuses(self) -> None:
        from deployment_api.services.data_status_mock import build_mock_shard_instruments

        result = build_mock_shard_instruments(
            service="instruments-service",
            category="CEFI",
            venue="BINANCE",
            day="2024-01-01",
            instrument_type="spot",
            data_type="ohlcv",
        )
        statuses = {i["capture_status"] for i in result["instruments"]}
        assert "captured" in statuses
        assert "empty_confirmed" in statuses
        assert "attempted_failed" in statuses

    def test_mock_flag_set(self) -> None:
        from deployment_api.services.data_status_mock import build_mock_shard_instruments

        result = build_mock_shard_instruments(
            service="instruments-service",
            category="CEFI",
            venue="BINANCE",
            day="2024-01-01",
            instrument_type="spot",
            data_type="ohlcv",
        )
        assert result["mock"] is True

    def test_fields_echoed(self) -> None:
        from deployment_api.services.data_status_mock import build_mock_shard_instruments

        result = build_mock_shard_instruments(
            service="svc",
            category="DEFI",
            venue="UNISWAP",
            day="2024-06-01",
            instrument_type="lp",
            data_type="rates",
        )
        assert result["service"] == "svc"
        assert result["venue"] == "UNISWAP"
        assert result["day"] == "2024-06-01"

    def test_total_count_matches_instruments(self) -> None:
        from deployment_api.services.data_status_mock import build_mock_shard_instruments

        result = build_mock_shard_instruments(
            service="instruments-service",
            category="CEFI",
            venue="BINANCE",
            day="2024-01-01",
            instrument_type="spot",
            data_type="ohlcv",
        )
        assert result["total_count"] == len(result["instruments"])

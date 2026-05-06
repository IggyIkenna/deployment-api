"""Unit tests for _features_sports_expected_dates_for_calculator (Phase 3).

Plan: features_sports_honest_coverage_2026_05_05.plan.md, Phase 3.D.
"""

from __future__ import annotations

from unittest.mock import patch

from deployment_api.services.data_status_service import (
    _features_sports_expected_dates_for_calculator,
)


class TestUnknownCalculator:
    def test_unknown_calc_returns_empty(self) -> None:
        result = _features_sports_expected_dates_for_calculator(
            calc_name="not_a_real_calculator",
            league_id="EPL",
            start_date="2024-04-01",
            end_date="2024-04-30",
        )
        assert result == []


class TestSingleSourceCalculator:
    def test_team_form_uses_api_football_calendar(self) -> None:
        """team_form requires only api_football FIXTURES → expected = full
        fixture calendar (no further intersection)."""
        with patch(
            "deployment_api.services.data_status_service.get_league_fixture_calendar"
        ) as mock_cal:
            mock_cal.return_value = ["2024-04-13", "2024-04-20", "2024-04-27"]
            result = _features_sports_expected_dates_for_calculator(
                calc_name="team_form",
                league_id="EPL",
                start_date="2024-04-01",
                end_date="2024-04-30",
            )
        assert result == ["2024-04-13", "2024-04-20", "2024-04-27"]

    def test_pre_launch_clipped_to_zero(self) -> None:
        """Pre-2018-01-01 (api_football coverage start) → clipped window
        empty, no expected dates."""
        result = _features_sports_expected_dates_for_calculator(
            calc_name="team_form",
            league_id="EPL",
            start_date="2017-01-01",
            end_date="2017-12-31",
        )
        assert result == []


class TestMultiUpstreamCalculator:
    def test_team_xg_intersection_of_understat_and_api_football(self) -> None:
        """team_xg = understat XG + api_football FIXTURES (both required).
        Expected = intersection of both upstreams' expected dates."""
        with patch(
            "deployment_api.services.data_status_service.get_league_fixture_calendar"
        ) as mock_cal:
            mock_cal.return_value = [
                "2024-04-13",
                "2024-04-20",
                "2024-04-27",
            ]
            # EPL is in both understat (6 leagues) and api_football coverage,
            # so expected = full fixture calendar (intersection of identical sets).
            result = _features_sports_expected_dates_for_calculator(
                calc_name="team_xg",
                league_id="EPL",
                start_date="2024-04-01",
                end_date="2024-04-30",
            )
        assert result == ["2024-04-13", "2024-04-20", "2024-04-27"]

    def test_team_xg_mls_understat_excluded_returns_empty(self) -> None:
        """team_xg in MLS: understat XG is out-of-coverage (only 6 European
        leagues) → required-upstream intersection collapses to empty.
        Calculator should produce no expected dates → NaN-by-design throughout."""
        with patch(
            "deployment_api.services.data_status_service.get_league_fixture_calendar"
        ) as mock_cal:
            mock_cal.return_value = ["2024-04-13", "2024-04-20"]
            result = _features_sports_expected_dates_for_calculator(
                calc_name="team_xg",
                league_id="MLS",
                start_date="2024-04-01",
                end_date="2024-04-30",
            )
        # Understat returns empty (out-of-coverage), api_football returns
        # full calendar; intersection = empty.
        assert result == []


class TestDerivedCalculator:
    def test_team_derived_resolves_through_dependents(self) -> None:
        """team_derived = derived team_form + derived team_goals (both
        required). Should walk into both, find their api_football FIXTURES
        requirements, and intersect."""
        with patch(
            "deployment_api.services.data_status_service.get_league_fixture_calendar"
        ) as mock_cal:
            mock_cal.return_value = ["2024-04-13", "2024-04-20"]
            result = _features_sports_expected_dates_for_calculator(
                calc_name="team_derived",
                league_id="EPL",
                start_date="2024-04-01",
                end_date="2024-04-30",
            )
        # team_form + team_goals both reduce to the same FIXTURES calendar
        # → intersection = full calendar.
        assert result == ["2024-04-13", "2024-04-20"]


class TestSfiProgressiveOverride:
    def test_pre_2020_clipped_by_data_type_override(self) -> None:
        """SFI_PROGRESSIVE_STATS has DATA_TYPE_COVERAGE_START override
        (2020-01-01) tighter than the source's SFI start (2019-01-01)."""
        result = _features_sports_expected_dates_for_calculator(
            calc_name="sfi_progressive",
            league_id="EPL",
            start_date="2019-06-01",
            end_date="2019-12-31",
        )
        assert result == []

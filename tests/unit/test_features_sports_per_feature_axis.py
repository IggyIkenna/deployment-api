"""Unit tests for _features_sports_expected_dates_for_calculator (Phase 3).

Plan: features_sports_honest_coverage_2026_05_05.md, Phase 3.D.
"""

from __future__ import annotations

from unittest.mock import MagicMock, patch

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
        with patch("deployment_api.services.data_status_service.get_league_fixture_calendar") as mock_cal:
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
        with patch("deployment_api.services.data_status_service.get_league_fixture_calendar") as mock_cal:
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
        with patch("deployment_api.services.data_status_service.get_league_fixture_calendar") as mock_cal:
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
        with patch("deployment_api.services.data_status_service.get_league_fixture_calendar") as mock_cal:
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


class TestSportsHonestCoveragePerFeatureBranch:
    """Phase 3.E1: ``_sports_honest_coverage`` routes per-calculator
    feature_group entries to the new per_feature axis branch and surfaces
    expected/found counts using FEATURE_UPSTREAM_REQUIREMENTS."""

    def test_unknown_calc_returns_none(self) -> None:
        """Unknown entity name → no meta match → caller falls back to legacy
        date-count model."""
        import pandas as pd

        from deployment_api.services.data_status_service import _sports_honest_coverage

        result = _sports_honest_coverage(
            filtered=pd.DataFrame(),
            entity_name="not_a_calculator",
            start_date="2024-04-01",
            end_date="2024-04-30",
        )
        assert result is None

    def test_team_form_routes_to_per_feature_axis(self) -> None:
        """team_form is in FEATURES_SPORTS_PER_CALC_META → routed to the new
        axis branch. Returns shape with axis='per_feature_per_league_per_fixture_date'."""
        import pandas as pd

        from deployment_api.services.data_status_service import _sports_honest_coverage

        # Empty manifest → expected_shards from the helper, found_shards = 0
        result = _sports_honest_coverage(
            filtered=pd.DataFrame(columns=["data_type", "feature_group", "league_id", "date", "capture_status"]),
            entity_name="team_form",
            start_date="2024-04-01",
            end_date="2024-04-30",
        )
        assert result is not None
        assert result["axis"] == "per_feature_per_league_per_fixture_date"
        assert result["unit"] == "fixture_dates"
        # api_football is the primary required upstream's source
        assert result["source"] == "api_football"
        # found_shards = 0 because empty manifest
        assert result["found_shards"] == 0
        # expected_shards > 0 because EPL+other leagues have fixtures in 2024-04
        # (don't pin exact number — depends on real LEAGUE_REGISTRY which can
        # change. Just verify the axis branch ran).
        assert isinstance(result["expected_shards"], int)


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


class TestPerFeatureAxisAlreadySatisfied:
    """ITEM 5 verification: per_feature_per_league_per_fixture_date axis is
    fully handled in ``_sports_honest_coverage``.

    Semantics: expected_shards for a calculator entry = sum over expected
    leagues of (number of expected fixture dates in that league). The
    architecture ensures each calculator is a separate entity_name in
    FEATURES_SPORTS_PER_CALC_META — no single-entity multi-feature scenario.
    The per_feature multiplier IS the architecture (N calculators = N rows
    in the meta dict, each with its own expected_shards). sports_master.md:352.
    """

    def test_axis_value_in_sports_axis_literal(self) -> None:
        """per_feature_per_league_per_fixture_date appears in SportsAxis."""
        from deployment_api.services.data_status_service import FEATURES_SPORTS_PER_CALC_META

        # Every calc in FEATURES_SPORTS_PER_CALC_META declares this axis
        for calc_name, meta in FEATURES_SPORTS_PER_CALC_META.items():
            assert meta["axis"] == "per_feature_per_league_per_fixture_date", (
                f"Calculator {calc_name!r} has unexpected axis {meta['axis']!r}"
            )

    def test_expected_shards_is_sum_across_leagues(self) -> None:
        """expected_shards = sum of fixture-date counts per expected league.

        For a 3-league scenario (EPL, LA_LIGA, BUNDESLIGA) with 2 fixture
        dates each → expected_shards = 6. Verifies the accumulation loop in
        _sports_honest_coverage correctly sums across leagues.
        """
        import pandas as pd

        from deployment_api.services.data_status_service import (
            FEATURES_SPORTS_PER_CALC_META,
            _sports_honest_coverage,
        )

        # Verify team_form is in the meta (sanity check)
        assert "team_form" in FEATURES_SPORTS_PER_CALC_META

        with (
            patch("deployment_api.services.data_status_service.get_expected_leagues_for_source") as mock_leagues,
            patch(
                "deployment_api.services.data_status_service._features_sports_expected_dates_for_calculator"
            ) as mock_dates,
        ):
            # 3 mock leagues, each with 2 fixture dates
            mock_league = MagicMock()
            mock_league.league_id = "EPL"
            mock_league2 = MagicMock()
            mock_league2.league_id = "LA_LIGA"
            mock_league3 = MagicMock()
            mock_league3.league_id = "BUNDESLIGA"
            mock_leagues.return_value = [mock_league, mock_league2, mock_league3]
            mock_dates.return_value = ["2024-04-13", "2024-04-27"]

            result = _sports_honest_coverage(
                filtered=pd.DataFrame(columns=["data_type", "feature_group", "league_id", "date", "capture_status"]),
                entity_name="team_form",
                start_date="2024-04-01",
                end_date="2024-04-30",
            )

        assert result is not None
        assert result["axis"] == "per_feature_per_league_per_fixture_date"
        # 3 leagues x 2 dates = 6 expected shards
        assert result["expected_shards"] == 6
        # Empty manifest → 0 found
        assert result["found_shards"] == 0

    def test_found_shards_counted_from_manifest(self) -> None:
        """found_shards counts (league, date) pairs present in the manifest."""
        import pandas as pd

        from deployment_api.services.data_status_service import _sports_honest_coverage

        df = pd.DataFrame(
            {
                "data_type": ["DERIVED_FEATURES", "DERIVED_FEATURES"],
                "feature_group": ["team_form", "team_form"],
                "league_id": ["EPL", "EPL"],
                "date": ["2024-04-13", "2024-04-27"],
                "capture_status": ["captured", "captured"],
            }
        )

        with (
            patch("deployment_api.services.data_status_service.get_expected_leagues_for_source") as mock_leagues,
            patch(
                "deployment_api.services.data_status_service._features_sports_expected_dates_for_calculator"
            ) as mock_dates,
        ):
            mock_league = MagicMock()
            mock_league.league_id = "EPL"
            mock_leagues.return_value = [mock_league]
            mock_dates.return_value = ["2024-04-13", "2024-04-27"]

            result = _sports_honest_coverage(
                filtered=df,
                entity_name="team_form",
                start_date="2024-04-01",
                end_date="2024-04-30",
            )

        assert result is not None
        assert result["expected_shards"] == 2
        assert result["found_shards"] == 2

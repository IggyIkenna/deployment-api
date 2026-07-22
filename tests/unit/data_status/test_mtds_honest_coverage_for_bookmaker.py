"""Unit tests for the SPORTS bookmaker x league x fixture-date honest-coverage
axis (``deployment_api.services.data_status.mtds.mtds_honest_coverage_for_bookmaker``,
Phase 6d, shipped 2026-07-22).

Mocks the two external dependencies (UAC's observed bookmaker-league coverage
map, and the real fixture-calendar lookup) so the arithmetic is pinned
independent of live data — the manifest-column contract
(``venue``/``league_id``/``date``/``capture_status``/``error_reason``) and the
found-vs-expected set logic are what's under test here, not the fixture
calendar or the coverage-observation data itself.

See ``plans/active/issues/sports_shard_enumeration_cartesian_blowup_2026_07_20.md``
§4.4 for why this axis needed its own function rather than reusing
``mtds_honest_coverage_for_venue``.
"""

from __future__ import annotations

from unittest.mock import patch

import pandas as pd
from unified_api_contracts import VenueMapping

from deployment_api.services.data_status.mtds import (
    is_mtds_honest_coverage_target,
    mtds_expected_venues,
    mtds_honest_coverage_for_bookmaker,
)

_PATCH_TARGET_COVERAGE = "deployment_api.services.data_status.mtds.BOOKMAKER_LEAGUE_COVERAGE"
_PATCH_TARGET_DATES = "deployment_api.services.data_status.mtds.sports_expected_dates_for_league"


def _row(
    venue: str, league_id: str, date: str, capture_status: str = "captured", error_reason: str = ""
) -> dict[str, str]:
    return {
        "venue": venue,
        "league_id": league_id,
        "date": date,
        "capture_status": capture_status,
        "error_reason": error_reason,
    }


class TestUnobservedBookmaker:
    def test_no_coverage_entry_returns_zero_expected(self) -> None:
        with patch(_PATCH_TARGET_COVERAGE, {}):
            result = mtds_honest_coverage_for_bookmaker(pd.DataFrame(), "ONEXBET", "2026-01-01", "2026-01-31")
        assert result["expected_shards"] == 0
        assert result["found_shards"] == 0
        assert result["missing_data_types"] == ["trades"]

    def test_empty_frozenset_coverage_returns_zero_expected(self) -> None:
        with patch(_PATCH_TARGET_COVERAGE, {"PINNACLE": frozenset()}):
            result = mtds_honest_coverage_for_bookmaker(pd.DataFrame(), "PINNACLE", "2026-01-01", "2026-01-31")
        assert result["expected_shards"] == 0
        assert result["found_shards"] == 0


class TestSingleLeagueCoverage:
    def test_all_fixture_dates_captured_is_full_coverage(self) -> None:
        df = pd.DataFrame(
            [
                _row("PINNACLE", "EPL", "2026-01-01"),
                _row("PINNACLE", "EPL", "2026-01-08"),
            ]
        )
        with (
            patch(_PATCH_TARGET_COVERAGE, {"PINNACLE": frozenset({"EPL"})}),
            patch(_PATCH_TARGET_DATES, return_value=["2026-01-01", "2026-01-08"]),
        ):
            result = mtds_honest_coverage_for_bookmaker(df, "PINNACLE", "2026-01-01", "2026-01-31")
        assert result["expected_shards"] == 2
        assert result["found_shards"] == 2
        assert result["missing_data_types"] == []
        data_types = result["data_types"]
        assert isinstance(data_types, dict)
        assert data_types["EPL"]["completion_pct"] == 100.0

    def test_partial_capture_computes_correct_ratio(self) -> None:
        df = pd.DataFrame([_row("PINNACLE", "EPL", "2026-01-01")])
        with (
            patch(_PATCH_TARGET_COVERAGE, {"PINNACLE": frozenset({"EPL"})}),
            patch(_PATCH_TARGET_DATES, return_value=["2026-01-01", "2026-01-08", "2026-01-15", "2026-01-22"]),
        ):
            result = mtds_honest_coverage_for_bookmaker(df, "PINNACLE", "2026-01-01", "2026-01-31")
        assert result["expected_shards"] == 4
        assert result["found_shards"] == 1
        assert result["missing_shards"] == 3

    def test_zero_captures_is_missing_not_absent(self) -> None:
        """A covered league with zero manifest rows still shows an honest 0% —
        the whole point of Phase 6d (was previously invisible)."""
        with (
            patch(_PATCH_TARGET_COVERAGE, {"PINNACLE": frozenset({"EPL"})}),
            patch(_PATCH_TARGET_DATES, return_value=["2026-01-01", "2026-01-08"]),
        ):
            result = mtds_honest_coverage_for_bookmaker(pd.DataFrame(), "PINNACLE", "2026-01-01", "2026-01-31")
        assert result["expected_shards"] == 2
        assert result["found_shards"] == 0
        assert result["missing_data_types"] == ["trades"]

    def test_captured_date_outside_expected_window_not_counted(self) -> None:
        """A manifest row on a non-fixture date (e.g. a stray capture) must not
        inflate found_shards past the real fixture-date universe."""
        df = pd.DataFrame(
            [
                _row("PINNACLE", "EPL", "2026-01-01"),
                _row("PINNACLE", "EPL", "2026-06-15"),  # not a fixture date
            ]
        )
        with (
            patch(_PATCH_TARGET_COVERAGE, {"PINNACLE": frozenset({"EPL"})}),
            patch(_PATCH_TARGET_DATES, return_value=["2026-01-01", "2026-01-08"]),
        ):
            result = mtds_honest_coverage_for_bookmaker(df, "PINNACLE", "2026-01-01", "2026-01-31")
        assert result["found_shards"] == 1


class TestMultiLeagueAggregation:
    def test_two_leagues_sum_independently(self) -> None:
        df = pd.DataFrame(
            [
                _row("PINNACLE", "EPL", "2026-01-01"),
                _row("PINNACLE", "LALIGA", "2026-01-02"),
            ]
        )

        def _dates_for(league_id: str, *_args: object, **_kwargs: object) -> list[str]:
            return {"EPL": ["2026-01-01", "2026-01-08"], "LALIGA": ["2026-01-02"]}[league_id]

        with (
            patch(_PATCH_TARGET_COVERAGE, {"PINNACLE": frozenset({"EPL", "LALIGA"})}),
            patch(_PATCH_TARGET_DATES, side_effect=_dates_for),
        ):
            result = mtds_honest_coverage_for_bookmaker(df, "PINNACLE", "2026-01-01", "2026-01-31")
        assert result["expected_shards"] == 3
        assert result["found_shards"] == 2
        data_types = result["data_types"]
        assert isinstance(data_types, dict)
        assert set(data_types.keys()) == {"EPL", "LALIGA"}


class TestVenueAndStatusFiltering:
    def test_other_bookmakers_rows_not_counted(self) -> None:
        df = pd.DataFrame(
            [
                _row("PINNACLE", "EPL", "2026-01-01"),
                _row("BETFAIR_EX_UK", "EPL", "2026-01-08"),
            ]
        )
        with (
            patch(_PATCH_TARGET_COVERAGE, {"PINNACLE": frozenset({"EPL"})}),
            patch(_PATCH_TARGET_DATES, return_value=["2026-01-01", "2026-01-08"]),
        ):
            result = mtds_honest_coverage_for_bookmaker(df, "PINNACLE", "2026-01-01", "2026-01-31")
        assert result["found_shards"] == 1

    def test_bookmaker_match_is_case_insensitive(self) -> None:
        df = pd.DataFrame([_row("pinnacle", "EPL", "2026-01-01")])
        with (
            patch(_PATCH_TARGET_COVERAGE, {"PINNACLE": frozenset({"EPL"})}),
            patch(_PATCH_TARGET_DATES, return_value=["2026-01-01"]),
        ):
            result = mtds_honest_coverage_for_bookmaker(df, "PINNACLE", "2026-01-01", "2026-01-31")
        assert result["found_shards"] == 1

    def test_attempted_failed_rows_not_counted_as_found(self) -> None:
        df = pd.DataFrame([_row("PINNACLE", "EPL", "2026-01-01", capture_status="attempted_failed")])
        with (
            patch(_PATCH_TARGET_COVERAGE, {"PINNACLE": frozenset({"EPL"})}),
            patch(_PATCH_TARGET_DATES, return_value=["2026-01-01"]),
        ):
            result = mtds_honest_coverage_for_bookmaker(df, "PINNACLE", "2026-01-01", "2026-01-31")
        assert result["found_shards"] == 0
        assert result["missing_shards"] == 1

    def test_empty_confirmed_rows_count_as_found(self) -> None:
        """empty_confirmed = source was attempted, no odds that fixture — an
        honest answer for the fixture-existence axis, distinct from the
        Part 4.1 honest-coverage RATIO decision (which excludes empty_confirmed
        from compute_honest_coverage's own numerator/denominator; this
        function feeds dates_found/dates_expected upstream of that, mirroring
        mtds_honest_coverage_for_venue's identical OK-mask convention)."""
        df = pd.DataFrame([_row("PINNACLE", "EPL", "2026-01-01", capture_status="empty_confirmed")])
        with (
            patch(_PATCH_TARGET_COVERAGE, {"PINNACLE": frozenset({"EPL"})}),
            patch(_PATCH_TARGET_DATES, return_value=["2026-01-01"]),
        ):
            result = mtds_honest_coverage_for_bookmaker(df, "PINNACLE", "2026-01-01", "2026-01-31")
        assert result["found_shards"] == 1


class TestPhase6dWiring:
    """The gate + expected-venues wiring that dispatches SPORTS to this axis
    (Phase 6d — previously SPORTS was explicitly excluded)."""

    def test_sports_is_now_a_honest_coverage_target(self) -> None:
        assert is_mtds_honest_coverage_target("market-tick-data-service", "SPORTS") is True
        assert is_mtds_honest_coverage_target("market-tick-data-service", "sports") is True

    def test_non_honest_coverage_service_still_excluded(self) -> None:
        assert is_mtds_honest_coverage_target("some-other-service", "SPORTS") is False

    def test_mtds_expected_venues_resolves_bookmaker_keys_for_sports(self) -> None:
        venues = mtds_expected_venues("SPORTS", VenueMapping())
        assert "PINNACLE" in venues
        assert venues == sorted(venues)
        assert all(v == v.upper() for v in venues)

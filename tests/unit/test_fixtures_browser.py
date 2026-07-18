"""Unit tests for the fixtures browser — league -> day grouping (P9)."""

from __future__ import annotations

from datetime import UTC, date, datetime
from unittest.mock import patch

import pandas as pd
import pytest

from deployment_api.services import fixtures_browser as fb
from deployment_api.services import upcoming_fixtures as uf


@pytest.fixture(autouse=True)
def _clear_browse_cache():
    """The ``_BROWSE_CACHE`` module-level dict persists across tests — clear
    before AND after each test to keep tests order-independent (same
    rationale as ``test_upcoming_fixtures.py``'s ``_clear_fixtures_cache``)."""
    fb._BROWSE_CACHE.clear()
    yield
    fb._BROWSE_CACHE.clear()


class _DatetimeStub:
    """Patches ``fb.datetime`` — ``now()`` fixed; ``fromisoformat`` real."""

    UTC = UTC
    fromisoformat = staticmethod(datetime.fromisoformat)

    @staticmethod
    def now(tz=None):
        return datetime(2026, 4, 21, 0, 0, tzinfo=UTC)


def _fixture_row(
    fixture_id: str,
    kickoff_utc: str,
    league_id: str = "EPL",
    home: str = "Home",
    away: str = "Away",
    status: str = "NS",
) -> dict[str, object]:
    return {
        "fixture_id": fixture_id,
        "kickoff_utc": kickoff_utc,
        "league_id": league_id,
        "home_team_id": "h",
        "away_team_id": "a",
        "home_team_name": home,
        "away_team_name": away,
        "venue_id": "v",
        "venue_name": "Stadium",
        "status": status,
        "round_name": "R1",
    }


def test_groups_by_league_then_day() -> None:
    df_apr21 = pd.DataFrame(
        [
            _fixture_row("a", "2026-04-21T12:00:00Z", league_id="EPL"),
            _fixture_row("b", "2026-04-21T15:00:00Z", league_id="MLS"),
        ]
    )
    df_apr22 = pd.DataFrame([_fixture_row("c", "2026-04-22T12:00:00Z", league_id="EPL")])

    def fake_read(_bucket: str, path: str) -> pd.DataFrame:
        if "day=2026-04-21" in path:
            return df_apr21
        if "day=2026-04-22" in path:
            return df_apr22
        return pd.DataFrame()

    with (
        patch.object(fb, "datetime", _DatetimeStub),
        patch.object(uf, "object_exists", return_value=True),
        patch.object(uf, "_read_fixtures_parquet", side_effect=fake_read),
    ):
        out = fb.list_fixtures_by_league_and_day(window_days_back=0, window_days_forward=1)

    assert set(out.keys()) == {"EPL", "MLS"}
    assert set(out["EPL"].keys()) == {"2026-04-21", "2026-04-22"}
    assert [f["fixture_id"] for f in out["EPL"]["2026-04-21"]] == ["a"]
    assert [f["fixture_id"] for f in out["EPL"]["2026-04-22"]] == ["c"]
    assert set(out["MLS"].keys()) == {"2026-04-21"}
    assert [f["fixture_id"] for f in out["MLS"]["2026-04-21"]] == ["b"]


def test_empty_day_produces_no_phantom_bucket() -> None:
    """A day with no captured fixtures gets no entry — no ``{}`` placeholder days."""
    df_apr21 = pd.DataFrame([_fixture_row("a", "2026-04-21T12:00:00Z")])

    def fake_read(_bucket: str, path: str) -> pd.DataFrame:
        if "day=2026-04-21" in path:
            return df_apr21
        return pd.DataFrame()

    with (
        patch.object(fb, "datetime", _DatetimeStub),
        patch.object(uf, "object_exists", return_value=True),
        patch.object(uf, "_read_fixtures_parquet", side_effect=fake_read),
    ):
        out = fb.list_fixtures_by_league_and_day(window_days_back=0, window_days_forward=2)

    assert set(out.keys()) == {"EPL"}
    assert set(out["EPL"].keys()) == {"2026-04-21"}


def test_league_filter() -> None:
    df = pd.DataFrame(
        [
            _fixture_row("x", "2026-04-21T12:00:00Z", league_id="EPL"),
            _fixture_row("y", "2026-04-21T14:00:00Z", league_id="MLS"),
        ]
    )

    with (
        patch.object(fb, "datetime", _DatetimeStub),
        patch.object(uf, "object_exists", return_value=True),
        patch.object(uf, "_read_fixtures_parquet", return_value=df),
    ):
        out = fb.list_fixtures_by_league_and_day(window_days_back=0, window_days_forward=0, league_id="EPL")

    assert set(out.keys()) == {"EPL"}
    assert [f["fixture_id"] for day in out["EPL"].values() for f in day] == ["x"]


def test_shard_isolated_failure_skips_only_bad_day() -> None:
    """One day's read failure must not empty the whole grouped response."""
    df_ok = pd.DataFrame([_fixture_row("z", "2026-04-22T12:00:00Z")])

    def fake_read(_bucket: str, path: str) -> pd.DataFrame:
        if "day=2026-04-21" in path:
            raise OSError("read failed")
        if "day=2026-04-22" in path:
            return df_ok
        return pd.DataFrame()

    with (
        patch.object(fb, "datetime", _DatetimeStub),
        patch.object(uf, "object_exists", return_value=True),
        patch.object(uf, "_read_fixtures_parquet", side_effect=fake_read),
    ):
        out = fb.list_fixtures_by_league_and_day(window_days_back=0, window_days_forward=1)

    assert set(out.keys()) == {"EPL"}
    assert set(out["EPL"].keys()) == {"2026-04-22"}
    assert [f["fixture_id"] for f in out["EPL"]["2026-04-22"]] == ["z"]


def test_no_frames_returns_empty_dict() -> None:
    with (
        patch.object(fb, "datetime", _DatetimeStub),
        patch.object(uf, "object_exists", return_value=False),
        patch.object(uf, "split_entity_league_blob_paths", return_value=[]),
    ):
        out = fb.list_fixtures_by_league_and_day(window_days_back=0, window_days_forward=0)

    assert out == {}


def test_window_clamped_to_max_side_days() -> None:
    """Absurd window params are clamped, not passed through unbounded."""
    with (
        patch.object(fb, "datetime", _DatetimeStub),
        patch.object(uf, "object_exists", return_value=False),
        patch.object(uf, "split_entity_league_blob_paths", return_value=[]),
        patch.object(fb, "_read_frames_for_window", wraps=fb._read_frames_for_window) as spy,
    ):
        fb.list_fixtures_by_league_and_day(window_days_back=9999, window_days_forward=9999)

    _, kwargs = spy.call_args
    assert kwargs["inclusive_extra_days"] == fb._MAX_WINDOW_SIDE_DAYS * 2


def _flatten(out: fb.FixturesByLeagueAndDay) -> list[str]:
    """All fixture_ids across every league/day, sorted — filter assertions."""
    return sorted(f["fixture_id"] for by_day in out.values() for day in by_day.values() for f in day)


class TestTeamFilter:
    """``team=`` — case-insensitive substring across home/away name AND id,
    matching whichever side the team played (operator ask 2026-07-17)."""

    def test_matches_home_and_away_side_case_insensitively(self) -> None:
        df = pd.DataFrame(
            [
                _fixture_row("home-side", "2026-04-21T12:00:00Z", home="Arsenal", away="Chelsea"),
                _fixture_row("away-side", "2026-04-21T14:00:00Z", home="Spurs", away="Arsenal"),
                _fixture_row("no-match", "2026-04-21T16:00:00Z", home="Leeds", away="Everton"),
            ]
        )
        with (
            patch.object(fb, "datetime", _DatetimeStub),
            patch.object(uf, "object_exists", return_value=True),
            patch.object(uf, "_read_fixtures_parquet", return_value=df),
        ):
            out = fb.list_fixtures_by_league_and_day(window_days_back=0, window_days_forward=0, team="arSEnal")

        assert _flatten(out) == ["away-side", "home-side"]

    def test_matches_on_team_id_not_only_name(self) -> None:
        row = _fixture_row("by-id", "2026-04-21T12:00:00Z", home="Some Club", away="Other Club")
        row["home_team_id"] = "team-arsenal-42"
        other = _fixture_row("other", "2026-04-21T13:00:00Z", home="X", away="Y")
        other["home_team_id"] = "team-leeds-7"
        with (
            patch.object(fb, "datetime", _DatetimeStub),
            patch.object(uf, "object_exists", return_value=True),
            patch.object(uf, "_read_fixtures_parquet", return_value=pd.DataFrame([row, other])),
        ):
            out = fb.list_fixtures_by_league_and_day(window_days_back=0, window_days_forward=0, team="arsenal-42")

        assert _flatten(out) == ["by-id"]

    def test_blank_team_is_a_no_op_not_a_match_everything_filter(self) -> None:
        df = pd.DataFrame([_fixture_row("a", "2026-04-21T12:00:00Z")])
        with (
            patch.object(fb, "datetime", _DatetimeStub),
            patch.object(uf, "object_exists", return_value=True),
            patch.object(uf, "_read_fixtures_parquet", return_value=df),
        ):
            out = fb.list_fixtures_by_league_and_day(window_days_back=0, window_days_forward=0, team="   ")

        assert _flatten(out) == ["a"]

    def test_team_narrow_is_not_served_from_the_unfiltered_cache_entry(self) -> None:
        """Regression: the cache key MUST include the team filter — otherwise a
        team search silently returns the previously-cached unfiltered rows."""
        df = pd.DataFrame(
            [
                _fixture_row("arsenal-fx", "2026-04-21T12:00:00Z", home="Arsenal", away="Chelsea"),
                _fixture_row("other-fx", "2026-04-21T14:00:00Z", home="Leeds", away="Everton"),
            ]
        )
        with (
            patch.object(fb, "datetime", _DatetimeStub),
            patch.object(uf, "object_exists", return_value=True),
            patch.object(uf, "_read_fixtures_parquet", return_value=df),
        ):
            unfiltered = fb.list_fixtures_by_league_and_day(window_days_back=0, window_days_forward=0)
            filtered = fb.list_fixtures_by_league_and_day(window_days_back=0, window_days_forward=0, team="arsenal")

        assert _flatten(unfiltered) == ["arsenal-fx", "other-fx"]
        assert _flatten(filtered) == ["arsenal-fx"]

    def test_combines_with_league_and_date_narrows(self) -> None:
        df = pd.DataFrame(
            [
                _fixture_row("want", "2026-04-21T12:00:00Z", league_id="EPL", home="Arsenal", away="Chelsea"),
                _fixture_row("wrong-league", "2026-04-21T13:00:00Z", league_id="MLS", home="Arsenal", away="X"),
                _fixture_row("wrong-team", "2026-04-21T14:00:00Z", league_id="EPL", home="Leeds", away="Y"),
            ]
        )
        with (
            patch.object(fb, "datetime", _DatetimeStub),
            patch.object(uf, "object_exists", return_value=True),
            patch.object(uf, "_read_fixtures_parquet", return_value=df),
        ):
            out = fb.list_fixtures_by_league_and_day(
                league_id="EPL", team="arsenal", start_date="2026-04-21", end_date="2026-04-21"
            )

        assert _flatten(out) == ["want"]


class TestAbsoluteDateWindow:
    """``start_date``/``end_date`` — the today-relative window can only reach 60
    days back, so an absolute range is the only way to search an arbitrary
    historical date (operator ask 2026-07-17)."""

    def test_absolute_range_addresses_a_window_far_outside_the_relative_one(self) -> None:
        with (
            patch.object(fb, "datetime", _DatetimeStub),  # "today" = 2026-04-21
            patch.object(fb, "_read_frames_for_window", return_value=[]) as spy,
        ):
            fb.list_fixtures_by_league_and_day(start_date="2024-01-01", end_date="2024-01-08")

        _, kwargs = spy.call_args
        assert kwargs["start"] == date(2024, 1, 1)
        assert kwargs["inclusive_extra_days"] == 7

    def test_start_date_only_fills_end_from_days_forward(self) -> None:
        with (
            patch.object(fb, "datetime", _DatetimeStub),
            patch.object(fb, "_read_frames_for_window", return_value=[]) as spy,
        ):
            fb.list_fixtures_by_league_and_day(start_date="2025-06-01", window_days_forward=3)

        _, kwargs = spy.call_args
        assert kwargs["start"] == date(2025, 6, 1)
        assert kwargs["inclusive_extra_days"] == 3

    def test_end_date_only_fills_start_from_days_back(self) -> None:
        with (
            patch.object(fb, "datetime", _DatetimeStub),
            patch.object(fb, "_read_frames_for_window", return_value=[]) as spy,
        ):
            fb.list_fixtures_by_league_and_day(end_date="2025-06-10", window_days_back=4)

        _, kwargs = spy.call_args
        assert kwargs["start"] == date(2025, 6, 6)
        assert kwargs["inclusive_extra_days"] == 4

    def test_reversed_range_is_swapped_not_negative(self) -> None:
        with (
            patch.object(fb, "datetime", _DatetimeStub),
            patch.object(fb, "_read_frames_for_window", return_value=[]) as spy,
        ):
            fb.list_fixtures_by_league_and_day(start_date="2025-03-10", end_date="2025-03-01")

        _, kwargs = spy.call_args
        assert kwargs["start"] == date(2025, 3, 1)
        assert kwargs["inclusive_extra_days"] == 9

    def test_absolute_span_is_capped(self) -> None:
        """An absolute range must never read more days than the relative window
        could — single-walk discipline (bounded read) preserved."""
        with (
            patch.object(fb, "datetime", _DatetimeStub),
            patch.object(fb, "_read_frames_for_window", return_value=[]) as spy,
        ):
            fb.list_fixtures_by_league_and_day(start_date="2020-01-01", end_date="2026-01-01")

        _, kwargs = spy.call_args
        assert kwargs["start"] == date(2020, 1, 1)
        assert kwargs["inclusive_extra_days"] == fb._MAX_WINDOW_SPAN_DAYS

    def test_unparseable_date_falls_back_to_the_relative_window(self) -> None:
        """A stray query param degrades to the default window rather than 500ing."""
        with (
            patch.object(fb, "datetime", _DatetimeStub),
            patch.object(fb, "_read_frames_for_window", return_value=[]) as spy,
        ):
            fb.list_fixtures_by_league_and_day(start_date="not-a-date", window_days_back=2, window_days_forward=3)

        _, kwargs = spy.call_args
        assert kwargs["start"] == date(2026, 4, 19)  # today(2026-04-21) - 2
        assert kwargs["inclusive_extra_days"] == 5

    def test_distinct_date_windows_do_not_share_a_cache_entry(self) -> None:
        with (
            patch.object(fb, "datetime", _DatetimeStub),
            patch.object(fb, "_read_frames_for_window", return_value=[]) as spy,
        ):
            fb.list_fixtures_by_league_and_day(start_date="2025-01-01", end_date="2025-01-02")
            fb.list_fixtures_by_league_and_day(start_date="2025-05-01", end_date="2025-05-02")

        assert spy.call_count == 2
        assert [c.kwargs["start"] for c in spy.call_args_list] == [date(2025, 1, 1), date(2025, 5, 1)]


class TestLeagueNames:
    """F1 (live UI review round 3, 2026-07-17): the browser grouped by the raw
    API-Football numeric league_id; resolve to the human ``display_name`` from
    UAC, honest-absence when unmapped. The resolver lives in
    ``upcoming_fixtures.py`` (shared with F9's league filter — see that
    module's docstring), ``fb.league_names_for`` is a thin wrapper over it."""

    def test_numeric_api_football_id_resolves_to_display_name(self) -> None:
        # 2 -> UEFA Champions League, 103 -> Eliteserien (real UAC registry entries).
        assert uf._resolve_league_name("2") == "UEFA Champions League"
        assert uf._resolve_league_name("103") == "Eliteserien"

    def test_canonical_string_id_resolves(self) -> None:
        assert uf._resolve_league_name("EPL") == "English Premier League"

    def test_unmapped_id_is_none_not_fabricated(self) -> None:
        # An id with no registry entry stays honest — None, so the map omits it and
        # the UI falls back to the raw id (never a placeholder name).
        assert uf._resolve_league_name("99999999") is None
        assert uf._resolve_league_name("") is None
        assert uf._resolve_league_name("   ") is None

    def test_league_names_for_maps_present_ids_only(self) -> None:
        grouped: fb.FixturesByLeagueAndDay = {
            "2": {"2026-04-21": []},
            "103": {"2026-04-21": []},
            "99999999": {"2026-04-21": []},  # unmapped
        }
        names = fb.league_names_for(grouped)
        assert names == {"2": "UEFA Champions League", "103": "Eliteserien"}
        assert "99999999" not in names  # honest-absence — UI shows the raw id


class TestLeagueFilter:
    """F9 (operator 2026-07-18): the league filter is a case-insensitive
    SUBSTRING match against the raw catalogue id OR its resolved human name —
    it used to be an exact match on the raw id only, so typing a human league
    name (e.g. "Allsvenskan") returned 0 rows even though the fixture's raw
    ``league_id`` was the numeric API-Football key it resolves from."""

    def test_human_name_matches_the_numeric_raw_id_it_resolves_to(self) -> None:
        # 113 -> Allsvenskan (real UAC registry entry) — the catalogue row's raw
        # league_id is the numeric key, never the human name.
        df = pd.DataFrame(
            [
                _fixture_row("swe", "2026-04-21T12:00:00Z", league_id="113"),
                _fixture_row("other", "2026-04-21T13:00:00Z", league_id="EPL"),
            ]
        )
        with (
            patch.object(fb, "datetime", _DatetimeStub),
            patch.object(uf, "object_exists", return_value=True),
            patch.object(uf, "_read_fixtures_parquet", return_value=df),
        ):
            out = fb.list_fixtures_by_league_and_day(window_days_back=0, window_days_forward=0, league_id="Allsvenskan")

        assert _flatten(out) == ["swe"]

    def test_matches_case_insensitively_and_as_a_substring(self) -> None:
        df = pd.DataFrame([_fixture_row("a", "2026-04-21T12:00:00Z", league_id="EPL")])
        with (
            patch.object(fb, "datetime", _DatetimeStub),
            patch.object(uf, "object_exists", return_value=True),
            patch.object(uf, "_read_fixtures_parquet", return_value=df),
        ):
            out = fb.list_fixtures_by_league_and_day(window_days_back=0, window_days_forward=0, league_id="epl")

        assert _flatten(out) == ["a"]

    def test_raw_id_still_matches_when_a_human_name_exists(self) -> None:
        """A numeric-id search must keep working even once the id resolves to a
        name — the filter checks BOTH the raw id and the resolved name."""
        df = pd.DataFrame([_fixture_row("a", "2026-04-21T12:00:00Z", league_id="113")])
        with (
            patch.object(fb, "datetime", _DatetimeStub),
            patch.object(uf, "object_exists", return_value=True),
            patch.object(uf, "_read_fixtures_parquet", return_value=df),
        ):
            out = fb.list_fixtures_by_league_and_day(window_days_back=0, window_days_forward=0, league_id="113")

        assert _flatten(out) == ["a"]

    def test_unmatched_needle_returns_no_leagues(self) -> None:
        df = pd.DataFrame([_fixture_row("a", "2026-04-21T12:00:00Z", league_id="EPL")])
        with (
            patch.object(fb, "datetime", _DatetimeStub),
            patch.object(uf, "object_exists", return_value=True),
            patch.object(uf, "_read_fixtures_parquet", return_value=df),
        ):
            out = fb.list_fixtures_by_league_and_day(
                window_days_back=0, window_days_forward=0, league_id="no-such-league"
            )

        assert out == {}

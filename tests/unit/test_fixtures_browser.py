"""Unit tests for the fixtures browser — single-catalogue source (P10-B).

Rewritten for ``plans/active/sports_fixtures_browser_single_catalogue_source_2026_07_24.md``:
the module now reads ``prod/catalog.parquet`` ONCE (TTL-cached), not a per-day
GCS walk, so these tests mock ``_read_catalogue_fixture_frame`` directly (same
pattern as the sibling ``test_prediction_catalogue.py``) rather than the retired
``upcoming_fixtures`` day-walk primitives.
"""

from __future__ import annotations

import io
from datetime import UTC, date, datetime
from unittest.mock import MagicMock, patch

import pandas as pd
import pytest

from deployment_api.services import fixtures_browser as fb
from deployment_api.services import upcoming_fixtures as uf


@pytest.fixture(autouse=True)
def _clear_caches():
    """Both module-level caches persist across tests — clear before AND after
    each test to keep tests order-independent."""
    fb._clear_cache()  # pyright: ignore[reportPrivateUsage]
    yield
    fb._clear_cache()  # pyright: ignore[reportPrivateUsage]


class _DatetimeStub:
    """Patches ``fb.datetime`` — ``now()`` fixed; ``fromisoformat`` real."""

    UTC = UTC
    fromisoformat = staticmethod(datetime.fromisoformat)

    @staticmethod
    def now(tz=None):
        return datetime(2026, 4, 21, 0, 0, tzinfo=UTC)


def _catalogue_row(
    instrument_id: str,
    kickoff_utc: str,
    *,
    available_from: str | None = None,
    league_id: str = "EPL",
    home: str = "Home",
    away: str = "Away",
    status: str = "NS",
    instrument_type: str = "fixture",
    round_: str = "R1",
) -> dict[str, object]:
    """One ``prod/catalog.parquet`` fixture row — the real schema this module
    reads (see ``fixtures_browser._CATALOGUE_READ_COLUMNS``), not the old
    day-walk parquet's ``fixture_id``/``home_team_id`` shape."""
    return {
        "instrument_id": instrument_id,
        "instrument_type": instrument_type,
        "league_id": league_id,
        "available_from": available_from or kickoff_utc[:10],
        "kickoff_utc": kickoff_utc,
        "status": status,
        "home_team_name": home,
        "away_team_name": away,
        "venue_name": "Stadium",
        "round": round_,
    }


def _df(*rows: dict[str, object]) -> pd.DataFrame:
    return pd.DataFrame(list(rows))


class TestCatalogueRowMapping:
    """The catalogue -> FixtureRow mapping this module owns (P10-B design):
    fixture_id=instrument_id; home/away team ids parsed from the id's
    HOME_v_AWAY segment; venue_id always honestly blank."""

    def test_team_ids_parsed_from_instrument_id(self) -> None:
        home, away = fb._team_ids_from_instrument_id(  # pyright: ignore[reportPrivateUsage]
            "EPL:ARSENAL_v_CHELSEA:20260421"
        )
        assert home == "ARSENAL"
        assert away == "CHELSEA"

    def test_malformed_instrument_id_degrades_honestly(self) -> None:
        assert fb._team_ids_from_instrument_id("not-a-fixture-id") == ("", "")  # pyright: ignore[reportPrivateUsage]
        assert fb._team_ids_from_instrument_id("EPL:NOVSEPARATOR:20260421") == (  # pyright: ignore[reportPrivateUsage]
            "",
            "",
        )

    def test_fixture_id_is_the_catalogue_instrument_id(self) -> None:
        df = _df(_catalogue_row("EPL:ARSENAL_v_CHELSEA:20260421", "2026-04-21T12:00:00Z"))
        with (
            patch.object(fb, "datetime", _DatetimeStub),
            patch.object(fb, "_read_catalogue_fixture_frame", return_value=df),
        ):
            out = fb.list_fixtures_by_league_and_day(window_days_back=0, window_days_forward=1)

        fx = out["EPL"]["2026-04-21"][0]
        assert fx["fixture_id"] == "EPL:ARSENAL_v_CHELSEA:20260421"
        assert fx["home_team_id"] == "ARSENAL"
        assert fx["away_team_id"] == "CHELSEA"

    def test_venue_id_is_always_honestly_blank(self) -> None:
        """The catalogue does not carry venue_id — never fabricate one."""
        df = _df(_catalogue_row("EPL:ARSENAL_v_CHELSEA:20260421", "2026-04-21T12:00:00Z"))
        with (
            patch.object(fb, "datetime", _DatetimeStub),
            patch.object(fb, "_read_catalogue_fixture_frame", return_value=df),
        ):
            out = fb.list_fixtures_by_league_and_day(window_days_back=0, window_days_forward=1)

        assert out["EPL"]["2026-04-21"][0]["venue_id"] == ""

    def test_non_fixture_instrument_types_are_filtered_by_the_reader(self) -> None:
        """_read_catalogue_fixture_frame itself excludes team/player catalogue
        rows — real parquet bytes end-to-end through pyarrow/pandas (no
        internal mocking), only the GCS download is faked."""
        raw = _df(
            _catalogue_row("EPL:ARSENAL_v_CHELSEA:20260421", "2026-04-21T12:00:00Z", instrument_type="fixture"),
            {
                "instrument_id": "ARSENAL",
                "instrument_type": "team",
                "league_id": "EPL",
                "available_from": "2026-04-21",
                "kickoff_utc": "",
                "status": "",
                "home_team_name": "",
                "away_team_name": "",
                "venue_name": "",
                "round": "",
            },
        )
        buf = io.BytesIO()
        raw.to_parquet(buf)
        fake_client = MagicMock()
        fake_client.download_bytes.return_value = buf.getvalue()

        with (
            patch.object(fb, "get_storage_client", return_value=fake_client),
            patch.object(fb, "_sports_bucket", return_value="bkt"),
        ):
            result = fb._read_catalogue_fixture_frame()  # pyright: ignore[reportPrivateUsage]

        assert result is not None
        assert set(result["instrument_id"]) == {"EPL:ARSENAL_v_CHELSEA:20260421"}


def test_groups_by_league_then_day() -> None:
    df = _df(
        _catalogue_row("EPL:A_v_B:20260421", "2026-04-21T12:00:00Z", league_id="EPL"),
        _catalogue_row("MLS:C_v_D:20260421", "2026-04-21T15:00:00Z", league_id="MLS"),
        _catalogue_row("EPL:E_v_F:20260422", "2026-04-22T12:00:00Z", league_id="EPL"),
    )

    with (
        patch.object(fb, "datetime", _DatetimeStub),
        patch.object(fb, "_read_catalogue_fixture_frame", return_value=df),
    ):
        out = fb.list_fixtures_by_league_and_day(window_days_back=0, window_days_forward=1)

    assert set(out.keys()) == {"EPL", "MLS"}
    assert set(out["EPL"].keys()) == {"2026-04-21", "2026-04-22"}
    assert [f["fixture_id"] for f in out["EPL"]["2026-04-21"]] == ["EPL:A_v_B:20260421"]
    assert [f["fixture_id"] for f in out["EPL"]["2026-04-22"]] == ["EPL:E_v_F:20260422"]
    assert set(out["MLS"].keys()) == {"2026-04-21"}
    assert [f["fixture_id"] for f in out["MLS"]["2026-04-21"]] == ["MLS:C_v_D:20260421"]


def test_groups_by_available_from_not_kickoff_day() -> None:
    """Regression: grouping/filtering keys on available_from (the verified true
    fixture date), NOT the kickoff_utc's own calendar day — a late-kickoff (or
    otherwise day-mismatched) row must still land under its available_from day."""
    df = _df(_catalogue_row("EPL:A_v_B:20260421", "2026-04-22T00:30:00Z", available_from="2026-04-21", league_id="EPL"))

    with (
        patch.object(fb, "datetime", _DatetimeStub),
        patch.object(fb, "_read_catalogue_fixture_frame", return_value=df),
    ):
        out = fb.list_fixtures_by_league_and_day(window_days_back=0, window_days_forward=1)

    assert set(out["EPL"].keys()) == {"2026-04-21"}


def test_empty_frame_produces_no_phantom_bucket() -> None:
    df = _df(_catalogue_row("EPL:A_v_B:20260421", "2026-04-21T12:00:00Z"))

    with (
        patch.object(fb, "datetime", _DatetimeStub),
        patch.object(fb, "_read_catalogue_fixture_frame", return_value=df),
    ):
        out = fb.list_fixtures_by_league_and_day(window_days_back=0, window_days_forward=2)

    assert set(out.keys()) == {"EPL"}
    assert set(out["EPL"].keys()) == {"2026-04-21"}


def test_league_filter() -> None:
    df = _df(
        _catalogue_row("EPL:X_v_Y:20260421", "2026-04-21T12:00:00Z", league_id="EPL"),
        _catalogue_row("MLS:Y_v_X:20260421", "2026-04-21T14:00:00Z", league_id="MLS"),
    )

    with (
        patch.object(fb, "datetime", _DatetimeStub),
        patch.object(fb, "_read_catalogue_fixture_frame", return_value=df),
    ):
        out = fb.list_fixtures_by_league_and_day(window_days_back=0, window_days_forward=0, league_id="EPL")

    assert set(out.keys()) == {"EPL"}
    assert [f["fixture_id"] for day in out["EPL"].values() for f in day] == ["EPL:X_v_Y:20260421"]


def test_catalogue_read_failure_returns_empty_not_raises() -> None:
    """A catalogue read failure (missing bucket/object, unreadable parquet)
    degrades to an honest empty result — never raises."""
    with (
        patch.object(fb, "datetime", _DatetimeStub),
        patch.object(fb, "_read_catalogue_fixture_frame", return_value=None),
    ):
        out = fb.list_fixtures_by_league_and_day(window_days_back=0, window_days_forward=1)

    assert out == {}


def test_no_rows_in_window_returns_empty_dict() -> None:
    df = _df(_catalogue_row("EPL:A_v_B:20200101", "2020-01-01T12:00:00Z", available_from="2020-01-01"))

    with (
        patch.object(fb, "datetime", _DatetimeStub),
        patch.object(fb, "_read_catalogue_fixture_frame", return_value=df),
    ):
        out = fb.list_fixtures_by_league_and_day(window_days_back=0, window_days_forward=0)

    assert out == {}


def test_relative_window_clamped_to_max_side_days() -> None:
    """Absurd window params are clamped before they reach _resolve_window."""
    with (
        patch.object(fb, "datetime", _DatetimeStub),
        patch.object(fb, "_read_catalogue_fixture_frame", return_value=None),
        patch.object(fb, "_resolve_window", wraps=fb._resolve_window) as spy,
    ):
        fb.list_fixtures_by_league_and_day(window_days_back=9999, window_days_forward=9999)

    _, kwargs = spy.call_args
    assert kwargs["window_days_back"] == fb._MAX_WINDOW_SIDE_DAYS
    assert kwargs["window_days_forward"] == fb._MAX_WINDOW_SIDE_DAYS


def _flatten(out: fb.FixturesByLeagueAndDay) -> list[str]:
    """All fixture_ids across every league/day, sorted — filter assertions."""
    return sorted(f["fixture_id"] for by_day in out.values() for day in by_day.values() for f in day)


class TestTeamFilter:
    """``team=`` — case-insensitive substring across home/away name AND id,
    matching whichever side the team played (operator ask 2026-07-17)."""

    def test_matches_home_and_away_side_case_insensitively(self) -> None:
        df = _df(
            _catalogue_row("EPL:home_v_away1:20260421", "2026-04-21T12:00:00Z", home="Arsenal", away="Chelsea"),
            _catalogue_row("EPL:home2_v_away2:20260421", "2026-04-21T14:00:00Z", home="Spurs", away="Arsenal"),
            _catalogue_row("EPL:home3_v_away3:20260421", "2026-04-21T16:00:00Z", home="Leeds", away="Everton"),
        )
        with (
            patch.object(fb, "datetime", _DatetimeStub),
            patch.object(fb, "_read_catalogue_fixture_frame", return_value=df),
        ):
            out = fb.list_fixtures_by_league_and_day(window_days_back=0, window_days_forward=0, team="arSEnal")

        assert _flatten(out) == ["EPL:home2_v_away2:20260421", "EPL:home_v_away1:20260421"]

    def test_matches_on_team_id_not_only_name(self) -> None:
        """Team-id search matches the id parsed out of the instrument_id, not a
        raw catalogue column (the catalogue doesn't carry team ids directly)."""
        df = _df(
            _catalogue_row("EPL:ARSENAL_v_OTHER:20260421", "2026-04-21T12:00:00Z", home="Some Club", away="Other Club"),
            _catalogue_row("EPL:LEEDS_v_OTHER:20260421", "2026-04-21T13:00:00Z", home="X", away="Y"),
        )
        with (
            patch.object(fb, "datetime", _DatetimeStub),
            patch.object(fb, "_read_catalogue_fixture_frame", return_value=df),
        ):
            out = fb.list_fixtures_by_league_and_day(window_days_back=0, window_days_forward=0, team="arsenal")

        assert _flatten(out) == ["EPL:ARSENAL_v_OTHER:20260421"]

    def test_blank_team_is_a_no_op_not_a_match_everything_filter(self) -> None:
        df = _df(_catalogue_row("EPL:A_v_B:20260421", "2026-04-21T12:00:00Z"))
        with (
            patch.object(fb, "datetime", _DatetimeStub),
            patch.object(fb, "_read_catalogue_fixture_frame", return_value=df),
        ):
            out = fb.list_fixtures_by_league_and_day(window_days_back=0, window_days_forward=0, team="   ")

        assert _flatten(out) == ["EPL:A_v_B:20260421"]

    def test_team_narrow_is_not_served_from_the_unfiltered_cache_entry(self) -> None:
        """Regression: the cache key MUST include the team filter — otherwise a
        team search silently returns the previously-cached unfiltered rows."""
        df = _df(
            _catalogue_row("EPL:ARSENAL_v_CHELSEA:20260421", "2026-04-21T12:00:00Z", home="Arsenal", away="Chelsea"),
            _catalogue_row("EPL:LEEDS_v_EVERTON:20260421", "2026-04-21T14:00:00Z", home="Leeds", away="Everton"),
        )
        with (
            patch.object(fb, "datetime", _DatetimeStub),
            patch.object(fb, "_read_catalogue_fixture_frame", return_value=df),
        ):
            unfiltered = fb.list_fixtures_by_league_and_day(window_days_back=0, window_days_forward=0)
            filtered = fb.list_fixtures_by_league_and_day(window_days_back=0, window_days_forward=0, team="arsenal")

        assert _flatten(unfiltered) == ["EPL:ARSENAL_v_CHELSEA:20260421", "EPL:LEEDS_v_EVERTON:20260421"]
        assert _flatten(filtered) == ["EPL:ARSENAL_v_CHELSEA:20260421"]

    def test_combines_with_league_and_date_narrows(self) -> None:
        df = _df(
            _catalogue_row(
                "EPL:ARSENAL_v_CHELSEA:20260421",
                "2026-04-21T12:00:00Z",
                league_id="EPL",
                home="Arsenal",
                away="Chelsea",
            ),
            _catalogue_row(
                "MLS:ARSENAL_v_X:20260421", "2026-04-21T13:00:00Z", league_id="MLS", home="Arsenal", away="X"
            ),
            _catalogue_row("EPL:LEEDS_v_Y:20260421", "2026-04-21T14:00:00Z", league_id="EPL", home="Leeds", away="Y"),
        )
        with (
            patch.object(fb, "datetime", _DatetimeStub),
            patch.object(fb, "_read_catalogue_fixture_frame", return_value=df),
        ):
            out = fb.list_fixtures_by_league_and_day(
                league_id="EPL", team="arsenal", start_date="2026-04-21", end_date="2026-04-21"
            )

        assert _flatten(out) == ["EPL:ARSENAL_v_CHELSEA:20260421"]


class TestResolveWindow:
    """``_resolve_window`` — the pure date-window resolver (P10-B: absolute
    mode is now UNCAPPED, unlike the retired day-walk's ``_MAX_WINDOW_SPAN_DAYS``)."""

    def test_relative_default_window(self) -> None:
        with patch.object(fb, "datetime", _DatetimeStub):
            start, extra_days = fb._resolve_window(  # pyright: ignore[reportPrivateUsage]
                window_days_back=7, window_days_forward=30, start_date=None, end_date=None
            )
        assert start == date(2026, 4, 14)
        assert extra_days == 37

    def test_absolute_range_addresses_a_window_far_outside_the_relative_one(self) -> None:
        with patch.object(fb, "datetime", _DatetimeStub):  # "today" = 2026-04-21
            start, extra_days = fb._resolve_window(  # pyright: ignore[reportPrivateUsage]
                window_days_back=7, window_days_forward=30, start_date="2024-01-01", end_date="2024-01-08"
            )
        assert start == date(2024, 1, 1)
        assert extra_days == 7

    def test_start_date_only_fills_end_from_days_forward(self) -> None:
        with patch.object(fb, "datetime", _DatetimeStub):
            start, extra_days = fb._resolve_window(  # pyright: ignore[reportPrivateUsage]
                window_days_back=7, window_days_forward=3, start_date="2025-06-01", end_date=None
            )
        assert start == date(2025, 6, 1)
        assert extra_days == 3

    def test_end_date_only_fills_start_from_days_back(self) -> None:
        with patch.object(fb, "datetime", _DatetimeStub):
            start, extra_days = fb._resolve_window(  # pyright: ignore[reportPrivateUsage]
                window_days_back=4, window_days_forward=30, start_date=None, end_date="2025-06-10"
            )
        assert start == date(2025, 6, 6)
        assert extra_days == 4

    def test_reversed_range_is_swapped_not_negative(self) -> None:
        with patch.object(fb, "datetime", _DatetimeStub):
            start, extra_days = fb._resolve_window(  # pyright: ignore[reportPrivateUsage]
                window_days_back=7, window_days_forward=30, start_date="2025-03-10", end_date="2025-03-01"
            )
        assert start == date(2025, 3, 1)
        assert extra_days == 9

    def test_absolute_span_is_no_longer_capped(self) -> None:
        """Regression guard for the P10-B ask: the old day-walk's
        _MAX_WINDOW_SPAN_DAYS clamp must be GONE — a huge absolute range spans
        its full length, uncapped."""
        with patch.object(fb, "datetime", _DatetimeStub):
            start, extra_days = fb._resolve_window(  # pyright: ignore[reportPrivateUsage]
                window_days_back=7, window_days_forward=30, start_date="2020-01-01", end_date="2026-01-01"
            )
        assert start == date(2020, 1, 1)
        assert extra_days == (date(2026, 1, 1) - date(2020, 1, 1)).days

    def test_max_window_span_days_constant_is_deleted(self) -> None:
        assert not hasattr(fb, "_MAX_WINDOW_SPAN_DAYS")

    def test_unparseable_date_falls_back_to_the_relative_window(self) -> None:
        """A stray query param degrades to the default window rather than 500ing."""
        with patch.object(fb, "datetime", _DatetimeStub):
            start, extra_days = fb._resolve_window(  # pyright: ignore[reportPrivateUsage]
                window_days_back=2, window_days_forward=3, start_date="not-a-date", end_date=None
            )
        assert start == date(2026, 4, 19)  # today(2026-04-21) - 2
        assert extra_days == 5


def test_distinct_date_windows_do_not_share_a_cache_entry() -> None:
    df = _df(
        _catalogue_row("EPL:A_v_B:20250101", "2025-01-01T12:00:00Z", available_from="2025-01-01"),
        _catalogue_row("EPL:C_v_D:20250501", "2025-05-01T12:00:00Z", available_from="2025-05-01"),
    )
    with (
        patch.object(fb, "datetime", _DatetimeStub),
        patch.object(fb, "_read_catalogue_fixture_frame", return_value=df),
    ):
        jan = fb.list_fixtures_by_league_and_day(start_date="2025-01-01", end_date="2025-01-02")
        may = fb.list_fixtures_by_league_and_day(start_date="2025-05-01", end_date="2025-05-02")

    assert _flatten(jan) == ["EPL:A_v_B:20250101"]
    assert _flatten(may) == ["EPL:C_v_D:20250501"]


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
        df = _df(
            _catalogue_row("113:A_v_B:20260421", "2026-04-21T12:00:00Z", league_id="113"),
            _catalogue_row("EPL:C_v_D:20260421", "2026-04-21T13:00:00Z", league_id="EPL"),
        )
        with (
            patch.object(fb, "datetime", _DatetimeStub),
            patch.object(fb, "_read_catalogue_fixture_frame", return_value=df),
        ):
            out = fb.list_fixtures_by_league_and_day(window_days_back=0, window_days_forward=0, league_id="Allsvenskan")

        assert _flatten(out) == ["113:A_v_B:20260421"]

    def test_matches_case_insensitively_and_as_a_substring(self) -> None:
        df = _df(_catalogue_row("EPL:A_v_B:20260421", "2026-04-21T12:00:00Z", league_id="EPL"))
        with (
            patch.object(fb, "datetime", _DatetimeStub),
            patch.object(fb, "_read_catalogue_fixture_frame", return_value=df),
        ):
            out = fb.list_fixtures_by_league_and_day(window_days_back=0, window_days_forward=0, league_id="epl")

        assert _flatten(out) == ["EPL:A_v_B:20260421"]

    def test_raw_id_still_matches_when_a_human_name_exists(self) -> None:
        """A numeric-id search must keep working even once the id resolves to a
        name — the filter checks BOTH the raw id and the resolved name."""
        df = _df(_catalogue_row("113:A_v_B:20260421", "2026-04-21T12:00:00Z", league_id="113"))
        with (
            patch.object(fb, "datetime", _DatetimeStub),
            patch.object(fb, "_read_catalogue_fixture_frame", return_value=df),
        ):
            out = fb.list_fixtures_by_league_and_day(window_days_back=0, window_days_forward=0, league_id="113")

        assert _flatten(out) == ["113:A_v_B:20260421"]

    def test_unmatched_needle_returns_no_leagues(self) -> None:
        df = _df(_catalogue_row("EPL:A_v_B:20260421", "2026-04-21T12:00:00Z", league_id="EPL"))
        with (
            patch.object(fb, "datetime", _DatetimeStub),
            patch.object(fb, "_read_catalogue_fixture_frame", return_value=df),
        ):
            out = fb.list_fixtures_by_league_and_day(
                window_days_back=0, window_days_forward=0, league_id="no-such-league"
            )

        assert out == {}

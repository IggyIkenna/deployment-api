"""Unit tests for upcoming fixtures GCS reader."""

from __future__ import annotations

from datetime import UTC, datetime
from unittest.mock import patch

import pandas as pd
import pytest

from deployment_api.services import upcoming_fixtures as uf


@pytest.fixture(autouse=True)
def _clear_fixtures_cache():
    """The ``_FIXTURES_CACHE`` module-level dict in upcoming_fixtures persists
    across tests.  ``test_list_upcoming_fixtures_concat_sort_dedupe`` runs with
    ``days=1`` (cache_key=(1, None)) and writes its results; subsequent tests
    using the same cache_key get those stale results back.  Clear before AND
    after each test to keep tests order-independent."""
    uf._FIXTURES_CACHE.clear()
    yield
    uf._FIXTURES_CACHE.clear()


class _DatetimeStub:
    """Patches ``uf.datetime`` — ``now()`` fixed; ``fromisoformat`` real."""

    UTC = UTC
    fromisoformat = staticmethod(datetime.fromisoformat)

    @staticmethod
    def now(tz=None):
        return datetime(2026, 4, 21, 0, 0, tzinfo=UTC)


def test_list_upcoming_fixtures_concat_sort_dedupe() -> None:
    df1 = pd.DataFrame(
        [
            {
                "fixture_id": "b",
                "kickoff_utc": "2026-04-22T15:00:00Z",
                "league_id": "EPL",
                "home_team_id": "h1",
                "away_team_id": "a1",
                "home_team_name": "Home",
                "away_team_name": "Away",
                "venue_id": "v1",
                "venue_name": "Stadium",
                "status": "NS",
                "round_name": "R10",
            },
            {
                "fixture_id": "a",
                "kickoff_utc": "2026-04-21T12:00:00+00:00",
                "league_id": "EPL",
                "home_team_id": "h2",
                "away_team_id": "a2",
                "home_team_name": "H2",
                "away_team_name": "A2",
                "venue_id": "v2",
                "venue_name": "Ground",
                "status": "NS",
                "round": "R9",
            },
        ]
    )
    df2 = pd.DataFrame(
        [
            {
                "fixture_id": "a",
                "kickoff_utc": "2026-04-21T12:00:00+00:00",
                "league_id": "EPL",
                "home_team_id": "h2",
                "away_team_id": "a2",
                "home_team_name": "H2",
                "away_team_name": "A2",
                "venue_id": "v2",
                "venue_name": "Ground",
                "status": "NS",
                "round": "R9",
            },
        ]
    )

    def fake_exists(_bucket: str, path: str) -> bool:
        return "day=2026-04-21" in path or "day=2026-04-22" in path

    def fake_read(_bucket: str, path: str) -> pd.DataFrame:
        if "day=2026-04-21" in path:
            return df1
        if "day=2026-04-22" in path:
            return df2
        return pd.DataFrame()

    with (
        patch.object(uf, "datetime", _DatetimeStub),
        patch.object(uf, "object_exists", side_effect=fake_exists),
        patch.object(uf, "_read_fixtures_parquet", side_effect=fake_read),
    ):
        out = uf.list_upcoming_fixtures(days=1, project_id="test-proj")

    assert [f["fixture_id"] for f in out] == ["a", "b"]
    assert out[0]["league_id"] == "EPL"
    assert out[0]["round"] == "R9"


def test_list_upcoming_fixtures_league_filter() -> None:
    df = pd.DataFrame(
        [
            {
                "fixture_id": "x",
                "kickoff_utc": "2026-04-21T12:00:00Z",
                "league_id": "EPL",
                "home_team_id": "h",
                "away_team_id": "a",
                "home_team_name": "H",
                "away_team_name": "A",
                "venue_id": "v",
                "venue_name": "V",
                "status": "NS",
                "round_name": "R1",
            },
            {
                "fixture_id": "y",
                "kickoff_utc": "2026-04-21T14:00:00Z",
                "league_id": "MLS",
                "home_team_id": "h",
                "away_team_id": "a",
                "home_team_name": "H",
                "away_team_name": "A",
                "venue_id": "v",
                "venue_name": "V",
                "status": "NS",
                "round_name": "R1",
            },
        ]
    )

    with (
        patch.object(uf, "datetime", _DatetimeStub),
        patch.object(uf, "object_exists", return_value=True),
        patch.object(uf, "_read_fixtures_parquet", return_value=df),
    ):
        out = uf.list_upcoming_fixtures(days=0, league_id="EPL", project_id="p")

    assert len(out) == 1
    assert out[0]["fixture_id"] == "x"


class TestLeagueFilterSubstring:
    """F9 (operator 2026-07-18): the league filter is a case-insensitive
    SUBSTRING match against the raw catalogue id OR its resolved human name —
    consistent with the ``team`` filter's existing substring semantics
    (previously an exact match on the raw id only, so a human league name
    typed into the filter — e.g. "Allsvenskan" — returned 0 rows)."""

    def _two_league_df(self) -> pd.DataFrame:
        return pd.DataFrame(
            [
                {
                    "fixture_id": "swe",
                    "kickoff_utc": "2026-04-21T12:00:00Z",
                    "league_id": "113",  # Allsvenskan's api_football_id (UAC)
                    "home_team_id": "h",
                    "away_team_id": "a",
                    "home_team_name": "H",
                    "away_team_name": "A",
                    "venue_id": "v",
                    "venue_name": "V",
                    "status": "NS",
                    "round_name": "R1",
                },
                {
                    "fixture_id": "eng",
                    "kickoff_utc": "2026-04-21T13:00:00Z",
                    "league_id": "EPL",
                    "home_team_id": "h",
                    "away_team_id": "a",
                    "home_team_name": "H",
                    "away_team_name": "A",
                    "venue_id": "v",
                    "venue_name": "V",
                    "status": "NS",
                    "round_name": "R1",
                },
            ]
        )

    def test_human_name_matches_the_numeric_raw_id_it_resolves_to(self) -> None:
        with (
            patch.object(uf, "datetime", _DatetimeStub),
            patch.object(uf, "object_exists", return_value=True),
            patch.object(uf, "_read_fixtures_parquet", return_value=self._two_league_df()),
        ):
            out = uf.list_upcoming_fixtures(days=0, league_id="Allsvenskan", project_id="p")

        assert [f["fixture_id"] for f in out] == ["swe"]

    def test_case_insensitive_substring_on_raw_id(self) -> None:
        with (
            patch.object(uf, "datetime", _DatetimeStub),
            patch.object(uf, "object_exists", return_value=True),
            patch.object(uf, "_read_fixtures_parquet", return_value=self._two_league_df()),
        ):
            out = uf.list_upcoming_fixtures(days=0, league_id="epl", project_id="p")

        assert [f["fixture_id"] for f in out] == ["eng"]

    def test_raw_id_still_matches_when_a_human_name_exists(self) -> None:
        with (
            patch.object(uf, "datetime", _DatetimeStub),
            patch.object(uf, "object_exists", return_value=True),
            patch.object(uf, "_read_fixtures_parquet", return_value=self._two_league_df()),
        ):
            out = uf.list_upcoming_fixtures(days=0, league_id="113", project_id="p")

        assert [f["fixture_id"] for f in out] == ["swe"]


class TestLeagueNamesForFixtures:
    """``league_names_for_fixtures`` — the ``/fixtures/upcoming`` counterpart to
    ``fixtures_browser.league_names_for``, so the flat upcoming-fixtures list
    can also render human league names (F9)."""

    def test_maps_distinct_league_ids_only_omits_unmapped(self) -> None:
        fixtures: list[uf.UpcomingFixture] = [
            uf.UpcomingFixture(
                fixture_id="a",
                kickoff_utc="2026-04-21T12:00:00+00:00",
                league_id="113",
                home_team_id="h",
                away_team_id="a",
                home_team_name="H",
                away_team_name="A",
                venue_id="v",
                venue_name="V",
                status="NS",
                round="R1",
            ),
            uf.UpcomingFixture(
                fixture_id="b",
                kickoff_utc="2026-04-21T13:00:00+00:00",
                league_id="99999999",  # unmapped
                home_team_id="h",
                away_team_id="a",
                home_team_name="H",
                away_team_name="A",
                venue_id="v",
                venue_name="V",
                status="NS",
                round="R1",
            ),
        ]

        names = uf.league_names_for_fixtures(fixtures)

        assert names == {"113": "Allsvenskan"}
        assert "99999999" not in names


def test_list_upcoming_fixtures_skips_failed_day() -> None:
    df_ok = pd.DataFrame(
        [
            {
                "fixture_id": "z",
                "kickoff_utc": "2026-04-22T12:00:00Z",
                "league_id": "EPL",
                "home_team_id": "h",
                "away_team_id": "a",
                "home_team_name": "H",
                "away_team_name": "A",
                "venue_id": "v",
                "venue_name": "V",
                "status": "NS",
                "round_name": "R1",
            },
        ]
    )

    def fake_read(_bucket: str, path: str) -> pd.DataFrame:
        if "day=2026-04-21" in path:
            raise OSError("read failed")
        if "day=2026-04-22" in path:
            return df_ok
        return pd.DataFrame()

    with (
        patch.object(uf, "datetime", _DatetimeStub),
        patch.object(uf, "object_exists", return_value=True),
        patch.object(uf, "_read_fixtures_parquet", side_effect=fake_read),
    ):
        out = uf.list_upcoming_fixtures(days=1, project_id="p")

    assert len(out) == 1
    assert out[0]["fixture_id"] == "z"


class TestSplitEntityFallback:
    """instruments-service cut the FIXTURES writer over to the
    fixtures_schedule/fixtures_outcomes entity-folder split with no legacy
    dual-write (first observed 2026-07-14, no feature-flag gate) — see
    plans/active/issues/features_sports_fixtures_split_reader_gap_2026_07_15.md.
    ``_read_one_day_frame`` must fall back to the split ``fixtures_schedule``
    per-league shards when the legacy singleton is absent.
    """

    def test_falls_back_to_split_schedule_shards_when_singleton_missing(self) -> None:
        raw_shard = pd.DataFrame(
            [
                {
                    "af_fixture_id": "12345",
                    "timestamp": "2026-04-21T12:00:00+00:00",
                    "af_league_id": "39",
                    "af_home_name": "Home FC",
                    "af_away_name": "Away FC",
                    "status_short": "NS",
                    "venue_id": "v1",
                    "venue_name": "Stadium",
                    "round": "R9",
                }
            ]
        )

        with (
            patch.object(uf, "datetime", _DatetimeStub),
            patch.object(uf, "object_exists", return_value=False),
            patch.object(
                uf,
                "split_entity_league_blob_paths",
                return_value=[
                    "sports_reference/by_date/day=2026-04-21/pipeline_mode=batch_api_football/entity=fixtures_schedule/league=EPL/fixtures_schedule.parquet"
                ],
            ),
            patch.object(uf, "_read_fixtures_parquet", return_value=raw_shard),
        ):
            out = uf.list_upcoming_fixtures(days=0, project_id="p")

        assert len(out) == 1
        assert out[0]["fixture_id"] == "12345"
        assert out[0]["home_team_name"] == "Home FC"
        assert out[0]["away_team_name"] == "Away FC"
        assert out[0]["league_id"] == "39"
        assert out[0]["status"] == "NS"
        assert out[0]["round"] == "R9"

    def test_genuine_gap_when_neither_singleton_nor_split_shards_exist(self) -> None:
        with (
            patch.object(uf, "datetime", _DatetimeStub),
            patch.object(uf, "object_exists", return_value=False),
            patch.object(uf, "split_entity_league_blob_paths", return_value=[]),
        ):
            out = uf.list_upcoming_fixtures(days=0, project_id="p")

        assert out == []

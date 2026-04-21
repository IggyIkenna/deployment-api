"""Unit tests for upcoming fixtures GCS reader."""

from __future__ import annotations

from datetime import UTC, datetime
from unittest.mock import patch

import pandas as pd

from deployment_api.services import upcoming_fixtures as uf


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

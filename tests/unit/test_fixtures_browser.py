"""Unit tests for the fixtures browser — league -> day grouping (P9)."""

from __future__ import annotations

from datetime import UTC, datetime
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

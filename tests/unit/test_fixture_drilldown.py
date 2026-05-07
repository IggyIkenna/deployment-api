"""Unit tests for per-fixture drilldown helpers.

Covers the Phase-1 + Phase-3 scope of
``sports_data_status_fixture_level_drilldown_2026_04_21``:

- ``build_fixture_breakdown`` for a (day, league_id) slice with:
  - all entities captured
  - mixed captured + empty_confirmed + missing
  - master fixtures parquet absent ("no schedule" / Phase-3 red-breakdown)
  - per-fixture missing even when the entity itself captured at the day level
- ``build_fixture_download`` for CSV + JSON shape, including 404 on unknown
  fixture_id.
- Defensive behaviours: unknown league_id / league with no api_football_id.

The tests mock ``_read_parquet_columns`` per-URI so no GCS calls are made.
"""

from __future__ import annotations

import json
from collections.abc import Callable, Iterable
from typing import cast
from unittest.mock import patch

import pandas as pd
import pytest

import deployment_api.services.data_status_drilldown as drilldown


@pytest.fixture(autouse=True)
def _clear_cache():
    drilldown.clear_drilldown_cache()
    yield
    drilldown.clear_drilldown_cache()


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


_DAY = "2024-09-15"
_LEAGUE = "EPL"
_AF_ID = 39  # EPL numeric in API-Football


def _fixtures_df(rows: list[dict[str, object]]) -> pd.DataFrame:
    """Build a fixtures.parquet shaped DataFrame."""
    columns = [
        "fixture_id",
        "af_league_id",
        "kickoff_utc",
        "home_team_name",
        "away_team_name",
        "status",
        "venue_id",
    ]
    if not rows:
        return pd.DataFrame({c: pd.Series(dtype="object") for c in columns})
    return pd.DataFrame(rows, columns=columns)


def _entity_df(
    fixture_ids: Iterable[str], extra_cols: dict[str, object] | None = None
) -> pd.DataFrame:
    """Build a per-entity parquet shaped DataFrame keyed by fixture_id."""
    fid_list = list(fixture_ids)
    base: dict[str, object] = {"fixture_id": fid_list}
    if extra_cols:
        for k, v in extra_cols.items():
            base[k] = [v] * len(fid_list)
    return pd.DataFrame(base)


def _dispatch_uri_reader(
    mapping: dict[str, pd.DataFrame | type[OSError]],
) -> Callable[..., pd.DataFrame]:
    """Return a side_effect callable for ``_read_parquet_columns``.

    ``mapping`` values are DataFrames to return, or an exception class to
    raise (for ``attempted_failed`` scenarios).
    """

    def _reader(gs_uri: str, columns: list[str] | None = None) -> pd.DataFrame:
        # Match by the entity path suffix rather than the full URI so tests
        # don't need to bake the pid into expectations.
        for key, val in mapping.items():
            if key in gs_uri:
                if isinstance(val, type) and issubclass(val, OSError):
                    raise val(f"Simulated missing parquet: {gs_uri}")
                return val.copy()
        raise FileNotFoundError(f"Unmapped URI in test: {gs_uri}")

    return _reader


def _dispatch_schema_prober(
    mapping: dict[str, pd.DataFrame | type[OSError]],
) -> Callable[[str], set[str]]:
    """Return a side_effect callable for ``_parquet_schema_names``.

    Added 2026-04-24 with the schema-adaptive drilldown refactor
    (``_probe_fid_column`` + ``_FIXTURE_META_ALIASES``). The probe derives the
    parquet column names from the mocked DataFrames so tests stay hermetic.
    """

    def _prober(gs_uri: str) -> set[str]:
        for key, val in mapping.items():
            if key in gs_uri:
                if isinstance(val, type) and issubclass(val, OSError):
                    raise val(f"Simulated missing parquet: {gs_uri}")
                return set(val.columns)
        raise FileNotFoundError(f"Unmapped URI in test: {gs_uri}")

    return _prober


# ---------------------------------------------------------------------------
# build_fixture_breakdown
# ---------------------------------------------------------------------------


class TestBuildFixtureBreakdown:
    def test_all_entities_captured(self):
        fixtures = _fixtures_df(
            [
                {
                    "fixture_id": "fx-1",
                    "af_league_id": _AF_ID,
                    "kickoff_utc": "2024-09-15T14:00:00Z",
                    "home_team_name": "Arsenal",
                    "away_team_name": "Liverpool",
                    "status": "FT",
                    "venue_id": "v-1",
                },
                {
                    "fixture_id": "fx-2",
                    "af_league_id": _AF_ID,
                    "kickoff_utc": "2024-09-15T16:30:00Z",
                    "home_team_name": "Chelsea",
                    "away_team_name": "Man City",
                    "status": "FT",
                    "venue_id": "v-2",
                },
            ]
        )
        all_ids = ["fx-1", "fx-2"]
        mapping: dict[str, pd.DataFrame | type[OSError]] = {
            "entity=fixtures": fixtures,
            "entity=fixture_stats": _entity_df(all_ids),
            "entity=fixture_lineups": _entity_df(all_ids),
            "entity=fixture_events": _entity_df(all_ids),
            "entity=player_stats": _entity_df(all_ids),
            "entity=injuries": _entity_df(all_ids),
            "entity=understat_xg": _entity_df(all_ids),
            "entity=weather": _entity_df(all_ids),
        }

        with (
            patch.object(
                drilldown, "_read_parquet_columns", side_effect=_dispatch_uri_reader(mapping)
            ),
            patch.object(
                drilldown, "_parquet_schema_names", side_effect=_dispatch_schema_prober(mapping)
            ),
        ):
            result = drilldown.build_fixture_breakdown(day=_DAY, league_id=_LEAGUE)

        assert result["status"] == "resolved"
        assert result["fixtures_expected"] == 2
        assert result["af_league_id"] == _AF_ID
        fx = cast(list[dict[str, object]], result["fixtures"])
        assert {f["fixture_id"] for f in fx} == {"fx-1", "fx-2"}
        for f in fx:
            cov = cast(dict[str, str], f["coverage"])
            assert set(cov.values()) == {"captured"}
            summary = cast(dict[str, int], f["coverage_summary"])
            assert summary["captured"] == 8
            assert summary["missing"] == 0
            assert summary["failed"] == 0
            assert summary["empty_confirmed"] == 0

    def test_mixed_coverage(self):
        """FIXTURES captured, INJURIES empty_confirmed, XG missing for fx-2, WEATHER attempted_failed."""
        fixtures = _fixtures_df(
            [
                {
                    "fixture_id": "fx-1",
                    "af_league_id": _AF_ID,
                    "kickoff_utc": "2024-09-15T14:00:00Z",
                    "home_team_name": "A",
                    "away_team_name": "B",
                    "status": "FT",
                    "venue_id": "v-1",
                },
                {
                    "fixture_id": "fx-2",
                    "af_league_id": _AF_ID,
                    "kickoff_utc": "2024-09-15T16:30:00Z",
                    "home_team_name": "C",
                    "away_team_name": "D",
                    "status": "FT",
                    "venue_id": "v-2",
                },
            ]
        )
        mapping: dict[str, pd.DataFrame | type[OSError]] = {
            "entity=fixtures": fixtures,
            "entity=fixture_stats": _entity_df(["fx-1", "fx-2"]),
            "entity=fixture_lineups": _entity_df(["fx-1", "fx-2"]),
            "entity=fixture_events": _entity_df(["fx-1", "fx-2"]),
            "entity=player_stats": _entity_df(["fx-1", "fx-2"]),
            # INJURIES: empty_confirmed (no fixture_id column)
            "entity=injuries": pd.DataFrame({"other_col": []}),
            # XG: only fx-1 captured → fx-2 is "missing"
            "entity=understat_xg": _entity_df(["fx-1"]),
            # WEATHER parquet absent → attempted_failed for whole entity
            "entity=weather": OSError,
        }

        with (
            patch.object(
                drilldown, "_read_parquet_columns", side_effect=_dispatch_uri_reader(mapping)
            ),
            patch.object(
                drilldown, "_parquet_schema_names", side_effect=_dispatch_schema_prober(mapping)
            ),
        ):
            result = drilldown.build_fixture_breakdown(day=_DAY, league_id=_LEAGUE)

        assert result["status"] == "resolved"
        fx = cast(list[dict[str, object]], result["fixtures"])
        by_id = {str(f["fixture_id"]): f for f in fx}

        cov_1 = cast(dict[str, str], by_id["fx-1"]["coverage"])
        assert cov_1["FIXTURES"] == "captured"
        assert cov_1["INJURIES"] == "empty_confirmed"
        assert cov_1["XG"] == "captured"
        assert cov_1["WEATHER"] == "attempted_failed"

        cov_2 = cast(dict[str, str], by_id["fx-2"]["coverage"])
        assert cov_2["XG"] == "missing"  # entity captured globally but not this fixture
        assert cov_2["WEATHER"] == "attempted_failed"
        assert cov_2["INJURIES"] == "empty_confirmed"

        summary_2 = cast(dict[str, int], by_id["fx-2"]["coverage_summary"])
        assert summary_2["captured"] == 5
        assert summary_2["missing"] == 1
        assert summary_2["empty_confirmed"] == 1
        assert summary_2["failed"] == 1

    def test_no_schedule_day_returns_empty_expected(self):
        """Phase-3: master fixtures parquet absent → status='no_schedule'."""
        with (
            patch.object(
                drilldown,
                "_read_parquet_columns",
                side_effect=FileNotFoundError("missing fixtures parquet"),
            ),
            patch.object(
                drilldown,
                "_parquet_schema_names",
                side_effect=FileNotFoundError("missing fixtures parquet"),
            ),
        ):
            result = drilldown.build_fixture_breakdown(day=_DAY, league_id=_LEAGUE)
        assert result["status"] == "no_schedule"
        assert result["fixtures_expected"] == 0
        assert result["fixtures"] == []

    def test_unknown_league_raises_value_error(self):
        with pytest.raises(ValueError, match="Unknown league_id"):
            drilldown.build_fixture_breakdown(day=_DAY, league_id="DEFINITELY_NOT_A_LEAGUE")

    def test_other_league_fixtures_filtered_out(self):
        """Fixtures for other af_league_ids must not leak into the response."""
        fixtures = _fixtures_df(
            [
                {
                    "fixture_id": "fx-epl",
                    "af_league_id": _AF_ID,
                    "kickoff_utc": "2024-09-15T14:00:00Z",
                    "home_team_name": "A",
                    "away_team_name": "B",
                    "status": "FT",
                    "venue_id": "v-1",
                },
                {
                    "fixture_id": "fx-other",
                    "af_league_id": 140,  # LaLiga
                    "kickoff_utc": "2024-09-15T14:00:00Z",
                    "home_team_name": "C",
                    "away_team_name": "D",
                    "status": "FT",
                    "venue_id": "v-2",
                },
            ]
        )
        mapping: dict[str, pd.DataFrame | type[OSError]] = {
            "entity=fixtures": fixtures,
            "entity=fixture_stats": _entity_df(["fx-epl", "fx-other"]),
            "entity=fixture_lineups": _entity_df(["fx-epl"]),
            "entity=fixture_events": _entity_df(["fx-epl"]),
            "entity=player_stats": _entity_df(["fx-epl"]),
            "entity=injuries": _entity_df(["fx-epl"]),
            "entity=understat_xg": _entity_df(["fx-epl"]),
            "entity=weather": _entity_df(["fx-epl"]),
        }
        with (
            patch.object(
                drilldown, "_read_parquet_columns", side_effect=_dispatch_uri_reader(mapping)
            ),
            patch.object(
                drilldown, "_parquet_schema_names", side_effect=_dispatch_schema_prober(mapping)
            ),
        ):
            result = drilldown.build_fixture_breakdown(day=_DAY, league_id=_LEAGUE)

        fx = cast(list[dict[str, object]], result["fixtures"])
        assert [str(f["fixture_id"]) for f in fx] == ["fx-epl"]


# ---------------------------------------------------------------------------
# build_fixture_download
# ---------------------------------------------------------------------------


class TestBuildFixtureDownload:
    def test_csv_union_shape(self):
        mapping: dict[str, pd.DataFrame | type[OSError]] = {
            "entity=fixtures": pd.DataFrame(
                {"fixture_id": ["fx-1"], "kickoff_utc": ["2024-09-15T14:00:00Z"]}
            ),
            "entity=fixture_stats": pd.DataFrame(
                {"fixture_id": ["fx-1", "fx-1"], "team": ["home", "away"], "shots": [10, 7]}
            ),
            "entity=fixture_lineups": pd.DataFrame({"fixture_id": []}),  # empty_confirmed
            "entity=fixture_events": OSError,  # attempted_failed
            "entity=player_stats": pd.DataFrame({"fixture_id": ["fx-other"]}),  # missing
            "entity=injuries": pd.DataFrame({"fixture_id": ["fx-1"], "player_id": ["p-1"]}),
            "entity=understat_xg": pd.DataFrame({"fixture_id": ["fx-1"], "xg": [1.2]}),
            "entity=weather": pd.DataFrame({"fixture_id": ["fx-1"], "temp_c": [15.0]}),
        }
        with (
            patch.object(
                drilldown, "_read_parquet_columns", side_effect=_dispatch_uri_reader(mapping)
            ),
            patch.object(
                drilldown, "_parquet_schema_names", side_effect=_dispatch_schema_prober(mapping)
            ),
        ):
            body, row_count, filename, media_type = drilldown.build_fixture_download(
                fixture_id="fx-1", day=_DAY, fmt="csv"
            )
        assert media_type == "text/csv; charset=utf-8"
        assert filename == "instruments-service_FIXTURE_fx-1.csv"
        # Header + union columns + captured rows (1 FIXTURES + 2 FIXTURE_STATS + 1 INJURIES + 1 XG + 1 WEATHER = 6)
        assert row_count == 6
        lines = body.strip().splitlines()
        assert lines[0].startswith("entity,")
        # Each captured entity contributes its own rows
        body_entities = {line.split(",", 1)[0] for line in lines[1:]}
        assert body_entities == {"FIXTURES", "FIXTURE_STATS", "INJURIES", "XG", "WEATHER"}

    def test_json_shape_with_coverage_sentinels(self):
        mapping: dict[str, pd.DataFrame | type[OSError]] = {
            "entity=fixtures": pd.DataFrame(
                {"fixture_id": ["fx-1"], "kickoff_utc": ["2024-09-15T14:00:00Z"]}
            ),
            "entity=fixture_stats": pd.DataFrame({"fixture_id": []}),  # empty_confirmed
            "entity=fixture_lineups": OSError,  # attempted_failed
            "entity=fixture_events": OSError,
            "entity=player_stats": pd.DataFrame({"fixture_id": ["fx-other"]}),  # missing
            "entity=injuries": OSError,
            "entity=understat_xg": pd.DataFrame({"fixture_id": ["fx-1"], "xg": [0.5]}),
            "entity=weather": OSError,
        }
        with (
            patch.object(
                drilldown, "_read_parquet_columns", side_effect=_dispatch_uri_reader(mapping)
            ),
            patch.object(
                drilldown, "_parquet_schema_names", side_effect=_dispatch_schema_prober(mapping)
            ),
        ):
            body, _row_count, filename, media_type = drilldown.build_fixture_download(
                fixture_id="fx-1", day=_DAY, fmt="json"
            )
        assert media_type == "application/json"
        assert filename == "instruments-service_FIXTURE_fx-1.json"
        payload = json.loads(body)
        assert payload["fixture_id"] == "fx-1"
        assert payload["day"] == _DAY
        cov = payload["coverage"]
        assert cov["FIXTURES"] == "captured"
        assert cov["FIXTURE_STATS"] == "empty_confirmed"
        assert cov["FIXTURE_LINEUPS"] == "attempted_failed"
        assert cov["PLAYER_STATS"] == "missing"
        assert cov["XG"] == "captured"
        # Captured entities have rows; others have only the sentinel
        assert "rows" in payload["entities"]["FIXTURES"]
        assert "rows" in payload["entities"]["XG"]
        assert "rows" not in payload["entities"]["FIXTURE_LINEUPS"]
        assert "rows" not in payload["entities"]["PLAYER_STATS"]

    def test_unknown_fixture_raises_file_not_found(self):
        """No entity contains the requested fixture_id → 404."""
        mapping: dict[str, pd.DataFrame | type[OSError]] = {
            "entity=fixtures": pd.DataFrame({"fixture_id": ["fx-other"]}),
            "entity=fixture_stats": pd.DataFrame({"fixture_id": ["fx-other"]}),
            "entity=fixture_lineups": OSError,
            "entity=fixture_events": OSError,
            "entity=player_stats": OSError,
            "entity=injuries": OSError,
            "entity=understat_xg": OSError,
            "entity=weather": OSError,
        }
        with (
            patch.object(
                drilldown, "_read_parquet_columns", side_effect=_dispatch_uri_reader(mapping)
            ),
            patch.object(
                drilldown, "_parquet_schema_names", side_effect=_dispatch_schema_prober(mapping)
            ),
        ):
            with pytest.raises(FileNotFoundError):
                drilldown.build_fixture_download(fixture_id="fx-1", day=_DAY, fmt="csv")

    def test_invalid_format_raises_value_error(self):
        with pytest.raises(ValueError, match="Unsupported format"):
            drilldown.build_fixture_download(fixture_id="fx-1", day=_DAY, fmt="xml")

    def test_day_required(self):
        with pytest.raises(ValueError, match="fixture_id alone cannot resolve a day"):
            drilldown.build_fixture_download(fixture_id="fx-1", day="", fmt="csv")

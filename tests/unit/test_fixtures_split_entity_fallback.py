"""Regression tests for the sports FIXTURES entity-folder split fallback.

instruments-service cut the FIXTURES writer over to a two-entity
``fixtures_schedule``/``fixtures_outcomes`` split with NO legacy dual-write
(first observed 2026-07-14, no feature-flag gate) — every direct
``entity=fixtures`` reader in this repo must also probe the split entities.
See ``plans/active/issues/features_sports_fixtures_split_reader_gap_2026_07_15.md``.

Covers the two deployment-api ``data_status_drilldown`` readers:
``build_fixtures_csv_export`` (CSV export) and ``build_fixture_breakdown`` /
``_load_fixture_meta`` (per-fixture breakdown pool). ``upcoming_fixtures.py``'s
split fallback is covered separately in ``test_upcoming_fixtures.py``.
"""

from __future__ import annotations

from unittest.mock import patch

import pandas as pd
import pytest

import deployment_api.services.data_status_drilldown as drilldown

_DAY = "2026-07-14"
_LEAGUE = "EPL"
_AF_ID = 39  # EPL numeric in API-Football


@pytest.fixture(autouse=True)
def _clear_cache():
    drilldown.clear_drilldown_cache()
    yield
    drilldown.clear_drilldown_cache()


class TestBuildFixturesCsvExportSplitFallback:
    def test_falls_back_to_split_schedule_and_outcomes_join(self):
        schedule_df = pd.DataFrame(
            [
                {"af_fixture_id": "1", "af_league_id": _AF_ID, "timestamp": "2026-07-14T14:00:00Z"},
                {"af_fixture_id": "2", "af_league_id": _AF_ID, "timestamp": "2026-07-14T16:30:00Z"},
            ]
        )
        outcomes_df = pd.DataFrame([{"af_fixture_id": "1", "home_score_regulation": 2, "away_score_regulation": 1}])

        def fake_blob_paths(_bucket: str, _day: str, entity_name: str) -> list[str]:
            return [f"sports_reference/.../entity={entity_name}/league=EPL/{entity_name}.parquet"]

        def fake_read(gs_uri: str, columns: list[str] | None = None) -> pd.DataFrame:
            if "fixtures_schedule" in gs_uri:
                return schedule_df.copy()
            if "fixtures_outcomes" in gs_uri:
                return outcomes_df.copy()
            raise FileNotFoundError(gs_uri)

        with (
            patch.object(drilldown, "_read_parquet_columns", side_effect=fake_read),
            patch.object(drilldown, "split_entity_league_blob_paths", side_effect=fake_blob_paths),
        ):
            csv_text, row_count, filename = drilldown.build_fixtures_csv_export(day=_DAY, league_id=_LEAGUE)

        assert row_count == 2
        assert filename == f"instruments-service_FIXTURES_{_LEAGUE}_{_DAY}.csv"
        assert "home_score_regulation" in csv_text
        lines = csv_text.strip().splitlines()
        assert len(lines) == 3  # header + 2 rows

    def test_genuine_gap_returns_empty_csv(self):
        with (
            patch.object(drilldown, "_read_parquet_columns", side_effect=FileNotFoundError("missing")),
            patch.object(drilldown, "split_entity_league_blob_paths", return_value=[]),
        ):
            csv_text, row_count, _filename = drilldown.build_fixtures_csv_export(day=_DAY, league_id=_LEAGUE)

        assert csv_text == ""
        assert row_count == 0


class TestLoadFixtureMetaSplitFallback:
    def test_build_fixture_breakdown_falls_back_to_split_schedule(self):
        schedule_df = pd.DataFrame(
            [
                {
                    "af_fixture_id": "fx-1",
                    "af_league_id": _AF_ID,
                    "timestamp": "2026-07-14T14:00:00Z",
                    "af_home_name": "Arsenal",
                    "af_away_name": "Liverpool",
                    "status_short": "NS",
                    "venue_id": "v-1",
                },
            ]
        )
        all_ids = ["fx-1"]

        def fake_schema_prober(gs_uri: str) -> set[str]:
            if "entity=fixtures/" in gs_uri:
                raise FileNotFoundError(gs_uri)
            return {"fixture_id"}

        def fake_reader(gs_uri: str, columns: list[str] | None = None) -> pd.DataFrame:
            if "entity=fixtures/" in gs_uri:
                raise FileNotFoundError(gs_uri)
            return pd.DataFrame({"fixture_id": all_ids})

        with (
            patch.object(drilldown, "_parquet_schema_names", side_effect=fake_schema_prober),
            patch.object(drilldown, "_read_parquet_columns", side_effect=fake_reader),
            patch.object(
                drilldown,
                "split_entity_league_blob_paths",
                return_value=["sports_reference/.../fixtures_schedule.parquet"],
            ),
        ):
            with patch(
                "deployment_api.services.data_status_drilldown._fixtures_pools._read_split_fixture_meta_frame",
                return_value=schedule_df,
            ):
                result = drilldown.build_fixture_breakdown(day=_DAY, league_id=_LEAGUE)

        assert result["status"] == "resolved"
        assert result["fixtures_expected"] == 1
        fx = result["fixtures"]
        assert fx[0]["fixture_id"] == "fx-1"
        assert fx[0]["home_team_name"] == "Arsenal"
        assert fx[0]["away_team_name"] == "Liverpool"

    def test_no_schedule_when_split_fallback_also_empty(self):
        with (
            patch.object(drilldown, "_parquet_schema_names", side_effect=FileNotFoundError("missing")),
            patch.object(drilldown, "split_entity_league_blob_paths", return_value=[]),
        ):
            result = drilldown.build_fixture_breakdown(day=_DAY, league_id=_LEAGUE)

        assert result["status"] == "no_schedule"
        assert result["fixtures_expected"] == 0

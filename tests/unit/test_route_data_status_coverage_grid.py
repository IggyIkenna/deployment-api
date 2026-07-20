"""Tests for routes/data_status/_coverage_grid.py — cherry-pick E (2026-07-20).

``GET /coverage-grid`` returns a flat (primary-axis x date) capture_status
matrix, heatmap-shaped: ``{service, asset_group, primary_axis, dates: [...],
rows: [{primary, cells: [{date, captured, empty_confirmed, attempted_failed,
expected_unattempted}, ...]}, ...]}``.

Mirrors ``test_route_data_status_axis_census.py``'s TestClient pattern + patch
surface (``build_bucket_name`` / ``_read_manifest_index`` are both resolved
through the shared ``deployment_api.routes.data_status`` package at call
time, so both are patched at that package path).
"""

from __future__ import annotations

from unittest.mock import MagicMock, patch

import pandas as pd
import pytest
from fastapi import FastAPI
from fastapi.testclient import TestClient

from deployment_api.routes.data_status._coverage_grid import (
    build_coverage_grid,
    clear_coverage_grid_cache,
)

_PATCH_DISABLE_AUTH = "deployment_api.rbac.DISABLE_AUTH"
_PATCH_CFG = "deployment_api.routes.data_status._cfg"
_PATCH_BUILD_BUCKET = "deployment_api.routes.data_status.build_bucket_name"
_PATCH_READ_MANIFEST_INDEX = "deployment_api.routes.data_status._read_manifest_index"


def _make_mock_cfg() -> MagicMock:
    cfg = MagicMock()
    cfg.is_mock_mode.return_value = False
    cfg.deployment_env = "dev"
    return cfg


@pytest.fixture(autouse=True)
def _clear_grid_cache():
    clear_coverage_grid_cache()
    yield
    clear_coverage_grid_cache()


@pytest.fixture
def client_ds_coverage_grid() -> TestClient:
    from deployment_api.routes.data_status import router

    app = FastAPI()
    app.include_router(router, prefix="/data-status")
    with (
        patch(_PATCH_DISABLE_AUTH, True),
        patch(_PATCH_CFG, _make_mock_cfg()),
    ):
        yield TestClient(app, raise_server_exceptions=False)  # type: ignore[misc]


def _sample_manifest_df() -> pd.DataFrame:
    """Two venues x two dates, mixing all 4 capture_status states, plus one
    (venue, date) combo entirely absent from venue B on the first date
    (proves the dense zero-fill)."""
    rows: list[dict[str, object]] = [
        {"venue": "BINANCE-FUTURES", "date": "2024-03-01", "capture_status": "captured"},
        {"venue": "BINANCE-FUTURES", "date": "2024-03-01", "capture_status": "captured"},
        {"venue": "BINANCE-FUTURES", "date": "2024-03-02", "capture_status": "empty_confirmed"},
        {"venue": "OKX-FUTURES", "date": "2024-03-02", "capture_status": "attempted_failed"},
        {"venue": "OKX-FUTURES", "date": "2024-03-02", "capture_status": "expected_unattempted"},
        # Venue B has NO rows at all on 2024-03-01 -> that cell must zero-fill.
    ]
    return pd.DataFrame(rows)


# ---------------------------------------------------------------------------
# Pure matrix-builder tests (synthetic df — no cloud, no route).
# ---------------------------------------------------------------------------


class TestBuildCoverageGridPure:
    def test_empty_df_returns_empty_grid(self) -> None:
        grid = build_coverage_grid(pd.DataFrame(), "venue")
        assert grid == {"dates": [], "rows": []}

    def test_missing_primary_axis_column_returns_empty_grid(self) -> None:
        df = pd.DataFrame({"date": ["2024-03-01"], "capture_status": ["captured"]})
        grid = build_coverage_grid(df, "venue")
        assert grid == {"dates": [], "rows": []}

    def test_missing_date_column_returns_empty_grid(self) -> None:
        df = pd.DataFrame({"venue": ["BINANCE"], "capture_status": ["captured"]})
        grid = build_coverage_grid(df, "venue")
        assert grid == {"dates": [], "rows": []}

    def test_dense_grid_shape_and_dates(self) -> None:
        grid = build_coverage_grid(_sample_manifest_df(), "venue")
        assert grid["dates"] == ["2024-03-01", "2024-03-02"]
        rows = grid["rows"]
        assert isinstance(rows, list)
        assert len(rows) == 2
        by_primary = {r["primary"]: r for r in rows}  # type: ignore[union-attr]
        assert set(by_primary) == {"BINANCE-FUTURES", "OKX-FUTURES"}

    def test_every_cell_carries_all_four_keys_even_when_absent(self) -> None:
        """OKX-FUTURES has no rows at all on 2024-03-01 — that cell must
        still be present, zero-filled, with all 4 capture_status keys."""
        grid = build_coverage_grid(_sample_manifest_df(), "venue")
        rows = grid["rows"]
        assert isinstance(rows, list)
        okx = next(r for r in rows if r["primary"] == "OKX-FUTURES")  # type: ignore[index]
        cells = {c["date"]: c for c in okx["cells"]}  # type: ignore[index]
        missing_cell = cells["2024-03-01"]
        assert missing_cell == {
            "date": "2024-03-01",
            "captured": 0,
            "empty_confirmed": 0,
            "attempted_failed": 0,
            "expected_unattempted": 0,
        }

    def test_present_cell_counts_match_manifest_rows(self) -> None:
        grid = build_coverage_grid(_sample_manifest_df(), "venue")
        rows = grid["rows"]
        assert isinstance(rows, list)
        binance = next(r for r in rows if r["primary"] == "BINANCE-FUTURES")  # type: ignore[index]
        cells = {c["date"]: c for c in binance["cells"]}  # type: ignore[index]
        assert cells["2024-03-01"]["captured"] == 2
        assert cells["2024-03-02"]["empty_confirmed"] == 1

        okx = next(r for r in rows if r["primary"] == "OKX-FUTURES")  # type: ignore[index]
        okx_cells = {c["date"]: c for c in okx["cells"]}  # type: ignore[index]
        assert okx_cells["2024-03-02"]["attempted_failed"] == 1
        assert okx_cells["2024-03-02"]["expected_unattempted"] == 1

    def test_legacy_rows_without_capture_status_column_coerce_to_captured(self) -> None:
        df = pd.DataFrame(
            {
                "venue": ["BINANCE", "BINANCE"],
                "date": ["2024-03-01", "2024-03-01"],
            }
        )
        grid = build_coverage_grid(df, "venue")
        rows = grid["rows"]
        assert isinstance(rows, list)
        cell = rows[0]["cells"][0]  # type: ignore[index]
        assert cell["captured"] == 2

    def test_unrecognised_capture_status_value_coerces_to_captured(self) -> None:
        """Regression parity with data_status_hierarchical's legacy-coercion
        fix (2026-06-18 sports prd incident): a literal ``"None"``/blank/
        unrecognised capture_status must NOT be silently dropped from the
        grid — it counts as captured, never disappears from the denominator."""
        df = pd.DataFrame(
            {
                "venue": ["BINANCE", "BINANCE", "BINANCE"],
                "date": ["2024-03-01", "2024-03-01", "2024-03-01"],
                "capture_status": ["captured", "None", "nan"],
            }
        )
        grid = build_coverage_grid(df, "venue")
        rows = grid["rows"]
        assert isinstance(rows, list)
        cell = rows[0]["cells"][0]  # type: ignore[index]
        assert cell["captured"] == 3

    def test_blank_and_nan_primary_values_excluded(self) -> None:
        df = pd.DataFrame(
            {
                "venue": ["BINANCE", "", "nan", "NaN"],
                "date": ["2024-03-01"] * 4,
                "capture_status": ["captured"] * 4,
            }
        )
        grid = build_coverage_grid(df, "venue")
        rows = grid["rows"]
        assert isinstance(rows, list)
        assert len(rows) == 1
        assert rows[0]["primary"] == "BINANCE"  # type: ignore[index]

    def test_dates_are_sorted(self) -> None:
        df = pd.DataFrame(
            {
                "venue": ["BINANCE"] * 3,
                "date": ["2024-03-03", "2024-03-01", "2024-03-02"],
                "capture_status": ["captured"] * 3,
            }
        )
        grid = build_coverage_grid(df, "venue")
        assert grid["dates"] == ["2024-03-01", "2024-03-02", "2024-03-03"]

    def test_rows_sorted_by_primary_value(self) -> None:
        df = pd.DataFrame(
            {
                "venue": ["OKX", "BINANCE", "AAVE"],
                "date": ["2024-03-01"] * 3,
                "capture_status": ["captured"] * 3,
            }
        )
        grid = build_coverage_grid(df, "venue")
        rows = grid["rows"]
        assert isinstance(rows, list)
        assert [r["primary"] for r in rows] == ["AAVE", "BINANCE", "OKX"]  # type: ignore[misc]


# ---------------------------------------------------------------------------
# Route-level tests (TestClient — mirrors _axis_census's fixture pattern).
# ---------------------------------------------------------------------------


class TestGetCoverageGridRoute:
    def test_returns_grid_shape_with_real_data(self, client_ds_coverage_grid: TestClient) -> None:
        with (
            patch(_PATCH_BUILD_BUCKET, return_value="market-data-tick-cefi-prd-fake"),
            patch(_PATCH_READ_MANIFEST_INDEX, return_value=_sample_manifest_df()),
        ):
            r = client_ds_coverage_grid.get(
                "/data-status/coverage-grid",
                params={
                    "service": "market-tick-data-service",
                    "asset_group": "cefi",
                    "start_date": "2024-03-01",
                    "end_date": "2024-03-02",
                },
            )
        assert r.status_code == 200
        body = r.json()
        assert set(body.keys()) == {"service", "asset_group", "primary_axis", "dates", "rows"}
        assert body["service"] == "market-tick-data-service"
        assert body["asset_group"] == "cefi"
        # MTDS CeFi SHARD_AXIS_MATRIX head axis is "venue".
        assert body["primary_axis"] == "venue"
        assert body["dates"] == ["2024-03-01", "2024-03-02"]
        assert len(body["rows"]) == 2
        for row in body["rows"]:
            assert set(row.keys()) == {"primary", "cells"}
            for cell in row["cells"]:
                assert set(cell.keys()) == {
                    "date",
                    "captured",
                    "empty_confirmed",
                    "attempted_failed",
                    "expected_unattempted",
                }

    def test_undeclared_pair_falls_back_to_venue_primary_axis(self, client_ds_coverage_grid: TestClient) -> None:
        with (
            patch(_PATCH_BUILD_BUCKET, return_value="market-data-tick-cefi-prd-fake"),
            patch(_PATCH_READ_MANIFEST_INDEX, return_value=pd.DataFrame()),
        ):
            r = client_ds_coverage_grid.get(
                "/data-status/coverage-grid",
                params={
                    "service": "totally-unregistered-service",
                    "asset_group": "cefi",
                    "start_date": "2024-03-01",
                    "end_date": "2024-03-02",
                },
            )
        assert r.status_code == 200
        assert r.json()["primary_axis"] == "venue"

    def test_empty_manifest_returns_empty_grid(self, client_ds_coverage_grid: TestClient) -> None:
        with (
            patch(_PATCH_BUILD_BUCKET, return_value="market-data-tick-cefi-prd-fake"),
            patch(_PATCH_READ_MANIFEST_INDEX, return_value=pd.DataFrame()),
        ):
            r = client_ds_coverage_grid.get(
                "/data-status/coverage-grid",
                params={
                    "service": "market-tick-data-service",
                    "asset_group": "cefi",
                    "start_date": "2024-03-01",
                    "end_date": "2024-03-02",
                },
            )
        assert r.status_code == 200
        body = r.json()
        assert body["dates"] == []
        assert body["rows"] == []

    def test_producer_less_pair_returns_empty_grid_not_500(self, client_ds_coverage_grid: TestClient) -> None:
        """A (service, asset_group) with no registered bucket (e.g. a
        producer-less asset_group retired from cloud-providers.yaml) renders
        an honest empty grid — mirrors the hierarchical drilldown's
        ``BucketNamingError`` -> empty-tree contract, never a 500."""
        from unified_trading_library import BucketNamingError

        with patch(_PATCH_BUILD_BUCKET, side_effect=BucketNamingError("no bucket registered")):
            r = client_ds_coverage_grid.get(
                "/data-status/coverage-grid",
                params={
                    "service": "market-tick-data-service",
                    "asset_group": "cefi",
                    "start_date": "2024-03-01",
                    "end_date": "2024-03-02",
                },
            )
        assert r.status_code == 200
        body = r.json()
        assert body["dates"] == []
        assert body["rows"] == []

    def test_manifest_read_failure_returns_500(self, client_ds_coverage_grid: TestClient) -> None:
        with (
            patch(_PATCH_BUILD_BUCKET, return_value="market-data-tick-cefi-prd-fake"),
            patch(_PATCH_READ_MANIFEST_INDEX, side_effect=OSError("gcs unavailable")),
        ):
            r = client_ds_coverage_grid.get(
                "/data-status/coverage-grid",
                params={
                    "service": "market-tick-data-service",
                    "asset_group": "cefi",
                    "start_date": "2024-03-01",
                    "end_date": "2024-03-02",
                },
            )
        assert r.status_code == 500

    def test_reader_called_with_date_window_pushdown(self, client_ds_coverage_grid: TestClient) -> None:
        """Reuses prod's reader with the SAME ``date_window`` pushdown
        contract ``data_status_hierarchical`` uses — never a fresh
        whole-corpus read."""
        with (
            patch(_PATCH_BUILD_BUCKET, return_value="market-data-tick-cefi-prd-fake"),
            patch(_PATCH_READ_MANIFEST_INDEX, return_value=_sample_manifest_df()) as mock_read,
        ):
            r = client_ds_coverage_grid.get(
                "/data-status/coverage-grid",
                params={
                    "service": "market-tick-data-service",
                    "asset_group": "cefi",
                    "start_date": "2024-03-01",
                    "end_date": "2024-03-02",
                },
            )
        assert r.status_code == 200
        mock_read.assert_called_once()
        _, kwargs = mock_read.call_args
        assert kwargs["date_window"] == ("2024-03-01", "2024-03-02")

    def test_ttl_cache_skips_second_read_for_same_window(self, client_ds_coverage_grid: TestClient) -> None:
        """The in-process TTL cache means an identical (bucket, window)
        request within the TTL does not re-read the manifest."""
        params = {
            "service": "market-tick-data-service",
            "asset_group": "cefi",
            "start_date": "2024-03-01",
            "end_date": "2024-03-02",
        }
        with (
            patch(_PATCH_BUILD_BUCKET, return_value="market-data-tick-cefi-prd-fake"),
            patch(_PATCH_READ_MANIFEST_INDEX, return_value=_sample_manifest_df()) as mock_read,
        ):
            r1 = client_ds_coverage_grid.get("/data-status/coverage-grid", params=params)
            r2 = client_ds_coverage_grid.get("/data-status/coverage-grid", params=params)
        assert r1.status_code == 200
        assert r2.status_code == 200
        mock_read.assert_called_once()

    def test_unknown_service_returns_500(self, client_ds_coverage_grid: TestClient) -> None:
        with patch(_PATCH_BUILD_BUCKET, side_effect=ValueError("Unknown service: nope")):
            r = client_ds_coverage_grid.get(
                "/data-status/coverage-grid",
                params={
                    "service": "nope",
                    "asset_group": "cefi",
                    "start_date": "2024-03-01",
                    "end_date": "2024-03-02",
                },
            )
        assert r.status_code == 500

"""Unit tests for data_status.py route handlers — mock mode paths.

Six endpoints have `if _cfg.is_mock_mode(): return ...` branches that need
route-level coverage. This file exercises those branches with is_mock_mode()
patched to True.
"""

from __future__ import annotations

from unittest.mock import MagicMock, patch

import pytest
from fastapi import FastAPI
from fastapi.testclient import TestClient

_PATCH_DISABLE_AUTH = "deployment_api.rbac.DISABLE_AUTH"
_PATCH_CFG = "deployment_api.routes.data_status._cfg"


@pytest.fixture
def client_ds_mock() -> TestClient:
    from deployment_api.routes.data_status import router

    app = FastAPI()
    app.include_router(router, prefix="/data-status")
    mock_cfg = MagicMock()
    mock_cfg.is_mock_mode.return_value = True
    with (
        patch(_PATCH_DISABLE_AUTH, True),
        patch(_PATCH_CFG, mock_cfg),
    ):
        yield TestClient(app, raise_server_exceptions=False)  # type: ignore[misc]


# ── GET / (get_data_status) ───────────────────────────────────────────────────


class TestGetDataStatusMock:
    def test_returns_mock_flag(self, client_ds_mock: TestClient) -> None:
        r = client_ds_mock.get(
            "/data-status",
            params={"service": "strategy-service", "start_date": "2026-01-01", "end_date": "2026-05-01"},
        )
        assert r.status_code == 200
        data = r.json()
        assert data["mock"] is True
        assert data["service"] == "strategy-service"
        assert data["status"] == "ok"

    def test_echoes_date_range(self, client_ds_mock: TestClient) -> None:
        r = client_ds_mock.get(
            "/data-status",
            params={"service": "execution-service", "start_date": "2026-03-01", "end_date": "2026-04-01"},
        )
        assert r.status_code == 200
        data = r.json()
        assert data["start_date"] == "2026-03-01"
        assert data["end_date"] == "2026-04-01"
        assert "sources" in data


# ── GET /manifest ─────────────────────────────────────────────────────────────


class TestGetDataStatusManifestMock:
    def test_returns_mock_turbo_response(self, client_ds_mock: TestClient) -> None:
        r = client_ds_mock.get(
            "/data-status/manifest",
            params={"service": "strategy-service", "start_date": "2026-01-01", "end_date": "2026-05-01"},
        )
        assert r.status_code == 200
        data = r.json()
        assert "mock" in data or "service" in data  # build_mock_turbo_response includes these

    def test_with_secondary_axis_echoed(self, client_ds_mock: TestClient) -> None:
        r = client_ds_mock.get(
            "/data-status/manifest",
            params={
                "service": "strategy-service",
                "start_date": "2026-01-01",
                "end_date": "2026-05-01",
                "secondary_axis": "venue",
            },
        )
        assert r.status_code == 200
        data = r.json()
        assert data.get("secondary_axis") == "venue"


# ── GET /coverage-summary ─────────────────────────────────────────────────────


class TestGetCoverageSummaryMock:
    def test_returns_mock_flag(self, client_ds_mock: TestClient) -> None:
        r = client_ds_mock.get(
            "/data-status/coverage-summary",
            params={"service": "instruments-service"},
        )
        assert r.status_code == 200
        data = r.json()
        assert data["mock"] is True
        assert "totals" in data
        assert data["service"] == "instruments-service"

    def test_returns_rich_nonzero_inventory(self, client_ds_mock: TestClient) -> None:
        """Cherry-pick D (2026-07-20): this handler wins the duplicate
        ``/coverage-summary`` registration (FastAPI first-registration-wins —
        see ``routes/data_status/__init__.py``), so its mock branch must
        carry the rich per-asset_group inventory ported from the unreachable
        ``_deploy_turbo.get_data_coverage_summary`` mock, not all-zeros."""
        r = client_ds_mock.get(
            "/data-status/coverage-summary",
            params={"service": "instruments-service"},
        )
        assert r.status_code == 200
        data = r.json()
        # 25000 (CEFI) + 40000 (TRADFI) + 10000 (DEFI) + 3800 (SPORTS).
        assert data["totals"]["shards"] == 78800
        assert data["totals"]["instrument_rows"] == 78800
        asset_groups = data["asset_groups"]
        assert set(asset_groups.keys()) == {"CEFI", "TRADFI", "DEFI", "SPORTS"}
        cefi = asset_groups["CEFI"]
        assert cefi["total_shards"] == 25000
        assert cefi["capture_status_counts"]["captured"] > 0

    def test_asset_groups_param_narrows_the_inventory(self, client_ds_mock: TestClient) -> None:
        r = client_ds_mock.get(
            "/data-status/coverage-summary",
            params={"service": "instruments-service", "asset_groups": "CEFI"},
        )
        assert r.status_code == 200
        data = r.json()
        assert set(data["asset_groups"].keys()) == {"CEFI"}
        assert data["totals"]["shards"] == 25000


# ── GET /drilldown/{service}/{asset_group} ────────────────────────────────────


class TestGetDataStatusDrilldownMock:
    def test_returns_mock_drilldown_shape(self, client_ds_mock: TestClient) -> None:
        r = client_ds_mock.get(
            "/data-status/drilldown/strategy-service/defi",
            params={"start_date": "2026-01-01", "end_date": "2026-05-01"},
        )
        assert r.status_code == 200
        data = r.json()
        assert data["mock"] is True
        assert data["service"] == "strategy-service"
        assert data["asset_group"] == "defi"
        assert "tree" in data
        assert "totals" in data

    def test_tree_and_totals_are_nonzero_in_mock(self, client_ds_mock: TestClient) -> None:
        """Cherry-pick D (2026-07-20): the mock drilldown tree is synthesized
        from SHARD_AXIS_MATRIX instead of the pre-2026-07-20 hardcoded empty
        ``tree: []`` — the deployment-ui drilldown component now has
        something to render in local dev / Playwright."""
        r = client_ds_mock.get(
            "/data-status/drilldown/execution-service/cefi",
            params={"start_date": "2026-01-01", "end_date": "2026-02-01"},
        )
        assert r.status_code == 200
        data = r.json()
        assert data["tree"], "expected a non-empty mock tree"
        assert data["totals"]["total"] > 0
        assert data["totals"]["completion_pct"] > 0.0
        # Every node — mock or real — carries the full DrilldownNode schema
        # (proves the mock was built via the real dataclass, not a hand-rolled dict).
        first = data["tree"][0]
        assert "reason_summary" in first
        assert "reason_category" in first
        assert "is_leaf" in first


# ── GET /turbo ────────────────────────────────────────────────────────────────


class TestGetDataStatusTurboMock:
    def test_returns_build_mock_turbo_response(self, client_ds_mock: TestClient) -> None:
        r = client_ds_mock.get(
            "/data-status/turbo",
            params={"service": "strategy-service", "start_date": "2026-01-01", "end_date": "2026-05-01"},
        )
        assert r.status_code == 200
        data = r.json()
        # build_mock_turbo_response always includes "service" and "mock"
        assert "service" in data or "categories" in data  # either shape from the mock builder

    def test_venue_and_include_params_ignored(self, client_ds_mock: TestClient) -> None:
        r = client_ds_mock.get(
            "/data-status/turbo",
            params={
                "service": "instruments-service",
                "start_date": "2026-01-01",
                "end_date": "2026-05-01",
                "include_sub_dimensions": "true",
                "include_instrument_types": "true",
            },
        )
        assert r.status_code == 200  # mock ignores these params, no exception


# ── GET /instruments-for-shard ────────────────────────────────────────────────


class TestGetInstrumentsForShardMock:
    def test_returns_mock_shard_instruments(self, client_ds_mock: TestClient) -> None:
        r = client_ds_mock.get(
            "/data-status/instruments-for-shard",
            params={
                "service": "market-tick-data-service",
                "asset_group": "defi",
                "venue": "AAVE_V3-MAINNET",
                "day": "2026-05-01",
                "instrument_type": "lending",
                "data_type": "lending_indices",
            },
        )
        assert r.status_code == 200
        data = r.json()
        assert "instruments" in data
        assert "mock" in data

    def test_three_instruments_with_all_capture_statuses(self, client_ds_mock: TestClient) -> None:
        r = client_ds_mock.get(
            "/data-status/instruments-for-shard",
            params={
                "service": "instruments-service",
                "asset_group": "cefi",
                "venue": "BINANCE",
                "day": "2026-05-01",
                "instrument_type": "spot",
                "data_type": "tick",
            },
        )
        assert r.status_code == 200
        data = r.json()
        # build_mock_shard_instruments returns 3 instruments with all capture statuses
        assert len(data["instruments"]) == 3
        statuses = {inst["capture_status"] for inst in data["instruments"]}
        assert "captured" in statuses

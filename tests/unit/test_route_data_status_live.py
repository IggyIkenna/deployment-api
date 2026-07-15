"""Tests for routes/data_status.py — non-mock (live) code paths.

Each test class exercises the non-mock branch of one endpoint,
patching the relevant service layer so no real GCS / Firestore calls happen.
"""

from __future__ import annotations

from unittest.mock import AsyncMock, MagicMock, patch

import pytest
from fastapi import FastAPI
from fastapi.testclient import TestClient

_PATCH_DISABLE_AUTH = "deployment_api.rbac.DISABLE_AUTH"
_PATCH_CFG = "deployment_api.routes.data_status._cfg"
_PATCH_DSS = "deployment_api.routes.data_status.data_status_service"
_PATCH_DAS = "deployment_api.routes.data_status.data_analytics_service"
_PATCH_DQS = "deployment_api.routes.data_status.data_query_service"


def _make_mock_cfg(is_mock: bool = False) -> MagicMock:
    cfg = MagicMock()
    cfg.is_mock_mode.return_value = is_mock
    cfg.deployment_env = "dev"
    return cfg


@pytest.fixture
def client_ds_live() -> TestClient:
    from deployment_api.routes.data_status import router

    app = FastAPI()
    app.include_router(router, prefix="/data-status")
    mock_cfg = _make_mock_cfg(is_mock=False)
    with (
        patch(_PATCH_DISABLE_AUTH, True),
        patch(_PATCH_CFG, mock_cfg),
    ):
        yield TestClient(app, raise_server_exceptions=False)  # type: ignore[misc]


# ── GET / (get_data_status) ───────────────────────────────────────────────────


class TestGetDataStatusLive:
    def test_happy_path_returns_result(self, client_ds_live: TestClient) -> None:
        mock_dss = MagicMock()
        mock_dss.run_data_status_cli = AsyncMock(return_value={"status": "ok", "service": "strategy-service"})
        with patch(_PATCH_DSS, mock_dss):
            r = client_ds_live.get(
                "/data-status",
                params={"service": "strategy-service", "start_date": "2026-01-01", "end_date": "2026-01-31"},
            )
        assert r.status_code == 200
        assert r.json()["service"] == "strategy-service"

    def test_error_in_result_returns_500(self, client_ds_live: TestClient) -> None:
        mock_dss = MagicMock()
        mock_dss.run_data_status_cli = AsyncMock(return_value={"error": "GCS down"})
        with patch(_PATCH_DSS, mock_dss):
            r = client_ds_live.get(
                "/data-status",
                params={"service": "strategy-service", "start_date": "2026-01-01", "end_date": "2026-01-31"},
            )
        assert r.status_code == 500

    def test_runtime_error_returns_500(self, client_ds_live: TestClient) -> None:
        mock_dss = MagicMock()
        mock_dss.run_data_status_cli = AsyncMock(side_effect=RuntimeError("fail"))
        with patch(_PATCH_DSS, mock_dss):
            r = client_ds_live.get(
                "/data-status",
                params={"service": "strategy-service", "start_date": "2026-01-01", "end_date": "2026-01-31"},
            )
        assert r.status_code == 500


# ── POST /missing-shards ──────────────────────────────────────────────────────


class TestCalculateMissingShardsLive:
    def test_happy_path_returns_result(self, client_ds_live: TestClient) -> None:
        mock_dss = MagicMock()
        mock_dss.calculate_missing_shards = AsyncMock(return_value={"total_missing": 0})
        with patch(_PATCH_DSS, mock_dss):
            r = client_ds_live.post(
                "/data-status/missing-shards",
                params={"service": "strategy-service", "start_date": "2026-01-01", "end_date": "2026-01-31"},
            )
        assert r.status_code == 200
        assert r.json()["total_missing"] == 0

    def test_error_in_result_returns_500(self, client_ds_live: TestClient) -> None:
        mock_dss = MagicMock()
        mock_dss.calculate_missing_shards = AsyncMock(return_value={"error": "timeout"})
        with patch(_PATCH_DSS, mock_dss):
            r = client_ds_live.post(
                "/data-status/missing-shards",
                params={"service": "strategy-service", "start_date": "2026-01-01", "end_date": "2026-01-31"},
            )
        assert r.status_code == 500

    def test_value_error_returns_500(self, client_ds_live: TestClient) -> None:
        mock_dss = MagicMock()
        mock_dss.calculate_missing_shards = AsyncMock(side_effect=ValueError("bad"))
        with patch(_PATCH_DSS, mock_dss):
            r = client_ds_live.post(
                "/data-status/missing-shards",
                params={"service": "strategy-service", "start_date": "2026-01-01", "end_date": "2026-01-31"},
            )
        assert r.status_code == 500


# ── GET /last-updated ─────────────────────────────────────────────────────────


class TestGetLastUpdatedLive:
    def test_happy_path(self, client_ds_live: TestClient) -> None:
        mock_dss = MagicMock()
        mock_dss.get_last_updated_info = AsyncMock(return_value={"last_updated": "2026-01-15"})
        with patch(_PATCH_DSS, mock_dss):
            r = client_ds_live.get("/data-status/last-updated", params={"service": "strategy-service"})
        assert r.status_code == 200

    def test_error_result_returns_500(self, client_ds_live: TestClient) -> None:
        mock_dss = MagicMock()
        mock_dss.get_last_updated_info = AsyncMock(return_value={"error": "no data"})
        with patch(_PATCH_DSS, mock_dss):
            r = client_ds_live.get("/data-status/last-updated", params={"service": "strategy-service"})
        assert r.status_code == 500

    def test_os_error_returns_500(self, client_ds_live: TestClient) -> None:
        mock_dss = MagicMock()
        mock_dss.get_last_updated_info = AsyncMock(side_effect=OSError("io fail"))
        with patch(_PATCH_DSS, mock_dss):
            r = client_ds_live.get("/data-status/last-updated", params={"service": "strategy-service"})
        assert r.status_code == 500


# ── GET /manifest ─────────────────────────────────────────────────────────────


class TestGetDataStatusManifestLive:
    def test_happy_path(self, client_ds_live: TestClient) -> None:
        mock_dss = MagicMock()
        mock_dss.get_manifest_status = AsyncMock(return_value={"mode": "turbo", "service": "strategy-service"})
        with patch(_PATCH_DSS, mock_dss):
            r = client_ds_live.get(
                "/data-status/manifest",
                params={"service": "strategy-service", "start_date": "2026-01-01", "end_date": "2026-01-31"},
            )
        assert r.status_code == 200

    def test_error_in_result_returns_500(self, client_ds_live: TestClient) -> None:
        mock_dss = MagicMock()
        mock_dss.get_manifest_status = AsyncMock(return_value={"error": "missing index"})
        with patch(_PATCH_DSS, mock_dss):
            r = client_ds_live.get(
                "/data-status/manifest",
                params={"service": "strategy-service", "start_date": "2026-01-01", "end_date": "2026-01-31"},
            )
        assert r.status_code == 500

    def test_runtime_error_returns_500(self, client_ds_live: TestClient) -> None:
        mock_dss = MagicMock()
        mock_dss.get_manifest_status = AsyncMock(side_effect=RuntimeError("boom"))
        with patch(_PATCH_DSS, mock_dss):
            r = client_ds_live.get(
                "/data-status/manifest",
                params={"service": "strategy-service", "start_date": "2026-01-01", "end_date": "2026-01-31"},
            )
        assert r.status_code == 500


# ── GET /coverage-summary ─────────────────────────────────────────────────────


class TestGetCoverageSummaryLive:
    def test_happy_path(self, client_ds_live: TestClient) -> None:
        mock_dss = MagicMock()
        mock_dss.get_coverage_summary = AsyncMock(return_value={"totals": {"shards": 100}})
        with patch(_PATCH_DSS, mock_dss):
            r = client_ds_live.get("/data-status/coverage-summary", params={"service": "instruments-service"})
        assert r.status_code == 200

    def test_with_asset_groups_param(self, client_ds_live: TestClient) -> None:
        mock_dss = MagicMock()
        mock_dss.get_coverage_summary = AsyncMock(return_value={"totals": {"shards": 50}})
        with patch(_PATCH_DSS, mock_dss):
            r = client_ds_live.get(
                "/data-status/coverage-summary",
                params={"service": "instruments-service", "asset_groups": "CEFI,DEFI"},
            )
        assert r.status_code == 200

    def test_os_error_returns_500(self, client_ds_live: TestClient) -> None:
        mock_dss = MagicMock()
        mock_dss.get_coverage_summary = AsyncMock(side_effect=OSError("gcs fail"))
        with patch(_PATCH_DSS, mock_dss):
            r = client_ds_live.get("/data-status/coverage-summary", params={"service": "instruments-service"})
        assert r.status_code == 500


# ── GET /drilldown/{service}/{asset_group} ────────────────────────────────────


class TestGetDataStatusDrilldownLive:
    def test_happy_path(self, client_ds_live: TestClient) -> None:
        fake_result = {"tree": {}, "totals": {"total": 5}}
        with patch("deployment_api.routes.data_status.get_hierarchical_drilldown", return_value=fake_result):
            r = client_ds_live.get(
                "/data-status/drilldown/strategy-service/cefi",
                params={"start_date": "2026-01-01", "end_date": "2026-01-31"},
            )
        assert r.status_code == 200
        assert r.json()["totals"]["total"] == 5

    def test_runtime_error_returns_500(self, client_ds_live: TestClient) -> None:
        with patch(
            "deployment_api.routes.data_status.get_hierarchical_drilldown",
            side_effect=RuntimeError("drilldown failed"),
        ):
            r = client_ds_live.get(
                "/data-status/drilldown/strategy-service/cefi",
                params={"start_date": "2026-01-01", "end_date": "2026-01-31"},
            )
        assert r.status_code == 500

    def test_with_filters_passed_through(self, client_ds_live: TestClient) -> None:
        fake_result = {"tree": {}, "totals": {"total": 2}}
        with patch("deployment_api.routes.data_status.get_hierarchical_drilldown", return_value=fake_result) as mock_fn:
            r = client_ds_live.get(
                "/data-status/drilldown/strategy-service/defi",
                params={"start_date": "2026-01-01", "end_date": "2026-01-31", "chain": "ETHEREUM"},
            )
        assert r.status_code == 200
        # chain should be in filters passed to drilldown
        call_kwargs = mock_fn.call_args[1]
        assert call_kwargs["filters"].get("chain") == "ETHEREUM"

    def test_sheds_load_with_503_at_capacity(self, client_ds_live: TestClient) -> None:
        """A burst past the in-flight cap fails fast (503 + Retry-After) instead of
        stacking heavy index builds until the container OOMs — the drill-down memory
        backstop (2026-07-15). The build fn is never reached when shedding."""
        with (
            patch("deployment_api.routes.data_status._deploy_turbo._drilldown_inflight", 999),
            patch("deployment_api.routes.data_status.get_hierarchical_drilldown") as mock_fn,
        ):
            r = client_ds_live.get(
                "/data-status/drilldown/strategy-service/cefi",
                params={"start_date": "2026-01-01", "end_date": "2026-01-31"},
            )
        assert r.status_code == 503
        assert r.headers.get("Retry-After") == "5"
        mock_fn.assert_not_called()


# ── POST /deploy-missing-preview ──────────────────────────────────────────────


class TestPostDeployMissingPreviewLive:
    def test_happy_path(self, client_ds_live: TestClient) -> None:
        preview = MagicMock()
        preview.to_dict.return_value = {"command": "gcloud compute ...", "service": "strategy-service"}
        with patch("deployment_api.routes.data_status.build_deploy_missing_preview", return_value=preview):
            r = client_ds_live.post(
                "/data-status/deploy-missing-preview",
                json={
                    "service": "strategy-service",
                    "asset_group": "cefi",
                    "row_key": {"venue": "BINANCE", "day": "2026-01-01"},
                },
            )
        assert r.status_code == 200
        assert r.json()["service"] == "strategy-service"

    def test_missing_service_returns_400(self, client_ds_live: TestClient) -> None:
        r = client_ds_live.post(
            "/data-status/deploy-missing-preview",
            json={"service": "", "asset_group": "cefi", "row_key": {}},
        )
        assert r.status_code == 400

    def test_row_key_not_dict_returns_400(self, client_ds_live: TestClient) -> None:
        r = client_ds_live.post(
            "/data-status/deploy-missing-preview",
            json={"service": "strategy-service", "asset_group": "cefi", "row_key": "not-a-dict"},
        )
        assert r.status_code == 400

    def test_deploy_missing_error_returns_400(self, client_ds_live: TestClient) -> None:
        from deployment_api.services.deploy_missing import DeployMissingError

        with patch(
            "deployment_api.routes.data_status.build_deploy_missing_preview",
            side_effect=DeployMissingError("unsupported mode"),
        ):
            r = client_ds_live.post(
                "/data-status/deploy-missing-preview",
                json={"service": "strategy-service", "asset_group": "cefi", "row_key": {"day": "2026-01-01"}},
            )
        assert r.status_code == 400


# ── GET /turbo ────────────────────────────────────────────────────────────────


class TestGetDataStatusTurboLive:
    def test_happy_path(self, client_ds_live: TestClient) -> None:
        mock_das = MagicMock()
        mock_das.get_data_status_turbo = AsyncMock(return_value={"mode": "turbo", "service": "strategy-service"})
        with patch(_PATCH_DAS, mock_das):
            r = client_ds_live.get(
                "/data-status/turbo",
                params={"service": "strategy-service", "start_date": "2026-01-01", "end_date": "2026-01-31"},
            )
        assert r.status_code == 200

    def test_error_in_result_returns_500(self, client_ds_live: TestClient) -> None:
        mock_das = MagicMock()
        mock_das.get_data_status_turbo = AsyncMock(return_value={"error": "index missing"})
        with patch(_PATCH_DAS, mock_das):
            r = client_ds_live.get(
                "/data-status/turbo",
                params={"service": "strategy-service", "start_date": "2026-01-01", "end_date": "2026-01-31"},
            )
        assert r.status_code == 500

    def test_value_error_returns_500(self, client_ds_live: TestClient) -> None:
        mock_das = MagicMock()
        mock_das.get_data_status_turbo = AsyncMock(side_effect=ValueError("bad"))
        with patch(_PATCH_DAS, mock_das):
            r = client_ds_live.get(
                "/data-status/turbo",
                params={"service": "strategy-service", "start_date": "2026-01-01", "end_date": "2026-01-31"},
            )
        assert r.status_code == 500


# ── GET /turbo/stats ──────────────────────────────────────────────────────────


class TestGetTurboCacheStatsLive:
    def test_happy_path(self, client_ds_live: TestClient) -> None:
        mock_das = MagicMock()
        mock_das.get_cache_stats = AsyncMock(return_value={"entries": 10, "ttl": 300})
        with patch(_PATCH_DAS, mock_das):
            r = client_ds_live.get("/data-status/turbo/stats")
        assert r.status_code == 200

    def test_runtime_error_returns_500(self, client_ds_live: TestClient) -> None:
        mock_das = MagicMock()
        mock_das.get_cache_stats = AsyncMock(side_effect=RuntimeError("fail"))
        with patch(_PATCH_DAS, mock_das):
            r = client_ds_live.get("/data-status/turbo/stats")
        assert r.status_code == 500


# ── POST /turbo/clear ─────────────────────────────────────────────────────────


class TestClearTurboCacheLive:
    def test_happy_path(self, client_ds_live: TestClient) -> None:
        mock_das = MagicMock()
        mock_das.clear_cache = AsyncMock(return_value={"cleared": True})
        with (
            patch(_PATCH_DAS, mock_das),
            patch("deployment_api.routes.data_status.clear_drilldown_cache"),
            patch("deployment_api.services.data_status_service.clear_index_cache"),
            patch("deployment_api.services.data_status_service.clear_rollup_cache"),
        ):
            r = client_ds_live.post("/data-status/turbo/clear")
        assert r.status_code == 200

    def test_runtime_error_returns_500(self, client_ds_live: TestClient) -> None:
        mock_das = MagicMock()
        mock_das.clear_cache = AsyncMock(side_effect=RuntimeError("fail"))
        with (
            patch(_PATCH_DAS, mock_das),
            patch("deployment_api.routes.data_status.clear_drilldown_cache"),
            patch("deployment_api.services.data_status_service.clear_index_cache"),
            patch("deployment_api.services.data_status_service.clear_rollup_cache"),
        ):
            r = client_ds_live.post("/data-status/turbo/clear")
        assert r.status_code == 500


# ── GET /venue-filters ────────────────────────────────────────────────────────


class TestGetVenueFiltersLive:
    def test_happy_path(self, client_ds_live: TestClient) -> None:
        mock_dqs = MagicMock()
        mock_dqs.get_venue_filters = AsyncMock(return_value={"venues": ["BINANCE", "OKX"]})
        with patch(_PATCH_DQS, mock_dqs):
            r = client_ds_live.get("/data-status/venue-filters", params={"service": "strategy-service"})
        assert r.status_code == 200
        assert "venues" in r.json()

    def test_error_in_result_returns_500(self, client_ds_live: TestClient) -> None:
        mock_dqs = MagicMock()
        mock_dqs.get_venue_filters = AsyncMock(return_value={"error": "no venues"})
        with patch(_PATCH_DQS, mock_dqs):
            r = client_ds_live.get("/data-status/venue-filters", params={"service": "strategy-service"})
        assert r.status_code == 500

    def test_value_error_returns_500(self, client_ds_live: TestClient) -> None:
        mock_dqs = MagicMock()
        mock_dqs.get_venue_filters = AsyncMock(side_effect=ValueError("bad svc"))
        with patch(_PATCH_DQS, mock_dqs):
            r = client_ds_live.get("/data-status/venue-filters", params={"service": "strategy-service"})
        assert r.status_code == 500


# ── GET /list-files ───────────────────────────────────────────────────────────


class TestListFilesInPathLive:
    def test_happy_path(self, client_ds_live: TestClient) -> None:
        mock_dqs = MagicMock()
        mock_dqs.list_files_in_path = AsyncMock(return_value={"files": ["a.parquet", "b.parquet"]})
        with patch(_PATCH_DQS, mock_dqs):
            r = client_ds_live.get(
                "/data-status/list-files",
                params={"bucket_name": "my-bucket", "path": "raw_tick_data/"},
            )
        assert r.status_code == 200
        assert "files" in r.json()

    def test_error_in_result_returns_500(self, client_ds_live: TestClient) -> None:
        mock_dqs = MagicMock()
        mock_dqs.list_files_in_path = AsyncMock(return_value={"error": "bucket not found"})
        with patch(_PATCH_DQS, mock_dqs):
            r = client_ds_live.get(
                "/data-status/list-files",
                params={"bucket_name": "my-bucket"},
            )
        assert r.status_code == 500

    def test_permission_error_returns_500(self, client_ds_live: TestClient) -> None:
        mock_dqs = MagicMock()
        mock_dqs.list_files_in_path = AsyncMock(side_effect=PermissionError("403"))
        with patch(_PATCH_DQS, mock_dqs):
            r = client_ds_live.get(
                "/data-status/list-files",
                params={"bucket_name": "my-bucket"},
            )
        assert r.status_code == 500


# ── GET /deploy-missing-services ─────────────────────────────────────────────


class TestGetDeployMissingServicesLive:
    def test_returns_services_key(self, client_ds_live: TestClient) -> None:
        fake_services = ["market-tick-data-service", "strategy-service"]
        with patch("deployment_api.routes.data_status.deploy_missing_supported_services", return_value=fake_services):
            r = client_ds_live.get("/data-status/deploy-missing-services")
        assert r.status_code == 200
        data = r.json()
        assert "services" in data or isinstance(data, list)


# ── GET /drilldown-pairs ──────────────────────────────────────────────────────


class TestGetDrilldownPairsLive:
    def test_returns_pairs(self, client_ds_live: TestClient) -> None:
        with patch(
            "deployment_api.routes.data_status.list_supported_pairs",
            return_value=[{"service": "s", "asset_group": "cefi"}],
        ):
            r = client_ds_live.get("/data-status/drilldown-pairs")
        assert r.status_code == 200
        data = r.json()
        assert isinstance(data, list) or "pairs" in data


# ── POST /deploy-live-cluster-preview ─────────────────────────────────────────


class TestPostDeployLiveClusterPreviewLive:
    def test_happy_path(self, client_ds_live: TestClient) -> None:
        preview = MagicMock()
        preview.to_dict.return_value = {"command": "gcloud compute create ...", "role": "mtds-live"}
        with patch("deployment_api.routes.data_status.build_live_cluster_launch_preview", return_value=preview):
            r = client_ds_live.post(
                "/data-status/deploy-live-cluster-preview",
                json={"role": "mtds-live", "asset_group": "cefi", "deployment_env": "prod"},
            )
        assert r.status_code == 200

    def test_missing_role_returns_400(self, client_ds_live: TestClient) -> None:
        r = client_ds_live.post(
            "/data-status/deploy-live-cluster-preview",
            json={"role": "", "asset_group": "cefi"},
        )
        assert r.status_code == 400

    def test_deploy_missing_error_returns_400(self, client_ds_live: TestClient) -> None:
        from deployment_api.services.deploy_missing import DeployMissingError

        with patch(
            "deployment_api.routes.data_status.build_live_cluster_launch_preview",
            side_effect=DeployMissingError("bad role"),
        ):
            r = client_ds_live.post(
                "/data-status/deploy-live-cluster-preview",
                json={"role": "invalid-role", "asset_group": "cefi"},
            )
        assert r.status_code == 400


# ── GET /deploy-live-cluster-roles ────────────────────────────────────────────


class TestGetDeployLiveClusterRolesLive:
    def test_returns_roles(self, client_ds_live: TestClient) -> None:
        with patch(
            "deployment_api.routes.data_status.deploy_missing_supported_live_cluster_roles",
            return_value=["mtds-live", "mdps-features-live"],
        ):
            r = client_ds_live.get("/data-status/deploy-live-cluster-roles")
        assert r.status_code == 200
        data = r.json()
        assert "roles" in data


# ── GET /instruments ──────────────────────────────────────────────────────────


class TestGetInstrumentsListLive:
    def test_happy_path(self, client_ds_live: TestClient) -> None:
        mock_dqs = MagicMock()
        mock_dqs.get_instruments_list = AsyncMock(return_value={"instruments": ["BTC-USDT"]})
        with patch(_PATCH_DQS, mock_dqs):
            r = client_ds_live.get("/data-status/instruments", params={"asset_group": "cefi"})
        assert r.status_code == 200

    def test_error_result_returns_500(self, client_ds_live: TestClient) -> None:
        mock_dqs = MagicMock()
        mock_dqs.get_instruments_list = AsyncMock(return_value={"error": "no data"})
        with patch(_PATCH_DQS, mock_dqs):
            r = client_ds_live.get("/data-status/instruments", params={"asset_group": "cefi"})
        assert r.status_code == 500

    def test_value_error_returns_500(self, client_ds_live: TestClient) -> None:
        mock_dqs = MagicMock()
        mock_dqs.get_instruments_list = AsyncMock(side_effect=ValueError("bad"))
        with patch(_PATCH_DQS, mock_dqs):
            r = client_ds_live.get("/data-status/instruments", params={"asset_group": "cefi"})
        assert r.status_code == 500


# ── GET /instruments/search ───────────────────────────────────────────────────


class TestSearchInstrumentsLive:
    def test_happy_path(self, client_ds_live: TestClient) -> None:
        mock_dqs = MagicMock()
        mock_dqs.search_instruments = AsyncMock(return_value={"matches": [{"canonical_id": "BTC-USDT"}]})
        with patch(_PATCH_DQS, mock_dqs):
            r = client_ds_live.get("/data-status/instruments/search", params={"query": "BTC"})
        assert r.status_code == 200

    def test_os_error_returns_500(self, client_ds_live: TestClient) -> None:
        mock_dqs = MagicMock()
        mock_dqs.search_instruments = AsyncMock(side_effect=OSError("fail"))
        with patch(_PATCH_DQS, mock_dqs):
            r = client_ds_live.get("/data-status/instruments/search", params={"query": "BTC"})
        assert r.status_code == 500


# ── GET /instrument-availability ──────────────────────────────────────────────


class TestGetInstrumentAvailabilityLive:
    def test_happy_path(self, client_ds_live: TestClient) -> None:
        mock_dqs = MagicMock()
        mock_dqs.get_instrument_availability = AsyncMock(return_value={"available": True})
        with patch(_PATCH_DQS, mock_dqs):
            r = client_ds_live.get(
                "/data-status/instrument-availability",
                params={
                    "venue": "BINANCE",
                    "instrument_type": "spot",
                    "instrument": "BTC-USDT",
                    "start_date": "2026-01-01",
                    "end_date": "2026-01-31",
                },
            )
        assert r.status_code == 200

    def test_error_result_returns_500(self, client_ds_live: TestClient) -> None:
        mock_dqs = MagicMock()
        mock_dqs.get_instrument_availability = AsyncMock(return_value={"error": "not found"})
        with patch(_PATCH_DQS, mock_dqs):
            r = client_ds_live.get(
                "/data-status/instrument-availability",
                params={
                    "venue": "BINANCE",
                    "instrument_type": "spot",
                    "instrument": "BTC-USDT",
                    "start_date": "2026-01-01",
                    "end_date": "2026-01-31",
                },
            )
        assert r.status_code == 500


# ── POST /analyze ─────────────────────────────────────────────────────────────


class TestAnalyzeDataPatternsLive:
    def test_happy_path(self, client_ds_live: TestClient) -> None:
        mock_dss = MagicMock()
        mock_dss.run_data_status_cli = AsyncMock(return_value={"status": "ok"})
        mock_das = MagicMock()
        mock_das.analyze_data_patterns = AsyncMock(return_value={"trends": []})
        with (
            patch(_PATCH_DSS, mock_dss),
            patch(_PATCH_DAS, mock_das),
        ):
            r = client_ds_live.post(
                "/data-status/analyze",
                params={"service": "strategy-service", "start_date": "2026-01-01", "end_date": "2026-01-31"},
            )
        assert r.status_code == 200

    def test_data_status_error_returns_500(self, client_ds_live: TestClient) -> None:
        mock_dss = MagicMock()
        mock_dss.run_data_status_cli = AsyncMock(return_value={"error": "gcs fail"})
        with patch(_PATCH_DSS, mock_dss):
            r = client_ds_live.post(
                "/data-status/analyze",
                params={"service": "strategy-service", "start_date": "2026-01-01", "end_date": "2026-01-31"},
            )
        assert r.status_code == 500

    def test_analytics_error_result_returns_500(self, client_ds_live: TestClient) -> None:
        mock_dss = MagicMock()
        mock_dss.run_data_status_cli = AsyncMock(return_value={"status": "ok"})
        mock_das = MagicMock()
        mock_das.analyze_data_patterns = AsyncMock(return_value={"error": "analysis fail"})
        with (
            patch(_PATCH_DSS, mock_dss),
            patch(_PATCH_DAS, mock_das),
        ):
            r = client_ds_live.post(
                "/data-status/analyze",
                params={"service": "strategy-service", "start_date": "2026-01-01", "end_date": "2026-01-31"},
            )
        assert r.status_code == 500


# ── POST /multi-service ───────────────────────────────────────────────────────


class TestGetMultiServiceStatusLive:
    def test_happy_path(self, client_ds_live: TestClient) -> None:
        mock_das = MagicMock()
        mock_das.aggregate_multi_service_status = AsyncMock(
            return_value={"services": {"strategy-service": {}, "execution-service": {}}}
        )
        with patch(_PATCH_DAS, mock_das):
            r = client_ds_live.post(
                "/data-status/multi-service",
                params={
                    "services": ["strategy-service", "execution-service"],
                    "start_date": "2026-01-01",
                    "end_date": "2026-01-31",
                },
            )
        assert r.status_code == 200

    def test_value_error_returns_500(self, client_ds_live: TestClient) -> None:
        mock_das = MagicMock()
        mock_das.aggregate_multi_service_status = AsyncMock(side_effect=ValueError("bad"))
        with patch(_PATCH_DAS, mock_das):
            r = client_ds_live.post(
                "/data-status/multi-service",
                params={"services": ["svc"], "start_date": "2026-01-01", "end_date": "2026-01-31"},
            )
        assert r.status_code == 500


# ── GET /schema ───────────────────────────────────────────────────────────────


class TestGetSchemaLive:
    def test_happy_path(self, client_ds_live: TestClient) -> None:
        fake_schema = {"columns": [{"name": "timestamp", "type": "datetime64"}]}
        with (
            patch("deployment_api.routes.data_status.build_bucket_name", return_value="my-bucket"),
            patch("deployment_api.routes.data_status.get_schema_for_shard", return_value=fake_schema),
        ):
            r = client_ds_live.get(
                "/data-status/schema",
                params={
                    "service": "strategy-service",
                    "asset_group": "cefi",
                    "instrument_type": "spot",
                    "data_type": "trades",
                },
            )
        assert r.status_code == 200

    def test_value_error_returns_500(self, client_ds_live: TestClient) -> None:
        with (
            patch("deployment_api.routes.data_status.build_bucket_name", side_effect=ValueError("no bucket")),
            patch("deployment_api.routes.data_status.get_schema_for_shard", side_effect=ValueError("fail")),
        ):
            r = client_ds_live.get(
                "/data-status/schema",
                params={
                    "service": "strategy-service",
                    "asset_group": "cefi",
                    "instrument_type": "spot",
                    "data_type": "trades",
                },
            )
        assert r.status_code == 500


# ── GET /instruments-for-shard (non-mock) ─────────────────────────────────────


class TestGetInstrumentsForShardLive:
    def test_happy_path(self, client_ds_live: TestClient) -> None:
        fake_result = {"instruments": [{"instrument_id": "BTC-USDT", "capture_status": "captured"}]}
        with patch("deployment_api.routes.data_status.list_instruments_for_shard", return_value=fake_result):
            r = client_ds_live.get(
                "/data-status/instruments-for-shard",
                params={
                    "service": "market-tick-data-service",
                    "asset_group": "cefi",
                    "venue": "BINANCE",
                    "day": "2026-01-15",
                    "instrument_type": "spot",
                    "data_type": "trades",
                },
            )
        assert r.status_code == 200
        assert "instruments" in r.json()

    def test_os_error_returns_500(self, client_ds_live: TestClient) -> None:
        with patch("deployment_api.routes.data_status.list_instruments_for_shard", side_effect=OSError("fail")):
            r = client_ds_live.get(
                "/data-status/instruments-for-shard",
                params={
                    "service": "market-tick-data-service",
                    "asset_group": "cefi",
                    "venue": "BINANCE",
                    "day": "2026-01-15",
                    "instrument_type": "spot",
                    "data_type": "trades",
                },
            )
        assert r.status_code == 500


# ── GET /bundle-preview ───────────────────────────────────────────────────────


class TestGetBundlePreviewLive:
    def test_happy_path(self, client_ds_live: TestClient) -> None:
        with patch(
            "deployment_api.routes.data_status.preview_bundle_symbols",
            return_value={"symbols": ["BTC", "ETH"], "message": "ok"},
        ):
            r = client_ds_live.get(
                "/data-status/bundle-preview",
                params={
                    "service": "strategy-service",
                    "asset_group": "cefi",
                    "venue": "BINANCE",
                    "day": "2026-01-15",
                    "instrument_type": "options_chain",
                    "data_type": "options",
                },
            )
        assert r.status_code == 200

    def test_value_error_returns_500(self, client_ds_live: TestClient) -> None:
        with patch(
            "deployment_api.routes.data_status.preview_bundle_symbols",
            side_effect=ValueError("fail"),
        ):
            r = client_ds_live.get(
                "/data-status/bundle-preview",
                params={
                    "service": "strategy-service",
                    "asset_group": "cefi",
                    "venue": "BINANCE",
                    "day": "2026-01-15",
                    "instrument_type": "options_chain",
                    "data_type": "options",
                },
            )
        assert r.status_code == 500


# ── GET /bucket-counts ────────────────────────────────────────────────────────


class TestGetBucketCountsLive:
    def test_happy_path(self, client_ds_live: TestClient) -> None:
        with patch(
            "deployment_api.routes.data_status.compute_bucket_counts",
            return_value={"named_market_count": 100, "other_market_count": 5},
        ):
            r = client_ds_live.get(
                "/data-status/bucket-counts",
                params={
                    "service": "strategy-service",
                    "asset_group": "cefi",
                    "venue": "BINANCE",
                    "day": "2026-01-15",
                },
            )
        assert r.status_code == 200
        assert r.json()["count"] == 105

    def test_os_error_returns_500(self, client_ds_live: TestClient) -> None:
        with patch("deployment_api.routes.data_status.compute_bucket_counts", side_effect=OSError("fail")):
            r = client_ds_live.get(
                "/data-status/bucket-counts",
                params={
                    "service": "strategy-service",
                    "asset_group": "cefi",
                    "venue": "BINANCE",
                },
            )
        assert r.status_code == 500

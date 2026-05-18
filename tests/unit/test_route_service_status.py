"""Unit tests for deployment_api/routes/service_status.py helpers and route handlers."""

from __future__ import annotations

import asyncio
from pathlib import Path
from unittest.mock import AsyncMock, MagicMock, patch

import pytest
import yaml
from fastapi import FastAPI
from fastapi.testclient import TestClient

_PATCH_DISABLE_AUTH = "deployment_api.rbac.DISABLE_AUTH"

# ── helpers ───────────────────────────────────────────────────────────────────


@pytest.fixture
def client_svc_status() -> TestClient:
    from deployment_api.routes.service_status import router

    app = FastAPI()
    app.include_router(router, prefix="/api/service-status")
    app.state.config_dir = Path("/tmp/test-config")
    with patch(_PATCH_DISABLE_AUTH, True):
        return TestClient(app, raise_server_exceptions=False)


# ── _read_checklist ───────────────────────────────────────────────────────────


class TestReadChecklist:
    def test_missing_file_returns_none(self, tmp_path: Path) -> None:
        from deployment_api.routes.service_status import _read_checklist

        result = _read_checklist(tmp_path, "strategy-service")
        assert result is None

    def test_valid_checklist_computes_percent(self, tmp_path: Path) -> None:
        from deployment_api.routes.service_status import _read_checklist

        checklist = {
            "categories": [
                {
                    "name": "Phase 1",
                    "items": [
                        {"name": "item1", "status": "done"},
                        {"name": "item2", "status": "pending"},
                        {"name": "item3", "status": "done"},
                    ],
                }
            ]
        }
        (tmp_path / "checklist.strategy-service.yaml").write_text(yaml.dump(checklist))
        result = _read_checklist(tmp_path, "strategy-service")
        assert result is not None
        assert result["total"] == 3
        assert result["completed"] == 2
        assert result["percent"] == pytest.approx(66.7, abs=0.2)

    def test_all_done_returns_100(self, tmp_path: Path) -> None:
        from deployment_api.routes.service_status import _read_checklist

        checklist = {
            "categories": [
                {
                    "items": [
                        {"status": "done"},
                        {"status": "done"},
                    ]
                }
            ]
        }
        (tmp_path / "checklist.execution-service.yaml").write_text(yaml.dump(checklist))
        result = _read_checklist(tmp_path, "execution-service")
        assert result is not None
        assert result["percent"] == 100.0

    def test_empty_categories_returns_none(self, tmp_path: Path) -> None:
        from deployment_api.routes.service_status import _read_checklist

        checklist = {"categories": []}
        (tmp_path / "checklist.alerting-service.yaml").write_text(yaml.dump(checklist))
        result = _read_checklist(tmp_path, "alerting-service")
        assert result is None  # total_items == 0, no result returned

    def test_oserror_on_open_returns_none(self, tmp_path: Path) -> None:
        from deployment_api.routes.service_status import _read_checklist

        # Create a directory where the file is expected — open() raises IsADirectoryError (OSError)
        (tmp_path / "checklist.alerting-service.yaml").mkdir()
        result = _read_checklist(tmp_path, "alerting-service")
        assert result is None


# ── _read_build_cache_for_service ─────────────────────────────────────────────


class TestReadBuildCacheForService:
    def test_empty_cache_returns_none_none(self) -> None:
        from deployment_api.routes.service_status import _read_build_cache_for_service

        with patch("deployment_api.routes.service_status.load_gcs_cache", return_value={}):
            ts, status = _read_build_cache_for_service("strategy-service")
        assert ts is None
        assert status is None

    def test_service_not_in_builds_returns_none(self) -> None:
        from deployment_api.routes.service_status import _read_build_cache_for_service

        cache = {"builds": {"other-service": {"timestamp": "2026-05-01", "status": "SUCCESS"}}}
        with patch("deployment_api.routes.service_status.load_gcs_cache", return_value=cache):
            ts, status = _read_build_cache_for_service("strategy-service")
        assert ts is None
        assert status is None

    def test_valid_build_info_returns_ts_and_status(self) -> None:
        from deployment_api.routes.service_status import _read_build_cache_for_service

        cache = {
            "builds": {
                "strategy-service": {
                    "timestamp": "2026-05-10T12:00:00Z",
                    "status": "SUCCESS",
                }
            }
        }
        with patch("deployment_api.routes.service_status.load_gcs_cache", return_value=cache):
            ts, status = _read_build_cache_for_service("strategy-service")
        assert ts == "2026-05-10T12:00:00Z"
        assert status == "SUCCESS"

    def test_non_dict_build_info_returns_none(self) -> None:
        from deployment_api.routes.service_status import _read_build_cache_for_service

        cache = {"builds": {"strategy-service": "not-a-dict"}}
        with patch("deployment_api.routes.service_status.load_gcs_cache", return_value=cache):
            ts, status = _read_build_cache_for_service("strategy-service")
        assert ts is None
        assert status is None

    def test_non_dict_builds_cache_returns_none(self) -> None:
        from deployment_api.routes.service_status import _read_build_cache_for_service

        cache = {"builds": "not-a-dict"}
        with patch("deployment_api.routes.service_status.load_gcs_cache", return_value=cache):
            ts, status = _read_build_cache_for_service("strategy-service")
        assert ts is None
        assert status is None


# ── _get_quota_manager_status ─────────────────────────────────────────────────


class TestGetQuotaManagerStatus:
    def test_no_broker_url_returns_unknown(self) -> None:
        from deployment_api.routes.service_status import _get_quota_manager_status

        with patch("deployment_api.routes.service_status.asyncio") as mock_asyncio:
            mock_asyncio.to_thread = AsyncMock()

            async def _run() -> dict:
                with patch("deployment_api.settings.QUOTA_BROKER_URL", ""):
                    return await _get_quota_manager_status("quota-manager")

            result = asyncio.run(_run())
        assert result["health"] == "unknown"
        assert result["service"] == "quota-manager"

    def test_healthy_broker_returns_healthy(self) -> None:
        from deployment_api.routes.service_status import _get_quota_manager_status

        async def _run() -> dict:
            with (
                patch("deployment_api.settings.QUOTA_BROKER_URL", "http://broker:8080"),
                patch("deployment_api.routes.service_status.asyncio.to_thread", new=AsyncMock(return_value=True)),
            ):
                return await _get_quota_manager_status("quota-manager")

        result = asyncio.run(_run())
        assert result["health"] == "healthy"

    def test_unhealthy_broker_returns_error(self) -> None:
        from deployment_api.routes.service_status import _get_quota_manager_status

        async def _run() -> dict:
            with (
                patch("deployment_api.settings.QUOTA_BROKER_URL", "http://broker:8080"),
                patch("deployment_api.routes.service_status.asyncio.to_thread", new=AsyncMock(return_value=False)),
            ):
                return await _get_quota_manager_status("quota-manager")

        result = asyncio.run(_run())
        assert result["health"] == "error"


# ── _get_quick_status ─────────────────────────────────────────────────────────


class TestGetQuickStatus:
    def test_quota_manager_delegates_to_quota_handler(self) -> None:
        from deployment_api.routes.service_status import _get_quick_status

        mock_result = {"service": "quota-manager", "health": "healthy"}

        async def _run() -> dict:
            with patch(
                "deployment_api.routes.service_status._get_quota_manager_status",
                new=AsyncMock(return_value=mock_result),
            ):
                return await _get_quick_status("quota-manager")

        result = asyncio.run(_run())
        assert result["health"] == "healthy"

    def test_regular_service_returns_status(self) -> None:
        from deployment_api.routes.service_status import _get_quick_status

        async def _run() -> dict:
            with (
                patch(
                    "deployment_api.routes.service_status.get_latest_data_timestamp_fast",
                    new=AsyncMock(return_value={"latest": "2026-05-01T10:00:00Z"}),
                ),
                patch(
                    "deployment_api.routes.service_status.get_latest_deployment",
                    new=AsyncMock(return_value={"timestamp": "2026-05-01T09:00:00Z", "status": "success"}),
                ),
                patch(
                    "deployment_api.routes.service_status._read_build_cache_for_service",
                    return_value=("2026-05-01T08:00:00Z", "SUCCESS"),
                ),
                patch(
                    "deployment_api.routes.service_status.determine_overview_health",
                    return_value="healthy",
                ),
            ):
                return await _get_quick_status("strategy-service")

        result = asyncio.run(_run())
        assert result["service"] == "strategy-service"
        assert result["health"] == "healthy"

    def test_error_path_returns_error_health(self) -> None:
        from deployment_api.routes.service_status import _get_quick_status

        async def _run() -> dict:
            with (
                patch(
                    "deployment_api.routes.service_status.get_latest_data_timestamp_fast",
                    new=AsyncMock(return_value={}),
                ),
                patch(
                    "deployment_api.routes.service_status.get_latest_deployment",
                    new=AsyncMock(return_value={}),
                ),
                patch(
                    "deployment_api.routes.service_status._read_build_cache_for_service",
                    side_effect=OSError("GCS unavailable"),
                ),
            ):
                return await _get_quick_status("strategy-service")

        result = asyncio.run(_run())
        assert result["health"] == "error"


# ── GET /{service}/status route ───────────────────────────────────────────────


class TestGetServiceStatusRoute:
    def test_invalid_service_returns_400(self, client_svc_status: TestClient) -> None:
        r = client_svc_status.get("/api/service-status/not-a-real-service/status")
        assert r.status_code == 400

    def test_valid_service_returns_200(self, client_svc_status: TestClient) -> None:
        with (
            patch(
                "deployment_api.routes.service_status.get_latest_data_timestamp",
                new=AsyncMock(return_value={"latest": "2026-05-01T10:00:00Z"}),
            ),
            patch(
                "deployment_api.routes.service_status.get_latest_deployment",
                new=AsyncMock(return_value={"timestamp": "2026-05-01T09:00:00Z", "status": "success"}),
            ),
            patch(
                "deployment_api.routes.service_status.get_latest_build",
                new=AsyncMock(return_value={"timestamp": "2026-05-01T08:00:00Z", "status": "SUCCESS"}),
            ),
            patch(
                "deployment_api.routes.service_status.asyncio.to_thread",
                new=AsyncMock(return_value="test-github-token"),
            ),
            patch(
                "deployment_api.routes.service_status.get_latest_code_push",
                new=AsyncMock(return_value={"timestamp": "2026-05-01T07:00:00Z"}),
            ),
            patch("deployment_api.routes.service_status._read_checklist", return_value=None),
            patch("deployment_api.routes.service_status.detect_anomalies", return_value=[]),
            patch("deployment_api.routes.service_status.determine_overview_health", return_value="healthy"),
        ):
            r = client_svc_status.get("/api/service-status/strategy-service/status")
        assert r.status_code == 200
        data = r.json()
        assert data["service"] == "strategy-service"
        assert "health" in data
        assert "_timing" in data

    def test_invalid_service_name_pattern_returns_400(self, client_svc_status: TestClient) -> None:
        r = client_svc_status.get("/api/service-status/UPPERCASE-SERVICE/status")
        assert r.status_code == 400

    def test_github_token_error_still_returns_200(self, client_svc_status: TestClient) -> None:
        with (
            patch(
                "deployment_api.routes.service_status.get_latest_data_timestamp",
                new=AsyncMock(return_value={}),
            ),
            patch(
                "deployment_api.routes.service_status.get_latest_deployment",
                new=AsyncMock(return_value={}),
            ),
            patch(
                "deployment_api.routes.service_status.get_latest_build",
                new=AsyncMock(return_value={}),
            ),
            patch(
                "deployment_api.routes.service_status.asyncio.to_thread",
                side_effect=OSError("Secret unavailable"),
            ),
            patch(
                "deployment_api.routes.service_status.get_latest_code_push",
                new=AsyncMock(return_value={}),
            ),
            patch("deployment_api.routes.service_status._read_checklist", return_value=None),
            patch("deployment_api.routes.service_status.detect_anomalies", return_value=[]),
            patch("deployment_api.routes.service_status.determine_overview_health", return_value="warning"),
        ):
            r = client_svc_status.get("/api/service-status/strategy-service/status")
        assert r.status_code == 200


# ── GET /overview route ───────────────────────────────────────────────────────


class TestGetServicesOverview:
    def test_overview_returns_services_list(self, client_svc_status: TestClient) -> None:
        mock_loader = MagicMock()
        mock_loader.list_available_services.return_value = ["strategy-service", "execution-service"]

        with (
            patch("deployment_api.config_loader.ConfigLoader", return_value=mock_loader),
            patch(
                "deployment_api.routes.service_status._get_quick_status",
                new=AsyncMock(
                    side_effect=lambda svc: {
                        "service": svc,
                        "health": "healthy",
                        "last_data_update": None,
                        "last_deployment": None,
                        "deployment_status": None,
                        "last_build": None,
                        "build_status": None,
                        "anomaly_count": 0,
                    }
                ),
            ),
        ):
            r = client_svc_status.get("/api/service-status/overview")
        assert r.status_code == 200
        data = r.json()
        assert "services" in data
        assert data["count"] >= 2
        assert "healthy" in data


# ── execution-service endpoints ───────────────────────────────────────────────


class TestExecutionServiceEndpoints:
    def test_data_status_delegates_to_helper(self, client_svc_status: TestClient) -> None:
        mock_result = {"shards": [], "total": 0}
        with patch(
            "deployment_api.routes.service_status.get_execution_service_data_status",
            new=AsyncMock(return_value=mock_result),
        ):
            r = client_svc_status.get(
                "/api/service-status/execution-service/data-status",
                params={"config_path": "/some/config"},
            )
        assert r.status_code == 200

    def test_missing_shards_delegates_to_helper(self, client_svc_status: TestClient) -> None:
        mock_result = {"missing": [], "total": 0}
        with patch(
            "deployment_api.routes.service_status.calculate_execution_missing_shards",
            new=AsyncMock(return_value=mock_result),
        ):
            r = client_svc_status.post(
                "/api/service-status/execution-service/missing-shards",
                params={
                    "config_path": "/some/config",
                    "start_date": "2026-05-01",
                    "end_date": "2026-05-10",
                },
            )
        assert r.status_code == 200

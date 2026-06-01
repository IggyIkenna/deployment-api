"""Unit tests for deployments.py mock mode paths (list, get, verify, create).

The existing test_route_deployments.py has an autouse fixture that forces live mode,
so these tests live in a separate file to exercise the mock=True code paths.
"""

from __future__ import annotations

from unittest.mock import MagicMock, patch

import pytest
from fastapi import FastAPI
from fastapi.testclient import TestClient

_PATCH_DISABLE_AUTH = "deployment_api.rbac.DISABLE_AUTH"
_PATCH_CFG = "deployment_api.routes.deployments._cfg"
_HEADERS = {"X-API-Key": "test-key"}


@pytest.fixture
def mock_store() -> MagicMock:
    """Minimal mock_state store with list/get/create."""
    store = MagicMock()
    store.list.return_value = [
        {
            "deployment_id": "dep-1",
            "service": "execution-service",
            "status": "running",
            "asset_group": "defi",
        },
        {
            "deployment_id": "dep-2",
            "service": "strategy-service",
            "status": "completed",
            "asset_group": "cefi",
        },
    ]
    store.get.return_value = {
        "deployment_id": "dep-1",
        "service": "execution-service",
        "status": "running",
    }
    store.create.return_value = None
    return store


@pytest.fixture
def client_dep_mock(mock_store: MagicMock):  # type: ignore[return]
    from deployment_api.routes.deployments import router

    app = FastAPI()
    app.include_router(router)
    mock_cfg = MagicMock()
    mock_cfg.is_mock_mode.return_value = True
    with (
        patch(_PATCH_DISABLE_AUTH, True),
        patch(_PATCH_CFG, mock_cfg),
        patch("deployment_api.mock_state.get_store", return_value=mock_store),
    ):
        yield TestClient(app, raise_server_exceptions=False)


# ── list_deployments mock mode ─────────────────────────────────────────────────


class TestListDeploymentsMock:
    def test_returns_all_deployments(self, client_dep_mock: TestClient, mock_store: MagicMock) -> None:
        r = client_dep_mock.get("/deployments")
        assert r.status_code == 200
        data = r.json()
        assert "deployments" in data
        assert data["total"] == 2

    def test_filter_by_status(self, mock_store: MagicMock) -> None:
        from deployment_api.routes.deployments import router

        app = FastAPI()
        app.include_router(router)
        mock_cfg = MagicMock()
        mock_cfg.is_mock_mode.return_value = True

        with (
            patch(_PATCH_DISABLE_AUTH, True),
            patch(_PATCH_CFG, mock_cfg),
            patch("deployment_api.mock_state.get_store", return_value=mock_store),
        ):
            client = TestClient(app, raise_server_exceptions=False)
            r = client.get("/deployments?status=running")
        assert r.status_code == 200
        data = r.json()
        assert all(d["status"] == "running" for d in data["deployments"])

    def test_filter_by_service(self, mock_store: MagicMock) -> None:
        from deployment_api.routes.deployments import router

        app = FastAPI()
        app.include_router(router)
        mock_cfg = MagicMock()
        mock_cfg.is_mock_mode.return_value = True

        with (
            patch(_PATCH_DISABLE_AUTH, True),
            patch(_PATCH_CFG, mock_cfg),
            patch("deployment_api.mock_state.get_store", return_value=mock_store),
        ):
            client = TestClient(app, raise_server_exceptions=False)
            r = client.get("/deployments?service=execution-service")
        assert r.status_code == 200
        data = r.json()
        assert all(d["service"] == "execution-service" for d in data["deployments"])

    def test_pagination(self, mock_store: MagicMock) -> None:
        from deployment_api.routes.deployments import router

        app = FastAPI()
        app.include_router(router)
        mock_cfg = MagicMock()
        mock_cfg.is_mock_mode.return_value = True

        with (
            patch(_PATCH_DISABLE_AUTH, True),
            patch(_PATCH_CFG, mock_cfg),
            patch("deployment_api.mock_state.get_store", return_value=mock_store),
        ):
            client = TestClient(app, raise_server_exceptions=False)
            r = client.get("/deployments?limit=1&offset=0")
        assert r.status_code == 200
        data = r.json()
        assert len(data["deployments"]) <= 1
        assert data["total"] == 2  # total is unfiltered count


# ── get_deployment_status mock mode ────────────────────────────────────────────


class TestGetDeploymentStatusMock:
    def test_found_returns_deployment(self, client_dep_mock: TestClient) -> None:
        r = client_dep_mock.get("/deployments/dep-1")
        assert r.status_code == 200
        data = r.json()
        assert data["deployment_id"] == "dep-1"

    def test_not_found_returns_404(self, mock_store: MagicMock) -> None:
        from deployment_api.routes.deployments import router

        app = FastAPI()
        app.include_router(router)
        mock_cfg = MagicMock()
        mock_cfg.is_mock_mode.return_value = True
        mock_store.get.return_value = None

        with (
            patch(_PATCH_DISABLE_AUTH, True),
            patch(_PATCH_CFG, mock_cfg),
            patch("deployment_api.mock_state.get_store", return_value=mock_store),
        ):
            client = TestClient(app, raise_server_exceptions=False)
            r = client.get("/deployments/nonexistent")
        assert r.status_code == 404


# ── verify_deployment_completion mock mode ─────────────────────────────────────


class TestVerifyDeploymentMock:
    def test_returns_verified_true(self, client_dep_mock: TestClient) -> None:
        r = client_dep_mock.get("/deployments/dep-1/verify")
        assert r.status_code == 200
        data = r.json()
        assert data["verified"] is True
        assert data["status"] == "completed"
        assert data["mock"] is True


# ── create_deployment mock mode ────────────────────────────────────────────────


class TestCreateDeploymentMock:
    def test_creates_deployment_returns_pending(self, client_dep_mock: TestClient) -> None:
        r = client_dep_mock.post(
            "/deployments",
            json={
                "service": "execution-service",
                "compute": "cloud_run",
                "mode": "batch",
                "cloud_provider": "gcp",
            },
        )
        assert r.status_code == 200
        data = r.json()
        assert data["status"] == "pending"
        assert data["deployment_id"].startswith("dep-mock-")

    def test_creates_deployment_with_shards(self, client_dep_mock: TestClient) -> None:
        r = client_dep_mock.post(
            "/deployments",
            json={
                "service": "strategy-service",
                "compute": "vm",
                "mode": "live",
                "cloud_provider": "gcp",
                "max_shards": 3,
            },
        )
        assert r.status_code == 200
        data = r.json()
        assert data["total_shards"] == 3


# ── asset_group filter in list mock mode ──────────────────────────────────────


class TestDeployMissingShardsMock:
    def test_mock_returns_mock_flag(self, client_dep_mock: TestClient) -> None:
        r = client_dep_mock.post(
            "/deployments/deploy-missing",
            json={
                "service": "market-tick-data-service",
                "start_date": "2026-01-01",
                "end_date": "2026-01-07",
            },
        )
        assert r.status_code == 200
        data = r.json()
        assert data["mock"] is True
        assert "missing_analysis" in data
        assert "deployment" in data

    def test_mock_returns_missing_analysis_shape(self, client_dep_mock: TestClient) -> None:
        r = client_dep_mock.post(
            "/deployments/deploy-missing",
            json={
                "service": "strategy-service",
                "start_date": "2026-02-01",
                "end_date": "2026-02-07",
                "dry_run": False,
            },
        )
        assert r.status_code == 200
        data = r.json()
        analysis = data["missing_analysis"]
        assert "total_missing" in analysis
        assert "summary" in analysis
        dep = data["deployment"]
        assert "deployment_id" in dep
        assert dep["status"] == "pending"

    def test_mock_dry_run_echoes_flag(self, client_dep_mock: TestClient) -> None:
        r = client_dep_mock.post(
            "/deployments/deploy-missing",
            json={
                "service": "execution-service",
                "start_date": "2026-01-01",
                "end_date": "2026-01-03",
                "dry_run": True,
            },
        )
        assert r.status_code == 200
        data = r.json()
        assert data["dry_run"] is True


class TestAssetGroupFilterMock:
    def test_asset_group_filter_applied(self, mock_store: MagicMock) -> None:
        from deployment_api.routes.deployments import router

        app = FastAPI()
        app.include_router(router)
        mock_cfg = MagicMock()
        mock_cfg.is_mock_mode.return_value = True

        # Mock store returns items with different asset groups
        mock_store.list.return_value = [
            {"deployment_id": "dep-1", "service": "execution-service", "status": "running", "asset_group": "DEFI"},
            {"deployment_id": "dep-2", "service": "strategy-service", "status": "completed", "asset_group": "CEFI"},
        ]

        with (
            patch(_PATCH_DISABLE_AUTH, True),
            patch(_PATCH_CFG, mock_cfg),
            patch("deployment_api.mock_state.get_store", return_value=mock_store),
            patch(
                "deployment_api.utils.trading_axis.trading_axis_from_deployment_state",
                side_effect=lambda d: d.get("asset_group"),
            ),
        ):
            client = TestClient(app, raise_server_exceptions=False)
            r = client.get("/deployments?asset_group=DEFI")
        assert r.status_code == 200
        data = r.json()
        assert all(d.get("asset_group") == "DEFI" for d in data["deployments"])

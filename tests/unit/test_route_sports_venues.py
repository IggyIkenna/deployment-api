"""Unit tests for routes/sports_venues.py — mock and live paths for all 5 endpoints."""

from __future__ import annotations

from unittest.mock import MagicMock, patch

import pytest
from fastapi import FastAPI
from fastapi.testclient import TestClient
from unified_trading_library import setup_events

setup_events("deployment-api", "test")

_PATCH_DISABLE_AUTH = "deployment_api.rbac.DISABLE_AUTH"
_PATCH_CLOUD_CFG = "deployment_api.routes.sports_venues._cloud_cfg"


@pytest.fixture
def client_sports() -> TestClient:
    from deployment_api.routes.sports_venues import router

    app = FastAPI()
    app.include_router(router)
    with patch(_PATCH_DISABLE_AUTH, True):
        return TestClient(app, raise_server_exceptions=False)


# ── list_sports_venues ────────────────────────────────────────────────────────


class TestListSportsVenues:
    def test_mock_mode_returns_all_venues(self, client_sports: TestClient) -> None:
        mock_cfg = MagicMock()
        mock_cfg.is_mock_mode.return_value = True
        with patch(_PATCH_CLOUD_CFG, mock_cfg):
            r = client_sports.get("/sports/venues")
        assert r.status_code == 200
        data = r.json()
        assert "venues" in data
        assert data["total"] >= 1

    def test_mock_mode_status_filter_active(self, client_sports: TestClient) -> None:
        mock_cfg = MagicMock()
        mock_cfg.is_mock_mode.return_value = True
        with patch(_PATCH_CLOUD_CFG, mock_cfg):
            r = client_sports.get("/sports/venues?status_filter=active")
        assert r.status_code == 200
        data = r.json()
        assert all(v["status"] == "active" for v in data["venues"])

    def test_mock_mode_status_filter_disabled(self, client_sports: TestClient) -> None:
        mock_cfg = MagicMock()
        mock_cfg.is_mock_mode.return_value = True
        with patch(_PATCH_CLOUD_CFG, mock_cfg):
            r = client_sports.get("/sports/venues?status_filter=disabled")
        assert r.status_code == 200
        data = r.json()
        assert all(v["status"] == "disabled" for v in data["venues"])

    def test_live_mode_returns_live_not_configured(self, client_sports: TestClient) -> None:
        mock_cfg = MagicMock()
        mock_cfg.is_mock_mode.return_value = False
        with patch(_PATCH_CLOUD_CFG, mock_cfg):
            r = client_sports.get("/sports/venues")
        assert r.status_code == 200
        data = r.json()
        assert data["status"] == "live_not_configured"
        assert data["venues"] == []
        assert data["total"] == 0


# ── update_venue_credentials ──────────────────────────────────────────────────


class TestUpdateVenueCredentials:
    def test_mock_mode_returns_updated(self, client_sports: TestClient) -> None:
        mock_cfg = MagicMock()
        mock_cfg.is_mock_mode.return_value = True
        with patch(_PATCH_CLOUD_CFG, mock_cfg):
            r = client_sports.put(
                "/sports/venues/betfair_ex_uk/credentials",
                json={"secret_manager_path": "sports-betfair-creds"},
            )
        assert r.status_code == 200
        data = r.json()
        assert data["venue_key"] == "betfair_ex_uk"
        assert data["status"] == "updated"
        assert data["secret_manager_path"] == "sports-betfair-creds"

    def test_live_mode_returns_live_not_configured(self, client_sports: TestClient) -> None:
        mock_cfg = MagicMock()
        mock_cfg.is_mock_mode.return_value = False
        with patch(_PATCH_CLOUD_CFG, mock_cfg):
            r = client_sports.put(
                "/sports/venues/betfair_ex_uk/credentials",
                json={"secret_manager_path": "sports-betfair-creds"},
            )
        assert r.status_code == 200
        data = r.json()
        assert data["venue_key"] == "betfair_ex_uk"
        assert data["status"] == "live_not_configured"


# ── check_venue_health ────────────────────────────────────────────────────────


class TestCheckVenueHealth:
    def test_mock_mode_returns_reachable(self, client_sports: TestClient) -> None:
        mock_cfg = MagicMock()
        mock_cfg.is_mock_mode.return_value = True
        with patch(_PATCH_CLOUD_CFG, mock_cfg):
            r = client_sports.get("/sports/venues/pinnacle/health")
        assert r.status_code == 200
        data = r.json()
        assert data["venue_key"] == "pinnacle"
        assert data["reachable"] is True
        assert data["login_url_status"] == 200

    def test_live_mode_returns_not_reachable(self, client_sports: TestClient) -> None:
        mock_cfg = MagicMock()
        mock_cfg.is_mock_mode.return_value = False
        with patch(_PATCH_CLOUD_CFG, mock_cfg):
            r = client_sports.get("/sports/venues/draftkings/health")
        assert r.status_code == 200
        data = r.json()
        assert data["venue_key"] == "draftkings"
        assert data["reachable"] is False


# ── enable_venue ──────────────────────────────────────────────────────────────


class TestEnableVenue:
    def test_mock_mode_returns_enabled(self, client_sports: TestClient) -> None:
        mock_cfg = MagicMock()
        mock_cfg.is_mock_mode.return_value = True
        with patch(_PATCH_CLOUD_CFG, mock_cfg):
            r = client_sports.post("/sports/venues/draftkings/enable")
        assert r.status_code == 200
        data = r.json()
        assert data["status"] == "enabled"
        assert data["venue_key"] == "draftkings"

    def test_live_mode_returns_live_not_configured(self, client_sports: TestClient) -> None:
        mock_cfg = MagicMock()
        mock_cfg.is_mock_mode.return_value = False
        with patch(_PATCH_CLOUD_CFG, mock_cfg):
            r = client_sports.post("/sports/venues/betfair_ex_uk/enable")
        assert r.status_code == 200
        data = r.json()
        assert data["status"] == "live_not_configured"
        assert data["venue_key"] == "betfair_ex_uk"


# ── disable_venue ─────────────────────────────────────────────────────────────


class TestDisableVenue:
    def test_mock_mode_returns_disabled(self, client_sports: TestClient) -> None:
        mock_cfg = MagicMock()
        mock_cfg.is_mock_mode.return_value = True
        with patch(_PATCH_CLOUD_CFG, mock_cfg):
            r = client_sports.post("/sports/venues/bet365_au/disable")
        assert r.status_code == 200
        data = r.json()
        assert data["status"] == "disabled"
        assert data["venue_key"] == "bet365_au"

    def test_live_mode_returns_live_not_configured(self, client_sports: TestClient) -> None:
        mock_cfg = MagicMock()
        mock_cfg.is_mock_mode.return_value = False
        with patch(_PATCH_CLOUD_CFG, mock_cfg):
            r = client_sports.post("/sports/venues/bet365_au/disable")
        assert r.status_code == 200
        data = r.json()
        assert data["status"] == "live_not_configured"
        assert data["venue_key"] == "bet365_au"

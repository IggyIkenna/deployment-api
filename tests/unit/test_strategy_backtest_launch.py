"""Unit tests for POST /api/strategy/backtest/launch (Phase 5.D)."""

from __future__ import annotations

from unittest.mock import MagicMock, patch

import pytest
from fastapi.testclient import TestClient

_PATCH_SUBPROCESS = "deployment_api.routes.strategy_backtest_launch.subprocess.run"
_PATCH_LOG = "deployment_api.routes.strategy_backtest_launch.log_event"


@pytest.fixture(scope="module")
def client() -> TestClient:
    from deployment_api.main import app

    return TestClient(app, raise_server_exceptions=False)


_HEADERS = {"X-API-Key": "test-key"}

_VALID_BODY = {
    "archetype": "carry_staked_basis",
    "start_date": "2024-01-01",
    "end_date": "2026-05-01",
    "grid_density": "medium",
    "dry_run": True,
}


def test_dry_run_success(client: TestClient) -> None:
    with patch(_PATCH_LOG):
        resp = client.post("/api/strategy/backtest/launch", json=_VALID_BODY, headers=_HEADERS)
    assert resp.status_code == 200
    data = resp.json()
    assert data["dry_run"] is True
    assert data["vm_name"].startswith("strategy-backtest-grid-")
    assert data["launcher_script"] == "launch-strategy-backtest-grid-vm.sh"
    assert "--archetype" in data["argv"]
    assert "carry_staked_basis" in data["argv"]
    assert "--dry-run" in data["argv"]


def test_invalid_archetype(client: TestClient) -> None:
    body = {**_VALID_BODY, "archetype": "unknown_archetype"}
    with patch(_PATCH_LOG):
        resp = client.post("/api/strategy/backtest/launch", json=body, headers=_HEADERS)
    assert resp.status_code == 400
    assert "archetype" in resp.text.lower()


def test_invalid_grid_density(client: TestClient) -> None:
    body = {**_VALID_BODY, "grid_density": "ultra"}
    with patch(_PATCH_LOG):
        resp = client.post("/api/strategy/backtest/launch", json=body, headers=_HEADERS)
    assert resp.status_code == 400


def test_arbitrage_archetype(client: TestClient) -> None:
    body = {**_VALID_BODY, "archetype": "arbitrage_price_dispersion"}
    with patch(_PATCH_LOG):
        resp = client.post("/api/strategy/backtest/launch", json=body, headers=_HEADERS)
    assert resp.status_code == 200
    data = resp.json()
    assert "arbitrage" in data["vm_name"]


def test_live_launch_success(client: TestClient) -> None:
    body = {**_VALID_BODY, "dry_run": False}
    mock_result = MagicMock()
    mock_result.returncode = 0
    mock_result.stdout = "VM launched"
    mock_result.stderr = ""
    with patch(_PATCH_SUBPROCESS, return_value=mock_result) as mock_sub, patch(_PATCH_LOG):
        with patch("deployment_api.routes.strategy_backtest_launch.Path.exists", return_value=True):
            with patch("deployment_api.routes.strategy_backtest_launch._cfg") as mock_cfg:
                mock_cfg.is_mock_mode.return_value = False
                mock_cfg.gcp_project_id = "test-project"
                resp = client.post("/api/strategy/backtest/launch", json=body, headers=_HEADERS)
    assert resp.status_code == 200
    assert mock_sub.called
    data = resp.json()
    assert data["dry_run"] is False
    assert data["argv"] == []


def test_force_flag_passed(client: TestClient) -> None:
    body = {**_VALID_BODY, "force": True}
    with patch(_PATCH_LOG):
        resp = client.post("/api/strategy/backtest/launch", json=body, headers=_HEADERS)
    assert resp.status_code == 200
    assert "--force" in resp.json()["argv"]

"""Unit tests for POST /api/execution/backtest/launch (Phase 5.D)."""

from __future__ import annotations

from unittest.mock import MagicMock, patch

import pytest
from fastapi.testclient import TestClient

_PATCH_SUBPROCESS = "deployment_api.routes.execution_backtest_launch.subprocess.run"
_PATCH_LOG = "deployment_api.routes.execution_backtest_launch.log_event"


@pytest.fixture(scope="module")
def client() -> TestClient:
    from deployment_api.main import app

    return TestClient(app, raise_server_exceptions=False)


_HEADERS = {"X-API-Key": "test-key"}

_VALID_BODY = {
    "archetype": "carry_staked_basis",
    "tick_interval": 3600,
    "dry_run": True,
}


def test_dry_run_success(client: TestClient) -> None:
    with patch(_PATCH_LOG):
        resp = client.post("/api/execution/backtest/launch", json=_VALID_BODY, headers=_HEADERS)
    assert resp.status_code == 200
    data = resp.json()
    assert data["dry_run"] is True
    assert data["vm_name"].startswith("strategy-paper-")
    assert data["launcher_script"] == "launch-strategy-paper-vm.sh"
    assert "--tick-interval" in data["argv"]
    assert "--dry-run" in data["argv"]


def test_invalid_archetype(client: TestClient) -> None:
    body = {**_VALID_BODY, "archetype": "not_a_real_archetype"}
    with patch(_PATCH_LOG):
        resp = client.post("/api/execution/backtest/launch", json=body, headers=_HEADERS)
    assert resp.status_code == 400
    assert "archetype" in resp.text.lower()


def test_continuous_flag(client: TestClient) -> None:
    body = {**_VALID_BODY, "continuous": True}
    with patch(_PATCH_LOG):
        resp = client.post("/api/execution/backtest/launch", json=body, headers=_HEADERS)
    assert resp.status_code == 200
    assert "--continuous" in resp.json()["argv"]


def test_live_launch_success(client: TestClient) -> None:
    body = {**_VALID_BODY, "dry_run": False}
    mock_result = MagicMock()
    mock_result.returncode = 0
    mock_result.stderr = ""
    with patch(_PATCH_SUBPROCESS, return_value=mock_result) as mock_sub, patch(_PATCH_LOG):
        with patch("deployment_api.routes.execution_backtest_launch.Path.exists", return_value=True):
            with patch("deployment_api.routes.execution_backtest_launch._cfg") as mock_cfg:
                mock_cfg.is_mock_mode.return_value = False
                mock_cfg.gcp_project_id = "test-project"
                resp = client.post("/api/execution/backtest/launch", json=body, headers=_HEADERS)
    assert resp.status_code == 200
    assert mock_sub.called


def test_tick_interval_too_small(client: TestClient) -> None:
    body = {**_VALID_BODY, "tick_interval": 30}
    resp = client.post("/api/execution/backtest/launch", json=body, headers=_HEADERS)
    assert resp.status_code == 422

"""Unit tests for POST /api/ml/experiment/launch (Phase 5.D)."""

from __future__ import annotations

from unittest.mock import MagicMock, patch

import pytest
from fastapi.testclient import TestClient

_PATCH_SUBPROCESS = "deployment_api.routes.ml_experiment_launch.subprocess.run"
_PATCH_LOG = "deployment_api.routes.ml_experiment_launch.log_event"


@pytest.fixture(scope="module")
def client() -> TestClient:
    from deployment_api.main import app

    return TestClient(app, raise_server_exceptions=False)


_HEADERS = {"X-API-Key": "test-key"}

_VALID_BODY = {
    "asset_group": "tradfi",
    "instruments": ["ES_FRONT"],
    "target_types": ["swing_high"],
    "timeframes": ["1m"],
    "dry_run": True,
}


def test_dry_run_success(client: TestClient) -> None:
    with patch(_PATCH_LOG):
        resp = client.post("/api/ml/experiment/launch", json=_VALID_BODY, headers=_HEADERS)
    assert resp.status_code == 200
    data = resp.json()
    assert data["dry_run"] is True
    assert data["vm_name"].startswith("ml-train-")
    assert data["launcher_script"] == "launch-ml-training-vm.sh"
    assert "--dry-run" in data["argv"]
    assert "gs://" in data["events_uri"]


def test_vm_name_encodes_instrument(client: TestClient) -> None:
    with patch(_PATCH_LOG):
        resp = client.post("/api/ml/experiment/launch", json=_VALID_BODY, headers=_HEADERS)
    assert resp.status_code == 200
    assert "es-front" in resp.json()["vm_name"]


def test_invalid_asset_group(client: TestClient) -> None:
    body = {**_VALID_BODY, "asset_group": "notanassetgroup"}
    resp = client.post("/api/ml/experiment/launch", json=body, headers=_HEADERS)
    assert resp.status_code == 422


def test_invalid_operation(client: TestClient) -> None:
    body = {**_VALID_BODY, "operation": "invalid_op"}
    resp = client.post("/api/ml/experiment/launch", json=body, headers=_HEADERS)
    assert resp.status_code == 422


def test_live_launch_success(client: TestClient) -> None:
    body = {**_VALID_BODY, "dry_run": False}
    mock_result = MagicMock()
    mock_result.returncode = 0
    mock_result.stdout = "VM launched"
    mock_result.stderr = ""
    with patch(_PATCH_SUBPROCESS, return_value=mock_result) as mock_sub, patch(_PATCH_LOG):
        with patch("deployment_api.routes.ml_experiment_launch.Path.exists", return_value=True):
            with patch("deployment_api.routes.ml_experiment_launch._cfg") as mock_cfg:
                mock_cfg.is_mock_mode.return_value = False
                mock_cfg.gcp_project_id = "test-project"
                resp = client.post("/api/ml/experiment/launch", json=body, headers=_HEADERS)
    assert resp.status_code == 200
    assert mock_sub.called
    data = resp.json()
    assert data["dry_run"] is False
    assert data["argv"] == []


def test_live_launch_subprocess_failure(client: TestClient) -> None:
    body = {**_VALID_BODY, "dry_run": False}
    mock_result = MagicMock()
    mock_result.returncode = 1
    mock_result.stderr = "gcloud error"
    with patch(_PATCH_SUBPROCESS, return_value=mock_result), patch(_PATCH_LOG):
        with patch("deployment_api.routes.ml_experiment_launch.Path.exists", return_value=True):
            with patch("deployment_api.routes.ml_experiment_launch._cfg") as mock_cfg:
                mock_cfg.is_mock_mode.return_value = False
                mock_cfg.gcp_project_id = "test-project"
                resp = client.post("/api/ml/experiment/launch", json=body, headers=_HEADERS)
    assert resp.status_code == 502

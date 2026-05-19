"""Unit tests for Phase BB.3 experiment lifecycle actions (stop/restart) — credential-free."""

from __future__ import annotations

from unittest.mock import MagicMock, patch

import pytest
from fastapi.testclient import TestClient

from deployment_api.main import app

client = TestClient(app, raise_server_exceptions=True)

_HEADERS = {"X-API-Key": "test-api-key"}


def _make_experiment_entry() -> MagicMock:
    """Construct a MagicMock registry entry for an experiment VM.

    conftest pre-mocks deployment_service.deployments_registry so the real
    dataclass is unavailable; all attributes must be set explicitly.
    """
    entry = MagicMock()
    entry.deployment_id = "exp-deploy-001"
    entry.vm_name = "strategy-backtest-abc123"
    entry.status = "running"
    return entry


def _make_live_entry() -> MagicMock:
    entry = MagicMock()
    entry.deployment_id = "live-deploy-001"
    entry.vm_name = "strategy-live-abc123"
    entry.status = "running"
    return entry




# ---------------------------------------------------------------------------
# _gce_action_cmd builder
# ---------------------------------------------------------------------------


def test_gce_stop_cmd_shape() -> None:
    from deployment_api.routes.monitor_experiments import _gce_action_cmd

    cmd = _gce_action_cmd("strategy-backtest-abc", "stop", "my-proj", "asia-northeast1-b")
    assert "instances" in cmd
    assert "stop" in cmd
    assert "strategy-backtest-abc" in cmd
    assert "--zone=asia-northeast1-b" in cmd
    assert "--project=my-proj" in cmd


def test_gce_restart_cmd_uses_reset() -> None:
    from deployment_api.routes.monitor_experiments import _gce_action_cmd

    cmd = _gce_action_cmd("ml-train-xyz", "restart", "my-proj", "asia-northeast1-b")
    assert "reset" in cmd
    assert "ml-train-xyz" in cmd


# ---------------------------------------------------------------------------
# stop endpoint — preview / mock mode
# ---------------------------------------------------------------------------


def test_stop_experiment_preview() -> None:
    with (
        patch("deployment_api.routes.monitor_experiments.DeploymentsRegistry") as MockReg,
        patch("deployment_api.routes.monitor_experiments._cfg") as mock_cfg,
    ):
        mock_cfg.is_mock_mode.return_value = True
        mock_cfg.gcp_project_id = "test-project"
        instance = MockReg.return_value
        instance.get.return_value = _make_experiment_entry()

        resp = client.post("/api/monitor/experiments/exp-deploy-001/stop", headers=_HEADERS)

    assert resp.status_code == 200
    body = resp.json()
    assert body["status"] == "preview"
    assert body["action"] == "stop"
    assert body["deployment_id"] == "exp-deploy-001"
    assert body["vm_name"] == "strategy-backtest-abc123"
    assert "stop" in body["command"]


def test_restart_experiment_preview() -> None:
    with (
        patch("deployment_api.routes.monitor_experiments.DeploymentsRegistry") as MockReg,
        patch("deployment_api.routes.monitor_experiments._cfg") as mock_cfg,
    ):
        mock_cfg.is_mock_mode.return_value = True
        mock_cfg.gcp_project_id = "test-project"
        instance = MockReg.return_value
        instance.get.return_value = _make_experiment_entry()

        resp = client.post("/api/monitor/experiments/exp-deploy-001/restart", headers=_HEADERS)

    assert resp.status_code == 200
    body = resp.json()
    assert body["status"] == "preview"
    assert body["action"] == "restart"
    assert "reset" in body["command"]


# ---------------------------------------------------------------------------
# 404 for unknown deployment
# ---------------------------------------------------------------------------


def test_stop_unknown_deployment_returns_404() -> None:
    with (
        patch("deployment_api.routes.monitor_experiments.DeploymentsRegistry") as MockReg,
        patch("deployment_api.routes.monitor_experiments._cfg") as mock_cfg,
    ):
        mock_cfg.is_mock_mode.return_value = True
        mock_cfg.gcp_project_id = "test-project"
        instance = MockReg.return_value
        instance.get.return_value = None

        resp = client.post("/api/monitor/experiments/does-not-exist/stop", headers=_HEADERS)

    assert resp.status_code == 404


def test_restart_unknown_deployment_returns_404() -> None:
    with (
        patch("deployment_api.routes.monitor_experiments.DeploymentsRegistry") as MockReg,
        patch("deployment_api.routes.monitor_experiments._cfg") as mock_cfg,
    ):
        mock_cfg.is_mock_mode.return_value = True
        mock_cfg.gcp_project_id = "test-project"
        instance = MockReg.return_value
        instance.get.return_value = None

        resp = client.post("/api/monitor/experiments/does-not-exist/restart", headers=_HEADERS)

    assert resp.status_code == 404


# ---------------------------------------------------------------------------
# 422 for non-experiment VM
# ---------------------------------------------------------------------------


def test_stop_live_vm_returns_422() -> None:
    with (
        patch("deployment_api.routes.monitor_experiments.DeploymentsRegistry") as MockReg,
        patch("deployment_api.routes.monitor_experiments._cfg") as mock_cfg,
    ):
        mock_cfg.is_mock_mode.return_value = True
        mock_cfg.gcp_project_id = "test-project"
        instance = MockReg.return_value
        instance.get.return_value = _make_live_entry()

        resp = client.post("/api/monitor/experiments/live-deploy-001/stop", headers=_HEADERS)

    assert resp.status_code == 422


# ---------------------------------------------------------------------------
# dry_run param forces preview even when not in mock mode
# ---------------------------------------------------------------------------


def test_stop_dry_run_param_forces_preview() -> None:
    with (
        patch("deployment_api.routes.monitor_experiments.DeploymentsRegistry") as MockReg,
        patch("deployment_api.routes.monitor_experiments._cfg") as mock_cfg,
    ):
        mock_cfg.is_mock_mode.return_value = False
        mock_cfg.gcp_project_id = "test-project"
        instance = MockReg.return_value
        instance.get.return_value = _make_experiment_entry()

        resp = client.post(
            "/api/monitor/experiments/exp-deploy-001/stop?dry_run=true",
            headers=_HEADERS,
        )

    assert resp.status_code == 200
    body = resp.json()
    assert body["status"] == "preview"


# ---------------------------------------------------------------------------
# Error response includes error field
# ---------------------------------------------------------------------------


def test_stop_propagates_gce_error() -> None:
    with (
        patch("deployment_api.routes.monitor_experiments.DeploymentsRegistry") as MockReg,
        patch("deployment_api.routes.monitor_experiments._cfg") as mock_cfg,
        patch("deployment_api.routes.monitor_experiments._run_gce_cmd") as mock_run,
    ):
        mock_cfg.is_mock_mode.return_value = False
        mock_cfg.gcp_project_id = "test-project"
        instance = MockReg.return_value
        instance.get.return_value = _make_experiment_entry()
        mock_run.return_value = (False, "ERROR: instance not found")

        resp = client.post("/api/monitor/experiments/exp-deploy-001/stop", headers=_HEADERS)

    assert resp.status_code == 200
    body = resp.json()
    assert body["status"] == "failed"
    assert "instance not found" in (body["error"] or "")


# ---------------------------------------------------------------------------
# Registry error returns 503
# ---------------------------------------------------------------------------


def test_stop_registry_error_returns_503() -> None:
    with (
        patch("deployment_api.routes.monitor_experiments.DeploymentsRegistry") as MockReg,
        patch("deployment_api.routes.monitor_experiments._cfg") as mock_cfg,
    ):
        mock_cfg.is_mock_mode.return_value = True
        mock_cfg.gcp_project_id = "test-project"
        instance = MockReg.return_value
        instance.get.side_effect = RuntimeError("GCS unavailable")

        resp = client.post("/api/monitor/experiments/exp-deploy-001/stop", headers=_HEADERS)

    assert resp.status_code == 503

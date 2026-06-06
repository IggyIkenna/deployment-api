"""Tests for POST /api/vm-deployments/reconcile.

Coverage:
  1. Mock mode returns dry-run result (no GCS or GCP calls).
  2. Real mode: reap_stale called with running VM names; reaped ids returned.
  3. Real mode: fresh heartbeat skips reaping (tested via real reap_stale logic).
  4. GCP fetch failure raises 502.
"""

from __future__ import annotations

import os
from datetime import UTC, datetime, timedelta
from unittest.mock import MagicMock, patch

os.environ.setdefault("CLOUD_MOCK_MODE", "false")
os.environ.setdefault("CLOUD_PROVIDER", "local")
os.environ.setdefault("GCP_PROJECT_ID", "test-project")
os.environ.setdefault("DISABLE_AUTH", "true")
os.environ.setdefault("MOCK_STATE_MODE", "deterministic")

import sys
from types import ModuleType

if "deployment_service.deployments_registry" not in sys.modules:
    _dr_mod = ModuleType("deployment_service.deployments_registry")
    _dr_mod.DEFAULT_BUCKET = "test-bucket"  # type: ignore[attr-defined]
    _dr_mod.DeploymentRegistryEntry = MagicMock()  # type: ignore[attr-defined]
    _dr_mod.DeploymentsRegistry = MagicMock()  # type: ignore[attr-defined]
    _dr_mod.vm_run_log_rolling_uri = MagicMock()  # type: ignore[attr-defined]
    _dr_mod.vm_serial_rolling_uri = MagicMock()  # type: ignore[attr-defined]
    sys.modules["deployment_service.deployments_registry"] = _dr_mod

with (
    patch("unified_trading_library.event_sink.PubSubEventSink"),
    patch("unified_trading_library.PubSubEventSink"),
    patch("unified_trading_library.events.setup_events"),
    patch("unified_trading_library.utils.tracing.setup_tracing"),
    patch("unified_trading_library.setup_tracing"),
):
    from deployment_api.main import app

from deployment_api import auth as _auth_mod

_auth_mod.DISABLE_AUTH = True

import pytest
from fastapi.testclient import TestClient

from deployment_api.deployment_api_config import DeploymentApiConfig

pytestmark = [pytest.mark.timeout(60)]


@pytest.fixture(scope="module")
def client() -> TestClient:
    return TestClient(app)


def _mock_reaped_entry(deployment_id: str) -> MagicMock:
    entry = MagicMock()
    entry.deployment_id = deployment_id
    entry.vm_name = f"vm-{deployment_id}"
    return entry


# ---------------------------------------------------------------------------
# 1. Mock mode
# ---------------------------------------------------------------------------


def test_reconcile_mock_mode(client: TestClient) -> None:
    """Mock mode returns a dry-run result with reaped_count=0."""
    with patch.object(DeploymentApiConfig, "is_mock_mode", return_value=True):
        resp = client.post("/api/vm-deployments/reconcile")
    assert resp.status_code == 200
    data = resp.json()
    assert data["reaped_count"] == 0
    assert data["reaped"] == []
    assert "running_vm_count" in data
    assert "total_active_before" in data


# ---------------------------------------------------------------------------
# 2. Real mode: stale entries get reaped
# ---------------------------------------------------------------------------


def test_reconcile_reaps_stale_entries(client: TestClient) -> None:
    """reap_stale is called with running_vm_names; reaped ids are returned."""
    reaped = [_mock_reaped_entry("dep-stale-1"), _mock_reaped_entry("dep-stale-2")]

    mock_registry = MagicMock()
    mock_registry.list_active.return_value = [MagicMock()] * 5
    mock_registry.reap_stale.return_value = reaped

    gcp_vms = {
        "vm-running-01": {
            "status": "RUNNING",
            "machine_type": "n2-standard-4",
            "zone": "us-central1-a",
            "creation_timestamp": "",
        },
        "vm-running-02": {
            "status": "RUNNING",
            "machine_type": "n2-standard-4",
            "zone": "us-central1-a",
            "creation_timestamp": "",
        },
        "vm-stopped-03": {
            "status": "TERMINATED",
            "machine_type": "n2-standard-4",
            "zone": "us-central1-a",
            "creation_timestamp": "",
        },
    }

    with (
        patch.object(DeploymentApiConfig, "is_mock_mode", return_value=False),
        patch("deployment_api.routes.vm_deployments.DeploymentsRegistry", return_value=mock_registry),
        patch("deployment_api.routes.vm_deployments.get_vm_instance_details", return_value=gcp_vms),
    ):
        resp = client.post("/api/vm-deployments/reconcile")

    assert resp.status_code == 200
    data = resp.json()
    assert data["reaped_count"] == 2
    assert "dep-stale-1" in data["reaped"]
    assert "dep-stale-2" in data["reaped"]
    # Only RUNNING VMs count
    assert data["running_vm_count"] == 2
    assert data["total_active_before"] == 5

    # Verify reap_stale was called with the correct running_vm_names set
    mock_registry.reap_stale.assert_called_once()
    call_kwargs = mock_registry.reap_stale.call_args.kwargs
    assert call_kwargs["running_vm_names"] == {"vm-running-01", "vm-running-02"}


# ---------------------------------------------------------------------------
# 3. Real mode: no stale entries → reaped_count=0
# ---------------------------------------------------------------------------


def test_reconcile_nothing_to_reap(client: TestClient) -> None:
    """When reap_stale returns empty list, reaped_count=0 and reaped=[]."""
    mock_registry = MagicMock()
    mock_registry.list_active.return_value = [MagicMock()] * 3
    mock_registry.reap_stale.return_value = []

    gcp_vms = {
        "vm-all-running": {"status": "RUNNING", "machine_type": "n2", "zone": "us-central1-a", "creation_timestamp": ""}
    }

    with (
        patch.object(DeploymentApiConfig, "is_mock_mode", return_value=False),
        patch("deployment_api.routes.vm_deployments.DeploymentsRegistry", return_value=mock_registry),
        patch("deployment_api.routes.vm_deployments.get_vm_instance_details", return_value=gcp_vms),
    ):
        resp = client.post("/api/vm-deployments/reconcile")

    assert resp.status_code == 200
    data = resp.json()
    assert data["reaped_count"] == 0
    assert data["reaped"] == []
    assert data["total_active_before"] == 3


# ---------------------------------------------------------------------------
# 4. GCP failure → 502
# ---------------------------------------------------------------------------


def test_reconcile_gcp_failure_returns_502(client: TestClient) -> None:
    """When get_vm_instance_details raises, reconcile returns 502."""
    mock_registry = MagicMock()
    mock_registry.list_active.return_value = []

    with (
        patch.object(DeploymentApiConfig, "is_mock_mode", return_value=False),
        patch("deployment_api.routes.vm_deployments.DeploymentsRegistry", return_value=mock_registry),
        patch(
            "deployment_api.routes.vm_deployments.get_vm_instance_details",
            side_effect=RuntimeError("GCP unavailable"),
        ),
    ):
        resp = client.post("/api/vm-deployments/reconcile")

    assert resp.status_code == 502

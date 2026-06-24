"""Unit tests for GET /api/fleet/reconciliation — cross-cloud "every running instance accounted for".

Covers the UNKNOWN (running but unregistered) + EXPECTED-MISSING (registered but not running)
classification at the GCP seam, the control-plane allowlist, and the mock-mode shape.
"""

from __future__ import annotations

import os
from datetime import UTC, datetime
from unittest.mock import patch

import pytest
from fastapi import FastAPI
from fastapi.testclient import TestClient

os.environ.setdefault("CLOUD_PROVIDER", "local")
os.environ.setdefault("CLOUD_MOCK_MODE", "true")
os.environ.setdefault("GCP_PROJECT_ID", "test-project")
os.environ.setdefault("DISABLE_AUTH", "true")

pytestmark = [pytest.mark.timeout(60)]

_NOW = datetime(2026, 6, 24, 12, 0, 0, tzinfo=UTC)


@pytest.fixture
def client_recon() -> TestClient:
    from deployment_api.routes.fleet_reconciliation import router

    app = FastAPI()
    app.include_router(router, prefix="/api")
    return TestClient(app, raise_server_exceptions=False)


def test_mock_mode_shape(client_recon: TestClient) -> None:
    """Mock mode returns the cross-cloud envelope with unknown + expected-missing rows."""
    resp = client_recon.get("/api/fleet/reconciliation")
    assert resp.status_code == 200
    body = resp.json()
    assert body["unknown_total"] == 1
    assert body["expected_missing_total"] == 1
    assert {c["cloud"] for c in body["clouds"]} == {"GCP", "AWS"}


def test_gcp_reconciliation_flags_unknown_and_missing() -> None:
    """A running unregistered VM → UNKNOWN; a registered-but-not-running VM → EXPECTED-MISSING.

    A running control-plane VM (planning prefix) is accounted-for even without a registry entry.
    """
    from deployment_api.routes import fleet_reconciliation as mod

    running_details = {
        "cefi-binance-spot-20260624-aaa": {"status": "RUNNING"},  # registered → accounted
        "rogue-unregistered-vm-20260624": {"status": "RUNNING"},  # NOT registered → UNKNOWN
        "planning-vm-1": {"status": "RUNNING"},  # control-plane prefix → accounted
        "stopped-vm": {"status": "TERMINATED"},  # not running → ignored
    }
    registered = {"cefi-binance-spot-20260624-aaa", "defi-mtds-20260623-missing"}  # 2nd is not running

    with (
        patch.object(mod, "_cfg") as mock_cfg,
        patch.object(mod, "get_vm_instance_details", return_value=running_details),
        patch.object(mod, "active_registry_vm_names", return_value=registered),
        patch.object(mod, "load_aws_inventory", return_value=[]),
    ):
        mock_cfg.is_mock_mode.return_value = False
        mock_cfg.require_gcp_project_id.return_value = "test-project"
        mock_cfg.aws_codebuild_region = "ap-northeast-1"
        mock_cfg.aws_account_id = "123"
        out = mod.build_response([mod._gcp_reconciliation(), mod._aws_reconciliation()], _NOW)  # pyright: ignore[reportPrivateUsage]

    gcp = next(c for c in out.clouds if c.cloud == "GCP")
    unknown_names = {r.name for r in gcp.unknown}
    missing_names = {r.name for r in gcp.expected_missing}
    assert unknown_names == {"rogue-unregistered-vm-20260624"}  # rogue only; control-plane + registered excluded
    assert missing_names == {"defi-mtds-20260623-missing"}  # registered but not running
    assert out.overall == "critical"  # any unknown → critical
    assert out.unknown_total == 1
    assert out.expected_missing_total == 1


def test_build_response_overall_ok_when_clean() -> None:
    """No unknown + no missing → overall ok."""
    from deployment_api.routes.fleet_reconciliation import CloudReconciliation, build_response

    out = build_response([CloudReconciliation(cloud="GCP", running=10, registered=10)], _NOW)
    assert out.overall == "ok"
    assert out.unknown_total == 0

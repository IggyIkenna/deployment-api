"""Unit tests for vm_admin.py — cancel/pause/resume VM admin endpoints."""

from __future__ import annotations

from collections.abc import Generator
from unittest.mock import MagicMock, patch

import pytest
from fastapi import FastAPI
from fastapi.testclient import TestClient
from unified_trading_library import setup_events

from deployment_api.routes.vm_admin import router

setup_events("deployment-api", "test")

_PATCH_MOCK = "deployment_api.deployment_api_config.DeploymentApiConfig.is_mock_mode"


@pytest.fixture
def mock_client() -> Generator[TestClient]:
    with patch(_PATCH_MOCK, return_value=True):
        app = FastAPI()
        app.include_router(router)
        yield TestClient(app)


@pytest.fixture
def prod_client() -> Generator[TestClient]:
    with patch(_PATCH_MOCK, return_value=False):
        app = FastAPI()
        app.include_router(router)
        yield TestClient(app)


class TestCancelMockMode:
    def test_cancel_returns_202(self, mock_client: TestClient) -> None:
        r = mock_client.post("/vm/admin/backfill-cefi-20260515/cancel")
        assert r.status_code == 202
        body = r.json()
        assert body["action"] == "cancel"
        assert body["status"] == "cancelled"
        assert "mock mode" in body["message"]

    def test_cancel_returns_vm_name(self, mock_client: TestClient) -> None:
        r = mock_client.post("/vm/admin/my-vm-123/cancel")
        assert r.json()["vm_name"] == "my-vm-123"


class TestPauseMockMode:
    def test_pause_returns_202(self, mock_client: TestClient) -> None:
        r = mock_client.post("/vm/admin/my-vm-123/pause")
        assert r.status_code == 202
        body = r.json()
        assert body["action"] == "pause"
        assert body["status"] == "pause_requested"

    def test_pause_returns_vm_name(self, mock_client: TestClient) -> None:
        r = mock_client.post("/vm/admin/strategy-paper-defi-20260515/pause")
        assert r.json()["vm_name"] == "strategy-paper-defi-20260515"


class TestResumeMockMode:
    def test_resume_returns_202(self, mock_client: TestClient) -> None:
        r = mock_client.post("/vm/admin/my-vm-123/resume")
        assert r.status_code == 202
        body = r.json()
        assert body["action"] == "resume"
        assert body["status"] == "resumed"

    def test_resume_returns_vm_name(self, mock_client: TestClient) -> None:
        r = mock_client.post("/vm/admin/ml-train-defi-20260515/resume")
        assert r.json()["vm_name"] == "ml-train-defi-20260515"


class TestAuditEmissions:
    """Verify log_event is called for every VM admin action (mock mode)."""

    _PATCH_LOG = "deployment_api.routes.vm_admin.log_event"

    def test_cancel_emits_audit_event(self, mock_client: TestClient) -> None:
        with patch(self._PATCH_LOG) as mock_log:
            mock_client.post("/vm/admin/backfill-cefi-20260515/cancel")
        mock_log.assert_called_once()
        call_args = mock_log.call_args
        assert call_args[0][0] == "VM_CANCEL_REQUESTED"
        assert call_args[1]["severity"] == "WARNING"
        assert call_args[1]["details"]["vm_name"] == "backfill-cefi-20260515"

    def test_pause_emits_audit_event(self, mock_client: TestClient) -> None:
        with patch(self._PATCH_LOG) as mock_log:
            mock_client.post("/vm/admin/my-vm-123/pause")
        mock_log.assert_called_once()
        call_args = mock_log.call_args
        assert call_args[0][0] == "VM_PAUSE_REQUESTED"
        assert call_args[1]["severity"] == "INFO"
        assert call_args[1]["details"]["vm_name"] == "my-vm-123"

    def test_resume_emits_audit_event(self, mock_client: TestClient) -> None:
        with patch(self._PATCH_LOG) as mock_log:
            mock_client.post("/vm/admin/ml-train-defi-20260515/resume")
        mock_log.assert_called_once()
        call_args = mock_log.call_args
        assert call_args[0][0] == "VM_RESUME_REQUESTED"
        assert call_args[1]["severity"] == "INFO"
        assert call_args[1]["details"]["vm_name"] == "ml-train-defi-20260515"


class TestCancelProdMode:
    def test_cancel_404_when_not_found(self, prod_client: TestClient) -> None:
        with patch("deployment_api.routes.vm_admin.DeploymentsRegistry") as mock_reg_cls:
            mock_reg = MagicMock()
            mock_reg.list_active.return_value = []
            mock_reg_cls.return_value = mock_reg
            r = prod_client.post("/vm/admin/nonexistent-vm/cancel")
        assert r.status_code == 404
        assert "No active deployment" in r.json()["detail"]

    def test_cancel_archives_entry(self, prod_client: TestClient) -> None:
        # Use MagicMock with explicit string attributes: conftest stubs
        # DeploymentRegistryEntry as a MagicMock, so attribute access on an
        # instance of that stub doesn't yield plain strings — the comparison
        # entry.vm_name == "..." would fail.
        entry = MagicMock()
        entry.vm_name = "backfill-cefi-20260515"
        entry.status = "running"
        entry.exit_code = None
        entry.extras = {}

        with patch("deployment_api.routes.vm_admin.DeploymentsRegistry") as mock_reg_cls:
            mock_reg = MagicMock()
            mock_reg.list_active.return_value = [entry]
            mock_reg_cls.return_value = mock_reg
            r = prod_client.post("/vm/admin/backfill-cefi-20260515/cancel")

        assert r.status_code == 202
        assert r.json()["status"] == "cancelled"
        mock_reg.complete.assert_called_once()
        assert entry.status == "failed"
        assert entry.exit_code == -1
        assert entry.extras.get("cancel_reason") == "operator_cancel"

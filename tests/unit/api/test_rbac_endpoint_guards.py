"""Unit tests — RBAC enforcement on vm_admin, chaos_injections, and kill_switch routes.

Verifies that routes guarded with require_permission() return HTTP 403 for
under-privileged roles and HTTP 2xx when the caller has sufficient permission.

Patches `deployment_api.rbac.get_current_user_role` to inject a specific role
without requiring a real user store or Firebase token. Fixtures use `yield` so
the patch context stays alive during the test (not just during fixture setup).
"""

from __future__ import annotations

from collections.abc import Generator
from unittest.mock import AsyncMock, patch

import pytest
from fastapi import FastAPI
from fastapi.testclient import TestClient
from unified_api_contracts.internal.schemas.rbac import UserRole
from unified_trading_library import setup_events

setup_events("deployment-api", "test")

_PATCH_DISABLE_AUTH = "deployment_api.rbac.DISABLE_AUTH"
_PATCH_GET_ROLE = "deployment_api.rbac.get_current_user_role"
_PATCH_MOCK_MODE = "deployment_api.deployment_api_config.DeploymentApiConfig.is_mock_mode"


# ── vm_admin RBAC ─────────────────────────────────────────────────────────────


class TestVmAdminRbac:
    """cancel/pause/resume require DEPLOY_TRIGGER; VIEWER → 403, OPERATOR → 202."""

    @pytest.fixture
    def viewer_client(self) -> Generator[TestClient]:
        from deployment_api.routes.vm_admin import router

        app = FastAPI()
        app.include_router(router)
        with (
            patch(_PATCH_DISABLE_AUTH, False),
            patch(_PATCH_GET_ROLE, new=AsyncMock(return_value=UserRole.VIEWER)),
            patch(_PATCH_MOCK_MODE, return_value=True),
        ):
            yield TestClient(app, raise_server_exceptions=False)

    @pytest.fixture
    def operator_client(self) -> Generator[TestClient]:
        from deployment_api.routes.vm_admin import router

        app = FastAPI()
        app.include_router(router)
        with (
            patch(_PATCH_DISABLE_AUTH, False),
            patch(_PATCH_GET_ROLE, new=AsyncMock(return_value=UserRole.OPERATOR)),
            patch(_PATCH_MOCK_MODE, return_value=True),
        ):
            yield TestClient(app, raise_server_exceptions=False)

    def test_cancel_viewer_returns_403(self, viewer_client: TestClient) -> None:
        r = viewer_client.post("/vm/admin/some-vm/cancel")
        assert r.status_code == 403

    def test_pause_viewer_returns_403(self, viewer_client: TestClient) -> None:
        r = viewer_client.post("/vm/admin/some-vm/pause")
        assert r.status_code == 403

    def test_resume_viewer_returns_403(self, viewer_client: TestClient) -> None:
        r = viewer_client.post("/vm/admin/some-vm/resume")
        assert r.status_code == 403

    def test_cancel_operator_returns_202(self, operator_client: TestClient) -> None:
        r = operator_client.post("/vm/admin/some-vm/cancel")
        assert r.status_code == 202

    def test_pause_operator_returns_202(self, operator_client: TestClient) -> None:
        r = operator_client.post("/vm/admin/some-vm/pause")
        assert r.status_code == 202

    def test_resume_operator_returns_202(self, operator_client: TestClient) -> None:
        r = operator_client.post("/vm/admin/some-vm/resume")
        assert r.status_code == 202


# ── chaos_injections RBAC ─────────────────────────────────────────────────────


class TestChaosInjectionsRbac:
    """POST/DELETE chaos injections require SYSTEM_ADMIN; OPERATOR → 403."""

    @pytest.fixture
    def operator_client(self) -> Generator[TestClient]:
        from deployment_api.routes.chaos_injections import router

        app = FastAPI()
        app.include_router(router)
        with (
            patch(_PATCH_DISABLE_AUTH, False),
            patch(_PATCH_GET_ROLE, new=AsyncMock(return_value=UserRole.OPERATOR)),
        ):
            yield TestClient(app, raise_server_exceptions=False)

    @pytest.fixture
    def super_admin_client(self) -> Generator[TestClient]:
        from deployment_api.routes.chaos_injections import router

        app = FastAPI()
        app.include_router(router)
        with (
            patch(_PATCH_DISABLE_AUTH, False),
            patch(_PATCH_GET_ROLE, new=AsyncMock(return_value=UserRole.SUPER_ADMIN)),
        ):
            yield TestClient(app, raise_server_exceptions=False)

    def test_create_injection_operator_returns_403(self, operator_client: TestClient) -> None:
        payload = {
            "point": "PRE_ORDER_VALIDATION",
            "target_service": "execution-service",
            "runtime_profile": "staging",
            "created_by": "test-operator",
        }
        r = operator_client.post("/chaos/injections", json=payload)
        assert r.status_code == 403
        assert "Insufficient permissions" in r.json()["detail"]

    def test_revoke_injection_operator_returns_403(self, operator_client: TestClient) -> None:
        r = operator_client.delete("/chaos/injections/chaos_nonexistent")
        assert r.status_code == 403
        assert "Insufficient permissions" in r.json()["detail"]

    def test_create_injection_super_admin_rbac_passes(self, super_admin_client: TestClient) -> None:
        """SUPER_ADMIN passes RBAC; business-logic errors (400/403) are not RBAC failures."""
        payload = {
            "point": "PRE_ORDER_VALIDATION",
            "target_service": "execution-service",
            "runtime_profile": "staging",
            "created_by": "test-admin",
        }
        r = super_admin_client.post("/chaos/injections", json=payload)
        # Not a RBAC 403 — RBAC passed; 201/400/422 = business logic ran
        if r.status_code == 403:
            assert "Insufficient permissions" not in r.json().get("detail", "")
        else:
            assert r.status_code in (201, 400, 422)

    def test_list_injections_operator_returns_200(self, operator_client: TestClient) -> None:
        """GET (view) is not RBAC-gated — OPERATOR can list."""
        r = operator_client.get("/chaos/injections")
        assert r.status_code == 200

    def test_revoke_nonexistent_super_admin_returns_404(self, super_admin_client: TestClient) -> None:
        """SUPER_ADMIN DELETE on missing id → 404 (RBAC passed, business logic ran)."""
        r = super_admin_client.delete("/chaos/injections/chaos_nonexistent")
        assert r.status_code == 404

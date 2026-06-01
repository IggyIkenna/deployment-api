"""Unit tests for rbac.py — get_current_user_role + require_role coverage.

These tests complement test_rbac_endpoint_guards.py (which tests endpoint-level
403 behaviour) by covering the utility functions directly:
- get_current_user_role: DISABLE_AUTH=False paths (email lookup + user service)
- require_role: role-hierarchy enforcement
"""

from __future__ import annotations

from unittest.mock import MagicMock, patch

import pytest
from fastapi import Request
from unified_api_contracts.internal.schemas.rbac import Permission, UserRole
from unified_trading_library import setup_events

setup_events("deployment-api", "test")

_PATCH_DISABLE_AUTH = "deployment_api.rbac.DISABLE_AUTH"
_PATCH_USER_SVC = "deployment_api.rbac._user_service"
_PATCH_LOG = "deployment_api.rbac.log_event"


# ── get_current_user_role ──────────────────────────────────────────────────────


class TestGetCurrentUserRole:
    """DISABLE_AUTH=False paths in get_current_user_role."""

    @pytest.mark.asyncio
    async def test_no_email_in_request_returns_viewer(self) -> None:
        from deployment_api.rbac import get_current_user_role

        request = MagicMock(spec=Request)
        request.state = MagicMock(spec=object)
        # no user_email attribute on state
        del request.state.user_email

        with patch(_PATCH_DISABLE_AUTH, False):
            role = await get_current_user_role(request)

        assert role == UserRole.VIEWER

    @pytest.mark.asyncio
    async def test_email_present_user_not_found_returns_viewer(self) -> None:
        from deployment_api.rbac import get_current_user_role

        request = MagicMock(spec=Request)
        request.state = MagicMock()
        request.state.user_email = "unknown@example.com"

        mock_svc = MagicMock()
        mock_svc.get_user_by_email.return_value = None

        with patch(_PATCH_DISABLE_AUTH, False), patch(_PATCH_USER_SVC, mock_svc):
            role = await get_current_user_role(request)

        assert role == UserRole.VIEWER
        mock_svc.get_user_by_email.assert_called_once_with("unknown@example.com")

    @pytest.mark.asyncio
    async def test_email_present_user_found_returns_user_role(self) -> None:
        from deployment_api.rbac import get_current_user_role

        request = MagicMock(spec=Request)
        request.state = MagicMock()
        request.state.user_email = "admin@example.com"

        mock_profile = MagicMock()
        mock_profile.role = UserRole.ADMIN.value

        mock_svc = MagicMock()
        mock_svc.get_user_by_email.return_value = mock_profile

        with patch(_PATCH_DISABLE_AUTH, False), patch(_PATCH_USER_SVC, mock_svc):
            role = await get_current_user_role(request)

        assert role == UserRole.ADMIN


# ── require_role ───────────────────────────────────────────────────────────────


class TestRequireRole:
    """require_role: DISABLE_AUTH=False + role hierarchy enforcement."""

    @pytest.mark.asyncio
    async def test_disable_auth_passes_all(self) -> None:
        from deployment_api.rbac import require_role

        request = MagicMock(spec=Request)
        request.state = MagicMock(spec=object)
        del request.state.user_email

        check_fn = require_role(UserRole.SUPER_ADMIN)
        with patch(_PATCH_DISABLE_AUTH, True):
            await check_fn(request)  # should not raise

    @pytest.mark.asyncio
    async def test_sufficient_role_passes(self) -> None:
        from deployment_api.rbac import require_role

        request = MagicMock(spec=Request)
        request.state = MagicMock()
        request.state.user_email = "op@example.com"

        mock_profile = MagicMock()
        mock_profile.role = UserRole.OPERATOR.value
        mock_svc = MagicMock()
        mock_svc.get_user_by_email.return_value = mock_profile

        check_fn = require_role(UserRole.OPERATOR)
        with patch(_PATCH_DISABLE_AUTH, False), patch(_PATCH_USER_SVC, mock_svc), patch(_PATCH_LOG):
            await check_fn(request)  # should not raise

    @pytest.mark.asyncio
    async def test_insufficient_role_raises_403(self) -> None:
        from fastapi import HTTPException

        from deployment_api.rbac import require_role

        request = MagicMock(spec=Request)
        request.url = MagicMock()
        request.url.path = "/guarded"
        request.state = MagicMock()
        request.state.user_email = "viewer@example.com"

        mock_profile = MagicMock()
        mock_profile.role = UserRole.VIEWER.value
        mock_svc = MagicMock()
        mock_svc.get_user_by_email.return_value = mock_profile

        check_fn = require_role(UserRole.ADMIN)
        with (
            patch(_PATCH_DISABLE_AUTH, False),
            patch(_PATCH_USER_SVC, mock_svc),
            patch(_PATCH_LOG),
            pytest.raises(HTTPException) as exc_info,
        ):
            await check_fn(request)

        assert exc_info.value.status_code == 403
        assert "admin" in exc_info.value.detail

    @pytest.mark.asyncio
    async def test_unknown_role_treated_as_below_all(self) -> None:
        from fastapi import HTTPException

        from deployment_api.rbac import require_role

        request = MagicMock(spec=Request)
        request.url = MagicMock()
        request.url.path = "/guarded"
        request.state = MagicMock()
        request.state.user_email = "ghost@example.com"

        mock_profile = MagicMock()
        mock_profile.role = "UNKNOWN_ROLE"
        mock_svc = MagicMock()
        mock_svc.get_user_by_email.return_value = mock_profile

        # Unknown role falls back to VIEWER; require ADMIN to prove it's denied
        check_fn = require_role(UserRole.ADMIN)
        with (
            patch(_PATCH_DISABLE_AUTH, False),
            patch(_PATCH_USER_SVC, mock_svc),
            patch(_PATCH_LOG),
            pytest.raises(HTTPException) as exc_info,
        ):
            await check_fn(request)

        assert exc_info.value.status_code == 403


# ── require_permission (non-DISABLE_AUTH paths) ────────────────────────────────


class TestRequirePermissionFullPath:
    """require_permission: DISABLE_AUTH=False — role resolution + grant/deny."""

    @pytest.mark.asyncio
    async def test_sufficient_permission_passes(self) -> None:
        from deployment_api.rbac import require_permission

        request = MagicMock(spec=Request)
        request.url = MagicMock()
        request.url.path = "/launch"
        request.state = MagicMock()
        request.state.user_email = "op@example.com"

        mock_profile = MagicMock()
        mock_profile.role = UserRole.OPERATOR.value
        mock_svc = MagicMock()
        mock_svc.get_user_by_email.return_value = mock_profile

        check_fn = require_permission(Permission.DEPLOY_TRIGGER)
        with patch(_PATCH_DISABLE_AUTH, False), patch(_PATCH_USER_SVC, mock_svc), patch(_PATCH_LOG):
            await check_fn(request)  # should not raise

    @pytest.mark.asyncio
    async def test_insufficient_permission_raises_403(self) -> None:
        from fastapi import HTTPException

        from deployment_api.rbac import require_permission

        request = MagicMock(spec=Request)
        request.url = MagicMock()
        request.url.path = "/launch"
        request.state = MagicMock()
        request.state.user_email = "viewer@example.com"

        mock_profile = MagicMock()
        mock_profile.role = UserRole.VIEWER.value
        mock_svc = MagicMock()
        mock_svc.get_user_by_email.return_value = mock_profile

        check_fn = require_permission(Permission.DEPLOY_TRIGGER)
        with (
            patch(_PATCH_DISABLE_AUTH, False),
            patch(_PATCH_USER_SVC, mock_svc),
            patch(_PATCH_LOG),
            pytest.raises(HTTPException) as exc_info,
        ):
            await check_fn(request)

        assert exc_info.value.status_code == 403
        assert "deploy:trigger" in exc_info.value.detail

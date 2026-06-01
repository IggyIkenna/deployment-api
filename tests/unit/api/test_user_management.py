"""Unit tests for user_management.py routes — audit emission coverage."""

from __future__ import annotations

from unittest.mock import MagicMock, patch

import pytest
from fastapi import FastAPI
from fastapi.testclient import TestClient
from unified_trading_library import setup_events

from deployment_api.routes.user_management import router

setup_events("deployment-api", "test")

_PATCH_LOG = "deployment_api.routes.user_management.log_event"

# conftest.py sets DISABLE_AUTH=true, so no RBAC patching needed.


def _make_user_mock(user_id: str = "uid123", email: str = "harsh@example.com", role: str = "viewer") -> MagicMock:
    m = MagicMock()
    m.user_id = user_id
    m.email = email
    m.role = role
    m.model_dump = lambda: {"user_id": user_id, "email": email, "role": role}
    return m


@pytest.fixture
def client() -> TestClient:
    app = FastAPI()
    app.include_router(router)
    return TestClient(app)


class TestCreateUserAudit:
    def test_create_user_emits_user_created(self, client: TestClient) -> None:
        user_mock = _make_user_mock(user_id="harsh_at_example_com", role="viewer")
        svc_mock = MagicMock()
        svc_mock.create_user.return_value = user_mock

        with (
            patch("deployment_api.routes.user_management._user_service", svc_mock),
            patch(_PATCH_LOG) as mock_log,
        ):
            r = client.post(
                "/users",
                json={"email": "harsh@example.com", "display_name": "Harsh", "role": "viewer"},
            )

        assert r.status_code == 201
        mock_log.assert_called_once()
        call_args = mock_log.call_args
        assert call_args[0][0] == "USER_CREATED"
        assert call_args[1]["severity"] == "INFO"
        assert call_args[1]["details"]["user_id"] == "harsh_at_example_com"

    def test_create_user_conflict_does_not_emit(self, client: TestClient) -> None:
        svc_mock = MagicMock()
        svc_mock.create_user.side_effect = ValueError("already exists")

        with (
            patch("deployment_api.routes.user_management._user_service", svc_mock),
            patch(_PATCH_LOG) as mock_log,
        ):
            r = client.post(
                "/users",
                json={"email": "x@y.com", "display_name": "X", "role": "viewer"},
            )

        assert r.status_code == 409
        mock_log.assert_not_called()


class TestUpdateUserAudit:
    def test_update_user_emits_user_updated(self, client: TestClient) -> None:
        user_mock = _make_user_mock(role="admin")
        svc_mock = MagicMock()
        svc_mock.update_user.return_value = user_mock

        with (
            patch("deployment_api.routes.user_management._user_service", svc_mock),
            patch(_PATCH_LOG) as mock_log,
        ):
            r = client.patch("/users/uid123", json={"role": "admin"})

        assert r.status_code == 200
        mock_log.assert_called_once()
        assert mock_log.call_args[0][0] == "USER_UPDATED"
        assert mock_log.call_args[1]["details"]["user_id"] == "uid123"

    def test_update_user_404_does_not_emit(self, client: TestClient) -> None:
        svc_mock = MagicMock()
        svc_mock.update_user.return_value = None

        with (
            patch("deployment_api.routes.user_management._user_service", svc_mock),
            patch(_PATCH_LOG) as mock_log,
        ):
            r = client.patch("/users/nonexistent", json={"role": "admin"})

        assert r.status_code == 404
        mock_log.assert_not_called()


class TestDeleteUserAudit:
    def test_delete_user_emits_user_deactivated(self, client: TestClient) -> None:
        svc_mock = MagicMock()
        svc_mock.delete_user.return_value = True

        with (
            patch("deployment_api.routes.user_management._user_service", svc_mock),
            patch(_PATCH_LOG) as mock_log,
        ):
            r = client.delete("/users/uid123")

        assert r.status_code == 200
        mock_log.assert_called_once()
        assert mock_log.call_args[0][0] == "USER_DEACTIVATED"
        assert mock_log.call_args[1]["severity"] == "WARNING"
        assert mock_log.call_args[1]["details"]["user_id"] == "uid123"

    def test_delete_user_404_does_not_emit(self, client: TestClient) -> None:
        svc_mock = MagicMock()
        svc_mock.delete_user.return_value = False

        with (
            patch("deployment_api.routes.user_management._user_service", svc_mock),
            patch(_PATCH_LOG) as mock_log,
        ):
            r = client.delete("/users/nonexistent")

        assert r.status_code == 404
        mock_log.assert_not_called()


class TestAssignRoleAudit:
    def test_assign_role_emits_user_role_assigned(self, client: TestClient) -> None:
        user_mock = _make_user_mock(role="admin")
        svc_mock = MagicMock()
        svc_mock.assign_role.return_value = user_mock

        with (
            patch("deployment_api.routes.user_management._user_service", svc_mock),
            patch(_PATCH_LOG) as mock_log,
        ):
            r = client.post("/users/uid123/role", json={"role": "admin"})

        assert r.status_code == 200
        mock_log.assert_called_once()
        assert mock_log.call_args[0][0] == "USER_ROLE_ASSIGNED"
        assert mock_log.call_args[1]["severity"] == "WARNING"
        assert mock_log.call_args[1]["details"]["role"] == "admin"

    def test_assign_role_404_does_not_emit(self, client: TestClient) -> None:
        svc_mock = MagicMock()
        svc_mock.assign_role.return_value = None

        with (
            patch("deployment_api.routes.user_management._user_service", svc_mock),
            patch(_PATCH_LOG) as mock_log,
        ):
            r = client.post("/users/nonexistent/role", json={"role": "admin"})

        assert r.status_code == 404
        mock_log.assert_not_called()

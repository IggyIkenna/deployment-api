"""Unit tests for routes/user_management.py — CRUD endpoints + RBAC."""

from __future__ import annotations

from collections.abc import Generator
from unittest.mock import MagicMock, patch

import pytest
from fastapi import FastAPI
from fastapi.testclient import TestClient
from unified_trading_library import setup_events

setup_events("deployment-api", "test")

_PATCH_DISABLE_AUTH = "deployment_api.rbac.DISABLE_AUTH"
_PATCH_SVC = "deployment_api.routes.user_management._user_service"
_PATCH_LOG = "deployment_api.routes.user_management.log_event"

_USER_DICT = {
    "user_id": "admin_at_example_com",
    "email": "admin@example.com",
    "display_name": "Admin User",
    "role": "admin",
    "permissions": ["user:view", "user:manage"],
    "is_active": True,
    "created_at": "2024-01-01T00:00:00+00:00",
    "last_login_at": None,
    "mfa_enabled": False,
    "provider": "google",
}


def _mock_profile() -> MagicMock:
    p = MagicMock()
    p.model_dump.return_value = _USER_DICT
    p.user_id = "admin_at_example_com"
    p.email = "admin@example.com"
    p.role = "admin"
    return p


@pytest.fixture
def client() -> Generator[TestClient]:
    from deployment_api.routes.user_management import router

    app = FastAPI()
    app.include_router(router)
    with patch(_PATCH_DISABLE_AUTH, True):
        yield TestClient(app, raise_server_exceptions=False)


# ── GET /users ────────────────────────────────────────────────────────────────


def test_list_users_empty(client: TestClient) -> None:
    with patch(_PATCH_SVC) as mock_svc:
        mock_svc.list_users.return_value = []
        r = client.get("/users")
    assert r.status_code == 200
    assert r.json()["count"] == 0


def test_list_users_returns_all(client: TestClient) -> None:
    profile = _mock_profile()
    with patch(_PATCH_SVC) as mock_svc:
        mock_svc.list_users.return_value = [profile]
        r = client.get("/users")
    assert r.status_code == 200
    assert r.json()["count"] == 1


# ── GET /users/{user_id} ──────────────────────────────────────────────────────


def test_get_user_found(client: TestClient) -> None:
    profile = _mock_profile()
    with patch(_PATCH_SVC) as mock_svc:
        mock_svc.get_user.return_value = profile
        r = client.get("/users/admin_at_example_com")
    assert r.status_code == 200
    assert r.json()["user"]["email"] == "admin@example.com"


def test_get_user_not_found(client: TestClient) -> None:
    with patch(_PATCH_SVC) as mock_svc:
        mock_svc.get_user.return_value = None
        r = client.get("/users/nonexistent")
    assert r.status_code == 404


# ── POST /users ───────────────────────────────────────────────────────────────


def test_create_user_success(client: TestClient) -> None:
    profile = _mock_profile()
    with patch(_PATCH_SVC) as mock_svc, patch(_PATCH_LOG):
        mock_svc.create_user.return_value = profile
        r = client.post(
            "/users",
            json={"email": "admin@example.com", "display_name": "Admin", "role": "admin"},
        )
    assert r.status_code == 201
    assert "user" in r.json()


def test_create_user_invalid_role(client: TestClient) -> None:
    r = client.post(
        "/users",
        json={"email": "x@x.com", "display_name": "X", "role": "INVALID_ROLE"},
    )
    assert r.status_code == 400
    assert "Invalid role" in r.json()["detail"]


def test_create_user_duplicate_email(client: TestClient) -> None:
    with patch(_PATCH_SVC) as mock_svc:
        mock_svc.create_user.side_effect = ValueError("User with email x@x.com already exists")
        r = client.post(
            "/users",
            json={"email": "x@x.com", "display_name": "X", "role": "viewer"},
        )
    assert r.status_code == 409
    assert "already exists" in r.json()["detail"]


# ── PATCH /users/{user_id} ────────────────────────────────────────────────────


def test_update_user_success(client: TestClient) -> None:
    profile = _mock_profile()
    with patch(_PATCH_SVC) as mock_svc, patch(_PATCH_LOG):
        mock_svc.update_user.return_value = profile
        r = client.patch("/users/admin_at_example_com", json={"display_name": "Updated"})
    assert r.status_code == 200
    assert "user" in r.json()


def test_update_user_invalid_role(client: TestClient) -> None:
    r = client.patch("/users/some-id", json={"role": "INVALID_ROLE"})
    assert r.status_code == 400


def test_update_user_not_found(client: TestClient) -> None:
    with patch(_PATCH_SVC) as mock_svc:
        mock_svc.update_user.return_value = None
        r = client.patch("/users/nonexistent", json={"display_name": "X"})
    assert r.status_code == 404


# ── DELETE /users/{user_id} ───────────────────────────────────────────────────


def test_delete_user_success(client: TestClient) -> None:
    with patch(_PATCH_SVC) as mock_svc, patch(_PATCH_LOG):
        mock_svc.delete_user.return_value = True
        r = client.delete("/users/admin_at_example_com")
    assert r.status_code == 200
    assert r.json()["deactivated"] is True


def test_delete_user_not_found(client: TestClient) -> None:
    with patch(_PATCH_SVC) as mock_svc:
        mock_svc.delete_user.return_value = False
        r = client.delete("/users/nonexistent")
    assert r.status_code == 404


# ── POST /users/{user_id}/role ────────────────────────────────────────────────


def test_assign_role_success(client: TestClient) -> None:
    profile = _mock_profile()
    with patch(_PATCH_SVC) as mock_svc, patch(_PATCH_LOG):
        mock_svc.assign_role.return_value = profile
        r = client.post("/users/admin_at_example_com/role", json={"role": "operator"})
    assert r.status_code == 200
    assert "user" in r.json()


def test_assign_role_invalid_role(client: TestClient) -> None:
    r = client.post("/users/some-id/role", json={"role": "INVALID_ROLE"})
    assert r.status_code == 400


def test_assign_role_not_found(client: TestClient) -> None:
    with patch(_PATCH_SVC) as mock_svc:
        mock_svc.assign_role.return_value = None
        r = client.post("/users/nonexistent/role", json={"role": "viewer"})
    assert r.status_code == 404


# ── GET /roles + GET /permissions ─────────────────────────────────────────────


def test_list_roles(client: TestClient) -> None:
    r = client.get("/roles")
    assert r.status_code == 200
    assert r.json()["count"] > 0


def test_list_permissions(client: TestClient) -> None:
    r = client.get("/permissions")
    assert r.status_code == 200
    assert r.json()["count"] > 0

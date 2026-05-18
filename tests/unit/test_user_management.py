"""Unit tests for services/user_management.py — full CRUD + helper coverage."""

from __future__ import annotations

import json
import tempfile
from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest
from unified_api_contracts.internal.schemas.rbac import UserRole
from unified_trading_library import setup_events

setup_events("deployment-api", "test")

_PATCH_LOAD = "deployment_api.services.user_management._load_users"
_PATCH_SAVE = "deployment_api.services.user_management._save_users"
_PATCH_LOG = "deployment_api.services.user_management.log_event"
_PATCH_CFG = "deployment_api.services.user_management._cfg"

_VIEWER_DATA: dict[str, object] = {
    "user_id": "viewer_at_example_com",
    "email": "viewer@example.com",
    "display_name": "Viewer User",
    "role": "viewer",
    "is_active": True,
    "created_at": "2024-01-01T00:00:00+00:00",
    "last_login_at": None,
    "mfa_enabled": False,
    "provider": "google",
}

_ADMIN_DATA: dict[str, object] = {
    "user_id": "admin_at_example_com",
    "email": "admin@example.com",
    "display_name": "Admin User",
    "role": "admin",
    "is_active": True,
    "created_at": "2024-02-01T00:00:00+00:00",
    "last_login_at": "2024-03-01T12:00:00+00:00",
    "mfa_enabled": True,
    "provider": "google",
}


# ── helper function coverage ──────────────────────────────────────────────────


class TestUserStorePath:
    def test_with_workspace_root(self) -> None:
        from deployment_api.services.user_management import _user_store_path

        mock_cfg = MagicMock()
        mock_cfg.workspace_root = "/workspace"
        with patch(_PATCH_CFG, mock_cfg):
            path = _user_store_path()

        assert str(path) == "/workspace/.user-management/users.json"

    def test_without_workspace_root(self) -> None:
        from deployment_api.services.user_management import _user_store_path

        mock_cfg = MagicMock()
        mock_cfg.workspace_root = None
        with patch(_PATCH_CFG, mock_cfg):
            path = _user_store_path()

        assert str(path).endswith("users.json")
        assert ".unified-trading" in str(path)


class TestLoadUsers:
    def test_file_not_found_returns_empty(self) -> None:
        from deployment_api.services.user_management import _load_users

        with patch("deployment_api.services.user_management._user_store_path") as mock_path:
            p = MagicMock(spec=Path)
            p.exists.return_value = False
            mock_path.return_value = p
            result = _load_users()

        assert result == {}

    def test_file_exists_valid_dict(self) -> None:
        from deployment_api.services.user_management import _load_users

        data = {"u1": {"email": "a@b.com"}}
        with tempfile.NamedTemporaryFile(mode="w", suffix=".json", delete=False) as f:
            json.dump(data, f)
            tmp_path = Path(f.name)

        with patch("deployment_api.services.user_management._user_store_path", return_value=tmp_path):
            result = _load_users()

        assert "u1" in result

    def test_file_non_dict_returns_empty(self) -> None:
        from deployment_api.services.user_management import _load_users

        with tempfile.NamedTemporaryFile(mode="w", suffix=".json", delete=False) as f:
            json.dump([1, 2, 3], f)
            tmp_path = Path(f.name)

        with patch("deployment_api.services.user_management._user_store_path", return_value=tmp_path):
            result = _load_users()

        assert result == {}

    def test_non_dict_values_filtered(self) -> None:
        from deployment_api.services.user_management import _load_users

        data: dict[str, object] = {"u1": {"email": "a@b.com"}, "u2": "not-a-dict", "u3": 42}
        with tempfile.NamedTemporaryFile(mode="w", suffix=".json", delete=False) as f:
            json.dump(data, f)
            tmp_path = Path(f.name)

        with patch("deployment_api.services.user_management._user_store_path", return_value=tmp_path):
            result = _load_users()

        assert "u1" in result
        assert "u2" not in result
        assert "u3" not in result


class TestSaveUsers:
    def test_save_creates_file(self) -> None:
        from deployment_api.services.user_management import _save_users

        with tempfile.TemporaryDirectory() as tmp_dir:
            tmp_path = Path(tmp_dir) / "sub" / "users.json"
            with patch("deployment_api.services.user_management._user_store_path", return_value=tmp_path):
                _save_users({"u1": {"email": "a@b.com"}})
            assert tmp_path.exists()
            with open(tmp_path) as f:
                data = json.load(f)
            assert "u1" in data


class TestDictToProfile:
    def test_full_data_with_dates(self) -> None:
        from deployment_api.services.user_management import _dict_to_profile

        profile = _dict_to_profile(dict(_ADMIN_DATA))
        assert profile.email == "admin@example.com"
        assert profile.role == UserRole.ADMIN
        assert profile.last_login_at is not None
        assert profile.mfa_enabled is True

    def test_no_dates_uses_defaults(self) -> None:
        from deployment_api.services.user_management import _dict_to_profile

        data: dict[str, object] = {
            "user_id": "x",
            "email": "x@x.com",
            "display_name": "X",
            "role": "viewer",
            "is_active": True,
            "mfa_enabled": False,
            "provider": "google",
        }
        profile = _dict_to_profile(data)
        assert profile.last_login_at is None
        assert profile.created_at is not None


class TestProfileToResponse:
    def test_with_last_login(self) -> None:
        from deployment_api.services.user_management import _dict_to_profile, _profile_to_response

        response = _profile_to_response(_dict_to_profile(dict(_ADMIN_DATA)))
        assert response.email == "admin@example.com"
        assert response.last_login_at is not None
        assert response.role == "admin"

    def test_without_last_login(self) -> None:
        from deployment_api.services.user_management import _dict_to_profile, _profile_to_response

        response = _profile_to_response(_dict_to_profile(dict(_VIEWER_DATA)))
        assert response.last_login_at is None


# ── UserManagementService ─────────────────────────────────────────────────────


class TestUserManagementServiceList:
    def test_list_users_empty(self) -> None:
        from deployment_api.services.user_management import UserManagementService

        svc = UserManagementService()
        with patch(_PATCH_LOAD, return_value={}):
            result = svc.list_users()
        assert result == []

    def test_list_users_returns_all(self) -> None:
        from deployment_api.services.user_management import UserManagementService

        store = {"v": dict(_VIEWER_DATA), "a": dict(_ADMIN_DATA)}
        svc = UserManagementService()
        with patch(_PATCH_LOAD, return_value=store):
            result = svc.list_users()
        assert len(result) == 2


class TestUserManagementServiceGetUser:
    def test_get_user_found(self) -> None:
        from deployment_api.services.user_management import UserManagementService

        store = {"viewer_at_example_com": dict(_VIEWER_DATA)}
        svc = UserManagementService()
        with patch(_PATCH_LOAD, return_value=store):
            result = svc.get_user("viewer_at_example_com")
        assert result is not None
        assert result.email == "viewer@example.com"

    def test_get_user_not_found(self) -> None:
        from deployment_api.services.user_management import UserManagementService

        svc = UserManagementService()
        with patch(_PATCH_LOAD, return_value={}):
            result = svc.get_user("nonexistent")
        assert result is None

    def test_get_user_by_email_found(self) -> None:
        from deployment_api.services.user_management import UserManagementService

        store = {"viewer_at_example_com": dict(_VIEWER_DATA)}
        svc = UserManagementService()
        with patch(_PATCH_LOAD, return_value=store):
            result = svc.get_user_by_email("viewer@example.com")
        assert result is not None
        assert result.role == "viewer"

    def test_get_user_by_email_not_found(self) -> None:
        from deployment_api.services.user_management import UserManagementService

        svc = UserManagementService()
        with patch(_PATCH_LOAD, return_value={}):
            result = svc.get_user_by_email("nobody@example.com")
        assert result is None


class TestUserManagementServiceCreate:
    def test_create_user_success(self) -> None:
        from deployment_api.services.user_management import CreateUserRequest, UserManagementService

        svc = UserManagementService()
        with patch(_PATCH_LOAD, return_value={}), patch(_PATCH_SAVE), patch(_PATCH_LOG):
            result = svc.create_user(
                CreateUserRequest(email="new@example.com", display_name="New User", role="operator")
            )
        assert result.email == "new@example.com"
        assert result.role == "operator"

    def test_create_user_duplicate_email_raises(self) -> None:
        from deployment_api.services.user_management import CreateUserRequest, UserManagementService

        store = {"viewer_at_example_com": dict(_VIEWER_DATA)}
        svc = UserManagementService()
        with patch(_PATCH_LOAD, return_value=store), pytest.raises(ValueError, match="already exists"):
            svc.create_user(
                CreateUserRequest(email="viewer@example.com", display_name="Dupe", role="viewer")
            )


class TestUserManagementServiceUpdate:
    def test_update_user_not_found(self) -> None:
        from deployment_api.services.user_management import UpdateUserRequest, UserManagementService

        svc = UserManagementService()
        with patch(_PATCH_LOAD, return_value={}):
            result = svc.update_user("nonexistent", UpdateUserRequest(display_name="X"))
        assert result is None

    def test_update_display_name(self) -> None:
        from deployment_api.services.user_management import UpdateUserRequest, UserManagementService

        store = {"viewer_at_example_com": dict(_VIEWER_DATA)}
        svc = UserManagementService()
        with patch(_PATCH_LOAD, return_value=store), patch(_PATCH_SAVE), patch(_PATCH_LOG):
            result = svc.update_user(
                "viewer_at_example_com",
                UpdateUserRequest(display_name="Updated Name"),
            )
        assert result is not None
        assert result.display_name == "Updated Name"

    def test_update_role_and_active_and_mfa(self) -> None:
        from deployment_api.services.user_management import UpdateUserRequest, UserManagementService

        store = {"viewer_at_example_com": dict(_VIEWER_DATA)}
        svc = UserManagementService()
        with patch(_PATCH_LOAD, return_value=store), patch(_PATCH_SAVE), patch(_PATCH_LOG):
            result = svc.update_user(
                "viewer_at_example_com",
                UpdateUserRequest(role="operator", is_active=False, mfa_enabled=True),
            )
        assert result is not None
        assert result.role == "operator"
        assert result.is_active is False
        assert result.mfa_enabled is True


class TestUserManagementServiceDelete:
    def test_delete_user_not_found(self) -> None:
        from deployment_api.services.user_management import UserManagementService

        svc = UserManagementService()
        with patch(_PATCH_LOAD, return_value={}):
            result = svc.delete_user("nonexistent")
        assert result is False

    def test_delete_user_deactivates(self) -> None:
        from deployment_api.services.user_management import UserManagementService

        store = {"viewer_at_example_com": dict(_VIEWER_DATA)}
        svc = UserManagementService()
        with patch(_PATCH_LOAD, return_value=store), patch(_PATCH_SAVE) as mock_save, patch(_PATCH_LOG):
            result = svc.delete_user("viewer_at_example_com")

        assert result is True
        saved = mock_save.call_args[0][0]
        assert saved["viewer_at_example_com"]["is_active"] is False


class TestUserManagementServiceAssignRole:
    def test_assign_role_not_found(self) -> None:
        from deployment_api.services.user_management import UserManagementService

        svc = UserManagementService()
        with patch(_PATCH_LOAD, return_value={}):
            result = svc.assign_role("nonexistent", "admin")
        assert result is None

    def test_assign_role_success(self) -> None:
        from deployment_api.services.user_management import UserManagementService

        store = {"viewer_at_example_com": dict(_VIEWER_DATA)}
        svc = UserManagementService()
        with patch(_PATCH_LOAD, return_value=store), patch(_PATCH_SAVE), patch(_PATCH_LOG):
            result = svc.assign_role("viewer_at_example_com", "admin")
        assert result is not None
        assert result.role == "admin"


class TestUserManagementServiceRecordLogin:
    def test_record_login_no_matching_user(self) -> None:
        from deployment_api.services.user_management import UserManagementService

        svc = UserManagementService()
        with patch(_PATCH_LOAD, return_value={}), patch(_PATCH_SAVE) as mock_save:
            svc.record_login("nobody@example.com")
        mock_save.assert_not_called()

    def test_record_login_updates_timestamp(self) -> None:
        from deployment_api.services.user_management import UserManagementService

        store = {"viewer_at_example_com": dict(_VIEWER_DATA)}
        svc = UserManagementService()
        with patch(_PATCH_LOAD, return_value=store), patch(_PATCH_SAVE) as mock_save:
            svc.record_login("viewer@example.com")
        mock_save.assert_called_once()
        saved = mock_save.call_args[0][0]
        assert saved["viewer_at_example_com"]["last_login_at"] is not None


class TestUserManagementServiceStaticMethods:
    def test_get_available_roles_contains_all(self) -> None:
        from deployment_api.services.user_management import UserManagementService

        roles = UserManagementService.get_available_roles()
        role_names = {r["role"] for r in roles}
        assert "viewer" in role_names
        assert "operator" in role_names
        assert "admin" in role_names
        assert "super_admin" in role_names

    def test_get_available_permissions_returns_list(self) -> None:
        from deployment_api.services.user_management import UserManagementService

        perms = UserManagementService.get_available_permissions()
        assert len(perms) > 0
        assert all("permission" in p and "domain" in p for p in perms)

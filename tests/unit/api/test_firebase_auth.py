"""Integration tests for Firebase auth middleware — verify_firebase_token + verify_any_auth.

Covers: valid API key, valid Firebase Bearer token, expired token, missing token,
wrong issuer, wrong audience (all via verify_firebase_token ValueError path).
"""

from __future__ import annotations

from unittest.mock import patch

from fastapi import Depends, FastAPI
from fastapi.testclient import TestClient
from unified_trading_library import setup_events

setup_events("deployment-api", "test")

_PATCH_DISABLE_AUTH = "deployment_api.firebase_auth.DISABLE_AUTH"
_PATCH_VERIFY_TOKEN = "deployment_api.firebase_auth._verify_firebase_id_token"
_PATCH_AUTH_CFG = "deployment_api.auth._auth_cfg"


def _make_app_with_firebase() -> FastAPI:
    """Small app that uses verify_firebase_token as dependency."""
    from deployment_api.firebase_auth import verify_firebase_token

    app = FastAPI()

    @app.get("/protected-firebase")
    async def protected(email: str = Depends(verify_firebase_token)) -> dict[str, str]:
        return {"email": email}

    return app


def _make_app_with_any_auth() -> FastAPI:
    """Small app that uses verify_any_auth as dependency."""
    from deployment_api.firebase_auth import verify_any_auth

    app = FastAPI()

    @app.get("/protected-any")
    async def protected(identity: str = Depends(verify_any_auth)) -> dict[str, str]:
        return {"identity": identity}

    return app


class TestVerifyFirebaseToken:
    def test_missing_authorization_header_returns_401(self) -> None:
        with patch(_PATCH_DISABLE_AUTH, False):
            client = TestClient(_make_app_with_firebase())
            r = client.get("/protected-firebase")
        assert r.status_code == 401
        assert "Missing Authorization header" in r.json()["detail"]

    def test_non_bearer_scheme_returns_401(self) -> None:
        with patch(_PATCH_DISABLE_AUTH, False):
            client = TestClient(_make_app_with_firebase())
            r = client.get("/protected-firebase", headers={"Authorization": "Basic abc"})
        assert r.status_code == 401
        assert "Bearer scheme" in r.json()["detail"]

    def test_empty_bearer_token_returns_401(self) -> None:
        with patch(_PATCH_DISABLE_AUTH, False):
            client = TestClient(_make_app_with_firebase())
            r = client.get("/protected-firebase", headers={"Authorization": "Bearer "})
        assert r.status_code == 401
        assert "empty" in r.json()["detail"].lower()

    def test_expired_token_returns_401(self) -> None:
        with (
            patch(_PATCH_DISABLE_AUTH, False),
            patch(_PATCH_VERIFY_TOKEN, side_effect=ValueError("Token_expired at 2026-05-14")),
        ):
            client = TestClient(_make_app_with_firebase())
            r = client.get("/protected-firebase", headers={"Authorization": "Bearer expired.jwt"})
        assert r.status_code == 401
        assert "expired" in r.json()["detail"].lower()

    def test_wrong_issuer_returns_401(self) -> None:
        with (
            patch(_PATCH_DISABLE_AUTH, False),
            patch(
                _PATCH_VERIFY_TOKEN,
                side_effect=ValueError("Token issuer is wrong; expected accounts.google.com"),
            ),
        ):
            client = TestClient(_make_app_with_firebase())
            r = client.get("/protected-firebase", headers={"Authorization": "Bearer wrong.issuer"})
        assert r.status_code == 401
        assert r.json()["detail"].startswith("Invalid Firebase token")

    def test_wrong_audience_returns_401(self) -> None:
        with (
            patch(_PATCH_DISABLE_AUTH, False),
            patch(
                _PATCH_VERIFY_TOKEN,
                side_effect=ValueError("Token audience (aud) mismatch; expected my-project-id"),
            ),
        ):
            client = TestClient(_make_app_with_firebase())
            r = client.get("/protected-firebase", headers={"Authorization": "Bearer bad.aud"})
        assert r.status_code == 401

    def test_valid_token_returns_200_with_email(self) -> None:
        with (
            patch(_PATCH_DISABLE_AUTH, False),
            patch(_PATCH_VERIFY_TOKEN, return_value="user@example.com"),
        ):
            client = TestClient(_make_app_with_firebase())
            r = client.get("/protected-firebase", headers={"Authorization": "Bearer valid.jwt"})
        assert r.status_code == 200
        assert r.json()["email"] == "user@example.com"


class TestVerifyAnyAuth:
    def test_valid_api_key_passes(self) -> None:
        mock_auth_cfg = type("cfg", (), {"api_key": "test-key"})()
        with (
            patch(_PATCH_DISABLE_AUTH, False),
            patch(_PATCH_AUTH_CFG, mock_auth_cfg),
        ):
            client = TestClient(_make_app_with_any_auth())
            r = client.get("/protected-any", headers={"X-API-Key": "test-key"})
        assert r.status_code == 200

    def test_invalid_api_key_returns_401(self) -> None:
        mock_auth_cfg = type("cfg", (), {"api_key": "correct-key"})()
        with (
            patch(_PATCH_DISABLE_AUTH, False),
            patch(_PATCH_AUTH_CFG, mock_auth_cfg),
        ):
            client = TestClient(_make_app_with_any_auth())
            r = client.get("/protected-any", headers={"X-API-Key": "wrong-key"})
        assert r.status_code == 401

    def test_no_auth_returns_401(self) -> None:
        with patch(_PATCH_DISABLE_AUTH, False):
            client = TestClient(_make_app_with_any_auth())
            r = client.get("/protected-any")
        assert r.status_code == 401

    def test_valid_firebase_token_accepted(self) -> None:
        with (
            patch(_PATCH_DISABLE_AUTH, False),
            patch(_PATCH_VERIFY_TOKEN, return_value="firebase@example.com"),
        ):
            client = TestClient(_make_app_with_any_auth())
            r = client.get(
                "/protected-any",
                headers={"Authorization": "Bearer valid.firebase.jwt"},
            )
        assert r.status_code == 200
        assert r.json()["identity"] == "firebase@example.com"

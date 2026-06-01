"""Auth parity tests for treasury endpoints — Firebase token vs API key.

Verifies that both auth paths (X-API-Key and Bearer Firebase token) are accepted
by the treasury route when wired with verify_any_auth, and that missing auth → 401.
"""

from __future__ import annotations

from unittest.mock import patch

from fastapi import APIRouter, Depends, FastAPI
from fastapi.testclient import TestClient
from unified_trading_library import setup_events

from deployment_api.firebase_auth import verify_any_auth
from deployment_api.routes.treasury import router as treasury_router

setup_events("deployment-api", "test")

_PATCH_DISABLE_AUTH = "deployment_api.firebase_auth.DISABLE_AUTH"
_PATCH_VERIFY_TOKEN = "deployment_api.firebase_auth._verify_firebase_id_token"
_PATCH_AUTH_CFG = "deployment_api.auth._auth_cfg"


def _make_authed_treasury_app() -> FastAPI:
    """Mini app with auth wiring for the treasury route."""
    app = FastAPI()
    authed = APIRouter(dependencies=[Depends(verify_any_auth)])
    authed.include_router(treasury_router)
    app.include_router(authed)
    return app


class TestTreasuryAuthParity:
    def test_missing_auth_returns_401(self) -> None:
        with patch(_PATCH_DISABLE_AUTH, False):
            client = TestClient(_make_authed_treasury_app(), raise_server_exceptions=False)
            r = client.get("/clients/demo/treasury")
        assert r.status_code == 401

    def test_api_key_auth_accepted(self) -> None:
        mock_auth_cfg = type("cfg", (), {"api_key": "test-key"})()
        with (
            patch(_PATCH_DISABLE_AUTH, False),
            patch(_PATCH_AUTH_CFG, mock_auth_cfg),
        ):
            client = TestClient(_make_authed_treasury_app(), raise_server_exceptions=False)
            r = client.get("/clients/demo/treasury", headers={"X-API-Key": "test-key"})
        assert r.status_code == 200
        assert r.json()["client_id"] == "demo"

    def test_firebase_token_accepted(self) -> None:
        with (
            patch(_PATCH_DISABLE_AUTH, False),
            patch(_PATCH_VERIFY_TOKEN, return_value="user@example.com"),
        ):
            client = TestClient(_make_authed_treasury_app(), raise_server_exceptions=False)
            r = client.get(
                "/clients/demo/treasury",
                headers={"Authorization": "Bearer valid.firebase.jwt"},
            )
        assert r.status_code == 200
        assert r.json()["client_id"] == "demo"

    def test_wrong_api_key_returns_401(self) -> None:
        mock_auth_cfg = type("cfg", (), {"api_key": "correct-key"})()
        with (
            patch(_PATCH_DISABLE_AUTH, False),
            patch(_PATCH_AUTH_CFG, mock_auth_cfg),
        ):
            client = TestClient(_make_authed_treasury_app(), raise_server_exceptions=False)
            r = client.get("/clients/demo/treasury", headers={"X-API-Key": "wrong-key"})
        assert r.status_code == 401

    def test_subscriptions_api_key_auth_accepted(self) -> None:
        mock_auth_cfg = type("cfg", (), {"api_key": "test-key"})()
        with (
            patch(_PATCH_DISABLE_AUTH, False),
            patch(_PATCH_AUTH_CFG, mock_auth_cfg),
        ):
            client = TestClient(_make_authed_treasury_app(), raise_server_exceptions=False)
            r = client.get("/clients/demo/subscriptions", headers={"X-API-Key": "test-key"})
        assert r.status_code == 200
        assert r.json()["client_id"] == "demo"

    def test_subscriptions_firebase_token_accepted(self) -> None:
        with (
            patch(_PATCH_DISABLE_AUTH, False),
            patch(_PATCH_VERIFY_TOKEN, return_value="user@example.com"),
        ):
            client = TestClient(_make_authed_treasury_app(), raise_server_exceptions=False)
            r = client.get(
                "/clients/demo/subscriptions",
                headers={"Authorization": "Bearer valid.firebase.jwt"},
            )
        assert r.status_code == 200
        assert r.json()["client_id"] == "demo"

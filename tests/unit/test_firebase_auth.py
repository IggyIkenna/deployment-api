"""Unit tests for Firebase token authentication dependency."""

from __future__ import annotations

from unittest.mock import patch

import pytest

import deployment_api.firebase_auth as firebase_auth_module
from deployment_api.firebase_auth import verify_firebase_token


@pytest.mark.asyncio
async def test_mock_mode_bypasses_verification() -> None:
    """In DISABLE_AUTH mode the dependency returns the mock email immediately."""
    with patch.object(firebase_auth_module, "DISABLE_AUTH", True):
        result = await verify_firebase_token(authorization=None)
    assert result == firebase_auth_module._MOCK_EMAIL


@pytest.mark.asyncio
async def test_missing_authorization_header_raises_401() -> None:
    """No Authorization header → HTTP 401."""
    from fastapi import HTTPException

    with patch.object(firebase_auth_module, "DISABLE_AUTH", False):
        with pytest.raises(HTTPException) as exc_info:
            await verify_firebase_token(authorization=None)
    assert exc_info.value.status_code == 401
    assert "Missing Authorization" in exc_info.value.detail


@pytest.mark.asyncio
async def test_non_bearer_scheme_raises_401() -> None:
    """Authorization header with wrong scheme → HTTP 401."""
    from fastapi import HTTPException

    with patch.object(firebase_auth_module, "DISABLE_AUTH", False):
        with pytest.raises(HTTPException) as exc_info:
            await verify_firebase_token(authorization="ApiKey some-key")
    assert exc_info.value.status_code == 401
    assert "Bearer" in exc_info.value.detail


@pytest.mark.asyncio
async def test_valid_firebase_token_returns_email() -> None:
    """A valid Firebase token → email is returned from verified payload."""
    with (
        patch.object(firebase_auth_module, "DISABLE_AUTH", False),
        patch.object(
            firebase_auth_module, "_verify_firebase_id_token", return_value="user@example.com"
        ),
    ):
        result = await verify_firebase_token(authorization="Bearer valid.jwt.token")
    assert result == "user@example.com"


@pytest.mark.asyncio
async def test_expired_firebase_token_raises_401() -> None:
    """An expired token raises HTTP 401 with 'expired' in the detail."""
    from fastapi import HTTPException

    with (
        patch.object(firebase_auth_module, "DISABLE_AUTH", False),
        patch.object(
            firebase_auth_module,
            "_verify_firebase_id_token",
            side_effect=ValueError("Token expired: token_expired at 2026-05-14"),
        ),
    ):
        with pytest.raises(HTTPException) as exc_info:
            await verify_firebase_token(authorization="Bearer expired.jwt.token")
    assert exc_info.value.status_code == 401
    assert "expired" in exc_info.value.detail.lower()


@pytest.mark.asyncio
async def test_invalid_firebase_token_raises_401() -> None:
    """A token with invalid signature raises HTTP 401."""
    from fastapi import HTTPException

    with (
        patch.object(firebase_auth_module, "DISABLE_AUTH", False),
        patch.object(
            firebase_auth_module,
            "_verify_firebase_id_token",
            side_effect=ValueError("Invalid token signature"),
        ),
    ):
        with pytest.raises(HTTPException) as exc_info:
            await verify_firebase_token(authorization="Bearer invalid.signature.token")
    assert exc_info.value.status_code == 401
    assert "Invalid Firebase token" in exc_info.value.detail

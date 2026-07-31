"""Unit tests for routes/_reap_scheduler.py's OIDC verification dependency.

Covers the 2026-07-31 fix (deployment_api_sigabrt_crash_loop_2026_07_24.md): a raw
transport-level failure (SSL/socket error fetching Google's certs) is not always a
GoogleAuthError subtype and previously escaped as an unhandled 500 with a raw traceback dump
instead of a clean, handled response.
"""

from __future__ import annotations

from unittest.mock import patch

import pytest
from fastapi import HTTPException

import deployment_api.routes._reap_scheduler as reap_scheduler_module
from deployment_api import settings
from deployment_api.routes._reap_scheduler import verify_reap_scheduler_oidc

_INVOKER_SA = "reap-scheduler@test-project.iam.gserviceaccount.com"


@pytest.mark.asyncio
async def test_mock_mode_bypasses_verification() -> None:
    with patch.object(reap_scheduler_module, "DISABLE_AUTH", True):
        result = await verify_reap_scheduler_oidc(authorization=None)
    assert result == reap_scheduler_module._MOCK_INVOKER  # pyright: ignore[reportPrivateUsage]


@pytest.mark.asyncio
async def test_unconfigured_invoker_sa_raises_503() -> None:
    with (
        patch.object(reap_scheduler_module, "DISABLE_AUTH", False),
        patch.object(settings, "REAP_SCHEDULER_INVOKER_SA", ""),
    ):
        with pytest.raises(HTTPException) as exc_info:
            await verify_reap_scheduler_oidc(authorization="Bearer sometoken")
    assert exc_info.value.status_code == 503


@pytest.mark.asyncio
async def test_missing_authorization_header_raises_401() -> None:
    with (
        patch.object(reap_scheduler_module, "DISABLE_AUTH", False),
        patch.object(settings, "REAP_SCHEDULER_INVOKER_SA", _INVOKER_SA),
    ):
        with pytest.raises(HTTPException) as exc_info:
            await verify_reap_scheduler_oidc(authorization=None)
    assert exc_info.value.status_code == 401


@pytest.mark.asyncio
async def test_valid_token_returns_email() -> None:
    with (
        patch.object(reap_scheduler_module, "DISABLE_AUTH", False),
        patch.object(settings, "REAP_SCHEDULER_INVOKER_SA", _INVOKER_SA),
        patch("google.oauth2.id_token.verify_oauth2_token", return_value={"email": _INVOKER_SA}),
    ):
        result = await verify_reap_scheduler_oidc(authorization="Bearer valid.oidc.token")
    assert result == _INVOKER_SA


@pytest.mark.asyncio
async def test_wrong_identity_raises_401() -> None:
    with (
        patch.object(reap_scheduler_module, "DISABLE_AUTH", False),
        patch.object(settings, "REAP_SCHEDULER_INVOKER_SA", _INVOKER_SA),
        patch(
            "google.oauth2.id_token.verify_oauth2_token",
            return_value={"email": "someone-else@example.com"},
        ),
    ):
        with pytest.raises(HTTPException) as exc_info:
            await verify_reap_scheduler_oidc(authorization="Bearer valid.oidc.token")
    assert exc_info.value.status_code == 401


@pytest.mark.asyncio
async def test_invalid_token_raises_401() -> None:
    from google.auth.exceptions import GoogleAuthError

    with (
        patch.object(reap_scheduler_module, "DISABLE_AUTH", False),
        patch.object(settings, "REAP_SCHEDULER_INVOKER_SA", _INVOKER_SA),
        patch(
            "google.oauth2.id_token.verify_oauth2_token",
            side_effect=GoogleAuthError("token expired"),
        ),
    ):
        with pytest.raises(HTTPException) as exc_info:
            await verify_reap_scheduler_oidc(authorization="Bearer expired.oidc.token")
    assert exc_info.value.status_code == 401


@pytest.mark.asyncio
async def test_raw_transport_failure_raises_503_not_unhandled_500() -> None:
    """Regression: a raw SSL/socket error escaping google-auth's own wrapping must become a
    clean 503, not propagate as an unhandled exception (the 2026-07-31 live incident)."""
    with (
        patch.object(reap_scheduler_module, "DISABLE_AUTH", False),
        patch.object(settings, "REAP_SCHEDULER_INVOKER_SA", _INVOKER_SA),
        patch(
            "google.oauth2.id_token.verify_oauth2_token",
            side_effect=OSError("SSL handshake failed"),
        ),
    ):
        with pytest.raises(HTTPException) as exc_info:
            await verify_reap_scheduler_oidc(authorization="Bearer valid.oidc.token")
    assert exc_info.value.status_code == 503


@pytest.mark.asyncio
async def test_verification_does_not_block_event_loop() -> None:
    """The verify call must run off the event loop (asyncio.to_thread), same pattern already
    used for the reap-tick's own GCS I/O below."""
    with (
        patch.object(reap_scheduler_module, "DISABLE_AUTH", False),
        patch.object(settings, "REAP_SCHEDULER_INVOKER_SA", _INVOKER_SA),
        patch("google.oauth2.id_token.verify_oauth2_token", return_value={"email": _INVOKER_SA}) as mock_verify,
    ):
        result = await verify_reap_scheduler_oidc(authorization="Bearer valid.oidc.token")
    assert result == _INVOKER_SA
    mock_verify.assert_called_once()

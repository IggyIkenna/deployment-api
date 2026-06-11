"""Firebase token verification for deployment-api.

Accepts a Firebase ID token from the Authorization: Bearer header.
Tokens are forwarded from UTS-UI / Deployment-UI after the user authenticates
with Firebase.

``verify_firebase_token``: dependency for routes that require a Firebase token.
``verify_any_auth``: accepts EITHER a valid X-API-Key OR a valid Firebase Bearer
token — use on routes that are called by both server-to-server (API key) and
human-from-UI (Firebase) clients.
"""

from __future__ import annotations

import logging

from fastapi import Header, HTTPException

from deployment_api.auth import DISABLE_AUTH

logger = logging.getLogger(__name__)

_MOCK_EMAIL = "mock-firebase-user@example.com"


def _verify_firebase_id_token(token: str) -> str:
    """Verify a Firebase ID token and return the subject email.

    Uses google-auth's id_token module to fetch Firebase public keys and verify.
    Raises ValueError on expired / invalid tokens.
    Raises google.auth.exceptions.TransportError on network failure.
    """
    # google auth SDK boundary — lazy so mock-mode/DISABLE_AUTH startup never loads it
    from google.auth.transport.requests import (  # noqa: imports-inside-functions
        Request as AuthRequest,  # pyright: ignore[reportMissingTypeStubs]
    )

    # google auth SDK boundary — lazy so mock-mode/DISABLE_AUTH startup never loads it
    from google.oauth2 import (
        id_token as google_id_token,  # noqa: imports-inside-functions # pyright: ignore[reportMissingTypeStubs]
    )

    raw = google_id_token.verify_firebase_token(  # pyright: ignore[reportUnknownMemberType]
        token, AuthRequest()
    )
    payload: dict[str, object] = dict(raw)  # pyright: ignore[reportUnknownArgumentType]
    email = payload.get("email") or payload.get("sub") or ""
    return str(email)


async def verify_any_auth(
    x_api_key: str | None = Header(default=None, alias="X-API-Key"),
    authorization: str | None = Header(default=None),
) -> str:
    """FastAPI dependency: accept either an API key or a Firebase Bearer token.

    Order of precedence:
    1. X-API-Key header → validated against the configured API key
    2. Authorization: Bearer <token> → validated as a Firebase ID token
    3. Neither → HTTP 401

    Use on routes accessible by both server-to-server (API key) and
    human-from-UI (Firebase) callers.
    """
    if DISABLE_AUTH:
        return _MOCK_EMAIL

    from deployment_api.auth import _auth_cfg  # pyright: ignore[reportPrivateUsage]

    if x_api_key:
        expected = _auth_cfg.api_key
        if expected and x_api_key == expected:
            return x_api_key
        raise HTTPException(status_code=401, detail="Invalid API key")

    if authorization:
        return await verify_firebase_token(authorization=authorization)

    raise HTTPException(
        status_code=401,
        detail=("Authentication required: provide X-API-Key or Authorization: Bearer <firebase-token>"),
    )


async def verify_firebase_token(
    authorization: str | None = Header(default=None),
) -> str:
    """FastAPI dependency: verify the Bearer token from the Authorization header.

    Returns the authenticated user's email.
    Raises HTTP 401 for missing / invalid / expired tokens.
    Raises HTTP 503 on Firebase network unavailability.
    """
    if DISABLE_AUTH:
        return _MOCK_EMAIL

    if not authorization:
        raise HTTPException(
            status_code=401,
            detail="Missing Authorization header — expected 'Bearer <firebase-id-token>'",
        )
    if not authorization.lower().startswith("bearer "):
        raise HTTPException(
            status_code=401,
            detail="Authorization header must use the Bearer scheme",
        )
    token = authorization[len("bearer ") :].strip()
    if not token:
        raise HTTPException(status_code=401, detail="Bearer token is empty")

    try:
        # google auth SDK boundary — lazy so mock-mode/DISABLE_AUTH startup never loads it
        from google.auth.exceptions import (
            TransportError,  # noqa: imports-inside-functions # pyright: ignore[reportMissingTypeStubs]
        )

        try:
            email = _verify_firebase_id_token(token)
            logger.info("Firebase auth successful: user=%s", email)
            return email
        except TransportError as exc:  # pyright: ignore[reportUnknownVariableType]
            logger.warning("Firebase auth: transport error: %s", exc)
            raise HTTPException(
                status_code=503,
                detail="Firebase token verification unavailable — network error",
            ) from None
    except HTTPException:
        raise
    except ValueError as exc:
        msg = str(exc)
        logger.warning("Firebase auth: invalid token: %s", msg)
        if "expired" in msg.lower() or "token_expired" in msg.lower():
            raise HTTPException(status_code=401, detail="Firebase token has expired") from None
        raise HTTPException(status_code=401, detail=f"Invalid Firebase token: {msg}") from None

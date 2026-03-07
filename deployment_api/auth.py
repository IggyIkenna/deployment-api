"""API key authentication for deployment-api."""

from __future__ import annotations

import logging

from fastapi import HTTPException, Security
from fastapi.security import APIKeyHeader
from unified_config_interface import UnifiedCloudConfig
from unified_events_interface import log_event

logger = logging.getLogger(__name__)

# --- Production guard for DISABLE_AUTH ---
_auth_cfg = UnifiedCloudConfig()
_disable_auth_raw: bool = _auth_cfg.disable_auth
_environment: str = _auth_cfg.environment
if _disable_auth_raw and _environment == "production":
    logging.getLogger(__name__).critical(
        "DISABLE_AUTH=true is forbidden in production. Auth remains ENABLED."
    )
    _disable_auth_guarded: bool = False
else:
    _disable_auth_guarded = _disable_auth_raw
DISABLE_AUTH: bool = _disable_auth_guarded

api_key_header = APIKeyHeader(name="X-API-Key", auto_error=False)


async def verify_api_key(
    api_key: str | None = Security(api_key_header),
) -> str:
    """Validate the X-API-Key header against the API_KEY env var.

    Set DISABLE_AUTH=true for local development (defaults to false).
    """
    if DISABLE_AUTH:
        return "dev-mode"
    if not api_key:
        log_event(
            "AUTH_FAILURE",
            severity="WARNING",
            details={"auth_type": "api_key", "reason": "missing_key"},
        )
        raise HTTPException(status_code=401, detail="Missing API key")
    expected_key = _auth_cfg.api_key
    if not expected_key or api_key != expected_key:
        log_event(
            "AUTH_FAILURE",
            severity="WARNING",
            details={"auth_type": "api_key", "reason": "invalid_key"},
        )
        raise HTTPException(status_code=401, detail="Invalid API key")
    logger.info("Authentication successful: auth_type=api_key")
    return api_key

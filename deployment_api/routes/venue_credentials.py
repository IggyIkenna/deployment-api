"""GET /api/venue-credentials — per-venue API key name + status (never the key value).

Probes each configured venue credential from Secret Manager, then calls a lightweight
venue probe to determine whether the key is active, expired, missing, or errored.

Statuses:
  active   — key present and probe confirms it works
  expired  — key present but probe shows it is expired/revoked
  missing  — secret not found in Secret Manager
  error    — unexpected probe failure (network, auth, etc.)
  mock     — running in mock mode (status is simulated)
"""

import logging
from datetime import UTC, datetime

import httpx
from fastapi import APIRouter
from pydantic import BaseModel

from deployment_api.deployment_api_config import DeploymentApiConfig

logger = logging.getLogger(__name__)
router = APIRouter()
_cfg = DeploymentApiConfig()

_TARDIS_KEY_INFO_URL = "https://api.tardis.dev/api-key-info"
_PROBE_TIMEOUT = 5.0


class VenueCredentialStatus(BaseModel):  # CORRECT-LOCAL
    """Status of a single venue API credential. Key value is NEVER included."""

    name: str
    venue: str
    status: str
    probe_detail: str | None
    checked_at: str


async def _probe_tardis(api_key: str) -> tuple[str, str | None]:
    """Return (status, detail) by calling Tardis api-key-info endpoint."""
    try:
        async with httpx.AsyncClient(timeout=_PROBE_TIMEOUT) as client:
            resp = await client.get(
                _TARDIS_KEY_INFO_URL,
                headers={"Authorization": f"Bearer {api_key}"},
            )
        if resp.status_code == 200:
            data = resp.json()
            if isinstance(data, list) and len(data) == 0:
                return "expired", "api-key-info returned [] — no datasets accessible (key expired or unentitled)"
            count = len(data) if isinstance(data, list) else "?"
            return "active", f"{count} dataset(s) accessible"
        if resp.status_code in (401, 403):
            return "expired", f"HTTP {resp.status_code} from api-key-info — key rejected"
        return "error", f"Unexpected HTTP {resp.status_code} from api-key-info"
    except httpx.TimeoutException:
        return "error", f"Probe timed out after {_PROBE_TIMEOUT}s"
    except Exception as exc:
        return "error", str(exc)[:150]


async def _collect() -> list[VenueCredentialStatus]:
    now = datetime.now(UTC).isoformat()

    if _cfg.is_mock_mode():
        return [
            VenueCredentialStatus(
                name="tardis-api-key",
                venue="tardis",
                status="expired",
                probe_detail="mock mode — status simulated as EXPIRED (mirrors live state 2026-05-29)",
                checked_at=now,
            )
        ]

    results: list[VenueCredentialStatus] = []

    # ── Tardis ──────────────────────────────────────────────────────────────
    try:
        from unified_trading_library import get_secret_client

        from deployment_api.auth import auth_cfg as _auth_cfg

        project_id: str = _auth_cfg.gcp_project_id or ""
        api_key: str | None = get_secret_client(project_id).get_secret("tardis-api-key")
        if not api_key:
            results.append(
                VenueCredentialStatus(
                    name="tardis-api-key",
                    venue="tardis",
                    status="missing",
                    probe_detail="Secret 'tardis-api-key' found in SM but value is empty",
                    checked_at=now,
                )
            )
        else:
            status, detail = await _probe_tardis(api_key)
            results.append(
                VenueCredentialStatus(
                    name="tardis-api-key",
                    venue="tardis",
                    status=status,
                    probe_detail=detail,
                    checked_at=now,
                )
            )
    except Exception as exc:
        msg = str(exc)
        if any(kw in msg.lower() for kw in ("not found", "does not exist", "no secret")):
            entry_status, entry_detail = "missing", "Secret 'tardis-api-key' not in Secret Manager"
        else:
            entry_status, entry_detail = "error", msg[:200]
        results.append(
            VenueCredentialStatus(
                name="tardis-api-key",
                venue="tardis",
                status=entry_status,
                probe_detail=entry_detail,
                checked_at=now,
            )
        )

    return results


@router.get("/api/venue-credentials", response_model=list[VenueCredentialStatus])
async def get_venue_credentials() -> list[VenueCredentialStatus]:
    """Return per-venue API key status (name + status). Key values are never returned."""
    return await _collect()

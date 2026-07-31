# Epic: observability_master
# Lifecycle: permanent
"""Cloud Scheduler-triggered synchronous reap tick — sidesteps the CPU-throttling starvation
that stalls the background asyncio reap loop between requests (Cloud Run's default CPU
allocation is request-time-only; a periodic `asyncio.sleep`-driven loop can stall for
arbitrarily long stretches when the container isn't actively serving traffic).

Operator ruling on `BLK-d5db60a5` (2026-07-25, deployment_registry_reaper_not_draining_stale_entries_2026_07_24.md):
Option B over `--no-cpu-throttling` — this endpoint gets guaranteed request-context CPU for the
reap, at zero extra ongoing billing (vs. committing the service to always-on 4-vCPU cost).

Auth: Google-signed OIDC ID token (Cloud Scheduler's `--oidc-service-account-email`), NOT
`verify_any_auth` (X-API-Key/Firebase — the scheme every OTHER internal route in this service
uses, e.g. `routes/data_status/_rollup.py`'s `rollup-run`, which is the wrong scheme for a
machine-to-machine Cloud Scheduler caller). The access-control decision is the token's `email`
claim matching `settings.REAP_SCHEDULER_INVOKER_SA` exactly — forging that claim requires
forging Google's signature (infeasible), so this is the load-bearing check; `audience` is left
unchecked (`verify_oauth2_token(..., audience=None)`) rather than hand-rolling a fragile
exact-URL-match, since the email check alone already scopes the caller to one specific identity.
"""

from __future__ import annotations

import logging

from fastapi import APIRouter, Depends, Header, HTTPException

from deployment_api import settings
from deployment_api.auth import DISABLE_AUTH

logger = logging.getLogger(__name__)

router = APIRouter()

_MOCK_INVOKER = "mock-reap-scheduler@example.com"


async def verify_reap_scheduler_oidc(
    authorization: str | None = Header(default=None),
) -> str:
    """FastAPI dependency: verify a Google-signed OIDC token asserting the configured invoker SA.

    Raises HTTP 401 for a missing/invalid/expired token or one asserting the wrong identity.
    Raises HTTP 503 if `REAP_SCHEDULER_INVOKER_SA` isn't configured (fail closed, never accept
    an unconfigured caller).
    """
    if DISABLE_AUTH:
        return _MOCK_INVOKER

    if not settings.REAP_SCHEDULER_INVOKER_SA:
        raise HTTPException(status_code=503, detail="REAP_SCHEDULER_INVOKER_SA not configured")

    if not authorization or not authorization.lower().startswith("bearer "):
        raise HTTPException(
            status_code=401,
            detail="Missing Authorization header — expected 'Bearer <oidc-id-token>'",
        )
    token = authorization[len("bearer ") :].strip()
    if not token:
        raise HTTPException(status_code=401, detail="Bearer token is empty")

    # google auth SDK boundary — lazy so mock-mode/DISABLE_AUTH startup never loads it
    import asyncio

    from google.auth.exceptions import GoogleAuthError  # noqa: imports-inside-functions
    from google.auth.transport.requests import (  # noqa: imports-inside-functions
        Request as AuthRequest,  # pyright: ignore[reportMissingTypeStubs]
    )
    from google.oauth2 import (
        id_token as google_id_token,  # noqa: imports-inside-functions # pyright: ignore[reportMissingTypeStubs]
    )

    def _verify() -> dict[str, object]:
        return google_id_token.verify_oauth2_token(  # pyright: ignore[reportUnknownMemberType]
            token, AuthRequest(), audience=None
        )

    try:
        # Run in a thread: this makes a real outbound HTTPS call (fetching Google's OAuth2
        # certs) — synchronous, so calling it directly here would block the event loop for the
        # life of the request, same class of bug as the GCS I/O below (already thread-wrapped).
        claims: dict[str, object] = await asyncio.to_thread(_verify)
    except (GoogleAuthError, ValueError) as exc:
        logger.warning("reap-scheduler OIDC verification failed: %s", exc)
        raise HTTPException(status_code=401, detail="Invalid OIDC token") from None
    except Exception as exc:
        # A raw transport-level failure (e.g. an SSL/socket error while fetching Google's certs)
        # is NOT a requests.exceptions.RequestException subtype in every case and can therefore
        # escape google-auth's own GoogleAuthError wrapping — confirmed live 2026-07-31: this
        # exact call produced an unhandled 500 with a raw uvicorn/urllib3 traceback instead of a
        # clean 401 (deployment_api_sigabrt_crash_loop_2026_07_24.md). Treat it as transient
        # (503, Cloud Scheduler retries) rather than "bad token" (401) and log it as ONE
        # structured record instead of letting it propagate to the ASGI layer's raw dump.
        logger.exception("reap-scheduler OIDC verification transport failure: %s", exc)
        raise HTTPException(status_code=503, detail="OIDC verification transiently unavailable") from None

    email = str(claims.get("email") or "")
    if email != settings.REAP_SCHEDULER_INVOKER_SA:
        logger.warning("reap-scheduler OIDC token asserted unexpected identity: %s", email)
        raise HTTPException(status_code=401, detail="Token does not match the configured invoker")

    return email


@router.post("/internal/reap-tick")
async def run_reap_tick(_invoker: str = Depends(verify_reap_scheduler_oidc)) -> dict[str, object]:
    """Synchronously archive stale `deployments/active/` entries — Cloud Scheduler's tick.

    Calls the SAME `SyncService.reap_stale_deployments` the background loop already calls
    (already idempotent/bounded — safe under overlapping or manual invocations). Synchronous:
    the caller is Cloud Scheduler with a long attempt-deadline, not a UI request. Runs the
    blocking GCS I/O in a thread so it doesn't block the event loop.
    """
    import asyncio

    from deployment_api.services.sync_service import SyncService

    reaped = await asyncio.to_thread(SyncService().reap_stale_deployments)
    return {"status": "ok", "reaped_count": reaped}

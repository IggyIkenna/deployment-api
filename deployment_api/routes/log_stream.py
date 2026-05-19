"""SSE log stream endpoint — Phase C.5 of deployment_ui_lifecycle_tabs_2026_05_08.md.

Route: GET /api/logs/stream/{target_ref}

Fans out per lifecycle class:
- Backfill / experiment / scheduled VMs: polls GCS events bucket (same source as
  vm_events.py + vm_events_ws.py) and yields each VMLifecycleEvent as SSE.
- Long-lived live clusters: 501 scaffold — Cloud Run / GKE log integration is
  post-cutover scope (successor plan: deployment_ui_lifecycle_tabs Phase E.2).
"""

from __future__ import annotations

import asyncio
import logging

from fastapi import APIRouter, HTTPException
from sse_starlette.sse import EventSourceResponse
from unified_trading_library import get_storage_client

from deployment_api.deployment_api_config import DeploymentApiConfig
from deployment_api.routes.vm_events import _collect_blob_names  # pyright: ignore[reportPrivateUsage]
from deployment_api.routes.vm_events import _event_to_log_line  # pyright: ignore[reportPrivateUsage]
from deployment_api.routes.vm_events import _fetch_and_parse_event  # pyright: ignore[reportPrivateUsage]
from deployment_api.routes.vm_events import _infer_service_from_vm_name  # pyright: ignore[reportPrivateUsage]
from deployment_api.routes.vm_events import _resolve_events_bucket  # pyright: ignore[reportPrivateUsage]

router = APIRouter()
logger = logging.getLogger(__name__)

_cfg = DeploymentApiConfig()
_POLL_INTERVAL_SECS = 5.0
_HEARTBEAT_INTERVAL_SECS = 30.0

_LIVE_CLUSTER_PREFIXES = frozenset(
    {
        "strategy-live",
        "strategy-paper",
        "execution-service",
        "risk-and-exposure",
        "position-balance",
        "alerting-service",
    }
)


def _is_live_cluster_ref(target_ref: str) -> bool:
    return any(target_ref.startswith(p) for p in _LIVE_CLUSTER_PREFIXES)


async def _vm_sse_generator(target_ref: str):
    """Poll GCS events bucket and yield SSE dicts for each new VMLifecycleEvent."""
    from datetime import UTC, datetime

    try:
        service = _infer_service_from_vm_name(target_ref)
    except HTTPException:
        service = target_ref  # fallback — let GCS listing surface 0 blobs gracefully

    bucket = _resolve_events_bucket()
    storage = get_storage_client(project_id=_cfg.gcp_project_id)
    last_seen_blob: str | None = None
    time_since_heartbeat = 0.0

    while True:
        now = datetime.now(UTC)
        date = now.strftime("%Y-%m-%d")
        current_hour = now.hour

        blobs, _ = _collect_blob_names(storage, bucket, service, date, target_ref, 0, current_hour)
        new_blobs = [(h, n) for h, n in blobs if last_seen_blob is None or n > last_seen_blob]

        for _, blob_name in new_blobs:
            event = _fetch_and_parse_event(storage, bucket, blob_name)
            if event is not None:
                line = _event_to_log_line(event)
                yield {
                    "event": "vm_event",
                    "data": line.model_dump_json(),
                }
            last_seen_blob = blob_name

        await asyncio.sleep(_POLL_INTERVAL_SECS)
        time_since_heartbeat += _POLL_INTERVAL_SECS
        if time_since_heartbeat >= _HEARTBEAT_INTERVAL_SECS:
            yield {"event": "heartbeat", "data": "ping"}
            time_since_heartbeat = 0.0


async def _mock_sse_generator(target_ref: str):
    """Yield 3 synthetic events then a done signal (mock mode only)."""
    from datetime import UTC, datetime

    now = datetime.now(UTC).isoformat()
    for evt_type in ("STARTED", "DATA_BROADCAST", "STOPPED"):
        yield {
            "event": "vm_event",
            "data": f'{{"event": "{evt_type}", "service": "mock", "timestamp": "{now}", "vm_name": "{target_ref}"}}',
        }
        await asyncio.sleep(0.01)
    yield {"event": "done", "data": "stream_complete"}


@router.get("/api/logs/stream/{target_ref}")
async def stream_logs(target_ref: str) -> EventSourceResponse:
    """Stream lifecycle events for a target as SSE.

    For VM-based targets (backfill / experiment / scheduled), polls the GCS events
    bucket. For long-lived live clusters, returns 501 (Cloud Run log integration
    is post-cutover scope).
    """
    if _is_live_cluster_ref(target_ref):
        raise HTTPException(
            status_code=501,
            detail={
                "message": f"Live-cluster log streaming for {target_ref!r} not yet implemented",
                "reason": "Cloud Run / GKE log tail is post-cutover scope (Phase E.2)",
                "target_ref": target_ref,
            },
        )

    generator = _mock_sse_generator(target_ref) if _cfg.is_mock_mode() else _vm_sse_generator(target_ref)
    return EventSourceResponse(generator)

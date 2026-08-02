"""SSE log stream endpoint — Phase C.5 of deployment_ui_lifecycle_tabs_2026_05_08.md.

Route: GET /api/logs/stream/{target_ref}

Fans out per lifecycle class — BOTH branches yield the IDENTICAL SSE envelope
(``{"event": "vm_event", "data": <VmLogLine JSON>}`` + heartbeats) so the
deployment-ui ``useDeployEventStream`` / ``StreamingLogsPanel`` hook is unchanged
across target kinds:

- Backfill / experiment / scheduled VMs: polls the GCS events bucket (same source as
  vm_events.py + vm_events_ws.py), keyed by the VM name.
- Long-lived live clusters (strategy-live / strategy-paper / execution-service /
  risk-and-exposure / position-balance / alerting-service): polls the SAME
  ``{pid}-events`` GCS bucket, keyed by the cluster's SERVICE name. Live services
  self-emit lifecycle + structured events there via ``ServiceBootstrap`` /
  ``log_event`` (UTL feature_service_base), so the SAME GCS-events poller serves them
  — no direct ``google.cloud.logging`` dependency (banned; UCI has no log-READER
  accessor, only an emit-only ``GCPLoggingProvider``). The Cloud Run execution
  ``log_uri`` deep-link remains available on the inventory row for the raw console.

  Closes the prior 501 scaffold (was: "Cloud Run / GKE log tail is post-cutover").
  Plan: unified_deployment_health_cockpit_2026_06_23.md Phase 3.
"""

from __future__ import annotations

import asyncio
import logging
from datetime import UTC, datetime

from fastapi import APIRouter, HTTPException
from sse_starlette.sse import EventSourceResponse
from unified_trading_library import get_storage_client

from deployment_api.deployment_api_config import DeploymentApiConfig
from deployment_api.routes.vm_events import (
    _collect_blob_names,  # pyright: ignore[reportPrivateUsage]
    _event_to_log_line,  # pyright: ignore[reportPrivateUsage]
    _fetch_and_parse_event,  # pyright: ignore[reportPrivateUsage]
    _infer_service_from_vm_name,  # pyright: ignore[reportPrivateUsage]
    _resolve_events_bucket,  # pyright: ignore[reportPrivateUsage]
)

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


async def _live_cluster_sse_generator(target_ref: str):
    """Poll the GCS events bucket for a long-lived LIVE/PAPER cluster's self-emitted events.

    Live clusters (Cloud Run services / long-lived VMs) self-emit lifecycle + structured
    events to the SAME ``{pid}-events`` bucket via ``ServiceBootstrap`` / ``log_event``,
    partitioned by SERVICE name. For a live cluster the ``target_ref`` IS the service name,
    so we key both the service partition and the per-shard name on ``target_ref`` and yield
    the IDENTICAL ``VmLogLine`` SSE envelope as the VM path — the UI hook is unchanged.

    Cloud-agnostic: reads only via the unified ``get_storage_client`` (no
    ``google.cloud.logging``). Shard-level isolation: a per-blob fetch/parse failure is
    skipped inside ``_fetch_and_parse_event`` (logged), never crashing the stream.
    """
    bucket = _resolve_events_bucket()
    storage = get_storage_client(project_id=_cfg.gcp_project_id)
    last_seen_blob: str | None = None
    time_since_heartbeat = 0.0

    while True:
        now = datetime.now(UTC)
        date = now.strftime("%Y-%m-%d")
        current_hour = now.hour

        # service == target_ref (the live cluster IS its own service partition).
        blobs, _ = _collect_blob_names(storage, bucket, target_ref, date, target_ref, 0, current_hour)
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
    """Stream lifecycle + structured log events for a target as SSE.

    Fans out by target kind, BOTH yielding the identical SSE envelope so the UI hook is
    unchanged:

    * VM-based targets (backfill / experiment / scheduled) → poll the GCS events bucket
      keyed by the VM name (``_vm_sse_generator``).
    * Long-lived LIVE/PAPER clusters (Cloud Run services / long-lived VMs) → poll the SAME
      GCS events bucket keyed by the cluster's SERVICE name (``_live_cluster_sse_generator``);
      live services self-emit there via ``ServiceBootstrap`` / ``log_event``. (Closes the
      former 501 — no direct ``google.cloud.logging`` dependency.)

    In mock mode both kinds yield synthetic lines via ``_mock_sse_generator``.
    """
    if _cfg.is_mock_mode():
        return EventSourceResponse(_mock_sse_generator(target_ref))
    if _is_live_cluster_ref(target_ref):
        return EventSourceResponse(_live_cluster_sse_generator(target_ref))
    return EventSourceResponse(_vm_sse_generator(target_ref))

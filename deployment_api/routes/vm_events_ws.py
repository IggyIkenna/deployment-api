"""WebSocket endpoint for live VM event streaming.

Route: /ws/vm/{vm_name}/events
Polls the GCS events bucket every 5 s and forwards new VMLifecycleEvents as JSON.

Designed to replace polling on GET /api/vm/events for live operator consoles.
"""

from __future__ import annotations

import asyncio
import contextlib
import logging
from datetime import UTC, datetime
from typing import cast

from fastapi import APIRouter, HTTPException, Query, WebSocket, WebSocketDisconnect
from unified_api_contracts.internal import VMLifecycleEvent
from unified_trading_library import get_storage_client

from deployment_api.deployment_api_config import DeploymentApiConfig
from deployment_api.routes.vm_events import (  # pyright: ignore[reportPrivateUsage]
    _collect_blob_names,
    _fetch_and_parse_event,
    _infer_service_from_vm_name,
    _resolve_events_bucket,
)

router = APIRouter()
logger = logging.getLogger(__name__)

_cfg = DeploymentApiConfig()
_POLL_INTERVAL_SECS = 5.0


@router.websocket("/ws/vm/{vm_name}/events")
async def ws_vm_events(
    websocket: WebSocket,
    vm_name: str,
    service: str | None = Query(default=None),
) -> None:
    """Stream VM lifecycle events from the GCS events bucket.

    In mock mode: sends 3 synthetic events then closes.
    In live mode: polls GCS every 5 s and pushes new events as JSON; terminates on disconnect.
    """
    await websocket.accept()

    if _cfg.is_mock_mode():
        await _stream_mock_events(websocket, vm_name)
        return

    try:
        resolved_service = service or _infer_service_from_vm_name(vm_name)
    except HTTPException as exc:
        # Extract message from detail if it's a dict, otherwise convert to string
        detail = exc.detail["message"] if isinstance(exc.detail, dict) and "message" in exc.detail else str(exc.detail)
        await websocket.send_json({"error": detail, "code": "SERVICE_INFERENCE_FAILED"})
        await websocket.close(code=4000)
        return

    bucket = _resolve_events_bucket()
    storage = get_storage_client(project_id=_cfg.gcp_project_id)
    last_seen_blob: str | None = None

    try:
        while True:
            now = datetime.now(UTC)
            date = now.strftime("%Y-%m-%d")
            current_hour = now.hour

            all_blobs, _ = _collect_blob_names(storage, bucket, resolved_service, date, vm_name, 0, current_hour)

            new_blobs = [(h, n) for h, n in all_blobs if last_seen_blob is None or n > last_seen_blob]

            for _, blob_name in new_blobs:
                parsed = _fetch_and_parse_event(storage, bucket, blob_name)
                if parsed is not None:
                    await websocket.send_json(_event_to_dict(parsed))
                last_seen_blob = blob_name

            await asyncio.sleep(_POLL_INTERVAL_SECS)

    except WebSocketDisconnect:
        logger.info("WS client disconnected: vm_name=%s", vm_name)
    except Exception as exc:
        logger.warning("WS stream error: vm_name=%s error=%s", vm_name, exc)
        with contextlib.suppress(Exception):
            await websocket.close(code=4001)


async def _stream_mock_events(websocket: WebSocket, vm_name: str) -> None:
    """Send 3 synthetic lifecycle events and close cleanly (mock mode only)."""
    now = datetime.now(UTC).isoformat()
    events: list[dict[str, object]] = [
        {
            "event": "STARTED",
            "service": "mock-service",
            "timestamp": now,
            "severity": "INFO",
            "correlation_id": "mock-cid",
            "details": {"vm_name": vm_name},
        },
        {
            "event": "DATA_BROADCAST",
            "service": "mock-service",
            "timestamp": now,
            "severity": "INFO",
            "correlation_id": "mock-cid",
            "details": {"rows": "100"},
        },
        {
            "event": "STOPPED",
            "service": "mock-service",
            "timestamp": now,
            "severity": "INFO",
            "correlation_id": "mock-cid",
            "details": {"exit_code": "0"},
        },
    ]
    for evt in events:
        await websocket.send_json(evt)
    await websocket.close()


def _event_to_dict(event: VMLifecycleEvent) -> dict[str, object]:
    return {
        "event": event.event,
        "service": event.service,
        "timestamp": event.timestamp.isoformat(),
        "severity": event.severity,
        "correlation_id": cast("object", event.correlation_id),
        "details": cast("object", event.details),
    }

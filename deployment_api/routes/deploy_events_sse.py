"""SSE endpoint for real-time deployment lifecycle events.

Streams deployment state updates to connected clients via Server-Sent Events.
Uses the UTL ``SSEChannel.DEPLOY_EVENTS`` channel and bridges from the existing
``deployment_events.subscribe()`` async generator that is fed by the background
auto-sync task and explicit refresh/cancel/resume actions.

Endpoint: ``GET /stream/deploy-events?deployment_id=<id>``

Without ``deployment_id``, the stream receives updates for **all** deployments.
"""

import asyncio
import logging
from collections.abc import AsyncGenerator

from fastapi import APIRouter, Query
from sse_starlette.sse import EventSourceResponse
from unified_trading_library import SSEChannel, SSEHeartbeat, SSEMessage

from deployment_api.utils.deployment_events import subscribe

logger = logging.getLogger(__name__)

router = APIRouter(tags=["sse"])

_HEARTBEAT_INTERVAL_S = 30


async def _deployment_event_stream(
    deployment_id: str,
) -> AsyncGenerator[dict[str, str]]:
    """Yield SSE-formatted events for a single deployment.

    Wraps the per-deployment ``subscribe()`` async generator with heartbeat
    injection using the same pattern as ``sse_event_stream`` from UTL.
    """
    gen = subscribe(deployment_id)
    try:
        while True:
            try:
                raw_msg = await asyncio.wait_for(
                    gen.__anext__(),
                    timeout=_HEARTBEAT_INTERVAL_S,
                )
                msg = SSEMessage(
                    channel=SSEChannel.DEPLOY_EVENTS,
                    event_type="deployment_updated",
                    data={
                        "deployment_id": deployment_id,
                        "detail": raw_msg,
                    },
                )
                yield {
                    "event": SSEChannel.DEPLOY_EVENTS.value,
                    "data": msg.model_dump_json(),
                }
            except TimeoutError:
                hb = SSEHeartbeat(channel=SSEChannel.DEPLOY_EVENTS)
                yield {"event": "heartbeat", "data": hb.model_dump_json()}
            except StopAsyncIteration:
                logger.info(
                    "SSE deploy-events generator exhausted for deployment %s",
                    deployment_id,
                )
                break
    except asyncio.CancelledError:
        logger.debug(
            "SSE deploy-events stream cancelled for deployment %s (client disconnect)",
            deployment_id,
        )
    finally:
        await gen.aclose()


@router.get("/stream/deploy-events")
async def stream_deploy_events(
    deployment_id: str = Query(..., description="Deployment ID to subscribe to"),
) -> EventSourceResponse:
    """Stream deployment lifecycle events as SSE.

    Pushes an event whenever a deployment's state changes (shard status
    transitions, overall status changes, cancellation, etc.).  The client
    receives structured ``SSEMessage`` payloads on the ``deploy_events``
    channel.  A heartbeat comment is sent every 30 s to keep the connection
    alive through proxies.
    """
    return EventSourceResponse(
        _deployment_event_stream(deployment_id),
    )

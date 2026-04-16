"""
Unit tests for the SSE deploy-events streaming endpoint.

Tests the ``/stream/deploy-events`` route and the ``_deployment_event_stream``
async generator that wraps ``deployment_events.subscribe()`` with heartbeat
injection and structured ``SSEMessage`` payloads.
"""

import asyncio
import json
from unittest.mock import patch

import pytest

from deployment_api.routes.deploy_events_sse import (
    _HEARTBEAT_INTERVAL_S,
    _deployment_event_stream,
    router,
)


class TestDeploymentEventStream:
    """Tests for _deployment_event_stream async generator."""

    @pytest.mark.asyncio
    async def test_yields_update_event_on_broadcast(self):
        """A broadcast pushes a deploy_events SSE message."""
        import deployment_api.utils.deployment_events as ev

        deployment_id = "dep-sse-test-1"

        async def _producer():
            # Let the stream subscribe first
            await asyncio.sleep(0.05)
            await ev._broadcast(deployment_id)

        producer_task = asyncio.create_task(_producer())

        events: list[dict[str, str]] = []
        async for event in _deployment_event_stream(deployment_id):
            events.append(event)
            break  # one event is enough

        assert len(events) == 1
        assert events[0]["event"] == "deploy_events"
        payload = json.loads(events[0]["data"])
        assert payload["channel"] == "deploy_events"
        assert payload["event_type"] == "deployment_updated"
        assert payload["data"]["deployment_id"] == deployment_id
        assert payload["data"]["detail"] == "updated"
        await producer_task

    @pytest.mark.asyncio
    async def test_emits_heartbeat_on_timeout(self):
        """When no events arrive within the heartbeat interval, a heartbeat is emitted."""
        # Use a very short heartbeat to avoid long test runtime
        with patch(
            "deployment_api.routes.deploy_events_sse._HEARTBEAT_INTERVAL_S",
            0.1,
        ):
            events: list[dict[str, str]] = []
            gen = _deployment_event_stream("dep-sse-heartbeat")
            try:
                event = await asyncio.wait_for(gen.__anext__(), timeout=2.0)
                events.append(event)
            except (StopAsyncIteration, TimeoutError):
                pass
            finally:
                await gen.aclose()

        # The heartbeat should have the heartbeat event type
        assert len(events) == 1
        assert events[0]["event"] == "heartbeat"
        payload = json.loads(events[0]["data"])
        assert payload["channel"] == "deploy_events"
        assert payload["event_type"] == "heartbeat"

    @pytest.mark.asyncio
    async def test_stream_closes_on_cancel(self):
        """Cancelling the consumer task cleans up the generator without error."""
        import deployment_api.utils.deployment_events as ev

        deployment_id = "dep-sse-cancel"
        gen = _deployment_event_stream(deployment_id)

        async def _consume():
            async for _ in gen:
                pass

        task = asyncio.create_task(_consume())
        await asyncio.sleep(0.05)
        task.cancel()
        # CancelledError is caught inside the generator, so await should finish cleanly
        try:
            await asyncio.wait_for(task, timeout=2.0)
        except (asyncio.CancelledError, TimeoutError):
            pass
        # Verify the subscription was cleaned up
        assert deployment_id not in ev._sse_queues


class TestRouterEndpoint:
    """Tests for the SSE router endpoint definition."""

    def test_router_has_stream_endpoint(self):
        """The router should have a GET /stream/deploy-events route."""
        paths = [r.path for r in router.routes]
        assert "/stream/deploy-events" in paths

    def test_heartbeat_interval_default(self):
        """Default heartbeat interval should be 30 seconds."""
        assert _HEARTBEAT_INTERVAL_S == 30

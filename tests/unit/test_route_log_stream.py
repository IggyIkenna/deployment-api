"""Unit tests for routes/log_stream.py — Phase C.5 log-stream endpoint."""

from __future__ import annotations

from unittest.mock import patch

import pytest
from fastapi import HTTPException
from unified_trading_library import setup_events

setup_events("deployment-api", "test")


class TestIsLiveClusterRef:
    def test_strategy_live_is_live(self) -> None:
        from deployment_api.routes.log_stream import _is_live_cluster_ref

        assert _is_live_cluster_ref("strategy-live-csb-001") is True

    def test_strategy_paper_is_live(self) -> None:
        from deployment_api.routes.log_stream import _is_live_cluster_ref

        assert _is_live_cluster_ref("strategy-paper-arb-001") is True

    def test_vm_name_is_not_live(self) -> None:
        from deployment_api.routes.log_stream import _is_live_cluster_ref

        assert _is_live_cluster_ref("instruments-defi-2026-05-18") is False

    def test_mtds_vm_is_not_live(self) -> None:
        from deployment_api.routes.log_stream import _is_live_cluster_ref

        assert _is_live_cluster_ref("mtds-cefi-2026-05-15") is False


class TestStreamLogsLiveClusterStreams:
    """The live-cluster 501 is CLOSED — live clusters now stream lifecycle/log
    events via the GCS events bucket keyed by the cluster's service name (no
    direct ``google.cloud.logging`` dependency)."""

    def test_live_cluster_streams(self) -> None:
        import asyncio

        from sse_starlette.sse import EventSourceResponse

        from deployment_api.routes.log_stream import stream_logs

        result = asyncio.run(stream_logs("strategy-live-csb-001"))
        assert isinstance(result, EventSourceResponse)

    def test_execution_service_streams(self) -> None:
        import asyncio

        from sse_starlette.sse import EventSourceResponse

        from deployment_api.routes.log_stream import stream_logs

        result = asyncio.run(stream_logs("execution-service"))
        assert isinstance(result, EventSourceResponse)


class TestMockSseGenerator:
    def test_mock_generator_yields_three_events_then_done(self) -> None:
        import asyncio

        from deployment_api.routes.log_stream import _mock_sse_generator

        async def collect() -> list[dict[str, str]]:
            return [item async for item in _mock_sse_generator("test-vm")]

        items = asyncio.run(collect())
        events = [i for i in items if i["event"] == "vm_event"]
        done_items = [i for i in items if i["event"] == "done"]
        assert len(events) == 3
        assert len(done_items) == 1

    def test_mock_generator_event_data_has_event_key(self) -> None:
        import asyncio
        import json

        from deployment_api.routes.log_stream import _mock_sse_generator

        async def first_event() -> dict[str, str]:
            async for item in _mock_sse_generator("test-vm"):
                if item["event"] == "vm_event":
                    return item
            return {}

        item = asyncio.run(first_event())
        data = json.loads(item["data"])
        assert "event" in data


class TestStreamLogsVmMockMode:
    def test_vm_ref_returns_sse_response_in_mock_mode(self) -> None:
        import asyncio

        from deployment_api.routes.log_stream import stream_logs

        with patch("deployment_api.routes.log_stream._cfg") as mock_cfg:
            mock_cfg.is_mock_mode.return_value = True

            result = asyncio.run(stream_logs("instruments-defi-2026-05-18"))
            # EventSourceResponse is returned (not raised)
            assert result is not None

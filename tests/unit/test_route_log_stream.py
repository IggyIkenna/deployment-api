"""Unit tests for routes/log_stream.py — Phase C.5 log-stream endpoint."""

from __future__ import annotations

import json
from collections.abc import AsyncIterator
from dataclasses import dataclass
from datetime import UTC, datetime
from unittest.mock import patch

import pytest
from fastapi import HTTPException
from unified_trading_library import setup_events

setup_events("deployment-api", "test")


@dataclass
class _FakeBlob:
    name: str


class _FakeStorageClient:
    """In-memory storage stand-in mirroring `test_vm_events.py`'s fake client —
    the exact surface `_vm_sse_generator` calls via `_collect_blob_names`/
    `_fetch_and_parse_event`: `list_blobs(bucket, prefix)` + `download_bytes(bucket, blob_path)`.
    """

    def __init__(self, blobs: dict[str, bytes]) -> None:
        self._blobs = blobs

    def list_blobs(self, bucket: str, prefix: str = "") -> list[_FakeBlob]:
        del bucket  # single-bucket fake
        return [_FakeBlob(name=name) for name in self._blobs if name.startswith(prefix)]

    def download_bytes(self, bucket: str, blob_path: str) -> bytes:
        del bucket
        if blob_path not in self._blobs:
            raise FileNotFoundError(blob_path)
        return self._blobs[blob_path]


def _make_event_row(*, event: str, service: str, timestamp: str, message: str = "") -> bytes:
    details = {"message": message} if message else {}
    payload = {
        "event": event,
        "service": service,
        "timestamp": timestamp,
        "metadata": {"service_name": service, "severity": "INFO", "details": details},
    }
    return (json.dumps(payload) + "\n").encode()


async def _drain(agen: AsyncIterator[dict[str, str]], max_items: int) -> list[dict[str, str]]:
    """Bounded drain — both real generators are literal `while True` loops with
    no natural termination, so we stop after `max_items` frames."""
    items: list[dict[str, str]] = []
    async for item in agen:
        items.append(item)
        if len(items) >= max_items:
            break
    return items


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


class TestVmSseGeneratorRealMode:
    """Drains `_vm_sse_generator` against a fake storage client — closes the gap in
    `deployment_api_log_stream_sse_generator_no_test_coverage_2026_07_30.md`: the only
    prior tests for this route never iterated the generator, so zero GCS I/O was ever
    exercised. Mirrors `test_vm_events.py::TestRealMode::test_empty_bucket_returns_zero_events`'s
    fake-client pattern against the SSE route's own (previously undrained) generator.
    """

    def test_empty_bucket_yields_heartbeats_only_no_fabricated_vm_event(self, monkeypatch: pytest.MonkeyPatch) -> None:
        import asyncio

        from deployment_api.routes import log_stream

        blobs: dict[str, bytes] = {}

        def _fake_get_storage_client(*_args: object, **_kwargs: object) -> _FakeStorageClient:
            return _FakeStorageClient(blobs)

        monkeypatch.setattr(log_stream, "get_storage_client", _fake_get_storage_client)
        monkeypatch.setattr(log_stream, "_POLL_INTERVAL_SECS", 0.01)
        monkeypatch.setattr(log_stream, "_HEARTBEAT_INTERVAL_SECS", 0.02)

        async def run() -> list[dict[str, str]]:
            return await asyncio.wait_for(
                _drain(log_stream._vm_sse_generator("af-backfill-test-empty"), max_items=3),
                timeout=5.0,
            )

        items = asyncio.run(run())

        vm_events = [i for i in items if i["event"] == "vm_event"]
        heartbeats = [i for i in items if i["event"] == "heartbeat"]
        assert vm_events == []
        assert len(heartbeats) >= 1
        assert all(i["event"] == "heartbeat" for i in items)

    def test_real_blobs_yield_matching_vm_event_frames(self, monkeypatch: pytest.MonkeyPatch) -> None:
        import asyncio

        from deployment_api.routes import log_stream

        vm_name = "af-backfill-test-real"
        service = "instruments-service"
        now = datetime.now(UTC)
        date = now.strftime("%Y-%m-%d")
        hour = now.hour
        blobs: dict[str, bytes] = {
            f"events/{service}/{date}/{vm_name}/hour={hour:02d}/1000_0.jsonl": _make_event_row(
                event="STARTED",
                service=service,
                timestamp="2026-07-30T00:00:00+00:00",
                message="vm started",
            ),
            f"events/{service}/{date}/{vm_name}/hour={hour:02d}/1000_1.jsonl": _make_event_row(
                event="STOPPED",
                service=service,
                timestamp="2026-07-30T00:01:00+00:00",
                message="vm stopped",
            ),
        }

        def _fake_get_storage_client(*_args: object, **_kwargs: object) -> _FakeStorageClient:
            return _FakeStorageClient(blobs)

        monkeypatch.setattr(log_stream, "get_storage_client", _fake_get_storage_client)
        monkeypatch.setattr(log_stream, "_POLL_INTERVAL_SECS", 0.01)
        monkeypatch.setattr(log_stream, "_HEARTBEAT_INTERVAL_SECS", 0.02)

        async def run() -> list[dict[str, str]]:
            return await asyncio.wait_for(
                _drain(log_stream._vm_sse_generator(vm_name), max_items=2),
                timeout=5.0,
            )

        items = asyncio.run(run())

        vm_events = [i for i in items if i["event"] == "vm_event"]
        assert len(vm_events) == 2
        parsed = [json.loads(i["data"]) for i in vm_events]
        assert [p["event"] for p in parsed] == ["STARTED", "STOPPED"]
        assert parsed[0]["message"] == "vm started"
        assert parsed[1]["message"] == "vm stopped"

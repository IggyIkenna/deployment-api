"""Integration tests for GET /api/vm/events.

Coverage matrix (per work-stream-A plan §2.B):
  1. Auth — covered by FastAPI verify_api_key dep.
  2. Missing vm_name → 422 (Query(...) required).
  3. Mock-mode returns synthesized 3-event VMEventListResult.
  4. Real-mode (storage client monkeypatched) — empty bucket returns total=0.
  5. Real-mode — single hour with 5 JSONL rows, all parsed.
  6. Severity filter drops rows below the floor.
  7. Malformed JSONL row in middle of partition is skipped (others returned).
  8. Pagination round-trip — page_size=2 with truncated=True + non-null token,
     subsequent request with the token yields the next page.
  9. Date defaults to today's UTC date.
"""

from __future__ import annotations

import json
import os
from collections.abc import Iterator
from dataclasses import dataclass
from datetime import UTC, datetime
from typing import cast

os.environ.setdefault("CLOUD_MOCK_MODE", "true")
os.environ.setdefault("CLOUD_PROVIDER", "local")
os.environ.setdefault("GCP_PROJECT_ID", "test-project")
os.environ.setdefault("DISABLE_AUTH", "true")
os.environ.setdefault("MOCK_STATE_MODE", "deterministic")

from unittest.mock import patch

import pytest
from fastapi.testclient import TestClient

with (
    patch("unified_trading_library.event_sink.PubSubEventSink"),
    patch("unified_trading_library.PubSubEventSink"),
    patch("unified_trading_library.events.setup_events"),
    patch("unified_trading_library.utils.tracing.setup_tracing"),
    patch("unified_trading_library.setup_tracing"),
):
    from deployment_api.main import app

from unified_trading_library.events import setup_events

setup_events("deployment-api", "test")

from deployment_api.routes import vm_events as vm_events_module
from deployment_api.deployment_api_config import DeploymentApiConfig

pytestmark = [pytest.mark.integration, pytest.mark.timeout(60)]


@pytest.fixture(scope="module")
def client() -> TestClient:
    return TestClient(app)


# ---------------------------------------------------------------------------
# Fake StorageClient
# ---------------------------------------------------------------------------


@dataclass
class _FakeBlob:
    name: str


class _FakeStorageClient:
    """In-memory storage stand-in that mimics the StorageClient surface used
    by `vm_events`: `list_blobs(bucket, prefix)` + `download_bytes(bucket, blob_path)`.
    """

    def __init__(self, blobs: dict[str, bytes]) -> None:
        # blobs: {full_blob_path: bytes content}
        self._blobs = blobs

    def list_blobs(self, bucket: str, prefix: str = "") -> list[_FakeBlob]:
        del bucket  # single-bucket fake
        return [_FakeBlob(name=name) for name in self._blobs if name.startswith(prefix)]

    def download_bytes(self, bucket: str, blob_path: str) -> bytes:
        del bucket
        if blob_path not in self._blobs:
            raise FileNotFoundError(blob_path)
        return self._blobs[blob_path]


def _make_event_row(
    *,
    event: str,
    service: str,
    timestamp: str,
    severity: str = "INFO",
    correlation_id: str = "test-corr-1",
    extra_details: dict[str, str] | None = None,
) -> bytes:
    details: dict[str, str] = {"correlation_id": correlation_id}
    if extra_details:
        details.update(extra_details)
    payload = {
        "event": event,
        "service": service,
        "timestamp": timestamp,
        "metadata": {
            "service_name": service,
            "severity": severity,
            "details": details,
        },
    }
    return (json.dumps(payload) + "\n").encode()


def _today() -> str:
    return datetime.now(UTC).strftime("%Y-%m-%d")


def _path_for(service: str, date: str, vm_name: str, hour: int, ts_us: int, seq: int) -> str:
    return f"events/{service}/{date}/{vm_name}/hour={hour:02d}/{ts_us}_{seq}.jsonl"


@pytest.fixture
def disable_mock_mode(monkeypatch: pytest.MonkeyPatch) -> Iterator[None]:
    """Force the route into real-mode — patch the class method (Pydantic
    blocks instance attribute writes for callables).
    """
    monkeypatch.setattr(DeploymentApiConfig, "is_mock_mode", lambda self: False)
    yield


@pytest.fixture
def fake_storage(monkeypatch: pytest.MonkeyPatch) -> dict[str, bytes]:
    """Patch `get_storage_client` to return a `_FakeStorageClient`. The dict
    returned IS the in-memory bucket; tests mutate it before calling the API.
    """
    blobs: dict[str, bytes] = {}

    def _fake_get_storage_client(*_args: object, **_kwargs: object) -> _FakeStorageClient:
        return _FakeStorageClient(blobs)

    monkeypatch.setattr(vm_events_module, "get_storage_client", _fake_get_storage_client)
    return blobs


# ---------------------------------------------------------------------------
# Validation
# ---------------------------------------------------------------------------


class TestRequestValidation:
    def test_missing_vm_name_returns_422(self, client: TestClient) -> None:
        resp = client.get("/api/vm/events")
        assert resp.status_code == 422

    def test_unknown_prefix_falls_back_to_400(self, client: TestClient) -> None:
        """Unknown vm_name prefix + no `service=` query param → 400."""
        resp = client.get("/api/vm/events", params={"vm_name": "totally-unknown-prefix-foo"})
        assert resp.status_code == 400, resp.json()
        assert resp.json()["error"]["details"]["code"] == "SERVICE_INFERENCE_FAILED"

    def test_bad_date_returns_400(self, client: TestClient) -> None:
        resp = client.get(
            "/api/vm/events",
            params={"vm_name": "af-backfill-20260507-002914", "date": "not-a-date"},
        )
        assert resp.status_code == 400
        assert resp.json()["error"]["details"]["code"] == "BAD_DATE"

    def test_bad_severity_returns_400(self, client: TestClient) -> None:
        resp = client.get(
            "/api/vm/events",
            params={"vm_name": "af-backfill-20260507-002914", "severity_floor": "WHATEVER"},
        )
        assert resp.status_code == 400
        assert resp.json()["error"]["details"]["code"] == "BAD_SEVERITY"


# ---------------------------------------------------------------------------
# Mock mode
# ---------------------------------------------------------------------------


class TestMockMode:
    def test_mock_returns_synthesized_events(self, client: TestClient) -> None:
        resp = client.get(
            "/api/vm/events",
            params={"vm_name": "af-backfill-20260507-002914", "date": "2026-05-07"},
        )
        assert resp.status_code == 200, resp.json()
        body = resp.json()
        assert body["vm_name"] == "af-backfill-20260507-002914"
        assert body["service"] == "instruments-service"
        assert body["date"] == "2026-05-07"
        assert body["truncated"] is False
        assert body["total_events"] == 3
        assert {evt["event"] for evt in body["events"]} == {
            "STARTED",
            "INSTRUMENT_PROCESSED",
            "STOPPED",
        }

    def test_mock_uses_explicit_service_when_passed(self, client: TestClient) -> None:
        resp = client.get(
            "/api/vm/events",
            params={
                "vm_name": "weird-prefix-that-doesnt-match-anything-20260507",
                "service": "explicit-service",
                "date": "2026-05-07",
            },
        )
        assert resp.status_code == 200
        assert resp.json()["service"] == "explicit-service"


# ---------------------------------------------------------------------------
# Real mode
# ---------------------------------------------------------------------------


class TestRealMode:
    def test_empty_bucket_returns_zero_events(
        self,
        client: TestClient,
        disable_mock_mode: None,
        fake_storage: dict[str, bytes],
    ) -> None:
        del fake_storage  # empty by default
        resp = client.get(
            "/api/vm/events",
            params={
                "vm_name": "af-backfill-20260507-002914",
                "date": "2026-05-07",
                "from_hour": 0,
                "to_hour": 23,
            },
        )
        assert resp.status_code == 200, resp.json()
        body = resp.json()
        assert body["total_events"] == 0
        assert body["events"] == []
        assert body["truncated"] is False

    def test_single_hour_five_rows_all_parsed(
        self,
        client: TestClient,
        disable_mock_mode: None,
        fake_storage: dict[str, bytes],
    ) -> None:
        vm_name = "af-backfill-20260507-002914"
        date = "2026-05-07"
        service = "instruments-service"
        for idx, event_name in enumerate(
            ["STARTED", "FETCHING_FIXTURES", "FETCHING_LINEUPS", "WRITING_PARQUET", "STOPPED"]
        ):
            ts_us = 1778113506490157 + idx
            path = _path_for(service, date, vm_name, hour=0, ts_us=ts_us, seq=idx)
            fake_storage[path] = _make_event_row(
                event=event_name,
                service=service,
                timestamp=f"2026-05-07T00:0{idx}:00+00:00",
            )
        resp = client.get(
            "/api/vm/events",
            params={"vm_name": vm_name, "date": date, "from_hour": 0, "to_hour": 0},
        )
        assert resp.status_code == 200, resp.json()
        body = resp.json()
        assert body["total_events"] == 5
        assert [evt["event"] for evt in body["events"]] == [
            "STARTED",
            "FETCHING_FIXTURES",
            "FETCHING_LINEUPS",
            "WRITING_PARQUET",
            "STOPPED",
        ]
        assert body["hours_scanned"] == [0]

    def test_severity_floor_drops_below(
        self,
        client: TestClient,
        disable_mock_mode: None,
        fake_storage: dict[str, bytes],
    ) -> None:
        vm_name = "af-backfill-20260507-002914"
        date = "2026-05-07"
        service = "instruments-service"
        rows: list[tuple[str, str]] = [
            ("INFO_EVENT", "INFO"),
            ("WARN_EVENT", "WARNING"),
            ("ERR_EVENT", "ERROR"),
            ("CRIT_EVENT", "CRITICAL"),
        ]
        for idx, (event_name, sev) in enumerate(rows):
            path = _path_for(service, date, vm_name, hour=0, ts_us=2000000 + idx, seq=idx)
            fake_storage[path] = _make_event_row(
                event=event_name,
                service=service,
                timestamp=f"2026-05-07T00:0{idx}:00+00:00",
                severity=sev,
            )

        resp = client.get(
            "/api/vm/events",
            params={
                "vm_name": vm_name,
                "date": date,
                "from_hour": 0,
                "to_hour": 0,
                "severity_floor": "ERROR",
            },
        )
        assert resp.status_code == 200, resp.json()
        body = resp.json()
        events_returned = {evt["event"] for evt in body["events"]}
        assert events_returned == {"ERR_EVENT", "CRIT_EVENT"}

    def test_malformed_jsonl_skipped(
        self,
        client: TestClient,
        disable_mock_mode: None,
        fake_storage: dict[str, bytes],
    ) -> None:
        vm_name = "af-backfill-20260507-002914"
        date = "2026-05-07"
        service = "instruments-service"
        # Good event
        fake_storage[
            _path_for(service, date, vm_name, hour=0, ts_us=3000000, seq=1)
        ] = _make_event_row(event="STARTED", service=service, timestamp="2026-05-07T00:00:00+00:00")
        # Garbage in middle
        fake_storage[
            _path_for(service, date, vm_name, hour=0, ts_us=3000001, seq=2)
        ] = b"not-valid-json{{{\n"
        # Good event after
        fake_storage[
            _path_for(service, date, vm_name, hour=0, ts_us=3000002, seq=3)
        ] = _make_event_row(event="STOPPED", service=service, timestamp="2026-05-07T00:01:00+00:00")
        resp = client.get(
            "/api/vm/events",
            params={"vm_name": vm_name, "date": date, "from_hour": 0, "to_hour": 0},
        )
        assert resp.status_code == 200, resp.json()
        body = resp.json()
        assert {evt["event"] for evt in body["events"]} == {"STARTED", "STOPPED"}
        assert body["total_events"] == 2

    def test_pagination_round_trip(
        self,
        client: TestClient,
        disable_mock_mode: None,
        fake_storage: dict[str, bytes],
    ) -> None:
        vm_name = "af-backfill-20260507-002914"
        date = "2026-05-07"
        service = "instruments-service"
        for idx in range(5):
            path = _path_for(service, date, vm_name, hour=0, ts_us=4000000 + idx, seq=idx)
            fake_storage[path] = _make_event_row(
                event=f"EVT_{idx}",
                service=service,
                timestamp=f"2026-05-07T00:0{idx}:00+00:00",
            )
        # Page 1: limit=2 → expect 2 events + truncated + token
        resp1 = client.get(
            "/api/vm/events",
            params={
                "vm_name": vm_name,
                "date": date,
                "from_hour": 0,
                "to_hour": 0,
                "page_size": 2,
            },
        )
        assert resp1.status_code == 200, resp1.json()
        body1 = resp1.json()
        assert body1["truncated"] is True
        assert body1["total_events"] == 2
        assert [evt["event"] for evt in body1["events"]] == ["EVT_0", "EVT_1"]
        token = cast(str, body1["next_page_token"])
        assert token

        # Page 2: same params + token
        resp2 = client.get(
            "/api/vm/events",
            params={
                "vm_name": vm_name,
                "date": date,
                "from_hour": 0,
                "to_hour": 0,
                "page_size": 2,
                "next_page_token": token,
            },
        )
        assert resp2.status_code == 200, resp2.json()
        body2 = resp2.json()
        assert body2["truncated"] is True
        assert [evt["event"] for evt in body2["events"]] == ["EVT_2", "EVT_3"]

        # Page 3: should drain
        resp3 = client.get(
            "/api/vm/events",
            params={
                "vm_name": vm_name,
                "date": date,
                "from_hour": 0,
                "to_hour": 0,
                "page_size": 2,
                "next_page_token": cast(str, body2["next_page_token"]),
            },
        )
        body3 = resp3.json()
        assert body3["truncated"] is False
        assert [evt["event"] for evt in body3["events"]] == ["EVT_4"]
        assert body3["next_page_token"] is None

    def test_date_defaults_to_today_utc(
        self,
        client: TestClient,
        disable_mock_mode: None,
        fake_storage: dict[str, bytes],
    ) -> None:
        vm_name = "af-backfill-test"
        service = "instruments-service"
        today = _today()
        path = _path_for(service, today, vm_name, hour=0, ts_us=5000000, seq=1)
        fake_storage[path] = _make_event_row(
            event="STARTED", service=service, timestamp=f"{today}T00:00:00+00:00"
        )
        resp = client.get(
            "/api/vm/events", params={"vm_name": vm_name, "from_hour": 0, "to_hour": 0}
        )
        assert resp.status_code == 200, resp.json()
        body = resp.json()
        assert body["date"] == today
        assert body["total_events"] == 1
        assert body["events"][0]["event"] == "STARTED"

    def test_correlation_id_extracted_from_details(
        self,
        client: TestClient,
        disable_mock_mode: None,
        fake_storage: dict[str, bytes],
    ) -> None:
        vm_name = "af-backfill-20260507-002914"
        date = "2026-05-07"
        service = "instruments-service"
        path = _path_for(service, date, vm_name, hour=0, ts_us=6000000, seq=1)
        fake_storage[path] = _make_event_row(
            event="STARTED",
            service=service,
            timestamp="2026-05-07T00:00:00+00:00",
            correlation_id="req-abc-123",
            extra_details={"foo": "bar"},
        )
        resp = client.get(
            "/api/vm/events",
            params={"vm_name": vm_name, "date": date, "from_hour": 0, "to_hour": 0},
        )
        assert resp.status_code == 200, resp.json()
        evt = resp.json()["events"][0]
        assert evt["correlation_id"] == "req-abc-123"
        assert evt["details"]["foo"] == "bar"
        assert evt["details"]["correlation_id"] == "req-abc-123"

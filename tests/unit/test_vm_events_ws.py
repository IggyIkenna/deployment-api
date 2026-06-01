"""Smoke test for /ws/vm/{vm_name}/events WebSocket endpoint."""

from __future__ import annotations

import pytest
from fastapi.testclient import TestClient


@pytest.fixture(scope="module")
def client() -> TestClient:
    from deployment_api.main import app

    return TestClient(app, raise_server_exceptions=False)


def test_ws_vm_events_mock_mode_three_events(client: TestClient) -> None:
    """In mock mode: 3 synthetic events are sent and the connection closes cleanly."""
    with client.websocket_connect("/ws/vm/cefi-test-vm-001/events") as ws:
        events = []
        for _ in range(3):
            data = ws.receive_json()
            events.append(data)

    assert len(events) == 3
    assert events[0]["event"] == "STARTED"
    assert events[1]["event"] == "DATA_BROADCAST"
    assert events[2]["event"] == "STOPPED"
    for evt in events:
        assert "timestamp" in evt
        assert "service" in evt
        assert "severity" in evt
        assert "correlation_id" in evt

"""Unit tests for vm_health.py — GET /api/vm/{vm_name}/health."""

from __future__ import annotations

from collections.abc import Generator
from datetime import UTC, datetime, timedelta
from unittest.mock import patch

import pytest
from fastapi import FastAPI
from fastapi.testclient import TestClient
from unified_api_contracts.internal import VMLifecycleEvent
from unified_trading_library import setup_events

from deployment_api.routes.vm_health import (
    derive_vm_health_state,
    router,
    summarise_vm_health,
)

setup_events("deployment-api", "test")

_PATCH_MOCK = "deployment_api.deployment_api_config.DeploymentApiConfig.is_mock_mode"
_PATCH_INFER = "deployment_api.routes.vm_health.infer_service_from_vm_name"
_PATCH_FETCH = "deployment_api.routes.vm_health._fetch_events_for_window"


@pytest.fixture
def mock_client() -> Generator[TestClient]:
    with patch(_PATCH_MOCK, return_value=True):
        app = FastAPI()
        app.include_router(router)
        yield TestClient(app)


@pytest.fixture
def prod_client() -> Generator[TestClient]:
    with patch(_PATCH_MOCK, return_value=False):
        app = FastAPI()
        app.include_router(router)
        yield TestClient(app)


def _make_event(event: str, minutes_ago: int = 5) -> VMLifecycleEvent:
    return VMLifecycleEvent(
        event=event,
        service="test-service",
        timestamp=datetime.now(UTC) - timedelta(minutes=minutes_ago),
    )


class TestMockMode:
    def test_returns_200(self, mock_client: TestClient) -> None:
        r = mock_client.get("/vm/cefi-backfill-20260515/health")
        assert r.status_code == 200

    def test_returns_green_in_mock(self, mock_client: TestClient) -> None:
        r = mock_client.get("/vm/cefi-backfill-20260515/health")
        body = r.json()
        assert body["state"] == "green"
        assert body["vm_name"] == "cefi-backfill-20260515"
        assert body["is_terminal"] is False

    def test_has_all_required_fields(self, mock_client: TestClient) -> None:
        r = mock_client.get("/vm/cefi-backfill-20260515/health")
        body = r.json()
        required = {
            "vm_name",
            "service",
            "state",
            "last_started_at",
            "last_event_at",
            "last_event_type",
            "expected_next_heartbeat_at",
            "threshold_warn_seconds",
            "threshold_error_seconds",
            "is_terminal",
            "scanned_hours",
            "message",
        }
        assert required <= body.keys()

    def test_threshold_values_correct(self, mock_client: TestClient) -> None:
        r = mock_client.get("/vm/cefi-backfill-20260515/health")
        body = r.json()
        assert body["threshold_warn_seconds"] == 3600
        assert body["threshold_error_seconds"] == 7200


class TestDeriveState:
    def test_none_events_returns_unknown(self) -> None:
        now = datetime.now(UTC)
        state, msg = derive_vm_health_state(None, False, now)
        assert state == "unknown"
        assert "No events" in msg

    def test_terminal_returns_red(self) -> None:
        now = datetime.now(UTC)
        last = now - timedelta(minutes=2)
        state, msg = derive_vm_health_state(last, True, now)
        assert state == "red"
        assert "terminal" in msg

    def test_recent_returns_green(self) -> None:
        now = datetime.now(UTC)
        last = now - timedelta(minutes=10)
        state, _ = derive_vm_health_state(last, False, now)
        assert state == "green"

    def test_stale_returns_amber(self) -> None:
        now = datetime.now(UTC)
        last = now - timedelta(hours=1, minutes=30)
        state, _ = derive_vm_health_state(last, False, now)
        assert state == "amber"

    def test_very_stale_returns_red(self) -> None:
        now = datetime.now(UTC)
        last = now - timedelta(hours=3)
        state, _ = derive_vm_health_state(last, False, now)
        assert state == "red"


class TestSummarise:
    def test_empty_events_returns_unknown(self) -> None:
        result = summarise_vm_health("vm1", "svc1", [], 0, datetime.now(UTC))
        assert result.state == "unknown"
        assert result.last_event_at is None

    def test_started_then_progress_is_green(self) -> None:
        now = datetime.now(UTC)
        events = [
            _make_event("STARTED", minutes_ago=60),
            _make_event("INSTRUMENT_PROCESSED", minutes_ago=5),
        ]
        result = summarise_vm_health("vm1", "svc1", events, 2, now)
        assert result.state == "green"
        assert result.is_terminal is False
        assert result.last_event_type == "INSTRUMENT_PROCESSED"
        assert result.last_started_at is not None

    def test_stopped_event_is_terminal_red(self) -> None:
        now = datetime.now(UTC)
        events = [
            _make_event("STARTED", minutes_ago=120),
            _make_event("STOPPED", minutes_ago=10),
        ]
        result = summarise_vm_health("vm1", "svc1", events, 2, now)
        assert result.state == "red"
        assert result.is_terminal is True

    def test_expected_next_set_when_not_terminal(self) -> None:
        now = datetime.now(UTC)
        events = [
            _make_event("STARTED", minutes_ago=30),
            _make_event("DATA_BROADCAST", minutes_ago=5),
        ]
        result = summarise_vm_health("vm1", "svc1", events, 1, now)
        assert result.expected_next_heartbeat_at is not None

    def test_expected_next_none_when_terminal(self) -> None:
        now = datetime.now(UTC)
        events = [
            _make_event("STARTED", minutes_ago=60),
            _make_event("FAILED", minutes_ago=2),
        ]
        result = summarise_vm_health("vm1", "svc1", events, 1, now)
        assert result.expected_next_heartbeat_at is None


class TestProdModeNoEvents:
    def test_unknown_when_no_events(self, prod_client: TestClient) -> None:
        with (
            patch(_PATCH_INFER, return_value="instruments-service"),
            patch(_PATCH_FETCH, return_value=([], 0)),
        ):
            r = prod_client.get("/vm/fs-backfill-20260515/health?service=instruments-service")
        assert r.status_code == 200
        body = r.json()
        assert body["state"] == "unknown"

"""Unit tests for ``deployment_api/routes/kill_switch_routes.py``.

Covers DR Phase 7.A surface:

- ``POST /api/kill-switch/{id}/arm`` — happy-path, idempotency, unknown id,
  bad body shape, metadata round-trip, provenance.
- ``POST /api/kill-switch/{id}/disarm`` — happy-path, no-op when never armed,
  bad body shape.
- ``GET  /api/kill-switch`` + ``GET /api/kill-switch/state`` — empty + armed
  states, typed ``armed_switch_ids`` round-trip.
- ``GET  /api/kill-switch/audit-log`` — empty (with note), populated after
  arm/disarm, switch_id filter, pagination.
- Operator-auth-gate (``X-API-Key``) — missing header → 401, valid header
  → 200.

Tests bypass the FastAPI app boot (Pub/Sub event sink + UnifiedCloudConfig)
and exercise the route handlers + router directly, mounted on a minimal
``FastAPI`` instance. This keeps the tests fully credential-free without
pulling on the production lifespan stack.
"""

from __future__ import annotations

from collections.abc import Iterator
from unittest.mock import patch

import pytest
from fastapi import FastAPI
from fastapi.testclient import TestClient
from unified_api_contracts import (
    KillSwitchArmedEvent,
    KillSwitchDisarmEvent,
    KillSwitchId,
    KillSwitchProvenance,
)
from unified_api_contracts.canonical.crosscutting.circuit_breaker import (  # noqa: qg-deep-import
    BreakerRecoveryMode,
)
from unified_api_contracts.internal.domain.deployment_service import KillSwitchScope
from unified_trading_library.events import setup_events
from unified_trading_library.kill_switch.bus import (
    get_kill_switch_bus,
    reset_kill_switch_bus,
)

import deployment_api.routes.kill_switch_routes as ks_routes
from deployment_api.routes.kill_switch_routes import (
    ArmRequestBody,
    arm_kill_switch,
    disarm_kill_switch,
    get_kill_switch_audit_log,
    get_kill_switch_state,
    list_armed_kill_switches,
)
from deployment_api.routes.kill_switch_routes import (
    router as kill_switch_router,
)

# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------


@pytest.fixture(autouse=True)
def _reset_bus() -> Iterator[None]:
    """Reset the process-singleton bus + audit-log writer between tests.

    Also initialises the UTL event sink (kill-switch arm/disarm emit lifecycle
    events; without ``setup_events()`` the bus raises ``RuntimeError: Event
    logging not initialized``).
    """
    setup_events(service_name="deployment-api-test", mode="test")
    reset_kill_switch_bus()
    ks_routes._AUDIT_LOG_WRITER.events.clear()
    yield
    reset_kill_switch_bus()
    ks_routes._AUDIT_LOG_WRITER.events.clear()


@pytest.fixture
def app_with_auth_disabled() -> Iterator[FastAPI]:
    """Minimal FastAPI app mounting the kill-switch router with auth bypassed."""
    with patch("deployment_api.auth.DISABLE_AUTH", True):
        app = FastAPI()
        app.include_router(kill_switch_router)
        yield app


@pytest.fixture
def app_with_auth_enabled() -> FastAPI:
    """Minimal FastAPI app mounting the kill-switch router with auth enforced."""
    app = FastAPI()
    app.include_router(kill_switch_router)
    return app


# ---------------------------------------------------------------------------
# Direct handler tests — POST /arm
# ---------------------------------------------------------------------------


class TestArm:
    @pytest.mark.asyncio
    async def test_arm_returns_armed_event(self) -> None:
        body = ArmRequestBody(requested_by="operator@example.com")
        event = await arm_kill_switch(KillSwitchId.KILL_PER_VENUE_BYBIT, body)
        assert isinstance(event, KillSwitchArmedEvent)
        assert event.switch_id == KillSwitchId.KILL_PER_VENUE_BYBIT
        assert event.provenance == KillSwitchProvenance.OPERATOR_MANUAL
        assert event.requested_by == "operator@example.com"
        assert event.metadata == {}

    @pytest.mark.asyncio
    async def test_arm_publishes_to_bus(self) -> None:
        body = ArmRequestBody(requested_by="op")
        await arm_kill_switch(KillSwitchId.KILL_PER_VENUE_BINANCE, body)
        bus = get_kill_switch_bus()
        assert bus.is_armed(KillSwitchId.KILL_PER_VENUE_BINANCE)

    @pytest.mark.asyncio
    async def test_arm_with_metadata_round_trips(self) -> None:
        body = ArmRequestBody(
            requested_by="op",
            metadata={"breaker_serial": "br-42", "correlation_id": "abc"},
        )
        event = await arm_kill_switch(KillSwitchId.KILL_ALL_LIVE, body)
        assert event.metadata == {"breaker_serial": "br-42", "correlation_id": "abc"}

    @pytest.mark.asyncio
    async def test_arm_global_maps_to_global_scope(self) -> None:
        body = ArmRequestBody(requested_by="op")
        await arm_kill_switch(KillSwitchId.KILL_ALL_LIVE, body)
        bus = get_kill_switch_bus()
        assert bus.is_armed(KillSwitchId.KILL_ALL_LIVE)

    @pytest.mark.asyncio
    async def test_arm_archetype_arms_switch(self) -> None:
        body = ArmRequestBody(requested_by="op")
        await arm_kill_switch(KillSwitchId.KILL_PER_ARCHETYPE_CARRY_STAKED_BASIS, body)
        bus = get_kill_switch_bus()
        assert bus.is_armed(KillSwitchId.KILL_PER_ARCHETYPE_CARRY_STAKED_BASIS)

    @pytest.mark.asyncio
    async def test_arm_idempotent_second_call_returns_event(self) -> None:
        body = ArmRequestBody(requested_by="op")
        event1 = await arm_kill_switch(KillSwitchId.KILL_PER_VENUE_BYBIT, body)
        event2 = await arm_kill_switch(KillSwitchId.KILL_PER_VENUE_BYBIT, body)
        assert event1.switch_id == event2.switch_id == KillSwitchId.KILL_PER_VENUE_BYBIT

    @pytest.mark.asyncio
    async def test_arm_writes_audit_log_row(self) -> None:
        body = ArmRequestBody(requested_by="op")
        await arm_kill_switch(KillSwitchId.KILL_PER_VENUE_BYBIT, body)
        assert len(ks_routes._AUDIT_LOG_WRITER.events) == 1
        assert isinstance(ks_routes._AUDIT_LOG_WRITER.events[0], KillSwitchArmedEvent)


# ---------------------------------------------------------------------------
# Direct handler tests — POST /disarm
# ---------------------------------------------------------------------------


class TestDisarm:
    @pytest.mark.asyncio
    async def test_disarm_returns_disarm_event_with_manual_unkill(self) -> None:
        body = ArmRequestBody(requested_by="op")
        await arm_kill_switch(KillSwitchId.KILL_PER_VENUE_BYBIT, body)
        event = await disarm_kill_switch(KillSwitchId.KILL_PER_VENUE_BYBIT, body)
        assert isinstance(event, KillSwitchDisarmEvent)
        assert event.switch_id == KillSwitchId.KILL_PER_VENUE_BYBIT
        assert event.disarmed_by == "op"
        assert event.recovery_mode == BreakerRecoveryMode.MANUAL_UNKILL
        assert event.cooldown_seconds_elapsed is None

    @pytest.mark.asyncio
    async def test_disarm_when_never_armed_returns_synthesised_event(self) -> None:
        """Bus disarm() returns None when not armed; the route synthesises a 200."""
        body = ArmRequestBody(requested_by="op")
        event = await disarm_kill_switch(KillSwitchId.KILL_PER_VENUE_OKX, body)
        assert event.switch_id == KillSwitchId.KILL_PER_VENUE_OKX
        bus = get_kill_switch_bus()
        assert not bus.is_armed(KillSwitchId.KILL_PER_VENUE_OKX)
        assert ks_routes._AUDIT_LOG_WRITER.events == []

    @pytest.mark.asyncio
    async def test_disarm_clears_bus_state(self) -> None:
        body = ArmRequestBody(requested_by="op")
        await arm_kill_switch(KillSwitchId.KILL_PER_VENUE_HYPERLIQUID, body)
        bus = get_kill_switch_bus()
        assert bus.is_armed(KillSwitchId.KILL_PER_VENUE_HYPERLIQUID)
        await disarm_kill_switch(KillSwitchId.KILL_PER_VENUE_HYPERLIQUID, body)
        assert not bus.is_armed(KillSwitchId.KILL_PER_VENUE_HYPERLIQUID)


# ---------------------------------------------------------------------------
# Direct handler tests — GET / and GET /state
# ---------------------------------------------------------------------------


class TestState:
    @pytest.mark.asyncio
    async def test_state_empty_when_nothing_armed(self) -> None:
        resp = await get_kill_switch_state()
        assert resp.armed == []

    @pytest.mark.asyncio
    async def test_list_alias_empty_when_nothing_armed(self) -> None:
        resp = await list_armed_kill_switches()
        assert resp.armed == []

    @pytest.mark.asyncio
    async def test_state_returns_armed_entries(self) -> None:
        body = ArmRequestBody(requested_by="op")
        await arm_kill_switch(KillSwitchId.KILL_PER_VENUE_BYBIT, body)
        await arm_kill_switch(KillSwitchId.KILL_PER_VENUE_DERIBIT, body)
        resp = await get_kill_switch_state()
        ids = {entry.switch_id for entry in resp.armed}
        assert ids == {
            KillSwitchId.KILL_PER_VENUE_BYBIT,
            KillSwitchId.KILL_PER_VENUE_DERIBIT,
        }
        for entry in resp.armed:
            assert entry.scope == KillSwitchScope.VENUE
            assert entry.provenance == KillSwitchProvenance.OPERATOR_MANUAL
            assert entry.requested_by == "op"

    @pytest.mark.asyncio
    async def test_state_round_trips_global_kill(self) -> None:
        body = ArmRequestBody(requested_by="op")
        await arm_kill_switch(KillSwitchId.KILL_ALL_LIVE, body)
        resp = await get_kill_switch_state()
        assert len(resp.armed) == 1
        assert resp.armed[0].switch_id == KillSwitchId.KILL_ALL_LIVE
        assert resp.armed[0].scope == KillSwitchScope.GLOBAL


# ---------------------------------------------------------------------------
# Direct handler tests — GET /audit-log
# ---------------------------------------------------------------------------


class TestAuditLog:
    @pytest.mark.asyncio
    async def test_audit_log_empty_returns_note(self) -> None:
        resp = await get_kill_switch_audit_log(
            switch_id=None, start_date=None, end_date=None, page=1, page_size=50
        )
        assert resp.entries == []
        assert resp.total == 0
        assert resp.note is not None

    @pytest.mark.asyncio
    async def test_audit_log_records_arm_and_disarm(self) -> None:
        body = ArmRequestBody(requested_by="op")
        await arm_kill_switch(KillSwitchId.KILL_PER_VENUE_BYBIT, body)
        await disarm_kill_switch(KillSwitchId.KILL_PER_VENUE_BYBIT, body)
        resp = await get_kill_switch_audit_log(
            switch_id=None, start_date=None, end_date=None, page=1, page_size=50
        )
        assert resp.total == 2
        actions = {e.action for e in resp.entries}
        assert actions == {"arm", "disarm"}
        assert resp.entries[0].timestamp >= resp.entries[-1].timestamp

    @pytest.mark.asyncio
    async def test_audit_log_switch_id_filter(self) -> None:
        body = ArmRequestBody(requested_by="op")
        await arm_kill_switch(KillSwitchId.KILL_PER_VENUE_BYBIT, body)
        await arm_kill_switch(KillSwitchId.KILL_PER_VENUE_DERIBIT, body)
        resp = await get_kill_switch_audit_log(
            switch_id=KillSwitchId.KILL_PER_VENUE_BYBIT,
            start_date=None,
            end_date=None,
            page=1,
            page_size=50,
        )
        assert resp.total == 1
        assert resp.entries[0].switch_id == KillSwitchId.KILL_PER_VENUE_BYBIT

    @pytest.mark.asyncio
    async def test_audit_log_pagination(self) -> None:
        body = ArmRequestBody(requested_by="op")
        for sid in (
            KillSwitchId.KILL_PER_VENUE_BYBIT,
            KillSwitchId.KILL_PER_VENUE_DERIBIT,
            KillSwitchId.KILL_PER_VENUE_BINANCE,
        ):
            await arm_kill_switch(sid, body)
        page1 = await get_kill_switch_audit_log(
            switch_id=None, start_date=None, end_date=None, page=1, page_size=2
        )
        page2 = await get_kill_switch_audit_log(
            switch_id=None, start_date=None, end_date=None, page=2, page_size=2
        )
        assert page1.total == 3
        assert len(page1.entries) == 2
        assert len(page2.entries) == 1


# ---------------------------------------------------------------------------
# TestClient — auth gate + body validation + routing
# ---------------------------------------------------------------------------


class TestAuthGate:
    def test_arm_with_disabled_auth_returns_200(self, app_with_auth_disabled: FastAPI) -> None:
        client = TestClient(app_with_auth_disabled)
        resp = client.post(
            "/api/kill-switch/KILL_PER_VENUE_ASTER/arm",
            json={"requested_by": "op"},
        )
        assert resp.status_code == 200, resp.text
        payload = resp.json()
        assert payload["switch_id"] == "KILL_PER_VENUE_ASTER"
        assert payload["provenance"] == "OPERATOR_MANUAL"

    def test_arm_without_api_key_rejected_when_auth_enabled(
        self, app_with_auth_enabled: FastAPI
    ) -> None:
        """No ``X-API-Key`` header → verify_api_key raises 401."""
        from unittest.mock import MagicMock

        mock_cfg = MagicMock()
        mock_cfg.api_key = "secret-key"
        with (
            patch("deployment_api.auth.DISABLE_AUTH", False),
            patch("deployment_api.auth._auth_cfg", mock_cfg),
            patch("deployment_api.auth.log_event"),
        ):
            client = TestClient(app_with_auth_enabled, raise_server_exceptions=False)
            resp = client.post(
                "/api/kill-switch/KILL_PER_VENUE_BYBIT/arm",
                json={"requested_by": "op"},
            )
            assert resp.status_code == 401

    def test_arm_with_valid_api_key_accepted(self, app_with_auth_enabled: FastAPI) -> None:
        from unittest.mock import MagicMock

        mock_cfg = MagicMock()
        mock_cfg.api_key = "secret-key"
        with (
            patch("deployment_api.auth.DISABLE_AUTH", False),
            patch("deployment_api.auth._auth_cfg", mock_cfg),
            patch("deployment_api.auth.log_event"),
        ):
            client = TestClient(app_with_auth_enabled)
            resp = client.post(
                "/api/kill-switch/KILL_PER_VENUE_BYBIT/arm",
                json={"requested_by": "op"},
                headers={"X-API-Key": "secret-key"},
            )
            assert resp.status_code == 200, resp.text

    def test_audit_log_with_disabled_auth_returns_200(
        self, app_with_auth_disabled: FastAPI
    ) -> None:
        client = TestClient(app_with_auth_disabled)
        resp = client.get("/api/kill-switch/audit-log")
        assert resp.status_code == 200, resp.text
        payload = resp.json()
        assert payload["entries"] == []
        assert payload["total"] == 0


class TestBodyValidation:
    def test_unknown_kill_switch_id_in_path_returns_422(
        self, app_with_auth_disabled: FastAPI
    ) -> None:
        client = TestClient(app_with_auth_disabled)
        resp = client.post(
            "/api/kill-switch/NOT_A_REAL_ID/arm",
            json={"requested_by": "op"},
        )
        assert resp.status_code == 422

    def test_missing_requested_by_returns_422(self, app_with_auth_disabled: FastAPI) -> None:
        client = TestClient(app_with_auth_disabled)
        resp = client.post(
            "/api/kill-switch/KILL_PER_VENUE_BYBIT/arm",
            json={},
        )
        assert resp.status_code == 422

    def test_extra_fields_rejected(self, app_with_auth_disabled: FastAPI) -> None:
        client = TestClient(app_with_auth_disabled)
        resp = client.post(
            "/api/kill-switch/KILL_PER_VENUE_BYBIT/arm",
            json={"requested_by": "op", "unexpected_field": "x"},
        )
        assert resp.status_code == 422

    def test_disarm_unknown_id_returns_422(self, app_with_auth_disabled: FastAPI) -> None:
        client = TestClient(app_with_auth_disabled)
        resp = client.post(
            "/api/kill-switch/NOT_REAL/disarm",
            json={"requested_by": "op"},
        )
        assert resp.status_code == 422

    def test_state_get_no_body_returns_200(self, app_with_auth_disabled: FastAPI) -> None:
        client = TestClient(app_with_auth_disabled)
        resp = client.get("/api/kill-switch/state")
        assert resp.status_code == 200
        payload = resp.json()
        assert payload["armed"] == []
        assert "queried_at" in payload

    def test_list_get_no_body_returns_200(self, app_with_auth_disabled: FastAPI) -> None:
        client = TestClient(app_with_auth_disabled)
        resp = client.get("/api/kill-switch")
        assert resp.status_code == 200
        payload = resp.json()
        assert payload["armed"] == []

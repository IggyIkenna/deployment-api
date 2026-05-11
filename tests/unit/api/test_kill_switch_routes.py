"""Unit tests for ``deployment_api/routes/kill_switch_routes.py``.

Covers DR Phase 7.A surface:

- ``POST /api/kill-switch/{id}/arm`` — happy-path, idempotency, unknown id,
  bad body shape.
- ``POST /api/kill-switch/{id}/disarm`` — happy-path, no-op when never armed,
  bad body shape.
- ``GET  /api/kill-switch/state`` — empty + armed states, round-trip
  ``KillSwitchId`` mapping.
- Operator-auth-gate (``X-API-Key``) — missing header → 403/401, valid header
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
from fastapi import FastAPI, HTTPException
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
from unified_trading_library.kill_switch.bus import (
    get_kill_switch_bus,
    reset_kill_switch_bus,
)

import deployment_api.routes.kill_switch_routes as ks_routes
from deployment_api.routes.kill_switch_routes import (
    ArmRequestBody,
    arm_kill_switch,
    disarm_kill_switch,
    get_kill_switch_state,
    router as kill_switch_router,
)


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------


@pytest.fixture(autouse=True)
def _reset_bus() -> Iterator[None]:
    """Reset the process-singleton bus between tests so state doesn't leak."""
    reset_kill_switch_bus()
    yield
    reset_kill_switch_bus()


@pytest.fixture
def app_with_auth_disabled() -> FastAPI:
    """Minimal FastAPI app mounting the kill-switch router with auth bypassed."""
    app = FastAPI()
    with patch("deployment_api.auth.DISABLE_AUTH", True):
        app.include_router(kill_switch_router)
    return app


@pytest.fixture
def app_with_auth_enabled() -> FastAPI:
    """Minimal FastAPI app mounting the kill-switch router with auth enforced.

    The fixture installs a fake ``api_key`` of ``"secret-key"`` and turns auth
    on. The router was already imported with the original auth dependency, so
    the patches must apply for the test's lifetime.
    """
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
        assert bus.is_active(KillSwitchScope.VENUE, "BINANCE")

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
        assert bus.is_active(KillSwitchScope.GLOBAL)

    @pytest.mark.asyncio
    async def test_arm_archetype_maps_to_archetype_scope(self) -> None:
        body = ArmRequestBody(requested_by="op")
        await arm_kill_switch(
            KillSwitchId.KILL_PER_ARCHETYPE_CARRY_STAKED_BASIS, body
        )
        bus = get_kill_switch_bus()
        assert bus.is_active(KillSwitchScope.ARCHETYPE, "CARRY_STAKED_BASIS")

    @pytest.mark.asyncio
    async def test_arm_idempotent_second_call_returns_event(self) -> None:
        body = ArmRequestBody(requested_by="op")
        event1 = await arm_kill_switch(KillSwitchId.KILL_PER_VENUE_BYBIT, body)
        event2 = await arm_kill_switch(KillSwitchId.KILL_PER_VENUE_BYBIT, body)
        # Both return successful armed events; bus is idempotent on re-fire.
        assert event1.switch_id == event2.switch_id == KillSwitchId.KILL_PER_VENUE_BYBIT


class TestArmMappingErrors:
    """``_resolve_scope`` raises 500 when a new KillSwitchId lacks a map entry."""

    @pytest.mark.asyncio
    async def test_missing_map_entry_raises_500(self) -> None:
        body = ArmRequestBody(requested_by="op")
        with patch.dict(ks_routes._KILL_SWITCH_ID_SCOPE_MAP, {}, clear=True):
            with pytest.raises(HTTPException) as exc:
                await arm_kill_switch(KillSwitchId.KILL_PER_VENUE_BYBIT, body)
            assert exc.value.status_code == 500
            assert "KillSwitchId=KILL_PER_VENUE_BYBIT" in exc.value.detail


# ---------------------------------------------------------------------------
# Direct handler tests — POST /disarm
# ---------------------------------------------------------------------------


class TestDisarm:
    @pytest.mark.asyncio
    async def test_disarm_returns_disarm_event_with_manual_unkill(self) -> None:
        body = ArmRequestBody(requested_by="op")
        # arm first so disarm has something to clear
        await arm_kill_switch(KillSwitchId.KILL_PER_VENUE_BYBIT, body)
        event = await disarm_kill_switch(KillSwitchId.KILL_PER_VENUE_BYBIT, body)
        assert isinstance(event, KillSwitchDisarmEvent)
        assert event.switch_id == KillSwitchId.KILL_PER_VENUE_BYBIT
        assert event.disarmed_by == "op"
        assert event.recovery_mode == BreakerRecoveryMode.MANUAL_UNKILL
        assert event.cooldown_seconds_elapsed is None

    @pytest.mark.asyncio
    async def test_disarm_when_never_armed_returns_event(self) -> None:
        """Bus clear() is a no-op when not fired; the route still returns 200."""
        body = ArmRequestBody(requested_by="op")
        event = await disarm_kill_switch(KillSwitchId.KILL_PER_VENUE_OKX, body)
        assert event.switch_id == KillSwitchId.KILL_PER_VENUE_OKX
        bus = get_kill_switch_bus()
        assert not bus.is_active(KillSwitchScope.VENUE, "OKX")

    @pytest.mark.asyncio
    async def test_disarm_clears_bus_state(self) -> None:
        body = ArmRequestBody(requested_by="op")
        await arm_kill_switch(KillSwitchId.KILL_PER_VENUE_HYPERLIQUID, body)
        bus = get_kill_switch_bus()
        assert bus.is_active(KillSwitchScope.VENUE, "HYPERLIQUID")
        await disarm_kill_switch(KillSwitchId.KILL_PER_VENUE_HYPERLIQUID, body)
        assert not bus.is_active(KillSwitchScope.VENUE, "HYPERLIQUID")


# ---------------------------------------------------------------------------
# Direct handler tests — GET /state
# ---------------------------------------------------------------------------


class TestState:
    @pytest.mark.asyncio
    async def test_state_empty_when_nothing_armed(self) -> None:
        resp = await get_kill_switch_state()
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

    @pytest.mark.asyncio
    async def test_state_round_trips_global_kill(self) -> None:
        body = ArmRequestBody(requested_by="op")
        await arm_kill_switch(KillSwitchId.KILL_ALL_LIVE, body)
        resp = await get_kill_switch_state()
        assert len(resp.armed) == 1
        assert resp.armed[0].switch_id == KillSwitchId.KILL_ALL_LIVE
        assert resp.armed[0].scope == KillSwitchScope.GLOBAL
        assert resp.armed[0].scope_key is None


# ---------------------------------------------------------------------------
# TestClient — auth gate + body validation
# ---------------------------------------------------------------------------


class TestAuthGate:
    def test_arm_with_disabled_auth_returns_200(
        self, app_with_auth_disabled: FastAPI
    ) -> None:
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
        # Force auth on for the duration of this request.
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

    def test_arm_with_valid_api_key_accepted(
        self, app_with_auth_enabled: FastAPI
    ) -> None:
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

    def test_missing_requested_by_returns_422(
        self, app_with_auth_disabled: FastAPI
    ) -> None:
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

    def test_disarm_unknown_id_returns_422(
        self, app_with_auth_disabled: FastAPI
    ) -> None:
        client = TestClient(app_with_auth_disabled)
        resp = client.post(
            "/api/kill-switch/NOT_REAL/disarm",
            json={"requested_by": "op"},
        )
        assert resp.status_code == 422

    def test_state_get_no_body_returns_200(
        self, app_with_auth_disabled: FastAPI
    ) -> None:
        client = TestClient(app_with_auth_disabled)
        resp = client.get("/api/kill-switch/state")
        assert resp.status_code == 200
        payload = resp.json()
        assert payload["armed"] == []
        assert "queried_at" in payload

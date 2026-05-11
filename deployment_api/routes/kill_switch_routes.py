"""Kill-switch arm / disarm / state REST surface (DR Phase 7.A).

Endpoints exposed under ``/api/kill-switch`` and routed through the global
:func:`deployment_api.auth.verify_api_key` operator-auth dependency (the same
``X-API-Key`` header used elsewhere in deployment-api):

- ``POST /api/kill-switch/{kill_switch_id}/arm`` — accepts a
  :class:`unified_api_contracts.KillSwitchArmRequest`-shaped body, publishes
  ``fire()`` to the UTL ``KillSwitchBus`` (Phase 2.A primitive), and returns the
  emitted :class:`KillSwitchArmedEvent`.
- ``POST /api/kill-switch/{kill_switch_id}/disarm`` — symmetric ``clear()``
  publish, returns :class:`KillSwitchDisarmEvent`.
- ``GET  /api/kill-switch/state`` — read-only enumeration of currently-armed
  switches with arm timestamps + provenance.

The UTL bus currently exposes ``fire(scope, scope_key)``/``clear(scope, scope_key)``
addressed by ``KillSwitchScope`` (workspace canonical), not the
finer-grained :class:`KillSwitchId` enum. This module maps from the
operator-facing ``KillSwitchId`` (e.g. ``KILL_PER_VENUE_BYBIT``) to the bus's
``(KillSwitchScope, scope_key)`` shape (e.g. ``(VENUE, "BYBIT")``) so the route
handler stays in the UAC vocabulary while the bus stays in its native shape.
Sub-J's parallel audit-log extension layers persistence on top of the same
``fire()``/``clear()`` callsites.

Plan: ``unified-trading-pm/plans/active/disaster_recovery_circuit_breakers_2026_05_10.md``
Phase 7.A.
"""

from __future__ import annotations

import logging
from datetime import datetime, timezone

from fastapi import APIRouter, Depends, HTTPException
from pydantic import BaseModel, ConfigDict, Field
from unified_api_contracts import (
    KillSwitchArmedEvent,
    KillSwitchArmRequest,
    KillSwitchDisarmEvent,
    KillSwitchId,
    KillSwitchProvenance,
)
from unified_api_contracts.canonical.crosscutting.circuit_breaker import (  # noqa: qg-deep-import
    BreakerRecoveryMode,
)
from unified_api_contracts.internal.domain.deployment_service import KillSwitchScope
from unified_trading_library.kill_switch.bus import (
    KillSwitchBus,
    get_kill_switch_bus,
)

from deployment_api.auth import verify_api_key

logger = logging.getLogger(__name__)

# Operator-auth-gated router (every endpoint requires X-API-Key).
router = APIRouter(
    prefix="/api/kill-switch",
    tags=["Kill Switch"],
    dependencies=[Depends(verify_api_key)],
)

# ---------------------------------------------------------------------------
# KillSwitchId -> (KillSwitchScope, scope_key) mapping
# ---------------------------------------------------------------------------

# The UTL ``KillSwitchBus`` is addressed by ``(KillSwitchScope, scope_key)``
# (workspace canonical). Operator-facing ``KillSwitchId`` is finer-grained;
# resolve via deterministic prefix parsing keyed on the closed set declared in
# UAC ``canonical/crosscutting/kill_switch.py``. The kill_switch.py docstring
# documents this same mapping (KILL_ALL_LIVE → GLOBAL, KILL_PER_ARCHETYPE_* →
# ARCHETYPE, KILL_PER_VENUE_* → VENUE, KILL_PER_ASSET_GROUP_* → GLOBAL filtered
# by asset_group).
_KILL_SWITCH_ID_SCOPE_MAP: dict[KillSwitchId, tuple[KillSwitchScope, str | None]] = {
    KillSwitchId.KILL_ALL_LIVE: (KillSwitchScope.GLOBAL, None),
    KillSwitchId.KILL_PER_ARCHETYPE_CARRY_STAKED_BASIS: (
        KillSwitchScope.ARCHETYPE,
        "CARRY_STAKED_BASIS",
    ),
    KillSwitchId.KILL_PER_ARCHETYPE_ARBITRAGE_PRICE_DISPERSION: (
        KillSwitchScope.ARCHETYPE,
        "ARBITRAGE_PRICE_DISPERSION",
    ),
    KillSwitchId.KILL_PER_VENUE_BYBIT: (KillSwitchScope.VENUE, "BYBIT"),
    KillSwitchId.KILL_PER_VENUE_DERIBIT: (KillSwitchScope.VENUE, "DERIBIT"),
    KillSwitchId.KILL_PER_VENUE_BINANCE: (KillSwitchScope.VENUE, "BINANCE"),
    KillSwitchId.KILL_PER_VENUE_OKX: (KillSwitchScope.VENUE, "OKX"),
    KillSwitchId.KILL_PER_VENUE_HYPERLIQUID: (KillSwitchScope.VENUE, "HYPERLIQUID"),
    KillSwitchId.KILL_PER_VENUE_ASTER: (KillSwitchScope.VENUE, "ASTER"),
    # Per-asset-group: bus has no ASSET_GROUP scope; fold into GLOBAL with the
    # asset_group as the key (consumers filter by metadata).
    KillSwitchId.KILL_PER_ASSET_GROUP_CEFI: (KillSwitchScope.GLOBAL, "asset_group=cefi"),
    KillSwitchId.KILL_PER_ASSET_GROUP_DEFI: (KillSwitchScope.GLOBAL, "asset_group=defi"),
}


def _resolve_scope(switch_id: KillSwitchId) -> tuple[KillSwitchScope, str | None]:
    """Return the ``(scope, scope_key)`` pair the UTL bus expects for an id."""
    try:
        return _KILL_SWITCH_ID_SCOPE_MAP[switch_id]
    except KeyError as err:
        # Closed-set enum + closed-set map; missing key here means a new
        # KillSwitchId was added without a map update. Surface loudly.
        raise HTTPException(
            status_code=500,
            detail=f"No bus scope mapping for KillSwitchId={switch_id.value}",
        ) from err


# Reverse map for the GET /state endpoint: (scope, scope_key) → KillSwitchId.
_SCOPE_TO_KILL_SWITCH_ID: dict[tuple[KillSwitchScope, str | None], KillSwitchId] = {
    pair: sid for sid, pair in _KILL_SWITCH_ID_SCOPE_MAP.items()
}


# ---------------------------------------------------------------------------
# Request / response schemas (route-local — these are HTTP-shape sugar around
# the UAC events; the UAC events themselves are the durable contract)
# ---------------------------------------------------------------------------


class ArmRequestBody(BaseModel):
    """HTTP body for the arm/disarm endpoints.

    ``requested_by`` carries the operator identity for the audit log + the UAC
    ``KillSwitchArmRequest.requested_by`` field. ``metadata`` is free-form
    structured fields forwarded to bus subscribers via the emitted event.
    """

    model_config = ConfigDict(extra="forbid")

    requested_by: str = Field(..., min_length=1, max_length=256)
    metadata: dict[str, str] | None = Field(default=None)


class KillSwitchStateEntry(BaseModel):
    """One row in the ``GET /state`` response.

    Carries the operator-facing ``KillSwitchId`` plus the wire ``(scope,
    scope_key)`` pair (so UI can render either axis without rerunning the
    mapping).
    """

    model_config = ConfigDict(frozen=True, extra="forbid")

    switch_id: KillSwitchId
    scope: KillSwitchScope
    scope_key: str | None


class KillSwitchStateResponse(BaseModel):
    """Read-only state of the bus at request time."""

    model_config = ConfigDict(frozen=True, extra="forbid")

    armed: list[KillSwitchStateEntry]
    queried_at: datetime


# ---------------------------------------------------------------------------
# Handlers
# ---------------------------------------------------------------------------


def _utc_now() -> datetime:
    return datetime.now(timezone.utc)


@router.post("/{kill_switch_id}/arm", response_model=KillSwitchArmedEvent)
async def arm_kill_switch(
    kill_switch_id: KillSwitchId,
    body: ArmRequestBody,
) -> KillSwitchArmedEvent:
    """Arm a kill switch by ``KillSwitchId``.

    Builds a :class:`KillSwitchArmRequest`, publishes ``fire()`` to the UTL
    ``KillSwitchBus``, and returns the :class:`KillSwitchArmedEvent` emitted to
    subscribers. Operator identity comes from the request body's
    ``requested_by`` field; per UAC docstring, ``OPERATOR_MANUAL`` is the
    provenance used for human-driven arming via this endpoint.

    ``KillSwitchBus.fire()`` is idempotent — re-arming an already-armed switch
    re-fires the event without erroring; this is intentional so operators can
    re-notify subscribers.
    """
    scope, scope_key = _resolve_scope(kill_switch_id)
    metadata = dict(body.metadata) if body.metadata is not None else {}
    arm_timestamp = _utc_now()

    request = KillSwitchArmRequest(
        switch_id=kill_switch_id,
        provenance=KillSwitchProvenance.OPERATOR_MANUAL,
        requested_by=body.requested_by,
        arm_timestamp=arm_timestamp,
        metadata=metadata,
    )

    bus: KillSwitchBus = get_kill_switch_bus()
    reason = f"OPERATOR_MANUAL arm of {kill_switch_id.value}"
    bus.fire(scope, scope_key, reason=reason, fired_by=body.requested_by)

    event = KillSwitchArmedEvent(
        switch_id=request.switch_id,
        provenance=request.provenance,
        armed_at=arm_timestamp,
        requested_by=request.requested_by,
        metadata=request.metadata,
    )
    logger.info(
        "kill_switch armed: id=%s scope=%s scope_key=%s requested_by=%s",
        kill_switch_id.value,
        scope.value,
        scope_key,
        body.requested_by,
    )
    return event


@router.post("/{kill_switch_id}/disarm", response_model=KillSwitchDisarmEvent)
async def disarm_kill_switch(
    kill_switch_id: KillSwitchId,
    body: ArmRequestBody,
) -> KillSwitchDisarmEvent:
    """Disarm a kill switch by ``KillSwitchId``.

    Returns a :class:`KillSwitchDisarmEvent` with
    ``recovery_mode=MANUAL_UNKILL`` (operator-driven). Bus's ``clear()`` is a
    no-op when the (scope, scope_key) wasn't fired — the route still returns a
    successful disarm event so the client can drive idempotent UI flows; the
    bus log distinguishes the no-op case.
    """
    scope, scope_key = _resolve_scope(kill_switch_id)
    disarmed_at = _utc_now()
    metadata = dict(body.metadata) if body.metadata is not None else {}

    bus: KillSwitchBus = get_kill_switch_bus()
    reason = f"OPERATOR_MANUAL disarm of {kill_switch_id.value}"
    bus.clear(scope, scope_key, reason=reason, fired_by=body.requested_by)

    event = KillSwitchDisarmEvent(
        switch_id=kill_switch_id,
        disarmed_at=disarmed_at,
        disarmed_by=body.requested_by,
        recovery_mode=BreakerRecoveryMode.MANUAL_UNKILL,
        cooldown_seconds_elapsed=None,
        metadata=metadata,
    )
    logger.info(
        "kill_switch disarmed: id=%s scope=%s scope_key=%s disarmed_by=%s",
        kill_switch_id.value,
        scope.value,
        scope_key,
        body.requested_by,
    )
    return event


@router.get("/state", response_model=KillSwitchStateResponse)
async def get_kill_switch_state() -> KillSwitchStateResponse:
    """Return every currently-armed kill switch from the bus.

    Maps the bus's ``(scope, scope_key)`` snapshot back onto ``KillSwitchId``
    where the mapping is round-trippable. Bus rows that don't round-trip (e.g.
    ad-hoc scope-keys outside the closed enum) are omitted — the operator
    surface is the closed-set ``KillSwitchId`` enum.
    """
    bus: KillSwitchBus = get_kill_switch_bus()
    active = bus.active_scopes()
    queried_at = _utc_now()

    armed: list[KillSwitchStateEntry] = []
    for scope, keys in active.items():
        for scope_key in keys:
            pair = (scope, scope_key)
            switch_id = _SCOPE_TO_KILL_SWITCH_ID.get(pair)
            if switch_id is None:
                # Bus row without a canonical KillSwitchId mapping — skip; the
                # canonical UI surface is the closed-set enum.
                continue
            armed.append(
                KillSwitchStateEntry(
                    switch_id=switch_id,
                    scope=scope,
                    scope_key=scope_key,
                )
            )

    return KillSwitchStateResponse(armed=armed, queried_at=queried_at)

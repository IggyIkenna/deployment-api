"""Kill-switch arm / disarm / state / audit-log REST surface (DR Phase 7.A).

Endpoints exposed under ``/api/kill-switch`` and routed through the global
:func:`deployment_api.auth.verify_api_key` operator-auth dependency (the same
``X-API-Key`` header used elsewhere in deployment-api):

- ``POST /api/kill-switch/{kill_switch_id}/arm`` — accepts a body carrying the
  operator identity + metadata, builds a
  :class:`unified_api_contracts.KillSwitchArmRequest` with
  ``provenance=OPERATOR_MANUAL``, calls
  :meth:`unified_trading_library.kill_switch.KillSwitchBus.arm`, and returns the
  emitted :class:`KillSwitchArmedEvent`.
- ``POST /api/kill-switch/{kill_switch_id}/disarm`` — symmetric
  :meth:`KillSwitchBus.disarm` (``recovery_mode=MANUAL_UNKILL``), returns the
  emitted :class:`KillSwitchDisarmEvent` (synthesised when the switch wasn't
  armed, so clients can drive idempotent UI flows).
- ``GET  /api/kill-switch`` and ``GET /api/kill-switch/state`` — read-only
  enumeration of currently-armed switches from :meth:`KillSwitchBus.armed_switch_ids`
  (the typed API; ``/state`` is kept as a back-compat alias the deployment-ui
  polls every 5s).
- ``GET  /api/kill-switch/audit-log`` — recent arm/disarm transitions from the
  bus's audit-log writer. The bus is wired with an in-process
  :class:`unified_trading_library.kill_switch.InMemoryAuditLogWriter` at module
  import so the endpoint always has a backing store; production deployments can
  swap a :class:`ParquetAuditLogWriter` via the bus once a cloud-backed log is
  desired. Supports ``switch_id`` / ``start_date`` / ``end_date`` filters +
  ``page`` / ``page_size`` pagination, matching the deployment-ui AuditLogViewer.

Architecture: routes are thin wrappers around the UTL ``KillSwitchBus`` typed
API; the ``KillSwitchId`` → ``(KillSwitchScope, scope_key)`` mapping lives in
UTL (:func:`unified_trading_library.kill_switch.map_switch_id_to_scope`), not
here.

Plan: ``unified-trading-pm/plans/active/disaster_recovery_circuit_breakers_2026_05_10.md``
Phase 7.A.
"""

from __future__ import annotations

import logging
from datetime import UTC, date, datetime

from fastapi import APIRouter, Depends, HTTPException, Query
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
from unified_trading_library import (
    InMemoryAuditLogWriter,
    KillSwitchBus,
    get_kill_switch_bus,
    map_switch_id_to_scope,
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
# Audit-log writer wiring
# ---------------------------------------------------------------------------

# In-process audit-log writer captured at module import so the audit-log
# endpoint always has a backing store. The bus singleton may be reset in tests
# (``reset_kill_switch_bus()``); ``_ensure_audit_writer_attached`` re-attaches
# the writer on every request so a reset doesn't orphan it. Production can swap
# a ``ParquetAuditLogWriter`` via ``bus.set_audit_log_writer(...)``.
_AUDIT_LOG_WRITER: InMemoryAuditLogWriter = InMemoryAuditLogWriter()


def _ensure_audit_writer_attached(bus: KillSwitchBus) -> None:
    """Attach the module audit-log writer to ``bus`` if not already set."""
    bus.set_audit_log_writer(_AUDIT_LOG_WRITER)


# ---------------------------------------------------------------------------
# Request / response schemas (route-local HTTP-shape sugar around the UAC
# events; the UAC events themselves are the durable contract)
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
    """One row in the ``GET /`` (and ``GET /state``) response.

    Carries the operator-facing ``KillSwitchId`` plus the wire ``(scope,
    scope_key)`` pair (so UI can render either axis without rerunning the
    mapping) plus arm provenance + timestamp + requested_by.
    """

    model_config = ConfigDict(frozen=True, extra="forbid")

    switch_id: KillSwitchId
    scope: KillSwitchScope
    scope_key: str | None
    provenance: KillSwitchProvenance
    armed_at: datetime
    requested_by: str
    metadata: dict[str, str]


class KillSwitchStateResponse(BaseModel):
    """Read-only state of the bus at request time."""

    model_config = ConfigDict(frozen=True, extra="forbid")

    armed: list[KillSwitchStateEntry]
    queried_at: datetime


class KillSwitchAuditEntry(BaseModel):
    """One row in the ``GET /audit-log`` response.

    Mirrors the deployment-ui ``AuditLogViewer`` contract: a normalised view of
    a :class:`KillSwitchArmedEvent` or :class:`KillSwitchDisarmEvent`.
    """

    model_config = ConfigDict(frozen=True, extra="forbid")

    entry_id: str
    switch_id: KillSwitchId
    action: str  # "arm" | "disarm"
    provenance: KillSwitchProvenance | None
    requested_by: str | None
    disarmed_by: str | None
    recovery_mode: BreakerRecoveryMode | None
    timestamp: datetime
    metadata: dict[str, str]


class KillSwitchAuditResponse(BaseModel):
    """Paginated audit-log response."""

    model_config = ConfigDict(frozen=True, extra="forbid")

    entries: list[KillSwitchAuditEntry]
    total: int
    page: int
    page_size: int
    note: str | None = None


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _utc_now() -> datetime:
    return datetime.now(UTC)


def _scope_for(switch_id: KillSwitchId) -> tuple[KillSwitchScope, str | None]:
    """Resolve ``(scope, scope_key)`` via the UTL SSOT mapping.

    The mapping lives in UTL (``map_switch_id_to_scope``); a missing entry there
    means a new ``KillSwitchId`` shipped without a UTL map update — surface as a
    500 so it's caught loudly rather than silently mis-scoped.
    """
    try:
        return map_switch_id_to_scope(switch_id)
    except (KeyError, ValueError) as err:
        raise HTTPException(
            status_code=500,
            detail=f"No bus scope mapping for KillSwitchId={switch_id.value}",
        ) from err


def _event_id_for(event: KillSwitchArmedEvent | KillSwitchDisarmEvent) -> str:
    """Stable per-event id for the audit-log row (same shape as UTL's writer)."""
    if isinstance(event, KillSwitchArmedEvent):
        ts = event.armed_at
        action = "arm"
    else:
        ts = event.disarmed_at
        action = "disarm"
    return f"{ts.strftime('%Y%m%dT%H%M%S%f')}-{event.switch_id.value}-{action}"


def _audit_entry(event: KillSwitchArmedEvent | KillSwitchDisarmEvent) -> KillSwitchAuditEntry:
    """Normalise a UAC arm/disarm event into a flat audit-log row."""
    if isinstance(event, KillSwitchArmedEvent):
        return KillSwitchAuditEntry(
            entry_id=_event_id_for(event),
            switch_id=event.switch_id,
            action="arm",
            provenance=event.provenance,
            requested_by=event.requested_by,
            disarmed_by=None,
            recovery_mode=None,
            timestamp=event.armed_at,
            metadata=dict(event.metadata),
        )
    return KillSwitchAuditEntry(
        entry_id=_event_id_for(event),
        switch_id=event.switch_id,
        action="disarm",
        provenance=None,
        requested_by=None,
        disarmed_by=event.disarmed_by,
        recovery_mode=event.recovery_mode,
        timestamp=event.disarmed_at,
        metadata=dict(event.metadata),
    )


# ---------------------------------------------------------------------------
# Handlers — arm / disarm
# ---------------------------------------------------------------------------


@router.post("/{kill_switch_id}/arm", response_model=KillSwitchArmedEvent)
async def arm_kill_switch(
    kill_switch_id: KillSwitchId,
    body: ArmRequestBody,
) -> KillSwitchArmedEvent:
    """Arm a kill switch by ``KillSwitchId``.

    Builds a :class:`KillSwitchArmRequest` with ``provenance=OPERATOR_MANUAL``
    (this endpoint is the human-driven path; circuit breakers use ``BREAKER_AUTO``)
    and calls :meth:`KillSwitchBus.arm`, which marks the scope active, writes an
    audit-log row, and emits :class:`KillSwitchArmedEvent` to subscribers.
    Idempotent — re-arming overwrites prior armed state + re-emits.

    NOTE: this endpoint is the operator's UI action; *arming on a live wallet*
    is the operator's decision, gated here by ``verify_api_key``.
    """
    # Validate the id maps cleanly before touching the bus (loud 500 otherwise).
    scope, scope_key = _scope_for(kill_switch_id)
    metadata = dict(body.metadata) if body.metadata is not None else {}

    request = KillSwitchArmRequest(
        switch_id=kill_switch_id,
        provenance=KillSwitchProvenance.OPERATOR_MANUAL,
        requested_by=body.requested_by,
        arm_timestamp=_utc_now(),
        metadata=metadata,
    )

    bus: KillSwitchBus = get_kill_switch_bus()
    _ensure_audit_writer_attached(bus)
    event = bus.arm(request)

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

    Calls :meth:`KillSwitchBus.disarm` with ``recovery_mode=MANUAL_UNKILL``.
    The bus returns ``None`` when the switch wasn't armed; the route synthesises
    a :class:`KillSwitchDisarmEvent` in that case (and does *not* write an
    audit-log row for the no-op) so clients can drive idempotent UI flows.
    """
    # Validate the id maps cleanly first (loud 500 on a missing UTL map entry).
    scope, scope_key = _scope_for(kill_switch_id)
    metadata = dict(body.metadata) if body.metadata is not None else {}

    bus: KillSwitchBus = get_kill_switch_bus()
    _ensure_audit_writer_attached(bus)
    event = bus.disarm(
        kill_switch_id,
        disarmed_by=body.requested_by,
        recovery_mode=BreakerRecoveryMode.MANUAL_UNKILL,
        cooldown_seconds_elapsed=None,
        metadata=metadata,
    )

    if event is None:
        # Switch wasn't armed — no-op. Return a successful disarm event so the
        # client can stay idempotent; bus log records the no-op.
        event = KillSwitchDisarmEvent(
            switch_id=kill_switch_id,
            disarmed_at=_utc_now(),
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


# ---------------------------------------------------------------------------
# Handlers — state listing
# ---------------------------------------------------------------------------


def _build_state_response() -> KillSwitchStateResponse:
    """Snapshot the bus's typed armed-switch state into the response model."""
    bus: KillSwitchBus = get_kill_switch_bus()
    _ensure_audit_writer_attached(bus)
    armed_events = bus.armed_switch_ids()
    queried_at = _utc_now()

    armed: list[KillSwitchStateEntry] = []
    for switch_id, armed_event in armed_events.items():
        try:
            scope, scope_key = map_switch_id_to_scope(switch_id)
        except (KeyError, ValueError):
            # Switch armed under an id that lost its UTL mapping — skip; the
            # operator surface is the closed-set KillSwitchId enum.
            continue
        armed.append(
            KillSwitchStateEntry(
                switch_id=switch_id,
                scope=scope,
                scope_key=scope_key,
                provenance=armed_event.provenance,
                armed_at=armed_event.armed_at,
                requested_by=armed_event.requested_by,
                metadata=dict(armed_event.metadata),
            )
        )

    return KillSwitchStateResponse(armed=armed, queried_at=queried_at)


@router.get("", response_model=KillSwitchStateResponse)
async def list_armed_kill_switches() -> KillSwitchStateResponse:
    """Return every currently-armed kill switch from the bus (typed API)."""
    return _build_state_response()


@router.get("/state", response_model=KillSwitchStateResponse)
async def get_kill_switch_state() -> KillSwitchStateResponse:
    """Back-compat alias for ``GET /api/kill-switch`` (deployment-ui polls this)."""
    return _build_state_response()


# ---------------------------------------------------------------------------
# Handlers — audit log
# ---------------------------------------------------------------------------


@router.get("/audit-log", response_model=KillSwitchAuditResponse)
async def get_kill_switch_audit_log(
    switch_id: KillSwitchId | None = Query(
        default=None, description="Filter to a specific KillSwitchId."
    ),
    start_date: date | None = Query(
        default=None, description="Inclusive lower bound on event date (YYYY-MM-DD)."
    ),
    end_date: date | None = Query(
        default=None, description="Inclusive upper bound on event date (YYYY-MM-DD)."
    ),
    page: int = Query(default=1, ge=1, description="1-indexed page number."),
    page_size: int = Query(default=50, ge=1, le=500, description="Rows per page."),
) -> KillSwitchAuditResponse:
    """Return recent kill-switch arm/disarm transitions from the audit log.

    Reads from the in-process :class:`InMemoryAuditLogWriter` wired into the
    bus at module import. Newest-first; filtered by ``switch_id`` /
    ``start_date`` / ``end_date`` and paginated. When no writer captured any
    events yet, returns an empty page with an explanatory ``note``.
    """
    # Make sure the writer is attached even if the bus was reset since import.
    _ensure_audit_writer_attached(get_kill_switch_bus())

    rows = [_audit_entry(ev) for ev in _AUDIT_LOG_WRITER.events]
    # Newest first.
    rows.sort(key=lambda r: r.timestamp, reverse=True)

    if switch_id is not None:
        rows = [r for r in rows if r.switch_id == switch_id]
    if start_date is not None:
        rows = [r for r in rows if r.timestamp.date() >= start_date]
    if end_date is not None:
        rows = [r for r in rows if r.timestamp.date() <= end_date]

    total = len(rows)
    offset = (page - 1) * page_size
    page_rows = rows[offset : offset + page_size]

    note: str | None = None
    if total == 0:
        note = (
            "No kill-switch arm/disarm events recorded yet. The audit log is "
            "backed by an in-process writer; events appear here after the first "
            "arm/disarm via this API in the current process."
        )

    return KillSwitchAuditResponse(
        entries=page_rows,
        total=total,
        page=page,
        page_size=page_size,
        note=note,
    )

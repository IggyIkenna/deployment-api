"""GET /api/vm/{vm_name}/health — aggregate VM health status.

Derives green/amber/red/unknown from GCS event history recency + terminal status.
"""

from __future__ import annotations

import logging
from datetime import UTC, datetime, timedelta
from typing import Literal

from fastapi import APIRouter, Query
from pydantic import BaseModel
from unified_api_contracts.internal import VMEventListResult, VMLifecycleEvent

from deployment_api.deployment_api_config import DeploymentApiConfig
from deployment_api.routes.vm_events import (
    fetch_vm_events_window,
    infer_service_from_vm_name,
)

router = APIRouter()
logger = logging.getLogger(__name__)

_cfg = DeploymentApiConfig()

_WARN_SECONDS = 3600  # 1 h without heartbeat → amber
_ERROR_SECONDS = 7200  # 2 h → red

_TERMINAL_EVENTS = frozenset({"STOPPED", "FAILED", "ERROR", "SHUTDOWN"})

VmHealthState = Literal["green", "amber", "red", "unknown"]


class VmHealthResult(BaseModel):  # CORRECT-LOCAL: FastAPI API contract model
    """Aggregate health summary for a single VM."""

    vm_name: str
    service: str
    state: VmHealthState
    last_started_at: str | None
    last_event_at: str | None
    last_event_type: str | None
    expected_next_heartbeat_at: str | None
    threshold_warn_seconds: int
    threshold_error_seconds: int
    is_terminal: bool
    scanned_hours: int
    message: str


def _iso(dt: datetime | None) -> str | None:
    if dt is None:
        return None
    return dt.isoformat(timespec="seconds").replace("+00:00", "Z")


def derive_vm_health_state(
    last_event_at: datetime | None,
    is_terminal: bool,
    now: datetime,
) -> tuple[VmHealthState, str]:
    if last_event_at is None:
        return "unknown", "No events found in scan window."
    if is_terminal:
        return "red", "VM has reached a terminal state (STOPPED/FAILED)."
    age_s = int((now - last_event_at).total_seconds())
    if age_s < _WARN_SECONDS:
        return "green", f"Last event {age_s}s ago — within healthy threshold."
    if age_s < _ERROR_SECONDS:
        return "amber", f"Last event {age_s}s ago — stale (warn threshold {_WARN_SECONDS}s)."
    return "red", f"Last event {age_s}s ago — critically stale (error threshold {_ERROR_SECONDS}s)."


def summarise_vm_health(
    vm_name: str,
    service: str,
    events: list[VMLifecycleEvent],
    scanned_hours: int,
    now: datetime,
) -> VmHealthResult:
    if not events:
        return VmHealthResult(
            vm_name=vm_name,
            service=service,
            state="unknown",
            last_started_at=None,
            last_event_at=None,
            last_event_type=None,
            expected_next_heartbeat_at=None,
            threshold_warn_seconds=_WARN_SECONDS,
            threshold_error_seconds=_ERROR_SECONDS,
            is_terminal=False,
            scanned_hours=scanned_hours,
            message="No events found in scan window.",
        )

    sorted_evts = sorted(events, key=lambda e: e.timestamp)
    last_evt = sorted_evts[-1]
    last_event_at: datetime = last_evt.timestamp
    last_event_type: str = last_evt.event
    is_terminal = last_event_type in _TERMINAL_EVENTS

    last_started: datetime | None = None
    for evt in sorted_evts:
        if evt.event == "STARTED":
            last_started = evt.timestamp

    expected_next: datetime | None = None
    if last_started is not None and not is_terminal:
        expected_next = last_event_at + timedelta(seconds=_WARN_SECONDS)

    state, message = derive_vm_health_state(last_event_at, is_terminal, now)

    return VmHealthResult(
        vm_name=vm_name,
        service=service,
        state=state,
        last_started_at=_iso(last_started),
        last_event_at=_iso(last_event_at),
        last_event_type=last_event_type,
        expected_next_heartbeat_at=_iso(expected_next),
        threshold_warn_seconds=_WARN_SECONDS,
        threshold_error_seconds=_ERROR_SECONDS,
        is_terminal=is_terminal,
        scanned_hours=scanned_hours,
        message=message,
    )


def _fetch_events_for_window(
    vm_name: str,
    service: str,
    since_dt: datetime,
    now: datetime,
) -> tuple[list[VMLifecycleEvent], int]:
    today = now.strftime("%Y-%m-%d")
    since_date = since_dt.strftime("%Y-%m-%d")
    end_hour_today = now.hour

    if since_date == today:
        result: VMEventListResult = fetch_vm_events_window(
            vm_name=vm_name,
            service=service,
            date=today,
            start_hour=since_dt.hour,
            end_hour=end_hour_today,
        )
        return list(result.events), len(result.hours_scanned)

    result_prev: VMEventListResult = fetch_vm_events_window(
        vm_name=vm_name,
        service=service,
        date=since_date,
        start_hour=since_dt.hour,
        end_hour=23,
    )
    result_today: VMEventListResult = fetch_vm_events_window(
        vm_name=vm_name,
        service=service,
        date=today,
        start_hour=0,
        end_hour=end_hour_today,
    )
    all_events = list(result_prev.events) + list(result_today.events)
    scanned = len(result_prev.hours_scanned) + len(result_today.hours_scanned)
    return all_events, scanned


@router.get("/vm/{vm_name}/health", response_model=VmHealthResult)
def get_vm_health(
    vm_name: str,
    service: str | None = Query(None, description="Service name. Inferred from vm_name prefix if omitted."),
    scan_hours: int = Query(24, ge=1, le=72, description="Trailing hours of events to scan (default 24)"),
) -> VmHealthResult:
    """Return aggregate health state for a VM based on its GCS event history."""
    resolved_service = service if service is not None else infer_service_from_vm_name(vm_name)
    now = datetime.now(UTC)

    if _cfg.is_mock_mode():
        # Build a mock "running" VM — 2 non-terminal events so state=green.
        mock_events = [
            VMLifecycleEvent(
                event="STARTED",
                service=resolved_service,
                timestamp=now - timedelta(minutes=5),
            ),
            VMLifecycleEvent(
                event="INSTRUMENT_PROCESSED",
                service=resolved_service,
                timestamp=now - timedelta(minutes=2),
            ),
        ]
        return summarise_vm_health(vm_name, resolved_service, mock_events, 1, now)

    since_dt = now - timedelta(hours=scan_hours)
    all_events, scanned_hours = _fetch_events_for_window(vm_name, resolved_service, since_dt, now)
    return summarise_vm_health(vm_name, resolved_service, all_events, scanned_hours, now)

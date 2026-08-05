# Epic: observability_master
"""Read endpoint for the durable resource-watchdog kill-event table.

Companion to the write path in ``routes/fleet.py`` (``POST /api/fleet/watchdog/kill-events``)
+ ``services/operational_data_writer.py``. Mirrors ``routes/vm_resource_history.py``'s
query-shape conventions: optional ``vm_name`` filter, ``hours`` lookback, and honest
degradation to an empty row list in mock mode / no GCP project / query failure — never a
5xx for an unconfigured or mock deployment.

Plan: unified-trading-pm/plans/active/watchdog_kill_events_deployment_observability_2026_08_05.md.
"""

from __future__ import annotations

import logging
from datetime import UTC, datetime

from fastapi import APIRouter, HTTPException, Query
from pydantic import BaseModel

from deployment_api.deployment_api_config import DeploymentApiConfig
from deployment_api.services.operational_data_queries import (
    InvalidIdentifierError,
    run_query,
    watchdog_kill_events_sql,
)

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/api/watchdog", tags=["Watchdog"])

_cfg = DeploymentApiConfig()


class WatchdogKillEventRow(BaseModel):  # CORRECT-LOCAL: FastAPI API contract model
    ts: str
    vm_name: str
    pid: int
    slot_id: str
    command: str
    reason: str
    rss_mb: int
    limit_mb: int
    pressure_level: str
    killed: bool


class WatchdogKillEventsResponse(BaseModel):  # CORRECT-LOCAL: FastAPI API contract model
    hours: int
    vm_name: str | None
    rows: list[WatchdogKillEventRow]


def _as_int_or_zero(v: object) -> int:
    return int(v) if v is not None else 0


def _as_bool(v: object) -> bool:
    return bool(v) if v is not None else False


def _as_iso_or_empty(v: object) -> str:
    """Normalize a BQ TIMESTAMP (datetime) or string to a stable ISO-8601 display string."""
    if v is None:
        return ""
    if isinstance(v, datetime):
        return v.astimezone(UTC).strftime("%Y-%m-%dT%H:%M:%SZ")
    return str(v)


@router.get("/kill-events", response_model=WatchdogKillEventsResponse)
def get_watchdog_kill_events(
    vm_name: str | None = Query(None, description="Filter to one VM; omit for all hosts"),
    hours: int = Query(24, ge=1, le=168, description="Lookback window in hours (max 7 days)"),
) -> WatchdogKillEventsResponse:
    """Recent resource-watchdog kill/violation events, newest first.

    Mirrors ``GET /api/vm-resources/rolling``'s query-shape: optional ``vm_name`` filter,
    ``hours`` lookback, honest empty row list in mock mode / no GCP project / query failure.
    """
    if _cfg.is_mock_mode() or not _cfg.effective_project_id:
        return WatchdogKillEventsResponse(hours=hours, vm_name=vm_name, rows=[])
    try:
        sql = watchdog_kill_events_sql(_cfg.require_gcp_project_id(), hours=hours, vm_name=vm_name)
    except InvalidIdentifierError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc
    try:
        raw_rows = run_query(_cfg.require_gcp_project_id(), sql)
    except Exception as exc:
        logger.warning("watchdog_kill_events query failed: %s", exc)
        return WatchdogKillEventsResponse(hours=hours, vm_name=vm_name, rows=[])
    rows = [
        WatchdogKillEventRow(
            ts=_as_iso_or_empty(r.get("ts")),
            vm_name=str(r.get("vm_name") or ""),
            pid=_as_int_or_zero(r.get("pid")),
            slot_id=str(r.get("slot_id") or ""),
            command=str(r.get("command") or ""),
            reason=str(r.get("reason") or ""),
            rss_mb=_as_int_or_zero(r.get("rss_mb")),
            limit_mb=_as_int_or_zero(r.get("limit_mb")),
            pressure_level=str(r.get("pressure_level") or ""),
            killed=_as_bool(r.get("killed")),
        )
        for r in raw_rows
    ]
    return WatchdogKillEventsResponse(hours=hours, vm_name=vm_name, rows=rows)


__all__ = ["router"]

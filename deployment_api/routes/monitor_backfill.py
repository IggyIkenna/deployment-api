"""GET /api/monitor/backfill — list running + recent EPHEMERAL_BATCH jobs.

Phase C (c1) of deployment_ui_lifecycle_tabs_2026_05_08.md.

Lists every EPHEMERAL_BATCH VM (instruments backfills, MTDS ingests, MDPS jobs,
features-* backfills, migration runs, smokes) from the deployments registry.
Filters out LONG_LIVED_LIVE prefixes (strategy-paper-*, strategy-live-*,
defi-recursive-*) so only ephemeral batch jobs appear.

Read-only; operator-auth (X-API-Key) inherited from the authenticated router.
"""

from __future__ import annotations

import logging
from datetime import UTC, datetime

from deployment_service.deployments_registry import (
    DEFAULT_BUCKET,
    DeploymentsRegistry,
)
from fastapi import APIRouter, Query
from pydantic import BaseModel

router = APIRouter()
logger = logging.getLogger(__name__)

# Prefixes whose VMs are LONG_LIVED_LIVE — exclude from backfill monitor.
# Kept in sync with VM_PREFIX_TO_BUCKET in vm_zombie_watchdog.py.
_LIVE_PREFIXES: tuple[str, ...] = (
    "strategy-paper-",
    "strategy-live-",
    "defi-recursive-",
)


def _is_backfill_vm(vm_name: str) -> bool:
    """Return True when vm_name is an EPHEMERAL_BATCH job (not a live strategy VM)."""
    return not any(vm_name.startswith(p) for p in _LIVE_PREFIXES)


class BackfillJobEntry(BaseModel):
    """One row in the /api/monitor/backfill response."""

    deployment_id: str
    vm_name: str
    asset_group: str
    task: str
    mode: str
    start_date: str
    end_date: str
    status: str
    started_at: str
    last_heartbeat_at: str
    completed_at: str | None = None
    exit_code: int | None = None
    rows_in: int = 0
    rows_out: int = 0
    rows_error: int = 0
    events_emitted: int = 0
    log_uri: str = ""
    lifecycle_class: str = "EPHEMERAL_BATCH"


class BackfillMonitorResponse(BaseModel):
    """Response from GET /api/monitor/backfill."""

    jobs: list[BackfillJobEntry]
    total: int
    queried_at: str
    cloud: str


@router.get("/monitor/backfill", response_model=BackfillMonitorResponse, tags=["Monitor"])
def list_backfill_jobs(
    cloud: str = Query(default="gcp", description="Cloud target: gcp or aws"),
    status: str | None = Query(default=None, description="Filter by status: running, completed, failed"),
    limit: int = Query(default=50, ge=1, le=500, description="Max rows to return"),
) -> BackfillMonitorResponse:
    """List running and recent EPHEMERAL_BATCH jobs (instruments/MTDS/MDPS/features backfills).

    Reads the deployments registry (active + 3-day archive) and returns only
    rows whose VM-name prefix is NOT a long-lived live strategy prefix.
    The ``cloud`` parameter is accepted for interface parity with the other Monitor
    endpoints; GCS registry is the source of truth regardless of cloud.
    """
    try:
        registry = DeploymentsRegistry(bucket=DEFAULT_BUCKET)
        active = list(registry.list_active())
        archived = list(registry.list_recent_archive(days=3))
    except Exception as exc:
        logger.warning("monitor/backfill: registry read failed: %s", exc)
        active, archived = [], []

    jobs: list[BackfillJobEntry] = []
    for entry in active + archived:
        if not _is_backfill_vm(entry.vm_name):
            continue
        if status is not None and entry.status != status:
            continue
        jobs.append(
            BackfillJobEntry(
                deployment_id=entry.deployment_id,
                vm_name=entry.vm_name,
                asset_group=entry.asset_group or "",
                task=entry.task or "",
                mode=entry.mode or "",
                start_date=entry.start_date or "",
                end_date=entry.end_date or "",
                status=entry.status or "",
                started_at=entry.started_at or "",
                last_heartbeat_at=entry.last_heartbeat_at or "",
                completed_at=entry.completed_at,
                exit_code=entry.exit_code,
                rows_in=entry.rows_in or 0,
                rows_out=entry.rows_out or 0,
                rows_error=entry.rows_error or 0,
                events_emitted=entry.events_emitted or 0,
                log_uri=entry.log_uri or "",
            )
        )

    jobs.sort(key=lambda j: j.started_at, reverse=True)
    jobs = jobs[:limit]

    return BackfillMonitorResponse(
        jobs=jobs,
        total=len(jobs),
        queried_at=datetime.now(UTC).isoformat(),
        cloud=cloud,
    )

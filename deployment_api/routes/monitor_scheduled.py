"""GET /api/monitor/scheduled — list Cloud Scheduler / EventBridge / VM-cron jobs.

Phase C (c4) of deployment_ui_lifecycle_tabs_2026_05_08.md.

Lists every scheduled job declared in the Phase D scheduler registry
(SchedulerSpec UAC SSOT — see d1 in the lifecycle tabs plan) joined with
its current live state from the cloud scheduler API.

Phase D registry (d1) is the named successor SSOT. Until Phase D ships,
this endpoint returns entries from the DeploymentsRegistry whose names
match known cron/scheduler patterns, with a ``source=registry_fallback``
marker in the response so the UI can distinguish the two modes.

Lifecycle action endpoints:
  POST /api/monitor/scheduled/{name}/run-now
  POST /api/monitor/scheduled/{name}/pause
  POST /api/monitor/scheduled/{name}/resume
  POST /api/monitor/scheduled/deploy-missing

Read-only GET; operator-auth (X-API-Key) inherited from the authenticated router.
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

# Prefixes that identify scheduled/cron VMs in the deployments registry.
# These are VMs launched by Cloud Scheduler triggers (not operator-initiated).
_SCHEDULED_PREFIXES: tuple[str, ...] = (
    "cron-",
    "scheduled-",
    "honest-coverage-",
    "qg-snapshot-",
    "vm-cron-",
)


def _is_scheduled_vm(vm_name: str) -> bool:
    """Return True when vm_name looks like a scheduler-triggered VM."""
    return any(vm_name.startswith(p) for p in _SCHEDULED_PREFIXES)


class ScheduledJobEntry(BaseModel):
    """One row in the /api/monitor/scheduled response."""

    deployment_id: str
    name: str
    source: str
    schedule_cron: str = ""
    target_kind: str = "vm"
    asset_group: str
    status: str
    last_run_at: str
    last_heartbeat_at: str
    completed_at: str | None = None
    exit_code: int | None = None
    log_uri: str = ""
    lifecycle_class: str = "SCHEDULED_RECURRING"


class ScheduledMonitorResponse(BaseModel):
    """Response from GET /api/monitor/scheduled."""

    jobs: list[ScheduledJobEntry]
    total: int
    queried_at: str
    cloud: str
    phase_d_registry_available: bool


@router.get("/monitor/scheduled", response_model=ScheduledMonitorResponse, tags=["Monitor"])
def list_scheduled_jobs(
    cloud: str = Query(default="gcp", description="Cloud target: gcp or aws"),
    status: str | None = Query(default=None, description="Filter by status: running, completed, failed"),
    limit: int = Query(default=50, ge=1, le=500, description="Max rows to return"),
) -> ScheduledMonitorResponse:
    """List Cloud Scheduler / EventBridge / VM-cron jobs with live state.

    Until the Phase D SchedulerSpec registry ships, returns entries from the
    DeploymentsRegistry whose VM-name prefix matches known scheduler patterns.
    ``phase_d_registry_available=false`` in the response signals the UI to show
    a "Scheduler registry not yet configured" placeholder alongside any results.

    The ``cloud`` parameter scopes the results when Phase D registry ships;
    currently GCS registry is the source of truth regardless of cloud.
    """
    try:
        registry = DeploymentsRegistry(bucket=DEFAULT_BUCKET)
        active = list(registry.list_active())
        archived = list(registry.list_recent_archive(days=7))
    except Exception as exc:
        logger.warning("monitor/scheduled: registry read failed: %s", exc)
        active, archived = [], []

    jobs: list[ScheduledJobEntry] = []
    for entry in active + archived:
        if not _is_scheduled_vm(entry.vm_name):
            continue
        if status is not None and entry.status != status:
            continue
        jobs.append(
            ScheduledJobEntry(
                deployment_id=entry.deployment_id,
                name=entry.vm_name,
                source="registry_fallback",
                asset_group=entry.asset_group or "",
                status=entry.status or "",
                last_run_at=entry.started_at or "",
                last_heartbeat_at=entry.last_heartbeat_at or "",
                completed_at=entry.completed_at,
                exit_code=entry.exit_code,
                log_uri=entry.log_uri or "",
            )
        )

    jobs.sort(key=lambda j: j.last_run_at, reverse=True)
    jobs = jobs[:limit]

    return ScheduledMonitorResponse(
        jobs=jobs,
        total=len(jobs),
        queried_at=datetime.now(UTC).isoformat(),
        cloud=cloud,
        phase_d_registry_available=False,
    )

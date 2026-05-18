"""GET /api/monitor/experiments — list running + recent EPHEMERAL_EXPERIMENT jobs.

Phase C (c2) of deployment_ui_lifecycle_tabs_2026_05_08.md.

Lists every ML-training, strategy-backtest, and execution-backtest VM from the
deployments registry, distinguished from EPHEMERAL_BATCH (instruments/MTDS/MDPS)
jobs by their VM-name prefix.

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

# VM-name prefixes whose jobs are EPHEMERAL_EXPERIMENT (research / backtest).
# Source: vm_zombie_watchdog.py VM_PREFIX_TO_BUCKET (None lifecycle entries that
# represent research rather than data-pipeline work).
_EXPERIMENT_PREFIXES: tuple[str, ...] = (
    "strategy-backtest-grid-",
    "strategy-backtest-",
    "ml-train-",
    "execution-backtest-",
)

# LONG_LIVED_LIVE prefixes — excluded from experiments list.
_LIVE_PREFIXES: tuple[str, ...] = (
    "strategy-paper-",
    "strategy-live-",
    "defi-recursive-",
)


def _is_experiment_vm(vm_name: str) -> bool:
    """Return True when vm_name belongs to an EPHEMERAL_EXPERIMENT job."""
    if any(vm_name.startswith(p) for p in _LIVE_PREFIXES):
        return False
    return any(vm_name.startswith(p) for p in _EXPERIMENT_PREFIXES)


def _infer_experiment_kind(vm_name: str) -> str:
    if vm_name.startswith("ml-train-"):
        return "ml_training"
    if vm_name.startswith("execution-backtest-"):
        return "execution_backtest"
    return "strategy_backtest"


class ExperimentJobEntry(BaseModel):
    """One row in the /api/monitor/experiments response."""

    deployment_id: str
    vm_name: str
    experiment_kind: str
    asset_group: str
    task: str
    status: str
    started_at: str
    last_heartbeat_at: str
    completed_at: str | None = None
    exit_code: int | None = None
    rows_in: int = 0
    rows_out: int = 0
    rows_error: int = 0
    log_uri: str = ""
    lifecycle_class: str = "EPHEMERAL_EXPERIMENT"


class ExperimentMonitorResponse(BaseModel):
    """Response from GET /api/monitor/experiments."""

    jobs: list[ExperimentJobEntry]
    total: int
    queried_at: str
    cloud: str


@router.get("/monitor/experiments", response_model=ExperimentMonitorResponse, tags=["Monitor"])
def list_experiment_jobs(
    cloud: str = Query(default="gcp", description="Cloud target: gcp or aws"),
    status: str | None = Query(default=None, description="Filter by status: running, completed, failed"),
    limit: int = Query(default=50, ge=1, le=500, description="Max rows to return"),
) -> ExperimentMonitorResponse:
    """List running and recent EPHEMERAL_EXPERIMENT jobs (ML training, strategy backtests).

    Reads the deployments registry (active + 3-day archive) and returns only
    rows whose VM-name prefix matches known experiment prefixes.
    The ``cloud`` parameter is accepted for interface parity; GCS registry is
    the source of truth regardless of cloud target.
    """
    try:
        registry = DeploymentsRegistry(bucket=DEFAULT_BUCKET)
        active = list(registry.list_active())
        archived = list(registry.list_recent_archive(days=3))
    except Exception as exc:
        logger.warning("monitor/experiments: registry read failed: %s", exc)
        active, archived = [], []

    jobs: list[ExperimentJobEntry] = []
    for entry in active + archived:
        if not _is_experiment_vm(entry.vm_name):
            continue
        if status is not None and entry.status != status:
            continue
        jobs.append(
            ExperimentJobEntry(
                deployment_id=entry.deployment_id,
                vm_name=entry.vm_name,
                experiment_kind=_infer_experiment_kind(entry.vm_name),
                asset_group=entry.asset_group or "",
                task=entry.task or "",
                status=entry.status or "",
                started_at=entry.started_at or "",
                last_heartbeat_at=entry.last_heartbeat_at or "",
                completed_at=entry.completed_at,
                exit_code=entry.exit_code,
                rows_in=entry.rows_in or 0,
                rows_out=entry.rows_out or 0,
                rows_error=entry.rows_error or 0,
                log_uri=entry.log_uri or "",
            )
        )

    jobs.sort(key=lambda j: j.started_at, reverse=True)
    jobs = jobs[:limit]

    return ExperimentMonitorResponse(
        jobs=jobs,
        total=len(jobs),
        queried_at=datetime.now(UTC).isoformat(),
        cloud=cloud,
    )

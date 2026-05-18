"""GET /api/monitor/live — list running + recent LONG_LIVED_LIVE strategy clusters.

Phase C (c3) of deployment_ui_lifecycle_tabs_2026_05_08.md.

Lists every paper-trade and live-trade strategy VM (strategy-paper-*, strategy-live-*,
defi-recursive-*) from the deployments registry. These are long-lived deployments that
persist across days, unlike EPHEMERAL_BATCH or EPHEMERAL_EXPERIMENT jobs.

Lifecycle action endpoints (start/stop/pause/restart/drain) are via:
  POST /api/monitor/live/{name}/{action}
(wired via vm_admin.py — reuses existing cancel/pause/resume infrastructure).

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

# LONG_LIVED_LIVE VM-name prefixes.
# Kept in sync with VM_PREFIX_TO_BUCKET in vm_zombie_watchdog.py.
_LIVE_PREFIXES: tuple[str, ...] = (
    "strategy-paper-",
    "strategy-live-",
    "defi-recursive-",
)


def _is_live_vm(vm_name: str) -> bool:
    """Return True when vm_name is a LONG_LIVED_LIVE strategy deployment."""
    return any(vm_name.startswith(p) for p in _LIVE_PREFIXES)


def _infer_deployment_kind(vm_name: str) -> str:
    if vm_name.startswith("strategy-paper-"):
        return "paper_trade"
    if vm_name.startswith("strategy-live-"):
        return "live_trade"
    if vm_name.startswith("defi-recursive-"):
        return "defi_recursive_borrow"
    return "unknown"


class LiveClusterEntry(BaseModel):
    """One row in the /api/monitor/live response."""

    deployment_id: str
    vm_name: str
    deployment_kind: str
    asset_group: str
    task: str
    status: str
    started_at: str
    last_heartbeat_at: str
    completed_at: str | None = None
    exit_code: int | None = None
    log_uri: str = ""
    lifecycle_class: str = "LONG_LIVED_LIVE"


class LiveMonitorResponse(BaseModel):
    """Response from GET /api/monitor/live."""

    clusters: list[LiveClusterEntry]
    total: int
    queried_at: str
    cloud: str


@router.get("/monitor/live", response_model=LiveMonitorResponse, tags=["Monitor"])
def list_live_clusters(
    cloud: str = Query(default="gcp", description="Cloud target: gcp or aws"),
    status: str | None = Query(default=None, description="Filter by status: running, completed, failed"),
    limit: int = Query(default=50, ge=1, le=500, description="Max rows to return"),
) -> LiveMonitorResponse:
    """List running and recent LONG_LIVED_LIVE strategy clusters (paper-trade, live-trade).

    Reads the deployments registry (active + 7-day archive — longer window than
    backfill/experiment because live clusters persist for days) and returns only
    rows whose VM-name prefix matches known long-lived strategy prefixes.
    The ``cloud`` parameter is accepted for interface parity; GCS registry is
    the source of truth regardless of cloud target.
    """
    try:
        registry = DeploymentsRegistry(bucket=DEFAULT_BUCKET)
        active = list(registry.list_active())
        archived = list(registry.list_recent_archive(days=7))
    except Exception as exc:
        logger.warning("monitor/live: registry read failed: %s", exc)
        active, archived = [], []

    clusters: list[LiveClusterEntry] = []
    seen_ids: set[str] = set()
    for entry in active + archived:
        if not _is_live_vm(entry.vm_name):
            continue
        if status is not None and entry.status != status:
            continue
        if entry.deployment_id in seen_ids:
            continue
        seen_ids.add(entry.deployment_id)
        clusters.append(
            LiveClusterEntry(
                deployment_id=entry.deployment_id,
                vm_name=entry.vm_name,
                deployment_kind=_infer_deployment_kind(entry.vm_name),
                asset_group=entry.asset_group or "",
                task=entry.task or "",
                status=entry.status or "",
                started_at=entry.started_at or "",
                last_heartbeat_at=entry.last_heartbeat_at or "",
                completed_at=entry.completed_at,
                exit_code=entry.exit_code,
                log_uri=entry.log_uri or "",
            )
        )

    clusters.sort(key=lambda c: c.started_at, reverse=True)
    clusters = clusters[:limit]

    return LiveMonitorResponse(
        clusters=clusters,
        total=len(clusters),
        queried_at=datetime.now(UTC).isoformat(),
        cloud=cloud,
    )

# Epic: monitoring_control_plane_master_2026_06_10
# Lifecycle: permanent
"""Fleet infra endpoints for the deployment-ui FleetInfra tile.

* ``GET /api/fleet/vm-census``       — running / expected / zombie / OOM / stopped census,
  derived from the live GCE instance inventory + lifecycle classification.
* ``GET /api/fleet/infra-vm-health`` — server-side proxy of the agent-orchestrator
  ``/api/fleet/summary`` (the per-VM slot / backlog / watchdog posture).

Both are read-only monitoring endpoints with honest degradation — a compute-API or
orchestrator failure yields an empty/all-zero response + a logged warning, never a 5xx.

Plan: unified-trading-pm/plans/active/deployment_ui_monitoring_pane_2026_06_19.md.
Master: monitoring_control_plane_master_2026_06_10.md (single-pane division of surfaces).
"""

from __future__ import annotations

import logging
from datetime import UTC, datetime

from fastapi import APIRouter, Depends, HTTPException, Query
from unified_api_contracts.internal.schemas.rbac import (  # noqa: deep-import — RBAC not re-exported from UIC top-level yet
    Permission,
)

from deployment_api.deployment_api_config import DeploymentApiConfig
from deployment_api.rbac import require_permission
from deployment_api.routes._fleet_census import build_vm_census, load_watchdog_census
from deployment_api.routes._fleet_infra_health import fetch_infra_vm_health
from deployment_api.routes._fleet_inventory import build_orphan_inventory
from deployment_api.routes._fleet_mocks import mock_infra_vm_health, mock_vm_census
from deployment_api.routes._fleet_types import (
    DeleteInstanceResponse,
    InfraVmHealthResponse,
    OrphanInventoryResponse,
    ReapRequest,
    ReapResponse,
    ReapResultEntry,
    VmCensusResponse,
)
from deployment_api.vm_utils import delete_vm_instance, get_disk_details, get_vm_instance_details

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/api/fleet", tags=["Fleet"])

_cfg = DeploymentApiConfig()


def _empty_orphan_inventory(grace_hours: float, now: datetime) -> OrphanInventoryResponse:
    """Honest all-zero orphan inventory (mock mode / no project / compute failure)."""
    return build_orphan_inventory({}, {}, now, grace_hours)


@router.get("/vm-census", response_model=VmCensusResponse)
def get_vm_census() -> VmCensusResponse:
    """Return the fleet VM census (running / expected / zombie / OOM / stopped + per-VM).

    Reads the live GCE instance inventory. On a compute-API failure ``get_vm_instance_details``
    returns an empty map (logged), which ``build_vm_census`` rolls into an honest all-zero
    census — this read-only monitoring endpoint never crashes.
    """
    if _cfg.is_mock_mode():
        return mock_vm_census()

    project_id = _cfg.gcp_project_id
    if not project_id:
        # No project configured → honest empty census (clearly logged: this is a misconfig).
        logger.warning("vm-census: no gcp_project_id configured; returning empty census")
        return build_vm_census({}, datetime.now(UTC))

    now = datetime.now(UTC)
    watchdog_census = load_watchdog_census(project_id, now)
    vm_details = get_vm_instance_details(project_id)
    return build_vm_census(vm_details, now, watchdog_census)


@router.get("/infra-vm-health", response_model=InfraVmHealthResponse)
async def get_infra_vm_health() -> InfraVmHealthResponse:
    """Proxy the agent-orchestrator fleet summary into the single devops pane.

    Degrades honestly (``available=False`` + empty ``vms``) when the orchestrator is
    unreachable or no token is configured; always returns the orchestrator deep-link URL.
    """
    if _cfg.is_mock_mode():
        return mock_infra_vm_health()
    return await fetch_infra_vm_health(_cfg.gcp_project_id or "")


@router.get("/orphans", response_model=OrphanInventoryResponse)
def get_orphans(
    grace_hours: float = Query(24.0, ge=0.0, description="Stopped-age (hours) before a VM is reapable."),
) -> OrphanInventoryResponse:
    """Return the STOPPED/TERMINATED-VM inventory + idle-spend rollup (read-only).

    Each entry carries lifecycle class, how long it has been stopped, boot-disk size + an
    ESTIMATED $/month, and a reap-verdict mirroring the zombie-watchdog's gates. A compute-API
    failure degrades to an honest zero-count inventory (logged), never a 5xx.
    """
    now = datetime.now(UTC)
    if _cfg.is_mock_mode():
        return _empty_orphan_inventory(grace_hours, now)
    project_id = _cfg.gcp_project_id
    if not project_id:
        logger.warning("orphans: no gcp_project_id configured; returning empty inventory")
        return _empty_orphan_inventory(grace_hours, now)
    vm_details = get_vm_instance_details(project_id)
    disk_details = get_disk_details(project_id)
    return build_orphan_inventory(vm_details, disk_details, now, grace_hours)


@router.post("/reap", response_model=ReapResponse)
def reap_orphans(
    request: ReapRequest,
    _check: None = Depends(require_permission(Permission.DEPLOY_TRIGGER)),
) -> ReapResponse:
    """Reap abandoned ephemeral TERMINATED VMs (+ their boot disks). Dry-run by default.

    Mirrors deployment-service ``vm_zombie_watchdog`` reaper gates exactly: only VMs whose
    orphan-verdict is ``reap`` (ephemeral lifecycle, stopped past ``grace_hours``, not
    ``keep=true``/daemon-labelled) are candidates. With ``dry_run=true`` (the default) it
    returns the candidate list without deleting; with ``dry_run=false`` it deletes each and
    reports per-VM outcomes.
    """
    now = datetime.now(UTC)
    grace = request.grace_hours
    if _cfg.is_mock_mode():
        return ReapResponse(
            dry_run=request.dry_run,
            grace_hours=grace,
            candidate_total=0,
            reaped_total=0,
            monthly_reclaimed_usd=0.0,
            results=[],
        )
    project_id = _cfg.gcp_project_id
    if not project_id:
        raise HTTPException(status_code=503, detail="No GCP project configured")

    inventory = build_orphan_inventory(get_vm_instance_details(project_id), get_disk_details(project_id), now, grace)
    candidates = [o for o in inventory.orphans if o.reapable]

    results: list[ReapResultEntry] = []
    reaped = 0
    reclaimed = 0.0
    for orphan in candidates:
        if request.dry_run:
            results.append(
                ReapResultEntry(
                    name=orphan.name,
                    zone=orphan.zone,
                    monthly_disk_usd=orphan.monthly_disk_usd,
                    deleted=False,
                )
            )
            continue
        deleted = delete_vm_instance(project_id, orphan.name, orphan.zone)
        if deleted:
            reaped += 1
            reclaimed += orphan.monthly_disk_usd
            logger.warning(
                "REAPED orphan VM %s (%s) — ~$%.2f/mo disk reclaimed",
                orphan.name,
                orphan.zone,
                orphan.monthly_disk_usd,
            )
        results.append(
            ReapResultEntry(
                name=orphan.name,
                zone=orphan.zone,
                monthly_disk_usd=orphan.monthly_disk_usd,
                deleted=deleted,
            )
        )

    return ReapResponse(
        dry_run=request.dry_run,
        grace_hours=grace,
        candidate_total=len(candidates),
        reaped_total=reaped,
        monthly_reclaimed_usd=round(reclaimed, 2),
        results=results,
    )


@router.delete("/instances/{name}", response_model=DeleteInstanceResponse)
def delete_instance(
    name: str,
    zone: str = Query(..., description="GCE zone of the instance."),
    _check: None = Depends(require_permission(Permission.DEPLOY_TRIGGER)),
) -> DeleteInstanceResponse:
    """Delete a single STOPPED instance (+ its boot disk).

    Safety rail: refuses (409) to delete a RUNNING instance — this surface is for cleaning up
    stopped/orphaned VMs; a running VM must be stopped first (or killed by the zombie-watchdog).
    """
    if _cfg.is_mock_mode():
        return DeleteInstanceResponse(name=name, zone=zone, deleted=False)
    project_id = _cfg.gcp_project_id
    if not project_id:
        raise HTTPException(status_code=503, detail="No GCP project configured")

    details = get_vm_instance_details(project_id).get(name)
    if details is None:
        raise HTTPException(status_code=404, detail=f"Instance {name!r} not found")
    if str(details.get("status", "")).upper() == "RUNNING":  # noqa: qg-empty-fallback — absent status reads as not-running (safe)
        raise HTTPException(
            status_code=409,
            detail=f"Refusing to delete RUNNING instance {name!r}; stop it first (this endpoint is for stopped VMs)",
        )
    deleted = delete_vm_instance(project_id, name, zone)
    logger.warning("DELETE instance %s (%s) via fleet API → deleted=%s", name, zone, deleted)
    return DeleteInstanceResponse(name=name, zone=zone, deleted=deleted)


__all__ = ["router"]

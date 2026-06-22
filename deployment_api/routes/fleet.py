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

from fastapi import APIRouter

from deployment_api.deployment_api_config import DeploymentApiConfig
from deployment_api.routes._fleet_census import build_vm_census, load_watchdog_census
from deployment_api.routes._fleet_infra_health import fetch_infra_vm_health
from deployment_api.routes._fleet_mocks import mock_infra_vm_health, mock_vm_census
from deployment_api.routes._fleet_types import InfraVmHealthResponse, VmCensusResponse
from deployment_api.vm_utils import get_vm_instance_details

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/api/fleet", tags=["Fleet"])

_cfg = DeploymentApiConfig()


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


__all__ = ["router"]

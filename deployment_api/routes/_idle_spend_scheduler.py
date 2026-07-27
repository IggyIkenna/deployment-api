# Epic: observability_master
"""Cloud Scheduler-triggered daily idle-spend snapshot.

deployment_durable_operational_data_bigquery_2026_07_21.md — runs the SAME orphan/idle
computation `/api/fleet/orphans` already exposes, and writes it durably to the
`idle_spend` BigQuery table (one rollup row + one per-resource row) so idle-spend
trend and reclaimed-over-time are queryable past whatever window the live GCE
inventory itself can show.

Auth: reuses `_reap_scheduler.verify_reap_scheduler_oidc` — the SAME Cloud Scheduler
invoker service-account identity triggers both internal ticks; no reason to stand up
a second OIDC-verified identity for what is the same trust boundary (one scheduler,
one Cloud Run service, two internal ticks).
"""

from __future__ import annotations

import logging
from datetime import UTC, datetime

from fastapi import APIRouter, Depends

from deployment_api.deployment_api_config import DeploymentApiConfig
from deployment_api.routes._fleet_inventory import build_orphan_inventory
from deployment_api.routes._reap_scheduler import verify_reap_scheduler_oidc
from deployment_api.services.operational_data_writer import write_idle_spend_snapshot
from deployment_api.vm_utils import get_disk_details, get_vm_instance_details

logger = logging.getLogger(__name__)

router = APIRouter()

_cfg = DeploymentApiConfig()

# Same default grace window as the /api/fleet/orphans UI default — the snapshot should
# reflect the same reapable/idle population an operator sees live, not a different one.
_DEFAULT_GRACE_HOURS = 24.0


@router.post("/internal/idle-spend-snapshot")
async def run_idle_spend_snapshot(_invoker: str = Depends(verify_reap_scheduler_oidc)) -> dict[str, object]:
    """Cloud Scheduler's daily tick — snapshot the orphan/idle inventory into BigQuery."""
    if _cfg.is_mock_mode():
        return {"status": "ok", "rows_written": 0, "reason": "mock_mode"}
    project_id = _cfg.gcp_project_id
    if not project_id:
        logger.warning("idle-spend-snapshot: no gcp_project_id configured; skipping")
        return {"status": "skipped", "rows_written": 0, "reason": "no_project_id"}

    now = datetime.now(UTC)
    vm_details = get_vm_instance_details(project_id)
    disk_details = get_disk_details(project_id)
    inventory = build_orphan_inventory(vm_details, disk_details, now, _DEFAULT_GRACE_HOURS)

    per_resource = [
        {
            "resource_name": o.name,
            "lifecycle_class": str(o.lifecycle_class),
            "age_hours": o.stopped_age_hours,
            "monthly_idle_usd": o.monthly_disk_usd,
            "monthly_reapable_usd": o.monthly_disk_usd if o.reapable else None,
        }
        for o in inventory.orphans
    ]
    rows_written = write_idle_spend_snapshot(
        project_id,
        stopped_total=inventory.stopped_total,
        reapable_total=inventory.reapable_total,
        monthly_idle_usd=inventory.monthly_idle_usd,
        monthly_reapable_usd=inventory.monthly_reapable_usd,
        per_resource=per_resource,
    )
    return {"status": "ok", "rows_written": rows_written, "resource_count": len(per_resource)}


__all__ = ["router"]

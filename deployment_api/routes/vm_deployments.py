"""
VM Deployments API — active + recent-archive view of VM-spawned jobs.

These are jobs fired by `vm-exec-with-gcs-tee.sh` (canonical migrations, backfills,
forward-polls, smoke runs). The GCS-backed registry lives at:

    gs://<bucket>/deployments/active/<id>.json
    gs://<bucket>/deployments/archive/<YYYY-MM-DD>/<id>.json

Distinct from the existing `/api/deployments` routes which cover Cloud Run /
VM deployments launched by deployment-service proper. Those are "I'm rolling out
a new version of market-tick-data-service"; these are "a batch VM is crunching
2024-06-01..2024-06-30 canonical-migration right now."
"""

from __future__ import annotations

import logging
from dataclasses import asdict
from datetime import UTC, datetime
from typing import cast

from deployment_service.deployments_registry import (
    DEFAULT_BUCKET,
    DeploymentRegistryEntry,
    DeploymentsRegistry,
    vm_run_log_rolling_uri,
    vm_serial_rolling_uri,
)
from fastapi import APIRouter, HTTPException, Query
from pydantic import BaseModel, Field

from deployment_api.deployment_api_config import DeploymentApiConfig
from deployment_api.vm_utils import get_vm_instance_details

router = APIRouter()
logger = logging.getLogger(__name__)

_cfg = DeploymentApiConfig()


class VmDeploymentEntryModel(BaseModel):  # CORRECT-LOCAL: FastAPI API contract model
    """API-side view of one deployments_registry entry."""

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
    archive_run_log_uri: str = ""
    archive_serial_uri: str = ""
    machine_type: str | None = None
    zone: str | None = None
    uptime_hours: float | None = None
    health_status: str | None = None  # "producing", "stalled", "boot-hung", etc.


class VmDeploymentsListModel(BaseModel):  # CORRECT-LOCAL: FastAPI API contract model
    """Combined list of active + recent-archive VM deployments."""

    active: list[VmDeploymentEntryModel] = Field(default_factory=list)  # type: ignore[reportUnknownVariableType]
    recent: list[VmDeploymentEntryModel] = Field(default_factory=list)  # type: ignore[reportUnknownVariableType]
    archive_days: int


def _calculate_health_status(entry: DeploymentRegistryEntry, is_running: bool) -> str:
    """Determine health status based on heartbeat age and progress."""
    if not is_running:
        return "stopped"

    try:
        last_hb = datetime.fromisoformat(entry.last_heartbeat_at.replace("Z", "+00:00"))
        age_minutes = (datetime.now(UTC) - last_hb).total_seconds() / 60

        if age_minutes > 15:
            return "stalled"
        elif entry.rows_out > 0 or entry.events_emitted > 0:
            return "producing"
        elif age_minutes < 5:
            return "starting"
        else:
            return "idle"
    except Exception:
        return "unknown"


def _to_model(
    entry: DeploymentRegistryEntry, vm_details: dict[str, dict[str, object]] | None = None
) -> VmDeploymentEntryModel:
    data = asdict(entry)
    data.pop("extras", None)

    # Add VM instance details if available
    if vm_details and entry.vm_name in vm_details:
        details = vm_details[entry.vm_name]
        data["machine_type"] = details.get("machine_type")
        data["zone"] = details.get("zone")

        # Calculate uptime if creation timestamp available
        if creation_ts := details.get("creation_timestamp"):
            try:
                created = datetime.fromisoformat(str(creation_ts).replace("Z", "+00:00"))
                data["uptime_hours"] = round((datetime.now(UTC) - created).total_seconds() / 3600, 2)
            except Exception:
                pass

        # Determine health status
        is_running = details.get("status") == "RUNNING"
        data["health_status"] = _calculate_health_status(entry, is_running)

    # Populate durable archive URIs for completed entries (rolling daily archive,
    # no 14-day TTL unlike vm-logs/).
    completed_at = data.get("completed_at")
    if completed_at and isinstance(completed_at, str) and len(completed_at) >= 10:
        try:
            date_stamp = completed_at[:10].replace("-", "")  # YYYYMMDD
            vm_name = str(data.get("vm_name", ""))  # noqa: qg-empty-fallback — registry blob display default
            if vm_name and date_stamp.isdigit():
                data["archive_run_log_uri"] = vm_run_log_rolling_uri(vm_name, date_stamp)
                data["archive_serial_uri"] = vm_serial_rolling_uri(vm_name, date_stamp)
        except Exception:
            pass

    return VmDeploymentEntryModel(**cast(dict[str, object], data))  # type: ignore[reportArgumentType]


def _mock_entry(**kwargs: object) -> VmDeploymentEntryModel:
    defaults: dict[str, object] = {
        "deployment_id": "dep-mock-1",
        "vm_name": "canonical-migration-cefi-20260418-042359",
        "asset_group": "CEFI",
        "task": "canonical-migration",
        "mode": "dry",
        "start_date": "2024-06-01",
        "end_date": "2024-06-30",
        "status": "running",
        "started_at": "2026-04-18T04:23:59Z",
        "last_heartbeat_at": "2026-04-18T04:28:59Z",
        "completed_at": None,
        "exit_code": None,
        "rows_in": 12_000,
        "rows_out": 11_987,
        "rows_error": 13,
        "events_emitted": 42,
        "log_uri": "gs://deployment-scripts-${GCP_PROJECT_ID}/vm-logs/canonical-migration-cefi-20260418-042359/run.log",  # noqa: gs-uri (mock fixture URI)
        "archive_run_log_uri": "",
        "archive_serial_uri": "",
    }
    defaults.update(kwargs)
    return VmDeploymentEntryModel(**defaults)  # type: ignore[reportArgumentType]


@router.get("/vm-deployments", response_model=VmDeploymentsListModel)
def list_vm_deployments(
    days: int = Query(7, ge=1, le=30, description="Archive lookback window in days"),
    filter_stale: bool = Query(True, description="Filter out stale registry entries"),
) -> VmDeploymentsListModel:
    """List currently-running VM deployments + those completed in the last N days.

    By default, filters registry entries to only show VMs that are actually RUNNING
    in GCP (avoiding the stale registry entries problem). Set filter_stale=false to
    see all registry entries including stale ones.
    """
    if _cfg.is_mock_mode():
        return VmDeploymentsListModel(
            active=[_mock_entry()],
            recent=[
                _mock_entry(
                    deployment_id="dep-mock-2",
                    status="completed",
                    completed_at="2026-04-17T23:59:00Z",
                    exit_code=0,
                    rows_in=30_000,
                    rows_out=30_000,
                    rows_error=0,
                    archive_run_log_uri="gs://deployment-scripts-${GCP_PROJECT_ID}/log-archive/rolling/20260417/canonical-migration-cefi-20260418-042359/run.log",  # noqa: gs-uri (mock fixture URI)
                    archive_serial_uri="gs://deployment-scripts-${GCP_PROJECT_ID}/log-archive/serial-rolling/20260417/canonical-migration-cefi-20260418-042359/serial-console.txt",  # noqa: gs-uri (mock fixture URI)
                ),
                _mock_entry(
                    deployment_id="dep-mock-3",
                    status="failed",
                    completed_at="2026-04-17T12:05:00Z",
                    exit_code=1,
                    rows_in=8_000,
                    rows_out=4_200,
                    rows_error=3_800,
                ),
            ],
            archive_days=days,
        )

    registry = DeploymentsRegistry(bucket=DEFAULT_BUCKET)
    project_id = _cfg.gcp_project_id or "central-element-323112"

    try:
        # Get actual VM details from GCP
        vm_details = get_vm_instance_details(project_id) if filter_stale else {}
        running_vm_names = set(vm_details.keys()) if filter_stale else None

        # Get all registry entries
        all_active = registry.list_active()

        # Filter active entries to only actually running VMs if requested
        if filter_stale and running_vm_names is not None:
            filtered_active = [e for e in all_active if e.vm_name in running_vm_names]
            logger.info(
                "Filtered active deployments: %d registry entries -> %d actually running",
                len(all_active),
                len(filtered_active),
            )
            active = [_to_model(e, vm_details) for e in filtered_active]
        else:
            active = [_to_model(e, vm_details) for e in all_active]

        # Recent archive entries (completed/failed) don't need filtering
        recent = [_to_model(e, vm_details) for e in registry.list_recent_archive(days=days)]

    except (OSError, ValueError, RuntimeError) as exc:
        logger.exception("Failed to read VM deployments registry: %s", exc)
        raise HTTPException(status_code=502, detail="VM deployments registry unavailable") from exc

    return VmDeploymentsListModel(active=active, recent=recent, archive_days=days)


class VmReconcileResult(BaseModel):  # CORRECT-LOCAL: FastAPI API contract model
    """Result of a registry reconcile sweep."""

    reaped_count: int
    reaped: list[str] = Field(default_factory=list, description="deployment_ids reaped")
    running_vm_count: int
    total_active_before: int


@router.post("/vm-deployments/reconcile", response_model=VmReconcileResult, status_code=200)
def reconcile_vm_deployments() -> VmReconcileResult:
    """Reap stale active-registry entries: archive any whose VM is no longer RUNNING.

    Compares the GCS active/ list against GCP's aggregated VM list.  Any entry
    whose vm_name is absent from GCP (and whose heartbeat is older than 5 min
    clock-skew tolerance) is archived with status=failed, exit_code=125,
    reap_reason=vm_not_running.

    Returns the count + ids of reaped entries so the caller can update the UI.
    In mock mode returns a dry-run result without touching GCS.
    """
    if _cfg.is_mock_mode():
        return VmReconcileResult(
            reaped_count=0,
            reaped=[],
            running_vm_count=1,
            total_active_before=1,
        )

    registry = DeploymentsRegistry(bucket=DEFAULT_BUCKET)
    project_id = _cfg.gcp_project_id or "central-element-323112"

    try:
        all_active = registry.list_active()
        total_active_before = len(all_active)

        vm_details = get_vm_instance_details(project_id)
        running_vm_names = {name for name, details in vm_details.items() if details.get("status") == "RUNNING"}

        reaped = registry.reap_stale(running_vm_names=running_vm_names)

    except (OSError, ValueError, RuntimeError) as exc:
        logger.exception("Registry reconcile failed: %s", exc)
        raise HTTPException(status_code=502, detail="Registry reconcile failed") from exc

    logger.info(
        "reconcile_vm_deployments: reaped %d of %d active entries (%d VMs running)",
        len(reaped),
        total_active_before,
        len(running_vm_names),
    )
    return VmReconcileResult(
        reaped_count=len(reaped),
        reaped=[e.deployment_id for e in reaped],
        running_vm_count=len(running_vm_names),
        total_active_before=total_active_before,
    )


@router.get("/vm-deployments/{deployment_id}", response_model=VmDeploymentEntryModel)
def get_vm_deployment(deployment_id: str) -> VmDeploymentEntryModel:
    """Return a single VM deployment by id (checks active + last 14 days archive)."""
    if _cfg.is_mock_mode():
        return _mock_entry(deployment_id=deployment_id)

    registry = DeploymentsRegistry(bucket=DEFAULT_BUCKET)
    project_id = _cfg.gcp_project_id or "central-element-323112"

    try:
        entry = registry.get(deployment_id)
        if entry is None:
            raise HTTPException(status_code=404, detail=f"VM deployment '{deployment_id}' not found")

        # Get VM details if it's an active deployment
        vm_details = get_vm_instance_details(project_id) if entry.status == "running" else {}
        return _to_model(entry, vm_details)

    except HTTPException:
        raise
    except (OSError, ValueError, RuntimeError) as exc:
        logger.exception("Failed to fetch VM deployment %s: %s", deployment_id, exc)
        raise HTTPException(status_code=502, detail="VM deployments registry unavailable") from exc

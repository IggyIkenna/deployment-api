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
from typing import cast

from deployment_service.deployments_registry import (
    DEFAULT_BUCKET,
    DeploymentRegistryEntry,
    DeploymentsRegistry,
)
from fastapi import APIRouter, HTTPException, Query
from pydantic import BaseModel, Field

from deployment_api.deployment_api_config import DeploymentApiConfig

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


class VmDeploymentsListModel(BaseModel):  # CORRECT-LOCAL: FastAPI API contract model
    """Combined list of active + recent-archive VM deployments."""

    active: list[VmDeploymentEntryModel] = Field(default_factory=list)  # type: ignore[reportUnknownVariableType]
    recent: list[VmDeploymentEntryModel] = Field(default_factory=list)  # type: ignore[reportUnknownVariableType]
    archive_days: int


def _to_model(entry: DeploymentRegistryEntry) -> VmDeploymentEntryModel:
    data = asdict(entry)
    data.pop("extras", None)
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
        "log_uri": "gs://deployment-scripts-${GCP_PROJECT_ID}/vm-logs/canonical-migration-cefi-20260418-042359/run.log",
    }
    defaults.update(kwargs)
    return VmDeploymentEntryModel(**cast(dict[str, object], defaults))


@router.get("/vm-deployments", response_model=VmDeploymentsListModel)
def list_vm_deployments(
    days: int = Query(7, ge=1, le=30, description="Archive lookback window in days"),
) -> VmDeploymentsListModel:
    """List currently-running VM deployments + those completed in the last N days."""
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
    try:
        active = [_to_model(e) for e in registry.list_active()]
        recent = [_to_model(e) for e in registry.list_recent_archive(days=days)]
    except (OSError, ValueError, RuntimeError) as exc:
        logger.exception("Failed to read VM deployments registry: %s", exc)
        raise HTTPException(status_code=502, detail="VM deployments registry unavailable") from exc
    return VmDeploymentsListModel(active=active, recent=recent, archive_days=days)


@router.get("/vm-deployments/{deployment_id}", response_model=VmDeploymentEntryModel)
def get_vm_deployment(deployment_id: str) -> VmDeploymentEntryModel:
    """Return a single VM deployment by id (checks active + last 14 days archive)."""
    if _cfg.is_mock_mode():
        return _mock_entry(deployment_id=deployment_id)
    registry = DeploymentsRegistry(bucket=DEFAULT_BUCKET)
    try:
        entry = registry.get(deployment_id)
    except (OSError, ValueError, RuntimeError) as exc:
        logger.exception("Failed to fetch VM deployment %s: %s", deployment_id, exc)
        raise HTTPException(status_code=502, detail="VM deployments registry unavailable") from exc
    if entry is None:
        raise HTTPException(status_code=404, detail=f"VM deployment '{deployment_id}' not found")
    return _to_model(entry)

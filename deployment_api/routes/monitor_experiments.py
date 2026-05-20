"""GET /api/monitor/experiments — list running + recent EPHEMERAL_EXPERIMENT jobs.
POST /api/monitor/experiments/{deployment_id}/stop|restart — lifecycle actions.

Phase C (c2) of deployment_ui_lifecycle_tabs_2026_05_08.md.
Phase BB.3 — stop/restart actions for GCE experiment VMs.

Lists every ML-training, strategy-backtest, and execution-backtest VM from the
deployments registry, distinguished from EPHEMERAL_BATCH (instruments/MTDS/MDPS)
jobs by their VM-name prefix.

Stop/restart actions issue gcloud compute instances stop/reset commands against
the VM. In mock/dry_run mode the command string is returned without execution.
"""

from __future__ import annotations

import logging
import shlex
import subprocess
from datetime import UTC, datetime

from deployment_service.deployments_registry import (
    DEFAULT_BUCKET,
    DeploymentsRegistry,
)
from fastapi import APIRouter, HTTPException, Query
from pydantic import BaseModel

from deployment_api import settings as _settings
from deployment_api.deployment_api_config import DeploymentApiConfig

_cfg = DeploymentApiConfig()

router = APIRouter()
logger = logging.getLogger(__name__)

# Primary zone for GCE experiment VMs. Derived from the workspace GCS region
# (same region, zone-b suffix). Matches the zone used by VM launch scripts.
_DEFAULT_GCE_ZONE = f"{_settings.GCS_REGION}-b"

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
    env: str


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
        env=_settings.DEPLOYMENT_ENV,
    )


# ---------------------------------------------------------------------------
# Phase BB.3 lifecycle action models + helpers
# ---------------------------------------------------------------------------


class ExperimentActionResponse(BaseModel):
    """Response from POST /api/monitor/experiments/{deployment_id}/{stop|restart}."""

    deployment_id: str
    vm_name: str
    action: str
    status: str
    command: str
    error: str | None = None
    executed_at: str


def _gce_action_cmd(vm_name: str, action: str, project_id: str, zone: str) -> str:
    """Build a gcloud compute instances stop/reset command string."""
    gcloud_action = "stop" if action == "stop" else "reset"
    return shlex.join(
        [
            "gcloud",
            "compute",
            "instances",
            gcloud_action,
            vm_name,
            f"--zone={zone}",
            f"--project={project_id}",
            "--quiet",
        ]
    )


def _run_gce_cmd(cmd: str) -> tuple[bool, str]:
    """Run a gcloud command; return (ok, stderr_or_empty)."""
    try:
        args = shlex.split(cmd)
        result = subprocess.run(args, capture_output=True, text=True, timeout=60)
        ok = result.returncode == 0
        return ok, result.stderr.strip() if not ok else ""
    except FileNotFoundError:
        return False, "gcloud CLI not found; run from authenticated terminal"
    except subprocess.TimeoutExpired:
        return False, "command timed out after 60s"


def _do_experiment_action(deployment_id: str, action: str, dry_run: bool) -> ExperimentActionResponse:
    try:
        registry = DeploymentsRegistry(bucket=DEFAULT_BUCKET)
        entry = registry.get(deployment_id)
    except Exception as exc:
        raise HTTPException(status_code=503, detail=f"Registry unavailable: {exc}") from exc

    if entry is None:
        raise HTTPException(status_code=404, detail=f"Deployment '{deployment_id}' not found in registry")

    vm_name = entry.vm_name
    if not _is_experiment_vm(vm_name):
        raise HTTPException(status_code=422, detail=f"VM '{vm_name}' is not an experiment VM")

    project_id = _cfg.gcp_project_id
    is_mock = _cfg.is_mock_mode() or dry_run

    cmd = _gce_action_cmd(vm_name, action, project_id, _DEFAULT_GCE_ZONE)
    if is_mock:
        return ExperimentActionResponse(
            deployment_id=deployment_id,
            vm_name=vm_name,
            action=action,
            status="preview",
            command=cmd,
            executed_at=datetime.now(UTC).isoformat(),
        )

    ok, err = _run_gce_cmd(cmd)
    return ExperimentActionResponse(
        deployment_id=deployment_id,
        vm_name=vm_name,
        action=action,
        status="ok" if ok else "failed",
        command=cmd,
        error=err or None,
        executed_at=datetime.now(UTC).isoformat(),
    )


@router.post(
    "/monitor/experiments/{deployment_id}/stop",
    response_model=ExperimentActionResponse,
    tags=["Monitor"],
)
def stop_experiment(
    deployment_id: str,
    dry_run: bool = Query(default=False, description="If true, return command without executing"),
) -> ExperimentActionResponse:
    """Stop a running experiment VM (gcloud compute instances stop)."""
    return _do_experiment_action(deployment_id, "stop", dry_run)


@router.post(
    "/monitor/experiments/{deployment_id}/restart",
    response_model=ExperimentActionResponse,
    tags=["Monitor"],
)
def restart_experiment(
    deployment_id: str,
    dry_run: bool = Query(default=False, description="If true, return command without executing"),
) -> ExperimentActionResponse:
    """Restart a running experiment VM (gcloud compute instances reset)."""
    return _do_experiment_action(deployment_id, "restart", dry_run)

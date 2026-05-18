"""POST /api/ml/experiment/launch — programmatic ML training VM launch.

Fires launch-ml-training-vm.sh for a given asset_group / instruments /
target_types / timeframes combination. Follows the same no-fire-and-forget
contract as backfill_launch.py: caller verifies STARTED via
GET /api/vm/events?vm_name=<vm_name> within 60s.

VM prefix `ml-train-` is registered in vm_zombie_watchdog.py.
"""

from __future__ import annotations

import logging
import subprocess
import uuid
from datetime import UTC, datetime
from os import environ as _process_env
from pathlib import Path

from fastapi import APIRouter, Depends, HTTPException
from pydantic import BaseModel, Field
from unified_api_contracts.internal.schemas.rbac import (  # noqa: deep-import — RBAC not re-exported from UIC top-level yet
    Permission,
)
from unified_trading_library import log_event

from deployment_api.deployment_api_config import DeploymentApiConfig
from deployment_api.rbac import require_permission

router = APIRouter()
logger = logging.getLogger(__name__)

_cfg = DeploymentApiConfig()

_DEFAULT_ZONE = "asia-northeast1-c"
_LAUNCHER_FILENAME = "launch-ml-training-vm.sh"
_VM_PREFIX = "ml-train-"
_SERVICE = "ml-training-service"
_SUBPROCESS_TIMEOUT_SECONDS = 600


class MlExperimentLaunchRequest(BaseModel):
    """Request payload for POST /api/ml/experiment/launch."""

    asset_group: str = Field(
        ...,
        description="Lowercase asset_group — one of {cefi, defi, tradfi, prediction, sports}.",
        pattern=r"^(cefi|defi|tradfi|prediction|sports)$",
    )
    instruments: list[str] = Field(
        ...,
        description="Instrument IDs (e.g. ['ES_FRONT', 'BTC']). Semicolon-joined for launcher.",
        min_length=1,
    )
    target_types: list[str] = Field(
        default_factory=lambda: ["swing_high", "swing_low"],
        description="ML target types (e.g. swing_high, swing_low, direction_1h).",
    )
    timeframes: list[str] = Field(
        default_factory=lambda: ["1m"],
        description="Feature timeframes passed to ml-training-service CLI (e.g. 1m, 5m, 1h).",
    )
    start_date: str | None = Field(
        default=None,
        description="ISO date YYYY-MM-DD (inclusive). Defaults to launcher default (2yr lookback).",
        pattern=r"^\d{4}-\d{2}-\d{2}$",
    )
    end_date: str | None = Field(
        default=None,
        description="ISO date YYYY-MM-DD (inclusive). Defaults to today UTC.",
        pattern=r"^\d{4}-\d{2}-\d{2}$",
    )
    operation: str = Field(
        default="train",
        description="ml-training-service operation: train | evaluate | grid-search | pipeline.",
        pattern=r"^(train|evaluate|grid-search|pipeline)$",
    )
    machine: str = Field(
        default="cpu",
        description="VM machine type: cpu | gpu | high.",
        pattern=r"^(cpu|gpu|high)$",
    )
    dry_run: bool = Field(
        default=False,
        description="Echo-only mode — resolves argv but does NOT shell out.",
    )


class MlExperimentLaunchResult(BaseModel):
    """Response for POST /api/ml/experiment/launch."""

    vm_name: str
    zone: str
    project_id: str
    launched_at: datetime
    correlation_id: str
    launcher_script: str
    dry_run: bool
    events_uri: str = Field(
        ...,
        description="GCS URI prefix: gs://{project_id}-events/events/{service}/{date}/{vm_name}/",
    )
    argv: list[str] = Field(default_factory=list)


def _launcher_dir() -> Path:
    workspace_root = _process_env.get("WORKSPACE_ROOT") or str(Path(__file__).resolve().parents[4])
    return Path(workspace_root) / "deployment-service" / "scripts" / "vm"


def _build_argv(
    launcher_path: Path,
    req: MlExperimentLaunchRequest,
    dry_run: bool,
) -> list[str]:
    argv: list[str] = ["bash", str(launcher_path)]
    argv += ["--asset-group", req.asset_group.upper()]
    argv += ["--instruments", ";".join(req.instruments)]
    argv += ["--target-types", ";".join(req.target_types)]
    argv += ["--timeframes", ";".join(req.timeframes)]
    if req.start_date:
        argv += ["--start-date", req.start_date]
    if req.end_date:
        argv += ["--end-date", req.end_date]
    argv += ["--operation", req.operation]
    argv += ["--machine", req.machine]
    if dry_run:
        argv.append("--dry-run")
    return argv


def _build_events_uri(project_id: str, vm_name: str, run_ts: str) -> str:
    date_fmt = f"{run_ts[:4]}-{run_ts[4:6]}-{run_ts[6:8]}"
    return f"gs://{project_id}-events/events/{_SERVICE}/{date_fmt}/{vm_name}/"  # noqa: gs-uri  — events bucket env-tier pending Phase 2.6 sub-step


@router.post("/launch", response_model=MlExperimentLaunchResult)
def launch_ml_experiment(
    request: MlExperimentLaunchRequest,
    _rbac: None = Depends(require_permission(Permission.DEPLOY_TRIGGER)),
) -> MlExperimentLaunchResult:
    """Launch an ML training VM for the given asset_group / instruments combination."""
    correlation_id = str(uuid.uuid4())
    launched_at = datetime.now(UTC)
    run_ts = launched_at.strftime("%Y%m%d-%H%M%S")
    first_inst = request.instruments[0].lower().replace("_", "-")
    vm_name = f"{_VM_PREFIX}{first_inst[:20]}-{run_ts}"
    project_id = _cfg.gcp_project_id
    launcher_path = _launcher_dir() / _LAUNCHER_FILENAME
    effective_dry_run = request.dry_run or _cfg.is_mock_mode()
    argv = _build_argv(launcher_path, request, dry_run=effective_dry_run)
    events_uri = _build_events_uri(project_id, vm_name, run_ts)

    log_event(
        "ADAPTER_FETCH_STARTED",
        service="deployment-api",
        details={
            "endpoint": "POST /api/ml/experiment/launch",
            "vm_name": vm_name,
            "asset_group": request.asset_group,
            "instruments": request.instruments,
            "dry_run": effective_dry_run,
            "correlation_id": correlation_id,
        },
    )

    if not effective_dry_run:
        if not launcher_path.exists():
            raise HTTPException(
                status_code=500,
                detail=f"Launcher script not found: {launcher_path}",
            )
        try:
            result = subprocess.run(
                argv,
                capture_output=True,
                text=True,
                timeout=_SUBPROCESS_TIMEOUT_SECONDS,
                env={**_process_env, "GCP_PROJECT_ID": project_id},
            )
        except subprocess.TimeoutExpired:
            raise HTTPException(status_code=504, detail="Launcher timed out") from None
        if result.returncode != 0:
            logger.error("ml-experiment launcher failed: %s", result.stderr[:2000])
            raise HTTPException(
                status_code=502,
                detail=f"Launcher exited {result.returncode}: {result.stderr[:500]}",
            )
        effective_argv: list[str] = []
    else:
        effective_argv = argv

    log_event(
        "ADAPTER_FETCH_COMPLETED",
        service="deployment-api",
        details={
            "endpoint": "POST /api/ml/experiment/launch",
            "vm_name": vm_name,
            "dry_run": effective_dry_run,
            "correlation_id": correlation_id,
        },
    )

    return MlExperimentLaunchResult(
        vm_name=vm_name,
        zone=_DEFAULT_ZONE,
        project_id=project_id,
        launched_at=launched_at,
        correlation_id=correlation_id,
        launcher_script=_LAUNCHER_FILENAME,
        dry_run=effective_dry_run,
        events_uri=events_uri,
        argv=effective_argv,
    )

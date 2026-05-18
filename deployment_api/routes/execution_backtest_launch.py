"""POST /api/execution/backtest/launch — paper-trade / execution-alpha VM.

Fires launch-strategy-paper-vm.sh in Tenderly-fork mode, which replays
execution through execution-service to measure execution-alpha (slippage,
fill quality, timing) against historical on-chain prices.

VM prefix `strategy-paper-` is registered in vm_zombie_watchdog.py.
Singleton-locked per archetype — use force=True to bypass.
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
_LAUNCHER_FILENAME = "launch-strategy-paper-vm.sh"
_VM_PREFIX = "strategy-paper-"
_SERVICE = "strategy-service"
_SUBPROCESS_TIMEOUT_SECONDS = 600

_VALID_ARCHETYPES = frozenset({"carry_staked_basis", "arbitrage_price_dispersion"})


class ExecutionBacktestLaunchRequest(BaseModel):
    """Request payload for POST /api/execution/backtest/launch."""

    archetype: str = Field(
        ...,
        description="DeFi archetype: carry_staked_basis | arbitrage_price_dispersion.",
    )
    tick_interval: int = Field(
        default=3600,
        description="Strategy tick interval in seconds (default: 3600 = 1h).",
        ge=60,
        le=86400,
    )
    continuous: bool = Field(
        default=False,
        description="Run continuously (paper-trading loop) rather than single-tick replay.",
    )
    force: bool = Field(
        default=False,
        description="Bypass per-archetype singleton-lock.",
    )
    dry_run: bool = Field(
        default=False,
        description="Echo-only mode — resolves argv but does NOT shell out.",
    )


class ExecutionBacktestLaunchResult(BaseModel):
    """Response for POST /api/execution/backtest/launch."""

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
    req: ExecutionBacktestLaunchRequest,
    dry_run: bool,
) -> list[str]:
    argv: list[str] = ["bash", str(launcher_path)]
    argv += ["--archetype", req.archetype]
    argv += ["--tick-interval", str(req.tick_interval)]
    if req.continuous:
        argv.append("--continuous")
    if req.force:
        argv.append("--force")
    if dry_run:
        argv.append("--dry-run")
    return argv


def _build_events_uri(project_id: str, vm_name: str, launched_at: datetime) -> str:
    date = launched_at.strftime("%Y-%m-%d")
    return f"gs://{project_id}-events/events/{_SERVICE}/{date}/{vm_name}/"  # noqa: gs-uri  — events bucket env-tier pending Phase 2.6 sub-step


@router.post("/launch", response_model=ExecutionBacktestLaunchResult)
def launch_execution_backtest(
    request: ExecutionBacktestLaunchRequest,
    _rbac: None = Depends(require_permission(Permission.DEPLOY_TRIGGER)),
) -> ExecutionBacktestLaunchResult:
    """Launch an execution-alpha paper-trade VM for the given archetype."""
    archetype_norm = request.archetype.lower()
    if archetype_norm not in _VALID_ARCHETYPES:
        raise HTTPException(
            status_code=400,
            detail=f"Unknown archetype '{request.archetype}'. Valid: {sorted(_VALID_ARCHETYPES)}",
        )

    correlation_id = str(uuid.uuid4())
    launched_at = datetime.now(UTC)
    run_ts = launched_at.strftime("%Y%m%d-%H%M%S")
    slug = archetype_norm.replace("_", "-")[:20]
    vm_name = f"{_VM_PREFIX}{slug}-{run_ts}"
    project_id = _cfg.gcp_project_id
    launcher_path = _launcher_dir() / _LAUNCHER_FILENAME
    effective_dry_run = request.dry_run or _cfg.is_mock_mode()
    argv = _build_argv(launcher_path, request, dry_run=effective_dry_run)
    events_uri = _build_events_uri(project_id, vm_name, launched_at)

    log_event(
        "ADAPTER_FETCH_STARTED",
        service="deployment-api",
        details={
            "endpoint": "POST /api/execution/backtest/launch",
            "vm_name": vm_name,
            "archetype": archetype_norm,
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
            logger.error("execution-backtest launcher failed: %s", result.stderr[:2000])
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
            "endpoint": "POST /api/execution/backtest/launch",
            "vm_name": vm_name,
            "dry_run": effective_dry_run,
            "correlation_id": correlation_id,
        },
    )

    return ExecutionBacktestLaunchResult(
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

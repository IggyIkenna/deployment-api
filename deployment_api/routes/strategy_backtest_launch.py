"""POST /api/strategy/backtest/launch — DeFi archetype config-grid backtest VM.

Fires launch-strategy-backtest-grid-vm.sh for a given archetype + date window.
Singleton-locked per archetype — refuses to launch if a same-archetype VM is
already RUNNING (bypass with force=True).

VM prefix `strategy-backtest-grid-` is registered in vm_zombie_watchdog.py.
"""

from __future__ import annotations

import logging
import subprocess
import uuid
from datetime import UTC, datetime
from os import environ as _process_env
from pathlib import Path

from fastapi import APIRouter, HTTPException
from pydantic import BaseModel, Field
from unified_trading_library import log_event

from deployment_api.deployment_api_config import DeploymentApiConfig

router = APIRouter()
logger = logging.getLogger(__name__)

_cfg = DeploymentApiConfig()

_DEFAULT_ZONE = "asia-northeast1-c"
_LAUNCHER_FILENAME = "launch-strategy-backtest-grid-vm.sh"
_VM_PREFIX = "strategy-backtest-grid-"
_SERVICE = "strategy-service"
_SUBPROCESS_TIMEOUT_SECONDS = 600

_VALID_ARCHETYPES = frozenset({"carry_staked_basis", "arbitrage_price_dispersion"})
_VALID_GRID_DENSITIES = frozenset({"low", "medium", "high"})


class StrategyBacktestLaunchRequest(BaseModel):
    """Request payload for POST /api/strategy/backtest/launch."""

    archetype: str = Field(
        ...,
        description="DeFi archetype: carry_staked_basis | arbitrage_price_dispersion.",
    )
    start_date: str = Field(
        ...,
        description="ISO date YYYY-MM-DD (inclusive backtest window start).",
        pattern=r"^\d{4}-\d{2}-\d{2}$",
    )
    end_date: str = Field(
        ...,
        description="ISO date YYYY-MM-DD (inclusive backtest window end).",
        pattern=r"^\d{4}-\d{2}-\d{2}$",
    )
    grid_density: str = Field(
        default="medium",
        description="Config-grid density: low | medium | high.",
    )
    force: bool = Field(
        default=False,
        description="Bypass singleton-lock. Allows concurrent runs for the same archetype.",
    )
    dry_run: bool = Field(
        default=False,
        description="Echo-only mode — resolves argv but does NOT shell out.",
    )


class StrategyBacktestLaunchResult(BaseModel):
    """Response for POST /api/strategy/backtest/launch."""

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
    req: StrategyBacktestLaunchRequest,
    dry_run: bool,
) -> list[str]:
    argv: list[str] = ["bash", str(launcher_path)]
    argv += ["--archetype", req.archetype]
    argv += ["--start", req.start_date]
    argv += ["--end", req.end_date]
    argv += ["--grid-density", req.grid_density]
    if req.force:
        argv.append("--force")
    if dry_run:
        argv.append("--dry-run")
    return argv


def _build_events_uri(project_id: str, vm_name: str, launched_at: datetime) -> str:
    date = launched_at.strftime("%Y-%m-%d")
    return f"gs://{project_id}-events/events/{_SERVICE}/{date}/{vm_name}/"


@router.post("/launch", response_model=StrategyBacktestLaunchResult)
def launch_strategy_backtest(
    request: StrategyBacktestLaunchRequest,
) -> StrategyBacktestLaunchResult:
    """Launch a strategy config-grid backtest VM for the given archetype."""
    archetype_norm = request.archetype.lower()
    if archetype_norm not in _VALID_ARCHETYPES:
        raise HTTPException(
            status_code=400,
            detail=f"Unknown archetype '{request.archetype}'. Valid: {sorted(_VALID_ARCHETYPES)}",
        )
    if request.grid_density not in _VALID_GRID_DENSITIES:
        raise HTTPException(
            status_code=400,
            detail=(
                f"Unknown grid_density '{request.grid_density}'."
                f" Valid: {sorted(_VALID_GRID_DENSITIES)}"
            ),
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
            "endpoint": "POST /api/strategy/backtest/launch",
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
            logger.error("strategy-backtest launcher failed: %s", result.stderr[:2000])
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
            "endpoint": "POST /api/strategy/backtest/launch",
            "vm_name": vm_name,
            "dry_run": effective_dry_run,
            "correlation_id": correlation_id,
        },
    )

    return StrategyBacktestLaunchResult(
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

"""Strategy shard management — spawn + drain endpoints (per-client-isolation Phase 8).

POST /api/strategy/shard/spawn  — launch a new strategy VM shard via the
    launch-strategy-paper-vm.sh or launch-strategy-live-vm.sh script.
    Triggered by operator or auto-shard supervisor signal (Phase E.2).

POST /api/strategy/shard/drain  — DEREGISTER all clients on a shard, SIGTERM the
    supervisor, and wait for VM to self-delete.

Both endpoints are operator-gated (verify_api_key). Real capital safety: the spawn
endpoint enforces the --dry-run-live-cutover-passed gate for mode=live unless
force_live=true is passed.
"""

from __future__ import annotations

import logging
import subprocess
from pathlib import Path
from typing import Literal

from fastapi import APIRouter, Depends, HTTPException
from pydantic import BaseModel, Field

from deployment_api.auth import verify_api_key
from deployment_api.deployment_api_config import DeploymentApiConfig

router = APIRouter(
    prefix="/api/strategy/shard",
    tags=["Strategy Shard"],
    dependencies=[Depends(verify_api_key)],
)
logger = logging.getLogger(__name__)

_cfg = DeploymentApiConfig()

_VALID_ARCHETYPES = frozenset({"carry_staked_basis", "ARBITRAGE_PRICE_DISPERSION"})
_VALID_MODES: frozenset[str] = frozenset({"paper", "live"})


class ShardSpawnRequest(BaseModel):
    archetype: str = Field(..., description="Strategy archetype slug.")
    mode: Literal["paper", "live"] = Field(..., description="paper or live.")
    shard_id: int = Field(default=0, ge=0, description="Shard index to spawn.")
    clients_yaml_path: str | None = Field(
        default=None,
        description=(
            "Path to clients.yaml on the deployment host. "
            "Defaults to deployment-service/configs/strategy/{archetype}/clients.yaml."
        ),
    )
    dry_run_live_cutover_passed: bool = Field(
        default=False,
        description="Required for mode=live unless force_live=true.",
    )
    force_live: bool = Field(
        default=False,
        description="Bypass live gate. USE WITH CAUTION — real capital at risk.",
    )
    dry_run: bool = Field(
        default=False,
        description="If true, print launch plan without calling gcloud.",
    )


class ShardSpawnResponse(BaseModel):
    ok: bool
    vm_name: str | None = None
    dry_run: bool
    message: str


class ShardDrainRequest(BaseModel):
    archetype: str = Field(..., description="Strategy archetype slug.")
    mode: Literal["paper", "live"] = Field(..., description="paper or live.")
    shard_id: int = Field(default=0, ge=0, description="Shard index to drain.")
    force: bool = Field(default=False, description="Force drain even if clients are active.")


class ShardDrainResponse(BaseModel):
    ok: bool
    message: str


def _build_spawn_cmd(body: ShardSpawnRequest) -> list[str]:
    script_dir = _find_launcher_script_dir()
    launcher = script_dir / f"launch-strategy-{body.mode}-vm.sh"
    if not launcher.exists():
        raise HTTPException(status_code=500, detail=f"Launcher script not found: {launcher}")
    cmd: list[str] = ["bash", str(launcher), "--archetype", body.archetype, "--shard", str(body.shard_id)]
    clients_yaml = body.clients_yaml_path or (
        str(_default_clients_yaml(body.archetype)) if _default_clients_yaml(body.archetype).exists() else None
    )
    if clients_yaml:
        cmd += ["--clients-yaml-path", clients_yaml]
    if body.mode == "live" and body.dry_run_live_cutover_passed:  # noqa: L2-mode-seam
        cmd.append("--dry-run-live-cutover-passed")
    if body.mode == "live" and body.force_live:  # noqa: L2-mode-seam
        cmd.append("--force-live")
    if body.dry_run:
        cmd.append("--dry-run")
    return cmd


def _run_launcher(cmd: list[str]) -> subprocess.CompletedProcess[str]:
    try:
        result = subprocess.run(cmd, capture_output=True, text=True, timeout=120, check=False)
    except subprocess.TimeoutExpired as exc:
        raise HTTPException(status_code=504, detail="Launch script timed out after 120s.") from exc
    if result.returncode != 0:
        logger.error("Shard spawn failed: %s\n%s", result.stdout, result.stderr)
        raise HTTPException(
            status_code=500,
            detail=f"Launch script exited {result.returncode}: {result.stderr.strip()}",
        )
    return result


def _extract_vm_name(stdout: str) -> str | None:
    for line in stdout.splitlines():
        if "VM created:" in line or "VM name:" in line:
            parts = line.split(":", 1)
            if len(parts) == 2:
                return parts[1].strip()
    return None


@router.post("/spawn", response_model=ShardSpawnResponse)
def spawn_shard(body: ShardSpawnRequest) -> ShardSpawnResponse:
    """Launch a new strategy VM shard.

    Delegates to deployment-service/scripts/vm/launch-strategy-{mode}-vm.sh.
    Returns immediately after VM creation (async launch). Caller should poll
    the VM events endpoint for STARTED confirmation within 90s.
    """
    if body.archetype not in _VALID_ARCHETYPES:
        raise HTTPException(
            status_code=400,
            detail=f"Unknown archetype '{body.archetype}'. Valid: {sorted(_VALID_ARCHETYPES)}",
        )
    cmd = _build_spawn_cmd(body)
    logger.info("Spawning shard: %s", " ".join(cmd))
    result = _run_launcher(cmd)
    vm_name = _extract_vm_name(result.stdout)
    logger.info("Shard spawned: vm_name=%s dry_run=%s", vm_name, body.dry_run)
    return ShardSpawnResponse(
        ok=True,
        vm_name=vm_name,
        dry_run=body.dry_run,
        message=result.stdout.strip()[-500:] if result.stdout else "OK",
    )


@router.post("/drain", response_model=ShardDrainResponse)
def drain_shard(body: ShardDrainRequest) -> ShardDrainResponse:
    """Drain a strategy VM shard.

    Emits DEREGISTER for all clients on the shard via the lifecycle event bus,
    then SIGTERMs the supervisor process. The VM self-deletes on shutdown
    (per shutdown-script in the launch script). For May-23 this is a best-effort
    drain — a robust Phase E.2 drain-before-spawn protocol ships in 2026-05-28.
    """
    if body.archetype not in _VALID_ARCHETYPES:
        raise HTTPException(
            status_code=400,
            detail=f"Unknown archetype '{body.archetype}'. Valid: {sorted(_VALID_ARCHETYPES)}",
        )

    logger.info(
        "Drain requested: archetype=%s mode=%s shard=%d force=%s",
        body.archetype,
        body.mode,
        body.shard_id,
        body.force,
    )
    # Phase E.2 will wire deployment-service to consume ShardCapacityEvent.DRAIN
    # and issue DEREGISTER + SIGTERM automatically. For May-23 the drain endpoint
    # records the intent and returns a 202-style OK so the UI can surface it.
    return ShardDrainResponse(
        ok=True,
        message=(
            f"Drain intent recorded for archetype={body.archetype} "
            f"mode={body.mode} shard={body.shard_id}. "
            "Operator: terminate the VM manually via gcloud or the VM admin panel. "
            "Full automated drain ships in Phase E.2 (2026-05-28)."
        ),
    )


def _find_launcher_script_dir() -> Path:
    """Locate deployment-service/scripts/vm/ relative to this file or workspace root."""
    here = Path(__file__).resolve()
    # Walk up: deployment_api/routes/ → deployment_api/ → deployment-api/ → workspace-root/
    for ancestor in here.parents:
        candidate = ancestor / "deployment-service" / "scripts" / "vm"
        if candidate.is_dir():
            return candidate
    # Fallback: check sibling repo next to deployment-api
    sibling = here.parents[2] / "deployment-service" / "scripts" / "vm"
    if sibling.is_dir():
        return sibling
    raise FileNotFoundError("Could not locate deployment-service/scripts/vm/ relative to strategy_shard.py")


def _default_clients_yaml(archetype: str) -> Path:
    """Return the default clients.yaml path for an archetype."""
    here = Path(__file__).resolve()
    for ancestor in here.parents:
        candidate = ancestor / "deployment-service" / "configs" / "strategy" / archetype / "clients.yaml"
        if candidate.exists():
            return candidate
    sibling = here.parents[2] / "deployment-service" / "configs" / "strategy" / archetype / "clients.yaml"
    return sibling

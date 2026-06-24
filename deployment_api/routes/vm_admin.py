"""POST /api/vm/admin/{vm_name}/(cancel|pause|resume) — operator VM control.

Cancel:  marks deployment as cancelled in the GCS registry (terminal state).
Pause:   writes a GCS pause-signal blob that VMs poll for cooperative pause.
Resume:  deletes the pause-signal blob to allow the VM to continue.

All three return 202 Accepted immediately; the VM acts asynchronously.
Mock mode: returns synthetic success without touching GCS.
"""

from __future__ import annotations

import logging
from datetime import UTC, datetime

from deployment_service.deployments_registry import (
    DEFAULT_BUCKET,
    DeploymentRegistryEntry,
    DeploymentsRegistry,
)
from fastapi import APIRouter, Depends, HTTPException
from pydantic import BaseModel
from unified_api_contracts.internal.schemas.rbac import (  # noqa: deep-import — RBAC not re-exported from UIC top-level yet
    Permission,
)
from unified_trading_library import log_event

from deployment_api.deployment_api_config import DeploymentApiConfig
from deployment_api.rbac import require_permission
from deployment_api.utils.storage_facade import delete_object, write_object_text

router = APIRouter()
logger = logging.getLogger(__name__)

_cfg = DeploymentApiConfig()

_SIGNALS_PREFIX = "deployments/signals/"


class AdminActionResult(BaseModel):  # CORRECT-LOCAL: FastAPI API contract model
    vm_name: str
    action: str
    status: str
    message: str


class RestartActionResult(AdminActionResult):  # CORRECT-LOCAL: FastAPI API contract model
    """A restart result — the stop half + the resolved relaunch handle.

    ``relaunch_launcher`` is the ``scripts/vm/launch-*.sh`` the caller relaunches through
    (or ``None`` when the VM has no deterministic relaunch — a singleton / one-off);
    ``relaunchable`` mirrors that as a bool. ``cancelled`` is whether an active deployment
    was stopped (a long-lived VM not in the active registry restarts via the launcher alone).
    """

    relaunch_launcher: str | None = None
    relaunchable: bool = False
    cancelled: bool = False


def _utcnow_iso() -> str:
    return datetime.now(UTC).isoformat(timespec="seconds").replace("+00:00", "Z")


def _find_active_by_vm_name(registry: DeploymentsRegistry, vm_name: str) -> DeploymentRegistryEntry | None:
    for entry in registry.list_active():
        if entry.vm_name == vm_name:
            return entry
    return None


@router.post("/vm/admin/{vm_name}/cancel", response_model=AdminActionResult, status_code=202)
def cancel_vm(
    vm_name: str,
    _check: None = Depends(require_permission(Permission.DEPLOY_TRIGGER)),
) -> AdminActionResult:
    """Mark a running VM deployment as cancelled (terminal state → archive).

    Returns 202 immediately; GCS is updated synchronously before responding.
    Returns 404 if no active deployment matches vm_name.
    In mock mode, returns a synthetic success without GCS writes.
    """
    if _cfg.is_mock_mode():
        logger.info("mock cancel for vm_name=%s", vm_name)
        log_event(
            "VM_CANCEL_REQUESTED",
            severity="WARNING",
            details={"vm_name": vm_name, "mock": True},
        )
        return AdminActionResult(
            vm_name=vm_name,
            action="cancel",
            status="cancelled",
            message=f"VM '{vm_name}' marked as cancelled (mock mode).",
        )

    registry = DeploymentsRegistry(bucket=DEFAULT_BUCKET)
    try:
        entry = _find_active_by_vm_name(registry, vm_name)
    except (OSError, ValueError, RuntimeError) as exc:
        logger.exception("Failed to read registry for cancel: %s", exc)
        raise HTTPException(status_code=502, detail="VM deployments registry unavailable") from exc

    if entry is None:
        raise HTTPException(
            status_code=404,
            detail=f"No active deployment found for vm_name '{vm_name}'",
        )

    entry.status = "failed"
    entry.exit_code = -1
    entry.completed_at = _utcnow_iso()
    entry.extras["cancel_reason"] = "operator_cancel"
    try:
        registry.complete(entry)
    except (OSError, ValueError, RuntimeError) as exc:
        logger.exception("Failed to archive cancelled deployment %s: %s", vm_name, exc)
        raise HTTPException(status_code=502, detail="Failed to persist cancel") from exc

    logger.info("cancelled vm_name=%s deployment_id=%s", vm_name, entry.deployment_id)
    log_event(
        "VM_CANCEL_REQUESTED",
        severity="WARNING",
        details={"vm_name": vm_name, "deployment_id": entry.deployment_id, "mock": False},
    )
    return AdminActionResult(
        vm_name=vm_name,
        action="cancel",
        status="cancelled",
        message=f"VM '{vm_name}' cancelled and archived as failed (id={entry.deployment_id}).",
    )


@router.post("/vm/admin/{vm_name}/pause", response_model=AdminActionResult, status_code=202)
def pause_vm(
    vm_name: str,
    _check: None = Depends(require_permission(Permission.DEPLOY_TRIGGER)),
) -> AdminActionResult:
    """Write a pause-signal blob to GCS.

    VMs that implement cooperative pause poll for this blob and suspend
    processing until the resume signal is received.
    Returns 202; the VM may not pause immediately.
    """
    if _cfg.is_mock_mode():
        log_event(
            "VM_PAUSE_REQUESTED",
            severity="INFO",
            details={"vm_name": vm_name, "mock": True},
        )
        return AdminActionResult(
            vm_name=vm_name,
            action="pause",
            status="pause_requested",
            message=f"Pause signal written for '{vm_name}' (mock mode).",
        )

    signal_key = f"{_SIGNALS_PREFIX}{vm_name}/pause"
    try:
        write_object_text(DEFAULT_BUCKET, signal_key, _utcnow_iso())
    except (OSError, RuntimeError) as exc:
        logger.exception("Failed to write pause signal for %s: %s", vm_name, exc)
        raise HTTPException(status_code=502, detail="Failed to write pause signal") from exc

    logger.info("pause signal written for vm_name=%s key=%s", vm_name, signal_key)
    log_event(
        "VM_PAUSE_REQUESTED",
        severity="INFO",
        details={"vm_name": vm_name, "signal_key": signal_key, "mock": False},
    )
    return AdminActionResult(
        vm_name=vm_name,
        action="pause",
        status="pause_requested",
        message=f"Pause signal written for '{vm_name}'. VM will pause on next heartbeat.",
    )


@router.post("/vm/admin/{vm_name}/resume", response_model=AdminActionResult, status_code=202)
def resume_vm(
    vm_name: str,
    _check: None = Depends(require_permission(Permission.DEPLOY_TRIGGER)),
) -> AdminActionResult:
    """Delete the pause-signal blob to allow the VM to resume processing.

    Returns 202. If no pause signal exists, returns success (idempotent).
    """
    if _cfg.is_mock_mode():
        log_event(
            "VM_RESUME_REQUESTED",
            severity="INFO",
            details={"vm_name": vm_name, "mock": True},
        )
        return AdminActionResult(
            vm_name=vm_name,
            action="resume",
            status="resumed",
            message=f"Pause signal cleared for '{vm_name}' (mock mode).",
        )

    signal_key = f"{_SIGNALS_PREFIX}{vm_name}/pause"
    try:
        delete_object(DEFAULT_BUCKET, signal_key)
    except (OSError, RuntimeError) as exc:
        logger.exception("Failed to delete pause signal for %s: %s", vm_name, exc)
        raise HTTPException(status_code=502, detail="Failed to clear pause signal") from exc

    logger.info("resume signal cleared for vm_name=%s", vm_name)
    log_event(
        "VM_RESUME_REQUESTED",
        severity="INFO",
        details={"vm_name": vm_name, "mock": False},
    )
    return AdminActionResult(
        vm_name=vm_name,
        action="resume",
        status="resumed",
        message=f"Pause signal cleared for '{vm_name}'. VM will resume processing.",
    )


@router.post("/vm/admin/{vm_name}/restart", response_model=RestartActionResult, status_code=202)
def restart_vm(
    vm_name: str,
    _check: None = Depends(require_permission(Permission.DEPLOY_TRIGGER)),
) -> RestartActionResult:
    """One-call restart = STOP the current deployment + resolve its relaunch launcher.

    A genuine convenience over the prior two-step stop-then-relaunch (the gap the Phase-6
    audit surfaced): it marks the active deployment cancelled (the same GCS-registry stop as
    ``/cancel`` — composed, not duplicated) AND resolves the relaunch launcher via
    deployment-service ``resolve_launcher_for_vm`` so the caller relaunches through the
    VERIFIED Deploy/launcher path. **It does NOT itself spawn a VM** — a fire-and-forget
    launch is banned (every VM launch needs STARTED+progress+STOPPED event verification), so
    the actual relaunch goes through the launcher this response names. Cloud-agnostic: the
    cancel + signals are GCS-based and polled identically by GCP and AWS VMs (so there is no
    GCP-only control here needing AWS parity — the audit's other half).

    202; 404 only when there is neither an active deployment to stop NOR a relaunch launcher.
    """
    if _cfg.is_mock_mode():
        log_event(
            "VM_RESTART_REQUESTED",
            severity="WARNING",
            details={"vm_name": vm_name, "mock": True},
        )
        return RestartActionResult(
            vm_name=vm_name,
            action="restart",
            status="restart_initiated",
            cancelled=True,
            message=f"VM '{vm_name}' restart initiated (mock mode).",
        )

    # Lazy import — the deployment_service.data_pipeline_monitors sub-package is imported at
    # CALL time (same deferred pattern as routes/_aws_deployments.py), so a top-level import
    # doesn't break app import in environments that resolve only the base package.
    from deployment_service.data_pipeline_monitors.launcher_registry import resolve_launcher_for_vm

    launcher = resolve_launcher_for_vm(vm_name)
    relaunchable = launcher is not None

    registry = DeploymentsRegistry(bucket=DEFAULT_BUCKET)
    try:
        entry = _find_active_by_vm_name(registry, vm_name)
    except (OSError, ValueError, RuntimeError) as exc:
        logger.exception("Failed to read registry for restart: %s", exc)
        raise HTTPException(status_code=502, detail="VM deployments registry unavailable") from exc

    if entry is None and not relaunchable:
        raise HTTPException(
            status_code=404,
            detail=f"No active deployment and no relaunch launcher for vm_name '{vm_name}' — nothing to restart",
        )

    cancelled = False
    if entry is not None:
        entry.status = "failed"
        entry.exit_code = -1
        entry.completed_at = _utcnow_iso()
        entry.extras["cancel_reason"] = "operator_restart"
        try:
            registry.complete(entry)
        except (OSError, ValueError, RuntimeError) as exc:
            logger.exception("Failed to archive deployment for restart %s: %s", vm_name, exc)
            raise HTTPException(status_code=502, detail="Failed to persist restart stop") from exc
        cancelled = True

    logger.info("restart requested vm_name=%s relaunch_launcher=%s cancelled=%s", vm_name, launcher, cancelled)
    log_event(
        "VM_RESTART_REQUESTED",
        severity="WARNING",
        details={
            "vm_name": vm_name,
            "deployment_id": entry.deployment_id if entry is not None else None,
            "relaunch_launcher": launcher,
            "cancelled": cancelled,
            "mock": False,
        },
    )
    relaunch_hint = (
        f"relaunch via `{launcher}`" if relaunchable else "no deterministic relaunch — relaunch from the Deploy console"
    )
    return RestartActionResult(
        vm_name=vm_name,
        action="restart",
        status="restart_initiated",
        relaunch_launcher=launcher,
        relaunchable=relaunchable,
        cancelled=cancelled,
        message=f"VM '{vm_name}' stopped ({cancelled=}); {relaunch_hint}.",
    )

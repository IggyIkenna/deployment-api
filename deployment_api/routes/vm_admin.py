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

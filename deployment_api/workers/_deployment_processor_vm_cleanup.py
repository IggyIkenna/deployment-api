"""
Orphan VM cleanup and completed-pending-delete (CPD) transition helpers.

Covers:
- _collect_retry_orphans / _collect_new_orphan_tuples / _build_orphan_fire_list
- _handle_orphan_vm_cleanup
- _terminate_stuck_vm
- _fire_cpd_running_vm_deletes / _transition_cpd_to_completed
- _handle_completed_pending_delete
"""

import json
import logging
import time
from datetime import datetime
from typing import cast

from deployment_api import settings

from ._deployment_processor_helpers import (
    DEPLOYMENT_ENV,
    PROJECT_ID,
    STATE_BUCKET,
    _cancel_vm_jobs_sync,
    _vm_status_from_map,
    _vm_zone_from_map,
)

# Import shared pending VM deletes dict from auto_sync.
from .auto_sync import pending_vm_deletes

logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Orphan VM cleanup helpers
# ---------------------------------------------------------------------------


def _collect_retry_orphans(
    vm_map: dict[str, object],
    now_ts: float,
    orphan_retry_s: int,
) -> list[str]:
    """
    Return job_ids in *pending_vm_deletes* that are overdue for a retry and
    whose VM is still RUNNING in *vm_map*.
    """
    return [
        jid
        for jid, val in pending_vm_deletes.items()
        if now_ts - val[0] >= orphan_retry_s and _vm_status_from_map(vm_map, jid) == "RUNNING"
    ]


def _collect_new_orphan_tuples(
    shards: list[dict[str, object]],
    vm_map: dict[str, object],
    shard_statuses: dict[str, tuple[str, str]],
) -> list[tuple[str, str | None, str, tuple[str, str]]]:
    """
    Return ``(job_id, zone, shard_id, status_tuple)`` for shards that have a
    terminal GCS status but whose VM is still RUNNING in *vm_map*.
    """
    result: list[tuple[str, str | None, str, tuple[str, str]]] = []
    for shard in shards:
        shard_id = cast(str, shard.get("shard_id"))
        job_id = cast(str, shard.get("job_id"))
        if not job_id or not shard_id:
            continue
        st = shard_statuses.get(shard_id)
        if not st or st[0] not in ("succeeded", "failed"):
            continue
        if _vm_status_from_map(vm_map, job_id) == "RUNNING":
            zone = _vm_zone_from_map(vm_map, job_id)
            result.append((job_id, zone, shard_id, st))
    return result


def _build_orphan_fire_list(
    retry_job_ids: list[str],
    orphan_tuples: list[tuple[str, str | None, str, tuple[str, str]]],
    vm_map: dict[str, object],
    orphan_max: int,
    now_ts: float,
) -> list[tuple[str, str | None]]:
    """
    Build the list of (job_id, zone) pairs to fire-and-forget delete, up to
    *orphan_max* entries.  Retries come first; then new orphans not already
    tracked in *pending_vm_deletes*.

    Side-effect: updates *pending_vm_deletes* for newly queued orphans.
    """
    to_fire: list[tuple[str, str | None]] = []
    for jid in retry_job_ids:
        if len(to_fire) >= orphan_max:
            break
        zone = _vm_zone_from_map(vm_map, jid)
        to_fire.append((jid, zone))
    for job_id, zone, _shard_id, _st in orphan_tuples:
        if len(to_fire) >= orphan_max:
            break
        if job_id not in pending_vm_deletes:
            to_fire.append((job_id, zone))
            pending_vm_deletes[job_id] = (now_ts, zone)
    return to_fire


def _handle_orphan_vm_cleanup(
    vm_map: dict[str, object],
    shards: list[dict[str, object]],
    shard_statuses: dict[str, tuple[str, str]],
    config: dict[str, object],
    deployment_id: str,
) -> int:
    """Handle cleanup of orphaned VMs that are still running after completion."""
    from deployment_api.utils.config_validation import ConfigurationError, ValidationUtils

    orphan_max = settings.ORPHAN_DELETE_MAX_PARALLEL
    orphan_retry_s = settings.ORPHAN_DELETE_RETRY_SECONDS
    now_ts = time.time()

    # 1. Retry: pending deletes older than Xs where VM still RUNNING.
    retry_job_ids = _collect_retry_orphans(vm_map, now_ts, orphan_retry_s)
    for jid in retry_job_ids:
        zone = _vm_zone_from_map(vm_map, jid) or pending_vm_deletes[jid][1]
        pending_vm_deletes[jid] = (now_ts, zone)

    # 2. Collect new orphans to terminate.
    orphan_tuples = _collect_new_orphan_tuples(shards, vm_map, shard_statuses)

    # 3. Clean pending: VMs no longer in vm_map (deleted).
    for jid in list(pending_vm_deletes.keys()):
        if jid not in vm_map:
            del pending_vm_deletes[jid]

    # 4. Fire-and-forget: retries first, then new orphans, up to orphan_max total.
    to_fire = _build_orphan_fire_list(retry_job_ids, orphan_tuples, vm_map, orphan_max, now_ts)

    if to_fire:
        try:
            service_account_email = str(
                ValidationUtils.get_required(
                    config, "service_account_email", "bulk cancellation orchestrator"
                )
            )
            job_name = str(
                ValidationUtils.get_required(config, "job_name", "bulk cancellation backend")
            )
            _cancel_vm_jobs_sync(
                deployment_id=deployment_id,
                project_id=PROJECT_ID,
                region=cast(str, config.get("region") or "asia-northeast1"),
                service_account_email=service_account_email,
                state_bucket=STATE_BUCKET,
                state_prefix=f"deployments.{DEPLOYMENT_ENV}",
                job_name=job_name,
                jobs=[(job_id, zone) for job_id, zone in to_fire],
                fire_and_forget=True,
            )
            logger.info("[AUTO_SYNC] Fired %s orphan VM deletes (job done)", len(to_fire))
        except ConfigurationError as e:
            logger.error("[BULK_CANCEL_VMS] %s: Configuration error - %s", deployment_id, e)
            return 0
        except (OSError, ValueError, RuntimeError) as e:
            logger.debug("[AUTO_SYNC] VM fire-and-forget failed: %s", e)

    return len(to_fire)


# ---------------------------------------------------------------------------
# Stuck VM termination helper
# ---------------------------------------------------------------------------


def _terminate_stuck_vm(
    shard: dict[str, object],
    config: dict[str, object],
    deployment_id: str,
    now: datetime,
) -> None:
    """
    Attempt to cancel the VM backing *shard* and close the latest execution
    history entry.  All errors are suppressed (best-effort).
    """
    job_id = shard.get("job_id")
    if job_id:
        try:
            _cancel_vm_jobs_sync(
                deployment_id=deployment_id,
                project_id=PROJECT_ID,
                region=cast(str, config.get("region") or "asia-northeast1"),
                service_account_email=cast(str, config.get("service_account_email") or ""),
                state_bucket=STATE_BUCKET,
                state_prefix=f"deployments.{settings.DEPLOYMENT_ENV}",
                job_name=cast(str, config.get("job_name") or ""),
                jobs=[(cast(str, job_id), cast(str | None, config.get("zone")))],
                fire_and_forget=False,
            )
            logger.info("[AUTO_SYNC] Terminated stuck VM %s (timeout exceeded)", job_id)
        except (OSError, ValueError, RuntimeError) as e:
            logger.debug("[AUTO_SYNC] Stuck VM termination failed: %s", e)

    history = cast(list[dict[str, object]], shard.get("execution_history") or [])
    if history:
        history[-1]["ended_at"] = now.isoformat()
        history[-1]["status"] = "failed"
        history[-1]["failure_reason"] = shard.get("error_message")
    shard["execution_history"] = history


# ---------------------------------------------------------------------------
# completed_pending_delete transition helpers
# ---------------------------------------------------------------------------


def _fire_cpd_running_vm_deletes(
    shards: list[dict[str, object]],
    vm_map_cpd: dict[str, object],
    config: dict[str, object],
    deployment_id: str,
) -> bool:
    """
    Fire delete requests for any RUNNING VMs found during ``completed_pending_delete`` handling.

    Returns True when at least one delete was fired (caller should return 0 and
    wait for the next tick), False when no VMs were running.
    """
    running_vms = [
        (
            cast(str, s.get("job_id")),
            _vm_zone_from_map(vm_map_cpd, cast(str, s.get("job_id"))),
        )
        for s in shards
        if s.get("job_id")
        and _vm_status_from_map(vm_map_cpd, cast(str, s.get("job_id"))) == "RUNNING"
    ]
    orphan_max_cpd = settings.ORPHAN_DELETE_MAX_PARALLEL
    to_fire_cpd = running_vms[:orphan_max_cpd]
    if not to_fire_cpd:
        return False
    now_ts_cpd = time.time()
    for job_id, zone in to_fire_cpd:
        pending_vm_deletes[job_id] = (now_ts_cpd, zone)
    try:
        _cancel_vm_jobs_sync(
            deployment_id=deployment_id,
            project_id=PROJECT_ID,
            region=cast(str, config.get("region") or "asia-northeast1"),
            service_account_email=cast(str, config.get("service_account_email") or ""),
            state_bucket=STATE_BUCKET,
            state_prefix=f"deployments.{DEPLOYMENT_ENV}",
            job_name=cast(str, config.get("job_name") or ""),
            jobs=[(jid, z) for jid, z in to_fire_cpd],
            fire_and_forget=True,
        )
        logger.info(
            "[AUTO_SYNC] completed_pending_delete: fired %s orphan deletes for %s",
            len(to_fire_cpd),
            deployment_id,
        )
    except (OSError, ValueError, RuntimeError) as e:
        logger.debug("[AUTO_SYNC] completed_pending_delete fire failed: %s", e)
    return True


def _transition_cpd_to_completed(
    state: dict[str, object],
    state_path: str,
    deployment_id: str,
    now: datetime,
) -> None:
    """
    Write the ``completed`` state to GCS and fire a deployment-updated event.

    Called when ``completed_pending_delete`` has no RUNNING VMs left.
    """
    from deployment_api.utils.storage_facade import write_object_text

    state["status"] = "completed"
    if not state.get("completed_at"):
        state["completed_at"] = now.isoformat()
    state["updated_at"] = now.isoformat()
    write_object_text(STATE_BUCKET, state_path, json.dumps(state, indent=2))
    logger.info(
        "[AUTO_SYNC] %s: completed_pending_delete -> completed (no RUNNING VMs)",
        deployment_id,
    )
    try:
        from deployment_api.utils.deployment_events import notify_deployment_updated_sync

        notify_deployment_updated_sync(deployment_id)
    except (OSError, ValueError, RuntimeError) as e:
        logger.warning("Unexpected error during operation: %s", e, exc_info=True)


def _handle_completed_pending_delete(
    state: dict[str, object],
    state_path: str,
    deployment_id: str,
    compute_type: str,
    shards: list[dict[str, object]],
    config: dict[str, object],
    now: datetime,
    release_deployment_lock: object,
) -> int | None:
    """
    Handle the ``completed_pending_delete`` state transition.

    Returns an integer result (0 or 1) if this function handled the state and
    the caller should return that value immediately.  Returns None if the
    deployment is NOT in ``completed_pending_delete`` and normal processing
    should continue.
    """
    from collections.abc import Callable

    from deployment_api.utils.storage_facade import write_object_text

    from ._deployment_processor_helpers import _is_deployment_completed_pending_delete

    _release_lock = cast(Callable[[str], bool], release_deployment_lock)

    if not _is_deployment_completed_pending_delete(state):
        return None

    # Non-VM deployments: immediately mark completed.
    if compute_type != "vm":
        state["status"] = "completed"
        state["updated_at"] = now.isoformat()
        write_object_text(STATE_BUCKET, state_path, json.dumps(state, indent=2))
        _release_lock(deployment_id)
        return 1

    service_name = cast(str, state.get("service") or "")
    if not service_name:
        return 0

    vm_map_cpd: dict[str, object] = {}
    try:
        from unified_trading_library.cloud_interface import get_compute_engine_client

        ce = get_compute_engine_client(project_id=PROJECT_ID)
        instances = ce.aggregated_list_instances(PROJECT_ID, f"name:{service_name}-*")
        for inst in instances:
            vm_map_cpd[cast(str, inst["name"])] = {
                "status": inst["status"],
                "zone": inst.get("zone"),
            }
    except (OSError, ValueError, RuntimeError) as e:
        logger.debug(
            "[AUTO_SYNC] completed_pending_delete aggregatedList failed for %s: %s",
            deployment_id,
            e,
        )
        return 0

    if _fire_cpd_running_vm_deletes(shards, vm_map_cpd, config, deployment_id):
        return 0

    _transition_cpd_to_completed(state, state_path, deployment_id, now)
    return 1

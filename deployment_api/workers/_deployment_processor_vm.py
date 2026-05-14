"""
VM health checks and serial-log event processing for deployment_processor.

Covers:
- OOM / startup-timeout detection (_check_oom_death_loop, _check_startup_timeout)
- Serial-log health inspection (_inspect_vm_shard_health, _collect_unhealthy_vms)
- Running shard / VM reconciliation (_reconcile_running_shards_with_vm_map)
- Serial-log event parsing for healthy VMs (_apply_serial_log_events)
- VM health kill application (_apply_vm_health_kills)
- VM map builder (_build_vm_map_for_service)
- Running job ID collection (_collect_running_job_ids)
- Top-level orchestrator (_process_vm_health_and_status)

Orphan VM cleanup and completed-pending-delete helpers:
    See _deployment_processor_vm_cleanup.py
"""

import logging
from datetime import datetime
from typing import cast

from deployment_api import settings
from deployment_api.utils.service_events import parse_service_event, update_shard_state_from_event

from ._deployment_processor_helpers import (
    DEPLOYMENT_ENV,
    PROJECT_ID,
    STATE_BUCKET,
    _cancel_vm_jobs_sync,
    _parse_shard_elapsed_seconds,
    _vm_status_from_map,
    _vm_zone_from_map,
)

# Re-export for callers that import these from this module.
from ._deployment_processor_vm_cleanup import (
    _handle_orphan_vm_cleanup,
)

logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# VM health sub-helpers
# ---------------------------------------------------------------------------


def _check_oom_death_loop(
    serial_logs: str,
    threshold: int,
) -> tuple[bool, str]:
    """
    Return (is_oom, message) for a VM serial log.

    *is_oom* is True when the OOM kill count meets or exceeds *threshold*.
    """
    oom_count = serial_logs.count("Out of memory: Killed process")
    if oom_count >= threshold:
        return True, f"{oom_count} OOM kills detected"
    return False, ""


def _check_startup_timeout(
    serial_logs: str,
    running_seconds: float,
    timeout: int,
) -> tuple[bool, str]:
    """
    Return (timed_out, message) for a VM that may have stalled during startup.

    *timed_out* is True when *running_seconds* exceeds *timeout* and no
    recognised startup marker appears in *serial_logs*.
    """
    has_startup = (
        "SERVICE_STARTED" in serial_logs
        or "SERVICE_EVENT: STARTED" in serial_logs
        or "Starting processing" in serial_logs
    )
    if running_seconds > timeout and not has_startup:
        return True, f"No startup signal after {int(running_seconds)}s"
    return False, ""


def _inspect_vm_shard_health(
    serial_logs: str,
    job_id: str,
    zone: str,
    shard_id: str,
    running_seconds: float,
    oom_threshold: int,
    startup_timeout: int,
) -> tuple[str, str | None, str, str, str] | None:
    """
    Classify VM health from pre-fetched *serial_logs*.

    Returns ``(job_id, zone, shard_id, reason, message)`` when the VM should be
    terminated, or None when it is healthy / no action required.
    """
    is_oom, oom_msg = _check_oom_death_loop(serial_logs, oom_threshold)
    if is_oom:
        return (job_id, zone, shard_id, "oom_death_loop", oom_msg)

    timed_out, timeout_msg = _check_startup_timeout(serial_logs, running_seconds, startup_timeout)
    if timed_out:
        return (job_id, zone, shard_id, "startup_timeout", timeout_msg)
    return None


def _collect_unhealthy_vms(
    shards: list[dict[str, object]],
    vm_map: dict[str, object],
    shard_statuses: dict[str, tuple[str, str]],
    oom_threshold: int,
    startup_timeout: int,
    now: datetime,
) -> list[tuple[str, str | None, str, str, str]]:
    """
    Inspect running VM shards for OOM loops and startup timeouts.

    Returns a list of ``(job_id, zone, shard_id, reason, message)`` tuples for
    VMs that should be terminated.
    """
    from unified_trading_library.cloud_interface import get_compute_engine_client

    ce_client = get_compute_engine_client(project_id=PROJECT_ID)
    kills: list[tuple[str, str | None, str, str, str]] = []

    for shard in shards:
        if shard.get("status") != "running":
            continue
        job_id = cast(str, shard.get("job_id"))
        shard_id = cast(str, shard.get("shard_id"))
        if not job_id or not shard_id or _vm_status_from_map(vm_map, job_id) != "RUNNING":
            continue
        if shard_id in shard_statuses:
            continue  # Already resolved via GCS

        running_seconds = _parse_shard_elapsed_seconds(shard, now)
        if running_seconds is None or running_seconds < 60:
            continue

        zone = _vm_zone_from_map(vm_map, job_id)
        if not zone:
            continue

        try:
            serial_logs = ce_client.get_serial_port_output(PROJECT_ID, zone, job_id, start=-8192)
            kill = _inspect_vm_shard_health(
                serial_logs, job_id, zone, shard_id, running_seconds, oom_threshold, startup_timeout
            )
            if kill:
                kills.append(kill)
        except (OSError, ValueError, RuntimeError) as e:
            logger.debug("[AUTO_SYNC] Serial log check failed for %s: %s", job_id, e)

    return kills


def _reconcile_running_shards_with_vm_map(
    running_job_ids: dict[str, str],
    vm_map: dict[str, object],
    shard_statuses: dict[str, tuple[str, str]],
) -> int:
    """
    For each job in *running_job_ids*, update *shard_statuses* based on
    whether the VM still exists in *vm_map*.

    Returns the count of shards whose status was set to 'failed' because the
    VM was no longer present in the VM map (i.e. terminated without a GCS
    status file).
    """
    failed_count = 0
    for job_id, shard_id in running_job_ids.items():
        vm_status = _vm_status_from_map(vm_map, job_id)
        if vm_status:
            shard_statuses[shard_id] = ("running", "vm_alive")
        else:
            if shard_id not in shard_statuses:
                shard_statuses[shard_id] = ("failed", "vm_terminated_no_status")
                failed_count += 1
    return failed_count


def _collect_running_job_ids(
    shards: list[dict[str, object]],
    shard_statuses: dict[str, tuple[str, str]],
) -> dict[str, str]:
    """
    Return a ``job_id -> shard_id`` map for running shards not yet resolved via GCS.
    """
    result: dict[str, str] = {}
    for shard in shards:
        if shard.get("status") != "running":
            continue
        job_id = cast(str, shard.get("job_id"))
        shard_id = cast(str, shard.get("shard_id"))
        if not job_id or not shard_id or shard_id in shard_statuses:
            continue
        result[job_id] = shard_id
    return result


def _apply_serial_log_events(
    serial_logs: str,
    shard_id: str,
    shards: list[dict[str, object]],
    updated: bool,
) -> bool:
    """
    Parse service events from *serial_logs* and apply them to the matching shard.

    Returns *updated* (True if any shard was mutated).
    """
    log_lines = serial_logs.splitlines()
    events: list[dict[str, object]] = []
    for line in log_lines:
        evt = parse_service_event(line)
        if evt:
            events.append(evt)
    if events:
        for s in shards:
            if s.get("shard_id") == shard_id:
                for evt in events:
                    update_shard_state_from_event(s, evt)
                updated = True
                break
    return updated


def _parse_healthy_vm_serial_events(
    shards: list[dict[str, object]],
    vm_map: dict[str, object],
    killed_shard_ids: set[str],
    now: datetime,
    updated: bool,
) -> bool:
    """
    For healthy (not-killed) running VMs, fetch serial logs and apply service events to shards.

    Returns *updated* (True if any shard was mutated).
    """
    from unified_trading_library.cloud_interface import get_compute_engine_client

    ce_client = get_compute_engine_client(project_id=PROJECT_ID)
    for shard in shards:
        if shard.get("status") != "running":
            continue
        shard_id = cast(str, shard.get("shard_id"))
        job_id = cast(str, shard.get("job_id"))
        if (
            not job_id
            or not shard_id
            or shard_id in killed_shard_ids
            or _vm_status_from_map(vm_map, job_id) != "RUNNING"
        ):
            continue
        elapsed = _parse_shard_elapsed_seconds(shard, now)
        if elapsed is None or elapsed < 60:
            continue
        zone = _vm_zone_from_map(vm_map, job_id)
        if not zone:
            continue
        try:
            serial_logs = ce_client.get_serial_port_output(PROJECT_ID, zone, job_id, start=-8192)
            updated = _apply_serial_log_events(serial_logs, shard_id, shards, updated)
        except (OSError, ValueError, RuntimeError) as e:
            logger.debug("[AUTO_SYNC] Serial log check failed for %s: %s", job_id, e)
    return updated


def _apply_vm_health_kills(
    vm_health_kills: list[tuple[str, str | None, str, str, str]],
    shards: list[dict[str, object]],
    config: dict[str, object],
    deployment_id: str,
    now: datetime,
    updated: bool,
) -> bool:
    """
    Terminate unhealthy VMs and mark their shards as failed.

    *vm_health_kills* is a list of ``(job_id, zone, shard_id, reason, msg)``.
    Returns *updated* (True if any shard was mutated).
    """
    if not vm_health_kills:
        return updated
    try:
        jobs_to_cancel = [(job_id, zone) for job_id, zone, _sid, _reason, _msg in vm_health_kills]
        _cancel_vm_jobs_sync(
            deployment_id=deployment_id,
            project_id=PROJECT_ID,
            region=cast(str, config.get("region") or "asia-northeast1"),
            service_account_email=cast(str, config.get("service_account_email") or ""),
            state_bucket=STATE_BUCKET,
            state_prefix=f"deployments.{DEPLOYMENT_ENV}",
            job_name=cast(str, config.get("job_name") or ""),
            jobs=jobs_to_cancel,
            fire_and_forget=True,
        )
        for job_id, _zone, shard_id, reason, msg in vm_health_kills:
            for shard in shards:
                if shard.get("shard_id") == shard_id:
                    shard["status"] = "failed"
                    shard["end_time"] = now.isoformat()
                    shard["error_message"] = msg
                    shard["failure_category"] = reason
                    updated = True
                    break
            logger.warning("[AUTO_SYNC] VM health check killed %s: %s - %s", job_id, reason, msg)
    except (OSError, ValueError, RuntimeError) as e:
        logger.debug("[AUTO_SYNC] VM health kill failed: %s", e)
    return updated


def _build_vm_map_for_service(service_name: str) -> dict[str, object]:
    """
    Call the Compute Engine aggregatedList API and return a *vm_map* dict.

    Returns an empty dict on any error (caller logs the warning).
    """
    from unified_trading_library.cloud_interface import get_compute_engine_client

    vm_map: dict[str, object] = {}
    try:
        ce = get_compute_engine_client(project_id=PROJECT_ID)
        instances = ce.aggregated_list_instances(PROJECT_ID, f"name:{service_name}-*")
        for inst in instances:
            vm_map[cast(str, inst["name"])] = {
                "status": inst["status"],
                "zone": inst.get("zone"),
            }
        logger.info("[AUTO_SYNC] aggregatedList found %s VMs for %s", len(vm_map), service_name)
    except (OSError, ValueError, RuntimeError) as e:
        logger.warning("[AUTO_SYNC] aggregatedList failed: %s", e)
    return vm_map


# ---------------------------------------------------------------------------
# Top-level VM orchestrator
# ---------------------------------------------------------------------------


def _process_vm_health_and_status(
    shards: list[dict[str, object]],
    vm_map: dict[str, object],
    now: datetime,
    config: dict[str, object],
    deployment_id: str,
    shard_statuses: dict[str, tuple[str, str]],
    updated: bool,
) -> bool:
    """Process VM health checks and update shard statuses."""
    # Collect running shard job_ids that need VM checks (not yet resolved via GCS).
    running_job_ids = _collect_running_job_ids(shards, shard_statuses)

    # VM health checks: OOM detection + startup timeout.
    vm_health_kills: list[tuple[str, str | None, str, str, str]] = []
    if vm_map:
        try:
            oom_threshold = getattr(settings, "OOM_KILL_THRESHOLD", 5)
            startup_timeout = getattr(settings, "VM_STARTUP_TIMEOUT_SECONDS", 300)
            vm_health_kills = _collect_unhealthy_vms(
                shards, vm_map, shard_statuses, oom_threshold, startup_timeout, now
            )
            # For healthy running VMs, parse serial log events.
            killed_shard_ids = {shard_id for _, _, shard_id, _, _ in vm_health_kills}
            updated = _parse_healthy_vm_serial_events(
                shards, vm_map, killed_shard_ids, now, updated
            )
        except (OSError, ValueError, RuntimeError) as e:
            logger.debug("[AUTO_SYNC] VM health check error: %s", e)

    # Terminate unhealthy VMs and mark shards failed.
    updated = _apply_vm_health_kills(vm_health_kills, shards, config, deployment_id, now, updated)

    if running_job_ids:
        # Resolve running shards via the vm_map (dict lookup).
        _reconcile_running_shards_with_vm_map(running_job_ids, vm_map, shard_statuses)

        # Handle orphan VM cleanup.
        _handle_orphan_vm_cleanup(vm_map, shards, shard_statuses, config, deployment_id)

    return updated

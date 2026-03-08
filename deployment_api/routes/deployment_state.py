"""
Deployment state management utilities.

Contains functions for managing deployment lifecycle, status updates,
and state synchronization with cloud resources.
"""

import json
import logging
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import UTC, datetime
from typing import cast

from backends.base import JobStatus
from backends.cloud_run import CloudRunBackend
from backends.vm import VMBackend
from deployment import StateManager
from deployment.state import DeploymentStatus, ShardStatus

from deployment_api import settings as _settings
from deployment_api.utils.deployment_events import notify_deployment_updated_sync
from deployment_api.utils.storage_facade import (
    delete_object,
    list_objects,
    object_exists,
    read_object_text,
)

logger = logging.getLogger(__name__)

# Default GCP settings
DEFAULT_PROJECT_ID = _settings.GCP_PROJECT_ID
DEFAULT_REGION = _settings.GCS_REGION
DEFAULT_STATE_BUCKET = _settings.STATE_BUCKET
DEPLOYMENT_ENV = getattr(_settings, "DEPLOYMENT_ENV", "default")


def _extract_severity_and_logger(line: str) -> tuple[str, str | None]:  # noqa: C901
    """
    Extract severity level and logger name from log line.

    Handles various log formats:
    - Python logging: "ERROR:service_name:Message"
    - Structured JSON: {"level": "error", "logger": "service_name"}
    - Cloud Logging: severity field
    """
    severity = "INFO"  # default
    logger_name = None

    # Try Python logging format first
    if ":" in line:
        parts = line.split(":", 2)
        if len(parts) >= 2:
            potential_level = parts[0].strip().upper()
            if potential_level in {"DEBUG", "INFO", "WARNING", "ERROR", "CRITICAL"}:
                severity = potential_level
                logger_name = parts[1].strip() if len(parts) > 1 else None

    # Try to extract from JSON if it looks like structured logging
    if line.strip().startswith("{"):
        try:
            log_obj = cast(dict[str, object], json.loads(line.strip()))
            if "level" in log_obj:
                severity = str(log_obj["level"]).upper()
            if "logger" in log_obj:
                logger_name = str(log_obj["logger"])
            elif "name" in log_obj:
                logger_name = str(log_obj["name"])
        except (json.JSONDecodeError, AttributeError) as e:
            logger.debug("Suppressed %s during operation: %s", type(e).__name__, e)
            pass

    # Map severity levels to standard format
    severity_map = {
        "WARN": "WARNING",
        "ERR": "ERROR",
        "CRIT": "CRITICAL",
        "FATAL": "CRITICAL",
        "TRACE": "DEBUG",
    }
    severity = severity_map.get(severity, severity)

    return severity, logger_name


def _refresh_live_cloud_run_status(state: object) -> int:  # noqa: C901
    """
    Refresh deployment state for live mode by checking Cloud Run Service revision health.

    Uses Cloud Run Services API (revisions, conditions). Updates state.shards or state.status
    based on service/revision readiness. Returns number of shards updated, or -1 if live
    refresh not applicable (e.g. no service_name in config).
    """
    service_name = state.config.get("service_name") or state.service
    region = state.config.get("region") or DEFAULT_REGION
    project_id = state.config.get("project_id") or DEFAULT_PROJECT_ID
    # Normalize service name for Cloud Run (lowercase, no underscores)
    service_name = (service_name or "").replace("_", "-").lower()
    if not service_name:
        return -1

    try:
        from google.cloud import (
            run_v2,
        )

        client = run_v2.ServicesClient()
        parent = f"projects/{project_id}/locations/{region}"
        name = f"{parent}/services/{service_name}"
        client.get_service(name=name)  # Validate service exists

        # Check latest revision: list revisions and take the latest by create time
        revisions_list = list(client.list_revisions(parent=name))

        def _revision_time(r: object) -> float:
            ct: object = getattr(r, "create_time", None)
            if ct is None:
                return 0.0
            if hasattr(ct, "timestamp"):
                return cast(float, ct.timestamp())
            return 0.0

        latest_raw = max(revisions_list, key=_revision_time) if revisions_list else None
        latest: object = cast(object, latest_raw)

        if not latest:
            return 0

        # Revision conditions: type "Ready" with state CONDITION_SUCCEEDED = healthy
        ready = False
        for cond in cast(list[object], getattr(latest, "conditions", []) or []):
            type_val: object = getattr(cond, "type_", None) or getattr(cond, "type", None)
            if type_val == "Ready":
                state_val: object = getattr(cond, "state", None)
                if state_val is not None:
                    ready = "SUCCEEDED" in str(state_val).upper()
                break

        # Map to shard status: if we have shards, update the first running one or all
        updated = 0
        for shard in state.shards:
            if shard.status != ShardStatus.RUNNING:
                continue
            if ready:
                shard.status = ShardStatus.SUCCEEDED
                shard.end_time = datetime.now(UTC).isoformat()
            else:
                shard.status = ShardStatus.FAILED
                shard.end_time = datetime.now(UTC).isoformat()
                shard.error_message = "Live service revision not ready"
            updated += 1

        if not state.shards and ready:
            state.status = DeploymentStatus.COMPLETED
            updated = 1
        elif not state.shards and not ready:
            state.status = DeploymentStatus.FAILED
            updated = 1

        return updated
    except (OSError, ValueError, RuntimeError):
        return -1


def _parse_execution_name(name: str) -> tuple[str | None, str | None]:
    """Parse execution name to extract region and job name."""
    # projects/{p}/locations/{r}/jobs/{j}/executions/{e}
    parts = name.split("/")
    region = None
    job_name = None
    for i, p in enumerate(parts):
        if p == "locations" and i + 1 < len(parts):
            region = parts[i + 1]
        if p == "jobs" and i + 1 < len(parts):
            job_name = parts[i + 1]
    return region, job_name


def _check_shard_logs_for_errors(shard, deployment_id: str) -> bool:
    """Check if shard logs contain ERROR severity. Returns True if errors found."""
    try:
        logs_path = f"deployments.{DEPLOYMENT_ENV}/{deployment_id}/{shard.shard_id}/logs.txt"
        if object_exists(DEFAULT_STATE_BUCKET, logs_path):
            content = read_object_text(DEFAULT_STATE_BUCKET, logs_path)
            # Check each line for ERROR severity using same logic as log display
            for line in content.split("\n"):
                stripped = line.strip()
                if not stripped or stripped == "No logs available":
                    continue
                severity, _ = _extract_severity_and_logger(line)
                if severity == "ERROR":
                    logger.warning(
                        "[BATCH_REFRESH] Found ERROR in %s: %s", shard.shard_id, line[:200]
                    )
                    return True
        return False
    except (OSError, ValueError, RuntimeError) as e:
        logger.warning("[BATCH_REFRESH] Failed to check logs for %s: %s", shard.shard_id, e)
        return False


def _refresh_deployment_status_sync(deployment_id: str) -> dict:  # noqa: C901
    """
    Synchronous helper for refresh_deployment_status.
    All blocking GCP/GCS calls are done here.
    """
    # Load deployment state
    state_manager = StateManager(
        bucket_name=DEFAULT_STATE_BUCKET,
        project_id=DEFAULT_PROJECT_ID,
    )

    state = state_manager.load_state(deployment_id)
    if not state:
        return {"error": "not_found", "deployment_id": deployment_id}

    # Only refresh if deployment is still "running"
    if state.status not in [DeploymentStatus.RUNNING, DeploymentStatus.PENDING]:
        return {
            "deployment_id": deployment_id,
            "message": f"Deployment already in terminal state: {state.status.value}",
            "updated": False,
        }

    updated_count = 0
    deployment_mode = getattr(state, "deployment_mode", "batch")

    # Live mode: monitor Cloud Run Services (revisions/health), not Jobs.
    if deployment_mode == "live" and state.compute_type == "cloud_run":
        updated_count = _refresh_live_cloud_run_status(state)
        if updated_count >= 0:
            state.updated_at = datetime.now(UTC).isoformat()
            state_manager.save_state(state)
            return {
                "deployment_id": deployment_id,
                "updated": updated_count > 0,
                "updated_count": updated_count,
            }
        # Fall through to batch path if live status unsupported (e.g. no service_name)

    if state.compute_type == "cloud_run":
        # Batch refresh for Cloud Run: group executions by region/job and list once per group.
        # This avoids N get_execution() calls for large deployments.
        running_shards = [s for s in state.shards if s.status == ShardStatus.RUNNING and s.job_id]

        # Group by (region, job_name)
        groups: dict[tuple[str, str], list[str]] = {}
        for shard in running_shards:
            r, j = _parse_execution_name(shard.job_id)
            r = r or state.config.get("region") or DEFAULT_REGION
            j = j or "unknown"
            key = (r, j)
            if key not in groups:
                groups[key] = []
            groups[key].append(shard.job_id)

        # Build mapping from execution_name -> our shard
        exec_to_shard = {s.job_id: s for s in running_shards}

        for (region, job_name), exec_names in groups.items():
            try:
                backend = CloudRunBackend(region=region)
                project_id = state.config.get("project_id") or DEFAULT_PROJECT_ID

                # List all executions for this job once
                all_executions = backend.list_job_executions(project_id, job_name)
                exec_statuses = {e.name: e for e in all_executions if e.name in exec_names}

                for execution_name, execution in exec_statuses.items():
                    shard = exec_to_shard.get(execution_name)
                    if not shard:
                        continue

                    # Update based on execution status
                    if execution.status == JobStatus.SUCCEEDED:
                        shard.status = ShardStatus.SUCCEEDED
                        shard.end_time = datetime.now(UTC).isoformat()
                        updated_count += 1
                    elif execution.status == JobStatus.FAILED:
                        shard.status = ShardStatus.FAILED
                        shard.end_time = datetime.now(UTC).isoformat()
                        shard.error_message = execution.error_message or "Job failed"
                        updated_count += 1
                    elif execution.status == JobStatus.CANCELLED:
                        shard.status = ShardStatus.CANCELLED
                        shard.end_time = datetime.now(UTC).isoformat()
                        updated_count += 1

            except (OSError, ValueError, RuntimeError) as e:
                logger.warning("Failed to refresh deployment shards: %s", e)

    elif state.compute_type == "vm":
        # VM refresh: check if compute instances are still running
        running_shards = [s for s in state.shards if s.status == ShardStatus.RUNNING]
        if running_shards:
            backend = VMBackend()
            zones = []
            # Extract zones from VM names or use default
            for shard in running_shards[:10]:  # Sample to avoid too many API calls
                if hasattr(shard, "compute_info") and shard.compute_info:
                    zone = shard.compute_info.get("zone")
                    if zone and zone not in zones:
                        zones.append(zone)

            if not zones:
                zones = [f"{DEFAULT_REGION}-a"]

            # Check VM status in batches
            for zone in zones:
                try:
                    zone_shards = [
                        s
                        for s in running_shards
                        if hasattr(s, "compute_info")
                        and s.compute_info
                        and s.compute_info.get("zone") == zone
                    ]

                    if not zone_shards:
                        continue

                    vm_names = [
                        s.compute_info.get("vm_name")
                        for s in zone_shards
                        if s.compute_info and s.compute_info.get("vm_name")
                    ]

                    if vm_names:
                        vm_statuses = backend.list_vm_status(zone, vm_names)

                        for shard in zone_shards:
                            vm_name = (
                                shard.compute_info.get("vm_name") if shard.compute_info else None
                            )
                            if vm_name and vm_name in vm_statuses:
                                vm_status = vm_statuses[vm_name]
                                if vm_status == "TERMINATED":
                                    shard.status = ShardStatus.SUCCEEDED
                                    shard.end_time = datetime.now(UTC).isoformat()
                                    updated_count += 1
                                elif vm_status in ["STOPPED", "STOPPING"]:
                                    shard.status = ShardStatus.FAILED
                                    shard.end_time = datetime.now(UTC).isoformat()
                                    shard.error_message = f"VM {vm_status}"
                                    updated_count += 1

                except (OSError, ValueError, RuntimeError) as e:
                    logger.warning("[BATCH_REFRESH] Failed to refresh VMs for zone %s: %s", zone, e)

    # CRITICAL FIX: Check succeeded shards for ERROR logs (VMs for now)
    # VMs may write SUCCESS to GCS even when there are ERROR severity logs.
    # We must validate logs before finalizing shard status.
    # Check if all shards are done (terminal state)
    shards_all_done = all(
        s.status in [ShardStatus.SUCCEEDED, ShardStatus.FAILED, ShardStatus.CANCELLED]
        for s in state.shards
    )
    if state.compute_type == "vm" and shards_all_done:
        logger.info("[BATCH_REFRESH] Checking logs for ERROR severity in succeeded shards...")
        succeeded_shards = [s for s in state.shards if s.status == ShardStatus.SUCCEEDED]

        # Check succeeded shards in parallel for errors
        shards_with_errors = []
        with ThreadPoolExecutor(max_workers=20) as executor:
            futures = {
                executor.submit(_check_shard_logs_for_errors, shard, deployment_id): shard
                for shard in succeeded_shards[:100]  # Limit to first 100 for performance
            }
            for future in as_completed(futures, timeout=30):
                try:
                    shard = futures[future]
                    has_errors = future.result(timeout=5)
                    if has_errors:
                        shards_with_errors.append(shard)
                except (OSError, ValueError, RuntimeError) as e:
                    logger.warning("Unexpected error during operation: %s", e, exc_info=True)
                    pass

        # Mark shards with errors as FAILED
        if shards_with_errors:
            logger.warning(
                "[BATCH_REFRESH] Marking %s shards as FAILED due to ERROR logs",
                len(shards_with_errors),
            )
            for shard in shards_with_errors:
                shard.status = ShardStatus.FAILED
                shard.error_message = "Job reported SUCCESS but logs contain ERROR severity"
                updated_count += 1

    # Update overall deployment status
    all_done = all(
        s.status in [ShardStatus.SUCCEEDED, ShardStatus.FAILED, ShardStatus.CANCELLED]
        for s in state.shards
    )
    any_failed = any(s.status == ShardStatus.FAILED for s in state.shards)
    all_failed = all(s.status == ShardStatus.FAILED for s in state.shards)

    old_status = state.status
    if all_done:
        if all_failed:
            state.status = DeploymentStatus.FAILED
        elif any_failed:
            state.status = DeploymentStatus.FAILED  # Partial failure = failed
        else:
            state.status = DeploymentStatus.COMPLETED
        state.updated_at = datetime.now(UTC).isoformat()

    # Also update total_shards if it's 0 but we have shards
    if state.total_shards == 0 and len(state.shards) > 0:
        state.total_shards = len(state.shards)
        updated_count += 1

    # Save if shards changed OR overall status changed
    status_changed = old_status != state.status
    if updated_count > 0 or status_changed:
        state_manager.save_state(state)

    try:
        notify_deployment_updated_sync(state.deployment_id)
    except (OSError, ValueError, RuntimeError) as e:
        logger.warning("Unexpected error during operation: %s", e, exc_info=True)
        pass

    return {
        "deployment_id": deployment_id,
        "status": state.status.value,
        "updated": updated_count > 0,
        "shards_updated": updated_count,
        "message": (
            f"Refreshed {updated_count} shard(s)" if updated_count > 0 else "No changes detected"
        ),
    }


def _cancel_deployment_sync(deployment_id: str) -> dict:
    """
    Synchronous cancellation logic.

    Cancels running shards, updates state, and cleans up resources.
    """
    # Load deployment state
    state_manager = StateManager(
        bucket_name=DEFAULT_STATE_BUCKET,
        project_id=DEFAULT_PROJECT_ID,
    )

    state = state_manager.load_state(deployment_id)
    if not state:
        return {"error": "not_found", "deployment_id": deployment_id}

    # Only cancel if deployment is still running
    if state.status in [
        DeploymentStatus.COMPLETED,
        DeploymentStatus.FAILED,
        DeploymentStatus.CANCELLED,
    ]:
        return {
            "deployment_id": deployment_id,
            "message": f"Deployment already in terminal state: {state.status.value}",
            "cancelled": False,
        }

    cancelled_count = 0

    # Cancel running/pending shards
    for shard in state.shards:
        if shard.status in [ShardStatus.RUNNING, ShardStatus.PENDING]:
            shard.status = ShardStatus.CANCELLED
            shard.end_time = datetime.now(UTC).isoformat()
            shard.error_message = "Cancelled by user"
            cancelled_count += 1

    # Update overall deployment status
    state.status = DeploymentStatus.CANCELLED
    state.updated_at = datetime.now(UTC).isoformat()

    # Save state
    state_manager.save_state(state)

    try:
        notify_deployment_updated_sync(state.deployment_id)
    except (OSError, ValueError, RuntimeError) as e:
        logger.warning("Unexpected error during operation: %s", e, exc_info=True)
        pass

    return {
        "deployment_id": deployment_id,
        "cancelled": True,
        "cancelled_shards": cancelled_count,
        "message": f"Cancelled deployment and {cancelled_count} shard(s)",
    }


def _resume_deployment_sync(deployment_id: str) -> dict:
    """
    Synchronous resume logic.

    Resumes cancelled deployment by resetting failed/cancelled shards to pending.
    """
    # Load deployment state
    state_manager = StateManager(
        bucket_name=DEFAULT_STATE_BUCKET,
        project_id=DEFAULT_PROJECT_ID,
    )

    state = state_manager.load_state(deployment_id)
    if not state:
        return {"error": "not_found", "deployment_id": deployment_id}

    # Only resume cancelled or failed deployments
    if state.status not in [DeploymentStatus.CANCELLED, DeploymentStatus.FAILED]:
        return {
            "deployment_id": deployment_id,
            "message": f"Cannot resume deployment with status: {state.status.value}",
            "resumed": False,
        }

    resumed_count = 0

    # Resume failed/cancelled shards
    for shard in state.shards:
        if shard.status in [ShardStatus.FAILED, ShardStatus.CANCELLED]:
            shard.status = ShardStatus.PENDING
            shard.start_time = None
            shard.end_time = None
            shard.error_message = None
            resumed_count += 1

    # Update overall deployment status
    if resumed_count > 0:
        state.status = DeploymentStatus.PENDING
        state.updated_at = datetime.now(UTC).isoformat()

        # Save state
        state_manager.save_state(state)

        try:
            notify_deployment_updated_sync(state.deployment_id)
        except (OSError, ValueError, RuntimeError) as e:
            logger.warning("Unexpected error during operation: %s", e, exc_info=True)
            pass

    return {
        "deployment_id": deployment_id,
        "resumed": resumed_count > 0,
        "resumed_shards": resumed_count,
        "message": f"Resumed {resumed_count} shard(s)"
        if resumed_count > 0
        else "No shards to resume",
    }


def _delete_deployment_sync(deployment_id: str) -> dict:
    """
    Synchronous deletion logic.

    Removes deployment state and cleans up associated resources.
    """
    # Load deployment state
    state_manager = StateManager(
        bucket_name=DEFAULT_STATE_BUCKET,
        project_id=DEFAULT_PROJECT_ID,
    )

    state = state_manager.load_state(deployment_id)
    if not state:
        return {"error": "not_found", "deployment_id": deployment_id}

    # Delete state file
    try:
        state_manager.delete_state(deployment_id)
    except (OSError, ValueError, RuntimeError) as e:
        logger.warning("Failed to delete state for %s: %s", deployment_id, e)

    # Delete associated files (logs, etc.)
    try:
        prefix = f"deployments.{DEPLOYMENT_ENV}/{deployment_id}/"
        objects = list_objects(DEFAULT_STATE_BUCKET, prefix=prefix)
        for obj in objects:
            try:
                delete_object(DEFAULT_STATE_BUCKET, obj.name)
            except (OSError, ValueError, RuntimeError) as e:
                logger.warning("Failed to delete object %s: %s", obj.name, e)
    except (OSError, PermissionError) as e:
        logger.warning("Failed to delete deployment artifacts: %s", e)

    return {
        "deployment_id": deployment_id,
        "deleted": True,
        "message": "Deployment deleted successfully",
    }


def _update_deployment_tag_sync(deployment_id: str, tag: str | None) -> dict:
    """
    Synchronous tag update logic.

    Updates the tag field in deployment state.
    """
    # Load deployment state
    state_manager = StateManager(
        bucket_name=DEFAULT_STATE_BUCKET,
        project_id=DEFAULT_PROJECT_ID,
    )

    state = state_manager.load_state(deployment_id)
    if not state:
        return {"error": "not_found", "deployment_id": deployment_id}

    # Update tag
    old_tag = getattr(state, "tag", None)
    state.tag = tag
    state.updated_at = datetime.now(UTC).isoformat()

    # Save state
    state_manager.save_state(state)

    return {
        "deployment_id": deployment_id,
        "updated": True,
        "old_tag": old_tag,
        "new_tag": tag,
        "message": f"Tag updated from '{old_tag}' to '{tag}'",
    }

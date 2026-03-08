"""
Auto-sync background task for deployment monitoring.

Handles automatic synchronization of running deployment statuses,
VM health checks, orphan cleanup, and deployment scheduling.
"""

import asyncio
import json
import logging
import os
import socket
import time as _time
import uuid
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import cast

from deployment_api import settings
from deployment_api.utils.config_validation import ConfigurationError, ValidationUtils
from deployment_api.utils.storage_client import get_storage_client as get_storage_client_with_pool

logger = logging.getLogger(__name__)

# Auto-sync configuration (from centralized settings)
PROJECT_ID = settings.GCP_PROJECT_ID
STATE_BUCKET = settings.STATE_BUCKET
DEFAULT_MAX_CONCURRENT = settings.DEFAULT_MAX_CONCURRENT
SYNC_INTERVAL = settings.AUTO_SYNC_INTERVAL_SECONDS
AUTO_SYNC_ENABLED = settings.AUTO_SYNC_ENABLED
LOCK_TTL_SECONDS = settings.AUTO_SYNC_LOCK_TTL_SECONDS
OWNER_ID = f"{socket.gethostname()}:{os.getpid()}:{uuid.uuid4().hex[:8]}"
# Environment-based state separation (development vs production)
DEPLOYMENT_ENV = settings.DEPLOYMENT_ENV
# Track held locks for cleanup on shutdown
_held_deployment_locks: set = set()
# Fire-and-forget VM delete tracking: job_id -> (timestamp, zone)
# Cleaned when VM no longer in vm_map; retried if still RUNNING after ORPHAN_DELETE_RETRY_SECONDS
_pending_vm_deletes: dict[str, tuple[float, str | None]] = {}

# Background task handles for auto-sync
_background_task = None
_events_drain_task = None
_shutdown_event = None


def get_config_dir():
    """Get the configs directory path."""
    # Try relative to this file
    api_dir = Path(__file__).parent.parent
    repo_root = api_dir.parent
    configs_dir = repo_root / "configs"

    if configs_dir.exists():
        return configs_dir

    raise RuntimeError(f"Could not find configs directory at {configs_dir}")


async def _auto_sync_running_deployments():  # noqa: C901
    """
    Background task that periodically syncs status for running deployments.

    This ensures that when VMs complete and self-delete, their completion
    status gets persisted to state.json without manual intervention.

    Runs every 60 seconds, syncing deployments that have been "running"
    for more than 5 minutes (to avoid syncing actively-launching deployments).
    """
    sync_interval_active = getattr(
        settings, "AUTO_SYNC_INTERVAL_ACTIVE", 30
    )  # 30 = default when active; fallback for CI/package version mismatch
    sync_interval_idle = 60  # seconds when no active deployments
    current_interval = sync_interval_active  # start fast in case there's something active

    logger.info(
        "[AUTO_SYNC] Started background sync task (active_interval=%ss, idle_interval=%ss)",
        sync_interval_active,
        sync_interval_idle,
    )

    # Run first sync immediately (don't wait for interval)
    first_run = True

    while not _shutdown_event.is_set():
        try:
            if first_run:
                # Small delay on first run to let API fully start, but don't wait full interval
                await asyncio.sleep(5)
                first_run = False
            else:
                await asyncio.sleep(current_interval)

            if _shutdown_event.is_set():
                break

            # Run GCS operations in thread pool to not block event loop
            def sync_running_deployments():  # noqa: C901
                logger.info("[AUTO_SYNC] Sync cycle starting...")
                client = get_storage_client_with_pool(PROJECT_ID)  # Uses default pool_size=200
                bucket = client.bucket(STATE_BUCKET)
                logger.info("[AUTO_SYNC] Using bucket: %s", STATE_BUCKET)
                now = datetime.now(UTC)
                synced = 0

                # Optional centralized quota broker (Cloud Run IAM). If not configured,
                # acquire/release calls become no-ops and scheduling behaves as before.
                try:
                    _quota_broker_client_cls = _importlib.import_module(  # noqa: F821
                        "deployment_service.deployment.quota_broker_client"
                    ).QuotaBrokerClient  # type: ignore[assignment]

                    quota_broker = _quota_broker_client_cls()
                    logger.info("[AUTO_SYNC] Quota broker initialized")
                except (OSError, ValueError, RuntimeError) as e:
                    quota_broker = None
                    logger.info("[AUTO_SYNC] Quota broker not available: %s", e)

                # Per-deployment lock helpers: each deployment gets its own lock
                # This allows multiple instances to work on different deployments concurrently
                def get_deployment_lock_blob_name(deployment_id: str) -> str:
                    return f"locks/deployment_{deployment_id}.lock"

                def try_acquire_deployment_lock(deployment_id: str) -> bool:
                    """Acquire lock for a specific deployment."""
                    lock_blob_name = get_deployment_lock_blob_name(deployment_id)
                    try:
                        lock_blob = bucket.blob(lock_blob_name)
                        payload = {
                            "owner": OWNER_ID,
                            "deployment_id": deployment_id,
                            "acquired_at": now.isoformat(),
                            "expires_at": now.timestamp() + LOCK_TTL_SECONDS,
                        }

                        # Fast path: create if missing
                        lock_blob.upload_from_string(
                            json.dumps(payload),
                            content_type="application/json",
                            if_generation_match=0,
                        )
                        _held_deployment_locks.add(deployment_id)
                        return True
                    except (OSError, ValueError, KeyError):
                        try:
                            existing = bucket.get_blob(lock_blob_name)
                            if not existing:
                                return False
                            meta = existing.metageneration
                            try:
                                existing_payload = cast(
                                    dict[str, object],
                                    json.loads(existing.download_as_text() or "{}"),
                                )
                            except (OSError, ValueError, RuntimeError):
                                existing_payload = {}

                            expires_at_raw2: object = existing_payload.get("expires_at", 0)
                            expires_at = (
                                float(cast(float, expires_at_raw2))
                                if isinstance(expires_at_raw2, (int, float))
                                else 0.0
                            )
                            owner_raw2: object = existing_payload.get("owner")
                            owner = cast(str, owner_raw2) if isinstance(owner_raw2, str) else None

                            # Allow renewal if we already own the lock or it's expired
                            is_expired = expires_at <= now.timestamp()
                            if not is_expired and owner != OWNER_ID:
                                return False

                            new_payload = {
                                "owner": OWNER_ID,
                                "deployment_id": deployment_id,
                                "acquired_at": now.isoformat(),
                                "expires_at": now.timestamp() + LOCK_TTL_SECONDS,
                                "prev_owner": owner,
                            }

                            lock_blob = bucket.blob(lock_blob_name)
                            lock_blob.upload_from_string(
                                json.dumps(new_payload),
                                content_type="application/json",
                                if_metageneration_match=meta,
                            )
                            _held_deployment_locks.add(deployment_id)
                            return True
                        except (OSError, ValueError, RuntimeError):
                            return False

                def release_deployment_lock(deployment_id: str) -> bool:
                    """Release lock for a specific deployment."""
                    lock_blob_name = get_deployment_lock_blob_name(deployment_id)
                    try:
                        lock_blob = bucket.blob(lock_blob_name)
                        if lock_blob.exists():
                            lock_data = cast(
                                dict[str, object], json.loads(lock_blob.download_as_text() or "{}")
                            )
                            if lock_data.get("owner") == OWNER_ID:
                                lock_blob.delete()
                                _held_deployment_locks.discard(deployment_id)
                                return True
                    except (OSError, ValueError, RuntimeError) as e:
                        logger.warning(
                            "Unexpected error during release deployment lock: %s", e, exc_info=True
                        )
                        pass
                    _held_deployment_locks.discard(deployment_id)
                    return False

                # List deployment directories via storage facade (FUSE when production)
                deployments_prefix = f"deployments.{DEPLOYMENT_ENV}/"
                logger.info(
                    "[AUTO_SYNC] Listing deployment directories (prefix=%s)...", deployments_prefix
                )
                try:
                    from deployment_api.utils.storage_facade import (
                        list_objects,
                        list_prefixes,
                        read_object_text,
                    )

                    prefixes = list_prefixes(STATE_BUCKET, deployments_prefix)
                    logger.info("[AUTO_SYNC] Found %s deployment directories", len(prefixes))

                    # State paths for each deployment
                    state_paths = [f"{p}state.json" for p in prefixes]
                except (OSError, ValueError, RuntimeError) as e:
                    logger.error("[AUTO_SYNC] Error listing directories: %s", e)
                    import traceback

                    logger.error(traceback.format_exc())
                    return 0, 0

                # ---- Phase 1: Parallel scan to find active deployments ----
                active_states = []
                skipped = 0
                scan_start = _time.monotonic()

                def _scan_one(path):
                    """Load state.json and return (path, data) if active, else None."""
                    try:
                        raw = cast(
                            dict[str, object], json.loads(read_object_text(STATE_BUCKET, path))
                        )
                        if raw.get("status") in (
                            "pending",
                            "running",
                            "completed_pending_delete",
                        ):
                            return (path, raw)
                    except (OSError, ValueError, RuntimeError) as e:
                        logger.warning("Unexpected error during scan one: %s", e, exc_info=True)
                        pass
                    return None

                with ThreadPoolExecutor(max_workers=20) as scan_pool:
                    futures = {scan_pool.submit(_scan_one, p): p for p in state_paths}
                    for future in as_completed(futures, timeout=30):
                        try:
                            result = future.result(timeout=10)
                            if result:
                                active_states.append(result)
                            else:
                                skipped += 1
                        except (OSError, ValueError, RuntimeError):
                            skipped += 1

                scan_elapsed = _time.monotonic() - scan_start
                logger.info(
                    "[AUTO_SYNC] Scan complete in %.1fs: %s active, %s terminal/skipped",
                    scan_elapsed,
                    len(active_states),
                    skipped,
                )

                def _run_orphan_cleanup_only(dep_state_path: str, state: dict[str, object]) -> int:  # noqa: C901
                    """Run orphan VM cleanup only (GCS + vm_map + fire). No state write. Returns count fired."""  # noqa: E501
                    config = state.get("config") or {}
                    deployment_id = dep_state_path.split("/")[1]
                    compute_type = state.get("compute_type", "vm")
                    if compute_type != "vm":
                        return 0
                    shards = state.get("shards") or []
                    service_name = cast(str, state.get("service") or "")
                    if not service_name:
                        return 0
                    status_prefix = f"deployments.{DEPLOYMENT_ENV}/{deployment_id}/"
                    status_objs = [
                        o
                        for o in list_objects(STATE_BUCKET, status_prefix)
                        if "/status" in o.name and not o.name.endswith("/state.json")
                    ]

                    def _read_status(obj):
                        parts = obj.name.split("/")
                        if len(parts) < 3:
                            return None
                        shard_id = parts[2]
                        try:
                            content = read_object_text(STATE_BUCKET, obj.name).strip()
                            status_part = content.split(":")[0]
                            if status_part == "SUCCESS":
                                return (shard_id, "succeeded")
                            if status_part in ("FAILED", "ZOMBIE"):
                                return (shard_id, "failed")
                        except (OSError, ValueError, RuntimeError) as e:
                            logger.warning(
                                "Unexpected error during read status: %s", e, exc_info=True
                            )
                            pass
                        return None

                    shard_statuses = {}
                    if status_objs:
                        with ThreadPoolExecutor(max_workers=min(len(status_objs), 20)) as pool:
                            for result in pool.map(_read_status, status_objs):
                                if result:
                                    shard_statuses[result[0]] = (result[1], "gcs")

                    vm_map = {}
                    try:
                        from google.cloud import compute_v1

                        inst_client = compute_v1.InstancesClient()
                        agg_req = compute_v1.AggregatedListInstancesRequest(
                            project=PROJECT_ID,
                            filter=f"name:{service_name}-*",
                        )

                        def _zone(scope: str) -> str:
                            if "zones/" in scope:
                                return scope.split("zones/")[-1].split("/")[0]
                            return scope.split("/")[-1] if scope else ""

                        for zone_scope, resp in inst_client.aggregated_list(request=agg_req):
                            z = _zone(str(zone_scope))
                            for inst in resp.instances or []:
                                vm_map[inst.name] = {
                                    "status": inst.status,
                                    "zone": z or None,
                                }
                    except (OSError, ValueError, RuntimeError) as e:
                        logger.debug(
                            "[AUTO_SYNC] Orphan cleanup aggregatedList failed for %s: %s",
                            deployment_id,
                            e,
                        )
                        return 0

                    def _vm_status(m: dict[str, object], jid: str) -> str | None:
                        v = m.get(jid)
                        return v.get("status") if isinstance(v, dict) else v

                    def _vm_zone(m: dict[str, object], jid: str) -> str | None:
                        v = m.get(jid)
                        return v.get("zone") if isinstance(v, dict) else None

                    orphan_tuples = []
                    for s in shards:
                        job_id = s.get("job_id")
                        shard_id = s.get("shard_id")
                        if not job_id or not shard_id:
                            continue
                        st = shard_statuses.get(shard_id)
                        if not st or st[0] not in ("succeeded", "failed"):
                            continue
                        if _vm_status(vm_map, job_id) == "RUNNING":
                            orphan_tuples.append((job_id, _vm_zone(vm_map, job_id), shard_id, st))
                    orphan_max = settings.ORPHAN_DELETE_MAX_PARALLEL
                    now_ts = _time.time()
                    to_fire = [
                        (job_id, zone)
                        for job_id, zone, _shard_id, _st in orphan_tuples[:orphan_max]
                        if job_id not in _pending_vm_deletes
                    ]
                    for job_id, zone in to_fire:
                        _pending_vm_deletes[job_id] = (now_ts, zone)

                    if not to_fire:
                        return 0
                    try:
                        _orchestrator_cls = _importlib.import_module(  # noqa: F821
                            "deployment_service.deployment.orchestrator"
                        ).DeploymentOrchestrator  # type: ignore[assignment]

                        try:
                            service_account_email = ValidationUtils.get_required(
                                config, "service_account_email", "auto_sync orchestrator"
                            )
                            job_name = ValidationUtils.get_required(
                                config, "job_name", "auto_sync VM backend"
                            )
                        except ConfigurationError as e:
                            logger.error(
                                "[AUTO_SYNC] %s: Configuration error - %s", deployment_id, e
                            )
                            return 0

                        orch = DeploymentOrchestrator(  # noqa: F821
                            project_id=PROJECT_ID,
                            region=config.get("region") or "asia-northeast1",
                            service_account_email=service_account_email,
                            state_bucket=STATE_BUCKET,
                            state_prefix=f"deployments.{DEPLOYMENT_ENV}",
                        )
                        backend = orch.get_backend(
                            "vm",
                            job_name=job_name,
                            zone=config.get("zone"),
                        )
                        if backend and hasattr(backend, "cancel_job_fire_and_forget"):
                            with ThreadPoolExecutor(
                                max_workers=min(len(to_fire), orphan_max)
                            ) as pool:
                                for job_id, zone in to_fire:
                                    pool.submit(
                                        backend.cancel_job_fire_and_forget,
                                        job_id,
                                        zone,
                                    )
                            logger.info(
                                "[AUTO_SYNC] Fired %s orphan VM deletes (recently completed: %s)",
                                len(to_fire),
                                deployment_id,
                            )
                            return len(to_fire)
                    except (OSError, ValueError, RuntimeError) as e:
                        logger.debug(
                            "[AUTO_SYNC] Orphan cleanup fire failed for %s: %s", deployment_id, e
                        )
                    return 0

                # Process deployment implementations...
                # (This would be the large _process_one_deployment function - extracted separately for brevity)  # noqa: E501
                from .deployment_processor import process_deployments_batch

                synced, num_active = process_deployments_batch(
                    active_states,
                    bucket,
                    now,
                    quota_broker,
                    try_acquire_deployment_lock,
                    release_deployment_lock,
                    _run_orphan_cleanup_only,
                )

                return synced, num_active

            # Run in thread pool
            loop = asyncio.get_event_loop()
            with ThreadPoolExecutor(max_workers=1) as executor:
                synced, num_active = await loop.run_in_executor(executor, sync_running_deployments)

            if synced > 0:
                logger.info("[AUTO_SYNC] Synced %s deployment(s)", synced)

            # State TTL cleanup: delete old deployment states (once per hour)
            # Keeps STATE_BUCKET bounded, prevents unbounded gcsfuse metadata growth
            if (_time.time() % 3600) < current_interval:  # Once per hour
                try:
                    ttl_hours = getattr(settings, "STATE_TTL_HOURS", 48)
                    cutoff = datetime.now(UTC) - timedelta(hours=ttl_hours)
                    logger.info(
                        "[AUTO_SYNC] State TTL cleanup: removing deployments older than %sh",
                        ttl_hours,
                    )

                    from deployment_api.utils.storage_facade import delete_objects, list_prefixes

                    deployments_prefix = f"deployments.{DEPLOYMENT_ENV}/"
                    prefixes = list_prefixes(STATE_BUCKET, deployments_prefix)
                    deleted_count = 0

                    for dep_prefix in prefixes:
                        # dep_prefix: deployments.development/{deployment-id}/
                        deployment_id = dep_prefix.rstrip("/").split("/")[-1]
                        state_path = f"{dep_prefix}state.json"

                        try:
                            from deployment_api.utils.storage_facade import get_object_metadata

                            metadata = get_object_metadata(STATE_BUCKET, state_path)
                            if not metadata:
                                continue

                            # Check if state.json is older than TTL
                            updated = metadata.get("updated")
                            if updated and updated < cutoff:
                                # Delete entire deployment directory
                                delete_objects(STATE_BUCKET, dep_prefix, max_results=1000)
                                deleted_count += 1
                                logger.info(
                                    "[AUTO_SYNC] TTL cleanup: deleted %s (age: %sd)",
                                    deployment_id,
                                    (datetime.now(UTC) - updated).days,
                                )
                        except (OSError, ValueError, RuntimeError) as e:
                            logger.debug(
                                "[AUTO_SYNC] TTL cleanup error for %s: %s", deployment_id, e
                            )

                    if deleted_count > 0:
                        logger.info(
                            "[AUTO_SYNC] TTL cleanup: deleted %s old deployment(s)", deleted_count
                        )
                except (OSError, ValueError, RuntimeError) as e:
                    logger.debug("[AUTO_SYNC] State TTL cleanup error: %s", e)

            # Adaptive interval: fast when active, slow when idle
            if num_active > 0:
                current_interval = sync_interval_active
                logger.debug(
                    "[AUTO_SYNC] %s active → next cycle in %ss", num_active, current_interval
                )
            else:
                current_interval = sync_interval_idle
                logger.debug(
                    "[AUTO_SYNC] No active deployments → next cycle in %ss", current_interval
                )

        except asyncio.CancelledError:
            break
        except (OSError, ValueError, RuntimeError) as e:
            logger.error("[AUTO_SYNC] Error: %s", e)

    logger.info("[AUTO_SYNC] Background sync task stopped")


def get_background_task_handles():
    """Get references to background task handles."""
    return _background_task, _events_drain_task, _shutdown_event


def set_background_task_handles(background_task, events_drain_task, shutdown_event):
    """Set references to background task handles."""
    global _background_task, _events_drain_task, _shutdown_event
    _background_task = background_task
    _events_drain_task = events_drain_task
    _shutdown_event = shutdown_event


def get_held_deployment_locks():
    """Get the set of held deployment locks."""
    return _held_deployment_locks


def get_owner_id():
    """Get the unique owner ID for this instance."""
    return OWNER_ID

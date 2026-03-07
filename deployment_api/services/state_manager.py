"""
State management service for deployment synchronization.

Handles VM state tracking, deployment locks, and cleanup operations
to manage the lifecycle of deployment synchronization.
"""

import importlib as _importlib
import json
import logging
import os
import socket
import time as _time
import uuid
from concurrent.futures import ThreadPoolExecutor as _Tpe
from datetime import UTC, datetime, timedelta
from typing import cast

from unified_trading_library import get_storage_client as get_storage_client_with_pool

from deployment_api import settings
from deployment_api.utils.config_validation import ConfigurationError, ValidationUtils

logger = logging.getLogger(__name__)


class StateManager:
    """
    Manages deployment state, locks, and VM lifecycle operations.

    This service provides:
    - Per-deployment lock management for safe concurrent operations
    - VM status tracking and orphan cleanup
    - State TTL management to prevent unbounded storage growth
    """

    def __init__(
        self,
        project_id: str | None = None,
        state_bucket: str | None = None,
        deployment_env: str | None = None,
    ):
        """Initialize state manager with configuration."""
        self.project_id = project_id or settings.GCP_PROJECT_ID
        self.state_bucket = state_bucket or settings.STATE_BUCKET
        self.deployment_env = deployment_env or settings.DEPLOYMENT_ENV
        self.lock_ttl_seconds = settings.AUTO_SYNC_LOCK_TTL_SECONDS

        # Create unique owner ID for lock management
        self.owner_id = f"{socket.gethostname()}:{os.getpid()}:{uuid.uuid4().hex[:8]}"

        # Track held locks for cleanup on shutdown
        self._held_deployment_locks: set[str] = set()

        # Fire-and-forget VM delete tracking: job_id -> (timestamp, zone)
        self._pending_vm_deletes: dict[str, tuple[float, str | None] | float] = {}

    @property
    def held_deployment_locks(self) -> set[str]:
        """Get the set of currently held deployment locks."""
        return self._held_deployment_locks.copy()

    def get_deployment_lock_blob_name(self, deployment_id: str) -> str:
        """Get the GCS blob name for a deployment lock."""
        return f"locks/deployment_{deployment_id}.lock"

    def try_acquire_deployment_lock(self, deployment_id: str) -> bool:
        """
        Acquire lock for a specific deployment.

        Uses conditional uploads to prevent race conditions.
        Supports lock renewal if already owned by this instance.

        Args:
            deployment_id: Unique identifier for the deployment

        Returns:
            True if lock was acquired, False otherwise
        """
        client = get_storage_client_with_pool(self.project_id)
        bucket = client.bucket(self.state_bucket)
        lock_blob_name = self.get_deployment_lock_blob_name(deployment_id)
        now = datetime.now(UTC)

        try:
            lock_blob = bucket.blob(lock_blob_name)
            payload = {
                "owner": self.owner_id,
                "deployment_id": deployment_id,
                "acquired_at": now.isoformat(),
                "expires_at": now.timestamp() + self.lock_ttl_seconds,
            }

            # Fast path: create if missing
            lock_blob.upload_from_string(
                json.dumps(payload),
                content_type="application/json",
                if_generation_match=0,
            )
            self._held_deployment_locks.add(deployment_id)
            return True
        except (OSError, ValueError, KeyError):
            try:
                existing = bucket.get_blob(lock_blob_name)
                if not existing:
                    return False
                meta = existing.metageneration
                try:
                    existing_payload = cast(
                        dict[str, object], json.loads(existing.download_as_text() or "{}")
                    )
                except (OSError, ValueError, RuntimeError):
                    existing_payload = {}

                expires_at_raw: object = existing_payload.get("expires_at", 0)
                expires_at = (
                    float(cast(float, expires_at_raw))
                    if isinstance(expires_at_raw, (int, float))
                    else 0.0
                )
                owner_raw: object = existing_payload.get("owner")
                owner = cast(str, owner_raw) if isinstance(owner_raw, str) else None

                # Allow renewal if we already own the lock or it's expired
                is_expired = expires_at <= now.timestamp()
                if not is_expired and owner != self.owner_id:
                    return False

                new_payload = {
                    "owner": self.owner_id,
                    "deployment_id": deployment_id,
                    "acquired_at": now.isoformat(),
                    "expires_at": now.timestamp() + self.lock_ttl_seconds,
                    "prev_owner": owner,
                }

                lock_blob = bucket.blob(lock_blob_name)
                lock_blob.upload_from_string(
                    json.dumps(new_payload),
                    content_type="application/json",
                    if_metageneration_match=meta,
                )
                self._held_deployment_locks.add(deployment_id)
                return True
            except (OSError, ValueError, RuntimeError):
                return False

    def release_deployment_lock(self, deployment_id: str) -> bool:
        """
        Release lock for a specific deployment.

        Only succeeds if the lock is owned by this instance.

        Args:
            deployment_id: Unique identifier for the deployment

        Returns:
            True if lock was released, False otherwise
        """
        client = get_storage_client_with_pool(self.project_id)
        bucket = client.bucket(self.state_bucket)
        lock_blob_name = self.get_deployment_lock_blob_name(deployment_id)

        try:
            lock_blob = bucket.blob(lock_blob_name)
            if lock_blob.exists():
                lock_data = cast(
                    dict[str, object], json.loads(lock_blob.download_as_text() or "{}")
                )
                if lock_data.get("owner") == self.owner_id:
                    lock_blob.delete()
                    self._held_deployment_locks.discard(deployment_id)
                    return True
        except (OSError, ValueError, RuntimeError) as e:
            logger.warning("Unexpected error during release deployment lock: %s", e, exc_info=True)
            pass

        self._held_deployment_locks.discard(deployment_id)
        return False

    def run_orphan_cleanup_only(self, state: dict[str, object]) -> int:
        """
        Run orphan VM cleanup for a completed deployment.

        Terminates VMs that are still running after their shards completed.

        Args:
            state: Deployment state dictionary

        Returns:
            Number of orphan VMs fired for deletion
        """
        if state.get("compute_type") != "vm":
            return 0

        config_raw: object = state.get("config") or {}
        config = cast(dict[str, object], config_raw) if isinstance(config_raw, dict) else {}
        shards_raw: object = state.get("shards") or []
        shards = cast(list[dict[str, object]], shards_raw) if isinstance(shards_raw, list) else []

        if not shards:
            return 0

        try:
            from deployment_api.utils.service_utils import parse_service_event
            from deployment_api.utils.storage_facade import read_object_text

            # Read VM status
            status_path = (
                f"deployments.{self.deployment_env}/{state.get('deployment_id')}/vm_status.json"
            )
            try:
                status_raw = read_object_text(self.state_bucket, status_path)
                vm_map = cast(dict[str, object], json.loads(status_raw))
            except (OSError, ValueError, RuntimeError):
                vm_map = {}

            # Read shard statuses
            shard_statuses = {}
            for shard in shards:
                shard_id = shard.get("shard_id")
                if not shard_id:
                    continue
                status_obj_path = f"deployments.{self.deployment_env}/{state.get('deployment_id')}/shards/{shard_id}/status.txt"
                try:
                    status_text = read_object_text(self.state_bucket, status_obj_path)
                    event_data = parse_service_event(status_text)
                    if event_data:
                        shard_statuses[shard_id] = (event_data.get("status"), event_data)
                except (OSError, ValueError, RuntimeError) as e:
                    logger.warning("Skipping item during operation: %s", e)
                    continue

            def vm_status(vm_map: dict[str, object], job_id: str) -> str | None:
                """Get VM status from vm_map."""
                for entry in vm_map.values():
                    if isinstance(entry, dict) and entry.get("job_id") == job_id:
                        return entry.get("status")
                return None

            def vm_zone(vm_map: dict[str, object], job_id: str) -> str | None:
                """Get VM zone from vm_map."""
                for entry in vm_map.values():
                    if isinstance(entry, dict) and entry.get("job_id") == job_id:
                        return entry.get("zone")
                return None

            # Find orphan VMs to terminate
            orphan_tuples: list[tuple[str, str | None, str, tuple[str, dict[str, object]]]] = []
            for shard in shards:
                shard_id_raw: object = shard.get("shard_id")
                job_id_raw: object = shard.get("job_id")
                if not job_id_raw or not shard_id_raw:
                    continue
                shard_id = cast(str, shard_id_raw)
                job_id = cast(str, job_id_raw)

                st = shard_statuses.get(shard_id)
                if not st or st[0] not in ("succeeded", "failed"):
                    continue

                if vm_status(vm_map, job_id) == "RUNNING":
                    zone = vm_zone(vm_map, job_id)
                    orphan_tuples.append((job_id, zone, shard_id, st))

            if orphan_tuples:
                try:
                    _orchestrator_cls = _importlib.import_module(
                        "deployment_service.deployment.orchestrator"
                    ).DeploymentOrchestrator  # type: ignore[assignment]

                    try:
                        service_account_email = ValidationUtils.get_required(
                            cast(dict[str, object], config), "service_account_email", "orchestrator"
                        )
                        job_name = ValidationUtils.get_required(
                            cast(dict[str, object], config), "job_name", "VM backend"
                        )
                    except ConfigurationError as e:
                        logger.error("[ORPHAN_CLEANUP] Configuration error: %s", e)
                        return 0

                    orch = DeploymentOrchestrator(  # noqa: F821
                        project_id=self.project_id,
                        region=config.get("region") or "asia-northeast1",
                        service_account_email=service_account_email,
                        state_bucket=self.state_bucket,
                        state_prefix=f"deployments.{self.deployment_env}",
                    )
                    backend = orch.get_backend(
                        "vm",
                        job_name=job_name,
                        zone=config.get("zone"),
                    )

                    if backend and hasattr(backend, "cancel_job_fire_and_forget"):
                        max_parallel = min(len(orphan_tuples), settings.ORPHAN_DELETE_MAX_PARALLEL)
                        with _Tpe(max_workers=max_parallel) as pool:
                            for job_id, zone, _shard_id, _st in orphan_tuples:
                                pool.submit(backend.cancel_job_fire_and_forget, job_id, zone)

                        logger.info(
                            "[ORPHAN_CLEANUP] Fired %s orphan VM deletes", len(orphan_tuples)
                        )
                        return len(orphan_tuples)

                except (OSError, ValueError, RuntimeError) as e:
                    logger.debug("[ORPHAN_CLEANUP] VM fire-and-forget failed: %s", e)

        except (OSError, ValueError, RuntimeError) as e:
            logger.error("[ORPHAN_CLEANUP] Error during cleanup: %s", e)

        return 0

    def track_pending_vm_delete(self, job_id: str, zone: str | None = None) -> None:
        """Track a VM deletion request for retry logic."""
        self._pending_vm_deletes[job_id] = (_time.time(), zone)

    def cleanup_pending_vm_deletes(self, vm_map: dict[str, object]) -> None:
        """Clean up pending VM deletes that are no longer in the VM map."""
        for jid in list(self._pending_vm_deletes.keys()):
            if jid not in vm_map:
                del self._pending_vm_deletes[jid]

    def get_retry_vm_deletes(self, vm_map: dict[str, object]) -> list[str]:
        """Get job IDs that should be retried for deletion."""
        now_ts = _time.time()
        retry_seconds = settings.ORPHAN_DELETE_RETRY_SECONDS

        def pending_ts(p: tuple[float, str | None] | float) -> float:
            return p[0] if isinstance(p, tuple) else p

        def vm_status(vm_map: dict[str, object], job_id: str) -> str | None:
            for entry in vm_map.values():
                if isinstance(entry, dict) and entry.get("job_id") == job_id:
                    return entry.get("status")
            return None

        return [
            jid
            for jid, val in self._pending_vm_deletes.items()
            if now_ts - pending_ts(val) >= retry_seconds and vm_status(vm_map, jid) == "RUNNING"
        ]

    def cleanup_state_ttl(self) -> int:
        """
        Clean up old deployment states based on TTL.

        Simplified implementation using direct GCS operations.

        Returns:
            Number of deployments deleted
        """
        try:
            ttl_hours = getattr(settings, "STATE_TTL_HOURS", 48)
            cutoff = datetime.now(UTC) - timedelta(hours=ttl_hours)

            logger.info("[STATE_TTL] Cleanup: removing deployments older than %sh", ttl_hours)

            client = get_storage_client_with_pool(self.project_id)
            bucket = client.bucket(self.state_bucket)

            deployments_prefix = f"deployments.{self.deployment_env}/"
            deleted_count = 0

            # List all state.json files and check their age
            for blob in bucket.list_blobs(prefix=deployments_prefix):
                if not blob.name.endswith("/state.json"):
                    continue

                try:
                    # Check if state.json is older than TTL
                    if blob.time_created and blob.time_created.replace(tzinfo=UTC) < cutoff:
                        # Extract deployment ID from path
                        parts = blob.name.split("/")
                        if len(parts) >= 3:
                            deployment_id = parts[-2]

                            # Delete entire deployment directory
                            dep_prefix = "/".join(parts[:-1]) + "/"
                            blobs_to_delete = list(bucket.list_blobs(prefix=dep_prefix))

                            for blob_to_delete in blobs_to_delete[:1000]:  # Limit batch size
                                blob_to_delete.delete()

                            deleted_count += 1
                            age_days = (
                                datetime.now(UTC) - blob.time_created.replace(tzinfo=UTC)
                            ).days
                            logger.info(
                                "[STATE_TTL] Deleted %s (age: %sd)", deployment_id, age_days
                            )

                except (OSError, ValueError, RuntimeError) as e:
                    logger.debug("[STATE_TTL] Error processing blob %s: %s", blob.name, e)

            if deleted_count > 0:
                logger.info("[STATE_TTL] Deleted %s old deployment(s)", deleted_count)

            return deleted_count

        except (OSError, ValueError, RuntimeError) as e:
            logger.debug("[STATE_TTL] Cleanup error: %s", e)
            return 0

    def release_all_locks(self) -> int:
        """
        Release all locks held by this instance.

        Used during graceful shutdown to clean up resources.

        Returns:
            Number of locks released
        """
        try:
            client = get_storage_client_with_pool(self.project_id)
            bucket = client.bucket(self.state_bucket)
            released_count = 0

            for deployment_id in list(self._held_deployment_locks):
                try:
                    lock_blob_name = self.get_deployment_lock_blob_name(deployment_id)
                    lock_blob = bucket.blob(lock_blob_name)

                    if lock_blob.exists():
                        lock_data = cast(
                            dict[str, object], json.loads(lock_blob.download_as_text() or "{}")
                        )
                        if lock_data.get("owner") == self.owner_id:
                            lock_blob.delete()
                            released_count += 1

                except (OSError, ValueError, RuntimeError):
                    pass  # Lock may have been taken by another instance

                self._held_deployment_locks.discard(deployment_id)

            return released_count

        except (OSError, ValueError, RuntimeError) as e:
            logger.warning("[SHUTDOWN] Could not release locks: %s", e)
            return 0

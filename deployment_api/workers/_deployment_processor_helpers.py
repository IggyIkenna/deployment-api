"""
Shared helpers for deployment_processor — low-level utilities.

Covers:
- Protocol definitions
- Synchronous wrappers around async clients
- Module-level constants
- VM map field accessors
- Predicate helpers (deployment status checks)
- GCS shard-status I/O helpers
- Shard-status merge / apply helpers
"""

import asyncio as _asyncio
import logging
from concurrent.futures import ThreadPoolExecutor
from datetime import UTC, datetime
from typing import Protocol, cast

from deployment_api import settings
from deployment_api.clients import deployment_service_client as _ds_client

logger = logging.getLogger(__name__)

PROJECT_ID = settings.gcp_project_id
STATE_BUCKET = settings.STATE_BUCKET
DEFAULT_MAX_CONCURRENT = settings.DEFAULT_MAX_CONCURRENT
DEPLOYMENT_ENV = settings.DEPLOYMENT_ENV


class _QuotaBrokerProtocol(Protocol):
    def enabled(self) -> bool: ...
    def release(self, *, lease_id: str) -> None: ...


def _cancel_vm_jobs_sync(
    deployment_id: str,
    project_id: str,
    region: str,
    service_account_email: str,
    state_bucket: str,
    state_prefix: str,
    job_name: str,
    jobs: list[tuple[str, str | None]],
    fire_and_forget: bool = True,
) -> dict[str, object]:
    """
    Synchronous wrapper for _ds_client.cancel_vm_jobs.

    Safe to call from ThreadPoolExecutor threads (creates a fresh event loop).
    """
    return _asyncio.run(
        _ds_client.cancel_vm_jobs(
            deployment_id=deployment_id,
            project_id=project_id,
            region=region,
            service_account_email=service_account_email,
            state_bucket=state_bucket,
            state_prefix=state_prefix,
            job_name=job_name,
            jobs=jobs,
            fire_and_forget=fire_and_forget,
        )
    )


# ---------------------------------------------------------------------------
# Private predicate / data helpers
# ---------------------------------------------------------------------------


def _vm_field(m: dict[str, object], jid: str, field: str) -> str | None:
    """Return a string field from a vm_map entry, or None."""
    v = m.get(jid)
    if isinstance(v, dict):
        return cast(str | None, cast(dict[str, object], v).get(field))
    return v if (field == "status" and isinstance(v, str)) else None


def _vm_status_from_map(m: dict[str, object], jid: str) -> str | None:
    """Return the 'status' string for a VM entry in *m*, or None."""
    return _vm_field(m, jid, "status")


def _vm_zone_from_map(m: dict[str, object], jid: str) -> str | None:
    """Return the 'zone' string for a VM entry in *m*, or None."""
    return _vm_field(m, jid, "zone")


def _is_deployment_completed_pending_delete(state: dict[str, object]) -> bool:
    """Return True when the deployment is in 'completed_pending_delete' status."""
    return state.get("status") == "completed_pending_delete"


def _should_launch_pending_shards(
    state: dict[str, object],
    shards: list[dict[str, object]],
) -> bool:
    """Return True when there are pending shards and the deployment is active."""
    if state.get("status") not in ("pending", "running"):
        return False
    return any(s.get("status") == "pending" for s in shards)


def _parse_shard_elapsed_seconds(
    shard: dict[str, object],
    now: datetime,
) -> float | None:
    """
    Return elapsed running seconds for *shard*, or None if start_time is absent
    or unparseable.
    """
    start_time = cast(str | None, shard.get("start_time"))
    if not start_time:
        return None
    try:
        started = datetime.fromisoformat(start_time.replace("Z", "+00:00"))
        if started.tzinfo is None:
            started = started.replace(tzinfo=UTC)
    except (OSError, ValueError, RuntimeError) as e:
        logger.warning("Skipping item during process stuck shards: %s", e)
        return None
    return (now - started).total_seconds()


# ---------------------------------------------------------------------------
# GCS shard-status I/O helpers
# ---------------------------------------------------------------------------


def _fetch_shard_gcs_statuses(
    state_bucket: str,
    deployment_id: str,
) -> list[object]:
    """
    List GCS status objects for *deployment_id*.

    Returns raw ObjectInfo items whose name contains '/status' (excluding
    state.json files).  Import of storage_facade is deferred so this helper
    can be called from within the ThreadPoolExecutor worker.
    """
    from deployment_api.utils.storage_facade import list_objects

    status_prefix = f"deployments.{DEPLOYMENT_ENV}/{deployment_id}/"
    return [
        o
        for o in list_objects(state_bucket, status_prefix)
        if "/status" in o.name and not o.name.endswith("/state.json")
    ]


def _read_gcs_status_obj(
    state_bucket: str,
    obj_name: str,
) -> tuple[str, str] | None:
    """
    Read a single GCS shard-status file and return ``(shard_id, status)`` or None.

    *status* is one of ``"succeeded"`` or ``"failed"``.
    """
    from deployment_api.utils.storage_facade import read_object_text

    parts = obj_name.split("/")
    if len(parts) < 3:
        return None
    shard_id = parts[2]
    try:
        content = read_object_text(state_bucket, obj_name).strip()
        status_part = content.split(":")[0]
        if status_part == "SUCCESS":
            return (shard_id, "succeeded")
        elif status_part in ("FAILED", "ZOMBIE"):
            return (shard_id, "failed")
    except (OSError, ValueError, RuntimeError) as e:
        logger.warning("Unexpected error during read status obj: %s", e, exc_info=True)
    return None


def _resolve_gcs_shard_statuses(
    deployment_id: str,
) -> dict[str, tuple[str, str]]:
    """
    Parallel-read all GCS shard status files for *deployment_id*.

    Returns a ``shard_id -> (status, "gcs")`` dict.
    """
    status_objs = _fetch_shard_gcs_statuses(STATE_BUCKET, deployment_id)
    shard_statuses: dict[str, tuple[str, str]] = {}
    if status_objs:
        with ThreadPoolExecutor(max_workers=min(len(status_objs), 20)) as pool:
            for result in pool.map(
                lambda o: _read_gcs_status_obj(STATE_BUCKET, str(getattr(o, "name", ""))),
                status_objs,
            ):
                if result:
                    shard_statuses[result[0]] = (result[1], "gcs")
    return shard_statuses


# ---------------------------------------------------------------------------
# Shard-status merge / apply helpers
# ---------------------------------------------------------------------------


def _build_merged_shard_status_map(
    gcs_statuses: dict[str, tuple[str, str]],
    vm_map: dict[str, object],
    cloud_run_statuses: dict[str, tuple[str, str]],
) -> dict[str, tuple[str, str]]:
    """
    Merge GCS, VM, and Cloud Run shard-status dictionaries.

    Later sources override earlier ones only if the shard is not yet resolved.
    GCS has highest precedence (written by the shard process on completion).
    """
    merged: dict[str, tuple[str, str]] = {}
    merged.update(gcs_statuses)
    for shard_id, entry in cloud_run_statuses.items():
        if shard_id not in merged:
            merged[shard_id] = entry
    # vm_map entries are added by _process_vm_health_and_status directly into
    # shard_statuses; this helper exists so callers have a single merge point.
    _ = vm_map  # vm_map integration is done inside _process_vm_health_and_status
    return merged


def _apply_shard_status_updates(
    shards: list[dict[str, object]],
    merged_statuses: dict[str, tuple[str, str]],
    now: datetime,
    quota_broker: "_QuotaBrokerProtocol | None",
    releases_this_tick: int,
    max_releases_per_tick: int,
) -> tuple[bool, int]:
    """
    Walk *shards* and apply any status transitions found in *merged_statuses*.

    Returns ``(updated, releases_this_tick)`` where *updated* is True when at
    least one shard was mutated.
    """
    updated = False
    for shard in shards:
        shard_id = cast(str, shard.get("shard_id"))
        if not shard_id or shard_id not in merged_statuses:
            continue
        if shard.get("status") not in ("running", "pending"):
            continue

        new_status, source = merged_statuses[shard_id]
        old_status = shard.get("status")
        if new_status == old_status:
            continue

        shard["status"] = new_status
        if new_status in ("succeeded", "failed"):
            shard["end_time"] = now.isoformat()

        # Release quota lease when shard reaches terminal state (best-effort).
        try:
            if (
                quota_broker
                and quota_broker.enabled()
                and shard.get("quota_lease_id")
                and releases_this_tick < max_releases_per_tick
                and new_status in ("succeeded", "failed", "cancelled")
            ):
                quota_broker.release(lease_id=str(shard.get("quota_lease_id")))
                shard.pop("quota_lease_id", None)
                releases_this_tick += 1
                updated = True
        except (OSError, ValueError, RuntimeError):
            pass

        updated = True
        logger.info(
            "[AUTO_SYNC] %s: %s -> %s (source: %s)",
            shard_id,
            old_status,
            new_status,
            source,
        )

    return updated, releases_this_tick

"""
Cloud Run execution status helpers for deployment_processor.

Covers:
- Parsing Cloud Run execution names
- Grouping executions by region/job
- Batch-fetching Cloud Run status
- Applying Cloud Run statuses to shard_statuses
- _process_cloud_run_status orchestrator
"""

import asyncio as _asyncio
import logging
from typing import cast

from deployment_api import settings
from deployment_api.clients import deployment_service_client as _ds_client

from ._deployment_processor_helpers import PROJECT_ID

logger = logging.getLogger(__name__)


def _get_cloud_run_status_batch_sync(
    project_id: str,
    region: str,
    service_account_email: str,
    job_name: str,
    job_ids: list[str],
) -> dict[str, str]:
    """Synchronous wrapper for get_cloud_run_status_batch. Safe for ThreadPoolExecutor threads."""
    return _asyncio.run(
        _ds_client.get_cloud_run_status_batch(
            project_id=project_id,
            region=region,
            service_account_email=service_account_email,
            job_name=job_name,
            job_ids=job_ids,
        )
    )


def _parse_exec_name(name: str) -> tuple[str | None, str | None]:
    """Parse Cloud Run execution name into (region, job_name)."""
    parts = name.split("/")
    region: str | None = None
    job_name: str | None = None
    for i, p in enumerate(parts):
        if p == "locations" and i + 1 < len(parts):
            region = parts[i + 1]
        if p == "jobs" and i + 1 < len(parts):
            job_name = parts[i + 1]
    return region, job_name


def _group_cloud_run_jobs_by_region(
    job_ids: list[str],
    config: dict[str, object],
) -> dict[tuple[str, str], list[str]]:
    """Group Cloud Run execution IDs by (region, job_name) for batch API calls."""
    groups: dict[tuple[str, str], list[str]] = {}
    for job_id in job_ids:
        r, j = _parse_exec_name(job_id)
        r = r or cast(str, config.get("region") or "asia-northeast1")
        j = j or cast(str, config.get("job_name"))
        if not j:
            continue
        groups.setdefault((r, j), []).append(job_id)
    return groups


def _collect_cloud_run_running_job_ids(
    shards: list[dict[str, object]],
    shard_statuses: dict[str, tuple[str, str]],
) -> tuple[list[str], dict[str, str]]:
    """
    Collect job_ids for running Cloud Run shards not yet resolved.

    Returns ``(job_ids, job_id_to_shard_id)``.
    """
    job_ids: list[str] = []
    job_id_to_shard_id: dict[str, str] = {}
    for shard in shards:
        if shard.get("status") != "running":
            continue
        shard_id = cast(str, shard.get("shard_id"))
        if not shard_id or shard_id in shard_statuses:
            continue
        job_id = cast(str, shard.get("job_id"))
        if not job_id:
            continue
        job_ids.append(job_id)
        job_id_to_shard_id[job_id] = shard_id
    return job_ids, job_id_to_shard_id


def _apply_cloud_run_batch_statuses(
    job_id_to_shard_id: dict[str, str],
    api_statuses: dict[str, str],
    shard_statuses: dict[str, tuple[str, str]],
) -> None:
    """
    Translate Cloud Run API status strings into shard_statuses entries.

    Never regresses a launched shard back to ``"pending"``.
    Mutates *shard_statuses* in place.
    """
    _cr_map = {
        "SUCCEEDED": "succeeded",
        "FAILED": "failed",
        "RUNNING": "running",
    }
    for job_id, status in api_statuses.items():
        shard_id = job_id_to_shard_id.get(job_id)
        if not shard_id:
            continue
        mapped = _cr_map.get(status)
        if mapped:
            shard_statuses[shard_id] = (mapped, "cloud_run")


def _process_cloud_run_status(
    shards: list[dict[str, object]],
    config: dict[str, object],
    deployment_id: str,
    shard_statuses: dict[str, tuple[str, str]],
    updated: bool,
) -> bool:
    """Process Cloud Run execution status updates via deployment-service HTTP API."""
    try:
        job_ids, job_id_to_shard_id = _collect_cloud_run_running_job_ids(shards, shard_statuses)

        if job_ids:
            service_account_email = settings.SERVICE_ACCOUNT
            # Group executions by region/job to handle multi-region failover.
            groups = _group_cloud_run_jobs_by_region(job_ids, config)

            for (region, job_name), group_job_ids in groups.items():
                try:
                    statuses = _get_cloud_run_status_batch_sync(
                        project_id=PROJECT_ID,
                        region=region,
                        service_account_email=service_account_email,
                        job_name=job_name,
                        job_ids=group_job_ids,
                    )
                    _apply_cloud_run_batch_statuses(job_id_to_shard_id, statuses, shard_statuses)
                except (OSError, ValueError, RuntimeError) as e:
                    logger.warning(
                        "[AUTO_SYNC] Cloud Run status batch failed for %s/%s: %s",
                        region,
                        job_name,
                        e,
                    )
    except (OSError, ValueError, RuntimeError) as e:
        logger.warning("Failed to process cloud run shard statuses: %s", e)

    return updated

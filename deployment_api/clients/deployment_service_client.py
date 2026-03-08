"""
HTTP client for the deployment-service REST API.

deployment-api must NOT import deployment-service Python modules directly.
All deployment-service functionality is accessed via HTTP calls through this client.

Base URL is configured via DEPLOYMENT_SERVICE_URL environment variable
(default: http://localhost:9000 for local development).

NOTE: deployment-service currently exposes a CLI interface only. HTTP endpoints
listed here are the canonical API surface that deployment-service will expose once
its HTTP layer is implemented (tracked: citadel_audit_remediation stream1).
Until then, calls to endpoints that don't exist yet will raise a clear RuntimeError
so that failures are loud and traceable rather than silently wrong.
"""

from __future__ import annotations

import logging

import aiohttp

from deployment_api import settings as _settings

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Internal helpers
# ---------------------------------------------------------------------------


def _base_url() -> str:
    """Return the base URL for the deployment-service API."""
    return _settings.DEPLOYMENT_SERVICE_URL


def _timeout() -> aiohttp.ClientTimeout:
    """Return the default timeout for deployment-service calls."""
    return aiohttp.ClientTimeout(total=_settings.DEPLOYMENT_SERVICE_TIMEOUT_SECONDS)


# ---------------------------------------------------------------------------
# Shard calculation
# ---------------------------------------------------------------------------


async def calculate_shards(
    service: str,
    config_dir: str,
    start_date: str | None = None,
    end_date: str | None = None,
    max_shards: int = 10000,
    cloud_config_path: str | None = None,
    respect_start_dates: bool = True,
    skip_existing: bool = False,
    skip_dimensions: list[str] | None = None,
    date_granularity_override: str | None = None,
    extra_filters: dict[str, object] | None = None,
) -> list[dict[str, object]]:
    """
    Calculate shards for a deployment via the deployment-service HTTP API.

    POST /api/v1/shards/calculate

    Args:
        service: Service name (e.g., "instruments-service")
        config_dir: Configuration directory path (relative to deployment-service root)
        start_date: ISO-format start date string, or None
        end_date: ISO-format end date string, or None
        max_shards: Maximum allowed shards
        cloud_config_path: Cloud storage path for dynamic config discovery
        respect_start_dates: Filter out shards before venue start dates
        skip_existing: Filter out shards where data already exists
        skip_dimensions: Dimension names to skip from sharding
        date_granularity_override: Override date granularity from config
        extra_filters: Additional filter kwargs (category, venue, etc.)

    Returns:
        List of shard dicts with keys: shard_index, total_shards, dimensions, cli_command

    Raises:
        RuntimeError: If the deployment-service HTTP call fails.
    """
    payload: dict[str, object] = {
        "service": service,
        "config_dir": config_dir,
        "max_shards": max_shards,
        "respect_start_dates": respect_start_dates,
        "skip_existing": skip_existing,
        "skip_dimensions": skip_dimensions or [],
        "extra_filters": extra_filters or {},
    }
    if start_date is not None:
        payload["start_date"] = start_date
    if end_date is not None:
        payload["end_date"] = end_date
    if cloud_config_path is not None:
        payload["cloud_config_path"] = cloud_config_path
    if date_granularity_override is not None:
        payload["date_granularity_override"] = date_granularity_override

    url = f"{_base_url()}/api/v1/shards/calculate"
    logger.debug("POST %s (service=%s)", url, service)

    async with (
        aiohttp.ClientSession(timeout=_timeout()) as session,
        session.post(url, json=payload) as resp,
    ):
        if resp.status != 200:
            body = await resp.text()
            raise RuntimeError(
                f"deployment-service /api/v1/shards/calculate returned HTTP {resp.status}: {body}"
            )
        data: dict[str, object] = await resp.json()
        shards: list[dict[str, object]] = data.get("shards", [])
        return shards


# ---------------------------------------------------------------------------
# Deployment execution
# ---------------------------------------------------------------------------


async def create_deployment(
    deployment_id: str,
    service: str,
    region: str,
    compute_type: str,
    deployment_mode: str,
    docker_image: str,
    job_name: str,
    compute_config: dict[str, object],
    env_vars: dict[str, str],
    max_concurrent: int,
    shards: list[dict[str, object]],
    tag: str | None = None,
    vm_zone: str | None = None,
) -> dict[str, object]:
    """
    Submit a deployment job to the deployment-service HTTP API.

    POST /api/v1/deployments

    Args:
        deployment_id: Unique deployment identifier (generated by caller)
        service: Service name
        region: Deployment region
        compute_type: Compute type (cloud_run or vm)
        deployment_mode: Deployment mode
        docker_image: Docker image URL
        job_name: Cloud Run job name or VM job name
        compute_config: Compute configuration dict
        env_vars: Environment variables for the deployed containers
        max_concurrent: Maximum concurrent shard executions
        shards: List of shard dicts from calculate_shards()
        tag: Optional deployment tag
        vm_zone: Optional VM zone override

    Returns:
        Dict containing deployment status with keys: deployment_id, status, message

    Raises:
        RuntimeError: If the deployment-service HTTP call fails.
    """
    payload: dict[str, object] = {
        "deployment_id": deployment_id,
        "service": service,
        "region": region,
        "compute_type": compute_type,
        "deployment_mode": deployment_mode,
        "docker_image": docker_image,
        "job_name": job_name,
        "compute_config": compute_config,
        "env_vars": env_vars,
        "max_concurrent": max_concurrent,
        "shards": shards,
    }
    if tag is not None:
        payload["tag"] = tag
    if vm_zone is not None:
        payload["vm_zone"] = vm_zone

    url = f"{_base_url()}/api/v1/deployments"
    logger.debug("POST %s (deployment_id=%s, service=%s)", url, deployment_id, service)

    async with (
        aiohttp.ClientSession(timeout=_timeout()) as session,
        session.post(url, json=payload) as resp,
    ):
        if resp.status not in (200, 201, 202):
            body = await resp.text()
            raise RuntimeError(
                f"deployment-service /api/v1/deployments returned HTTP {resp.status}: {body}"
            )
        result: dict[str, object] = await resp.json()
        return result


# ---------------------------------------------------------------------------
# Data status
# ---------------------------------------------------------------------------


async def get_data_status(
    service: str,
    start_date: str,
    end_date: str,
    categories: list[str] | None = None,
    venues: list[str] | None = None,
    show_missing: bool = False,
    check_venues: bool = False,
    check_data_types: bool = False,
    check_feature_groups: bool = False,
    check_timeframes: bool = False,
    mode: str = "batch",
) -> dict[str, object]:
    """
    Query data status from the deployment-service HTTP API.

    GET /api/v1/data-status

    This replaces the previous subprocess call:
        python -m deployment_service.cli data-status --service ...

    Args:
        service: Service name to check
        start_date: ISO-format start date
        end_date: ISO-format end date
        categories: Optional list of categories to filter
        venues: Optional list of venues to filter
        show_missing: Include missing data in results
        check_venues: Check venue coverage
        check_data_types: Check data types
        check_feature_groups: Check feature groups
        check_timeframes: Check timeframes
        mode: Query mode (batch or live)

    Returns:
        Dict containing data status information

    Raises:
        RuntimeError: If the deployment-service HTTP call fails.
    """
    params: dict[str, object] = {
        "service": service,
        "start_date": start_date,
        "end_date": end_date,
        "mode": mode,
        "show_missing": show_missing,
        "check_venues": check_venues,
        "check_data_types": check_data_types,
        "check_feature_groups": check_feature_groups,
        "check_timeframes": check_timeframes,
    }
    if categories:
        params["categories"] = categories
    if venues:
        params["venues"] = venues

    url = f"{_base_url()}/api/v1/data-status"
    logger.debug("GET %s (service=%s, %s..%s)", url, service, start_date, end_date)

    async with aiohttp.ClientSession(timeout=_timeout()) as session:  # noqa: SIM117
        async with session.get(url, params=params) as resp:  # type: ignore[arg-type]
            if resp.status != 200:
                body = await resp.text()
                raise RuntimeError(
                    f"deployment-service /api/v1/data-status returned HTTP {resp.status}: {body}"
                )
            result: dict[str, object] = await resp.json()
            return result


# ---------------------------------------------------------------------------
# VM job cancellation
# ---------------------------------------------------------------------------


async def cancel_vm_jobs(
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
    Cancel VM jobs via the deployment-service HTTP API.

    POST /api/v1/vm-jobs/cancel

    This replaces:
        orch = DeploymentOrchestrator(project_id, region, service_account_email, state_bucket, state_prefix)
        backend = orch.get_backend("vm", job_name=job_name, zone=zone)
        backend.cancel_job_fire_and_forget(job_id, zone)

    Args:
        deployment_id: Deployment ID for logging context
        project_id: GCP project ID
        region: GCP region
        service_account_email: Service account email
        state_bucket: GCS state bucket
        state_prefix: Prefix for state files in the bucket
        job_name: Cloud Run / VM job name
        jobs: List of (job_id, zone) tuples to cancel
        fire_and_forget: If True, deployment-service cancels asynchronously

    Returns:
        Dict with keys: cancelled_count, failed_count

    Raises:
        RuntimeError: If the deployment-service HTTP call fails.
    """  # noqa: E501
    payload: dict[str, object] = {
        "deployment_id": deployment_id,
        "project_id": project_id,
        "region": region,
        "service_account_email": service_account_email,
        "state_bucket": state_bucket,
        "state_prefix": state_prefix,
        "job_name": job_name,
        "jobs": [{"job_id": jid, "zone": zone} for jid, zone in jobs],
        "fire_and_forget": fire_and_forget,
    }

    url = f"{_base_url()}/api/v1/vm-jobs/cancel"
    logger.debug("POST %s (deployment_id=%s, %d jobs)", url, deployment_id, len(jobs))

    async with (
        aiohttp.ClientSession(timeout=_timeout()) as session,
        session.post(url, json=payload) as resp,
    ):
        if resp.status not in (200, 202):
            body = await resp.text()
            raise RuntimeError(
                f"deployment-service /api/v1/vm-jobs/cancel returned HTTP {resp.status}: {body}"
            )
        result: dict[str, object] = await resp.json()
        return result


# ---------------------------------------------------------------------------
# Quota broker
# ---------------------------------------------------------------------------


async def quota_acquire_batch(
    quota_shape: dict[str, object],
    batch_size: int,
) -> int:
    """
    Acquire quota from the quota broker via the deployment-service HTTP API.

    POST /api/v1/quota/acquire

    This replaces:
        QuotaBrokerClient().try_acquire_batch(quota_shape, batch_size)

    Returns:
        Number of slots acquired (0 if none available)

    Raises:
        RuntimeError: If the deployment-service HTTP call fails.
    """
    payload: dict[str, object] = {
        "quota_shape": quota_shape,
        "batch_size": batch_size,
    }
    url = f"{_base_url()}/api/v1/quota/acquire"
    logger.debug("POST %s (batch_size=%d)", url, batch_size)

    async with (
        aiohttp.ClientSession(timeout=_timeout()) as session,
        session.post(url, json=payload) as resp,
    ):
        if resp.status != 200:
            body = await resp.text()
            raise RuntimeError(
                f"deployment-service /api/v1/quota/acquire returned HTTP {resp.status}: {body}"
            )
        data: dict[str, object] = await resp.json()
        acquired: int = int(data.get("acquired", 0))
        return acquired


async def quota_release_batch(
    quota_shape: dict[str, object],
    batch_size: int,
) -> None:
    """
    Release quota back to the quota broker via the deployment-service HTTP API.

    POST /api/v1/quota/release

    This replaces:
        QuotaBrokerClient().release_batch(quota_shape, batch_size)

    Raises:
        RuntimeError: If the deployment-service HTTP call fails.
    """
    payload: dict[str, object] = {
        "quota_shape": quota_shape,
        "batch_size": batch_size,
    }
    url = f"{_base_url()}/api/v1/quota/release"
    logger.debug("POST %s (batch_size=%d)", url, batch_size)

    async with (
        aiohttp.ClientSession(timeout=_timeout()) as session,
        session.post(url, json=payload) as resp,
    ):
        if resp.status not in (200, 204):
            body = await resp.text()
            raise RuntimeError(
                f"deployment-service /api/v1/quota/release returned HTTP {resp.status}: {body}"
            )

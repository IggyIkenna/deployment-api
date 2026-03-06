"""
Service Status API Routes

Provides temporal audit trail for services:
- Last data update (GCS file timestamps)
- Last deployment (from state files)
- Last build (Cloud Build API)
- Last code push (GitHub API)
- Anomaly detection (stale data, failed builds)
"""

import asyncio
import logging
import os
import sys
import time
from pathlib import Path
from typing import cast

import yaml
from fastapi import APIRouter, FastAPI, Request
from google.auth import default, impersonated_credentials
from unified_cloud_interface import get_secret_client

from deployment_api.settings import GITHUB_TOKEN_SA
from deployment_api.utils.storage_facade import get_gcs_fuse_status

# Import extracted modules
from .service_status_cache import load_gcs_cache
from .service_status_checkers import (
    get_latest_build,
    get_latest_code_push,
    get_latest_data_timestamp,
    get_latest_deployment,
)
from .service_status_execution import (
    calculate_execution_missing_shards,
    get_execution_service_data_status,
)
from .service_status_fast_data import get_latest_data_timestamp_fast
from .service_status_health import detect_anomalies, determine_overview_health

# Add parent directory to path
sys.path.insert(0, str(__file__).rsplit("/", 3)[0])

router = APIRouter()
logger = logging.getLogger(__name__)


@router.get("/{service}/status")
async def get_service_status(service: str, request: Request):
    """
    Get comprehensive status for a service.

    Returns temporal audit trail with timestamps for:
    - Last data update (GCS)
    - Last deployment (state files)
    - Last build (Cloud Build API)
    - Last code push (GitHub API)

    Plus anomaly detection.
    """
    start_time = time.time()
    timing = {}

    # Fetch all timestamps with individual timing
    parallel_start = time.time()

    data_start = time.time()
    data_info = await get_latest_data_timestamp(service)
    timing["data_fetch_ms"] = int((time.time() - data_start) * 1000)

    deploy_start = time.time()
    deployment_info = await get_latest_deployment(service)
    timing["deploy_fetch_ms"] = int((time.time() - deploy_start) * 1000)

    build_start = time.time()
    build_info = await get_latest_build(service)
    timing["build_fetch_ms"] = int((time.time() - build_start) * 1000)

    timing["parallel_fetch_ms"] = int((time.time() - parallel_start) * 1000)

    # GitHub requires token - access via Secret Manager API
    # Uses Cloud Build SA impersonation (semantically correct - Cloud Build uses GitHub)
    # Works locally (via impersonation) and on VMs (direct SA credentials)
    github_token = None
    try:

        def _get_token_sync():
            token_start = time.time()
            try:
                # Use dedicated GitHub Token SA (created by Ikenna)
                target_sa = GITHUB_TOKEN_SA

                # Get source credentials
                source_credentials, _project = default()
                logger.info("[PERF] Got default credentials in %.2fs", time.time() - token_start)

                # Check if we're already running as a SA with secret access
                if hasattr(source_credentials, "service_account_email"):
                    sa_email = source_credentials.service_account_email
                    # If running as github-token-sa, Compute Engine SA, or instruments-service SA - use directly
                    if any(
                        x in sa_email
                        for x in [
                            "github-token-sa@",
                            "compute@developer.gserviceaccount.com",
                            "instruments-service",
                        ]
                    ):
                        logger.info("[PERF] Running as %s, accessing secret directly", sa_email)
                        secret_client = get_secret_client()
                        secret_value = secret_client.get_secret("github-token")
                        return secret_value

                # Running locally - impersonate GitHub Token SA
                target_scopes = ["https://www.googleapis.com/auth/cloud-platform"]
                _ = impersonated_credentials.Credentials(
                    source_credentials=source_credentials,
                    target_principal=target_sa,
                    target_scopes=target_scopes,
                )

                logger.info("[PERF] About to access secret (elapsed: %.2fs)", time.time() - token_start)
                secret_client = get_secret_client()
                secret_value = secret_client.get_secret("github-token")
                logger.info("[PERF] Secret accessed in %.2fs total", time.time() - token_start)
                return secret_value

            except (OSError, ValueError, RuntimeError) as e:
                logger.warning("Could not access github-token (took %.2fs): %s", time.time() - token_start, e)
                return None

        token_overall_start = time.time()
        github_token = await asyncio.to_thread(_get_token_sync)
        logger.info("[PERF] GitHub token fetch took %.2fs total", time.time() - token_overall_start)
    except (OSError, ValueError, RuntimeError) as e:
        logger.warning("Error accessing github-token: %s", e)

    github_start = time.time()
    code_info = await get_latest_code_push(service, github_token)
    logger.info("[TIMING] GitHub fetch took %.2fs", time.time() - github_start)

    # Extract timestamps
    data_ts = None
    if isinstance(data_info, dict) and "latest" in data_info:
        data_ts = data_info["latest"]

    deploy_ts = None
    if isinstance(deployment_info, dict) and "timestamp" in deployment_info:
        deploy_ts = deployment_info["timestamp"]

    build_ts = None
    build_status = None
    if isinstance(build_info, dict) and "timestamp" in build_info:
        build_ts = build_info["timestamp"]
        build_status = build_info.get("status")

    code_ts = None
    if isinstance(code_info, dict) and "timestamp" in code_info:
        code_ts = code_info["timestamp"]

    # Detect anomalies
    anomalies = detect_anomalies(data_ts, deploy_ts, build_ts, code_ts)

    # Determine overall health using extracted function
    deploy_status = None
    if isinstance(deployment_info, dict):
        deploy_status = deployment_info.get("status")

    from .service_status_health import determine_service_health

    health = determine_service_health(
        data_ts=data_ts,
        deploy_ts=deploy_ts,
        deploy_status=deploy_status,
        build_status=build_status,
        anomalies=anomalies,
    )

    # Fetch checklist status (quick lookup from YAML file)
    checklist_status = None

    try:
        config_dir = cast(Path, cast(FastAPI, request.app).state.config_dir)
        checklist_file = f"{config_dir}/checklist.{service}.yaml"

        logger.info("Attempting to load checklist from: %s", checklist_file)

        if os.path.exists(checklist_file):
            with open(checklist_file) as f:
                checklist_data = cast(dict[str, object], yaml.safe_load(f))

            total_items = 0
            completed_items = 0

            for category in cast(list[dict[str, object]], checklist_data.get("categories") or []):
                for item in cast(list[dict[str, object]], category.get("items") or []):
                    total_items += 1
                    if item.get("status") == "done":
                        completed_items += 1

            if total_items > 0:
                checklist_status = {
                    "percent": round((completed_items / total_items) * 100, 1),
                    "completed": completed_items,
                    "total": total_items,
                }
                logger.info("Checklist loaded: %s", checklist_status)
        else:
            logger.warning("Checklist file not found: %s", checklist_file)
    except (OSError, ValueError, RuntimeError) as e:
        logger.warning("Could not load checklist for %s: %s", service, e, exc_info=True)

    # Data coverage - intentionally skipped (too slow for status page)
    # Use dedicated Data Status tab for detailed coverage info
    data_coverage = None

    timing["total_ms"] = int((time.time() - start_time) * 1000)

    return {
        "service": service,
        "health": health,
        "_timing": timing,  # Debug timing info
        "last_data_update": data_ts,
        "last_deployment": deploy_ts,
        "last_build": build_ts,
        "last_code_push": code_ts,
        "anomalies": anomalies,
        "api": {
            "gcs_fuse": get_gcs_fuse_status(),
        },
        "details": {
            "data": data_info if isinstance(data_info, dict) else None,
            "deployment": (deployment_info if isinstance(deployment_info, dict) else None),
            "build": build_info if isinstance(build_info, dict) else None,
            "code": code_info if isinstance(code_info, dict) else None,
        },
        "checklist_status": checklist_status,
        "data_coverage": data_coverage,
    }


@router.get("/overview")
async def get_services_overview(request: Request):
    """
    Get FAST status overview for all services.

    Returns a lightweight summary - skips slow GitHub/Build lookups.
    For full details, query individual service status.
    """
    from deployment_service.config_loader import ConfigLoader

    config_dir = cast(Path, cast(FastAPI, request.app).state.config_dir)
    loader = ConfigLoader(str(config_dir))

    # Get all services (from sharding configs) and include quota-manager for overview
    services = loader.list_available_services()
    if "quota-manager" not in services:
        services = sorted([*services, "quota-manager"])

    # Fetch data timestamps + cached deployments (fast with caching)
    async def get_quick_status(service: str) -> dict[str, object]:
        """Get status info - data timestamps + cached deployment info."""
        # Quota-manager: health from quota broker /health (no sharding/deployment in this dashboard)
        if service == "quota-manager":
            from deployment_api import settings as api_settings

            broker_url = api_settings.QUOTA_BROKER_URL
            if not broker_url:
                return {
                    "service": service,
                    "health": "unknown",
                    "last_data_update": None,
                    "last_deployment": None,
                    "deployment_status": None,
                    "last_build": None,
                    "build_status": None,
                    "anomaly_count": 0,
                }

            def _check_broker_health() -> bool:
                import urllib.request

                try:
                    from google.auth.transport.requests import Request as AuthRequest
                    from google.oauth2 import id_token

                    token = id_token.fetch_id_token(AuthRequest(), broker_url)
                    req = urllib.request.Request(
                        f"{broker_url}/health",
                        method="GET",
                        headers={"Authorization": f"Bearer {token}"},
                    )
                    import http.client
                    with cast(http.client.HTTPResponse, urllib.request.urlopen(req, timeout=5)) as resp:
                        return resp.status == 200
                except (OSError, ValueError, RuntimeError) as e:
                    logger.debug("Quota broker health check failed: %s", e)
                    return False

            ok = await asyncio.to_thread(_check_broker_health)
            return {
                "service": service,
                "health": "healthy" if ok else "error",
                "last_data_update": None,
                "last_deployment": None,
                "deployment_status": None,
                "last_build": None,
                "build_status": None,
                "anomaly_count": 0,
            }
        try:
            # Parallel fetch: data timestamps + deployment (cached, fast after first call)
            data_info, deployment_info = await asyncio.gather(
                get_latest_data_timestamp_fast(service),
                get_latest_deployment(service, use_cache=True),  # Uses 5-min cache
                return_exceptions=True,
            )

            data_ts: str | None = None
            if isinstance(data_info, dict) and "latest" in data_info:
                data_ts = cast(str | None, data_info["latest"])

            deploy_ts: str | None = None
            deploy_status: str | None = None
            if isinstance(deployment_info, dict) and "timestamp" in deployment_info:
                deploy_ts = cast(str | None, deployment_info["timestamp"])
                deploy_status = cast(str | None, deployment_info.get("status"))
                # Normalize status to lowercase string for comparison
                if deploy_status:
                    deploy_status = str(deploy_status).lower()

            # Check GCS cache for build info (populated by individual service views)
            cache = load_gcs_cache()
            builds_cache = cache.get("builds") or {}
            build_ts = None
            build_status = None
            if service in builds_cache:
                build_info = builds_cache[service]
                if isinstance(build_info, dict):
                    build_ts = build_info.get("timestamp")
                    build_status = build_info.get("status")

            # Determine health using extracted function
            health = determine_overview_health(
                data_ts=data_ts,
                deploy_ts=deploy_ts,
                deploy_status=deploy_status,
                build_status=build_status,
            )

            return {
                "service": service,
                "health": health,
                "last_data_update": data_ts,
                "last_deployment": deploy_ts,
                "deployment_status": deploy_status,
                "last_build": build_ts,
                "build_status": build_status,
                "anomaly_count": 0,
            }
        except (OSError, ValueError, RuntimeError) as e:
            return {
                "service": service,
                "health": "error",
                "error": str(e)[:100],
            }

    # Fetch all in parallel (fast - only GCS lookups)
    results = await asyncio.gather(*[get_quick_status(svc) for svc in services])

    return {
        "services": list(results),
        "count": len(results),
        "healthy": sum(1 for s in results if s.get("health") == "healthy"),
        "warnings": sum(1 for s in results if s.get("health") in ["warning", "stale"]),
        "errors": sum(1 for s in results if s.get("health") == "error"),
        "note": "For full status including builds/deployments, query individual services",
    }


@router.get("/execution-service/data-status")
async def get_execution_service_data_status_endpoint(
    request: Request,
    config_path: str,
    start_date: str | None = None,
    end_date: str | None = None,
):
    """Get execution-service data status by checking configs vs results."""

    return await get_execution_service_data_status(config_path, start_date, end_date)


@router.post("/execution-service/missing-shards")
async def calculate_execution_missing_shards_endpoint(
    request: Request,
    config_path: str,
    start_date: str,
    end_date: str,
    strategy: str | None = None,
    mode: str | None = None,
    timeframe: str | None = None,
    algo: str | None = None,
):
    """Calculate missing config x date shards for execution-service."""
    return await calculate_execution_missing_shards(config_path, start_date, end_date, strategy, mode, timeframe, algo)

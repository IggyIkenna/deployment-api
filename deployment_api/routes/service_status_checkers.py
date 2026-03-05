"""
Service Status Checking Functions

Functions for getting timestamps and status from various sources:
- GCS data timestamps
- Deployment info
- Build info
- GitHub code pushes
"""

import asyncio
import logging
import subprocess
import time
from datetime import UTC, datetime, timedelta
from typing import TypedDict, cast

from deployment_service.deployment.state import StateManager
from github import Github
from google.cloud.devtools import cloudbuild_v1

from deployment_api.settings import GCP_PROJECT_ID as DEFAULT_PROJECT_ID
from deployment_api.settings import GCS_REGION as DEFAULT_REGION
from deployment_api.settings import GITHUB_ORG
from deployment_api.settings import STATE_BUCKET as DEFAULT_STATE_BUCKET
from deployment_api.utils.storage_facade import list_objects

from .service_status_cache import load_gcs_cache, save_gcs_cache

logger = logging.getLogger(__name__)

# Service to GCS bucket mapping (constructed from project ID)
_pid = DEFAULT_PROJECT_ID
SERVICE_OUTPUT_BUCKETS = {
    "instruments-service": {
        "CEFI": f"instruments-store-cefi-{_pid}",
        "DEFI": f"instruments-store-defi-{_pid}",
        "TRADFI": f"instruments-store-tradfi-{_pid}",
    },
    "market-tick-data-handler": {
        "CEFI": f"market-data-tick-cefi-{_pid}",
        "DEFI": f"market-data-tick-defi-{_pid}",
        "TRADFI": f"market-data-tick-tradfi-{_pid}",
    },
    "market-data-processing-service": {
        "CEFI": f"market-data-tick-cefi-{_pid}",
        "DEFI": f"market-data-tick-defi-{_pid}",
        "TRADFI": f"market-data-tick-tradfi-{_pid}",
    },
}


class CategoryTimestampDict(TypedDict, total=False):
    """Timestamp info for a single category bucket."""

    timestamp: str | None
    file: str
    size_mb: float
    error: str


class DataTimestampResultDict(TypedDict, total=False):
    """Result from get_latest_data_timestamp."""

    by_category: dict[str, CategoryTimestampDict]
    latest: str | None
    error: str


class DeploymentInfoDict(TypedDict, total=False):
    """Result from get_latest_deployment."""

    deployment_id: str | None
    timestamp: str | None
    status: str | None
    compute_type: str | None
    used_force: bool
    tag: str | None
    total_shards: int
    completed_shards: int
    failed_shards: int
    error: str


class BuildInfoDict(TypedDict, total=False):
    """Result from get_latest_build."""

    build_id: str
    timestamp: str | None
    status: str
    duration_seconds: float | None
    commit_sha: str | None
    error: str


class CodePushInfoDict(TypedDict, total=False):
    """Result from get_latest_code_push."""

    commit_sha: str
    timestamp: str
    message: str
    author: str
    error: str


async def get_latest_data_timestamp(service: str, use_cache: bool = True) -> DataTimestampResultDict | None:
    """
    Get the most recent data file timestamp from GCS for a service.

    Returns dict with category-level timestamps.
    OPTIMIZED: Uses 2-minute cache (data timestamps don't change frequently).
    """
    start = time.time()

    # Check cache first (GCS scanning is slow - 6-8 seconds)
    if use_cache:
        cache = load_gcs_cache()
        data_cache = cache.get("data_timestamps") or {}
        data_times = cache.get("data_timestamp_times") or {}

        if service in data_cache and service in data_times:
            try:
                cache_time = datetime.fromisoformat(data_times[service])
                age = datetime.now(UTC) - cache_time
                if age < timedelta(minutes=2):  # 2-minute cache
                    logger.info("Using cached data timestamps for %s (age: %ss)", service, age.seconds)
                    return data_cache[service]
            except (ValueError, TypeError, KeyError) as e:
                logger.debug("Cache invalid for %s: %s", service, e)

    def _get_timestamps_sync():
        try:
            buckets = SERVICE_OUTPUT_BUCKETS.get(service, {})

            results = {}
            for category, bucket_name in buckets.items():
                try:
                    # List recent files (reduced from 100 to 10 for speed - we only need latest)
                    blobs = list_objects(bucket_name, prefix="", max_results=10)

                    if blobs:
                        # Find most recently updated blob
                        latest_blob = max(
                            blobs,
                            key=lambda b: b.updated if b.updated else datetime.min.replace(tzinfo=UTC),
                        )

                        # Verify GCS blob timestamps are UTC-aware (RFC3339 format)
                        # GCS always returns timestamps in UTC with timezone info
                        latest_ts = latest_blob.updated if latest_blob.updated else None
                        if latest_ts and latest_ts.tzinfo is None:
                            logger.warning("GCS blob timestamp is naive (missing timezone): %s", latest_blob.name)

                        results[category] = {
                            "timestamp": (latest_ts.isoformat() if latest_ts else None),
                            "file": latest_blob.name,
                            "size_mb": (round(latest_blob.size / (1024 * 1024), 2) if latest_blob.size else 0),
                        }
                except (OSError, ValueError, RuntimeError) as e:
                    logger.warning("Error checking %s bucket %s: %s", category, bucket_name, e)
                    results[category] = {"error": str(e)}

            # Overall latest (most recent across all categories)
            valid_timestamps = [datetime.fromisoformat(r["timestamp"]) for r in results.values() if r.get("timestamp")]

            return {
                "by_category": results,
                "latest": (max(valid_timestamps).isoformat() if valid_timestamps else None),
            }
        except (OSError, ValueError, RuntimeError) as e:
            logger.exception("Error getting data timestamps for %s: %s", service, e)
            return {"error": str(e)}

    result = cast(DataTimestampResultDict | None, cast(object, await asyncio.to_thread(_get_timestamps_sync)))

    # Cache the result
    cache = load_gcs_cache()
    data_cache = cache.get("data_timestamps") or {}
    data_times = cache.get("data_timestamp_times") or {}
    data_cache[service] = cast(object, result)
    data_times[service] = datetime.now(UTC).isoformat()
    cache["data_timestamps"] = data_cache
    cache["data_timestamp_times"] = data_times
    save_gcs_cache()

    logger.info("[PERF] get_latest_data_timestamp for %s took %.2fs", service, time.time() - start)
    return result


# Deployment cache TTL (how long before we refresh from GCS state)
DEPLOYMENT_CACHE_TTL = timedelta(minutes=5)


async def get_latest_deployment(service: str, use_cache: bool = True) -> DeploymentInfoDict | None:
    """Get the most recent deployment for a service (with GCS-based caching)."""
    # Load GCS cache
    cache = load_gcs_cache()
    deployments_cache = cache.get("deployments") or {}
    deployment_times = cache.get("deployment_times") or {}

    # Check cache first
    now = datetime.now(UTC)
    if use_cache and deployments_cache.get(service, None) is not None:
        cache_time_str = deployment_times.get(service)
        if cache_time_str:
            try:
                cache_time = datetime.fromisoformat(cache_time_str)
                if (now - cache_time) < DEPLOYMENT_CACHE_TTL:
                    return deployments_cache[service]
            except (ValueError, TypeError, KeyError) as e:
                logger.debug("Deployment cache invalid for %s: %s", service, e)

    def _get_latest_sync():
        try:
            state_manager = StateManager(
                bucket_name=DEFAULT_STATE_BUCKET,
                project_id=DEFAULT_PROJECT_ID,
            )

            deployments = state_manager.list_deployments(service=service, limit=1)

            if deployments:
                latest = deployments[0]

                # Parse cli_args to detect --force flag
                cli_args = latest.get("cli_args") or ""
                used_force = "--force" in cli_args

                # Get shard counts
                progress = latest.get("progress") or {}
                total_shards = (
                    progress.get("total_shards", 0) if isinstance(progress, dict) else latest.get("total_shards", 0)
                )
                completed = (
                    progress.get("completed", 0) if isinstance(progress, dict) else latest.get("completed_shards", 0)
                )
                failed = progress.get("failed", 0) if isinstance(progress, dict) else latest.get("failed_shards", 0)

                return {
                    "deployment_id": latest.get("deployment_id"),
                    "timestamp": latest.get("created_at"),
                    "status": latest.get("status"),
                    "compute_type": latest.get("compute_type"),
                    "used_force": used_force,
                    "tag": latest.get("tag"),
                    "total_shards": total_shards,
                    "completed_shards": completed,
                    "failed_shards": failed,
                }
            return None
        except (OSError, ValueError, RuntimeError) as e:
            logger.exception("Error getting latest deployment for %s: %s", service, e)
            return {"error": str(e)}

    result = cast(DeploymentInfoDict | None, await asyncio.to_thread(_get_latest_sync))

    # Update GCS cache
    deployments_cache[service] = cast(object, result)
    deployment_times[service] = now.isoformat()
    cache["deployments"] = deployments_cache
    cache["deployment_times"] = deployment_times
    save_gcs_cache()  # Persist to GCS

    return result


async def get_latest_build(service: str, use_cache: bool = True) -> BuildInfoDict | None:
    """
    Get the most recent Cloud Build for a service.

    OPTIMIZED: Uses GCS cache aggressively (5-min TTL) to avoid slow Cloud Build API.
    """
    # Check GCS cache first (Cloud Build API is VERY slow - 20+ seconds)
    if use_cache:
        cache = load_gcs_cache()
        builds_cache = cache.get("builds") or {}
        build_times = cache.get("build_times") or {}

        if service in builds_cache and service in build_times:
            try:
                cache_time = datetime.fromisoformat(build_times[service])
                age = datetime.now(UTC) - cache_time
                if age < timedelta(minutes=5):  # 5-minute cache
                    logger.info("Using cached build info for %s (age: %ss)", service, age.seconds)
                    return builds_cache[service]
            except (ValueError, TypeError, KeyError) as e:
                logger.debug("Build cache invalid for %s: %s", service, e)

    def _get_build_sync():
        try:
            # Load GCS cache
            cache = load_gcs_cache()
            trigger_ids = cache.get("trigger_ids") or {}

            # Get trigger ID (from GCS cache or fetch and cache)
            if service not in trigger_ids:
                trigger_result = subprocess.run(
                    [
                        "gcloud",
                        "builds",
                        "triggers",
                        "describe",
                        f"{service}-build",
                        f"--region={DEFAULT_REGION}",
                        "--format=value(id)",
                    ],
                    capture_output=True,
                    text=True,
                    timeout=10,
                )

                if trigger_result.returncode != 0:
                    logger.warning("Trigger %s-build not found", service)
                    return None

                trigger_ids[service] = trigger_result.stdout.strip()
                cache["trigger_ids"] = trigger_ids
                save_gcs_cache()  # Persist to GCS

            trigger_id = trigger_ids[service]

            # Query builds (client-side filtering - server-side filter has issues)
            client = cloudbuild_v1.CloudBuildClient()
            parent = f"projects/{DEFAULT_PROJECT_ID}/locations/{DEFAULT_REGION}"

            # Fetch recent builds without filter (API v1 filter syntax is problematic)
            request = cloudbuild_v1.ListBuildsRequest(
                parent=parent,
                page_size=50,  # Fetch recent builds to filter client-side
            )

            # Get builds and filter by trigger ID client-side
            all_builds = list(client.list_builds(request=request))
            builds = [b for b in all_builds if b.build_trigger_id == trigger_id]
            def _build_sort_key(b: object) -> datetime:
                ct: object = getattr(b, "create_time", None)
                if ct is None:
                    return datetime.min.replace(tzinfo=UTC)
                if isinstance(ct, datetime):
                    return ct
                # Protobuf Timestamp: convert via seconds attribute
                ct_seconds: object = getattr(ct, "seconds", None)
                if ct_seconds is not None:
                    return datetime.fromtimestamp(cast(float, ct_seconds), tz=UTC)
                return datetime.min.replace(tzinfo=UTC)

            builds.sort(key=_build_sort_key, reverse=True)
            builds = builds[:1]  # Get most recent

            if builds:
                build = builds[0]
                result = {
                    "build_id": build.id,
                    "timestamp": (build.create_time.isoformat() if build.create_time else None),
                    "status": build.status.name,
                    "duration_seconds": (
                        (build.finish_time - build.create_time).total_seconds()
                        if build.finish_time and build.create_time
                        else None
                    ),
                    "commit_sha": build.substitutions.get("COMMIT_SHA") or build.substitutions.get("SHORT_SHA"),
                }

                # Cache build info to GCS with timestamp
                builds_cache = cache.get("builds") or {}
                build_times = cache.get("build_times") or {}
                builds_cache[service] = result
                build_times[service] = datetime.now(UTC).isoformat()
                cache["builds"] = builds_cache
                cache["build_times"] = build_times
                save_gcs_cache()

                return result
            return None
        except (OSError, ValueError, RuntimeError) as e:
            logger.exception("Error getting build for %s: %s", service, e)
            return {"error": str(e)}

    return cast(BuildInfoDict | None, await asyncio.to_thread(_get_build_sync))


async def get_latest_code_push(service: str, github_token: str | None = None) -> CodePushInfoDict | None:
    """
    Get the most recent code push (commit) from GitHub.

    Requires github_token from Secret Manager.
    """
    if not github_token:
        return {"error": "GitHub token not provided"}

    def _get_commit_sync():
        try:
            g = Github(github_token)
            repo = g.get_repo(f"{GITHUB_ORG}/{service}")

            # Get latest commit on main branch
            commits = repo.get_commits(sha="main")
            latest_commit = commits[0]

            return {
                "commit_sha": latest_commit.sha[:7],
                "timestamp": latest_commit.commit.author.date.isoformat(),
                "message": latest_commit.commit.message.split("\n")[0][:100],
                "author": latest_commit.commit.author.name,
            }
        except (OSError, ValueError, RuntimeError) as e:
            logger.exception("Error getting GitHub commits for %s: %s", service, e)
            return {"error": str(e)}

    return cast(CodePushInfoDict | None, cast(object, await asyncio.to_thread(_get_commit_sync)))

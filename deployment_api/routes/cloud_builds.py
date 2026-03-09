"""
Cloud Build triggers API routes.

Provides endpoints for:
- Listing Cloud Build triggers
- Running builds manually
- Getting build history
"""

from __future__ import annotations

import asyncio
import concurrent.futures
import logging
import time
import tomllib
from datetime import UTC, datetime
from itertools import islice
from pathlib import Path
from typing import TYPE_CHECKING, Required, TypedDict, cast

from fastapi import APIRouter, HTTPException, Query
from pydantic import BaseModel, Field
from unified_trading_library import __version__ as uts_version

from deployment_api.settings import (
    CLOUD_PROVIDER,
    GITHUB_ORG,
    WORKSPACE_ROOT,
)
from deployment_api.settings import GCS_REGION as DEFAULT_REGION
from deployment_api.settings import gcp_project_id as default_project_id
from deployment_api.utils.cache import TTL_BUILD_INFO, cache

if TYPE_CHECKING:
    from google.cloud.devtools import cloudbuild_v1

logger = logging.getLogger(__name__)


def _cloudbuild_v1():
    # TODO(GH-BACKLOG): migrate to UCI CloudBuildClient when abstraction is available.
    """Deferred cloudbuild_v1 import — no UCI CloudBuildClient abstraction yet."""
    from google.cloud.devtools import cloudbuild_v1  # Deferred — deployment Cloud Build boundary

    return cloudbuild_v1


def _build_op_meta_cls():
    """Deferred BuildOperationMetadata import — deployment Cloud Build boundary."""
    from google.cloud.devtools.cloudbuild_v1 import BuildOperationMetadata  # Deferred

    return BuildOperationMetadata


def _ensure_gcp() -> None:
    """Raise if CLOUD_PROVIDER is aws — CodeBuild integration placeholder."""
    if CLOUD_PROVIDER == "aws":
        raise HTTPException(
            status_code=501,
            detail="CodeBuild integration placeholder — AWS not yet implemented. See ISS-XXX.",
        )


class BuildInfoDict(TypedDict):
    """Serialized Cloud Build information."""

    build_id: str
    status: str
    create_time: str | None
    finish_time: str | None
    duration_seconds: float | None
    commit_sha: str | None
    branch: str | None
    log_url: str | None


class TriggerDict(TypedDict, total=False):
    """Cloud Build trigger information."""

    trigger_id: Required[str]
    trigger_name: str
    service: str
    type: str
    github_repo: str | None
    branch_pattern: str | None
    disabled: bool
    status: str
    last_build: BuildInfoDict | None


class TriggersResponseDict(TypedDict):
    """Response from list_triggers endpoint."""

    triggers: list[TriggerDict]
    total: int
    project: str
    region: str


class BuildHistoryResponseDict(TypedDict):
    """Response from get_build_history endpoint."""

    service: str
    trigger_name: str
    builds: list[BuildInfoDict]
    total: int


class QualityGatesStatusDict(TypedDict, total=False):
    """Quality gates status for a library."""

    status: str
    is_passing: bool
    last_build_time: str | None
    commit_sha: str | None
    branch: str | None


class LibraryStatusDict(TypedDict, total=False):
    """Response from get_library_status endpoint."""

    library: str
    package_version: str | None
    version_in_init: str | None
    github_repo: str
    latest_commit: str | None
    recent_builds: list[BuildInfoDict]
    dependent_services: list[str]
    quality_gates_status: QualityGatesStatusDict | None
    dependency_note: str


class DependencyIssueDict(TypedDict, total=False):
    """A dependency issue found during check."""

    library: str
    issue: str
    status: str
    last_build_time: str | None
    affected_services: list[str]
    pyproject_version: str
    installed_version: str


class DependencyCheckResponseDict(TypedDict):
    """Response from check_dependencies endpoint."""

    has_issues: bool
    issue_count: int
    issues: list[DependencyIssueDict]
    libraries: list[LibraryStatusDict]


class RecentBuildDict(TypedDict):
    """A recently found build (from trigger)."""

    build_id: str
    log_url: str | None
    status: str


class TriggerRunResultDict(TypedDict):
    """Result from running a build trigger."""

    success: bool
    build_id: str | None
    log_url: str | None


router = APIRouter(prefix="/api/cloud-builds", tags=["cloud-builds"])

# Service to trigger name mapping (naming convention: {service}-build)
SERVICES_WITH_TRIGGERS = [
    "instruments-service",
    "market-tick-data-service",
    "market-data-processing-service",
    "features-delta-one-service",
    "features-volatility-service",
    "features-onchain-service",
    "features-calendar-service",
    "ml-training-service",
    "ml-inference-service",
    "strategy-service",
    "execution-service",
    "pnl-attribution-service",
    "position-balance-monitor-service",
    "risk-and-exposure-service",
    "alerting-service",
    "strategy-validation-service",
    "execution-results-api",
    "market-data-api",
    "client-reporting-api",
]

# Libraries/SDKs that publish to Artifact Registry (Python packages, asia-northeast1)
LIBRARIES_WITH_TRIGGERS = [
    "unified-api-contracts",
    "unified-reference-data-interface",
    "unified-config-interface",
    "unified-trading-library",
]

# Infrastructure services (deployment tools, not data pipeline services)
INFRASTRUCTURE_WITH_TRIGGERS = [
    "unified-trading-deployment-v2",
]

# All trackable repos (services + libraries + infrastructure)
ALL_REPOS_WITH_TRIGGERS = (
    SERVICES_WITH_TRIGGERS + LIBRARIES_WITH_TRIGGERS + INFRASTRUCTURE_WITH_TRIGGERS
)


class TriggerBuildRequest(BaseModel):  # CORRECT-LOCAL: FastAPI API contract model
    """Request to trigger a Cloud Build."""

    service: str = Field(..., description="Service name (e.g., 'market-tick-data-service')")
    branch: str = Field(default="main", description="Branch to build from")


class TriggerBuildResponse(BaseModel):  # CORRECT-LOCAL: FastAPI API contract model
    """Response from triggering a Cloud Build."""

    success: bool
    build_id: str | None = None
    log_url: str | None = None
    message: str
    service: str
    branch: str


class BuildTriggerInfo(BaseModel):  # CORRECT-LOCAL: FastAPI API contract model
    """Information about a Cloud Build trigger."""

    trigger_id: str
    trigger_name: str
    service: str
    github_repo: str | None = None
    branch_pattern: str | None = None
    last_build: BuildInfoDict | None = None
    status: str = "unknown"  # active, disabled, unknown


class BuildHistoryEntry(BaseModel):  # CORRECT-LOCAL: FastAPI API contract model
    """A single build history entry."""

    build_id: str
    status: str
    create_time: str | None = None
    finish_time: str | None = None
    duration_seconds: float | None = None
    commit_sha: str | None = None
    branch: str | None = None
    log_url: str | None = None


def _format_build_info(build) -> BuildInfoDict:
    """Format a Cloud Build object into a serializable dict."""
    return {
        "build_id": build.id,
        "status": build.status.name,
        "create_time": build.create_time.isoformat() if build.create_time else None,
        "finish_time": build.finish_time.isoformat() if build.finish_time else None,
        "duration_seconds": (
            (build.finish_time - build.create_time).total_seconds()
            if build.finish_time and build.create_time
            else None
        ),
        "commit_sha": (
            (build.substitutions.get("COMMIT_SHA") or "")[:7] if build.substitutions else None
        ),
        "branch": (build.substitutions.get("BRANCH_NAME") if build.substitutions else None),
        "log_url": build.log_url,
    }


# Trigger ID cache - avoids re-listing triggers for every API call
_trigger_id_cache: dict[str, str] = {}  # trigger_name -> trigger_id
_trigger_cache_time: float = 0
_TRIGGER_CACHE_TTL = 3600  # 1 hour


def _populate_trigger_cache(triggers_list) -> None:
    """Populate trigger ID cache from a list of Cloud Build trigger objects."""
    global _trigger_id_cache, _trigger_cache_time
    new_cache = {}
    for t in triggers_list:
        new_cache[t.name] = t.id
    _trigger_id_cache = new_cache
    _trigger_cache_time = time.time()


def _get_cached_trigger_id(trigger_name: str) -> str | None:
    """Get trigger ID from cache. Returns None if not cached or expired."""
    if time.time() - _trigger_cache_time > _TRIGGER_CACHE_TTL:
        return None
    return _trigger_id_cache.get(trigger_name)


@router.get("/triggers")
async def list_triggers(  # noqa: C901
    force_refresh: bool = Query(False, description="Bypass cache and fetch fresh data"),
) -> TriggersResponseDict:
    """
    List all Cloud Build triggers with their current status.

    Returns a list of triggers with last build info.
    Results are cached for 5 minutes (TTL_BUILD_INFO) to avoid slow Cloud Build API calls.
    Use force_refresh=true to bypass cache.
    """
    _ensure_gcp()
    cache_key = f"cloud_builds:triggers:{default_project_id}:{DEFAULT_REGION}"

    async def fetch_triggers():  # noqa: C901
        def _list_triggers_sync() -> list[TriggerDict]:  # noqa: C901
            _cb = _cloudbuild_v1()
            client = _cb.CloudBuildClient()
            parent = f"projects/{default_project_id}/locations/{DEFAULT_REGION}"

            request = _cb.ListBuildTriggersRequest(
                parent=parent,
                page_size=50,
            )

            triggers = list(client.list_build_triggers(request=request))

            # Populate trigger ID cache for use by history/trigger endpoints
            _populate_trigger_cache(triggers)

            # Filter to services, libraries, and infrastructure we track
            result = []
            for trigger in triggers:
                # Check if this is one of our service triggers
                repo_name = None
                repo_type = None

                for service in SERVICES_WITH_TRIGGERS:
                    if trigger.name == f"{service}-build":
                        repo_name = service
                        repo_type = "service"
                        break

                if not repo_name:
                    # Check if it's a library
                    for library in LIBRARIES_WITH_TRIGGERS:
                        if trigger.name == f"{library}-build":
                            repo_name = library
                            repo_type = "library"
                            break

                if not repo_name:
                    # Check if it's infrastructure
                    for infra in INFRASTRUCTURE_WITH_TRIGGERS:
                        if trigger.name == f"{infra}-build":
                            repo_name = infra
                            repo_type = "infrastructure"
                            break

                if not repo_name:
                    continue

                # Extract GitHub repo info
                github_repo = None
                branch_pattern = None
                if trigger.github:
                    github_repo = f"{trigger.github.owner}/{trigger.github.name}"
                    if trigger.github.push and trigger.github.push.branch:
                        branch_pattern = trigger.github.push.branch
                elif trigger.repository_event_config:
                    repo_config = trigger.repository_event_config
                    if repo_config.repository:
                        # Format: projects/PROJECT/locations/REGION/connections/CONNECTION/repositories/REPO  # noqa: E501
                        parts = repo_config.repository.split("/")
                        if parts:
                            github_repo = parts[-1]
                    if repo_config.push and repo_config.push.branch:
                        branch_pattern = repo_config.push.branch

                result.append(
                    {
                        "trigger_id": trigger.id,
                        "trigger_name": trigger.name,
                        "service": repo_name,  # Legacy field name matching existing API consumers
                        "type": repo_type,  # "service", "library", or "infrastructure"
                        "github_repo": github_repo,
                        "branch_pattern": branch_pattern,
                        "disabled": trigger.disabled,
                        "status": "disabled" if trigger.disabled else "active",
                    }
                )

            return result

        triggers = await asyncio.to_thread(_list_triggers_sync)

        # Fetch last build for each trigger
        builds_info = await _get_recent_builds_for_triggers([t["trigger_id"] for t in triggers])

        # Merge build info into triggers
        for trigger in triggers:
            trigger["last_build"] = builds_info.get(trigger["trigger_id"])

        return {
            "triggers": triggers,
            "total": len(triggers),
            "project": default_project_id,
            "region": DEFAULT_REGION,
        }

    return cast(
        TriggersResponseDict,
        await cache.get_or_fetch(
            cache_key, fetch_triggers, TTL_BUILD_INFO, force_refresh=force_refresh
        ),
    )


async def _get_recent_builds_for_triggers(  # noqa: C901
    trigger_ids: list[str],
) -> dict[str, BuildInfoDict]:
    """Get the most recent build for each trigger using parallel filtered queries.

    Uses Cloud Build API filter parameter to query per-trigger with page_size=1,
    running all queries in parallel via ThreadPoolExecutor.
    This is much faster than fetching all builds and filtering in memory.
    """
    if not trigger_ids:
        return {}

    try:

        def _fetch_latest_build(
            client: cloudbuild_v1.CloudBuildClient, trigger_id: str
        ) -> tuple | None:
            """Fetch the latest build for a single trigger using API-level filter."""
            _cb = _cloudbuild_v1()
            parent = f"projects/{default_project_id}/locations/{DEFAULT_REGION}"
            request = _cb.ListBuildsRequest(
                parent=parent,
                page_size=1,
                filter=f'build_trigger_id="{trigger_id}"',
            )
            # Use next(iter(...)) to get only the first build without exhausting the pager
            build = next(iter(client.list_builds(request=request)), None)
            if not build:
                return None
            return (trigger_id, _format_build_info(build))

        def _fetch_all_sync() -> dict[str, BuildInfoDict]:
            _cb = _cloudbuild_v1()
            client = _cb.CloudBuildClient()
            results = {}

            # Run parallel queries - one per trigger, max 8 concurrent
            with concurrent.futures.ThreadPoolExecutor(
                max_workers=min(len(trigger_ids), 8)
            ) as executor:
                futures = {
                    executor.submit(_fetch_latest_build, client, tid): tid for tid in trigger_ids
                }
                for future in concurrent.futures.as_completed(futures):
                    try:
                        result = future.result()
                        if result:
                            tid, build_info = result
                            results[tid] = build_info
                    except (OSError, ValueError, RuntimeError) as e:
                        tid = futures[future]
                        logger.warning("Error fetching latest build for trigger %s: %s", tid, e)

            return results

        return await asyncio.to_thread(_fetch_all_sync)

    except (OSError, ValueError, RuntimeError) as e:
        logger.warning("Error getting recent builds: %s", e)
        return {}


@router.post("/trigger", response_model=TriggerBuildResponse)
async def trigger_build(request: TriggerBuildRequest) -> TriggerBuildResponse:  # noqa: C901
    """
    Manually trigger a Cloud Build for a service.

    This runs the build trigger as if code was pushed to the specified branch.

    Requires: roles/cloudbuild.builds.editor on the service account.
    """
    if request.service not in ALL_REPOS_WITH_TRIGGERS:
        return TriggerBuildResponse(
            success=False,
            message=f"Unknown service/library: {request.service}. Valid options: {', '.join(ALL_REPOS_WITH_TRIGGERS)}",  # noqa: E501
            service=request.service,
            branch=request.branch,
        )

    trigger_name = f"{request.service}-build"

    try:

        def _get_trigger_id(client: cloudbuild_v1.CloudBuildClient) -> str | None:
            """Get the trigger ID for this service (uses cache first)."""
            cached_id = _get_cached_trigger_id(trigger_name)
            if cached_id:
                return cached_id

            # Cache miss - fetch from API and populate cache
            _cb = _cloudbuild_v1()
            parent = f"projects/{default_project_id}/locations/{DEFAULT_REGION}"
            triggers_request = _cb.ListBuildTriggersRequest(parent=parent)
            triggers = list(client.list_build_triggers(request=triggers_request))
            _populate_trigger_cache(triggers)
            return _trigger_id_cache.get(trigger_name)

        def _find_recent_build(
            trigger_id: str,
            started_after: datetime,
        ) -> RecentBuildDict | None:
            """Find a build for this trigger that started after the given time."""
            _cb = _cloudbuild_v1()
            client = _cb.CloudBuildClient()
            parent = f"projects/{default_project_id}/locations/{DEFAULT_REGION}"
            builds_request = _cb.ListBuildsRequest(
                parent=parent,
                page_size=5,
                filter=f'build_trigger_id="{trigger_id}"',
            )
            # Only check the first few builds (ordered by create_time desc)
            for build in islice(client.list_builds(request=builds_request), 5):
                if build.create_time and build.create_time >= started_after:
                    return {
                        "build_id": build.id,
                        "log_url": build.log_url,
                        "status": build.status.name,
                    }
            return None

        def _run_trigger_sync() -> TriggerRunResultDict:  # noqa: C901
            _cb = _cloudbuild_v1()
            client = _cb.CloudBuildClient()

            # Get trigger ID first (needed for fallback query)
            trigger_id = _get_trigger_id(client)
            if not trigger_id:
                logger.warning("Could not find trigger ID for %s", trigger_name)

            # Record time before triggering (for fallback query)
            trigger_time = datetime.now(UTC)

            # Trigger name format: projects/PROJECT/locations/REGION/triggers/TRIGGER_NAME
            name = (
                f"projects/{default_project_id}/locations/{DEFAULT_REGION}/triggers/{trigger_name}"
            )

            logger.info("Attempting to run trigger: %s on branch %s", name, request.branch)

            # Create the run trigger request
            run_request = _cb.RunBuildTriggerRequest(
                name=name,
                source=_cb.RepoSource(
                    branch_name=request.branch,
                ),
            )

            # Run the trigger - returns a long-running operation
            operation = client.run_build_trigger(request=run_request)

            op_name: str | None = cast(str | None, getattr(operation, "name", None))
            op_done = getattr(operation, "done", None)
            logger.info(
                "Trigger operation returned. Operation name: %s, done: %s", op_name, op_done
            )

            # Get the build metadata from the operation
            build_id = None
            log_url = None

            # Try multiple approaches to get build info
            # Approach 1: Unpack metadata to BuildOperationMetadata
            try:
                if hasattr(operation, "metadata") and operation.metadata:
                    meta = _build_op_meta_cls()()
                    if operation.metadata.Unpack(meta) and meta.build:
                        build_id = meta.build.id
                        log_url = meta.build.log_url
                        logger.info("Got build info from metadata: build_id=%s", build_id)
            except (OSError, ValueError, RuntimeError) as unpack_err:
                logger.warning("Could not unpack BuildOperationMetadata: %s", unpack_err)

            # Approach 2: Try to extract build ID from operation name
            if not build_id and op_name:
                try:
                    # Operation name format: projects/PROJECT/locations/REGION/operations/BUILD_ID
                    parts = op_name.split("/")
                    if len(parts) >= 2 and parts[-2] == "operations":
                        potential_id = parts[-1]
                        # Build IDs are UUIDs like "a1b2c3d4-..."
                        if "-" in potential_id and len(potential_id) > 30:
                            build_id = potential_id
                            logger.info("Got build_id from operation name: %s", build_id)
                except (ValueError, KeyError, TypeError) as e:
                    logger.debug("Suppressed %s during operation: %s", type(e).__name__, e)
                    pass

            # Approach 3 is handled by the async caller after this sync function returns,
            # so that asyncio.sleep can be used instead of blocking time.sleep.
            return {
                "success": True,
                "build_id": build_id,
                "log_url": log_url,
                "trigger_id": trigger_id,
                "trigger_time": trigger_time,
            }

        result = await asyncio.to_thread(_run_trigger_sync)

        # Approach 3: Wait briefly and query for recent builds (done here in async context
        # so we use asyncio.sleep instead of blocking time.sleep in the thread).
        build_id = result.get("build_id")
        log_url = result.get("log_url")
        trigger_id_result = result.get("trigger_id")
        trigger_time_result = result.get("trigger_time")

        if not build_id and trigger_id_result and trigger_time_result:
            logger.info("Build ID not in operation response, querying recent builds...")
            # Wait a moment for the build to be registered
            await asyncio.sleep(2)

            for _attempt in range(3):
                recent_build = await asyncio.to_thread(
                    _find_recent_build, trigger_id_result, trigger_time_result
                )
                if recent_build:
                    build_id = recent_build["build_id"]
                    log_url = recent_build.get("log_url")
                    logger.info("Found build via query: build_id=%s", build_id)
                    break
                await asyncio.sleep(1)

        if not build_id:
            logger.warning("Could not extract build_id from any source")

        # Fall back to Cloud Build console URL if no direct log URL was returned
        if not log_url:
            log_url = (
                f"https://console.cloud.google.com/cloud-build/builds;region={DEFAULT_REGION}/{build_id}?project={default_project_id}"
                if build_id
                else f"https://console.cloud.google.com/cloud-build/builds;region={DEFAULT_REGION}?project={default_project_id}"
            )

        if build_id:
            message = (
                f"Build triggered successfully for {request.service} on branch {request.branch}"
            )
        else:
            message = f"Build trigger called for {request.service} on branch {request.branch}, but could not get build ID. Check Cloud Build console."  # noqa: E501

        return TriggerBuildResponse(
            success=True,
            build_id=build_id,
            log_url=log_url,
            message=message,
            service=request.service,
            branch=request.branch,
        )

    except (OSError, ValueError, RuntimeError) as e:
        error_msg = str(e)

        # Check for permission errors
        if "403" in error_msg or "PERMISSION_DENIED" in error_msg:
            return TriggerBuildResponse(
                success=False,
                message=f"Permission denied. Service account needs 'roles/cloudbuild.builds.editor' role. Error: {error_msg}",  # noqa: E501
                service=request.service,
                branch=request.branch,
            )

        logger.exception("Error triggering build for %s: %s", request.service, e)
        return TriggerBuildResponse(
            success=False,
            message=f"Failed to trigger build: {error_msg}",
            service=request.service,
            branch=request.branch,
        )


@router.get("/history/{service}")
async def get_build_history(service: str, limit: int = 10) -> BuildHistoryResponseDict:
    """
    Get build history for a specific service.

    Returns the most recent builds for the service's trigger.
    """
    if service not in ALL_REPOS_WITH_TRIGGERS:
        raise HTTPException(
            status_code=400,
            detail=f"Unknown service/library: {service}. Valid options: {ALL_REPOS_WITH_TRIGGERS}",
        )

    trigger_name = f"{service}-build"

    try:

        def _get_history_sync() -> list[BuildInfoDict]:
            _cb = _cloudbuild_v1()
            client = _cb.CloudBuildClient()
            parent = f"projects/{default_project_id}/locations/{DEFAULT_REGION}"

            # Try cached trigger ID first (avoids re-listing all triggers)
            trigger_id = _get_cached_trigger_id(trigger_name)

            if not trigger_id:
                # Cache miss - fetch from API and populate cache
                triggers_request = _cb.ListBuildTriggersRequest(
                    parent=parent,
                )
                triggers = list(client.list_build_triggers(request=triggers_request))
                _populate_trigger_cache(triggers)
                trigger_id = _trigger_id_cache.get(trigger_name)

            if not trigger_id:
                return []

            # Get builds filtered by trigger ID (API-level filter, much faster)
            builds_request = _cb.ListBuildsRequest(
                parent=parent,
                page_size=limit,
                filter=f'build_trigger_id="{trigger_id}"',
            )
            # Use islice to stop after getting 'limit' results (avoids exhausting pager)
            builds = list(islice(client.list_builds(request=builds_request), limit))

            return [_format_build_info(b) for b in builds]

        history = await asyncio.to_thread(_get_history_sync)

        return {
            "service": service,
            "trigger_name": trigger_name,
            "builds": history,
            "total": len(history),
        }

    except (OSError, ValueError, RuntimeError) as e:
        logger.exception("Error getting build history for %s: %s", service, e)
        raise HTTPException(status_code=500, detail="Internal server error") from e


@router.get("/library-status/{library}")
async def get_library_status(library: str) -> LibraryStatusDict:  # noqa: C901
    """
    Get detailed status for a library/SDK (like unified-trading-library).

    Returns:
    - Package version from pyproject.toml
    - Latest commit info from GitHub
    - Recent build status from Cloud Build
    - Which services depend on this library
    """
    if library not in LIBRARIES_WITH_TRIGGERS:
        raise HTTPException(
            status_code=404,
            detail=f"Library '{library}' not tracked. Available: {LIBRARIES_WITH_TRIGGERS}",
        )

    result: LibraryStatusDict = {
        "library": library,
        "package_version": None,
        "version_in_init": None,
        "github_repo": f"{GITHUB_ORG}/{library}",
        "latest_commit": None,
        "recent_builds": [],
        "dependent_services": [],
        "quality_gates_status": None,
    }

    # Get package version from pyproject.toml (local workspace)
    try:
        pyproject_path = (
            Path(WORKSPACE_ROOT) / library / "pyproject.toml" if WORKSPACE_ROOT else None
        )
        if pyproject_path and pyproject_path.exists():
            with open(pyproject_path, "rb") as f:
                pyproject = tomllib.load(f)
                result["package_version"] = pyproject.get("project") or {}.get("version")
    except (OSError, ValueError, KeyError) as e:
        logger.debug("Suppressed %s during operation: %s", type(e).__name__, e)
        pass

    # Try to get version from installed package
    try:
        if library == "unified-trading-library":
            result["version_in_init"] = uts_version
    except (OSError, ValueError, RuntimeError) as e:
        logger.warning("Could not get __version__ for %s: %s", library, e)

    # Get recent builds
    try:
        history_response = await get_build_history(library, limit=5)
        result["recent_builds"] = history_response.get("builds") or []

        # Determine quality gates status from recent builds
        if result["recent_builds"]:
            latest_build = result["recent_builds"][0]
            status = latest_build.get("status", "UNKNOWN")
            result["quality_gates_status"] = {
                "status": status,
                "is_passing": status == "SUCCESS",
                "last_build_time": latest_build.get("finish_time")
                or latest_build.get("create_time"),
                "commit_sha": latest_build.get("commit_sha"),
                "branch": latest_build.get("branch"),
            }
    except (OSError, ValueError, RuntimeError) as e:
        logger.warning("Could not get build history for %s: %s", library, e)

    # List services that depend on this library
    if library == "unified-trading-library":
        result["dependent_services"] = SERVICES_WITH_TRIGGERS + INFRASTRUCTURE_WITH_TRIGGERS
        result["dependency_note"] = (
            "All services import unified-trading-library from Git. "
            "If quality gates fail, downstream services may break on rebuild."
        )

    return result


@router.get("/dependency-check")
async def check_dependencies() -> DependencyCheckResponseDict:
    """
    Check if any libraries have failing quality gates that could affect services.

    This is useful for catching version mismatches before they cause runtime errors.
    """
    issues = []
    libraries_status = []

    for library in LIBRARIES_WITH_TRIGGERS:
        try:
            status = await get_library_status(library)
            libraries_status.append(status)

            qg_status = status.get("quality_gates_status") or {}
            if qg_status and not qg_status.get("is_passing", True):
                issues.append(
                    {
                        "library": library,
                        "issue": "Quality gates failing",
                        "status": qg_status.get("status"),
                        "last_build_time": qg_status.get("last_build_time"),
                        "affected_services": status.get("dependent_services") or [],
                    }
                )

            # Check for version mismatch
            pkg_version = status.get("package_version")
            init_version = status.get("version_in_init")
            if pkg_version and init_version and pkg_version != init_version:
                issues.append(
                    {
                        "library": library,
                        "issue": "Version mismatch",
                        "pyproject_version": pkg_version,
                        "installed_version": init_version,
                        "affected_services": status.get("dependent_services") or [],
                    }
                )

        except (OSError, ValueError, RuntimeError) as e:
            logger.warning("Could not check %s: %s", library, e)
            issues.append(
                {
                    "library": library,
                    "issue": f"Check failed: {e!s}",
                }
            )

    return {
        "has_issues": len(issues) > 0,
        "issue_count": len(issues),
        "issues": issues,
        "libraries": libraries_status,
    }

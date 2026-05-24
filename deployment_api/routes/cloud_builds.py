"""
Cloud Build triggers API routes.

Provides endpoints for:
- Listing Cloud Build triggers
- Running builds manually
- Getting build history

Sub-modules:
    _cloud_builds_types    — TypedDicts, Pydantic models, constants, GCP client helpers
    _cloud_builds_trigger  — trigger listing, caching, run operation helpers
    _cloud_builds_history  — build history / recent-build helpers
"""

from __future__ import annotations

import asyncio
import logging
import tomllib
from pathlib import Path
from typing import cast

from fastapi import APIRouter, HTTPException, Query
from unified_trading_library import __version__ as uts_version

from deployment_api.settings import (
    CLOUD_MOCK_MODE,
    CLOUD_PROVIDER,
    GITHUB_ORG,
    WORKSPACE_ROOT,
)
from deployment_api.settings import GCS_REGION as DEFAULT_REGION
from deployment_api.settings import gcp_project_id as default_project_id
from deployment_api.utils.cache import TTL_BUILD_INFO, cache

from ._cloud_builds_history import (  # type: ignore[reportPrivateUsage]
    _format_build_info,
    _get_recent_builds_for_triggers,
)
from ._cloud_builds_trigger import (  # type: ignore[reportPrivateUsage]
    _build_trigger_list_sync,
    _find_recent_build_sync,
    _get_cached_trigger_id,
    _populate_trigger_cache,
    _run_trigger_operation_sync,
    _trigger_id_cache,
)
from ._cloud_builds_types import (
    ALL_REPOS_WITH_TRIGGERS,
    INFRASTRUCTURE_WITH_TRIGGERS,
    LIBRARIES_WITH_TRIGGERS,
    SERVICES_WITH_TRIGGERS,
    BuildHistoryResponseDict,
    DependencyCheckResponseDict,
    DependencyIssueDict,
    LibraryStatusDict,
    TriggerBuildRequest,
    TriggerBuildResponse,
    TriggersResponseDict,
    _ensure_gcp,  # type: ignore[reportPrivateUsage]
    get_cloudbuild_v1,
    get_gcp_build_client,
)
from ._code_builds_aws import (
    get_codebuild_history_sync,
    get_recent_builds_for_projects_sync,
    is_aws_provider,
    list_codebuild_projects_sync,
    start_codebuild_sync,
)

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/api/cloud-builds", tags=["cloud-builds"])

# Re-export public symbols that other modules may import from this package.
__all__ = [
    "ALL_REPOS_WITH_TRIGGERS",
    "INFRASTRUCTURE_WITH_TRIGGERS",
    "LIBRARIES_WITH_TRIGGERS",
    "SERVICES_WITH_TRIGGERS",
    "BuildHistoryResponseDict",
    "DependencyCheckResponseDict",
    "LibraryStatusDict",
    "TriggerBuildRequest",
    "TriggerBuildResponse",
    "TriggersResponseDict",
    "get_gcp_build_client",
    "router",
]

# Public alias used by other modules in this package.

# ---------------------------------------------------------------------------
# Route: list triggers
# ---------------------------------------------------------------------------


@router.get("/triggers")
async def list_triggers(
    force_refresh: bool = Query(False, description="Bypass cache and fetch fresh data"),
) -> TriggersResponseDict:
    """
    List all Cloud Build triggers with their current status.

    Returns a list of triggers with last build info.
    Results are cached for 5 minutes (TTL_BUILD_INFO) to avoid slow Cloud Build API calls.
    Use force_refresh=true to bypass cache.
    """
    if CLOUD_MOCK_MODE or CLOUD_PROVIDER == "local":
        mock_triggers = [
            {
                "trigger_id": f"mock-trigger-{svc}",
                "name": f"{svc}-build",
                "description": f"Build trigger for {svc}",
                "repo": f"IggyIkenna/{svc}",
                "branch": "live-defi-rollout",
                "status": "SUCCESS",
                "last_build": {
                    "build_id": f"mock-build-{svc}-001",
                    "status": "SUCCESS",
                    "create_time": "2026-03-29T06:00:00Z",
                    "finish_time": "2026-03-29T06:08:30Z",
                    "duration_seconds": 510,
                    "commit_sha": "abc1234",
                    "branch": "live-defi-rollout",
                    "log_url": f"https://console.cloud.google.com/cloud-build/builds/mock-{svc}",
                },
            }
            for svc in SERVICES_WITH_TRIGGERS
        ]
        return cast(
            TriggersResponseDict,
            {
                "triggers": mock_triggers,
                "total": len(mock_triggers),
                "project": default_project_id or "mock-project",
                "region": DEFAULT_REGION,
            },
        )

    if is_aws_provider():
        cache_key = f"code_builds:triggers:aws:{DEFAULT_REGION}"

        async def fetch_aws_triggers():
            triggers = await asyncio.to_thread(list_codebuild_projects_sync)
            builds_info = await asyncio.to_thread(
                get_recent_builds_for_projects_sync,
                [t["trigger_id"] for t in triggers],
            )
            for trigger in triggers:
                trigger["last_build"] = builds_info.get(trigger["trigger_id"])
            return {
                "triggers": triggers,
                "total": len(triggers),
                "project": "aws",
                "region": "ap-northeast-1",
            }

        return cast(
            TriggersResponseDict,
            await cache.get_or_fetch(cache_key, fetch_aws_triggers, TTL_BUILD_INFO, force_refresh=force_refresh),
        )

    _ensure_gcp()
    cache_key = f"cloud_builds:triggers:{default_project_id}:{DEFAULT_REGION}"

    async def fetch_triggers():
        triggers = await asyncio.to_thread(_build_trigger_list_sync)
        builds_info = await _get_recent_builds_for_triggers([t["trigger_id"] for t in triggers])
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
        await cache.get_or_fetch(cache_key, fetch_triggers, TTL_BUILD_INFO, force_refresh=force_refresh),
    )


# ---------------------------------------------------------------------------
# Route: trigger build
# ---------------------------------------------------------------------------


async def _trigger_gcp_build(trigger_name: str, service: str, branch: str) -> TriggerBuildResponse:
    """Execute GCP Cloud Build trigger and resolve build ID."""
    result = await asyncio.to_thread(_run_trigger_operation_sync, trigger_name, branch)

    build_id = result.get("build_id")
    log_url = result.get("log_url")
    trigger_id_result = result.get("trigger_id")
    trigger_time_result = result.get("trigger_time")

    if not build_id and trigger_id_result and trigger_time_result:
        logger.info("Build ID not in operation response, querying recent builds...")
        await asyncio.sleep(2)
        for _attempt in range(3):
            recent_build = await asyncio.to_thread(_find_recent_build_sync, trigger_id_result, trigger_time_result)
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

    message = (
        f"Build triggered successfully for {service} on branch {branch}"
        if build_id
        else f"Build trigger called for {service} on branch {branch},"
        " but could not get build ID. Check Cloud Build console."
    )

    return TriggerBuildResponse(
        success=True,
        build_id=build_id,
        log_url=log_url,
        message=message,
        service=service,
        branch=branch,
    )


@router.post("/trigger", response_model=TriggerBuildResponse)
async def trigger_build(request: TriggerBuildRequest) -> TriggerBuildResponse:
    """
    Manually trigger a Cloud Build / CodeBuild for a service.

    Dispatches to GCP Cloud Build or AWS CodeBuild based on CLOUD_PROVIDER.
    """
    if request.service not in ALL_REPOS_WITH_TRIGGERS:
        return TriggerBuildResponse(
            success=False,
            message=(
                f"Unknown service/library: {request.service}. Valid options: {', '.join(ALL_REPOS_WITH_TRIGGERS)}"
            ),
            service=request.service,
            branch=request.branch,
        )

    trigger_name = f"{request.service}-build"

    # AWS CodeBuild path
    if is_aws_provider():
        try:
            result = await asyncio.to_thread(start_codebuild_sync, trigger_name, request.branch)
            return TriggerBuildResponse(
                success=True,
                build_id=result.get("build_id"),
                log_url=result.get("log_url"),
                message=f"CodeBuild triggered for {request.service} on branch {request.branch}",
                service=request.service,
                branch=request.branch,
            )
        except Exception as e:
            logger.exception("Error triggering CodeBuild for %s: %s", request.service, e)
            return TriggerBuildResponse(
                success=False,
                message=f"Failed to trigger CodeBuild: {e}",
                service=request.service,
                branch=request.branch,
            )

    # GCP Cloud Build path
    try:
        return await _trigger_gcp_build(trigger_name, request.service, request.branch)

    except Exception as e:
        error_msg = str(e)

        # Check for permission errors
        if "403" in error_msg or "PERMISSION_DENIED" in error_msg:
            return TriggerBuildResponse(
                success=False,
                message=(
                    "Permission denied. Service account needs"
                    f" 'roles/cloudbuild.builds.editor' role. Error: {error_msg}"
                ),
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


# ---------------------------------------------------------------------------
# Route: build history
# ---------------------------------------------------------------------------


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

    if CLOUD_MOCK_MODE or CLOUD_PROVIDER == "local":
        return {
            "service": service,
            "trigger_name": f"{service}-build",
            "builds": [
                {
                    "build_id": f"mock-build-{service}-{i:03d}",
                    "status": "SUCCESS" if i < 3 else "FAILURE",
                    "create_time": f"2026-03-{29 - i:02d}T06:00:00Z",
                    "finish_time": f"2026-03-{29 - i:02d}T06:08:30Z",
                    "duration_seconds": 510,
                    "commit_sha": f"abc{i:04d}",
                    "branch": "live-defi-rollout",
                    "log_url": f"https://console.cloud.google.com/cloud-build/builds/mock-{service}-{i}",
                }
                for i in range(min(limit, 5))
            ],
            "total": min(limit, 5),
        }

    trigger_name = f"{service}-build"

    # AWS CodeBuild path
    if is_aws_provider():
        try:
            history = await asyncio.to_thread(get_codebuild_history_sync, trigger_name, limit)
            return {
                "service": service,
                "trigger_name": trigger_name,
                "builds": history,
                "total": len(history),
            }
        except Exception as e:
            logger.exception("Error getting CodeBuild history for %s: %s", service, e)
            raise HTTPException(status_code=500, detail=f"CodeBuild API error: {e}") from e

    # GCP Cloud Build path
    try:

        def _get_history_sync() -> list[object]:
            _cb = get_cloudbuild_v1()
            client = get_gcp_build_client()
            parent = f"projects/{default_project_id}/locations/{DEFAULT_REGION}"
            from itertools import islice

            # Try cached trigger ID first (avoids re-listing all triggers)
            trigger_id = _get_cached_trigger_id(trigger_name)

            if not trigger_id:
                # Cache miss - fetch from API and populate cache
                triggers_request = _cb.ListBuildTriggersRequest(
                    parent=parent,
                )
                triggers = list(client.list_build_triggers(request=triggers_request))  # pyright: ignore[reportUnknownMemberType]  # CloudBuild stubs incomplete
                _populate_trigger_cache(triggers)
                trigger_id = _trigger_id_cache.get(trigger_name)

            if not trigger_id:
                return []

            # Use project_id (v1 API style) — regional parent path fails with 400 on REST transport
            builds_request = _cb.ListBuildsRequest(
                project_id=default_project_id,
                page_size=limit,
                filter=f'trigger_id="{trigger_id}"',
            )
            # Use islice to stop after getting 'limit' results (avoids exhausting pager)
            builds = list(islice(client.list_builds(request=builds_request), limit))  # pyright: ignore[reportUnknownMemberType]  # CloudBuild stubs incomplete

            return builds

        raw_builds = await asyncio.to_thread(_get_history_sync)
        history = [_format_build_info(b) for b in raw_builds]

        return {
            "service": service,
            "trigger_name": trigger_name,
            "builds": history,
            "total": len(history),
        }

    except Exception as e:
        logger.exception("Error getting build history for %s: %s", service, e)
        raise HTTPException(status_code=500, detail=f"Cloud Build API error: {e}") from e


# ---------------------------------------------------------------------------
# Route: library status
# ---------------------------------------------------------------------------


@router.get("/library-status/{library}")
async def get_library_status(library: str) -> LibraryStatusDict:
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
        pyproject_path = Path(WORKSPACE_ROOT) / library / "pyproject.toml" if WORKSPACE_ROOT else None
        if pyproject_path and pyproject_path.exists():
            with open(pyproject_path, "rb") as f:
                pyproject: dict[str, object] = cast(dict[str, object], tomllib.load(f))
                project_section_raw: object = pyproject.get("project") or {}
                if isinstance(project_section_raw, dict):
                    project_section = cast(dict[str, object], project_section_raw)
                    version_raw = project_section.get("version")
                    result["package_version"] = str(version_raw) if version_raw is not None else None
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
                "last_build_time": latest_build.get("finish_time") or latest_build.get("create_time"),
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


# ---------------------------------------------------------------------------
# Route: dependency check
# ---------------------------------------------------------------------------


@router.get("/dependency-check")
async def check_dependencies() -> DependencyCheckResponseDict:
    """
    Check if any libraries have failing quality gates that could affect services.

    This is useful for catching version mismatches before they cause runtime errors.
    """
    issues: list[DependencyIssueDict] = []
    libraries_status: list[LibraryStatusDict] = []

    for library in LIBRARIES_WITH_TRIGGERS:
        try:
            status = await get_library_status(library)
            libraries_status.append(status)

            qg_status = status.get("quality_gates_status") or {}
            if qg_status and not qg_status.get("is_passing", True):
                _qg_status_raw: object = qg_status.get("status")
                _qg_last_build_raw: object = qg_status.get("last_build_time")
                _dep_services: list[str] = list(status.get("dependent_services") or [])
                issues.append(
                    cast(
                        DependencyIssueDict,
                        {
                            "library": library,
                            "issue": "Quality gates failing",
                            "status": str(_qg_status_raw) if _qg_status_raw is not None else "",
                            "last_build_time": str(_qg_last_build_raw) if _qg_last_build_raw is not None else None,
                            "affected_services": _dep_services,
                        },
                    )
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

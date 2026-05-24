"""
AWS CodeBuild client helpers — parallel to GCP Cloud Build modules.

Provides:
- CodeBuild client factory
- Project listing (equivalent to GCP triggers)
- Build history retrieval
- Build trigger (start_build)

Uses boto3 CodeBuild client. All functions are sync (run via asyncio.to_thread).
"""

from __future__ import annotations

import logging
from datetime import datetime
from typing import TYPE_CHECKING, Any, cast

if TYPE_CHECKING:
    from typing import Any

    # boto3 clients don't have complete typing, using Any for compatibility
    CodeBuildClient = Any
    STSClient = Any
else:
    # Runtime imports
    CodeBuildClient = object
    STSClient = object

from deployment_api.settings import CLOUD_PROVIDER

from ._cloud_builds_types import (
    ALL_REPOS_WITH_TRIGGERS,
    BuildInfoDict,
    TriggerDict,
)

logger = logging.getLogger(__name__)

# AWS region for CodeBuild — matches GCP asia-northeast1
_AWS_REGION = "ap-northeast-1"


def _get_codebuild_client() -> Any:
    """Return a boto3 CodeBuild client for ap-northeast-1."""
    import boto3  # Deferred — AWS SDK boundary

    return boto3.client("codebuild", region_name=_AWS_REGION)


def _get_aws_account_id() -> str:
    """Get AWS account ID from STS."""
    import boto3  # Deferred — AWS SDK boundary

    sts: Any = boto3.client("sts", region_name=_AWS_REGION)
    identity: dict[str, Any] = cast(dict[str, Any], sts.get_caller_identity())
    return identity["Account"]


def _format_codebuild_build(build: dict[str, object]) -> BuildInfoDict:
    """Format a CodeBuild build dict into our standard BuildInfoDict."""
    build_id_full = str(build.get("id", ""))  # noqa: qg-empty-fallback — AWS SDK boundary
    # CodeBuild build IDs are "project-name:build-uuid"
    build_id = build_id_full.split(":")[-1] if ":" in build_id_full else build_id_full

    status_raw = str(build.get("buildStatus", "UNKNOWN"))
    # Map CodeBuild statuses to our standard statuses
    status_map: dict[str, str] = {
        "SUCCEEDED": "SUCCESS",
        "FAILED": "FAILURE",
        "FAULT": "FAILURE",
        "TIMED_OUT": "TIMEOUT",
        "IN_PROGRESS": "WORKING",
        "STOPPED": "CANCELLED",
    }
    status = status_map.get(status_raw, status_raw)

    start_time = build.get("startTime")
    end_time = build.get("endTime")
    start_str = start_time.isoformat() if isinstance(start_time, datetime) else None
    end_str = end_time.isoformat() if isinstance(end_time, datetime) else None

    duration: float | None = None
    if isinstance(start_time, datetime) and isinstance(end_time, datetime):
        duration = (end_time - start_time).total_seconds()

    # Extract source info
    source_version = str(build.get("sourceVersion", "")) if build.get("sourceVersion") else None  # noqa: qg-empty-fallback — AWS SDK boundary
    commit_sha = (
        str(build.get("resolvedSourceVersion", ""))[:7]  # noqa: qg-empty-fallback — AWS SDK boundary
        if build.get("resolvedSourceVersion")
        else None
    )

    # Build log URL
    logs: dict[str, Any] = cast(dict[str, Any], build.get("logs", {}))  # noqa: qg-empty-fallback — AWS SDK boundary
    log_url: str | None = None
    deep_link: Any = logs.get("deepLink")
    if isinstance(deep_link, str):
        log_url = deep_link
    if not log_url:
        project_name = str(build.get("projectName", ""))  # noqa: qg-empty-fallback — AWS SDK boundary
        log_url = f"https://{_AWS_REGION}.console.aws.amazon.com/codesuite/codebuild/projects/{project_name}/build/{build_id_full}"

    return {
        "build_id": build_id,
        "status": status,
        "create_time": start_str,
        "finish_time": end_str,
        "duration_seconds": duration,
        "commit_sha": commit_sha,
        "branch": source_version,
        "log_url": log_url,
    }


def list_codebuild_projects_sync() -> list[TriggerDict]:
    """List CodeBuild projects that match our service naming convention.

    CodeBuild projects are the AWS equivalent of Cloud Build triggers.
    Convention: project name = "{service}-build" (same as GCP trigger names).
    """
    client: Any = _get_codebuild_client()
    all_projects: list[str] = []

    # List all projects (paginated)
    paginator = client.get_paginator("list_projects")
    for page in paginator.paginate():
        project_names: list[str] = cast(list[str], page.get("projects", []))  # noqa: qg-empty-fallback — AWS SDK boundary
        all_projects.extend(project_names)

    # Filter to known services
    known_trigger_names = {f"{svc}-build" for svc in ALL_REPOS_WITH_TRIGGERS}
    matched_projects = [p for p in all_projects if p in known_trigger_names]

    if not matched_projects:
        logger.info("No matching CodeBuild projects found (have %d total)", len(all_projects))
        return []

    # Get project details in batches of 100
    triggers: list[TriggerDict] = []
    for i in range(0, len(matched_projects), 100):
        batch = matched_projects[i : i + 100]
        response = client.batch_get_projects(names=batch)
        for proj in response.get("projects", []):  # noqa: qg-empty-fallback — AWS SDK boundary
            proj_name = str(proj.get("name", ""))  # noqa: qg-empty-fallback — AWS SDK boundary
            service_name = proj_name.removesuffix("-build")
            source = proj.get("source", {})  # noqa: qg-empty-fallback — AWS SDK boundary
            source_type = str(source.get("type", "")) if isinstance(source, dict) else ""  # noqa: qg-empty-fallback — AWS SDK boundary

            github_repo: str | None = None
            if isinstance(source, dict) and source_type in ("GITHUB", "GITHUB_ENTERPRISE"):
                location = str(source.get("location", ""))
                # Extract org/repo from URL
                if "github.com/" in location:
                    github_repo = location.split("github.com/")[-1].rstrip(".git")

            triggers.append(
                cast(
                    TriggerDict,
                    {
                        "trigger_id": proj_name,
                        "trigger_name": proj_name,
                        "service": service_name,
                        "type": "codebuild",
                        "github_repo": github_repo,
                        "branch_pattern": None,
                        "disabled": False,
                        "status": "active",
                    },
                )
            )

    return triggers


def get_codebuild_history_sync(project_name: str, limit: int = 10) -> list[BuildInfoDict]:
    """Get build history for a CodeBuild project."""
    client: Any = _get_codebuild_client()

    # List build IDs for the project
    response = client.list_builds_for_project(
        projectName=project_name,
        sortOrder="DESCENDING",
    )
    build_ids: list[str] = response.get("ids", [])[:limit]  # noqa: qg-empty-fallback — AWS SDK boundary

    if not build_ids:
        return []

    # Get build details
    builds_response = client.batch_get_builds(ids=build_ids)
    builds: list[dict[str, object]] = builds_response.get("builds", [])  # noqa: qg-empty-fallback — AWS SDK boundary

    return [_format_codebuild_build(b) for b in builds]


def get_recent_builds_for_projects_sync(
    project_names: list[str],
) -> dict[str, BuildInfoDict | None]:
    """Get the most recent build for each CodeBuild project."""
    result: dict[str, BuildInfoDict | None] = {}
    client: Any = _get_codebuild_client()

    for proj_name in project_names:
        try:
            response = client.list_builds_for_project(
                projectName=proj_name,
                sortOrder="DESCENDING",
            )
            build_ids: list[str] = response.get("ids", [])[:1]  # noqa: qg-empty-fallback — AWS SDK boundary
            if build_ids:
                builds_response = client.batch_get_builds(ids=build_ids)
                builds: list[dict[str, object]] = builds_response.get("builds", [])  # noqa: qg-empty-fallback — AWS SDK boundary
                if builds:
                    result[proj_name] = _format_codebuild_build(builds[0])
                    continue
            result[proj_name] = None
        except Exception:
            logger.debug("Failed to get recent build for %s", proj_name)
            result[proj_name] = None

    return result


def start_codebuild_sync(project_name: str, branch: str = "live-defi-rollout") -> dict[str, str | None]:
    """Start a CodeBuild build for a project.

    Returns dict with build_id, log_url, status.
    """
    client: Any = _get_codebuild_client()

    response = client.start_build(
        projectName=project_name,
        sourceVersion=branch,
    )
    build = response.get("build", {})  # noqa: qg-empty-fallback — AWS SDK boundary
    build_id_full = str(build.get("id", ""))  # noqa: qg-empty-fallback — AWS SDK boundary
    build_id = build_id_full.split(":")[-1] if ":" in build_id_full else build_id_full

    log_url = f"https://{_AWS_REGION}.console.aws.amazon.com/codesuite/codebuild/projects/{project_name}/build/{build_id_full}"

    return {
        "build_id": build_id,
        "log_url": log_url,
        "status": "IN_PROGRESS",
    }


def is_aws_provider() -> bool:
    """Check if current cloud provider is AWS."""
    return CLOUD_PROVIDER == "aws"

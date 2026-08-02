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
import time
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

from deployment_api.settings import (
    AWS_CODEBUILD_READER_ROLE_ARN,
    AWS_CODEBUILD_REGION,
    CLOUD_PROVIDER,
)

from ._cloud_builds_types import (
    ALL_REPOS_WITH_TRIGGERS,
    BuildInfoDict,
    TriggerDict,
)

logger = logging.getLogger(__name__)

# AWS region for CodeBuild — matches GCP asia-northeast1 (config-driven, same default).
_AWS_REGION = AWS_CODEBUILD_REGION

# Keyless GCP->AWS WIF credential cache. The assumed-role creds are valid 1h; refresh at 50min so a
# request never races the expiry. Module-global (process-wide) — the reader is read-only + idempotent.
_WIF_CACHE_TTL_SECONDS = 50 * 60
_wif_creds_cache: tuple[float, dict[str, str]] | None = None


def _import_boto3():  # type: ignore[reportAny]
    """Deferred boto3 import — the single AWS-SDK-boundary site shared by WIF + client construction."""
    import boto3  # noqa: imports-inside-functions — Deferred, AWS SDK boundary

    return boto3  # type: ignore[reportUnknownVariableType, reportAny]


def _assume_codebuild_reader_role() -> dict[str, str]:
    """Mint short-lived AWS creds via keyless GCP->AWS Workload Identity Federation.

    Flow (no static AWS key anywhere): (1) mint a Google OIDC ID token for the Cloud Run service
    account from the metadata server; (2) exchange it for short-lived STS creds via
    ``AssumeRoleWithWebIdentity`` against ``AWS_CODEBUILD_READER_ROLE_ARN`` (the role's trust policy
    is locked to this SA's OIDC subject + grants read-only CodeBuild). Returns boto3 client kwargs
    (``aws_access_key_id``/``aws_secret_access_key``/``aws_session_token``); cached ~50min.
    """
    global _wif_creds_cache
    now = time.monotonic()
    if _wif_creds_cache is not None and now < _wif_creds_cache[0]:
        return _wif_creds_cache[1]

    boto3 = _import_boto3()
    import google.auth.transport.requests  # noqa: imports-inside-functions — Deferred, Google auth boundary
    import google.oauth2.id_token  # noqa: imports-inside-functions — Deferred, Google auth boundary

    # The role trust conditions only on the SA's OIDC subject, so any stable audience works; the role
    # ARN is a convenient, self-documenting choice.
    id_token: str = cast(
        str,
        google.oauth2.id_token.fetch_id_token(  # type: ignore[reportUnknownMemberType]
            google.auth.transport.requests.Request(), AWS_CODEBUILD_READER_ROLE_ARN
        ),
    )
    # AssumeRoleWithWebIdentity needs NO existing AWS credentials (the web-identity token IS the auth),
    # so a plain STS client works even on a host with no AWS creds (the GCP-hosted dashboard).
    sts: STSClient = boto3.client("sts", region_name=_AWS_REGION)  # type: ignore[reportUnknownMemberType, reportAny]
    resp: dict[str, object] = cast(
        dict[str, object],
        sts.assume_role_with_web_identity(  # type: ignore[reportUnknownMemberType]
            RoleArn=AWS_CODEBUILD_READER_ROLE_ARN,
            RoleSessionName="repo-ci-codebuild",
            WebIdentityToken=id_token,
        ),
    )
    creds: dict[str, object] = cast(dict[str, object], resp["Credentials"])
    out = {
        "aws_access_key_id": str(creds["AccessKeyId"]),
        "aws_secret_access_key": str(creds["SecretAccessKey"]),
        "aws_session_token": str(creds["SessionToken"]),
    }
    _wif_creds_cache = (now + _WIF_CACHE_TTL_SECONDS, out)
    return out


def _get_codebuild_client() -> CodeBuildClient:  # type: ignore[reportAny]
    """Return a boto3 CodeBuild client for the configured region.

    When ``AWS_CODEBUILD_READER_ROLE_ARN`` is set (the GCP-hosted dashboard reading AWS build status),
    auth is keyless GCP->AWS WIF — short-lived assumed-role creds via a per-request boto3 Session, no
    static AWS key. When unset (native-AWS deployment / local AWS profile), the default boto3
    credential chain is used.
    """
    boto3 = _import_boto3()

    if AWS_CODEBUILD_READER_ROLE_ARN:
        creds = _assume_codebuild_reader_role()
        session = boto3.Session(  # type: ignore[reportUnknownMemberType, reportAny]
            aws_access_key_id=creds["aws_access_key_id"],
            aws_secret_access_key=creds["aws_secret_access_key"],
            aws_session_token=creds["aws_session_token"],
        )
        return session.client("codebuild", region_name=_AWS_REGION)  # type: ignore[reportUnknownVariableType, reportUnknownMemberType, reportAny]
    return boto3.client("codebuild", region_name=_AWS_REGION)  # type: ignore[reportUnknownVariableType, reportAny]


def _get_aws_account_id() -> str:
    """Get AWS account ID from STS."""
    import boto3  # noqa: imports-inside-functions — Deferred, AWS SDK boundary

    sts: STSClient = boto3.client("sts", region_name=_AWS_REGION)  # type: ignore[reportUnknownMemberType, reportAny]
    identity: dict[str, object] = cast(dict[str, object], sts.get_caller_identity())  # type: ignore[reportUnknownMemberType]
    account_id = identity.get("Account", "")  # noqa: qg-empty-fallback — AWS SDK boundary
    return str(account_id)


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
    logs: dict[str, object] = cast(dict[str, object], build.get("logs", {}))  # noqa: qg-empty-fallback — AWS SDK boundary
    log_url: str | None = None
    deep_link: object = logs.get("deepLink")
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
    client: CodeBuildClient = _get_codebuild_client()  # type: ignore[reportAny]
    all_projects: list[str] = []

    # List all projects (paginated)
    paginator = client.get_paginator("list_projects")  # type: ignore[reportAny, reportUnknownMemberType]
    for page in paginator.paginate():  # type: ignore[reportAny, reportUnknownMemberType]
        page_dict = cast(dict[str, object], page)
        project_names: list[str] = cast(list[str], page_dict.get("projects", []))  # noqa: qg-empty-fallback — AWS SDK boundary
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
        response = client.batch_get_projects(names=batch)  # type: ignore[reportAny, reportUnknownMemberType]
        response_dict = cast(dict[str, object], response)
        projects_raw = response_dict.get("projects", [])  # noqa: qg-empty-fallback — AWS SDK boundary
        projects = cast(list[dict[str, object]], projects_raw) if projects_raw else []

        for proj in projects:  # noqa: qg-empty-fallback — AWS SDK boundary
            proj_name = str(proj.get("name", ""))  # noqa: qg-empty-fallback — AWS SDK boundary
            service_name = proj_name.removesuffix("-build")
            source_raw = proj.get("source", {})  # noqa: qg-empty-fallback — AWS SDK boundary
            source = cast(dict[str, object], source_raw) if isinstance(source_raw, dict) else {}
            source_type = str(source.get("type", "")) if source else ""  # noqa: qg-empty-fallback — AWS SDK boundary

            github_repo: str | None = None
            if source and source_type in ("GITHUB", "GITHUB_ENTERPRISE"):
                location = str(source.get("location", ""))  # noqa: qg-empty-fallback — AWS SDK boundary
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
    client: CodeBuildClient = _get_codebuild_client()  # type: ignore[reportAny]

    # List build IDs for the project
    response = client.list_builds_for_project(  # type: ignore[reportAny, reportUnknownMemberType]
        projectName=project_name,
        sortOrder="DESCENDING",
    )
    response_dict = cast(dict[str, object], response)
    build_ids_raw = response_dict.get("ids", [])  # noqa: qg-empty-fallback — AWS SDK boundary
    build_ids: list[str] = cast(list[str], build_ids_raw)[:limit] if build_ids_raw else []  # noqa: qg-empty-fallback — AWS SDK boundary

    if not build_ids:
        return []

    # Get build details
    builds_response = client.batch_get_builds(ids=build_ids)  # type: ignore[reportAny, reportUnknownMemberType]
    builds_response_dict = cast(dict[str, object], builds_response)
    builds_raw = builds_response_dict.get("builds", [])  # noqa: qg-empty-fallback — AWS SDK boundary
    builds: list[dict[str, object]] = cast(list[dict[str, object]], builds_raw) if builds_raw else []  # noqa: qg-empty-fallback — AWS SDK boundary

    return [_format_codebuild_build(b) for b in builds]


def get_recent_builds_for_projects_sync(
    project_names: list[str],
) -> dict[str, BuildInfoDict | None]:
    """Get the most recent build for each CodeBuild project."""
    result: dict[str, BuildInfoDict | None] = {}
    client: CodeBuildClient = _get_codebuild_client()  # type: ignore[reportAny]

    for proj_name in project_names:
        try:
            response = client.list_builds_for_project(  # type: ignore[reportAny, reportUnknownMemberType]
                projectName=proj_name,
                sortOrder="DESCENDING",
            )
            response_dict = cast(dict[str, object], response)
            build_ids_raw = response_dict.get("ids", [])  # noqa: qg-empty-fallback — AWS SDK boundary
            build_ids: list[str] = cast(list[str], build_ids_raw)[:1] if build_ids_raw else []  # noqa: qg-empty-fallback — AWS SDK boundary
            if build_ids:
                builds_response = client.batch_get_builds(ids=build_ids)  # type: ignore[reportAny, reportUnknownMemberType]
                builds_response_dict = cast(dict[str, object], builds_response)
                builds_raw = builds_response_dict.get("builds", [])  # noqa: qg-empty-fallback — AWS SDK boundary
                builds: list[dict[str, object]] = cast(list[dict[str, object]], builds_raw) if builds_raw else []  # noqa: qg-empty-fallback — AWS SDK boundary
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
    client: CodeBuildClient = _get_codebuild_client()  # type: ignore[reportAny]

    response = client.start_build(  # type: ignore[reportAny, reportUnknownMemberType]
        projectName=project_name,
        sourceVersion=branch,
    )
    response_dict = cast(dict[str, object], response)
    build_raw = response_dict.get("build", {})  # noqa: qg-empty-fallback — AWS SDK boundary
    build = cast(dict[str, object], build_raw) if isinstance(build_raw, dict) else {}
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

"""
Build listing and manual deploy API routes.

Provides endpoints for:
- Listing available builds per service from Artifact Registry (or deployed_versions fallback)
- Manually deploying any build tag to any environment (no version gate — deployment is manual)

Build tag format (produced by cloud-build-router.yml):
  main         → {semver}                  e.g. 1.0.0
  staging      → {semver}-staging          e.g. 1.0.0-staging
  feat/*       → {semver}-{branch-slug}    e.g. 0.3.168-feat-my-feature

Display format: "{version} @ {branch}"  e.g. "0.3.168 @ feat/my-feature"
"""

from __future__ import annotations

import json
import logging
import re
import subprocess
from pathlib import Path
from typing import Literal, cast

from fastapi import APIRouter, HTTPException, Query
from pydantic import BaseModel, Field

from deployment_api.settings import (
    CLOUD_MOCK_MODE,
    CLOUD_PROVIDER,
    WORKSPACE_ROOT,
)
from deployment_api.settings import gcp_project_id as default_project_id

# AWS ECR configuration
_ECR_REGION = "ap-northeast-1"

logger = logging.getLogger(__name__)

router = APIRouter()

# Artifact Registry configuration
_AR_HOST = "asia-northeast1-docker.pkg.dev"

# Legacy AR repos use shortened names (pre-terraform); all other services push to
# unified-trading-system via cloudbuild.yaml _REGISTRY_REPO, which is the default.
_AR_REPO_OVERRIDES: dict[str, str] = {
    "instruments-service": "instruments",
    "execution-service": "execution",
    "market-data-processing-service": "market-data-processing",
}
# Cloud Build canonical repo — all services push here via cloudbuild.yaml _REGISTRY_REPO
_CB_REGISTRY_REPO = "unified-trading-system"
_SEMVER_RE = re.compile(r"^(\d+\.\d+\.\d+)(.*)$")
_BRANCH_PREFIX_RE = re.compile(r"^(feat|fix|chore|refactor|perf|ci|docs|test)-(.+)$")

Environment = Literal["dev", "staging", "prod"]


class BuildEntry(BaseModel):  # CORRECT-LOCAL — API response schema local to this route; not a domain contract
    """A single available build in Artifact Registry."""

    tag: str = Field(..., description="Artifact Registry image tag")
    display: str = Field(..., description="Human-readable '{version} @ {branch}' label")
    version: str = Field(..., description="Semver string extracted from tag")
    branch: str = Field(..., description="Branch name extracted from tag slug")
    is_v1: bool = Field(..., description="True if version >= 1.0.0")


class DeployRequest(BaseModel):  # CORRECT-LOCAL — API request schema local to this route; not a domain contract
    """Request body for deploying a specific build tag to an environment."""

    image_tag: str = Field(..., description="Artifact Registry image tag to deploy")
    environment: Environment = Field(..., description="Target environment: dev, staging, or prod")
    cloud_run_service_name: str | None = Field(
        None,
        description=(
            "Override the live Cloud Run SERVICE name to deploy to. Defaults to the "
            "`service` path parameter when omitted (the common case — image name == "
            "Cloud Run service name). Needed when the built image's package/repo name "
            "diverges from the actual deployed Cloud Run resource name, e.g. "
            "alerting-service's image is `alerting-service:<tag>` but the live Cloud "
            "Run service is `dp-alerting-subscriber` "
            "(terraform/gcp/critical_service_uptime.tf). The `service` path param "
            "still drives Artifact Registry image resolution either way."
        ),
    )


_COMMIT_SHA_RE = re.compile(r"^[0-9a-f]{7,40}$")


def _tag_to_entry(tag: str) -> BuildEntry:
    """Parse an AR tag string into a BuildEntry with human-readable display."""
    # Handle commit SHA tags (e.g., "3c1f37a", "abc1234def")
    if _COMMIT_SHA_RE.match(tag):
        return BuildEntry(tag=tag, display=f"{tag[:7]} @ commit", version=tag[:7], branch="commit", is_v1=False)
    m = _SEMVER_RE.match(tag)
    if not m:
        return BuildEntry(tag=tag, display=tag, version=tag, branch="unknown", is_v1=False)

    version = m.group(1)
    suffix = m.group(2).lstrip("-")  # e.g. "feat-my-feature", "staging", ""

    if not suffix:
        branch = "main"
    elif suffix == "staging":
        branch = "staging"
    elif suffix in ("live-defi-rollout", "ldr"):
        # LDR — the integration branch. cloud-build-router tags LDR builds
        # {semver}-live-defi-rollout; recognise it as a first-class branch so the UI can
        # offer "launch from LDR latest" alongside main / staging.
        branch = "live-defi-rollout"
    else:
        # Reverse branch slug: feat-my-feature → feat/my-feature
        pm = _BRANCH_PREFIX_RE.match(suffix)
        branch = f"{pm.group(1)}/{pm.group(2)}" if pm else suffix

    major, minor, _ = (int(x) for x in version.split("."))
    is_v1 = major >= 1 or (major == 1 and minor >= 0)

    return BuildEntry(
        tag=tag,
        display=f"{version} @ {branch}",
        version=version,
        branch=branch,
        is_v1=is_v1,
    )


def _sort_key(entry: BuildEntry) -> tuple[int, int, int]:
    """Sort descending by (major, minor, patch)."""
    try:
        parts = tuple(int(x) for x in entry.version.split("."))
        return (-parts[0], -parts[1], -parts[2])
    except (ValueError, IndexError):
        return (0, 0, 0)


def _mock_builds_from_manifest(service: str, env: str) -> list[BuildEntry]:
    """Fallback for mock mode: read deployed_versions from PM manifest."""
    manifest_path = Path(WORKSPACE_ROOT) / "unified-trading-pm" / "workspace-manifest.json"
    if not manifest_path.is_file():
        return []
    try:
        manifest = cast(dict[str, object], json.loads(manifest_path.read_text()))
        deployed_raw: object = manifest.get("deployed_versions")  # noqa: qg-empty-fallback — dict.get default for missing key in manifest
        deployed: dict[str, dict[str, str]] = (
            cast(dict[str, dict[str, str]], deployed_raw) if isinstance(deployed_raw, dict) else {}
        )
        env_deployed = deployed.get(env, {})  # noqa: qg-empty-fallback — dict.get default for missing env
        tag = env_deployed.get(service, "")  # noqa: qg-empty-fallback — dict.get default for missing service
        if tag:
            return [_tag_to_entry(tag)]
    except (json.JSONDecodeError, OSError, KeyError):
        logger.warning("Could not read manifest for mock builds fallback")
    return []


def _get_ar_repo_name(service: str) -> str:
    """Map service name to its Artifact Registry repository name.

    Defaults to _CB_REGISTRY_REPO (unified-trading-system) — the repo all services push
    to via cloudbuild.yaml _REGISTRY_REPO. _AR_REPO_OVERRIDES covers only the three legacy
    repos with shortened pre-terraform names that have their own named AR repo.
    """
    return _AR_REPO_OVERRIDES.get(service, _CB_REGISTRY_REPO)


async def _list_ar_tags_from_repo(service: str, project: str, ar_repo: str) -> list[str]:
    """List image tags from a single Artifact Registry repo."""
    from google.cloud import (  # noqa: cloud-sdk-direct, imports-inside-functions
        artifactregistry_v1,  # type: ignore[reportAttributeAccessIssue, reportUnknownVariableType]  # no stubs for artifactregistry
    )

    ar_client: object = artifactregistry_v1.ArtifactRegistryAsyncClient()  # type: ignore[reportUnknownVariableType, reportUnknownMemberType]
    repo = f"projects/{project}/locations/asia-northeast1/repositories/{ar_repo}"
    parent = f"{repo}/packages/{service}"
    tags: list[str] = []
    ar_request: object = artifactregistry_v1.ListTagsRequest(parent=parent, page_size=100)  # type: ignore[reportUnknownVariableType, reportUnknownMemberType]
    _list_tags_fn: object = getattr(ar_client, "list_tags", None)  # type: ignore[reportUnknownArgumentType]
    _pager: object = await _list_tags_fn(ar_request) if callable(_list_tags_fn) else None  # type: ignore[reportUnknownVariableType, reportGeneralTypeIssues]
    if _pager is None:
        return tags
    async for tag in _pager:  # type: ignore[reportUnknownVariableType]
        tag_name: str = str(getattr(cast(object, tag), "name", "")).split("/")[-1]
        tags.append(tag_name)
    return tags


async def _list_ar_tags(service: str, project: str) -> list[str]:
    """List image tags from Artifact Registry, checking both legacy and CB repos."""
    ar_repo = _get_ar_repo_name(service)
    all_tags: list[str] = []
    # Check legacy repo (e.g. "instruments") and CB repo ("unified-trading-system")
    repos_to_check = [ar_repo]
    if ar_repo != _CB_REGISTRY_REPO:
        repos_to_check.append(_CB_REGISTRY_REPO)
    for repo in repos_to_check:
        try:
            tags = await _list_ar_tags_from_repo(service, project, repo)
            all_tags.extend(tags)
        except Exception as e:
            logger.debug("AR repo %s/%s: %s", repo, service, e)
    # Deduplicate while preserving order
    seen: set[str] = set()
    deduped: list[str] = []
    for t in all_tags:
        if t not in seen:
            seen.add(t)
            deduped.append(t)
    return deduped


async def _list_ecr_tags(service: str) -> list[str]:
    """List image tags from AWS ECR for a service."""
    import boto3  # noqa: imports-inside-functions — Deferred, AWS SDK boundary

    ecr = boto3.client("ecr", region_name=_ECR_REGION)  # type: ignore[reportUnknownVariableType, reportUnknownMemberType]
    tags: list[str] = []
    try:
        paginator = ecr.get_paginator("list_images")  # type: ignore[reportUnknownMemberType]
        for page in paginator.paginate(repositoryName=service, filter={"tagStatus": "TAGGED"}):  # type: ignore[reportUnknownMemberType]
            page_dict = cast(dict[str, object], page)
            image_ids_raw = page_dict.get("imageIds", [])  # noqa: qg-empty-fallback — AWS ECR pagination
            image_ids = cast(list[object], image_ids_raw) if image_ids_raw else []
            for image_id in image_ids:  # type: ignore[reportUnknownVariableType, reportGeneralTypeIssues]
                image_dict = cast(dict[str, object], image_id)
                tag = image_dict.get("imageTag")
                if tag:
                    tags.append(str(tag))
    except Exception as e:
        logger.debug("ECR %s: %s", service, e)
    return tags


class BranchBuilds(BaseModel):  # CORRECT-LOCAL — API response schema local to this route; not a domain contract
    """All builds for one branch + the latest (highest-version) of them.

    The cockpit Deploy console reads ``latest`` for "launch from <branch> latest" and
    ``builds`` for "rollback to <tag>" (the full descending-version history per branch).
    """

    branch: str = Field(..., description="Branch name (e.g. 'live-defi-rollout', 'main', 'staging')")
    latest: BuildEntry = Field(..., description="Highest-version build on this branch")
    builds: list[BuildEntry] = Field(..., description="All builds on this branch, version-descending")


class BuildsByBranchResponse(BaseModel):  # CORRECT-LOCAL — API response schema local to this route
    """Builds grouped by source branch — the branch→image resolution surface."""

    service: str
    env: Environment
    branches: list[BranchBuilds] = Field(..., description="Per-branch build groups, branches sorted by latest version")


async def _resolve_build_entries(service: str, env: Environment) -> list[BuildEntry]:
    """Resolve all available build entries for a service (mock → AWS ECR → GCP AR).

    The single source of truth for both ``list_builds`` and ``list_builds_by_branch`` —
    returns version-descending ``BuildEntry`` rows (each carries its branch + tag/sha).
    """
    is_mock = CLOUD_MOCK_MODE or CLOUD_PROVIDER == "local"

    if is_mock:
        entries = _mock_builds_from_manifest(service, env)
        if not entries:
            logger.info("Mock mode: no deployed_versions entry for %s/%s", service, env)
        return sorted(entries, key=_sort_key)

    # AWS ECR path
    if CLOUD_PROVIDER == "aws":
        tags = await _list_ecr_tags(service)
        if not tags:
            logger.warning("No ECR tags for %s — falling back to manifest", service)
            return sorted(_mock_builds_from_manifest(service, env), key=_sort_key)
        return sorted([_tag_to_entry(t) for t in tags], key=_sort_key)

    # GCP Artifact Registry path
    project = default_project_id
    if not project:
        raise HTTPException(status_code=400, detail="GCP_PROJECT_ID not configured")  # noqa: qg-gcp-project-id

    tags = await _list_ar_tags(service, project)
    if not tags:
        # AR unavailable or no tags yet — fall back to manifest
        logger.warning("No AR tags found for %s in %s — falling back to manifest", service, env)
        return sorted(_mock_builds_from_manifest(service, env), key=_sort_key)

    return sorted([_tag_to_entry(t) for t in tags], key=_sort_key)


@router.get("/api/builds/{service}", response_model=list[BuildEntry], tags=["Builds"])
async def list_builds(
    service: str,
    env: Environment = Query(..., description="Target environment: dev, staging, or prod"),
) -> list[BuildEntry]:
    """
    List available builds for a service from Artifact Registry.

    Returns builds sorted by version descending. In mock mode (CLOUD_MOCK_MODE=true or
    CLOUD_PROVIDER=local), falls back to deployed_versions from the PM manifest.

    Build display format: "{version} @ {branch}" — human-readable and idempotent
    (same version + branch = same artifact, enforced by immutable AR tags).
    """
    return await _resolve_build_entries(service, env)


@router.get("/api/builds/{service}/by-branch", response_model=BuildsByBranchResponse, tags=["Builds"])
async def list_builds_by_branch(
    service: str,
    env: Environment = Query(..., description="Target environment: dev, staging, or prod"),
) -> BuildsByBranchResponse:
    """Branch→image resolution: the same builds, grouped by source branch.

    Each group carries the **latest** (highest-version) build for "launch from <branch>
    latest" + the **full** version-descending list for "rollback to <tag>". Branch groups
    are themselves ordered by their latest version (so LDR/main/staging tips surface first).
    Reuses the existing AR/ECR tag listing — no new data source.
    """
    entries = await _resolve_build_entries(service, env)
    by_branch: dict[str, list[BuildEntry]] = {}
    for e in entries:
        by_branch.setdefault(e.branch, []).append(e)
    groups = [
        BranchBuilds(branch=branch, latest=builds[0], builds=builds)
        for branch, builds in by_branch.items()
        if builds  # builds[0] is the highest version — entries arrive version-descending
    ]
    # Order branch groups by their latest version (descending), like the flat list.
    groups.sort(key=lambda g: _sort_key(g.latest))
    return BuildsByBranchResponse(service=service, env=env, branches=groups)


@router.post("/api/deployments/{service}/deploy", tags=["Builds"])
async def deploy_build(service: str, deploy_request: DeployRequest) -> dict[str, object]:
    """
    Deploy a specific build tag to any environment.

    No version gate — deployment is always a manual decision. Pre-1.0.0 builds
    are explicitly allowed for development and testing.

    This triggers a Cloud Run revision update with the specified image tag.
    In mock mode, returns a dry-run response without calling Cloud Run.
    """
    image_tag = deploy_request.image_tag
    env = deploy_request.environment

    is_mock = CLOUD_MOCK_MODE or CLOUD_PROVIDER == "local"

    if is_mock:
        logger.info("Mock deploy: %s:%s → %s (dry run)", service, image_tag, env)
        return {
            "status": "accepted",
            "service": service,
            "image_tag": image_tag,
            "environment": env,
            "mock": True,
            "message": f"Mock mode — would deploy {service}:{image_tag} to {env}",
        }

    # AWS ECS/App Runner deploy path
    if CLOUD_PROVIDER == "aws":
        import boto3  # noqa: imports-inside-functions — Deferred, AWS SDK boundary

        try:
            sts = boto3.client("sts", region_name=_ECR_REGION)  # type: ignore[reportUnknownVariableType, reportUnknownMemberType]
            caller_identity = sts.get_caller_identity()  # type: ignore[reportUnknownVariableType, reportUnknownMemberType]
            caller_identity_dict = cast(dict[str, object], caller_identity)
            account_id: str = str(caller_identity_dict["Account"])
        except Exception as e:
            raise HTTPException(status_code=400, detail=f"AWS credentials error: {e}") from e

        ecr_image = f"{account_id}.dkr.ecr.{_ECR_REGION}.amazonaws.com/{service}:{image_tag}"
        logger.info("AWS deploy: %s:%s → %s (image=%s)", service, image_tag, env, ecr_image)
        # TODO: Implement ECS service update or App Runner deployment
        # For now, return the image URI for manual deployment
        return {
            "status": "accepted",
            "service": service,
            "image_tag": image_tag,
            "environment": env,
            "image": ecr_image,
            "cloud": "aws",
            "message": f"AWS deploy queued — {service}:{image_tag} to {env}. ECS update pending.",
        }

    # GCP Cloud Run deploy path
    project = default_project_id
    if not project:
        raise HTTPException(status_code=400, detail="GCP_PROJECT_ID not configured")  # noqa: qg-gcp-project-id

    ar_repo = _get_ar_repo_name(service)
    ar_image = f"{_AR_HOST}/{project}/{ar_repo}/{service}:{image_tag}"
    region = "asia-northeast1"
    # The Cloud Run SERVICE we actually deploy to defaults to `service` (image name ==
    # Cloud Run name, the common case) but can diverge — see DeployRequest.cloud_run_service_name.
    target_service = deploy_request.cloud_run_service_name or service

    try:
        result = subprocess.run(
            [
                "gcloud",
                "run",
                "deploy",
                target_service,
                f"--image={ar_image}",
                f"--region={region}",
                f"--project={project}",
                "--quiet",
            ],
            capture_output=True,
            text=True,
            timeout=120,
            check=False,
        )
        if result.returncode != 0:
            logger.error("gcloud run deploy failed: %s", result.stderr)
            raise HTTPException(
                status_code=502,
                detail=f"Cloud Run deploy failed: {result.stderr[:500]}",
            )
        logger.info("Deployed %s:%s to %s/%s (cloud_run_service=%s)", service, image_tag, env, project, target_service)
        return {
            "status": "deploying",
            "service": service,
            "cloud_run_service_name": target_service,
            "image_tag": image_tag,
            "environment": env,
            "image": ar_image,
        }
    except subprocess.TimeoutExpired as e:
        raise HTTPException(status_code=504, detail="gcloud deploy timed out") from e
    except OSError as e:
        raise HTTPException(status_code=500, detail=f"Internal error: {e}") from e

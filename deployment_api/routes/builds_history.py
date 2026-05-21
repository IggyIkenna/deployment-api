"""GET /api/builds/history — tarball + Docker-image lineage per service.

Returns a combined view of:
  - Code tarballs in GCS at gs://{tarball_bucket}/code/{service}-code.tar.gz
  - Docker image tags in Artifact Registry for each service

In mock mode (CLOUD_MOCK_MODE=true) returns synthetic entries derived from
the workspace manifest deployed_versions — no GCS or AR calls.

Tarball bucket: deployment-scripts-{gcp_project_id}  (same as create-code-tarballs.sh).
AR repo: asia-northeast1-docker.pkg.dev/{project}/unified-trading-system/{service}.
"""

from __future__ import annotations

import json
import logging
from datetime import datetime
from os import environ as _process_env
from pathlib import Path
from typing import cast

from fastapi import APIRouter, Query
from pydantic import BaseModel, Field
from unified_trading_library import log_event

from deployment_api.deployment_api_config import DeploymentApiConfig

router = APIRouter()
logger = logging.getLogger(__name__)

_cfg = DeploymentApiConfig()

_AR_HOST = "asia-northeast1-docker.pkg.dev"
_CB_REGISTRY_REPO = "unified-trading-system"
_TARBALL_BUCKET_PREFIX = "deployment-scripts-"
_TARBALL_PATH_PREFIX = "code/"
_TARBALL_SUFFIX = "-code.tar.gz"

# Services shipped as tarballs (canonical list from create-code-tarballs.sh).
_KNOWN_TARBALL_SERVICES = [
    "unified-api-contracts",
    "unified-trading-library",
    "market-tick-data-service",
    "market-data-processing-service",
    "instruments-service",
    "features-service",
    "features-onchain-service",
    "strategy-service",
    "execution-service",
    # ml-training-service + ml-inference-service consolidated into ml-service (2026-05-21)
    "ml-service",
    "alerting-service",
    "deployment-service",
]


class TarballInfo(BaseModel):
    """GCS tarball metadata (no full gs:// URI — callers compose from bucket + object_path)."""

    bucket: str = Field(..., description="GCS bucket name (without gs:// prefix).")
    object_path: str = Field(..., description="Object path within bucket, e.g. code/strategy-service-code.tar.gz.")
    updated_at: datetime | None = Field(default=None, description="Last-modified timestamp from GCS object metadata.")
    size_bytes: int | None = Field(default=None, description="Object size in bytes from GCS metadata.")
    sha256_hex: str | None = Field(default=None, description="SHA-256 from sibling manifest.json if present.")


class BuildLineageEntry(BaseModel):
    """Combined tarball + Docker-image lineage for a single service."""

    service: str
    tarball: TarballInfo | None = Field(
        default=None, description="Tarball info; None when no tarball exists for this service."
    )
    image_tags: list[str] = Field(
        default_factory=list, description="Available Docker image tags from Artifact Registry."
    )
    latest_image_tag: str | None = Field(default=None, description="Most recent semver tag (highest version).")


class BuildsHistoryResponse(BaseModel):
    """Response for GET /api/builds/history."""

    entries: list[BuildLineageEntry]
    total: int
    project_id: str
    dry_run: bool


def _tarball_bucket(project_id: str) -> str:
    return f"{_TARBALL_BUCKET_PREFIX}{project_id}"


def _tarball_object_path(service: str) -> str:
    return f"{_TARBALL_PATH_PREFIX}{service}{_TARBALL_SUFFIX}"


def _workspace_root() -> Path:
    ws = _process_env.get("WORKSPACE_ROOT") or str(Path(__file__).resolve().parents[4])
    return Path(ws)


def _mock_entries(project_id: str, services: list[str], limit: int) -> list[BuildLineageEntry]:
    """Mock entries from workspace manifest deployed_versions."""
    manifest_path = _workspace_root() / "unified-trading-pm" / "workspace-manifest.json"
    deployed: dict[str, str] = {}
    if manifest_path.is_file():
        try:
            raw = cast(dict[str, object], json.loads(manifest_path.read_text()))
            deployed_raw: object = raw.get("deployed_versions")  # noqa: qg-empty-fallback
            if isinstance(deployed_raw, dict):
                prod: object = cast(dict[str, object], deployed_raw).get("production")  # noqa: qg-empty-fallback
                if isinstance(prod, dict):
                    deployed = cast(dict[str, str], prod)
        except (json.JSONDecodeError, OSError):
            logger.warning("could not read workspace manifest for mock builds history")

    bucket = _tarball_bucket(project_id)
    entries: list[BuildLineageEntry] = []
    for svc in services[:limit]:
        tag = deployed.get(svc)  # noqa: qg-empty-fallback
        entries.append(
            BuildLineageEntry(
                service=svc,
                tarball=TarballInfo(
                    bucket=bucket,
                    object_path=_tarball_object_path(svc),
                    updated_at=None,
                    size_bytes=None,
                    sha256_hex=None,
                ),
                image_tags=[tag] if tag else [],
                latest_image_tag=tag,
            )
        )
    return entries


async def _live_entries(project_id: str, services: list[str], limit: int) -> list[BuildLineageEntry]:
    """Fetch tarball metadata from GCS + image tags from AR per service."""
    from google.cloud import (  # noqa: cloud-sdk-direct
        artifactregistry_v1,  # pyright: ignore[reportMissingTypeStubs]
    )
    from google.cloud import storage as gcs_storage  # noqa: cloud-sdk-direct

    bucket_name = _tarball_bucket(project_id)
    try:
        gcs_client: object = gcs_storage.Client(project=project_id)  # pyright: ignore[reportUnknownMemberType]
        bucket_obj: object = gcs_client.bucket(bucket_name)  # pyright: ignore[reportUnknownMemberType, reportAttributeAccessIssue]
    except Exception:
        logger.exception("could not connect to GCS bucket %s", bucket_name)
        bucket_obj = None

    ar_client: object = artifactregistry_v1.ArtifactRegistryAsyncClient()  # pyright: ignore[reportUnknownMemberType]
    entries: list[BuildLineageEntry] = []

    for svc in services[:limit]:
        # --- tarball ---
        tarball_info: TarballInfo | None = None
        if bucket_obj is not None:
            obj_path = _tarball_object_path(svc)
            try:
                blob: object = bucket_obj.blob(obj_path)  # pyright: ignore[reportAttributeAccessIssue, reportUnknownMemberType]
                blob.reload()  # pyright: ignore[reportAttributeAccessIssue]
                tarball_info = TarballInfo(
                    bucket=bucket_name,
                    object_path=obj_path,
                    updated_at=getattr(blob, "updated", None),
                    size_bytes=getattr(blob, "size", None),
                    sha256_hex=None,
                )
            except Exception:
                tarball_info = TarballInfo(
                    bucket=bucket_name,
                    object_path=obj_path,
                    updated_at=None,
                    size_bytes=None,
                    sha256_hex=None,
                )

        # --- AR image tags ---
        image_tags: list[str] = []
        try:
            repo = f"projects/{project_id}/locations/asia-northeast1/repositories/{_CB_REGISTRY_REPO}"
            parent = f"{repo}/packages/{svc}"
            pager: object = await ar_client.list_tags(parent=parent)  # pyright: ignore[reportAttributeAccessIssue, reportUnknownMemberType]
            async for tag in pager:  # pyright: ignore[reportUnknownMemberType, reportUnknownVariableType]
                tag_name: str = str(getattr(tag, "name", "")).rsplit("/", 1)[-1]
                if tag_name:
                    image_tags.append(tag_name)
        except Exception:
            pass

        latest = image_tags[0] if image_tags else None
        entries.append(
            BuildLineageEntry(
                service=svc,
                tarball=tarball_info,
                image_tags=image_tags,
                latest_image_tag=latest,
            )
        )

    return entries


@router.get("/history", response_model=BuildsHistoryResponse)
async def list_builds_history(
    service: str | None = Query(default=None, description="Filter by service name. Omit to return all known services."),
    limit: int = Query(default=10, ge=1, le=100, description="Maximum number of service entries to return."),
) -> BuildsHistoryResponse:
    """Return tarball + Docker-image lineage for all known services (or a specific one)."""
    project_id = _cfg.gcp_project_id
    effective_dry_run = _cfg.is_mock_mode()

    services = [service] if service else _KNOWN_TARBALL_SERVICES

    log_event(
        "ADAPTER_FETCH_STARTED",
        service="deployment-api",
        details={
            "endpoint": "GET /api/builds/history",
            "service_filter": service,
            "limit": limit,
            "dry_run": effective_dry_run,
        },
    )

    if effective_dry_run:
        entries = _mock_entries(project_id, services, limit)
    else:
        entries = await _live_entries(project_id, services, limit)

    log_event(
        "ADAPTER_FETCH_COMPLETED",
        service="deployment-api",
        details={
            "endpoint": "GET /api/builds/history",
            "total": len(entries),
            "dry_run": effective_dry_run,
        },
    )

    return BuildsHistoryResponse(
        entries=entries,
        total=len(entries),
        project_id=project_id,
        dry_run=effective_dry_run,
    )

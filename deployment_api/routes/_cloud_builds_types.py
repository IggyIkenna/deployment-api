"""
TypedDict definitions, Pydantic models, constants, and client helpers for cloud_builds.

Covers:
- TypedDict response/request types
- Pydantic API models
- Service / library / infrastructure registry constants
- GCP Cloud Build client factory helpers
"""

from __future__ import annotations

import logging
from typing import TYPE_CHECKING, Required, TypedDict, cast

from fastapi import HTTPException
from pydantic import BaseModel, Field
from unified_trading_library import get_cloud_build_client

from deployment_api.settings import (
    CLOUD_PROVIDER,
)
from deployment_api.settings import gcp_project_id as default_project_id

if TYPE_CHECKING:
    from google.cloud.devtools import cloudbuild_v1

logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Deferred GCP imports
# ---------------------------------------------------------------------------


def _cloudbuild_v1():
    """Deferred cloudbuild_v1 import — used for request/response type construction only."""
    from google.cloud.devtools import cloudbuild_v1  # Deferred — deployment Cloud Build boundary

    return cloudbuild_v1


def _build_op_meta_cls():
    """Deferred BuildOperationMetadata import — deployment Cloud Build boundary."""
    from google.cloud.devtools.cloudbuild_v1 import BuildOperationMetadata  # Deferred

    return BuildOperationMetadata


# ---------------------------------------------------------------------------
# Cloud Build client helpers
# ---------------------------------------------------------------------------


def _get_gcp_build_client() -> cloudbuild_v1.CloudBuildClient:
    """Return the underlying GCP CloudBuildClient via UCI factory."""
    uci_client = get_cloud_build_client(project_id=default_project_id)
    if hasattr(uci_client, "_client"):
        _native: object = uci_client._client()  # pyright: ignore[reportUnknownMemberType, reportPrivateUsage]  # UCI internal accessor
        return cast("cloudbuild_v1.CloudBuildClient", _native)
    # Fallback: direct construction (should not be reached in production)
    return _cloudbuild_v1().CloudBuildClient()


def get_gcp_build_client() -> cloudbuild_v1.CloudBuildClient:
    """Public alias for _get_gcp_build_client — for use by other modules in this package."""
    return _get_gcp_build_client()


def _ensure_gcp() -> None:
    """Raise if CLOUD_PROVIDER is aws — CodeBuild integration placeholder."""
    if CLOUD_PROVIDER == "aws":
        raise HTTPException(
            status_code=501,
            detail="CodeBuild integration placeholder — AWS not yet implemented. See ISS-XXX.",
        )


# ---------------------------------------------------------------------------
# Service / library / infrastructure registry
# ---------------------------------------------------------------------------

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


# ---------------------------------------------------------------------------
# TypedDicts
# ---------------------------------------------------------------------------


class BuildInfoDict(TypedDict):  # CORRECT-LOCAL
    """Serialized Cloud Build information."""

    build_id: str
    status: str
    create_time: str | None
    finish_time: str | None
    duration_seconds: float | None
    commit_sha: str | None
    branch: str | None
    log_url: str | None


class TriggerDict(TypedDict, total=False):  # CORRECT-LOCAL
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


class TriggersResponseDict(TypedDict):  # CORRECT-LOCAL
    """Response from list_triggers endpoint."""

    triggers: list[TriggerDict]
    total: int
    project: str
    region: str


class BuildHistoryResponseDict(TypedDict):  # CORRECT-LOCAL
    """Response from get_build_history endpoint."""

    service: str
    trigger_name: str
    builds: list[BuildInfoDict]
    total: int


class QualityGatesStatusDict(TypedDict, total=False):  # CORRECT-LOCAL
    """Quality gates status for a library."""

    status: str
    is_passing: bool
    last_build_time: str | None
    commit_sha: str | None
    branch: str | None


class LibraryStatusDict(TypedDict, total=False):  # CORRECT-LOCAL
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


class DependencyIssueDict(TypedDict, total=False):  # CORRECT-LOCAL
    """A dependency issue found during check."""

    library: str
    issue: str
    status: str
    last_build_time: str | None
    affected_services: list[str]
    pyproject_version: str
    installed_version: str


class DependencyCheckResponseDict(TypedDict):  # CORRECT-LOCAL
    """Response from check_dependencies endpoint."""

    has_issues: bool
    issue_count: int
    issues: list[DependencyIssueDict]
    libraries: list[LibraryStatusDict]


class RecentBuildDict(TypedDict):  # CORRECT-LOCAL
    """A recently found build (from trigger)."""

    build_id: str
    log_url: str | None
    status: str


class TriggerRunResultDict(TypedDict, total=False):  # CORRECT-LOCAL
    """Result from running a build trigger."""

    success: bool
    build_id: str | None
    log_url: str | None
    trigger_id: str | None
    trigger_time: object  # datetime


# ---------------------------------------------------------------------------
# Pydantic API models
# ---------------------------------------------------------------------------


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

"""
Deployments API Routes

Endpoints for managing deployments - list, status, deploy, cancel.
Pure route handlers that delegate business logic to service modules.
"""

import logging
import uuid
from datetime import datetime, timedelta
from typing import cast

from fastapi import APIRouter, BackgroundTasks, HTTPException, Query, Request
from pydantic import BaseModel, Field
from unified_api_contracts.internal.domain.deployment_service import RuntimeProfile

# Import service modules for business logic
from deployment_api.app_config import get_config_dir
from deployment_api.deployment_api_config import DeploymentApiConfig
from deployment_api.messages import (
    CLOUD_SERVICE_UNAVAILABLE,
    INTERNAL_ERROR,
    INTERNAL_SERVER_ERROR,
    INVALID_CONFIG,
    INVALID_PARAMETERS,
    SERVICE_CONFIG_NOT_FOUND,
    SERVICE_TEMPORARILY_UNAVAILABLE,
)
from deployment_api.services import DataStatusService
from deployment_api.services.deployment_manager import DeploymentManager
from deployment_api.services.deployment_state import DeploymentStateManager

router = APIRouter()
logger = logging.getLogger(__name__)

# Initialize service managers and config
deployment_manager = DeploymentManager()
state_manager = DeploymentStateManager()
data_status_service = DataStatusService()
_cfg = DeploymentApiConfig()


# Pydantic models for request/response
class DeployRequest(BaseModel):  # CORRECT-LOCAL: FastAPI API contract model
    """Request body for creating a deployment.

    The trading venue axis filter is ``asset_group`` (JSON).
    :attr:`filters` emits the ``"asset_group"`` key for deployment-service ``extra_filters``
    (aligned with sharding dimension ``name: asset_group``). GCS path templates may still
    use a ``category=`` path segment literal; that is layout, not this field name.
    """

    service: str = Field(..., description="Service name to deploy")
    compute: str = Field("cloud_run", description="Compute mode: cloud_run or vm")
    mode: str = Field("batch", description="Deployment mode: 'batch' or 'live'")
    cloud_provider: str = Field("gcp", description="Cloud provider: gcp, aws, or local")
    operational_mode: str = Field("", description="Service-specific operational mode (e.g. train_phase1, execute)")

    # Date range for data processing
    start_date: str | None = Field(None, description="Start date (YYYY-MM-DD)")
    end_date: str | None = Field(None, description="End date (YYYY-MM-DD)")

    # Deployment configuration
    max_shards: int | None = Field(10000, description="Maximum allowed shards")
    max_concurrent: int | None = Field(None, description="Max concurrent jobs")
    region: str | None = Field(None, description="GCP region for deployment")
    vm_zone: str | None = Field(None, description="VM zone (for VM compute)")
    tag: str | None = Field("latest", description="Docker image tag")

    # Filtering and configuration options
    cloud_config_path: str | None = Field(None, description="Cloud config path")
    ignore_start_dates: bool = Field(False, description="Ignore venue start date filtering")
    deploy_missing: bool = Field(False, description="Only deploy missing data")
    skip_dimensions: list[str] | None = Field(None, description="Dimensions to skip")
    date_granularity: str | None = Field(None, description="Date granularity override")
    max_workers: int | None = Field(None, description="Max workers per job")

    # Filter parameters
    asset_group: str | list[str] | None = Field(
        default=None,
        description="Asset group filter (e.g. CEFI, DEFI)",
    )
    venue: str | None = Field(None, description="Venue filter")
    feature_group: str | None = Field(None, description="Feature group filter")
    data_type: str | None = Field(None, description="Data type filter")

    # Exclude dates: mapping of asset group -> list of date strings to skip
    # Used by "Deploy Missing" to exclude dates that already have complete data
    exclude_dates: dict[str, list[str]] | None = Field(None, description="Dates to exclude per asset group")

    # Logging configuration
    log_level: str = Field("INFO", description="Log level for deployment jobs")

    # v7 additions: runtime profile + client scoping (used by deployment-service to
    # fan out env vars and resolve per-service client isolation)
    runtime_profile: RuntimeProfile | None = Field(
        None,
        description=(
            "Deployment runtime profile (backtest/paper/mock-live/staging/prod). "
            "Collapses CLOUD_MOCK_MODE, MOCK_STATE_MODE, DISABLE_AUTH, VITE_MOCK_API, "
            "VITE_SKIP_AUTH into one axis. See runtime-topology.yaml `runtime_profiles`."
        ),
    )
    client_id: str | None = Field(
        None,
        description=(
            "Client identifier for per-client isolation. When set, deployment-service "
            "loads the matching ClientSubscription and materialises isolated services "
            "per the subscription + SLA tier policy."
        ),
    )

    @property
    def filters(self) -> dict[str, object]:
        """Extract filter parameters for shard extra_filters."""
        return {
            k: v
            for k, v in {
                "asset_group": self.asset_group,
                "venue": self.venue,
                "feature_group": self.feature_group,
                "data_type": self.data_type,
            }.items()
            if v is not None
        }


class ShardInfo(BaseModel):  # CORRECT-LOCAL: FastAPI API contract model
    """Information about a single shard."""

    shard_id: str
    shard_index: int
    dimensions: dict[str, object]
    cli_args: list[str]


class ShardPreview(BaseModel):  # CORRECT-LOCAL: FastAPI API contract model
    """Preview of shards for a deployment."""

    total_shards: int
    sample_shards: list[ShardInfo]
    cli_command: str


class DeploymentResult(BaseModel):  # CORRECT-LOCAL: FastAPI API contract model
    """Result of creating a deployment."""

    deployment_id: str
    status: str
    total_shards: int
    cli_command: str


class DeploymentSummary(BaseModel):  # CORRECT-LOCAL: FastAPI API contract model
    """Summary information about a deployment."""

    deployment_id: str
    service: str
    status: str
    total_shards: int
    created_at: str


class UpdateDeploymentRequest(BaseModel):  # CORRECT-LOCAL: FastAPI API contract model
    """Request to update deployment properties."""

    tag: str | None = Field(None, description="New Docker image tag")


class BulkDeleteRequest(BaseModel):  # CORRECT-LOCAL: FastAPI API contract model
    """Request to delete multiple deployments."""

    deployment_ids: list[str] = Field(..., description="List of deployment IDs to delete")


class DeployMissingRequest(BaseModel):  # CORRECT-LOCAL: FastAPI API contract model
    """Request to detect and deploy missing data shards (JSON field ``asset_group``)."""

    service: str = Field(..., description="Service name")
    start_date: str = Field(..., description="Start date (YYYY-MM-DD)")
    end_date: str = Field(..., description="End date (YYYY-MM-DD)")
    asset_group: str | None = Field(None, description="Asset group filter (e.g. CEFI, DEFI)")
    venue: str | None = Field(None, description="Venue filter")
    region: str | None = Field(None, description="GCP region")
    force: bool = Field(False, description="Force redeploy even if data exists")
    dry_run: bool = Field(False, description="Preview only, don't actually deploy")
    compute: str = Field("cloud_run", description="Compute mode: cloud_run or vm")
    tag: str | None = Field("latest", description="Docker image tag")
    mode: str = Field("batch", description="batch or live")
    date_granularity: str | None = Field(None, description="Date granularity override")
    deploy_missing_only: bool = Field(True, description="Use backend missing-shard calculation")
    log_level: str = Field("INFO", description="Log level")


# Route handlers (pure route logic, business logic delegated to services)


# ---------------------------------------------------------------------------
# Route registration — package facade (split 2026-06-11 from the 968-line
# ``routes/deployments.py`` per ``codex_violations_ratchet_to_five_2026_06_10.md``
# Phase-1 P2 — pure code motion). Submodules attach their endpoints to the
# shared ``router`` above at import time; import order replays the original
# inline definition order. The module-level singletons above are the test
# patch surface — submodules resolve them through this module at call time.
# ---------------------------------------------------------------------------
# isort: off
from deployment_api.routes.deployments import _crud
from deployment_api.routes.deployments import _lifecycle

# isort: on

# ---------------------------------------------------------------------------
# Public-surface re-exports — every symbol previously importable from
# ``deployment_api.routes.deployments`` resolves here unchanged.
# ---------------------------------------------------------------------------
from deployment_api.routes.deployments._crud import (
    _submit_missing_deployment,  # pyright: ignore[reportPrivateUsage]
    create_deployment,
    deploy_missing_shards,
    get_deployment_status,
    list_deployments,
    quota_info,
    verify_deployment_completion,
)
from deployment_api.routes.deployments._lifecycle import (
    RollbackRequest,
    bulk_delete_deployments,
    cancel_deployment,
    delete_deployment,
    get_deployment_events,
    get_deployment_logs,
    get_deployment_report,
    get_deployment_vm_events,
    refresh_deployment_status,
    resume_deployment,
    retry_failed_shards,
    update_deployment,
)

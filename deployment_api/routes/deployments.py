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


@router.get("/deployments")
async def list_deployments(
    limit: int = Query(50, ge=1, le=200, description="Max deployments to return"),
    offset: int = Query(0, ge=0, description="Offset for pagination"),
    status: str | None = Query(None, description="Filter by status"),
    service: str | None = Query(None, description="Filter by service"),
    asset_group: str | None = Query(None, description="Filter by trading axis (CEFI, DEFI, SPORTS, …)"),
    category: str | None = Query(
        None,
        description="Deprecated — use asset_group",
        include_in_schema=False,
    ),
) -> dict[str, object]:
    """List deployments with optional filtering and pagination."""
    ag_filter = asset_group or category
    if _cfg.is_mock_mode():
        from deployment_api.mock_state import get_store

        items = get_store().list("deployments")
        if status:
            items = [d for d in items if d.get("status") == status]
        if service:
            items = [d for d in items if d.get("service") == service]
        if ag_filter:
            want = str(ag_filter).strip().upper()
            from deployment_api.utils.trading_axis import trading_axis_from_deployment_state

            def _matches(d: object) -> bool:
                if not isinstance(d, dict):
                    return False
                got = trading_axis_from_deployment_state(cast(dict[str, object], d))
                return bool(got and got == want)

            items = [d for d in items if _matches(d)]
        page = items[offset : offset + limit]
        return {"deployments": page, "total": len(items), "limit": limit, "offset": offset}
    try:
        result = state_manager.list_deployments(
            limit=limit,
            offset=offset,
            status_filter=status,
            service_filter=service,
            asset_group=ag_filter,
        )
        return result
    except ValueError as e:
        logger.error("Invalid parameters for listing deployments: %s", e)
        raise HTTPException(status_code=400, detail=INVALID_PARAMETERS) from e
    except ConnectionError as e:
        logger.error("Database connection failed while listing deployments: %s", e)
        raise HTTPException(status_code=503, detail=SERVICE_TEMPORARILY_UNAVAILABLE) from e
    except (OSError, RuntimeError) as e:
        logger.exception("Failed to list deployments: %s", e)
        raise HTTPException(status_code=500, detail=INTERNAL_SERVER_ERROR) from e


@router.get("/deployments/{deployment_id}")
async def get_deployment_status(
    deployment_id: str,
    detailed: bool = Query(True, description="Include detailed shard information"),
) -> dict[str, object]:
    """Get detailed status for a specific deployment."""
    if _cfg.is_mock_mode():
        from deployment_api.mock_state import get_store

        item = get_store().get("deployments", deployment_id)
        if item is None:
            raise HTTPException(status_code=404, detail=f"Deployment '{deployment_id}' not found (mock)")
        return item
    try:
        result = state_manager.get_deployment_status(deployment_id, detailed=detailed)
        return result
    except ValueError as e:
        logger.exception("Deployment not found: %s", deployment_id)
        raise HTTPException(status_code=404, detail=INTERNAL_ERROR) from e
    except ConnectionError as e:
        logger.error("Cloud provider connection failed for deployment %s: %s", deployment_id, e)
        raise HTTPException(status_code=503, detail=CLOUD_SERVICE_UNAVAILABLE) from e
    except (OSError, RuntimeError) as e:
        logger.exception("Failed to get deployment status for %s: %s", deployment_id, e)
        raise HTTPException(status_code=500, detail=INTERNAL_SERVER_ERROR) from e


@router.get("/deployments/{deployment_id}/verify")
async def verify_deployment_completion(
    deployment_id: str,
    force: bool = Query(False, description="Force refresh of verification"),
) -> dict[str, object]:
    """Verify deployment completion and data integrity."""
    if _cfg.is_mock_mode():
        return {
            "deployment_id": deployment_id,
            "verified": True,
            "status": "completed",
            "mock": True,
        }
    try:
        result = state_manager.verify_deployment_completion(deployment_id, force_refresh=force)
        return result
    except ValueError as e:
        logger.exception("Deployment not found for verification: %s", deployment_id)
        raise HTTPException(status_code=404, detail=INTERNAL_ERROR) from e
    except ConnectionError as e:
        logger.error("Cloud provider connection failed during verification for %s: %s", deployment_id, e)
        raise HTTPException(status_code=503, detail=CLOUD_SERVICE_UNAVAILABLE) from e
    except (OSError, RuntimeError) as e:
        logger.exception("Failed to verify deployment %s: %s", deployment_id, e)
        raise HTTPException(status_code=500, detail=INTERNAL_SERVER_ERROR) from e


@router.post("/deployments/quota-info")
async def quota_info(deploy_request: DeployRequest, request: Request) -> dict[str, object]:
    """Get quota requirements and recommendations for a deployment."""
    try:
        result = await deployment_manager.calculate_quota_requirements(
            deploy_request,
            config_dir=deploy_request.cloud_config_path or str(get_config_dir()),
        )
        return result
    except ValueError as e:
        logger.exception("Invalid request for quota calculation")
        raise HTTPException(status_code=400, detail=INTERNAL_ERROR) from e
    except FileNotFoundError as e:
        logger.error("Configuration file not found for quota calculation: %s", e)
        raise HTTPException(status_code=400, detail=SERVICE_CONFIG_NOT_FOUND) from e
    except KeyError as e:
        logger.error("Missing configuration key for quota calculation: %s", e)
        raise HTTPException(status_code=400, detail=INVALID_CONFIG) from e
    except (OSError, RuntimeError) as e:
        logger.exception("Failed to calculate quota info: %s", e)
        raise HTTPException(status_code=500, detail=INTERNAL_ERROR) from e


@router.post("/deployments")
async def create_deployment(
    deploy_request: DeployRequest, request: Request, background_tasks: BackgroundTasks
) -> DeploymentResult:
    """Create a new deployment."""
    if _cfg.is_mock_mode():
        from deployment_api.mock_state import get_store

        dep_id = f"dep-mock-{uuid.uuid4().hex[:8]}"
        new_deployment: dict[str, object] = {
            "deployment_id": dep_id,
            "id": dep_id,
            "service": deploy_request.service,
            "status": "pending",
            "total_shards": deploy_request.max_shards or 1,
            "compute": deploy_request.compute,
            "mode": deploy_request.mode,
            "cloud_provider": deploy_request.cloud_provider,
            "created_at": "2026-03-14T12:00:00Z",
        }
        get_store().create("deployments", new_deployment)
        return DeploymentResult(
            deployment_id=dep_id,
            status="pending",
            total_shards=deploy_request.max_shards or 1,
            cli_command=f"mock deploy --service {deploy_request.service}",
        )
    try:
        # Create background task for deployment execution
        def background_task_runner(
            req: DeployRequest,
            config_dir: str,
            shard_list: list[dict[str, object]],
            cli_cmd: str,
            dep_id: str,
        ) -> None:
            background_tasks.add_task(
                deployment_manager.run_deployment_background,
                req,
                config_dir,
                shard_list,
                cli_cmd,
                dep_id,
            )

        result = await deployment_manager.create_deployment(
            deploy_request,
            config_dir=deploy_request.cloud_config_path or str(get_config_dir()),
            background_task_func=background_task_runner,
        )

        return DeploymentResult(
            deployment_id=cast(str, result["deployment_id"]),
            status=cast(str, result["status"]),
            total_shards=cast(int, result["total_shards"]),
            cli_command=cast(str, result["cli_command"]),
        )
    except ValueError as e:
        logger.exception("Invalid request for deployment creation")
        raise HTTPException(status_code=400, detail=INTERNAL_ERROR) from e
    except FileNotFoundError as e:
        logger.error("Configuration file not found for deployment creation: %s", e)
        raise HTTPException(status_code=400, detail=SERVICE_CONFIG_NOT_FOUND) from e
    except ConnectionError as e:
        logger.error("Cloud provider connection failed during deployment creation: %s", e)
        raise HTTPException(status_code=503, detail=CLOUD_SERVICE_UNAVAILABLE) from e
    except (OSError, RuntimeError) as e:
        logger.exception("Failed to create deployment: %s", e)
        raise HTTPException(status_code=500, detail=INTERNAL_ERROR) from e


async def _submit_missing_deployment(
    req: DeployMissingRequest,
    missing_analysis: dict[str, object],
    total_missing: int,
    background_tasks: BackgroundTasks,
) -> dict[str, object]:
    """Build exclude_dates from complete dates and submit the deployment."""
    missing_by_date = cast(dict[str, int], missing_analysis.get("missing_by_date", {}))  # noqa: qg-empty-fallback — analysis result default
    start_dt = datetime.strptime(req.start_date, "%Y-%m-%d")
    end_dt = datetime.strptime(req.end_date, "%Y-%m-%d")
    all_dates: list[str] = []
    current = start_dt
    while current <= end_dt:
        all_dates.append(current.strftime("%Y-%m-%d"))
        current += timedelta(days=1)

    complete_dates = [d for d in all_dates if d not in missing_by_date]
    exclude_key = req.asset_group or "all"
    exclude_dates: dict[str, list[str]] = {exclude_key: complete_dates}

    deploy_request = DeployRequest(
        service=req.service,
        compute=req.compute,
        mode=req.mode,
        start_date=req.start_date,
        end_date=req.end_date,
        deploy_missing=True,
        exclude_dates=exclude_dates,
        region=req.region,
        tag=req.tag,
        asset_group=req.asset_group,
        venue=req.venue,
        date_granularity=req.date_granularity,
        log_level=req.log_level,
    )

    def background_task_runner(
        r: DeployRequest,
        config_dir: str,
        shard_list: list[dict[str, object]],
        cli_cmd: str,
        dep_id: str,
    ) -> None:
        background_tasks.add_task(
            deployment_manager.run_deployment_background,
            r,
            config_dir,
            shard_list,
            cli_cmd,
            dep_id,
        )

    result = await deployment_manager.create_deployment(
        deploy_request,
        config_dir=deploy_request.cloud_config_path or str(get_config_dir()),
        background_task_func=background_task_runner,
    )

    return {
        "missing_analysis": missing_analysis,
        "deployment": {
            "deployment_id": cast(str, result["deployment_id"]),
            "status": cast(str, result["status"]),
            "total_shards": cast(int, result["total_shards"]),
            "cli_command": cast(str, result["cli_command"]),
        },
        "dry_run": False,
        "message": f"Found {total_missing} missing shard(s) — deployment created",
    }


@router.post("/deployments/deploy-missing")
async def deploy_missing_shards(
    req: DeployMissingRequest,
    request: Request,
    background_tasks: BackgroundTasks,
) -> dict[str, object]:
    """Detect missing data shards and deploy jobs to fill gaps.

    Combines missing-shard detection with deployment creation in a single call.
    When dry_run=True, returns only the missing analysis without deploying.
    """
    if _cfg.is_mock_mode():
        dep_id = f"dep-mock-{uuid.uuid4().hex[:8]}"
        return {
            "missing_analysis": {
                "service": req.service,
                "date_range": {"start": req.start_date, "end": req.end_date},
                "total_missing": 3,
                "missing_by_date": {
                    req.start_date: 2,
                    req.end_date: 1,
                },
                "missing_by_venue": {"binance": 2, "okx": 1},
                "missing_by_asset_group": {"cefi": 3},
                "summary": {
                    "total_days_checked": 7,
                    "days_with_missing": 2,
                    "venues_with_missing": 2,
                    "asset_groups_with_missing": 1,
                    "completion_rate": 0.85,
                },
            },
            "deployment": {
                "deployment_id": dep_id,
                "status": "pending",
                "total_shards": 3,
                "cli_command": f"mock deploy --service {req.service} --deploy-missing",
            },
            "dry_run": req.dry_run,
            "mock": True,
        }
    try:
        # Step 1: Calculate missing shards
        asset_groups = [req.asset_group] if req.asset_group else None
        venues = [req.venue] if req.venue else None

        missing_analysis = await data_status_service.calculate_missing_shards(
            service=req.service,
            start_date=req.start_date,
            end_date=req.end_date,
            asset_groups=asset_groups,
            venues=venues,
            mode=req.mode,
        )

        if "error" in missing_analysis:
            raise HTTPException(status_code=500, detail=cast(str, missing_analysis["error"]))

        total_missing = cast(int, missing_analysis.get("total_missing", 0))

        # Step 2: If dry_run, return analysis only
        if req.dry_run:
            return {
                "missing_analysis": missing_analysis,
                "deployment": None,
                "dry_run": True,
                "message": (f"Found {total_missing} missing shard(s) — dry run, no deployment created"),
            }

        # Step 3: If no missing shards and not forcing, return early
        if total_missing == 0 and not req.force:
            return {
                "missing_analysis": missing_analysis,
                "deployment": None,
                "dry_run": False,
                "message": "No missing shards detected — nothing to deploy",
            }

        # Step 4-6: Build and submit the deployment
        deploy_result = await _submit_missing_deployment(req, missing_analysis, total_missing, background_tasks)
        return deploy_result
    except ValueError as e:
        logger.exception("Invalid request for deploy-missing")
        raise HTTPException(status_code=400, detail=INTERNAL_ERROR) from e
    except FileNotFoundError as e:
        logger.error("Configuration file not found for deploy-missing: %s", e)
        raise HTTPException(status_code=400, detail=SERVICE_CONFIG_NOT_FOUND) from e
    except ConnectionError as e:
        logger.error("Cloud provider connection failed during deploy-missing: %s", e)
        raise HTTPException(status_code=503, detail=CLOUD_SERVICE_UNAVAILABLE) from e
    except (OSError, RuntimeError) as e:
        logger.exception("Failed to deploy missing shards: %s", e)
        raise HTTPException(status_code=500, detail=INTERNAL_ERROR) from e


@router.post("/deployments/{deployment_id}/cancel")
async def cancel_deployment(deployment_id: str, request: Request) -> dict[str, str]:
    """Cancel a running deployment."""
    if _cfg.is_mock_mode():
        from deployment_api.mock_state import get_store

        updated = get_store().update("deployments", deployment_id, {"status": "cancelled"})
        if updated is None:
            raise HTTPException(status_code=404, detail=f"Deployment '{deployment_id}' not found (mock)")
        return {"deployment_id": deployment_id, "status": "cancelled"}
    try:
        result = state_manager.cancel_deployment(deployment_id)
        return result
    except ValueError as e:
        logger.exception("Deployment not found for cancellation: %s", deployment_id)
        raise HTTPException(status_code=404, detail=INTERNAL_ERROR) from e
    except ConnectionError as e:
        logger.error("Cloud provider connection failed during cancellation for %s: %s", deployment_id, e)
        raise HTTPException(status_code=503, detail=CLOUD_SERVICE_UNAVAILABLE) from e
    except (OSError, RuntimeError) as e:
        logger.exception("Failed to cancel deployment %s: %s", deployment_id, e)
        raise HTTPException(status_code=500, detail=INTERNAL_ERROR) from e


@router.post("/deployments/{deployment_id}/resume")
async def resume_deployment(deployment_id: str, request: Request) -> dict[str, str]:
    """Resume a cancelled or failed deployment."""
    if _cfg.is_mock_mode():
        from deployment_api.mock_state import get_store

        updated = get_store().update("deployments", deployment_id, {"status": "running"})
        if updated is None:
            raise HTTPException(status_code=404, detail=f"Deployment '{deployment_id}' not found (mock)")
        return {"deployment_id": deployment_id, "status": "running"}
    try:
        result = state_manager.resume_deployment(deployment_id)
        return result
    except ValueError as e:
        logger.exception("Deployment not found for resume: %s", deployment_id)
        raise HTTPException(status_code=404, detail=INTERNAL_ERROR) from e
    except ConnectionError as e:
        logger.error("Cloud provider connection failed during resume for %s: %s", deployment_id, e)
        raise HTTPException(status_code=503, detail=CLOUD_SERVICE_UNAVAILABLE) from e
    except (OSError, RuntimeError) as e:
        logger.exception("Failed to resume deployment %s: %s", deployment_id, e)
        raise HTTPException(status_code=500, detail=INTERNAL_ERROR) from e


@router.post("/deployments/{deployment_id}/retry-failed")
async def retry_failed_shards(
    deployment_id: str,
    request: Request,
    dry_run: bool = Query(False, description="Preview retry without executing"),
) -> dict[str, object]:
    """Retry failed shards in a deployment."""
    if _cfg.is_mock_mode():
        from deployment_api.mock_state import get_store

        item = get_store().get("deployments", deployment_id)
        if item is None:
            raise HTTPException(status_code=404, detail=f"Deployment '{deployment_id}' not found (mock)")
        if not dry_run:
            get_store().update("deployments", deployment_id, {"status": "retrying"})
        return {
            "deployment_id": deployment_id,
            "status": "retrying" if not dry_run else "dry_run",
            "retried_shards": 0,
            "dry_run": dry_run,
        }
    try:
        result = state_manager.retry_failed_shards(deployment_id, dry_run=dry_run)
        return result
    except (ValueError, AttributeError) as e:
        logger.exception("Deployment not found for retry: %s", deployment_id)
        raise HTTPException(status_code=404, detail=INTERNAL_ERROR) from e
    except ConnectionError as e:
        logger.error("Cloud provider connection failed during retry for %s: %s", deployment_id, e)
        raise HTTPException(status_code=503, detail=CLOUD_SERVICE_UNAVAILABLE) from e
    except (OSError, RuntimeError) as e:
        logger.exception("Failed to retry deployment %s: %s", deployment_id, e)
        raise HTTPException(status_code=500, detail=INTERNAL_ERROR) from e


@router.patch("/deployments/{deployment_id}")
async def update_deployment(
    deployment_id: str, update_request: UpdateDeploymentRequest, request: Request
) -> dict[str, str]:
    """Update deployment properties."""
    if _cfg.is_mock_mode():
        from deployment_api.mock_state import get_store

        fields: dict[str, object] = {}
        if update_request.tag:
            fields["tag"] = update_request.tag
        if not fields:
            raise HTTPException(status_code=400, detail="No update parameters provided")
        updated = get_store().update("deployments", deployment_id, fields)
        if updated is None:
            raise HTTPException(status_code=404, detail=f"Deployment '{deployment_id}' not found (mock)")
        return {"deployment_id": deployment_id, "status": "updated"}
    try:
        if update_request.tag:
            result = state_manager.update_deployment_tag(deployment_id, update_request.tag)
            return result
        else:
            raise HTTPException(status_code=400, detail="No update parameters provided")
    except ValueError as e:
        logger.exception("Deployment not found for update: %s", deployment_id)
        raise HTTPException(status_code=404, detail=INTERNAL_ERROR) from e
    except ConnectionError as e:
        logger.error("Cloud provider connection failed during update for %s: %s", deployment_id, e)
        raise HTTPException(status_code=503, detail=CLOUD_SERVICE_UNAVAILABLE) from e
    except (OSError, RuntimeError) as e:
        logger.exception("Failed to update deployment %s: %s", deployment_id, e)
        raise HTTPException(status_code=500, detail=INTERNAL_ERROR) from e


@router.delete("/deployments/{deployment_id}")
async def delete_deployment(deployment_id: str, request: Request) -> dict[str, str]:
    """Delete a deployment and its resources."""
    if _cfg.is_mock_mode():
        from deployment_api.mock_state import get_store

        deleted = get_store().delete("deployments", deployment_id)
        if not deleted:
            raise HTTPException(status_code=404, detail=f"Deployment '{deployment_id}' not found (mock)")
        return {"deployment_id": deployment_id, "status": "deleted"}
    try:
        result = state_manager.delete_deployment(deployment_id)
        return result
    except ValueError as e:
        logger.exception("Deployment not found for deletion: %s", deployment_id)
        raise HTTPException(status_code=404, detail=INTERNAL_ERROR) from e
    except ConnectionError as e:
        logger.error("Cloud provider connection failed during deletion for %s: %s", deployment_id, e)
        raise HTTPException(status_code=503, detail=CLOUD_SERVICE_UNAVAILABLE) from e
    except (OSError, RuntimeError) as e:
        logger.exception("Failed to delete deployment %s: %s", deployment_id, e)
        raise HTTPException(status_code=500, detail=INTERNAL_ERROR) from e


@router.post("/deployments/bulk-delete")
async def bulk_delete_deployments(bulk_request: BulkDeleteRequest, request: Request) -> dict[str, object]:
    """Delete multiple deployments."""
    try:
        result = state_manager.bulk_delete_deployments(bulk_request.deployment_ids)
        return result
    except ConnectionError as e:
        logger.error("Cloud provider connection failed during bulk delete: %s", e)
        raise HTTPException(status_code=503, detail=CLOUD_SERVICE_UNAVAILABLE) from e
    except (OSError, ValueError, RuntimeError) as e:
        logger.exception("Failed to bulk delete deployments: %s", e)
        raise HTTPException(status_code=500, detail=INTERNAL_ERROR) from e


@router.post("/deployments/{deployment_id}/refresh")
async def refresh_deployment_status(deployment_id: str, request: Request) -> dict[str, object]:
    """Refresh deployment status from cloud provider."""
    try:
        result = state_manager.refresh_deployment_status(deployment_id)
        return result
    except ValueError as e:
        logger.exception("Deployment not found for refresh: %s", deployment_id)
        raise HTTPException(status_code=404, detail=INTERNAL_ERROR) from e
    except ConnectionError as e:
        logger.error("Cloud provider connection failed during refresh for %s: %s", deployment_id, e)
        raise HTTPException(status_code=503, detail=CLOUD_SERVICE_UNAVAILABLE) from e
    except (OSError, RuntimeError) as e:
        logger.exception("Failed to refresh deployment %s: %s", deployment_id, e)
        raise HTTPException(status_code=500, detail=INTERNAL_ERROR) from e


@router.get("/deployments/{deployment_id}/logs")
async def get_deployment_logs(
    deployment_id: str,
    shard_filter: str | None = Query(None, description="Filter logs by shard pattern"),
    log_type: str = Query("all", description="Type of logs: all, error, warning"),
    tail: int = Query(100, ge=1, le=10000, description="Number of recent lines"),
    stream: bool = Query(False, description="Stream logs in real-time"),
) -> dict[str, object]:
    """Get deployment logs with filtering options."""
    try:
        if stream:
            # For streaming, we'd need to implement a different response type
            # For now, return regular logs
            result = state_manager.get_deployment_logs(
                deployment_id,
                shard_filter=shard_filter,
                log_type=log_type,
                tail_lines=tail,
            )
            return result
        else:
            result = state_manager.get_deployment_logs(
                deployment_id,
                shard_filter=shard_filter,
                log_type=log_type,
                tail_lines=tail,
            )
            return result
    except ValueError as e:
        logger.exception("Deployment not found for log retrieval: %s", deployment_id)
        raise HTTPException(status_code=404, detail=INTERNAL_ERROR) from e
    except ConnectionError as e:
        logger.error("Cloud provider connection failed while getting logs for %s: %s", deployment_id, e)
        raise HTTPException(status_code=503, detail=CLOUD_SERVICE_UNAVAILABLE) from e
    except (OSError, RuntimeError) as e:
        logger.exception("Failed to get logs for deployment %s: %s", deployment_id, e)
        raise HTTPException(status_code=500, detail=INTERNAL_ERROR) from e


@router.get("/deployments/{deployment_id}/report")
async def get_deployment_report(deployment_id: str, request: Request) -> dict[str, object]:
    """Get a detailed deployment report with analysis and recommendations."""
    try:
        result = deployment_manager.get_deployment_report(deployment_id)
        return result
    except ValueError as e:
        logger.exception("Deployment not found for report: %s", deployment_id)
        raise HTTPException(status_code=404, detail=INTERNAL_ERROR) from e
    except FileNotFoundError as e:
        logger.error("Report data not found for deployment %s: %s", deployment_id, e)
        raise HTTPException(status_code=404, detail="Report data not available") from e
    except ConnectionError as e:
        logger.error("Cloud provider connection failed while generating report for %s: %s", deployment_id, e)
        raise HTTPException(status_code=503, detail=CLOUD_SERVICE_UNAVAILABLE) from e
    except (OSError, RuntimeError) as e:
        logger.exception("Failed to get deployment report for %s: %s", deployment_id, e)
        raise HTTPException(status_code=500, detail=INTERNAL_ERROR) from e


# ── Live deployment & event stream endpoints ──────────────────────────────────


class RollbackRequest(BaseModel):  # CORRECT-LOCAL: FastAPI API contract model
    """Request body for a live deployment rollback."""

    service: str = Field(..., description="Cloud Run Service name")
    region: str = Field(..., description="GCP region")
    target_revision: str | None = Field(None, description="Specific revision to roll back to (None = previous)")


@router.get("/deployments/{deployment_id}/events")
async def get_deployment_events(
    deployment_id: str,
    shard_id: str | None = Query(None, description="Filter by shard ID"),
) -> dict[str, object]:
    """
    Return the full shard event stream for a deployment.

    Events are written by deployment-service backends to GCS as JSONL and aggregated
    here. Each event captures a lifecycle step (JOB_STARTED, VM_PREEMPTED, etc.)
    with timestamp, message, and optional metadata.
    """
    if _cfg.is_mock_mode():
        mock_events: list[dict[str, object]] = [
            {
                "event_type": "JOB_STARTED",
                "timestamp": "2026-03-17T00:00:00Z",
                "message": "Deployment started",
                "deployment_id": deployment_id,
            },
            {
                "event_type": "JOB_COMPLETED",
                "timestamp": "2026-03-17T00:00:01Z",
                "message": "Deployment completed",
                "deployment_id": deployment_id,
            },
        ]
        return {"deployment_id": deployment_id, "events": mock_events, "count": len(mock_events)}

    from deployment_api.clients import deployment_service_client as _client

    try:
        events = await _client.get_deployment_events(deployment_id, shard_id=shard_id)
        return {"deployment_id": deployment_id, "events": events, "count": len(events)}
    except RuntimeError as e:
        logger.error("Failed to get events for deployment %s: %s", deployment_id, e)
        raise HTTPException(status_code=502, detail="Event stream unavailable") from e
    except (OSError, ValueError) as e:
        logger.exception("Error fetching events for deployment %s", deployment_id)
        raise HTTPException(status_code=500, detail=INTERNAL_ERROR) from e


@router.get("/deployments/{deployment_id}/vm-events")
async def get_deployment_vm_events(
    deployment_id: str,
) -> dict[str, object]:
    """
    Return VM-level infrastructure events for a deployment.

    Filters the full event stream to VM_PREEMPTED, VM_DELETED, VM_QUOTA_EXHAUSTED,
    VM_ZONE_UNAVAILABLE, VM_TIMEOUT, CONTAINER_OOM, CLOUD_RUN_REVISION_FAILED.
    Used by the History tab to surface infrastructure failure badges on shard rows.
    """
    if _cfg.is_mock_mode():
        empty_events: list[dict[str, object]] = []
        return {"deployment_id": deployment_id, "events": empty_events, "count": 0}

    from deployment_api.clients import deployment_service_client as _client

    try:
        events = await _client.get_vm_events(deployment_id)
        return {"deployment_id": deployment_id, "events": events, "count": len(events)}
    except RuntimeError as e:
        logger.error("Failed to get VM events for deployment %s: %s", deployment_id, e)
        raise HTTPException(status_code=502, detail="Event stream unavailable") from e
    except (OSError, ValueError) as e:
        logger.exception("Error fetching VM events for deployment %s", deployment_id)
        raise HTTPException(status_code=500, detail=INTERNAL_ERROR) from e


@router.post("/deployments/{deployment_id}/rollback")
async def rollback_live_deployment(deployment_id: str, rollback_request: RollbackRequest) -> dict[str, object]:
    """
    Roll back a live Cloud Run Service deployment to the previous revision.

    Only valid for deployments with deploy_mode="live". Calls the deployment-service
    LiveDeployer to revert traffic to the specified (or previous) Cloud Run revision.
    """
    if _cfg.is_mock_mode():
        return {
            "deployment_id": deployment_id,
            "service": rollback_request.service,
            "status": "rolled_back",
            "events": [],
            "error": None,
        }

    from deployment_api.clients import deployment_service_client as _client

    try:
        result = await _client.live_rollback(
            deployment_id=deployment_id,
            service=rollback_request.service,
            region=rollback_request.region,
            target_revision=rollback_request.target_revision,
        )
        return result
    except RuntimeError as e:
        logger.error("Rollback failed for deployment %s: %s", deployment_id, e)
        raise HTTPException(status_code=502, detail="Rollback request failed") from e
    except (OSError, ValueError) as e:
        logger.exception("Error during rollback for deployment %s", deployment_id)
        raise HTTPException(status_code=500, detail=INTERNAL_ERROR) from e


@router.get("/deployments/{deployment_id}/live-health")
async def get_live_deployment_health(
    deployment_id: str,
    service: str = Query(..., description="Cloud Run Service name"),
    region: str = Query(..., description="GCP region"),
) -> dict[str, object]:
    """
    Return the current health check status of a live Cloud Run Service.

    Polls the service /health endpoint and returns a structured response.
    Used by DeploymentDetails to show a live health badge for live-mode deployments.
    """
    if _cfg.is_mock_mode():
        return {
            "deployment_id": deployment_id,
            "service": service,
            "healthy": True,
            "checked_at": "2026-03-17T00:00:00Z",
            "status_code": 200,
        }

    from deployment_api.clients import deployment_service_client as _client

    try:
        result = await _client.get_live_health(
            deployment_id=deployment_id,
            service=service,
            region=region,
        )
        return result
    except RuntimeError as e:
        logger.error(
            "Failed to get live health for deployment %s service %s: %s",
            deployment_id,
            service,
            e,
        )
        raise HTTPException(status_code=502, detail="Health check unavailable") from e
    except (OSError, ValueError) as e:
        logger.exception("Error fetching live health for deployment %s service %s", deployment_id, service)
        raise HTTPException(status_code=500, detail=INTERNAL_ERROR) from e

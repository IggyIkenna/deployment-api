"""Deployment CRUD routes — list / status / verify / quota / create / deploy-missing.

Split from ``routes/deployments.py`` (pure code motion; plan:
``codex_violations_ratchet_to_five_2026_06_10.md`` Phase-1 P2). Routes
register on the package facade's shared ``router``; patched module-level
collaborators (``_cfg`` / ``deployment_manager`` / ``state_manager`` /
``data_status_service``) are resolved through the facade module (``_dp``)
at call time so the existing test patch surface
``deployment_api.routes.deployments.<name>`` keeps intercepting.
"""

import logging
import uuid
from datetime import datetime, timedelta
from typing import cast

from fastapi import BackgroundTasks, HTTPException, Query, Request

import deployment_api.routes.deployments as _dp
from deployment_api.app_config import get_config_dir
from deployment_api.messages import (
    CLOUD_SERVICE_UNAVAILABLE,
    INTERNAL_ERROR,
    INTERNAL_SERVER_ERROR,
    INVALID_CONFIG,
    INVALID_PARAMETERS,
    SERVICE_CONFIG_NOT_FOUND,
    SERVICE_TEMPORARILY_UNAVAILABLE,
)
from deployment_api.routes.deployments import (
    DeploymentResult,
    DeployMissingRequest,
    DeployRequest,
    router,
)

logger = logging.getLogger(__name__)


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
    if _dp._cfg.is_mock_mode():  # pyright: ignore[reportPrivateUsage]
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
        total_count = len(items)
        return {
            "deployments": page,
            "total_count": total_count,
            "limit": limit,
            "offset": offset,
            "has_more": offset + limit < total_count,
        }
    try:
        result = _dp.state_manager.list_deployments(
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
    if _dp._cfg.is_mock_mode():  # pyright: ignore[reportPrivateUsage]
        from deployment_api.mock_state import get_store

        item = get_store().get("deployments", deployment_id)
        if item is None:
            raise HTTPException(status_code=404, detail=f"Deployment '{deployment_id}' not found (mock)")
        return item
    try:
        result = _dp.state_manager.get_deployment_status(deployment_id, detailed=detailed)
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
    if _dp._cfg.is_mock_mode():  # pyright: ignore[reportPrivateUsage]
        return {
            "deployment_id": deployment_id,
            "verified": True,
            "status": "completed",
            "mock": True,
        }
    try:
        result = _dp.state_manager.verify_deployment_completion(deployment_id, force_refresh=force)
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
        result = await _dp.deployment_manager.calculate_quota_requirements(
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
    if _dp._cfg.is_mock_mode():  # pyright: ignore[reportPrivateUsage]
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
                _dp.deployment_manager.run_deployment_background,
                req,
                config_dir,
                shard_list,
                cli_cmd,
                dep_id,
            )

        result = await _dp.deployment_manager.create_deployment(
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
        cloud_provider=getattr(req, "cloud_provider", "gcp"),
        operational_mode=getattr(req, "operational_mode", ""),
        start_date=req.start_date,
        end_date=req.end_date,
        max_shards=getattr(req, "max_shards", 10000),
        max_concurrent=getattr(req, "max_concurrent", None),
        vm_zone=getattr(req, "vm_zone", None),
        cloud_config_path=getattr(req, "cloud_config_path", None),
        ignore_start_dates=getattr(req, "ignore_start_dates", False),
        deploy_missing=True,
        exclude_dates=exclude_dates,
        skip_dimensions=getattr(req, "skip_dimensions", None),
        max_workers=getattr(req, "max_workers", None),
        region=req.region,
        tag=req.tag,
        asset_group=req.asset_group,
        venue=req.venue,
        feature_group=getattr(req, "feature_group", None),
        data_type=getattr(req, "data_type", None),
        date_granularity=req.date_granularity,
        log_level=req.log_level,
        runtime_profile=getattr(req, "runtime_profile", None),
        client_id=getattr(req, "client_id", None),
    )

    def background_task_runner(
        r: DeployRequest,
        config_dir: str,
        shard_list: list[dict[str, object]],
        cli_cmd: str,
        dep_id: str,
    ) -> None:
        background_tasks.add_task(
            _dp.deployment_manager.run_deployment_background,
            r,
            config_dir,
            shard_list,
            cli_cmd,
            dep_id,
        )

    result = await _dp.deployment_manager.create_deployment(
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
    if _dp._cfg.is_mock_mode():  # pyright: ignore[reportPrivateUsage]
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

        missing_analysis = await _dp.data_status_service.calculate_missing_shards(
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

"""Deployment lifecycle routes — cancel / resume / retry / update / delete / logs / events.

Split from ``routes/deployments.py`` (pure code motion; plan:
``codex_violations_ratchet_to_five_2026_06_10.md`` Phase-1 P2). Routes
register on the package facade's shared ``router``; patched module-level
collaborators are resolved through the facade module (``_dp``) at call
time so the existing test patch surface keeps intercepting.
"""

import logging

from fastapi import HTTPException, Query, Request
from pydantic import BaseModel, Field

import deployment_api.routes.deployments as _dp
from deployment_api.messages import (
    CLOUD_SERVICE_UNAVAILABLE,
    INTERNAL_ERROR,
)
from deployment_api.routes.deployments import (
    BulkDeleteRequest,
    UpdateDeploymentRequest,
    router,
)

logger = logging.getLogger(__name__)


@router.post("/deployments/{deployment_id}/cancel")
async def cancel_deployment(deployment_id: str, request: Request) -> dict[str, str]:
    """Cancel a running deployment."""
    if _dp._cfg.is_mock_mode():  # pyright: ignore[reportPrivateUsage]
        from deployment_api.mock_state import get_store

        updated = get_store().update("deployments", deployment_id, {"status": "cancelled"})
        if updated is None:
            raise HTTPException(status_code=404, detail=f"Deployment '{deployment_id}' not found (mock)")
        return {"deployment_id": deployment_id, "status": "cancelled"}
    try:
        result = _dp.state_manager.cancel_deployment(deployment_id)
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
    if _dp._cfg.is_mock_mode():  # pyright: ignore[reportPrivateUsage]
        from deployment_api.mock_state import get_store

        updated = get_store().update("deployments", deployment_id, {"status": "running"})
        if updated is None:
            raise HTTPException(status_code=404, detail=f"Deployment '{deployment_id}' not found (mock)")
        return {"deployment_id": deployment_id, "status": "running"}
    try:
        result = _dp.state_manager.resume_deployment(deployment_id)
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
    if _dp._cfg.is_mock_mode():  # pyright: ignore[reportPrivateUsage]
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
        result = _dp.state_manager.retry_failed_shards(deployment_id, dry_run=dry_run)  # pyright: ignore[reportUnknownVariableType,reportUnknownMemberType,reportAttributeAccessIssue]
        return result  # pyright: ignore[reportUnknownVariableType]
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
    if _dp._cfg.is_mock_mode():  # pyright: ignore[reportPrivateUsage]
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
            result = _dp.state_manager.update_deployment_tag(deployment_id, update_request.tag)
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
    if _dp._cfg.is_mock_mode():  # pyright: ignore[reportPrivateUsage]
        from deployment_api.mock_state import get_store

        deleted = get_store().delete("deployments", deployment_id)
        if not deleted:
            raise HTTPException(status_code=404, detail=f"Deployment '{deployment_id}' not found (mock)")
        return {"deployment_id": deployment_id, "status": "deleted"}
    try:
        result = _dp.state_manager.delete_deployment(deployment_id)
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
        result = _dp.state_manager.bulk_delete_deployments(bulk_request.deployment_ids)
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
        result = _dp.state_manager.refresh_deployment_status(deployment_id)
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
            result = _dp.state_manager.get_deployment_logs(
                deployment_id,
                shard_filter=shard_filter,
                log_type=log_type,
                tail_lines=tail,
            )
            return result
        else:
            result = _dp.state_manager.get_deployment_logs(
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
        result = _dp.deployment_manager.get_deployment_report(deployment_id)
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
    if _dp._cfg.is_mock_mode():  # pyright: ignore[reportPrivateUsage]
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
    if _dp._cfg.is_mock_mode():  # pyright: ignore[reportPrivateUsage]
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
    if _dp._cfg.is_mock_mode():  # pyright: ignore[reportPrivateUsage]
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
    if _dp._cfg.is_mock_mode():  # pyright: ignore[reportPrivateUsage]
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

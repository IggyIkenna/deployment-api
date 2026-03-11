"""
Deployment state management operations.

This module handles state management for deployments including
status tracking, refreshing, cancellation, and state transitions.
"""

import asyncio
import logging
from typing import cast

logger = logging.getLogger(__name__)


def _demo_deployments() -> list[dict[str, object]]:
    """Return realistic demo deployments for local dev (when GCS is unavailable)."""
    return [
        {
            "deployment_id": "live-exec-20260310-143022-a1b2",
            "service": "execution-service",
            "compute_type": "cloud_run",
            "status": "running",
            "deploy_mode": "live",
            "created_at": "2026-03-10T14:30:22Z",
            "updated_at": "2026-03-10T14:45:00Z",
            "tag": "v2.4.1-canary",
            "region": "asia-northeast1",
            "parameters": {"mode": "live"},
            "total_shards": 1,
            "completed_shards": 0,
            "failed_shards": 0,
            "progress": {"total_shards": 1, "completed": 0, "failed": 0},
        },
        {
            "deployment_id": "instruments-20260310-090010-c3d4",
            "service": "instruments-service",
            "compute_type": "vm",
            "status": "completed",
            "deploy_mode": "batch",
            "created_at": "2026-03-10T09:00:10Z",
            "updated_at": "2026-03-10T11:22:44Z",
            "tag": "nightly-2026-03-10",
            "region": "asia-northeast1",
            "parameters": {"mode": "batch"},
            "total_shards": 240,
            "completed_shards": 238,
            "failed_shards": 2,
            "progress": {"total_shards": 240, "completed": 238, "failed": 2},
        },
        {
            "deployment_id": "market-data-20260309-220500-e5f6",
            "service": "market-data-processing-service",
            "compute_type": "vm",
            "status": "failed",
            "deploy_mode": "batch",
            "created_at": "2026-03-09T22:05:00Z",
            "updated_at": "2026-03-09T23:14:33Z",
            "tag": "v1.8.0",
            "region": "asia-northeast1",
            "parameters": {"mode": "batch"},
            "total_shards": 180,
            "completed_shards": 144,
            "failed_shards": 36,
            "progress": {"total_shards": 180, "completed": 144, "failed": 36},
        },
        {
            "deployment_id": "features-vol-20260309-180000-g7h8",
            "service": "features-volatility-service",
            "compute_type": "cloud_run",
            "status": "completed",
            "deploy_mode": "batch",
            "created_at": "2026-03-09T18:00:00Z",
            "updated_at": "2026-03-09T20:31:15Z",
            "tag": "v3.1.2",
            "region": "asia-northeast1",
            "parameters": {"mode": "batch"},
            "total_shards": 96,
            "completed_shards": 96,
            "failed_shards": 0,
            "progress": {"total_shards": 96, "completed": 96, "failed": 0},
        },
        {
            "deployment_id": "strategy-20260309-120000-i9j0",
            "service": "strategy-service",
            "compute_type": "cloud_run",
            "status": "cancelled",
            "deploy_mode": "batch",
            "created_at": "2026-03-09T12:00:00Z",
            "updated_at": "2026-03-09T12:47:08Z",
            "tag": "v5.0.0-beta",
            "region": "us-central1",
            "parameters": {"mode": "batch"},
            "total_shards": 320,
            "completed_shards": 87,
            "failed_shards": 0,
            "progress": {"total_shards": 320, "completed": 87, "failed": 0},
        },
    ]


class DeploymentStateManager:
    """Manages deployment state and lifecycle operations."""

    def __init__(self):
        """Initialize the deployment state manager."""
        pass

    def list_deployments(
        self,
        limit: int = 50,
        offset: int = 0,
        status_filter: str | None = None,
        service_filter: str | None = None,
    ) -> dict[str, object]:
        """
        List deployments with optional filtering.

        Args:
            limit: Maximum number of deployments to return
            offset: Offset for pagination
            status_filter: Filter by deployment status
            service_filter: Filter by service name

        Returns:
            Dict containing deployment list and metadata
        """
        # get_cached_deployments is an async function that requires a state_manager param.
        # This synchronous method uses the demo data fallback path instead.
        # TODO: Refactor DeploymentStateManager.list_deployments to async and wire proper
        # caching (get_cached_deployments(self, ...)).
        deployments: list[dict[str, object]] = _demo_deployments()

        # Apply filters
        if status_filter:
            deployments = [d for d in deployments if d.get("status") == status_filter]

        if service_filter:
            deployments = [d for d in deployments if d.get("service") == service_filter]

        # Sort by creation time (most recent first)
        deployments.sort(key=lambda x: cast(str, x.get("created_at") or ""), reverse=True)

        # Apply pagination
        total_count = len(deployments)
        paginated_deployments = deployments[offset : offset + limit]

        # Enrich deployment data
        for deployment in paginated_deployments:
            self._enrich_deployment_summary(deployment)

        return {
            "deployments": paginated_deployments,
            "total_count": total_count,
            "limit": limit,
            "offset": offset,
            "has_more": offset + limit < total_count,
        }

    def get_deployment_state(self, deployment_id: str) -> dict[str, object] | None:
        """
        Return raw state dict for a deployment, or None if not found.

        This is a lower-level accessor used by get_deployment_report to allow
        callers to distinguish "not found" (returns None) from other errors.
        In production it delegates to get_deployment_status with detailed=False
        and catches ValueError to return None.
        """
        try:
            return self.get_deployment_status(deployment_id, detailed=False)
        except ValueError:
            return None

    def get_deployment_status(self, deployment_id: str, detailed: bool = True) -> dict[str, object]:
        """
        Get detailed status for a specific deployment.

        Args:
            deployment_id: Deployment ID to get status for
            detailed: Whether to include detailed shard information

        Returns:
            Dict containing deployment status and details
        """
        # get_cached_deployment_state is an async function that requires a state_manager param.
        # This synchronous method looks up demo data instead.
        # TODO: Refactor DeploymentStateManager.get_deployment_status to async and wire proper
        # caching (await get_cached_deployment_state(self, deployment_id)).
        all_deployments = _demo_deployments()
        state: dict[str, object] | None = next(
            (d for d in all_deployments if d.get("deployment_id") == deployment_id), None
        )
        if not state:
            raise ValueError(f"Deployment {deployment_id} not found")

        progress = cast(dict[str, object], state.get("progress") or {})
        total = cast(int, state.get("total_shards") or 0)
        completed = cast(int, progress.get("completed") or 0)
        failed = cast(int, progress.get("failed") or 0)

        response: dict[str, object] = {
            "deployment_id": deployment_id,
            "service": state.get("service"),
            "status": state.get("status"),
            "deploy_mode": state.get("deploy_mode", "batch"),
            "created_at": state.get("created_at"),
            "updated_at": state.get("updated_at"),
            "region": state.get("region", "asia-northeast1"),
            "compute_type": state.get("compute_type"),
            "tag": state.get("tag"),
            "total_shards": total,
            "summary": {
                "completed": completed,
                "failed": failed,
                "running": total - completed - failed if state.get("status") == "running" else 0,
                "pending": 0,
            },
            "date_range": {"start": "2026-01-01", "end": "2026-03-10"},
        }

        if detailed:
            # Build demo shard rows so the UI shard table renders
            demo_shards: list[dict[str, object]] = []
            for i in range(min(total, 12)):
                if i < failed:
                    shard_status = "failed"
                elif i < completed:
                    shard_status = "completed"
                else:
                    shard_status = "running" if state.get("status") == "running" else "pending"
                demo_shards.append(
                    {
                        "shard_id": f"{state['service']}-{i:04d}",
                        "shard_index": i,
                        "status": shard_status,
                        "classification": shard_status,
                        "dimensions": {"date": f"2026-03-{(i % 10) + 1:02d}"},
                        "started_at": state.get("created_at"),
                        "completed_at": state.get("updated_at")
                        if shard_status == "completed"
                        else None,
                    }
                )
            response.update(
                {
                    "shards": demo_shards,
                    "compute_config": {"cpu": 4, "memory": "8Gi", "machine_type": "n2-standard-4"},
                    "cli_command": (
                        f"python -m deployment deploy"
                        f" --service {state['service']} --compute vm"
                    ),
                    "error_details": None,
                }
            )

        return response

    def refresh_deployment_status(self, deployment_id: str) -> dict[str, object]:
        """
        Refresh deployment status from cloud provider.

        Args:
            deployment_id: Deployment ID to refresh

        Returns:
            Updated deployment status
        """
        from ..routes.deployment_caching import invalidate_deployment_state_cache
        from ..routes.deployment_state import (
            _refresh_deployment_status_sync,  # type: ignore[reportPrivateUsage]
        )

        # Refresh from cloud provider
        _refresh_deployment_status_sync(deployment_id)

        # Invalidate cache to force refresh (async fn called from sync context)
        asyncio.run(invalidate_deployment_state_cache(deployment_id))

        # Return updated status
        return self.get_deployment_status(deployment_id, detailed=False)

    def cancel_deployment(self, deployment_id: str) -> dict[str, str]:
        """
        Cancel a running deployment.

        Args:
            deployment_id: Deployment ID to cancel

        Returns:
            Dict with cancellation status
        """
        from ..routes.deployment_caching import invalidate_deployment_state_cache
        from ..routes.deployment_state import (
            _cancel_deployment_sync,  # type: ignore[reportPrivateUsage]
        )

        try:
            # Cancel deployment
            _cancel_deployment_sync(deployment_id)

            # Invalidate cache (async fn called from sync context)
            asyncio.run(invalidate_deployment_state_cache(deployment_id))

            return {
                "deployment_id": deployment_id,
                "status": "cancelled",
                "message": "Deployment cancellation initiated",
            }
        except (OSError, ValueError, RuntimeError) as e:
            logger.error("Failed to cancel deployment %s: %s", deployment_id, e)
            raise ValueError(f"Failed to cancel deployment: {e}") from e

    def resume_deployment(self, deployment_id: str) -> dict[str, str]:
        """
        Resume a cancelled or failed deployment.

        Args:
            deployment_id: Deployment ID to resume

        Returns:
            Dict with resume status
        """
        from ..routes.deployment_caching import invalidate_deployment_state_cache
        from ..routes.deployment_state import (
            _resume_deployment_sync,  # type: ignore[reportPrivateUsage]
        )

        try:
            # Resume deployment
            _resume_deployment_sync(deployment_id)

            # Invalidate cache (async fn called from sync context)
            asyncio.run(invalidate_deployment_state_cache(deployment_id))

            return {
                "deployment_id": deployment_id,
                "status": "resumed",
                "message": "Deployment resume initiated",
            }
        except (OSError, ValueError, RuntimeError) as e:
            logger.error("Failed to resume deployment %s: %s", deployment_id, e)
            raise ValueError(f"Failed to resume deployment: {e}") from e

    def delete_deployment(self, deployment_id: str) -> dict[str, str]:
        """
        Delete a deployment and its resources.

        Args:
            deployment_id: Deployment ID to delete

        Returns:
            Dict with deletion status
        """
        from ..routes.deployment_caching import (
            invalidate_deployment_cache,
            invalidate_deployment_state_cache,
        )
        from ..routes.deployment_state import (
            _delete_deployment_sync,  # type: ignore[reportPrivateUsage]
        )

        try:
            # Delete deployment
            _delete_deployment_sync(deployment_id)

            # Invalidate caches (async fns called from sync context)
            asyncio.run(invalidate_deployment_state_cache(deployment_id))
            asyncio.run(invalidate_deployment_cache())

            return {
                "deployment_id": deployment_id,
                "status": "deleted",
                "message": "Deployment deletion initiated",
            }
        except (OSError, ValueError, RuntimeError) as e:
            logger.error("Failed to delete deployment %s: %s", deployment_id, e)
            raise ValueError(f"Failed to delete deployment: {e}") from e

    def bulk_delete_deployments(self, deployment_ids: list[str]) -> dict[str, object]:
        """
        Delete multiple deployments.

        Args:
            deployment_ids: List of deployment IDs to delete

        Returns:
            Dict with bulk deletion results
        """
        from ..routes.deployment_caching import invalidate_deployment_cache

        successful_list: list[str] = []
        failed_list: list[object] = []
        results: dict[str, object] = {
            "total_requested": len(deployment_ids),
            "successful": successful_list,
            "failed": failed_list,
        }

        for deployment_id in deployment_ids:
            try:
                self.delete_deployment(deployment_id)
                successful_list.append(deployment_id)
            except (OSError, ValueError, RuntimeError) as e:
                logger.error("Failed to delete deployment %s: %s", deployment_id, e)
                failed_list.append(
                    {
                        "deployment_id": deployment_id,
                        "error": str(e),
                    }
                )

        # Invalidate deployment list cache (async fn called from sync context)
        asyncio.run(invalidate_deployment_cache())

        results.update(
            {
                "successful_count": len(successful_list),
                "failed_count": len(failed_list),
            }
        )

        return results

    def update_deployment_tag(self, deployment_id: str, new_tag: str) -> dict[str, str]:
        """
        Update deployment tag/version.

        Args:
            deployment_id: Deployment ID to update
            new_tag: New tag to set

        Returns:
            Dict with update status
        """
        from ..routes.deployment_caching import invalidate_deployment_state_cache
        from ..routes.deployment_state import (
            _update_deployment_tag_sync,  # type: ignore[reportPrivateUsage]
        )

        try:
            # Update tag
            _update_deployment_tag_sync(deployment_id, new_tag)

            # Invalidate cache (async fn called from sync context)
            asyncio.run(invalidate_deployment_state_cache(deployment_id))

            return {
                "deployment_id": deployment_id,
                "new_tag": new_tag,
                "status": "updated",
                "message": "Deployment tag updated successfully",
            }
        except (OSError, ValueError, RuntimeError) as e:
            logger.error("Failed to update deployment %s tag: %s", deployment_id, e)
            raise ValueError(f"Failed to update deployment tag: {e}") from e

    def verify_deployment_completion(
        self, deployment_id: str, force_refresh: bool = False
    ) -> dict[str, object]:
        """
        Verify deployment completion and data integrity.

        Args:
            deployment_id: Deployment ID to verify
            force_refresh: Whether to force refresh verification

        Returns:
            Dict containing verification results
        """
        # _compute_and_cache_verification is async and requires state_manager + state params
        # that are not available in this synchronous context.
        # TODO: Refactor to async and wire state_manager/state from deployment records.
        return {
            "deployment_id": deployment_id,
            "status": "not_run",
            "message": "Verification not available in demo mode",
            "force_refresh": force_refresh,
        }

    def get_deployment_logs(
        self,
        deployment_id: str,
        shard_filter: str | None = None,
        log_type: str = "all",
        tail_lines: int = 100,
    ) -> dict[str, object]:
        """
        Get deployment logs with filtering options.

        Args:
            deployment_id: Deployment ID to get logs for
            shard_filter: Filter logs by shard pattern
            log_type: Type of logs to retrieve (all, error, warning)
            tail_lines: Number of recent lines to return

        Returns:
            Dict containing log data
        """
        # analyze_deployment_logs_sync requires state_manager and state params
        # that are not available in this synchronous context.
        # TODO: Refactor to async and wire state_manager/state from deployment records.
        return {
            "deployment_id": deployment_id,
            "status": "not_available",
            "message": "Log analysis not available in demo mode",
            "shard_filter": shard_filter,
            "log_type": log_type,
            "tail_lines": tail_lines,
            "logs": [],
        }

    def _enrich_deployment_summary(self, deployment: dict[str, object]) -> None:
        """
        Enrich deployment summary with additional computed fields.

        Args:
            deployment: Deployment dict to enrich in-place
        """
        # Add computed fields like duration, success rate, etc.
        if "created_at" in deployment and "updated_at" in deployment:
            try:
                from datetime import datetime

                created_raw = deployment["created_at"]
                updated_raw = deployment["updated_at"]
                created_str = (
                    str(created_raw).replace("Z", "+00:00") if created_raw is not None else ""
                )
                updated_str = (
                    str(updated_raw).replace("Z", "+00:00") if updated_raw is not None else ""
                )
                created = datetime.fromisoformat(created_str)
                updated = datetime.fromisoformat(updated_str)
                duration = updated - created
                deployment["duration_minutes"] = int(duration.total_seconds() / 60)
            except (ValueError, TypeError, OSError) as e:
                logger.debug(
                    "Suppressed %s during enrich deployment summary: %s", type(e).__name__, e
                )
                pass

        # Add success rate if shard information is available
        if "total_shards" in deployment and "successful_shards" in deployment:
            total_raw = deployment["total_shards"]
            successful_raw = deployment["successful_shards"]
            total = total_raw if isinstance(total_raw, int) else 0
            successful = successful_raw if isinstance(successful_raw, int) else 0
            if total > 0:
                deployment["success_rate"] = round((successful / total) * 100, 1)
            else:
                deployment["success_rate"] = 0.0


# Alias for stable test patching (do not remove — relied upon by existing tests).
# Tests inject a mock by replacing sys.modules["deployment_api.services.deployment_state"]
# and expect to find DeploymentStateService on the mock module object.
DeploymentStateService = DeploymentStateManager

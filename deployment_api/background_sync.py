"""
Background synchronization task for deployment status monitoring.

Thin orchestrator that coordinates the sync service and state management
to ensure deployment completion status gets persisted when VMs complete and self-delete.
"""

import asyncio
import logging
import time as _time
from concurrent.futures import ThreadPoolExecutor

from deployment_api import settings
from deployment_api.services import SyncService

logger = logging.getLogger(__name__)

# Global sync service instance
_sync_service: SyncService | None = None
_shutdown_event: asyncio.Event | None = None


def get_held_deployment_locks() -> set[str]:
    """Get the set of currently held deployment locks."""
    if _sync_service:
        return _sync_service.get_held_deployment_locks()
    return set()


def set_shutdown_event(event: asyncio.Event) -> None:
    """Set the shutdown event for graceful shutdown."""
    global _shutdown_event
    _shutdown_event = event


async def auto_sync_running_deployments():  # noqa: C901
    """
    Background task that periodically syncs status for running deployments.

    This ensures that when VMs complete and self-delete, their completion
    status gets persisted to state.json without manual intervention.

    Runs every 30-60 seconds with adaptive intervals based on activity.
    """
    global _sync_service

    # Initialize sync service
    _sync_service = SyncService(
        project_id=settings.gcp_project_id,
        state_bucket=settings.STATE_BUCKET,
        deployment_env=settings.DEPLOYMENT_ENV,
    )

    # Sync OWNER_ID store with the initialized state manager owner_id
    _set_owner_id(_sync_service.state_manager.owner_id)

    sync_interval_active = getattr(settings, "AUTO_SYNC_INTERVAL_ACTIVE", 30)
    sync_interval_idle = 60  # seconds when no active deployments
    current_interval = sync_interval_active  # start fast in case there's something active

    logger.info(
        "[AUTO_SYNC] Started background sync task (active_interval=%ss, idle_interval=%ss)",
        sync_interval_active,
        sync_interval_idle,
    )

    # Run first sync immediately (don't wait for interval)
    first_run = True

    while _shutdown_event is None or not _shutdown_event.is_set():
        try:
            if first_run:
                # Small delay on first run to let API fully start, but don't wait full interval
                await asyncio.sleep(5)
                first_run = False
            else:
                await asyncio.sleep(current_interval)

            if _shutdown_event is not None and _shutdown_event.is_set():
                break

            # Run sync operations in thread pool to not block event loop
            loop = asyncio.get_event_loop()
            with ThreadPoolExecutor(max_workers=1) as executor:
                synced, num_active = await loop.run_in_executor(
                    executor, _sync_service.sync_deployments
                )

            if synced > 0:
                logger.info("[AUTO_SYNC] Synced %s deployment(s)", synced)

            # State TTL cleanup: delete old deployment states (once per hour)
            if (_time.time() % 3600) < current_interval:
                try:
                    with ThreadPoolExecutor(max_workers=1) as executor:
                        deleted_count = await loop.run_in_executor(
                            executor, _sync_service.cleanup_state_ttl
                        )
                    if deleted_count > 0:
                        logger.info(
                            "[AUTO_SYNC] TTL cleanup: deleted %s old deployment(s)", deleted_count
                        )
                except (OSError, ValueError, RuntimeError) as e:
                    logger.debug("[AUTO_SYNC] State TTL cleanup error: %s", e)

            # Adaptive interval: fast when active, slow when idle
            if num_active > 0:
                current_interval = sync_interval_active
                logger.debug(
                    "[AUTO_SYNC] %s active → next cycle in %ss", num_active, current_interval
                )
            else:
                current_interval = sync_interval_idle
                logger.debug(
                    "[AUTO_SYNC] No active deployments → next cycle in %ss", current_interval
                )

        except asyncio.CancelledError:
            break
        except (OSError, ValueError, RuntimeError) as e:
            logger.error("[AUTO_SYNC] Error: %s", e)

    logger.info("[AUTO_SYNC] Background sync task stopped")


# Private alias for testability (tests can patch this name)
_auto_sync_running_deployments = auto_sync_running_deployments

# Module-level aliases for settings constants used by processors in this module
PROJECT_ID = settings.gcp_project_id
STATE_BUCKET = settings.STATE_BUCKET


def get_owner_id() -> str:
    """Get the owner ID from the sync service state manager."""
    if _sync_service:
        return _sync_service.state_manager.owner_id
    return ""


# Mutable container for OWNER_ID — set after sync service initialization.
# Using a list avoids reportConstantRedefinition on the uppercase symbol.
_owner_id_store: list[str] = [""]


def _set_owner_id(value: str) -> None:
    """Update owner ID in the shared mutable store."""
    _owner_id_store[0] = value

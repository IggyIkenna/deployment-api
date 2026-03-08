"""
Application lifespan management for FastAPI.

Handles startup and shutdown events including background tasks,
cache initialization, and resource cleanup.
"""

import asyncio
import json
import logging
from contextlib import asynccontextmanager, suppress
from typing import cast

from fastapi import FastAPI

from deployment_api.background_sync import (
    PROJECT_ID,
    STATE_BUCKET,
    _auto_sync_running_deployments,
    get_held_deployment_locks,
    get_owner_id,
    set_shutdown_event,
)
from deployment_api.utils.service_utils import get_config_dir
from deployment_api.utils.storage_client import get_storage_client as get_storage_client_with_pool

logger = logging.getLogger(__name__)

# Background task handles
_background_task = None
_events_drain_task = None
_shutdown_event = None


@asynccontextmanager
async def lifespan(app: FastAPI):  # noqa: C901
    """Application lifespan - startup and shutdown."""
    global _background_task, _shutdown_event, _events_drain_task

    # Startup
    app.state.config_dir = get_config_dir()
    logger.info("Config directory: %s", app.state.config_dir)

    # Initialize cache
    from .utils.cache import cache

    await cache.initialize()

    # Start background sync task
    _shutdown_event = asyncio.Event()
    set_shutdown_event(_shutdown_event)
    _background_task = asyncio.create_task(_auto_sync_running_deployments())
    logger.info("Background auto-sync task started")

    # Start deployment events drain (for low-latency SSE notify when state is saved from sync code)
    from deployment_api.utils.deployment_events import _drain_sync_queue

    _events_drain_task = asyncio.create_task(_drain_sync_queue())
    logger.info("Deployment events drain task started")

    yield

    # Shutdown
    logger.info("Shutting down API...")
    _shutdown_event.set()
    if _background_task:
        _background_task.cancel()
        try:
            await asyncio.wait_for(_background_task, timeout=5)
        except asyncio.CancelledError as e:
            logger.debug("Suppressed %s during operation: %s", type(e).__name__, e)
            pass
        except TimeoutError:
            logger.warning("Background auto-sync task did not stop in time")
    if _events_drain_task:
        _events_drain_task.cancel()
        with suppress(TimeoutError, asyncio.CancelledError):
            await asyncio.wait_for(_events_drain_task, timeout=2)

    # Release any held per-deployment locks on graceful shutdown
    try:
        client = get_storage_client_with_pool(PROJECT_ID)  # Uses shared client with large pool
        bucket = client.bucket(STATE_BUCKET)

        # Release all locks held by this instance
        released_count = 0
        held_deployment_locks = get_held_deployment_locks()
        for deployment_id in list(held_deployment_locks):
            try:
                lock_blob_name = f"locks/deployment_{deployment_id}.lock"
                lock_blob = bucket.blob(lock_blob_name)
                if lock_blob.exists():
                    lock_data = cast(
                        dict[str, object], json.loads(lock_blob.download_as_text() or "{}")
                    )
                    if lock_data.get("owner") == get_owner_id():
                        lock_blob.delete()
                        released_count += 1
            except (OSError, ValueError, RuntimeError):
                pass  # Lock may have been taken by another instance
            held_deployment_locks.discard(deployment_id)

        if released_count > 0:
            logger.info("[SHUTDOWN] Released %s per-deployment lock(s)", released_count)
    except (OSError, ValueError, RuntimeError) as e:
        logger.warning("[SHUTDOWN] Could not release locks: %s", e)

    try:
        from .utils.cache import cache

        await cache.shutdown()
    except (OSError, ValueError, RuntimeError) as e:
        logger.warning("Cache shutdown failed: %s", e)

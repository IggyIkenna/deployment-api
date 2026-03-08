"""
Application configuration and lifecycle management.

Handles FastAPI app setup, CORS configuration, and application lifespan.
"""

import asyncio
import json
import logging
from contextlib import asynccontextmanager, suppress
from pathlib import Path
from typing import cast

from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware

from deployment_api import __version__ as _api_version
from deployment_api import settings
from deployment_api.auth import _auth_cfg as _cloud_cfg
from deployment_api.utils.storage_client import get_storage_client as get_storage_client_with_pool
from deployment_api.workers.auto_sync import (
    _auto_sync_running_deployments,
    get_held_deployment_locks,
    get_owner_id,
    set_background_task_handles,
)

logger = logging.getLogger(__name__)


def get_config_dir() -> Path:
    """Get the configs directory path."""
    # Try relative to this file
    api_dir = Path(__file__).parent
    repo_root = api_dir.parent
    configs_dir = repo_root / "configs"

    if configs_dir.exists():
        return configs_dir

    raise RuntimeError(f"Could not find configs directory at {configs_dir}")


def get_ui_dist_dir() -> Path | None:
    """Get the UI dist directory if it exists (for production serving)."""
    api_dir = Path(__file__).parent
    repo_root = api_dir.parent
    ui_dist = repo_root / "ui" / "dist"

    if ui_dist.exists() and (ui_dist / "index.html").exists():
        return ui_dist
    return None


@asynccontextmanager
async def lifespan(app: FastAPI):  # noqa: C901
    """Application lifespan - startup and shutdown."""
    # Startup
    app.state.config_dir = get_config_dir()
    logger.info("Config directory: %s", app.state.config_dir)

    # Initialize cache
    from .utils.cache import cache

    await cache.initialize()

    # Start background sync task
    shutdown_event = asyncio.Event()
    background_task = asyncio.create_task(_auto_sync_running_deployments())
    logger.info("Background auto-sync task started")

    # Start deployment events drain (for low-latency SSE notify when state is saved from sync code)
    from deployment_api.utils.deployment_events import _drain_sync_queue

    events_drain_task = asyncio.create_task(_drain_sync_queue())
    logger.info("Deployment events drain task started")

    # Store task handles in the auto_sync module
    set_background_task_handles(background_task, events_drain_task, shutdown_event)

    yield

    # Shutdown
    logger.info("Shutting down API...")
    shutdown_event.set()
    if background_task:
        background_task.cancel()
        try:
            await asyncio.wait_for(background_task, timeout=5)
        except asyncio.CancelledError as e:
            logger.debug("Suppressed %s during operation: %s", type(e).__name__, e)
            pass
        except TimeoutError:
            logger.warning("Background auto-sync task did not stop in time")
    if events_drain_task:
        events_drain_task.cancel()
        with suppress(TimeoutError, asyncio.CancelledError):
            await asyncio.wait_for(events_drain_task, timeout=2)

    # Release any held per-deployment locks on graceful shutdown
    try:
        project_id = settings.GCP_PROJECT_ID
        state_bucket = settings.STATE_BUCKET
        owner_id = get_owner_id()
        held_deployment_locks = get_held_deployment_locks()

        client = get_storage_client_with_pool(project_id)  # Uses shared client with large pool
        bucket = client.bucket(state_bucket)

        # Release all locks held by this instance
        released_count = 0
        for deployment_id in list(held_deployment_locks):
            try:
                lock_blob_name = f"locks/deployment_{deployment_id}.lock"
                lock_blob = bucket.blob(lock_blob_name)
                if lock_blob.exists():
                    lock_data = cast(
                        dict[str, object], json.loads(lock_blob.download_as_text() or "{}")
                    )
                    if lock_data.get("owner") == owner_id:
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
        await cache.shutdown()
    except (OSError, ValueError, RuntimeError) as e:
        logger.warning("Cache shutdown failed: %s", e)


def create_app() -> FastAPI:
    """Create and configure the FastAPI application."""
    app = FastAPI(
        title="Deployment Monitoring API",
        description="API for managing and monitoring service deployments",
        version=_api_version,
        lifespan=lifespan,
        docs_url="/docs" if _cloud_cfg.environment != "production" else None,
        redoc_url="/redoc" if _cloud_cfg.environment != "production" else None,
        openapi_url="/openapi.json" if _cloud_cfg.environment != "production" else None,
    )

    # Build CORS allowed origins from env vars
    _api_port = settings.API_PORT
    _frontend_port = settings.FRONTEND_PORT
    allowed_origins = [
        "http://localhost:3000",
        f"http://localhost:{_frontend_port}",
        f"http://127.0.0.1:{_frontend_port}",
        # Keep 5174 allowed for execution-service visualizer UI by default.
        "http://localhost:5174",
        "http://127.0.0.1:5174",
        f"http://localhost:{_api_port}",
        "http://localhost:8080",
    ]
    # Allow specific Cloud Run origins (restrict to known project)
    project_id = settings.GCP_PROJECT_ID or "unknown-project"
    app.add_middleware(
        CORSMiddleware,
        allow_origins=allowed_origins,
        allow_origin_regex=rf"https://{project_id}--.*\.run\.app",
        allow_credentials=True,
        allow_methods=["GET", "POST", "PUT", "DELETE", "OPTIONS"],
        allow_headers=["Content-Type", "Authorization", "X-Requested-With"],
    )

    return app

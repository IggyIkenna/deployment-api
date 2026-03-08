"""
FastAPI Application Entry Point

Main application setup with CORS, routes, and WebSocket support.
Serves the UI static files when deployed (from ui/dist).
Includes background task for auto-syncing running deployment statuses.
"""

import logging
from pathlib import Path
from typing import cast

from fastapi import APIRouter, Depends, FastAPI
from fastapi.staticfiles import StaticFiles
from unified_config_interface import UnifiedCloudConfig
from unified_events_interface import setup_events
from unified_trading_library import setup_tracing

# TODO: PrometheusMiddleware and get_metrics_response are not yet implemented in
# unified_trading_library. Re-enable once available (ISS-xxx).
# from unified_trading_library import PrometheusMiddleware, get_metrics_response

# Event logging for UTD v2 observability (before any log_event)
setup_events(service_name="deployment-api", mode="live", sink="cloud_logging")
setup_tracing("deployment-api")

from deployment_api import __version__ as _api_version
from deployment_api.auth import verify_api_key
from deployment_api.health_routes import router as health_router
from deployment_api.lifespan import lifespan
from deployment_api.middleware import configure_middleware
from deployment_api.utils.service_utils import get_ui_dist_dir

from .routes import (
    capabilities,
    checklist,
    cloud_builds,
    commentary,
    config,
    config_management,
    data_status,
    deployments,
    infra_health,
    service_status,
    services,
)

# Configure logging for the main API process
logging.basicConfig(
    level=logging.INFO,
    format="[API] %(asctime)s - %(levelname)s - %(name)s - %(message)s",
)
logger = logging.getLogger(__name__)

# Create FastAPI app
_env = UnifiedCloudConfig().environment
app = FastAPI(
    title="Deployment Monitoring API",
    description="API for managing and monitoring service deployments",
    version=_api_version,
    lifespan=lifespan,
    docs_url="/docs" if _env != "production" else None,
    redoc_url="/redoc" if _env != "production" else None,
    openapi_url="/openapi.json" if _env != "production" else None,
)

# Configure middleware (CORS, etc.)
configure_middleware(app)

# TODO: Re-enable once PrometheusMiddleware is available in unified_trading_library.
# app.add_middleware(PrometheusMiddleware, service_name="deployment-api")

# --- Authenticated API routes (require API key) ---
_authenticated_router = APIRouter(dependencies=[Depends(verify_api_key)])
_authenticated_router.include_router(services.router, prefix="/api/services", tags=["Services"])
_authenticated_router.include_router(
    deployments.router, prefix="/api/deployments", tags=["Deployments"]
)
_authenticated_router.include_router(config.router, prefix="/api/config", tags=["Configuration"])
_authenticated_router.include_router(
    checklist.router, prefix="/api/checklists", tags=["Checklists"]
)
_authenticated_router.include_router(
    data_status.router, prefix="/api/data-status", tags=["Data Status"]
)
_authenticated_router.include_router(
    service_status.router, prefix="/api/service-status", tags=["Service Status"]
)
_authenticated_router.include_router(
    capabilities.router, prefix="/api/capabilities", tags=["Capabilities"]
)
_authenticated_router.include_router(cloud_builds.router)  # Has its own prefix /api/cloud-builds
_authenticated_router.include_router(config_management.router, prefix="/api")
_authenticated_router.include_router(commentary.router, prefix="/api", tags=["Commentary"])
app.include_router(_authenticated_router)

# --- Unauthenticated health / utility routes (no API key required) ---
app.include_router(health_router)
app.include_router(infra_health.router)  # GET /infra/health — Layer 2 infra verification


@app.get("/metrics")
async def metrics() -> object:
    """Prometheus metrics endpoint."""
    # TODO: Return real Prometheus metrics once PrometheusMiddleware is available in unified_trading_library.  # noqa: E501
    return {"status": "metrics not yet available"}


# Mount static files if UI dist exists (production mode)
_ui_dist = get_ui_dist_dir()
if _ui_dist:
    # Serve static assets (JS, CSS, images)
    app.mount("/assets", StaticFiles(directory=_ui_dist / "assets"), name="assets")
    logger.info("Serving UI static files from %s", _ui_dist)


# Override the health check to include config_dir from app state
@app.get("/api/health")
async def health_check_with_config() -> dict[str, object]:
    """Detailed health check. Includes GCS FUSE status for UI display."""
    from deployment_api.utils.storage_facade import get_gcs_fuse_status

    return {
        "status": "healthy",
        "version": _api_version,
        "config_dir": (
            str(cast(Path, app.state.config_dir)) if hasattr(app.state, "config_dir") else None
        ),
        "gcs_fuse": get_gcs_fuse_status(),
    }

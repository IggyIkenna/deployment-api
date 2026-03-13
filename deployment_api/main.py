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
from fastapi.responses import Response
from fastapi.staticfiles import StaticFiles
from prometheus_client import CONTENT_TYPE_LATEST, generate_latest
from unified_events_interface import setup_events
from unified_trading_library import (
    PubSubEventSink,
    RequestAuditMiddleware,
    make_events_relay_router,
    setup_tracing,
)

from deployment_api.deployment_api_config import DeploymentApiConfig
from deployment_api.metrics import PROCESSING_LATENCY, RECORDS_PROCESSED

__all__ = ["PROCESSING_LATENCY", "RECORDS_PROCESSED"]

_cfg = DeploymentApiConfig()
_event_sink = PubSubEventSink(
    project_id=_cfg.gcp_project_id,
    topic="deployment-api-events",
    service_name="deployment-api",
)
# Event logging for UTD v2 observability (before any log_event)
setup_events(service_name="deployment-api", mode="live", sink=_event_sink)
setup_tracing("deployment-api")

from deployment_api import __version__ as _api_version
from deployment_api.auth import auth_cfg as _auth_cfg
from deployment_api.auth import verify_api_key
from deployment_api.health_routes import router as health_router
from deployment_api.lifespan import lifespan
from deployment_api.middleware import (
    CorrelationIdMiddleware,
    PrometheusMiddleware,
    configure_middleware,
)
from deployment_api.utils.service_utils import get_ui_dist_dir

from .routes import (
    builds,
    capabilities,
    checklist,
    cloud_builds,
    commentary,
    config,
    config_management,
    data_status,
    deployments,
    epics,
    infra_health,
    service_status,
    services,
)

logger = logging.getLogger(__name__)

# Create FastAPI app
_env = _auth_cfg.environment
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

app.add_middleware(PrometheusMiddleware, service_name="deployment-api")  # pyright: ignore[reportArgumentType]
app.add_middleware(CorrelationIdMiddleware)
app.add_middleware(RequestAuditMiddleware)

# --- Authenticated API routes (require API key) ---
_authenticated_router = APIRouter(dependencies=[Depends(verify_api_key)])
_authenticated_router.include_router(services.router, prefix="/api/services", tags=["Services"])
_authenticated_router.include_router(deployments.router, prefix="/api", tags=["Deployments"])
_authenticated_router.include_router(config.router, prefix="/api/config", tags=["Configuration"])
_authenticated_router.include_router(
    checklist.router, prefix="/api/checklists", tags=["Checklists"]
)
_authenticated_router.include_router(epics.router, prefix="/api/epics", tags=["Epics"])
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
_authenticated_router.include_router(
    builds.router
)  # /api/builds/{service} + /api/deployments/{service}/deploy
_authenticated_router.include_router(config_management.router, prefix="/api")
_authenticated_router.include_router(commentary.router, prefix="/api", tags=["Commentary"])
app.include_router(_authenticated_router)

# --- Unauthenticated health / utility routes (no API key required) ---
app.include_router(health_router)
app.include_router(infra_health.router)  # GET /infra/health — Layer 2 infra verification
app.include_router(make_events_relay_router())


@app.get("/metrics", include_in_schema=False)
async def metrics() -> Response:
    """Prometheus metrics endpoint."""
    return Response(content=generate_latest(), media_type=CONTENT_TYPE_LATEST)


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
    from unified_config_interface import UnifiedCloudConfig

    from deployment_api.utils.storage_facade import get_gcs_fuse_status

    _cloud_cfg = UnifiedCloudConfig()
    return {
        "status": "healthy",
        "version": _api_version,
        "config_dir": (
            str(cast(Path, app.state.config_dir)) if hasattr(app.state, "config_dir") else None
        ),
        "gcs_fuse": get_gcs_fuse_status(),
        "cloud_provider": _cloud_cfg.cloud_provider,
        "mock_mode": _cloud_cfg.cloud_mock_mode,
    }

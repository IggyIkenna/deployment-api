"""
FastAPI Application Entry Point

Main application setup with CORS, routes, and WebSocket support.
Serves the UI static files when deployed (from ui/dist).
Includes background task for auto-syncing running deployment statuses.
"""

import logging
import uuid
from pathlib import Path
from typing import cast

from fastapi import APIRouter, Depends, FastAPI, HTTPException, Request
from fastapi.responses import FileResponse, JSONResponse, Response
from fastapi.staticfiles import StaticFiles
from prometheus_client import CONTENT_TYPE_LATEST, generate_latest
from pydantic import BaseModel, Field
from unified_trading_library import (
    PubSubEventSink,
    RequestAuditMiddleware,
    make_events_relay_router,
    setup_events,
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
from deployment_api.firebase_auth import verify_any_auth
from deployment_api.health_routes import router as health_router
from deployment_api.lifespan import lifespan
from deployment_api.middleware import (
    CorrelationIdMiddleware,
    PrometheusMiddleware,
    configure_middleware,
)
from deployment_api.utils.service_utils import get_ui_dist_dir

from .routes import (
    backfill_launch,
    builds,
    builds_history,
    capabilities,
    chaos_injections,
    checklist,
    client_treasury,
    cloud_builds,
    commentary,
    config,
    config_management,
    data_status,
    deploy_events_sse,
    deployments,
    epics,
    execution_backtest_launch,
    fixtures,
    infra_health,
    kill_switch_routes,
    manual_pending,
    ml_experiment_launch,
    promote,
    recursive_borrow_coverage,
    repo_readiness,
    risk_routes,
    service_status,
    services,
    shard_detail,
    sports_venues,
    strategy_backtest_launch,
    strategy_runs,
    subscriptions,
    treasury,
    treasury_routes,
    user_management,
    vm_deployments,
    vm_events,
    vm_events_ws,
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


# --- Standard error handler ---
@app.exception_handler(HTTPException)
async def standard_error_handler(request: Request, exc: HTTPException) -> JSONResponse:
    """Return errors in a standard envelope: {error: {code, message, details}, request_id}."""
    request_id: str = getattr(request.state, "request_id", str(uuid.uuid4()))
    raw_detail: str | dict[str, str] = (
        exc.detail if isinstance(exc.detail, dict) else {"message": str(exc.detail)}
    )
    message = (
        raw_detail.get("message", str(exc.detail))
        if isinstance(raw_detail, dict)
        else str(raw_detail)
    )
    return JSONResponse(
        status_code=exc.status_code,
        content={
            "error": {
                "code": f"HTTP_{exc.status_code}",
                "message": message,
                "details": raw_detail if isinstance(raw_detail, dict) else None,
            },
            "request_id": request_id,
        },
    )


# --- Authenticated API routes (accept X-API-Key or Firebase Bearer token) ---
_authenticated_router = APIRouter(dependencies=[Depends(verify_any_auth)])
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
    shard_detail.router, prefix="/api/data-status", tags=["Data Status"]
)
_authenticated_router.include_router(
    recursive_borrow_coverage.router,
    prefix="/api/data-status",
    tags=["Data Status"],
)
_authenticated_router.include_router(fixtures.router, prefix="/api")
_authenticated_router.include_router(
    service_status.router, prefix="/api/service-status", tags=["Service Status"]
)
_authenticated_router.include_router(
    capabilities.router, prefix="/api/capabilities", tags=["Capabilities"]
)
_authenticated_router.include_router(cloud_builds.router)  # Has its own prefix /api/cloud-builds
_authenticated_router.include_router(
    subscriptions.router, prefix="/api", tags=["Client Subscriptions"]
)
_authenticated_router.include_router(
    chaos_injections.router, prefix="/api", tags=["Chaos Injection"]
)
_authenticated_router.include_router(
    builds_history.router, prefix="/api/builds", tags=["Builds"]
)  # must be registered BEFORE builds.router (static /history before /{service})
_authenticated_router.include_router(
    builds.router
)  # /api/builds/{service} + /api/deployments/{service}/deploy
_authenticated_router.include_router(config_management.router, prefix="/api")
_authenticated_router.include_router(commentary.router, prefix="/api", tags=["Commentary"])
_authenticated_router.include_router(sports_venues.router)
_authenticated_router.include_router(
    user_management.router, prefix="/api/user-management", tags=["User Management"]
)
_authenticated_router.include_router(vm_deployments.router, prefix="/api", tags=["VM Deployments"])
_authenticated_router.include_router(
    backfill_launch.router, prefix="/api/backfill", tags=["Backfill"]
)
_authenticated_router.include_router(
    ml_experiment_launch.router, prefix="/api/ml/experiment", tags=["ML Experiment"]
)
_authenticated_router.include_router(
    strategy_backtest_launch.router, prefix="/api/strategy/backtest", tags=["Strategy Backtest"]
)
_authenticated_router.include_router(
    execution_backtest_launch.router, prefix="/api/execution/backtest", tags=["Execution Backtest"]
)
_authenticated_router.include_router(vm_events.router, prefix="/api/vm", tags=["VM Events"])
_authenticated_router.include_router(risk_routes.router, prefix="/api/risk", tags=["Risk"])
_authenticated_router.include_router(repo_readiness.router, prefix="/api/repos", tags=["Repos"])
_authenticated_router.include_router(strategy_runs.router, prefix="/api", tags=["Strategy Runs"])
_authenticated_router.include_router(promote.router, prefix="/api", tags=["Promote Workflow"])
_authenticated_router.include_router(
    manual_pending.router, prefix="/api", tags=["Manual Pending Queue"]
)
_authenticated_router.include_router(treasury.router, prefix="/api", tags=["Treasury"])
_authenticated_router.include_router(
    treasury_routes.router, prefix="/api/treasury", tags=["Treasury"]
)
# /treasury/nav is intentionally also exposed without the /api prefix
# (matches plan spec "/treasury/nav?client_id=<id>") so include on app directly too.
app.include_router(treasury_routes.router, prefix="/treasury", tags=["Treasury"], dependencies=[])
# Phase 6.A + 6.B: per-client attribution view + subscription list.
# CONSUMER role — builds on /api/treasury/rollup (Phase 3.D canonical).
_authenticated_router.include_router(client_treasury.router, prefix="/api", tags=["Treasury"])
# Kill-switch router already declares /api/kill-switch prefix + verify_api_key
# dependency internally; include directly on app so we don't double-gate.
app.include_router(kill_switch_routes.router)
app.include_router(_authenticated_router)


# Register /metrics BEFORE health_router to avoid the health_router /{full_path:path}
# catch-all intercepting it (routes are matched in registration order).
@app.get("/metrics", include_in_schema=False)
async def metrics() -> Response:
    """Prometheus metrics endpoint."""
    return Response(content=generate_latest(), media_type=CONTENT_TYPE_LATEST)


# /api/openapi.json — convenience alias for the auto-generated FastAPI schema.
# Registered before health_router to prevent the catch-all from intercepting it.
@app.get("/api/openapi.json", include_in_schema=False)
async def api_openapi_spec() -> JSONResponse:
    """Return the auto-generated OpenAPI 3.x schema for this service."""
    return JSONResponse(app.openapi())


# --- Unauthenticated health / utility routes (no API key required) ---
app.include_router(health_router)
app.include_router(infra_health.router)  # GET /infra/health — Layer 2 infra verification
app.include_router(make_events_relay_router())
app.include_router(deploy_events_sse.router)  # GET /stream/deploy-events (SSE)
app.include_router(vm_events_ws.router)  # WS /ws/vm/{vm_name}/events — live event stream


class PipelineTriggerRequest(BaseModel):  # CORRECT-LOCAL: FastAPI API contract model
    """Request body for triggering a data pipeline."""

    date: str = Field(..., description="Date to process (YYYY-MM-DD)")
    venue: str = Field(..., description="Venue identifier")
    instrument: str | None = Field(None, description="Instrument filter")


@app.post("/pipeline/trigger", include_in_schema=False)
async def pipeline_trigger(request: PipelineTriggerRequest) -> dict[str, object]:
    """Trigger a data pipeline run for a given date and venue.

    In mock mode, returns a synthetic pipeline_id without triggering real work.
    In production mode, delegates to the deployment manager.
    """
    if _cfg.is_mock_mode():
        return {
            "pipeline_id": "mock-pipeline-001",
            "status": "triggered",
            "date": request.date,
            "venue": request.venue,
        }
    # Production path: not yet implemented
    raise HTTPException(status_code=501, detail="Pipeline trigger not yet implemented")


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
    from unified_trading_library import UnifiedCloudConfig

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
        "mock_mode": _cloud_cfg.is_mock_mode(),
    }


# SPA fallback — registered LAST so all named routes (api, metrics, docs,
# events relay, /assets mount) win first. Any GET that lands here either
# resolves to a real file under the UI dist (favicon, manifest.json, etc.)
# or falls through to index.html so client-side router can handle the path.
if _ui_dist:
    _ui_index = _ui_dist / "index.html"

    @app.get("/", include_in_schema=False)
    async def spa_root() -> FileResponse:
        return FileResponse(_ui_index)

    @app.get("/{full_path:path}", include_in_schema=False)
    async def spa_catchall(full_path: str) -> FileResponse:
        # Reserved prefixes that must NOT be intercepted (would mask real 404s).
        if (
            full_path.startswith("api/")
            or full_path.startswith("assets/")
            or full_path.startswith("stream/")
            or full_path.startswith("infra/")
            or full_path in {"metrics", "docs", "redoc", "openapi.json", "ws"}
        ):
            raise HTTPException(status_code=404, detail="Not Found")
        candidate = _ui_dist / full_path
        if candidate.is_file():
            return FileResponse(candidate)
        return FileResponse(_ui_index)

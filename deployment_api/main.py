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
    RequestAuditMiddleware,
    UnifiedCloudConfig,
    make_events_relay_router,
    setup_cloud_logging,
)

from deployment_api.deployment_api_config import DeploymentApiConfig
from deployment_api.metrics import PROCESSING_LATENCY, RECORDS_PROCESSED

__all__ = ["PROCESSING_LATENCY", "RECORDS_PROCESSED"]

_cfg = DeploymentApiConfig()

from deployment_api import __version__ as _api_version
from deployment_api.auth import auth_cfg as _auth_cfg
from deployment_api.firebase_auth import verify_any_auth
from deployment_api.health_routes import router as health_router
from deployment_api.lifespan import lifespan
from deployment_api.middleware import (
    CorrelationIdMiddleware,
    PrometheusMiddleware,
    RateLimitMiddleware,
    configure_middleware,
)
from deployment_api.utils.service_utils import get_ui_dist_dir

from .routes import (
    _idle_spend_scheduler,
    _reap_scheduler,
    artifacts,
    backfill_launch,
    builds,
    builds_history,
    capabilities,
    catalogue_lifecycle,
    change_freeze,
    chaos_injections,
    checklist,
    client_treasury,
    cloud_builds,
    commentary,
    config,
    config_management,
    costs,
    data_status,
    data_status_tardis_windows,
    deploy_events_sse,
    deployment_diff,
    deployment_digest,
    deployment_freshness,
    deployments,
    deployments_inventory,
    epics,
    execution_backtest_launch,
    fixtures,
    fixtures_browse,
    fleet,
    fleet_reconciliation,
    health_consolidator,
    health_overview,
    infra_health,
    kill_switch_routes,
    log_stream,
    manual_pending,
    ml_experiment_launch,
    monitor_backfill,
    monitor_experiments,
    monitor_live,
    monitor_scheduled,
    prediction_catalogue,
    promote,
    recursive_borrow_coverage,
    repo_ci,
    repo_coverage,
    repo_gh_rate_limit,
    repo_readiness,
    risk_routes,
    scenarios,
    service_status,
    services,
    shard_detail,
    sports_venues,
    strategy_backtest_launch,
    strategy_runs,
    strategy_shard,
    subscriptions,
    treasury,
    treasury_routes,
    unified_alerts,
    user_management,
    venue_credentials,
    venue_date_ranges,
    venue_relaunch_estimate,
    version_coherence,
    vm_admin,
    vm_cost_estimate,
    vm_deployments,
    vm_events,
    vm_events_ws,
    vm_health,
    vm_resource_history,
    watchdog_events,
)

# MEASURED 2026-07-24: no handler was ever attached to the root logger in this process (no
# basicConfig / ServiceBootstrap-style setup existed), so every logger.warning()/logger.error()
# call anywhere in this app — including artifact_pipeline.providers.safe()'s per-source failure
# isolation — was silently discarded. Cloud Run only ships whatever the container actually writes
# to stdout/stderr; with zero handlers there was nothing to ship, so prod ran with ZERO
# application-level log lines in Cloud Logging (confirmed via `gcloud logging read`, only the
# auto-generated HTTP access logs existed).
#
# MEASURED 2026-07-31 (plans/active/issues/deployment_api_sigabrt_crash_loop_2026_07_24.md, the
# "stdout/stderr blackout" todo): a bare `logging.basicConfig(level=logging.INFO)` was still not
# enough — its default formatter emits plain, unstructured text with no recognizable severity, and
# this project's `_Default` Cloud Logging sink has a cost-control exclusion
# (`severity <= "DEBUG" AND NOT resource.type="cloud_run_job"`, see
# gcs_data_access_audit_log_cost_2026_07_24.md's sibling sink-exclusion work). Cloud Run's log
# ingestion stamps any non-JSON-structured stdout/stderr line with `severity=DEFAULT` (0), which is
# `<= DEBUG` (100) — so EVERY plain-text log line (including gunicorn's own hook lines and
# faulthandler dumps) was silently excluded, sink-side, regardless of what the app itself logged.
# Confirmed empirically on this exact service via 4 live zero-traffic canary deploys: a bare
# unstructured stdout write (with or without `--no-cpu-throttling`, ruling that out too) never
# appeared in Cloud Logging, while a structured `{"severity": "INFO", ...}` JSON line on stdout
# did. Switching to
# `setup_cloud_logging()` (this repo's own `CloudRunJSONFormatter`, already built for exactly this
# — GCP-recognized structured JSON with an explicit `severity` field) is the fix: it survives the
# exclusion at INFO and above, matching this app's own `logging.INFO` level.
setup_cloud_logging(log_level="INFO", json_format=True)

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
app.add_middleware(RateLimitMiddleware, requests_per_minute=60)  # pyright: ignore[reportArgumentType]


# --- Standard error handler ---
@app.exception_handler(HTTPException)
async def standard_error_handler(request: Request, exc: HTTPException) -> JSONResponse:
    """Return errors in a standard envelope: {error: {code, message, details}, request_id}."""
    request_id: str = getattr(request.state, "request_id", str(uuid.uuid4()))

    if isinstance(exc.detail, dict):
        raw_detail: dict[str, object] = cast(dict[str, object], exc.detail)
        message_value = raw_detail.get("message", str(exc.detail))
        message: str = str(message_value)
    else:
        raw_detail = {"message": str(exc.detail)}
        message = str(exc.detail)
    return JSONResponse(
        status_code=exc.status_code,
        content={
            "error": {
                "code": f"HTTP_{exc.status_code}",
                "message": message,
                "details": raw_detail,
            },
            "request_id": request_id,
        },
    )


# --- Authenticated API routes (accept X-API-Key or Firebase Bearer token) ---
_authenticated_router = APIRouter(dependencies=[Depends(verify_any_auth)])
_authenticated_router.include_router(services.router, prefix="/api/services", tags=["Services"])
# Inventory router FIRST: its literal ``/deployments/inventory`` +
# ``/deployments/umbrella/{umbrella}/summary`` routes MUST register before
# deployments.router's parametric ``/deployments/{deployment_id}`` — FastAPI
# matches in registration order, so the parametric route otherwise shadows the
# literal (``GET /api/deployments/inventory`` → get_deployment_status("inventory")
# → 404 state.json → 500, which broke the cockpit Live/Batch/Paper tabs;
# caught on real cloud 2026-06-24). A real deployment id still falls through to
# the parametric route (it doesn't match the literals).
_authenticated_router.include_router(deployments_inventory.router, prefix="/api", tags=["Deployment Inventory"])
_authenticated_router.include_router(deployment_digest.router, prefix="/api/deployments", tags=["Deployment Digest"])
# Freshness BEFORE the parametric deployments.router (same shadowing reason): its
# ``/deployments/{deployment_id}/freshness`` must register ahead of the parametric CRUD.
_authenticated_router.include_router(deployment_freshness.router, prefix="/api", tags=["Deployment Freshness"])
# deployment_diff BEFORE the parametric deployments.router (same shadowing reason): its
# literal ``/deployments/diff`` was being swallowed by ``/deployments/{deployment_id}``
# (GET /api/deployments/diff -> get_deployment_status("diff") -> 404 "not found (mock)"),
# making the endpoint permanently unreachable — found via the mock-endpoint smoke gate.
_authenticated_router.include_router(deployment_diff.router, tags=["Deployments"])
_authenticated_router.include_router(deployments.router, prefix="/api", tags=["Deployments"])
_authenticated_router.include_router(config.router, prefix="/api/config", tags=["Configuration"])
_authenticated_router.include_router(checklist.router, prefix="/api/checklists", tags=["Checklists"])
_authenticated_router.include_router(epics.router, prefix="/api/epics", tags=["Epics"])
_authenticated_router.include_router(data_status.router, prefix="/api/data-status", tags=["Data Status"])
_authenticated_router.include_router(shard_detail.router, prefix="/api/data-status", tags=["Data Status"])
_authenticated_router.include_router(
    recursive_borrow_coverage.router,
    prefix="/api/data-status",
    tags=["Data Status"],
)
_authenticated_router.include_router(fixtures.router, prefix="/api")
_authenticated_router.include_router(fixtures_browse.router, prefix="/api")
_authenticated_router.include_router(catalogue_lifecycle.router, prefix="/api")
_authenticated_router.include_router(prediction_catalogue.router, prefix="/api")
_authenticated_router.include_router(service_status.router, prefix="/api/service-status", tags=["Service Status"])
_authenticated_router.include_router(capabilities.router, prefix="/api/capabilities", tags=["Capabilities"])
_authenticated_router.include_router(cloud_builds.router)  # Has its own prefix /api/cloud-builds
_authenticated_router.include_router(subscriptions.router, prefix="/api", tags=["Client Subscriptions"])
_authenticated_router.include_router(chaos_injections.router, prefix="/api", tags=["Chaos Injection"])
_authenticated_router.include_router(
    builds_history.router, prefix="/api/builds", tags=["Builds"]
)  # must be registered BEFORE builds.router (static /history before /{service})
_authenticated_router.include_router(builds.router)  # /api/builds/{service} + /api/deployments/{service}/deploy
_authenticated_router.include_router(config_management.router, prefix="/api")
_authenticated_router.include_router(commentary.router, prefix="/api", tags=["Commentary"])
_authenticated_router.include_router(sports_venues.router)
_authenticated_router.include_router(user_management.router, prefix="/api/user-management", tags=["User Management"])
_authenticated_router.include_router(vm_deployments.router, prefix="/api", tags=["VM Deployments"])
_authenticated_router.include_router(backfill_launch.router, prefix="/api/backfill", tags=["Backfill"])
_authenticated_router.include_router(ml_experiment_launch.router, prefix="/api/ml/experiment", tags=["ML Experiment"])
_authenticated_router.include_router(
    strategy_backtest_launch.router, prefix="/api/strategy/backtest", tags=["Strategy Backtest"]
)
_authenticated_router.include_router(
    execution_backtest_launch.router, prefix="/api/execution/backtest", tags=["Execution Backtest"]
)
_authenticated_router.include_router(venue_credentials.router, tags=["Venue Credentials"])
_authenticated_router.include_router(data_status_tardis_windows.router, tags=["Data Status"])
_authenticated_router.include_router(venue_date_ranges.router, tags=["Venue Date Ranges"])
_authenticated_router.include_router(venue_relaunch_estimate.router, tags=["Venue Relaunch Estimate"])
_authenticated_router.include_router(vm_admin.router, prefix="/api", tags=["VM Admin"])
_authenticated_router.include_router(vm_cost_estimate.router, tags=["VM Cost"])
_authenticated_router.include_router(vm_events.router, prefix="/api/vm", tags=["VM Events"])
_authenticated_router.include_router(vm_health.router, prefix="/api", tags=["VM Health"])
_authenticated_router.include_router(vm_resource_history.router)  # Has its own prefix /api/vm-resources
_authenticated_router.include_router(watchdog_events.router)  # Has its own prefix /api/watchdog
_authenticated_router.include_router(costs.router, prefix="/api", tags=["Costs"])
_authenticated_router.include_router(artifacts.router, prefix="/api", tags=["Artifacts"])
_authenticated_router.include_router(risk_routes.router, prefix="/api/risk", tags=["Risk"])
_authenticated_router.include_router(repo_readiness.router, prefix="/api/repos", tags=["Repos"])
_authenticated_router.include_router(repo_ci.router)  # Has its own prefix /api/repo-ci
_authenticated_router.include_router(version_coherence.router)  # Has its own prefix /api/version-coherence
_authenticated_router.include_router(change_freeze.router)  # Has its own prefix /api/change-freeze
_authenticated_router.include_router(fleet.router)  # Has its own prefix /api/fleet
_authenticated_router.include_router(fleet_reconciliation.router, prefix="/api", tags=["Fleet Reconciliation"])
_authenticated_router.include_router(unified_alerts.router, prefix="/api", tags=["Alerts"])  # GET /api/alerts
_authenticated_router.include_router(health_overview.router, prefix="/api", tags=["Health"])  # GET /api/health/overview
_authenticated_router.include_router(
    health_consolidator.router, prefix="/api", tags=["Health"]
)  # GET /api/health/consolidator
_authenticated_router.include_router(repo_coverage.router, prefix="/api/repos", tags=["Repos"])
_authenticated_router.include_router(repo_gh_rate_limit.router, prefix="/api/repos", tags=["Repos"])
_authenticated_router.include_router(scenarios.router, prefix="/api/scenarios", tags=["Scenarios"])
_authenticated_router.include_router(strategy_runs.router, prefix="/api", tags=["Strategy Runs"])
_authenticated_router.include_router(strategy_shard.router)  # prefix=/api/strategy/shard (set on router)
_authenticated_router.include_router(promote.router, prefix="/api", tags=["Promote Workflow"])
_authenticated_router.include_router(manual_pending.router, prefix="/api", tags=["Manual Pending Queue"])
_authenticated_router.include_router(monitor_backfill.router, prefix="/api", tags=["Monitor"])
_authenticated_router.include_router(monitor_experiments.router, prefix="/api", tags=["Monitor"])
_authenticated_router.include_router(monitor_live.router, prefix="/api", tags=["Monitor"])
_authenticated_router.include_router(monitor_scheduled.router, prefix="/api", tags=["Monitor"])
_authenticated_router.include_router(log_stream.router, tags=["Log Stream"])  # GET /api/logs/stream/{target_ref}
_authenticated_router.include_router(treasury.router, prefix="/api", tags=["Treasury"])
_authenticated_router.include_router(treasury_routes.router, prefix="/api/treasury", tags=["Treasury"])
# /treasury/nav is intentionally also exposed without the /api prefix
# (matches plan spec "/treasury/nav?client_id=<id>") so include on app directly too.
app.include_router(treasury_routes.router, prefix="/treasury", tags=["Treasury"], dependencies=[])
# Phase 6.A + 6.B: per-client attribution view + subscription list.
# CONSUMER role — builds on /api/treasury/rollup (Phase 3.D canonical).
_authenticated_router.include_router(client_treasury.router, prefix="/api", tags=["Treasury"])
# Kill-switch router already declares /api/kill-switch prefix + verify_api_key
# dependency internally; include directly on app so we don't double-gate.
app.include_router(kill_switch_routes.router)
# Cloud Scheduler reap-tick: OIDC-authed internally (verify_reap_scheduler_oidc), NOT
# verify_any_auth (X-API-Key/Firebase is the wrong scheme for a machine-to-machine
# Cloud Scheduler caller) — include directly on app, not under _authenticated_router.
app.include_router(_reap_scheduler.router, prefix="/api", tags=["Deployment Registry Reaper"])
# Same OIDC scheme as the reap-tick above (same invoker SA, same trust boundary).
app.include_router(_idle_spend_scheduler.router, prefix="/api", tags=["Idle Spend Snapshot"])
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
    from deployment_api.utils.storage_facade import get_gcs_fuse_status

    _cloud_cfg = UnifiedCloudConfig()
    return {
        "status": "healthy",
        "version": _api_version,
        "config_dir": (str(cast(Path, app.state.config_dir)) if hasattr(app.state, "config_dir") else None),
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
        if _ui_dist is not None:
            candidate = _ui_dist / full_path
            if candidate.is_file():
                return FileResponse(candidate)
        return FileResponse(_ui_index)

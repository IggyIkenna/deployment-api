"""
VM Deployments API — active + recent-archive view of VM-spawned jobs.

These are jobs fired by `vm-exec-with-gcs-tee.sh` (canonical migrations, backfills,
forward-polls, smoke runs). The GCS-backed registry lives at:

    gs://<bucket>/deployments/active/<id>.json
    gs://<bucket>/deployments/archive/<YYYY-MM-DD>/<id>.json

Distinct from the existing `/api/deployments` routes which cover Cloud Run /
VM deployments launched by deployment-service proper. Those are "I'm rolling out
a new version of market-tick-data-service"; these are "a batch VM is crunching
2024-06-01..2024-06-30 canonical-migration right now."
"""

from __future__ import annotations

import logging
import threading
import time
from concurrent.futures import ThreadPoolExecutor
from dataclasses import asdict
from datetime import UTC, datetime
from typing import cast

from fastapi import APIRouter, HTTPException, Query
from pydantic import BaseModel, Field
from unified_trading_library import (
    DEFAULT_BUCKET,
    DeploymentRegistryEntry,
    DeploymentsRegistry,
    vm_run_log_final_uri,
    vm_serial_rolling_uri,
)

from deployment_api.deployment_api_config import DeploymentApiConfig
from deployment_api.registry_reader import resolve_active_registry, resolve_deployment_by_id
from deployment_api.utils.request_memory_profiling import log_rss_delta
from deployment_api.vm_utils import get_vm_instance_details

router = APIRouter()
logger = logging.getLogger(__name__)

_cfg = DeploymentApiConfig()


class VmDeploymentEntryModel(BaseModel):  # CORRECT-LOCAL: FastAPI API contract model
    """API-side view of one deployments_registry entry."""

    deployment_id: str
    vm_name: str
    asset_group: str
    task: str
    mode: str
    start_date: str
    end_date: str
    status: str
    started_at: str
    last_heartbeat_at: str
    completed_at: str | None = None
    exit_code: int | None = None
    rows_in: int = 0
    rows_out: int = 0
    rows_error: int = 0
    events_emitted: int = 0
    log_uri: str = ""
    archive_run_log_uri: str = ""
    archive_serial_uri: str = ""
    machine_type: str | None = None
    zone: str | None = None
    uptime_hours: float | None = None
    health_status: str | None = None  # "producing", "stalled", "boot-hung", etc.
    # Build provenance (bill-of-materials) — stamped on the registry entry at launch
    # time (deployment_service.bom). `_to_model` builds from `asdict(entry)` and
    # pydantic silently DROPS unknown keys, so without these declarations the BoM
    # reaches the GCS rows but never the API response. "" / {} = honestly unknown.
    image_digest: str = ""
    git_commit: str = ""
    dep_versions: dict[str, str] = Field(default_factory=dict)  # type: ignore[reportUnknownVariableType]


class VmDeploymentsListModel(BaseModel):  # CORRECT-LOCAL: FastAPI API contract model
    """Combined list of active + recent-archive VM deployments."""

    active: list[VmDeploymentEntryModel] = Field(default_factory=list)  # type: ignore[reportUnknownVariableType]
    recent: list[VmDeploymentEntryModel] = Field(default_factory=list)  # type: ignore[reportUnknownVariableType]
    archive_days: int


def _calculate_health_status(entry: DeploymentRegistryEntry, is_running: bool) -> str:
    """Determine health status based on heartbeat age and progress."""
    if not is_running:
        return "stopped"

    try:
        last_hb = datetime.fromisoformat(entry.last_heartbeat_at.replace("Z", "+00:00"))
        age_minutes = (datetime.now(UTC) - last_hb).total_seconds() / 60

        if age_minutes > 15:
            return "stalled"
        elif entry.rows_out > 0 or entry.events_emitted > 0:
            return "producing"
        elif age_minutes < 5:
            return "starting"
        else:
            return "idle"
    except (ValueError, TypeError, AttributeError):
        return "unknown"


def _to_model(
    entry: DeploymentRegistryEntry, vm_details: dict[str, dict[str, object]] | None = None
) -> VmDeploymentEntryModel:
    data = asdict(entry)
    data.pop("extras", None)

    # Add VM instance details if available
    if vm_details and entry.vm_name in vm_details:
        details = vm_details[entry.vm_name]
        data["machine_type"] = details.get("machine_type")
        data["zone"] = details.get("zone")

        # Calculate uptime if creation timestamp available
        if creation_ts := details.get("creation_timestamp"):
            try:
                created = datetime.fromisoformat(str(creation_ts).replace("Z", "+00:00"))
                data["uptime_hours"] = round((datetime.now(UTC) - created).total_seconds() / 3600, 2)
            except (ValueError, TypeError):
                pass

        # Determine health status
        is_running = details.get("status") == "RUNNING"
        data["health_status"] = _calculate_health_status(entry, is_running)

    # Serial console: still rolling-archived daily (a separate mechanism from run.log,
    # unaffected by the run.log final-snapshot fix below) — no 14-day TTL unlike vm-logs/.
    completed_at = data.get("completed_at")
    if completed_at and isinstance(completed_at, str) and len(completed_at) >= 10:
        try:
            date_stamp = completed_at[:10].replace("-", "")  # YYYYMMDD
            vm_name = str(data.get("vm_name", ""))  # noqa: qg-empty-fallback — registry blob display default
            if vm_name and date_stamp.isdigit():
                data["archive_serial_uri"] = vm_serial_rolling_uri(vm_name, date_stamp)
        except (OSError, ValueError, RuntimeError):
            pass

    # Run-log archive candidate (WS-4 decision 2): the live path (log_uri, already set
    # above from the registry entry) is the primary read path for ANY vm regardless of
    # completed_at; archive_run_log_uri is the durable final-snapshot fallback, written
    # once at actual VM completion (a fixed, deterministic path — no date needed).
    # Replaces the broken completed_at[:10]-keyed rolling-date guess (vm_run_log_rolling_uri),
    # which 404s because the archiver writes daily rolling copies keyed by cron-run date,
    # not completion date.
    if completed_at and isinstance(completed_at, str) and len(completed_at) >= 10:
        vm_name = str(data.get("vm_name", ""))  # noqa: qg-empty-fallback — registry blob display default
        if vm_name:
            data["archive_run_log_uri"] = vm_run_log_final_uri(vm_name)

    return VmDeploymentEntryModel(**cast(dict[str, object], data))  # type: ignore[reportArgumentType]


# Mock-fixture URI placeholder: the literal shell-style token the VM launcher
# writes into registry blobs (substituted at launch time) -- NOT an env read.
_MOCK_PID_TOKEN = "${GCP_PROJECT_ID}"  # noqa: qg-gcp-project-id


def _mock_entry(**kwargs: object) -> VmDeploymentEntryModel:
    defaults: dict[str, object] = {
        "deployment_id": "dep-mock-1",
        "vm_name": "canonical-migration-cefi-20260418-042359",
        "asset_group": "CEFI",
        "task": "canonical-migration",
        "mode": "dry",
        "start_date": "2024-06-01",
        "end_date": "2024-06-30",
        "status": "running",
        "started_at": "2026-04-18T04:23:59Z",
        "last_heartbeat_at": "2026-04-18T04:28:59Z",
        "completed_at": None,
        "exit_code": None,
        "rows_in": 12_000,
        "rows_out": 11_987,
        "rows_error": 13,
        "events_emitted": 42,
        "log_uri": f"gs://deployment-scripts-{_MOCK_PID_TOKEN}/vm-logs/canonical-migration-cefi-20260418-042359/run.log",  # noqa: gs-uri (mock fixture URI)
        "archive_run_log_uri": "",
        "archive_serial_uri": "",
    }
    defaults.update(kwargs)
    return VmDeploymentEntryModel(**defaults)  # type: ignore[reportArgumentType]


# Stale-while-revalidate snapshot cache for list_vm_deployments — mirrors
# routes/deployments_inventory.py's `_load_inventory` / `_inventory_cache` (same
# proven shape: a full uncached walk of this registry (active + N-day archive, both
# GCS-backed) plus a per-VM Compute API census measured at avg 93.75s / max 99.27s in
# prod, occasionally 503/500ing outright under load. SWR serves the last-known-good
# snapshot instantly, refreshes in the background on a timer, and collapses concurrent
# refreshes for the same cache key to a single in-flight walk.
_VM_DEPLOYMENTS_TTL_SEC = 45.0

_vm_deployments_cache: dict[str, tuple[float, VmDeploymentsListModel]] = {}
_vm_deployments_lock = threading.Lock()
# cache keys with an in-flight background refresh (so we kick off exactly one).
_vm_deployments_refreshing: set[str] = set()
_vm_deployments_refresh_pool = ThreadPoolExecutor(max_workers=2, thread_name_prefix="vmdeploy-refresh")


def _compute_vm_deployments(days: int, filter_stale: bool) -> VmDeploymentsListModel:
    """The real (slow) synchronous registry walk + Compute API census.

    Never call directly from a route — go through `_load_vm_deployments` so callers
    get the stale-while-revalidate cache.
    """
    registry = DeploymentsRegistry(bucket=DEFAULT_BUCKET)
    project_id = _cfg.require_gcp_project_id()

    # Instrumented (not the cache-hit path in `_load_vm_deployments`) — this is the
    # actual heavy synchronous walk (measured avg 93.75s/max 99.27s in prod) that only
    # runs cold (empty cache) or on the ~45s background refresh, which is exactly the
    # "freshly autoscaled instance" window the parent SIGABRT/OOM issue's burst
    # correlates with. See deployment_api/utils/request_memory_profiling.py.
    with log_rss_delta("vm_deployments._compute_vm_deployments"):
        try:
            # Get actual VM details from GCP (None on API failure → degrade to {})
            vm_details = (get_vm_instance_details(project_id) or {}) if filter_stale else {}
            running_vm_names = set(vm_details.keys()) if filter_stale else None

            # Get all registry entries (Firestore-first via resolve_active_registry, GCS fallback)
            all_active = resolve_active_registry(gcs=registry)

            # Filter active entries to only actually running VMs if requested
            if filter_stale and running_vm_names is not None:
                filtered_active = [e for e in all_active if e.vm_name in running_vm_names]
                logger.info(
                    "Filtered active deployments: %d registry entries -> %d actually running",
                    len(all_active),
                    len(filtered_active),
                )
                active = [_to_model(e, vm_details) for e in filtered_active]
            else:
                active = [_to_model(e, vm_details) for e in all_active]

            # Recent archive entries (completed/failed) don't need filtering
            recent = [_to_model(e, vm_details) for e in registry.list_recent_archive(days=days)]

        except (OSError, ValueError, RuntimeError) as exc:
            logger.exception("Failed to read VM deployments registry: %s", exc)
            raise HTTPException(status_code=502, detail="VM deployments registry unavailable") from exc

    return VmDeploymentsListModel(active=active, recent=recent, archive_days=days)


def _store_vm_deployments(cache_key: str, result: VmDeploymentsListModel) -> None:
    """Atomically record a fresh vm-deployments snapshot for ``cache_key``."""
    with _vm_deployments_lock:
        _vm_deployments_cache[cache_key] = (time.monotonic(), result)


def _refresh_vm_deployments(cache_key: str, days: int, filter_stale: bool) -> None:
    """Background cache refresh — recompute + store, then clear the in-flight flag."""
    try:
        _store_vm_deployments(cache_key, _compute_vm_deployments(days, filter_stale))
    except (HTTPException, OSError, ValueError, RuntimeError) as exc:
        # Keep the stale snapshot on a failed refresh — never poison the cache.
        logger.warning("vm-deployments: background refresh for %s failed: %s", cache_key, exc)
    finally:
        with _vm_deployments_lock:
            _vm_deployments_refreshing.discard(cache_key)


def _kick_background_vm_refresh(cache_key: str, days: int, filter_stale: bool) -> None:
    """Schedule exactly one background refresh per cache key (stale-while-revalidate)."""
    with _vm_deployments_lock:
        if cache_key in _vm_deployments_refreshing:
            return
        _vm_deployments_refreshing.add(cache_key)
    _vm_deployments_refresh_pool.submit(_refresh_vm_deployments, cache_key, days, filter_stale)


def _load_vm_deployments(days: int, filter_stale: bool) -> VmDeploymentsListModel:
    """Load VM deployments, stale-while-revalidate cached for a fast, smooth UI.

    Cache policy (mock mode bypasses it — already cheap + deterministic, and callers
    never reach this helper in mock mode):

    * **Fresh** (< TTL) -> served instantly.
    * **Stale** (> TTL) -> the stale snapshot is served instantly AND a single
      background refresh is kicked off, so the operator never waits on the slow
      registry walk after the first ever load (the cockpit polls repeatedly -> always
      warm).
    * **Cold** (no snapshot) -> computed synchronously, under a lock so a burst of
      polls collapses to ONE walk, not N.
    """
    cache_key = f"{days}|{filter_stale}"
    cached = _vm_deployments_cache.get(cache_key)
    if cached is not None:
        if (time.monotonic() - cached[0]) >= _VM_DEPLOYMENTS_TTL_SEC:
            _kick_background_vm_refresh(cache_key, days, filter_stale)
        return cached[1]

    # Cold path — lock so concurrent first-polls trigger exactly ONE walk.
    with _vm_deployments_lock:
        cached = _vm_deployments_cache.get(cache_key)
        if cached is not None:
            return cached[1]
        result = _compute_vm_deployments(days, filter_stale)
        _vm_deployments_cache[cache_key] = (time.monotonic(), result)
        return result


@router.get("/vm-deployments", response_model=VmDeploymentsListModel)
def list_vm_deployments(
    days: int = Query(7, ge=1, le=30, description="Archive lookback window in days"),
    filter_stale: bool = Query(True, description="Filter out stale registry entries"),
) -> VmDeploymentsListModel:
    """List currently-running VM deployments + those completed in the last N days.

    By default, filters registry entries to only show VMs that are actually RUNNING
    in GCP (avoiding the stale registry entries problem). Set filter_stale=false to
    see all registry entries including stale ones.

    Backed by a stale-while-revalidate snapshot cache (see `_load_vm_deployments`) —
    the underlying registry walk + Compute API census is the slowest endpoint in the
    service (measured avg 93.75s / max 99.27s in prod), so this route serves the
    last-known-good snapshot instantly and refreshes it on a ~45s background timer.
    """
    if _cfg.is_mock_mode():
        return VmDeploymentsListModel(
            active=[_mock_entry()],
            recent=[
                _mock_entry(
                    deployment_id="dep-mock-2",
                    status="completed",
                    completed_at="2026-04-17T23:59:00Z",
                    exit_code=0,
                    rows_in=30_000,
                    rows_out=30_000,
                    rows_error=0,
                    archive_run_log_uri=f"gs://deployment-scripts-{_MOCK_PID_TOKEN}/log-archive/final/canonical-migration-cefi-20260418-042359/run.log",  # noqa: gs-uri (mock fixture URI)
                    archive_serial_uri=f"gs://deployment-scripts-{_MOCK_PID_TOKEN}/log-archive/serial-rolling/20260417/canonical-migration-cefi-20260418-042359/serial-console.txt",  # noqa: gs-uri (mock fixture URI)
                ),
                _mock_entry(
                    deployment_id="dep-mock-3",
                    status="failed",
                    completed_at="2026-04-17T12:05:00Z",
                    exit_code=1,
                    rows_in=8_000,
                    rows_out=4_200,
                    rows_error=3_800,
                ),
            ],
            archive_days=days,
        )

    return _load_vm_deployments(days, filter_stale)


class VmReconcileResult(BaseModel):  # CORRECT-LOCAL: FastAPI API contract model
    """Result of a registry reconcile sweep."""

    reaped_count: int
    reaped: list[str] = Field(default_factory=list, description="deployment_ids reaped")
    running_vm_count: int
    total_active_before: int


@router.post("/vm-deployments/reconcile", response_model=VmReconcileResult, status_code=200)
def reconcile_vm_deployments() -> VmReconcileResult:
    """Reap stale active-registry entries: archive any whose VM is no longer RUNNING.

    Compares the GCS active/ list against GCP's aggregated VM list.  Any entry
    whose vm_name is absent from GCP (and whose heartbeat is older than 5 min
    clock-skew tolerance) is archived with status=failed, exit_code=125,
    reap_reason=vm_not_running.

    Returns the count + ids of reaped entries so the caller can update the UI.
    In mock mode returns a dry-run result without touching GCS.
    """
    if _cfg.is_mock_mode():
        return VmReconcileResult(
            reaped_count=0,
            reaped=[],
            running_vm_count=1,
            total_active_before=1,
        )

    registry = DeploymentsRegistry(bucket=DEFAULT_BUCKET)
    project_id = _cfg.require_gcp_project_id()

    try:
        all_active = registry.list_active()
        total_active_before = len(all_active)

        vm_details = get_vm_instance_details(project_id)
        if vm_details is None:
            # Census unavailable — pass None so _reap_reason falls back to
            # heartbeat-age-only classification instead of treating every
            # stale-heartbeat entry as vm_not_running (empty-set-over-reap bug).
            running_vm_names: set[str] | None = None
        else:
            running_vm_names = {name for name, details in vm_details.items() if details.get("status") == "RUNNING"}

        reaped = registry.reap_stale(running_vm_names=running_vm_names)

    except (OSError, ValueError, RuntimeError) as exc:
        logger.exception("Registry reconcile failed: %s", exc)
        raise HTTPException(status_code=502, detail="Registry reconcile failed") from exc

    logger.info(
        "reconcile_vm_deployments: reaped %d of %d active entries (%s VMs running)",
        len(reaped),
        total_active_before,
        len(running_vm_names) if running_vm_names is not None else "unknown",
    )
    return VmReconcileResult(
        reaped_count=len(reaped),
        reaped=[e.deployment_id for e in reaped],
        running_vm_count=len(running_vm_names) if running_vm_names is not None else 0,
        total_active_before=total_active_before,
    )


@router.get("/vm-deployments/{deployment_id}", response_model=VmDeploymentEntryModel)
def get_vm_deployment(deployment_id: str) -> VmDeploymentEntryModel:
    """Return a single VM deployment by id (checks active + last 14 days archive)."""
    if _cfg.is_mock_mode():
        return _mock_entry(deployment_id=deployment_id)

    registry = DeploymentsRegistry(bucket=DEFAULT_BUCKET)
    project_id = _cfg.require_gcp_project_id()

    try:
        entry = resolve_deployment_by_id(deployment_id, gcs=registry)
        if entry is None:
            raise HTTPException(status_code=404, detail=f"VM deployment '{deployment_id}' not found")

        # Get VM details if it's an active deployment
        vm_details = (get_vm_instance_details(project_id) or {}) if entry.status == "running" else {}
        return _to_model(entry, vm_details)

    except HTTPException:
        raise
    except (OSError, ValueError, RuntimeError) as exc:
        logger.exception("Failed to fetch VM deployment %s: %s", deployment_id, exc)
        raise HTTPException(status_code=502, detail="VM deployments registry unavailable") from exc

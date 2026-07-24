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
from unified_trading_library import fastapi_uei_lifespan

from deployment_api.background_sync import (
    STATE_BUCKET,
    auto_sync_running_deployments,
    get_held_deployment_locks,
    get_owner_id,
    set_shutdown_event,
)
from deployment_api.services.catalogue_lifecycle import (
    list_new_listings_page,
    list_upcoming_expiries_page,
)
from deployment_api.services.data_status.manifest import prewarm_indexes
from deployment_api.settings import DATA_STATUS_PREWARM_SERVICE
from deployment_api.utils.bounded_cache import start_sweeper, stop_sweeper
from deployment_api.utils.service_utils import (
    get_codex_dir,
    get_config_dir,
    get_epics_dir,
    get_plans_dir,
)
from deployment_api.utils.storage_facade import (
    delete_object as _delete_storage_object,
)
from deployment_api.utils.storage_facade import (
    object_exists as _storage_object_exists,
)
from deployment_api.utils.storage_facade import (
    read_object_text as _read_storage_object_text,
)
from deployment_api.utils.worker_identity import is_leader_worker

logger = logging.getLogger(__name__)

# Private alias for testability (tests can patch this name)
_auto_sync_running_deployments = auto_sync_running_deployments

# Background task handles
_background_task = None
_events_drain_task = None
_prewarm_task = None
_catalogue_lifecycle_warm_task = None
_shutdown_event = None

# Just under catalogue_lifecycle's 5-min (300s) in-process TTL cache, so a
# refresh always lands before the previous warm's cache entry would expire —
# a user request should essentially never observe a cold miss.
_CATALOGUE_LIFECYCLE_WARM_INTERVAL_SEC = 270


async def _warm_catalogue_lifecycle() -> None:
    """Background, best-effort cache warm for the New Listings / Upcoming
    Expiries panels (F10, 2026-07-18).

    Each cold read does 5 per-AG transpacific GCS parquet reads; even
    parallelised (services/catalogue_lifecycle.py) that's dominated by the
    slowest single AG (~17s for prediction) which can still be tight against
    the Cloud Run / browser fetch timeout on a request that lands during a
    5-min-TTL cache miss. Priming the default-params cache here, on a timer
    just under the TTL, means a real request almost always hits the warm
    in-process cache instead of paying that cold read synchronously. Runs on
    EVERY worker (unlike the leader-only auto-sync task) — the cache is a
    per-process module dict, not shared storage, so each worker needs its own
    warm to benefit whichever worker a request lands on. Swallows every error
    — a failed warm must never affect readiness; the next request just pays
    the cold read as before."""
    # Small initial stagger (matches _prewarm_data_status) so the first warm's
    # GCS reads don't contend with the rest of startup.
    await asyncio.sleep(5)
    while True:
        try:
            # Default params only — the same (max_age_days=30/within_days=7,
            # asset_group=None, venue=None) shape the UI cards request on
            # first mount (LifecycleCards.tsx defaultThreshold).
            await asyncio.to_thread(list_new_listings_page)
            await asyncio.to_thread(list_upcoming_expiries_page)
            logger.info("Catalogue-lifecycle cache warm complete (new-listings + upcoming-expiries)")
        except asyncio.CancelledError:
            raise
        except Exception as exc:  # best-effort: never let a warm failure affect the service
            logger.warning("Catalogue-lifecycle cache warm failed (non-fatal): %s", exc)
        try:
            await asyncio.sleep(_CATALOGUE_LIFECYCLE_WARM_INTERVAL_SEC)
        except asyncio.CancelledError:
            raise


async def _prewarm_data_status(service: str) -> None:
    """Background, best-effort Data Status index pre-warm (gated by DATA_STATUS_PREWARM_SERVICE).

    Sleeps briefly so the warm's index load doesn't contend with the rest of startup, then
    runs a tiny manifest query that populates _INDEX_CACHE. Swallows every error — a failed
    pre-warm must never affect readiness or surface to a caller (the first real query just
    pays the cold load as before)."""
    try:
        await asyncio.sleep(5)
        await prewarm_indexes(service)
        logger.info("Data Status pre-warm complete for %s", service)
    except asyncio.CancelledError:
        raise
    except Exception as exc:  # best-effort: never let a warm failure affect the service
        logger.warning("Data Status pre-warm for %s failed (non-fatal): %s", service, exc)


async def _cancel_background_tasks() -> None:
    """Cancel and await the background sync and events drain tasks."""
    global _background_task, _events_drain_task
    if _background_task:
        _background_task.cancel()
        try:
            # 20s, not 5s (MEASURED root cause, 2026-07-24): the reaper tick this task hosts
            # (background_sync._run_deployment_reaper) does a blocking, uninterruptible
            # ThreadPoolExecutor call — list_running_vm_names() + a per-blob sequential
            # download of every `deployments/active/*.json` (background_sync.py's own comment
            # measured ~138s at ~3k stale entries) + one archive write per reaped entry.
            # asyncio cancellation cannot stop a `run_in_executor` call once its thread has
            # started, so a 5s budget almost never covers a real tick: prod stderr showed this
            # exact CancelledError recurring at background_sync.py:72 many times/day, which is
            # why `deployments/active/` was stuck at ~400 entries instead of draining toward the
            # true running-VM count. 20s leaves headroom under gunicorn's graceful_timeout=30
            # (gunicorn.conf.py) for the sequential waits below (6s) plus lock release/cache
            # shutdown that must still run after this returns.
            await asyncio.wait_for(_background_task, timeout=20)
        except asyncio.CancelledError as e:
            logger.debug("Suppressed %s during operation: %s", type(e).__name__, e)
        except TimeoutError:
            logger.warning("Background auto-sync task did not stop in time")
    if _events_drain_task:
        _events_drain_task.cancel()
        with suppress(TimeoutError, asyncio.CancelledError):
            await asyncio.wait_for(_events_drain_task, timeout=2)
    if _prewarm_task:
        _prewarm_task.cancel()
        with suppress(TimeoutError, asyncio.CancelledError):
            await asyncio.wait_for(_prewarm_task, timeout=2)
    if _catalogue_lifecycle_warm_task:
        _catalogue_lifecycle_warm_task.cancel()
        with suppress(TimeoutError, asyncio.CancelledError):
            await asyncio.wait_for(_catalogue_lifecycle_warm_task, timeout=2)


def _release_one_lock(deployment_id: str, held_locks: set[str]) -> int:
    """Attempt to release a single deployment lock. Returns 1 if released, 0 otherwise."""
    try:
        lock_blob_name = f"locks/deployment_{deployment_id}.lock"
        if _storage_object_exists(STATE_BUCKET, lock_blob_name):
            lock_data = cast(
                dict[str, object],
                json.loads(_read_storage_object_text(STATE_BUCKET, lock_blob_name) or "{}"),
            )
            if lock_data.get("owner") == get_owner_id():
                _delete_storage_object(STATE_BUCKET, lock_blob_name)
                held_locks.discard(deployment_id)
                return 1
    except (OSError, ValueError, RuntimeError):
        pass  # Lock may have been taken by another instance
    held_locks.discard(deployment_id)
    return 0


def _release_deployment_locks() -> None:
    """Release all per-deployment locks held by this instance on graceful shutdown."""
    try:
        released_count = 0
        held_deployment_locks = get_held_deployment_locks()
        for deployment_id in list(held_deployment_locks):
            released_count += _release_one_lock(deployment_id, held_deployment_locks)
        if released_count > 0:
            logger.info("[SHUTDOWN] Released %s per-deployment lock(s)", released_count)
    except (OSError, ValueError, RuntimeError) as e:
        logger.warning("[SHUTDOWN] Could not release locks: %s", e)


@asynccontextmanager
async def lifespan(app: FastAPI):
    """Application lifespan - startup and shutdown."""
    global _background_task, _shutdown_event, _events_drain_task, _prewarm_task, _catalogue_lifecycle_warm_task

    async with fastapi_uei_lifespan("deployment-api", entrypoint="gunicorn/uvicorn"):
        # Startup
        app.state.config_dir = get_config_dir()
        logger.info("Config directory: %s", app.state.config_dir)

        app.state.codex_dir = get_codex_dir()
        if app.state.codex_dir is not None:
            logger.info("Codex readiness dir: %s", app.state.codex_dir)
        else:
            logger.warning("Codex readiness dir: not found — checklist endpoints will return 404 (codex v3.0 is SSOT)")

        app.state.epics_dir = get_epics_dir()
        if app.state.epics_dir is not None:
            logger.info("Epics dir: %s", app.state.epics_dir)
        else:
            logger.warning(
                "Epics dir: not found — /api/epics will return 503 (codex 11-project-management/epics/ must be present)"
            )

        app.state.plans_dir = get_plans_dir()
        if app.state.plans_dir is not None:
            logger.info("PM plans dir: %s", app.state.plans_dir)
        else:
            logger.info("PM plans dir: not found — plans visualization endpoints unavailable")

        # Initialize cache
        # call-time patch surface (tests/unit/test_lifespan.py patch.dict's sys.modules["deployment_api.utils.cache"])
        from .utils.cache import cache  # noqa: imports-inside-functions

        await cache.initialize()

        # Start background sync task — only on the elected leader worker. Gunicorn forks
        # WORKERS UvicornWorker processes per Cloud Run instance; running N duplicate
        # sync loops against the same GCS deployments registry is pure waste (redundant
        # scans) plus contention on shared per-deployment locks. See
        # deployment_api.utils.worker_identity for the election mechanism (also gates the
        # reap_stale tick, which runs inside this same background loop).
        _shutdown_event = asyncio.Event()
        set_shutdown_event(_shutdown_event)
        if is_leader_worker():
            _background_task = asyncio.create_task(_auto_sync_running_deployments())
            logger.info("Background auto-sync task started (leader worker)")
        else:
            logger.info("Background auto-sync task skipped (non-leader worker)")

        # Start deployment events drain (for low-latency SSE notify when state is saved from sync code)
        from deployment_api.utils.deployment_events import drain_sync_queue

        _events_drain_task = asyncio.create_task(drain_sync_queue())
        logger.info("Deployment events drain task started")

        # Single periodic sweeper for every BoundedCache instance (proactively expires
        # entries a cold key would otherwise leave sitting on stale memory indefinitely).
        start_sweeper()
        logger.info("Bounded-cache sweeper task started")

        # Optional Data Status index pre-warm (DATA_STATUS_PREWARM_SERVICE; off by default).
        # Background + best-effort: warms _INDEX_CACHE so the first Data Status query is fast
        # (see services/data_status/manifest.prewarm_indexes). Never blocks readiness.
        if DATA_STATUS_PREWARM_SERVICE:
            _prewarm_task = asyncio.create_task(_prewarm_data_status(DATA_STATUS_PREWARM_SERVICE))
            logger.info("Data Status pre-warm scheduled for %s", DATA_STATUS_PREWARM_SERVICE)

        # New Listings / Upcoming Expiries cache warm (F10, 2026-07-18) — unconditional
        # (unlike the DATA_STATUS_PREWARM_SERVICE toggle above) and runs on every worker,
        # not just the leader: see _warm_catalogue_lifecycle for why.
        _catalogue_lifecycle_warm_task = asyncio.create_task(_warm_catalogue_lifecycle())
        logger.info("Catalogue-lifecycle cache warm task started")

        yield

        # Shutdown
        logger.info("Shutting down API...")
        _shutdown_event.set()
        await _cancel_background_tasks()
        await stop_sweeper()
        _release_deployment_locks()

        try:
            # call-time patch surface (tests/unit/test_lifespan.py patch.dict's
            # sys.modules["deployment_api.utils.cache"])
            from .utils.cache import cache  # noqa: imports-inside-functions

            await cache.shutdown()
        except (OSError, ValueError, RuntimeError) as e:
            logger.warning("Cache shutdown failed: %s", e)

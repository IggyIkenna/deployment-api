"""Internal endpoint: compute the offline data-status rollup IN the gen1 service.

The standalone Cloud Run JOB cannot run this — Cloud Run Jobs are gen2-only and the
native pyarrow/pandas compute in ``_get_manifest_status_sync`` crashes on gen2 (a
C-level death, faulthandler-invisible; ruled out beta/fork/memory/threading/events —
R7 follow-up #4 in ``proper_instrument_catalogue_lifecycle_rollup_2026_06_04.md``)
while running fine on gen1. The deployment-api SERVICE runs gen1 (the Cloud Run
service default), so the Cloud Scheduler hits THIS route (authenticated by X-API-Key
via the shared ``verify_any_auth`` on the data-status router) instead of executing
the gen2 Job.

Routes register on the package facade's shared ``router`` (mounted at
``/api/data-status``); patched module-level collaborators resolve through the facade
module (``_ds``) at call time.
"""

from __future__ import annotations

import asyncio
import contextlib
import logging

from fastapi import Query
from unified_trading_library import GcsEventSink, run_lifecycle, setup_events

from deployment_api.routes.data_status import router
from deployment_api.settings import gcp_project_id

logger = logging.getLogger(__name__)

# The in-service endpoint calls ``run_rollup()`` directly (NOT via ``main()``),
# so it must set up the event sink itself — otherwise ``log_event()`` calls
# inside ``run_rollup()`` either crash (if no prior init) or route to whatever
# sink a different code path last configured (silently writing to the wrong
# bucket/prefix).  Mirror the ``main()`` init exactly: a dedicated
# ``GcsEventSink`` scoped to ``data-status-rollup-worker`` so every
# SERVICE_PROCESSED / SERVICE_FAILED event lands under the correct GCS prefix.
# ``RuntimeError`` is suppressed for the "already initialized by an outer
# bootstrap" case (same guard ``main()`` carries).
_ROLLUP_SERVICE_NAME = "data-status-rollup-worker"


@router.post("/rollup-run")
async def run_data_status_rollup(services: list[str] | None = Query(None)) -> dict[str, object]:
    """Compute + write the offline data-status rollup for each tracked service.

    Runs in the gen1 SERVICE (the gen2 Job crashes natively on the per-asset-group
    compute). Synchronous — returns when every blob is written — because the caller is
    the Cloud Scheduler with a long attempt-deadline, not a UI request.

    ALWAYS recompute every tracked service's live ``{svc}/full.json.gz`` (+
    ``coverage.json.gz``) over the live consolidated index. That blob MUST stay fresh,
    or the manifest read falls through to a multi-minute all-AG compute on the 8 GiB
    service → 503. Each write is overwrite-by-name (idempotent); a partial failure
    returns ``status="partial"`` and the next scheduler tick recomputes.

    ``run_rollup`` isolates each service's actual compute in its own spawned child
    process (see ``scripts/data_status_rollup_worker.py``), which sets the
    ProcessPool/ThreadPool-serial overrides on ITS OWN fresh module state — this
    process's ``data_status_service`` globals are never touched, so no save/restore
    is needed here.
    """
    from deployment_api.scripts.data_status_rollup_worker import DEFAULT_SERVICES, run_rollup

    svc_list: list[str] = list(services) if services else list(DEFAULT_SERVICES)
    bucket = f"{gcp_project_id}-data-status-rollups"

    # ------------------------------------------------------------------
    # Ensure events are initialized for THIS worker before calling
    # ``run_rollup()``.  The ``main()`` entrypoint (CLI / gen2 Job path)
    # does this itself, but the in-service endpoint calls ``run_rollup()``
    # directly — without this init the ``log_event()`` calls inside the
    # sweep are either a crash (RuntimeError: not initialized) or silently
    # routed to whatever sink another code path last configured.
    # Fixed 2026-08-04 — the events bucket had been dead for this worker
    # since 2026-06-17 because this init was missing in the only
    # production code path.
    # ------------------------------------------------------------------
    with contextlib.suppress(RuntimeError):
        _sink = GcsEventSink(
            project_id=gcp_project_id,
            bucket=f"{gcp_project_id}-events",
            service_name=_ROLLUP_SERVICE_NAME,
        )
        setup_events(service_name=_ROLLUP_SERVICE_NAME, mode="batch", sink=_sink)

    logger.info("data-status rollup (LIVE): %d service(s) -> gs://%s", len(svc_list), bucket)

    with run_lifecycle(
        service_name=_ROLLUP_SERVICE_NAME,
        details={"project_id": gcp_project_id, "bucket": bucket},
    ):
        rc_live = await asyncio.to_thread(run_rollup, gcp_project_id, bucket, svc_list)

    return {
        "status": "ok" if rc_live == 0 else "partial",
        "live_services": svc_list,
        "exit_code_live": rc_live,
        "bucket": bucket,
    }

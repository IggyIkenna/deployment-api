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
import logging

from fastapi import Query

from deployment_api.routes.data_status import router
from deployment_api.settings import gcp_project_id

logger = logging.getLogger(__name__)


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
    """
    import deployment_api.services.data_status_service as _dss_mod
    from deployment_api.scripts.data_status_rollup_worker import DEFAULT_SERVICES, run_rollup

    svc_list: list[str] = list(services) if services else list(DEFAULT_SERVICES)
    bucket = f"{gcp_project_id}-data-status-rollups"

    # Force FULLY-SERIAL per-AG compute: ``run_rollup`` disables the ProcessPool, and we
    # also disable the ThreadPool here. Running all asset groups in PARALLEL threads holds
    # every AG's cell-grid intermediate at once and OOMs the service; serial holds one AG at
    # a time. Save/restore the flags so on-demand requests on this process (and the next
    # invocation) see the original config afterwards.
    _prev_proc = _dss_mod._PROCESS_POOL_DISABLED  # pyright: ignore[reportPrivateUsage]
    _prev_thread = _dss_mod._THREAD_POOL_DISABLED  # pyright: ignore[reportPrivateUsage]
    _dss_mod._THREAD_POOL_DISABLED = True  # pyright: ignore[reportPrivateUsage]
    try:
        logger.info("data-status rollup (LIVE): %d service(s) -> gs://%s", len(svc_list), bucket)
        rc_live = await asyncio.to_thread(run_rollup, gcp_project_id, bucket, svc_list)
    finally:
        _dss_mod._PROCESS_POOL_DISABLED = _prev_proc  # pyright: ignore[reportPrivateUsage]
        _dss_mod._THREAD_POOL_DISABLED = _prev_thread  # pyright: ignore[reportPrivateUsage]

    return {
        "status": "ok" if rc_live == 0 else "partial",
        "live_services": svc_list,
        "exit_code_live": rc_live,
        "bucket": bucket,
    }

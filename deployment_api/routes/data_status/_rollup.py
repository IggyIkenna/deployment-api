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
    compute). Synchronous — returns when every service's blob is written — because
    the caller is the Cloud Scheduler with a long attempt-deadline, not a UI request.
    Each service write is overwrite-by-name (idempotent); a partial failure returns
    ``status="partial"`` and the next scheduler tick recomputes.
    """
    import deployment_api.services.data_status_service as _dss_mod
    import deployment_api.services.manifest_source as _ms
    from deployment_api.scripts.data_status_rollup_worker import DEFAULT_SERVICES, beta_eligible, run_rollup

    svc_list: list[str] = list(services) if services else list(DEFAULT_SERVICES)
    # Mirror the worker's main(): in CF-20 beta mode only roll up services that have a
    # v9 projected index (instruments-service) — the beta read fails LOUD on a missing
    # projection, so sweeping a non-projected service (mtds/features) would crash it.
    if _ms.is_beta_mode():
        svc_list = beta_eligible(svc_list)
    bucket = f"{gcp_project_id}-data-status-rollups"
    logger.info("data-status rollup (service path): computing %d service(s) -> gs://%s", len(svc_list), bucket)

    # Force FULLY-SERIAL per-AG compute: ``run_rollup`` disables the ProcessPool, and we
    # also disable the ThreadPool here. Running all asset groups in PARALLEL threads holds
    # every AG's cell-grid intermediate at once and OOMs the 8 GiB service ("Memory limit of
    # 8192 MiB exceeded with 8435 MiB used"); serial holds one AG at a time (~1.7 GiB) so it
    # fits. Save/restore both flags so we don't leave them toggled for on-demand API requests.
    _prev_proc = _dss_mod._PROCESS_POOL_DISABLED  # pyright: ignore[reportPrivateUsage]
    _prev_thread = _dss_mod._THREAD_POOL_DISABLED  # pyright: ignore[reportPrivateUsage]
    _dss_mod._THREAD_POOL_DISABLED = True  # pyright: ignore[reportPrivateUsage]
    try:
        rc = await asyncio.to_thread(run_rollup, gcp_project_id, bucket, svc_list)
    finally:
        _dss_mod._PROCESS_POOL_DISABLED = _prev_proc  # pyright: ignore[reportPrivateUsage]
        _dss_mod._THREAD_POOL_DISABLED = _prev_thread  # pyright: ignore[reportPrivateUsage]

    return {"status": "ok" if rc == 0 else "partial", "services": svc_list, "exit_code": rc, "bucket": bucket}

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

    **TWO PHASES (2026-06-16 — fixes the market-tick-data-service 503):**
      1. **LIVE** — ALWAYS recompute every tracked service's live ``{svc}/full.json.gz``
         (+ ``coverage.json.gz``). Unconditional: a non-beta-eligible service
         (mtds/features/…) has no beta blob, so the manifest endpoint reads its LIVE blob
         even while the deployment is in beta-preview mode — that blob MUST stay fresh, or
         the read falls through to a multi-minute all-AG compute on the 8 GiB service → 503.
      2. **BETA** — when ``DATA_STATUS_BETA_MANIFEST_BLOB`` is set, ADDITIONALLY recompute
         the beta-eligible service(s) (instruments-service) from the projected v9 index →
         ``{svc}/full.beta.json.gz``. The eligible service's manifest read flips to this
         blob; every other service keeps the live blob.

    Beta-mode is forced PER-PHASE by toggling the process-global
    ``manifest_source.DATA_STATUS_BETA_MANIFEST_BLOB`` (which ``is_beta_mode`` /
    ``read_manifest_index`` / ``rollup_blob_path`` all read), restored in ``finally``. SAFE
    here because this endpoint runs on the DEDICATED single-purpose rollup service that
    serves no concurrent UI traffic (only the Cloud Scheduler hits it) — the toggle is a
    process-global, so do NOT route UI requests to that service. Each write is
    overwrite-by-name (idempotent); a partial failure returns ``status="partial"`` and the
    next scheduler tick recomputes.
    """
    import deployment_api.services.data_status_service as _dss_mod
    import deployment_api.services.manifest_source as _ms
    from deployment_api.scripts.data_status_rollup_worker import DEFAULT_SERVICES, run_rollup

    svc_list: list[str] = list(services) if services else list(DEFAULT_SERVICES)
    bucket = f"{gcp_project_id}-data-status-rollups"

    _orig_beta = _ms.DATA_STATUS_BETA_MANIFEST_BLOB
    beta_configured = bool(_orig_beta)

    # Force FULLY-SERIAL per-AG compute: ``run_rollup`` disables the ProcessPool, and we
    # also disable the ThreadPool here. Running all asset groups in PARALLEL threads holds
    # every AG's cell-grid intermediate at once and OOMs the service; serial holds one AG at
    # a time. Save/restore the flags + the beta-blob global so on-demand requests on this
    # process (and the next invocation) see the original config afterwards.
    _prev_proc = _dss_mod._PROCESS_POOL_DISABLED  # pyright: ignore[reportPrivateUsage]
    _prev_thread = _dss_mod._THREAD_POOL_DISABLED  # pyright: ignore[reportPrivateUsage]
    _dss_mod._THREAD_POOL_DISABLED = True  # pyright: ignore[reportPrivateUsage]
    beta_svcs: list[str] = []
    rc_beta = 0
    try:
        # Phase 1 — LIVE rollup for EVERY service (beta forced OFF → live index + full.json.gz).
        _ms.DATA_STATUS_BETA_MANIFEST_BLOB = ""
        logger.info("data-status rollup phase 1 (LIVE): %d service(s) -> gs://%s", len(svc_list), bucket)
        rc_live = await asyncio.to_thread(run_rollup, gcp_project_id, bucket, svc_list)

        # Phase 2 — BETA rollup for beta-eligible service(s) only (beta forced ON → projected
        # v9 index + full.beta.json.gz). Skipped when the deployment isn't in beta mode.
        if beta_configured:
            _ms.DATA_STATUS_BETA_MANIFEST_BLOB = _orig_beta
            beta_svcs = _ms.beta_eligible(svc_list)
            if beta_svcs:
                logger.info("data-status rollup phase 2 (BETA): %d service(s) -> gs://%s", len(beta_svcs), bucket)
                rc_beta = await asyncio.to_thread(run_rollup, gcp_project_id, bucket, beta_svcs)
    finally:
        _ms.DATA_STATUS_BETA_MANIFEST_BLOB = _orig_beta
        _dss_mod._PROCESS_POOL_DISABLED = _prev_proc  # pyright: ignore[reportPrivateUsage]
        _dss_mod._THREAD_POOL_DISABLED = _prev_thread  # pyright: ignore[reportPrivateUsage]

    rc = rc_live if rc_live != 0 else rc_beta
    return {
        "status": "ok" if rc == 0 else "partial",
        "live_services": svc_list,
        "beta_services": beta_svcs,
        "exit_code_live": rc_live,
        "exit_code_beta": rc_beta,
        "bucket": bucket,
    }

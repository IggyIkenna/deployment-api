"""Manifest-status surface: entry point + live-build guard + process-pool fan-out.

Split out of the 6,663-line ``data_status_service.py`` god-module
(codex ratchet plan 2026-06-10). The per-category builder itself
(``_build_manifest_category`` + its helpers) further split out into
``manifest_category_builder.py`` per
``plans/active/issues/deployment_api_qg_size_gate_debt_2026_07_30.md``. The
facade module re-exports every public + legacy-underscore name, so callers
keep importing from ``deployment_api.services.data_status_service``.
"""

import asyncio
import datetime as dt
import logging
import multiprocessing
from concurrent.futures import ProcessPoolExecutor, ThreadPoolExecutor

import pandas as pd
from unified_api_contracts import (
    VenueMapping,
)
from unified_api_contracts.internal import MarketCategory

import deployment_api.services.data_status_service as _dss

logger = logging.getLogger(__name__)

from deployment_api.services.data_status.live_build_guard import (
    estimate_live_build_bytes,
    would_exceed_budget,
)
from deployment_api.services.data_status.rollup_cache import slice_rollup_to_window
from deployment_api.settings import (
    DATA_STATUS_LIVE_BUILD_CHILD_RLIMIT_BYTES as _LIVE_BUILD_CHILD_RLIMIT_BYTES,
)
from deployment_api.settings import (
    DATA_STATUS_LIVE_BUILD_CHILD_TIMEOUT_SECONDS as _LIVE_BUILD_CHILD_TIMEOUT_S,
)
from deployment_api.settings import (
    DATA_STATUS_LIVE_BUILD_MEMORY_BUDGET_BYTES as _LIVE_BUILD_MEMORY_BUDGET_BYTES,
)
from deployment_api.utils.bounded_subprocess import BoundedSubprocessError, run_bounded


async def prewarm_indexes(service: str, *, days: int = 7, cloud: str = "gcp") -> None:
    """Warm ``_INDEX_CACHE`` for a service by running a tiny manifest query at startup.

    The manifest index load is whole-file (not date-windowed) — so a cheap 7-day query
    loads and caches (5-min TTL) every asset-group index the Data Status landing page
    needs, with negligible cell-grid compute. The user's first real query then hits warm
    cache (~10s) instead of the cold ~50s/asset-group transpacific GCS fetch. Best-effort:
    raises nothing the caller must handle (the lifespan wrapper logs + swallows)."""
    end = dt.datetime.now(dt.UTC).date()
    start = end - dt.timedelta(days=days)
    svc = _dss.DataStatusService()
    await svc.get_manifest_status(service, start.isoformat(), end.isoformat(), cloud=cloud)


def build_category_in_subprocess(
    service: str,
    cat: str,
    start_date: str,
    end_date: str,
    all_date_strs: list[str],
    total_days: int,
    cloud: str = "gcp",
) -> dict[str, object]:
    """ProcessPool worker — build one category's manifest entry in a forked child.

    Each call runs in a child process forked from the parent gunicorn worker
    just before the request fans out. Fork start-method means the child
    inherits everything the parent had loaded — most importantly the
    module-level ``_INDEX_CACHE`` dict, so the child does NOT re-read the
    manifest from GCS. Copy-on-write semantics keep memory cheap until the
    child mutates a shared page.

    Returns the per-category result dict that the parent's serial path
    would have produced. Internal counters (``_venue_found`` /
    ``_venue_expected``) are kept on the dict for the parent to drain into
    the overall totals.
    """
    # Construct a fresh DataStatusService inside the child. Cheap — it's
    # just lookup-table init; no GCS, no network. Avoids pickling the
    # parent's instance through the Pipe.
    dss = _dss.DataStatusService()
    venue_mapping = _dss.VenueMapping()
    return dss._build_manifest_category(  # type: ignore[reportPrivateUsage]
        service,
        cat,
        start_date,
        end_date,
        all_date_strs,
        total_days,
        venue_mapping,
        cloud=cloud,
    )


def _build_manifest_live_in_subprocess(
    service: str,
    start_date: str,
    end_date: str,
    asset_groups: list[str] | None,
    secondary_axis: str | None,
    league_id: str | None,
    fixture_id: str | None,
    canonical_question_group: str | None,
    job_id: str | None,
    chain: str | None,
    cloud: str,
    pipeline_modes: list[str] | None,
    venue: list[str] | None,
    scope: str,
) -> dict[str, object]:
    """Spawned-child entrypoint for ``bounded_subprocess.run_bounded``.

    Defense-in-depth layer 2 for the manifest live-build OOM guard (layer 1
    is the pre-flight :func:`~deployment_api.services.data_status.live_build_guard.estimate_live_build_bytes`
    refusal in :meth:`ManifestStatusMixin.get_manifest_status`). Runs the
    EXACT same sync build the un-guarded code used to call directly, just
    inside a ``multiprocessing.get_context("spawn")`` child capped by
    ``RLIMIT_AS`` — so a build the pre-flight estimate underestimated raises
    a catchable ``MemoryError`` in this throwaway child instead of the
    platform OOM-killer taking down the parent gunicorn worker.

    Constructs a fresh ``DataStatusService`` inside the child (cheap — no
    GCS/network at construction time; mirrors
    :func:`build_category_in_subprocess` above) rather than pickling a live
    instance across the process boundary — bound methods / instances with
    live client handles do not reliably pickle under ``spawn``.
    """
    dss = _dss.DataStatusService()
    return dss._get_manifest_status_sync(  # pyright: ignore[reportPrivateUsage]
        service,
        start_date,
        end_date,
        asset_groups,
        secondary_axis,
        league_id,
        fixture_id,
        canonical_question_group,
        job_id,
        chain,
        cloud,
        pipeline_modes,
        venue,
        scope,
    )


from deployment_api.services.data_status.manifest_category_builder import ManifestCategoryBuilderMixin


class ManifestStatusMixin(ManifestCategoryBuilderMixin):
    """get_manifest_status + live-build guard + subprocess fan-out dispatch.

    The data_status mixins form a single linear inheritance chain
    (cli -> defi -> sports -> breakdowns_domain -> breakdowns_core ->
    venue_resolution -> coverage -> missing_shards -> manifest_category_builder
    -> manifest) so that every cross-group ``self._method`` reference resolves
    statically under basedpyright strict. ``DataStatusService`` composes the
    top of the chain and is the ONLY public entry point — import it from
    ``deployment_api.services.data_status_service`` (the facade).
    """

    async def get_manifest_status(
        self,
        service: str,
        start_date: str,
        end_date: str,
        asset_groups: list[str] | None = None,
        *,
        cloud: str = "gcp",
        secondary_axis: str | None = None,
        league_id: str | None = None,
        fixture_id: str | None = None,
        canonical_question_group: str | None = None,
        job_id: str | None = None,
        chain: str | None = None,
        pipeline_modes: list[str] | None = None,
        venue: list[str] | None = None,
        scope: str = "could_exist",
    ) -> dict[str, object]:
        """Return data status from manifest indices in TurboDataStatusResponse shape.

        **Two paths**:

        1. **Rollup fast-path** — if a fresh (< 30 min old) rollup blob
           exists at ``gs://{pid}-data-status-rollups/{service}/full.json.gz``,
           read it and slice to the requested window in-memory. Sub-500ms.
           Skipped when any filter param (``league_id`` / ``fixture_id`` /
           ``canonical_question_group`` / ``job_id`` / ``chain``) is set
           — the rollup is filter-free; falling through to the on-demand
           path is the only way to honour the filter.
        2. **On-demand fall-through** — original honest-coverage compute.
           Used on cold deploys (first 5 min before first cron fires) and
           if the rollup is stale or absent.

        The rollup is computed offline by ``data_status_rollup_worker``
        (Cloud Run Job + ``*/5 * * * *`` Scheduler cron). See plan:
        ``data_status_offline_rollup_2026_05_06.md``.

        **Live-build OOM guard** (2026-07-13/14 incident — see
        ``deployment_api.services.data_status.live_build_guard`` module
        docstring): before attempting the on-demand fall-through, a cheap
        request-shape estimate decides whether it's safe to attempt at all.
        Too large -> refused outright (stale rollup if one exists, else a
        structured error) rather than risking the ~18-81 GB peak-RSS blowup
        that was OOM-crash-looping this container. A build that DOES pass
        the estimate still runs inside a resource-bounded spawned child
        (``deployment_api.utils.bounded_subprocess``) as defense-in-depth
        against the estimate itself being wrong.
        """
        any_row_filter = (
            any(f is not None and f != "" for f in (league_id, fixture_id, canonical_question_group, job_id, chain))
            or bool(pipeline_modes)
            or bool(venue)
        )
        # The all-asset-group view is served from the precomputed rollup cache
        # (``{service}/full.json.gz`` via ``_read_rollup_if_fresh``) instead of
        # live-computing every AG per request (which exceeds the Cloud Run request
        # timeout -> HTTP 503). If the rollup hasn't been written yet, the read
        # returns None and we fall through to the (slower) on-demand compute.
        if not any_row_filter:
            rollup = await asyncio.to_thread(_dss._read_rollup_if_fresh, service)  # pyright: ignore[reportPrivateUsage]  # facade patch-point (late-bound)
            if rollup is not None:
                response = slice_rollup_to_window(rollup, start_date, end_date, asset_groups)
                if secondary_axis:
                    response["secondary_axis"] = secondary_axis
                return response

        # ── Layer 1: pre-flight refusal ──
        # Cheap (no GCS/network) estimate of what the on-demand live build
        # would cost. ``has_row_filter`` covers the secondary-axis row
        # filters (venue has its own dedicated, separately-weighted count).
        has_secondary_row_filter = any(
            f is not None and f != "" for f in (league_id, fixture_id, canonical_question_group, job_id, chain)
        ) or bool(pipeline_modes)
        cat_list = self._resolve_cat_list(service, asset_groups)
        estimate_bytes = estimate_live_build_bytes(
            service=service,
            start_date=start_date,
            end_date=end_date,
            category_count=len(cat_list),
            venue_count=len(venue) if venue else None,
            has_row_filter=has_secondary_row_filter,
        )
        if would_exceed_budget(estimate_bytes, _LIVE_BUILD_MEMORY_BUDGET_BYTES):
            logger.warning(
                "manifest live-build REFUSED (pre-flight) for service=%s [%s..%s]: estimate=%dMB > budget=%dMB",
                service,
                start_date,
                end_date,
                estimate_bytes // (1024 * 1024),
                _LIVE_BUILD_MEMORY_BUDGET_BYTES // (1024 * 1024),
            )
            return await self._live_build_fallback(
                service,
                start_date,
                end_date,
                asset_groups,
                secondary_axis,
                any_row_filter,
                estimate_bytes,
            )

        # ── Layer 2: defense-in-depth ──
        # Even an estimate under budget runs inside a resource-bounded
        # spawned child (mirrors the isolation pattern shipped in commit
        # 8d260ad for the offline rollup worker) — an underestimate raises a
        # catchable error in the throwaway child instead of OOM-killing this
        # gunicorn worker (and every other request sharing its container).
        try:
            return await asyncio.to_thread(
                run_bounded,
                _build_manifest_live_in_subprocess,
                service,
                start_date,
                end_date,
                asset_groups,
                secondary_axis,
                league_id,
                fixture_id,
                canonical_question_group,
                job_id,
                chain,
                cloud,
                pipeline_modes,
                venue,
                scope,
                rlimit_bytes=_LIVE_BUILD_CHILD_RLIMIT_BYTES,
                timeout_s=_LIVE_BUILD_CHILD_TIMEOUT_S,
                name=f"manifest-live-{service[:24]}",
            )
        except BoundedSubprocessError as exc:
            logger.error(
                "manifest live-build FAILED in bounded child for service=%s [%s..%s]: %s",
                service,
                start_date,
                end_date,
                exc,
            )
            return await self._live_build_fallback(
                service,
                start_date,
                end_date,
                asset_groups,
                secondary_axis,
                any_row_filter,
                estimate_bytes,
                child_error=str(exc),
            )

    async def _live_build_fallback(
        self,
        service: str,
        start_date: str,
        end_date: str,
        asset_groups: list[str] | None,
        secondary_axis: str | None,
        any_row_filter: bool,
        estimate_bytes: int,
        *,
        child_error: str | None = None,
    ) -> dict[str, object]:
        """Serve a stale rollup if one exists, else a structured refusal.

        Called when the pre-flight guard refuses the live build outright, or
        the bounded child failed anyway. The rollup is filter-free (see
        ``get_manifest_status`` docstring) so the stale-serve fallback only
        applies to unfiltered requests (``not any_row_filter``) — serving a
        filter-ignorant stale rollup for a secondary-axis drilldown query
        would silently answer the wrong question.
        """
        if not any_row_filter:
            stale = await asyncio.to_thread(_dss._read_rollup_allow_stale, service)  # pyright: ignore[reportPrivateUsage]  # facade patch-point (late-bound)
            if stale is not None:
                payload, last_modified_iso = stale
                response = slice_rollup_to_window(payload, start_date, end_date, asset_groups)
                response["stale"] = True
                response["stale_as_of"] = last_modified_iso
                response["stale_reason"] = (
                    "on-demand live build refused or failed (too large for a single request) "
                    "-- serving the last precomputed rollup instead"
                )
                if secondary_axis:
                    response["secondary_axis"] = secondary_axis
                logger.info(
                    "manifest live-build for service=%s served STALE rollup (as_of=%s, child_error=%s)",
                    service,
                    last_modified_iso,
                    child_error,
                )
                return response

        try:
            days: int | None = (dt.date.fromisoformat(end_date) - dt.date.fromisoformat(start_date)).days + 1
        except ValueError:
            days = None
        budget_mb = _LIVE_BUILD_MEMORY_BUDGET_BYTES // (1024 * 1024)
        estimate_mb = estimate_bytes // (1024 * 1024)
        detail = (
            f"On-demand build estimated at ~{estimate_mb} MB, over the {budget_mb} MB safety budget "
            "for a single request -- refused to protect the shared container from an OOM crash. "
            "Narrow the date range (e.g. the last 3-6 months), select fewer asset groups, or add a "
            "venue filter, then retry."
        )
        if child_error is not None:
            detail = f"On-demand build failed inside its resource-bounded child process ({child_error}). {detail}"
        logger.info("manifest live-build for service=%s refused with NO rollup fallback available: %s", service, detail)
        response: dict[str, object] = {
            "service": service,
            "date_range": {"start": start_date, "end": end_date, "days": days},
            "mode": "live_build_refused",
            "refused": True,
            "detail": detail,
            "estimated_bytes": estimate_bytes,
            "budget_bytes": _LIVE_BUILD_MEMORY_BUDGET_BYTES,
            "overall_completion_pct": 0.0,
            "asset_groups": {},
        }
        if secondary_axis:
            response["secondary_axis"] = secondary_axis
        return response

    def _resolve_cat_list(self, service: str, asset_groups: list[str] | None) -> list[str]:
        """Category list a manifest build would actually process for this service.

        Shared by the pre-flight cost estimate (``get_manifest_status``) and
        the actual sync build (``_get_manifest_status_sync``) so the two can
        never drift on what "how many categories" means for a given request.
        """
        cat_list = asset_groups or [str(c) for c in MarketCategory]
        # Filter to only the categories this service actually targets (Appendix A
        # SSOT).  Rendering CEFI/TRADFI/SPORTS/PREDICTION as 0/0 for a
        # DEFI-only service (e.g. features-onchain-service) is misleading — it
        # looks like "missing data" when in fact the category is out-of-scope.
        allowed = self._SERVICE_CATEGORY_RESTRICTIONS.get(service)
        if allowed:
            cat_list = [c for c in cat_list if c.upper() in allowed]
        return cat_list

    def _get_manifest_status_sync(
        self,
        service: str,
        start_date: str,
        end_date: str,
        asset_groups: list[str] | None = None,
        secondary_axis: str | None = None,
        league_id: str | None = None,
        fixture_id: str | None = None,
        canonical_question_group: str | None = None,
        job_id: str | None = None,
        chain: str | None = None,
        cloud: str = "gcp",
        pipeline_modes: list[str] | None = None,
        venue: list[str] | None = None,
        scope: str = "could_exist",
    ) -> dict[str, object]:
        """Synchronous manifest status — returns TurboDataStatusResponse shape."""
        cat_list = self._resolve_cat_list(service, asset_groups)
        all_dates_range = pd.date_range(start_date, end_date, freq="D")
        all_date_strs = [d.strftime("%Y-%m-%d") for d in all_dates_range]
        total_days = len(all_dates_range)
        result_categories: dict[str, object] = {}
        overall_found = 0
        overall_expected = 0

        venue_mapping = _dss.VenueMapping()

        overall_shards_found = 0
        overall_shards_expected = 0
        # sum(attempt_coverage_pct * expected_cells) per category -> the overall
        # attempt-coverage (shards-weighted). Lets the UI show an explicit
        # capture-vs-attempt split instead of one ambiguous "completion %". (R7, 2026-06-15)
        overall_attempt_weighted = 0.0

        # Pack secondary-axis filter params into a dict the per-category builder
        # applies after the date mask but before the cell-grid compute. Empty/None
        # values are dropped so a no-filter request behaves identically.
        row_filters = self._pack_row_filters(
            league_id=league_id,
            fixture_id=fixture_id,
            canonical_question_group=canonical_question_group,
            job_id=job_id,
            chain=chain,
        )

        results = self._dispatch_category_builds(
            cat_list,
            service,
            start_date,
            end_date,
            all_date_strs,
            total_days,
            venue_mapping,
            row_filters=row_filters,
            cloud=cloud,
            pipeline_modes=pipeline_modes,
            venue=venue,
            scope=scope,
        )

        for cat in cat_list:
            cat_result = results[cat]
            result_categories[cat] = cat_result
            overall_found += int(cat_result.get("dates_found", 0))  # pyright: ignore[reportArgumentType]
            overall_expected += int(cat_result.get("dates_expected", 0))  # pyright: ignore[reportArgumentType]
            overall_shards_found += int(cat_result.get("_venue_found", 0))  # pyright: ignore[reportArgumentType]
            overall_shards_expected += int(cat_result.get("_venue_expected", 0))  # pyright: ignore[reportArgumentType]
            overall_attempt_weighted += float(cat_result.get("attempt_coverage_pct", 0.0)) * int(  # pyright: ignore[reportArgumentType]
                cat_result.get("_venue_expected", 0)
            )
            del cat_result["_venue_found"]
            del cat_result["_venue_expected"]

        overall_pct_dates = min(round(overall_found / max(1, overall_expected) * 100, 2), 100.0)
        overall_pct_shards = (
            min(round(overall_shards_found / overall_shards_expected * 100, 2), 100.0)
            if overall_shards_expected > 0
            else overall_pct_dates
        )
        # Primary ``overall_completion_pct`` mirrors the per-category
        # ``completion_pct`` (which is shards-weighted) so the overall and
        # sub-rows use the same metric. Where no shards denominator exists we
        # fall back to the date-based figure so the number is still meaningful.
        overall_pct = overall_pct_shards
        # Explicit capture-vs-attempt coverage (R7, 2026-06-15) so the UI headline can
        # drop the ambiguous "completion %": CAPTURE = captured / could-exist (==
        # overall_pct_shards); ATTEMPT = (captured + empty_confirmed + failed) / could-exist,
        # shards-weighted across categories — "could-exist cells we have an HONEST answer
        # for" (empty_confirmed counts as covered, the operator-correct reading).
        overall_capture_coverage_pct = overall_pct_shards
        overall_attempt_coverage_pct = (
            min(round(overall_attempt_weighted / overall_shards_expected, 2), 100.0)
            if overall_shards_expected > 0
            else overall_pct_dates
        )
        # Flag migrations in progress so the UI can explain a suspiciously-low
        # overall number without the user having to cross-check running VMs.
        # Heuristic: overall < 10% of shards expected AND a backfill/migration
        # VM is currently running for this service. We keep this shallow —
        # deeper VM introspection lives in the deployment-service API.
        migration_in_progress = bool(
            overall_shards_expected > 0
            and overall_shards_found < (overall_shards_expected * 0.1)
            and self._has_active_migration_vm(service)
        )

        response: dict[str, object] = {
            "service": service,
            "date_range": {"start": start_date, "end": end_date, "days": total_days},
            "mode": "turbo",
            "sub_dimension": "venue",
            "overall_completion_pct": overall_pct,
            "overall_completion_pct_dates": overall_pct_dates,
            "overall_completion_pct_shards_weighted": overall_pct_shards,
            "overall_capture_coverage_pct": overall_capture_coverage_pct,
            "overall_attempt_coverage_pct": overall_attempt_coverage_pct,
            "overall_dates_found": overall_found,
            "overall_dates_expected": overall_expected,
            "overall_shards_found": overall_shards_found,
            "overall_shards_expected": overall_shards_expected,
            "migration_in_progress": migration_in_progress,
            "asset_groups": result_categories,
        }
        # Echo secondary_axis + active filters back so the UI can confirm
        # which slice it received. No-filter requests omit these keys --
        # existing /manifest consumers expect them absent on unfiltered reads.
        if secondary_axis:
            response["secondary_axis"] = secondary_axis
        if row_filters:
            response["filters"] = dict(row_filters)
        return response

    def _has_active_migration_vm(self, service: str) -> bool:
        """Best-effort check whether a migration/backfill VM is running.

        Uses the shared ``get_compute_engine_client`` facade the rest of
        deployment-api uses. Failures return ``False`` — this is purely an
        advisory flag for the UI, never a gate on completion math. Includes
        ``ValueError`` in the caught set: ``get_compute_engine_client`` raises
        it when the ambient ``UnifiedCloudConfig`` resolves to a non-GCP
        provider (e.g. ``CloudProvider.LOCAL`` in some local/test
        environments) — that is exactly the kind of environment mismatch this
        best-effort advisory check must degrade past, not surface as an
        unhandled 500.
        """
        try:
            from unified_trading_library import get_compute_engine_client

            svc_key = service.replace("-service", "").replace("market-data-processing", "mdps")
            svc_key = svc_key.replace("market-tick-data", "mtds").lower()
            ce = get_compute_engine_client(project_id=self.project_id)
            # aggregatedList returns every zone — we only care about name +
            # status. The API is dict-like on the .items() mapping; each
            # zone bucket carries an ``instances`` list we walk once.
            raw = ce.instances().aggregatedList(project=self.project_id).execute()  # pyright: ignore[reportAttributeAccessIssue,reportUnknownVariableType,reportUnknownMemberType]
            items: object = raw.get("items", {}) if isinstance(raw, dict) else {}  # pyright: ignore[reportUnknownVariableType,reportUnknownMemberType]  # noqa: qg-empty-fallback — defensive GCS JSON parse
            if not isinstance(items, dict):
                return False
            for zone_bucket in items.values():  # pyright: ignore[reportUnknownVariableType]
                if not isinstance(zone_bucket, dict):
                    continue
                instances = zone_bucket.get("instances") or []  # pyright: ignore[reportUnknownVariableType,reportUnknownMemberType]
                if not isinstance(instances, list):
                    continue
                for inst in instances:  # pyright: ignore[reportUnknownVariableType]
                    if not isinstance(inst, dict):
                        continue
                    name = str(inst.get("name") or "").lower()  # pyright: ignore[reportUnknownMemberType,reportUnknownArgumentType]
                    status = str(inst.get("status") or "").upper()  # pyright: ignore[reportUnknownMemberType,reportUnknownArgumentType]
                    if status != "RUNNING":
                        continue
                    if svc_key in name and "backfill" in name:
                        return True
            return False
        except (ImportError, OSError, RuntimeError, KeyError, AttributeError, ValueError):
            return False

    def _dispatch_category_builds(
        self,
        cat_list: list[str],
        service: str,
        start_date: str,
        end_date: str,
        all_date_strs: list[str],
        total_days: int,
        venue_mapping: VenueMapping,
        *,
        row_filters: dict[str, str] | None,
        cloud: str,
        pipeline_modes: list[str] | None,
        venue: list[str] | None = None,
        scope: str = "could_exist",
    ) -> dict[str, dict[str, object]]:
        """Build every category's manifest entry, returning ``{cat: result}``. Three paths:

        - PROCESS pool (Linux): fork children inherit the parent's loaded ``_INDEX_CACHE``
          (~30 MB DataFrames/bucket) via copy-on-write; only small picklable args cross the Pipe.
        - THREAD pool (process pool disabled — e.g. macOS dev, where fork after grpc/GCS init dies
          → BrokenProcessPool): each category's dominant cost is ``_read_defi_merged_index`` — an
          I/O-bound GCS download + pyarrow parse that RELEASES the GIL — so threading overlaps the
          per-asset-group index loads even though the cell-grid compute stays GIL-serialised. That
          collapses the cold ~5x (index load) serial cost to ~1x (the macOS slowness fix). Both
          pools cap at ``_dss._MAX_BUILD_WORKERS`` to bound concurrent cell-grid memory (the OOM fix
          — fanning all 5 AGs out at once peaked at 8604 MiB and killed the 8 GiB instance).
        - SERIAL: a single category — fan-out overhead would dwarf the work.

        ``scope != "could_exist"`` (i.e. ``"mvp"``) disables the process pool the
        same way ``row_filters``/``pipeline_modes``/``venue`` already do:
        ``build_category_in_subprocess``'s fixed positional signature doesn't
        carry it across the fork boundary, so a non-default scope falls
        through to the thread/serial paths below, which call
        ``_build_manifest_category`` directly (kwargs, no pickling needed).
        """
        use_process_pool = (
            len(cat_list) > 1
            and not _dss._PROCESS_POOL_DISABLED  # pyright: ignore[reportPrivateUsage]  # facade patch-point (late-bound)
            and not row_filters
            and not pipeline_modes
            and not venue
            and scope == "could_exist"
        )
        if use_process_pool:
            ctx = multiprocessing.get_context("fork")
            with ProcessPoolExecutor(
                max_workers=min(len(cat_list), _dss._MAX_BUILD_WORKERS),  # pyright: ignore[reportPrivateUsage]  # memory-budget cap (OOM fix)
                mp_context=ctx,
            ) as pool:
                pp_futures = {
                    pool.submit(
                        build_category_in_subprocess,
                        service,
                        cat,
                        start_date,
                        end_date,
                        all_date_strs,
                        total_days,
                        cloud,
                    ): cat
                    for cat in cat_list
                }
                return {pp_futures[f]: f.result() for f in pp_futures}
        if len(cat_list) > 1 and not _dss._THREAD_POOL_DISABLED:  # pyright: ignore[reportPrivateUsage]  # facade patch-point (late-bound)
            with ThreadPoolExecutor(max_workers=min(len(cat_list), _dss._MAX_BUILD_WORKERS)) as tpool:  # pyright: ignore[reportPrivateUsage]  # memory-budget cap (OOM fix)
                tp_futures = {
                    tpool.submit(
                        self._build_manifest_category,
                        service,
                        cat,
                        start_date,
                        end_date,
                        all_date_strs,
                        total_days,
                        venue_mapping,
                        row_filters=row_filters,
                        cloud=cloud,
                        pipeline_modes=pipeline_modes,
                        venue=venue,
                        scope=scope,
                    ): cat
                    for cat in cat_list
                }
                return {tp_futures[f]: f.result() for f in tp_futures}
        return {
            cat: self._build_manifest_category(
                service,
                cat,
                start_date,
                end_date,
                all_date_strs,
                total_days,
                venue_mapping,
                row_filters=row_filters,
                cloud=cloud,
                pipeline_modes=pipeline_modes,
                venue=venue,
                scope=scope,
            )
            for cat in cat_list
        }

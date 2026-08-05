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
from concurrent.futures import ThreadPoolExecutor

import pandas as pd
from unified_api_contracts import VenueMapping
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


from deployment_api.services.data_status.manifest_status_helpers import (
    ManifestStatusHelpersMixin,
    _CategoryBuildContext,
    _ManifestBuildRequest,
)


class ManifestStatusMixin(ManifestStatusHelpersMixin):
    """get_manifest_status + live-build guard + subprocess fan-out dispatch.

    The data_status mixins form a single linear inheritance chain
    (cli -> defi -> sports -> breakdowns_domain -> breakdowns_core ->
    venue_resolution -> coverage -> missing_shards -> manifest_category_builder
    -> manifest_status_helpers -> manifest) so that every cross-group
    ``self._method`` reference resolves statically under basedpyright strict.
    ``DataStatusService`` composes the top of the chain and is the ONLY
    public entry point — import it from
    ``deployment_api.services.data_status_service`` (the facade).

    The methods below stay here rather than in the ``manifest_status_helpers``
    sibling because each either calls a name (``slice_rollup_to_window`` /
    ``run_bounded`` / ``ThreadPoolExecutor``) that
    ``tests/unit/test_data_status_service.py`` /
    ``tests/unit/services/test_manifest_source.py`` /
    ``tests/unit/test_data_status_beta_rollup_and_cli_config.py`` patch by
    the literal module path ``deployment_api.services.data_status.manifest.<name>``
    (a patch only intercepts a bare-name lookup resolved through the
    PATCHED module's own globals), or calls a sibling method that does —
    the mixin chain's ordering invariant means an earlier link
    (``manifest_status_helpers``) can never call back into a method that
    lives only here.
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
        quote_asset: str | None = None,
        margin_type: str | None = None,
        pipeline_modes: list[str] | None = None,
        venue: list[str] | None = None,
        scope: str = "could_exist",
    ) -> dict[str, object]:
        """Return data status: rollup fast-path, else an on-demand build
        behind a two-layer OOM guard — see :meth:`_manifest_status_preflight`
        / :meth:`_manifest_status_bounded_build`.
        """
        req = _ManifestBuildRequest(
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
            quote_asset=quote_asset,
            margin_type=margin_type,
        )
        _row_filters = (league_id, fixture_id, canonical_question_group, job_id, chain, quote_asset, margin_type)
        any_row_filter = any(f is not None and f != "" for f in _row_filters) or bool(pipeline_modes) or bool(venue)
        if not any_row_filter and (rollup := await self._manifest_status_rollup_fast_path(req)) is not None:
            return rollup

        estimate_bytes, refusal = await self._manifest_status_preflight(req, pipeline_modes, venue, any_row_filter)
        if refusal is not None:
            return refusal

        return await self._manifest_status_bounded_build(
            req, cloud, pipeline_modes, venue, scope, any_row_filter, estimate_bytes
        )

    async def _manifest_status_rollup_fast_path(self, req: _ManifestBuildRequest) -> dict[str, object] | None:
        """Read + slice the precomputed rollup blob if fresh, else ``None``.

        The all-asset-group view is served from the precomputed rollup cache
        (``{service}/full.json.gz`` via ``_read_rollup_if_fresh``) instead of
        live-computing every AG per request (which exceeds the Cloud Run
        request timeout -> HTTP 503). Only called for filter-free requests —
        the rollup itself carries no row-filter dimension.
        """
        rollup = await asyncio.to_thread(_dss._read_rollup_if_fresh, req.service)  # pyright: ignore[reportPrivateUsage]  # facade patch-point (late-bound)
        if rollup is None:
            return None
        response = slice_rollup_to_window(rollup, req.start_date, req.end_date, req.asset_groups)
        if req.secondary_axis:
            response["secondary_axis"] = req.secondary_axis
        return response

    async def _manifest_status_preflight(
        self,
        req: _ManifestBuildRequest,
        pipeline_modes: list[str] | None,
        venue: list[str] | None,
        any_row_filter: bool,
    ) -> tuple[int, dict[str, object] | None]:
        """Layer-1 pre-flight cost estimate.

        Returns ``(estimate_bytes, refusal)`` — ``refusal`` is non-``None``
        (already computed via :meth:`_live_build_fallback`) when the
        estimate exceeds ``_LIVE_BUILD_MEMORY_BUDGET_BYTES``.
        """
        has_secondary_row_filter = any(
            f is not None and f != ""
            for f in (req.league_id, req.fixture_id, req.canonical_question_group, req.job_id, req.chain)
        ) or bool(pipeline_modes)
        cat_list = self._resolve_cat_list(req.service, req.asset_groups)
        estimate_bytes = estimate_live_build_bytes(
            service=req.service,
            start_date=req.start_date,
            end_date=req.end_date,
            category_count=len(cat_list),
            venue_count=len(venue) if venue else None,
            has_row_filter=has_secondary_row_filter,
        )
        if not would_exceed_budget(estimate_bytes, _LIVE_BUILD_MEMORY_BUDGET_BYTES):
            return estimate_bytes, None
        logger.warning(
            "manifest live-build REFUSED (pre-flight) for service=%s [%s..%s]: estimate=%dMB > budget=%dMB",
            req.service,
            req.start_date,
            req.end_date,
            estimate_bytes // (1024 * 1024),
            _LIVE_BUILD_MEMORY_BUDGET_BYTES // (1024 * 1024),
        )
        refusal = await self._live_build_fallback(req, any_row_filter, estimate_bytes)
        return estimate_bytes, refusal

    async def _manifest_status_bounded_build(
        self,
        req: _ManifestBuildRequest,
        cloud: str,
        pipeline_modes: list[str] | None,
        venue: list[str] | None,
        scope: str,
        any_row_filter: bool,
        estimate_bytes: int,
    ) -> dict[str, object]:
        """Layer 2 defense-in-depth: run the on-demand build inside a
        resource-bounded spawned child, falling back to
        :meth:`_live_build_fallback` on failure. ``run_bounded`` is patched
        by the literal path ``deployment_api.services.data_status.manifest.run_bounded``
        in ``tests/unit/test_data_status_service.py`` — this call must stay
        physically in this module for that patch to keep intercepting it.
        """
        try:
            return await asyncio.to_thread(
                run_bounded,
                _build_manifest_live_in_subprocess,
                req.service,
                req.start_date,
                req.end_date,
                req.asset_groups,
                req.secondary_axis,
                req.league_id,
                req.fixture_id,
                req.canonical_question_group,
                req.job_id,
                req.chain,
                cloud,
                pipeline_modes,
                venue,
                scope,
                rlimit_bytes=_LIVE_BUILD_CHILD_RLIMIT_BYTES,
                timeout_s=_LIVE_BUILD_CHILD_TIMEOUT_S,
                name=f"manifest-live-{req.service[:24]}",
            )
        except BoundedSubprocessError as exc:
            logger.error(
                "manifest live-build FAILED in bounded child for service=%s [%s..%s]: %s",
                req.service,
                req.start_date,
                req.end_date,
                exc,
            )
            return await self._live_build_fallback(req, any_row_filter, estimate_bytes, child_error=str(exc))

    async def _live_build_fallback(
        self,
        req: _ManifestBuildRequest,
        any_row_filter: bool,
        estimate_bytes: int,
        *,
        child_error: str | None = None,
    ) -> dict[str, object]:
        """Serve a stale rollup if one exists, else a structured refusal.

        Called when the pre-flight guard refuses the live build outright, or
        the bounded child failed anyway. The rollup is filter-free so the
        stale-serve fallback only applies to unfiltered requests — a
        filter-ignorant stale rollup for a secondary-axis drilldown query
        would silently answer the wrong question.
        """
        if not any_row_filter:
            stale_response = await self._live_build_stale_rollup_response(req, child_error)
            if stale_response is not None:
                return stale_response
        return self._live_build_refusal_response(req, estimate_bytes, child_error)

    async def _live_build_stale_rollup_response(
        self, req: _ManifestBuildRequest, child_error: str | None
    ) -> dict[str, object] | None:
        """Slice the last precomputed rollup (however stale) if one exists."""
        stale = await asyncio.to_thread(_dss._read_rollup_allow_stale, req.service)  # pyright: ignore[reportPrivateUsage]  # facade patch-point (late-bound)
        if stale is None:
            return None
        payload, last_modified_iso = stale
        response = slice_rollup_to_window(payload, req.start_date, req.end_date, req.asset_groups)
        response["stale"] = True
        response["stale_as_of"] = last_modified_iso
        response["stale_reason"] = (
            "on-demand live build refused or failed (too large for a single request) "
            "-- serving the last precomputed rollup instead"
        )
        if req.secondary_axis:
            response["secondary_axis"] = req.secondary_axis
        logger.info(
            "manifest live-build for service=%s served STALE rollup (as_of=%s, child_error=%s)",
            req.service,
            last_modified_iso,
            child_error,
        )
        return response

    def _resolve_cat_list(self, service: str, asset_groups: list[str] | None) -> list[str]:
        """Category list a manifest build would actually process for this service.

        Shared by the pre-flight cost estimate (:meth:`_manifest_status_preflight`)
        and the actual sync build (:meth:`_get_manifest_status_sync`) so the
        two can never drift on what "how many categories" means for a given
        request.
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

    def _manifest_sync_dispatch(
        self,
        service: str,
        start_date: str,
        end_date: str,
        asset_groups: list[str] | None,
        league_id: str | None,
        fixture_id: str | None,
        canonical_question_group: str | None,
        job_id: str | None,
        chain: str | None,
        cloud: str,
        pipeline_modes: list[str] | None,
        venue: list[str] | None,
        scope: str,
    ) -> tuple[list[str], int, dict[str, str] | None, dict[str, dict[str, object]]]:
        """Resolve the category list + date grid, pack row filters, and fan
        out the per-category builds — the setup half of
        :meth:`_get_manifest_status_sync`, split out to keep that method
        under the 50-line method-size gate."""
        cat_list = self._resolve_cat_list(service, asset_groups)
        all_date_strs = [d.strftime("%Y-%m-%d") for d in pd.date_range(start_date, end_date, freq="D")]
        total_days = len(all_date_strs)
        venue_mapping = _dss.VenueMapping()
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
        return cat_list, total_days, row_filters, results

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
        cat_list, total_days, row_filters, results = self._manifest_sync_dispatch(
            service,
            start_date,
            end_date,
            asset_groups,
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
        result_categories, totals = self._manifest_sync_aggregate_categories(cat_list, results)
        pcts = self._manifest_sync_compute_overall_pcts(totals)
        migration_in_progress = self._manifest_sync_migration_flag(service, totals)
        return self._manifest_sync_build_response(
            service,
            start_date,
            end_date,
            total_days,
            secondary_axis,
            row_filters,
            result_categories,
            totals,
            pcts,
            migration_in_progress,
        )

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
        """Build every category's manifest entry, returning ``{cat: result}``.

        Three paths — :meth:`_dispatch_via_process_pool` (Linux fork),
        :meth:`_dispatch_via_thread_pool` (process pool disabled), or
        :meth:`_dispatch_serial` (a single category). Both pools cap at
        ``_dss._MAX_BUILD_WORKERS`` (the OOM fix).
        """
        ctx = _CategoryBuildContext(
            service,
            start_date,
            end_date,
            all_date_strs,
            total_days,
            venue_mapping,
            row_filters,
            cloud,
            pipeline_modes,
            venue,
            scope,
        )
        use_process_pool = (
            len(cat_list) > 1
            and not _dss._PROCESS_POOL_DISABLED  # pyright: ignore[reportPrivateUsage]  # facade patch-point (late-bound)
            and not row_filters
            and not pipeline_modes
            and not venue
            and scope == "could_exist"
        )
        if use_process_pool:
            return self._dispatch_via_process_pool(cat_list, ctx)
        if len(cat_list) > 1 and not _dss._THREAD_POOL_DISABLED:  # pyright: ignore[reportPrivateUsage]  # facade patch-point (late-bound)
            return self._dispatch_via_thread_pool(cat_list, ctx)
        return self._dispatch_serial(cat_list, ctx)

    def _dispatch_via_thread_pool(
        self, cat_list: list[str], ctx: _CategoryBuildContext
    ) -> dict[str, dict[str, object]]:
        """ThreadPoolExecutor fan-out — each category's dominant cost is the
        I/O-bound GCS/pyarrow index load, which releases the GIL, so threads
        still overlap even though the cell-grid compute stays GIL-serialised.
        ``ThreadPoolExecutor`` is patched by the literal path
        ``deployment_api.services.data_status.manifest.ThreadPoolExecutor``
        in ``tests/unit/test_data_status_beta_rollup_and_cli_config.py`` —
        this call must stay physically in this module for that patch to
        keep intercepting it."""
        with ThreadPoolExecutor(max_workers=min(len(cat_list), _dss._MAX_BUILD_WORKERS)) as tpool:  # pyright: ignore[reportPrivateUsage]  # memory-budget cap (OOM fix)
            tp_futures = {
                tpool.submit(
                    self._build_manifest_category,
                    ctx.service,
                    cat,
                    ctx.start_date,
                    ctx.end_date,
                    ctx.all_date_strs,
                    ctx.total_days,
                    ctx.venue_mapping,
                    row_filters=ctx.row_filters,
                    cloud=ctx.cloud,
                    pipeline_modes=ctx.pipeline_modes,
                    venue=ctx.venue,
                    scope=ctx.scope,
                ): cat
                for cat in cat_list
            }
            return {tp_futures[f]: f.result() for f in tp_futures}

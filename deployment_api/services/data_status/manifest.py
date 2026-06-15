"""Manifest-status surface: per-category build + process-pool fan-out.

Split out of the 6,663-line ``data_status_service.py`` god-module
(codex ratchet plan 2026-06-10). The facade module re-exports every
public + legacy-underscore name, so callers keep importing from
``deployment_api.services.data_status_service``.
"""

import asyncio
import datetime as dt
import logging
import multiprocessing
from concurrent.futures import ProcessPoolExecutor, ThreadPoolExecutor
from typing import cast

import pandas as pd
from unified_api_contracts import (
    VenueMapping,
)
from unified_api_contracts.internal import MarketCategory

import deployment_api.services.data_status_service as _dss
from deployment_api.services.data_status_drilldown import (
    COMMODITY_BUCKET_TEMPLATE,
    PREDICTION_KIND_MAP,
    SERVICE_TO_KIND,
)

logger = logging.getLogger(__name__)

from deployment_api.services.data_status.coverage_metrics import (
    build_coverage_metrics,
    build_failure_rate_by_dimension,
)
from deployment_api.services.data_status.mtds import (
    MTDS_CATEGORY_META,
    canonicalise_defi_data_types,
    is_mtds_honest_coverage_target,
    mtds_expected_venues,
)
from deployment_api.services.data_status.rollup_cache import slice_rollup_to_window


async def prewarm_indexes(service: str, *, days: int = 7, cloud: str = "gcp") -> None:
    """Warm ``_INDEX_CACHE`` for a service by running a tiny manifest query at startup.

    The manifest index load is whole-file (not date-windowed) — so a cheap 7-day query
    loads and caches (5-min TTL) every asset-group index the Data Status landing page
    needs, with negligible cell-grid compute. The user's first real query then hits warm
    cache (~10s) instead of the cold ~50s/asset-group transpacific GCS fetch. Best-effort:
    raises nothing the caller must handle (the lifespan wrapper logs + swallows). Honours
    beta mode (reads through the same beta-aware seam)."""
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


from deployment_api.services.data_status.missing_shards import MissingShardsMixin


class ManifestStatusMixin(MissingShardsMixin):
    """get_manifest_status + per-category manifest entry builders.

    The data_status mixins form a single linear inheritance chain
    (cli -> defi -> sports -> breakdowns_domain -> breakdowns_core ->
    venue_resolution -> coverage -> missing_shards -> manifest) so that
    every cross-group ``self._method`` reference resolves statically
    under basedpyright strict. ``DataStatusService`` composes the top of
    the chain and is the ONLY public entry point — import it from
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
        """
        any_row_filter = any(
            f is not None and f != "" for f in (league_id, fixture_id, canonical_question_group, job_id, chain)
        ) or bool(pipeline_modes)
        # CF-20 beta preview: the rollup is now BETA-NAMESPACED (the worker writes
        # ``{service}/full.beta.json.gz`` when it runs with the beta env;
        # ``_read_rollup_if_fresh`` reads the same beta blob in beta mode — see
        # ``rollup_cache.rollup_blob_path``). So serving it in beta mode renders
        # BETA-derived data, not live — the invariant holds, and the all-asset-group
        # beta view is served from cache instead of live-computing every AG per
        # request (which exceeds the Cloud Run request timeout -> HTTP 503). If the
        # beta rollup hasn't been written yet, the read returns None and we fall
        # through to the (slower) beta-aware on-demand compute.
        if not any_row_filter:
            rollup = await asyncio.to_thread(_dss._read_rollup_if_fresh, service)  # pyright: ignore[reportPrivateUsage]  # facade patch-point (late-bound)
            if rollup is not None:
                response = slice_rollup_to_window(rollup, start_date, end_date, asset_groups)
                if secondary_axis:
                    response["secondary_axis"] = secondary_axis
                return response
        return await asyncio.to_thread(
            self._get_manifest_status_sync,
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
        )

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
    ) -> dict[str, object]:
        """Synchronous manifest status — returns TurboDataStatusResponse shape."""
        cat_list = asset_groups or [str(c) for c in MarketCategory]
        # Filter to only the categories this service actually targets (Appendix A
        # SSOT).  Rendering CEFI/TRADFI/SPORTS/PREDICTION as 0/0 for a
        # DEFI-only service (e.g. features-onchain-service) is misleading — it
        # looks like "missing data" when in fact the category is out-of-scope.
        allowed = self._SERVICE_CATEGORY_RESTRICTIONS.get(service)
        if allowed:
            cat_list = [c for c in cat_list if c.upper() in allowed]
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
        advisory flag for the UI, never a gate on completion math.
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
        except (ImportError, OSError, RuntimeError, KeyError, AttributeError):
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
    ) -> dict[str, dict[str, object]]:
        """Build every category's manifest entry, returning ``{cat: result}``. Three paths:

        - PROCESS pool (Linux): fork children inherit the parent's loaded ``_INDEX_CACHE``
          (~30 MB DataFrames/bucket) via copy-on-write; only small picklable args cross the Pipe.
        - THREAD pool (process pool disabled — e.g. macOS dev, where fork after grpc/GCS init dies
          → BrokenProcessPool): each category's dominant cost is ``_read_defi_merged_index`` — an
          I/O-bound GCS download + pyarrow parse that RELEASES the GIL — so threading overlaps the
          per-asset-group index loads even though the cell-grid compute stays GIL-serialised. That
          collapses the cold ~5x (index load) serial cost to ~1x (the macOS slowness fix). Capped
          at 4 workers to bound concurrent cell-grid memory on a wide date range.
        - SERIAL: a single category — fan-out overhead would dwarf the work.
        """
        use_process_pool = (
            len(cat_list) > 1
            and not _dss._PROCESS_POOL_DISABLED  # pyright: ignore[reportPrivateUsage]  # facade patch-point (late-bound)
            and not row_filters
            and not pipeline_modes
        )
        if use_process_pool:
            ctx = multiprocessing.get_context("fork")
            with ProcessPoolExecutor(max_workers=min(len(cat_list), 5), mp_context=ctx) as pool:
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
            with ThreadPoolExecutor(max_workers=min(len(cat_list), 4)) as tpool:
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
            )
            for cat in cat_list
        }

    def _build_manifest_category(
        self,
        service: str,
        cat: str,
        start_date: str,
        end_date: str,
        all_date_strs: list[str],
        total_days: int,
        venue_mapping: VenueMapping,
        row_filters: dict[str, str] | None = None,
        cloud: str = "gcp",
        pipeline_modes: list[str] | None = None,
    ) -> dict[str, object]:
        """Build a single category entry for manifest status.

        ``row_filters`` is an optional ``{column: value}`` map applied to
        the manifest rows after the date-range mask (and venue alias
        canonicalisation) but before the cell-grid compute. Used by the
        ``secondary_axis`` query parameter on ``/api/data-status/manifest``
        so the UI can drill into a single ``league_id`` /
        ``canonical_question_group`` / ``job_id`` / ``chain`` /
        ``fixture_id`` slice. Empty/None == no filter.

        ``pipeline_modes`` narrows the manifest slice to rows whose
        ``pipeline_mode`` column matches any of the supplied values (OR semantics).
        Used by the deployment-ui pipeline_mode filter chip.
        """
        empty: dict[str, object] = {
            "category": cat,
            "bucket": "",
            "prefixes_queried": 0,
            "dates_found": 0,
            "dates_expected": 0,
            "dates_missing": 0,
            "completion_pct": 0.0,
            "missing_dates": [],
            "venues": {},
            "_venue_found": 0,
            "_venue_expected": 0,
        }
        if service not in SERVICE_TO_KIND and service != "features-commodity-service":
            return empty

        # Skip categories that don't apply to this service (single-bucket services)
        allowed = self._SERVICE_CATEGORY_RESTRICTIONS.get(service)
        if allowed and cat.upper() not in allowed:
            return empty

        # Resolve the main bucket name (for display in the response)
        override = self._BUCKET_CATEGORY_OVERRIDES.get((service, cat.lower()))
        if override:
            bucket = override.format(pid=self.project_id, env=self.deployment_env_short)
        elif service == "features-commodity-service":
            bucket = COMMODITY_BUCKET_TEMPLATE.format(pid=self.project_id)
        else:
            kind = SERVICE_TO_KIND[service]
            ag = cat.lower() or None
            if ag == "prediction":
                pred_kind = PREDICTION_KIND_MAP.get(kind)
                bucket = _dss.resolve_bucket_name(cloud=cast(object, cloud), kind=pred_kind if pred_kind else kind)  # pyright: ignore[reportArgumentType]
            else:
                bucket = _dss.resolve_bucket_name(cloud=cast(object, cloud), kind=kind, asset_group=cast(object, ag))  # pyright: ignore[reportArgumentType]

        index = self._read_defi_merged_index(service, cat, cloud=cloud)
        if index.empty:
            return empty

        # Clamp the category-level start date to the configured launch date
        # (from expected_start_dates.yaml). Pre-launch dates are not "missing"
        # — they never existed. Only the aggregation math is clamped; the raw
        # manifest data is untouched.
        effective_start = _dss.get_effective_start_date(start_date, service, cat)
        # Genesis clip (R7, 2026-06-15): expected_start_dates.yaml has no launch date
        # for most instruments-service asset_groups, so a YOUNG asset_group was charged
        # for every day back to the search-horizon start — e.g. PREDICTION showed
        # dates 436/3088 = 14% purely from pre-launch days, while its shards were 95%.
        # Clamp ``effective_start`` to ALSO be >= the category's DATA-OBSERVED genesis
        # (the earliest manifest date for this service+category, already loaded above) so
        # pre-genesis calendar days drop out of ``dates_expected``. A configured launch
        # date still wins when it is LATER. The raw manifest data is untouched (display-only).
        _svc_dates = (
            index.loc[index["service_name"] == service, "date"] if "service_name" in index.columns else index["date"]
        )
        if len(_svc_dates) > 0:
            _genesis = str(_svc_dates.min())
            if _genesis > effective_start:
                effective_start = _genesis
        cat_date_strs = [d for d in all_date_strs if d >= effective_start]
        cat_total_days = len(cat_date_strs)

        mask = (index["date"] >= effective_start) & (index["date"] <= end_date)
        if "service_name" in index.columns:
            mask = mask & (index["service_name"] == service)
        filtered = index.loc[mask].copy()

        # Apply per-row filter params from the /manifest secondary-axis
        # query (see ``_apply_row_filters`` for semantics).
        if row_filters:
            filtered = self._apply_row_filters(filtered, row_filters)
        # Apply pipeline_mode filter (OR across requested modes).
        if pipeline_modes:
            filtered = self._apply_pipeline_mode_filter(filtered, pipeline_modes)

        # Fold bare venue aliases (e.g. "OKX" → "OKX-SPOT", "COINBASE" → "COINBASE-SPOT")
        if "venue" in filtered.columns and not filtered.empty:
            filtered["venue"] = filtered["venue"].replace(self._VENUE_ALIASES)

        # Drop pre-canonicalisation DeFi venue-alias rows (e.g.
        # ``venue='AAVE_V3-ETHEREUM' chain=''``) so they don't inflate
        # ``venue_dates_expected`` against canonical rows
        # (``venue='AAVE_V3' chain='ETHEREUM'``). DEFI-scoped only —
        # CeFi hyphenated venues (BINANCE-FUTURES, OKX-SWAP, ...) are
        # in category=='cefi' and are not touched.
        if cat.lower() == "defi" and not filtered.empty and "venue" in filtered.columns:
            chain_series = (
                filtered["chain"]
                if "chain" in filtered.columns
                else pd.Series([""] * len(filtered), index=filtered.index)
            )
            legacy_mask = [
                self._is_legacy_defi_venue_row(v, c)  # pyright: ignore[reportAny]
                for v, c in zip(filtered["venue"].tolist(), chain_series.tolist(), strict=True)  # pyright: ignore[reportAny]
            ]
            if any(legacy_mask):
                dropped = int(sum(legacy_mask))
                logger.debug(
                    "Filtered %d legacy DeFi venue-alias rows (pre-canonicalisation) from %s",
                    dropped,
                    cat,
                )
                filtered = filtered.loc[[not m for m in legacy_mask]].copy()

        # 2026-05-07 DEFI fallback removal: venue canonicalisation moved to
        # the writer (UTL ``ManifestWriter`` hook) + 2026-05-07 migration
        # closed all legacy underscore DeFi-venue rows. Per workspace rule
        # "Manifest migration, NOT fallback" the venue-side read-time
        # fallback is gone. Hyphenated ``data_type`` values
        # (``dex-pools``/``lending-indices``/…) still need normalisation
        # downstream until a paired data_type migration runs.
        if cat.lower() == "defi" and not filtered.empty:
            filtered = canonicalise_defi_data_types(filtered)

        cat_found_dates = {str(d) for d in filtered["date"].unique()} if not filtered.empty else set()  # pyright: ignore[reportUnknownVariableType,reportAny]
        cat_missing = sorted(set(cat_date_strs) - cat_found_dates)
        cat_found = len(cat_found_dates)  # pyright: ignore[reportUnknownArgumentType]

        # Per-venue breakdown (includes data_type sub-dimension for multi-data-type services)
        venues_dict, venue_found_total, venue_expected_total = self._build_venue_breakdown(
            filtered,
            effective_start,
            end_date,
            venue_mapping,
            cat_found,
            cat_total_days,
            service=service,
            category=cat,
            cloud=cloud,
        )

        # MTDS honest-coverage override (Phase 6c). For CEFI / TRADFI / DEFI /
        # PREDICTION, recompute per-venue ``dates_found`` / ``dates_expected``
        # from the UAC-driven ``(venue, data_type, date)`` shard space AND
        # inject UAC-declared venues that had zero manifest rows. The old
        # path iterated only venues observed in the manifest, so a venue
        # missing completely (e.g. UPBIT with no trades shipped) was
        # invisible. SSOT: codex/02-data/mtds-data-source-coverage-matrix.md.
        if is_mtds_honest_coverage_target(service, cat):
            (
                venues_dict,
                venue_found_total,
                venue_expected_total,
            ) = self._apply_mtds_honest_coverage(
                venues_dict,
                filtered,
                cat,
                effective_start,
                end_date,
                venue_mapping,
                cloud=cloud,
            )

        # When no venues or all are empty (sports instruments pattern), group
        # by data_type.  If there are BOTH empty-venue v4 rows AND old non-empty
        # v3 venue rows, prefer the v4 data_type grouping (it's the canonical view).
        # ``regrouped_to_data_type`` flips ``breakdown_axis`` below so MDPS rows
        # (CEFI/DEFI with venue="" + real data_types) render as data_types in the
        # UI instead of being mislabelled as venues.
        (
            venues_dict,
            venue_found_total,
            venue_expected_total,
            regrouped_to_data_type,
        ) = self._maybe_group_by_data_type(
            venues_dict,
            filtered,
            effective_start,
            end_date,
            cat,
            venue_found_total,
            venue_expected_total,
            service,
        )

        # Category-level completion, two variants exposed for the UI:
        #
        #   * ``completion_pct_dates`` — fraction of dates in the clamped
        #     range that had ANY data. Over-states the real coverage when
        #     a single venue fills a date but most shards stay empty.
        #   * ``completion_pct_shards_weighted`` — fraction of expected
        #     shard-days present (per_bucket.dates_found / per_bucket.dates_expected
        #     rolled up across every per-bucket breakdown). Matches the
        #     shard-level math the user sees in the sub-rows
        #     (e.g. Polymarket header shows 94.4% = what the sub-row
        #     completion averages to, not 100%).
        #
        # ``completion_pct`` — primary metric — is the shards-weighted
        # value. Where the shards denominator is zero (categories with
        # no per-bucket breakdown yet) we fall back to the date-based
        # figure so the number is still meaningful.
        cat_pct_dates = min(round(cat_found / max(1, cat_total_days) * 100, 2), 100.0)
        if venue_expected_total > 0:
            cat_pct_shards = min(round(venue_found_total / venue_expected_total * 100, 2), 100.0)
        else:
            cat_pct_shards = cat_pct_dates

        coverage = build_coverage_metrics(
            filtered,
            cat,
            cat_pct_shards,
            total_expected_cells=venue_expected_total,
        )
        coverage_semantics = coverage["coverage_semantics"]
        capture_coverage_pct = coverage["capture_coverage_pct"]
        attempt_coverage_pct = coverage["attempt_coverage_pct"]
        empty_rate_estimate = coverage["empty_rate_estimate"]
        failure_rate = coverage["failure_rate"]
        capture_status_counts = coverage["capture_status_counts"]
        counts = coverage["counts"]
        coverage_val = float(coverage["coverage"])  # pyright: ignore[reportArgumentType]
        cat_pct = coverage["completion_pct"]

        # v4 sub-dimension breakdowns (DeFi, chains, feature groups)
        sub_dims = self._build_v4_sub_dimensions(
            filtered,
            service,
            cat,
            effective_start,
            end_date,
            venue_mapping,
        )

        cat_found_sorted = sorted(cat_found_dates)  # pyright: ignore[reportUnknownArgumentType]
        # Sports uses fixture-based unit; other categories use dates
        unit = "fixtures" if cat.upper() == "SPORTS" and venues_dict else "dates"

        # Per-venue failure_rate map — surfaced so the UI's "show only failures"
        # filter and drill-down tooltip can scope to shards with failures
        # without walking the full venues_dict tree on the client.
        failure_rate_by_dimension = build_failure_rate_by_dimension(venues_dict)

        # Axis discriminator: tells consumers which breakdown key holds the
        # drilldown. For SPORTS the venue column is structurally empty so the
        # drilldown is always under ``data_types``. For other categories the
        # discriminator follows whatever ``_maybe_group_by_data_type`` actually
        # did: if it regrouped (MDPS CEFI/DEFI rows have empty venue + real
        # data_types) we flip to ``data_type`` so the UI doesn't render
        # data_type strings as venues. Otherwise the venue grouping is real
        # (CEFI/TRADFI/DEFI/PREDICTION on instruments-service / MTDS).
        # SSOT: codex/02-data/sports-data-source-coverage-matrix.md §3.
        breakdown_axis = "data_type" if cat.upper() == "SPORTS" or regrouped_to_data_type else "venue"
        result: dict[str, object] = {
            "category": cat,
            "bucket": bucket,
            "prefixes_queried": 0,
            "dates_found": cat_found,
            "dates_expected": cat_total_days,
            "dates_missing": len(cat_missing),
            # ``shards_*`` mirrors ``venue_dates_*`` under canonical names so
            # the UI can render a consistent pair alongside the row-level
            # ``completion_pct`` (which is the shards-weighted ratio — see
            # `cat_pct = cat_pct_shards` above). Before this field existed
            # the UI showed ``dates_found / dates_expected`` next to a
            # shards-weighted ``completion_pct``, which looked wrong (e.g.
            # ``1 / 2577`` = 0.04%, but the row displayed ``20%``).
            "shards_found": venue_found_total,
            "shards_expected": venue_expected_total,
            # PRIMARY metric = shards-weighted captured/could-exist (operator
            # decision 2026-06-14: canonical completion = captured / could-exist
            # universe). Was ``cat_pct`` (the build_coverage_metrics attempt/
            # date-blended value) which read ~42% while the shards ratio read
            # ~29% — the doc comment already claimed completion_pct was
            # shards-weighted, and the overall rollup already is, so this
            # aligns the per-category number with both.
            "completion_pct": cat_pct_shards,
            "completion_pct_dates": cat_pct_dates,
            "completion_pct_shards_weighted": cat_pct_shards,
            "completion_pct_attempt_blended": cat_pct,
            "attempt_coverage_pct": attempt_coverage_pct,
            "capture_coverage_pct": capture_coverage_pct,
            "coverage_semantics": coverage_semantics,
            "empty_rate_estimate": empty_rate_estimate,
            "failure_rate": failure_rate,
            "capture_status_counts": capture_status_counts,
            "counts": counts,
            "coverage": coverage_val,
            "venue_weighted": bool(venues_dict),
            "venue_dates_found": venue_found_total,
            "venue_dates_expected": venue_expected_total,
            "unit": unit,
            "effective_start_date": effective_start,
            "missing_dates": cat_missing,
            "dates_found_list": cat_found_sorted,
            "dates_missing_list": cat_missing,
            # Axis-aware breakdown: SPORTS drilldown is by data_type (no real
            # venues); other categories keep the existing ``venues`` shape.
            # UI reads ``breakdown_axis`` to pick the right key. ``venues``
            # stays populated (even if empty) for consumers that haven't
            # migrated yet — the aggregator only writes data under ONE of
            # ``venues`` or ``data_types`` depending on axis.
            "breakdown_axis": breakdown_axis,
            "venues": {} if breakdown_axis == "data_type" else venues_dict,
            "data_types": venues_dict if breakdown_axis == "data_type" else {},
            "failure_rate_by_dimension": failure_rate_by_dimension,
            "_venue_found": venue_found_total,
            "_venue_expected": venue_expected_total,
        }

        # MTDS honest-coverage — surface UAC-declared expected/missing venue
        # sets at the category level so the UI can render "venue X shipped
        # no data in this window" as a first-class gap. SSOT:
        # codex/02-data/mtds-data-source-coverage-matrix.md §1.
        self._annotate_mtds_category(result, service, cat, venues_dict, venue_mapping)

        result.update(sub_dims)
        return result

    def _maybe_group_by_data_type(
        self,
        venues_dict: dict[str, object],
        filtered: pd.DataFrame,
        effective_start: str,
        end_date: str,
        cat: str,
        venue_found_total: int,
        venue_expected_total: int,
        service: str = "instruments-service",
    ) -> tuple[dict[str, object], int, int, bool]:
        """Fall back to data_type-keyed grouping for the SPORTS instruments
        pattern (empty-venue v4 rows). Returns the input unchanged for
        categories that have real venues.

        Returns a 4-tuple: ``(grouping_dict, found_total, expected_total,
        switched_to_data_type)``. ``switched_to_data_type=True`` means the
        returned dict is keyed by data_type (not venue) and the caller MUST
        flip ``breakdown_axis`` to ``"data_type"`` so the UI renders the
        result under ``data_types`` instead of ``venues``. Reference incident
        2026-05-06: MDPS UI showed data_type strings ("book_snapshot_5",
        "ohlcv_15m", ...) labelled as venues because the discriminator was
        hardcoded to ``cat == "SPORTS"`` — MDPS hits this fallback for CEFI,
        DEFI, etc. but the axis stayed "venue" so drilldowns / schema links /
        deploy-missing all sent garbage downstream.
        """
        all_venues_empty = not venues_dict or all(str(k).strip() == "" for k in venues_dict)
        has_empty_venue_dt_rows = (
            "data_type" in filtered.columns
            and "venue" in filtered.columns
            and (filtered["venue"].str.strip() == "").any()
            and filtered.loc[filtered["venue"].str.strip() == "", "data_type"].str.len().sum() > 0
        )
        if not (all_venues_empty or has_empty_venue_dt_rows):
            return venues_dict, venue_found_total, venue_expected_total, False
        if "data_type" not in filtered.columns:
            return venues_dict, venue_found_total, venue_expected_total, False
        dt_filtered = (
            filtered[filtered["venue"].str.strip() == ""]
            if has_empty_venue_dt_rows and not all_venues_empty
            else filtered
        )
        new_dict, found_total, expected_total = self._build_data_type_grouping(
            dt_filtered, effective_start, end_date, cat, service
        )
        return new_dict, found_total, expected_total, True

    @staticmethod
    def _annotate_mtds_category(
        result: dict[str, object],
        service: str,
        cat: str,
        venues_dict: dict[str, object],
        venue_mapping: VenueMapping,
    ) -> None:
        """Inject MTDS ``expected_venues`` / ``missing_venues`` / ``honest_axis``.

        No-op for non-MTDS services or SPORTS (bookmaker axis is Phase 6d).
        """
        if service != "market-tick-data-service":
            return
        cat_key = cat.upper()
        if cat_key not in MTDS_CATEGORY_META or cat_key == "SPORTS":
            return
        expected_venues_list = mtds_expected_venues(cat, venue_mapping)
        present_venues = {
            v
            for v, entry in venues_dict.items()
            if isinstance(entry, dict) and int(cast(int, entry.get("dates_found", 0))) > 0  # pyright: ignore[reportUnknownMemberType,reportUnknownArgumentType]
        }
        missing_venues = sorted(set(expected_venues_list) - present_venues)
        result["expected_venues"] = expected_venues_list
        result["missing_venues"] = missing_venues
        result["honest_axis"] = str(MTDS_CATEGORY_META[cat_key]["axis"])

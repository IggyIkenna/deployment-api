"""Manifest-status LEAF helpers: refusal-response construction, sync-build
aggregation math, and the process-pool/serial category-build dispatch legs.

Split out of ``manifest.py`` (2026-07-31,
``deployment_api_qg_size_gate_debt_2026_07_30.md``) to bring
``ManifestStatusMixin``'s oversized methods under the 50-line method-size
gate. Every method here is a LEAF, called only from ``manifest.py``'s side
of the mixin chain — none of them calls back into a method that lives only
on ``ManifestStatusMixin`` (in ``manifest.py``, which sits AFTER this mixin
in the chain: ... -> manifest_category_builder -> manifest_status_helpers ->
manifest). That direction is a hard invariant of this codebase's mixin
architecture ("every cross-group ``self._method`` reference resolves
statically under basedpyright strict" — see ``ManifestStatusHelpersMixin``'s
own docstring below): an EARLIER mixin can never call a method defined only
on a LATER one. The orchestrator methods that need to reach BOTH a leaf here
AND a patch-sensitive call site (``slice_rollup_to_window`` / ``run_bounded``
/ ``ThreadPoolExecutor``, each patched by tests via the literal module path
``deployment_api.services.data_status.manifest.<name>``) stay physically in
``manifest.py`` for that reason — see that file's module docstring.
"""

import datetime as dt
import logging
import multiprocessing
from concurrent.futures import ProcessPoolExecutor
from dataclasses import dataclass

from unified_api_contracts import VenueMapping

import deployment_api.services.data_status_service as _dss
from deployment_api.services.data_status.manifest_category_builder import (
    ManifestCategoryBuilderMixin,
)
from deployment_api.settings import (
    DATA_STATUS_LIVE_BUILD_MEMORY_BUDGET_BYTES as _LIVE_BUILD_MEMORY_BUDGET_BYTES,
)

logger = logging.getLogger(__name__)


@dataclass(frozen=True, slots=True)
class _ManifestBuildRequest:
    """Bundles the row-filter/date-range params that travel together across
    ``get_manifest_status``'s internal call chain (pre-flight estimate /
    bounded build / fallback) — keeps each helper's OWN signature short
    instead of repeating all ~10 params on every one."""

    service: str
    start_date: str
    end_date: str
    asset_groups: list[str] | None
    secondary_axis: str | None
    league_id: str | None
    fixture_id: str | None
    canonical_question_group: str | None
    job_id: str | None
    chain: str | None


@dataclass(frozen=True, slots=True)
class _CategoryBuildContext:
    """Bundles the params ``_dispatch_category_builds`` threads unchanged
    into whichever fan-out leg (process pool / thread pool / serial) it
    picks, for the same reason as :class:`_ManifestBuildRequest`."""

    service: str
    start_date: str
    end_date: str
    all_date_strs: list[str]
    total_days: int
    venue_mapping: VenueMapping
    row_filters: dict[str, str] | None
    cloud: str
    pipeline_modes: list[str] | None
    venue: list[str] | None
    scope: str


class ManifestStatusHelpersMixin(ManifestCategoryBuilderMixin):
    """LEAF helpers for ``manifest.py``'s ``ManifestStatusMixin``.

    Sits between ``ManifestCategoryBuilderMixin`` and ``ManifestStatusMixin``
    in the data_status mixins' single linear inheritance chain (cli -> defi
    -> sports -> breakdowns_domain -> breakdowns_core -> venue_resolution ->
    coverage -> missing_shards -> manifest_category_builder ->
    manifest_status_helpers -> manifest) so every cross-group
    ``self._method`` reference resolves statically under basedpyright
    strict. ``DataStatusService`` composes the top of the chain and is the
    ONLY public entry point — import it from
    ``deployment_api.services.data_status_service`` (the facade). See this
    module's docstring for why every method here is a LEAF (never calls
    forward into ``ManifestStatusMixin``-only code).
    """

    def _live_build_refusal_response(
        self,
        req: _ManifestBuildRequest,
        estimate_bytes: int,
        child_error: str | None,
    ) -> dict[str, object]:
        """Structured refusal payload when no rollup fallback is available."""
        try:
            days: int | None = (dt.date.fromisoformat(req.end_date) - dt.date.fromisoformat(req.start_date)).days + 1
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
        logger.info(
            "manifest live-build for service=%s refused with NO rollup fallback available: %s", req.service, detail
        )
        response: dict[str, object] = {
            "service": req.service,
            "date_range": {"start": req.start_date, "end": req.end_date, "days": days},
            "mode": "live_build_refused",
            "refused": True,
            "detail": detail,
            "estimated_bytes": estimate_bytes,
            "budget_bytes": _LIVE_BUILD_MEMORY_BUDGET_BYTES,
            "overall_completion_pct": 0.0,
            "asset_groups": {},
        }
        if req.secondary_axis:
            response["secondary_axis"] = req.secondary_axis
        return response

    def _manifest_sync_aggregate_categories(
        self, cat_list: list[str], results: dict[str, dict[str, object]]
    ) -> tuple[dict[str, object], dict[str, float]]:
        """Drain each category's result into ``result_categories`` + overall totals.

        Returns ``(result_categories, totals)`` where ``totals`` carries
        ``found`` / ``expected`` / ``shards_found`` / ``shards_expected`` /
        ``attempt_weighted`` — the inputs :func:`_manifest_sync_compute_overall_pcts`
        and :func:`_manifest_sync_migration_flag` need.
        """
        result_categories: dict[str, object] = {}
        totals = {"found": 0.0, "expected": 0.0, "shards_found": 0.0, "shards_expected": 0.0, "attempt_weighted": 0.0}
        for cat in cat_list:
            cat_result = results[cat]
            result_categories[cat] = cat_result
            totals["found"] += int(cat_result.get("dates_found", 0))  # pyright: ignore[reportArgumentType]
            totals["expected"] += int(cat_result.get("dates_expected", 0))  # pyright: ignore[reportArgumentType]
            totals["shards_found"] += int(cat_result.get("_venue_found", 0))  # pyright: ignore[reportArgumentType]
            totals["shards_expected"] += int(cat_result.get("_venue_expected", 0))  # pyright: ignore[reportArgumentType]
            totals["attempt_weighted"] += float(cat_result.get("attempt_coverage_pct", 0.0)) * int(  # pyright: ignore[reportArgumentType]
                cat_result.get("_venue_expected", 0)
            )
            del cat_result["_venue_found"]
            del cat_result["_venue_expected"]
        return result_categories, totals

    def _manifest_sync_compute_overall_pcts(self, totals: dict[str, float]) -> dict[str, float]:
        """Dates-based / shards-weighted / capture-vs-attempt completion percentages.

        See the original ``_get_manifest_status_sync`` docstring (R7,
        2026-06-15): CAPTURE = captured / could-exist (== shards-weighted);
        ATTEMPT = (captured + empty_confirmed + failed) / could-exist,
        shards-weighted across categories.
        """
        overall_pct_dates = min(round(totals["found"] / max(1, totals["expected"]) * 100, 2), 100.0)
        shards_expected = totals["shards_expected"]
        overall_pct_shards = (
            min(round(totals["shards_found"] / shards_expected * 100, 2), 100.0)
            if shards_expected > 0
            else overall_pct_dates
        )
        overall_attempt_coverage_pct = (
            min(round(totals["attempt_weighted"] / shards_expected, 2), 100.0)
            if shards_expected > 0
            else overall_pct_dates
        )
        return {
            "overall_pct_dates": overall_pct_dates,
            "overall_pct_shards": overall_pct_shards,
            # Primary ``overall_completion_pct`` mirrors the per-category
            # ``completion_pct`` (shards-weighted) so the overall and
            # sub-rows use the same metric; falls back to dates-based where
            # no shards denominator exists.
            "overall_pct": overall_pct_shards,
            "overall_capture_coverage_pct": overall_pct_shards,
            "overall_attempt_coverage_pct": overall_attempt_coverage_pct,
        }

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

    def _manifest_sync_migration_flag(self, service: str, totals: dict[str, float]) -> bool:
        """Flag a probably-in-progress migration so the UI can explain a
        suspiciously-low overall number without cross-checking running VMs.

        Heuristic: overall < 10% of shards expected AND a backfill/migration
        VM is currently running for this service (:meth:`_has_active_migration_vm`).
        """
        return bool(
            totals["shards_expected"] > 0
            and totals["shards_found"] < (totals["shards_expected"] * 0.1)
            and self._has_active_migration_vm(service)
        )

    def _manifest_sync_build_response(
        self,
        service: str,
        start_date: str,
        end_date: str,
        total_days: int,
        secondary_axis: str | None,
        row_filters: dict[str, str] | None,
        result_categories: dict[str, object],
        totals: dict[str, float],
        pcts: dict[str, float],
        migration_in_progress: bool,
    ) -> dict[str, object]:
        """Assemble the final TurboDataStatusResponse dict from the pieces
        ``manifest.py``'s ``_get_manifest_status_sync`` helpers computed."""
        response: dict[str, object] = {
            "service": service,
            "date_range": {"start": start_date, "end": end_date, "days": total_days},
            "mode": "turbo",
            "sub_dimension": "venue",
            "overall_completion_pct": pcts["overall_pct"],
            "overall_completion_pct_dates": pcts["overall_pct_dates"],
            "overall_completion_pct_shards_weighted": pcts["overall_pct_shards"],
            "overall_capture_coverage_pct": pcts["overall_capture_coverage_pct"],
            "overall_attempt_coverage_pct": pcts["overall_attempt_coverage_pct"],
            "overall_dates_found": int(totals["found"]),
            "overall_dates_expected": int(totals["expected"]),
            "overall_shards_found": int(totals["shards_found"]),
            "overall_shards_expected": int(totals["shards_expected"]),
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

    def _dispatch_via_process_pool(
        self, cat_list: list[str], ctx: _CategoryBuildContext
    ) -> dict[str, dict[str, object]]:
        """ProcessPoolExecutor fan-out (Linux fork; ``build_category_in_subprocess``
        entrypoint — a fixed positional signature that can't carry
        ``row_filters``/``pipeline_modes``/``venue``/non-default ``scope``
        across the fork boundary, hence ``manifest.py``'s
        ``_dispatch_category_builds`` only selects this path when none of
        those are set)."""
        # Lazy import: manifest.py imports ManifestStatusHelpersMixin (this
        # class) at module level, so a top-level import back would be
        # circular — same sanctioned pattern as data_status/defi.py's
        # shared_venue_mapping import.
        from deployment_api.services.data_status.manifest import (  # noqa: qg-inside-import
            build_category_in_subprocess,
        )

        mp_ctx = multiprocessing.get_context("fork")
        with ProcessPoolExecutor(
            max_workers=min(len(cat_list), _dss._MAX_BUILD_WORKERS),  # pyright: ignore[reportPrivateUsage]  # memory-budget cap (OOM fix)
            mp_context=mp_ctx,
        ) as pool:
            pp_futures = {
                pool.submit(
                    build_category_in_subprocess,
                    ctx.service,
                    cat,
                    ctx.start_date,
                    ctx.end_date,
                    ctx.all_date_strs,
                    ctx.total_days,
                    ctx.cloud,
                ): cat
                for cat in cat_list
            }
            return {pp_futures[f]: f.result() for f in pp_futures}

    def _dispatch_serial(self, cat_list: list[str], ctx: _CategoryBuildContext) -> dict[str, dict[str, object]]:
        """Single-category (or pools-disabled) serial build — no fan-out overhead."""
        return {
            cat: self._build_manifest_category(
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
            )
            for cat in cat_list
        }

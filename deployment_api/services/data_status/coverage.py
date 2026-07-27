"""Coverage-summary surface: row filters, breakdowns + per-cat coverage.

Split out of the 6,663-line ``data_status_service.py`` god-module
(codex ratchet plan 2026-06-10). The facade module re-exports every
public + legacy-underscore name, so callers keep importing from
``deployment_api.services.data_status_service``.
"""

import asyncio
import datetime as dt
import logging
from typing import cast

import pandas as pd
from unified_api_contracts import (
    OUT_OF_COVERAGE_WINDOW_REASONS,
    SCHEDULE_DEFINING_DATA_TYPES,
    EmptyConfirmedReason,
)

import deployment_api.services.data_status_service as _dss
from deployment_api.services.data_status.live_build_guard import (
    estimate_live_build_bytes,
    would_exceed_budget,
)
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

# coverage-summary has no date-range param — it always builds the full
# history, mirroring data_status_rollup_worker.py's _ROLLUP_START_DATE.
_COVERAGE_FULL_HISTORY_START = "2018-01-01"

# Services whose ON-DEMAND coverage-summary build is PROVEN to threaten the
# shared container: coverage-summary has no date-range param to narrow (unlike
# manifest-status), so it always attempts the same full-history shape the
# manifest live-build guard measured at 81 GB (market-tick-data-service) / 56
# GB-for-just-3-months (market-data-processing-service) peak RSS for a SINGLE
# request — see live_build_guard's calibration anchors and
# data_status_rollup_worker.py's per-service child-process memory-ceiling
# comment ("no RAM tier through 64GB survives it"). Deliberately excludes
# instruments-service: its on-demand build is also large (18 GB, same
# incident) but its rollup blob has been reliably produced since the very
# first rollup-worker run (2026-06), so it essentially never reaches this
# on-demand path in production — gating it too would only add refusal
# false-positives to its still-exercised on-demand unit-test coverage with
# no matching production incident.
_COVERAGE_SUMMARY_OOM_RISK_SERVICES: frozenset[str] = frozenset(
    {"market-tick-data-service", "market-data-processing-service"}
)

# DATA-TYPE-AWARE schedule-empty resolution (operator direction 2026-06-23):
# the only empty reason a schedule-defining data_type (FIXTURES) resolves to
# out-of-window. Mirrors UAC ``is_resolved_schedule_empty``.
_SCHEDULE_EMPTY_RESOLVED_REASON = EmptyConfirmedReason.SOURCE_RETURNED_ZERO.value
from unified_api_contracts.internal import MarketCategory
from unified_api_contracts.registry import (
    get_breakdown_axes,
    get_primary_axis,
)

logger = logging.getLogger(__name__)

from deployment_api.services.data_status.rollup_cache import (
    filter_coverage_to_asset_groups,
    read_coverage_rollup_allow_stale,
    read_coverage_rollup_if_fresh,
)
from deployment_api.services.data_status.venue_resolution import VenueResolutionMixin


def _build_coverage_summary_in_subprocess(
    service: str,
    asset_groups: list[str] | None,
    cloud: str,
) -> dict[str, object]:
    """Spawned-child entrypoint for ``bounded_subprocess.run_bounded``.

    Defense-in-depth layer 2 for the coverage-summary live-build OOM guard
    (layer 1 is the pre-flight :func:`~deployment_api.services.data_status.live_build_guard.estimate_live_build_bytes`
    refusal in :meth:`CoverageStatusMixin.get_coverage_summary`) — mirrors
    ``manifest.py``'s ``_build_manifest_live_in_subprocess`` (see that
    docstring for the full rationale). Constructs a fresh
    ``DataStatusService`` inside the child rather than pickling a live
    instance across the process boundary.
    """
    dss = _dss.DataStatusService()
    return dss._get_coverage_summary_sync(service, asset_groups, cloud)  # pyright: ignore[reportPrivateUsage]


class CoverageStatusMixin(VenueResolutionMixin):
    """get_coverage_summary + row-filter / breakdown helpers.

    The data_status mixins form a single linear inheritance chain
    (cli -> defi -> sports -> breakdowns_domain -> breakdowns_core ->
    venue_resolution -> coverage -> missing_shards -> manifest) so that
    every cross-group ``self._method`` reference resolves statically
    under basedpyright strict. ``DataStatusService`` composes the top of
    the chain and is the ONLY public entry point — import it from
    ``deployment_api.services.data_status_service`` (the facade).
    """

    async def get_coverage_summary(
        self,
        service: str = "instruments-service",
        asset_groups: list[str] | None = None,
        cloud: str = "gcp",
    ) -> dict[str, object]:
        """Return shard counts and latest-day instrument totals per asset_group.

        Three paths (mirrors :meth:`get_manifest_status`):

        1. **Rollup fast-path** — if a fresh coverage rollup blob exists at
           ``gs://{pid}-data-status-rollups/{service}/coverage.json.gz`` the
           response is read from there + filtered to the requested asset_groups
           in-memory. Sub-second.
        2. **Live-build OOM guard** (root-caused 2026-07-26,
           ``deployment_api_honest_coverage_regression_2026_07_26.md``): a
           service whose rollup blob is missing/stale falls through to
           ``_get_coverage_summary_sync``, which — like the manifest-status
           on-demand build this guard was first added for — iterates the
           FULL 2018-today availability index unconditionally (coverage-
           summary takes no date-range param). For market-tick-data-service
           this measures ~81 GB peak RSS for a SINGLE request (same
           calibration anchor as the manifest guard) — enough to OOM-kill the
           whole shared 16 GiB container and cancel every other in-flight
           request, not just this one. A cheap pre-flight estimate refuses
           the build outright (falling back to a stale rollup, or a
           structured refusal) before ever attempting it; a build that DOES
           pass the estimate still runs inside a resource-bounded spawned
           child as defense-in-depth.
        3. **On-demand fall-through** — the guarded synchronous compute that
           iterates the availability indices.

        See plan: ``data_status_offline_rollup_2026_05_06.md``.
        """
        # Rollup fast-path — ALWAYS attempted (mirrors get_manifest_status post-2026-06-16):
        # ``read_coverage_rollup_if_fresh`` reads the precomputed ``coverage.json.gz``. A
        # service whose live blob is stale falls through (staleness gate enforced) to the
        # on-demand compute — honest, not a fake.
        rollup = await asyncio.to_thread(read_coverage_rollup_if_fresh, service)
        if rollup is not None:
            return filter_coverage_to_asset_groups(rollup, asset_groups)

        # The guard below only engages for _COVERAGE_SUMMARY_OOM_RISK_SERVICES
        # (see that constant's docstring) — every other service keeps the
        # pre-existing, unguarded on-demand path exactly as before.
        if service not in _COVERAGE_SUMMARY_OOM_RISK_SERVICES:
            return await asyncio.to_thread(self._get_coverage_summary_sync, service, asset_groups, cloud)

        # ── Layer 1: pre-flight refusal ──
        cat_list = self._resolve_coverage_cat_list(service, asset_groups)
        today = dt.datetime.now(dt.UTC).date().isoformat()
        estimate_bytes = estimate_live_build_bytes(
            service=service,
            start_date=_COVERAGE_FULL_HISTORY_START,
            end_date=today,
            category_count=len(cat_list),
        )
        if would_exceed_budget(estimate_bytes, _LIVE_BUILD_MEMORY_BUDGET_BYTES):
            logger.warning(
                "coverage-summary live-build REFUSED (pre-flight) for service=%s: estimate=%dMB > budget=%dMB",
                service,
                estimate_bytes // (1024 * 1024),
                _LIVE_BUILD_MEMORY_BUDGET_BYTES // (1024 * 1024),
            )
            return await self._coverage_summary_fallback(service, asset_groups, estimate_bytes)

        # ── Layer 2: defense-in-depth ──
        # Even an estimate under budget runs inside a resource-bounded spawned
        # child (mirrors the manifest live-build guard) — an underestimate
        # raises a catchable error in the throwaway child instead of
        # OOM-killing this gunicorn worker (and every other request sharing
        # its container).
        try:
            return await asyncio.to_thread(
                run_bounded,
                _build_coverage_summary_in_subprocess,
                service,
                asset_groups,
                cloud,
                rlimit_bytes=_LIVE_BUILD_CHILD_RLIMIT_BYTES,
                timeout_s=_LIVE_BUILD_CHILD_TIMEOUT_S,
                name=f"coverage-summary-live-{service[:24]}",
            )
        except BoundedSubprocessError as exc:
            logger.error("coverage-summary live-build FAILED in bounded child for service=%s: %s", service, exc)
            return await self._coverage_summary_fallback(service, asset_groups, estimate_bytes, child_error=str(exc))

    async def _coverage_summary_fallback(
        self,
        service: str,
        asset_groups: list[str] | None,
        estimate_bytes: int,
        *,
        child_error: str | None = None,
    ) -> dict[str, object]:
        """Serve a stale coverage rollup if one exists, else a structured refusal.

        Called when the pre-flight guard refuses the coverage-summary live
        build outright, or the bounded child failed anyway. Mirrors
        ``ManifestStatusMixin._live_build_fallback``.
        """
        stale = await asyncio.to_thread(read_coverage_rollup_allow_stale, service)
        if stale is not None:
            payload, last_modified_iso = stale
            response = filter_coverage_to_asset_groups(payload, asset_groups)
            response["stale"] = True
            response["stale_as_of"] = last_modified_iso
            response["stale_reason"] = (
                "on-demand live build refused or failed (too large for a single request) "
                "-- serving the last precomputed rollup instead"
            )
            logger.info(
                "coverage-summary live-build for service=%s served STALE rollup (as_of=%s, child_error=%s)",
                service,
                last_modified_iso,
                child_error,
            )
            return response

        budget_mb = _LIVE_BUILD_MEMORY_BUDGET_BYTES // (1024 * 1024)
        estimate_mb = estimate_bytes // (1024 * 1024)
        detail = (
            f"On-demand build estimated at ~{estimate_mb} MB, over the {budget_mb} MB safety budget "
            "for a single request -- refused to protect the shared container from an OOM crash. "
            "This service's full-history coverage summary is too large to compute on demand; "
            "select fewer asset groups or wait for the next background rollup cycle."
        )
        if child_error is not None:
            detail = f"On-demand build failed inside its resource-bounded child process ({child_error}). {detail}"
        logger.info(
            "coverage-summary live-build for service=%s refused with NO rollup fallback available: %s",
            service,
            detail,
        )
        return {
            "service": service,
            "mode": "live_build_refused",
            "refused": True,
            "detail": detail,
            "estimated_bytes": estimate_bytes,
            "budget_bytes": _LIVE_BUILD_MEMORY_BUDGET_BYTES,
            "asset_groups": {},
            "totals": {
                "shards": 0,
                "instrument_rows": 0,
                "dates_across_categories": 0,
                "latest_day_instruments": 0,
                "unique_instruments": 0,
                "capture_status_counts": {
                    "captured": 0,
                    "empty_confirmed": 0,
                    "attempted_failed": 0,
                    "expected_unattempted": 0,
                    "out_of_window": 0,
                },
                "completion_pct": 0.0,
            },
            "totals_source": "refused-too-large",
        }

    def _resolve_coverage_cat_list(self, service: str, asset_groups: list[str] | None) -> list[str]:
        """Pick which asset_groups to iterate for this service's coverage summary.

        Cross-asset shared services collapse to a single ``SHARED`` pseudo-key.
        Otherwise apply the per-service restriction map so domain-bound services
        (features-onchain DEFI-only, features-sports SPORTS-only, etc.) don't
        report the same single-bucket numbers under every asset_group key.
        """
        cat_list = asset_groups or [str(c) for c in MarketCategory]
        if service in self._SHARED_BUCKET_SERVICES:
            return ["SHARED"]
        restriction = self._SERVICE_CATEGORY_RESTRICTIONS.get(service)
        if restriction is not None:
            cat_list = [c for c in cat_list if c in restriction]
        return cat_list

    def _select_coverage_group_axis(self, service: str, cat: str, index: pd.DataFrame) -> str:
        """Per-(service, cat) primary display axis for ``latest_day_instruments``.

        SSOT: ``unified_api_contracts.registry.data_status_axis_matrix.PRIMARY_AXIS``.
        Each (service, asset_group) declares its primary axis (the cell-grid
        main dimension). Falls back to ``data_type`` for sports / ``venue``
        for the pricing pipeline default when the SSOT has no entry.
        Cross-asset shared services (calendar / ml-training / ml-inference)
        register under the ``shared`` pseudo-asset-group.
        """
        cat_lower = cat.lower()
        # Cross-asset shared services keep their primary axis even when the
        # caller iterates with cat="SHARED".
        primary = get_primary_axis(service, cat_lower)
        if primary and primary in index.columns:
            return primary
        if cat_lower == "sports":
            return "data_type"
        return "venue"

    @staticmethod
    def _pack_row_filters(
        *,
        league_id: str | None,
        fixture_id: str | None,
        canonical_question_group: str | None,
        job_id: str | None,
        chain: str | None,
    ) -> dict[str, str]:
        """Drop falsy values + uppercase chain — single SSOT for filter packing."""
        filters: dict[str, str] = {}
        if league_id:
            filters["league_id"] = league_id
        if fixture_id:
            filters["fixture_id"] = fixture_id
        if canonical_question_group:
            filters["canonical_question_group"] = canonical_question_group
        if job_id:
            filters["job_id"] = job_id
        if chain:
            filters["chain"] = chain.upper()
        return filters

    @staticmethod
    def _apply_row_filters(filtered: pd.DataFrame, row_filters: dict[str, str]) -> pd.DataFrame:
        """Apply secondary-axis filters to a per-category manifest slice.

        Filters that target a column the manifest doesn't carry yet narrow
        to zero rows — correct: the writer hasn't populated the column, so
        no shards match. UI then renders an empty grid with "no shards
        captured for this filter yet" instead of mis-classifying as missing.
        """
        if filtered.empty:
            return filtered
        for col, value in row_filters.items():
            if col not in filtered.columns:
                return filtered.iloc[0:0].copy()
            filtered = filtered.loc[filtered[col].fillna("").astype(str) == value].copy()
            if filtered.empty:
                return filtered
        return filtered

    @staticmethod
    def _apply_pipeline_mode_filter(filtered: pd.DataFrame, pipeline_modes: list[str]) -> pd.DataFrame:
        """Filter manifest slice to rows whose pipeline_mode is in pipeline_modes.

        OR semantics: any row matching at least one of the requested modes is kept.
        Used by the deployment-ui pipeline_mode filter chip (Phase 4 consumer migration,
        pipeline_mode_implementation_2026_05_28.md).
        """
        if filtered.empty or not pipeline_modes:
            return filtered
        if "pipeline_mode" not in filtered.columns:
            return filtered.iloc[0:0].copy()
        mode_set = set(pipeline_modes)
        return filtered.loc[filtered["pipeline_mode"].fillna("").astype(str).isin(mode_set)].copy()

    @staticmethod
    def _apply_venue_filter(filtered: pd.DataFrame, venue: list[str]) -> pd.DataFrame:
        """Filter manifest slice to rows whose venue is in ``venue`` (case-insensitive).

        OR semantics: any row whose ``venue`` (upper-cased) matches at least one
        of the requested venues (upper-cased) is kept. Applied by the data-status
        tab's venue filter chip on ``/api/data-status/manifest``. A manifest slice
        without a ``venue`` column narrows to zero rows — correct: no shard can
        match the requested venue.
        """
        if filtered.empty or not venue:
            return filtered
        if "venue" not in filtered.columns:
            return filtered.iloc[0:0].copy()
        wanted = {v.upper() for v in venue}
        return filtered.loc[filtered["venue"].fillna("").astype(str).str.upper().isin(wanted)].copy()

    def _build_breakdowns(
        self,
        service: str,
        cat: str,
        index: pd.DataFrame,
        primary_axis: str,
    ) -> dict[str, dict[str, int]]:
        """Build ``breakdowns: dict[axis, dict[value, count]]`` for the panel.

        The deployment-ui ``BreakdownsAccordion`` renders one section per
        axis returned here. Axes come from the UAC SSOT (shard + display
        minus primary, preserving the SHARD-then-DISPLAY ordering).

        Empty axes are still emitted (with ``{}``) when the UAC SSOT
        declares them — UI shape leads, backend writers follow. The
        ``BreakdownsAccordion`` renders a "no data yet" placeholder for
        empty axes rather than hiding them, so the operator can see the
        expected shape even before Phase 1 writers populate the column.
        ``__legacy__`` surfaces older rows that have an empty value (e.g.
        pre-Phase-1B ML/strategy/execution writes that didn't yet stamp
        ``job_id``).
        """
        cat_lower = cat.lower()
        axes = get_breakdown_axes(service, cat_lower)
        if not axes:
            return {}
        has_count = "instrument_count" in index.columns
        breakdowns: dict[str, dict[str, int]] = {}
        for axis in axes:
            if axis == primary_axis:
                continue
            if axis not in index.columns:
                breakdowns[axis] = {}
                continue
            values = index[axis].fillna("").astype(str)
            if has_count:
                grouped = index.assign(_axis=values).groupby("_axis")["instrument_count"].sum(min_count=1)
                axis_counts = {
                    (str(k) if str(k).strip() else "__legacy__"): int(v)  # pyright: ignore[reportAny]
                    for k, v in grouped.items()  # pyright: ignore[reportAny]
                    if pd.notna(v) and v > 0  # pyright: ignore[reportAny]
                }
            else:
                # Vectorised value-counts (single O(n) pass). The prior
                # ``for v in values.unique(): (values == v).sum()`` was
                # O(unique x rows) — pathological on a high-cardinality axis
                # (e.g. instrument_id) of a multi-million-row beta projected
                # index, which lacks ``instrument_count`` so always lands here
                # (minutes per asset_group; the operator-beta-eyeball slowness).
                vc = values.value_counts()
                axis_counts = {
                    (str(k) if str(k).strip() else "__legacy__"): int(v)  # pyright: ignore[reportAny]
                    for k, v in vc.items()  # pyright: ignore[reportAny]
                    if v > 0  # pyright: ignore[reportAny]
                }
            breakdowns[axis] = axis_counts
        return breakdowns

    def _filter_to_iso_dates(self, index: pd.DataFrame) -> pd.DataFrame:
        """Drop sports ``date='all'`` sentinels + prediction future-dated rows.

        Sports reference rows write ``date='all'`` for non-day-bound entities;
        prediction long-dated markets stamp resolution-time as ``date``. Both
        leak into ``max(date)`` and surface as bogus ``latest_day`` values.
        Restrict to ISO ``YYYY-MM-DD`` strings <= today.
        """
        from datetime import date as _today_date

        if "date" not in index.columns:
            return index
        today_iso = _today_date.today().isoformat()
        is_iso = index["date"].astype(str).str.match(r"^\d{4}-\d{2}-\d{2}$").fillna(False)
        is_not_future = index["date"].astype(str) <= today_iso
        return index[is_iso & is_not_future]

    def _build_latest_day_breakdown(
        self,
        date_index: pd.DataFrame,
        latest_day: str | None,
        group_axis: str,
    ) -> tuple[dict[str, int], int]:
        """Sum instrument_count per group_axis on the latest day.

        Returns (per-axis-value totals, latest_day total). Falls back to
        row-count if the manifest has no ``instrument_count`` column.
        """
        if not latest_day or "date" not in date_index.columns:
            return {}, 0
        latest = date_index[date_index["date"] == latest_day]
        if "instrument_count" in latest.columns:
            total = int(latest["instrument_count"].fillna(0).sum())  # pyright: ignore[reportAny]
            if group_axis not in latest.columns:
                return {}, total
            grouped = latest.groupby(group_axis)["instrument_count"].sum()
            counts = {str(v): int(c) for v, c in grouped.items() if c > 0 and str(v).strip()}  # pyright: ignore[reportAny]
            return counts, total  # pyright: ignore[reportUnknownVariableType]
        # No instrument_count column -> fall back to row-count. Vectorised
        # value-counts (single O(n) pass) — the prior per-unique-value
        # ``(latest[group_axis] == v).sum()`` loop was O(unique x rows).
        total = len(latest)
        if group_axis not in latest.columns:
            return {}, total
        vc = latest[group_axis].astype(str).value_counts()
        counts = {str(k): int(v) for k, v in vc.items() if str(k).strip()}  # pyright: ignore[reportAny]
        return counts, total  # pyright: ignore[reportUnknownVariableType]

    def _build_coverage_for_cat(self, service: str, cat: str, cloud: str = "gcp") -> dict[str, object] | None:
        """Build one asset_group's coverage entry. Returns None if empty.

        Out of scope for the MDPS timeframe-aware honest-coverage extension
        (mtds_data_status_page_parity_2026_07_21): this ``completion_pct``
        (the offline rollup worker's coverage-summary surface, ``GET
        /api/data-status/coverage``) is a SEPARATE, independent 4-state
        tally over raw ``capture_status`` counts — it has no relationship to
        ``mtds_honest_coverage_for_venue`` / ``per_instrument_coverage``'s
        UAC-driven ``(venue, data_type, [timeframe,] date)`` shard-space math
        and is NOT made timeframe-aware by that work.
        """
        index = self._read_defi_merged_index(service, cat, cloud=cloud)
        if index.empty:
            return None
        if "venue" in index.columns:
            index = index.copy()
            index["venue"] = index["venue"].replace(self._VENUE_ALIASES)
        index = self._filter_legacy_defi_rows(index, cat)
        shards = len(index)
        # B1: honest 4-state breakdown so completion% is captured /
        # (captured+empty+failed+expected_unattempted), NOT the self-referential
        # captured/len(index)≈100%. Aligns the coverage-summary with
        # manifest-status + the drilldown's _aggregate_counts. v4 rows without a
        # ``capture_status`` column are the legacy "every row captured" path.
        #
        # OOW partition: within ``empty_confirmed`` rows, those carrying a
        # lifecycle reason (pre-genesis chain, pre-launch venue, delisted
        # instrument, post/pre-season, deprecated data_type, etc.) are
        # NEVER-COLLECTABLE — they are NOT gaps. We split them into a separate
        # ``out_of_window`` bucket and exclude them from the denominator so
        # the completion-% reflects only genuinely actionable absences.
        if "capture_status" in index.columns:
            # Legacy/NaN coercion (SSOT parity with
            # ``coverage_metrics.compute_capture_status_counts`` + UTL
            # ``ManifestWriter.lookup``): a blank / NaN / ``"None"`` / ``"nan"`` /
            # any non-4-state capture_status is a legacy pre-v5 row that means
            # "captured". WITHOUT this coercion ``(cs == "captured")`` silently
            # DROPS those rows from BOTH numerator and denominator (they match
            # none of the four literal states), under-reporting coverage and
            # diverging from the per-venue breakdown which DOES coerce — the same
            # manifest then yields two different completion_pct values per
            # endpoint (sports prd: 17,288 legacy v4 ``"None"`` rows dropped from
            # the denominator, 2026-06-18 audit). Coerce here so the
            # coverage-summary denominator is identical to the breakdown SSOT.
            _valid_4state = ("captured", "empty_confirmed", "attempted_failed", "expected_unattempted")
            _cs_raw = index["capture_status"].astype(str).str.lower()
            cs = _cs_raw.where(_cs_raw.isin(_valid_4state), "captured")
            total_empty = int((cs == "empty_confirmed").sum())
            # Compute out-of-window subset of empty_confirmed rows.
            # Accept both "error_reason" (live consolidated index schema) and "reason"
            # (projected / beta index schema written by the CF-20 dry-run builder).
            _cols = index.columns
            _reason_col = "error_reason" if "error_reason" in _cols else ("reason" if "reason" in _cols else None)
            if total_empty > 0 and _reason_col is not None:
                empty_mask = cs == "empty_confirmed"
                reasons = index.loc[empty_mask, _reason_col].fillna("").astype(str).str.strip()
                # Vectorised set-membership: ~6x faster than a per-row
                # ``.apply(is_out_of_coverage_window)`` over the ~1.2M empty rows a
                # large asset_group carries — same verdict (the frozenset IS the
                # canonical set the helper checks). Blank/calendar reasons stay False.
                oow_reason_mask = reasons.isin(OUT_OF_COVERAGE_WINDOW_REASONS)  # pyright: ignore[reportUnknownMemberType]
                # DATA-TYPE-AWARE schedule-empty resolution (operator direction
                # 2026-06-23): a schedule-DEFINING data_type (sports FIXTURES — the
                # API-Football schedule) that is empty_confirmed with
                # SOURCE_RETURNED_ZERO means "no matches that day = complete" →
                # RESOLVED, out-of-window. Still vectorised: FIXTURES rows that
                # carry SOURCE_RETURNED_ZERO. An ENRICHMENT data_type's
                # SOURCE_RETURNED_ZERO is NOT excluded (its zero may be a real gap).
                if "data_type" in index.columns:
                    dtypes = index.loc[empty_mask, "data_type"].fillna("").astype(str).str.strip().str.upper()
                    schedule_empty_mask = dtypes.isin(SCHEDULE_DEFINING_DATA_TYPES) & (
                        reasons == _SCHEDULE_EMPTY_RESOLVED_REASON
                    )
                    oow_reason_mask = oow_reason_mask | schedule_empty_mask
                oow_count = int(oow_reason_mask.sum())  # pyright: ignore[reportUnknownMemberType]
            else:
                oow_count = 0
            within_window_empty = total_empty - oow_count
            capture_status_counts: dict[str, int] = {
                "captured": int((cs == "captured").sum()),
                "empty_confirmed": within_window_empty,
                "attempted_failed": int((cs == "attempted_failed").sum()),
                "expected_unattempted": int((cs == "expected_unattempted").sum()),
                "out_of_window": oow_count,
            }
        else:
            capture_status_counts = {
                "captured": shards,
                "empty_confirmed": 0,
                "attempted_failed": 0,
                "expected_unattempted": 0,
                "out_of_window": 0,
            }
        # Denominator EXCLUDES out_of_window: those cells were never collectable.
        cov_total = (
            capture_status_counts["captured"]
            + capture_status_counts["empty_confirmed"]
            + capture_status_counts["attempted_failed"]
            + capture_status_counts["expected_unattempted"]
        )
        completion_pct = round(capture_status_counts["captured"] / cov_total * 100, 2) if cov_total > 0 else 0.0
        date_index = self._filter_to_iso_dates(index)
        unique_dates = sorted(date_index["date"].unique()) if "date" in date_index.columns else []  # pyright: ignore[reportAny]
        group_axis = self._select_coverage_group_axis(service, cat, index)
        unique_groups_list = (
            sorted(str(v) for v in index[group_axis].unique() if str(v).strip()) if group_axis in index.columns else []  # pyright: ignore[reportAny]
        )
        date_range: dict[str, str] | None = None
        if unique_dates:
            date_range = {"start": str(unique_dates[0]), "end": str(unique_dates[-1])}  # pyright: ignore[reportAny]
        latest_day: str | None = str(unique_dates[-1]) if unique_dates else None  # pyright: ignore[reportAny]
        latest_day_instruments, latest_day_total = self._build_latest_day_breakdown(date_index, latest_day, group_axis)
        total_instruments = (
            int(index["instrument_count"].fillna(0).sum()) if "instrument_count" in index.columns else shards  # pyright: ignore[reportAny]
        )
        # Per-(service, asset_group) multi-axis breakdowns for the UI
        # ``BreakdownsAccordion``. Empty/absent columns surface as ``{}`` so
        # the UI renders the axis selector with an "expected, no data yet"
        # placeholder rather than hiding the dropdown.
        breakdowns = self._build_breakdowns(service, cat, date_index, group_axis)
        # Deduplicated identity count from the lifecycle catalogue — the manifest
        # only carries per-shard counts, so every other total here multi-counts
        # across days/venues; this is the "how many distinct instruments exist"
        # figure (None when the AG has no catalogue, e.g. the SHARED pseudo-key).
        from deployment_api.services.manifest_source import read_unique_instrument_count

        unique_instruments = read_unique_instrument_count(cat) if cat.upper() != "SHARED" else None
        return {
            "total_shards": shards,
            "total_instrument_rows": shards,
            "total_instruments": total_instruments,
            "unique_instruments": unique_instruments,
            "unique_dates": len(unique_dates),
            "unique_venues": len(unique_groups_list),
            "group_axis": group_axis,
            "date_range": date_range,
            "latest_day": latest_day,
            "latest_day_instruments": latest_day_instruments,
            "latest_day_total": latest_day_total,
            "breakdowns": breakdowns,
            "capture_status_counts": capture_status_counts,
            "completion_pct": completion_pct,
            "_unique_dates_set": [str(d) for d in unique_dates],  # pyright: ignore[reportAny]
        }

    def _get_coverage_summary_sync(
        self,
        service: str,
        asset_groups: list[str] | None = None,
        cloud: str = "gcp",
    ) -> dict[str, object]:
        """Synchronous coverage summary implementation."""
        cat_list = self._resolve_coverage_cat_list(service, asset_groups)
        result_categories: dict[str, object] = {}
        total_shards = 0
        total_instrument_rows = 0
        all_dates: set[str] = set()
        total_latest_day_instruments = 0
        total_unique_instruments = 0
        # B1: aggregate the per-cat 4-state so the service-level totals carry an
        # honest completion%, not the self-referential shards count.
        # out_of_window is tracked separately and EXCLUDED from the denominator.
        total_capture_status: dict[str, int] = {
            "captured": 0,
            "empty_confirmed": 0,
            "attempted_failed": 0,
            "expected_unattempted": 0,
            "out_of_window": 0,
        }

        for cat in cat_list:
            entry = self._build_coverage_for_cat(service, cat, cloud=cloud)
            if entry is None:
                continue
            unique_dates_list = entry.pop("_unique_dates_set", [])
            if isinstance(unique_dates_list, list):
                all_dates.update(unique_dates_list)  # pyright: ignore[reportUnknownArgumentType]
            shards_int = entry["total_shards"]
            ld_total_int = entry["latest_day_total"]
            assert isinstance(shards_int, int)
            assert isinstance(ld_total_int, int)
            cat_counts = entry.get("capture_status_counts")
            if isinstance(cat_counts, dict):
                cat_counts_typed = cast(dict[str, int], cat_counts)
                for _k, _v in total_capture_status.items():
                    total_capture_status[_k] = _v + int(cat_counts_typed.get(_k, 0))
            result_categories[cat] = entry
            total_shards += shards_int
            total_instrument_rows += shards_int
            total_latest_day_instruments += ld_total_int
            uniq = entry.get("unique_instruments")
            if isinstance(uniq, int):
                total_unique_instruments += uniq

        # Exclude out_of_window from the denominator (never-collectable cells).
        cov_total = (
            total_capture_status["captured"]
            + total_capture_status["empty_confirmed"]
            + total_capture_status["attempted_failed"]
            + total_capture_status["expected_unattempted"]
        )
        completion_pct = round(total_capture_status["captured"] / cov_total * 100, 2) if cov_total > 0 else 0.0
        return {
            "service": service,
            "asset_groups": result_categories,
            "totals": {
                "shards": total_shards,
                "instrument_rows": total_instrument_rows,
                "dates_across_categories": len(all_dates),
                "latest_day_instruments": total_latest_day_instruments,
                "unique_instruments": total_unique_instruments,
                "capture_status_counts": total_capture_status,
                "completion_pct": completion_pct,
            },
            "totals_source": "manifest",
        }

"""Coverage-summary surface: row filters, breakdowns + per-cat coverage.

Split out of the 6,663-line ``data_status_service.py`` god-module
(codex ratchet plan 2026-06-10). The facade module re-exports every
public + legacy-underscore name, so callers keep importing from
``deployment_api.services.data_status_service``.
"""

import asyncio
import logging
from typing import cast

import pandas as pd
from unified_api_contracts.internal import MarketCategory
from unified_api_contracts.registry import (
    get_breakdown_axes,
    get_primary_axis,
)

logger = logging.getLogger(__name__)

from deployment_api.services.data_status.rollup_cache import (
    filter_coverage_to_asset_groups,
    read_coverage_rollup_if_fresh,
)
from deployment_api.services.data_status.venue_resolution import VenueResolutionMixin


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

        Two paths (mirrors :meth:`get_manifest_status`):

        1. **Rollup fast-path** — if a fresh coverage rollup blob exists at
           ``gs://{pid}-data-status-rollups/{service}/coverage.json.gz`` the
           response is read from there + filtered to the requested asset_groups
           in-memory. Sub-second.
        2. **On-demand fall-through** — original synchronous compute that
           iterates the availability indices.

        See plan: ``data_status_offline_rollup_2026_05_06.md``.
        """
        rollup = await asyncio.to_thread(read_coverage_rollup_if_fresh, service)
        if rollup is not None:
            return filter_coverage_to_asset_groups(rollup, asset_groups)
        return await asyncio.to_thread(self._get_coverage_summary_sync, service, asset_groups, cloud)

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
                    if v and v > 0  # pyright: ignore[reportAny]
                }
            else:
                axis_counts = {}
                for v in values.unique():  # pyright: ignore[reportAny]
                    key = v if v.strip() else "__legacy__"  # pyright: ignore[reportAny]
                    axis_counts[str(key)] = int((values == v).sum())  # pyright: ignore[reportAny]
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
        # No instrument_count column -> fall back to row-count.
        total = len(latest)
        if group_axis not in latest.columns:
            return {}, total
        counts = {}
        for v in latest[group_axis].unique():  # pyright: ignore[reportAny]
            if not str(v).strip():  # pyright: ignore[reportAny]
                continue
            counts[str(v)] = int((latest[group_axis] == v).sum())  # pyright: ignore[reportAny]
        return counts, total  # pyright: ignore[reportUnknownVariableType]

    def _build_coverage_for_cat(self, service: str, cat: str, cloud: str = "gcp") -> dict[str, object] | None:
        """Build one asset_group's coverage entry. Returns None if empty."""
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
        if "capture_status" in index.columns:
            cs = index["capture_status"].astype(str)
            capture_status_counts: dict[str, int] = {
                "captured": int((cs == "captured").sum()),
                "empty_confirmed": int((cs == "empty_confirmed").sum()),
                "attempted_failed": int((cs == "attempted_failed").sum()),
                "expected_unattempted": int((cs == "expected_unattempted").sum()),
            }
        else:
            capture_status_counts = {
                "captured": shards,
                "empty_confirmed": 0,
                "attempted_failed": 0,
                "expected_unattempted": 0,
            }
        cov_total = sum(capture_status_counts.values())
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
        return {
            "total_shards": shards,
            "total_instrument_rows": shards,
            "total_instruments": total_instruments,
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
        # B1: aggregate the per-cat 4-state so the service-level totals carry an
        # honest completion%, not the self-referential shards count.
        total_capture_status: dict[str, int] = {
            "captured": 0,
            "empty_confirmed": 0,
            "attempted_failed": 0,
            "expected_unattempted": 0,
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

        cov_total = sum(total_capture_status.values())
        completion_pct = round(total_capture_status["captured"] / cov_total * 100, 2) if cov_total > 0 else 0.0
        return {
            "service": service,
            "asset_groups": result_categories,
            "totals": {
                "shards": total_shards,
                "instrument_rows": total_instrument_rows,
                "dates_across_categories": len(all_dates),
                "latest_day_instruments": total_latest_day_instruments,
                "capture_status_counts": total_capture_status,
                "completion_pct": completion_pct,
            },
            "totals_source": "manifest",
        }

"""Per-category manifest-entry builder — the compute-heavy core of manifest-status.

Split out of ``manifest.py`` (1131 lines, containing the 360-line
``_build_manifest_category``) per
``plans/active/issues/deployment_api_qg_size_gate_debt_2026_07_30.md`` — pure
code motion, no behavior change. The facade module re-exports every public +
legacy-underscore name, so callers keep importing from
``deployment_api.services.data_status_service``.
"""

from __future__ import annotations

import logging
from typing import cast

import pandas as pd
from unified_api_contracts import VenueMapping

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
from deployment_api.services.data_status_drilldown import (
    COMMODITY_BUCKET_TEMPLATE,
    PREDICTION_KIND_MAP,
    SERVICE_TO_KIND,
)

logger = logging.getLogger(__name__)

import deployment_api.services.data_status_service as _dss
from deployment_api.services.data_status.missing_shards import MissingShardsMixin


class ManifestCategoryBuilderMixin(MissingShardsMixin):
    """``_build_manifest_category`` + its data_type-grouping/MTDS-annotation helpers.

    Sits between ``MissingShardsMixin`` and ``ManifestStatusMixin`` in the
    data_status mixins' single linear inheritance chain (cli -> defi -> sports
    -> breakdowns_domain -> breakdowns_core -> venue_resolution -> coverage ->
    missing_shards -> manifest_category_builder -> manifest) so every
    cross-group ``self._method`` reference resolves statically under
    basedpyright strict. ``DataStatusService`` composes the top of the chain
    and is the ONLY public entry point — import it from
    ``deployment_api.services.data_status_service`` (the facade).
    """

    def _clamp_manifest_dates(
        self,
        index: pd.DataFrame,
        service: str,
        cat: str,
        start_date: str,
        end_date: str,
        all_date_strs: list[str],
    ) -> tuple[str, list[str], int, pd.DataFrame]:
        """Clamp the effective start date and produce the date+service-masked slice.

        Extracted from ``_build_manifest_category`` (further split of
        ``_prepare_manifest_slice`` for the per-method size gate) — pure code
        motion, no behavior change. Returns ``(effective_start, cat_date_strs,
        cat_total_days, filtered)`` where ``filtered`` is masked by date range
        + service only (row/pipeline/venue filters applied downstream).
        """
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
        return effective_start, cat_date_strs, cat_total_days, filtered

    def _apply_manifest_filters(
        self,
        filtered: pd.DataFrame,
        cat: str,
        row_filters: dict[str, str] | None,
        pipeline_modes: list[str] | None,
        venue: list[str] | None,
    ) -> pd.DataFrame:
        """Apply row/pipeline_mode/venue filters + the venue-alias fold.

        Extracted from ``_build_manifest_category`` (further split of
        ``_prepare_manifest_slice`` for the per-method size gate) — pure code
        motion, no behavior change.
        """
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

        # Apply the venue filter (data-status tab venue chip) AFTER the bare-alias
        # fold so the requested value matches the canonical venue. OR semantics
        # across the requested venues; case-insensitive (the UI may send any case).
        if venue:
            filtered = self._apply_venue_filter(filtered, venue)
        return filtered

    def _drop_legacy_defi_and_canonicalise(self, filtered: pd.DataFrame, cat: str) -> pd.DataFrame:
        """Drop pre-canonicalisation DeFi venue-alias rows + canonicalise data_types.

        Extracted from ``_build_manifest_category`` (further split of
        ``_prepare_manifest_slice`` for the per-method size gate) — pure code
        motion, no behavior change.
        """
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
        return filtered

    def _prepare_manifest_slice(
        self,
        index: pd.DataFrame,
        service: str,
        cat: str,
        start_date: str,
        end_date: str,
        all_date_strs: list[str],
        row_filters: dict[str, str] | None,
        pipeline_modes: list[str] | None,
        venue: list[str] | None,
    ) -> tuple[str, list[str], int, pd.DataFrame, set[str], list[str], int]:
        """Clamp dates, mask + filter the manifest slice, compute found/missing dates.

        Extracted from ``_build_manifest_category`` — pure code motion, no
        behavior change. Returns ``(effective_start, cat_date_strs,
        cat_total_days, filtered, cat_found_dates, cat_missing, cat_found)``.
        """
        effective_start, cat_date_strs, cat_total_days, filtered = self._clamp_manifest_dates(
            index, service, cat, start_date, end_date, all_date_strs
        )
        filtered = self._apply_manifest_filters(filtered, cat, row_filters, pipeline_modes, venue)
        filtered = self._drop_legacy_defi_and_canonicalise(filtered, cat)

        cat_found_dates = {str(d) for d in filtered["date"].unique()} if not filtered.empty else set()  # pyright: ignore[reportUnknownVariableType,reportAny]
        cat_missing = sorted(set(cat_date_strs) - cat_found_dates)
        cat_found = len(cat_found_dates)  # pyright: ignore[reportUnknownArgumentType]

        return effective_start, cat_date_strs, cat_total_days, filtered, cat_found_dates, cat_missing, cat_found  # pyright: ignore[reportUnknownVariableType]

    def _apply_mtds_override_if_target(
        self,
        venues_dict: dict[str, object],
        venue_found_total: int,
        venue_expected_total: int,
        filtered: pd.DataFrame,
        cat: str,
        effective_start: str,
        end_date: str,
        venue_mapping: VenueMapping,
        service: str,
        cloud: str,
        scope: str,
    ) -> tuple[dict[str, object], int, int]:
        """MTDS/MDPS honest-coverage override, applied only for MTDS honest-coverage targets.

        Extracted from ``_compute_venue_breakdown_with_overrides`` (further
        split for the per-method size gate) — pure code motion, no behavior
        change.

        (Phase 6c; MDPS extension mtds_data_status_page_parity_2026_07_21).
        For CEFI / TRADFI / DEFI / PREDICTION, recompute per-venue
        ``dates_found`` / ``dates_expected`` from the UAC-driven ``(venue,
        data_type, date)`` shard space AND inject UAC-declared venues that
        had zero manifest rows. The old path iterated only venues observed
        in the manifest, so a venue missing completely (e.g. UPBIT with no
        trades shipped) was invisible. SSOT:
        codex/02-data/mtds-data-source-coverage-matrix.md. ``service=service``
        is the CRITICAL fix (all 3 adversarial reviews converged on it):
        without it, ``get_expected_data_types_for_venue`` defaults to
        ``service=""`` and MDPS's expected-dt list resolves to the FULL MTDS
        raw vocabulary instead of the narrowed MDPS-derivable subset.
        """
        if not is_mtds_honest_coverage_target(service, cat):
            return venues_dict, venue_found_total, venue_expected_total
        return self._apply_mtds_honest_coverage(
            venues_dict,
            filtered,
            cat,
            effective_start,
            end_date,
            venue_mapping,
            cloud=cloud,
            scope=scope,
            service=service,
        )

    def _build_and_override_venue_breakdown(
        self,
        filtered: pd.DataFrame,
        effective_start: str,
        end_date: str,
        venue_mapping: VenueMapping,
        cat_found: int,
        cat_total_days: int,
        service: str,
        cat: str,
        cloud: str,
        scope: str,
    ) -> tuple[dict[str, object], int, int]:
        """Per-venue breakdown + the MTDS honest-coverage override.

        Extracted from ``_compute_venue_breakdown_with_overrides`` (further
        split for the per-method size gate) — pure code motion, no behavior
        change.
        """
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
        return self._apply_mtds_override_if_target(
            venues_dict,
            venue_found_total,
            venue_expected_total,
            filtered,
            cat,
            effective_start,
            end_date,
            venue_mapping,
            service,
            cloud,
            scope,
        )

    def _compute_venue_breakdown_with_overrides(
        self,
        filtered: pd.DataFrame,
        effective_start: str,
        end_date: str,
        venue_mapping: VenueMapping,
        cat_found: int,
        cat_total_days: int,
        service: str,
        cat: str,
        cloud: str,
        scope: str,
    ) -> tuple[dict[str, object], int, int, bool]:
        """Per-venue breakdown, MTDS honest-coverage override, data_type regroup.

        Extracted from ``_build_manifest_category`` — pure code motion, no
        behavior change. Returns ``(venues_dict, venue_found_total,
        venue_expected_total, regrouped_to_data_type)``.
        """
        venues_dict, venue_found_total, venue_expected_total = self._build_and_override_venue_breakdown(
            filtered, effective_start, end_date, venue_mapping, cat_found, cat_total_days, service, cat, cloud, scope
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

        return venues_dict, venue_found_total, venue_expected_total, regrouped_to_data_type

    def _assemble_category_count_fields(
        self,
        cat_found: int,
        cat_total_days: int,
        cat_missing: list[str],
        venue_found_total: int,
        venue_expected_total: int,
    ) -> dict[str, object]:
        """Raw dates/shards count fields of the per-category result dict.

        Extracted from ``_assemble_category_completion_fields`` (further
        split for the per-method size gate) — pure code motion, no behavior
        change. ``shards_*`` mirrors ``venue_dates_*`` under canonical names
        so the UI can render a consistent pair alongside the row-level
        ``completion_pct`` (shards-weighted) — before this field existed the
        UI showed ``dates_found / dates_expected`` next to a shards-weighted
        ``completion_pct``, which looked wrong (e.g. ``1 / 2577`` = 0.04%,
        but the row displayed ``20%``).
        """
        return {
            "dates_found": cat_found,
            "dates_expected": cat_total_days,
            "dates_missing": len(cat_missing),
            "shards_found": venue_found_total,
            "shards_expected": venue_expected_total,
            "venue_dates_found": venue_found_total,
            "venue_dates_expected": venue_expected_total,
            "_venue_found": venue_found_total,
            "_venue_expected": venue_expected_total,
        }

    def _assemble_category_completion_fields(
        self,
        cat_found: int,
        cat_total_days: int,
        cat_missing: list[str],
        venue_found_total: int,
        venue_expected_total: int,
        cat_pct_dates: float,
        cat_pct_shards: float,
        coverage: dict[str, object],
    ) -> dict[str, object]:
        """Completion/coverage-derived fields of the per-category result dict.

        Extracted from ``_assemble_category_result`` (further split for the
        per-method size gate) — pure code motion, no behavior change.
        ``completion_pct`` (PRIMARY metric, operator decision 2026-06-14) is
        the shards-weighted captured/could-exist ratio — falls back to the
        date-based ``cat_pct_dates`` only when the shards denominator is zero
        (no per-bucket breakdown yet).
        """
        fields = self._assemble_category_count_fields(
            cat_found, cat_total_days, cat_missing, venue_found_total, venue_expected_total
        )
        fields.update(
            {
                "completion_pct": cat_pct_shards,
                "completion_pct_dates": cat_pct_dates,
                "completion_pct_shards_weighted": cat_pct_shards,
                "completion_pct_attempt_blended": coverage["completion_pct"],
                "attempt_coverage_pct": coverage["attempt_coverage_pct"],
                "capture_coverage_pct": coverage["capture_coverage_pct"],
                "coverage_semantics": coverage["coverage_semantics"],
                "empty_rate_estimate": coverage["empty_rate_estimate"],
                "failure_rate": coverage["failure_rate"],
                "capture_status_counts": coverage["capture_status_counts"],
                "counts": coverage["counts"],
                "coverage": float(coverage["coverage"]),  # pyright: ignore[reportArgumentType]
            }
        )
        return fields

    def _assemble_category_result(
        self,
        cat: str,
        bucket: str,
        cat_found: int,
        cat_total_days: int,
        cat_missing: list[str],
        venue_found_total: int,
        venue_expected_total: int,
        cat_pct_dates: float,
        cat_pct_shards: float,
        coverage: dict[str, object],
        venues_dict: dict[str, object],
        unit: str,
        effective_start: str,
        cat_found_sorted: list[str],
        breakdown_axis: str,
        failure_rate_by_dimension: dict[str, object],
    ) -> dict[str, object]:
        """Assemble the per-category result dict (pure code motion). ``venues``/
        ``data_types`` — the aggregator writes data under exactly ONE, per ``breakdown_axis``."""
        result: dict[str, object] = {
            "category": cat,
            "bucket": bucket,
            "prefixes_queried": 0,
            "venue_weighted": bool(venues_dict),
            "unit": unit,
            "effective_start_date": effective_start,
            "missing_dates": cat_missing,
            "dates_found_list": cat_found_sorted,
            "dates_missing_list": cat_missing,
            "breakdown_axis": breakdown_axis,
            "venues": {} if breakdown_axis == "data_type" else venues_dict,
            "data_types": venues_dict if breakdown_axis == "data_type" else {},
            "failure_rate_by_dimension": failure_rate_by_dimension,
        }
        completion_args = (
            cat_found,
            cat_total_days,
            cat_missing,
            venue_found_total,
            venue_expected_total,
            cat_pct_dates,
            cat_pct_shards,
            coverage,
        )
        result.update(self._assemble_category_completion_fields(*completion_args))
        return result

    def _empty_category_result(self, cat: str) -> dict[str, object]:
        """The placeholder result for a service/category combo with nothing to show.

        Extracted from ``_build_manifest_category`` (further split for the
        per-method size gate) — pure code motion, no behavior change.
        """
        return {
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

    def _resolve_category_bucket_and_index(self, service: str, cat: str, cloud: str) -> tuple[str, pd.DataFrame] | None:
        """Validate service/category, resolve the bucket name, load the index.

        Extracted from ``_build_manifest_category`` (further split for the
        per-method size gate) — pure code motion, no behavior change. Returns
        ``None`` when the caller should return the empty placeholder (invalid
        service, restricted category, or no manifest data at all).
        """
        if service not in SERVICE_TO_KIND and service != "features-commodity-service":
            return None

        # Skip categories that don't apply to this service (single-bucket services)
        allowed = self._SERVICE_CATEGORY_RESTRICTIONS.get(service)
        if allowed and cat.upper() not in allowed:
            return None

        # Resolve the main bucket name (for display in the response)
        override = self._BUCKET_CATEGORY_OVERRIDES.get((service, cat.lower()))
        if override:
            bucket = override.format(pid=self.project_id, env=self.deployment_env_short)
        elif service == "features-commodity-service":
            bucket = COMMODITY_BUCKET_TEMPLATE.format(pid=self.project_id)
        else:
            kind = SERVICE_TO_KIND[service]
            ag = cat.lower() or None
            if ag == "prediction" and PREDICTION_KIND_MAP.get(kind):
                # Prediction-SPECIAL single-bucket kind (its own KIND, resolved kind-only).
                bucket = _dss.resolve_bucket_name(cloud=cast(object, cloud), kind=PREDICTION_KIND_MAP[kind])  # pyright: ignore[reportArgumentType]
            else:
                # Normal per-asset_group kind — incl. a per-AG kind that merely serves prediction
                # as one of its asset_groups (e.g. features-cross-instrument, no PREDICTION_KIND_MAP
                # entry). Resolve WITH asset_group; kind-only raised "asset_group= is required".
                bucket = _dss.resolve_bucket_name(cloud=cast(object, cloud), kind=kind, asset_group=cast(object, ag))  # pyright: ignore[reportArgumentType]

        index = self._read_defi_merged_index(service, cat, cloud=cloud)
        if index.empty:
            return None
        return bucket, index

    def _compute_category_coverage_and_subdims(
        self,
        filtered: pd.DataFrame,
        cat: str,
        cat_found: int,
        cat_total_days: int,
        venue_found_total: int,
        venue_expected_total: int,
        service: str,
        effective_start: str,
        end_date: str,
        venue_mapping: VenueMapping,
    ) -> tuple[float, float, dict[str, object], dict[str, object]]:
        """Completion percentages, ``build_coverage_metrics``, and v4 sub-dims.

        Extracted from ``_compute_category_metrics`` (further split for the
        per-method size gate) — pure code motion, no behavior change. Returns
        ``(cat_pct_dates, cat_pct_shards, coverage, sub_dims)``.
        """
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

        # v4 sub-dimension breakdowns (DeFi, chains, feature groups)
        sub_dims = self._build_v4_sub_dimensions(
            filtered,
            service,
            cat,
            effective_start,
            end_date,
            venue_mapping,
        )
        return cat_pct_dates, cat_pct_shards, coverage, sub_dims

    def _compute_category_metrics(
        self,
        filtered: pd.DataFrame,
        cat: str,
        cat_found: int,
        cat_total_days: int,
        venue_found_total: int,
        venue_expected_total: int,
        venues_dict: dict[str, object],
        cat_found_dates: set[str],
        service: str,
        effective_start: str,
        end_date: str,
        venue_mapping: VenueMapping,
        regrouped_to_data_type: bool,
    ) -> tuple[float, float, dict[str, object], dict[str, object], list[str], str, dict[str, object], str]:
        """Completion pcts/coverage/sub-dims/axis discriminator (pure code motion). Returns
        ``(cat_pct_dates, cat_pct_shards, coverage, sub_dims, cat_found_sorted, unit,
        failure_rate_by_dimension, breakdown_axis)``. SSOT: codex/02-data/sports-data-source-coverage-matrix.md §3."""
        cat_pct_dates, cat_pct_shards, coverage, sub_dims = self._compute_category_coverage_and_subdims(
            filtered,
            cat,
            cat_found,
            cat_total_days,
            venue_found_total,
            venue_expected_total,
            service,
            effective_start,
            end_date,
            venue_mapping,
        )

        cat_found_sorted = sorted(cat_found_dates)  # pyright: ignore[reportUnknownArgumentType]
        unit = "fixtures" if cat.upper() == "SPORTS" and venues_dict else "dates"
        failure_rate_by_dimension = build_failure_rate_by_dimension(venues_dict)
        breakdown_axis = "data_type" if cat.upper() == "SPORTS" or regrouped_to_data_type else "venue"

        return (
            cat_pct_dates,
            cat_pct_shards,
            coverage,
            sub_dims,
            cat_found_sorted,
            unit,
            cast(dict[str, object], failure_rate_by_dimension),
            breakdown_axis,
        )

    def _prepare_category_slice_and_breakdown(
        self,
        index: pd.DataFrame,
        service: str,
        cat: str,
        start_date: str,
        end_date: str,
        all_date_strs: list[str],
        row_filters: dict[str, str] | None,
        pipeline_modes: list[str] | None,
        venue: list[str] | None,
        venue_mapping: VenueMapping,
        cloud: str,
        scope: str,
    ) -> tuple[str, int, pd.DataFrame, set[str], list[str], int, dict[str, object], int, int, bool]:
        """Combine ``_prepare_manifest_slice`` + ``_compute_venue_breakdown_with_overrides`` (pure code
        motion). Returns ``(effective_start, cat_total_days, filtered, cat_found_dates, cat_missing,
        cat_found, venues_dict, venue_found_total, venue_expected_total, regrouped_to_data_type)``."""
        effective_start, _cat_date_strs, cat_total_days, filtered, cat_found_dates, cat_missing, cat_found = (
            self._prepare_manifest_slice(
                index, service, cat, start_date, end_date, all_date_strs, row_filters, pipeline_modes, venue
            )
        )
        venues_dict, venue_found_total, venue_expected_total, regrouped_to_data_type = (
            self._compute_venue_breakdown_with_overrides(
                filtered,
                effective_start,
                end_date,
                venue_mapping,
                cat_found,
                cat_total_days,
                service,
                cat,
                cloud,
                scope,
            )
        )
        return (
            effective_start,
            cat_total_days,
            filtered,
            cat_found_dates,
            cat_missing,
            cat_found,
            venues_dict,
            venue_found_total,
            venue_expected_total,
            regrouped_to_data_type,
        )

    def _assemble_and_annotate_category_result(
        self,
        cat: str,
        bucket: str,
        cat_found: int,
        cat_total_days: int,
        cat_missing: list[str],
        venue_found_total: int,
        venue_expected_total: int,
        venues_dict: dict[str, object],
        service: str,
        effective_start: str,
        venue_mapping: VenueMapping,
        metrics: tuple[float, float, dict[str, object], dict[str, object], list[str], str, dict[str, object], str],
    ) -> dict[str, object]:
        """Assemble + MTDS-annotate (codex/02-data/mtds-data-source-coverage-matrix.md §1) + merge
        sub_dims (pure code motion). ``metrics`` is ``_compute_category_metrics``'s return tuple."""
        (
            cat_pct_dates,
            cat_pct_shards,
            coverage,
            sub_dims,
            cat_found_sorted,
            unit,
            failure_rate_by_dimension,
            breakdown_axis,
        ) = metrics
        result = self._assemble_category_result(
            cat,
            bucket,
            cat_found,
            cat_total_days,
            cat_missing,
            venue_found_total,
            venue_expected_total,
            cat_pct_dates,
            cat_pct_shards,
            coverage,
            venues_dict,
            unit,
            effective_start,
            cat_found_sorted,
            breakdown_axis,
            failure_rate_by_dimension,
        )
        self._annotate_mtds_category(result, service, cat, venues_dict, venue_mapping)
        result.update(sub_dims)
        return result

    def _finalize_manifest_category(
        self,
        bucket: str,
        cat: str,
        service: str,
        end_date: str,
        venue_mapping: VenueMapping,
        slice_result: tuple[str, int, pd.DataFrame, set[str], list[str], int, dict[str, object], int, int, bool],
    ) -> dict[str, object]:
        (
            effective_start,
            cat_total_days,
            filtered,
            cat_found_dates,
            cat_missing,
            cat_found,
            venues_dict,
            venue_found_total,
            venue_expected_total,
            regrouped_to_data_type,
        ) = slice_result
        metrics = self._compute_category_metrics(
            filtered,
            cat,
            cat_found,
            cat_total_days,
            venue_found_total,
            venue_expected_total,
            venues_dict,
            cat_found_dates,
            service,
            effective_start,
            end_date,
            venue_mapping,
            regrouped_to_data_type,
        )
        return self._assemble_and_annotate_category_result(
            cat,
            bucket,
            cat_found,
            cat_total_days,
            cat_missing,
            venue_found_total,
            venue_expected_total,
            venues_dict,
            service,
            effective_start,
            venue_mapping,
            metrics,
        )

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
        venue: list[str] | None = None,
        scope: str = "could_exist",
    ) -> dict[str, object]:
        """Build a single category entry for manifest status. ``row_filters``/``pipeline_modes``/``venue``
        narrow the slice for the secondary-axis + filter-chip query params — see ``_apply_manifest_filters``."""
        empty = self._empty_category_result(cat)
        resolved = self._resolve_category_bucket_and_index(service, cat, cloud)
        if resolved is None:
            return empty
        bucket, index = resolved

        slice_args = (index, service, cat, start_date, end_date, all_date_strs, row_filters, pipeline_modes, venue)
        slice_result = self._prepare_category_slice_and_breakdown(*slice_args, venue_mapping, cloud, scope)
        return self._finalize_manifest_category(bucket, cat, service, end_date, venue_mapping, slice_result)

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

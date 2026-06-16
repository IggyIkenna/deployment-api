"""Per-venue entry + generic dimension breakdown builders.

Split out of the 6,663-line ``data_status_service.py`` god-module
(codex ratchet plan 2026-06-10). The facade module re-exports every
public + legacy-underscore name, so callers keep importing from
``deployment_api.services.data_status_service``.
"""

import logging
from typing import cast

import pandas as pd
from unified_api_contracts import (
    VenueMapping,
)
from unified_api_contracts.features import (
    EXPECTED_FEATURE_GROUPS_BY_SERVICE,
    get_feature_coverage_start,
    is_known_feature_group,
)
from unified_api_contracts.registry import (
    get_raw_source_data_types,
    is_expected,
    is_in_tradfi_tick_window,
    is_processed_data_type,
)

import deployment_api.services.data_status_service as _dss

logger = logging.getLogger(__name__)

from deployment_api.services.data_status.breakdowns_domain import DomainBreakdownsMixin
from deployment_api.services.data_status.coverage_metrics import (
    compute_capture_status_counts,
    compute_empty_reason_counts,
    compute_failure_pillar_counts,
    compute_out_of_window_count,
    derive_capture_status_rates,
)
from deployment_api.services.data_status.mtds import TRADFI_TICK_ONLY_DATA_TYPES
from deployment_api.services.data_status.reference_scope import (
    is_reference_bundle_service,
    is_reference_venue_day_in_scope,
)


class CoreBreakdownsMixin(DomainBreakdownsMixin):
    """Single-venue entries + data_type / underlying / feature-group breakdowns.

    The data_status mixins form a single linear inheritance chain
    (cli -> defi -> sports -> breakdowns_domain -> breakdowns_core ->
    venue_resolution -> coverage -> missing_shards -> manifest) so that
    every cross-group ``self._method`` reference resolves statically
    under basedpyright strict. ``DataStatusService`` composes the top of
    the chain and is the ONLY public entry point — import it from
    ``deployment_api.services.data_status_service`` (the facade).
    """

    @staticmethod
    def _apply_dimensional_granularity(
        venue_entry: dict[str, object],
        breakdown: dict[str, object],
    ) -> None:
        """Replace venue-level found/expected with SUM across sub-dimensions.

        Prevents a venue with 3 instrument_types / data_types showing 100%
        when only 1 of 3 has data on a given day.
        """
        dim_found = 0
        dim_expected = 0
        for entry_raw in breakdown.values():
            entry = cast(dict[str, object], entry_raw)
            dim_found += int(entry.get("dates_found", 0))  # pyright: ignore[reportArgumentType]
            dim_expected += int(entry.get("dates_expected", 0))  # pyright: ignore[reportArgumentType]
        if dim_expected > 0:
            venue_entry["dates_found"] = dim_found
            venue_entry["dates_expected"] = dim_expected
            venue_entry["completion_pct"] = min(round(dim_found / max(1, dim_expected) * 100, 2), 100.0)

    def _build_single_venue_entry(
        self,
        v_df: pd.DataFrame,
        venue: str,
        venue_start: str | None,
        eff_start: str,
        end_date: str,
        v_dates_all: set[str],
        v_all_dates: set[str],
        has_data_type: bool,
        venue_mapping: VenueMapping,
        category: str = "",
        service: str = "",
    ) -> dict[str, object]:
        """Build stats dict for a single venue."""
        v_dates = v_dates_all & v_all_dates
        expected = len(v_all_dates)
        found = len(v_dates)
        v_missing = sorted(v_all_dates - v_dates)
        v_found_sorted = sorted(v_dates)

        # Capture-status rollup for this venue — Phase-C honest-coverage.
        # ``expected`` is the shards-expected denominator; once Phase B's
        # sentinel rows land the split (captured / empty_confirmed /
        # attempted_failed) is meaningful at the venue level so the UI can
        # drive the 4-state heatmap and the failure-rate drill-down.
        v_capture_counts = compute_capture_status_counts(v_df)
        v_capture_rates = derive_capture_status_rates(v_capture_counts, expected)
        v_failure_pillars = compute_failure_pillar_counts(v_df)
        v_empty_reasons = compute_empty_reason_counts(v_df)
        # OOW count: never-collectable cells surfaced as a distinct bucket so the
        # UI can display them as "outside window — not a gap" without counting
        # them as coverage gaps.
        v_oow_count = compute_out_of_window_count(v_df)

        venue_entry: dict[str, object] = {
            "dates_found": found,
            "dates_expected": expected,
            "dates_expected_venue": expected,
            "dates_missing": len(v_missing),
            "missing_dates": v_missing,
            "dates_found_list": v_found_sorted,
            "dates_missing_list": v_missing,
            "completion_pct": min(round(found / max(1, expected) * 100, 2), 100.0),
            "venue_start_date": venue_start,
            "capture_status_counts": {
                "captured": v_capture_counts.captured,
                "empty_confirmed": v_capture_counts.empty_confirmed,
                "attempted_failed": v_capture_counts.attempted_failed,
                "expected_unattempted_known_empty": v_capture_counts.expected_unattempted_known_empty,
                "expected_unattempted_pending_fetch": v_capture_counts.expected_unattempted_pending_fetch,
                "out_of_window": v_oow_count,
            },
            "counts": {
                "captured": v_capture_counts.captured,
                "empty_confirmed": v_capture_counts.empty_confirmed,
                "attempted_failed": v_capture_counts.attempted_failed,
                "expected_unattempted_known_empty": v_capture_counts.expected_unattempted_known_empty,
                "expected_unattempted_pending_fetch": v_capture_counts.expected_unattempted_pending_fetch,
                "out_of_window": v_oow_count,
            },
            "coverage": v_capture_rates["honest_coverage"],
            "failure_pillars": v_failure_pillars,
            "empty_reasons": v_empty_reasons,
            "attempt_coverage_pct": v_capture_rates["attempt_coverage_pct"],
            "capture_coverage_pct": v_capture_rates["capture_coverage_pct"],
            "honest_coverage": v_capture_rates["honest_coverage"],
            "empty_rate": v_capture_rates["empty_rate"],
            "failure_rate": v_capture_rates["failure_rate"],
        }

        # v4: instrument_type breakdown (spot, perpetuals, equity, pool, etc.)
        has_instrument_type = (
            "instrument_type" in v_df.columns
            and not v_df["instrument_type"].isna().all()
            and (v_df["instrument_type"].str.len() > 0).any()
        )
        if has_instrument_type:
            itype_breakdown = self._build_instrument_type_breakdown(
                v_df,
                venue,
                eff_start,
                end_date,
                venue_mapping,
                has_data_type,
                category=category,
                service=service,
            )
            if itype_breakdown:
                venue_entry["instrument_types"] = itype_breakdown
                self._apply_dimensional_granularity(venue_entry, itype_breakdown)

        if has_data_type and not has_instrument_type:
            dt_breakdown = self._build_data_type_breakdown(
                v_df,  # pyright: ignore[reportUnknownArgumentType]
                venue,
                eff_start,
                end_date,
                venue_mapping,
                service=service,
                category=category,
            )
            if dt_breakdown:
                venue_entry["data_types"] = dt_breakdown
                self._apply_dimensional_granularity(venue_entry, dt_breakdown)

        # Leagues only for SPORTS (not CEFI/TRADFI/DEFI/PREDICTION)
        if category.upper() in ("SPORTS",):
            league_breakdown = self._build_league_breakdown(v_df, eff_start, end_date)
            if league_breakdown:
                venue_entry["leagues"] = league_breakdown

        # Features services emit per-(timeframe, feature_group) shards. Surface
        # those as drill-down dimensions when the manifest carries them so the
        # deployment-ui can show coverage per (15s/1m/15m/1h x feature_group).
        is_features_service = service.startswith("features-")
        if is_features_service:
            tf_breakdown = self._build_timeframe_breakdown(v_df, eff_start, end_date)
            if tf_breakdown:
                venue_entry["timeframes"] = tf_breakdown
            fg_breakdown = self._build_feature_group_breakdown_uac(v_df, eff_start, end_date, service=service)
            if fg_breakdown:
                venue_entry["feature_groups"] = fg_breakdown

        return venue_entry

    @staticmethod
    def _build_simple_dimension_breakdown(
        venue_df: pd.DataFrame,
        column: str,
        start_date: str,
        end_date: str,
    ) -> dict[str, object]:
        """Generic per-dimension breakdown for features-* services.

        Counts unique (date, value) pairs for ``column`` and returns
        per-value dates_found / dates_expected (= unique observed dates in
        the slice) / completion_pct. No phantom-clamp / UAC-expected logic
        because features services are downstream and inherit upstream
        coverage; the surfaced ``completion_pct`` reflects produced-vs-seen.
        """
        if column not in venue_df.columns:
            return {}
        col_series = venue_df[column].astype(str)
        values = sorted(v for v in col_series.unique() if v and v.strip() and v != "nan")  # pyright: ignore[reportAny]
        if not values:
            return {}
        # Universe of dates observed in this slice (clamped to the requested
        # window). For features services this is the natural "expected" set —
        # if delta-one wrote feature_group=X on day D, that shard is expected.
        slice_dates = {str(d) for d in venue_df["date"].unique() if start_date <= str(d) <= end_date}  # pyright: ignore[reportAny]
        total_expected = len(slice_dates)
        result: dict[str, object] = {}
        for value in values:  # pyright: ignore[reportAny]
            sub_df = venue_df[col_series == value]  # pyright: ignore[reportUnknownVariableType]
            sub_dates = {str(d) for d in sub_df["date"].unique() if start_date <= str(d) <= end_date}  # pyright: ignore[reportUnknownVariableType,reportUnknownMemberType,reportUnknownArgumentType]
            found = len(sub_dates)
            missing = sorted(slice_dates - sub_dates)
            pct = round(found / max(1, total_expected) * 100, 2)
            result[value] = {
                "dates_found": found,
                "dates_expected": total_expected,
                "dates_missing": len(missing),
                "dates_found_list": sorted(sub_dates),
                "missing_dates": missing,
                "completion_pct": min(pct, 100.0),
            }
        return result

    def _build_timeframe_breakdown(
        self,
        venue_df: pd.DataFrame,
        start_date: str,
        end_date: str,
    ) -> dict[str, object]:
        """Per-timeframe coverage for features-* services (15s/1m/15m/1h)."""
        return self._build_simple_dimension_breakdown(venue_df, "timeframe", start_date, end_date)

    def _build_feature_group_breakdown_legacy(
        self,
        venue_df: pd.DataFrame,
        start_date: str,
        end_date: str,
    ) -> dict[str, object]:
        """Per-feature_group coverage for features-* services.

        Note: this is the legacy version, renamed to avoid conflict with
        the timeframe-aware version used by the v4 detail breakdown path.
        Preserved here to keep the file shape stable.
        Phase 2C of feature_dag plan introduces a sibling
        ``_build_feature_group_breakdown_uac`` for the UAC-denominator path.
        """
        return self._build_simple_dimension_breakdown(venue_df, "feature_group", start_date, end_date)

    @staticmethod
    def _clip_dates_to_feature_coverage(
        service: str,
        feature_group: str,
        start_date: str,
        end_date: str,
    ) -> tuple[str, str]:
        """Clip ``[start_date, end_date]`` to UAC ``FEATURE_COVERAGE_START``.

        Mirrors the sports ``clip_dates_to_source_coverage`` shape (UAC
        ``unified_api_contracts.sports``). Used by
        ``_build_feature_group_breakdown_uac`` so pre-coverage dates (e.g. Aave
        V3 before its 2022-03-16 mainnet launch) drop out of the denominator
        instead of inflating ``missing`` for ``aave_lending_rates``.

        Returns the input window unchanged when no floor is registered for
        ``(service, feature_group)`` — falls back to "no clip" semantics.
        """
        floor = get_feature_coverage_start(service, feature_group)
        if floor is None:
            return start_date, end_date
        floor_iso = floor.isoformat()
        return max(start_date, floor_iso), end_date

    def _build_feature_group_breakdown_uac(
        self,
        venue_df: pd.DataFrame,
        start_date: str,
        end_date: str,
        service: str,
    ) -> dict[str, object]:
        """UAC-denominator per-feature_group breakdown for features-* services.

        Distinct from the existing ``_build_feature_group_breakdown`` (which uses
        observed-dates inference + optional timeframe sub-grouping); kept as a
        sibling method so the two callers don't collide on signature. Plan:
        ``feature_dag_uac_ssot_and_features_coverage_2026_05_06.md`` Phase 2C.

        When ``service`` is registered in UAC ``EXPECTED_FEATURE_GROUPS_BY_SERVICE``,
        the denominator becomes ``len(expected_feature_groups) * dates_in_clipped_window``
        per CLAUDE.md "honest absence" principle: declared-not-yet-written
        feature_groups render as ``missing`` instead of being silently absent.
        Each ``(service, feature_group)`` pair clips its own dates window via
        UAC ``FEATURE_COVERAGE_START`` (fallback: epoch — no clip).

        When ``service`` is unregistered or has no expected feature_groups
        declared (volatility / cross-instrument stubs today), falls through
        to the legacy ``_build_simple_dimension_breakdown`` behaviour so
        existing data-status calls remain wire-compatible.
        """
        expected_fgs = EXPECTED_FEATURE_GROUPS_BY_SERVICE.get(service, []) if service else []
        if not expected_fgs:
            return self._build_simple_dimension_breakdown(venue_df, "feature_group", start_date, end_date)
        if "feature_group" not in venue_df.columns:
            return {}
        col_series = venue_df["feature_group"].astype(str)

        observed_dates_by_fg: dict[str, set[str]] = {}
        for fg in expected_fgs:
            sub_df = venue_df[col_series == fg]
            observed_dates_by_fg[fg] = {str(d) for d in sub_df["date"].unique() if start_date <= str(d) <= end_date}  # pyright: ignore[reportAny]

        result: dict[str, object] = {}
        for fg in expected_fgs:
            # Defensive — every entry MUST be in EXPECTED_FEATURE_GROUPS_BY_SERVICE
            # by construction; the assert documents the invariant.
            assert is_known_feature_group(service, fg)
            clip_start, clip_end = self._clip_dates_to_feature_coverage(service, fg, start_date, end_date)
            expected_dates: set[str] = set()
            for ts in pd.date_range(clip_start, clip_end, freq="D"):
                expected_dates.add(str(ts.date()))
            found_dates = observed_dates_by_fg[fg] & expected_dates
            missing_dates = sorted(expected_dates - found_dates)
            found = len(found_dates)
            expected_count = len(expected_dates)
            pct = round(found / max(1, expected_count) * 100, 2)
            result[fg] = {
                "dates_found": found,
                "dates_expected": expected_count,
                "dates_missing": len(missing_dates),
                "dates_found_list": sorted(found_dates),
                "missing_dates": missing_dates,
                "completion_pct": min(pct, 100.0),
            }
        return result

    def _build_instrument_type_breakdown(
        self,
        venue_df: pd.DataFrame,
        venue: str,
        start_date: str,
        end_date: str,
        venue_mapping: VenueMapping,
        has_data_type: bool,
        category: str = "",
        service: str = "",
    ) -> dict[str, object]:
        """Build per-instrument_type stats for a venue (v4).

        Each instrument_type (spot, perpetuals, futures_chain, etc.) gets its own
        entry with dates_found/expected and optional data_type sub-breakdown.

        When data_types are present, the instrument_type's found/expected is the
        SUM across its data_type children (weighted aggregation), not just
        unique-date counts. This gives correct percentages when some data_types
        have fewer expected dates (e.g. TradFi tbbo/trades in tick windows only).
        """
        if "instrument_type" not in venue_df.columns:
            return {}

        itypes = sorted(it for it in venue_df["instrument_type"].unique() if it and str(it).strip())  # pyright: ignore[reportAny]
        if not itypes:
            return {}

        itype_dict: dict[str, object] = {}
        for it in itypes:  # pyright: ignore[reportAny]
            it_df = venue_df[venue_df["instrument_type"] == it]  # pyright: ignore[reportUnknownVariableType]
            it_dates = {str(d) for d in it_df["date"].unique()}  # pyright: ignore[reportUnknownVariableType,reportUnknownMemberType,reportUnknownArgumentType,reportAny]
            # Phantom-expected clamp: use the first observed date for this
            # (venue, instrument_type) as the effective start for the
            # expected-date calendar. Prevents cartesian inflation when a
            # venue has N instrument_types with different launch dates
            # (e.g. POLYMARKET: BTC since 2023-01, HYPE since 2024-11 —
            # without this clamp, HYPE's expected days include 22 months
            # of phantom pre-launch dates).
            it_eff_start = max(start_date, min(it_dates)) if it_dates else start_date
            all_dates = set(venue_mapping.get_expected_trading_dates(venue, it_eff_start, end_date))
            found = len(it_dates & all_dates)
            expected = len(all_dates)

            entry: dict[str, object] = {
                "dates_found": found,
                "dates_expected": expected,
                "completion_pct": min(round(found / max(1, expected) * 100, 2), 100.0),
            }

            # Nest underlying within instrument_type for options/futures venues
            has_underlying = (  # pyright: ignore[reportUnknownVariableType]
                "underlying" in it_df.columns  # pyright: ignore[reportUnknownMemberType]
                and not it_df["underlying"].isna().all()  # pyright: ignore[reportUnknownMemberType]
                and (it_df["underlying"].str.len() > 0).any()  # pyright: ignore[reportUnknownMemberType]
            )
            if has_underlying:
                ul_sub = self._build_underlying_breakdown(
                    it_df,  # pyright: ignore[reportUnknownArgumentType]
                    venue,
                    it_eff_start,
                    end_date,
                    venue_mapping,
                    has_data_type,
                    service=service,
                    category=category,
                )
                if ul_sub:
                    entry["underlyings"] = ul_sub
                    self._apply_dimensional_granularity(entry, ul_sub)

            # Nest data_types within instrument_type if available
            # (only when underlying is absent — otherwise data_types nest
            # inside each underlying entry via _build_underlying_breakdown)
            if has_data_type and not has_underlying:
                dt_sub = self._build_data_type_breakdown(
                    it_df,  # pyright: ignore[reportUnknownArgumentType]
                    venue,
                    it_eff_start,
                    end_date,
                    venue_mapping,
                    service=service,
                    category=category,
                )
                if dt_sub:
                    entry["data_types"] = dt_sub
                    # Use data_type-weighted aggregation: sum across children
                    # so tick-window-filtered types reduce the expected total
                    dt_found_sum = 0
                    dt_expected_sum = 0
                    for dt_entry_raw in dt_sub.values():
                        dt_entry = cast(dict[str, object], dt_entry_raw)
                        dt_found_sum += int(dt_entry.get("dates_found", 0))  # pyright: ignore[reportArgumentType]
                        dt_expected_sum += int(dt_entry.get("dates_expected", 0))  # pyright: ignore[reportArgumentType]
                    if dt_expected_sum > 0:
                        entry["dates_found"] = dt_found_sum
                        entry["dates_expected"] = dt_expected_sum
                        entry["completion_pct"] = min(
                            round(dt_found_sum / max(1, dt_expected_sum) * 100, 2),
                            100.0,
                        )

            itype_dict[it] = entry

        return itype_dict

    def _build_underlying_breakdown(
        self,
        itype_df: pd.DataFrame,
        venue: str,
        start_date: str,
        end_date: str,
        venue_mapping: VenueMapping,
        has_data_type: bool,
        service: str = "",
        category: str = "",
    ) -> dict[str, object]:
        """Build per-underlying stats within an instrument_type for derivatives venues.

        For venues like DERIBIT and CME that trade options/futures on multiple
        underlyings (BTC, ETH, ES), this groups completion by underlying asset
        so users can see which underlyings have gaps.

        Each underlying entry optionally nests data_type sub-breakdowns.
        """
        if "underlying" not in itype_df.columns:
            return {}

        underlyings = sorted(ul for ul in itype_df["underlying"].unique() if ul and str(ul).strip())  # pyright: ignore[reportAny]
        if not underlyings:
            return {}

        ul_dict: dict[str, object] = {}
        for ul in underlyings:  # pyright: ignore[reportAny]
            ul_df = itype_df[itype_df["underlying"] == ul]  # pyright: ignore[reportUnknownVariableType]
            ul_dates = {str(d) for d in ul_df["date"].unique()}  # pyright: ignore[reportAny,reportUnknownVariableType,reportUnknownMemberType,reportUnknownArgumentType]
            # Phantom-expected clamp: earliest observed date for this
            # (venue, instrument_type, underlying) slice sets the effective
            # start. Prevents cartesian inflation when a venue trades a new
            # underlying that launched mid-history (e.g. DERIBIT SOL options
            # launched long after BTC/ETH — counting 2018-2020 as "expected"
            # for SOL is phantom).
            ul_eff_start = max(start_date, min(ul_dates)) if ul_dates else start_date
            all_dates = set(venue_mapping.get_expected_trading_dates(venue, ul_eff_start, end_date))
            found = len(ul_dates & all_dates)
            expected = len(all_dates)

            entry: dict[str, object] = {
                "dates_found": found,
                "dates_expected": expected,
                "completion_pct": min(round(found / max(1, expected) * 100, 2), 100.0),
            }

            # Nest data_types within underlying if available
            if has_data_type:
                dt_sub = self._build_data_type_breakdown(
                    ul_df,  # pyright: ignore[reportUnknownArgumentType]
                    venue,
                    ul_eff_start,
                    end_date,
                    venue_mapping,
                    service=service,
                    category=category,
                )
                if dt_sub:
                    entry["data_types"] = dt_sub
                    self._apply_dimensional_granularity(entry, dt_sub)

            ul_dict[ul] = entry

        return ul_dict

    def _build_data_type_breakdown(
        self,
        venue_df: pd.DataFrame,
        venue: str,
        start_date: str,
        end_date: str,
        venue_mapping: VenueMapping,
        service: str = "",
        category: str = "",
    ) -> dict[str, object]:
        """Build per-data-type stats for a single venue.

        For downstream services (MTDS, MDPS, etc.) uses UAC
        get_expected_data_types_for_venue() to know which data types should
        exist.  For instruments-service, only shows data types actually present
        in the index — UAC expectations are for market data, not reference data.

        TradFi tick-window handling: for TradFi venues, tbbo/trades are only
        expected on dates within TRADFI_TICK_DATA_WINDOWS. Outside those windows,
        only ohlcv_1m (and other non-tick types) are expected.

        **Phantom-expected clamp (Option a, 2026-04-19)**: UAC
        ``get_expected_data_types_for_venue`` returns the data_types declared
        for a venue OVERALL. When this function is called from a narrowed
        slice (e.g. ``(venue, instrument_type, underlying)`` via
        ``_build_underlying_breakdown``), applying the venue-level list
        cartesian-multiplies the expected shards — e.g. DERIBIT's
        ``options_chain`` data_type appears as "expected" under every
        instrument_type (futures_chain, perpetual, options_chain) even though
        it only applies to one. We therefore intersect UAC's declared set
        with the data_types actually observed anywhere in this slice. dt's
        declared by UAC but never observed in this slice are treated as
        "not expected for this sub-context" — they are dropped, not counted
        as missing phantoms.
        """
        if "data_type" not in venue_df.columns:
            return {}

        is_tradfi = category.upper() == "TRADFI"

        present_dts = {str(dt) for dt in venue_df["data_type"].unique() if dt and str(dt).strip()}  # pyright: ignore[reportAny]
        uac_expected_dts = set(_dss.get_expected_data_types_for_venue(venue, service=service))
        # Only count UAC-declared dts that this sub-slice has ever observed —
        # drops cartesian phantoms. ``expected_dts`` retains the "is this
        # UAC-declared?" semantic for the per-row ``is_expected`` flag.
        expected_dts = uac_expected_dts & present_dts
        all_dts = sorted(expected_dts | present_dts)

        if not all_dts:
            return {}

        dt_dict: dict[str, object] = {}
        for dt in all_dts:
            dt_df = venue_df[venue_df["data_type"] == dt]
            dt_dates = {str(d) for d in dt_df["date"].unique()} if not dt_df.empty else set()  # pyright: ignore[reportAny,reportUnknownVariableType]

            # Per-data-type start date from UAC; when UAC has no declared
            # start for this (venue, dt), use the earliest date observed in
            # this slice to avoid phantom pre-launch expected dates.
            dt_start = _dss.get_venue_data_type_start_date(venue, dt)
            if dt_start:
                dt_eff_start = max(start_date, dt_start)
            elif dt_dates:
                dt_eff_start = max(start_date, min(dt_dates))  # pyright: ignore[reportUnknownArgumentType]
            else:
                dt_eff_start = start_date
            # Use compound key (venue:data_type) for trading schedule lookup —
            # e.g. POLYMARKET:CRUDE_OIL gets weekday-only, POLYMARKET:BTC gets 24/7
            shard_key = f"{venue}:{dt}" if dt else venue
            dt_expected_list = venue_mapping.get_expected_trading_dates(
                shard_key,
                dt_eff_start,
                end_date,
            )
            dt_expected = set(dt_expected_list)

            # TradFi tick-window filter: tbbo/trades are only expected on dates
            # within configured tick windows (Databento cost management).
            if is_tradfi and dt in TRADFI_TICK_ONLY_DATA_TYPES:
                dt_expected = {d for d in dt_expected if is_in_tradfi_tick_window(d)}

            dt_found = dt_dates & dt_expected  # pyright: ignore[reportUnknownVariableType]
            dt_missing_dates = sorted(dt_expected - dt_found)  # pyright: ignore[reportUnknownArgumentType]

            scope_in, dt_is_processed, actionable_missing, blocked_dates = self._classify_data_type_for_venue(
                category=category,
                venue=venue,
                data_type=dt,
                missing_dates=dt_missing_dates,
                venue_df=venue_df,
                service=service,
                expected_dates=sorted(dt_expected),  # pyright: ignore[reportUnknownArgumentType]
            )

            pct = round(len(dt_found) / max(1, len(dt_expected)) * 100, 2)  # pyright: ignore[reportUnknownArgumentType]

            dt_dict[dt] = {
                "dates_found": len(dt_found),  # pyright: ignore[reportUnknownArgumentType]
                "dates_expected": len(dt_expected),  # pyright: ignore[reportUnknownArgumentType]
                "dates_missing": len(actionable_missing),
                "dates_blocked_on_raw": len(blocked_dates),
                "dates_found_list": sorted(dt_found),  # pyright: ignore[reportUnknownArgumentType]
                "missing_dates": actionable_missing,
                "blocked_on_raw_dates": blocked_dates,
                "completion_pct": min(pct, 100.0),
                "start_date": dt_start,
                "is_expected": dt in expected_dts,
                "in_expected_coverage": scope_in,
                "is_processed_data_type": dt_is_processed,
                "out_of_scope": not scope_in and not dt_is_processed,
            }
        return dt_dict

    def _classify_data_type_for_venue(
        self,
        *,
        category: str,
        venue: str,
        data_type: str,
        missing_dates: list[str],
        venue_df: pd.DataFrame,
        service: str = "",
        expected_dates: list[str] | None = None,
    ) -> tuple[bool, bool, list[str], list[str]]:
        """Apply the four-state classification to a (venue, data_type) row.

        Returns ``(scope_in, is_processed, actionable_missing, blocked_dates)``.

        **Reference-data bundle services** (``instruments-service`` /
        ``corporate-actions`` — ``reference_scope.REFERENCE_BUNDLE_SERVICES``)
        emit REFERENCE data on a ``data_type=""`` bundle row that is in NEITHER
        the market-data ``EXPECTED_COVERAGE`` registry nor
        ``PROCESSED_REQUIRES_RAW``. Scoping them against the market-data
        registry flags every row ``out_of_scope``. So for those services we
        branch the scope onto a REFERENCE expectation source (the
        instruments-service catalogue): in-scope ⟺ ``(asset_group, venue)`` is
        a configured IS venue AND the row covers a day on/after that venue's
        genesis. A configured venue missing data stays in-scope + missing (a
        real gap in the denominator); an unlisted venue or a purely pre-genesis
        row is out_of_scope. Grain = venue/day (per-instrument_type scoping is a
        separate follow-on tied to a manifest-schema change).

        Four states (Phase 1 of the unified data-status work):

        - ``captured``       — already counted via ``dates_found``; this method
                               only splits the *missing* set.
        - ``missing``        — in expected-coverage scope, raw is captured (or
                               this IS raw), processed shard is absent.
                               Actionable.
        - ``blocked_on_raw`` — processed shard absent because the underlying raw
                               shard is also absent. Fix raw first; not
                               actionable on this row.
        - ``out_of_scope``   — ``(asset_group, venue, data_type)`` not in
                               EXPECTED_COVERAGE. Pure-derived processed types
                               stay in scope (they inherit from raw). Excluded
                               from the denominator at the UI layer.

        If the UAC EXPECTED_COVERAGE policy lists this ``(venue, data_type)``
        explicitly, treat it as venue-native raw for this venue — even if the
        data_type token appears in PROCESSED_REQUIRES_RAW (e.g. CBOE emits
        ``ohlcv_15m`` natively while CeFi MDPS derives ``ohlcv_15m`` from
        trades — same token, different role per venue).
        """
        # Reference-data bundle services scope off the instruments-service
        # catalogue (venue/day grain), NOT the market-data registry — the
        # bundled reference row has no market-data data_type so the registry
        # would always flag it out_of_scope (the audit §B bug).
        if is_reference_bundle_service(service):
            scope_days = expected_dates if expected_dates is not None else list(missing_dates)
            ref_scope_in = is_reference_venue_day_in_scope(category, venue, scope_days)
            # Reference rows are never "processed" candles; a missing day on a
            # configured venue is a genuine actionable gap (never blocked-on-raw).
            return ref_scope_in, False, list(missing_dates), []

        scope_in = is_expected(category, venue, data_type) if category and venue else True
        dt_is_processed = is_processed_data_type(data_type)
        apply_precondition = dt_is_processed and not scope_in
        if not (apply_precondition and missing_dates):
            return scope_in, dt_is_processed, list(missing_dates), []
        raw_sources = get_raw_source_data_types(data_type)
        if not raw_sources:
            return scope_in, dt_is_processed, list(missing_dates), []
        raw_captured: set[str] = set()
        if "data_type" in venue_df.columns and "date" in venue_df.columns:
            raw_df = venue_df[venue_df["data_type"].isin(raw_sources)]
            if not raw_df.empty:
                raw_captured = {str(d) for d in raw_df["date"].unique()}  # pyright: ignore[reportAny]
        actionable_missing = [d for d in missing_dates if d in raw_captured]
        blocked_dates = [d for d in missing_dates if d not in raw_captured]
        return scope_in, dt_is_processed, actionable_missing, blocked_dates

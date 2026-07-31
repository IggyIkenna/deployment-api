"""Domain-specific breakdowns: leagues, chains, DeFi sub-dims, v4 groupings.

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
from unified_api_contracts.sports import (
    get_entity_league_coverage,
    get_sports_entity_start_date,
)

logger = logging.getLogger(__name__)

from deployment_api.services.data_status.frame_utils import ensure_underlying_column
from deployment_api.services.data_status.rollup_cache import (
    ALL_DEFI_GHOST_VENUES,
    DEFI_NON_PROTOCOL_VENUE_PREFIXES,
)
from deployment_api.services.data_status.sports import SportsStatusMixin
from deployment_api.services.data_status.sports_helpers import (
    FEATURES_SPORTS_DATA_TYPE_META,
    SPORTS_DATA_TYPE_META,
    sports_honest_coverage,
)


class DomainBreakdownsMixin(SportsStatusMixin):
    """League / chain / DeFi-sub-dimension / v4 grouping builders.

    The data_status mixins form a single linear inheritance chain
    (cli -> defi -> sports -> breakdowns_domain -> breakdowns_core ->
    venue_resolution -> coverage -> missing_shards -> manifest_category_builder ->
    manifest) so that
    every cross-group ``self._method`` reference resolves statically
    under basedpyright strict. ``DataStatusService`` composes the top of
    the chain and is the ONLY public entry point — import it from
    ``deployment_api.services.data_status_service`` (the facade).
    """

    def _build_league_breakdown(
        self,
        venue_df: pd.DataFrame,
        start_date: str,
        end_date: str,
        fixture_league_calendar: dict[str, set[str]] | None = None,
        fixture_counts_by_league: dict[str, int] | None = None,
        is_fixtures_entity: bool = False,
        entity_coverage: frozenset[str] | None = None,
        full_manifest: pd.DataFrame | None = None,
    ) -> dict[str, object]:
        """Per-league stats for a sports entity — fixture-based when ``fixture_counts_by_league`` is
        given (unit is the fixture, not the date; FIXTURES is ground truth, others are denominated
        against its per-league count), else legacy date-counting via ``fixture_league_calendar``.
        ``entity_coverage`` restricts expected league_ids; ``full_manifest`` locates FIXTURES dates.
        """
        if "league_id" not in venue_df.columns:
            return {}
        leagues_in_data = {str(lid) for lid in venue_df["league_id"].unique() if lid}  # pyright: ignore[reportAny]
        if not leagues_in_data:
            return {}
        use_fixture_counts = fixture_counts_by_league is not None and "instrument_count" in venue_df.columns
        league_dict: dict[str, object] = {}
        for lid in sorted(leagues_in_data):
            l_df = venue_df[venue_df["league_id"] == lid]
            if use_fixture_counts:
                entry = self._build_fixture_based_league_entry(
                    l_df,
                    lid,
                    is_fixtures_entity,
                    entity_coverage,
                    cast(dict[str, int], fixture_counts_by_league),
                    full_manifest if full_manifest is not None else venue_df,
                )
                if entry is None:  # not covered by this entity — skip it
                    continue
                league_dict[lid] = entry
            else:
                league_dict[lid] = self._build_legacy_league_entry(l_df, lid, start_date, fixture_league_calendar)

        self._add_na_leagues(league_dict, entity_coverage, fixture_counts_by_league, is_fixtures_entity)
        return league_dict

    @staticmethod
    def _build_fixture_based_league_entry(
        l_df: pd.DataFrame,
        lid: str,
        is_fixtures_entity: bool,
        entity_coverage: frozenset[str] | None,
        fixture_counts_by_league: dict[str, int],
        manifest_src: pd.DataFrame,
    ) -> dict[str, object] | None:
        """One league's fixture-based stats entry, or ``None`` if not covered by this entity."""
        league_fixture_found = int(l_df["instrument_count"].sum())  # pyright: ignore[reportAny]
        if is_fixtures_entity:
            # FIXTURES is ground truth — expected = found
            league_fixture_expected = league_fixture_found
        elif entity_coverage is not None and lid.upper() not in entity_coverage:
            return None
        else:
            # Other entities: expected = FIXTURES count for this league
            league_fixture_expected = fixture_counts_by_league.get(lid, league_fixture_found)

        # Date lists for backfill targeting (which dates have gaps?). Uses
        # full_manifest to find FIXTURES dates (l_df is filtered to the
        # current entity, so FIXTURES rows are absent from it).
        found_dates = sorted({str(d) for d in l_df["date"].unique()})  # pyright: ignore[reportAny]
        fixtures_rows_for_league = (
            manifest_src[(manifest_src["data_type"] == "FIXTURES") & (manifest_src["league_id"] == lid)]
            if "data_type" in manifest_src.columns
            else pd.DataFrame()
        )
        # For non-FIXTURES entities, dates where FIXTURES exist but this
        # entity has no data are "missing dates" for backfill targeting.
        if not is_fixtures_entity and not fixtures_rows_for_league.empty:
            fixture_dates = {str(d) for d in fixtures_rows_for_league["date"].unique()}  # pyright: ignore[reportAny]
            entity_dates = {str(d) for d in l_df["date"].unique()}  # pyright: ignore[reportAny]
            missing_dates = sorted(fixture_dates - entity_dates)
        else:
            missing_dates = []

        return {
            "dates_found": league_fixture_found,
            "dates_expected": max(1, league_fixture_expected),
            "dates_missing": max(0, league_fixture_expected - league_fixture_found),
            "missing_dates": missing_dates[:500],
            "dates_found_list": found_dates[:500],
            "completion_pct": min(round(league_fixture_found / max(1, league_fixture_expected) * 100, 2), 100.0),
            "unit": "fixtures",
        }

    @staticmethod
    def _build_legacy_league_entry(
        l_df: pd.DataFrame,
        lid: str,
        start_date: str,
        fixture_league_calendar: dict[str, set[str]] | None,
    ) -> dict[str, object]:
        """One league's legacy date-based stats entry (non-sports callers)."""
        found_dates_set = {str(d) for d in l_df["date"].unique()}  # pyright: ignore[reportAny]
        found_count = len(found_dates_set)
        if fixture_league_calendar and lid in fixture_league_calendar:
            expected_dates = {d for d in fixture_league_calendar[lid] if d >= start_date}
            expected_count = len(expected_dates)
            missing_dates_list = sorted(expected_dates - found_dates_set)
        else:
            expected_count = found_count
            missing_dates_list = []
        return {
            "dates_found": found_count,
            "dates_expected": max(1, expected_count),
            "dates_missing": len(missing_dates_list),
            "missing_dates": missing_dates_list[:500],
            "dates_found_list": sorted(found_dates_set)[:500],
            "completion_pct": min(round(found_count / max(1, expected_count) * 100, 2), 100.0),
        }

    @staticmethod
    def _add_na_leagues(
        league_dict: dict[str, object],
        entity_coverage: frozenset[str] | None,
        fixture_counts_by_league: dict[str, int] | None,
        is_fixtures_entity: bool,
    ) -> None:
        """Mutate ``league_dict`` in place, adding greyed-out entries for leagues this entity
        does not cover (e.g. J-League has no Understat xG)."""
        if entity_coverage is None or not fixture_counts_by_league or is_fixtures_entity:
            return
        all_fixture_leagues = set(fixture_counts_by_league.keys())
        uncovered = all_fixture_leagues - {lid.upper() for lid in league_dict}
        uncovered -= set(entity_coverage)
        for lid in sorted(uncovered):
            league_dict[lid] = {
                "dates_found": 0,
                "dates_expected": 0,
                "dates_missing": 0,
                "missing_dates": [],
                "dates_found_list": [],
                "completion_pct": 0.0,
                "unit": "fixtures",
                "not_applicable": True,
            }

    def _build_defi_sub_dimension_breakdown(
        self,
        filtered: pd.DataFrame,
        start_date: str,
        end_date: str,
    ) -> dict[str, object]:
        """Per-sub-dimension stats for DEFI, grouped by ``_defi_source`` (gas-fees, dex-pools,
        etc.); the "main" DEFI bucket rows (source="") are shown as "defi-core"."""
        if "_defi_source" not in filtered.columns:
            return {}

        # All known sub-dims plus any that appeared in the data
        all_sources = set(self._MTDS_DEFI_SUB_DIMENSIONS)
        data_sources = {str(s) for s in filtered["_defi_source"].unique() if s}  # pyright: ignore[reportAny]
        all_sources |= data_sources

        sub_dim_dict: dict[str, object] = {}
        for src in sorted(all_sources):
            src_df = filtered[filtered["_defi_source"] == src]
            sub_dim_dict[src] = self._build_defi_sub_dim_entry(src_df, start_date, end_date)

        # Include "defi-core" for the main bucket rows
        core_mask = filtered["_defi_source"] == ""
        if core_mask.any():
            sub_dim_dict["defi-core"] = self._build_defi_sub_dim_entry(filtered[core_mask], start_date, end_date)

        return sub_dim_dict

    @staticmethod
    def _build_defi_sub_dim_entry(src_df: pd.DataFrame, start_date: str, end_date: str) -> dict[str, object]:
        """One DeFi sub-dimension (or the "defi-core" bucket)'s stats entry."""
        src_dates = {str(d) for d in src_df["date"].unique()} if not src_df.empty else set()  # pyright: ignore[reportAny,reportUnknownVariableType]
        src_venues = sorted(src_df["venue"].unique()) if not src_df.empty else []
        all_dates_range = pd.date_range(start_date, end_date, freq="D")
        expected_dates = {d.strftime("%Y-%m-%d") for d in all_dates_range}
        found_dates = src_dates & expected_dates  # pyright: ignore[reportUnknownVariableType]
        missing_dates = sorted(expected_dates - found_dates)  # pyright: ignore[reportUnknownArgumentType]
        return {
            "dates_found": len(found_dates),  # pyright: ignore[reportUnknownArgumentType]
            "dates_expected": len(expected_dates),
            "dates_missing": len(missing_dates),
            "completion_pct": min(round(len(found_dates) / max(1, len(expected_dates)) * 100, 2), 100.0),  # pyright: ignore[reportUnknownArgumentType]
            "venues": src_venues,
            "venue_count": len(src_venues),
        }

    def _venue_expected_dates_for_chain(
        self,
        chain_df: pd.DataFrame,
        chain_venues: list[str],
        start_date: str,
        end_date: str,
        venue_mapping: VenueMapping,
    ) -> dict[str, set[str]]:
        """Per-venue expected-dates lookup for the chain breakdown. Each venue's expected window
        is clipped to ``max(start_date, venue_start, inferred_min)``; falls back to the calendar
        range when the venue mapping has no expected-trading-dates entry."""
        venue_expected_dates: dict[str, set[str]] = {}
        for v in chain_venues:
            vs = venue_mapping.get_venue_start_date(v)  # pyright: ignore[reportAny]
            if not vs:
                v_mask = chain_df["venue"] == v
                v_dates = {str(d) for d in chain_df.loc[v_mask, "date"].unique()}  # pyright: ignore[reportAny]
                vs = min(v_dates) if v_dates else start_date
            eff_start = max(start_date, vs) if vs else start_date
            v_expected = set(venue_mapping.get_expected_trading_dates(v, eff_start, end_date))
            if not v_expected:
                v_expected = {d.strftime("%Y-%m-%d") for d in pd.date_range(eff_start, end_date, freq="D")}
            venue_expected_dates[v] = v_expected
        return venue_expected_dates

    @staticmethod
    def _shards_expected_for_chain(
        chain_df: pd.DataFrame,
        chain_venues: list[str],
        venue_expected_dates: dict[str, set[str]],
    ) -> int:
        """Sum ``expected_dates * distinct_leaves`` over the chain's venues. Leaf axis is
        ``(data_type, instrument_id)`` when present, else just ``data_type`` (bundled shards like
        ``gas_fees``); empty leaf set falls back to one bundled instrument."""
        inst_col = "instrument_id" if "instrument_id" in chain_df.columns else None
        shards_expected = 0
        for v in chain_venues:
            v_dates_count = len(venue_expected_dates.get(v, set()))
            if v_dates_count == 0:
                continue
            v_mask = chain_df["venue"] == v
            if inst_col is not None:
                leaf_keys = chain_df.loc[v_mask, ["data_type", inst_col]].drop_duplicates()
            else:
                leaf_keys = chain_df.loc[v_mask, ["data_type"]].drop_duplicates()
            leaf_count = max(1, len(leaf_keys))
            shards_expected += v_dates_count * leaf_count
        return shards_expected

    def _build_chain_breakdown(
        self,
        filtered: pd.DataFrame,
        start_date: str,
        end_date: str,
        venue_mapping: VenueMapping,
    ) -> dict[str, object]:
        """Per-chain breakdown for DeFi data (v4 chain column): chain -> {venues, completion_pct,
        dates_found/expected, shards_found/expected}. Headline ratio is ``shards_found /
        shards_expected`` per the codex DeFi shard atom ``(chain, venue, data_type,
        instrument_id, day)``; the pre-2026-05-07 date-only math (which collapsed the
        within-day protocol x data_type x instrument fan-out) is kept as ``dates_found/expected``
        for existing deployment-ui consumers — new consumers use the shard fields.
        """
        if "chain" not in filtered.columns:
            return {}

        chains = sorted(c for c in filtered["chain"].unique() if c)  # pyright: ignore[reportAny]
        if not chains:
            return {}

        chain_dict: dict[str, object] = {}

        # ``capture_status`` is v5+; older manifests omit it. When
        # present, only ``captured`` rows count toward the numerator.
        has_capture_status = "capture_status" in filtered.columns

        for chain in chains:  # pyright: ignore[reportAny]
            chain_df = filtered[filtered["chain"] == chain]  # pyright: ignore[reportUnknownVariableType,reportAny]
            chain_dict[chain] = self._build_single_chain_entry(
                chain_df,  # pyright: ignore[reportUnknownArgumentType]
                start_date,
                end_date,
                venue_mapping,
                has_capture_status,
            )

        return chain_dict

    def _build_single_chain_entry(
        self,
        chain_df: pd.DataFrame,
        start_date: str,
        end_date: str,
        venue_mapping: VenueMapping,
        has_capture_status: bool,
    ) -> dict[str, object]:
        """One chain's shard/date coverage entry, extracted from ``_build_chain_breakdown``."""
        chain_dates = {str(d) for d in chain_df["date"].unique()}  # pyright: ignore[reportUnknownArgumentType,reportAny,reportUnknownVariableType,reportUnknownMemberType]
        chain_venues, chain_expected_dates, venue_expected_dates = self._chain_venues_and_expected_dates(
            chain_df,  # pyright: ignore[reportUnknownArgumentType]
            start_date,
            end_date,
            venue_mapping,
        )

        dates_expected = len(chain_expected_dates) if chain_expected_dates else 1
        dates_found = len(chain_dates & chain_expected_dates) if chain_expected_dates else len(chain_dates)

        shards_found = self._shards_found_for_chain(chain_df, has_capture_status)  # pyright: ignore[reportUnknownArgumentType]
        shards_expected = self._shards_expected_for_chain(chain_df, chain_venues, venue_expected_dates)  # pyright: ignore[reportUnknownArgumentType]
        if shards_expected == 0:
            shards_expected = max(1, shards_found)

        return {
            "shards_found": shards_found,
            "shards_expected": shards_expected,
            "completion_pct": min(round(shards_found / max(1, shards_expected) * 100, 2), 100.0),
            "dates_found": dates_found,
            "dates_expected": dates_expected,
            "venues": chain_venues,
            "venue_count": len(chain_venues),  # pyright: ignore[reportUnknownArgumentType]
        }

    def _chain_venues_and_expected_dates(
        self,
        chain_df: pd.DataFrame,
        start_date: str,
        end_date: str,
        venue_mapping: VenueMapping,
    ) -> tuple[list[str], set[str], dict[str, set[str]]]:
        """Real (non-ghost) venues for a chain, plus their union'd + per-venue expected dates."""
        chain_venues = [
            v  # pyright: ignore[reportAny]
            for v in (sorted(chain_df["venue"].unique()) if not chain_df.empty else [])  # pyright: ignore[reportUnknownMemberType,reportUnknownArgumentType,reportUnknownVariableType,reportAny]
            if v not in ALL_DEFI_GHOST_VENUES
            and str(v).split("-", 1)[0] not in ALL_DEFI_GHOST_VENUES  # pyright: ignore[reportUnknownArgumentType,reportAny]
            and str(v) not in DEFI_NON_PROTOCOL_VENUE_PREFIXES  # pyright: ignore[reportUnknownArgumentType,reportAny]
        ]
        venue_expected_dates = self._venue_expected_dates_for_chain(
            chain_df, chain_venues, start_date, end_date, venue_mapping
        )
        chain_expected_dates: set[str] = set()
        for v_expected in venue_expected_dates.values():
            chain_expected_dates |= v_expected
        return chain_venues, chain_expected_dates, venue_expected_dates

    @staticmethod
    def _shards_found_for_chain(chain_df: pd.DataFrame, has_capture_status: bool) -> int:
        """Distinct shard-atom count for a chain (not a raw row count — de-duplicates on the
        shard-atom axes present, since pipeline_mode can carry multiple rows per atom)."""
        captured_chain_df = chain_df[chain_df["capture_status"] == "captured"] if has_capture_status else chain_df
        _shard_atom_cols = [
            c for c in ("venue", "data_type", "instrument_id", "date") if c in captured_chain_df.columns
        ]
        return (
            len(captured_chain_df.drop_duplicates(subset=_shard_atom_cols))
            if _shard_atom_cols
            else len(captured_chain_df)
        )

    def _build_feature_group_breakdown(
        self,
        filtered: pd.DataFrame,
        start_date: str,
        end_date: str,
    ) -> dict[str, object]:
        """Per-feature_group breakdown (v4 feature_group column), with optional timeframe sub-level."""
        if "feature_group" not in filtered.columns:
            return {}

        groups = sorted(g for g in filtered["feature_group"].unique() if g)  # pyright: ignore[reportAny]
        if not groups:
            return {}

        fg_dict: dict[str, object] = {}

        for fg in groups:  # pyright: ignore[reportAny]
            fg_df = filtered[filtered["feature_group"] == fg]  # pyright: ignore[reportUnknownVariableType,reportAny]
            fg_dict[fg] = self._build_single_feature_group_entry(fg_df, start_date, end_date)  # pyright: ignore[reportUnknownArgumentType]

        return fg_dict

    @staticmethod
    def _build_single_feature_group_entry(
        fg_df: pd.DataFrame,
        start_date: str,
        end_date: str,
    ) -> dict[str, object]:
        """One feature_group's stats entry, with an optional timeframe sub-breakdown."""
        fg_dates = {str(d) for d in fg_df["date"].unique()}  # pyright: ignore[reportUnknownArgumentType,reportUnknownVariableType,reportUnknownMemberType,reportAny]
        found = len(fg_dates)

        # Expected = dates from first data appearance to end_date (not full query range)
        fg_start = min(fg_dates) if fg_dates else start_date
        fg_eff_start = max(start_date, fg_start)
        fg_expected = len(pd.date_range(fg_eff_start, end_date, freq="D"))

        entry: dict[str, object] = {
            "dates_found": found,
            "dates_expected": fg_expected,
            "completion_pct": min(round(found / max(1, fg_expected) * 100, 2), 100.0),
        }

        has_tf = "timeframe" in fg_df.columns and fg_df["timeframe"].str.len().sum() > 0  # pyright: ignore[reportUnknownMemberType,reportAttributeAccessIssue,reportUnknownVariableType]
        if has_tf:
            timeframes: dict[str, object] = {}
            for tf in sorted(fg_df["timeframe"].unique()):  # pyright: ignore[reportUnknownVariableType,reportUnknownMemberType,reportUnknownArgumentType,reportAny]
                if not tf:
                    continue
                tf_mask = fg_df["timeframe"] == tf  # pyright: ignore[reportUnknownVariableType,reportAny]
                tf_dates = {str(d) for d in fg_df.loc[tf_mask, "date"].unique()}  # pyright: ignore[reportUnknownArgumentType,reportUnknownVariableType,reportUnknownMemberType,reportAttributeAccessIssue]
                tf_start = min(tf_dates) if tf_dates else fg_eff_start
                tf_eff_start = max(start_date, tf_start)
                tf_expected = len(pd.date_range(tf_eff_start, end_date, freq="D"))
                timeframes[tf] = {
                    "dates_found": len(tf_dates),
                    "dates_expected": tf_expected,
                    "completion_pct": min(round(len(tf_dates) / max(1, tf_expected) * 100, 2), 100.0),
                }
            if timeframes:
                entry["timeframes"] = timeframes

        return entry

    def _build_underlying_grouping(
        self,
        filtered: pd.DataFrame,
        start_date: str,
        end_date: str,
        venue_mapping: VenueMapping,
    ) -> dict[str, object]:
        """Top-level per-underlying breakdown across all venues (BTC -> all BTC instruments, etc.)
        using the manifest's ``underlying`` column, falling back to ``ensure_underlying_column``."""
        df = ensure_underlying_column(filtered)

        has_underlying = (
            "underlying" in df.columns and not df.empty and df["underlying"].astype(str).str.len().sum() > 0
        )
        if not has_underlying:
            return {}

        ul_dict: dict[str, object] = {}
        has_venue = "venue" in df.columns
        has_itype = "instrument_type" in df.columns

        # Per-(venue, eff_start) expected-dates cache — venues repeat across
        # thousands of underlyings, so this collapses what was an O(N²) scan
        # (the >400s operator-beta drilldown hang) to one groupby pass.
        vexp_cache: dict[tuple[str, str], set[str]] = {}

        for ul_raw, ul_df in df.groupby("underlying", sort=False):  # pyright: ignore[reportUnknownVariableType,reportUnknownMemberType]
            ul = str(ul_raw)
            if not ul.strip():
                continue
            ul_dict[ul] = self._build_single_underlying_grouping_entry(
                ul_df,  # pyright: ignore[reportUnknownArgumentType]
                start_date,
                end_date,
                venue_mapping,
                has_venue,
                has_itype,
                vexp_cache,
            )

        return ul_dict

    @staticmethod
    def _build_single_underlying_grouping_entry(
        ul_df: pd.DataFrame,
        start_date: str,
        end_date: str,
        venue_mapping: VenueMapping,
        has_venue: bool,
        has_itype: bool,
        vexp_cache: dict[tuple[str, str], set[str]],
    ) -> dict[str, object]:
        """One underlying's cross-venue stats entry, extracted from ``_build_underlying_grouping``."""
        ul_dates = {str(d) for d in ul_df["date"].unique()}  # pyright: ignore[reportUnknownArgumentType,reportUnknownVariableType,reportUnknownMemberType,reportAny]

        ul_venues = (
            sorted(str(v) for v in ul_df["venue"].unique() if v and str(v).strip())  # pyright: ignore[reportUnknownArgumentType,reportUnknownVariableType,reportUnknownMemberType,reportAny]
            if has_venue
            else []
        )
        ul_itypes = (
            sorted(str(it) for it in ul_df["instrument_type"].unique() if it and str(it).strip())  # pyright: ignore[reportUnknownArgumentType,reportUnknownVariableType,reportUnknownMemberType,reportAny]
            if has_itype
            else []
        )

        ul_expected_dates = DomainBreakdownsMixin._underlying_expected_dates(
            ul_df, ul_venues, start_date, end_date, venue_mapping, vexp_cache
        )
        expected = len(ul_expected_dates)
        found = len(ul_dates & ul_expected_dates)

        return {
            "dates_found": found,
            "dates_expected": expected,
            "completion_pct": min(round(found / max(1, expected) * 100, 2), 100.0),
            "venues": ul_venues,
            "venue_count": len(ul_venues),
            "instrument_types": ul_itypes,
        }

    @staticmethod
    def _underlying_expected_dates(
        ul_df: pd.DataFrame,
        ul_venues: list[str],
        start_date: str,
        end_date: str,
        venue_mapping: VenueMapping,
        vexp_cache: dict[tuple[str, str], set[str]],
    ) -> set[str]:
        """Union of per-venue expected dates for venues carrying this underlying."""
        ul_expected_dates: set[str] = set()
        for v in ul_venues:
            vs = venue_mapping.get_venue_start_date(v)  # pyright: ignore[reportAny]
            if not vs:
                v_dates = {str(d) for d in ul_df.loc[ul_df["venue"] == v, "date"].unique()}  # pyright: ignore[reportUnknownArgumentType,reportUnknownVariableType,reportUnknownMemberType,reportAny]
                vs = min(v_dates) if v_dates else start_date
            eff_start = max(start_date, vs) if vs else start_date
            key = (v, eff_start)
            v_expected = vexp_cache.get(key)
            if v_expected is None:
                v_expected = set(venue_mapping.get_expected_trading_dates(v, eff_start, end_date))
                if not v_expected:
                    v_expected = {d.strftime("%Y-%m-%d") for d in pd.date_range(eff_start, end_date, freq="D")}
                vexp_cache[key] = v_expected
            ul_expected_dates |= v_expected
        if not ul_expected_dates:
            ul_expected_dates = {d.strftime("%Y-%m-%d") for d in pd.date_range(start_date, end_date, freq="D")}
        return ul_expected_dates

    def _build_data_type_grouping(
        self,
        filtered: pd.DataFrame,
        start_date: str,
        end_date: str,
        cat: str,
        service: str = "instruments-service",
    ) -> tuple[dict[str, object], int, int]:
        """Group manifest data by data_type when venues are empty (sports instruments pattern).

        For SPORTS, uses a fixture-based model: unit is the fixture, not the
        date; FIXTURES is found/expected from its own manifest counts, other
        entities are denominated against the total FIXTURES ``instrument_count``.
        """
        is_sports = cat.upper() == "SPORTS"
        fixtures_by_league, total_fixture_count = self._precompute_fixture_counts_by_league(filtered, is_sports)
        dt_venues: dict[str, object] = {}
        dt_found_total = 0
        dt_expected_total = 0
        # SPORTS: always include every known data_type from the active SSOT meta (so the UI shows
        # 0/N for not-yet-captured types, dropping residual rows for removed entities); non-SPORTS
        # falls back to manifest vals. Service dispatch: features-sports-service has its own
        # derived-product SSOT meta rather than instruments-service's raw-source one.
        manifest_dt_vals: set[str] = set()
        if "data_type" in filtered.columns:
            manifest_dt_vals = {str(v) for v in filtered["data_type"].unique() if v and str(v).strip()}  # pyright: ignore[reportAny]
        is_fss = service == "features-sports-service"
        active_meta: dict[str, dict[str, object]] = FEATURES_SPORTS_DATA_TYPE_META if is_fss else SPORTS_DATA_TYPE_META
        sports_ssot_vals: set[str] = set(active_meta.keys()) if is_sports else set()
        all_dt_vals: set[str] = sports_ssot_vals if is_sports else manifest_dt_vals
        for dt_val in sorted(all_dt_vals):
            if not dt_val or not str(dt_val).strip():
                continue
            dt_entry = self._build_single_data_type_grouping_entry(
                filtered,
                dt_val,
                is_sports,
                active_meta,
                fixtures_by_league,
                total_fixture_count,
                start_date,
                end_date,
            )
            dt_venues[str(dt_val)] = dt_entry
            dt_found_total += int(dt_entry["dates_found"])  # pyright: ignore[reportArgumentType]
            dt_expected_total += int(dt_entry["dates_expected"])  # pyright: ignore[reportArgumentType]
        return dt_venues, dt_found_total, dt_expected_total

    @staticmethod
    def _precompute_fixture_counts_by_league(filtered: pd.DataFrame, is_sports: bool) -> tuple[dict[str, int], int]:
        """Fixture counts per league from FIXTURES rows — the ground-truth denominator for other sports entities."""
        fixtures_by_league: dict[str, int] = {}
        if not is_sports or "instrument_count" not in filtered.columns:
            return fixtures_by_league, 0
        fix_rows = filtered[(filtered["data_type"] == "FIXTURES") & (filtered["league_id"].fillna("").str.len() > 0)]
        if fix_rows.empty:
            return fixtures_by_league, 0
        for lid in fix_rows["league_id"].unique():  # pyright: ignore[reportAny]
            if lid:
                lid_count = int(fix_rows.loc[fix_rows["league_id"] == lid, "instrument_count"].sum())  # pyright: ignore[reportUnknownMemberType,reportUnknownArgumentType,reportAttributeAccessIssue]
                fixtures_by_league[str(lid)] = lid_count  # pyright: ignore[reportAny]
        return fixtures_by_league, sum(fixtures_by_league.values())

    def _build_single_data_type_grouping_entry(
        self,
        filtered: pd.DataFrame,
        dt_val: str,
        is_sports: bool,
        active_meta: dict[str, dict[str, object]],
        fixtures_by_league: dict[str, int],
        total_fixture_count: int,
        start_date: str,
        end_date: str,
    ) -> dict[str, object]:
        """One data_type's stats entry, extracted from ``_build_data_type_grouping``."""
        if "data_type" in filtered.columns:
            dt_mask = filtered["data_type"] == dt_val
        else:
            dt_mask = pd.Series([False] * len(filtered), index=filtered.index)
        dt_df = filtered[dt_mask]
        dt_name = str(dt_val).upper()

        if is_sports and dt_name in active_meta:
            return self._build_sports_entity_entry(
                dt_df,
                dt_name,
                filtered,
                fixtures_by_league,
                total_fixture_count,
                start_date,
                end_date,
            )

        # Non-sports: keep existing date-based logic
        dt_dates = {str(d) for d in dt_df["date"].unique()}  # pyright: ignore[reportAny]
        dt_start = min(dt_dates) if dt_dates else start_date  # pyright: ignore[reportUnknownVariableType,reportUnknownArgumentType]
        dt_eff_start = max(start_date, dt_start)
        dt_found = len(dt_dates)  # pyright: ignore[reportUnknownArgumentType]
        dt_expected = len(pd.date_range(dt_eff_start, end_date, freq="D"))
        return {
            "dates_found": dt_found,
            "dates_expected": dt_expected,
            "dates_expected_venue": dt_expected,
            "dates_missing": dt_expected - dt_found,
            "completion_pct": min(round(dt_found / max(1, dt_expected) * 100, 2), 100.0),
        }

    @staticmethod
    def _clamp_fixtures_to_entity_start(
        entity_name: str,
        full_filtered: pd.DataFrame,
        fixtures_by_league: dict[str, int],
        total_fixture_count: int,
    ) -> tuple[dict[str, int], int]:
        """Recompute fixture counts excluding rows before the entity's provider-specific start date
        (UAC ``SPORTS_ENTITY_START_DATES``). Returns the (possibly clamped) counts."""
        entity_start = get_sports_entity_start_date(entity_name)
        if not entity_start or "date" not in full_filtered.columns:
            return fixtures_by_league, total_fixture_count

        fix_rows = full_filtered[(full_filtered["data_type"] == "FIXTURES") & (full_filtered["date"] >= entity_start)]
        if fix_rows.empty:
            return {}, 0
        if "league_id" not in fix_rows.columns:
            return fixtures_by_league, total_fixture_count

        clamped: dict[str, int] = {}
        for lid in fix_rows["league_id"].unique():  # pyright: ignore[reportAny]
            if lid:  # pyright: ignore[reportAny]
                clamped[str(lid)] = int(fix_rows.loc[fix_rows["league_id"] == lid, "instrument_count"].sum())  # pyright: ignore[reportAny,reportUnknownMemberType,reportUnknownArgumentType,reportAttributeAccessIssue]
        return clamped, sum(clamped.values())

    def _build_sports_entity_entry(
        self,
        dt_df: pd.DataFrame,
        entity_name: str,
        full_filtered: pd.DataFrame,
        fixtures_by_league: dict[str, int],
        total_fixture_count: int,
        start_date: str,
        end_date: str,
    ) -> dict[str, object]:
        """Honest-coverage entry for a sports data_type (SSOT: sports-data-source-coverage-matrix.md).

        Preferred path — ``sports_honest_coverage`` resolves expected leagues/dates via
        ``SPORTS_DATA_TYPE_META`` + UAC helpers; both sides are shard counts, no cross-entity
        row-count comparison. Legacy path (entity not in the SSOT map): pre-2026-04-20
        fixture-row-count model — extend ``SPORTS_DATA_TYPE_META`` for new SPORTS data_types.
        """
        honest = sports_honest_coverage(full_filtered, entity_name, start_date, end_date)
        if honest is not None:
            return self._honest_sports_entity_entry(honest)
        return self._build_legacy_sports_entity_entry(
            dt_df, entity_name, full_filtered, fixtures_by_league, total_fixture_count, start_date, end_date
        )

    @staticmethod
    def _honest_sports_entity_entry(honest: dict[str, object]) -> dict[str, object]:
        """Map ``sports_honest_coverage``'s return dict to the UI entry shape."""
        expected_shards = int(cast(int, honest["expected_shards"]))
        found_shards = int(cast(int, honest["found_shards"]))
        completion = round(found_shards / max(1, expected_shards) * 100, 2)
        dt_entry: dict[str, object] = {
            "found_shards": found_shards,
            "expected_shards": expected_shards,
            "missing_shards": max(0, expected_shards - found_shards),
            "completion_pct": min(completion, 100.0),
            "unit": str(honest["unit"]),
            "axis": str(honest["axis"]),
            "source": str(honest["source"]),
            "expected_leagues": honest["expected_leagues"],
            # Legacy aliases — kept for UI/tests that haven't migrated to found_shards/expected_shards.
            "dates_found": found_shards,
            "dates_expected": expected_shards,
            "dates_expected_venue": expected_shards,
            "dates_missing": max(0, expected_shards - found_shards),
        }
        per_league = honest["per_league"]
        if per_league:
            dt_entry["leagues"] = per_league
        return dt_entry

    def _build_legacy_sports_entity_entry(
        self,
        dt_df: pd.DataFrame,
        entity_name: str,
        full_filtered: pd.DataFrame,
        fixtures_by_league: dict[str, int],
        total_fixture_count: int,
        start_date: str,
        end_date: str,
    ) -> dict[str, object]:
        """Pre-2026-04-20 fixture-row-count entity entry, for entities not yet in the SSOT map."""
        is_fixtures = entity_name == "FIXTURES"
        has_league = "league_id" in dt_df.columns
        entity_fixture_count = int(dt_df["instrument_count"].sum()) if not dt_df.empty else 0  # pyright: ignore[reportAny]
        _entity_coverage = get_entity_league_coverage(entity_name)
        if is_fixtures:
            eff_fixtures_by_league, eff_total_fixture_count = fixtures_by_league, total_fixture_count
        else:
            eff_fixtures_by_league, eff_total_fixture_count = self._clamp_fixtures_to_entity_start(
                entity_name, full_filtered, fixtures_by_league, total_fixture_count
            )
        dt_found, dt_expected = self._legacy_sports_found_expected(
            entity_fixture_count, is_fixtures, _entity_coverage, eff_fixtures_by_league, eff_total_fixture_count
        )
        dt_entry: dict[str, object] = {
            "dates_found": dt_found,
            "dates_expected": dt_expected,
            "dates_expected_venue": dt_expected,
            "dates_missing": max(0, dt_expected - dt_found),
            "completion_pct": min(round(dt_found / max(1, dt_expected) * 100, 2), 100.0),
            "unit": "fixtures",
        }
        if has_league:
            has_league_data = dt_df["league_id"].fillna("").str.len().sum() > 0
            if has_league_data:
                league_breakdown = self._build_league_breakdown(
                    dt_df,
                    start_date,
                    end_date,
                    fixture_counts_by_league=eff_fixtures_by_league,
                    is_fixtures_entity=is_fixtures,
                    entity_coverage=_entity_coverage,
                    full_manifest=full_filtered,
                )
                if league_breakdown:
                    dt_entry["leagues"] = league_breakdown
        return dt_entry

    @staticmethod
    def _legacy_sports_found_expected(
        entity_fixture_count: int,
        is_fixtures: bool,
        entity_coverage: frozenset[str] | None,
        eff_fixtures_by_league: dict[str, int],
        eff_total_fixture_count: int,
    ) -> tuple[int, int]:
        """(found, expected) for the legacy fixture-row-count model."""
        if is_fixtures:
            return entity_fixture_count, entity_fixture_count
        if entity_coverage is not None and eff_fixtures_by_league:
            return entity_fixture_count, sum(eff_fixtures_by_league.get(lid, 0) for lid in entity_coverage)
        return entity_fixture_count, (eff_total_fixture_count if eff_total_fixture_count > 0 else entity_fixture_count)

    def _build_v4_sub_dimensions(
        self,
        filtered: pd.DataFrame,
        service: str,
        cat: str,
        start_date: str,
        end_date: str,
        venue_mapping: VenueMapping,
    ) -> dict[str, object]:
        """Build v4 manifest sub-dimension breakdowns."""
        extras: dict[str, object] = {}
        extras.update(self._defi_specific_v4_extras(filtered, service, cat, start_date, end_date, venue_mapping))
        extras.update(self._cross_category_v4_extras(filtered, cat, start_date, end_date, venue_mapping))
        return extras

    def _defi_specific_v4_extras(
        self,
        filtered: pd.DataFrame,
        service: str,
        cat: str,
        start_date: str,
        end_date: str,
        venue_mapping: VenueMapping,
    ) -> dict[str, object]:
        """DeFi-only v4 sub-dimensions: sub-dimension + chain breakdowns."""
        extras: dict[str, object] = {}

        if "_defi_source" in filtered.columns and service == "market-tick-data-service" and cat.lower() == "defi":
            defi_sub_dims = self._build_defi_sub_dimension_breakdown(filtered, start_date, end_date)
            if defi_sub_dims:
                extras["defi_sub_dimensions"] = defi_sub_dims

        # Chain breakdown for DeFi ONLY. ``chain`` is a shard axis exclusively
        # for defi (UAC ``SHARD_AXIS_MATRIX`` — cefi/tradfi key on ``venue``
        # alone); gating on ``cat==defi`` keeps CeFi CLOB-perp venues that
        # carry DeFi-style ``{PROTOCOL}-{CHAIN}`` names (e.g. PACIFICA-SOLANA)
        # from manufacturing chain sub-rows from residual ``chain=`` values.
        has_chain_data = (
            cat.lower() == "defi"
            and "chain" in filtered.columns
            and not filtered.empty
            and filtered["chain"].str.len().sum() > 0
        )
        if has_chain_data:
            chains_dict = self._build_chain_breakdown(filtered, start_date, end_date, venue_mapping)
            if chains_dict:
                extras["chains"] = chains_dict

        return extras

    def _cross_category_v4_extras(
        self,
        filtered: pd.DataFrame,
        cat: str,
        start_date: str,
        end_date: str,
        venue_mapping: VenueMapping,
    ) -> dict[str, object]:
        """Cross-category v4 sub-dimensions: feature_group + underlying breakdowns."""
        extras: dict[str, object] = {}

        has_fg_data = (
            "feature_group" in filtered.columns and not filtered.empty and filtered["feature_group"].str.len().sum() > 0
        )
        if has_fg_data:
            fg_dict = self._build_feature_group_breakdown(filtered, start_date, end_date)
            if fg_dict:
                extras["feature_groups"] = fg_dict

        # Underlying (base asset) grouping — top-level cross-venue view.
        # Applicable to CEFI, TRADFI, DEFI (instruments have a base asset,
        # e.g. BTC, ETH, ES); not applicable to SPORTS.
        if cat.upper() not in ("SPORTS",):
            ul_dict = self._build_underlying_grouping(filtered, start_date, end_date, venue_mapping)
            if ul_dict:
                extras["underlyings"] = ul_dict

        return extras

"""MTDS per-category coverage metadata + expected-dates helpers.

Split out of the 6,663-line ``data_status_service.py`` god-module
(codex ratchet plan 2026-06-10). The facade module re-exports every
public + legacy-underscore name, so callers keep importing from
``deployment_api.services.data_status_service``.
"""

import logging
from collections.abc import Callable
from functools import lru_cache
from typing import cast

import pandas as pd
from unified_api_contracts import (
    VenueMapping,
    is_per_instrument_shard_data_type,
)
from unified_api_contracts.registry import (
    EMPTY_OR_DEPRECATED_DEFI_VENUES,
    get_coverage_windows,
    get_lst_venue_genesis,
    is_in_tradfi_tick_window,
    venue_has_no_expected_defi_coverage,
)

import deployment_api.services.data_status_service as _dss

logger = logging.getLogger(__name__)

from deployment_api.services.data_status.instrument_coverage import (
    DEFAULT_PER_INSTRUMENT_SENTINEL_CAP,
    per_instrument_coverage,
)

# TradFi data types that are only expected within tick windows (Databento cost mgmt).
# Outside tick windows, only ohlcv_1m (and other non-tick types) are expected.
#
# **2026-05-05 narrowed:** ``trades`` REMOVED — we capture trades year-round
# on every CME futures parent symbol and IBIT/ETHA NASDAQ ETFs (≥99% of
# trading days), so clipping the trades denominator to the global
# TRADFI_TICK_DATA_WINDOWS (May 2023 + Jul 2024) was understating reality
# and inflating coverage_pct by silently dropping 1500+ days from the
# expected denominator.
#
# ``tbbo`` retained — kept here for the legacy global-clip path; the new
# per-(venue, data_type) registry below (UAC ``VENUE_DATA_TYPE_COVERAGE_WINDOWS``)
# is the preferred mechanism and overrides this set when present.
TRADFI_TICK_ONLY_DATA_TYPES: frozenset[str] = frozenset({"tbbo"})

MTDS_CATEGORY_META: dict[str, dict[str, object]] = {
    "CEFI": {
        "venue_accessor": "all_cefi_venues",
        "axis": "per_venue_per_data_type_daily",
        "tradfi_tick_gate": False,
        "record_empty_expected": True,
        "unit": "shard_days",
    },
    "TRADFI": {
        # FLAG-4 (verified, slot-6 2026-06-03): the tradfi honest-coverage denominator
        # is ALREADY complete — `all_databento_venues` despite its name contains the full
        # 6-venue tradfi universe [CME, CBOE, NASDAQ, NYSE, ICE, FX] incl. CBOE→Barchart(VIX)
        # + FX→Yahoo (per UAC venue_mapping venue→source map). No undercount. UAC adds a
        # correctly-named `all_tradfi_venues` alias; SWITCH this string to it AFTER UAC version
        # cascades into deployment-api's pinned dep (else runtime AttributeError vs the older
        # installed UAC). Tracked: tradfi_manifest_canonicalisation_2026_06_01 FLAG-4 follow-up.
        "venue_accessor": "all_databento_venues",
        "axis": "per_venue_per_data_type_daily",
        "tradfi_tick_gate": True,
        "record_empty_expected": True,
        "unit": "shard_days",
    },
    "DEFI": {
        # DEFI uses the same per-venue x per-data_type x daily axis with an
        # additional ``chain`` dimension. Today UAC ``all_defi_venues`` only
        # lists ``PROTOCOL-ETHEREUM``; Arbitrum / Base / Optimism expansion
        # is Phase 6d follow-up once adapters start writing those rows.
        "venue_accessor": "all_defi_venues",
        "axis": "per_venue_per_data_type_per_chain_daily",
        "tradfi_tick_gate": False,
        "record_empty_expected": True,
        "unit": "shard_days",
    },
    "PREDICTION": {
        # PREDICTION venues declare only ``trades`` by design. ``book_snapshot_5``
        # was intentionally removed 2026-04-19 because neither adapter captures
        # book snapshots. ``prediction_market_metadata`` lives in the
        # instrument_availability index, not MTDS's market_tick_data.
        "venue_accessor": "all_prediction_venues",
        "axis": "per_venue_per_data_type_daily",
        "tradfi_tick_gate": False,
        "record_empty_expected": True,
        "unit": "shard_days",
    },
    "SPORTS": {
        # MTDS SPORTS = bookmaker odds per league x per fixture-date. The
        # honest-coverage helper below currently returns ``None`` so the
        # existing instruments-service SPORTS path handles it. The category
        # still needs to be in this map so the aggregator knows which UAC
        # venue list to iterate (there is no ``all_bookmaker_venues`` yet —
        # Phase 6d adds ``get_expected_bookmakers`` to UAC).
        "venue_accessor": "",  # bookmakers resolved via sports accessor (Phase 6d)
        "axis": "per_league_per_bookmaker_per_fixture_date",
        "tradfi_tick_gate": False,
        "record_empty_expected": False,  # bookmaker-dark day = attempted_failed
        "unit": "bookmaker_fixture_dates",
    },
}


# Canonical per-PREDICTION-data_type metadata — mirrors UAC SchemaContracts
# registered in ``unified_api_contracts.internal.schemas._sports_prediction_contracts``
# (commit ``c7642f3`` registered ``book_snapshot`` / ``market_metadata`` / ``fills``
# alongside the pre-existing ``trades`` contract on the
# ``(prediction, prediction_market, *)`` triple).
#
# All four data_types share the same 6-dim shard
# (data_source x venue x chain x market_category x underlying x market_type x
# resolution_period) and pivot on ``condition_id``. UAC ``VENUE_DATA_TYPE_CAPABILITIES``
# only declares ``trades`` for POLYMARKET / KALSHI today — so the manifest
# enumeration loop ``mtds_honest_coverage_for_venue`` would surface only
# ``trades`` as an expected row, hiding the SSOT gap for the three new
# contracts. Unioning ``PREDICTION_DATA_TYPE_META.keys()`` into ``expected_dts``
# for PREDICTION venues makes ``book_snapshot`` / ``market_metadata`` / ``fills``
# appear as expected rows (with 0 captured, 0% completion) until adapters land.
#
# **expected_count_per_day = "indeterminate"** for the three new types: per the
# Follow-up B prompt + plan SSOT, none of these has a tractable per-(venue,
# date) cardinality bound today. ``book_snapshot`` cardinality depends on
# active condition_ids x snapshot frequency; ``market_metadata`` is one row per
# active market per day (finite but only enumerable post-hoc via Polymarket
# Gamma API); ``fills`` mirrors ``trades`` open-market dynamics. UI renders the
# captured count without an arbitrary denominator (mirrors the SFI_PROGRESSIVE_STATS
# pattern in ``SPORTS_DATA_TYPE_META`` where the per-day count is unbounded
# and only the per-(league, fixture-date) shard count is tracked). ``trades``
# keeps the existing per-venue daily denominator from
# ``mtds_expected_dates_for_venue_dt``.
#
# Adding a new PREDICTION data_type: register the SchemaContract in UAC
# ``_sports_prediction_contracts``, then add the corresponding entry here in
# the same commit (codex-first SSOT rule). SSOT for PREDICTION coverage:
# ``codex/02-data/data-status-and-availability.md`` §6.
PREDICTION_DATA_TYPE_META: dict[str, dict[str, object]] = {
    # Polymarket Data API + Kalshi trades stream — enumerated per-condition_id
    # via the existing per-venue-daily denominator (UAC declares this on the
    # venue today).
    "trades": {
        "schema_contract": "PREDICTION_PREDICTION_MARKET_TRADES",
        "shard_dim": (
            "data_source",
            "venue",
            "chain",
            "market_category",
            "underlying",
            "market_type",
            "resolution_period",
        ),
        "key": "condition_id",
        "expected_count_per_day": "per_venue_daily",
        "unit": "shard_days",
    },
    # Polymarket CLOB API (per-condition_id snapshot stream). Cardinality
    # bound is the active-market count, not enumerable per-day pre-hoc.
    "book_snapshot": {
        "schema_contract": "PREDICTION_PREDICTION_MARKET_BOOK_SNAPSHOT",
        "shard_dim": (
            "data_source",
            "venue",
            "chain",
            "market_category",
            "underlying",
            "market_type",
            "resolution_period",
        ),
        "key": "condition_id",
        "expected_count_per_day": "indeterminate",
        "unit": "shard_days",
    },
    # Polymarket Gamma API — per-day-per-venue catalogue. Finite but only
    # enumerable post-hoc; we mark as indeterminate so the UI shows the
    # captured count without a synthetic denominator.
    "market_metadata": {
        "schema_contract": "PREDICTION_PREDICTION_MARKET_METADATA",
        "shard_dim": (
            "data_source",
            "venue",
            "chain",
            "market_category",
            "underlying",
            "market_type",
            "resolution_period",
        ),
        "key": "condition_id",
        "expected_count_per_day": "indeterminate",
        "unit": "shard_days",
    },
    # Polymarket Data API fills mirror — same 6-dim shard as trades but
    # cardinality depends on user-account-level fill events, not enumerable
    # at venue granularity.
    "fills": {
        "schema_contract": "PREDICTION_PREDICTION_MARKET_FILLS",
        "shard_dim": (
            "data_source",
            "venue",
            "chain",
            "market_category",
            "underlying",
            "market_type",
            "resolution_period",
        ),
        "key": "condition_id",
        "expected_count_per_day": "indeterminate",
        "unit": "shard_days",
    },
}


def is_mtds_honest_coverage_target(service: str, category: str) -> bool:
    """True iff ``(service, category)`` should run the MTDS honest-coverage
    override. Excludes SPORTS (bookmaker axis is Phase 6d)."""
    if service != "market-tick-data-service":
        return False
    cat_key = category.upper()
    return cat_key in MTDS_CATEGORY_META and cat_key != "SPORTS"


# DEFI data_type canonicalisation maps. Sub-dim buckets write the hyphenated
# form (``lending-indices``, ``dex-swaps``) but UAC
# ``VENUE_DATA_TYPE_CAPABILITIES`` declares the underscore form
# (``lending_indices``, ``dex_pool_swaps``). The honest-coverage per-(venue, dt)
# filter needs the rows canonicalised before matching. Module-level
# constants per ruff N806 — they're configuration, not per-call state.
DEFI_DATA_TYPE_ALIASES: dict[str, str] = {
    "dex-swaps": "dex_pool_swaps",
    "dex-pools": "dex_pool_state",
    "lending-indices": "lending_indices",
    "lst-rates": "lst_rates",
    "oracle-prices": "oracle_prices",
    "perp-funding": "perp_funding",
    "gas-fees": "gas_fees",
    "eigenlayer-rewards": "eigenlayer_rewards",
    # Phase 2 event-typed handlers (defi_data_types_completeness_2026_04_24)
    "liquidation-events": "liquidation_events",
    "flash-loan-events": "flash_loan_events",
    "staking-yields": "staking_yields",
    "position-data": "position_data",
    "token-transfers": "token_transfers",
    "bridge-events": "bridge_events",
    "governance-events": "governance_events",
    "mev-events": "mev_events",
}
DEFI_SOURCE_TO_DATA_TYPE: dict[str, str] = {
    "dex-swaps": "dex_pool_swaps",
    "dex-pools": "dex_pool_state",
    "lending-indices": "lending_indices",
    "lst-rates": "lst_rates",
    "oracle-prices": "oracle_prices",
    "liquidations": "liquidations",
    "perp-funding": "perp_funding",
    "gas-fees": "gas_fees",
    "eigenlayer-rewards": "eigenlayer_rewards",
    # Phase 2 event-typed handlers
    "liquidation-events": "liquidation_events",
    "flash-loan-events": "flash_loan_events",
    "staking-yields": "staking_yields",
    "position-data": "position_data",
    "token-transfers": "token_transfers",
    "bridge-events": "bridge_events",
    "governance-events": "governance_events",
    "mev-events": "mev_events",
    "evm-defi": "",
    "solana-defi": "",
    "": "",
}


def canonicalise_defi_data_types(filtered: pd.DataFrame) -> pd.DataFrame:
    """Normalise hyphenated DEFI ``data_type`` values to underscore form.

    Sub-dim buckets (``lending-indices``, ``dex-swaps``, ``dex-pools``,
    ``lst-rates``, ``oracle-prices``, ``perp-funding``) write hyphenated
    ``data_type`` values but UAC ``VENUE_DATA_TYPE_CAPABILITIES`` uses
    canonical underscore form (``lending_indices``, ``dex_pool_swaps``, …). Two
    transforms applied here, both safe to remove once the corresponding
    one-shot manifest migration runs (Plan B follow-up — currently no
    successor plan; data_type alias migration is the natural next step):

    * Case 1: infer ``data_type`` from ``_defi_source`` for blank rows.
    * Case 2: map hyphenated forms to canonical underscore form via
      ``DEFI_DATA_TYPE_ALIASES``.

    DeFi VENUE canonicalisation is no longer done here — UTL
    ``manifest_writer._coerce_row_key`` + ``ManifestWriter.add`` apply
    ``LEGACY_DEFI_VENUE_ALIASES`` at write time, and the 2026-05-07 MTDS
    DEFI migration script rewrote 411,620 historical rows in place
    (``market_tick_data_service/scripts/migrate_mtds_defi_legacy_venue_underscore.py``).
    Live re-probe across 11 DEFI buckets confirmed 0 residual legacy-
    underscore DeFi-venue rows. Per workspace rule "Manifest migration,
    NOT fallback", the venue-side fallback is gone.
    """
    if "data_type" not in filtered.columns:
        return filtered

    out = filtered.copy()

    # Case (1): infer from _defi_source for blank rows.
    if "_defi_source" in out.columns:
        blank_dt = out["data_type"].fillna("").astype(str).str.len() == 0  # pyright: ignore[reportUnknownMemberType]
        if blank_dt.any():
            inferred = out["_defi_source"].fillna("").astype(str).map(DEFI_SOURCE_TO_DATA_TYPE).fillna("")  # pyright: ignore[reportUnknownMemberType]
            out.loc[blank_dt, "data_type"] = inferred[blank_dt]
    # Case (2): map hyphenated DEFI data_types to canonical underscore form.
    out["data_type"] = out["data_type"].fillna("").astype(str).replace(DEFI_DATA_TYPE_ALIASES)  # pyright: ignore[reportUnknownMemberType]
    return out


def mtds_expected_venues(cat: str, venue_mapping: VenueMapping) -> list[str]:
    """Return UAC-declared venue list for an MTDS category.

    Reads ``MTDS_CATEGORY_META[cat]['venue_accessor']`` and resolves it on
    ``VenueMapping``. The PREDICTION accessor (``all_prediction_venues``)
    does not exist on VenueMapping today — we hardcode the pair
    ``["POLYMARKET", "KALSHI"]`` from the codex SSOT §1 as a fallback so the
    aggregator has a deterministic denominator regardless of UAC surface.
    Falls back to ``[]`` if the accessor is missing or empty — the caller
    then skips the MTDS honest-coverage path and keeps the legacy
    observed-only denominator.
    """
    meta = MTDS_CATEGORY_META.get(cat.upper())
    if meta is None:
        return []
    accessor = str(meta.get("venue_accessor") or "")
    if not accessor:
        return []
    # Accessor is either a property on VenueMapping (all_cefi_venues etc.)
    # or one of the missing PREDICTION fallbacks.
    if accessor == "all_prediction_venues":
        # No UAC accessor for prediction venues yet — codex SSOT §1 lists
        # POLYMARKET + KALSHI. Hardcoded fallback mirrors the matrix.
        return ["KALSHI", "POLYMARKET"]
    venues = getattr(venue_mapping, accessor, None)
    if venues is None:
        return []
    # Filter out deprecated ghost venue names (prefix before first "-")
    if EMPTY_OR_DEPRECATED_DEFI_VENUES:
        return [v for v in venues if str(v).split("-", 1)[0] not in EMPTY_OR_DEPRECATED_DEFI_VENUES]  # pyright: ignore[reportAny]
    return list(venues)  # pyright: ignore[reportAny]


# Process-shared VenueMapping for the lru_cache'd expected-dates helper.
# UAC venue/calendar/coverage data is process-immutable (read once on import),
# so a single shared instance is safe and avoids per-request VenueMapping
# instantiation cost when fanning out across ~30-50 venues x ~8 data_types.
_shared_venue_mapping_instance: VenueMapping | None = None


def shared_venue_mapping() -> VenueMapping:
    global _shared_venue_mapping_instance
    if _shared_venue_mapping_instance is None:
        _shared_venue_mapping_instance = _dss.VenueMapping()
    return _shared_venue_mapping_instance


@lru_cache(maxsize=512)
def mtds_expected_dates_cached(
    venue: str,
    data_type: str,
    category: str,
    window_start: str,
    window_end: str,
) -> frozenset[str]:
    """Process-level cache for :func:`mtds_expected_dates_for_venue_dt`.

    The expected-dates set is a pure function of UAC config (venue start
    dates, trading calendars, coverage windows, LST genesis dates) —
    process-immutable. Caching it avoids rebuilding the same trading-day
    calendar 8x per venue (once per data_type) on every data-status
    request. Cleared on ``/turbo/clear`` for defensive freshness.
    """
    venue_mapping = shared_venue_mapping()
    venue_start = venue_mapping.get_venue_start_date(venue) or window_start
    dt_start = _dss.get_venue_data_type_start_date(venue, data_type) or venue_start

    if category.upper() == "DEFI" and venue_has_no_expected_defi_coverage(venue):
        return frozenset()

    if data_type == "lst_rates":
        lst_genesis = get_lst_venue_genesis(venue)
        if lst_genesis is not None:
            dt_start = max(dt_start, lst_genesis)

    # P1-C: per-chain + per-protocol pre-launch clipping for DEFI venues.
    # Canonical DEFI venues are ``PROTOCOL-CHAIN`` (e.g. ``AAVE_V3-ARBITRUM``);
    # extract both halves and clip ``effective_start`` to whichever launch
    # date is later — the chain genesis (e.g. ARBITRUM 2021-08-31) OR the
    # protocol-on-chain deploy date (e.g. AAVE_V3 on Arbitrum 2022-03-16).
    # Without the protocol-launch clip, AAVE_V3-ARBITRUM expected dates
    # stretch back to ARBITRUM genesis and the leaf-shard denominator
    # inflates with ~6 months of always-empty days, producing the
    # "ARBITRUM 32/54" misleading panel headline (2026-05-07 incident).
    # Only applies when the venue parses cleanly as PROTOCOL-CHAIN AND
    # the chain is in the SSOT — unknown chains/protocols fall through
    # unchanged so a freshly-added pair doesn't break the panel before
    # its launch date is declared.
    chain_genesis = ""
    if category.upper() == "DEFI" and "-" in venue:
        _protocol, chain_suffix = venue.rsplit("-", 1)
        from unified_api_contracts.registry import (
            get_chain_genesis_date,
            get_protocol_launch_date,
        )

        chain_genesis = get_chain_genesis_date(chain_suffix) or ""
        protocol_launch = get_protocol_launch_date(chain_suffix, _protocol) or ""
        # max() of two ISO YYYY-MM-DD strings is the lexicographically-later
        # date, which is also the chronologically-later date for ISO format.
        chain_genesis = max(chain_genesis, protocol_launch)

    effective_start = max(window_start, venue_start, dt_start, chain_genesis)
    if effective_start > window_end:
        return frozenset()

    if category.upper() == "TRADFI":
        expected_list = venue_mapping.get_expected_trading_dates(venue, effective_start, window_end)
        per_venue_windows = get_coverage_windows(venue, data_type)
        if per_venue_windows:
            expected_list = [d for d in expected_list if any(s <= d <= e for s, e in per_venue_windows)]
        elif data_type in TRADFI_TICK_ONLY_DATA_TYPES:
            expected_list = [d for d in expected_list if is_in_tradfi_tick_window(d)]
        return frozenset(expected_list)

    expected_list = venue_mapping.get_expected_trading_dates(venue, effective_start, window_end)
    if expected_list:
        return frozenset(expected_list)
    return frozenset(d.strftime("%Y-%m-%d") for d in pd.date_range(effective_start, window_end, freq="D"))


def mtds_expected_dates_for_venue_dt(
    venue_mapping: VenueMapping,
    venue: str,
    data_type: str,
    category: str,
    window_start: str,
    window_end: str,
) -> set[str]:
    """Compute expected shard dates for an MTDS ``(venue, data_type)`` tuple.

    Mirrors codex SSOT §7 ("Aggregator algorithm v5 honest-coverage — MTDS"):

      effective_start = max(start_date, venue_start, dt_start)
      if category == "TRADFI":
          expected = get_expected_trading_dates(venue, effective_start, end)
          if dt in {"trades", "tbbo"}:
              expected = [d for d in expected if is_in_tradfi_tick_window(d)]
      else:
          expected = daily_grid(effective_start, end)

    Returns a set of ``YYYY-MM-DD`` strings. Empty set if venue_start is
    after ``window_end`` (pre-launch venue).

    Delegates to :func:`mtds_expected_dates_cached` — UAC config is
    process-immutable so caching by (venue, data_type, category, window) is
    safe; the ``venue_mapping`` arg is preserved for signature compatibility
    with existing callers and tests.
    """
    return set(mtds_expected_dates_cached(venue, data_type, category, window_start, window_end))


def _seeded_expected_unattempted_dts(venue_df: pd.DataFrame) -> set[str]:
    """Return the data_types with ≥1 materialised ``expected_unattempted`` row.

    F4 seed-guard (codex/02-data/availability-manifest-and-data-status.md):
    ``expected_unattempted`` is MATERIALISED by the writer (MTDS pre-flight /
    IS ``enumerate_expected_universe`` v2 enumerator) and READ by consumers.
    A (venue, data_type) with seeded rows must take the 4-state READ
    denominator — re-deriving genesis/launch math on top of seeded rows
    double-counts / diverges. Cheap per-venue-frame presence check: one
    vectorised equality + one ``unique()``.
    """
    if venue_df.empty or "capture_status" not in venue_df.columns or "data_type" not in venue_df.columns:
        return set()
    seeded_mask = venue_df["capture_status"].fillna("").astype(str) == "expected_unattempted"  # pyright: ignore[reportUnknownMemberType]
    if not bool(seeded_mask.any()):  # pyright: ignore[reportUnknownArgumentType,reportUnknownMemberType]
        return set()
    return {str(d) for d in venue_df.loc[seeded_mask, "data_type"].astype(str).unique()}  # pyright: ignore[reportAny]


def _mtds_seeded_4state_dt_entry(
    venue_df: pd.DataFrame,
    venue_df_ok: pd.DataFrame,
    dt: str,
    window_start: str,
    window_end: str,
) -> dict[str, object]:
    """4-state READ entry for a (venue, dt) whose expected universe is seeded.

    F4: when the writer/enumerator has materialised ``expected_unattempted``
    rows for this (venue, data_type), the manifest rows ARE the expected
    universe. Denominator = distinct shard cells across ``captured`` +
    ``empty_confirmed`` + ``attempted_failed`` + ``expected_unattempted``
    (the M5/union rule used by ``data_status_hierarchical`` /
    ``data_status_union``); numerator keeps this module's existing
    skip-worthy convention (``venue_df_ok``: captured / empty_confirmed /
    EXPECTED_*-reasoned expected_unattempted). The genesis/launch
    re-derivation (``_mtds_expected_dates_for_venue_dt``) is NOT consulted.

    Grain dispatch mirrors the derived path: if the (venue, dt) rows carry
    non-empty ``instrument_id`` values, cells are (instrument_id, date)
    pairs (Tier-3 ``shard_instrument_days``); otherwise cells are dates
    (``shard_days``). Dates are clipped to the request window for parity
    with the derived denominator (callers pre-filter ``filtered`` by date,
    so this is a no-op on the service path but keeps direct calls honest).
    """

    def _dt_slice(df: pd.DataFrame) -> pd.DataFrame:
        if "data_type" not in df.columns or df.empty:
            return df.iloc[0:0]
        return df[df["data_type"] == dt]

    all_rows = _dt_slice(venue_df)
    ok_rows = _dt_slice(venue_df_ok)

    def _windowed(df: pd.DataFrame) -> pd.DataFrame:
        if "date" not in df.columns or df.empty:
            return df.iloc[0:0]
        date_s = df["date"].astype(str)  # pyright: ignore[reportUnknownMemberType]
        return df[(date_s >= window_start) & (date_s <= window_end)]

    all_rows = _windowed(all_rows)
    ok_rows = _windowed(ok_rows)

    has_iid = "instrument_id" in all_rows.columns
    iid_all = all_rows["instrument_id"].fillna("").astype(str).str.strip() if has_iid else pd.Series([], dtype=str)  # pyright: ignore[reportUnknownMemberType]
    per_instrument_grain = bool((iid_all.str.len() > 0).any()) if len(iid_all) else False  # pyright: ignore[reportUnknownMemberType,reportUnknownArgumentType]

    def _cells(df: pd.DataFrame) -> set[tuple[str, str]]:
        """Distinct shard cells in ``df`` at the resolved grain."""
        if df.empty:
            return set()
        date_list = [str(d) for d in df["date"].tolist()]  # pyright: ignore[reportAny]
        if per_instrument_grain:
            iid_list = [str(i).strip() for i in df["instrument_id"].fillna("").tolist()]  # pyright: ignore[reportAny,reportUnknownMemberType]
            return {(iid, d) for iid, d in zip(iid_list, date_list, strict=True) if iid}
        return {("", d) for d in date_list if d}

    expected_cells = _cells(all_rows)
    found_cells = _cells(ok_rows)
    # ok_rows is a row-subset of venue_df, but guard the intersection anyway
    # so found can never exceed expected.
    found_cells &= expected_cells

    expected_count = len(expected_cells)
    found_count = len(found_cells)
    expected_dates = {d for _, d in expected_cells}
    found_dates = {d for _, d in found_cells}

    entry: dict[str, object] = {
        "expected_shards": expected_count,
        "found_shards": found_count,
        "missing_shards": max(0, expected_count - found_count),
        "completion_pct": min(round(found_count / max(1, expected_count) * 100, 2), 100.0),
        # Collapsed across instruments at the dt level — same convention as
        # the Tier-3 derived path so the UI drill-down stays compatible.
        "missing_dates": sorted(expected_dates - found_dates)[:500],
        "dates_found_list": sorted(found_dates)[:500],
        "unit": "shard_instrument_days" if per_instrument_grain else "shard_days",
        # Provenance marker: the denominator came from materialised
        # expected_unattempted rows, NOT the genesis/launch re-derivation.
        "denominator_source": "materialised_expected_unattempted",
    }
    if per_instrument_grain:
        expected_instruments = sorted({iid for iid, _ in expected_cells})
        found_instruments = {iid for iid, _ in found_cells}
        entry["expected_instruments"] = expected_instruments
        entry["missing_instruments"] = [iid for iid in expected_instruments if iid not in found_instruments]
    return entry


def mtds_honest_coverage_for_venue(
    filtered: pd.DataFrame,
    venue: str,
    category: str,
    window_start: str,
    window_end: str,
    venue_mapping: VenueMapping,
    instruments_provider: Callable[[str, str], list[str] | None] | None = None,
) -> dict[str, object]:
    """Honest-coverage rollup for one ``(category, venue)`` pair.

    For each UAC-declared data_type on this venue, compute:
      - expected_dates: from ``mtds_expected_dates_for_venue_dt`` (honest
        per-(venue, dt) window with TRADFI tick-window gate applied).
      - found_dates: distinct dates in ``filtered`` where
        ``(venue, data_type)`` matches AND ``capture_status in
        {captured, empty_confirmed}`` (v4 rows without capture_status are
        implicit ``captured``, same convention as ``sports_honest_coverage``).

    Returns per-dt entries + aggregate totals. ``expected_shards`` /
    ``found_shards`` count distinct ``(venue, data_type, date)`` triples
    for venue-level shard dt, and distinct ``(venue, data_type,
    instrument_id, date)`` 4-tuples for per-instrument shard dt (Phase 8D
    Tier-3). :func:`is_per_instrument_shard_data_type` dispatches.

    ``expected_data_types`` lists the UAC-declared dt set for this venue;
    ``missing_data_types`` lists declared dt with zero found shards (surfaces
    adapter-coverage gaps even when some dt are 100% complete).

    PREDICTION asset_group: UAC ``VENUE_DATA_TYPE_CAPABILITIES`` only declares
    ``trades`` on POLYMARKET / KALSHI today. We extend the expected-dt set
    with ``PREDICTION_DATA_TYPE_META`` keys (``trades`` / ``book_snapshot`` /
    ``market_metadata`` / ``fills``) so the three new SchemaContracts
    registered at UAC ``c7642f3`` surface as expected rows in the manifest
    panel even before adapters write parquet rows. See
    ``PREDICTION_DATA_TYPE_META`` docstring for ``expected_count_per_day``
    semantics.
    """
    expected_dts = list(_dss.get_expected_data_types_for_venue(venue))
    if category.upper() == "PREDICTION":
        # Union the UAC SchemaContract registry — surface the 3 newly-registered
        # PREDICTION data_types as expected rows even when adapters haven't
        # backfilled yet (0/N denominator over the daily grid for the indeterminate
        # types is the honest "we know we're missing" signal).
        expected_dts = sorted(set(expected_dts) | set(PREDICTION_DATA_TYPE_META.keys()))
    if not expected_dts:
        return {
            "expected_shards": 0,
            "found_shards": 0,
            "missing_shards": 0,
            "data_types": {},
            "expected_data_types": [],
            "missing_data_types": [],
        }

    # Pre-filter to this venue once for speed.
    if "venue" not in filtered.columns or filtered.empty:
        venue_df = pd.DataFrame(columns=filtered.columns)
    else:
        venue_df = filtered[filtered["venue"] == venue]

    if "capture_status" in venue_df.columns:
        _vst_s = venue_df["capture_status"].fillna("captured").astype(str)
        _vreason_s = (
            venue_df["error_reason"].fillna("").astype(str)
            if "error_reason" in venue_df.columns
            else pd.Series("", index=venue_df.index)
        )
        ok_mask = _vst_s.isin(["captured", "empty_confirmed"]) | (
            (_vst_s == "expected_unattempted") & _vreason_s.str.startswith("EXPECTED_")
        )
    else:
        ok_mask = pd.Series([True] * len(venue_df), index=venue_df.index)
    venue_df_ok = venue_df[ok_mask] if not venue_df.empty else venue_df

    dt_entries: dict[str, object] = {}
    total_expected = 0
    total_found = 0
    missing_dts: list[str] = []

    # F4 seed-guard: data_types with materialised ``expected_unattempted``
    # rows take the 4-state READ denominator below and SKIP the
    # genesis/launch re-derivation entirely (re-deriving on top of seeded
    # rows double-counts / diverges). Pre-seed (no such rows) keeps the
    # derived path unchanged.
    seeded_dts = _seeded_expected_unattempted_dts(venue_df)

    for dt in sorted(expected_dts):
        if dt in seeded_dts:
            # Seed-aware 4-state READ — denominator = captured + empty +
            # failed + expected_unattempted over the manifest rows; no
            # ``mtds_expected_dates_for_venue_dt`` call, no IS-provider
            # derived universe for this dt.
            dt_entry = _mtds_seeded_4state_dt_entry(venue_df, venue_df_ok, dt, window_start, window_end)
            dt_entries[dt] = dt_entry
            expected_count = int(cast(int, dt_entry["expected_shards"]))
            found_count = int(cast(int, dt_entry["found_shards"]))
            total_expected += expected_count
            total_found += found_count
            if found_count == 0 and expected_count > 0:
                missing_dts.append(dt)
            continue

        expected_dates = mtds_expected_dates_for_venue_dt(venue_mapping, venue, dt, category, window_start, window_end)

        if is_per_instrument_shard_data_type(dt):
            # Phase 8D Tier-3 branch — per-(venue, dt, instrument_id,
            # date) denominator.
            # For CEFI: pass the live IS catalog provider + cap=None so the
            # full real universe is not truncated by the MVP seed cap of 50.
            # For other asset_groups: instruments_provider=None falls back to
            # UAC MVP seed tables (existing behaviour).
            _instr_cap: int | None = None if instruments_provider is not None else DEFAULT_PER_INSTRUMENT_SENTINEL_CAP
            dt_entry = per_instrument_coverage(
                venue_df_ok,
                venue,
                dt,
                expected_dates,
                _instr_cap,
                instruments_provider=instruments_provider,
            )
            dt_entries[dt] = dt_entry
            expected_count = int(cast(int, dt_entry["expected_shards"]))
            found_count = int(cast(int, dt_entry["found_shards"]))
        else:
            # Venue-level dt — preserve the Phase 6d per-(venue, dt, date)
            # denominator.
            #
            # FLAG-1 (TradFi v9 multi-source): the v9 manifest carries one row
            # per (venue, data_type, date, source) — e.g. databento + massive
            # both captured for the same date produce two rows. The UNION
            # semantics (≥1 captured → cell captured) are enforced here by
            # ``dt_rows["date"].unique()``, which deduplicates across sources
            # before computing found_dates_set. A per-source breakdown is also
            # surfaced in ``per_source`` for the UI drilldown (plan FLAG-1,
            # operator 2026-06-02: UNION + manifest-derived per-source breakdown).
            if "data_type" in venue_df_ok.columns:
                dt_rows = venue_df_ok[venue_df_ok["data_type"] == dt]
                # Union dedup: distinct dates regardless of how many sources
                # have a captured/empty_confirmed row for that date.
                found_dates_set = {str(d) for d in dt_rows["date"].unique()}  # pyright: ignore[reportAny]
            else:
                dt_rows = venue_df_ok.iloc[0:0]
                found_dates_set: set[str] = set()
            # Only count dates that fall inside the expected window — a
            # row from before ``effective_start`` should not inflate
            # ``found_shards``.
            found_in_expected = found_dates_set & expected_dates  # pyright: ignore[reportUnknownVariableType]
            missing_dates = sorted(expected_dates - found_dates_set)  # pyright: ignore[reportUnknownVariableType]
            expected_count = len(expected_dates)
            found_count = len(found_in_expected)  # pyright: ignore[reportUnknownVariableType]

            # Per-source breakdown — free from the in-memory ``_index`` rows
            # (the v9 manifest denormalises source to the row level so this is
            # a single groupby, no per-parquet scan). Only emitted when the
            # ``source`` column is present (v9 manifests); v8 manifests omit it.
            per_source: dict[str, object] = {}
            if "source" in dt_rows.columns and not dt_rows.empty:
                for src, src_group in dt_rows.groupby("source"):  # pyright: ignore[reportUnknownVariableType]
                    src_str = str(src)
                    src_dates = {str(d) for d in src_group["date"].unique()}  # pyright: ignore[reportAny]
                    src_found = src_dates & expected_dates  # pyright: ignore[reportUnknownVariableType]
                    per_source[src_str] = {
                        "found_shards": len(src_found),  # pyright: ignore[reportUnknownArgumentType]
                        "dates_found_list": sorted(src_found)[:500],  # pyright: ignore[reportUnknownVariableType]
                    }

            dt_entry: dict[str, object] = {
                "expected_shards": expected_count,
                "found_shards": found_count,
                "missing_shards": max(0, expected_count - found_count),
                "completion_pct": min(round(found_count / max(1, expected_count) * 100, 2), 100.0),
                "missing_dates": missing_dates[:500],
                "dates_found_list": sorted(found_in_expected)[:500],  # pyright: ignore[reportUnknownVariableType]
                "unit": "shard_days",
            }
            if per_source:
                dt_entry["per_source"] = per_source
            dt_entries[dt] = dt_entry

        total_expected += expected_count
        total_found += found_count
        if found_count == 0 and expected_count > 0:
            missing_dts.append(dt)

    return {
        "expected_shards": total_expected,
        "found_shards": total_found,
        "missing_shards": max(0, total_expected - total_found),
        "data_types": dt_entries,
        "expected_data_types": sorted(expected_dts),
        "missing_data_types": missing_dts,
    }

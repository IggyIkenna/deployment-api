"""MTDS per-category metadata constants + the honest-coverage target gate.

Split out of ``mtds.py`` (2026-07-31,
``deployment_api_qg_size_gate_debt_2026_07_30.md``) to bring that facade
module under the 900-line file-size gate. Re-exported through ``mtds.py`` so
every existing import path (``deployment_api.services.data_status.mtds.*``)
keeps working unchanged.
"""

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
        # MTDS SPORTS = bookmaker odds per league x per fixture-date (Phase 6d,
        # shipped 2026-07-22). "venue" here means BOOKMAKER; the accessor
        # string is a sentinel ("bookmaker") ``mtds_expected_venues`` special-
        # cases to UAC's ``expected_odds_api_bookmaker_keys()`` rather than a
        # ``VenueMapping`` attribute name (bookmakers aren't in that class).
        # The league x fixture-date sub-axis is NOT handled by the generic
        # per-(venue, data_type, calendar-date) path every other category
        # uses — see ``mtds_honest_coverage_for_bookmaker`` (dispatched from
        # ``_apply_mtds_honest_coverage`` for this category only), which
        # iterates each bookmaker's covered leagues (UAC
        # ``BOOKMAKER_LEAGUE_COVERAGE``) and their real fixture dates
        # (``sports_expected_dates_for_league``) instead of a trading calendar.
        "venue_accessor": "bookmaker",
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


# Services whose per-category manifest entries take the honest-coverage
# override below. Extended 2026-07-21 (mtds_data_status_page_parity) to cover
# market-data-processing-service (MDPS) alongside the original MTDS raw-tick
# path — MDPS emits candles over the SAME (venue, category) shape (its
# manifest ``data_type`` axis is the SOURCE token per the 2026-07-21 operator
# ruling, with the candle timeframe in a separate ``timeframe`` column), so
# the identical UAC-driven (venue, data_type, date) shard-space math applies,
# service-scoped via ``get_expected_data_types_for_venue(..., service=...)``.
_HONEST_COVERAGE_SERVICES: frozenset[str] = frozenset({"market-tick-data-service", "market-data-processing-service"})


def is_mtds_honest_coverage_target(service: str, category: str) -> bool:
    """True iff ``(service, category)`` should run the MTDS/MDPS honest-coverage
    override. Includes SPORTS as of Phase 6d (2026-07-22) — dispatched to
    ``mtds_honest_coverage_for_bookmaker`` instead of the generic per-venue
    path, see ``_apply_mtds_honest_coverage``."""
    if service not in _HONEST_COVERAGE_SERVICES:
        return False
    cat_key = category.upper()
    return cat_key in MTDS_CATEGORY_META

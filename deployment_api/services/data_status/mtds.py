"""MTDS per-category coverage metadata + expected-dates helpers.

Split out of the 6,663-line ``data_status_service.py`` god-module
(codex ratchet plan 2026-06-10). The facade module re-exports every
public + legacy-underscore name, so callers keep importing from
``deployment_api.services.data_status_service``.

Further decomposed 2026-07-31 (``deployment_api_qg_size_gate_debt_2026_07_30.md``)
into sibling modules (``mtds_meta``, ``mtds_defi_alias``, ``mtds_expected``,
``mtds_dt_entries``) to bring this file under the 900-line file-size gate and
``mtds_honest_coverage_for_venue`` under the 200-line function-size gate. This
module re-exports every symbol from those siblings so
``deployment_api.services.data_status.mtds.<name>`` (the existing import
surface every caller + test uses, including direct-file-load tests and
``unittest.mock.patch`` targets) is unchanged.
"""

import logging
from collections.abc import Callable

import pandas as pd
from unified_api_contracts import VenueMapping
from unified_api_contracts.registry import BOOKMAKER_LEAGUE_COVERAGE

import deployment_api.services.data_status_service as _dss
from deployment_api.services.data_status.mtds_defi_alias import (
    DEFI_DATA_TYPE_ALIASES,
    DEFI_SOURCE_TO_DATA_TYPE,
    canonicalise_defi_data_types,
)
from deployment_api.services.data_status.mtds_dt_entries import (
    _mtds_derived_entry_counts,
    _mtds_seeded_4state_dt_entry,
    _mtds_seeded_entry_counts,
    _mtds_venue_rows,
    _seeded_expected_unattempted_dts,
    _tier2_dt_entry,
)
from deployment_api.services.data_status.mtds_expected import (
    mtds_expected_dates_cached,
    mtds_expected_dates_for_venue_dt,
    mtds_expected_venues,
    shared_venue_mapping,
)
from deployment_api.services.data_status.mtds_meta import (
    MTDS_CATEGORY_META,
    PREDICTION_DATA_TYPE_META,
    TRADFI_TICK_ONLY_DATA_TYPES,
    is_mtds_honest_coverage_target,
)
from deployment_api.services.data_status.sports_helpers import (
    sports_expected_dates_for_league,
)

logger = logging.getLogger(__name__)

# Re-export marker for lint tools that flag "imported but unused" — every
# name below IS the public surface of this facade module (see module
# docstring). Referencing them here keeps ruff/basedpyright quiet without a
# blanket per-import noqa.
__all__ = [
    "BOOKMAKER_LEAGUE_COVERAGE",
    "DEFI_DATA_TYPE_ALIASES",
    "DEFI_SOURCE_TO_DATA_TYPE",
    "MTDS_CATEGORY_META",
    "PREDICTION_DATA_TYPE_META",
    "TRADFI_TICK_ONLY_DATA_TYPES",
    "_mtds_seeded_4state_dt_entry",
    "_tier2_dt_entry",
    "canonicalise_defi_data_types",
    "is_mtds_honest_coverage_target",
    "mtds_expected_dates_cached",
    "mtds_expected_dates_for_venue_dt",
    "mtds_expected_venues",
    "mtds_honest_coverage_for_bookmaker",
    "mtds_honest_coverage_for_venue",
    "shared_venue_mapping",
    "sports_expected_dates_for_league",
]


def mtds_honest_coverage_for_venue(
    filtered: pd.DataFrame,
    venue: str,
    category: str,
    window_start: str,
    window_end: str,
    venue_mapping: VenueMapping,
    instruments_provider: Callable[[str, str], list[str] | None] | None = None,
    instrument_windows: dict[str, tuple[str | None, str | None]] | None = None,
    scope: str = "could_exist",
    instrument_types: dict[str, str] | None = None,
    service: str = "",
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

    ``scope``/``instrument_types`` — MVP-scope toggle (mirrors the
    already-shipped venue-year-coverage grid's ``CoverageScope``, see
    ``routes/data_status/_coverage_scope.py``). Threaded ONLY into the
    per-instrument-shard (Tier-3) branch via :func:`per_instrument_coverage`
    — a venue-level dt has no single ``instrument_type`` to evaluate UAC
    ``is_mvp(...)`` against, so ``scope=mvp`` is a no-op on non-per-instrument
    dt entries today (a known, deliberate limitation, not silently wrong: the
    MVP concept is instrument-grained for cefi, not venue-grained).

    ``service`` (added mtds_data_status_page_parity_2026_07_21, MDPS
    extension): threaded to ``get_expected_data_types_for_venue(venue,
    service=service)`` so the expected-dt list is narrowed correctly for
    market-data-processing-service (MDPS) — see that function's docstring.
    Defaults to ``""``, which is BYTE-FOR-BYTE the pre-2026-07-21 behaviour
    (``get_expected_data_types_for_venue(venue)`` with no ``service=`` kwarg
    at all) — every existing MTDS call path that doesn't pass ``service``
    (including this module's own direct-call unit tests) is unaffected.

    When ``service == "market-data-processing-service"``, the per-instrument
    Tier-3 branch (:func:`per_instrument_coverage`) additionally becomes
    TIMEFRAME-aware: MDPS writes one candle parquet per (instrument, date,
    timeframe) — a strictly finer shard grain than MTDS's per-(instrument,
    date) raw ticks — over UAC ``get_expected_timeframes_for_venue_dt``'s
    canonical timeframe set. For any other ``service`` value the Tier-3
    call is unchanged (``timeframes=None``). The Tier-2 venue-level branch
    (:func:`_tier2_dt_entry`) is ALSO timeframe-aware as of the 2026-07-22
    follow-up (deferred from the original 2026-07-21 ship, which scoped only
    the Tier-3 per-instrument branch per its 3 converged/individual review
    findings) — MDPS's one venue-level derivable dt today is ``liquidations``,
    which previously under-multiplied its denominator by ``len(timeframes)``;
    see :func:`_tier2_dt_entry`'s docstring for the exact mirrored pattern.

    ``historical_coverage_gap`` (MDPS only, open design question DEFAULT
    resolution): pre-cutover MDPS manifest rows written under the legacy
    aggregated-``data_type`` convention (``data_type="ohlcv_1m"`` directly,
    no separate ``timeframe`` column / no source-keyed ``data_type``) are
    INVISIBLE to this function's source-keyed ``(venue, data_type)`` query —
    they simply never match ``dt`` (a raw SOURCE token like ``"trades"``).
    We do not attempt a reverse-mapping compat shim (real, separate design
    work); instead every MDPS-scoped response is annotated
    ``historical_coverage_gap=True`` so a consumer knows pre-cutover history
    for this venue may be undercounted here.

    Per-dt dispatch (seeded 4-state read / Tier-3 per-instrument / Tier-2
    venue-level) lives in :func:`_mtds_one_dt_entry`
    (``deployment_api/services/data_status/mtds_dt_entries.py``) — extracted
    2026-07-31 to keep this function under the 200-line size gate; behaviour
    is unchanged from the original inline loop.
    """
    is_mdps = service == "market-data-processing-service"
    expected_dts = list(_dss.get_expected_data_types_for_venue(venue, service=service))
    if category.upper() == "PREDICTION":
        # Union the UAC SchemaContract registry — surface the 3 newly-registered
        # PREDICTION data_types as expected rows even when adapters haven't
        # backfilled yet (0/N denominator over the daily grid for the indeterminate
        # types is the honest "we know we're missing" signal).
        expected_dts = sorted(set(expected_dts) | set(PREDICTION_DATA_TYPE_META.keys()))
    if not expected_dts:
        empty_result: dict[str, object] = {
            "expected_shards": 0,
            "found_shards": 0,
            "missing_shards": 0,
            "data_types": {},
            "expected_data_types": [],
            "missing_data_types": [],
        }
        if is_mdps:
            empty_result["historical_coverage_gap"] = True
        return empty_result

    venue_df, venue_df_ok = _mtds_venue_rows(filtered, venue)

    dt_entries: dict[str, object] = {}
    total_expected = 0
    total_found = 0
    missing_dts: list[str] = []

    # F4 seed-guard: data_types with materialised ``expected_unattempted``
    # rows take the 4-state READ denominator and SKIP the genesis/launch
    # re-derivation entirely (re-deriving on top of seeded rows
    # double-counts / diverges). Pre-seed (no such rows) keeps the derived
    # path unchanged. ``mtds_expected_dates_for_venue_dt`` is called directly
    # here (not inside a helper) so ``unittest.mock.patch`` targeting that
    # name on this module still intercepts every derived-path call — see
    # ``_mtds_derived_entry_counts``'s docstring
    # (``deployment_api/services/data_status/mtds_dt_entries.py``).
    seeded_dts = _seeded_expected_unattempted_dts(venue_df)

    for dt in sorted(expected_dts):
        if dt in seeded_dts:
            dt_entry, expected_count, found_count = _mtds_seeded_entry_counts(
                venue_df, venue_df_ok, dt, window_start, window_end
            )
        else:
            expected_dates = mtds_expected_dates_for_venue_dt(
                venue_mapping, venue, dt, category, window_start, window_end
            )
            dt_entry, expected_count, found_count = _mtds_derived_entry_counts(
                dt,
                expected_dates,
                venue,
                venue_df_ok,
                category,
                is_mdps,
                instruments_provider,
                instrument_windows,
                scope,
                instrument_types,
            )
        dt_entries[dt] = dt_entry
        total_expected += expected_count
        total_found += found_count
        if found_count == 0 and expected_count > 0:
            missing_dts.append(dt)

    result: dict[str, object] = {
        "expected_shards": total_expected,
        "found_shards": total_found,
        "missing_shards": max(0, total_expected - total_found),
        "data_types": dt_entries,
        "expected_data_types": sorted(expected_dts),
        "missing_data_types": missing_dts,
    }
    if is_mdps:
        # Open design question DEFAULT (see docstring): pre-cutover MDPS rows
        # (legacy aggregated data_type) are invisible to this function's
        # source-keyed query, not reverse-mapped. Flagged, never silently
        # dropped. Never emitted for MTDS -- new key is additive-only and
        # gated behind ``is_mdps`` so MTDS callers get a byte-for-byte
        # unchanged dict.
        result["historical_coverage_gap"] = True
    return result


_SPORTS_ODDS_SOURCE_KEY = "odds_api"
_SPORTS_ODDS_DATA_TYPE = "trades"


def mtds_honest_coverage_for_bookmaker(
    filtered: pd.DataFrame,
    bookmaker: str,
    window_start: str,
    window_end: str,
) -> dict[str, object]:
    """Honest-coverage rollup for one SPORTS bookmaker (Phase 6d).

    Sibling to :func:`mtds_honest_coverage_for_venue` for the ONE category
    whose axis isn't per-(venue, data_type, calendar-date):
    ``per_league_per_bookmaker_per_fixture_date``. The generic function
    can't be reused here — it has no league dimension and its denominator
    source (``mtds_expected_dates_for_venue_dt``, a trading-day calendar)
    doesn't apply to sports fixture schedules. Reuses the SAME manifest
    columns everything else does (``venue`` = bookmaker key, ``date`` =
    fixture day, ``capture_status``/``error_reason`` as usual) plus
    ``league_id``, which sports odds rows carry
    (``venue_fetch.py::_build_sports_shard_path``:
    ``venue={bookmaker}/league_id={league}/...instrument_type=odds/data_type=trades``).

    For each league UAC's ``BOOKMAKER_LEAGUE_COVERAGE`` says this bookmaker
    has ever priced (the SAME observed-coverage oracle
    ``is_bookmaker_league_covered_exact`` uses on the sentinel-emission
    path, so the denominator here can never disagree with what the writer
    considers in-scope), gets real fixture dates via
    ``sports_expected_dates_for_league`` (floor-clipped to the sport's
    ``odds_api`` UAC coverage-start, e.g. the 2020-06 sports data floor —
    NOT a raw calendar range) and counts distinct captured/empty_confirmed
    dates for that (bookmaker, league) pair. A bookmaker UAC has never
    observed pricing ANY league for (unobserved — ``BOOKMAKER_LEAGUE_COVERAGE``
    has no entry) contributes 0 expected / 0 found, exactly like an
    unrequested bookmaker: it renders as a zero-row entry via the caller's
    union-of-expected-venues injection (:func:`mtds_expected_venues`'s
    ``expected_odds_api_bookmaker_keys()`` branch), not as a silently-absent
    row — the whole point of Phase 6d.

    Returns the SAME shape as :func:`mtds_honest_coverage_for_venue` so
    :func:`_apply_mtds_honest_coverage`'s per-venue loop is unchanged;
    ``data_types`` is keyed by league_id here (not data_type — sports odds
    has exactly one data_type, ``trades``, so league is the meaningful
    per-cell breakdown the UI drilldown wants).
    """
    leagues = sorted(BOOKMAKER_LEAGUE_COVERAGE.get(bookmaker.strip().upper(), frozenset()))
    if not leagues:
        return {
            "expected_shards": 0,
            "found_shards": 0,
            "missing_shards": 0,
            "data_types": {},
            "expected_data_types": [],
            "missing_data_types": [_SPORTS_ODDS_DATA_TYPE],
        }

    if "venue" in filtered.columns and not filtered.empty:
        bm_df = filtered[filtered["venue"].astype(str).str.upper() == bookmaker.strip().upper()]
    else:
        bm_df = filtered.iloc[0:0]

    if "capture_status" in bm_df.columns:
        status_s = bm_df["capture_status"].fillna("captured").astype(str)
        reason_s = (
            bm_df["error_reason"].fillna("").astype(str)
            if "error_reason" in bm_df.columns
            else pd.Series("", index=bm_df.index)
        )
        ok_mask = status_s.isin(["captured", "empty_confirmed"]) | (
            (status_s == "expected_unattempted") & reason_s.str.startswith("EXPECTED_")
        )
    else:
        ok_mask = pd.Series([True] * len(bm_df), index=bm_df.index)
    bm_df_ok = bm_df[ok_mask] if not bm_df.empty else bm_df

    league_entries: dict[str, object] = {}
    total_expected = 0
    total_found = 0
    missing_leagues: list[str] = []

    for league_id in leagues:
        expected_dates = set(
            sports_expected_dates_for_league(
                league_id,
                "per_league_per_fixture_date",
                1,
                window_start,
                window_end,
                source_key=_SPORTS_ODDS_SOURCE_KEY,
                data_type=_SPORTS_ODDS_DATA_TYPE,
            )
        )
        if not expected_dates:
            continue

        if "league_id" in bm_df_ok.columns and not bm_df_ok.empty:
            league_rows = bm_df_ok[bm_df_ok["league_id"].astype(str) == league_id]
        else:
            league_rows = bm_df_ok.iloc[0:0]

        found_dates_set = {str(d) for d in league_rows["date"].unique()} if not league_rows.empty else set()  # pyright: ignore[reportAny]
        found_in_expected = found_dates_set & expected_dates
        found_count = len(found_in_expected)
        expected_count = len(expected_dates)

        league_entries[league_id] = {
            "expected_shards": expected_count,
            "found_shards": found_count,
            "missing_shards": max(0, expected_count - found_count),
            "completion_pct": min(round(found_count / max(1, expected_count) * 100, 2), 100.0),
            "missing_dates": sorted(expected_dates - found_dates_set)[:500],
            "dates_found_list": sorted(found_in_expected)[:500],
            "unit": "fixture_dates",
        }
        total_expected += expected_count
        total_found += found_count
        if found_count == 0 and expected_count > 0:
            missing_leagues.append(league_id)

    return {
        "expected_shards": total_expected,
        "found_shards": total_found,
        "missing_shards": max(0, total_expected - total_found),
        "data_types": league_entries,
        "expected_data_types": [_SPORTS_ODDS_DATA_TYPE],
        "missing_data_types": [_SPORTS_ODDS_DATA_TYPE] if total_found == 0 and total_expected > 0 else [],
    }

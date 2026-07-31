"""MTDS expected-venue / expected-dates helpers.

Split out of ``mtds.py`` (2026-07-31,
``deployment_api_qg_size_gate_debt_2026_07_30.md``) to bring that facade
module under the 900-line file-size gate. Re-exported through ``mtds.py`` so
every existing import path (``deployment_api.services.data_status.mtds.*``)
keeps working unchanged.
"""

from functools import lru_cache

import pandas as pd
from unified_api_contracts import VenueMapping
from unified_api_contracts.registry import (
    EMPTY_OR_DEPRECATED_DEFI_VENUES,
    expected_odds_api_bookmaker_keys,
    get_coverage_windows,
    get_lst_venue_genesis,
    is_in_tradfi_tick_window,
    venue_has_no_expected_defi_coverage,
)

import deployment_api.services.data_status_service as _dss
from deployment_api.services.data_status.mtds_meta import (
    MTDS_CATEGORY_META,
    TRADFI_TICK_ONLY_DATA_TYPES,
)


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
    # or one of the missing PREDICTION/SPORTS fallbacks.
    if accessor == "all_prediction_venues":
        # No UAC accessor for prediction venues yet — codex SSOT §1 lists
        # POLYMARKET + KALSHI. Hardcoded fallback mirrors the matrix.
        return ["KALSHI", "POLYMARKET"]
    if accessor == "bookmaker":
        # Phase 6d: bookmakers aren't a VenueMapping property (they're an
        # Odds-API request-scope list, not a venue registry) — resolve via
        # the dedicated UAC accessor instead.
        return expected_odds_api_bookmaker_keys()
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

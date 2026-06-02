"""
Data status business logic service.

Handles core data status operations including CLI integration,
missing shards calculation, and status aggregation.
"""

import asyncio
import json
import logging
import multiprocessing
import re
import sys
import time
from collections import Counter
from collections.abc import Callable
from concurrent.futures import ProcessPoolExecutor
from functools import lru_cache
from typing import Any, ClassVar, Literal, cast

import pandas as pd
from unified_api_contracts import (
    CaptureStatusCounts,
    VenueMapping,
    compute_honest_coverage,
    get_expected_data_types_for_venue,
    get_expected_instruments_for_venue,
    get_venue_data_type_start_date,
    is_per_instrument_shard_data_type,
)
from unified_api_contracts.features import (
    EXPECTED_FEATURE_GROUPS_BY_SERVICE,
    get_feature_coverage_start,
    is_known_feature_group,
)
from unified_api_contracts.internal import MarketCategory
from unified_api_contracts.registry import (
    DEPRECATED_DEFI_GHOST_VENUE_NAMES,
    EMPTY_OR_DEPRECATED_DEFI_VENUES,
    get_coverage_windows,
    get_lst_venue_genesis,
    get_raw_source_data_types,
    is_expected,
    is_in_tradfi_tick_window,
    is_processed_data_type,
    venue_has_no_expected_defi_coverage,
)
from unified_api_contracts.registry.data_status_axis_matrix import (
    get_breakdown_axes,
    get_primary_axis,
)
from unified_api_contracts.sports import (
    FEATURE_UPSTREAM_REQUIREMENTS,
    UpstreamReq,
    get_entity_league_coverage,
    get_expected_leagues_for_source,
    get_league_fixture_calendar,
    get_sports_entity_start_date,
    get_transfer_windows_for_year,
    in_coverage,
)
from unified_api_contracts.sports import (
    clip_dates_to_source_coverage as _clip_dates_to_source_coverage,
)
from unified_api_contracts.sports import (
    is_in_known_gap as _is_in_known_gap,
)
from unified_trading_library import read_availability_index, resolve_bucket_name

from deployment_api.services.data_status_drilldown import (
    COMMODITY_BUCKET_TEMPLATE,
    PREDICTION_KIND_MAP,
    SERVICE_TO_KIND,
)
from deployment_api.services.data_status_drilldown import (
    build_bucket_name as _drilldown_build_bucket_name,
)
from deployment_api.settings import deployment_env_short as _env_short
from deployment_api.settings import gcp_project_id as _pid
from deployment_api.utils.storage_facade import list_objects

logger = logging.getLogger(__name__)

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
_TRADFI_TICK_ONLY_DATA_TYPES: frozenset[str] = frozenset({"tbbo"})

# Phase 8D — MVP cap for the per-(venue, data_type, instrument_id) Tier-3
# denominator. Mirrors the MTDS orchestrator constant in
# ``market_tick_data_service/engine/orchestrator.py`` so the aggregator
# denominator matches the sentinel fan-out the orchestrator writes.
_DEFAULT_PER_INSTRUMENT_SENTINEL_CAP: int = 50

# Per-instrument dt whose per_instrument drill-down panel is inlined on the
# response. Above this threshold the aggregator suppresses the dict (kept as
# totals only) to avoid ballooning the API payload size on big perp boards.
_PER_INSTRUMENT_BREAKDOWN_MAX_SIZE: int = 20

# Per-category coverage semantics — distinguishes "dense" categories where every
# underlying is expected to produce data every day (CeFi, TradFi, DeFi) from
# "event-driven" categories where underlyings only trade on a fraction of days
# (sports fixtures, Polymarket conditionIds). For event-driven categories the
# shards-weighted ``capture_coverage_pct`` vastly understates real coverage
# because the denominator assumes every (underlying x day) combo should have
# trades. The displayed ``completion_pct`` is therefore the ``attempt_coverage_pct``
# (did we observe this underlying at all), with ``capture_coverage_pct`` kept
# for the detail drill-down and ``empty_rate_estimate`` showing the fraction
# of underlying-days that had no trades.
COVERAGE_SEMANTICS: dict[str, Literal["dense", "event_driven"]] = {
    "CEFI": "dense",
    "TRADFI": "dense",
    "DEFI": "dense",
    "SPORTS": "event_driven",
    "PREDICTION": "event_driven",
}


# Coverage axes supported by SPORTS data-status aggregation. Each data_type
# maps to exactly one axis via ``SPORTS_DATA_TYPE_META``.
#
# - ``per_league_per_fixture_date``: one expected shard per (league, fixture-
#   date). Numerator/denominator both count distinct (league, date) pairs.
#   Honest-coverage: expected = sum over expected_leagues of
#   len(get_league_fixture_calendar(l, start, end)).
# - ``per_league_periodic``: one expected shard per (league, cadence-date)
#   inside each league's active season (weekly standings refresh, etc).
# - ``global_periodic``: one expected shard per cadence-date (daily
#   reference-list snapshot). No league axis.
# - ``global_season``: one expected shard per season (venues list, etc).
SportsAxis = Literal[
    "per_league_per_fixture_date",
    "per_league_periodic",
    "per_feature_per_league_per_fixture_date",
    "global_periodic",
    "global_season",
]


# Canonical per-data_type metadata — mirrors
# ``codex/02-data/sports-data-source-coverage-matrix.md`` (§2).
#
# For every SPORTS data_type that lands in the availability manifest:
#   - ``source``: data_sources key (``api_football``, ``footystats``, …)
#   - ``classifications``: league classifications the source covers
#   - ``axis``: coverage axis (see SportsAxis above)
#   - ``cadence_days``: only for ``per_league_periodic`` / ``global_periodic``
#     axes — dates are grid-sampled at this cadence inside the window.
#   - ``unit``: display unit for the response (fixture_dates / cadence_refreshes
#     / season_snapshots / daily_snapshots).
#
# Adding a new SPORTS data_type: update this map AND the codex SSOT in the same
# commit (see codex-first feedback rule).
SPORTS_DATA_TYPE_META: dict[str, dict[str, object]] = {
    # API-Football — 95 leagues (Prediction 33 + Features 22 + Reference 40)
    "FIXTURES": {
        "source": "api_football",
        "classifications": ("Prediction", "Features", "Reference"),
        "axis": "per_league_per_fixture_date",
        "unit": "fixture_dates",
    },
    "FIXTURE_EVENTS": {
        "source": "api_football",
        "classifications": ("Prediction", "Features", "Reference"),
        "axis": "per_league_per_fixture_date",
        "unit": "fixture_dates",
    },
    "FIXTURE_LINEUPS": {
        "source": "api_football",
        "classifications": ("Prediction", "Features", "Reference"),
        "axis": "per_league_per_fixture_date",
        "unit": "fixture_dates",
    },
    "FIXTURE_STATS": {
        "source": "api_football",
        "classifications": ("Prediction", "Features", "Reference"),
        "axis": "per_league_per_fixture_date",
        "unit": "fixture_dates",
    },
    "PLAYER_STATS": {
        "source": "api_football",
        "classifications": ("Prediction", "Features", "Reference"),
        "axis": "per_league_per_fixture_date",
        "unit": "fixture_dates",
    },
    "INJURIES": {
        "source": "api_football",
        "classifications": ("Prediction", "Features", "Reference"),
        "axis": "per_league_periodic",
        "cadence_days": 1,
        "unit": "daily_snapshots",
    },
    "STANDINGS": {
        "source": "api_football",
        "classifications": ("Prediction", "Features", "Reference"),
        "axis": "per_league_periodic",
        "cadence_days": 7,
        "unit": "cadence_refreshes",
    },
    "LEAGUES": {
        "source": "api_football",
        "classifications": ("Prediction", "Features", "Reference"),
        "axis": "global_periodic",
        "cadence_days": 1,
        "unit": "daily_snapshots",
    },
    "TEAMS": {
        "source": "api_football",
        "classifications": ("Prediction", "Features", "Reference"),
        "axis": "global_periodic",
        "cadence_days": 1,
        "unit": "daily_snapshots",
    },
    "VENUES": {
        "source": "api_football",
        "classifications": ("Prediction", "Features", "Reference"),
        "axis": "global_season",
        "unit": "season_snapshots",
    },
    # FootyStats — 46 leagues (Prediction 28 + Features 18; no Reference)
    "MATCHES": {
        "source": "footystats",
        "classifications": ("Prediction", "Features"),
        "axis": "per_league_per_fixture_date",
        "unit": "fixture_dates",
    },
    "PREDICTIONS": {
        "source": "footystats",
        "classifications": ("Prediction", "Features"),
        "axis": "per_league_per_fixture_date",
        "unit": "fixture_dates",
    },
    # ODDS merges odds_api live + footystats backfill — use footystats (46)
    # as denominator since backfill is the dominant source on disk today.
    # See codex SSOT §4 for the open question on splitting ODDS_LIVE / ODDS_HIST.
    "ODDS": {
        "source": "footystats",
        "classifications": ("Prediction", "Features"),
        "axis": "per_league_per_fixture_date",
        "unit": "fixture_dates",
    },
    # Understat — 5 Prediction leagues
    "XG": {
        "source": "understat",
        "classifications": ("Prediction",),
        "axis": "per_league_per_fixture_date",
        "unit": "fixture_dates",
    },
    # Transfermarkt — 55 leagues (Prediction 33 + Features 22).
    # PLAYER_VALUES is a weekly periodic snapshot from transfermarkt, not a
    # per-fixture-date feed. The 2026-04-29 reconciliation deleted 167k phantom
    # denorm-to-fixture-date rows (per user pushback: "placeholder parquets
    # only for genuinely-empty external API responses, not to fudge bad data
    # quality"), so the axis now matches TRANSFERMARKT_LEAGUES — same source,
    # same weekly cadence.
    "PLAYER_VALUES": {
        "source": "transfermarkt",
        "classifications": ("Prediction", "Features"),
        "axis": "per_league_periodic",
        # PLAYER_VALUES is trigger-based, not weekly: the orchestrator only
        # fetches at season-start + transfer-window-open + transfer-window-
        # close + mid-season (~4 capture dates per league per year, see
        # ``get_leagues_needing_refresh``). cadence_days=90 (quarterly)
        # matches the actual capture cadence — pre-2026-05-05 the ``7``
        # weekly setting capped UI coverage at ~10% even with full data.
        "cadence_days": 90,
        "unit": "cadence_refreshes",
    },
    # TRANSFERMARKT_LEAGUES + SFI_LEAGUES retired 2026-05-05 — both were
    # static provider-catalog mappings (provider_id -> canonical_name + country)
    # that don't change day-to-day. Mappings now live in UAC
    # (TRANSFERMARKT_IDS / SOCCER_FOOTBALL_INFO_IDS) as versioned config
    # rather than as captured GCS data. Orchestrator still calls
    # adapter.get_leagues() at runtime for prediction-tier filtering, but
    # the result isn't written to GCS or manifest.
    # SFI_STANDINGS also retired 2026-04-24 — SFI has no standings endpoint.
    "SFI_PROGRESSIVE_STATS": {
        "source": "soccer_football_info",
        "classifications": ("Prediction",),
        "axis": "per_league_per_fixture_date",
        "unit": "fixture_dates",
    },
    # Open-Meteo weather — 33 Prediction leagues (scoped to fixture dates)
    "WEATHER": {
        "source": "open_meteo",
        "classifications": ("Prediction",),
        "axis": "per_league_per_fixture_date",
        "unit": "fixture_dates",
    },
}


# features-sports-service data types — derived (not raw source).
# These are computed by features-sports-service from instruments-service
# outputs (FIXTURES + EVENTS/LINEUPS/STATS/PLAYER_STATS/INJURIES/WEATHER/
# STANDINGS/TM_LEAGUES/PLAYER_VALUES/ODDS/PREDICTIONS/XG/SFI_PROGRESSIVE_STATS)
# and surface in the data-status UI under the ``features-sports-service``
# service tab — NOT under instruments-service SPORTS, because they're a
# different production stage of the pipeline. See SSOT:
# plans/active/features_sports_pipeline_deployment_2026_04_21.md.
#
# Denominator: ``api_football`` per-fixture calendar (FSS can only compute
# features on dates where upstream FIXTURES exist; api_football is the
# gating dependency for the whole denormalisation join).
FEATURES_SPORTS_DATA_TYPE_META: dict[str, dict[str, object]] = {
    "FIXTURE_FEATURES": {
        "source": "api_football",
        "classifications": ("Prediction",),
        "axis": "per_league_per_fixture_date",
        "unit": "fixture_dates",
    },
    "ODDS_FEATURES": {
        # Per-fixture odds-derived features (movement, vig, market signals).
        # Gated on footystats fixture-day calendar — every fixture with
        # footystats odds becomes one ODDS_FEATURES row.
        "source": "footystats",
        "classifications": ("Prediction", "Features"),
        "axis": "per_league_per_fixture_date",
        "unit": "fixture_dates",
    },
    "DERIVED_FEATURES": {
        # Cross-source per-fixture aggregations (form, momentum, h2h).
        # Gated on api_football fixture calendar (same gate as FIXTURE_FEATURES).
        "source": "api_football",
        "classifications": ("Prediction",),
        "axis": "per_league_per_fixture_date",
        "unit": "fixture_dates",
    },
}


# Per-calculator feature-group metadata (Phase 3.E1). Each calculator that
# writes manifest rows under feature_group=<calc> gets a per_feature axis
# entry so the data-status UI can surface honest per-calculator coverage
# rows instead of collapsing all 34 calculators into a single
# DERIVED_FEATURES rollup.
#
# For each calculator: ``source`` is the primary required upstream's source
# (used by `_sports_honest_coverage` for the legacy clip path; the per_feature
# axis branch reads the full FEATURE_UPSTREAM_REQUIREMENTS list to compute
# the proper intersection denominator). ``classifications`` defaults to
# Prediction since features are computed for prediction leagues.
#
# Phase 3 honest-coverage: expected = intersection of every required
# upstream's per-league fixture calendar (clipped to source coverage windows).
# See ``_features_sports_expected_dates_for_calculator``.
FEATURES_SPORTS_PER_CALC_META: dict[str, dict[str, object]] = {
    calc_name: {
        "source": next(
            (r.source for r in reqs if r.required and r.source != "derived"),
            "derived",
        ),
        "classifications": ("Prediction",),
        "axis": "per_feature_per_league_per_fixture_date",
        "unit": "fixture_dates",
    }
    for calc_name, reqs in FEATURE_UPSTREAM_REQUIREMENTS.items()
}


def _features_sports_expected_dates_for_calculator(
    calc_name: str,
    league_id: str,
    start_date: str,
    end_date: str,
) -> list[str]:
    """Per-feature-group expected dates for one (calculator, league).

    Phase 3 honest-coverage: a calculator runs for date D x league L iff every
    *required* upstream is in-coverage on (D, L). The expected denominator
    for the calculator is the intersection of expected-date sets across its
    required upstreams (UAC ``FEATURE_UPSTREAM_REQUIREMENTS``).

    For ``derived``-source upstreams (Stage D dependencies on other
    calculators' outputs) we recurse — the dependent calculator's coverage
    propagates. To prevent runaway recursion on a malformed catalogue the
    walker carries a visited set; a cycle aborts to a "no expected dates"
    answer (caller treats as zero, surfaces in the data-status as a config
    bug).

    Returns the sorted list of dates in [start_date, end_date] where this
    calculator is expected to have a manifest row for ``league_id``.
    Empty if the calculator is unknown OR every date is out of every
    required upstream's coverage (NaN-by-design throughout).
    """

    def _walk(
        name: str,
        visited: frozenset[str],
    ) -> list[str] | None:
        if name in visited:
            # Cycle in the derived-DAG; bail to "unknown" rather than infinite-loop.
            return None
        next_visited = visited | {name}
        reqs = FEATURE_UPSTREAM_REQUIREMENTS.get(name) or FEATURE_UPSTREAM_REQUIREMENTS.get(f"{name}_calculator", [])
        if not reqs:
            return None
        required_reqs = [r for r in reqs if r.required]
        if not required_reqs:
            # Calculator with only optional upstreams — fall back to the
            # league's full fixture calendar.
            return get_league_fixture_calendar(league_id, start_date, end_date)
        intersection: set[str] | None = None
        for req in required_reqs:
            dates = _expected_dates_for_upstream(req, league_id, start_date, end_date, _walk, next_visited)
            if dates is None:
                # Required upstream has no dates we can model; skip silently —
                # the data-status reads from the manifest anyway, and over-
                # counting expected here is worse than under-counting.
                continue
            date_set = set(dates)
            intersection = date_set if intersection is None else intersection & date_set
        if intersection is None:
            return []
        return sorted(intersection)

    out = _walk(calc_name, frozenset())
    return out or []


def _expected_dates_for_upstream(
    req: UpstreamReq,
    league_id: str,
    start_date: str,
    end_date: str,
    walk: Callable[[str, frozenset[str]], list[str] | None],
    visited: frozenset[str],
) -> list[str] | None:
    """Expected dates for a single ``UpstreamReq`` of a feature calculator.

    ``derived`` upstreams recurse via ``walk``. Raw upstreams use
    ``in_coverage`` (date floor + known-gap + league filter) over the
    league's fixture calendar.
    """
    if req.source == "derived":
        # walk(req.data_type, visited) — req.data_type carries the upstream
        # calculator's name when source="derived".
        return walk(req.data_type, visited)

    # League may be out of coverage for this entity (e.g. understat XG
    # for MLS) — but in_coverage's league-check is league_id-only, not
    # date-dependent. Single check at end_date is sufficient.
    if not in_coverage(req.source, req.data_type, league_id, end_date) and not in_coverage(
        req.source, req.data_type, league_id, start_date
    ):
        return []

    # Date-floor clip + known-gap filter via the existing helper. Reuse the
    # league fixture calendar as the candidate set — features only run on
    # fixture days.
    clipped_start, clipped_end = _clip_dates_to_source_coverage(
        req.source, start_date, end_date, data_type=req.data_type or None
    )
    if not clipped_end or clipped_end < clipped_start:
        return []
    candidate = get_league_fixture_calendar(league_id, clipped_start, clipped_end)
    return [
        d
        for d in candidate
        if not _is_in_known_gap(req.source, req.data_type, d) and in_coverage(req.source, req.data_type, league_id, d)
    ]


def _sports_expected_dates_for_league(
    league_id: str,
    axis: str,
    cadence_days: int,
    start_date: str,
    end_date: str,
    source_key: str = "",
    data_type: str = "",
) -> list[str]:
    """Expected capture dates for one league on a given axis.

    - ``per_league_per_fixture_date`` → active-season dates via
      ``get_league_fixture_calendar`` (off-season excluded).
    - ``per_league_periodic`` → active-season dates sampled every
      ``cadence_days`` (cadence=1 gives daily active-season; cadence=7
      gives weekly refreshes).

    Clips ``start_date`` forward to the source's UAC-declared coverage
    start. When ``data_type`` is supplied, applies the per-(source,
    data_type) override from ``DATA_TYPE_COVERAGE_START`` (e.g.
    SFI_PROGRESSIVE_STATS starts 2020-01-01 even though the SFI source
    starts 2019-01-01). Then drops any date that falls inside a registered
    known-coverage-gap window (``KNOWN_COVERAGE_GAPS``) — useful for
    documented provider outages.

    Pass empty strings to skip clipping/gap-filtering (preserves legacy
    callers that don't yet know the source/data_type).
    """
    if source_key:
        start_date, end_date = _clip_dates_to_source_coverage(
            source_key, start_date, end_date, data_type=data_type or None
        )
        if not end_date or end_date < start_date:
            return []
    season_dates = get_league_fixture_calendar(league_id, start_date, end_date)
    if axis == "per_league_per_fixture_date" or cadence_days <= 1:
        result = season_dates
    else:
        result = season_dates[::cadence_days]
    if source_key and data_type:
        result = [d for d in result if not _is_in_known_gap(source_key, data_type, d)]
    return result


def _sports_honest_coverage(
    filtered: pd.DataFrame,
    entity_name: str,
    start_date: str,
    end_date: str,
) -> dict[str, object] | None:
    """Return honest-coverage stats for a SPORTS data_type using the SSOT meta.

    Uses ``SPORTS_DATA_TYPE_META`` + UAC ``get_expected_leagues_for_source``
    + ``get_league_fixture_calendar`` to compute:

      - ``expected_shards`` = len(expected_leagues) * len(expected_dates_per_league)
        (or len(expected_dates) for global axes).
      - ``found_shards`` — distinct (league_id, date) pairs with
        ``capture_status in {captured, empty_confirmed}``. v4 rows without a
        ``capture_status`` column are treated as ``captured``.
      - ``missing_shards`` — per-league date-sets missing from the manifest
        (for UI drill-down).

    Returns ``None`` if the entity isn't in the SSOT map (caller falls back
    to the legacy date-count model).
    """
    meta = (
        SPORTS_DATA_TYPE_META.get(entity_name)
        or FEATURES_SPORTS_DATA_TYPE_META.get(entity_name)
        or FEATURES_SPORTS_PER_CALC_META.get(entity_name)
    )
    if meta is None:
        return None

    axis = str(cast(dict[str, Any], meta)["axis"])  # pyright: ignore[reportAny]
    cadence_days = int(cast(dict[str, Any], meta).get("cadence_days") or 1)
    source_key = str(cast(dict[str, Any], meta)["source"])  # pyright: ignore[reportAny]
    classifications = tuple(cast(tuple[str, ...], cast(dict[str, Any], meta)["classifications"]))

    expected_leagues = get_expected_leagues_for_source(source_key, classifications=list(classifications))

    # skip-worthy = captured | empty_confirmed | expected_unattempted(EXPECTED_* reason).
    # v4 rows without capture_status are implicit ``captured``.
    if "capture_status" in filtered.columns:
        _status_s = filtered["capture_status"].fillna("captured").astype(str)  # pyright: ignore[reportUnknownMemberType]
        _reason_s = (
            filtered["error_reason"].fillna("").astype(str)  # pyright: ignore[reportUnknownMemberType]
            if "error_reason" in filtered.columns
            else pd.Series("", index=filtered.index)
        )
        ok_mask = _status_s.isin(["captured", "empty_confirmed"]) | (
            (_status_s == "expected_unattempted") & _reason_s.str.startswith("EXPECTED_")
        )
    else:
        ok_mask = pd.Series([True] * len(filtered), index=filtered.index)
    if axis == "per_feature_per_league_per_fixture_date":
        # features-sports-service writes manifest rows keyed by
        # feature_group=<calc_name> (NOT by data_type). Match accordingly.
        if "feature_group" in filtered.columns:
            ent_mask = filtered["feature_group"] == entity_name
        else:
            ent_mask = pd.Series([False] * len(filtered), index=filtered.index)
    else:
        ent_mask = filtered["data_type"] == entity_name
    ent_rows = filtered[ent_mask & ok_mask]

    if axis == "per_feature_per_league_per_fixture_date":
        # Phase 3 honest-coverage per-calculator axis. For each league in the
        # expected set, expected dates = intersection of every required
        # upstream's per-league fixture calendar (clipped + known-gap-filtered).
        # ``_features_sports_expected_dates_for_calculator`` walks
        # FEATURE_UPSTREAM_REQUIREMENTS and recurses through derived deps.
        per_league_pf: dict[str, dict[str, object]] = {}
        total_expected_pf = 0
        total_found_pf = 0
        if "league_id" in ent_rows.columns:
            ent_rows_by_league_pf = ent_rows.groupby(ent_rows["league_id"].fillna(""))
        else:
            ent_rows_by_league_pf = None
        for league in expected_leagues:
            lid = league.league_id
            expected_dates_pf = _features_sports_expected_dates_for_calculator(
                calc_name=entity_name,
                league_id=lid,
                start_date=start_date,
                end_date=end_date,
            )
            if not expected_dates_pf:
                continue
            expected_set_pf = set(expected_dates_pf)
            found_set_pf: set[str] = set()
            if ent_rows_by_league_pf is not None and lid in ent_rows_by_league_pf.groups:
                found_set_pf = {str(d) for d in ent_rows_by_league_pf.get_group(lid)["date"].unique()}  # pyright: ignore[reportAny]
            covered_pf = expected_set_pf & found_set_pf
            missing_pf = sorted(expected_set_pf - found_set_pf)
            per_league_pf[lid] = {
                "expected_dates": len(expected_set_pf),
                "found_dates": len(covered_pf),
                "missing_dates": missing_pf[:50],
                "missing_count": len(missing_pf),
            }
            total_expected_pf += len(expected_set_pf)
            total_found_pf += len(covered_pf)
        return {
            "axis": axis,
            "unit": str(meta["unit"]),
            "source": source_key,
            "expected_leagues": [lg.league_id for lg in expected_leagues],
            "found_shards": total_found_pf,
            "expected_shards": total_expected_pf,
            "per_league": per_league_pf,
        }

    if axis in ("global_periodic", "global_season"):
        # No league axis — expected = number of cadence-dates in window,
        # clipped to the source's UAC-declared coverage start so pre-launch
        # dates don't show as missing.
        clipped_start, clipped_end = _clip_dates_to_source_coverage(
            source_key, start_date, end_date, data_type=entity_name
        )
        if not clipped_end or clipped_end < clipped_start:
            date_range = pd.DatetimeIndex([])
        else:
            try:
                date_range = pd.date_range(clipped_start, clipped_end, freq="D")
            except ValueError:
                date_range = pd.DatetimeIndex([])
        expected_dates = 1 if axis == "global_season" else max(1, len(date_range) // max(1, cadence_days))
        found_dates = len({str(d) for d in ent_rows["date"].unique()}) if not ent_rows.empty else 0  # pyright: ignore[reportAny]
        return {
            "axis": axis,
            "unit": str(meta["unit"]),
            "source": source_key,
            "expected_leagues": [],
            "found_shards": min(found_dates, expected_dates),
            "expected_shards": expected_dates,
            "per_league": None,
        }

    # per_league_per_fixture_date / per_league_periodic
    # Single SSOT: per-league subpartition only. Legacy bare-path date-aggregate
    # captures were migrated to per-league via
    # ``instruments-service/scripts/migrate_bare_to_per_league.py`` +
    # ``reconcile_manifest_from_per_league_parquets.py`` (2026-05-01) and the
    # orchestrator now writes per-league exclusively for league-axis data
    # types. No bare-path fallback in coverage accounting.
    per_league: dict[str, dict[str, object]] = {}
    total_expected = 0
    total_found = 0
    ent_rows_by_league = ent_rows.groupby(ent_rows["league_id"].fillna("")) if "league_id" in ent_rows.columns else None

    # Bucket-based week match for periodic cadences (2026-05-05 fix):
    # The orchestrator writes manifest rows for every active-season fixture
    # date (e.g. ~190 days/year for EPL), but ``_sports_expected_dates_for_league``
    # subsamples to ``[::cadence_days]`` (every 7th element = ~27/year for
    # cadence=7). A direct ``expected_set & found_set`` intersection then
    # only counted manifest rows that happened to land on those exact 27
    # subsampled positions — capping TM_LEAGUES at ~50%, SFI_LEAGUES at ~14%,
    # PLAYER_VALUES at ~5% no matter how complete the manifest was.
    #
    # Fix: for periodic cadences (cadence_days > 1), bucket each expected
    # date into a window of ``cadence_days`` and credit a bucket as covered
    # if the manifest has ANY row for that league within the window. For
    # per_league_per_fixture_date (cadence_days <= 1) the original exact-
    # match semantics still apply.
    use_bucket_match = axis == "per_league_periodic" and cadence_days > 1

    for league in expected_leagues:
        lid = league.league_id
        expected_dates_for_l = _sports_expected_dates_for_league(
            lid,
            axis,
            cadence_days,
            start_date,
            end_date,
            source_key=source_key,
            data_type=entity_name,
        )
        if not expected_dates_for_l:
            continue
        expected_set = set(expected_dates_for_l)
        found_set: set[str] = set()
        if ent_rows_by_league is not None and lid in ent_rows_by_league.groups:
            found_set = {str(d) for d in ent_rows_by_league.get_group(lid)["date"].unique()}  # pyright: ignore[reportAny]

        if use_bucket_match:
            # Each expected date anchors a bucket of [d, d+cadence_days). A
            # bucket is "covered" if any manifest date falls inside it.
            sorted_expected = sorted(expected_set)
            sorted_found = sorted(found_set)
            covered_buckets: set[str] = set()
            missing_buckets: set[str] = set()
            f_idx = 0
            for anchor in sorted_expected:
                # Bucket window: [anchor, next_anchor) — last bucket gets cadence_days width.
                bucket_end = pd.Timestamp(anchor) + pd.Timedelta(days=cadence_days)
                hit = False
                while f_idx < len(sorted_found) and sorted_found[f_idx] < anchor:
                    f_idx += 1
                probe = f_idx
                while probe < len(sorted_found) and pd.Timestamp(sorted_found[probe]) < bucket_end:
                    hit = True
                    probe += 1
                (covered_buckets if hit else missing_buckets).add(anchor)
            per_league[lid] = {
                "found_shards": len(covered_buckets),
                "expected_shards": len(expected_set),
                "missing_shards": len(missing_buckets),
                "completion_pct": round(len(covered_buckets) / max(1, len(expected_set)) * 100, 2),
                "unit": str(meta["unit"]),
                "missing_dates": sorted(missing_buckets)[:500],
                "found_dates_list": sorted(covered_buckets)[:500],
            }
            total_expected += len(expected_set)
            total_found += len(covered_buckets)
        else:
            covered = expected_set & found_set
            per_league[lid] = {
                "found_shards": len(covered),
                "expected_shards": len(expected_set),
                "missing_shards": len(expected_set - found_set),
                "completion_pct": round(len(covered) / max(1, len(expected_set)) * 100, 2),
                "unit": str(meta["unit"]),
                "missing_dates": sorted(expected_set - found_set)[:500],
                "found_dates_list": sorted(covered)[:500],
            }
            total_expected += len(expected_set)
            total_found += len(covered)

    return {
        "axis": axis,
        "unit": str(meta["unit"]),
        "source": source_key,
        "expected_leagues": [lg.league_id for lg in expected_leagues],
        "found_shards": total_found,
        "expected_shards": total_expected,
        "per_league": per_league,
    }


# ── MTDS honest-coverage meta (Phase 6c) ─────────────────────────────────────
#
# SSOT: ``codex/02-data/mtds-data-source-coverage-matrix.md``.
#
# For each MTDS category, ``MTDS_CATEGORY_META`` declares:
#   - ``venue_accessor``: attribute name on ``VenueMapping`` returning the list
#     of UAC-declared venues for that category (e.g. ``all_cefi_venues``).
#   - ``axis``: coverage axis for the category (``per_venue_per_data_type_daily``
#     for CEFI/TRADFI/PREDICTION, ``per_venue_per_data_type_per_chain_daily``
#     for DEFI, ``per_league_per_bookmaker_per_fixture_date`` for SPORTS).
#   - ``tradfi_tick_gate``: only True for TRADFI — applies
#     ``is_in_tradfi_tick_window`` to filter expected dates for tick-only
#     data_types.
#   - ``record_empty_expected``: whether adapters in this category should emit
#     ``capture_status=empty_confirmed`` (informational; not enforced here).
#   - ``unit``: display unit for the per-category response.
#
# SPORTS entry is present for completeness; the MTDS sports branch
# (bookmaker odds per league x per fixture-date) is a Phase 6d follow-up —
# the MTDS honest-coverage helper currently returns ``None`` for SPORTS so
# the existing SPORTS code path (instruments-service ``_sports_honest_coverage``)
# keeps running. See §5 of the codex SSOT.
MTDS_CATEGORY_META: dict[str, dict[str, object]] = {
    "CEFI": {
        "venue_accessor": "all_cefi_venues",
        "axis": "per_venue_per_data_type_daily",
        "tradfi_tick_gate": False,
        "record_empty_expected": True,
        "unit": "shard_days",
    },
    "TRADFI": {
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
# enumeration loop ``_mtds_honest_coverage_for_venue`` would surface only
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
# ``_mtds_expected_dates_for_venue_dt``.
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


def _is_mtds_honest_coverage_target(service: str, category: str) -> bool:
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
_DEFI_DATA_TYPE_ALIASES: dict[str, str] = {
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
_DEFI_SOURCE_TO_DATA_TYPE: dict[str, str] = {
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


def _canonicalise_defi_data_types(filtered: pd.DataFrame) -> pd.DataFrame:
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
      ``_DEFI_DATA_TYPE_ALIASES``.

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
            inferred = out["_defi_source"].fillna("").astype(str).map(_DEFI_SOURCE_TO_DATA_TYPE).fillna("")  # pyright: ignore[reportUnknownMemberType]
            out.loc[blank_dt, "data_type"] = inferred[blank_dt]
    # Case (2): map hyphenated DEFI data_types to canonical underscore form.
    out["data_type"] = out["data_type"].fillna("").astype(str).replace(_DEFI_DATA_TYPE_ALIASES)  # pyright: ignore[reportUnknownMemberType]
    return out


def _mtds_expected_venues(cat: str, venue_mapping: VenueMapping) -> list[str]:
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


# ProcessPool toggle. Hardcoded False — set to True here only as a temporary
# rollback if a deployment hits subtle pickling / fork issues. Workspace rule
# bans os.environ access in service source (use UnifiedCloudConfig for any
# real runtime toggles).
_PROCESS_POOL_DISABLED = False


def _build_category_in_subprocess(
    service: str,
    cat: str,
    start_date: str,
    end_date: str,
    all_date_strs: list[str],
    total_days: int,
    cloud: str = "gcp",
) -> dict[str, object]:
    """ProcessPool worker — build one category's manifest entry in a forked child.

    Each call runs in a child process forked from the parent gunicorn worker
    just before the request fans out. Fork start-method means the child
    inherits everything the parent had loaded — most importantly the
    module-level ``_INDEX_CACHE`` dict, so the child does NOT re-read the
    manifest from GCS. Copy-on-write semantics keep memory cheap until the
    child mutates a shared page.

    Returns the per-category result dict that the parent's serial path
    would have produced. Internal counters (``_venue_found`` /
    ``_venue_expected``) are kept on the dict for the parent to drain into
    the overall totals.
    """
    # Construct a fresh DataStatusService inside the child. Cheap — it's
    # just lookup-table init; no GCS, no network. Avoids pickling the
    # parent's instance through the Pipe.
    dss = DataStatusService()
    venue_mapping = VenueMapping()
    return dss._build_manifest_category(  # type: ignore[reportPrivateUsage]
        service,
        cat,
        start_date,
        end_date,
        all_date_strs,
        total_days,
        venue_mapping,
        cloud=cloud,
    )


# Process-shared VenueMapping for the lru_cache'd expected-dates helper.
# UAC venue/calendar/coverage data is process-immutable (read once on import),
# so a single shared instance is safe and avoids per-request VenueMapping
# instantiation cost when fanning out across ~30-50 venues x ~8 data_types.
_shared_venue_mapping_instance: VenueMapping | None = None


def _shared_venue_mapping() -> VenueMapping:
    global _shared_venue_mapping_instance
    if _shared_venue_mapping_instance is None:
        _shared_venue_mapping_instance = VenueMapping()
    return _shared_venue_mapping_instance


@lru_cache(maxsize=8192)
def _mtds_expected_dates_cached(
    venue: str,
    data_type: str,
    category: str,
    window_start: str,
    window_end: str,
) -> frozenset[str]:
    """Process-level cache for :func:`_mtds_expected_dates_for_venue_dt`.

    The expected-dates set is a pure function of UAC config (venue start
    dates, trading calendars, coverage windows, LST genesis dates) —
    process-immutable. Caching it avoids rebuilding the same trading-day
    calendar 8x per venue (once per data_type) on every data-status
    request. Cleared on ``/turbo/clear`` for defensive freshness.
    """
    venue_mapping = _shared_venue_mapping()
    venue_start = venue_mapping.get_venue_start_date(venue) or window_start
    dt_start = get_venue_data_type_start_date(venue, data_type) or venue_start

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
        from unified_api_contracts.registry.chain_env import (
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
        elif data_type in _TRADFI_TICK_ONLY_DATA_TYPES:
            expected_list = [d for d in expected_list if is_in_tradfi_tick_window(d)]
        return frozenset(expected_list)

    expected_list = venue_mapping.get_expected_trading_dates(venue, effective_start, window_end)
    if expected_list:
        return frozenset(expected_list)
    return frozenset(d.strftime("%Y-%m-%d") for d in pd.date_range(effective_start, window_end, freq="D"))


def _mtds_expected_dates_for_venue_dt(
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

    Delegates to :func:`_mtds_expected_dates_cached` — UAC config is
    process-immutable so caching by (venue, data_type, category, window) is
    safe; the ``venue_mapping`` arg is preserved for signature compatibility
    with existing callers and tests.
    """
    return set(_mtds_expected_dates_cached(venue, data_type, category, window_start, window_end))


def _per_instrument_coverage(
    venue_df_ok: pd.DataFrame,
    venue: str,
    dt: str,
    expected_dates: set[str],
    cap: int,
) -> dict[str, object]:
    """Phase 8D — compute the per-(instrument_id, date) denominator for a
    per-instrument shard ``data_type``.

    Extracted into its own helper so the parent
    :func:`_mtds_honest_coverage_for_venue` stays under ruff C901 and so
    the Tier-3 branch is trivially unit-testable without re-plumbing the
    full manifest DataFrame.

    Mirrors the sentinel fan-out landed in MTDS orchestrator commit
    ``2947dd2``: expected denominator = ``|instruments| x |dates|`` where
    ``instruments`` comes from :func:`get_expected_instruments_for_venue`
    (with ``instruments_provider=None`` so UAC falls back to its MVP seed
    tables). The found set counts distinct ``(instrument_id, date)``
    tuples in ``venue_df_ok`` whose ``instrument_id`` is non-empty.

    **Legacy-row fallback:** if the manifest has (venue, dt) rows that
    land with an empty ``instrument_id`` (pre-Phase-8C writes), we degrade
    to the venue-level per-(venue, dt, date) denominator for that (venue,
    dt) pair so coverage % doesn't regress on already-shipped backfills.
    The degraded response is annotated with ``legacy_row_count``.

    Parameters
    ----------
    venue_df_ok:
        Pre-filtered manifest rows for this venue, gated on
        ``capture_status in {captured, empty_confirmed}``.
    venue:
        Canonical MTDS venue key.
    dt:
        Per-instrument shard data_type (caller MUST verify via
        :func:`is_per_instrument_shard_data_type`).
    expected_dates:
        Pre-computed expected date set from
        :func:`_mtds_expected_dates_for_venue_dt`.
    cap:
        Hard ceiling on the returned instrument universe size. Passed to
        UAC; the Phase 8 MVP default is
        :data:`_DEFAULT_PER_INSTRUMENT_SENTINEL_CAP` (50).

    Returns
    -------
    dict[str, object]
        ``{"expected_shards", "found_shards", "missing_shards",
        "completion_pct", "missing_dates", "dates_found_list", "unit",
        "expected_instruments", "missing_instruments", "per_instrument"?,
        "legacy_row_count"?}``. ``per_instrument`` is only emitted when
        the instrument universe size is below
        :data:`_PER_INSTRUMENT_BREAKDOWN_MAX_SIZE` (keeps response bloat
        bounded on big perp boards).
    """
    expected_instruments = get_expected_instruments_for_venue(
        venue,
        dt,
        instruments_provider=None,
        cap=cap,
    )

    # Slice to the (venue, dt) rows once.
    if "data_type" in venue_df_ok.columns and not venue_df_ok.empty:
        dt_rows = venue_df_ok[venue_df_ok["data_type"] == dt]
    else:
        dt_rows = venue_df_ok.iloc[0:0]

    has_instrument_col = "instrument_id" in dt_rows.columns
    instrument_series = (
        dt_rows["instrument_id"].fillna("").astype(str) if has_instrument_col else pd.Series([], dtype=str)  # pyright: ignore[reportUnknownMemberType]
    )
    date_series = dt_rows["date"].astype(str) if "date" in dt_rows.columns else pd.Series([], dtype=str)  # pyright: ignore[reportUnknownMemberType]

    if has_instrument_col and len(dt_rows) > 0:
        # Rows that land with empty ``instrument_id`` predate Phase-8C
        # fan-out. Count them separately so we can fall back to the
        # venue-level denominator when the (venue, dt) slice is fully
        # legacy.
        legacy_mask = instrument_series.str.strip() == ""  # pyright: ignore[reportUnknownMemberType]
        legacy_row_count = int(legacy_mask.sum())  # pyright: ignore[reportUnknownMemberType]
        non_legacy_mask = ~legacy_mask
        non_legacy_instr = instrument_series[non_legacy_mask]
        non_legacy_dates = date_series[non_legacy_mask]
    else:
        legacy_row_count = len(dt_rows)
        non_legacy_instr = pd.Series([], dtype=str)
        non_legacy_dates = pd.Series([], dtype=str)

    # Legacy-row fallback: the aggregator hasn't seen any Phase-8C rows
    # for this (venue, dt) yet -- preserve the prior per-(venue, dt, date)
    # denominator so historical backfills don't regress in the UI.
    if legacy_row_count > 0 and len(non_legacy_instr) == 0:
        found_dates_set = {str(d) for d in date_series.unique() if str(d)}  # pyright: ignore[reportAny]
        found_in_expected = found_dates_set & expected_dates
        missing_dates = sorted(expected_dates - found_dates_set)
        expected_count = len(expected_dates)
        found_count = len(found_in_expected)
        return {
            "expected_shards": expected_count,
            "found_shards": found_count,
            "missing_shards": max(0, expected_count - found_count),
            "completion_pct": min(round(found_count / max(1, expected_count) * 100, 2), 100.0),
            "missing_dates": missing_dates[:500],
            "dates_found_list": sorted(found_in_expected)[:500],
            "unit": "shard_days_legacy",
            "expected_instruments": list(expected_instruments),
            "missing_instruments": list(expected_instruments),
            "legacy_row_count": legacy_row_count,
        }

    # Phase 8D Tier-3 denominator.
    # Vectorised: build the (instrument_id, date) pair set with pandas mask
    # operations rather than a Python ``for/zip`` loop. For BINANCE-FUTURES
    # at 50 perps x ~3000 dates the prior loop iterated 150k times per
    # (venue, dt) pair; the masked path is dominated by the C-level isin().
    found_dates_in_window: set[str] = set()
    found_iid_dates_zipped: list[tuple[str, str]] = []
    if len(non_legacy_instr) > 0:
        iid_str = non_legacy_instr.astype(str).str.strip()
        rd_str = non_legacy_dates.astype(str)
        mask = (iid_str.str.len() > 0) & rd_str.isin(expected_dates)
        if bool(mask.any()):
            iid_kept = iid_str[mask].tolist()
            rd_kept = rd_str[mask].tolist()
            found_iid_dates_zipped = list(zip(iid_kept, rd_kept, strict=True))
            found_dates_in_window = set(rd_kept)
    found_pairs: set[tuple[str, str]] = set(found_iid_dates_zipped)

    n_instruments = len(expected_instruments)
    n_dates = len(expected_dates)
    expected_count = n_instruments * n_dates
    found_count = len(found_pairs)

    # Counter does both the per-instrument count AND gives us
    # ``instruments_with_shards`` as ``.keys()`` in one pass — replaces the
    # prior O(|instruments|*|pairs|) ``sum(1 for ...)`` loop below.
    iid_counts = Counter(iid for iid, _ in found_pairs)
    instruments_with_shards = set(iid_counts)
    missing_instruments = [iid for iid in expected_instruments if iid not in instruments_with_shards]

    entry: dict[str, object] = {
        "expected_shards": expected_count,
        "found_shards": found_count,
        "missing_shards": max(0, expected_count - found_count),
        "completion_pct": min(round(found_count / max(1, expected_count) * 100, 2), 100.0),
        # Missing-dates at the dt level collapses across instruments so the
        # drill-down stays backwards-compatible with the venue-level UI panel.
        "missing_dates": sorted(expected_dates - found_dates_in_window)[:500],
        "dates_found_list": sorted(found_dates_in_window)[:500],
        "unit": "shard_instrument_days",
        "expected_instruments": list(expected_instruments),
        "missing_instruments": missing_instruments,
    }

    if legacy_row_count > 0:
        # Some Phase-8C rows + some legacy rows coexist -- keep the Tier-3
        # denominator (authoritative) but surface the legacy-row count so
        # the UI can display a migration-in-progress badge.
        entry["legacy_row_count"] = legacy_row_count

    # Only inline the per-instrument breakdown on venues with a small
    # universe (<20). BINANCE-FUTURES with 50 perps would double the
    # response size per (venue, dt) pair otherwise.
    if n_instruments and n_instruments < _PER_INSTRUMENT_BREAKDOWN_MAX_SIZE:
        per_instrument: dict[str, dict[str, object]] = {
            iid: {
                "found": iid_counts.get(iid, 0),
                "expected": n_dates,
                "completion_pct": min(round(iid_counts.get(iid, 0) / max(1, n_dates) * 100, 2), 100.0),
            }
            for iid in expected_instruments
        }
        entry["per_instrument"] = per_instrument

    return entry


def _mtds_honest_coverage_for_venue(
    filtered: pd.DataFrame,
    venue: str,
    category: str,
    window_start: str,
    window_end: str,
    venue_mapping: VenueMapping,
) -> dict[str, object]:
    """Honest-coverage rollup for one ``(category, venue)`` pair.

    For each UAC-declared data_type on this venue, compute:
      - expected_dates: from ``_mtds_expected_dates_for_venue_dt`` (honest
        per-(venue, dt) window with TRADFI tick-window gate applied).
      - found_dates: distinct dates in ``filtered`` where
        ``(venue, data_type)`` matches AND ``capture_status in
        {captured, empty_confirmed}`` (v4 rows without capture_status are
        implicit ``captured``, same convention as ``_sports_honest_coverage``).

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
    expected_dts = list(get_expected_data_types_for_venue(venue))
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

    for dt in sorted(expected_dts):
        expected_dates = _mtds_expected_dates_for_venue_dt(venue_mapping, venue, dt, category, window_start, window_end)

        if is_per_instrument_shard_data_type(dt):
            # Phase 8D Tier-3 branch — per-(venue, dt, instrument_id,
            # date) denominator.
            dt_entry = _per_instrument_coverage(
                venue_df_ok,
                venue,
                dt,
                expected_dates,
                _DEFAULT_PER_INSTRUMENT_SENTINEL_CAP,
            )
            dt_entries[dt] = dt_entry
            expected_count = int(cast(int, dt_entry["expected_shards"]))
            found_count = int(cast(int, dt_entry["found_shards"]))
        else:
            # Venue-level dt — preserve the Phase 6d per-(venue, dt, date)
            # denominator.
            if "data_type" in venue_df_ok.columns:
                dt_rows = venue_df_ok[venue_df_ok["data_type"] == dt]
                found_dates_set = {str(d) for d in dt_rows["date"].unique()}  # pyright: ignore[reportAny]
            else:
                found_dates_set: set[str] = set()
            # Only count dates that fall inside the expected window — a
            # row from before ``effective_start`` should not inflate
            # ``found_shards``.
            found_in_expected = found_dates_set & expected_dates  # pyright: ignore[reportUnknownVariableType]
            missing_dates = sorted(expected_dates - found_dates_set)  # pyright: ignore[reportUnknownVariableType]
            expected_count = len(expected_dates)
            found_count = len(found_in_expected)  # pyright: ignore[reportUnknownVariableType]

            dt_entries[dt] = {
                "expected_shards": expected_count,
                "found_shards": found_count,
                "missing_shards": max(0, expected_count - found_count),
                "completion_pct": min(round(found_count / max(1, expected_count) * 100, 2), 100.0),
                "missing_dates": missing_dates[:500],
                "dates_found_list": sorted(found_in_expected)[:500],  # pyright: ignore[reportUnknownVariableType]
                "unit": "shard_days",
            }

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


def _distinct_pairs(df: pd.DataFrame, col_a: str, col_b: str) -> int:
    """Return count of distinct non-empty (col_a, col_b) pairs. 0 if cols missing."""
    if col_a not in df.columns or col_b not in df.columns:
        return 0
    pairs = {
        (str(a), str(b))  # pyright: ignore[reportAny]
        for a, b in zip(df[col_a].tolist(), df[col_b].tolist(), strict=True)  # pyright: ignore[reportAny]
        if a and b and str(a).strip() and str(b).strip()  # pyright: ignore[reportAny]
    }
    return len(pairs)


def _distinct_values(df: pd.DataFrame, col: str) -> int:
    """Return count of distinct non-empty values in col. 0 if col missing."""
    if col not in df.columns:
        return 0
    return len({str(v) for v in df[col].tolist() if v and str(v).strip()})  # pyright: ignore[reportAny]


def _sports_attempt_count(filtered: pd.DataFrame) -> int:
    """Pick the most specific sports attempt axis available in the manifest."""
    n = _distinct_pairs(filtered, "league_id", "fixture_type")
    if n:
        return n
    n = _distinct_values(filtered, "league_id")
    if n:
        return n
    n = _distinct_pairs(filtered, "venue", "instrument_type")
    if n:
        return n
    return _distinct_values(filtered, "venue")


_CAPTURE_STATUS_COL = "capture_status"
_CAPTURE_STATUS_CAPTURED = "captured"
_CAPTURE_STATUS_EMPTY = "empty_confirmed"
_CAPTURE_STATUS_FAILED = "attempted_failed"
_EXPECTED_REASON_PREFIX = "EXPECTED_"


def _compute_capture_status_counts(df: pd.DataFrame) -> CaptureStatusCounts:
    """Bucket manifest rows by ``capture_status`` (UTL v5 column) into a 5-field CaptureStatusCounts.

    Legacy rows (pre-Phase-A parquet, no ``capture_status`` column, or NaN
    values inside a mixed DataFrame) coerce to ``"captured"`` — matches the
    legacy-read semantics of ``ManifestWriter.lookup`` in UTL.
    ``expected_unattempted`` rows are split by ``error_reason``:
    - EXPECTED_* prefix → ``expected_unattempted_known_empty`` (skip-worthy)
    - other → ``expected_unattempted_pending_fetch`` (retry)
    """
    if df.empty:
        return CaptureStatusCounts()
    if _CAPTURE_STATUS_COL not in df.columns:
        return CaptureStatusCounts(captured=len(df))
    series = df[_CAPTURE_STATUS_COL].fillna(_CAPTURE_STATUS_CAPTURED).astype(str).str.lower()
    reason_col = df["error_reason"].astype(str) if "error_reason" in df.columns else pd.Series("", index=df.index)
    eu_mask = series == "expected_unattempted"
    known_empty = 0
    pending_fetch = 0
    if eu_mask.any():
        eu_reasons = reason_col[eu_mask]
        known_empty = int(eu_reasons.str.startswith(_EXPECTED_REASON_PREFIX).sum())  # pyright: ignore[reportUnknownMemberType]
        pending_fetch = int((~eu_reasons.str.startswith(_EXPECTED_REASON_PREFIX)).sum())  # pyright: ignore[reportUnknownMemberType]
    return CaptureStatusCounts(
        captured=int(
            (
                (series == _CAPTURE_STATUS_CAPTURED)
                | ~series.isin(
                    [_CAPTURE_STATUS_CAPTURED, _CAPTURE_STATUS_EMPTY, _CAPTURE_STATUS_FAILED, "expected_unattempted"]
                )
            ).sum()
        ),
        empty_confirmed=int((series == _CAPTURE_STATUS_EMPTY).sum()),
        attempted_failed=int((series == _CAPTURE_STATUS_FAILED).sum()),
        expected_unattempted_known_empty=known_empty,
        expected_unattempted_pending_fetch=pending_fetch,
    )


# ---------------------------------------------------------------------------
# Per-pillar failure breakdown (writegate Phase 4.A item 1)
# ---------------------------------------------------------------------------
#
# The ``capture_status=attempted_failed`` rows carry a free-form
# ``error_reason`` string set by the writer to ``repr(typed_error)``. To give
# operators (deployment-ui DataStatusTab) per-pillar visibility instead of one
# opaque "failure_rate" gauge, we bucket these strings by typed-error class
# name into a fixed taxonomy.
#
# Each entry maps a UTL/MTDS typed-error class name to the manifest-row
# breakdown column the UI binds. New typed-error classes ship over time
# (``NanRatioExceededError``, ``SchemaMismatchError``, etc. per the writegate
# plan); add them here in the same change as the typed-error class lands.
# Anything unrecognised falls into ``failed_other`` so we don't silently drop
# a new failure mode from operator visibility.

_FAILURE_PILLAR_BY_ERROR_PREFIX: dict[str, str] = {
    "UpstreamTimestampBiasError": "failed_timestamp_bias",
    "MalformedTickFieldError": "failed_malformed",
    "ClusterCoverageError": "failed_cluster",
    "MissingClusterValidationError": "failed_cluster",
    "LookaheadBiasError": "failed_lookahead_bias",
}

_FAILURE_PILLAR_KEYS: tuple[str, ...] = (
    "failed_timestamp_bias",
    "failed_malformed",
    "failed_cluster",
    "failed_lookahead_bias",
    "failed_nan_ratio",  # placeholder — class lands in writegate Phase 1A.future
    "failed_schema",  # placeholder — class lands in writegate Phase 1A.future
    "failed_empty_placeholder_backfill",  # placeholder — reconciler error
    "failed_missing_available_at",  # placeholder — write-time guard
    "failed_other",  # catch-all for unrecognised reprs
)


# ---------------------------------------------------------------------------
# Per-empty-reason breakdown (writegate Phase 4.A — empty_confirmed taxonomy).
# ---------------------------------------------------------------------------
#
# Companion to ``_compute_failure_pillar_counts``. Where pillars bucket
# ``attempted_failed`` rows by typed-error class, this rolls up
# ``empty_confirmed`` rows by their ``error_reason`` — the closed taxonomy
# from ``unified_api_contracts.canonical.crosscutting.honest_coverage.EMPTY_CONFIRMED_REASONS``
# stamped by Tier 3D.1 reconciler (existing rows) + Tier 3D.2 reader-side
# fallback + Tier 2.E.2 writer-side ``record_expected_empty(reason=...)``
# + Tier 3B sports ``record_empty(reason=SOURCE_RETURNED_ZERO)``.
#
# Without this rollup, the Phase 2.E + Phase 3.D + Phase 3.B work that
# stamps typed reasons on every empty_confirmed row stays invisible to the
# operator — the UI would see "X empty_confirmed shards" with no breakdown
# of WHY (calendar holiday vs paused league vs source returned zero vs
# pre-genesis chain). This rollup lets the data-status panel render a
# stacked-bar of empty reasons next to the failure-pillars stack.

# Closed-set keys exact-match the EMPTY_CONFIRMED_REASONS taxonomy plus a
# ``empty_unclassified`` catch-all for legacy null-reason rows that haven't
# been re-stamped yet by the Tier 3D.1 reconciler. Once the back-fill
# completes for an asset_group, the catch-all should drop to zero — its
# count is a cheap progress indicator for the back-fill rollout.
_EMPTY_REASON_KEYS: tuple[str, ...] = (
    "EXPECTED_HOLIDAY",
    "EXPECTED_WEEKEND",
    "EXPECTED_PAUSED_LEAGUE",
    "EXPECTED_PRE_SOURCE_COVERAGE_START",
    "EXPECTED_PRE_GENESIS_CHAIN",
    "EXPECTED_PRE_VENUE_LAUNCH",
    "EXPECTED_INSTRUMENT_NOT_LISTED",
    "EXPECTED_INSTRUMENT_DELISTED",
    "EXPECTED_PARTIAL_HALF_DAY",
    "EXPECTED_OUTSIDE_TRADING_HOURS",
    "EXPECTED_OUTSIDE_TRANSFER_WINDOW",
    "EXPECTED_PRE_SEASON",
    "EXPECTED_POST_SEASON",
    "EXPECTED_SOURCE_DOES_NOT_COVER_LEAGUE",
    "EXPECTED_REFDATA_CADENCE_CHANGE",
    "EXPECTED_DEPRECATED_DATA_TYPE",
    "EXPECTED_KNOWN_SOURCE_GAP",
    "EXPECTED_UPSTREAM_EMPTY",
    "EXPECTED_OUT_OF_COVERAGE_WINDOW",
    "EXPECTED_FIXTURE_CANCELLED",
    "EXPECTED_FIXTURE_POSTPONED",
    "EXPECTED_NO_FIXTURE",
    "EXPECTED_NO_MAPPING",
    "EXPECTED_OUTSIDE_PROCESSING_SCOPE",
    "EXPECTED_LEGACY_MIGRATION_MISSING_EXPIRY",
    "EXPECTED_NO_FUNDING_RATE_TICKS",
    "EXPECTED_NO_PNL_STREAM",
    "EXPECTED_PROTOCOL_PAUSED",
    "EXPECTED_PAST_SOURCE_COVERAGE_END",
    "SOURCE_RETURNED_ZERO",
    "NO_INPUT_AVAILABLE",
    "LEG_ABSENT_LEFT",
    "LEG_ABSENT_RIGHT",
    "empty_unclassified",  # legacy rows pre-Tier-3D.1 back-fill
)


def _compute_empty_reason_counts(df: pd.DataFrame) -> dict[str, int]:
    """Bucket ``empty_confirmed`` rows by ``error_reason`` per the closed taxonomy.

    Args:
        df: Manifest slice — typically a venue or category sub-frame.

    Returns:
        ``{empty_reason: count}`` for every key in ``_EMPTY_REASON_KEYS``.
        Reasons with zero matches are included with count 0 so the UI can
        render the full grid without conditional checks.

    Empty rows whose ``error_reason`` doesn't match any registered closed-set
    member fall into ``empty_unclassified`` rather than being silently
    dropped — this counts the legacy rows that pre-date the Tier 3D.1
    reconciler back-fill and surfaces back-fill progress to the operator.
    Empty rows with NULL/blank ``error_reason`` (Tier 3D.1 hasn't reached
    them yet) also land in ``empty_unclassified``.
    """
    out: dict[str, int] = dict.fromkeys(_EMPTY_REASON_KEYS, 0)
    if df.empty or _CAPTURE_STATUS_COL not in df.columns:
        return out
    empty_mask = (
        df[_CAPTURE_STATUS_COL].fillna(_CAPTURE_STATUS_CAPTURED).astype(str).str.lower() == _CAPTURE_STATUS_EMPTY
    )
    if not bool(empty_mask.any()):
        return out
    if "error_reason" not in df.columns:
        # Whole slice is legacy null-reason rows.
        out["empty_unclassified"] = int(empty_mask.sum())
        return out
    reasons = df.loc[empty_mask, "error_reason"].fillna("").astype(str).str.strip()
    known = set(_EMPTY_REASON_KEYS) - {"empty_unclassified"}
    for reason in reasons:
        if reason in known:
            out[reason] += 1
        else:
            # Empty string, NaN-coerced "", or unrecognised value → unclassified.
            out["empty_unclassified"] += 1
    return out


def _compute_failure_pillar_counts(df: pd.DataFrame) -> dict[str, int]:
    """Bucket ``attempted_failed`` rows by typed-error class prefix.

    Args:
        df: Manifest slice — typically a venue or category sub-frame.

    Returns:
        ``{pillar_key: count}`` for every key in ``_FAILURE_PILLAR_KEYS``.
        Pillars with zero matches are included with count 0 so the UI can
        render the full grid without conditional checks.

    Failed rows whose ``error_reason`` doesn't match any registered prefix
    fall into ``failed_other`` rather than being silently dropped — this
    catches future typed-error classes that ship before this taxonomy is
    extended, surfacing them as "unclassified failures" in the UI.
    """
    out: dict[str, int] = dict.fromkeys(_FAILURE_PILLAR_KEYS, 0)
    if df.empty or _CAPTURE_STATUS_COL not in df.columns:
        return out
    failed_mask = (
        df[_CAPTURE_STATUS_COL].fillna(_CAPTURE_STATUS_CAPTURED).astype(str).str.lower() == _CAPTURE_STATUS_FAILED
    )
    if not bool(failed_mask.any()):
        return out
    if "error_reason" not in df.columns:
        # All failures are unclassified.
        out["failed_other"] = int(failed_mask.sum())
        return out
    failed_reasons = df.loc[failed_mask, "error_reason"].fillna("").astype(str)
    for reason in failed_reasons:
        matched = False
        for prefix, pillar in _FAILURE_PILLAR_BY_ERROR_PREFIX.items():
            if reason.startswith(prefix):
                out[pillar] += 1
                matched = True
                break
        if not matched:
            out["failed_other"] += 1
    return out


def _derive_capture_status_rates(
    counts: CaptureStatusCounts,
    total_expected_cells: int,
) -> dict[str, float | int]:
    """Turn capture_status counts + expected-cells denominator into rates.

    ``attempt_coverage_pct`` / ``capture_coverage_pct`` are rounded to 2 dp
    and clamped to 100 so malformed denominators don't produce >100% figures.
    ``empty_rate`` / ``failure_rate`` are rounded to 4 dp and clamped to
    ``[0, 1]``.  Returns 0.0 for all rates when ``total_expected_cells`` is
    0 so callers always get a well-formed dict.
    ``honest_coverage`` uses the canonical 5-field formula (numerator =
    captured + empty_confirmed + expected_unattempted_known_empty).
    """
    captured = counts.captured
    empty = counts.empty_confirmed
    failed = counts.attempted_failed
    attempted = captured + empty + failed
    denom = max(1, int(total_expected_cells))
    attempted_denom = max(1, attempted)
    return {
        "captured_count": captured,
        "empty_confirmed_count": empty,
        "attempted_failed_count": failed,
        "attempted_total": attempted,
        "honest_coverage": round(compute_honest_coverage(counts), 6),
        "attempt_coverage_pct": min(round(attempted / denom * 100, 2), 100.0) if total_expected_cells > 0 else 0.0,
        "capture_coverage_pct": min(round(captured / denom * 100, 2), 100.0) if total_expected_cells > 0 else 0.0,
        "empty_rate": max(0.0, min(1.0, round(empty / attempted_denom, 4))),
        "failure_rate": max(0.0, min(1.0, round(failed / attempted_denom, 4))),
    }


def _build_failure_rate_by_dimension(
    venues_dict: dict[str, object],
) -> dict[str, dict[str, float | int]]:
    """Project the ``venues_dict`` into a {venue: {failure_rate, attempted_failed_count}} map.

    Only includes venues whose ``capture_status_counts.attempted_failed`` is
    strictly positive, so the UI can bind the "show only failures" filter
    without walking the full per-venue tree client-side.
    """
    out: dict[str, dict[str, float | int]] = {}
    for venue_name, venue_entry_raw in venues_dict.items():
        if not isinstance(venue_entry_raw, dict):
            continue
        venue_entry = cast(dict[str, object], venue_entry_raw)
        venue_failure_rate_raw = venue_entry.get("failure_rate", 0.0)
        venue_failure_rate = float(venue_failure_rate_raw) if isinstance(venue_failure_rate_raw, (int, float)) else 0.0
        v_counts_raw = venue_entry.get("capture_status_counts", {})
        v_counts = cast(dict[str, int], v_counts_raw) if isinstance(v_counts_raw, dict) else {}
        failed_count = int(v_counts.get("attempted_failed", 0) or 0)
        if failed_count > 0:
            out[str(venue_name)] = {
                "failure_rate": venue_failure_rate,
                "attempted_failed_count": failed_count,
            }
    return out


def _compute_attempt_coverage(
    filtered: pd.DataFrame,
    category: str,
) -> tuple[int, int]:
    """Return (attempt_found, attempt_expected) for an event-driven category.

    For PREDICTION (Polymarket), the attempt unit is a distinct ``underlying``
    in the filtered manifest — an underlying is "observed" if at least one
    shard exists for it in the date range. Since the availability manifest
    only contains rows that were actually captured, expected == found by
    definition: if we attempted a conditionId and it had zero trades, it
    never reaches the manifest.

    For SPORTS the attempt unit is ``(league_id, fixture_type)`` — fixtures
    only occur on match days, so (league, fixture_type) observation is the right
    attempt axis. When either column is missing we fall back to ``league_id``
    alone, then to ``(venue, instrument_type)``, then to ``venue`` so this
    is robust to manifest schema variation.

    Returns ``(0, 0)`` when no suitable attempt axis is found — callers must
    fall back to capture coverage in that case.
    """
    if filtered.empty:
        return 0, 0
    cat_upper = category.upper()
    if cat_upper == "PREDICTION":
        n = _distinct_values(filtered, "underlying")
        return n, n
    if cat_upper == "SPORTS":
        n = _sports_attempt_count(filtered)
        return n, n
    return 0, 0


def _build_coverage_metrics(
    filtered: pd.DataFrame,
    category: str,
    capture_coverage_pct: float,
    total_expected_cells: int = 0,
) -> dict[str, object]:
    """Resolve the event-driven vs dense coverage metrics for one category.

    See ``COVERAGE_SEMANTICS`` for the per-category classification.

    Dense categories (CeFi / TradFi / DeFi): every underlying is expected to
    produce data every day, so ``attempt == capture`` and the existing
    shards-weighted ``capture_coverage_pct`` is the right displayed number.
    Event-driven categories (sports fixtures, Polymarket conditionIds): the
    shards-weighted ratio understates real coverage because the denominator
    assumes every (underlying x day) combo should have trades. We display
    attempt coverage and expose capture + empty-rate for the drill-down.

    Phase-C honest-coverage upgrade: when the manifest exposes a
    ``capture_status`` column (UTL v5) with any non-``captured`` rows, we
    also derive ``failure_rate`` + structured ``capture_status_counts`` from
    the column directly. The proxy path (distinct-underlying count) remains
    the default for event-driven categories whose adapters haven't been
    re-run post-Phase-B — that keeps the PREDICTION attempt number honest
    even before the Phase-B sentinel rows land.
    """
    coverage_semantics = COVERAGE_SEMANTICS.get(category.upper(), "dense")
    attempt_found, attempt_expected = _compute_attempt_coverage(filtered, category)
    capture_counts = _compute_capture_status_counts(filtered)
    has_phase_b_rows = capture_counts.empty_confirmed + capture_counts.attempted_failed > 0
    capture_rates = _derive_capture_status_rates(capture_counts, total_expected_cells)

    if coverage_semantics == "event_driven" and attempt_expected > 0 and not has_phase_b_rows:
        attempt_coverage_pct = min(round(attempt_found / attempt_expected * 100, 2), 100.0)
        empty_rate_estimate: float | None = None
        if attempt_coverage_pct > 0:
            # empty_rate_estimate: fraction of attempted underlying-days that
            # had no trades. Clamped to [0, 1].
            empty_rate_estimate = max(
                0.0,
                min(1.0, round(1.0 - (capture_coverage_pct / attempt_coverage_pct), 4)),
            )
        completion_pct = attempt_coverage_pct
        failure_rate = float(capture_rates["failure_rate"])
    elif has_phase_b_rows and total_expected_cells > 0:
        # Phase B sentinel rows are present — prefer capture_status-derived
        # metrics over the distinct-underlying proxy. ``empty_rate_estimate``
        # becomes the concrete ``empty_rate`` (fraction of attempts returning
        # zero rows).
        attempt_coverage_pct = float(capture_rates["attempt_coverage_pct"])
        empty_rate_estimate = float(capture_rates["empty_rate"])
        completion_pct = attempt_coverage_pct
        failure_rate = float(capture_rates["failure_rate"])
    else:
        attempt_coverage_pct = capture_coverage_pct
        empty_rate_estimate = None
        completion_pct = capture_coverage_pct
        failure_rate = float(capture_rates["failure_rate"])
    counts_dict = {
        "captured": capture_counts.captured,
        "empty_confirmed": capture_counts.empty_confirmed,
        "attempted_failed": capture_counts.attempted_failed,
        "expected_unattempted_known_empty": capture_counts.expected_unattempted_known_empty,
        "expected_unattempted_pending_fetch": capture_counts.expected_unattempted_pending_fetch,
    }
    return {
        "coverage_semantics": coverage_semantics,
        "capture_coverage_pct": capture_coverage_pct,
        "attempt_coverage_pct": attempt_coverage_pct,
        "empty_rate_estimate": empty_rate_estimate,
        "failure_rate": failure_rate,
        "completion_pct": completion_pct,
        "capture_status_counts": counts_dict,
        "counts": counts_dict,
        "coverage": float(capture_rates["honest_coverage"]),
    }


# Countries tracked for transfer window calendar (denominator for Transfermarkt data)
_TRANSFER_COUNTRIES = (
    "ENG",
    "ESP",
    "DEU",
    "ITA",
    "FRA",
    "NLD",
    "PRT",
    "BEL",
    "TUR",
    "SCO",
    "AUT",
    "CHE",
    "DNK",
    "NOR",
    "SWE",
    "POL",
    "KOR",
    "ARG",
    "BRA",
    "CHL",
    "USA",
    "MEX",
    "JPN",
    "AUS",
)

# Cache for availability index reads — avoids repeated GCS downloads
_INDEX_CACHE: dict[str, tuple[float, pd.DataFrame]] = {}
_INDEX_CACHE_TTL = 300  # 5 minutes


def _read_index_cached(bucket: str) -> pd.DataFrame:
    """Read availability index with 5-minute TTL cache."""
    now = time.monotonic()
    cached = _INDEX_CACHE.get(bucket)
    if cached and (now - cached[0]) < _INDEX_CACHE_TTL:
        return cached[1]
    idx = read_availability_index(bucket)
    if _ALL_DEFI_GHOST_VENUES and "venue" in idx.columns:
        # Match both bare form (AAVE_V3) and hyphenated-chain form (AAVE_V3-ETHEREUM)
        venue_prefix = idx["venue"].str.split("-", n=1).str[0]
        ghost_mask = venue_prefix.isin(_ALL_DEFI_GHOST_VENUES)
        if ghost_mask.any():
            logger.debug("_read_index_cached: dropping %d ghost venue rows from %s", int(ghost_mask.sum()), bucket)
            idx = idx[~ghost_mask].reset_index(drop=True)
    _INDEX_CACHE[bucket] = (now, idx)
    return idx


# ── Offline rollup fast-path (plan: data_status_offline_rollup_2026_05_06) ──
#
# Cloud Run Job ``uts-prod-data-status-rollup`` writes
# ``gs://{pid}-data-status-rollups/{service}/full.json.gz`` every 5 min via
# ``*/5 * * * *`` Cloud Scheduler. We read it here, in-process-cache for
# 60s (refresh window inside one warm Cloud Run instance), and slice to the
# user's date window. Falls through to the on-demand compute when the rollup
# is missing or older than ``_ROLLUP_STALENESS_SEC``.

# Full set of deprecated ghost DeFi venue names — now canonical in UAC.
_ALL_DEFI_GHOST_VENUES: frozenset[str] = DEPRECATED_DEFI_GHOST_VENUE_NAMES

# Infrastructure/oracle entries that appear in DeFi sub-buckets but are NOT
# DeFi protocols — they pollute the chain venue breakdown. Checked on the
# prefix (before the first "-") so both "ALCHEMY" and "ALCHEMY-ETHEREUM" match.
_DEFI_NON_PROTOCOL_VENUE_PREFIXES: frozenset[str] = frozenset(
    {
        "COINBASE",  # COINBASE-SPOT — CeFi oracle source leaking from oracle-prices bucket
        "ALCHEMY",  # gas-fee RPC provider — data tracked in gas-fees sub-bucket
        "ANKR",  # LST staking RPC provider — data tracked in lst-rates sub-bucket
        "GAS_FEES",  # data_type string appearing as venue (defensive)
    }
)

_ROLLUP_BUCKET_TEMPLATE: str = "{pid}-data-status-rollups"
_ROLLUP_STALENESS_SEC: int = 1800  # 30 min — cron fires every 5; 30 covers 6 missed cycles
_ROLLUP_CACHE: dict[str, tuple[float, dict[str, object]]] = {}
_ROLLUP_CACHE_TTL_SEC: int = 1800  # in-process re-read TTL — match the GCS-staleness window above.
# Rationale: the rollup worker (Cloud Run Job) fires every 5 min, so any rollup
# we hold in-process is at most 5 min behind canonical. A 60s TTL was the
# initial conservative pick, but it forces a fresh transpacific 9-19 MB GCS
# round-trip every time the UI is idle for >60s — which is the common case
# (the user clicks Data Status, reads, comes back ~minutes later). Bumping to
# 1800s means a warm UI session never re-downloads the rollup; correctness is
# unchanged because data_status_service still falls through to on-demand if the
# blob mtime is older than _ROLLUP_STALENESS_SEC.


def _rollup_bucket() -> str:
    return _ROLLUP_BUCKET_TEMPLATE.format(pid=_pid)


def _read_rollup_if_fresh(service: str) -> dict[str, object] | None:
    """Read ``gs://{pid}-data-status-rollups/{service}/full.json.gz``.

    Returns the deserialised payload if the blob exists AND is younger than
    ``_ROLLUP_STALENESS_SEC``. Returns ``None`` otherwise — caller then falls
    through to the on-demand compute path.

    Two cache layers:
      * **In-process** (60s TTL): avoids re-fetching the same rollup every
        request from the same Cloud Run instance.
      * **GCS staleness check** (30 min TTL): protects against serving a
        rollup older than 6 cron cycles when the worker is broken.
    """
    cached = _ROLLUP_CACHE.get(service)
    now = time.monotonic()
    if cached is not None and (now - cached[0]) < _ROLLUP_CACHE_TTL_SEC:
        return cached[1]

    try:
        from unified_trading_library import get_storage_client

        client = get_storage_client(project_id=_pid)
        bucket_name = _rollup_bucket()
        blob_path = f"{service}/full.json.gz"
        # Unified cloud_interface flat API (NOT raw google-cloud-storage Blob):
        #   blob_exists(bucket, blob_path) -> bool
        #   get_blob_metadata(bucket, blob_path) -> BlobMetadata | None
        #   download_bytes(bucket, blob_path) -> bytes
        if not client.blob_exists(bucket_name, blob_path):  # pyright: ignore[reportAttributeAccessIssue]
            return None
        meta = client.get_blob_metadata(bucket_name, blob_path)  # pyright: ignore[reportAttributeAccessIssue]
        # BlobMetadata exposes ``updated`` as a datetime (or string ISO depending
        # on backend). Treat missing/parse-errors as "fresh enough" to read.
        if meta is not None and getattr(meta, "updated", None) is not None:
            age_sec = (pd.Timestamp.now(tz="UTC") - pd.Timestamp(meta.updated)).total_seconds()  # type: ignore[reportAttributeAccessIssue, reportUnknownArgumentType]
            if age_sec > _ROLLUP_STALENESS_SEC:
                logger.info(
                    "rollup for %s is stale (%.0fs > %ds threshold) — falling through to on-demand",
                    service,
                    age_sec,
                    _ROLLUP_STALENESS_SEC,
                )
                return None
        raw = client.download_bytes(bucket_name, blob_path)  # pyright: ignore[reportAttributeAccessIssue]
        # Worker uploaded with content_encoding=gzip. The unified
        # download_bytes returns raw bytes (no auto-decompress); we
        # decompress explicitly. The first two bytes (0x1f 0x8b) are the
        # gzip magic — defensive sniff lets us handle a future change to
        # auto-decompressing transports without churning this code.
        import gzip

        payload_bytes = gzip.decompress(raw) if raw[:2] == b"\x1f\x8b" else raw
        payload = json.loads(payload_bytes.decode("utf-8"))  # pyright: ignore[reportAny]
        if not isinstance(payload, dict):
            logger.warning("rollup for %s is not a dict — ignoring", service)
            return None
        _ROLLUP_CACHE[service] = (now, payload)
        return payload  # pyright: ignore[reportUnknownVariableType]
    except (OSError, ValueError, RuntimeError, json.JSONDecodeError) as exc:
        logger.info("rollup read failed for %s (%s) — falling through to on-demand", service, exc)
        return None


def _filter_dates_in_window(dates: list[str] | None, start_date: str, end_date: str) -> list[str]:
    if not dates:
        return []
    return [d for d in dates if start_date <= d <= end_date]


def _strip_defi_ghost_venues(cat_payload: dict[str, object]) -> dict[str, object]:
    """Remove era-2 no-underscore venue names from a DEFI asset-group payload."""
    if not _ALL_DEFI_GHOST_VENUES:
        return cat_payload

    def _excluded(v: str) -> bool:
        prefix = v.split("-", 1)[0]
        return (
            v in _ALL_DEFI_GHOST_VENUES
            or prefix in _ALL_DEFI_GHOST_VENUES
            or prefix in _DEFI_NON_PROTOCOL_VENUE_PREFIXES
        )

    venues = cat_payload.get("venues")
    if isinstance(venues, dict):
        clean = {v: p for v, p in venues.items() if not _excluded(v)}  # pyright: ignore[reportUnknownVariableType,reportUnknownArgumentType]
        if len(clean) < len(venues):  # pyright: ignore[reportUnknownArgumentType]
            cat_payload = {**cat_payload, "venues": clean}
    chains_data = cat_payload.get("chains")
    if not isinstance(chains_data, dict):
        return cat_payload
    cleaned: dict[str, object] = {}
    for chain_name, chain_data in chains_data.items():  # pyright: ignore[reportUnknownVariableType]
        if isinstance(chain_data, dict):
            chain_venues = chain_data.get("venues")  # pyright: ignore[reportUnknownMemberType,reportUnknownVariableType]
            if isinstance(chain_venues, list):
                cv = [v for v in chain_venues if not _excluded(v)]  # pyright: ignore[reportUnknownVariableType,reportUnknownArgumentType]
                chain_data = {**chain_data, "venues": cv, "venue_count": len(cv)}  # pyright: ignore[reportUnknownVariableType,reportUnknownArgumentType]
            elif isinstance(chain_venues, dict):
                cv2 = {v: p for v, p in chain_venues.items() if not _excluded(v)}  # pyright: ignore[reportUnknownVariableType,reportUnknownArgumentType]
                chain_data = {**chain_data, "venues": cv2, "venue_count": len(cv2)}  # pyright: ignore[reportUnknownVariableType,reportUnknownArgumentType]
        cleaned[chain_name] = chain_data
    return {**cat_payload, "chains": cleaned}


def _slice_rollup_to_window(
    rollup: dict[str, object],
    start_date: str,
    end_date: str,
    asset_groups_filter: list[str] | None,
) -> dict[str, object]:
    """Return a windowed subset of a full-range rollup.

    The rollup payload was computed with ``start_date="2018-01-01",
    end_date=today``. The slicer:
      1. Drops asset_groups not in the user's filter (if any).
      2. For each asset_group → venue → data_type, filters
         ``missing_dates``, ``dates_found_list``, and per-instrument date
         lists to the requested window.
      3. Recomputes per-(asset_group, venue, data_type) counts.
      4. Recomputes the overall totals.

    Behaviour parity with the on-demand path: same response shape, same
    ``mode="turbo"``, same percentages — only the date arrays + counts shrink
    to the requested window.
    """
    overall_found = 0
    overall_expected = 0
    overall_shards_found = 0
    overall_shards_expected = 0

    asset_groups = rollup.get("asset_groups")
    if not isinstance(asset_groups, dict):
        # Malformed rollup — surface loud rather than slice garbage.
        raise RuntimeError(f"rollup payload missing 'asset_groups' dict (got {type(asset_groups).__name__})")

    sliced_asset_groups: dict[str, object] = {}
    filter_set = {ag.upper() for ag in asset_groups_filter} if asset_groups_filter else None
    for cat, cat_payload in asset_groups.items():  # pyright: ignore[reportUnknownVariableType]
        if filter_set is not None and cat.upper() not in filter_set:  # pyright: ignore[reportUnknownMemberType]
            continue
        if not isinstance(cat_payload, dict):
            sliced_asset_groups[cat] = cat_payload  # pass-through unknown shapes
            continue
        if cat.lower() == "defi":  # pyright: ignore[reportUnknownMemberType]
            cat_payload = _strip_defi_ghost_venues(cat_payload)  # pyright: ignore[reportUnknownArgumentType]
        sliced_cat = _slice_asset_group(cat_payload, start_date, end_date)  # pyright: ignore[reportUnknownArgumentType]
        sliced_asset_groups[cat] = sliced_cat
        overall_found += int(cast(int, sliced_cat.get("dates_found", 0)))
        overall_expected += int(cast(int, sliced_cat.get("dates_expected", 0)))
        overall_shards_found += int(cast(int, sliced_cat.get("_venue_found_sliced", 0)))
        overall_shards_expected += int(cast(int, sliced_cat.get("_venue_expected_sliced", 0)))
        sliced_cat.pop("_venue_found_sliced", None)
        sliced_cat.pop("_venue_expected_sliced", None)

    overall_pct_dates = (
        min(round(overall_found / max(1, overall_expected) * 100, 2), 100.0) if overall_expected > 0 else 0.0
    )
    overall_pct_shards = (
        min(round(overall_shards_found / overall_shards_expected * 100, 2), 100.0)
        if overall_shards_expected > 0
        else overall_pct_dates
    )

    total_days = (pd.Timestamp(end_date) - pd.Timestamp(start_date)).days + 1

    return {
        "service": rollup.get("service"),
        "date_range": {"start": start_date, "end": end_date, "days": total_days},
        "mode": "turbo",
        "sub_dimension": rollup.get("sub_dimension", "venue"),
        "overall_completion_pct": overall_pct_shards,
        "overall_completion_pct_dates": overall_pct_dates,
        "overall_completion_pct_shards_weighted": overall_pct_shards,
        "overall_dates_found": overall_found,
        "overall_dates_expected": overall_expected,
        "overall_shards_found": overall_shards_found,
        "overall_shards_expected": overall_shards_expected,
        "migration_in_progress": rollup.get("migration_in_progress", False),
        "asset_groups": sliced_asset_groups,
        "served_from": "rollup",
    }


def _slice_asset_group(cat_payload: dict[str, object], start_date: str, end_date: str) -> dict[str, object]:
    """Slice one asset_group's payload to the date window. See _slice_rollup_to_window."""
    sliced: dict[str, object] = dict(cat_payload)
    venue_found_total = 0
    venue_expected_total = 0
    cat_found_dates: set[str] = set()

    venues_in = cat_payload.get("venues")
    if isinstance(venues_in, dict):
        sliced_venues: dict[str, object] = {}
        for venue, venue_payload in venues_in.items():  # pyright: ignore[reportUnknownVariableType]
            if not isinstance(venue_payload, dict):
                sliced_venues[venue] = venue_payload
                continue
            sv = _slice_venue(venue_payload, start_date, end_date)  # pyright: ignore[reportUnknownArgumentType]
            sliced_venues[venue] = sv
            venue_found_total += int(cast(int, sv.get("dates_found", 0)))
            venue_expected_total += int(cast(int, sv.get("dates_expected", 0)))
            for d in sv.get("dates_found_list", []) or []:  # pyright: ignore[reportGeneralTypeIssues,reportUnknownVariableType]
                cat_found_dates.add(str(d))  # pyright: ignore[reportUnknownArgumentType]
        sliced["venues"] = sliced_venues

    cat_total_days = (pd.Timestamp(end_date) - pd.Timestamp(start_date)).days + 1
    cat_found = len(cat_found_dates)
    cat_pct_dates = min(round(cat_found / max(1, cat_total_days) * 100, 2), 100.0)
    cat_pct_shards = (
        min(round(venue_found_total / venue_expected_total * 100, 2), 100.0)
        if venue_expected_total > 0
        else cat_pct_dates
    )

    sliced["dates_found"] = cat_found
    sliced["dates_expected"] = cat_total_days
    sliced["dates_missing"] = max(0, cat_total_days - cat_found)
    sliced["completion_pct"] = cat_pct_shards
    sliced["completion_pct_dates"] = cat_pct_dates
    # Internal counters drained by the parent — popped before returning to the user.
    sliced["_venue_found_sliced"] = venue_found_total
    sliced["_venue_expected_sliced"] = venue_expected_total
    return sliced


def _slice_venue(venue_payload: dict[str, object], start_date: str, end_date: str) -> dict[str, object]:
    """Slice one venue's payload to the date window. Recursive into per-data_type if present."""
    sliced: dict[str, object] = dict(venue_payload)

    found = _filter_dates_in_window(cast(list[str] | None, venue_payload.get("dates_found_list")), start_date, end_date)
    missing = _filter_dates_in_window(cast(list[str] | None, venue_payload.get("missing_dates")), start_date, end_date)
    expected_dates_full = cast(list[str] | None, venue_payload.get("dates_expected_list")) or (found + missing)
    expected = _filter_dates_in_window(expected_dates_full, start_date, end_date)

    sliced["dates_found_list"] = found
    sliced["missing_dates"] = missing
    sliced["dates_found"] = len(found)
    sliced["dates_expected"] = len(expected) if expected else (len(found) + len(missing))
    sliced["dates_missing"] = sliced["dates_expected"] - sliced["dates_found"]
    if sliced["dates_expected"] > 0:
        sliced["completion_pct"] = min(round(sliced["dates_found"] / sliced["dates_expected"] * 100, 2), 100.0)
    else:
        sliced["completion_pct"] = 0.0

    # Per-data_type breakdown (MTDS honest-coverage shape).
    honest_dts = venue_payload.get("honest_data_types")
    if isinstance(honest_dts, dict):
        sliced["honest_data_types"] = {
            dt: _slice_venue(dt_payload, start_date, end_date) if isinstance(dt_payload, dict) else dt_payload  # pyright: ignore[reportUnknownArgumentType]
            for dt, dt_payload in honest_dts.items()  # pyright: ignore[reportUnknownVariableType]
        }

    return sliced


def _read_coverage_rollup_if_fresh(service: str) -> dict[str, object] | None:
    """Read ``gs://{pid}-data-status-rollups/{service}/coverage.json.gz``.

    Companion to :func:`_read_rollup_if_fresh` for the
    ``/api/data-status/coverage-summary`` endpoint. Same staleness threshold,
    same in-process cache, just a different blob path.
    """
    cache_key = f"coverage:{service}"
    cached = _ROLLUP_CACHE.get(cache_key)
    now = time.monotonic()
    if cached is not None and (now - cached[0]) < _ROLLUP_CACHE_TTL_SEC:
        return cached[1]

    try:
        from unified_trading_library import get_storage_client

        client = get_storage_client(project_id=_pid)
        bucket_name = _rollup_bucket()
        blob_path = f"{service}/coverage.json.gz"
        if not client.blob_exists(bucket_name, blob_path):  # pyright: ignore[reportAttributeAccessIssue]
            return None
        meta = client.get_blob_metadata(bucket_name, blob_path)  # pyright: ignore[reportAttributeAccessIssue]
        if meta is not None and getattr(meta, "updated", None) is not None:
            age_sec = (pd.Timestamp.now(tz="UTC") - pd.Timestamp(meta.updated)).total_seconds()  # type: ignore[reportAttributeAccessIssue, reportUnknownArgumentType]
            if age_sec > _ROLLUP_STALENESS_SEC:
                logger.info(
                    "coverage rollup for %s is stale (%.0fs > %ds threshold) — falling through",
                    service,
                    age_sec,
                    _ROLLUP_STALENESS_SEC,
                )
                return None
        raw = client.download_bytes(bucket_name, blob_path)  # pyright: ignore[reportAttributeAccessIssue]
        import gzip

        payload_bytes = gzip.decompress(raw) if raw[:2] == b"\x1f\x8b" else raw
        payload = json.loads(payload_bytes.decode("utf-8"))  # pyright: ignore[reportAny]
        if not isinstance(payload, dict):
            logger.warning("coverage rollup for %s is not a dict — ignoring", service)
            return None
        _ROLLUP_CACHE[cache_key] = (now, payload)
        return payload  # pyright: ignore[reportUnknownVariableType]
    except (OSError, ValueError, RuntimeError, json.JSONDecodeError) as exc:
        logger.info(
            "coverage rollup read failed for %s (%s) — falling through to on-demand",
            service,
            exc,
        )
        return None


def _filter_coverage_to_asset_groups(
    rollup: dict[str, object], asset_groups_filter: list[str] | None
) -> dict[str, object]:
    """Filter a coverage-summary rollup to the requested asset_groups.

    Coverage-summary has no date axis — the rollup IS the answer for any
    request. We just trim ``asset_groups`` to the user's filter and
    recompute ``totals`` from the survivors.
    """
    if not asset_groups_filter:
        return {**rollup, "served_from": "rollup", "totals_source": "rollup"}

    filter_set = {ag.upper() for ag in asset_groups_filter}
    asset_groups = rollup.get("asset_groups", {})
    if not isinstance(asset_groups, dict):
        return {**rollup, "served_from": "rollup", "totals_source": "rollup"}

    filtered: dict[str, object] = {cat: payload for cat, payload in asset_groups.items() if cat.upper() in filter_set}  # pyright: ignore[reportUnknownVariableType,reportUnknownMemberType]
    totals_keys = (
        "shards",
        "instrument_rows",
        "dates_across_asset_groups",
        "latest_day_instruments",
    )
    totals: dict[str, int] = dict.fromkeys(totals_keys, 0)
    for cat_payload in filtered.values():
        if isinstance(cat_payload, dict):
            for k in totals_keys:
                v = cat_payload.get(k, 0)  # pyright: ignore[reportUnknownMemberType,reportUnknownVariableType]
                if isinstance(v, (int, float)):
                    totals[k] += int(v)

    return {
        **rollup,
        "asset_groups": filtered,
        "totals": totals,
        "served_from": "rollup",
        "totals_source": "rollup",
    }


def clear_rollup_cache() -> None:
    """Flush the in-process rollup cache. Wired into ``/turbo/clear``."""
    _ROLLUP_CACHE.clear()


def clear_index_cache() -> None:
    """Clear the availability index cache.

    Also clears the process-level :func:`_mtds_expected_dates_cached`
    LRU. That cache is keyed only on UAC config (process-immutable in
    steady state) and so cannot serve stale manifest data, but
    ``/turbo/clear`` is a "make sure nothing is cached" knob — so we
    flush it too to keep the endpoint's contract honest.
    """
    _INDEX_CACHE.clear()
    _EXPECTED_START_DATES_CACHE["value"] = None
    _mtds_expected_dates_cached.cache_clear()


# ── expected_start_dates.yaml cached loader ────────────────────────────────
# Loaded once at first use; `clear_index_cache()` resets it (also called by
# /api/data-status/turbo/clear). The config is the SSOT for per-category
# launch dates — used to clamp category-level aggregations to
# max(user_start_date, category_start) so the Data Status page does not
# score pre-launch phantom dates as "missing".
_EXPECTED_START_DATES_CACHE: dict[str, object] = {"value": None}


def _load_expected_start_dates_cached() -> dict[str, object]:
    """Load expected_start_dates.yaml from the PM configs directory.

    Cached for process lifetime. Returns an empty dict if the file is
    missing (callers then fall back to the user-supplied start_date).
    """
    cached = _EXPECTED_START_DATES_CACHE.get("value")
    if isinstance(cached, dict):
        return cast(dict[str, object], cached)

    # Prefer app_config.get_config_dir() (respects bundled pm-configs/ + sibling
    # workspace lookup). Fall back to repo-relative path for test contexts where
    # the FastAPI app hasn't been initialised.
    import yaml

    from deployment_api.app_config import get_config_dir

    config_dir: object
    try:
        config_dir = get_config_dir()
    except RuntimeError:
        # Test / standalone contexts: fall back to workspace sibling
        from pathlib import Path as _Path

        here = _Path(__file__).resolve()
        # deployment-api/deployment_api/services/data_status_service.py
        # → workspace root = parents[4]
        workspace_root = here.parents[4]
        config_dir = workspace_root / "unified-trading-pm" / "configs"

    yaml_path = config_dir / "expected_start_dates.yaml"  # pyright: ignore[reportOperatorIssue]

    if not yaml_path.exists():
        logger.warning(
            "expected_start_dates.yaml not found at %s — category aggregates will NOT clamp to launch dates",
            yaml_path,
        )
        _EXPECTED_START_DATES_CACHE["value"] = {}
        return {}

    with open(yaml_path) as f:
        raw = yaml.safe_load(f) or {}  # pyright: ignore[reportUnknownVariableType]
    if not isinstance(raw, dict):
        _EXPECTED_START_DATES_CACHE["value"] = {}
        return {}

    _EXPECTED_START_DATES_CACHE["value"] = raw
    return cast(dict[str, object], raw)


def get_effective_start_date(
    user_start_date: str,
    service: str,
    category: str,
    venue: str | None = None,
) -> str:
    """Return ``max(user_start_date, configured_launch_date)``.

    Uses `expected_start_dates.yaml` as the SSOT:
    - If ``venue`` is provided and has a venue-specific date, that wins.
    - Otherwise falls back to the service/category ``category_start``.
    - If neither is configured, returns ``user_start_date`` unchanged.

    This is used to clamp category-level aggregations so pre-launch dates
    are not counted as "missing" in the Data Status page.
    """
    cfg = _load_expected_start_dates_cached()
    svc_cfg_val = cfg.get(service)
    if not isinstance(svc_cfg_val, dict):
        return user_start_date
    svc_cfg = cast(dict[str, object], svc_cfg_val)
    cat_cfg_val = svc_cfg.get(category) or svc_cfg.get(category.upper())
    if not isinstance(cat_cfg_val, dict):
        return user_start_date
    cat_cfg = cast(dict[str, object], cat_cfg_val)

    candidate: str | None = None
    if venue is not None:
        venues_val = cat_cfg.get("venues")
        if isinstance(venues_val, dict):
            v_val = cast(dict[str, object], venues_val).get(venue)
            if isinstance(v_val, str):
                candidate = v_val

    if candidate is None:
        cat_start_val = cat_cfg.get("category_start")
        if isinstance(cat_start_val, str):
            candidate = cat_start_val

    if candidate is None:
        return user_start_date
    return max(user_start_date, candidate)


def _derive_underlying_from_instrument_id(instrument_id: str) -> str:
    """Extract the base asset (underlying) from a canonical instrument_id.

    Handles common naming conventions:
    - "BTC-USDT-PERP" -> "BTC"
    - "ETH-USDC" -> "ETH"
    - "BTC-USD-241227-C-100000" -> "BTC"
    - "ES-FUT-20260320" -> "ES"
    - "SPY" -> "SPY" (single-symbol equity)

    The first segment before the first dash is always the base asset.
    For single-symbol instruments (no dash), the full string is returned.
    """
    if not instrument_id or not instrument_id.strip():
        return ""
    parts = instrument_id.strip().split("-")
    return parts[0].upper()


def _ensure_underlying_column(df: pd.DataFrame) -> pd.DataFrame:
    """Ensure the DataFrame has a populated ``underlying`` column.

    Fills blank/missing ``underlying`` rows by deriving from ``instrument_id``
    (when present) using ``_derive_underlying_from_instrument_id``. Rows whose
    ``underlying`` is already non-empty are preserved as-is.
    Returns the DataFrame (modified in-place when derivation is needed).
    """
    if "instrument_id" not in df.columns:
        return df

    if "underlying" not in df.columns:
        df["underlying"] = ""
    blank_mask = df["underlying"].isna() | (df["underlying"].astype(str).str.strip() == "")
    if blank_mask.any():
        df.loc[blank_mask, "underlying"] = (
            df.loc[blank_mask, "instrument_id"].astype(str).map(_derive_underlying_from_instrument_id)
        )
    return df


def _clamp_to_venue_starts(filtered: pd.DataFrame, start_date: str) -> str:
    """Clamp start date forward to the latest venue launch date."""
    effective_start = start_date
    if "venue" not in filtered.columns or filtered.empty:
        return effective_start
    venue_mapping = VenueMapping()
    for v in filtered["venue"].unique():  # pyright: ignore[reportAny]
        vs = venue_mapping.get_venue_start_date(v)  # pyright: ignore[reportAny]
        if not vs and ":" in v:
            vs = venue_mapping.get_venue_start_date(v.split(":")[0])  # pyright: ignore[reportAny]
        if vs:
            effective_start = max(effective_start, vs)
    return effective_start


class DataStatusService:
    """
    Business logic service for data status operations.

    This service handles:
    - CLI wrapper for data status commands
    - Missing shards calculation
    - Data completeness validation
    - Cross-service status aggregation
    """

    def __init__(self, project_id: str | None = None, deployment_env_short: str | None = None):
        """Initialize data status service."""
        self.project_id = project_id or _pid
        self.deployment_env_short = deployment_env_short or _env_short

    def _build_cli_cmd(
        self,
        service: str,
        start_date: str,
        end_date: str,
        asset_groups: list[str] | None,
        venues: list[str] | None,
        show_missing: bool,
        check_venues: bool,
        check_data_types: bool,
        check_feature_groups: bool,
        check_timeframes: bool,
        mode: str,
    ) -> list[str]:
        """Build the data-status CLI command list."""
        cmd = [
            sys.executable,
            "-m",
            "deployment_service",
            "data-status",
            "-s",
            service,
            "--start-date",
            start_date,
            "--end-date",
            end_date,
            "--output",
            "json",
            "--mode",
            mode,
        ]
        for ag in asset_groups or []:
            # The deployment-service CLI still accepts ``-c`` for the
            # asset_group filter (legacy short flag preserved during the
            # asset_group canonical-vocabulary rollout per CLAUDE.md SSOT).
            cmd.extend(["-c", ag])
        for venue in venues or []:
            cmd.extend(["-v", venue])
        if show_missing:
            cmd.append("--show-missing")
        if check_venues:
            cmd.append("--check-venues")
        elif check_feature_groups:
            cmd.append("--check-feature-groups")
        elif check_timeframes:
            cmd.append("--check-timeframes")
        elif service in ["market-tick-data-handler", "market-data-processing-service"]:
            cmd.append("--fast")
        if check_data_types:
            cmd.append("--check-data-types")
        return cmd

    async def run_data_status_cli(
        self,
        service: str,
        start_date: str,
        end_date: str,
        asset_groups: list[str] | None = None,
        venues: list[str] | None = None,
        show_missing: bool = False,
        check_venues: bool = False,
        check_data_types: bool = False,
        check_feature_groups: bool = False,
        check_timeframes: bool = False,
        mode: str = "batch",
    ) -> dict[str, object]:
        """
        Run data-status CLI command and return parsed JSON output.

        Returns parsed JSON output from CLI command.
        """
        cmd = self._build_cli_cmd(
            service,
            start_date,
            end_date,
            asset_groups,
            venues,
            show_missing,
            check_venues,
            check_data_types,
            check_feature_groups,
            check_timeframes,
            mode,
        )
        logger.info("Running CLI: %s", " ".join(cmd))

        try:
            process = await asyncio.create_subprocess_exec(
                *cmd,
                stdout=asyncio.subprocess.PIPE,
                stderr=asyncio.subprocess.PIPE,
                cwd=None,
            )
            stdout, stderr = await process.communicate()

            if process.returncode != 0:
                error_msg = f"CLI command failed with code {process.returncode}: {stderr.decode()}"
                logger.error(error_msg)
                return {"error": error_msg, "stderr": stderr.decode()}

            try:
                result = cast(dict[str, object], json.loads(stdout.decode()))
                return result
            except json.JSONDecodeError as e:
                logger.error("Failed to parse CLI JSON output: %s", e)
                return {"error": f"Invalid JSON output: {e}", "raw_output": stdout.decode()}

        except (OSError, ValueError, RuntimeError) as e:
            logger.error("Error running CLI command: %s", e)
            return {"error": str(e)}

    def _tally_missing_venues(
        self,
        date_info: dict[str, object],
        missing_by_venue: dict[str, int],
        missing_by_category: dict[str, int],
    ) -> int:
        """Count missing venues in a date entry and update tallies in-place."""
        missing_count = 0
        venues_raw: object = date_info.get("venues")
        if not (venues_raw and isinstance(venues_raw, list)):
            return 0
        for venue_info_raw in cast(list[object], venues_raw):
            if not isinstance(venue_info_raw, dict):
                continue
            venue_info = cast(dict[str, object], venue_info_raw)
            venue_name_raw: object = venue_info.get("venue")
            venue_name = venue_name_raw if isinstance(venue_name_raw, str) else ""
            if venue_info.get("status") == "missing":
                missing_count += 1
                missing_by_venue[venue_name] = missing_by_venue.get(venue_name, 0) + 1
                cat_raw: object = venue_info.get("category", "unknown")
                category = cat_raw if isinstance(cat_raw, str) else "unknown"
                missing_by_category[category] = missing_by_category.get(category, 0) + 1
        return missing_count

    # ── Venue aliases (mirrors deployment-service ManifestReader._VENUE_ALIASES) ──
    _VENUE_ALIASES: ClassVar[dict[str, str]] = {
        "OKX": "OKX-SPOT",
        "COINBASE": "COINBASE-SPOT",
    }

    # ── Pre-canonicalisation DeFi venue aliases to drop from aggregation ──
    # The DeFi availability indices contain BOTH post-migration canonical rows
    # (``venue=AAVE_V3`` + ``chain=ETHEREUM``) AND pre-canonicalisation rows
    # (``venue=AAVE_V3-ETHEREUM`` + ``chain=""``) where the legacy format
    # combined protocol and chain into a single ``venue`` field. The legacy
    # rows inflate ``venue_dates_expected`` but have no matching shard data
    # under canonical paths, which drives DEFI category completion from ~99%
    # to ~40%.
    #
    # Deterministic filter, DEFI-scoped only:
    #   (category == 'defi') AND (venue contains '-') AND (chain is NaN/'')
    # → drop these rows from the aggregate.
    #
    # CeFi hyphenated venues (``BINANCE-FUTURES``, ``OKX-SWAP``, ...) live in
    # category=='cefi' and are therefore untouched by this filter.
    _DEFI_LEGACY_PROTOCOL_PREFIXES: ClassVar[tuple[str, ...]] = (
        "AAVE",
        "UNISWAP",
        "CURVE",
        "LIDO",
        "BALANCER",
        "COMPOUND",
        "SUSHISWAP",
        "PANCAKESWAP",
        "RAYDIUM",
        "MAKER",
        "YEARN",
        "CONVEX",
        "ROCKETPOOL",
        # Additional DeFi protocols observed in the DeFi availability index
        # with empty chain column (pre-canonicalisation alias rows).
        "CAMELOT",
        "GMX",
        "JITO",
        "ORCA",
        "MARINADE",
        "KAMINO",
        "MORPHO",
        "FLUID",
        "ETHENA",
        "ETHERFI",
        "EIGENLAYER",
        # Added 2026-04-19 after Playwright audit flagged AERODROME_V3-BASE
        # leaking into DeFi venue summary. Canonical form is
        # `AERODROME-BASE` + `chain='base'`; legacy is `AERODROME_V3-BASE` +
        # `chain=''`. Filter keys on empty chain so canonical rows survive.
        "AERODROME",
        # Velodrome mirrors Aerodrome (same Solidly v3 fork on Optimism).
        "VELODROME",
        # Drift perps — recent addition to UAC KNOWN_VENUE_TOKENS 2026-04-19.
        "DRIFT",
    )

    @classmethod
    def _is_legacy_defi_venue_row(cls, venue: object, chain: object) -> bool:
        """Detect pre-canonicalisation DeFi venue alias row.

        Legacy rows look like ``venue='AAVE_V3-ETHEREUM' chain=''`` — the
        protocol+chain pair was squashed into a single field before the
        canonical migration. New canonical rows are
        ``venue='AAVE_V3' chain='ETHEREUM'``.
        """
        if not isinstance(venue, str):
            return False
        if "-" not in venue:
            return False
        # Legacy format is uppercase letters/digits with an optional 'V<N>'
        # segment, then a hyphen, then the chain name. We treat rows as
        # legacy only when chain is empty/NaN AND the venue name starts
        # with a known DeFi protocol prefix. This avoids matching
        # fictional or CeFi-shaped hyphenated venues.
        chain_str = "" if chain is None else str(chain).strip()
        # ``pd.isna`` returns True for NaN but is not safe on arbitrary
        # objects; guard with the string coercion above, then also allow
        # the literal 'nan' produced by ``str(float('nan'))``.
        if chain_str and chain_str.lower() != "nan":
            return False
        head = venue.split("-", 1)[0]
        # Strip optional underscore + ``V<digits>`` tail so both canonical
        # (``AAVE_V3`` → ``AAVE``) and ghost (``AAVEV3`` → ``AAVE``) forms
        # normalise to the same root before the prefix lookup.
        root = re.sub(r"_?V\d+$", "", head)
        return root in cls._DEFI_LEGACY_PROTOCOL_PREFIXES

    # Per-service category scope (SSOT: deployment-ui-playwright-audit-checklist
    # Appendix A — Service x category matrix).  Services listed here ONLY apply
    # to the categories in the frozenset — categories outside this set are
    # omitted from the /turbo response (rather than rendered as misleading 0/0).
    # Services NOT listed (e.g. instruments-service, market-tick-data-service,
    # market-tick-data-handler, features-calendar-service) apply to ALL 5
    # categories (CEFI / TRADFI / DEFI / SPORTS / PREDICTION).
    _SERVICE_CATEGORY_RESTRICTIONS: ClassVar[dict[str, frozenset[str]]] = {
        "market-data-processing-service": frozenset({"CEFI", "TRADFI", "DEFI"}),
        "features-delta-one-service": frozenset({"CEFI", "TRADFI", "DEFI"}),
        "features-volatility-service": frozenset({"CEFI", "TRADFI", "DEFI"}),
        "features-multi-timeframe-service": frozenset({"CEFI", "TRADFI", "DEFI"}),
        "features-cross-instrument-service": frozenset({"CEFI", "TRADFI", "DEFI", "PREDICTION"}),
        "features-onchain-service": frozenset({"DEFI"}),
        "features-sports-service": frozenset({"SPORTS"}),
        "features-commodity-service": frozenset({"TRADFI"}),
        # features-calendar is intentionally NOT restricted: it serves all
        # asset_groups via a single shared bucket (``features-calendar-{pid}``),
        # surfaced under the ``SHARED`` pseudo-asset_group below rather than
        # duplicating identical numbers across every CEFI/TRADFI/DEFI/...
        "strategy-service": frozenset({"CEFI", "TRADFI", "DEFI"}),
        "execution-service": frozenset({"CEFI", "TRADFI", "DEFI"}),
    }

    # Services whose data is cross-asset by design (single shared bucket, no
    # per-asset-group sharding). The coverage-summary surfaces these under a
    # single ``SHARED`` pseudo-asset_group instead of duplicating the same
    # totals under every CEFI/TRADFI/DEFI/SPORTS/PREDICTION key. This matches
    # the user-facing taxonomy: calendar events are consumed by every
    # asset_group's strategies, so attributing them to one is misleading.
    _SHARED_BUCKET_SERVICES: ClassVar[frozenset[str]] = frozenset(
        {
            "features-calendar-service",
            # ml-service pools all asset groups under shared ml-*-artifacts buckets
            # (model checkpoints, training metrics, inference results). Consolidated
            # from ml-training-service + ml-inference-service (2026-05-21).
            "ml-service",
        }
    )

    # Per-service display axis for the ``latest_day_instruments`` breakdown.
    # The pricing→features pipeline uses ``venue`` (or ``data_type`` for sports;
    # see SPORTS axis swap in `_get_coverage_summary_sync`); experiment-based
    # services use the manifest column that identifies the experiment unit.
    # Falls through to the default (venue/data_type) if the named column is
    # missing or empty across all rows.
    _SERVICE_GROUP_AXIS_OVERRIDE: ClassVar[dict[str, str]] = {
        # ml-training-service + ml-inference-service consolidated into ml-service (2026-05-21)
        "ml-service": "model_family",
        "strategy-service": "strategy_id",
        "execution-service": "instruction_type",
    }

    # Categories whose bucket name doesn't follow the template pattern.
    # Only Phase-1 DeFi data types live in dedicated per-data-type buckets via
    # ``get_write_bucket_name(data_type)``. Phase-2 event-typed handlers
    # (liquidation_events, flash_loan_events, staking_yields, position_data,
    # token_transfers, bridge_events, governance_events, mev_events) and
    # eigenlayer_rewards write into the main ``market-data-tick-defi-{pid}``
    # bucket (via ``get_tick_data_bucket(asset_group="defi")``) so they're
    # picked up by the default ``SERVICE_TO_KIND`` → ``resolve_bucket_name`` path — no override needed.
    _BUCKET_CATEGORY_OVERRIDES: ClassVar[dict[tuple[str, str], str]] = {
        ("market-tick-data-service", "gas-fees"): "gas-fees-{env}-{pid}",
        ("market-tick-data-service", "evm-defi"): "evm-defi-{env}-{pid}",
        ("market-tick-data-service", "solana-defi"): "solana-defi-{env}-{pid}",
        ("market-tick-data-service", "dex-pools"): "dex-pools-{env}-{pid}",
        ("market-tick-data-service", "dex-swaps"): "dex-swaps-{env}-{pid}",
        ("market-tick-data-service", "lending-indices"): "lending-indices-{env}-{pid}",
        ("market-tick-data-service", "liquidations"): "liquidations-{env}-{pid}",
        ("market-tick-data-service", "lst-rates"): "lst-rates-{env}-{pid}",
        ("market-tick-data-service", "oracle-prices"): "oracle-prices-{env}-{pid}",
        ("market-tick-data-service", "perp-funding"): "perp-funding-{env}-{pid}",
    }

    # DeFi sub-dimension bucket keys for MTDS — merged into DEFI category.
    # Limited to Phase-1 sub-buckets; Phase-2 + eigenlayer-rewards already
    # land in the main DEFI bucket so they appear without enumeration here.
    _MTDS_DEFI_SUB_DIMENSIONS: ClassVar[list[str]] = [
        "gas-fees",
        "evm-defi",
        "solana-defi",
        "dex-pools",
        "dex-swaps",
        "lending-indices",
        "liquidations",
        "lst-rates",
        "oracle-prices",
        "perp-funding",
    ]

    def _read_defi_merged_index(self, service: str, cat: str, cloud: str = "gcp") -> pd.DataFrame:
        """Read availability index, merging sub-dimension buckets for MTDS DEFI.

        For market-tick-data-service + DEFI category, reads the main DEFI bucket
        AND all sub-dimension buckets (gas-fees, dex-swaps, etc.), concatenating
        them so venues from sub-dimensions appear under DEFI in the UI.

        Each row is tagged with ``_defi_source`` so the category builder can
        produce a per-sub-dimension breakdown.

        Phase 3 (data-status multi-axis drilldown) — after concatenation,
        filters merged rows to only those whose ``(venue, chain)`` pair is
        a canonical DeFi protocol per the UAC ``ALL_DEFI_VENUES``
        registry. Without this filter, sub-dimension buckets like
        ``oracle-prices-{pid}`` and ``perp-funding-{pid}`` (which carry
        oracle-price and perp-funding rows for CeFi venues that DEFI
        feeds reference, e.g. COINBASE-SPOT-as-oracle-source) silently
        leaked into the DEFI cell-grid as if they were DeFi protocols.
        """
        override = self._BUCKET_CATEGORY_OVERRIDES.get((service, cat.lower()))
        if override:
            main_bucket = override.format(pid=self.project_id, env=self.deployment_env_short)
        elif service == "features-commodity-service":
            main_bucket = COMMODITY_BUCKET_TEMPLATE.format(pid=self.project_id)
        else:
            kind = SERVICE_TO_KIND.get(service)
            if kind is None:
                return pd.DataFrame()
            ag = cat.lower() or None
            if ag == "prediction":
                pred_kind = PREDICTION_KIND_MAP.get(kind)
                main_bucket = resolve_bucket_name(cloud=cast(object, cloud), kind=pred_kind if pred_kind else kind)  # pyright: ignore[reportArgumentType]
            else:
                main_bucket = resolve_bucket_name(cloud=cast(object, cloud), kind=kind, asset_group=cast(object, ag))  # pyright: ignore[reportArgumentType]

        frames: list[pd.DataFrame] = []
        try:
            idx = _read_index_cached(main_bucket)
            if not idx.empty:
                idx = idx.copy()
                idx["_defi_source"] = ""
                frames.append(idx)
        except Exception:
            logger.debug("No manifest index in %s", main_bucket)

        # Merge sub-dimension buckets for MTDS DEFI
        if service == "market-tick-data-service" and cat.lower() == "defi":
            for sub_dim in self._MTDS_DEFI_SUB_DIMENSIONS:
                sub_override = self._BUCKET_CATEGORY_OVERRIDES.get((service, sub_dim))
                if not sub_override:
                    continue
                sub_bucket = sub_override.format(pid=self.project_id, env=self.deployment_env_short)
                try:
                    sub_idx = _read_index_cached(sub_bucket)
                    if not sub_idx.empty:
                        sub_idx = sub_idx.copy()
                        sub_idx["_defi_source"] = sub_dim
                        frames.append(sub_idx)
                except Exception:
                    logger.debug("No sub-dimension index in %s", sub_bucket)

        if not frames:
            return pd.DataFrame()
        merged = pd.concat(frames, ignore_index=True)

        # DeFi-venue whitelist filter — only applies to the MTDS-DEFI merge
        # path. Drops rows whose ``(venue, chain)`` pair is NOT in the UAC
        # canonical DeFi venue registry. Catches the COINBASE-SPOT-under-
        # ETHEREUM leak from oracle-prices / perp-funding sub-buckets that
        # legitimately carry CeFi venues but shouldn't appear in the DEFI
        # cell-grid.
        if service == "market-tick-data-service" and cat.lower() == "defi" and not merged.empty:
            merged = self._filter_to_canonical_defi_venues(merged)
        return merged

    @classmethod
    def _allowed_defi_venue_chain_pairs(cls) -> frozenset[tuple[str, str]]:
        """Return the canonical ``(venue_upper, chain_upper)`` set for DeFi.

        Sourced from UAC ``ALL_DEFI_VENUES`` (canonical hyphenated form
        ``PROTOCOL-CHAIN``) AND ``LEGACY_DEFI_VENUE_ALIASES`` (raw
        underscore forms like ``AAVE_V3`` that pre-2026-04 manifests
        carry). Adding both shapes means the filter accepts canonical
        rows AND legacy-underscore rows that haven't yet been migrated.
        """
        from unified_api_contracts.registry.defi_venues import (
            ALL_DEFI_VENUES,
            LEGACY_DEFI_VENUE_ALIASES,
        )

        pairs: set[tuple[str, str]] = set()
        for entry in ALL_DEFI_VENUES:
            if "-" not in entry:
                continue
            protocol, chain = entry.rsplit("-", 1)
            pairs.add((protocol.upper(), chain.upper()))
        # Legacy: every entry in LEGACY_DEFI_VENUE_ALIASES maps an old
        # underscore form to its canonical no-underscore form. Accept the
        # legacy key alongside its canonical pair so unmigrated rows still
        # pass the whitelist.
        for legacy, canonical in LEGACY_DEFI_VENUE_ALIASES.items():
            if "-" not in canonical:
                continue
            _, chain = canonical.rsplit("-", 1)
            pairs.add((legacy.upper(), chain.upper()))
        return frozenset(pairs)

    def _filter_to_canonical_defi_venues(self, index: pd.DataFrame) -> pd.DataFrame:
        """Keep rows whose ``(venue, chain)`` is a canonical DeFi pair.

        Rows missing one or both axes are kept (defensive — better to
        let the downstream legacy-row filter clean them than to drop
        valid rows that just predate the chain split).
        """
        if "venue" not in index.columns or "chain" not in index.columns:
            return index
        allowed = self._allowed_defi_venue_chain_pairs()
        venues = index["venue"].fillna("").astype(str).str.upper()
        chains = index["chain"].fillna("").astype(str).str.upper()
        # Keep rows with empty venue OR empty chain — those are caught by
        # the downstream ``_filter_legacy_defi_rows`` pass; we don't want
        # to drop them here without context.
        empty_axis = (venues == "") | (chains == "")
        in_whitelist = pd.Series(
            [(v, c) in allowed for v, c in zip(venues.tolist(), chains.tolist(), strict=True)],
            index=index.index,
        )
        keep = empty_axis | in_whitelist
        dropped = int((~keep).sum())
        if dropped > 0:
            logger.debug("DEFI venue whitelist dropped %d non-DeFi rows from merged index", dropped)
        return index.loc[keep].copy()

    async def calculate_missing_shards(
        self,
        service: str,
        start_date: str,
        end_date: str,
        asset_groups: list[str] | None = None,
        venues: list[str] | None = None,
        mode: str = "batch",
        cloud: str = "gcp",
    ) -> dict[str, object]:
        """Calculate missing shards by reading manifest indices directly.

        Uses read_availability_index (same as deployment-service CLI) instead
        of shelling out to the data-status CLI subprocess.
        Runs in a thread to avoid blocking the async event loop.
        """
        return await asyncio.to_thread(
            self._calculate_missing_shards_sync,
            service,
            start_date,
            end_date,
            asset_groups,
            venues,
            cloud,
        )

    def _calculate_missing_shards_sync(
        self,
        service: str,
        start_date: str,
        end_date: str,
        asset_groups: list[str] | None = None,
        venues: list[str] | None = None,
        cloud: str = "gcp",
    ) -> dict[str, object]:
        """Synchronous implementation of missing shard calculation."""
        try:
            cat_list = asset_groups or [str(c) for c in MarketCategory]
            missing_by_date: dict[str, int] = {}
            missing_by_category: dict[str, int] = {}
            total_missing = 0
            total_days_checked = 0

            for cat in cat_list:
                cat_result = self._scan_category_manifest(service, cat, start_date, end_date, cloud=cloud)
                if not cat_result:
                    continue
                for md in cat_result["missing"]:  # pyright: ignore[reportGeneralTypeIssues,reportUnknownVariableType]
                    missing_by_date[md] = missing_by_date.get(md, 0) + 1  # pyright: ignore[reportUnknownArgumentType]
                missing_by_category[cat] = len(cat_result["missing"])  # pyright: ignore[reportArgumentType]
                total_missing += len(cat_result["missing"])  # pyright: ignore[reportArgumentType]
                total_days_checked += cat_result["days_checked"]  # pyright: ignore[reportOperatorIssue,reportUnknownVariableType]

            days_total = max(1, total_days_checked)  # pyright: ignore[reportUnknownArgumentType,reportUnknownVariableType]
            days_complete = days_total - len(missing_by_date)  # pyright: ignore[reportUnknownVariableType]
            completion = round(days_complete / days_total * 100, 2)  # pyright: ignore[reportUnknownArgumentType]

            return {
                "service": service,
                "date_range": {"start": start_date, "end": end_date},
                "total_missing": total_missing,
                "missing_by_date": missing_by_date,
                "missing_by_venue": {},
                "missing_by_category": missing_by_category,
                "summary": {
                    "total_days_checked": total_days_checked,
                    "days_with_missing": len(missing_by_date),
                    "venues_with_missing": 0,
                    "categories_with_missing": len(missing_by_category),
                    "completion_rate": completion,
                },
            }
        except Exception as e:
            logger.exception("Error calculating missing shards")
            return {"error": str(e)}

    def _scan_category_manifest(
        self,
        service: str,
        cat: str,
        start_date: str,
        end_date: str,
        cloud: str = "gcp",
    ) -> dict[str, list[str] | int] | None:
        """Read manifest index for one category and return missing dates."""
        # Skip categories that don't apply to this service
        allowed = self._SERVICE_CATEGORY_RESTRICTIONS.get(service)
        if allowed and cat.upper() not in allowed:
            return None

        index = self._read_defi_merged_index(service, cat, cloud=cloud)
        if index.empty:
            return None

        mask = (index["date"] >= start_date) & (index["date"] <= end_date)
        if "service_name" in index.columns:
            mask = mask & (index["service_name"] == service)
        filtered = index.loc[mask].copy()

        # Fold bare venue aliases
        if "venue" in filtered.columns and not filtered.empty:
            filtered["venue"] = filtered["venue"].replace(self._VENUE_ALIASES)

        effective_start = _clamp_to_venue_starts(filtered, start_date)
        all_dates = pd.date_range(effective_start, end_date, freq="D")
        found_dates = set(filtered["date"].unique())  # pyright: ignore[reportAny]
        missing = [d.strftime("%Y-%m-%d") for d in all_dates if d.strftime("%Y-%m-%d") not in found_dates]
        if not missing:
            return None
        return {"missing": missing, "days_checked": len(all_dates)}

    def _calculate_completion_rate(self, data_status_result: dict[str, object]) -> float:
        """
        Calculate completion rate from data status result.

        Args:
            data_status_result: Result from data status CLI

        Returns:
            Completion rate as percentage (0.0-100.0)
        """
        if "dates" not in data_status_result:
            return 0.0

        total_checks = 0
        completed_checks = 0

        dates_raw: object = data_status_result.get("dates")
        if not isinstance(dates_raw, list):
            return 0.0

        for date_info_raw in cast(list[object], dates_raw):
            if not isinstance(date_info_raw, dict):
                continue
            date_info = cast(dict[str, object], date_info_raw)
            venues_raw: object = date_info.get("venues")
            if not isinstance(venues_raw, list):
                continue
            for venue_info_raw in cast(list[object], venues_raw):
                if not isinstance(venue_info_raw, dict):
                    continue
                venue_info = cast(dict[str, object], venue_info_raw)
                total_checks += 1
                if venue_info.get("status") != "missing":
                    completed_checks += 1

        if total_checks == 0:
            return 0.0

        return (completed_checks / total_checks) * 100.0

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
        rollup = await asyncio.to_thread(_read_coverage_rollup_if_fresh, service)
        if rollup is not None:
            return _filter_coverage_to_asset_groups(rollup, asset_groups)
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

    def _filter_legacy_defi_rows(self, index: pd.DataFrame, cat: str) -> pd.DataFrame:
        """Drop pre-canonicalisation DeFi venue-alias rows.

        Rows like ``venue='AAVE_V3-ETHEREUM' chain=''`` predate the venue/chain
        split; same filter the per-shard rollup already applies.
        """
        if cat.lower() != "defi" or "venue" not in index.columns or index.empty:
            return index
        chain_series = index["chain"] if "chain" in index.columns else pd.Series([""] * len(index), index=index.index)
        legacy_mask = [
            self._is_legacy_defi_venue_row(v, c)  # pyright: ignore[reportAny]
            for v, c in zip(index["venue"].tolist(), chain_series.tolist(), strict=True)  # pyright: ignore[reportAny]
        ]
        if not any(legacy_mask):
            return index
        dropped = int(sum(legacy_mask))
        logger.debug(
            "coverage-summary: filtered %d legacy DeFi venue-alias rows from %s",
            dropped,
            cat,
        )
        return index.loc[[not m for m in legacy_mask]].copy()

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
            result_categories[cat] = entry
            total_shards += shards_int
            total_instrument_rows += shards_int
            total_latest_day_instruments += ld_total_int

        return {
            "service": service,
            "asset_groups": result_categories,
            "totals": {
                "shards": total_shards,
                "instrument_rows": total_instrument_rows,
                "dates_across_categories": len(all_dates),
                "latest_day_instruments": total_latest_day_instruments,
            },
            "totals_source": "manifest",
        }

    async def get_manifest_status(
        self,
        service: str,
        start_date: str,
        end_date: str,
        asset_groups: list[str] | None = None,
        *,
        cloud: str = "gcp",
        secondary_axis: str | None = None,
        league_id: str | None = None,
        fixture_id: str | None = None,
        canonical_question_group: str | None = None,
        job_id: str | None = None,
        chain: str | None = None,
        pipeline_modes: list[str] | None = None,
    ) -> dict[str, object]:
        """Return data status from manifest indices in TurboDataStatusResponse shape.

        **Two paths**:

        1. **Rollup fast-path** — if a fresh (< 30 min old) rollup blob
           exists at ``gs://{pid}-data-status-rollups/{service}/full.json.gz``,
           read it and slice to the requested window in-memory. Sub-500ms.
           Skipped when any filter param (``league_id`` / ``fixture_id`` /
           ``canonical_question_group`` / ``job_id`` / ``chain``) is set
           — the rollup is filter-free; falling through to the on-demand
           path is the only way to honour the filter.
        2. **On-demand fall-through** — original honest-coverage compute.
           Used on cold deploys (first 5 min before first cron fires) and
           if the rollup is stale or absent.

        The rollup is computed offline by ``data_status_rollup_worker``
        (Cloud Run Job + ``*/5 * * * *`` Scheduler cron). See plan:
        ``data_status_offline_rollup_2026_05_06.md``.
        """
        any_row_filter = any(
            f is not None and f != "" for f in (league_id, fixture_id, canonical_question_group, job_id, chain)
        ) or bool(pipeline_modes)
        if not any_row_filter:
            rollup = await asyncio.to_thread(_read_rollup_if_fresh, service)
            if rollup is not None:
                response = _slice_rollup_to_window(rollup, start_date, end_date, asset_groups)
                if secondary_axis:
                    response["secondary_axis"] = secondary_axis
                return response
        return await asyncio.to_thread(
            self._get_manifest_status_sync,
            service,
            start_date,
            end_date,
            asset_groups,
            secondary_axis,
            league_id,
            fixture_id,
            canonical_question_group,
            job_id,
            chain,
            cloud,
            pipeline_modes,
        )

    def _get_manifest_status_sync(
        self,
        service: str,
        start_date: str,
        end_date: str,
        asset_groups: list[str] | None = None,
        secondary_axis: str | None = None,
        league_id: str | None = None,
        fixture_id: str | None = None,
        canonical_question_group: str | None = None,
        job_id: str | None = None,
        chain: str | None = None,
        cloud: str = "gcp",
        pipeline_modes: list[str] | None = None,
    ) -> dict[str, object]:
        """Synchronous manifest status — returns TurboDataStatusResponse shape."""
        cat_list = asset_groups or [str(c) for c in MarketCategory]
        # Filter to only the categories this service actually targets (Appendix A
        # SSOT).  Rendering CEFI/TRADFI/SPORTS/PREDICTION as 0/0 for a
        # DEFI-only service (e.g. features-onchain-service) is misleading — it
        # looks like "missing data" when in fact the category is out-of-scope.
        allowed = self._SERVICE_CATEGORY_RESTRICTIONS.get(service)
        if allowed:
            cat_list = [c for c in cat_list if c.upper() in allowed]
        all_dates_range = pd.date_range(start_date, end_date, freq="D")
        all_date_strs = [d.strftime("%Y-%m-%d") for d in all_dates_range]
        total_days = len(all_dates_range)
        result_categories: dict[str, object] = {}
        overall_found = 0
        overall_expected = 0

        venue_mapping = VenueMapping()

        overall_shards_found = 0
        overall_shards_expected = 0

        # Parallelise per-category builds across processes. Each category is
        # independent (own filter slice, own honest-coverage compute, writes to
        # its own ``result_categories[cat]`` slot), so we map them onto a fork
        # ProcessPool. Fork start-method is critical: children inherit the
        # parent's loaded ``_INDEX_CACHE`` (the ~30 MB manifest DataFrames per
        # bucket) via copy-on-write, so we don't pay the pickle/transfer cost
        # of sending those over a Pipe. Only the small picklable args per task
        # cross the boundary. Single-category requests stay serial — the fork
        # + pickle overhead would dwarf the work.
        #
        # Why not threads: the honest-coverage inner loops are GIL-bound
        # Python (set comprehensions, Counter, dict mutations); a previous
        # ThreadPoolExecutor attempt gave zero speedup and OOM'd at 8 GiB.
        # Pack secondary-axis filter params into a dict the per-category
        # builder can apply after the date mask but before the cell-grid
        # compute. Empty/None values are dropped so a no-filter request
        # behaves identically to the previous code path.
        row_filters = self._pack_row_filters(
            league_id=league_id,
            fixture_id=fixture_id,
            canonical_question_group=canonical_question_group,
            job_id=job_id,
            chain=chain,
        )

        if len(cat_list) <= 1 or _PROCESS_POOL_DISABLED or row_filters or pipeline_modes:
            for cat in cat_list:
                cat_result = self._build_manifest_category(
                    service,
                    cat,
                    start_date,
                    end_date,
                    all_date_strs,
                    total_days,
                    venue_mapping,
                    row_filters=row_filters,
                    cloud=cloud,
                    pipeline_modes=pipeline_modes,
                )
                result_categories[cat] = cat_result
                overall_found += int(cat_result.get("dates_found", 0))  # pyright: ignore[reportArgumentType]
                overall_expected += int(cat_result.get("dates_expected", 0))  # pyright: ignore[reportArgumentType]
                overall_shards_found += int(cat_result.get("_venue_found", 0))  # pyright: ignore[reportArgumentType]
                overall_shards_expected += int(cat_result.get("_venue_expected", 0))  # pyright: ignore[reportArgumentType]
                del cat_result["_venue_found"]
                del cat_result["_venue_expected"]
        else:
            ctx = multiprocessing.get_context("fork")
            with ProcessPoolExecutor(max_workers=min(len(cat_list), 5), mp_context=ctx) as pool:
                futures = {
                    pool.submit(
                        _build_category_in_subprocess,
                        service,
                        cat,
                        start_date,
                        end_date,
                        all_date_strs,
                        total_days,
                        cloud,
                    ): cat
                    for cat in cat_list
                }
                for future in futures:
                    cat = futures[future]
                    cat_result = future.result()
                    result_categories[cat] = cat_result
                    overall_found += int(cat_result.get("dates_found", 0))  # pyright: ignore[reportArgumentType]
                    overall_expected += int(cat_result.get("dates_expected", 0))  # pyright: ignore[reportArgumentType]
                    overall_shards_found += int(cat_result.get("_venue_found", 0))  # pyright: ignore[reportArgumentType]
                    overall_shards_expected += int(cat_result.get("_venue_expected", 0))  # pyright: ignore[reportArgumentType]
                    del cat_result["_venue_found"]
                    del cat_result["_venue_expected"]

        overall_pct_dates = min(round(overall_found / max(1, overall_expected) * 100, 2), 100.0)
        overall_pct_shards = (
            min(round(overall_shards_found / overall_shards_expected * 100, 2), 100.0)
            if overall_shards_expected > 0
            else overall_pct_dates
        )
        # Primary ``overall_completion_pct`` mirrors the per-category
        # ``completion_pct`` (which is shards-weighted) so the overall and
        # sub-rows use the same metric. Where no shards denominator exists we
        # fall back to the date-based figure so the number is still meaningful.
        overall_pct = overall_pct_shards
        # Flag migrations in progress so the UI can explain a suspiciously-low
        # overall number without the user having to cross-check running VMs.
        # Heuristic: overall < 10% of shards expected AND a backfill/migration
        # VM is currently running for this service. We keep this shallow —
        # deeper VM introspection lives in the deployment-service API.
        migration_in_progress = bool(
            overall_shards_expected > 0
            and overall_shards_found < (overall_shards_expected * 0.1)
            and self._has_active_migration_vm(service)
        )

        response: dict[str, object] = {
            "service": service,
            "date_range": {"start": start_date, "end": end_date, "days": total_days},
            "mode": "turbo",
            "sub_dimension": "venue",
            "overall_completion_pct": overall_pct,
            "overall_completion_pct_dates": overall_pct_dates,
            "overall_completion_pct_shards_weighted": overall_pct_shards,
            "overall_dates_found": overall_found,
            "overall_dates_expected": overall_expected,
            "overall_shards_found": overall_shards_found,
            "overall_shards_expected": overall_shards_expected,
            "migration_in_progress": migration_in_progress,
            "asset_groups": result_categories,
        }
        # Echo secondary_axis + active filters back so the UI can confirm
        # which slice it received. No-filter requests omit these keys
        # (backward-compat with existing /manifest consumers).
        if secondary_axis:
            response["secondary_axis"] = secondary_axis
        if row_filters:
            response["filters"] = dict(row_filters)
        return response

    def _has_active_migration_vm(self, service: str) -> bool:
        """Best-effort check whether a migration/backfill VM is running.

        Uses the shared ``get_compute_engine_client`` facade the rest of
        deployment-api uses. Failures return ``False`` — this is purely an
        advisory flag for the UI, never a gate on completion math.
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
            items: object = raw.get("items", {}) if isinstance(raw, dict) else {}  # pyright: ignore[reportUnknownVariableType,reportUnknownMemberType]
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
        except (ImportError, OSError, RuntimeError, KeyError, AttributeError):
            return False

    # Sports reference venues — fixture-dependent, not every-calendar-day expected.
    # Expected dates = dates where ANY sports reference entity has data (fixture calendar).
    _SPORTS_REFERENCE_PREFIXES: ClassVar[tuple[str, ...]] = (
        "FOOTYSTATS_",
        "UNDERSTAT_",
        "SFI_",
        "API_FOOTBALL_",
    )

    # Understat XG covers only 6 leagues — denominator must be filtered to
    # fixture dates from these leagues only, not the full fixture calendar.
    # Mapping: Understat league name → canonical league_id in LEAGUE_REGISTRY.
    _UNDERSTAT_LEAGUE_IDS: ClassVar[tuple[str, ...]] = (
        "EPL",
        "LA_LIGA",
        "BUNDESLIGA",
        "SERIE_A",
        "LIGUE_1",
        # RFPL (Russian) — not in LEAGUE_REGISTRY; omitted from calendar filter.
    )

    # Transfermarkt is fixture-INDEPENDENT reference data (squad composition,
    # league metadata). It's fetched on every batch day, not just fixture days.
    # Player values are expected year-round; transfer_records (future) only
    # during/after transfer windows.
    _TRANSFER_WINDOW_PREFIXES: ClassVar[tuple[str, ...]] = ("TRANSFERMARKT_",)

    def _is_sports_reference_venue(self, venue: str) -> bool:
        """Check if a venue is a sports reference entity (fixture-dependent)."""
        return venue.startswith(self._SPORTS_REFERENCE_PREFIXES)

    # Sparse sports entities where the full fixture calendar is the wrong
    # denominator. These are either provider-specific (Understat = 6 leagues),
    # infrequent (Transfermarkt = transfer windows), or partially backfilled
    # (FootyStats predictions/matches). For these, expected = found (show raw
    # count, not misleading percentage against 2660 fixture dates).
    _SPARSE_SPORTS_ENTITIES: ClassVar[frozenset[str]] = frozenset(
        {
            "XG",
            "UNDERSTAT_XG",  # Understat: 6 leagues only
            "PLAYER_VALUES",  # Transfermarkt: window-based
            "MATCHES",
            "PREDICTIONS",  # FootyStats: partial backfill
            # TRANSFERMARKT_LEAGUES + SFI_LEAGUES retired 2026-05-05
        }
    )

    def _is_understat_venue(self, venue: str) -> bool:
        """Check if a venue is Understat XG (6-league subset)."""
        v = venue.upper()
        return v == "XG" or v == "UNDERSTAT_XG" or v.startswith("UNDERSTAT_")

    def _is_transfer_window_venue(self, venue: str) -> bool:
        """Check if a venue is transfer-window-aware (Transfermarkt)."""
        v = venue.upper()
        return v.startswith("TRANSFERMARKT") or v == "PLAYER_VALUES"

    def _is_sparse_sports_entity(self, venue: str) -> bool:
        """Check if this is a sparse sports entity with wrong fixture-calendar denominator."""
        return venue.upper() in self._SPARSE_SPORTS_ENTITIES

    # ── Reference-data-driven expected dates ──────────────────────────────

    # Cache for upstream reference data (keyed by upstream:category:date_range)
    _REF_DATA_CACHE: ClassVar[dict[str, tuple[float, dict[str, set[str]]]]] = {}
    _REF_DATA_CACHE_TTL = 300  # 5 minutes

    def _get_reference_expected_dates(
        self,
        category: str,
        start_date: str,
        end_date: str,
        service: str = "",
        cloud: str = "gcp",
    ) -> dict[str, set[str]]:
        """Read upstream service availability index to get per-venue expected dates.

        Uses the denominator chain: each service's expected dates come from its
        direct upstream (``_UPSTREAM_SERVICE_MAP``).  Falls back to
        instruments-service if no upstream is defined.

        Returns {venue: {date_str, ...}} — the set of dates where the upstream
        service has data for that venue.
        """
        upstream = self._UPSTREAM_SERVICE_MAP.get(service, "instruments-service")
        cache_key = f"{upstream}:{category}:{start_date}:{end_date}"
        now = time.monotonic()
        cached = self._REF_DATA_CACHE.get(cache_key)
        if cached and (now - cached[0]) < self._REF_DATA_CACHE_TTL:
            return cached[1]

        if upstream == "features-commodity-service":
            bucket = COMMODITY_BUCKET_TEMPLATE.format(pid=self.project_id)
        else:
            upstream_kind = SERVICE_TO_KIND.get(upstream, "")
            if not upstream_kind:
                return {}
            ag = category.lower() or None
            if ag == "prediction":
                pred_kind = PREDICTION_KIND_MAP.get(upstream_kind)
                bucket = resolve_bucket_name(cloud=cloud, kind=pred_kind if pred_kind else upstream_kind)  # pyright: ignore[reportArgumentType]
            else:
                bucket = resolve_bucket_name(cloud=cloud, kind=upstream_kind, asset_group=ag)  # pyright: ignore[reportArgumentType]

        result: dict[str, set[str]] = {}
        try:
            idx = _read_index_cached(bucket)
            if idx.empty:
                return result
            mask = (idx["date"] >= start_date) & (idx["date"] <= end_date)
            # Filter by upstream service_name if shared bucket
            if "service_name" in idx.columns:
                mask = mask & (idx["service_name"] == upstream)
            filtered = idx.loc[mask]
            if "venue" in filtered.columns:
                for v in filtered["venue"].unique():  # pyright: ignore[reportAny]
                    v_str = str(v)  # pyright: ignore[reportAny]
                    v_dates = {str(d) for d in filtered.loc[filtered["venue"] == v, "date"].unique()}  # pyright: ignore[reportUnknownVariableType,reportUnknownMemberType,reportUnknownArgumentType,reportAny,reportAttributeAccessIssue]
                    result[v_str] = v_dates
        except Exception:
            logger.debug("No upstream index for %s in %s", upstream, bucket)

        self._REF_DATA_CACHE[cache_key] = (now, result)
        return result

    # Denominator chain: each downstream service uses its direct upstream's
    # ACTUAL availability as the expected-date set.  Missing key = use
    # instruments-service (the root reference layer).
    _UPSTREAM_SERVICE_MAP: ClassVar[dict[str, str]] = {
        "market-tick-data-service": "instruments-service",
        "market-data-processing-service": "market-tick-data-service",
        "features-delta-one-service": "market-data-processing-service",
        "features-volatility-service": "market-data-processing-service",
        "features-onchain-service": "market-tick-data-service",
        "features-multi-timeframe-service": "market-data-processing-service",
        "features-cross-instrument-service": "market-data-processing-service",
        "features-sports-service": "market-tick-data-service",
        "features-calendar-service": "instruments-service",
        "features-commodity-service": "market-data-processing-service",
        # ml-training-service + ml-inference-service consolidated into ml-service (2026-05-21)
        "ml-service": "features-delta-one-service",
        "strategy-service": "ml-service",
        "execution-service": "strategy-service",
    }

    # Services whose expected-date denominator comes from upstream availability
    # rather than calendar trading days.
    _REFERENCE_DRIVEN_SERVICES: ClassVar[frozenset[str]] = frozenset(
        {
            "market-tick-data-service",
            "market-data-processing-service",
            "features-delta-one-service",
            "features-volatility-service",
            "features-onchain-service",
            "features-sports-service",
            "features-calendar-service",
            "features-multi-timeframe-service",
            "features-cross-instrument-service",
            "features-commodity-service",
            # ml-training-service + ml-inference-service consolidated into ml-service (2026-05-21)
            "ml-service",
            "strategy-service",
            "execution-service",
        }
    )

    @staticmethod
    def _resolve_venue_start(
        venue: str,
        venue_mapping: VenueMapping,
        is_instruments_service: bool,
    ) -> str | None:
        """Resolve the per-venue start date for the breakdown denominator.

        For instruments-service, prefer the per-venue instrument-discovery
        start (UAC SSOT, added 2026-05-06 in unified-api-contracts@89db18f)
        so HYPERLIQUID returns 2023-11-01 (when instruments first existed)
        instead of 2023-04-15 (when book_snapshot_5 archive starts). Other
        services keep using venue_start_date as before. Falls back through
        the ``venue:variant`` split for both lookups.
        """
        if is_instruments_service:
            vs = venue_mapping.get_instrument_discovery_start(venue)
            if not vs and ":" in venue:
                vs = venue_mapping.get_instrument_discovery_start(venue.split(":")[0])
            if vs:
                return vs
        vs = venue_mapping.get_venue_start_date(venue)
        if not vs and ":" in venue:
            vs = venue_mapping.get_venue_start_date(venue.split(":")[0])
        return vs

    def _build_venue_breakdown(
        self,
        filtered: pd.DataFrame,
        start_date: str,
        end_date: str,
        venue_mapping: VenueMapping,
        cat_found: int,
        total_days: int,
        service: str = "",
        category: str = "",
        cloud: str = "gcp",
    ) -> tuple[dict[str, object], int, int]:
        """Build per-venue stats from filtered index data.

        Includes data_type sub-dimension for services that write per-data-type
        manifest entries (e.g. market-tick-data-service).

        **Reference-data-driven denominator**: For market-data and feature
        services, the expected-date set comes from instruments-service (i.e.
        "dates where reference data exists for this venue"), NOT from calendar
        trading days.  This means coverage = "missing market data given
        available instruments/fixtures".

        For sports reference venues the denominator is the fixture calendar.
        """
        if "venue" not in filtered.columns or filtered.empty:
            return {}, cat_found, total_days

        has_data_type = (
            "data_type" in filtered.columns
            and not filtered["data_type"].isna().all()
            and (filtered["data_type"].str.len() > 0).any()
        )

        # Reference-data-driven expected dates from upstream service
        use_ref_denominator = service in self._REFERENCE_DRIVEN_SERVICES and category
        ref_dates: dict[str, set[str]] = {}
        if use_ref_denominator:
            ref_dates = self._get_reference_expected_dates(category, start_date, end_date, service=service, cloud=cloud)

        # Build fixture calendar — union of dates across all sports reference
        # venues in this category.  Used as the expected-date set for each
        # individual sports reference venue instead of all calendar days.
        all_venues = [str(x) for x in filtered["venue"].unique() if str(x).strip()]  # pyright: ignore[reportAny]
        sports_ref_venues = [v for v in all_venues if self._is_sports_reference_venue(v)]
        fixture_calendar: set[str] | None = None
        if sports_ref_venues:
            sports_mask = filtered["venue"].isin(sports_ref_venues)
            fixture_calendar = {str(d) for d in filtered.loc[sports_mask, "date"].unique()}  # pyright: ignore[reportAny]

        venues_dict: dict[str, object] = {}
        venue_found_total = 0
        venue_expected_total = 0
        is_instruments_service = service == "instruments-service"
        for v in sorted(all_venues):
            v_mask = filtered["venue"] == v
            v_df = filtered[v_mask]
            v_dates_all = {str(d) for d in v_df["date"].unique()}  # pyright: ignore[reportAny]
            vs = self._resolve_venue_start(v, venue_mapping, is_instruments_service)
            if not vs and v_dates_all:
                vs = min(v_dates_all)
            eff_start = max(start_date, vs) if vs else start_date

            v_all_dates = self._resolve_expected_dates(
                v,
                eff_start,
                end_date,
                fixture_calendar,
                ref_dates,
                venue_mapping,
            )
            # Sparse sports entities return None → use found dates as expected
            # (shows raw count, not misleading % against full fixture calendar)
            # Note: v_all_dates is always set[str], so None check is unnecessary
            # if v_all_dates is None:
            #     v_all_dates = v_dates_all
            venue_entry = self._build_single_venue_entry(
                v_df,
                v,
                vs,
                eff_start,
                end_date,
                v_dates_all,
                v_all_dates,
                bool(has_data_type),  # pyright: ignore[reportArgumentType]
                venue_mapping,
                category=category,
                service=service,
            )

            venues_dict[v] = venue_entry
            venue_found_total += int(venue_entry["dates_found"])  # pyright: ignore[reportArgumentType]
            venue_expected_total += int(venue_entry["dates_expected"])  # pyright: ignore[reportArgumentType]
        return venues_dict, venue_found_total, venue_expected_total

    def _apply_mtds_honest_coverage(
        self,
        venues_dict: dict[str, object],
        filtered: pd.DataFrame,
        category: str,
        start_date: str,
        end_date: str,
        venue_mapping: VenueMapping,
    ) -> tuple[dict[str, object], int, int]:
        """Override per-venue denominator with UAC-driven honest-coverage.

        SSOT: ``codex/02-data/mtds-data-source-coverage-matrix.md`` §7.

        Rebuilds the per-venue ``dates_found`` / ``dates_expected`` /
        ``completion_pct`` using the UAC ``(venue, data_type, date)`` shard
        space instead of whatever happened to be in the manifest. Also
        injects UAC-declared venues that had ZERO manifest rows (would
        otherwise be invisible — e.g. UPBIT shipped nothing for a quarter).

        Per-venue fields added / overridden:
          - ``dates_found`` (honest count of (venue, dt, date) tuples)
          - ``dates_expected`` (UAC-declared denominator)
          - ``dates_missing``
          - ``completion_pct`` (shards-weighted)
          - ``expected_data_types`` (list of UAC-declared dt for this venue)
          - ``missing_data_types`` (declared dt with zero found shards)
          - ``honest_data_types`` (dict: dt → per-dt honest-coverage stats,
            keeps the existing ``data_types`` legacy block untouched)

        At category level:
          - adds UAC-declared venues missing from manifest (zero-row entries)
          - returns the new aggregate ``(venue_found_total, venue_expected_total)``
        """
        expected_venues = _mtds_expected_venues(category, venue_mapping)

        # DEFI hyphenated-data_type canonicalisation (Phase 6e.1b residual).
        # Venue canonicalisation no longer needed — UTL write-hook +
        # 2026-05-07 manifest migration completed that path.
        if category.upper() == "DEFI" and not filtered.empty:
            filtered = _canonicalise_defi_data_types(filtered)

        # Start from the (possibly remapped) dict (preserves instrument_types /
        # chains / capture_status_counts sub-structures built by
        # _build_venue_breakdown).
        new_venues: dict[str, object] = dict(venues_dict)
        total_found = 0
        total_expected = 0

        # Build the union of (a) venues in the manifest (post-normalisation for
        # DEFI), (b) UAC-declared venues. UAC-declared venues not in manifest
        # get a zero-row entry surfaced as ``missing_data_types``.
        union_venues = set(new_venues.keys()) | set(expected_venues)

        for venue in sorted(union_venues):
            honest = _mtds_honest_coverage_for_venue(filtered, venue, category, start_date, end_date, venue_mapping)
            expected_shards = int(cast(int, honest["expected_shards"]))
            found_shards = int(cast(int, honest["found_shards"]))
            if expected_shards == 0:
                # Venue not UAC-declared for MTDS (e.g. legacy DeFi names) —
                # keep the existing entry untouched. Do NOT roll its
                # observed-only totals into the honest aggregate; the
                # legacy rollup is preserved under the existing keys, but
                # we don't let it pollute UAC-declared totals.
                entry = new_venues.get(venue)
                if entry is None:
                    continue
                if isinstance(entry, dict):
                    # Sum the legacy per-venue totals so the category
                    # header still reflects them.
                    total_found += int(cast(int, entry.get("dates_found", 0)))  # pyright: ignore[reportUnknownMemberType]
                    total_expected += int(cast(int, entry.get("dates_expected", 0)))  # pyright: ignore[reportUnknownMemberType]
                continue

            existing_entry = new_venues.get(venue)
            if isinstance(existing_entry, dict):
                venue_entry: dict[str, object] = dict(existing_entry)  # pyright: ignore[reportUnknownArgumentType]
            else:
                # UAC-declared venue with zero manifest rows — materialise a
                # zero-row placeholder so the UI shows it under
                # ``missing_venues`` with 0% completion.
                _zero_counts = {
                    "captured": 0,
                    "empty_confirmed": 0,
                    "attempted_failed": 0,
                    "expected_unattempted_known_empty": 0,
                    "expected_unattempted_pending_fetch": 0,
                }
                venue_entry = {
                    "dates_found": 0,
                    "dates_expected": 0,
                    "completion_pct": 0.0,
                    "venue_start_date": venue_mapping.get_venue_start_date(venue),
                    "capture_status_counts": _zero_counts,
                    "counts": _zero_counts,
                    "coverage": 0.0,
                    "missing_dates": [],
                    "dates_found_list": [],
                    "dates_missing_list": [],
                }

            # Override top-level found/expected with honest-coverage values.
            venue_entry["dates_found"] = found_shards
            venue_entry["dates_expected"] = expected_shards
            venue_entry["dates_expected_venue"] = expected_shards
            venue_entry["dates_missing"] = max(0, expected_shards - found_shards)
            venue_entry["completion_pct"] = min(round(found_shards / max(1, expected_shards) * 100, 2), 100.0)
            # Honest-coverage annotations — UI surfaces these as a second
            # tab / tooltip alongside the legacy ``data_types`` block.
            venue_entry["expected_data_types"] = honest["expected_data_types"]
            venue_entry["missing_data_types"] = honest["missing_data_types"]
            venue_entry["honest_data_types"] = honest["data_types"]
            venue_entry["honest_axis"] = str(MTDS_CATEGORY_META[category.upper()]["axis"])

            new_venues[venue] = venue_entry
            total_found += found_shards
            total_expected += expected_shards

        return new_venues, total_found, total_expected

    def _resolve_expected_dates(
        self,
        venue: str,
        eff_start: str,
        end_date: str,
        fixture_calendar: set[str] | None,
        ref_dates: dict[str, set[str]],
        venue_mapping: VenueMapping,
    ) -> set[str]:
        """Resolve the expected-date set for a venue.

        Priority:
        1. Transfermarkt → transfer-window dates only (open/close +/- grace)
        2. Understat XG → fixture calendar filtered to 6 covered leagues
        3. Sports reference venues → full fixture calendar
        4. Reference-driven services → instruments-service dates
        5. Fallback → calendar trading days
        """
        # Sparse sports entities: expected = found (show raw count, not
        # misleading % against full fixture calendar). Returns None to signal
        # "use found dates as denominator" to the caller.
        if self._is_sparse_sports_entity(venue):
            return None  # pyright: ignore[reportReturnType]
        if self._is_sports_reference_venue(venue) and fixture_calendar is not None:
            return {d for d in fixture_calendar if d >= eff_start}
        if venue in ref_dates:
            return {d for d in ref_dates[venue] if d >= eff_start}
        return set(venue_mapping.get_expected_trading_dates(venue, eff_start, end_date))

    def _resolve_understat_fixture_dates(
        self,
        start: str,
        end: str,
    ) -> set[str]:
        """Return expected dates for Understat XG.

        Understat XG covers only 6 leagues. We don't have a league-filtered
        fixture calendar available in this service, so we return None to signal
        that the expected dates should equal the found dates (self-referencing
        denominator). The completion shows as the raw date count rather than
        a misleading percentage against the full 38-league calendar.
        """
        # Return None — caller handles this as "use found dates as expected"
        return None  # pyright: ignore[reportReturnType]

    @staticmethod
    def _resolve_transfer_window_dates(start: str, end: str) -> set[str]:
        """Return dates where Transfermarkt reference data is expected.

        Uses the UAC transfer window calendar across all tracked countries.
        Only dates that fall within an open transfer window (or within a
        7-day grace period after close) are considered expected.

        This prevents the denominator from including mid-season dates
        where no transfer activity occurs.
        """
        from datetime import date as dt_date
        from datetime import timedelta

        start_d = dt_date.fromisoformat(start)
        end_d = dt_date.fromisoformat(end)

        expected: set[str] = set()
        grace_days = 7

        for year in range(start_d.year, end_d.year + 1):
            for country in _TRANSFER_COUNTRIES:
                for window in get_transfer_windows_for_year(country, year):
                    # Include all dates within the window
                    w_start = max(window.open_date, start_d)
                    # Extend close by grace period for post-window data lag
                    w_end = min(window.close_date + timedelta(days=grace_days), end_d)
                    if w_start > w_end:
                        continue
                    d = w_start
                    while d <= w_end:
                        expected.add(d.isoformat())
                        d += timedelta(days=1)

        return expected

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
        v_capture_counts = _compute_capture_status_counts(v_df)
        v_capture_rates = _derive_capture_status_rates(v_capture_counts, expected)
        v_failure_pillars = _compute_failure_pillar_counts(v_df)
        v_empty_reasons = _compute_empty_reason_counts(v_df)

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
            },
            "counts": {
                "captured": v_capture_counts.captured,
                "empty_confirmed": v_capture_counts.empty_confirmed,
                "attempted_failed": v_capture_counts.attempted_failed,
                "expected_unattempted_known_empty": v_capture_counts.expected_unattempted_known_empty,
                "expected_unattempted_pending_fetch": v_capture_counts.expected_unattempted_pending_fetch,
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
        uac_expected_dts = set(get_expected_data_types_for_venue(venue, service=service))
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
            dt_start = get_venue_data_type_start_date(venue, dt)
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
            if is_tradfi and dt in _TRADFI_TICK_ONLY_DATA_TYPES:
                dt_expected = {d for d in dt_expected if is_in_tradfi_tick_window(d)}

            dt_found = dt_dates & dt_expected  # pyright: ignore[reportUnknownVariableType]
            dt_missing_dates = sorted(dt_expected - dt_found)  # pyright: ignore[reportUnknownArgumentType]

            scope_in, dt_is_processed, actionable_missing, blocked_dates = self._classify_data_type_for_venue(
                category=category,
                venue=venue,
                data_type=dt,
                missing_dates=dt_missing_dates,
                venue_df=venue_df,
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
    ) -> tuple[bool, bool, list[str], list[str]]:
        """Apply the four-state classification to a (venue, data_type) row.

        Returns ``(scope_in, is_processed, actionable_missing, blocked_dates)``.

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
        """Build per-league stats for a sports entity.

        Fixture-based model (when ``fixture_counts_by_league`` is provided):
        - The unit is the fixture (a single match), not the calendar date.
        - For each league, ``instrument_count`` sums give the fixture count.
        - For FIXTURES entity: expected = found (ground truth).
        - For other entities: expected = FIXTURES fixture count for that league.
        - Date lists are still included for backfill targeting.

        Legacy date-based model (``fixture_league_calendar`` provided):
        - Falls back to the old date-counting logic for non-sports callers.

        Args:
            fixture_league_calendar: Legacy mapping of league_id to fixture
                dates (date-based denominator). Used by non-sports callers.
            fixture_counts_by_league: Fixture-based mapping of league_id to
                total fixture count (from FIXTURES instrument_count sums).
            is_fixtures_entity: Whether this entity IS the FIXTURES entity.
            entity_coverage: If set, only these league_ids are expected for
                this entity (e.g. Understat XG covers 6 leagues).
            full_manifest: The full filtered manifest DataFrame (all entities)
                used to look up FIXTURES dates for missing-date calculation
                when the current entity is not FIXTURES.
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
                # Fixture-based model
                league_fixture_found = int(l_df["instrument_count"].sum())  # pyright: ignore[reportAny]

                if is_fixtures_entity:
                    # FIXTURES is ground truth — expected = found
                    league_fixture_expected = league_fixture_found
                elif entity_coverage is not None and lid.upper() not in entity_coverage:
                    # This league is not covered by this entity — skip it
                    continue
                else:
                    # Other entities: expected = FIXTURES count for this league
                    league_fixture_expected = fixture_counts_by_league.get(lid, league_fixture_found)  # pyright: ignore[reportOptionalMemberAccess]

                # Date lists for backfill targeting (which dates have gaps?)
                found_dates = sorted({str(d) for d in l_df["date"].unique()})  # pyright: ignore[reportAny]
                # Use full_manifest to find FIXTURES dates (venue_df is
                # filtered to the current entity, so FIXTURES rows are absent).
                manifest_src = full_manifest if full_manifest is not None else venue_df
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

                league_dict[lid] = {
                    "dates_found": league_fixture_found,
                    "dates_expected": max(1, league_fixture_expected),
                    "dates_missing": max(0, league_fixture_expected - league_fixture_found),
                    "missing_dates": missing_dates[:500],
                    "dates_found_list": found_dates[:500],
                    "completion_pct": min(
                        round(league_fixture_found / max(1, league_fixture_expected) * 100, 2),
                        100.0,
                    ),
                    "unit": "fixtures",
                }
            else:
                # Legacy date-based model (non-sports callers)
                found_dates_set = {str(d) for d in l_df["date"].unique()}  # pyright: ignore[reportAny]
                found_count = len(found_dates_set)

                if fixture_league_calendar and lid in fixture_league_calendar:
                    expected_dates = {d for d in fixture_league_calendar[lid] if d >= start_date}
                    expected_count = len(expected_dates)
                    missing_dates_list = sorted(expected_dates - found_dates_set)
                else:
                    expected_count = found_count
                    missing_dates_list = []

                league_dict[lid] = {
                    "dates_found": found_count,
                    "dates_expected": max(1, expected_count),
                    "dates_missing": len(missing_dates_list),
                    "missing_dates": missing_dates_list[:500],
                    "dates_found_list": sorted(found_dates_set)[:500],
                    "completion_pct": min(
                        round(found_count / max(1, expected_count) * 100, 2),
                        100.0,
                    ),
                }

        self._add_na_leagues(league_dict, entity_coverage, fixture_counts_by_league, is_fixtures_entity)
        return league_dict

    @staticmethod
    def _add_na_leagues(
        league_dict: dict[str, object],
        entity_coverage: frozenset[str] | None,
        fixture_counts_by_league: dict[str, int] | None,
        is_fixtures_entity: bool,
    ) -> None:
        """Add N/A entries for fixture leagues not covered by this entity.

        Mutates ``league_dict`` in place, appending greyed-out entries for
        leagues the entity does not cover (e.g. J-League has no Understat xG).
        """
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
        """Build per-sub-dimension stats for DEFI category.

        Groups rows by ``_defi_source`` (gas-fees, dex-pools, etc.) and produces
        per-sub-dimension stats with venues, found/expected, and completion_pct.
        The "main" DEFI bucket rows (source="") are shown as "defi-core".
        """
        if "_defi_source" not in filtered.columns:
            return {}

        # All known sub-dims plus any that appeared in the data
        all_sources = set(self._MTDS_DEFI_SUB_DIMENSIONS)
        data_sources = {str(s) for s in filtered["_defi_source"].unique() if s}  # pyright: ignore[reportAny]
        all_sources |= data_sources

        sub_dim_dict: dict[str, object] = {}
        for src in sorted(all_sources):
            src_mask = filtered["_defi_source"] == src
            src_df = filtered[src_mask]
            src_dates = {str(d) for d in src_df["date"].unique()} if not src_df.empty else set()  # pyright: ignore[reportAny,reportUnknownVariableType]
            src_venues = sorted(src_df["venue"].unique()) if not src_df.empty else []

            # Expected = dates where ANY venue in this sub-dim has data
            all_dates_range = pd.date_range(start_date, end_date, freq="D")
            expected_dates = {d.strftime("%Y-%m-%d") for d in all_dates_range}
            found_dates = src_dates & expected_dates  # pyright: ignore[reportUnknownVariableType]
            missing_dates = sorted(expected_dates - found_dates)  # pyright: ignore[reportUnknownArgumentType]

            sub_dim_dict[src] = {
                "dates_found": len(found_dates),  # pyright: ignore[reportUnknownArgumentType]
                "dates_expected": len(expected_dates),
                "dates_missing": len(missing_dates),
                "completion_pct": min(
                    round(len(found_dates) / max(1, len(expected_dates)) * 100, 2),  # pyright: ignore[reportUnknownArgumentType]
                    100.0,
                ),
                "venues": src_venues,
                "venue_count": len(src_venues),
            }

        # Include "defi-core" for the main bucket rows
        core_mask = filtered["_defi_source"] == ""
        if core_mask.any():
            core_df = filtered[core_mask]
            core_dates = {str(d) for d in core_df["date"].unique()}  # pyright: ignore[reportAny]
            core_venues = sorted(core_df["venue"].unique())  # pyright: ignore[reportAny]
            all_dates_range = pd.date_range(start_date, end_date, freq="D")
            expected_dates = {d.strftime("%Y-%m-%d") for d in all_dates_range}
            found_dates = core_dates & expected_dates

            sub_dim_dict["defi-core"] = {
                "dates_found": len(found_dates),
                "dates_expected": len(expected_dates),
                "dates_missing": len(expected_dates - found_dates),
                "completion_pct": min(
                    round(len(found_dates) / max(1, len(expected_dates)) * 100, 2),
                    100.0,
                ),
                "venues": core_venues,
                "venue_count": len(core_venues),
            }

        return sub_dim_dict

    def _venue_expected_dates_for_chain(
        self,
        chain_df: pd.DataFrame,
        chain_venues: list[str],
        start_date: str,
        end_date: str,
        venue_mapping: VenueMapping,
    ) -> dict[str, set[str]]:
        """Per-venue expected-dates lookup used by the chain breakdown.

        Extracted from ``_build_chain_breakdown`` to keep that method
        under the C901 complexity ceiling. Each venue's expected window
        is clipped to ``max(start_date, venue_start, inferred_min)``;
        when the venue mapping has no expected-trading-dates entry, the
        fallback is the calendar range.
        """
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
        """Sum ``expected_dates * distinct_leaves`` over the chain's venues.

        Leaf axis is ``(data_type, instrument_id)`` when the manifest
        carries an ``instrument_id`` column, otherwise just ``data_type``
        (per-protocol bundled shards like ``gas_fees`` / ``lending_indices``
        whose instrument fan-out lives at the row level, not a separate
        column). Empty leaf set falls back to one bundled instrument so
        single-protocol chains don't drop to a zero denominator.
        """
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
        """Build per-chain breakdown for DeFi data (v4 chain column).

        Groups venues by chain, producing a hierarchy:
        chain -> {venues, completion_pct, dates_found, dates_expected,
        shards_found, shards_expected}.

        The headline ratio used by the UI is now ``shards_found /
        shards_expected`` per the codex DeFi shard atom
        ``(asset_group=defi, chain, venue/protocol, data_type,
        instrument_id_or_protocol_id, day)``. The pre-2026-05-07
        date-only math (which collapsed the within-day fan-out across
        protocols x data_types x instruments and produced misleading
        "ARBITRUM 32/54" rollups when the real shard universe was ~25k)
        is preserved as ``dates_found`` / ``dates_expected`` for
        backward-compat -- UI consumers should switch to the new fields.
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
            chain_mask = filtered["chain"] == chain  # pyright: ignore[reportAny]
            chain_df = filtered[chain_mask]  # pyright: ignore[reportUnknownVariableType]
            chain_dates = {str(d) for d in chain_df["date"].unique()}  # pyright: ignore[reportUnknownArgumentType,reportAny,reportUnknownVariableType,reportUnknownMemberType]
            chain_venues = [  # pyright: ignore[reportUnknownVariableType]
                v  # pyright: ignore[reportUnknownVariableType]
                for v in (sorted(chain_df["venue"].unique()) if not chain_df.empty else [])  # pyright: ignore[reportUnknownMemberType,reportUnknownArgumentType,reportUnknownMemberType,reportUnknownVariableType]
                if v not in _ALL_DEFI_GHOST_VENUES
                and str(v).split("-", 1)[0] not in _ALL_DEFI_GHOST_VENUES  # pyright: ignore[reportUnknownArgumentType]
                and str(v).split("-", 1)[0] not in _DEFI_NON_PROTOCOL_VENUE_PREFIXES  # pyright: ignore[reportUnknownArgumentType]
            ]

            venue_expected_dates = self._venue_expected_dates_for_chain(
                chain_df,  # pyright: ignore[reportUnknownArgumentType]
                chain_venues,  # pyright: ignore[reportUnknownArgumentType]
                start_date,
                end_date,
                venue_mapping,
            )
            chain_expected_dates: set[str] = set()
            for v_expected in venue_expected_dates.values():
                chain_expected_dates |= v_expected

            dates_expected = len(chain_expected_dates) if chain_expected_dates else 1
            dates_found = len(chain_dates & chain_expected_dates) if chain_expected_dates else len(chain_dates)

            captured_chain_df = chain_df[chain_df["capture_status"] == "captured"] if has_capture_status else chain_df  # pyright: ignore[reportUnknownVariableType]
            shards_found = len(captured_chain_df)  # pyright: ignore[reportUnknownArgumentType]

            shards_expected = self._shards_expected_for_chain(chain_df, chain_venues, venue_expected_dates)  # pyright: ignore[reportUnknownArgumentType]
            if shards_expected == 0:
                shards_expected = max(1, shards_found)

            chain_dict[chain] = {
                "shards_found": shards_found,
                "shards_expected": shards_expected,
                "completion_pct": min(round(shards_found / max(1, shards_expected) * 100, 2), 100.0),
                "dates_found": dates_found,
                "dates_expected": dates_expected,
                "venues": chain_venues,
                "venue_count": len(chain_venues),  # pyright: ignore[reportUnknownArgumentType]
            }

        return chain_dict

    def _build_feature_group_breakdown(
        self,
        filtered: pd.DataFrame,
        start_date: str,
        end_date: str,
    ) -> dict[str, object]:
        """Build per-feature_group breakdown (v4 feature_group column).

        Groups by feature_group, with optional timeframe sub-level.
        """
        if "feature_group" not in filtered.columns:
            return {}

        groups = sorted(g for g in filtered["feature_group"].unique() if g)  # pyright: ignore[reportAny]
        if not groups:
            return {}

        fg_dict: dict[str, object] = {}

        for fg in groups:  # pyright: ignore[reportAny]
            fg_mask = filtered["feature_group"] == fg  # pyright: ignore[reportAny]
            fg_df = filtered[fg_mask]  # pyright: ignore[reportUnknownVariableType]
            fg_dates = {str(d) for d in fg_df["date"].unique()}  # pyright: ignore[reportUnknownArgumentType,reportUnknownVariableType,reportUnknownMemberType]
            found = len(fg_dates)

            # Expected = dates from first data appearance to end_date (not full query range)
            fg_start = min(fg_dates) if fg_dates else start_date
            fg_eff_start = max(start_date, fg_start)
            fg_expected_range = pd.date_range(fg_eff_start, end_date, freq="D")
            fg_expected = len(fg_expected_range)

            entry: dict[str, object] = {
                "dates_found": found,
                "dates_expected": fg_expected,
                "completion_pct": min(round(found / max(1, fg_expected) * 100, 2), 100.0),
            }

            # Add timeframe sub-breakdown if present
            has_tf = "timeframe" in fg_df.columns and fg_df["timeframe"].str.len().sum() > 0  # pyright: ignore[reportUnknownMemberType,reportAttributeAccessIssue,reportUnknownVariableType]
            if has_tf:
                timeframes: dict[str, object] = {}
                for tf in sorted(fg_df["timeframe"].unique()):  # pyright: ignore[reportUnknownVariableType,reportUnknownMemberType,reportUnknownArgumentType]
                    if not tf:
                        continue
                    tf_mask = fg_df["timeframe"] == tf  # pyright: ignore[reportUnknownVariableType]
                    tf_dates = {str(d) for d in fg_df.loc[tf_mask, "date"].unique()}  # pyright: ignore[reportUnknownArgumentType,reportUnknownVariableType,reportUnknownMemberType]
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

            fg_dict[fg] = entry

        return fg_dict

    def _build_underlying_grouping(
        self,
        filtered: pd.DataFrame,
        start_date: str,
        end_date: str,
        venue_mapping: VenueMapping,
    ) -> dict[str, object]:
        """Build top-level per-underlying breakdown across all venues.

        Groups instruments by their base asset (underlying) so the UI can show:
        BTC -> [all BTC instruments across venues], ETH -> [all ETH instruments], etc.

        Uses the ``underlying`` column from the manifest. When that column is
        empty, falls back to deriving the underlying from ``instrument_id``
        via ``_ensure_underlying_column``.
        """
        df = _ensure_underlying_column(filtered)

        has_underlying = (
            "underlying" in df.columns and not df.empty and df["underlying"].astype(str).str.len().sum() > 0
        )
        if not has_underlying:
            return {}

        underlyings = sorted(ul for ul in df["underlying"].unique() if ul and str(ul).strip())  # pyright: ignore[reportAny]
        if not underlyings:
            return {}

        ul_dict: dict[str, object] = {}

        for ul in underlyings:  # pyright: ignore[reportAny]
            ul_mask = df["underlying"] == ul  # pyright: ignore[reportAny]
            ul_df = df[ul_mask]  # pyright: ignore[reportUnknownVariableType]
            ul_dates = {str(d) for d in ul_df["date"].unique()}  # pyright: ignore[reportUnknownArgumentType,reportUnknownVariableType,reportUnknownMemberType]

            # Venues that carry this underlying
            ul_venues = (
                sorted(str(v) for v in ul_df["venue"].unique() if v and str(v).strip())  # pyright: ignore[reportUnknownArgumentType,reportUnknownVariableType,reportUnknownMemberType]
                if "venue" in ul_df.columns  # pyright: ignore[reportUnknownMemberType]
                else []
            )

            # Instrument types that carry this underlying
            ul_itypes = (
                sorted(str(it) for it in ul_df["instrument_type"].unique() if it and str(it).strip())  # pyright: ignore[reportUnknownArgumentType,reportUnknownVariableType,reportUnknownMemberType]
                if "instrument_type" in ul_df.columns  # pyright: ignore[reportUnknownMemberType]
                else []
            )

            # Expected = union of per-venue expected dates for venues carrying
            # this underlying.
            ul_expected_dates: set[str] = set()
            for v in ul_venues:
                vs = venue_mapping.get_venue_start_date(v)  # pyright: ignore[reportAny]
                if not vs:
                    v_mask = ul_df["venue"] == v  # pyright: ignore[reportUnknownVariableType]
                    v_dates = {str(d) for d in ul_df.loc[v_mask, "date"].unique()}  # pyright: ignore[reportUnknownArgumentType,reportUnknownVariableType,reportUnknownMemberType]
                    vs = min(v_dates) if v_dates else start_date
                eff_start = max(start_date, vs) if vs else start_date
                v_expected = set(venue_mapping.get_expected_trading_dates(v, eff_start, end_date))
                if not v_expected:
                    v_expected = {d.strftime("%Y-%m-%d") for d in pd.date_range(eff_start, end_date, freq="D")}
                ul_expected_dates |= v_expected

            if not ul_expected_dates:
                # Fallback when no venues are present
                ul_expected_dates = {d.strftime("%Y-%m-%d") for d in pd.date_range(start_date, end_date, freq="D")}

            expected = len(ul_expected_dates)
            found = len(ul_dates & ul_expected_dates)

            ul_dict[ul] = {
                "dates_found": found,
                "dates_expected": expected,
                "completion_pct": min(round(found / max(1, expected) * 100, 2), 100.0),
                "venues": ul_venues,
                "venue_count": len(ul_venues),
                "instrument_types": ul_itypes,
            }

        return ul_dict

    def _build_data_type_grouping(
        self,
        filtered: pd.DataFrame,
        start_date: str,
        end_date: str,
        cat: str,
        service: str = "instruments-service",
    ) -> tuple[dict[str, object], int, int]:
        """Group manifest data by data_type when venues are empty.

        Used for sports instruments pattern where venue column is blank but
        data_type distinguishes different entity categories.

        For SPORTS category, uses a fixture-based model:
        - The fundamental unit is the fixture (a single match), not the date.
        - ``instrument_count`` in each manifest row = number of fixtures on
          that date for that league.
        - FIXTURES entity: found/expected are fixture counts from the manifest.
        - Other entities: expected = total FIXTURES instrument_count (how many
          fixtures exist), found = this entity's instrument_count sum.
        - Per-league breakdowns use the same fixture-count logic.
        """
        is_sports = cat.upper() == "SPORTS"

        # For sports, pre-compute fixture counts from FIXTURES rows
        # (the ground-truth denominator for all other entities).
        fixtures_by_league: dict[str, int] = {}
        total_fixture_count = 0
        if is_sports and "instrument_count" in filtered.columns:
            fix_rows = filtered[
                (filtered["data_type"] == "FIXTURES") & (filtered["league_id"].fillna("").str.len() > 0)
            ]
            if not fix_rows.empty:
                for lid in fix_rows["league_id"].unique():  # pyright: ignore[reportAny]
                    if lid:
                        lid_count = int(fix_rows.loc[fix_rows["league_id"] == lid, "instrument_count"].sum())  # pyright: ignore[reportUnknownMemberType,reportUnknownArgumentType,reportAttributeAccessIssue]
                        fixtures_by_league[str(lid)] = lid_count  # pyright: ignore[reportAny]
                total_fixture_count = sum(fixtures_by_league.values())

        dt_venues: dict[str, object] = {}
        dt_found_total = 0
        dt_expected_total = 0
        # For SPORTS, always include all known data_types from SPORTS_DATA_TYPE_META
        # so the UI shows 0/N coverage for data types that haven't been captured yet,
        # rather than silently omitting them. For non-SPORTS, use only what's in the
        # manifest (iterating over non-SPORTS SSOT maps is not implemented yet).
        manifest_dt_vals: set[str] = set()
        if "data_type" in filtered.columns:
            manifest_dt_vals = {str(v) for v in filtered["data_type"].unique() if v and str(v).strip()}  # pyright: ignore[reportAny]
        # SPORTS: SSOT is canonical. Drop manifest data_types not in
        # SPORTS_DATA_TYPE_META — they're residual rows from removed entities
        # (e.g. SFI_STANDINGS, intentionally absent: SFI has no standings
        # endpoint) that would otherwise render as a row with empty
        # source/axis/unit and a misleading completion %. For non-SPORTS,
        # iterating over SSOT maps is not implemented yet — fall back to
        # whatever the manifest contains.
        # Dispatch on service: features-sports-service has its own SSOT meta
        # (FIXTURE_FEATURES, future ODDS_FEATURES/DERIVED_FEATURES) — those
        # are derived products, not raw source data, so they live under the
        # FSS service tab rather than instruments-service SPORTS.
        is_fss = service == "features-sports-service"
        active_meta: dict[str, dict[str, object]] = FEATURES_SPORTS_DATA_TYPE_META if is_fss else SPORTS_DATA_TYPE_META
        sports_ssot_vals: set[str] = set(active_meta.keys()) if is_sports else set()
        all_dt_vals: set[str] = sports_ssot_vals if is_sports else manifest_dt_vals
        for dt_val in sorted(all_dt_vals):
            if not dt_val or not str(dt_val).strip():
                continue
            if "data_type" in filtered.columns:
                dt_mask = filtered["data_type"] == dt_val
            else:
                dt_mask = pd.Series([False] * len(filtered), index=filtered.index)
            dt_df = filtered[dt_mask]
            dt_name = str(dt_val).upper()

            if is_sports and dt_name in active_meta:
                dt_entry = self._build_sports_entity_entry(
                    dt_df,
                    dt_name,
                    filtered,
                    fixtures_by_league,
                    total_fixture_count,
                    start_date,
                    end_date,
                )
            else:
                # Non-sports: keep existing date-based logic
                dt_dates = {str(d) for d in dt_df["date"].unique()}  # pyright: ignore[reportAny]
                dt_start = min(dt_dates) if dt_dates else start_date  # pyright: ignore[reportUnknownVariableType,reportUnknownArgumentType]
                dt_eff_start = max(start_date, dt_start)
                dt_found = len(dt_dates)  # pyright: ignore[reportUnknownArgumentType]
                dt_expected = len(pd.date_range(dt_eff_start, end_date, freq="D"))
                dt_entry = {
                    "dates_found": dt_found,
                    "dates_expected": dt_expected,
                    "dates_expected_venue": dt_expected,
                    "dates_missing": dt_expected - dt_found,
                    "completion_pct": min(round(dt_found / max(1, dt_expected) * 100, 2), 100.0),
                }

            dt_venues[str(dt_val)] = dt_entry
            dt_found_total += int(dt_entry["dates_found"])  # pyright: ignore[reportArgumentType]
            dt_expected_total += int(dt_entry["dates_expected"])  # pyright: ignore[reportArgumentType]
        return dt_venues, dt_found_total, dt_expected_total

    @staticmethod
    def _clamp_fixtures_to_entity_start(
        entity_name: str,
        full_filtered: pd.DataFrame,
        fixtures_by_league: dict[str, int],
        total_fixture_count: int,
    ) -> tuple[dict[str, int], int]:
        """Recompute fixture counts excluding rows before entity start date.

        Provider-specific start dates (from UAC ``SPORTS_ENTITY_START_DATES``)
        ensure pre-start fixtures are excluded from the denominator.
        Returns the (possibly clamped) fixtures_by_league and total count.
        """
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
        """Build a honest-coverage entry for a single sports data_type.

        SSOT: ``codex/02-data/sports-data-source-coverage-matrix.md``.

        Preferred path — ``_sports_honest_coverage`` uses
        ``SPORTS_DATA_TYPE_META`` to resolve the expected league set via UAC
        ``get_expected_leagues_for_source`` and the expected dates via
        ``get_league_fixture_calendar``. Numerator/denominator are both
        shard counts (distinct ``(league, date)`` pairs) — no cross-entity
        row-count comparison.

        Legacy path (when the entity is not in the SSOT map) falls back to
        the pre-2026-04-20 fixture-row-count model. Only kicks in for
        unrecognised data_types; adding a new SPORTS data_type should extend
        ``SPORTS_DATA_TYPE_META`` in the same commit as the adapter.
        """
        honest = _sports_honest_coverage(full_filtered, entity_name, start_date, end_date)
        if honest is not None:
            expected_shards = int(cast(int, honest["expected_shards"]))
            found_shards = int(cast(int, honest["found_shards"]))
            completion = round(found_shards / max(1, expected_shards) * 100, 2)
            dt_entry: dict[str, object] = {
                # Canonical honest-coverage fields
                "found_shards": found_shards,
                "expected_shards": expected_shards,
                "missing_shards": max(0, expected_shards - found_shards),
                "completion_pct": min(completion, 100.0),
                "unit": str(honest["unit"]),
                "axis": str(honest["axis"]),
                "source": str(honest["source"]),
                "expected_leagues": honest["expected_leagues"],
                # Legacy aliases — kept for UI/tests that haven't migrated yet.
                # Will be removed once deployment-ui DataStatusTab reads
                # ``found_shards`` / ``expected_shards`` directly.
                "dates_found": found_shards,
                "dates_expected": expected_shards,
                "dates_expected_venue": expected_shards,
                "dates_missing": max(0, expected_shards - found_shards),
            }
            per_league = honest["per_league"]
            if per_league:
                dt_entry["leagues"] = per_league
            return dt_entry

        # Legacy fallback for entities not yet in the SSOT map. Retained
        # unchanged to avoid regressions for adapters we haven't catalogued.
        is_fixtures = entity_name == "FIXTURES"
        has_league = "league_id" in dt_df.columns
        entity_fixture_count = int(dt_df["instrument_count"].sum()) if not dt_df.empty else 0  # pyright: ignore[reportAny]
        _entity_coverage = get_entity_league_coverage(entity_name)
        if is_fixtures:
            eff_fixtures_by_league = fixtures_by_league
            eff_total_fixture_count = total_fixture_count
        else:
            eff_fixtures_by_league, eff_total_fixture_count = self._clamp_fixtures_to_entity_start(
                entity_name, full_filtered, fixtures_by_league, total_fixture_count
            )
        if is_fixtures:
            dt_found = entity_fixture_count
            dt_expected = entity_fixture_count
        elif _entity_coverage is not None and eff_fixtures_by_league:
            dt_found = entity_fixture_count
            dt_expected = sum(eff_fixtures_by_league.get(lid, 0) for lid in _entity_coverage)
        else:
            dt_found = entity_fixture_count
            dt_expected = eff_total_fixture_count if eff_total_fixture_count > 0 else entity_fixture_count
        dt_entry = {
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

        # DeFi sub-dimension breakdown
        if "_defi_source" in filtered.columns and service == "market-tick-data-service" and cat.lower() == "defi":
            defi_sub_dims = self._build_defi_sub_dimension_breakdown(
                filtered,
                start_date,
                end_date,
            )
            if defi_sub_dims:
                extras["defi_sub_dimensions"] = defi_sub_dims

        # Chain breakdown for DeFi
        has_chain_data = "chain" in filtered.columns and not filtered.empty and filtered["chain"].str.len().sum() > 0
        if has_chain_data:
            chains_dict = self._build_chain_breakdown(
                filtered,
                start_date,
                end_date,
                venue_mapping,
            )
            if chains_dict:
                extras["chains"] = chains_dict

        # Feature group breakdown
        has_fg_data = (
            "feature_group" in filtered.columns and not filtered.empty and filtered["feature_group"].str.len().sum() > 0
        )
        if has_fg_data:
            fg_dict = self._build_feature_group_breakdown(
                filtered,
                start_date,
                end_date,
            )
            if fg_dict:
                extras["feature_groups"] = fg_dict

        # Underlying (base asset) grouping — top-level cross-venue view
        # Applicable to CEFI, TRADFI, DEFI categories where instruments have
        # a base asset (e.g. BTC, ETH, ES). Not applicable to SPORTS.
        if cat.upper() not in ("SPORTS",):
            ul_dict = self._build_underlying_grouping(
                filtered,
                start_date,
                end_date,
                venue_mapping,
            )
            if ul_dict:
                extras["underlyings"] = ul_dict

        return extras

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
    ) -> dict[str, object]:
        """Build a single category entry for manifest status.

        ``row_filters`` is an optional ``{column: value}`` map applied to
        the manifest rows after the date-range mask (and venue alias
        canonicalisation) but before the cell-grid compute. Used by the
        ``secondary_axis`` query parameter on ``/api/data-status/manifest``
        so the UI can drill into a single ``league_id`` /
        ``canonical_question_group`` / ``job_id`` / ``chain`` /
        ``fixture_id`` slice. Empty/None == no filter.

        ``pipeline_modes`` narrows the manifest slice to rows whose
        ``pipeline_mode`` column matches any of the supplied values (OR semantics).
        Used by the deployment-ui pipeline_mode filter chip.
        """
        empty: dict[str, object] = {
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
        if service not in SERVICE_TO_KIND and service != "features-commodity-service":
            return empty

        # Skip categories that don't apply to this service (single-bucket services)
        allowed = self._SERVICE_CATEGORY_RESTRICTIONS.get(service)
        if allowed and cat.upper() not in allowed:
            return empty

        # Resolve the main bucket name (for display in the response)
        override = self._BUCKET_CATEGORY_OVERRIDES.get((service, cat.lower()))
        if override:
            bucket = override.format(pid=self.project_id, env=self.deployment_env_short)
        elif service == "features-commodity-service":
            bucket = COMMODITY_BUCKET_TEMPLATE.format(pid=self.project_id)
        else:
            kind = SERVICE_TO_KIND[service]
            ag = cat.lower() or None
            if ag == "prediction":
                pred_kind = PREDICTION_KIND_MAP.get(kind)
                bucket = resolve_bucket_name(cloud=cast(object, cloud), kind=pred_kind if pred_kind else kind)  # pyright: ignore[reportArgumentType]
            else:
                bucket = resolve_bucket_name(cloud=cast(object, cloud), kind=kind, asset_group=cast(object, ag))  # pyright: ignore[reportArgumentType]

        index = self._read_defi_merged_index(service, cat, cloud=cloud)
        if index.empty:
            return empty

        # Clamp the category-level start date to the configured launch date
        # (from expected_start_dates.yaml). Pre-launch dates are not "missing"
        # — they never existed. Only the aggregation math is clamped; the raw
        # manifest data is untouched.
        effective_start = get_effective_start_date(start_date, service, cat)
        cat_date_strs = [d for d in all_date_strs if d >= effective_start]
        cat_total_days = len(cat_date_strs)

        mask = (index["date"] >= effective_start) & (index["date"] <= end_date)
        if "service_name" in index.columns:
            mask = mask & (index["service_name"] == service)
        filtered = index.loc[mask].copy()

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
            filtered = _canonicalise_defi_data_types(filtered)

        cat_found_dates = {str(d) for d in filtered["date"].unique()} if not filtered.empty else set()  # pyright: ignore[reportUnknownVariableType,reportAny]
        cat_missing = sorted(set(cat_date_strs) - cat_found_dates)
        cat_found = len(cat_found_dates)  # pyright: ignore[reportUnknownArgumentType]

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

        # MTDS honest-coverage override (Phase 6c). For CEFI / TRADFI / DEFI /
        # PREDICTION, recompute per-venue ``dates_found`` / ``dates_expected``
        # from the UAC-driven ``(venue, data_type, date)`` shard space AND
        # inject UAC-declared venues that had zero manifest rows. The old
        # path iterated only venues observed in the manifest, so a venue
        # missing completely (e.g. UPBIT with no trades shipped) was
        # invisible. SSOT: codex/02-data/mtds-data-source-coverage-matrix.md.
        if _is_mtds_honest_coverage_target(service, cat):
            (
                venues_dict,
                venue_found_total,
                venue_expected_total,
            ) = self._apply_mtds_honest_coverage(
                venues_dict,
                filtered,
                cat,
                effective_start,
                end_date,
                venue_mapping,
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

        # Category-level completion, two variants exposed for the UI:
        #
        #   * ``completion_pct_dates`` — fraction of dates in the clamped
        #     range that had ANY data. Over-states the real coverage when
        #     a single venue fills a date but most shards stay empty.
        #   * ``completion_pct_shards_weighted`` — fraction of expected
        #     shard-days present (per_bucket.dates_found / per_bucket.dates_expected
        #     rolled up across every per-bucket breakdown). Matches the
        #     shard-level math the user sees in the sub-rows
        #     (e.g. Polymarket header shows 94.4% = what the sub-row
        #     completion averages to, not 100%).
        #
        # ``completion_pct`` — primary metric — is the shards-weighted
        # value. Where the shards denominator is zero (categories with
        # no per-bucket breakdown yet) we fall back to the date-based
        # figure so the number is still meaningful.
        cat_pct_dates = min(round(cat_found / max(1, cat_total_days) * 100, 2), 100.0)
        if venue_expected_total > 0:
            cat_pct_shards = min(round(venue_found_total / venue_expected_total * 100, 2), 100.0)
        else:
            cat_pct_shards = cat_pct_dates

        coverage = _build_coverage_metrics(
            filtered,
            cat,
            cat_pct_shards,
            total_expected_cells=venue_expected_total,
        )
        coverage_semantics = coverage["coverage_semantics"]
        capture_coverage_pct = coverage["capture_coverage_pct"]
        attempt_coverage_pct = coverage["attempt_coverage_pct"]
        empty_rate_estimate = coverage["empty_rate_estimate"]
        failure_rate = coverage["failure_rate"]
        capture_status_counts = coverage["capture_status_counts"]
        counts = coverage["counts"]
        coverage_val = float(coverage["coverage"])  # pyright: ignore[reportArgumentType]
        cat_pct = coverage["completion_pct"]

        # v4 sub-dimension breakdowns (DeFi, chains, feature groups)
        sub_dims = self._build_v4_sub_dimensions(
            filtered,
            service,
            cat,
            effective_start,
            end_date,
            venue_mapping,
        )

        cat_found_sorted = sorted(cat_found_dates)  # pyright: ignore[reportUnknownArgumentType]
        # Sports uses fixture-based unit; other categories use dates
        unit = "fixtures" if cat.upper() == "SPORTS" and venues_dict else "dates"

        # Per-venue failure_rate map — surfaced so the UI's "show only failures"
        # filter and drill-down tooltip can scope to shards with failures
        # without walking the full venues_dict tree on the client.
        failure_rate_by_dimension = _build_failure_rate_by_dimension(venues_dict)

        # Axis discriminator: tells consumers which breakdown key holds the
        # drilldown. For SPORTS the venue column is structurally empty so the
        # drilldown is always under ``data_types``. For other categories the
        # discriminator follows whatever ``_maybe_group_by_data_type`` actually
        # did: if it regrouped (MDPS CEFI/DEFI rows have empty venue + real
        # data_types) we flip to ``data_type`` so the UI doesn't render
        # data_type strings as venues. Otherwise the venue grouping is real
        # (CEFI/TRADFI/DEFI/PREDICTION on instruments-service / MTDS).
        # SSOT: codex/02-data/sports-data-source-coverage-matrix.md §3.
        breakdown_axis = "data_type" if cat.upper() == "SPORTS" or regrouped_to_data_type else "venue"
        result: dict[str, object] = {
            "category": cat,
            "bucket": bucket,
            "prefixes_queried": 0,
            "dates_found": cat_found,
            "dates_expected": cat_total_days,
            "dates_missing": len(cat_missing),
            # ``shards_*`` mirrors ``venue_dates_*`` under canonical names so
            # the UI can render a consistent pair alongside the row-level
            # ``completion_pct`` (which is the shards-weighted ratio — see
            # `cat_pct = cat_pct_shards` above). Before this field existed
            # the UI showed ``dates_found / dates_expected`` next to a
            # shards-weighted ``completion_pct``, which looked wrong (e.g.
            # ``1 / 2577`` = 0.04%, but the row displayed ``20%``).
            "shards_found": venue_found_total,
            "shards_expected": venue_expected_total,
            "completion_pct": cat_pct,
            "completion_pct_dates": cat_pct_dates,
            "completion_pct_shards_weighted": cat_pct_shards,
            "attempt_coverage_pct": attempt_coverage_pct,
            "capture_coverage_pct": capture_coverage_pct,
            "coverage_semantics": coverage_semantics,
            "empty_rate_estimate": empty_rate_estimate,
            "failure_rate": failure_rate,
            "capture_status_counts": capture_status_counts,
            "counts": counts,
            "coverage": coverage_val,
            "venue_weighted": bool(venues_dict),
            "venue_dates_found": venue_found_total,
            "venue_dates_expected": venue_expected_total,
            "unit": unit,
            "effective_start_date": effective_start,
            "missing_dates": cat_missing,
            "dates_found_list": cat_found_sorted,
            "dates_missing_list": cat_missing,
            # Axis-aware breakdown: SPORTS drilldown is by data_type (no real
            # venues); other categories keep the existing ``venues`` shape.
            # UI reads ``breakdown_axis`` to pick the right key. ``venues``
            # stays populated (even if empty) for consumers that haven't
            # migrated yet — the aggregator only writes data under ONE of
            # ``venues`` or ``data_types`` depending on axis.
            "breakdown_axis": breakdown_axis,
            "venues": {} if breakdown_axis == "data_type" else venues_dict,
            "data_types": venues_dict if breakdown_axis == "data_type" else {},
            "failure_rate_by_dimension": failure_rate_by_dimension,
            "_venue_found": venue_found_total,
            "_venue_expected": venue_expected_total,
        }

        # MTDS honest-coverage — surface UAC-declared expected/missing venue
        # sets at the category level so the UI can render "venue X shipped
        # no data in this window" as a first-class gap. SSOT:
        # codex/02-data/mtds-data-source-coverage-matrix.md §1.
        self._annotate_mtds_category(result, service, cat, venues_dict, venue_mapping)

        result.update(sub_dims)
        return result

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
        expected_venues_list = _mtds_expected_venues(cat, venue_mapping)
        present_venues = {
            v
            for v, entry in venues_dict.items()
            if isinstance(entry, dict) and int(cast(int, entry.get("dates_found", 0))) > 0  # pyright: ignore[reportUnknownMemberType,reportUnknownArgumentType]
        }
        missing_venues = sorted(set(expected_venues_list) - present_venues)
        result["expected_venues"] = expected_venues_list
        result["missing_venues"] = missing_venues
        result["honest_axis"] = str(MTDS_CATEGORY_META[cat_key]["axis"])

    async def get_last_updated_info(
        self,
        service: str,
        asset_groups: list[str] | None = None,
    ) -> dict[str, object]:
        """
        Get last updated information for a service.

        Args:
            service: Service name to check
            asset_groups: Optional list of asset_groups to filter

        Returns:
            Dictionary containing last updated information
        """
        if service not in SERVICE_TO_KIND and service != "features-commodity-service":
            return {"error": f"Unknown service: {service}"}

        # Default asset_groups if none specified
        if not asset_groups:
            asset_groups = [cat.value.lower() for cat in MarketCategory]

        asset_groups_info: dict[str, object] = {}
        last_updated_info: dict[str, object] = {
            "service": service,
            "asset_groups": asset_groups_info,
            "overall_last_updated": None,
        }

        for category in asset_groups:
            try:
                bucket_name = _drilldown_build_bucket_name(service, category)

                # Check if bucket has any recent activity
                # Use the most recent object in the bucket as proxy
                objects = list_objects(bucket_name, "", max_results=10)

                if objects:
                    # Get the most recently created object
                    # This is a simplified approach - in production you might want
                    # to check specific paths or use bucket metadata
                    asset_groups_info[category] = {
                        "status": "active",
                        "object_count": len(objects),
                        "sample_paths": objects[:5],  # First 5 as examples
                    }
                else:
                    asset_groups_info[category] = {
                        "status": "empty",
                        "object_count": 0,
                    }

            except (OSError, ValueError, RuntimeError) as e:
                logger.debug("Error checking category %s: %s", category, e)
                asset_groups_info[category] = {
                    "status": "error",
                    "error": str(e),
                }

        return last_updated_info

    async def validate_data_completeness(
        self,
        service: str,
        date: str,
        asset_groups: list[str] | None = None,
        venues: list[str] | None = None,
    ) -> dict[str, object]:
        """
        Validate data completeness for a specific date.

        Args:
            service: Service name to validate
            date: Date in YYYY-MM-DD format
            asset_groups: Optional list of asset_groups to check
            venues: Optional list of venues to check

        Returns:
            Validation result with completeness details
        """
        # Get data status for single day
        result = await self.run_data_status_cli(
            service=service,
            start_date=date,
            end_date=date,
            asset_groups=asset_groups,
            venues=venues,
            show_missing=True,
        )

        if "error" in result:
            return result

        # Analyze completeness
        missing_venues: list[str] = []
        validation_errors: list[object] = []
        is_complete = True
        total_venues = 0
        completed_venues = 0

        dates_val: object = result.get("dates")
        if dates_val and isinstance(dates_val, list):
            dates_list = cast(list[object], dates_val)
            if dates_list and isinstance(dates_list[0], dict):
                date_data = cast(dict[str, object], dates_list[0])  # Single date

                venues_val: object = date_data.get("venues")
                if venues_val and isinstance(venues_val, list):
                    venues_list = cast(list[object], venues_val)
                    total_venues = len(venues_list)

                    for venue_info_raw in venues_list:
                        if not isinstance(venue_info_raw, dict):
                            continue
                        venue_info = cast(dict[str, object], venue_info_raw)
                        vname_raw: object = venue_info.get("venue", "unknown")
                        venue_name = vname_raw if isinstance(vname_raw, str) else "unknown"
                        status_raw: object = venue_info.get("status")
                        status = status_raw if isinstance(status_raw, str) else ""

                        if status == "missing":
                            is_complete = False
                            missing_venues.append(venue_name)
                        elif status == "error":
                            err_raw: object = venue_info.get("error", "Unknown error")
                            validation_errors.append(
                                {
                                    "venue": venue_name,
                                    "error": err_raw if isinstance(err_raw, str) else "Unknown error",
                                }
                            )
                        else:
                            completed_venues += 1

        completion_rate = (completed_venues / total_venues * 100) if total_venues > 0 else 0.0

        validation: dict[str, object] = {
            "service": service,
            "date": date,
            "is_complete": is_complete,
            "total_venues": total_venues,
            "completed_venues": completed_venues,
            "missing_venues": missing_venues,
            "errors": validation_errors,
            "completion_rate": completion_rate,
        }

        return validation

"""SPORTS / FEATURES-SPORTS coverage metadata + honest-coverage helpers.

Split out of the 6,663-line ``data_status_service.py`` god-module
(codex ratchet plan 2026-06-10). The facade module re-exports every
public + legacy-underscore name, so callers keep importing from
``deployment_api.services.data_status_service``.
"""

import logging
from collections.abc import Callable
from typing import Any, Literal, cast

import pandas as pd
from unified_api_contracts import LeagueDefinition
from unified_api_contracts.sports import (
    FEATURE_UPSTREAM_REQUIREMENTS,
    LEAGUE_REGISTRY,
    UpstreamReq,
    get_reference_refresh_dates,
    in_coverage,
)
from unified_api_contracts.sports import (
    clip_dates_to_source_coverage as _clip_dates_to_source_coverage,
)

import deployment_api.services.data_status_service as _dss

logger = logging.getLogger(__name__)


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
# - ``global_trigger_date``: one expected shard per trigger date (season-start
#   + transfer-window-open + transfer-window-close), across all leagues.
#   Used for global reference entities like TEAMS that are written at trigger
#   dates rather than daily (instrument-service writes master/ + snapshots/).
# - ``per_league_trigger_date``: one expected shard per (league, trigger_date).
#   Used for per-league reference entities like PLAYER_VALUES that are written
#   at league-specific trigger dates (season-start + window open/close).
SportsAxis = Literal[
    "per_league_per_fixture_date",
    "per_league_periodic",
    "per_feature_per_league_per_fixture_date",
    "global_periodic",
    "global_season",
    "global_trigger_date",
    "per_league_trigger_date",
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
        # TEAMS is a PER-LEAGUE reference entity written at trigger dates (season-
        # start + transfer-window-open + transfer-window-close), not daily and not
        # global. The instruments-service writer keys TEAMS rows on
        # ``(date, data_type='TEAMS', league_id)`` (sports_reference_core.py), and
        # both the UAC ``SHARD_AXIS_MATRIX`` (instruments-service/sports =
        # ("data_type","league_id")) and ``gcs_paths.py`` classify TEAMS per-league —
        # so the read-side axis MUST be ``per_league_trigger_date`` to restore
        # shard-atom identity (data_status_page_ux_and_canonicalisation_2026_07_16 P8;
        # was ``global_trigger_date`` — a 4-way drift vs writer/UAC/gcs_paths). This
        # shares PLAYER_VALUES' trigger-date cadence, so the per_league_trigger_date
        # branch (which computes expected_shards as the count of actual trigger dates
        # per league inside the window) applies directly. Each season's roster is a
        # distinct (date, league_id) snapshot, so the date axis under each league
        # surfaces per-season change; off-season dates read as legitimately empty
        # (honest-absence), not gaps. Depends on sports_master item A2.4 (write-path);
        # degrades gracefully per league if trigger dates can't be computed.
        "axis": "per_league_trigger_date",
        "unit": "trigger_date_snapshots",
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
    # Understat shot-level data — same 5 leagues, same per-(date,league) atom as XG.
    # Registered 2026-07-26 (was absent, so the Data Status tab filtered it out
    # entirely — see plans/active/issues/understat_bulk_download_backfill_2026_06_29.md §5).
    "XG_SHOTS": {
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
        # PLAYER_VALUES is trigger-based: the orchestrator only fetches at
        # season-start + transfer-window-open + transfer-window-close
        # (~3-4 capture dates per league per year via
        # ``get_reference_refresh_dates``). The per_league_trigger_date axis
        # computes expected_shards as the count of actual trigger dates per
        # league inside the window (not a daily or quarterly approximation).
        # Depends on sports_master item A2.4 (write-path); degrades gracefully.
        # See sports_master.md:1064.
        "axis": "per_league_trigger_date",
        "unit": "trigger_date_snapshots",
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
# (used by `sports_honest_coverage` for the legacy clip path; the per_feature
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


def expected_dates_for_upstream(
    req: UpstreamReq,
    league_id: str,
    start_date: str,
    end_date: str,
    walk: Callable[[str, frozenset[str]], list[str] | None],
    visited: frozenset[str],
) -> list[str] | None:
    """Expected dates for a single ``UpstreamReq`` of a feature calculator.

    ``derived`` upstreams recurse via ``walk``. Raw upstreams use
    ``in_coverage`` (date floor + league filter) over the league's fixture
    calendar.
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

    # Date-floor clip via the existing helper. Reuse the league fixture
    # calendar as the candidate set — features only run on fixture days.
    clipped_start, clipped_end = _clip_dates_to_source_coverage(
        req.source, start_date, end_date, data_type=req.data_type or None
    )
    if not clipped_end or clipped_end < clipped_start:
        return []
    candidate = _dss.get_league_fixture_calendar(league_id, clipped_start, clipped_end)
    return [d for d in candidate if in_coverage(req.source, req.data_type, league_id, d)]


def sports_expected_dates_for_league(
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
    starts 2019-01-01).

    Bounded source-level gaps (documented provider outages) are declared in
    the evidence-gated ``unified_api_contracts.canonical.coverage_exclusions``
    SSOT and applied by the cross-asset ``expected_coverage()`` oracle
    upstream of this helper — see
    ``codex/02-data/honest-coverage-model.md`` § Bounded coverage exclusions.

    Pass empty strings to skip clipping (preserves legacy callers that don't
    yet know the source/data_type).
    """
    if source_key:
        start_date, end_date = _clip_dates_to_source_coverage(
            source_key, start_date, end_date, data_type=data_type or None
        )
        if not end_date or end_date < start_date:
            return []
    season_dates = _dss.get_league_fixture_calendar(league_id, start_date, end_date)
    if axis == "per_league_per_fixture_date" or cadence_days <= 1:
        result = season_dates
    else:
        result = season_dates[::cadence_days]
    return result


def sports_trigger_dates_for_window(
    start_date: str,
    end_date: str,
) -> list[str]:
    """Derive global trigger dates for reference-entity denominator.

    Trigger dates are union of ``get_reference_refresh_dates(league_id, year)``
    across all leagues in ``LEAGUE_REGISTRY`` for every year in [start_date,
    end_date]. These correspond to season-start + transfer-window-open +
    transfer-window-close events — the dates when instruments-service writes
    reference data to ``master/`` and ``snapshots/trigger=T/`` paths.

    Degrades gracefully when ``get_reference_refresh_dates`` raises or returns
    an empty set — returns a sorted list of unique ISO date strings.

    Used by the ``global_trigger_date`` axis branch in ``sports_honest_coverage``
    (TEAMS denominator) and the ``per_league_trigger_date`` branch (PLAYER_VALUES
    denominator). See sports_master.md:1064 for the soft-gating note on the
    instruments-service write-path dependency.
    """
    from datetime import date as _date

    try:
        start_d = _date.fromisoformat(start_date)
        end_d = _date.fromisoformat(end_date)
    except ValueError:
        return []

    triggers: set[str] = set()
    for year in range(start_d.year, end_d.year + 1):
        for league_id in LEAGUE_REGISTRY:
            try:
                for t in get_reference_refresh_dates(league_id, year):
                    t_iso = t.isoformat()
                    if start_date <= t_iso <= end_date:
                        triggers.add(t_iso)
            except Exception:
                # Never hard-fail the denominator — missing a league or year
                # silently drops those trigger dates; the manifest rows still
                # surface in found_shards so coverage is at worst under-counted.
                continue
    return sorted(triggers)


def sports_trigger_dates_for_league(
    league_id: str,
    start_date: str,
    end_date: str,
) -> list[str]:
    """Derive per-league trigger dates for the ``per_league_trigger_date`` axis.

    Returns sorted ISO date strings in [start_date, end_date] that are trigger
    dates for this league (season-start + transfer-window-open/close via
    ``get_reference_refresh_dates``). Degrades gracefully on error.
    """
    from datetime import date as _date

    try:
        start_d = _date.fromisoformat(start_date)
        end_d = _date.fromisoformat(end_date)
    except ValueError:
        return []

    triggers: set[str] = set()
    for year in range(start_d.year, end_d.year + 1):
        try:
            for t in get_reference_refresh_dates(league_id, year):
                t_iso = t.isoformat()
                if start_date <= t_iso <= end_date:
                    triggers.add(t_iso)
        except Exception:
            continue
    return sorted(triggers)


def _sports_entity_rows(filtered: pd.DataFrame, entity_name: str, axis: str) -> pd.DataFrame:
    """Manifest rows for ``entity_name`` at this axis, gated on capture-status "ok-ness".

    skip-worthy = captured | empty_confirmed | expected_unattempted(EXPECTED_* reason). v4 rows
    without ``capture_status`` are implicit ``captured``. ``per_feature_per_league_per_fixture_date``
    matches on ``feature_group`` (features-sports-service writes keyed by feature_group=<calc_name>,
    NOT by data_type); every other axis matches on ``data_type``.
    """
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
        ent_mask = (
            filtered["feature_group"] == entity_name
            if "feature_group" in filtered.columns
            else pd.Series([False] * len(filtered), index=filtered.index)
        )
    else:
        ent_mask = filtered["data_type"] == entity_name
    return filtered[ent_mask & ok_mask]


def _honest_coverage_per_feature(
    entity_name: str,
    start_date: str,
    end_date: str,
    axis: str,
    source_key: str,
    expected_leagues: list[LeagueDefinition],
    ent_rows: pd.DataFrame,
    meta: dict[str, object],
) -> dict[str, object]:
    """Phase 3 honest-coverage per-calculator axis. For each league in the expected set, expected
    dates = intersection of every required upstream's per-league fixture calendar (clipped +
    known-gap-filtered) — ``_features_sports_expected_dates_for_calculator`` walks
    ``FEATURE_UPSTREAM_REQUIREMENTS`` and recurses through derived deps."""
    per_league_pf: dict[str, dict[str, object]] = {}
    total_expected_pf = 0
    total_found_pf = 0
    ent_rows_by_league_pf = (
        ent_rows.groupby(ent_rows["league_id"].fillna("")) if "league_id" in ent_rows.columns else None
    )
    for league in expected_leagues:
        lid = league.league_id
        expected_dates_pf = _dss._features_sports_expected_dates_for_calculator(  # pyright: ignore[reportPrivateUsage]  # facade patch-point (late-bound)
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


def _honest_coverage_global_trigger(
    axis: str,
    source_key: str,
    start_date: str,
    end_date: str,
    ent_rows: pd.DataFrame,
    meta: dict[str, object],
) -> dict[str, object]:
    """No league axis — expected = number of trigger dates in window derived from
    ``get_reference_refresh_dates`` across all LEAGUE_REGISTRY entries (season-start +
    transfer-window-open/close). Soft-gated on sports_master item A2.4
    (instruments-service write-path); degrades gracefully if the trigger set is empty."""
    trigger_dates = sports_trigger_dates_for_window(start_date, end_date)
    expected_shards_t = len(trigger_dates)
    found_dates_t: set[str] = {str(d) for d in ent_rows["date"].unique()} if not ent_rows.empty else set()  # pyright: ignore[reportAny]
    found_shards_t = len(found_dates_t & set(trigger_dates)) if trigger_dates else len(found_dates_t)
    return {
        "axis": axis,
        "unit": str(meta["unit"]),
        "source": source_key,
        "expected_leagues": [],
        "found_shards": found_shards_t,
        "expected_shards": expected_shards_t,
        "per_league": None,
        "trigger_dates": sorted(trigger_dates)[:100],
    }


def _honest_coverage_per_league_trigger(
    axis: str,
    source_key: str,
    start_date: str,
    end_date: str,
    expected_leagues: list[LeagueDefinition],
    ent_rows: pd.DataFrame,
    meta: dict[str, object],
) -> dict[str, object]:
    """Per-league axis — expected = count of trigger dates for each league in the expected set
    (season-start + transfer-window-open/close). Soft-gated on sports_master item A2.4
    (instruments-service write-path); degrades gracefully per league if trigger dates can't be
    computed."""
    per_league_td: dict[str, dict[str, object]] = {}
    total_expected_td = 0
    total_found_td = 0
    ent_rows_by_league_td = (
        ent_rows.groupby(ent_rows["league_id"].fillna("")) if "league_id" in ent_rows.columns else None
    )
    for league in expected_leagues:
        lid = league.league_id
        trigger_dates_l = sports_trigger_dates_for_league(lid, start_date, end_date)
        if not trigger_dates_l:
            continue
        expected_set_td = set(trigger_dates_l)
        found_set_td: set[str] = set()
        if ent_rows_by_league_td is not None and lid in ent_rows_by_league_td.groups:
            found_set_td = {str(d) for d in ent_rows_by_league_td.get_group(lid)["date"].unique()}  # pyright: ignore[reportAny]
        covered_td = expected_set_td & found_set_td
        missing_td = sorted(expected_set_td - found_set_td)
        per_league_td[lid] = {
            "found_shards": len(covered_td),
            "expected_shards": len(expected_set_td),
            "missing_shards": len(missing_td),
            "completion_pct": round(len(covered_td) / max(1, len(expected_set_td)) * 100, 2),
            "unit": str(meta["unit"]),
            "missing_dates": missing_td[:500],
            "found_dates_list": sorted(covered_td)[:500],
            "trigger_dates": trigger_dates_l[:100],
        }
        total_expected_td += len(expected_set_td)
        total_found_td += len(covered_td)
    return {
        "axis": axis,
        "unit": str(meta["unit"]),
        "source": source_key,
        "expected_leagues": [lg.league_id for lg in expected_leagues],
        "found_shards": total_found_td,
        "expected_shards": total_expected_td,
        "per_league": per_league_td,
    }


def _honest_coverage_global(
    axis: str,
    source_key: str,
    entity_name: str,
    start_date: str,
    end_date: str,
    cadence_days: int,
    ent_rows: pd.DataFrame,
    meta: dict[str, object],
) -> dict[str, object]:
    """No league axis — expected = number of cadence-dates in window, clipped to the source's
    UAC-declared coverage start so pre-launch dates don't show as missing."""
    clipped_start, clipped_end = _clip_dates_to_source_coverage(source_key, start_date, end_date, data_type=entity_name)
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


def _bucket_match_league_coverage(
    expected_set: set[str], found_set: set[str], cadence_days: int
) -> tuple[set[str], set[str]]:
    """Bucket-based week match for periodic cadences (2026-05-05 fix).

    The orchestrator writes manifest rows for every active-season fixture date (e.g. ~190 days/
    year for EPL), but ``sports_expected_dates_for_league`` subsamples to ``[::cadence_days]``
    (every 7th element = ~27/year for cadence=7). A direct ``expected_set & found_set``
    intersection then only counted manifest rows that happened to land on those exact 27
    subsampled positions — capping TM_LEAGUES at ~50%, SFI_LEAGUES at ~14%, PLAYER_VALUES at ~5%
    no matter how complete the manifest was. Fix: each expected date anchors a bucket of
    ``[d, d+cadence_days)``; a bucket is "covered" if any manifest date falls inside it. Returns
    ``(covered_buckets, missing_buckets)``.
    """
    sorted_expected = sorted(expected_set)
    sorted_found = sorted(found_set)
    covered_buckets: set[str] = set()
    missing_buckets: set[str] = set()
    f_idx = 0
    for anchor in sorted_expected:
        bucket_end = pd.Timestamp(anchor) + pd.Timedelta(days=cadence_days)
        hit = False
        while f_idx < len(sorted_found) and sorted_found[f_idx] < anchor:
            f_idx += 1
        probe = f_idx
        while probe < len(sorted_found) and pd.Timestamp(sorted_found[probe]) < bucket_end:
            hit = True
            probe += 1
        (covered_buckets if hit else missing_buckets).add(anchor)
    return covered_buckets, missing_buckets


def _honest_coverage_per_league(
    axis: str,
    source_key: str,
    entity_name: str,
    start_date: str,
    end_date: str,
    cadence_days: int,
    expected_leagues: list[LeagueDefinition],
    ent_rows: pd.DataFrame,
    meta: dict[str, object],
) -> dict[str, object]:
    """``per_league_per_fixture_date`` / ``per_league_periodic``. Single SSOT: per-league
    subpartition only — legacy bare-path date-aggregate captures were migrated to per-league via
    ``instruments-service/scripts/migrate_bare_to_per_league.py`` +
    ``reconcile_manifest_from_per_league_parquets.py`` (2026-05-01); the orchestrator now writes
    per-league exclusively for league-axis data types, so there is no bare-path fallback here."""
    per_league: dict[str, dict[str, object]] = {}
    total_expected = 0
    total_found = 0
    ent_rows_by_league = ent_rows.groupby(ent_rows["league_id"].fillna("")) if "league_id" in ent_rows.columns else None
    # For per_league_per_fixture_date (cadence_days <= 1) exact-match semantics apply; for
    # periodic cadences (cadence_days > 1) use the bucket match (see its own docstring).
    use_bucket_match = axis == "per_league_periodic" and cadence_days > 1

    for league in expected_leagues:
        lid = league.league_id
        expected_dates_for_l = sports_expected_dates_for_league(
            lid, axis, cadence_days, start_date, end_date, source_key=source_key, data_type=entity_name
        )
        if not expected_dates_for_l:
            continue
        expected_set = set(expected_dates_for_l)
        found_set: set[str] = set()
        if ent_rows_by_league is not None and lid in ent_rows_by_league.groups:
            found_set = {str(d) for d in ent_rows_by_league.get_group(lid)["date"].unique()}  # pyright: ignore[reportAny]

        if use_bucket_match:
            covered_buckets, missing_buckets = _bucket_match_league_coverage(expected_set, found_set, cadence_days)
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


def sports_honest_coverage(
    filtered: pd.DataFrame,
    entity_name: str,
    start_date: str,
    end_date: str,
) -> dict[str, object] | None:
    """Return honest-coverage stats for a SPORTS data_type using the SSOT meta.

    Uses ``SPORTS_DATA_TYPE_META`` + UAC ``get_expected_leagues_for_source`` +
    ``get_league_fixture_calendar`` to compute ``expected_shards`` = len(expected_leagues) *
    len(expected_dates_per_league) (or len(expected_dates) for global axes), ``found_shards`` —
    distinct (league_id, date) pairs with ``capture_status in {captured, empty_confirmed}`` — and
    ``missing_shards`` — per-league date-sets missing from the manifest (for UI drill-down). Each
    ``SportsAxis`` dispatches to its own helper (see ``_honest_coverage_*``). Returns ``None`` if
    the entity isn't in the SSOT map (caller falls back to the legacy date-count model).
    """
    meta = (
        SPORTS_DATA_TYPE_META.get(entity_name)
        or FEATURES_SPORTS_DATA_TYPE_META.get(entity_name)
        or FEATURES_SPORTS_PER_CALC_META.get(entity_name)
    )
    if meta is None:
        return None

    meta = cast(dict[str, Any], meta)
    axis = str(meta["axis"])  # pyright: ignore[reportAny]
    cadence_days = int(meta.get("cadence_days") or 1)
    source_key = str(meta["source"])  # pyright: ignore[reportAny]
    classifications = tuple(cast(tuple[str, ...], meta["classifications"]))

    expected_leagues = _dss.get_expected_leagues_for_source(source_key, classifications=list(classifications))
    ent_rows = _sports_entity_rows(filtered, entity_name, axis)

    if axis == "per_feature_per_league_per_fixture_date":
        return _honest_coverage_per_feature(
            entity_name, start_date, end_date, axis, source_key, expected_leagues, ent_rows, meta
        )
    if axis == "global_trigger_date":
        return _honest_coverage_global_trigger(axis, source_key, start_date, end_date, ent_rows, meta)
    if axis == "per_league_trigger_date":
        return _honest_coverage_per_league_trigger(
            axis, source_key, start_date, end_date, expected_leagues, ent_rows, meta
        )
    if axis in ("global_periodic", "global_season"):
        return _honest_coverage_global(
            axis, source_key, entity_name, start_date, end_date, cadence_days, ent_rows, meta
        )
    return _honest_coverage_per_league(
        axis, source_key, entity_name, start_date, end_date, cadence_days, expected_leagues, ent_rows, meta
    )


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
# the existing SPORTS code path (instruments-service ``sports_honest_coverage``)
# keeps running. See §5 of the codex SSOT.

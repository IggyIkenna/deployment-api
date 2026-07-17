"""Group catalogue sports fixtures by league then day for the fixtures browser.

Variant of ``upcoming_fixtures.py``: instead of "next N days forward from
today" (a flat sorted list), this groups ALL fixtures in a BOUNDED window
around today — ``[today-window_days_back, today+window_days_forward]`` — by
``league_id`` then by UTC calendar day, for the data-status fixtures browser
drilldown (operator request, P9,
``plans/active/data_status_page_ux_and_canonicalisation_2026_07_16.md``).

Reuses ``upcoming_fixtures``'s per-day threaded reader (``_read_frames_for_window``),
row parser (``_row_to_fixture``) and bucket resolver (``_sports_bucket``) —
same legacy-singleton / split-``fixtures_schedule``-shard fallback, same
shard-level failure isolation (one bad day is skipped, never aborts the
whole response). Cross-module import of these leading-underscore helpers
mirrors the established in-repo convention (e.g.
``deployment_api/routes/_fleet_inventory.py`` importing symbols from
``deployment_api/routes/_fleet_census.py``).

Single-walk discipline: this module does NOT ``list_blobs``/glob over the
whole ``sports_reference/`` prefix — it reads explicit per-day paths across a
capped window (``_MAX_WINDOW_SIDE_DAYS`` each direction), the same
bounded-window approach ``upcoming_fixtures.py`` uses. A full-catalogue,
unwindowed listing was considered (the sports instrument catalogue rolls up
fixture rows too — see
``instruments-service/scripts/build_instrument_catalogue.py``'s
``build_sports_fixture_team_player_catalogue``) but rejected: that catalogue
is itself windowed (``SPORTS_FTP_WINDOW_DAYS`` trailing days) AND drops the
``kickoff_utc``/``status`` columns the operator's ask needs (kickoff time,
status) — it only carries first/last-seen dates. Reading the day-window
parquets directly (this module) is both cheaper and carries the fields the
UI needs.
"""

from __future__ import annotations

import time
from datetime import UTC, date, datetime, timedelta

import pandas as pd

from deployment_api.services.upcoming_fixtures import (
    UpcomingFixture,
    _read_frames_for_window,  # shared day-window reader, see module docstring
    _row_to_fixture,
    _sports_bucket,
)

#: A fixture row grouped for the browser response — identical shape to
#: ``upcoming_fixtures.UpcomingFixture`` (same parser produces both).
FixtureRow = UpcomingFixture

#: league_id -> day (YYYY-MM-DD, UTC) -> fixtures for that league/day.
FixturesByLeagueAndDay = dict[str, dict[str, list[FixtureRow]]]

#: Cap on days_back / days_forward each — keeps the per-day threaded read
#: bounded even if a caller passes an unreasonable query param.
_MAX_WINDOW_SIDE_DAYS = 60

#: Cap on an ABSOLUTE ``[start_date, end_date]`` span (see ``_resolve_window``).
#: Same bound the today-relative window can reach (60 back + 60 forward), so an
#: absolute range can JUMP anywhere in history but never reads more days than
#: the relative window already could — single-walk discipline unchanged.
_MAX_WINDOW_SPAN_DAYS = _MAX_WINDOW_SIDE_DAYS * 2

# In-process TTL cache, same rationale as upcoming_fixtures._FIXTURES_CACHE
# (transpacific GCS round-trips per day; the Data Status panel re-mounts this
# query on every tab open). Keyed on the RESOLVED window + every filter, so a
# team/date narrow can't serve another query's rows.
_BROWSE_CACHE: dict[tuple[str, int, str | None, str | None], tuple[float, FixturesByLeagueAndDay]] = {}
_BROWSE_CACHE_TTL_SEC: int = 300

#: Fixture fields a ``team=`` needle is matched against (case-insensitive
#: substring). Both the human name and the canonical id are searched so the
#: operator can type either "Arsenal" or the raw team id, and a fixture matches
#: whether the team played HOME or AWAY.
_TEAM_MATCH_FIELDS: tuple[str, ...] = (
    "home_team_name",
    "away_team_name",
    "home_team_id",
    "away_team_id",
)


def _parse_iso_date(value: str | None) -> date | None:
    """``YYYY-MM-DD`` -> ``date``; ``None`` for blank/unparseable (caller falls
    back to the today-relative window rather than erroring on a stray param)."""
    if not value or not value.strip():
        return None
    try:
        return date.fromisoformat(value.strip())
    except ValueError:
        return None


def _resolve_window(
    *,
    window_days_back: int,
    window_days_forward: int,
    start_date: str | None,
    end_date: str | None,
) -> tuple[date, int]:
    """Resolve the day window to read → ``(start, inclusive_extra_days)``.

    Two modes:

    * **Absolute** (either ``start_date`` or ``end_date`` given) — the window is
      the given ``[start, end]``, letting the UI jump to ANY date range (the
      today-relative window can only reach 60 days back, so a search for last
      season's fixtures was previously impossible). A missing side is filled
      from the corresponding relative default; the span is clamped to
      ``_MAX_WINDOW_SPAN_DAYS`` (keeping ``start``, truncating ``end``).
    * **Relative** (neither given) — the pre-existing
      ``[today-window_days_back, today+window_days_forward]`` default.
    """
    start_d = _parse_iso_date(start_date)
    end_d = _parse_iso_date(end_date)

    if start_d is None and end_d is None:
        today = datetime.now(UTC).date()
        return today - timedelta(days=window_days_back), window_days_back + window_days_forward

    # Absolute mode — fill whichever side is missing from its relative default.
    if start_d is None:
        anchor = end_d if end_d is not None else datetime.now(UTC).date()
        start_d = anchor - timedelta(days=window_days_back)
    if end_d is None:
        end_d = start_d + timedelta(days=window_days_forward)

    if end_d < start_d:
        start_d, end_d = end_d, start_d
    span = min((end_d - start_d).days, _MAX_WINDOW_SPAN_DAYS)
    return start_d, span


def _matches_team(fx: UpcomingFixture, needle: str) -> bool:
    """Case-insensitive substring match across home/away team name + id."""
    return any(needle in str(fx.get(field, "")).lower() for field in _TEAM_MATCH_FIELDS)  # type: ignore[literal-required]


def _day_key(kickoff_iso: str) -> str:
    """UTC calendar day (``YYYY-MM-DD``) from an ISO ``kickoff_utc`` string."""
    return kickoff_iso[:10] if len(kickoff_iso) >= 10 else kickoff_iso


def _dedupe_preserve_order(fixtures: list[UpcomingFixture]) -> list[UpcomingFixture]:
    seen: set[str] = set()
    unique: list[UpcomingFixture] = []
    for fx in fixtures:
        fid = fx["fixture_id"]
        if fid in seen:
            continue
        seen.add(fid)
        unique.append(fx)
    return unique


def list_fixtures_by_league_and_day(
    *,
    window_days_back: int = 7,
    window_days_forward: int = 30,
    league_id: str | None = None,
    team: str | None = None,
    start_date: str | None = None,
    end_date: str | None = None,
) -> FixturesByLeagueAndDay:
    """Group catalogue fixtures in a bounded window by league then UTC day.

    Filterable on all three axes the operator asked for (2026-07-17):

    * **date** — either the today-relative ``[today-window_days_back,
      today+window_days_forward]`` default (both clamped to
      ``[0, _MAX_WINDOW_SIDE_DAYS]``), or an ABSOLUTE ``[start_date, end_date]``
      (``YYYY-MM-DD``) which can address ANY range in history, span-capped to
      ``_MAX_WINDOW_SPAN_DAYS`` — see :func:`_resolve_window`.
    * **league** — exact ``league_id`` match (unchanged).
    * **team** — case-insensitive substring across home/away team name AND id,
      matching whether the team played home or away (:func:`_matches_team`).

    Missing parquet objects or per-day read failures are skipped (shard-level
    failure isolation) — one bad day never empties the whole response.
    Days/leagues with no fixtures simply have no entry in the result (no
    phantom empty buckets).
    """
    window_days_back = max(0, min(window_days_back, _MAX_WINDOW_SIDE_DAYS))
    window_days_forward = max(0, min(window_days_forward, _MAX_WINDOW_SIDE_DAYS))
    league_filter = league_id.strip() if league_id and league_id.strip() else None
    team_filter = team.strip().lower() if team and team.strip() else None

    start, inclusive_extra_days = _resolve_window(
        window_days_back=window_days_back,
        window_days_forward=window_days_forward,
        start_date=start_date,
        end_date=end_date,
    )

    cache_key = (start.isoformat(), inclusive_extra_days, league_filter, team_filter)
    now = time.monotonic()
    cached = _BROWSE_CACHE.get(cache_key)
    if cached is not None and (now - cached[0]) < _BROWSE_CACHE_TTL_SEC:
        return cached[1]

    bucket = _sports_bucket()

    frames = _read_frames_for_window(
        bucket,
        start=start,
        inclusive_extra_days=inclusive_extra_days,
    )
    if not frames:
        _BROWSE_CACHE[cache_key] = (now, {})
        return {}

    all_df = pd.concat(frames, ignore_index=True)
    if league_filter and "league_id" in all_df.columns:
        all_df = all_df[all_df["league_id"].astype(str) == league_filter]

    if "kickoff_utc" not in all_df.columns:
        _BROWSE_CACHE[cache_key] = (now, {})
        return {}

    parsed: list[UpcomingFixture] = []
    for rec in all_df.to_dict(orient="records"):
        fx = _row_to_fixture(rec)
        if fx is None:
            continue
        # Team narrow is applied post-parse (not on the raw frame) so it keys on
        # the parser's normalized field names — the split-shard variants
        # (``af_home_name`` etc.) are only unified at that point.
        if team_filter and not _matches_team(fx, team_filter):
            continue
        parsed.append(fx)

    parsed.sort(key=lambda f: f["kickoff_utc"])
    unique = _dedupe_preserve_order(parsed)

    grouped: FixturesByLeagueAndDay = {}
    for fx in unique:
        lid = fx["league_id"] or "UNKNOWN"
        day = _day_key(fx["kickoff_utc"])
        grouped.setdefault(lid, {}).setdefault(day, []).append(fx)

    _BROWSE_CACHE[cache_key] = (now, grouped)
    return grouped

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
from datetime import UTC, datetime, timedelta

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

# In-process TTL cache, same rationale as upcoming_fixtures._FIXTURES_CACHE
# (transpacific GCS round-trips per day; the Data Status panel re-mounts this
# query on every tab open).
_BROWSE_CACHE: dict[tuple[int, int, str | None], tuple[float, FixturesByLeagueAndDay]] = {}
_BROWSE_CACHE_TTL_SEC: int = 300


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
) -> FixturesByLeagueAndDay:
    """Group catalogue fixtures in a bounded window by league then UTC day.

    Window is ``[today-window_days_back, today+window_days_forward]``
    inclusive (both clamped to ``[0, _MAX_WINDOW_SIDE_DAYS]``). Missing
    parquet objects or per-day read failures are skipped (shard-level
    failure isolation) — one bad day never empties the whole response.
    Days/leagues with no fixtures simply have no entry in the result (no
    phantom empty buckets).
    """
    window_days_back = max(0, min(window_days_back, _MAX_WINDOW_SIDE_DAYS))
    window_days_forward = max(0, min(window_days_forward, _MAX_WINDOW_SIDE_DAYS))
    league_filter = league_id.strip() if league_id and league_id.strip() else None

    cache_key = (window_days_back, window_days_forward, league_filter)
    now = time.monotonic()
    cached = _BROWSE_CACHE.get(cache_key)
    if cached is not None and (now - cached[0]) < _BROWSE_CACHE_TTL_SEC:
        return cached[1]

    bucket = _sports_bucket()
    today = datetime.now(UTC).date()
    start = today - timedelta(days=window_days_back)

    frames = _read_frames_for_window(
        bucket,
        start=start,
        inclusive_extra_days=window_days_back + window_days_forward,
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
        if fx is not None:
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

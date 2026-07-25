"""Group catalogue sports fixtures by league then day for the fixtures browser.

Single-file catalogue source (P10-B,
``plans/active/sports_fixtures_browser_single_catalogue_source_2026_07_24.md``):
reads the SAME rolled-up ``prod/catalog.parquet`` ``upcoming_fixtures`` sibling
services already read (schema-aware projection, ONE GCS GET), filters
``instrument_type=="fixture"``, and TTL-caches the PARSED frame — mirrors
``prediction_catalogue.py``'s ``_read_catalogue`` pattern (same bucket family,
same single-GET-then-cache shape).

This REPLACES the prior ``<=120-day`` per-day GCS walk (``_read_frames_for_window``
in ``upcoming_fixtures.py``) — the catalogue roll-up (``build_instrument_catalogue.py``'s
``build_sports_fixture_team_player_catalogue``) already carries every field this
browser needs (``kickoff_utc``/``status``/team names/``venue_name``/``round``,
instruments-service@684a1b2b) over its FULL history (``--since`` full-history
rollup: 105,509 fixtures, 2019-01-01→2026-07-17, kickoff/status/team names 100%,
independently verified 14/14 PASS), so reading it once and filtering in-memory is
both cheaper AND removes the artificial 120-day span cap the old walk needed to
bound its per-day read cost.

Row mapping: ``fixture_id`` = the catalogue's ``instrument_id`` (already the
canonical fixture id, ``{league}:{home}_v_{away}:{YYYYMMDD[_HHMM]}`` — UAC
``build_fixture_id``); ``home_team_id``/``away_team_id`` are parsed straight out
of that id's ``HOME_v_AWAY`` segment (already canonical team ids, no re-slugging
needed); ``venue_id`` is honestly blank — the catalogue does not carry it.

**Filter AND group on ``available_from``** (not ``kickoff_utc``) — it is the
catalogue's first-observed-day column and was verified 17,064/17,064 (100%)
identical to the id's ``:YYYYMMDD`` suffix (zero drift), so it is the true
fixture date; ``kickoff_utc`` is carried through onto the row for display/sort
only. Reuses ``upcoming_fixtures.py``'s row parser (``_row_to_fixture``, fed an
augmented record so it can build the row from catalogue columns unchanged),
league-name/filter helpers (``league_names_for_ids``, ``_matching_league_ids``)
and bucket resolver (``_sports_bucket``) — see that module's docstring for why
the league helpers live there (avoids a circular import: this module already
imports FROM ``upcoming_fixtures``).
"""

from __future__ import annotations

import io
import logging
import time
from datetime import UTC, date, datetime, timedelta

import pandas as pd

from deployment_api.services.upcoming_fixtures import (
    UpcomingFixture,
    _matching_league_ids,  # shared league-filter matcher, see module docstring
    _norm_str,  # shared missing-safe stringifier, see module docstring
    _row_to_fixture,
    _sports_bucket,
    league_names_for_ids,  # shared league-name resolver, see module docstring
)
from deployment_api.utils.storage_client import get_storage_client

logger = logging.getLogger(__name__)

#: A fixture row grouped for the browser response — identical shape to
#: ``upcoming_fixtures.UpcomingFixture`` (same parser produces both).
FixtureRow = UpcomingFixture

#: league_id -> day (YYYY-MM-DD, UTC) -> fixtures for that league/day.
FixturesByLeagueAndDay = dict[str, dict[str, list[FixtureRow]]]

#: Cap on days_back / days_forward each for the TODAY-RELATIVE window mode —
#: a sane UI-facing default bound, NOT a read-cost bound anymore (the catalogue
#: is read once regardless of window size; see module docstring).
_MAX_WINDOW_SIDE_DAYS = 60

#: Only genuine fixture rows (excludes the catalogue's team/player grain rows —
#: see ``build_instrument_catalogue.py``'s ``SPORTS_FIXTURE_INSTRUMENT_TYPE``).
_FIXTURE_INSTRUMENT_TYPE = "fixture"

#: Columns projected from ``prod/catalog.parquet`` (schema-aware — only present
#: columns are read, so an older/newer catalogue degrades gracefully). Matches
#: the plan's spec exactly (P10-B design).
_CATALOGUE_READ_COLUMNS: list[str] = [
    "instrument_id",
    "instrument_type",
    "league_id",
    "available_from",
    "kickoff_utc",
    "status",
    "home_team_name",
    "away_team_name",
    "venue_name",
    "round",
]

# In-process TTL cache of the PARSED, fixture-filtered catalogue frame — read
# ONCE per TTL window, not per query (the catalogue only regenerates on its own
# rollup cadence, so a 5-min cache is generous, matching the sibling
# prediction_catalogue.py / upcoming_fixtures.py caches' rationale).
_CATALOGUE_CACHE_TTL_SEC: int = 300
_catalogue_cache: tuple[float, pd.DataFrame] | None = None

# Per-query grouped-result cache — keyed on the RESOLVED window + every filter,
# same rationale as before (a cheap re-serve for the Data Status panel's
# repeated tab-open re-mounts, even though the underlying catalogue frame is
# itself already cached).
_BROWSE_CACHE: dict[tuple[str, str, str | None, str | None], tuple[float, FixturesByLeagueAndDay]] = {}
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
    """Resolve the day window to filter -> ``(start, inclusive_extra_days)``.

    Two modes:

    * **Absolute** (either ``start_date`` or ``end_date`` given) — the window is
      the given ``[start, end]``, letting the UI jump to ANY date range in the
      catalogue's full history. A missing side is filled from the corresponding
      relative default. UNCAPPED (unlike the old day-walk): filtering an
      already-loaded in-memory frame costs the same regardless of span, so
      there is no read-cost reason to bound it anymore.
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
    return start_d, (end_d - start_d).days


def _matches_team(fx: UpcomingFixture, needle: str) -> bool:
    """Case-insensitive substring match across home/away team name + id."""
    return any(needle in str(fx.get(field, "")).lower() for field in _TEAM_MATCH_FIELDS)  # type: ignore[literal-required]


def _available_from_day(val: object) -> str:
    """UTC calendar day (``YYYY-MM-DD``) from a catalogue ``available_from`` cell.

    The verified true fixture date (17,064/17,064 identical to the id's
    ``:YYYYMMDD`` suffix, zero drift) — see module docstring. Handles a pandas
    Timestamp, a python ``date``, or an already-ISO string uniformly (all
    stringify with the date first).
    """
    s = _norm_str(val)
    return s[:10] if len(s) >= 10 else s


def _team_ids_from_instrument_id(instrument_id: str) -> tuple[str, str]:
    """Parse ``(home_team_id, away_team_id)`` from the fixture id's ``HOME_v_AWAY``
    segment.

    Fixture ids are ``{league_id}:{home}_v_{away}:{date}[_time]`` (UAC
    ``build_fixture_id``) — the middle segment already carries the canonical
    (SCREAMING_SNAKE_CASE) team ids the writer built at capture time, so this is
    a straight split, never a re-derivation. Returns ``("", "")`` for any id
    that doesn't match the expected shape (honest absence, never a guessed split).
    """
    parts = instrument_id.split(":")
    if len(parts) < 2 or "_v_" not in parts[1]:
        return "", ""
    home, _, away = parts[1].partition("_v_")
    return home, away


def league_names_for(grouped: FixturesByLeagueAndDay) -> dict[str, str]:
    """Map each ``league_id`` present in ``grouped`` → its human ``display_name``.

    Unresolved ids are OMITTED (honest-absence): the UI renders the raw id for any
    league not in the map, never a placeholder/fabricated name. Pure in-memory
    lookup over the already-read grouping — no extra GCS walk. Thin wrapper over
    ``upcoming_fixtures.league_names_for_ids`` (the shared resolver — see module
    docstring for why it lives there).
    """
    return league_names_for_ids(grouped.keys())


def _read_catalogue_fixture_frame() -> pd.DataFrame | None:
    """Read ``prod/catalog.parquet`` (ONE GCS GET), filtered to fixture rows —
    TTL-cached so this only actually hits GCS once per cache window regardless
    of how many queries land in between.

    ``None`` on any read failure (missing bucket/object, unreadable parquet) or
    an empty/fixture-less catalogue — shard-isolated, the caller returns an
    honest empty result, never raises.
    """
    global _catalogue_cache
    now = time.monotonic()
    if _catalogue_cache is not None and (now - _catalogue_cache[0]) < _CATALOGUE_CACHE_TTL_SEC:
        return _catalogue_cache[1]

    import pyarrow.parquet as pq  # noqa: imports-inside-functions — lazy heavy SDK

    try:
        bucket = _sports_bucket()
        raw = get_storage_client().download_bytes(bucket, "prod/catalog.parquet")
        schema_names = set(pq.read_schema(io.BytesIO(raw)).names)
        present = [c for c in _CATALOGUE_READ_COLUMNS if c in schema_names]
        df = pd.read_parquet(io.BytesIO(raw), columns=present or None)
    except Exception as exc:
        # Shard-isolated: a catalogue read failure must not sink the caller.
        logger.warning("fixtures-browser: catalogue read failed (%s) — returning empty", exc)
        return None
    if df.empty or "instrument_type" not in df.columns:
        return None
    fixtures = df[df["instrument_type"].astype(str) == _FIXTURE_INSTRUMENT_TYPE]
    if fixtures.empty:
        return None
    _catalogue_cache = (now, fixtures)
    return fixtures


def _catalogue_rec_to_fixture(rec: dict[str, object]) -> FixtureRow | None:
    """Map one catalogue fixture record -> ``FixtureRow``.

    Augments the record with ``fixture_id``/``home_team_id``/``away_team_id``
    (derived, see module docstring) then delegates to ``upcoming_fixtures``'s
    ``_row_to_fixture`` for the actual parsing (kickoff normalisation, honest
    missing-value handling) — one parser, one set of edge cases, for both the
    day-walk (``upcoming_fixtures``) and catalogue (this module) sources.
    ``venue_id`` is deliberately left unset — the catalogue does not carry it
    (honest absence, never fabricated).
    """
    instrument_id = _norm_str(rec.get("instrument_id"))
    if not instrument_id:
        return None
    home_id, away_id = _team_ids_from_instrument_id(instrument_id)
    augmented: dict[str, object] = dict(rec)
    augmented["fixture_id"] = instrument_id
    augmented["home_team_id"] = home_id
    augmented["away_team_id"] = away_id
    return _row_to_fixture(augmented)


def list_fixtures_by_league_and_day(
    *,
    window_days_back: int = 7,
    window_days_forward: int = 30,
    league_id: str | None = None,
    team: str | None = None,
    start_date: str | None = None,
    end_date: str | None = None,
) -> FixturesByLeagueAndDay:
    """Group catalogue fixtures in a window by league then ``available_from`` day.

    Filterable on all three axes the operator asked for (2026-07-17):

    * **date** — either the today-relative ``[today-window_days_back,
      today+window_days_forward]`` default (both clamped to
      ``[0, _MAX_WINDOW_SIDE_DAYS]``), or an ABSOLUTE ``[start_date, end_date]``
      (``YYYY-MM-DD``) which can address ANY range in the catalogue's full
      history, UNCAPPED — see :func:`_resolve_window`.
    * **league** — case-insensitive substring against either the raw catalogue
      league key OR its resolved human display_name (e.g. "Allsvenskan" matches
      the numeric raw id it resolves to; "113" still matches the raw id even
      once a name exists) — see ``upcoming_fixtures.league_matches_filter``.
    * **team** — case-insensitive substring across home/away team name AND id,
      matching whether the team played home or away (:func:`_matches_team`).

    A catalogue read failure or an empty/out-of-range result yields an honest
    empty dict — never phantom empty buckets for a day with no fixtures.
    """
    window_days_back = max(0, min(window_days_back, _MAX_WINDOW_SIDE_DAYS))
    window_days_forward = max(0, min(window_days_forward, _MAX_WINDOW_SIDE_DAYS))
    league_filter = league_id.strip().lower() if league_id and league_id.strip() else None
    team_filter = team.strip().lower() if team and team.strip() else None

    start, inclusive_extra_days = _resolve_window(
        window_days_back=window_days_back,
        window_days_forward=window_days_forward,
        start_date=start_date,
        end_date=end_date,
    )
    end = start + timedelta(days=inclusive_extra_days)

    cache_key = (start.isoformat(), end.isoformat(), league_filter, team_filter)
    now = time.monotonic()
    cached = _BROWSE_CACHE.get(cache_key)
    if cached is not None and (now - cached[0]) < _BROWSE_CACHE_TTL_SEC:
        return cached[1]

    df = _read_catalogue_fixture_frame()
    if df is None or "available_from" not in df.columns:
        _BROWSE_CACHE[cache_key] = (now, {})
        return {}

    avail_day = df["available_from"].map(_available_from_day)
    windowed = df[(avail_day >= start.isoformat()) & (avail_day <= end.isoformat())]
    if windowed.empty:
        _BROWSE_CACHE[cache_key] = (now, {})
        return {}

    if league_filter and "league_id" in windowed.columns:
        league_id_col = windowed["league_id"].astype(str)
        matching_ids = _matching_league_ids(league_id_col.unique().tolist(), league_filter)
        windowed = windowed[league_id_col.isin(matching_ids)]

    parsed: list[tuple[str, FixtureRow]] = []
    for rec in windowed.to_dict(orient="records"):
        day = _available_from_day(rec.get("available_from"))
        if not day:
            continue
        fx = _catalogue_rec_to_fixture(rec)
        if fx is None:
            continue
        # Team narrow is applied post-parse (not on the raw frame) so it keys on
        # the parser's normalized field names.
        if team_filter and not _matches_team(fx, team_filter):
            continue
        parsed.append((day, fx))

    parsed.sort(key=lambda pair: pair[1]["kickoff_utc"])

    grouped: FixturesByLeagueAndDay = {}
    seen: set[str] = set()
    for day, fx in parsed:
        fid = fx["fixture_id"]
        if fid in seen:
            continue
        seen.add(fid)
        lid = fx["league_id"] or "UNKNOWN"
        grouped.setdefault(lid, {}).setdefault(day, []).append(fx)

    _BROWSE_CACHE[cache_key] = (now, grouped)
    return grouped


def _clear_cache() -> None:  # test hook
    global _catalogue_cache
    _catalogue_cache = None
    _BROWSE_CACHE.clear()

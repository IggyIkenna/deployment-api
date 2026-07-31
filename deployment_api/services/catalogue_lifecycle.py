"""New-listings + upcoming-expiries, derived read-only from the per-asset-group
lifecycle catalogue (``prod/catalog.parquet`` in ``instruments-store-{ag}-…``).

Mirrors ``upcoming_fixtures.py`` (5-min in-process TTL cache, TypedDict rows,
shard-isolated per-AG reads — a missing/failed AG parquet is skipped, never a
cross-AG raise). The catalogue is the ONLY identity-level source: one row per
instrument with ``available_from`` (listing date = MIN(first-observed,
venue-declared)) and ``available_to`` (a 3-way value: expiry / None-if-active /
last-observed).

``delisted_at`` is NOT a fourth ``available_to`` source. It exists only as a UAC
schema field (``internal/reference/instrument.py``, ``_instruments_parquet_schema``)
that **no adapter/writer ever populates**: the only assignments workspace-wide are
in ``build_instrument_catalogue.py``'s roll-up, and those read it back out of the
per-date rows (``_extract_meta``) — nothing puts it there. Measured 2026-07-17:
non-blank on **0 rows across 98 per-date cefi venue files / 38,582 rows** spanning
four day-scans two years apart (2024-01-15 / 2024-07-15 / 2025-07-15 / 2026-07-15);
the column is present in every file and empty in all of them. The roll-up's
``delisted_at``-wins-over-liveness branch is therefore unreachable on real data.

Load-bearing correctness rule for **Upcoming Expiries** (plan
data_status_page_ux_and_canonicalisation_2026_07_16 P2): filter
``instrument_type ∈ {FUTURE, OPTION, COMBO}`` AND ``available_to`` inside the
forward window ``[today, today+within_days]``. Because last-observed values are
always ≤ today, the forward window admits only genuine future expiries — the type
filter + forward window together make it correct even though ``available_to`` is
overloaded.
"""

from __future__ import annotations

import io
import logging
import threading
import time
from collections.abc import Iterator
from concurrent.futures import ThreadPoolExecutor
from contextlib import contextmanager
from datetime import UTC, datetime, timedelta
from typing import TypedDict

import pandas as pd

from deployment_api.utils.storage_client import get_storage_client

logger = logging.getLogger(__name__)


class CatalogueLifecycleBuildBusyError(RuntimeError):
    """Too many uncached new-listings/upcoming-expiries builds already in flight.

    Raised by ``_guarded_build`` (never by a route directly) so the route layer
    can translate it into a 503 + Retry-After, mirroring
    ``routes/data_status/_deploy_turbo.py``'s drilldown load-shed.
    """


# ── Concurrent-build guard (2026-07-31, deployment_api_sigabrt_crash_loop_2026_07_24.md
# OOM-kill investigation) ────────────────────────────────────────────────────
# An UNCACHED new-listings/upcoming-expiries request fans out up to
# ``_MAX_AG_WORKERS`` (5) concurrent per-AG GCS parquet reads via ThreadPoolExecutor
# (``_build_new_listings_frame``/``_build_expiries_frame`` below) — this exact
# multi-AG "first-mount burst" is what forced the 8Gi->16Gi bump on 2026-07-17
# (see cloudbuild.yaml's deploy-step comment). That bump assumed at most ONE such
# burst in flight at a time; it does not bound how many DISTINCT uncached requests
# (different filter params -> different 5-min TTL cache keys, e.g. several
# dashboard panels/pages) can each independently fan out 5 reads concurrently on
# the SAME worker. A freshly AUTOSCALED instance starts with an empty cache, so a
# burst landing right after cold-start is exactly the scenario most likely to
# stack multiple 5-way fan-outs and reproduce the original OOM shape — measured
# 2026-07-29..31: 8 "Container terminated on signal 9" events in 7 days, all with
# memory usage only marginally (0.06%-4%) over the 16384 MiB limit, most
# correlated with an "AUTOSCALING"-reason "Starting new instance" log line.
# PER-WORKER (module-level), matching the drilldown guard's own scoping.
_MAX_CONCURRENT_BUILDS = 2
_build_semaphore = threading.Semaphore(_MAX_CONCURRENT_BUILDS)


@contextmanager
def _guarded_build(*, kind: str) -> Iterator[None]:
    """Shed load (raise, never block) when too many uncached builds are already
    running on this worker, instead of letting them stack past the container's
    memory ceiling. Cached hits never reach this guard (checked upstream)."""
    if not _build_semaphore.acquire(blocking=False):
        logger.warning("catalogue-lifecycle %s build shed — %d already in flight", kind, _MAX_CONCURRENT_BUILDS)
        raise CatalogueLifecycleBuildBusyError(
            f"catalogue-lifecycle {kind} build is at capacity (too many concurrent uncached builds)"
        )
    try:
        yield
    finally:
        _build_semaphore.release()


# Asset groups whose lifecycle catalogue we scan. instruments-store-{ag} for
# all; prediction is its own bucket KIND (no asset_group entry) — matches
# ``manifest_source.read_unique_instrument_count`` + build_instrument_catalogue.
_CATALOGUE_ASSET_GROUPS: tuple[str, ...] = ("cefi", "defi", "tradfi", "sports", "prediction")

# instrument_type values that genuinely carry a forward EXPIRY. Spot/perp/pool
# rows never expire, so their ``available_to`` is only ever a delisting /
# last-observed (≤ today) and must NOT surface as an "upcoming expiry".
_EXPIRING_TYPES: frozenset[str] = frozenset({"FUTURE", "OPTION", "COMBO"})

_MAX_NEW_LISTING_AGE_DAYS = 365
_MAX_EXPIRY_WINDOW_DAYS = 365

# Pagination (mirrors routes/data_status/_catalogue.py's DEFAULT/MAX_CATALOGUE_LIMIT).
DEFAULT_LIFECYCLE_LIMIT = 50
MAX_LIFECYCLE_LIMIT = 500

# Per-AG reads are independent transpacific GCS GETs (~3-17s each depending on
# asset_group size — prediction alone is 2.9M rows / ~17s). Serial-summed that
# was measured 35s/31s for new-listings(30)/upcoming-expiries(5) — past the
# Cloud Run / browser fetch timeout -> 500 "Unknown error" (plan F10, measured
# 2026-07-17). Threading across the 5 asset groups collapses wall-clock to
# roughly the SLOWEST single read instead of the serial sum.
_MAX_AG_WORKERS = len(_CATALOGUE_ASSET_GROUPS)

# Columns projected from catalog.parquet (a subset — the file has ~24 columns).
_READ_COLUMNS: list[str] = [
    "instrument_id",
    "instrument_type",
    "venue",
    "chain",
    "available_from",
    "available_to",
    "base_asset",
    "raw_symbol",
    "mvp",
]

# 5-min TTL, keyed on (kind, sorted-params). Same rationale as upcoming_fixtures:
# the catalogue refreshes at most daily, the panel re-mounts on every tab open,
# and each miss is a transpacific GCS GET per asset_group.
# Internal (non-response) frame columns. The venue-first-day tag is computed
# over the WHOLE catalogue for an asset_group -- never over the windowed slice,
# or every row in the window would trivially tag against the window's own min.
_AF_COL: str = "_af_date"
_AT_COL: str = "_at_date"
_VENUE_FIRST_DAY_COL: str = "_af_is_venue_first_day"
_AG_COL: str = "_lifecycle_asset_group"

_CACHE_TTL_SEC: int = 300
# Caches hold the PREPARED (narrowed/tagged/sorted, cross-AG-concatenated)
# DataFrame, not built row dicts -- so a page request only ever materialises
# its own slice into dicts (previously every matching row was built even for
# an unbounded caller, e.g. 644,380 rows for list_new_listings(30)). Building
# ALL rows (unpaginated ``list_new_listings``/``list_upcoming_expiries``) still
# reads through this same cache, it just materialises the whole frame.
_NEW_LISTINGS_FRAME_CACHE: dict[tuple[int, str | None, str | None], tuple[float, pd.DataFrame]] = {}
_EXPIRIES_FRAME_CACHE: dict[tuple[int, str | None, str | None], tuple[float, pd.DataFrame]] = {}


class CatalogueLifecycleRow(TypedDict):  # CORRECT-LOCAL — API response shape, no cross-service consumer
    """One instrument row for the new-listings / upcoming-expiries API."""

    instrument_id: str
    instrument_type: str
    asset_group: str
    venue: str
    chain: str
    base_asset: str
    raw_symbol: str
    available_from: str
    available_to: str
    mvp: bool
    #: True when this row's ``available_from`` is the EARLIEST date the catalogue
    #: holds for its venue — i.e. the listing date may be an artifact of when the
    #: pipeline onboarded the venue rather than a real listing. See
    #: ``_mark_venue_first_day`` for why this is a fact, not a guess.
    available_from_is_venue_first_day: bool


def _catalogue_bucket(asset_group: str, *, cloud: str = "gcp") -> str:
    from unified_trading_library import resolve_bucket_name  # noqa: qg-inside-import

    if asset_group == "prediction":
        return resolve_bucket_name(cloud=cloud, kind="instruments-store-prediction")
    return resolve_bucket_name(cloud=cloud, kind="instruments-store", asset_group=asset_group)


def _read_catalogue(asset_group: str) -> pd.DataFrame | None:
    """Read ``prod/catalog.parquet`` for one asset_group (ONE GCS GET). ``None``
    on any failure (missing bucket/object, unreadable parquet) — the caller skips
    this AG, never raising across the other asset groups (shard-level failure
    isolation). Projects only ``_READ_COLUMNS`` that the parquet actually carries
    so an older catalogue (missing a column) degrades gracefully."""
    import pyarrow.parquet as pq  # noqa: imports-inside-functions — lazy heavy SDK

    try:
        bucket = _catalogue_bucket(asset_group)
        raw = get_storage_client().download_bytes(bucket, "prod/catalog.parquet")
        schema_names = set(pq.read_schema(io.BytesIO(raw)).names)
        present = [c for c in _READ_COLUMNS if c in schema_names]
        df = pd.read_parquet(io.BytesIO(raw), columns=present or None)
    except Exception as exc:
        # Shard-isolated: a bad asset_group must not sink the rest.
        logger.warning("catalogue-lifecycle read failed for %s (%s) — skipping this asset_group", asset_group, exc)
        return None
    if df.empty:
        return None
    return df


def _norm(val: object) -> str:
    if val is None:
        return ""
    try:
        if pd.isna(val):  # type: ignore[reportArgumentType]
            return ""
    except (TypeError, ValueError):
        pass
    return str(val).strip()


def _iso_date(val: object) -> str:
    """Normalise a catalogue date value to a ``YYYY-MM-DD`` prefix (or "")."""
    s = _norm(val)
    return s[:10] if s else ""


def _mark_venue_first_day(df: pd.DataFrame) -> pd.DataFrame:
    """Tag each row with whether its ``available_from`` is the earliest date the
    catalogue holds for that row's venue (``_VENUE_FIRST_DAY_COL``).

    **Why this exists (new-listings false-positive guard).** The catalogue's
    ``available_from`` is ``MIN(first_day_observed, venue_declared_listing_date)``
    (``build_instrument_catalogue.py`` ~1086-1092). When a venue declares no
    listing date, that MIN silently degrades to *the day the pipeline first saw
    the instrument*. For a venue the pipeline onboarded RECENTLY, its entire
    pre-existing universe therefore lands on one day and floods "new listings"
    as though it had all just been listed.

    **This tag is a FACT, not an inference**: it says only "this row's
    available_from equals the earliest date we hold for this venue". It is
    deliberately NOT called "is a false positive" — a venue that genuinely
    launched a product line on its first captured day would carry the same
    coincidence, and we cannot distinguish the two from the catalogue alone
    (it stores the MIN result, not which side won). So we surface the fact and
    let the reader judge, rather than silently dropping rows that may be real.

    Measured on real prod GCS 2026-07-17 (all 5 asset groups, 4,310,720 rows):
    37,308 rows (0.87%) corpus-wide carry the coincidence, but only **99 of the
    439,940 rows inside the live 30-day new-listings window** (0.02%) — a single
    cluster, ``COINBASE-CDE`` on 2026-07-10, all 99 with expiries running out to
    2030-12-20 and ``market_created_at`` empty (no venue-truth date existed, so
    available_from fell back to the pipeline's onboarding day). Control: the
    signature discriminates cleanly — onboarding floods score high
    (``BITGET-SPOT`` 61% of its rows on its first day, ``LIGHTER-ZKSYNC`` 100%)
    while established venues are negligible (``DERIBIT`` 0.09%), and a genuine
    new listing on an established venue is never tagged.
    """
    if "venue" not in df.columns or _AF_COL not in df.columns:
        df[_VENUE_FIRST_DAY_COL] = False
        return df
    venue_min = df.groupby("venue")[_AF_COL].transform("min")
    df[_VENUE_FIRST_DAY_COL] = df[_AF_COL] == venue_min
    return df


def _row(asset_group: str, rec: dict[str, object]) -> CatalogueLifecycleRow:
    mvp_raw = rec.get("mvp")
    return CatalogueLifecycleRow(
        instrument_id=_norm(rec.get("instrument_id")),
        instrument_type=_norm(rec.get("instrument_type")),
        asset_group=asset_group,
        venue=_norm(rec.get("venue")),
        chain=_norm(rec.get("chain")),
        base_asset=_norm(rec.get("base_asset")),
        raw_symbol=_norm(rec.get("raw_symbol")),
        available_from=_iso_date(rec.get("available_from")),
        available_to=_iso_date(rec.get("available_to")),
        mvp=bool(mvp_raw) if mvp_raw is not None and not _pandas_missing(mvp_raw) else False,
        available_from_is_venue_first_day=bool(rec.get(_VENUE_FIRST_DAY_COL, False)),
    )


def _pandas_missing(val: object) -> bool:
    try:
        return bool(pd.isna(val))  # type: ignore[reportArgumentType]
    except (TypeError, ValueError):
        return False


def _scan_asset_groups(asset_group: str | None) -> tuple[str, ...]:
    if asset_group:
        ag = asset_group.strip().lower()
        return (ag,) if ag in _CATALOGUE_ASSET_GROUPS else ()
    return _CATALOGUE_ASSET_GROUPS


def _venue_filter(df: pd.DataFrame, venue: str | None) -> pd.DataFrame:
    if venue and "venue" in df.columns:
        return df[df["venue"].astype(str) == venue.strip()]
    return df


def _read_and_filter_new_listings(ag: str, *, cutoff: str, today_iso: str, venue_f: str | None) -> pd.DataFrame | None:
    """One asset_group's worth of new-listings work: read + venue-narrow + tag
    + date-mask. Runs inside the ``ThreadPoolExecutor`` in ``_build_new_listings_frame``
    — every read is independent (own bytes download, own DataFrame), so this is
    safe to fan out across asset groups without shared mutable state."""
    df = _read_catalogue(ag)
    if df is None or "available_from" not in df.columns:
        return None
    df = _venue_filter(df, venue_f)
    if df.empty:
        return None
    df = df.copy()
    df[_AF_COL] = df["available_from"].astype(str).str.slice(0, 10)
    # Tag against the venue's WHOLE-catalogue first day, BEFORE the window
    # below narrows the frame — otherwise every row would tag against the
    # window's own minimum and the flag would be meaningless.
    df = _mark_venue_first_day(df)
    # available_from within [cutoff, today] — a listing date can't be in the future.
    mask = (df[_AF_COL] >= cutoff) & (df[_AF_COL] <= today_iso)
    out = df[mask]
    if out.empty:
        return None
    out = out.copy()
    out[_AG_COL] = ag
    return out


def _build_new_listings_frame(*, max_age_days: int, asset_group: str | None, venue_f: str | None) -> pd.DataFrame:
    """Narrowed + tagged + SORTED cross-AG frame for new-listings — no row
    dicts built here (that only happens for the requested page/full list).
    The 5 per-AG reads are independent + shard-isolated, so they run in a
    bounded ThreadPool: cold wall-clock collapses to roughly the slowest
    single read (~17s) instead of the serial sum (~35s, plan F10)."""
    today = datetime.now(UTC).date()
    cutoff = (today - timedelta(days=max_age_days)).isoformat()
    today_iso = today.isoformat()
    ags = _scan_asset_groups(asset_group)
    if not ags:
        return pd.DataFrame()
    with _guarded_build(kind="new-listings"), ThreadPoolExecutor(max_workers=min(len(ags), _MAX_AG_WORKERS)) as ex:
        frames = list(
            ex.map(
                lambda ag: _read_and_filter_new_listings(ag, cutoff=cutoff, today_iso=today_iso, venue_f=venue_f),
                ags,
            )
        )
    parts = [f for f in frames if f is not None]
    if not parts:
        return pd.DataFrame()
    combined = pd.concat(parts, ignore_index=True)
    return combined.sort_values(_AF_COL, ascending=False, kind="stable")  # newest first


def _new_listings_frame(*, max_age_days: int, asset_group: str | None, venue: str | None) -> pd.DataFrame:
    """Cached (5-min TTL) prepared frame — the single read/compute path shared
    by ``list_new_listings`` (full list) and ``list_new_listings_page`` (one page)."""
    max_age_days = max(1, min(max_age_days, _MAX_NEW_LISTING_AGE_DAYS))
    venue_f = venue.strip() if venue and venue.strip() else None
    key = (max_age_days, asset_group, venue_f)
    now = time.monotonic()
    cached = _NEW_LISTINGS_FRAME_CACHE.get(key)
    if cached is not None and (now - cached[0]) < _CACHE_TTL_SEC:
        return cached[1]
    frame = _build_new_listings_frame(max_age_days=max_age_days, asset_group=asset_group, venue_f=venue_f)
    _NEW_LISTINGS_FRAME_CACHE[key] = (now, frame)
    return frame


def _rows_from_lifecycle_frame(df: pd.DataFrame) -> list[CatalogueLifecycleRow]:
    """Materialise row dicts from an already-prepared (narrowed/tagged/sorted)
    frame slice — callers pass either the whole frame or just a page."""
    if df.empty:
        return []
    out: list[CatalogueLifecycleRow] = []
    for rec in df.to_dict(orient="records"):
        ag = str(rec.get(_AG_COL) or "")
        out.append(_row(ag, rec))
    return out


def list_new_listings(
    *,
    max_age_days: int = 30,
    asset_group: str | None = None,
    venue: str | None = None,
) -> list[CatalogueLifecycleRow]:
    """Instruments whose ``available_from`` is within the last ``max_age_days``
    days (newest-first). Read-only, 5-min TTL, shard-isolated per asset_group.

    Builds row dicts for EVERY matching row — for the paginated/fast path used
    by the API route, see ``list_new_listings_page``."""
    frame = _new_listings_frame(max_age_days=max_age_days, asset_group=asset_group, venue=venue)
    return _rows_from_lifecycle_frame(frame)


def list_new_listings_page(
    *,
    max_age_days: int = 30,
    asset_group: str | None = None,
    venue: str | None = None,
    limit: int = DEFAULT_LIFECYCLE_LIMIT,
    offset: int = 0,
) -> tuple[list[CatalogueLifecycleRow], int]:
    """``(page_rows, total_count)`` — the paginated twin of ``list_new_listings``.
    Same cached prepared frame; only the requested page ever gets built into
    row dicts, so a 50-row page costs 50 row builds instead of every matching
    row (previously up to 644,380 for the 30-day default, plan F10)."""
    safe_limit = max(1, min(int(limit or DEFAULT_LIFECYCLE_LIMIT), MAX_LIFECYCLE_LIMIT))
    safe_offset = max(0, int(offset or 0))
    frame = _new_listings_frame(max_age_days=max_age_days, asset_group=asset_group, venue=venue)
    total_count = len(frame)
    page = frame.iloc[safe_offset : safe_offset + safe_limit]
    return _rows_from_lifecycle_frame(page), total_count


def _read_and_filter_expiries(ag: str, *, today_iso: str, horizon_iso: str, venue_f: str | None) -> pd.DataFrame | None:
    """One asset_group's worth of upcoming-expiries work — mirrors
    ``_read_and_filter_new_listings``' independence/thread-safety."""
    df = _read_catalogue(ag)
    if df is None or "available_to" not in df.columns or "instrument_type" not in df.columns:
        return None
    df = _venue_filter(df, venue_f)
    if df.empty:
        return None
    df = df.copy()
    # Same tag as new-listings: the row type carries the field on BOTH
    # endpoints, so computing it here too keeps it truthful rather than
    # letting _row default it to a silent False.
    df[_AF_COL] = df["available_from"].astype(str).str.slice(0, 10) if "available_from" in df.columns else ""
    df = _mark_venue_first_day(df)
    itype = df["instrument_type"].astype(str).str.upper()
    df[_AT_COL] = df["available_to"].astype(str).str.slice(0, 10)
    mask = itype.isin(_EXPIRING_TYPES) & (df[_AT_COL] >= today_iso) & (df[_AT_COL] <= horizon_iso)
    out = df[mask]
    if out.empty:
        return None
    out = out.copy()
    out[_AG_COL] = ag
    return out


def _build_expiries_frame(*, within_days: int, asset_group: str | None, venue_f: str | None) -> pd.DataFrame:
    """Narrowed + tagged + SORTED cross-AG frame for upcoming-expiries — same
    bounded-ThreadPool fan-out across asset groups as new-listings."""
    today = datetime.now(UTC).date()
    today_iso = today.isoformat()
    horizon_iso = (today + timedelta(days=within_days)).isoformat()
    ags = _scan_asset_groups(asset_group)
    if not ags:
        return pd.DataFrame()
    with _guarded_build(kind="upcoming-expiries"), ThreadPoolExecutor(max_workers=min(len(ags), _MAX_AG_WORKERS)) as ex:
        frames = list(
            ex.map(
                lambda ag: _read_and_filter_expiries(ag, today_iso=today_iso, horizon_iso=horizon_iso, venue_f=venue_f),
                ags,
            )
        )
    parts = [f for f in frames if f is not None]
    if not parts:
        return pd.DataFrame()
    combined = pd.concat(parts, ignore_index=True)
    return combined.sort_values(_AT_COL, ascending=True, kind="stable")  # soonest first


def _expiries_frame(*, within_days: int, asset_group: str | None, venue: str | None) -> pd.DataFrame:
    """Cached (5-min TTL) prepared frame — shared by ``list_upcoming_expiries``
    (full list) and ``list_upcoming_expiries_page`` (one page)."""
    within_days = max(1, min(within_days, _MAX_EXPIRY_WINDOW_DAYS))
    venue_f = venue.strip() if venue and venue.strip() else None
    key = (within_days, asset_group, venue_f)
    now = time.monotonic()
    cached = _EXPIRIES_FRAME_CACHE.get(key)
    if cached is not None and (now - cached[0]) < _CACHE_TTL_SEC:
        return cached[1]
    frame = _build_expiries_frame(within_days=within_days, asset_group=asset_group, venue_f=venue_f)
    _EXPIRIES_FRAME_CACHE[key] = (now, frame)
    return frame


def list_upcoming_expiries(
    *,
    within_days: int = 7,
    asset_group: str | None = None,
    venue: str | None = None,
) -> list[CatalogueLifecycleRow]:
    """Derivatives (``instrument_type ∈ {FUTURE, OPTION, COMBO}``) whose
    ``available_to`` falls inside ``[today, today+within_days]`` (soonest-first).

    The type filter + forward window are BOTH load-bearing: last-observed
    ``available_to`` values are always ≤ today, so the forward window admits only
    genuine future expiries. (``available_to`` is a 3-way value — expiry /
    None-if-active / last-observed; ``delisted_at`` is a schema field no writer
    populates, see the module docstring.) Read-only, 5-min TTL, shard-isolated per
    asset_group.

    Builds row dicts for EVERY matching row — for the paginated/fast path used
    by the API route, see ``list_upcoming_expiries_page``."""
    frame = _expiries_frame(within_days=within_days, asset_group=asset_group, venue=venue)
    return _rows_from_lifecycle_frame(frame)


def list_upcoming_expiries_page(
    *,
    within_days: int = 7,
    asset_group: str | None = None,
    venue: str | None = None,
    limit: int = DEFAULT_LIFECYCLE_LIMIT,
    offset: int = 0,
) -> tuple[list[CatalogueLifecycleRow], int]:
    """``(page_rows, total_count)`` — the paginated twin of ``list_upcoming_expiries``."""
    safe_limit = max(1, min(int(limit or DEFAULT_LIFECYCLE_LIMIT), MAX_LIFECYCLE_LIMIT))
    safe_offset = max(0, int(offset or 0))
    frame = _expiries_frame(within_days=within_days, asset_group=asset_group, venue=venue)
    total_count = len(frame)
    page = frame.iloc[safe_offset : safe_offset + safe_limit]
    return _rows_from_lifecycle_frame(page), total_count


def _clear_caches() -> None:  # test hook
    _NEW_LISTINGS_FRAME_CACHE.clear()
    _EXPIRIES_FRAME_CACHE.clear()


__all__ = [
    "DEFAULT_LIFECYCLE_LIMIT",
    "MAX_LIFECYCLE_LIMIT",
    "CatalogueLifecycleBuildBusyError",
    "CatalogueLifecycleRow",
    "list_new_listings",
    "list_new_listings_page",
    "list_upcoming_expiries",
    "list_upcoming_expiries_page",
]

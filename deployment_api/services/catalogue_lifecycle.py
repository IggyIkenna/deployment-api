"""New-listings + upcoming-expiries, derived read-only from the per-asset-group
lifecycle catalogue (``prod/catalog.parquet`` in ``instruments-store-{ag}-…``).

Mirrors ``upcoming_fixtures.py`` (5-min in-process TTL cache, TypedDict rows,
shard-isolated per-AG reads — a missing/failed AG parquet is skipped, never a
cross-AG raise). The catalogue is the ONLY identity-level source: one row per
instrument with ``available_from`` (listing date = MIN(first-observed,
venue-declared)) and ``available_to`` (a 4-way value: delisted_at / expiry /
None-if-active / last-observed).

Load-bearing correctness rule for **Upcoming Expiries** (plan
data_status_page_ux_and_canonicalisation_2026_07_16 P2): filter
``instrument_type ∈ {FUTURE, OPTION, COMBO}`` AND ``available_to`` inside the
forward window ``[today, today+within_days]``. Because delistings + last-observed
values are always ≤ today, the forward window admits only genuine future
expiries — the type filter + forward window together make it correct even though
``available_to`` is overloaded.
"""

from __future__ import annotations

import io
import logging
import time
from datetime import UTC, datetime, timedelta
from typing import TypedDict

import pandas as pd

from deployment_api.utils.storage_client import get_storage_client

logger = logging.getLogger(__name__)

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
_CACHE_TTL_SEC: int = 300
_NEW_LISTINGS_CACHE: dict[tuple[int, str | None, str | None], tuple[float, list[CatalogueLifecycleRow]]] = {}
_EXPIRIES_CACHE: dict[tuple[int, str | None, str | None], tuple[float, list[CatalogueLifecycleRow]]] = {}


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


def list_new_listings(
    *,
    max_age_days: int = 30,
    asset_group: str | None = None,
    venue: str | None = None,
) -> list[CatalogueLifecycleRow]:
    """Instruments whose ``available_from`` is within the last ``max_age_days``
    days (newest-first). Read-only, 5-min TTL, shard-isolated per asset_group."""
    max_age_days = max(1, min(max_age_days, _MAX_NEW_LISTING_AGE_DAYS))
    venue_f = venue.strip() if venue and venue.strip() else None
    key = (max_age_days, asset_group, venue_f)
    now = time.monotonic()
    cached = _NEW_LISTINGS_CACHE.get(key)
    if cached is not None and (now - cached[0]) < _CACHE_TTL_SEC:
        return cached[1]

    today = datetime.now(UTC).date()
    cutoff = (today - timedelta(days=max_age_days)).isoformat()
    today_iso = today.isoformat()
    out: list[CatalogueLifecycleRow] = []
    for ag in _scan_asset_groups(asset_group):
        df = _read_catalogue(ag)
        if df is None or "available_from" not in df.columns:
            continue
        df = _venue_filter(df, venue_f)
        af = df["available_from"].astype(str).str.slice(0, 10)
        # available_from within [cutoff, today] — a listing date can't be in the future.
        mask = (af >= cutoff) & (af <= today_iso)
        for rec in df[mask].to_dict(orient="records"):
            out.append(_row(ag, rec))

    out.sort(key=lambda r: r["available_from"], reverse=True)  # newest first
    _NEW_LISTINGS_CACHE[key] = (now, out)
    return out


def list_upcoming_expiries(
    *,
    within_days: int = 7,
    asset_group: str | None = None,
    venue: str | None = None,
) -> list[CatalogueLifecycleRow]:
    """Derivatives (``instrument_type ∈ {FUTURE, OPTION, COMBO}``) whose
    ``available_to`` falls inside ``[today, today+within_days]`` (soonest-first).

    The type filter + forward window are BOTH load-bearing: delistings +
    last-observed ``available_to`` values are always ≤ today, so the forward
    window admits only genuine future expiries. Read-only, 5-min TTL,
    shard-isolated per asset_group."""
    within_days = max(1, min(within_days, _MAX_EXPIRY_WINDOW_DAYS))
    venue_f = venue.strip() if venue and venue.strip() else None
    key = (within_days, asset_group, venue_f)
    now = time.monotonic()
    cached = _EXPIRIES_CACHE.get(key)
    if cached is not None and (now - cached[0]) < _CACHE_TTL_SEC:
        return cached[1]

    today = datetime.now(UTC).date()
    today_iso = today.isoformat()
    horizon_iso = (today + timedelta(days=within_days)).isoformat()
    out: list[CatalogueLifecycleRow] = []
    for ag in _scan_asset_groups(asset_group):
        df = _read_catalogue(ag)
        if df is None or "available_to" not in df.columns or "instrument_type" not in df.columns:
            continue
        df = _venue_filter(df, venue_f)
        itype = df["instrument_type"].astype(str).str.upper()
        at = df["available_to"].astype(str).str.slice(0, 10)
        mask = itype.isin(_EXPIRING_TYPES) & (at >= today_iso) & (at <= horizon_iso)
        for rec in df[mask].to_dict(orient="records"):
            out.append(_row(ag, rec))

    out.sort(key=lambda r: r["available_to"])  # soonest first
    _EXPIRIES_CACHE[key] = (now, out)
    return out


def _clear_caches() -> None:  # test hook
    _NEW_LISTINGS_CACHE.clear()
    _EXPIRIES_CACHE.clear()


__all__ = [
    "CatalogueLifecycleRow",
    "list_new_listings",
    "list_upcoming_expiries",
]

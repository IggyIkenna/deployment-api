"""Longer-lived availability-index cache for the read-only Data Status UI.

``unified_trading_library.read_availability_index`` caches the consolidated
index in-process for only **60 seconds** (so backfill VMs pick up their own
fresh writes quickly). For the Data Status UI that TTL is too short: the
drilldown cascade fires several index reads per interaction, and with the API
running multiple uvicorn workers (each its own UTL cache) a cold ~7s read of
the 172 MB index recurs roughly every minute and once per worker — janky every
time the operator pauses or switches a column.

deployment-api is a READ-ONLY consumer of the index, so it can safely hold the
DataFrame longer. This module wraps ``read_availability_index`` with a
process-local cache at a UI-appropriate TTL (default 5 min, matching the turbo
rollup cache). Freshness on demand comes from the "Clear cache" control, which
calls :func:`clear_index_cache` via ``/turbo/clear``.

Tradeoff: drilldown detail can be up to ``_TTL`` seconds stale relative to the
once-a-minute consolidator. That is acceptable for an operator surface (the
overview/turbo path already caches 5 min) and is overridable with Clear cache.
"""

from __future__ import annotations

import threading
import time
from typing import TYPE_CHECKING

from unified_trading_library import read_availability_index

if TYPE_CHECKING:
    import pandas as pd

__all__ = ["clear_index_cache", "index_cache_stats", "read_index_cached"]

# 5 minutes — matches the turbo rollup + ref-data caches so the whole Data
# Status surface has one coherent freshness window.
_TTL_SECONDS = 300.0

_lock = threading.Lock()
_cache: dict[str, tuple[float, pd.DataFrame]] = {}


def read_index_cached(bucket: str) -> pd.DataFrame:
    """Read the availability index for ``bucket``, cached for ``_TTL_SECONDS``.

    Returns the same DataFrame object on a hit — callers MUST NOT mutate it in
    place (the Data Status services only filter/copy, never mutate).
    """
    now = time.monotonic()
    cached = _cache.get(bucket)
    if cached is not None and (now - cached[0]) < _TTL_SECONDS:
        return cached[1]
    df = read_availability_index(bucket)
    with _lock:
        _cache[bucket] = (now, df)
    return df


def clear_index_cache() -> None:
    """Flush every cached index (wired into ``POST /data-status/turbo/clear``)."""
    with _lock:
        _cache.clear()


def index_cache_stats() -> dict[str, object]:
    """Lightweight introspection for the cache-stats endpoint / debugging."""
    now = time.monotonic()
    return {
        "ttl_seconds": _TTL_SECONDS,
        "buckets": sorted(_cache.keys()),
        "ages_seconds": {b: round(now - ts, 1) for b, (ts, _df) in _cache.items()},
    }

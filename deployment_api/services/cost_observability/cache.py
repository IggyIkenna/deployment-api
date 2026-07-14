"""Tiny thread-safe TTL cache for normalized cost windows.

Billing data is ~daily-lagged (GCP reconciles ~2 days, AWS refreshes ≤3x/day), so re-querying
BigQuery/Athena — or even re-reading the local snapshot parquet — on every page load buys nothing
but latency. One process-local cache keyed by the query window is plenty. The value is generic:
the service caches a compact Arrow window table (Increment 2), so the cache is ~15 MB/window
instead of the ~300 MB a full ``CostRecord`` window used to cost.
"""

from __future__ import annotations

import threading
from collections.abc import Callable
from datetime import UTC, datetime

_DEFAULT_TTL_SECONDS = 3600.0
# Hard cap on distinct cached windows. The UI touches at most ~6 (days in {7,30,90} x current+
# prior), so a small cap keeps every live window warm while preventing unbounded growth. An
# uncapped store of fat CostRecord windows was the +1.9 GB residency measured 2026-07-13 (now the
# value is a compact Arrow table, but the cap still bounds it). Oldest-inserted entry is evicted on
# overflow (FIFO — windows are equally cheap to reload from the local parquet).
_DEFAULT_MAX_ENTRIES = 8


class CostWindowCache[T]:
    """Generic FIFO-bounded TTL cache keyed by a window string → any value ``T``."""

    def __init__(self, ttl_seconds: float = _DEFAULT_TTL_SECONDS, max_entries: int = _DEFAULT_MAX_ENTRIES) -> None:
        self._ttl = ttl_seconds
        self._max_entries = max(1, max_entries)
        self._lock = threading.Lock()
        self._store: dict[str, tuple[float, T]] = {}

    def get_or_load(self, key: str, loader: Callable[[], T], *, force: bool = False) -> T:
        now = datetime.now(UTC).timestamp()
        if not force:
            with self._lock:
                hit = self._store.get(key)
                if hit is not None and (now - hit[0]) < self._ttl:
                    return hit[1]
        # Load outside the lock — a slow BigQuery/Athena call must not block other keys.
        value = loader()
        with self._lock:
            # Refresh insertion order on re-store so a re-loaded key isn't the eviction victim.
            self._store.pop(key, None)
            self._store[key] = (now, value)
            while len(self._store) > self._max_entries:
                oldest = next(iter(self._store))  # dicts preserve insertion order → FIFO evict
                del self._store[oldest]
        return value

    def clear(self) -> None:
        with self._lock:
            self._store.clear()

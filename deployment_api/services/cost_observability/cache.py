"""Tiny thread-safe TTL cache for normalized cost windows.

Billing data is ~daily-lagged (GCP reconciles ~2 days, AWS refreshes ≤3x/day), so
re-querying BigQuery/Athena on every page load buys nothing but latency and (on AWS,
which has no free tier) cost. One process-local cache keyed by the query window is
plenty — the payload is a few thousand small records. A future move to the app's Redis
cache is a drop-in behind this same interface.
"""

from __future__ import annotations

import threading
from collections.abc import Callable
from datetime import UTC, datetime

from deployment_api.services.cost_observability.models import CostRecord

_DEFAULT_TTL_SECONDS = 3600.0
# Hard cap on distinct cached windows. The UI touches at most ~6 (days in {7,30,90} x current+
# prior), so a small cap keeps every live window warm while preventing unbounded growth. Each
# entry can be a full ~168 K-record window (~300 MB), so an uncapped store was the +1.9 GB
# residency measured 2026-07-13. Oldest-inserted entry is evicted on overflow (FIFO — windows are
# equally cheap to reload from the local parquet). Env-overridable for tests.
_DEFAULT_MAX_ENTRIES = 8


class CostWindowCache:
    def __init__(self, ttl_seconds: float = _DEFAULT_TTL_SECONDS, max_entries: int = _DEFAULT_MAX_ENTRIES) -> None:
        self._ttl = ttl_seconds
        self._max_entries = max(1, max_entries)
        self._lock = threading.Lock()
        self._store: dict[str, tuple[float, list[CostRecord]]] = {}

    def get_or_load(self, key: str, loader: Callable[[], list[CostRecord]], *, force: bool = False) -> list[CostRecord]:
        now = datetime.now(UTC).timestamp()
        if not force:
            with self._lock:
                hit = self._store.get(key)
                if hit is not None and (now - hit[0]) < self._ttl:
                    return hit[1]
        # Load outside the lock — a slow BigQuery/Athena call must not block other keys.
        records = loader()
        with self._lock:
            # Refresh insertion order on re-store so a re-loaded key isn't the eviction victim.
            self._store.pop(key, None)
            self._store[key] = (now, records)
            while len(self._store) > self._max_entries:
                oldest = next(iter(self._store))  # dicts preserve insertion order → FIFO evict
                del self._store[oldest]
        return records

    def clear(self) -> None:
        with self._lock:
            self._store.clear()

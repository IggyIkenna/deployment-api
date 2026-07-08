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


class CostWindowCache:
    def __init__(self, ttl_seconds: float = _DEFAULT_TTL_SECONDS) -> None:
        self._ttl = ttl_seconds
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
            self._store[key] = (now, records)
        return records

    def clear(self) -> None:
        with self._lock:
            self._store.clear()

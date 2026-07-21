"""Tiny thread-safe TTL cache for artifact-pipeline reads.

The cloud metadata this service reads (Cloud Build history, registry inventory, Cloud Run revisions,
App Runner/ECS ops) is slow to enumerate but changes on the order of minutes, so re-scanning on every
page load buys nothing but latency and OOM risk. One process-local cache keyed by the query
window/view is plenty (mirrors `cost_observability.cache.CostWindowCache`). Values are compact —
normalized fact lists or already-built response models, not raw cloud payloads — so a handful of live
windows stays well inside the deployment-api memory budget (see the OOM remediation constraints in the
plan). Oldest-inserted entry is evicted on overflow (FIFO — each window is cheap to reload).
"""

from __future__ import annotations

import threading
from collections.abc import Callable
from datetime import UTC, datetime

_DEFAULT_TTL_SECONDS = 300.0  # 5 min — build/registry state moves slower than a page reload
_DEFAULT_MAX_ENTRIES = 12  # the UI touches ~5 views x a couple of windows; a small cap bounds residency


class ArtifactWindowCache[T]:
    """Generic FIFO-bounded TTL cache keyed by a view/window string → any value ``T``."""

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
        # Load outside the lock — a slow multi-cloud scan must not block other keys.
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

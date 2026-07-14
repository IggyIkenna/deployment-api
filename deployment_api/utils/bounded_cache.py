"""Generalized bounded, TTL-evicting, size-reporting in-process cache primitive.

Generalizes the max-entries + evict-oldest-on-every-set + TTL pattern already proven
correct in ``routes/deployment_caching.py`` (the ``_logs_cache`` / ``_vm_logs_cache`` /
``_verification_cache`` shape) using ``cachetools.TTLCache`` instead of hand-rolling the
same "prune the oldest key, stamp an expiry, overwrite" logic per call site. On top of
that eviction primitive this module adds:

- A process-wide NAMED registry so every cache instance's stats are enumerable in one
  place (``all_cache_stats()`` — feeds ``GET /api/debug/cache-stats``).
- A single periodic sweeper task (``start_sweeper`` / ``stop_sweeper``, wired into
  ``deployment_api.lifespan``) that proactively expires stale entries across every
  instance — ``TTLCache`` only expires a key lazily on the NEXT touch of that exact key,
  so a cache key that goes cold (nobody re-requests it) would otherwise sit on expired
  memory indefinitely between touches.
- A best-effort ``estimated_bytes`` via ``sys.getsizeof`` (shallow — no container
  recursion — which is fine for a memory-utilization ALERT signal, not a precise
  accounting).

Each named :class:`BoundedCache` is itself a ``MutableMapping[str, object]`` — the same
``cache[key] = value`` / ``cache.get(key)`` / ``cache.pop(key, None)`` / ``key in cache`` /
``len(cache)`` / ``cache.clear()`` surface a plain ``dict`` already offered — so migrating
an existing dict-shaped module-level cache is a rename plus a lock, not a rewrite. Eviction
(LRU-oldest-on-overflow + lazy TTL expiry) is delegated entirely to ``cachetools.TTLCache``;
this class adds thread-safety (``TTLCache`` is not safe under concurrent mutation from
multiple request-handling threads) plus the name + registry + stats reporting.
"""

from __future__ import annotations

import asyncio
import contextlib
import logging
import sys
import threading
import time
from collections.abc import Iterator, MutableMapping
from typing import ClassVar

from cachetools import TTLCache

logger = logging.getLogger(__name__)

_DEFAULT_SWEEP_INTERVAL_SECONDS = 60.0


def _monotonic() -> float:
    """Indirection so tests can monkeypatch ``time.monotonic`` and have it take effect.

    ``cachetools.TTLCache``'s own default ``timer=time.monotonic`` argument captures the
    function object once at class-definition time, which a later
    ``monkeypatch.setattr(time, "monotonic", fake)`` can no longer reach. Passing this
    closure instead performs a fresh ``time.monotonic()`` module-attribute lookup on every
    call — the same live-lookup behavior every hand-rolled cache this primitive replaces
    already had (they all called ``time.monotonic()`` directly).
    """
    return time.monotonic()


class BoundedCache(MutableMapping[str, object]):
    """Thread-safe max-entries + TTL cache, self-registered by name for stats reporting."""

    _registry: ClassVar[dict[str, BoundedCache]] = {}
    _registry_lock = threading.Lock()

    def __init__(self, name: str, maxsize: int, ttl_seconds: float) -> None:
        self.name = name
        self.maxsize = maxsize
        self.ttl_seconds = ttl_seconds
        self._store: TTLCache[str, object] = TTLCache(maxsize=maxsize, ttl=ttl_seconds, timer=_monotonic)
        self._lock = threading.Lock()
        with self._registry_lock:
            if name in self._registry:
                logger.warning(
                    "BoundedCache %r registered a second time (module reload?) — replacing the prior instance",
                    name,
                )
            self._registry[name] = self

    # -- MutableMapping protocol (locked; everything else — get/pop/clear/__contains__ —
    #    is provided by the MutableMapping mixins on top of these five, each individually
    #    locked call) ---------------------------------------------------------------

    def __getitem__(self, key: str) -> object:
        with self._lock:
            return self._store[key]

    def __setitem__(self, key: str, value: object) -> None:
        with self._lock:
            self._store[key] = value

    def __delitem__(self, key: str) -> None:
        with self._lock:
            del self._store[key]

    def __iter__(self) -> Iterator[str]:
        with self._lock:
            keys = list(self._store.keys())
        return iter(keys)

    def __len__(self) -> int:
        with self._lock:
            return len(self._store)

    def clear(self) -> None:
        with self._lock:
            self._store.clear()

    # -- Reporting ----------------------------------------------------------------

    def expire(self) -> int:
        """Proactively purge expired entries. Returns the count removed."""
        with self._lock:
            return len(self._store.expire())

    def stats(self) -> dict[str, object]:
        """Current entry count + a cheap, shallow byte estimate — feeds the debug endpoint."""
        with self._lock:
            self._store.expire()
            items = list(self._store.items())
        estimated_bytes = sum(_estimate_bytes(k) + _estimate_bytes(v) for k, v in items)
        return {
            "name": self.name,
            "entries": len(items),
            "max_entries": self.maxsize,
            "ttl_seconds": self.ttl_seconds,
            "estimated_bytes": estimated_bytes,
        }

    @classmethod
    def registry(cls) -> list[BoundedCache]:
        with cls._registry_lock:
            return list(cls._registry.values())


def _estimate_bytes(value: object) -> int:
    """Cheap, best-effort, SHALLOW size estimate (no container recursion) — sufficient for
    a memory-utilization alert signal, not a precise accounting."""
    try:
        return sys.getsizeof(value)
    except TypeError:
        return 0


def all_cache_stats() -> list[dict[str, object]]:
    """Stats for every registered :class:`BoundedCache`, sorted by name."""
    return sorted((c.stats() for c in BoundedCache.registry()), key=lambda s: str(s["name"]))


_sweeper_task: asyncio.Task[None] | None = None
_sweeper_shutdown_event: asyncio.Event | None = None


async def _sweep_loop(interval_seconds: float) -> None:
    assert _sweeper_shutdown_event is not None
    while not _sweeper_shutdown_event.is_set():
        try:
            await asyncio.wait_for(_sweeper_shutdown_event.wait(), timeout=interval_seconds)
        except TimeoutError:
            for instance in BoundedCache.registry():
                try:
                    evicted = instance.expire()
                    if evicted:
                        logger.debug("bounded_cache: swept %s expired entries from %s", evicted, instance.name)
                except (RuntimeError, ValueError) as exc:
                    logger.warning("bounded_cache: sweep failed for %s: %s", instance.name, exc)


def start_sweeper(interval_seconds: float = _DEFAULT_SWEEP_INTERVAL_SECONDS) -> None:
    """Start the single periodic sweeper task (idempotent — a second call is a no-op)."""
    global _sweeper_task, _sweeper_shutdown_event
    if _sweeper_task is not None and not _sweeper_task.done():
        return
    _sweeper_shutdown_event = asyncio.Event()
    _sweeper_task = asyncio.create_task(_sweep_loop(interval_seconds))


async def stop_sweeper() -> None:
    """Cancel + await the sweeper task (app shutdown)."""
    global _sweeper_task
    if _sweeper_shutdown_event is not None:
        _sweeper_shutdown_event.set()
    task = _sweeper_task
    if task is not None:
        task.cancel()
        with contextlib.suppress(asyncio.CancelledError):
            await task
        _sweeper_task = None

"""
Unified Caching Layer for API

Two-tier caching architecture:
- Tier 1 (Hot): In-Memory (per-worker, always on) - fastest, volatile
- Tier 2 (Distributed): Redis (optional) - fast, volatile, shared across workers

Read path:  In-Memory -> Redis -> Fetch
Write path: Fetch -> In-Memory + Redis

A GCS-backed persistent third tier (``GCSCache``) previously existed here but was
confirmed unused in production (the only object under the configured GCS cache prefix was
a stale 146-byte leftover; the live cache path never wrote there) and carried a real design
flaw — it mirrored the WHOLE cache blob into memory per worker and fire-and-forgot a
full-blob JSON rewrite on every ``set()`` with unbounded executor concurrency. Removed
rather than fixed (2026-07 memory-bounding pass).

Usage:
    from deployment_api.utils.cache import cache

    # Basic operations
    value = await cache.get("my_key")
    await cache.set("my_key", {"data": "value"}, ttl=60)
    await cache.delete("my_key")

    # Get or fetch with automatic caching
    value = await cache.get_or_fetch("key", fetch_func, ttl=60)

Environment Variables:
    REDIS_URL: Redis connection URL (default: redis://localhost:6379/0)
"""

import asyncio
import json
import logging
import time
from collections.abc import Callable, Coroutine
from typing import cast

from deployment_api.settings import REDIS_URL

logger = logging.getLogger(__name__)


# Serialization helpers
# SECURITY: jsonpickle removed — it can execute arbitrary code during decode.
# Cache values must be plain JSON-serializable (dicts, lists, strings, numbers).
def serialize(value: object) -> str:
    """Serialize Python objects into JSON string."""
    return json.dumps(value, default=str)


def deserialize(value: object) -> object:
    """Deserialize JSON string (or bytes) into Python objects."""
    if isinstance(value, (bytes, bytearray)):
        value = value.decode("utf-8")
    if not isinstance(value, str):
        raise TypeError(f"Expected str for JSON deserialization, got {type(value).__name__}")
    return cast(object, json.loads(value))


from redis.exceptions import RedisError
from unified_trading_library import AsyncRedisProvider

REDIS_AVAILABLE = True


class CacheBackend:
    """Abstract cache backend interface."""

    async def get(self, key: str) -> object | None:
        raise NotImplementedError(f"{self.__class__.__name__} does not implement get()")

    async def set(self, key: str, value: object, ttl: int) -> None:
        raise NotImplementedError(f"{self.__class__.__name__} does not implement set()")

    async def delete(self, key: str) -> None:
        raise NotImplementedError(f"{self.__class__.__name__} does not implement delete()")

    async def clear_pattern(self, pattern: str) -> int:
        raise NotImplementedError(f"{self.__class__.__name__} does not implement clear_pattern()")


class InMemoryCache(CacheBackend):
    """In-memory cache with per-entry TTL support (fastest, volatile).

    Bounded by entry count, evicting the earliest-to-expire entry on overflow — the same
    max-entries + evict-oldest-on-set shape as ``BoundedCache``/``routes/deployment_caching.py``,
    but NOT built on ``BoundedCache`` itself: every call site sharing this ONE tier supplies
    its OWN per-call ``ttl`` (``TTL_HEALTH=5`` next to ``TTL_TRIGGER_ID=3600`` in the same
    cache instance — see the TTL_* constants below), and ``cachetools.TTLCache`` only
    supports a single fixed ttl per cache instance, not per-entry. A genuine "primitive
    doesn't fit this shape" case.
    """

    _MAX_ENTRIES = 5000

    def __init__(self):
        self._cache: dict[str, dict[str, object]] = {}
        self._lock = asyncio.Lock()

    async def get(self, key: str) -> object | None:
        async with self._lock:
            if key not in self._cache:
                return None

            entry = self._cache[key]
            expires_at = entry.get("expires_at")
            if isinstance(expires_at, float) and time.time() > expires_at:
                del self._cache[key]
                return None

            return entry.get("value")

    async def set(self, key: str, value: object, ttl: int) -> None:
        async with self._lock:
            if key not in self._cache and len(self._cache) >= self._MAX_ENTRIES:
                self._evict_earliest_expiry_locked()
            self._cache[key] = {
                "value": value,
                "expires_at": time.time() + ttl,
            }

    def _evict_earliest_expiry_locked(self) -> None:
        """Drop the entry closest to expiry. Caller MUST already hold ``self._lock``."""
        if not self._cache:
            return
        oldest_key = min(self._cache, key=lambda k: cast(float, self._cache[k].get("expires_at") or 0.0))
        del self._cache[oldest_key]

    async def delete(self, key: str) -> None:
        async with self._lock:
            self._cache.pop(key, None)

    async def clear_pattern(self, pattern: str) -> int:
        """Clear all keys matching pattern (simple prefix matching)."""
        async with self._lock:
            prefix = pattern.replace("*", "")
            keys_to_delete = [k for k in self._cache if k.startswith(prefix)]
            for k in keys_to_delete:
                del self._cache[k]
            return len(keys_to_delete)

    async def cleanup_expired(self):
        """Remove expired entries (called periodically)."""
        async with self._lock:
            now = time.time()

            def _is_expired(v: dict[str, object]) -> bool:
                exp = v.get("expires_at")
                return isinstance(exp, float) and now > exp

            expired = [k for k, v in self._cache.items() if _is_expired(v)]
            for k in expired:
                del self._cache[k]
            if expired:
                logger.debug("Cleaned up %s expired cache entries", len(expired))


class RedisCache(CacheBackend):
    """Redis-backed cache with JSON serialization (distributed, volatile)."""

    def __init__(self, redis_url: str):
        self.redis_url = redis_url
        self._provider: AsyncRedisProvider | None = None

    async def connect(self):
        """Connect to Redis."""
        try:
            provider = AsyncRedisProvider(url=self.redis_url)
            _get_client_attr: object = getattr(provider, "_get_client", None)
            if callable(_get_client_attr):
                get_client_fn = cast(
                    Callable[[], Coroutine[object, object, object]],
                    _get_client_attr,
                )
                await get_client_fn()
            self._provider = provider
            logger.info("Connected to Redis at %s", self.redis_url)
        except (OSError, ValueError, RuntimeError, RedisError) as e:
            # RedisError (redis-py) is NOT an OSError subclass — without it a dropped/unreachable
            # Redis connection propagates out of get_or_fetch and 500s the endpoint (e.g. the
            # deployment-ui /api/cloud-builds/triggers pane). The cache is best-effort: degrade.
            logger.warning("Failed to connect to Redis: %s", e)
            self._provider = None

    async def close(self):
        """Close Redis connection."""
        if not self._provider:
            return
        try:
            await self._provider.close()
        except (OSError, ValueError, RuntimeError) as e:
            logger.warning("Failed to close Redis connection: %s", e)

    async def get(self, key: str) -> object | None:
        if not self._provider:
            return None

        try:
            value = await self._provider.get(key)
            if value is None:
                return None
            return deserialize(value)
        except (OSError, ValueError, RuntimeError, RedisError) as e:
            logger.error("Redis GET error for %s: %s", key, e)
            return None

    async def set(self, key: str, value: object, ttl: int) -> None:
        if not self._provider:
            return

        try:
            await self._provider.set(key, serialize(value).encode("utf-8"), ttl_seconds=ttl)
        except (OSError, ValueError, RuntimeError, RedisError) as e:
            logger.error("Redis SET error for %s: %s", key, e)

    async def delete(self, key: str) -> None:
        if not self._provider:
            return

        try:
            await self._provider.delete(key)
        except (OSError, ValueError, RuntimeError, RedisError) as e:
            logger.error("Redis DELETE error for %s: %s", key, e)

    async def clear_pattern(self, pattern: str) -> int:
        """Clear all keys matching pattern."""
        if not self._provider:
            return 0

        try:
            _get_client_attr: object = getattr(self._provider, "_get_client", None)
            if not callable(_get_client_attr):
                return 0
            get_client_fn = cast(
                Callable[[], Coroutine[object, object, object]],
                _get_client_attr,
            )
            client: object = await get_client_fn()
            _keys_attr: object = getattr(client, "keys", None)
            _del_attr: object = getattr(client, "delete", None)
            if not callable(_keys_attr) or not callable(_del_attr):
                return 0
            keys_fn = cast(
                Callable[[str], Coroutine[object, object, object]],
                _keys_attr,
            )
            del_fn = cast(
                Callable[..., Coroutine[object, object, object]],
                _del_attr,
            )
            keys_raw: object = await keys_fn(pattern)
            keys: list[object] = cast(list[object], keys_raw) if isinstance(keys_raw, list) else []
            if keys:
                await del_fn(*keys)
            return len(keys)
        except (OSError, ValueError, RuntimeError, RedisError) as e:
            logger.error("Redis CLEAR_PATTERN error for %s: %s", pattern, e)
            return 0


class UnifiedCache:
    """
    Unified caching layer with two tiers:

    1. In-Memory (fastest, per-instance)
    2. Redis (fast, distributed) - optional

    Read order: In-Memory -> Redis -> Fetch
    Write order: All tiers simultaneously
    """

    def __init__(self, redis_url: str | None = None):
        redis_url = redis_url or REDIS_URL

        self.in_memory = InMemoryCache()
        self.redis = RedisCache(redis_url) if REDIS_AVAILABLE else None
        self._initialized = False
        self._cleanup_task: asyncio.Task[None] | None = None
        self._shutdown_event = asyncio.Event()

    async def initialize(self):
        """Initialize all cache backends."""
        if self._initialized:
            return

        if self.redis:
            await self.redis.connect()

        # Start background cleanup task
        self._cleanup_task = asyncio.create_task(self._cleanup_loop())

        self._initialized = True
        logger.info(
            "Unified cache initialized (Redis: %s)",
            "enabled" if self.redis and getattr(self.redis, "_provider", None) else "disabled",
        )

    async def _cleanup_loop(self):
        """Background task to clean up expired in-memory entries."""
        while not self._shutdown_event.is_set():
            try:
                await asyncio.wait_for(self._shutdown_event.wait(), timeout=60)
            except TimeoutError:
                await self.in_memory.cleanup_expired()

    async def shutdown(self):
        """Shutdown cache background tasks and connections."""
        if not self._initialized:
            return
        self._shutdown_event.set()
        if self._cleanup_task:
            self._cleanup_task.cancel()
            try:
                await self._cleanup_task
            except asyncio.CancelledError as e:
                logger.debug("Suppressed %s during operation: %s", type(e).__name__, e)
                pass
        if self.redis:
            await self.redis.close()
        self._initialized = False

    async def get(self, key: str) -> object | None:
        """
        Get value from cache (checks all tiers).

        Order: In-Memory -> Redis
        If found in a lower tier, promotes to the in-memory tier.
        """
        # Tier 1: In-Memory (fastest)
        value = await self.in_memory.get(key)
        if value is not None:
            return value

        # Tier 2: Redis (distributed)
        if self.redis:
            value = await self.redis.get(key)
            if value is not None:
                # Promote to in-memory
                await self.in_memory.set(key, value, ttl=60)
                return value

        return None

    async def set(self, key: str, value: object, ttl: int) -> None:
        """
        Set value in cache (all applicable tiers).

        Args:
            key: Cache key
            value: Value to cache
            ttl: Time to live in seconds
        """
        # Always set in in-memory
        await self.in_memory.set(key, value, ttl)

        # Set in Redis if available
        if self.redis:
            await self.redis.set(key, value, ttl)

    async def delete(self, key: str) -> None:
        """Delete key from all cache tiers."""
        await self.in_memory.delete(key)
        if self.redis:
            await self.redis.delete(key)

    async def clear_pattern(self, pattern: str) -> int:
        """Clear all keys matching pattern from all tiers."""
        count = await self.in_memory.clear_pattern(pattern)
        if self.redis:
            count += await self.redis.clear_pattern(pattern)
        return count

    async def get_or_fetch(
        self,
        key: str,
        fetch_func: Callable[[], Coroutine[object, object, object]],
        ttl: int,
        force_refresh: bool = False,
    ) -> object:
        """
        Get from cache or fetch if missing.

        Args:
            key: Cache key
            fetch_func: Async function to call if cache miss
            ttl: Time to live in seconds
            force_refresh: Skip cache and fetch fresh data

        Returns:
            Cached or freshly fetched value

        Note:
            None values are NOT cached to avoid infinite re-fetch loops.
        """
        if not force_refresh:
            cached = await self.get(key)
            if cached is not None:
                return cached

        # Fetch fresh data
        value: object = await fetch_func()

        # Only cache non-None values
        if value is not None:
            await self.set(key, value, ttl)

        return value


# =============================================================================
# Singleton instance and backwards-compatible aliases
# =============================================================================

# Primary unified cache instance
cache = UnifiedCache()

# Backwards compatibility: SmartCache alias
SmartCache = UnifiedCache


# =============================================================================
# Cache key builders
# =============================================================================


def deployment_key(deployment_id: str) -> str:
    """Cache key for deployment state."""
    return f"deployment:{deployment_id}"


def deployment_list_key(service: str | None = None, limit: int = 20) -> str:
    """Cache key for deployment list."""
    return f"deployments:{service or 'all'}:limit-{limit}"


def data_status_key(service: str, start_date: str, end_date: str, asset_groups: str = "") -> str:
    """Cache key for data status."""
    return f"data-status:{service}:{start_date}:{end_date}:{asset_groups}"


def service_status_key(service: str) -> str:
    """Cache key for service status (data timestamps, deployments, builds)."""
    return f"service-status:{service}"


def build_info_key(service: str) -> str:
    """Cache key for Cloud Build info."""
    return f"build:{service}"


def trigger_id_key(service: str) -> str:
    """Cache key for Cloud Build trigger ID."""
    return f"trigger:{service}"


# =============================================================================
# Cache invalidation helpers
# =============================================================================


# Background tasks set to prevent GC of fire-and-forget tasks
_background_tasks: set[asyncio.Task[None]] = set()


def invalidate_deployment_cache_async(deployment_id: str | None = None):
    """Invalidate deployment-related caches."""

    async def _invalidate():
        if deployment_id:
            await cache.delete(deployment_key(deployment_id))
        await cache.clear_pattern("deployments:*")

    task = asyncio.create_task(_invalidate())
    _background_tasks.add(task)
    task.add_done_callback(_background_tasks.discard)


def invalidate_service_status_cache_async(service: str | None = None):
    """Invalidate service status caches."""

    async def _invalidate():
        if service:
            await cache.delete(service_status_key(service))
            await cache.delete(build_info_key(service))
        else:
            await cache.clear_pattern("service-status:*")
            await cache.clear_pattern("build:*")

    task = asyncio.create_task(_invalidate())
    _background_tasks.add(task)
    task.add_done_callback(_background_tasks.discard)


# =============================================================================
# Cache TTL recommendations by endpoint
# =============================================================================

TTL_HEALTH = 5  # Health checks - very short
TTL_DEPLOYMENT_STATE = 10  # Individual deployment state (active deployments - short for near-real-time)
TTL_DEPLOYMENT_STATE_TERMINAL = 60  # Terminal deployment state (completed/failed/cancelled - longer)
TTL_DEPLOYMENT_LIST = 30  # Deployment list (30s - keep list fresh for running/pending status)
TTL_DATA_STATUS = 1800  # Data status checks (30 min - expensive queries, data rarely changes)
TTL_SERVICE_STATUS = 120  # Service status (2 min)
TTL_BUILD_INFO = 300  # Cloud Build info (5 min, slow API)
TTL_TRIGGER_ID = 3600  # Trigger IDs (1 hour, rarely changes)
TTL_SERVICE_CONFIG = 600  # Service configs (10 min)
TTL_LOGS = 10  # Logs - short (real-time important)

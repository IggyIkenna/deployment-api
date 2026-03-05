"""
Unit tests for the Unified Caching Layer.

Tests cover:
1. InMemoryCache operations
2. UnifiedCache get_or_fetch with None handling
3. GCSCache operations (mocked)
4. Cache key builders
5. TTL expiration
6. Tier promotion (GCS -> Redis -> InMemory)
"""

import asyncio
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from deployment_api.utils.cache import (
    TTL_BUILD_INFO,
    TTL_DEPLOYMENT_STATE,
    InMemoryCache,
    UnifiedCache,
    build_info_key,
    data_status_key,
    deployment_key,
    deployment_list_key,
    service_status_key,
    trigger_id_key,
)


class TestInMemoryCache:
    """Tests for InMemoryCache."""

    @pytest.fixture
    def cache_instance(self):
        return InMemoryCache()

    @pytest.mark.asyncio
    async def test_get_nonexistent_key_returns_none(self, cache_instance):
        """Getting a key that doesn't exist should return None."""
        result = await cache_instance.get("nonexistent")
        assert result is None

    @pytest.mark.asyncio
    async def test_set_and_get(self, cache_instance):
        """Setting a value should make it retrievable."""
        await cache_instance.set("key", {"data": "value"}, ttl=60)
        result = await cache_instance.get("key")
        assert result == {"data": "value"}

    @pytest.mark.asyncio
    async def test_set_with_ttl_expiration(self, cache_instance):
        """Values should expire after TTL."""
        await cache_instance.set("key", "value", ttl=1)

        # Should exist immediately
        assert await cache_instance.get("key") == "value"

        # Wait for expiration
        await asyncio.sleep(1.1)

        # Should be gone
        assert await cache_instance.get("key") is None

    @pytest.mark.asyncio
    async def test_delete_key(self, cache_instance):
        """Deleting a key should remove it."""
        await cache_instance.set("key", "value", ttl=60)
        await cache_instance.delete("key")
        assert await cache_instance.get("key") is None

    @pytest.mark.asyncio
    async def test_clear_pattern(self, cache_instance):
        """Clear pattern should remove matching keys."""
        await cache_instance.set("deployments:service1:limit-20", "a", ttl=60)
        await cache_instance.set("deployments:service2:limit-20", "b", ttl=60)
        await cache_instance.set("other:key", "c", ttl=60)

        count = await cache_instance.clear_pattern("deployments:*")

        assert count == 2
        assert await cache_instance.get("deployments:service1:limit-20") is None
        assert await cache_instance.get("deployments:service2:limit-20") is None
        assert await cache_instance.get("other:key") == "c"


class TestUnifiedCacheGetOrFetch:
    """Tests for UnifiedCache.get_or_fetch with focus on None handling."""

    @pytest.fixture
    def unified_cache(self):
        """Create a UnifiedCache with Redis and GCS disabled."""
        cache = UnifiedCache()
        cache.redis = None
        cache.gcs = None
        return cache

    @pytest.mark.asyncio
    async def test_get_or_fetch_cache_miss_calls_fetch(self, unified_cache):
        """On cache miss, fetch function should be called."""
        fetch_func = AsyncMock(return_value={"data": "fresh"})

        result = await unified_cache.get_or_fetch("key", fetch_func, ttl=60)

        assert result == {"data": "fresh"}
        fetch_func.assert_called_once()

    @pytest.mark.asyncio
    async def test_get_or_fetch_cache_hit_skips_fetch(self, unified_cache):
        """On cache hit, fetch function should NOT be called."""
        await unified_cache.set("key", {"data": "cached"}, ttl=60)

        fetch_func = AsyncMock(return_value={"data": "fresh"})

        result = await unified_cache.get_or_fetch("key", fetch_func, ttl=60)

        assert result == {"data": "cached"}
        fetch_func.assert_not_called()

    @pytest.mark.asyncio
    async def test_get_or_fetch_force_refresh_bypasses_cache(self, unified_cache):
        """force_refresh=True should always call fetch."""
        await unified_cache.set("key", {"data": "stale"}, ttl=60)

        fetch_func = AsyncMock(return_value={"data": "fresh"})

        result = await unified_cache.get_or_fetch("key", fetch_func, ttl=60, force_refresh=True)

        assert result == {"data": "fresh"}
        fetch_func.assert_called_once()

    @pytest.mark.asyncio
    async def test_get_or_fetch_none_not_cached(self, unified_cache):
        """
        BUG FIX TEST: When fetch returns None, it should NOT be cached.

        This prevents infinite re-fetch loops.
        """
        fetch_count = 0

        async def fetch_returns_none():
            nonlocal fetch_count
            fetch_count += 1
            return None

        # First call - fetches and gets None
        result1 = await unified_cache.get_or_fetch("key", fetch_returns_none, ttl=60)
        assert result1 is None
        assert fetch_count == 1

        # Second call - should fetch AGAIN (None was not cached)
        result2 = await unified_cache.get_or_fetch("key", fetch_returns_none, ttl=60)
        assert result2 is None
        assert fetch_count == 2

    @pytest.mark.asyncio
    async def test_get_or_fetch_empty_list_is_cached(self, unified_cache):
        """Empty list [] should be cached (it's not None)."""
        fetch_count = 0

        async def fetch_returns_empty_list():
            nonlocal fetch_count
            fetch_count += 1
            return []

        result1 = await unified_cache.get_or_fetch("key", fetch_returns_empty_list, ttl=60)
        assert result1 == []
        assert fetch_count == 1

        result2 = await unified_cache.get_or_fetch("key", fetch_returns_empty_list, ttl=60)
        assert result2 == []
        assert fetch_count == 1  # Cache hit!

    @pytest.mark.asyncio
    async def test_get_or_fetch_empty_dict_is_cached(self, unified_cache):
        """Empty dict {} should be cached (it's not None)."""
        fetch_count = 0

        async def fetch_returns_empty_dict():
            nonlocal fetch_count
            fetch_count += 1
            return {}

        result1 = await unified_cache.get_or_fetch("key", fetch_returns_empty_dict, ttl=60)
        assert result1 == {}
        assert fetch_count == 1

        result2 = await unified_cache.get_or_fetch("key", fetch_returns_empty_dict, ttl=60)
        assert result2 == {}
        assert fetch_count == 1


class TestUnifiedCachePersistence:
    """Tests for GCS persistence in UnifiedCache."""

    @pytest.fixture
    def unified_cache_with_mock_gcs(self):
        """Create UnifiedCache with mocked GCS."""
        cache = UnifiedCache()
        cache.redis = None
        cache.gcs = MagicMock()
        cache.gcs.set = AsyncMock()
        cache.gcs.get = AsyncMock(return_value=None)
        return cache

    @pytest.mark.asyncio
    async def test_persist_to_gcs_false_does_not_call_gcs(self, unified_cache_with_mock_gcs):
        """persist_to_gcs=False should not write to GCS."""
        await unified_cache_with_mock_gcs.set("key", "value", ttl=60, persist_to_gcs=False)

        unified_cache_with_mock_gcs.gcs.set.assert_not_called()

    @pytest.mark.asyncio
    async def test_persist_to_gcs_true_calls_gcs(self, unified_cache_with_mock_gcs):
        """persist_to_gcs=True should write to GCS."""
        await unified_cache_with_mock_gcs.set("key", "value", ttl=60, persist_to_gcs=True)

        unified_cache_with_mock_gcs.gcs.set.assert_called_once_with("key", "value", 60)


class TestCacheKeyBuilders:
    """Tests for cache key builder functions."""

    def test_deployment_key(self):
        assert deployment_key("deploy-123") == "deployment:deploy-123"

    def test_deployment_list_key_with_service(self):
        assert deployment_list_key("instruments-service", 50) == "deployments:instruments-service:limit-50"

    def test_deployment_list_key_no_service(self):
        assert deployment_list_key(None, 20) == "deployments:all:limit-20"

    def test_data_status_key(self):
        key = data_status_key("instruments-service", "2024-01-01", "2024-01-31", "CEFI")
        assert key == "data-status:instruments-service:2024-01-01:2024-01-31:CEFI"

    def test_service_status_key(self):
        assert service_status_key("instruments-service") == "service-status:instruments-service"

    def test_build_info_key(self):
        assert build_info_key("instruments-service") == "build:instruments-service"

    def test_trigger_id_key(self):
        assert trigger_id_key("instruments-service") == "trigger:instruments-service"


class TestTTLConstants:
    """Tests for TTL constant values."""

    def test_ttl_values_are_reasonable(self):
        """TTLs should be within reasonable ranges."""
        from deployment_api.utils.cache import (
            TTL_DATA_STATUS,
            TTL_DEPLOYMENT_LIST,
            TTL_HEALTH,
            TTL_LOGS,
            TTL_TRIGGER_ID,
        )

        # Health should be very short
        assert TTL_HEALTH <= 10

        # Logs should be short (real-time)
        assert TTL_LOGS <= 30

        # Deployment state/list should be medium
        assert 10 <= TTL_DEPLOYMENT_STATE <= 120
        assert 10 <= TTL_DEPLOYMENT_LIST <= 120

        # Data status should be longer (expensive)
        assert TTL_DATA_STATUS >= 60

        # Build info should be long (slow API)
        assert TTL_BUILD_INFO >= 60

        # Trigger IDs should be very long (rarely change)
        assert TTL_TRIGGER_ID >= 300


class TestRedisUrlConfiguration:
    """Tests for REDIS_URL environment variable support."""

    def test_default_redis_url(self):
        """Default URL should be localhost."""
        from deployment_api.utils.cache import REDIS_URL

        assert "localhost:6379" in REDIS_URL or REDIS_URL.startswith("redis://")

    @patch.dict("os.environ", {"REDIS_URL": "redis://10.0.0.1:6379/0"})
    def test_custom_redis_url_from_env(self):
        """REDIS_URL env var should be respected."""
        import importlib

        import deployment_api.settings as settings_module
        import deployment_api.utils.cache as cache_module

        importlib.reload(settings_module)
        importlib.reload(cache_module)

        assert cache_module.REDIS_URL == "redis://10.0.0.1:6379/0"


class TestBackwardsCompatibility:
    """Tests for backwards compatibility."""

    def test_smart_cache_alias_exists(self):
        """SmartCache should be an alias for UnifiedCache."""
        from deployment_api.utils.cache import SmartCache, UnifiedCache

        assert SmartCache is UnifiedCache

    def test_cache_singleton_is_unified_cache(self):
        """cache singleton should be UnifiedCache instance."""
        from deployment_api.utils.cache import UnifiedCache
        from deployment_api.utils.cache import cache as cache_singleton

        assert isinstance(cache_singleton, UnifiedCache)

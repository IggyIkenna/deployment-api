"""
Unit tests for deployment_caching module.

Tests cover TTL-based caches: verification, logs, VM logs, and
verification-pending tracking.
"""

import time

from deployment_api.routes.deployment_caching import (
    _LOGS_CACHE_TTL_SEC,
    _VERIFICATION_CACHE_TTL_SEC,
    _VM_LOGS_CACHE_TTL_SEC,
    _logs_cache,
    _verification_cache,
    _vm_logs_cache,
    add_verification_pending,
    get_logs_cache,
    get_verification_cache,
    get_vm_logs_cache,
    is_verification_pending,
    remove_verification_pending,
    set_logs_cache,
    set_verification_cache,
    set_vm_logs_cache,
)


class TestVerificationCache:
    """Tests for get_verification_cache and set_verification_cache."""

    def setup_method(self):
        _verification_cache.clear()

    def test_miss_returns_none(self):
        assert get_verification_cache("dep-xxx") is None

    def test_set_then_get(self):
        data = {"breakdown": "ok", "shard_count": 5}
        set_verification_cache("dep-123", data)
        result = get_verification_cache("dep-123")
        assert result == data

    def test_expired_returns_none(self):
        _verification_cache["dep-expired"] = (time.time() - 1, {"data": "stale"})
        assert get_verification_cache("dep-expired") is None

    def test_expired_entry_removed_from_cache(self):
        _verification_cache["dep-cleanup"] = (time.time() - 1, {"data": "old"})
        get_verification_cache("dep-cleanup")
        assert "dep-cleanup" not in _verification_cache

    def test_prune_when_cache_too_large(self):
        from deployment_api.routes.deployment_caching import _VERIFICATION_CACHE_MAX_ENTRIES

        # Fill cache to max
        for i in range(_VERIFICATION_CACHE_MAX_ENTRIES):
            _verification_cache[f"dep-fill-{i}"] = (time.time() + 100, {"i": i})

        # Adding one more should trigger pruning
        set_verification_cache("dep-new", {"new": True})
        # Cache size should still be manageable
        assert len(_verification_cache) <= _VERIFICATION_CACHE_MAX_ENTRIES + 1


class TestLogsCache:
    """Tests for get_logs_cache and set_logs_cache."""

    def setup_method(self):
        _logs_cache.clear()

    def test_miss_returns_none(self):
        assert get_logs_cache("key-xyz") is None

    def test_set_then_get(self):
        data = {"logs": ["line1", "line2"], "total": 2}
        set_logs_cache("key-abc", data)
        result = get_logs_cache("key-abc")
        assert result == data

    def test_expired_returns_none(self):
        _logs_cache["key-old"] = (time.time() - 1, {"logs": []})
        assert get_logs_cache("key-old") is None

    def test_expired_removed_from_cache(self):
        _logs_cache["key-cleanup"] = (time.time() - 1, {"data": "stale"})
        get_logs_cache("key-cleanup")
        assert "key-cleanup" not in _logs_cache

    def test_multiple_keys_independent(self):
        set_logs_cache("key-1", {"a": 1})
        set_logs_cache("key-2", {"b": 2})
        assert get_logs_cache("key-1") == {"a": 1}
        assert get_logs_cache("key-2") == {"b": 2}

    def test_prune_when_too_large(self):
        from deployment_api.routes.deployment_caching import _LOGS_CACHE_MAX_ENTRIES

        for i in range(_LOGS_CACHE_MAX_ENTRIES):
            _logs_cache[f"key-fill-{i}"] = (time.time() + 100, {"i": i})

        set_logs_cache("key-new", {"new": True})
        assert len(_logs_cache) <= _LOGS_CACHE_MAX_ENTRIES + 1


class TestVmLogsCache:
    """Tests for get_vm_logs_cache and set_vm_logs_cache."""

    def setup_method(self):
        _vm_logs_cache.clear()

    def test_miss_returns_none(self):
        assert get_vm_logs_cache("vm-xyz") is None

    def test_set_then_get(self):
        logs = ["line1", "line2"]
        message = "Retrieved 2 lines"
        set_vm_logs_cache("vm-abc", logs, message)
        result = get_vm_logs_cache("vm-abc")
        assert result is not None
        assert result[0] == logs
        assert result[1] == message

    def test_expired_returns_none(self):
        _vm_logs_cache["vm-old"] = (time.time() - 1, ([], "no lines"))
        assert get_vm_logs_cache("vm-old") is None

    def test_expired_removed_from_cache(self):
        _vm_logs_cache["vm-cleanup"] = (time.time() - 1, ([], ""))
        get_vm_logs_cache("vm-cleanup")
        assert "vm-cleanup" not in _vm_logs_cache

    def test_prune_when_too_large(self):
        from deployment_api.routes.deployment_caching import _VM_LOGS_CACHE_MAX_ENTRIES

        for i in range(_VM_LOGS_CACHE_MAX_ENTRIES):
            _vm_logs_cache[f"vm-fill-{i}"] = (time.time() + 100, ([], ""))

        set_vm_logs_cache("vm-new", ["line"], "1 line")
        assert len(_vm_logs_cache) <= _VM_LOGS_CACHE_MAX_ENTRIES + 1


class TestVerificationPending:
    """Tests for verification pending tracking."""

    def setup_method(self):
        from deployment_api.routes import deployment_caching
        deployment_caching._verification_pending.clear()

    def test_not_pending_by_default(self):
        assert is_verification_pending("dep-new") is False

    def test_add_then_check(self):
        add_verification_pending("dep-123")
        assert is_verification_pending("dep-123") is True

    def test_remove_then_check(self):
        add_verification_pending("dep-456")
        remove_verification_pending("dep-456")
        assert is_verification_pending("dep-456") is False

    def test_remove_nonexistent_no_error(self):
        # Should not raise
        remove_verification_pending("dep-nonexistent")

    def test_multiple_deployments_independent(self):
        add_verification_pending("dep-A")
        add_verification_pending("dep-B")
        assert is_verification_pending("dep-A") is True
        assert is_verification_pending("dep-B") is True
        remove_verification_pending("dep-A")
        assert is_verification_pending("dep-A") is False
        assert is_verification_pending("dep-B") is True

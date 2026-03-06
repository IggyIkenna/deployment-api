"""
Unit tests for log_analysis module.

Tests cover:
- invalidate_log_analysis_cache
- analyze_deployment_logs_sync (status filtering behavior)
"""

from types import SimpleNamespace
from unittest.mock import MagicMock

from deployment_api.routes.log_analysis import (
    _log_analysis_cache,
    analyze_deployment_logs_sync,
    invalidate_log_analysis_cache,
)


class TestInvalidateLogAnalysisCache:
    """Tests for invalidate_log_analysis_cache."""

    def setup_method(self):
        _log_analysis_cache.clear()

    def test_clear_all(self):
        _log_analysis_cache["dep1"] = {"timestamp": 0, "data": {}}
        _log_analysis_cache["dep2"] = {"timestamp": 0, "data": {}}
        invalidate_log_analysis_cache()
        assert len(_log_analysis_cache) == 0

    def test_clear_specific(self):
        _log_analysis_cache["dep1"] = {"timestamp": 0, "data": {}}
        _log_analysis_cache["dep2"] = {"timestamp": 0, "data": {}}
        invalidate_log_analysis_cache("dep1")
        assert "dep1" not in _log_analysis_cache
        assert "dep2" in _log_analysis_cache

    def test_clear_nonexistent_no_error(self):
        invalidate_log_analysis_cache("nonexistent")  # Should not raise

    def test_clear_empty_no_error(self):
        invalidate_log_analysis_cache()  # Should not raise


class TestAnalyzeDeploymentLogsSyncStatusFilter:
    """Tests for analyze_deployment_logs_sync status filtering."""

    def setup_method(self):
        _log_analysis_cache.clear()

    def test_running_returns_status_detail(self):
        """Running deployments skip log analysis."""
        state = SimpleNamespace(status=SimpleNamespace(value="running"))
        state_manager = MagicMock()
        result = analyze_deployment_logs_sync(state_manager, "dep-1", state)
        assert result["status_detail"] == "running"
        assert result["log_analysis"] is None

    def test_pending_returns_status_detail(self):
        state = SimpleNamespace(status=SimpleNamespace(value="pending"))
        state_manager = MagicMock()
        result = analyze_deployment_logs_sync(state_manager, "dep-1", state)
        assert result["status_detail"] == "pending"

    def test_completed_analyzes_logs_no_shards(self):
        """Completed deployments with no shards return empty log analysis."""
        state = SimpleNamespace(status=SimpleNamespace(value="completed"))
        state_manager = MagicMock()
        state_manager.get_deployment_shards.return_value = []
        result = analyze_deployment_logs_sync(state_manager, "dep-1", state)
        assert result.get("log_analysis") is not None
        assert result["log_analysis"]["shards_analyzed"] == 0

    def test_failed_with_string_status(self):
        """String status values also work (for backwards compatibility)."""
        state = SimpleNamespace(status="running")  # plain string, no .value
        state_manager = MagicMock()
        result = analyze_deployment_logs_sync(state_manager, "dep-1", state)
        assert result["status_detail"] == "running"

    def test_result_cached(self):
        """Results are cached after full log analysis completes."""
        # A completed state with a completed shard that has compute_info will
        # execute the full path and cache the result.
        state = SimpleNamespace(status=SimpleNamespace(value="completed"))
        state_manager = MagicMock()
        # Shard with status=completed but no compute_info (vm_name missing → no logs)
        state_manager.get_deployment_shards.return_value = [
            {"status": "completed", "shard_id": "s1", "compute_info": {}},
        ]
        result1 = analyze_deployment_logs_sync(state_manager, "dep-cached2", state)
        assert "dep-cached2" in _log_analysis_cache
        # Second call should return from cache
        state_manager_unused = MagicMock()
        result2 = analyze_deployment_logs_sync(state_manager_unused, "dep-cached2", state)
        assert result1 == result2


class TestAnalyzeDeploymentLogsWithShards:
    """Tests for analyze_deployment_logs_sync with shard log data."""

    def setup_method(self):
        _log_analysis_cache.clear()

    def test_no_completed_shards_returns_none_analysis(self):
        state = SimpleNamespace(status=SimpleNamespace(value="completed"))
        state_manager = MagicMock()
        # Shards all pending — not completed/succeeded/failed
        state_manager.get_deployment_shards.return_value = [
            {"status": "pending", "shard_id": "s1"},
        ]
        result = analyze_deployment_logs_sync(state_manager, "dep-1", state)
        assert result["log_analysis"] is None
        assert result["status_detail"] == "no_completed_shards"

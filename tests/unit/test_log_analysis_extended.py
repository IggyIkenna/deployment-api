"""
Extended unit tests for log_analysis module.

Covers:
- analyze_deployment_logs_sync (various status paths)
- invalidate_log_analysis_cache
- Cache hit behaviour
"""

import time
from types import SimpleNamespace
from unittest.mock import MagicMock

import pytest

import deployment_api.routes.log_analysis as la_mod
from deployment_api.routes.log_analysis import (
    _log_analysis_cache,
    analyze_deployment_logs_sync,
    invalidate_log_analysis_cache,
)


@pytest.fixture(autouse=True)
def clear_cache():
    _log_analysis_cache.clear()
    yield
    _log_analysis_cache.clear()


def _make_state(status_val: str = "completed"):
    state = SimpleNamespace(status=SimpleNamespace(value=status_val))
    return state


def _make_state_manager(shards=None, vm_logs=None):
    sm = MagicMock()
    sm.get_deployment_shards.return_value = shards or []
    sm.get_vm_serial_console.return_value = vm_logs or ""
    return sm


class TestAnalyzeDeploymentLogsSync:
    """Tests for analyze_deployment_logs_sync."""

    def test_running_state_returns_early(self):
        state = _make_state("running")
        sm = _make_state_manager()
        result = analyze_deployment_logs_sync(sm, "dep-001", state)
        assert result["status_detail"] == "running"
        assert result["log_analysis"] is None
        sm.get_deployment_shards.assert_not_called()

    def test_pending_state_returns_early(self):
        state = _make_state("pending")
        sm = _make_state_manager()
        result = analyze_deployment_logs_sync(sm, "dep-001", state)
        assert result["status_detail"] == "pending"

    def test_no_shards_returns_no_shards(self):
        state = _make_state("completed")
        sm = _make_state_manager(shards=[])
        result = analyze_deployment_logs_sync(sm, "dep-001", state)
        assert result["status_detail"] == "no_shards"
        assert result["log_analysis"]["shards_analyzed"] == 0

    def test_no_completed_shards_returns_no_completed_shards(self):
        state = _make_state("completed")
        shards = [{"status": "running", "shard_id": "s1"}]
        sm = _make_state_manager(shards=shards)
        result = analyze_deployment_logs_sync(sm, "dep-001", state)
        assert result["status_detail"] == "no_completed_shards"

    def test_completed_shard_no_compute_info(self):
        state = _make_state("completed")
        shards = [{"status": "completed", "shard_id": "s1", "compute_info": {}}]
        sm = _make_state_manager(shards=shards)
        result = analyze_deployment_logs_sync(sm, "dep-001", state)
        assert result["status_detail"] == "completed"  # no errors, no stack traces
        assert result["log_analysis"]["shards_analyzed"] == 1

    def test_completed_shard_with_error_in_logs(self):
        state = _make_state("completed")
        shards = [{"status": "completed", "shard_id": "s1", "compute_info": {"vm_name": "vm-01"}}]
        sm = _make_state_manager(
            shards=shards,
            vm_logs="some text\nERROR: something went wrong\nmore text",
        )
        result = analyze_deployment_logs_sync(sm, "dep-001", state)
        assert result["status_detail"] == "failed_with_errors"
        assert len(result["log_analysis"]["errors"]) >= 1

    def test_completed_shard_with_warning_in_logs(self):
        state = _make_state("completed")
        shards = [{"status": "completed", "shard_id": "s1", "compute_info": {"vm_name": "vm-01"}}]
        sm = _make_state_manager(
            shards=shards,
            vm_logs="WARNING: disk almost full\nok",
        )
        result = analyze_deployment_logs_sync(sm, "dep-001", state)
        assert result["status_detail"] in ("completed_with_warnings", "completed")
        assert len(result["log_analysis"]["warnings"]) >= 1

    def test_completed_shard_with_success_indicators(self):
        state = _make_state("completed")
        shards = [{"status": "completed", "shard_id": "s1", "compute_info": {"vm_name": "vm-01"}}]
        sm = _make_state_manager(
            shards=shards,
            vm_logs="Processing complete\nAll done",
        )
        result = analyze_deployment_logs_sync(sm, "dep-001", state)
        assert result["status_detail"] == "succeeded"

    def test_failed_state_keeps_failed(self):
        state = _make_state("failed")
        shards = [{"status": "failed", "shard_id": "s1", "compute_info": {"vm_name": "vm-01"}}]
        sm = _make_state_manager(shards=shards, vm_logs="")
        result = analyze_deployment_logs_sync(sm, "dep-001", state)
        assert result["status_detail"] == "failed"

    def test_succeeded_state_keeps_succeeded(self):
        state = _make_state("succeeded")
        shards = [{"status": "succeeded", "shard_id": "s1", "compute_info": {"vm_name": "vm-01"}}]
        sm = _make_state_manager(shards=shards, vm_logs="")
        result = analyze_deployment_logs_sync(sm, "dep-001", state)
        assert result["status_detail"] == "succeeded"

    def test_stack_trace_detected(self):
        state = _make_state("completed")
        shards = [{"status": "completed", "shard_id": "s1", "compute_info": {"vm_name": "vm-01"}}]
        sm = _make_state_manager(
            shards=shards,
            vm_logs="Traceback (most recent call last):\n  File foo.py line 10\nValueError: bad value",
        )
        result = analyze_deployment_logs_sync(sm, "dep-001", state)
        assert result["log_analysis"]["has_stack_traces"] is True
        assert result["status_detail"] == "failed_with_errors"

    def test_result_is_cached(self):
        state = _make_state("completed")
        shards = [{"status": "completed", "shard_id": "s1", "compute_info": {}}]
        sm = _make_state_manager(shards=shards)
        analyze_deployment_logs_sync(sm, "dep-001", state)
        assert "dep-001" in _log_analysis_cache

    def test_cache_hit_skips_shards_call(self):
        state = _make_state("completed")
        sm = _make_state_manager()
        # Pre-populate cache
        _log_analysis_cache["dep-001"] = {
            "data": {"status_detail": "cached", "log_analysis": {}},
            "timestamp": time.time(),
        }
        result = analyze_deployment_logs_sync(sm, "dep-001", state)
        assert result["status_detail"] == "cached"
        sm.get_deployment_shards.assert_not_called()

    def test_expired_cache_re_runs(self):
        state = _make_state("completed")
        shards = [{"status": "completed", "shard_id": "s1", "compute_info": {}}]
        sm = _make_state_manager(shards=shards)
        _log_analysis_cache["dep-001"] = {
            "data": {"status_detail": "old_cached", "log_analysis": {}},
            "timestamp": 0,  # expired
        }
        result = analyze_deployment_logs_sync(sm, "dep-001", state)
        # Should re-run analysis, not return expired cache
        sm.get_deployment_shards.assert_called_once()

    def test_status_via_str_not_value(self):
        """Status can come from str(state.status) if no .value attribute."""
        state = SimpleNamespace(status="completed")
        shards = [{"status": "completed", "shard_id": "s1", "compute_info": {}}]
        sm = _make_state_manager(shards=shards)
        result = analyze_deployment_logs_sync(sm, "dep-001", state)
        assert "status_detail" in result

    def test_exception_from_state_manager_returns_error(self):
        state = _make_state("completed")
        sm = MagicMock()
        sm.get_deployment_shards.side_effect = OSError("GCS down")
        result = analyze_deployment_logs_sync(sm, "dep-001", state)
        assert result["status_detail"] == "analysis_error"
        assert "error" in result


class TestInvalidateLogAnalysisCache:
    """Tests for invalidate_log_analysis_cache."""

    def test_invalidate_specific_entry(self):
        _log_analysis_cache["dep-001"] = {"data": {}, "timestamp": time.time()}
        _log_analysis_cache["dep-002"] = {"data": {}, "timestamp": time.time()}
        invalidate_log_analysis_cache("dep-001")
        assert "dep-001" not in _log_analysis_cache
        assert "dep-002" in _log_analysis_cache

    def test_invalidate_nonexistent_is_noop(self):
        invalidate_log_analysis_cache("nonexistent")

    def test_invalidate_all_when_none(self):
        _log_analysis_cache["dep-a"] = {"data": {}, "timestamp": time.time()}
        _log_analysis_cache["dep-b"] = {"data": {}, "timestamp": time.time()}
        invalidate_log_analysis_cache(None)
        assert len(_log_analysis_cache) == 0

    def test_invalidate_all_when_no_arg(self):
        _log_analysis_cache["dep-a"] = {"data": {}, "timestamp": time.time()}
        invalidate_log_analysis_cache()
        assert len(_log_analysis_cache) == 0

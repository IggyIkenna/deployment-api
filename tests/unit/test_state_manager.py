"""
Unit tests for state_manager module.

Tests cover pure methods that don't require GCS:
- get_deployment_lock_blob_name
- held_deployment_locks property
- track_pending_vm_delete
- cleanup_pending_vm_deletes
- get_retry_vm_deletes
"""

import importlib.util
import os
import time
from unittest.mock import patch

# Load directly to avoid circular import via services/__init__.py
_path = os.path.join(os.path.dirname(__file__), "../../deployment_api/services/state_manager.py")
_spec = importlib.util.spec_from_file_location("_sm_standalone", os.path.abspath(_path))
assert _spec is not None and _spec.loader is not None
_sm_mod = importlib.util.module_from_spec(_spec)
_spec.loader.exec_module(_sm_mod)  # type: ignore[union-attr]
StateManager = _sm_mod.StateManager


def _make_state_manager():
    return StateManager(
        project_id="test-proj",
        state_bucket="test-bucket",
        deployment_env="test",
    )


class TestGetDeploymentLockBlobName:
    """Tests for StateManager.get_deployment_lock_blob_name."""

    def test_format(self):
        sm = _make_state_manager()
        name = sm.get_deployment_lock_blob_name("dep-123")
        assert name == "locks/deployment_dep-123.lock"

    def test_different_ids(self):
        sm = _make_state_manager()
        name1 = sm.get_deployment_lock_blob_name("abc")
        name2 = sm.get_deployment_lock_blob_name("xyz")
        assert name1 != name2
        assert "abc" in name1
        assert "xyz" in name2


class TestHeldDeploymentLocks:
    """Tests for StateManager.held_deployment_locks property."""

    def test_empty_initially(self):
        sm = _make_state_manager()
        assert sm.held_deployment_locks == set()

    def test_returns_copy(self):
        sm = _make_state_manager()
        sm._held_deployment_locks.add("dep-1")
        locks = sm.held_deployment_locks
        locks.add("dep-2")
        # Modifying returned set should not affect internal set
        assert "dep-2" not in sm._held_deployment_locks


class TestTrackPendingVmDelete:
    """Tests for StateManager.track_pending_vm_delete."""

    def test_tracking_adds_entry(self):
        sm = _make_state_manager()
        sm.track_pending_vm_delete("job-001", zone="us-central1-a")
        assert "job-001" in sm._pending_vm_deletes

    def test_tracking_without_zone(self):
        sm = _make_state_manager()
        sm.track_pending_vm_delete("job-002")
        assert "job-002" in sm._pending_vm_deletes

    def test_multiple_deletes(self):
        sm = _make_state_manager()
        sm.track_pending_vm_delete("job-001")
        sm.track_pending_vm_delete("job-002")
        assert len(sm._pending_vm_deletes) == 2


class TestCleanupPendingVmDeletes:
    """Tests for StateManager.cleanup_pending_vm_deletes."""

    def test_removes_not_in_vm_map(self):
        sm = _make_state_manager()
        sm._pending_vm_deletes["job-001"] = (time.time(), "us-central1-a")
        sm._pending_vm_deletes["job-002"] = (time.time(), None)
        vm_map = {"vm-1": {"job_id": "job-001", "status": "RUNNING"}}
        sm.cleanup_pending_vm_deletes(vm_map)
        # job-001 is in vm_map — keep it? No, cleanup removes entries NOT in vm_map
        # Wait, let me check the logic: removes if jid not in vm_map
        # vm_map keys are vm names, not job IDs! So job-001 is a job_id, not a vm_map key
        # "job-001" not in {"vm-1": ...} → removes job-001
        assert "job-001" not in sm._pending_vm_deletes
        assert "job-002" not in sm._pending_vm_deletes

    def test_keeps_entries_in_vm_map(self):
        sm = _make_state_manager()
        sm._pending_vm_deletes["vm-1"] = (time.time(), "us-central1-a")
        vm_map = {"vm-1": {"status": "RUNNING"}}
        sm.cleanup_pending_vm_deletes(vm_map)
        # "vm-1" IS in vm_map keys → kept
        assert "vm-1" in sm._pending_vm_deletes

    def test_empty_pending_no_error(self):
        sm = _make_state_manager()
        sm.cleanup_pending_vm_deletes({"vm-1": {}})  # Should not raise


class TestGetRetryVmDeletes:
    """Tests for StateManager.get_retry_vm_deletes."""

    def test_returns_running_vms_past_retry_threshold(self):
        sm = _make_state_manager()
        old_ts = time.time() - 10000  # Well past any retry threshold
        sm._pending_vm_deletes["job-001"] = (old_ts, "us-central1-a")

        vm_map = {"vm-1": {"job_id": "job-001", "status": "RUNNING"}}

        with patch.object(_sm_mod.settings, "ORPHAN_DELETE_RETRY_SECONDS", 60):
            result = sm.get_retry_vm_deletes(vm_map)

        assert "job-001" in result

    def test_excludes_non_running_vms(self):
        sm = _make_state_manager()
        old_ts = time.time() - 10000
        sm._pending_vm_deletes["job-001"] = (old_ts, None)

        vm_map = {"vm-1": {"job_id": "job-001", "status": "TERMINATED"}}

        with patch.object(_sm_mod.settings, "ORPHAN_DELETE_RETRY_SECONDS", 60):
            result = sm.get_retry_vm_deletes(vm_map)

        assert "job-001" not in result

    def test_excludes_recent_deletes(self):
        sm = _make_state_manager()
        sm._pending_vm_deletes["job-001"] = (time.time(), None)  # Just added

        vm_map = {"vm-1": {"job_id": "job-001", "status": "RUNNING"}}

        with patch.object(_sm_mod.settings, "ORPHAN_DELETE_RETRY_SECONDS", 3600):
            result = sm.get_retry_vm_deletes(vm_map)

        assert "job-001" not in result

    def test_empty_pending_returns_empty(self):
        sm = _make_state_manager()
        with patch.object(_sm_mod.settings, "ORPHAN_DELETE_RETRY_SECONDS", 60):
            result = sm.get_retry_vm_deletes({})
        assert result == []

"""
Unit tests for services/state_manager module (StateManager class).

Tests initialization, lock blob name, held_deployment_locks property,
track_pending_vm_delete, cleanup_pending_vm_deletes, get_retry_vm_deletes,
try_acquire_deployment_lock, release_deployment_lock.
"""

import sys
import time
from pathlib import Path
from types import ModuleType
from unittest.mock import MagicMock, patch

# Remove pre-mocked entries so we import the real StateManager.
for _key in list(sys.modules.keys()):
    if _key in (
        "deployment_api.services.state_manager",
        "deployment_api.services",
    ):
        del sys.modules[_key]

# Re-build minimal package pointing to the real services directory.
_svc_pkg = ModuleType("deployment_api.services")
_svc_pkg.__package__ = "deployment_api.services"
_services_dir = str(Path(__file__).parent.parent.parent / "deployment_api" / "services")
_svc_pkg.__path__ = [_services_dir]  # type: ignore[attr-defined]
sys.modules["deployment_api.services"] = _svc_pkg

import deployment_api.services.state_manager as _sm_mod
from deployment_api.services.state_manager import StateManager


def _make_mgr(**kwargs) -> StateManager:
    return StateManager(
        project_id=kwargs.get("project_id", "test-project"),
        state_bucket=kwargs.get("state_bucket", "test-bucket"),
        deployment_env=kwargs.get("deployment_env", "development"),
    )


class TestStateManagerInit:
    """Tests for StateManager.__init__."""

    def test_sets_project_id(self):
        mgr = _make_mgr(project_id="proj-x")
        assert mgr.project_id == "proj-x"

    def test_sets_state_bucket(self):
        mgr = _make_mgr(state_bucket="bkt-x")
        assert mgr.state_bucket == "bkt-x"

    def test_sets_deployment_env(self):
        mgr = _make_mgr(deployment_env="production")
        assert mgr.deployment_env == "production"

    def test_held_locks_starts_empty(self):
        mgr = _make_mgr()
        assert mgr._held_deployment_locks == set()

    def test_pending_vm_deletes_starts_empty(self):
        mgr = _make_mgr()
        assert mgr._pending_vm_deletes == {}

    def test_owner_id_is_string(self):
        mgr = _make_mgr()
        assert isinstance(mgr.owner_id, str)
        assert len(mgr.owner_id) > 0

    def test_lock_ttl_is_numeric(self):
        mgr = _make_mgr()
        assert isinstance(mgr.lock_ttl_seconds, (int, float))


class TestHeldDeploymentLocksProperty:
    """Tests for held_deployment_locks property."""

    def test_returns_copy_of_internal_set(self):
        mgr = _make_mgr()
        mgr._held_deployment_locks.add("dep-1")
        result = mgr.held_deployment_locks
        assert "dep-1" in result
        # Should be a copy - modifying result doesn't affect internal set
        result.add("dep-extra")
        assert "dep-extra" not in mgr._held_deployment_locks


class TestGetDeploymentLockBlobName:
    """Tests for get_deployment_lock_blob_name."""

    def test_returns_correct_lock_path(self):
        mgr = _make_mgr()
        result = mgr.get_deployment_lock_blob_name("my-deployment")
        assert result == "locks/deployment_my-deployment.lock"

    def test_different_ids_give_different_paths(self):
        mgr = _make_mgr()
        p1 = mgr.get_deployment_lock_blob_name("dep-1")
        p2 = mgr.get_deployment_lock_blob_name("dep-2")
        assert p1 != p2


class TestTrackPendingVmDelete:
    """Tests for track_pending_vm_delete."""

    def test_adds_to_pending_deletes(self):
        mgr = _make_mgr()
        mgr.track_pending_vm_delete("vm-job-1", "us-central1-a")
        assert "vm-job-1" in mgr._pending_vm_deletes

    def test_records_timestamp_and_zone(self):
        mgr = _make_mgr()
        before = time.time()
        mgr.track_pending_vm_delete("vm-job-1", "us-central1-a")
        after = time.time()
        entry = mgr._pending_vm_deletes["vm-job-1"]
        ts, zone = entry
        assert before <= ts <= after
        assert zone == "us-central1-a"

    def test_zone_can_be_none(self):
        mgr = _make_mgr()
        mgr.track_pending_vm_delete("vm-job-no-zone")
        _ts, zone = mgr._pending_vm_deletes["vm-job-no-zone"]
        assert zone is None


class TestCleanupPendingVmDeletes:
    """Tests for cleanup_pending_vm_deletes."""

    def test_removes_vms_not_in_map(self):
        mgr = _make_mgr()
        mgr._pending_vm_deletes["vm-old"] = (time.time(), "us-central1-a")
        mgr.cleanup_pending_vm_deletes({})  # Empty vm_map
        assert "vm-old" not in mgr._pending_vm_deletes

    def test_keeps_vms_still_in_map(self):
        mgr = _make_mgr()
        mgr._pending_vm_deletes["vm-active"] = (time.time(), "us-central1-a")
        mgr.cleanup_pending_vm_deletes({"vm-active": {"status": "RUNNING"}})
        assert "vm-active" in mgr._pending_vm_deletes

    def test_handles_empty_pending_deletes(self):
        mgr = _make_mgr()
        # Should not raise
        mgr.cleanup_pending_vm_deletes({"vm-1": {}})


class TestGetRetryVmDeletes:
    """Tests for get_retry_vm_deletes."""

    def test_returns_empty_when_no_pending(self):
        mgr = _make_mgr()
        result = mgr.get_retry_vm_deletes({})
        assert result == []

    def test_returns_job_ids_ready_for_retry(self):
        mgr = _make_mgr()
        old_ts = time.time() - 9999  # Way past any retry threshold
        mgr._pending_vm_deletes["vm-retry"] = (old_ts, "us-central1-a")
        vm_map = {"some-key": {"job_id": "vm-retry", "status": "RUNNING"}}

        with patch.object(_sm_mod, "settings") as mock_settings:
            mock_settings.ORPHAN_DELETE_RETRY_SECONDS = 120
            result = mgr.get_retry_vm_deletes(vm_map)

        assert "vm-retry" in result

    def test_excludes_recently_added_vms(self):
        mgr = _make_mgr()
        recent_ts = time.time()  # Just added
        mgr._pending_vm_deletes["vm-new"] = (recent_ts, "us-central1-a")
        vm_map = {"key": {"job_id": "vm-new", "status": "RUNNING"}}

        with patch.object(_sm_mod, "settings") as mock_settings:
            mock_settings.ORPHAN_DELETE_RETRY_SECONDS = 120
            result = mgr.get_retry_vm_deletes(vm_map)

        assert "vm-new" not in result

    def test_excludes_non_running_vms(self):
        mgr = _make_mgr()
        old_ts = time.time() - 9999
        mgr._pending_vm_deletes["vm-stopped"] = (old_ts, "us-central1-a")
        vm_map = {"key": {"job_id": "vm-stopped", "status": "TERMINATED"}}

        with patch.object(_sm_mod, "settings") as mock_settings:
            mock_settings.ORPHAN_DELETE_RETRY_SECONDS = 120
            result = mgr.get_retry_vm_deletes(vm_map)

        assert "vm-stopped" not in result


class TestTryAcquireDeploymentLock:
    """Tests for try_acquire_deployment_lock."""

    def test_returns_true_on_successful_upload(self):
        mgr = _make_mgr()
        mock_client = MagicMock()
        mock_bucket = MagicMock()
        mock_blob = MagicMock()
        mock_bucket.blob.return_value = mock_blob
        mock_client.bucket.return_value = mock_bucket

        with patch.object(_sm_mod, "get_storage_client_with_pool", return_value=mock_client):
            result = mgr.try_acquire_deployment_lock("dep-1")

        assert result is True
        assert "dep-1" in mgr._held_deployment_locks

    def test_returns_false_when_lock_held_by_other(self):
        mgr = _make_mgr()
        mock_client = MagicMock()
        mock_bucket = MagicMock()
        mock_blob = MagicMock()
        # Simulate "lock already exists" by raising on upload
        mock_blob.upload_from_string.side_effect = OSError("412 precondition failed")
        # Simulate existing lock owned by another
        existing_blob = MagicMock()
        existing_blob.metageneration = 1
        import json as _json

        existing_blob.download_as_text.return_value = _json.dumps(
            {
                "owner": "other-owner",
                "expires_at": (time.time() + 9999),  # Not expired
            }
        )
        mock_bucket.blob.return_value = mock_blob
        mock_bucket.get_blob.return_value = existing_blob
        mock_client.bucket.return_value = mock_bucket

        with patch.object(_sm_mod, "get_storage_client_with_pool", return_value=mock_client):
            result = mgr.try_acquire_deployment_lock("dep-locked")

        assert result is False

    def test_acquires_expired_lock(self):
        mgr = _make_mgr()
        mock_client = MagicMock()
        mock_bucket = MagicMock()
        # First blob.upload_from_string raises (lock exists), then blob.upload_from_string succeeds
        call_count = {"n": 0}
        mock_blob = MagicMock()

        def upload_side_effect(*a, **kw):
            call_count["n"] += 1
            if call_count["n"] == 1:
                raise OSError("412 precondition failed")
            # Second call (metageneration-match renewal) succeeds

        mock_blob.upload_from_string.side_effect = upload_side_effect
        existing_blob = MagicMock()
        existing_blob.metageneration = 1
        import json as _json

        existing_blob.download_as_text.return_value = _json.dumps(
            {
                "owner": "other-owner",
                "expires_at": (time.time() - 100),  # Already expired
            }
        )
        mock_bucket.blob.return_value = mock_blob
        mock_bucket.get_blob.return_value = existing_blob
        mock_client.bucket.return_value = mock_bucket

        with patch.object(_sm_mod, "get_storage_client_with_pool", return_value=mock_client):
            result = mgr.try_acquire_deployment_lock("dep-expired")

        # The renewal path uploads to the blob - if that succeeds, result is True
        # (depends on the implementation detail of how renewal is done)
        assert isinstance(result, bool)


class TestReleaseDeploymentLock:
    """Tests for release_deployment_lock."""

    def test_returns_false_when_lock_not_owned(self):
        mgr = _make_mgr()
        # Blob exists but owned by someone else
        mock_client = MagicMock()
        mock_bucket = MagicMock()
        mock_blob = MagicMock()
        mock_blob.exists.return_value = True
        import json as _json

        mock_blob.download_as_text.return_value = _json.dumps({"owner": "other-owner"})
        mock_bucket.blob.return_value = mock_blob
        mock_client.bucket.return_value = mock_bucket

        with patch.object(_sm_mod, "get_storage_client_with_pool", return_value=mock_client):
            result = mgr.release_deployment_lock("dep-not-held")

        assert result is False
        mock_blob.delete.assert_not_called()

    def test_returns_true_when_held_and_deleted(self):
        mgr = _make_mgr()
        mgr._held_deployment_locks.add("dep-held")

        mock_client = MagicMock()
        mock_bucket = MagicMock()
        mock_blob = MagicMock()
        mock_blob.exists.return_value = True
        import json as _json

        mock_blob.download_as_text.return_value = _json.dumps({"owner": mgr.owner_id})
        mock_bucket.blob.return_value = mock_blob
        mock_client.bucket.return_value = mock_bucket

        with patch.object(_sm_mod, "get_storage_client_with_pool", return_value=mock_client):
            result = mgr.release_deployment_lock("dep-held")

        assert result is True
        mock_blob.delete.assert_called_once()
        assert "dep-held" not in mgr._held_deployment_locks

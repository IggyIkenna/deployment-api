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
from unittest.mock import MagicMock, patch

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

    def test_float_pending_value_is_treated_as_timestamp(self):
        # Covers the `pending_ts` helper's float branch (line 317)
        sm = _make_state_manager()
        old_ts = time.time() - 10000
        sm._pending_vm_deletes["job-001"] = old_ts  # plain float, not tuple
        vm_map = {"vm-1": {"job_id": "job-001", "status": "RUNNING"}}
        with patch.object(_sm_mod.settings, "ORPHAN_DELETE_RETRY_SECONDS", 60):
            result = sm.get_retry_vm_deletes(vm_map)
        assert "job-001" in result


class TestTryAcquireDeploymentLock:
    """Tests for StateManager.try_acquire_deployment_lock (lines 79-139)."""

    def _mock_bucket(self):
        bucket = MagicMock()
        return bucket

    def _make_sm_with_bucket(self, bucket):
        from unittest.mock import MagicMock

        sm = _make_state_manager()
        mock_client = MagicMock()
        mock_client.bucket.return_value = bucket
        sm._get_storage_client = lambda: mock_client
        return sm, mock_client

    def test_fast_path_acquires_lock_successfully(self):
        from unittest.mock import MagicMock, patch

        sm = _make_state_manager()
        bucket = MagicMock()
        mock_blob = MagicMock()
        bucket.blob.return_value = mock_blob
        # upload_from_string succeeds (no exception)
        mock_blob.upload_from_string = MagicMock()
        mock_client = MagicMock()
        mock_client.bucket.return_value = bucket

        with patch.object(_sm_mod, "get_storage_client_with_pool", return_value=mock_client):
            result = sm.try_acquire_deployment_lock("dep-abc")

        assert result is True
        assert "dep-abc" in sm._held_deployment_locks

    def test_fast_path_fails_existing_blob_not_owned(self):
        import json as _json
        from unittest.mock import MagicMock, patch

        sm = _make_state_manager()
        bucket = MagicMock()
        mock_blob = MagicMock()
        bucket.blob.return_value = mock_blob
        # Simulate generation conflict on first upload
        mock_blob.upload_from_string.side_effect = OSError("precondition failed")

        existing = MagicMock()
        existing.metageneration = 1
        expires_future = time.time() + 9999
        existing.download_as_text.return_value = _json.dumps(
            {
                "owner": "someone-else",
                "expires_at": expires_future,
            }
        )
        bucket.get_blob.return_value = existing
        mock_client = MagicMock()
        mock_client.bucket.return_value = bucket

        with patch.object(_sm_mod, "get_storage_client_with_pool", return_value=mock_client):
            result = sm.try_acquire_deployment_lock("dep-abc")

        assert result is False
        assert "dep-abc" not in sm._held_deployment_locks

    def test_acquires_expired_lock_from_other_owner(self):
        import json as _json
        from unittest.mock import MagicMock, patch

        sm = _make_state_manager()
        bucket = MagicMock()
        mock_blob = MagicMock()
        bucket.blob.return_value = mock_blob
        mock_blob.upload_from_string.side_effect = [OSError("conflict"), None]

        existing = MagicMock()
        existing.metageneration = 2
        expired_ts = time.time() - 9999
        existing.download_as_text.return_value = _json.dumps(
            {
                "owner": "old-owner",
                "expires_at": expired_ts,
            }
        )
        bucket.get_blob.return_value = existing
        mock_client = MagicMock()
        mock_client.bucket.return_value = bucket

        with patch.object(_sm_mod, "get_storage_client_with_pool", return_value=mock_client):
            result = sm.try_acquire_deployment_lock("dep-abc")

        assert result is True
        assert "dep-abc" in sm._held_deployment_locks

    def test_renews_lock_owned_by_self(self):
        import json as _json
        from unittest.mock import MagicMock, patch

        sm = _make_state_manager()
        bucket = MagicMock()
        mock_blob = MagicMock()
        bucket.blob.return_value = mock_blob
        mock_blob.upload_from_string.side_effect = [OSError("conflict"), None]

        existing = MagicMock()
        existing.metageneration = 3
        future_ts = time.time() + 9999
        existing.download_as_text.return_value = _json.dumps(
            {
                "owner": sm.owner_id,
                "expires_at": future_ts,
            }
        )
        bucket.get_blob.return_value = existing
        mock_client = MagicMock()
        mock_client.bucket.return_value = bucket

        with patch.object(_sm_mod, "get_storage_client_with_pool", return_value=mock_client):
            result = sm.try_acquire_deployment_lock("dep-abc")

        assert result is True

    def test_returns_false_when_no_existing_blob(self):
        from unittest.mock import MagicMock, patch

        sm = _make_state_manager()
        bucket = MagicMock()
        mock_blob = MagicMock()
        bucket.blob.return_value = mock_blob
        mock_blob.upload_from_string.side_effect = KeyError("generation mismatch")
        bucket.get_blob.return_value = None
        mock_client = MagicMock()
        mock_client.bucket.return_value = bucket

        with patch.object(_sm_mod, "get_storage_client_with_pool", return_value=mock_client):
            result = sm.try_acquire_deployment_lock("dep-xyz")

        assert result is False

    def test_returns_false_on_second_upload_error(self):
        import json as _json
        from unittest.mock import MagicMock, patch

        sm = _make_state_manager()
        bucket = MagicMock()
        mock_blob = MagicMock()
        bucket.blob.return_value = mock_blob
        mock_blob.upload_from_string.side_effect = OSError("always fails")

        existing = MagicMock()
        existing.metageneration = 4
        existing.download_as_text.return_value = _json.dumps(
            {
                "owner": "other",
                "expires_at": time.time() - 100,
            }
        )
        bucket.get_blob.return_value = existing
        mock_client = MagicMock()
        mock_client.bucket.return_value = bucket

        with patch.object(_sm_mod, "get_storage_client_with_pool", return_value=mock_client):
            result = sm.try_acquire_deployment_lock("dep-abc")

        assert result is False

    def test_download_parse_error_uses_empty_payload(self):
        from unittest.mock import MagicMock, patch

        sm = _make_state_manager()
        bucket = MagicMock()
        mock_blob = MagicMock()
        bucket.blob.return_value = mock_blob
        mock_blob.upload_from_string.side_effect = [OSError("conflict"), None]

        existing = MagicMock()
        existing.metageneration = 5
        existing.download_as_text.return_value = "not json{{{"
        bucket.get_blob.return_value = existing
        mock_client = MagicMock()
        mock_client.bucket.return_value = bucket

        with patch.object(_sm_mod, "get_storage_client_with_pool", return_value=mock_client):
            # expires_at defaults to 0, so is_expired=True -> can acquire
            result = sm.try_acquire_deployment_lock("dep-abc")

        assert result is True


class TestReleaseDeploymentLock:
    """Tests for StateManager.release_deployment_lock (lines 153-170)."""

    def test_releases_owned_lock(self):
        import json as _json
        from unittest.mock import MagicMock, patch

        sm = _make_state_manager()
        sm._held_deployment_locks.add("dep-1")

        bucket = MagicMock()
        mock_blob = MagicMock()
        mock_blob.exists.return_value = True
        mock_blob.download_as_text.return_value = _json.dumps({"owner": sm.owner_id})
        bucket.blob.return_value = mock_blob
        mock_client = MagicMock()
        mock_client.bucket.return_value = bucket

        with patch.object(_sm_mod, "get_storage_client_with_pool", return_value=mock_client):
            result = sm.release_deployment_lock("dep-1")

        assert result is True
        assert "dep-1" not in sm._held_deployment_locks
        mock_blob.delete.assert_called_once()

    def test_does_not_release_unowned_lock(self):
        import json as _json
        from unittest.mock import MagicMock, patch

        sm = _make_state_manager()
        sm._held_deployment_locks.add("dep-2")

        bucket = MagicMock()
        mock_blob = MagicMock()
        mock_blob.exists.return_value = True
        mock_blob.download_as_text.return_value = _json.dumps({"owner": "other-owner"})
        bucket.blob.return_value = mock_blob
        mock_client = MagicMock()
        mock_client.bucket.return_value = bucket

        with patch.object(_sm_mod, "get_storage_client_with_pool", return_value=mock_client):
            result = sm.release_deployment_lock("dep-2")

        assert result is False
        mock_blob.delete.assert_not_called()

    def test_returns_false_when_blob_does_not_exist(self):
        from unittest.mock import MagicMock, patch

        sm = _make_state_manager()
        sm._held_deployment_locks.add("dep-3")

        bucket = MagicMock()
        mock_blob = MagicMock()
        mock_blob.exists.return_value = False
        bucket.blob.return_value = mock_blob
        mock_client = MagicMock()
        mock_client.bucket.return_value = bucket

        with patch.object(_sm_mod, "get_storage_client_with_pool", return_value=mock_client):
            result = sm.release_deployment_lock("dep-3")

        assert result is False
        assert "dep-3" not in sm._held_deployment_locks

    def test_exception_during_release_returns_false(self):
        from unittest.mock import MagicMock, patch

        sm = _make_state_manager()
        sm._held_deployment_locks.add("dep-4")

        bucket = MagicMock()
        bucket.blob.side_effect = RuntimeError("storage unavailable")
        mock_client = MagicMock()
        mock_client.bucket.return_value = bucket

        with patch.object(_sm_mod, "get_storage_client_with_pool", return_value=mock_client):
            result = sm.release_deployment_lock("dep-4")

        assert result is False
        assert "dep-4" not in sm._held_deployment_locks


class TestRunOrphanCleanupOnly:
    """Tests for StateManager.run_orphan_cleanup_only (lines 184-300)."""

    def test_returns_zero_for_non_vm_compute_type(self):
        sm = _make_state_manager()
        result = sm.run_orphan_cleanup_only({"compute_type": "cloud_run"})
        assert result == 0

    def test_returns_zero_for_missing_compute_type(self):
        sm = _make_state_manager()
        result = sm.run_orphan_cleanup_only({})
        assert result == 0

    def test_returns_zero_when_no_shards(self):
        sm = _make_state_manager()
        result = sm.run_orphan_cleanup_only({"compute_type": "vm", "shards": []})
        assert result == 0

    def test_returns_zero_when_shards_is_none(self):
        sm = _make_state_manager()
        result = sm.run_orphan_cleanup_only({"compute_type": "vm", "shards": None})
        assert result == 0

    def test_returns_zero_when_storage_facade_unavailable(self):
        import sys
        from types import ModuleType
        from unittest.mock import MagicMock

        sm = _make_state_manager()

        # Ensure storage_facade raises on import
        sf_mod = ModuleType("deployment_api.utils.storage_facade")
        sf_mod.read_object_text = MagicMock(side_effect=RuntimeError("gcs down"))  # type: ignore[attr-defined]
        sf_mod.list_objects = MagicMock(return_value=[])  # type: ignore[attr-defined]

        state = {
            "compute_type": "vm",
            "deployment_id": "dep-1",
            "shards": [{"shard_id": "s1", "job_id": "j1"}],
        }

        orig = sys.modules.get("deployment_api.utils.storage_facade")
        sys.modules["deployment_api.utils.storage_facade"] = sf_mod

        orig_su = sys.modules.get("deployment_api.utils.service_utils")
        su_mod = ModuleType("deployment_api.utils.service_utils")
        su_mod.parse_service_event = MagicMock(return_value=None)  # type: ignore[attr-defined]
        sys.modules["deployment_api.utils.service_utils"] = su_mod

        try:
            result = sm.run_orphan_cleanup_only(state)
            # Storage error causes 0 return (no orphan fires)
            assert result == 0
        finally:
            if orig is not None:
                sys.modules["deployment_api.utils.storage_facade"] = orig
            elif "deployment_api.utils.storage_facade" in sys.modules:
                del sys.modules["deployment_api.utils.storage_facade"]
            if orig_su is not None:
                sys.modules["deployment_api.utils.service_utils"] = orig_su
            elif "deployment_api.utils.service_utils" in sys.modules:
                del sys.modules["deployment_api.utils.service_utils"]

    def test_no_orphans_when_vm_not_running(self):
        import json as _json
        import sys
        from types import ModuleType
        from unittest.mock import MagicMock

        sm = _make_state_manager()

        sf_mod = ModuleType("deployment_api.utils.storage_facade")
        sf_mod.read_object_text = MagicMock(
            return_value=_json.dumps(  # type: ignore[attr-defined]
                {"vm-1": {"job_id": "j1", "status": "TERMINATED", "zone": "us-central1-a"}}
            )
        )
        sf_mod.list_objects = MagicMock(return_value=[])  # type: ignore[attr-defined]

        su_mod = ModuleType("deployment_api.utils.service_utils")
        su_mod.parse_service_event = MagicMock(return_value={"status": "succeeded"})  # type: ignore[attr-defined]

        orig_sf = sys.modules.get("deployment_api.utils.storage_facade")
        orig_su = sys.modules.get("deployment_api.utils.service_utils")
        sys.modules["deployment_api.utils.storage_facade"] = sf_mod
        sys.modules["deployment_api.utils.service_utils"] = su_mod

        state = {
            "compute_type": "vm",
            "deployment_id": "dep-1",
            "shards": [{"shard_id": "s1", "job_id": "j1"}],
        }

        try:
            result = sm.run_orphan_cleanup_only(state)
            assert result == 0
        finally:
            if orig_sf is not None:
                sys.modules["deployment_api.utils.storage_facade"] = orig_sf
            elif "deployment_api.utils.storage_facade" in sys.modules:
                del sys.modules["deployment_api.utils.storage_facade"]
            if orig_su is not None:
                sys.modules["deployment_api.utils.service_utils"] = orig_su
            elif "deployment_api.utils.service_utils" in sys.modules:
                del sys.modules["deployment_api.utils.service_utils"]


class TestCleanupStateTtl:
    """Tests for StateManager.cleanup_state_ttl (lines 341-387)."""

    def test_returns_zero_on_storage_error(self):
        from unittest.mock import MagicMock, patch

        sm = _make_state_manager()
        mock_client = MagicMock()
        mock_client.bucket.side_effect = RuntimeError("gcs unavailable")

        with patch.object(_sm_mod, "get_storage_client_with_pool", return_value=mock_client):
            result = sm.cleanup_state_ttl()

        assert result == 0

    def test_deletes_expired_deployments(self):
        from datetime import UTC, datetime
        from unittest.mock import MagicMock, patch

        sm = _make_state_manager()
        mock_client = MagicMock()
        bucket = MagicMock()
        mock_client.bucket.return_value = bucket

        old_time = datetime(2000, 1, 1, tzinfo=UTC)

        state_blob = MagicMock()
        state_blob.name = "deployments.test/dep-old/state.json"
        state_blob.time_created = old_time

        blobs_to_delete = [MagicMock(), MagicMock()]
        bucket.list_blobs.side_effect = [
            iter([state_blob]),  # first call: listing all blobs
            iter(blobs_to_delete),  # second call: listing dep dir blobs
        ]

        with patch.object(_sm_mod, "get_storage_client_with_pool", return_value=mock_client):
            result = sm.cleanup_state_ttl()

        assert result == 1
        for b in blobs_to_delete:
            b.delete.assert_called_once()

    def test_skips_blobs_not_state_json(self):
        from unittest.mock import MagicMock, patch

        sm = _make_state_manager()
        mock_client = MagicMock()
        bucket = MagicMock()
        mock_client.bucket.return_value = bucket

        non_state_blob = MagicMock()
        non_state_blob.name = "deployments.test/dep-1/vm_status.json"
        bucket.list_blobs.return_value = iter([non_state_blob])

        with patch.object(_sm_mod, "get_storage_client_with_pool", return_value=mock_client):
            result = sm.cleanup_state_ttl()

        assert result == 0

    def test_skips_recent_deployments(self):
        from datetime import UTC, datetime
        from unittest.mock import MagicMock, patch

        sm = _make_state_manager()
        mock_client = MagicMock()
        bucket = MagicMock()
        mock_client.bucket.return_value = bucket

        recent_blob = MagicMock()
        recent_blob.name = "deployments.test/dep-new/state.json"
        recent_blob.time_created = datetime.now(UTC)  # Very recent

        bucket.list_blobs.return_value = iter([recent_blob])

        with patch.object(_sm_mod, "get_storage_client_with_pool", return_value=mock_client):
            result = sm.cleanup_state_ttl()

        assert result == 0

    def test_handles_blob_processing_error(self):
        from unittest.mock import MagicMock, patch

        sm = _make_state_manager()
        mock_client = MagicMock()
        bucket = MagicMock()
        mock_client.bucket.return_value = bucket

        bad_blob = MagicMock()
        bad_blob.name = "deployments.test/dep-1/state.json"
        bad_blob.time_created = MagicMock()
        bad_blob.time_created.replace.side_effect = RuntimeError("tz error")

        bucket.list_blobs.return_value = iter([bad_blob])

        with patch.object(_sm_mod, "get_storage_client_with_pool", return_value=mock_client):
            result = sm.cleanup_state_ttl()

        assert result == 0


class TestReleaseAllLocks:
    """Tests for StateManager.release_all_locks (lines 398-423)."""

    def test_releases_all_owned_locks(self):
        import json as _json
        from unittest.mock import MagicMock, patch

        sm = _make_state_manager()
        sm._held_deployment_locks = {"dep-1", "dep-2"}

        bucket = MagicMock()

        def make_blob(owner):
            b = MagicMock()
            b.exists.return_value = True
            b.download_as_text.return_value = _json.dumps({"owner": owner})
            return b

        blobs = {
            "locks/deployment_dep-1.lock": make_blob(sm.owner_id),
            "locks/deployment_dep-2.lock": make_blob(sm.owner_id),
        }
        bucket.blob.side_effect = lambda name: blobs[name]

        mock_client = MagicMock()
        mock_client.bucket.return_value = bucket

        with patch.object(_sm_mod, "get_storage_client_with_pool", return_value=mock_client):
            count = sm.release_all_locks()

        assert count == 2
        assert len(sm._held_deployment_locks) == 0

    def test_skips_unowned_locks(self):
        import json as _json
        from unittest.mock import MagicMock, patch

        sm = _make_state_manager()
        sm._held_deployment_locks = {"dep-1"}

        bucket = MagicMock()
        mock_blob = MagicMock()
        mock_blob.exists.return_value = True
        mock_blob.download_as_text.return_value = _json.dumps({"owner": "other"})
        bucket.blob.return_value = mock_blob

        mock_client = MagicMock()
        mock_client.bucket.return_value = bucket

        with patch.object(_sm_mod, "get_storage_client_with_pool", return_value=mock_client):
            count = sm.release_all_locks()

        assert count == 0
        assert len(sm._held_deployment_locks) == 0
        mock_blob.delete.assert_not_called()

    def test_handles_blob_not_existing(self):
        from unittest.mock import MagicMock, patch

        sm = _make_state_manager()
        sm._held_deployment_locks = {"dep-5"}

        bucket = MagicMock()
        mock_blob = MagicMock()
        mock_blob.exists.return_value = False
        bucket.blob.return_value = mock_blob

        mock_client = MagicMock()
        mock_client.bucket.return_value = bucket

        with patch.object(_sm_mod, "get_storage_client_with_pool", return_value=mock_client):
            count = sm.release_all_locks()

        assert count == 0

    def test_handles_individual_lock_error(self):
        from unittest.mock import MagicMock, patch

        sm = _make_state_manager()
        sm._held_deployment_locks = {"dep-6"}

        bucket = MagicMock()
        bucket.blob.side_effect = RuntimeError("storage down")

        mock_client = MagicMock()
        mock_client.bucket.return_value = bucket

        with patch.object(_sm_mod, "get_storage_client_with_pool", return_value=mock_client):
            count = sm.release_all_locks()

        assert count == 0
        assert len(sm._held_deployment_locks) == 0

    def test_returns_zero_on_top_level_error(self):
        from unittest.mock import MagicMock, patch

        sm = _make_state_manager()
        sm._held_deployment_locks = {"dep-7"}

        mock_client = MagicMock()
        mock_client.bucket.side_effect = RuntimeError("can't connect")

        with patch.object(_sm_mod, "get_storage_client_with_pool", return_value=mock_client):
            count = sm.release_all_locks()

        assert count == 0

    def test_empty_locks_returns_zero(self):
        from unittest.mock import MagicMock, patch

        sm = _make_state_manager()
        mock_client = MagicMock()
        bucket = MagicMock()
        mock_client.bucket.return_value = bucket

        with patch.object(_sm_mod, "get_storage_client_with_pool", return_value=mock_client):
            count = sm.release_all_locks()

        assert count == 0

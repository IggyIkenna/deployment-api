"""
Unit tests for workers/deployment_processor module.

Tests process_deployments_batch, _process_vm_health_and_status,
_process_cloud_run_status, _process_stuck_shards, _launch_pending_shards,
and _handle_orphan_vm_cleanup.
"""

import json
import sys
from datetime import UTC, datetime, timedelta
from unittest.mock import MagicMock, patch

import pytest

# Mock modules that deployment_processor imports at module level
sys.modules.setdefault("deployment", MagicMock())
sys.modules.setdefault("deployment.state", MagicMock(DeploymentStatus=MagicMock(), StateManager=MagicMock()))
sys.modules.setdefault(
    "deployment_service.config.config_validator",
    MagicMock(
        ConfigurationError=Exception,
        ValidationUtils=MagicMock(get_required=MagicMock(return_value="value")),
    ),
)
# Mock backends module (lazy imported inside _process_cloud_run_status)
sys.modules.setdefault("backends", MagicMock())
sys.modules.setdefault("backends.base", MagicMock(JobStatus=MagicMock(SUCCEEDED="SUCCEEDED", FAILED="FAILED", RUNNING="RUNNING")))
sys.modules.setdefault("backends.cloud_run", MagicMock())

from deployment_api.workers.deployment_processor import (  # noqa: E402
    _handle_orphan_vm_cleanup,
    _launch_pending_shards,
    _process_stuck_shards,
    _process_vm_health_and_status,
    process_deployments_batch,
)


def _make_shard(shard_id="s1", job_id="vm-job-1", status="running", start_time=None):
    start_time = start_time or datetime.now(UTC).isoformat()
    return {
        "shard_id": shard_id,
        "job_id": job_id,
        "status": status,
        "start_time": start_time,
    }


class TestProcessDeploymentsBatch:
    """Tests for process_deployments_batch function."""

    def test_empty_active_states_returns_zero(self):
        result = process_deployments_batch(
            active_states=[],
            bucket=MagicMock(),
            now=datetime.now(UTC),
            quota_broker=None,
            try_acquire_deployment_lock=lambda dep: True,
            release_deployment_lock=lambda dep: True,
            run_orphan_cleanup_only=lambda path, state: 0,
        )
        assert result == (0, 0)

    def test_returns_tuple_of_synced_and_active(self):
        state = {
            "status": "running",
            "compute_type": "cloud_run",
            "service": "my-service",
            "shards": [],
            "config": {"region": "us-central1", "job_name": "my-job"},
        }

        mock_facade = MagicMock()
        mock_facade.list_objects = MagicMock(return_value=[])
        mock_facade.read_object_text = MagicMock(return_value="")
        mock_facade.write_object_text = MagicMock()

        with patch.dict(sys.modules, {"deployment_api.utils.storage_facade": mock_facade}):
            synced, num_active = process_deployments_batch(
                active_states=[("deployments.development/dep-1/state.json", state)],
                bucket=MagicMock(),
                now=datetime.now(UTC),
                quota_broker=None,
                try_acquire_deployment_lock=lambda dep: True,
                release_deployment_lock=lambda dep: True,
                run_orphan_cleanup_only=lambda path, state: 0,
            )
        assert isinstance(synced, int)
        assert isinstance(num_active, int)
        assert num_active == 1

    def test_skips_when_lock_not_acquired(self):
        state = {
            "status": "running",
            "compute_type": "vm",
            "shards": [],
            "config": {},
        }

        mock_facade = MagicMock()
        mock_facade.list_objects = MagicMock(return_value=[])
        mock_facade.read_object_text = MagicMock(return_value="")
        mock_facade.write_object_text = MagicMock()

        with patch.dict(sys.modules, {"deployment_api.utils.storage_facade": mock_facade}):
            synced, num_active = process_deployments_batch(
                active_states=[("deployments.development/dep-1/state.json", state)],
                bucket=MagicMock(),
                now=datetime.now(UTC),
                quota_broker=None,
                try_acquire_deployment_lock=lambda dep: False,  # Always fail to acquire
                release_deployment_lock=lambda dep: True,
                run_orphan_cleanup_only=lambda path, state: 0,
            )
        assert synced == 0

    def test_completed_pending_delete_non_vm_transitions_to_completed(self):
        state = {
            "status": "completed_pending_delete",
            "compute_type": "cloud_run",
            "shards": [],
            "config": {},
            "service": "my-service",
        }

        written_state = {}

        def mock_write(bucket, path, text):
            written_state.update(json.loads(text))

        mock_facade = MagicMock()
        mock_facade.list_objects = MagicMock(return_value=[])
        mock_facade.read_object_text = MagicMock(return_value="")
        mock_facade.write_object_text = MagicMock(side_effect=mock_write)

        with patch.dict(sys.modules, {"deployment_api.utils.storage_facade": mock_facade}):
            synced, _ = process_deployments_batch(
                active_states=[("deployments.development/dep-1/state.json", state)],
                bucket=MagicMock(),
                now=datetime.now(UTC),
                quota_broker=None,
                try_acquire_deployment_lock=lambda dep: True,
                release_deployment_lock=lambda dep: True,
                run_orphan_cleanup_only=lambda path, state: 0,
            )

        assert synced == 1
        assert written_state.get("status") == "completed"


class TestProcessVmHealthAndStatus:
    """Tests for _process_vm_health_and_status function."""

    def test_returns_updated_when_no_changes(self):
        shards = [_make_shard()]
        updated = _process_vm_health_and_status(
            shards=shards,
            vm_map={},
            now=datetime.now(UTC),
            config={},
            deployment_id="dep-1",
            shard_statuses={},
            updated=False,
        )
        assert isinstance(updated, bool)

    def test_vm_not_in_map_marks_as_failed(self):
        shards = [_make_shard(job_id="vm-missing")]
        shard_statuses = {}

        _process_vm_health_and_status(
            shards=shards,
            vm_map={},  # VM not present
            now=datetime.now(UTC),
            config={},
            deployment_id="dep-1",
            shard_statuses=shard_statuses,
            updated=False,
        )

        # VM not in map -> shard should be marked failed
        assert shard_statuses.get("s1") == ("failed", "vm_terminated_no_status")

    def test_vm_in_map_marks_as_running(self):
        shards = [_make_shard(job_id="vm-alive")]
        shard_statuses = {}
        vm_map = {"vm-alive": {"status": "RUNNING", "zone": "us-central1-a"}}

        _process_vm_health_and_status(
            shards=shards,
            vm_map=vm_map,
            now=datetime.now(UTC),
            config={},
            deployment_id="dep-1",
            shard_statuses=shard_statuses,
            updated=False,
        )

        assert shard_statuses.get("s1") == ("running", "vm_alive")

    def test_already_resolved_via_gcs_skipped(self):
        shards = [_make_shard(job_id="vm-finished")]
        shard_statuses = {"s1": ("succeeded", "gcs")}  # Already resolved
        vm_map = {"vm-finished": {"status": "RUNNING", "zone": "us-central1-a"}}

        _process_vm_health_and_status(
            shards=shards,
            vm_map=vm_map,
            now=datetime.now(UTC),
            config={},
            deployment_id="dep-1",
            shard_statuses=shard_statuses,
            updated=False,
        )

        # Should remain as resolved from GCS
        assert shard_statuses["s1"] == ("succeeded", "gcs")

    def test_non_running_shard_skipped(self):
        shards = [_make_shard(status="succeeded")]
        shard_statuses = {}

        _process_vm_health_and_status(
            shards=shards,
            vm_map={},
            now=datetime.now(UTC),
            config={},
            deployment_id="dep-1",
            shard_statuses=shard_statuses,
            updated=False,
        )

        # Non-running shard should not be in shard_statuses
        assert "s1" not in shard_statuses


class TestProcessStuckShards:
    """Tests for _process_stuck_shards function."""

    def test_vm_shard_exceeds_timeout_marked_failed(self):
        # Start time in the past (exceeded timeout + grace)
        old_start = (datetime.now(UTC) - timedelta(seconds=7200)).isoformat()
        shards = [_make_shard(status="running", start_time=old_start, job_id=None)]
        config = {"compute_config": {"timeout_seconds": 3600}}

        with patch("deployment_api.workers.deployment_processor.settings") as mock_settings:
            mock_settings.STUCK_SHARD_GRACE_SECONDS = 300

            updated = _process_stuck_shards(
                shards=shards,
                config=config,
                compute_type="vm",
                now=datetime.now(UTC),
                deployment_id="dep-1",
                updated=False,
            )

        assert updated is True
        assert shards[0]["status"] == "failed"
        assert "timeout" in shards[0].get("failure_category", "")

    def test_recent_shard_not_marked_failed(self):
        recent_start = datetime.now(UTC).isoformat()
        shards = [_make_shard(status="running", start_time=recent_start)]
        config = {"compute_config": {"timeout_seconds": 3600}}

        with patch("deployment_api.workers.deployment_processor.settings") as mock_settings:
            mock_settings.STUCK_SHARD_GRACE_SECONDS = 300

            updated = _process_stuck_shards(
                shards=shards,
                config=config,
                compute_type="vm",
                now=datetime.now(UTC),
                deployment_id="dep-1",
                updated=False,
            )

        assert updated is False
        assert shards[0]["status"] == "running"

    def test_no_timeout_configured_skips_check(self):
        old_start = (datetime.now(UTC) - timedelta(seconds=7200)).isoformat()
        shards = [_make_shard(status="running", start_time=old_start)]
        config = {"compute_config": {"timeout_seconds": 0}}

        with patch("deployment_api.workers.deployment_processor.settings") as mock_settings:
            mock_settings.STUCK_SHARD_GRACE_SECONDS = 300

            updated = _process_stuck_shards(
                shards=shards,
                config=config,
                compute_type="vm",
                now=datetime.now(UTC),
                deployment_id="dep-1",
                updated=False,
            )

        assert updated is False

    def test_cloud_run_type_skips_vm_check(self):
        old_start = (datetime.now(UTC) - timedelta(seconds=7200)).isoformat()
        shards = [_make_shard(status="running", start_time=old_start)]
        config = {"compute_config": {"timeout_seconds": 3600}}

        with patch("deployment_api.workers.deployment_processor.settings") as mock_settings:
            mock_settings.STUCK_SHARD_GRACE_SECONDS = 300

            updated = _process_stuck_shards(
                shards=shards,
                config=config,
                compute_type="cloud_run",  # Not VM
                now=datetime.now(UTC),
                deployment_id="dep-1",
                updated=False,
            )

        assert updated is False


class TestLaunchPendingShards:
    """Tests for _launch_pending_shards function."""

    def test_returns_zero_launched(self):
        state = {"status": "running", "shards": []}
        result = _launch_pending_shards(
            state=state,
            config={},
            now=datetime.now(UTC),
            deployment_id="dep-1",
            compute_type="vm",
            quota_broker=None,
            updated=False,
        )
        assert result == 0


class TestHandleOrphanVmCleanup:
    """Tests for _handle_orphan_vm_cleanup function."""

    def test_no_orphans_does_nothing(self):
        shards = [_make_shard(status="running")]
        shard_statuses = {"s1": ("running", "vm_alive")}

        with patch("deployment_api.workers.deployment_processor.settings") as mock_settings:
            mock_settings.ORPHAN_DELETE_MAX_PARALLEL = 10
            mock_settings.ORPHAN_DELETE_RETRY_SECONDS = 120

            # No exception should be raised
            _handle_orphan_vm_cleanup(
                vm_map={"vm-job-1": {"status": "RUNNING", "zone": "us-central1-a"}},
                shards=shards,
                shard_statuses=shard_statuses,
                config={"service_account_email": "sa@proj.iam", "job_name": "my-job"},
                deployment_id="dep-1",
            )

    def test_cleans_pending_vm_deletes_not_in_map(self):
        from deployment_api.workers.deployment_processor import _pending_vm_deletes

        # Add a fake pending delete for a VM that no longer exists
        _pending_vm_deletes["vm-already-deleted"] = (0, "us-central1-a")

        with patch("deployment_api.workers.deployment_processor.settings") as mock_settings:
            mock_settings.ORPHAN_DELETE_MAX_PARALLEL = 10
            mock_settings.ORPHAN_DELETE_RETRY_SECONDS = 120

            _handle_orphan_vm_cleanup(
                vm_map={},  # vm-already-deleted is NOT in vm_map
                shards=[],
                shard_statuses={},
                config={"service_account_email": "sa@proj.iam", "job_name": "my-job"},
                deployment_id="dep-1",
            )

        # VM should have been cleaned from pending deletes
        assert "vm-already-deleted" not in _pending_vm_deletes

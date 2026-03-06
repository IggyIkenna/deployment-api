"""
Unit tests for event_processor module.

Tests cover pure methods:
- get_vm_status
- get_vm_zone
- check_all_shards_terminal
- update_deployment_status
"""

import importlib.util
import os
from datetime import UTC, datetime

# Load directly to avoid circular import via services/__init__.py
_path = os.path.join(os.path.dirname(__file__), "../../deployment_api/services/event_processor.py")
_spec = importlib.util.spec_from_file_location("_ep_standalone", os.path.abspath(_path))
assert _spec is not None and _spec.loader is not None
_ep_mod = importlib.util.module_from_spec(_spec)
_spec.loader.exec_module(_ep_mod)  # type: ignore[union-attr]
DeploymentEventProcessor = _ep_mod.EventProcessor


class TestGetVmStatus:
    """Tests for DeploymentEventProcessor.get_vm_status."""

    def setup_method(self):
        self.processor = DeploymentEventProcessor(
            project_id="test-proj",
            state_bucket="test-bucket",
            deployment_env="test",
        )

    def test_finds_matching_job_id(self):
        vm_map = {
            "vm-1": {"job_id": "job-001", "status": "running", "zone": "us-central1-a"},
            "vm-2": {"job_id": "job-002", "status": "terminated"},
        }
        assert self.processor.get_vm_status(vm_map, "job-001") == "running"
        assert self.processor.get_vm_status(vm_map, "job-002") == "terminated"

    def test_returns_none_when_not_found(self):
        vm_map = {"vm-1": {"job_id": "job-001", "status": "running"}}
        assert self.processor.get_vm_status(vm_map, "job-999") is None

    def test_empty_vm_map(self):
        assert self.processor.get_vm_status({}, "job-001") is None

    def test_skips_non_dict_entries(self):
        vm_map = {"vm-1": "not-a-dict", "vm-2": {"job_id": "job-002", "status": "done"}}
        assert self.processor.get_vm_status(vm_map, "job-002") == "done"


class TestGetVmZone:
    """Tests for DeploymentEventProcessor.get_vm_zone."""

    def setup_method(self):
        self.processor = DeploymentEventProcessor(
            project_id="test-proj",
            state_bucket="test-bucket",
            deployment_env="test",
        )

    def test_finds_zone_for_job_id(self):
        vm_map = {
            "vm-1": {"job_id": "job-001", "zone": "us-central1-a"},
        }
        assert self.processor.get_vm_zone(vm_map, "job-001") == "us-central1-a"

    def test_returns_none_when_not_found(self):
        vm_map = {"vm-1": {"job_id": "job-001", "zone": "us-central1-a"}}
        assert self.processor.get_vm_zone(vm_map, "job-999") is None


class TestCheckAllShardsTerminal:
    """Tests for DeploymentEventProcessor.check_all_shards_terminal."""

    def setup_method(self):
        self.processor = DeploymentEventProcessor(
            project_id="test-proj",
            state_bucket="test-bucket",
            deployment_env="test",
        )

    def test_all_succeeded_terminal_no_failures(self):
        shards = [{"status": "succeeded"}, {"status": "succeeded"}]
        all_terminal, has_failures = self.processor.check_all_shards_terminal(shards)
        assert all_terminal is True
        assert has_failures is False

    def test_some_failed_terminal_has_failures(self):
        shards = [{"status": "succeeded"}, {"status": "failed"}]
        all_terminal, has_failures = self.processor.check_all_shards_terminal(shards)
        assert all_terminal is True
        assert has_failures is True

    def test_running_shard_not_terminal(self):
        shards = [{"status": "succeeded"}, {"status": "running"}]
        all_terminal, has_failures = self.processor.check_all_shards_terminal(shards)
        assert all_terminal is False

    def test_all_cancelled_terminal(self):
        shards = [{"status": "cancelled"}, {"status": "cancelled"}]
        all_terminal, has_failures = self.processor.check_all_shards_terminal(shards)
        assert all_terminal is True
        assert has_failures is False

    def test_empty_shards_all_terminal(self):
        all_terminal, has_failures = self.processor.check_all_shards_terminal([])
        assert all_terminal is True
        assert has_failures is False

    def test_pending_not_terminal(self):
        shards = [{"status": "pending"}]
        all_terminal, has_failures = self.processor.check_all_shards_terminal(shards)
        assert all_terminal is False


class TestUpdateDeploymentStatus:
    """Tests for DeploymentEventProcessor.update_deployment_status."""

    def setup_method(self):
        self.processor = DeploymentEventProcessor(
            project_id="test-proj",
            state_bucket="test-bucket",
            deployment_env="test",
        )
        self.now = datetime.now(UTC)

    def test_no_update_when_not_all_terminal(self):
        state: dict = {"status": "running", "compute_type": "cloud_run"}
        updated = self.processor.update_deployment_status(state, all_terminal=False, has_failures=False, now=self.now)
        assert updated is False
        assert state["status"] == "running"

    def test_cloud_run_all_succeeded(self):
        state: dict = {"status": "running", "compute_type": "cloud_run"}
        updated = self.processor.update_deployment_status(state, all_terminal=True, has_failures=False, now=self.now)
        assert updated is True
        assert state["status"] == "completed"

    def test_vm_all_succeeded_becomes_completed_pending_delete(self):
        state: dict = {"status": "running", "compute_type": "vm"}
        updated = self.processor.update_deployment_status(state, all_terminal=True, has_failures=False, now=self.now)
        assert updated is True
        assert state["status"] == "completed_pending_delete"

    def test_has_failures_becomes_failed(self):
        state: dict = {"status": "running", "compute_type": "cloud_run"}
        updated = self.processor.update_deployment_status(state, all_terminal=True, has_failures=True, now=self.now)
        assert updated is True
        assert state["status"] == "failed"

    def test_no_change_when_status_already_set(self):
        state: dict = {"status": "completed", "compute_type": "cloud_run"}
        updated = self.processor.update_deployment_status(state, all_terminal=True, has_failures=False, now=self.now)
        assert updated is False

    def test_completed_at_set_on_update(self):
        state: dict = {"status": "running", "compute_type": "cloud_run"}
        self.processor.update_deployment_status(state, all_terminal=True, has_failures=False, now=self.now)
        assert "completed_at" in state
        assert "updated_at" in state

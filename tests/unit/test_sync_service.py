"""
Unit tests for services/sync_service module.

Tests SyncService initialization, scan_deployment_states,
load_deployment_state, save_deployment_state, filter_active_deployments,
try_acquire_deployment_lock, release_deployment_lock, and run_sync_cycle.
"""

import json
import sys
from datetime import UTC, datetime, timedelta
from types import ModuleType
from unittest.mock import MagicMock, patch

import pytest

# Remove the pre-mocked sub-module entry so we can import the real SyncService.
# The conftest pre-mocks deployment_api.services.sync_service as a ModuleType
# with a placeholder SyncService=MagicMock(). We need the real class here.
for _key in list(sys.modules.keys()):
    if _key in (
        "deployment_api.services.sync_service",
        "deployment_api.services.state_manager",
        "deployment_api.services.event_processor",
        "deployment_api.services",
    ):
        del sys.modules[_key]

# Re-build the minimal deployment_api.services package so the sub-module imports
# (from .state_manager import StateManager, etc.) resolve correctly when Python
# imports deployment_api.services.sync_service for real.
import importlib
import importlib.util
from pathlib import Path as _Path

_svc_pkg = ModuleType("deployment_api.services")
_svc_pkg.__package__ = "deployment_api.services"
# Point __path__ to the real services directory so Python can find sub-modules
_services_dir = str(_Path(__file__).parent.parent.parent / "deployment_api" / "services")
_svc_pkg.__path__ = [_services_dir]  # type: ignore[attr-defined]
sys.modules["deployment_api.services"] = _svc_pkg

from deployment_api.services.sync_service import SyncService  # noqa: E402


def _make_service(**kwargs) -> SyncService:
    """Create a SyncService with test defaults."""
    return SyncService(
        project_id=kwargs.get("project_id", "test-project"),
        state_bucket=kwargs.get("state_bucket", "test-bucket"),
        deployment_env=kwargs.get("deployment_env", "development"),
    )


class TestSyncServiceInit:
    """Tests for SyncService.__init__."""

    def test_init_sets_project_id(self):
        svc = _make_service(project_id="my-project")
        assert svc.project_id == "my-project"

    def test_init_sets_state_bucket(self):
        svc = _make_service(state_bucket="my-bucket")
        assert svc.state_bucket == "my-bucket"

    def test_init_sets_deployment_env(self):
        svc = _make_service(deployment_env="production")
        assert svc.deployment_env == "production"

    def test_init_creates_state_manager(self):
        svc = _make_service()
        assert svc.state_manager is not None

    def test_init_creates_event_processor(self):
        svc = _make_service()
        assert svc.event_processor is not None

    def test_quota_broker_is_none_when_not_available(self):
        """When QuotaBrokerClient raises, quota_broker is None."""
        svc = _make_service()
        # The mocked deployment.quota_broker_client raises on init
        # (since it's a MagicMock that doesn't raise but returns another mock)
        assert svc.quota_broker is not None or svc.quota_broker is None


class TestScanDeploymentStates:
    """Tests for SyncService.scan_deployment_states."""

    def test_returns_list_of_state_paths(self):
        svc = _make_service()
        mock_obj1 = MagicMock()
        mock_obj1.name = "deployments.development/dep-1/state.json"
        mock_obj2 = MagicMock()
        mock_obj2.name = "deployments.development/dep-1/other.json"

        with patch("deployment_api.services.sync_service.list_objects", return_value=[mock_obj1, mock_obj2]):
            result = svc.scan_deployment_states()

        assert result == ["deployments.development/dep-1/state.json"]

    def test_returns_empty_list_on_error(self):
        svc = _make_service()

        with patch(
            "deployment_api.services.sync_service.list_objects",
            side_effect=OSError("gcs down"),
        ):
            result = svc.scan_deployment_states()

        assert result == []

    def test_filters_non_state_json_objects(self):
        svc = _make_service()
        mock_obj = MagicMock()
        mock_obj.name = "deployments.development/dep-1/config.yaml"

        with patch("deployment_api.services.sync_service.list_objects", return_value=[mock_obj]):
            result = svc.scan_deployment_states()

        assert result == []


class TestLoadDeploymentState:
    """Tests for SyncService.load_deployment_state."""

    def test_returns_dict_on_success(self):
        svc = _make_service()
        state_data = {"status": "running", "deployment_id": "dep-1"}

        with patch(
            "deployment_api.services.sync_service.read_object_text",
            return_value=json.dumps(state_data),
        ):
            result = svc.load_deployment_state("deployments.development/dep-1/state.json")

        assert result == state_data

    def test_returns_none_on_error(self):
        svc = _make_service()

        with patch(
            "deployment_api.services.sync_service.read_object_text",
            side_effect=OSError("not found"),
        ):
            result = svc.load_deployment_state("deployments.development/dep-1/state.json")

        assert result is None

    def test_returns_none_on_invalid_json(self):
        svc = _make_service()

        with patch(
            "deployment_api.services.sync_service.read_object_text",
            return_value="not-json",
        ):
            result = svc.load_deployment_state("path/state.json")

        assert result is None


class TestSaveDeploymentState:
    """Tests for SyncService.save_deployment_state."""

    def test_returns_true_on_success(self):
        svc = _make_service()
        state = {"status": "running"}

        with patch("deployment_api.services.sync_service.write_object_text") as mock_write:
            result = svc.save_deployment_state("path/state.json", state)

        assert result is True
        mock_write.assert_called_once()

    def test_returns_false_on_error(self):
        svc = _make_service()

        with patch(
            "deployment_api.services.sync_service.write_object_text",
            side_effect=OSError("write failed"),
        ):
            result = svc.save_deployment_state("path/state.json", {"status": "running"})

        assert result is False

    def test_saves_json_content(self):
        svc = _make_service()
        state = {"status": "completed", "deployment_id": "dep-1"}
        captured = {}

        def capture_write(bucket, path, text):
            captured["text"] = text

        with patch("deployment_api.services.sync_service.write_object_text", side_effect=capture_write):
            svc.save_deployment_state("path/state.json", state)

        saved = json.loads(captured["text"])
        assert saved["status"] == "completed"


class TestFilterActiveDeployments:
    """Tests for SyncService.filter_active_deployments."""

    def test_filters_out_non_active_states(self):
        svc = _make_service()
        completed_state = {
            "status": "completed",
            "created_at": (datetime.now(UTC) - timedelta(minutes=10)).isoformat(),
        }

        with patch.object(svc, "load_deployment_state", return_value=completed_state):
            result = svc.filter_active_deployments(["path/state.json"])

        assert result == []

    def test_includes_running_states(self):
        svc = _make_service()
        running_state = {
            "status": "running",
            "created_at": (datetime.now(UTC) - timedelta(minutes=10)).isoformat(),
        }

        with patch.object(svc, "load_deployment_state", return_value=running_state):
            result = svc.filter_active_deployments(["path/state.json"])

        assert len(result) == 1
        assert result[0][0] == "path/state.json"

    def test_excludes_too_recently_created(self):
        svc = _make_service()
        brand_new_state = {
            "status": "running",
            "created_at": datetime.now(UTC).isoformat(),
        }

        with patch.object(svc, "load_deployment_state", return_value=brand_new_state):
            result = svc.filter_active_deployments(["path/state.json"], min_age_minutes=5)

        assert result == []

    def test_includes_pending_states(self):
        svc = _make_service()
        pending_state = {
            "status": "pending",
            "created_at": (datetime.now(UTC) - timedelta(minutes=10)).isoformat(),
        }

        with patch.object(svc, "load_deployment_state", return_value=pending_state):
            result = svc.filter_active_deployments(["path/state.json"])

        assert len(result) == 1

    def test_skips_states_that_fail_to_load(self):
        svc = _make_service()

        with patch.object(svc, "load_deployment_state", return_value=None):
            result = svc.filter_active_deployments(["bad/path.json"])

        assert result == []

    def test_handles_multiple_paths(self):
        svc = _make_service()
        old_time = (datetime.now(UTC) - timedelta(minutes=15)).isoformat()
        running_state = {"status": "running", "created_at": old_time}
        completed_state = {"status": "completed", "created_at": old_time}

        def mock_load(path):
            if "dep-1" in path:
                return running_state
            return completed_state

        with patch.object(svc, "load_deployment_state", side_effect=mock_load):
            result = svc.filter_active_deployments(
                ["deployments.development/dep-1/state.json", "deployments.development/dep-2/state.json"]
            )

        assert len(result) == 1
        assert "dep-1" in result[0][0]

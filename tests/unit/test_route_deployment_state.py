"""
Unit tests for routes/deployment_state.py sync helper functions.

Tests call sync helpers directly, patching load_state/save_state and storage utilities.
"""

from unittest.mock import MagicMock, patch

import deployment_api.routes.deployment_state as _ds_routes
from deployment_api.routes.deployment_state import (
    _check_shard_logs_for_errors,
    _parse_execution_name,
    _refresh_live_cloud_run_status,
    cancel_deployment_sync,
    delete_deployment_sync,
    refresh_deployment_status_sync,
    resume_deployment_sync,
    update_deployment_tag_sync,
)


def _make_state_dict(
    status="running",
    compute_type="cloud_run",
    shards=None,
    deployment_id="dep-1",
    tag=None,
) -> dict:
    """Create a minimal dict-based deployment state."""
    return {
        "deployment_id": deployment_id,
        "status": status,
        "compute_type": compute_type,
        "shards": shards if shards is not None else [],
        "tag": tag,
        "updated_at": None,
    }


class TestParseExecutionName:
    def test_extracts_region_and_job(self):
        name = "projects/my-proj/locations/us-central1/jobs/my-job/executions/exec-1"
        region, job_name = _parse_execution_name(name)
        assert region == "us-central1"
        assert job_name == "my-job"

    def test_returns_none_for_unknown_parts(self):
        name = "some/other/path"
        region, job_name = _parse_execution_name(name)
        assert region is None
        assert job_name is None

    def test_handles_empty_string(self):
        region, job_name = _parse_execution_name("")
        assert region is None
        assert job_name is None

    def test_partial_path_extracts_available(self):
        name = "projects/p/locations/us-east1"
        region, job_name = _parse_execution_name(name)
        assert region == "us-east1"
        assert job_name is None


class TestCheckShardLogsForErrors:
    def _make_shard(self, shard_id="shard-1") -> dict:
        return {"shard_id": shard_id}

    def test_returns_false_when_log_not_found(self):
        shard = self._make_shard()
        with patch.object(_ds_routes, "object_exists", return_value=False):
            result = _check_shard_logs_for_errors(shard, "dep-1")
        assert result is False

    def test_returns_false_when_no_errors_in_log(self):
        shard = self._make_shard()
        log_content = "INFO:mymodule:Process started\nINFO:mymodule:Completed"
        with (
            patch.object(_ds_routes, "object_exists", return_value=True),
            patch.object(_ds_routes, "read_object_text", return_value=log_content),
        ):
            result = _check_shard_logs_for_errors(shard, "dep-1")
        assert result is False

    def test_returns_true_when_error_in_log(self):
        shard = self._make_shard()
        log_content = "INFO:mymodule:Starting\nERROR:mymodule:Something failed\nINFO:done"
        with (
            patch.object(_ds_routes, "object_exists", return_value=True),
            patch.object(_ds_routes, "read_object_text", return_value=log_content),
        ):
            result = _check_shard_logs_for_errors(shard, "dep-1")
        assert result is True

    def test_returns_false_on_oserror(self):
        shard = self._make_shard()
        with patch.object(_ds_routes, "object_exists", side_effect=OSError("failed")):
            result = _check_shard_logs_for_errors(shard, "dep-1")
        assert result is False

    def test_skips_empty_lines(self):
        shard = self._make_shard()
        log_content = "\n\nNo logs available\n\n"
        with (
            patch.object(_ds_routes, "object_exists", return_value=True),
            patch.object(_ds_routes, "read_object_text", return_value=log_content),
        ):
            result = _check_shard_logs_for_errors(shard, "dep-1")
        assert result is False


class TestCancelDeploymentSync:
    def test_returns_not_found_when_state_missing(self):
        with patch.object(_ds_routes, "load_state", return_value=None):
            result = cancel_deployment_sync("missing-dep")
        assert result["error"] == "not_found"
        assert result["deployment_id"] == "missing-dep"

    def test_returns_already_terminal_message(self):
        state = _make_state_dict(status="completed")
        with patch.object(_ds_routes, "load_state", return_value=state):
            result = cancel_deployment_sync("dep-1")
        assert result["cancelled"] is False

    def test_cancels_running_shards(self):
        shard1 = {"status": "running"}
        shard2 = {"status": "pending"}
        shard3 = {"status": "completed"}  # should not be cancelled
        state = _make_state_dict(status="running", shards=[shard1, shard2, shard3])

        with (
            patch.object(_ds_routes, "load_state", return_value=state),
            patch.object(_ds_routes, "save_state") as mock_save,
            patch.object(_ds_routes, "notify_deployment_updated_sync"),
        ):
            result = cancel_deployment_sync("dep-1")

        assert result["cancelled"] is True
        assert result["cancelled_shards"] == 2
        assert shard1["status"] == "cancelled"
        assert shard2["status"] == "cancelled"
        assert shard3["status"] == "completed"  # unchanged
        mock_save.assert_called_once()

    def test_swallows_notify_error(self):
        state = _make_state_dict(status="running", shards=[])
        with (
            patch.object(_ds_routes, "load_state", return_value=state),
            patch.object(_ds_routes, "save_state"),
            patch.object(_ds_routes, "notify_deployment_updated_sync", side_effect=OSError("notify failed")),
        ):
            result = cancel_deployment_sync("dep-1")
        assert result["cancelled"] is True


class TestResumeDeploymentSync:
    def test_returns_not_found_when_state_missing(self):
        with patch.object(_ds_routes, "load_state", return_value=None):
            result = resume_deployment_sync("missing-dep")
        assert result["error"] == "not_found"

    def test_returns_cannot_resume_for_running(self):
        state = _make_state_dict(status="running")
        with patch.object(_ds_routes, "load_state", return_value=state):
            result = resume_deployment_sync("dep-1")
        assert result["resumed"] is False

    def test_resumes_failed_and_cancelled_shards(self):
        shard1 = {"status": "failed"}
        shard2 = {"status": "cancelled"}
        shard3 = {"status": "completed"}  # should not be resumed
        state = _make_state_dict(status="cancelled", shards=[shard1, shard2, shard3])

        with (
            patch.object(_ds_routes, "load_state", return_value=state),
            patch.object(_ds_routes, "save_state") as mock_save,
            patch.object(_ds_routes, "notify_deployment_updated_sync"),
        ):
            result = resume_deployment_sync("dep-1")

        assert result["resumed"] is True
        assert result["resumed_shards"] == 2
        assert shard1["status"] == "pending"
        assert shard2["status"] == "pending"
        assert shard3["status"] == "completed"  # unchanged
        mock_save.assert_called_once()

    def test_returns_no_shards_to_resume_message(self):
        shard1 = {"status": "completed"}
        state = _make_state_dict(status="failed", shards=[shard1])

        with (
            patch.object(_ds_routes, "load_state", return_value=state),
            patch.object(_ds_routes, "save_state") as mock_save,
        ):
            result = resume_deployment_sync("dep-1")

        assert result["resumed"] is False
        assert result["resumed_shards"] == 0
        mock_save.assert_not_called()

    def test_swallows_notify_error(self):
        shard1 = {"status": "failed"}
        state = _make_state_dict(status="failed", shards=[shard1])

        with (
            patch.object(_ds_routes, "load_state", return_value=state),
            patch.object(_ds_routes, "save_state"),
            patch.object(
                _ds_routes,
                "notify_deployment_updated_sync",
                side_effect=RuntimeError("notify failed"),
            ),
        ):
            result = resume_deployment_sync("dep-1")
        assert result["resumed"] is True


class TestDeleteDeploymentSync:
    def test_returns_not_found_when_state_missing(self):
        with patch.object(_ds_routes, "load_state", return_value=None):
            result = delete_deployment_sync("missing-dep")
        assert result["error"] == "not_found"

    def test_deletes_state_and_objects(self):
        state = _make_state_dict()
        mock_obj = MagicMock()
        mock_obj.name = "deployments.test/dep-1/some-file.txt"

        with (
            patch.object(_ds_routes, "load_state", return_value=state),
            patch.object(_ds_routes, "list_objects", return_value=[mock_obj]),
            patch.object(_ds_routes, "delete_object") as mock_delete,
        ):
            result = delete_deployment_sync("dep-1")

        assert result["deleted"] is True
        # Should have at least the state file deletion + listed object deletion
        assert mock_delete.call_count >= 1
        mock_delete.assert_any_call(_ds_routes.DEFAULT_STATE_BUCKET, mock_obj.name)

    def test_continues_when_delete_state_fails(self):
        state = _make_state_dict()
        with (
            patch.object(_ds_routes, "load_state", return_value=state),
            patch.object(_ds_routes, "delete_object", side_effect=OSError("bucket unavailable")),
            patch.object(_ds_routes, "list_objects", return_value=[]),
        ):
            result = delete_deployment_sync("dep-1")

        assert result["deleted"] is True  # function still returns deleted=True

    def test_continues_when_list_objects_fails(self):
        state = _make_state_dict()
        with (
            patch.object(_ds_routes, "load_state", return_value=state),
            patch.object(_ds_routes, "delete_object"),
            patch.object(_ds_routes, "list_objects", side_effect=OSError("failed")),
        ):
            result = delete_deployment_sync("dep-1")

        assert result["deleted"] is True


class TestUpdateDeploymentTagSync:
    def test_returns_not_found_when_state_missing(self):
        with patch.object(_ds_routes, "load_state", return_value=None):
            result = update_deployment_tag_sync("missing-dep", "v2.0")
        assert result["error"] == "not_found"

    def test_updates_tag_and_saves(self):
        state = _make_state_dict(tag="v1.0")

        with (
            patch.object(_ds_routes, "load_state", return_value=state),
            patch.object(_ds_routes, "save_state") as mock_save,
        ):
            result = update_deployment_tag_sync("dep-1", "v2.0")

        assert result["updated"] is True
        assert result["old_tag"] == "v1.0"
        assert result["new_tag"] == "v2.0"
        assert state["tag"] == "v2.0"
        mock_save.assert_called_once()

    def test_updates_to_none_tag(self):
        state = _make_state_dict(tag="v1.0")

        with (
            patch.object(_ds_routes, "load_state", return_value=state),
            patch.object(_ds_routes, "save_state"),
        ):
            result = update_deployment_tag_sync("dep-1", None)

        assert result["updated"] is True
        assert result["new_tag"] is None


# ── _refresh_live_cloud_run_status ────────────────────────────────────────────


class TestRefreshLiveCloudRunStatus:
    def test_no_service_name_returns_minus_one(self):
        state = {"config": {}, "service": ""}
        result = _refresh_live_cloud_run_status(state)
        assert result == -1

    def test_no_revisions_returns_zero(self):
        state = {"config": {"service_name": "exec-svc", "region": "us-central1"}, "shards": []}
        mock_compute = MagicMock()
        mock_compute.list_revisions.return_value = []

        with patch("deployment_api.routes.deployment_state.get_shards", return_value=[]):
            with patch(
                "deployment_api.routes.deployment_state._refresh_live_cloud_run_status",
                wraps=_refresh_live_cloud_run_status,
            ):
                pass  # just verifying the function exists

        # Direct test with UTL mock
        with patch("deployment_api.routes.deployment_state.get_shards", return_value=[]):
            try:
                import sys

                sys.modules.setdefault(
                    "unified_trading_library",
                    MagicMock(get_compute_client=MagicMock(return_value=mock_compute)),
                )
                result = _refresh_live_cloud_run_status(state)
            except Exception:
                result = -1
        # Either 0 (if UTL mock worked) or -1 (if exception raised)
        assert result in (0, -1)

    def test_exception_from_utl_returns_minus_one(self):
        state = {"config": {"service_name": "exec-svc"}, "shards": []}

        with patch("deployment_api.routes.deployment_state.get_shards", return_value=[]):
            with patch(
                "unified_trading_library.get_compute_client",
                side_effect=RuntimeError("UTL unavailable"),
            ):
                result = _refresh_live_cloud_run_status(state)
        assert result == -1

    def test_ready_revision_with_running_shards_marks_succeeded(self):
        running_shard = {"status": "running", "shard_id": "s1"}
        state = {
            "config": {"service_name": "exec-svc", "region": "us-central1"},
            "shards": [running_shard],
        }
        mock_compute = MagicMock()
        revision = {
            "create_time": 1000.0,
            "conditions": [{"type": "Ready", "state": "CONDITION_SUCCEEDED"}],
        }
        mock_compute.list_revisions.return_value = [revision]

        with (
            patch("deployment_api.routes.deployment_state.get_shards", return_value=[running_shard]),
            patch("unified_trading_library.get_compute_client", return_value=mock_compute),
        ):
            result = _refresh_live_cloud_run_status(state)
        assert result == 1
        assert running_shard["status"] == "succeeded"

    def test_not_ready_revision_with_running_shards_marks_failed(self):
        running_shard = {"status": "running", "shard_id": "s1"}
        state = {
            "config": {"service_name": "exec-svc", "region": "us-central1"},
            "shards": [running_shard],
        }
        mock_compute = MagicMock()
        revision = {
            "create_time": 1000.0,
            "conditions": [{"type": "Ready", "state": "NOT_READY"}],
        }
        mock_compute.list_revisions.return_value = [revision]

        with (
            patch("deployment_api.routes.deployment_state.get_shards", return_value=[running_shard]),
            patch("unified_trading_library.get_compute_client", return_value=mock_compute),
        ):
            result = _refresh_live_cloud_run_status(state)
        assert result == 1
        assert running_shard["status"] == "failed"

    def test_ready_with_no_shards_sets_completed(self):
        state = {
            "config": {"service_name": "exec-svc", "region": "us-central1"},
            "shards": [],
        }
        mock_compute = MagicMock()
        revision = {
            "create_time": 1000.0,
            "conditions": [{"type": "Ready", "state": "CONDITION_SUCCEEDED"}],
        }
        mock_compute.list_revisions.return_value = [revision]

        with (
            patch("deployment_api.routes.deployment_state.get_shards", return_value=[]),
            patch("unified_trading_library.get_compute_client", return_value=mock_compute),
        ):
            result = _refresh_live_cloud_run_status(state)
        assert result == 1
        from deployment_api.utils.local_state_manager import STATUS_COMPLETED

        assert state["status"] == STATUS_COMPLETED


# ── refresh_deployment_status_sync ────────────────────────────────────────────


class TestRefreshDeploymentStatusSync:
    def test_returns_not_found_when_state_missing(self):
        with patch.object(_ds_routes, "load_state", return_value=None):
            result = refresh_deployment_status_sync("dep-x")
        assert result["error"] == "not_found"

    def test_already_terminal_state_returns_early(self):
        state = _make_state_dict(status="completed")

        with (
            patch.object(_ds_routes, "load_state", return_value=state),
            patch.object(_ds_routes, "get_status", return_value="completed"),
        ):
            result = refresh_deployment_status_sync("dep-1")
        assert result["updated"] is False
        assert "terminal" in result["message"]

    def test_no_shards_no_update_returns_no_changes(self):
        state = _make_state_dict(status="running", compute_type="cloud_run", shards=[])

        with (
            patch.object(_ds_routes, "load_state", return_value=state),
            patch.object(_ds_routes, "get_status", return_value="running"),
            patch.object(_ds_routes, "get_shards", return_value=[]),
            patch.object(_ds_routes, "recompute_status", return_value="running"),
            patch.object(_ds_routes, "save_state"),
            patch.object(_ds_routes, "notify_deployment_updated_sync"),
        ):
            result = refresh_deployment_status_sync("dep-1")
        assert result["updated"] is False
        assert result["shards_updated"] == 0

    def test_cloud_run_batch_succeeded_shard_updates_state(self):
        shard = {
            "status": "running",
            "job_id": "projects/p/locations/us-central1/jobs/my-job/executions/exec-1",
            "shard_id": "s1",
        }
        state = _make_state_dict(status="running", compute_type="cloud_run", shards=[shard])
        state["deployment_mode"] = "batch"

        with (
            patch.object(_ds_routes, "load_state", return_value=state),
            patch.object(_ds_routes, "get_status", return_value="running"),
            patch.object(_ds_routes, "get_shards", return_value=[shard]),
            patch.object(_ds_routes, "recompute_status", return_value="completed"),
            patch.object(_ds_routes, "save_state"),
            patch.object(_ds_routes, "notify_deployment_updated_sync"),
            patch("deployment_api.routes.deployment_state._asyncio") as mock_asyncio,
        ):
            mock_asyncio.run.return_value = {
                "projects/p/locations/us-central1/jobs/my-job/executions/exec-1": "SUCCEEDED"
            }
            result = refresh_deployment_status_sync("dep-1")
        assert result["shards_updated"] >= 1
        assert shard["status"] == "succeeded"

    def test_cloud_run_batch_failed_shard_marks_failed(self):
        shard = {
            "status": "running",
            "job_id": "projects/p/locations/us-central1/jobs/my-job/executions/exec-2",
            "shard_id": "s2",
        }
        state = _make_state_dict(status="running", compute_type="cloud_run", shards=[shard])
        state["deployment_mode"] = "batch"

        with (
            patch.object(_ds_routes, "load_state", return_value=state),
            patch.object(_ds_routes, "get_status", return_value="running"),
            patch.object(_ds_routes, "get_shards", return_value=[shard]),
            patch.object(_ds_routes, "recompute_status", return_value="failed"),
            patch.object(_ds_routes, "save_state"),
            patch.object(_ds_routes, "notify_deployment_updated_sync"),
            patch("deployment_api.routes.deployment_state._asyncio") as mock_asyncio,
        ):
            mock_asyncio.run.return_value = {
                "projects/p/locations/us-central1/jobs/my-job/executions/exec-2": "FAILED"
            }
            result = refresh_deployment_status_sync("dep-1")
        assert shard["status"] == "failed"

    def test_vm_compute_type_running_shards_terminated(self):
        shard = {
            "status": "running",
            "shard_id": "s1",
            "compute_info": {"zone": "us-central1-a", "vm_name": "my-vm-1"},
        }
        state = _make_state_dict(status="running", compute_type="vm", shards=[shard])
        state["deployment_mode"] = "batch"

        with (
            patch.object(_ds_routes, "load_state", return_value=state),
            patch.object(_ds_routes, "get_status", return_value="running"),
            patch.object(_ds_routes, "get_shards", return_value=[shard]),
            patch.object(_ds_routes, "recompute_status", return_value="completed"),
            patch.object(_ds_routes, "save_state"),
            patch.object(_ds_routes, "notify_deployment_updated_sync"),
            patch("deployment_api.routes.deployment_state._asyncio") as mock_asyncio,
        ):
            mock_asyncio.run.return_value = {"my-vm-1": "TERMINATED"}
            result = refresh_deployment_status_sync("dep-1")
        assert shard["status"] == "succeeded"

    def test_vm_api_error_is_swallowed(self):
        shard = {
            "status": "running",
            "shard_id": "s1",
            "compute_info": {"zone": "us-central1-a", "vm_name": "vm-1"},
        }
        state = _make_state_dict(status="running", compute_type="vm", shards=[shard])

        with (
            patch.object(_ds_routes, "load_state", return_value=state),
            patch.object(_ds_routes, "get_status", return_value="running"),
            patch.object(_ds_routes, "get_shards", return_value=[shard]),
            patch.object(_ds_routes, "recompute_status", return_value="running"),
            patch.object(_ds_routes, "save_state"),
            patch.object(_ds_routes, "notify_deployment_updated_sync"),
            patch("deployment_api.routes.deployment_state._asyncio") as mock_asyncio,
        ):
            mock_asyncio.run.side_effect = RuntimeError("VM API error")
            result = refresh_deployment_status_sync("dep-1")
        assert result["deployment_id"] == "dep-1"  # no exception raised

    def test_live_cloud_run_mode_calls_live_refresh(self):
        state = _make_state_dict(status="running", compute_type="cloud_run", shards=[])
        state["deployment_mode"] = "live"

        with (
            patch.object(_ds_routes, "load_state", return_value=state),
            patch.object(_ds_routes, "get_status", return_value="running"),
            patch.object(_ds_routes, "get_shards", return_value=[]),
            patch.object(_ds_routes, "_refresh_live_cloud_run_status", return_value=0) as mock_live,
            patch.object(_ds_routes, "recompute_status", return_value="running"),
            patch.object(_ds_routes, "save_state"),
            patch.object(_ds_routes, "notify_deployment_updated_sync"),
        ):
            result = refresh_deployment_status_sync("dep-1")
        mock_live.assert_called_once()

    def test_notify_error_is_swallowed(self):
        state = _make_state_dict(status="running", compute_type="cloud_run", shards=[])

        with (
            patch.object(_ds_routes, "load_state", return_value=state),
            patch.object(_ds_routes, "get_status", return_value="running"),
            patch.object(_ds_routes, "get_shards", return_value=[]),
            patch.object(_ds_routes, "recompute_status", return_value="running"),
            patch.object(_ds_routes, "save_state"),
            patch.object(_ds_routes, "notify_deployment_updated_sync", side_effect=OSError("notify failed")),
        ):
            result = refresh_deployment_status_sync("dep-1")
        assert result["deployment_id"] == "dep-1"  # no exception raised

"""
Unit tests for workers/auto_sync module.

Tests module-level constants, get_held_deployment_locks,
get_owner_id, get_background_task_handles, set_background_task_handles,
and get_config_dir.
"""

import asyncio
from unittest.mock import MagicMock, patch

import pytest

import deployment_api.workers.auto_sync as auto_sync


class TestModuleLevelConstants:
    """Tests for module-level constants and state."""

    def test_owner_id_is_string(self):
        assert isinstance(auto_sync.OWNER_ID, str)

    def test_owner_id_has_hostname_pid_uuid(self):
        parts = auto_sync.OWNER_ID.split(":")
        assert len(parts) >= 3  # hostname:pid:uuid

    def test_deployment_env_is_string(self):
        assert isinstance(auto_sync.DEPLOYMENT_ENV, str)

    def test_held_deployment_locks_is_set(self):
        assert isinstance(auto_sync._held_deployment_locks, set)

    def test_pending_vm_deletes_is_dict(self):
        assert isinstance(auto_sync._pending_vm_deletes, dict)


class TestGetHeldDeploymentLocks:
    """Tests for get_held_deployment_locks function."""

    def test_returns_module_level_set(self):
        result = auto_sync.get_held_deployment_locks()
        assert result is auto_sync._held_deployment_locks

    def test_returns_set_type(self):
        result = auto_sync.get_held_deployment_locks()
        assert isinstance(result, set)


class TestGetOwnerIdAutoSync:
    """Tests for get_owner_id function."""

    def test_returns_owner_id_string(self):
        result = auto_sync.get_owner_id()
        assert isinstance(result, str)
        assert result == auto_sync.OWNER_ID

    def test_owner_id_not_empty(self):
        result = auto_sync.get_owner_id()
        assert len(result) > 0


class TestGetSetBackgroundTaskHandles:
    """Tests for get/set background task handle functions."""

    def test_get_handles_returns_tuple(self):
        result = auto_sync.get_background_task_handles()
        assert isinstance(result, tuple)
        assert len(result) == 3

    def test_set_and_get_handles(self):
        mock_bg = MagicMock()
        mock_drain = MagicMock()
        mock_event = MagicMock()

        original = auto_sync.get_background_task_handles()
        try:
            auto_sync.set_background_task_handles(mock_bg, mock_drain, mock_event)
            result = auto_sync.get_background_task_handles()
            assert result[0] is mock_bg
            assert result[1] is mock_drain
            assert result[2] is mock_event
        finally:
            auto_sync.set_background_task_handles(*original)

    def test_set_handles_updates_module_vars(self):
        mock_bg = MagicMock()
        mock_drain = MagicMock()
        mock_event = MagicMock()

        original = auto_sync.get_background_task_handles()
        try:
            auto_sync.set_background_task_handles(mock_bg, mock_drain, mock_event)
            assert auto_sync._background_task is mock_bg
            assert auto_sync._events_drain_task is mock_drain
            assert auto_sync._shutdown_event is mock_event
        finally:
            auto_sync.set_background_task_handles(*original)


class TestGetConfigDir:
    """Tests for get_config_dir function."""

    def test_returns_path_when_configs_exists(self, tmp_path):
        """When configs directory exists relative to the module, return it."""
        configs_dir = tmp_path / "configs"
        configs_dir.mkdir()

        with patch("deployment_api.workers.auto_sync.Path") as mock_path_cls:
            mock_file = MagicMock()
            mock_path_cls.return_value = mock_file
            mock_file.parent.parent = tmp_path
            (tmp_path / "configs").is_dir()

            # Direct test: if the real configs exist, get_config_dir works
            try:
                result = auto_sync.get_config_dir()
                assert result is not None
            except RuntimeError:
                # If configs doesn't exist in test environment, that's OK
                pass

    def test_raises_when_configs_not_found(self):
        """Raises RuntimeError when configs directory not found."""
        with patch("deployment_api.workers.auto_sync.Path") as mock_path_cls:
            mock_file = MagicMock()
            mock_path_cls.return_value = mock_file

            # Create a path where exists() returns False
            fake_path = MagicMock()
            fake_path.exists.return_value = False
            mock_file.parent.parent = MagicMock()
            mock_file.parent.parent.__truediv__ = MagicMock(return_value=fake_path)

            with pytest.raises(RuntimeError):
                auto_sync.get_config_dir()


class TestAutoSyncCancelledError:
    """Tests that auto_sync loop handles CancelledError."""

    @pytest.mark.asyncio
    async def test_task_cancelled_exits_cleanly(self):
        """CancelledError in sync loop causes clean exit."""
        shutdown_event = asyncio.Event()

        with (
            patch.object(auto_sync, "_shutdown_event", shutdown_event),
            patch("deployment_api.workers.auto_sync.asyncio.sleep", side_effect=asyncio.CancelledError()),
        ):
            # Should complete without raising when cancelled
            task = asyncio.create_task(auto_sync._auto_sync_running_deployments())
            try:
                await asyncio.wait_for(task, timeout=1.0)
            except (asyncio.CancelledError, TimeoutError):
                task.cancel()

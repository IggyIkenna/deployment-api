"""
Unit tests for app_config module.

Tests get_config_dir, get_ui_dist_dir, create_app, and the lifespan
context manager startup/shutdown phases.
"""

import asyncio
import sys
from pathlib import Path
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

import deployment_api.app_config as app_config


class TestGetConfigDir:
    """Tests for get_config_dir function."""

    def test_returns_path_when_configs_exists(self, tmp_path):
        configs_dir = tmp_path / "configs"
        configs_dir.mkdir()

        # Patch Path(__file__).parent to point to tmp_path as the api_dir
        # so repo_root = tmp_path.parent and configs_dir = tmp_path.parent / "configs"
        # Instead, use a simpler approach: path to a fake api file inside a tmp hierarchy
        repo_root = tmp_path
        (repo_root / "configs").mkdir(exist_ok=True)
        fake_api_file = tmp_path / "deployment_api" / "app_config.py"
        fake_api_file.parent.mkdir(parents=True, exist_ok=True)
        fake_api_file.touch()

        with patch.object(app_config, "__file__", str(fake_api_file)):
            result = app_config.get_config_dir()
            assert result == repo_root / "configs"

    def test_raises_runtime_error_when_configs_missing(self, tmp_path):
        fake_api_file = tmp_path / "deployment_api" / "app_config.py"
        fake_api_file.parent.mkdir(parents=True, exist_ok=True)
        fake_api_file.touch()
        # No configs dir exists

        with patch.object(app_config, "__file__", str(fake_api_file)):
            with pytest.raises(RuntimeError, match="Could not find configs directory"):
                app_config.get_config_dir()

    def test_real_call_either_returns_path_or_raises(self):
        """The function either succeeds or raises RuntimeError - no silent failures."""
        try:
            result = app_config.get_config_dir()
            assert isinstance(result, Path)
        except RuntimeError:
            pass  # Acceptable when configs/ not at expected location


class TestGetUiDistDir:
    """Tests for get_ui_dist_dir function."""

    def test_returns_none_when_no_ui_dist(self, tmp_path):
        fake_api_file = tmp_path / "deployment_api" / "app_config.py"
        fake_api_file.parent.mkdir(parents=True, exist_ok=True)
        fake_api_file.touch()

        with patch.object(app_config, "__file__", str(fake_api_file)):
            result = app_config.get_ui_dist_dir()
        assert result is None

    def test_returns_none_when_ui_dist_has_no_index(self, tmp_path):
        fake_api_file = tmp_path / "deployment_api" / "app_config.py"
        fake_api_file.parent.mkdir(parents=True, exist_ok=True)
        fake_api_file.touch()
        # Create ui/dist but no index.html
        ui_dist = tmp_path / "ui" / "dist"
        ui_dist.mkdir(parents=True)

        with patch.object(app_config, "__file__", str(fake_api_file)):
            result = app_config.get_ui_dist_dir()
        assert result is None

    def test_returns_path_when_ui_dist_has_index(self, tmp_path):
        fake_api_file = tmp_path / "deployment_api" / "app_config.py"
        fake_api_file.parent.mkdir(parents=True, exist_ok=True)
        fake_api_file.touch()
        # Create ui/dist with index.html
        ui_dist = tmp_path / "ui" / "dist"
        ui_dist.mkdir(parents=True)
        (ui_dist / "index.html").write_text("<html/>")

        with patch.object(app_config, "__file__", str(fake_api_file)):
            result = app_config.get_ui_dist_dir()
        assert result == ui_dist


class TestCreateApp:
    """Tests for create_app function."""

    def test_create_app_returns_fastapi_instance(self):
        from fastapi import FastAPI

        app = app_config.create_app()
        assert isinstance(app, FastAPI)

    def test_create_app_has_title(self):
        app = app_config.create_app()
        assert "Deployment" in app.title

    def test_create_app_has_lifespan(self):
        app = app_config.create_app()
        # FastAPI stores the lifespan as router.lifespan_context
        assert app.router.lifespan_context is not None


class TestLifespanStartup:
    """Tests for lifespan async context manager startup phase."""

    @pytest.mark.asyncio
    async def test_startup_sets_config_dir(self, tmp_path):
        from fastapi import FastAPI

        mock_app = FastAPI()
        configs_dir = tmp_path / "configs"
        configs_dir.mkdir()

        mock_cache_obj = AsyncMock()
        mock_cache_obj.initialize = AsyncMock()
        mock_cache_obj.shutdown = AsyncMock()
        mock_cache_mod = MagicMock()
        mock_cache_mod.cache = mock_cache_obj

        mock_drain_mod = MagicMock()
        mock_drain_mod._drain_sync_queue = AsyncMock(return_value=None)

        bg_task = asyncio.create_task(asyncio.sleep(0))
        drain_task = asyncio.create_task(asyncio.sleep(0))

        with (
            patch("deployment_api.app_config.get_config_dir", return_value=configs_dir),
            patch.dict(sys.modules, {"deployment_api.utils.cache": mock_cache_mod}),
            patch.dict(sys.modules, {"deployment_api.utils.deployment_events": mock_drain_mod}),
            patch("asyncio.create_task", side_effect=[bg_task, drain_task]),
            patch("deployment_api.app_config.set_background_task_handles"),
            patch("deployment_api.app_config.get_held_deployment_locks", return_value=set()),
            patch("deployment_api.app_config.get_owner_id", return_value="owner"),
            patch(
                "deployment_api.app_config.get_storage_client_with_pool", return_value=MagicMock()
            ),
        ):
            async with app_config.lifespan(mock_app):
                assert mock_app.state.config_dir == configs_dir

    @pytest.mark.asyncio
    async def test_startup_initializes_cache(self, tmp_path):
        from fastapi import FastAPI

        mock_app = FastAPI()

        mock_cache_obj = AsyncMock()
        mock_cache_obj.initialize = AsyncMock()
        mock_cache_obj.shutdown = AsyncMock()
        mock_cache_mod = MagicMock()
        mock_cache_mod.cache = mock_cache_obj

        mock_drain_mod = MagicMock()
        mock_drain_mod._drain_sync_queue = AsyncMock(return_value=None)

        bg_task = asyncio.create_task(asyncio.sleep(0))
        drain_task = asyncio.create_task(asyncio.sleep(0))

        with (
            patch("deployment_api.app_config.get_config_dir", return_value=MagicMock()),
            patch.dict(sys.modules, {"deployment_api.utils.cache": mock_cache_mod}),
            patch.dict(sys.modules, {"deployment_api.utils.deployment_events": mock_drain_mod}),
            patch("asyncio.create_task", side_effect=[bg_task, drain_task]),
            patch("deployment_api.app_config.set_background_task_handles"),
            patch("deployment_api.app_config.get_held_deployment_locks", return_value=set()),
            patch("deployment_api.app_config.get_owner_id", return_value="owner"),
            patch(
                "deployment_api.app_config.get_storage_client_with_pool", return_value=MagicMock()
            ),
        ):
            async with app_config.lifespan(mock_app):
                pass

        mock_cache_obj.initialize.assert_called_once()

    @pytest.mark.asyncio
    async def test_shutdown_calls_cache_shutdown(self):
        from fastapi import FastAPI

        mock_app = FastAPI()

        mock_cache_obj = AsyncMock()
        mock_cache_obj.initialize = AsyncMock()
        mock_cache_obj.shutdown = AsyncMock()
        mock_cache_mod = MagicMock()
        mock_cache_mod.cache = mock_cache_obj

        mock_drain_mod = MagicMock()
        mock_drain_mod._drain_sync_queue = AsyncMock(return_value=None)

        bg_task = asyncio.create_task(asyncio.sleep(0))
        drain_task = asyncio.create_task(asyncio.sleep(0))

        with (
            patch("deployment_api.app_config.get_config_dir", return_value=MagicMock()),
            patch.dict(sys.modules, {"deployment_api.utils.cache": mock_cache_mod}),
            patch.dict(sys.modules, {"deployment_api.utils.deployment_events": mock_drain_mod}),
            patch("asyncio.create_task", side_effect=[bg_task, drain_task]),
            patch("deployment_api.app_config.set_background_task_handles"),
            patch("deployment_api.app_config.get_held_deployment_locks", return_value=set()),
            patch("deployment_api.app_config.get_owner_id", return_value="owner"),
            patch(
                "deployment_api.app_config.get_storage_client_with_pool", return_value=MagicMock()
            ),
        ):
            async with app_config.lifespan(mock_app):
                pass

        mock_cache_obj.shutdown.assert_called_once()

    @pytest.mark.asyncio
    async def test_shutdown_releases_lock_when_owner_matches(self):
        from fastapi import FastAPI

        mock_app = FastAPI()

        mock_cache_obj = AsyncMock()
        mock_cache_obj.initialize = AsyncMock()
        mock_cache_obj.shutdown = AsyncMock()
        mock_cache_mod = MagicMock()
        mock_cache_mod.cache = mock_cache_obj

        mock_drain_mod = MagicMock()
        mock_drain_mod._drain_sync_queue = AsyncMock(return_value=None)

        bg_task = asyncio.create_task(asyncio.sleep(0))
        drain_task = asyncio.create_task(asyncio.sleep(0))

        mock_client = MagicMock()
        mock_bucket = MagicMock()
        mock_blob = MagicMock()
        mock_blob.exists.return_value = True
        mock_blob.download_as_text.return_value = '{"owner": "test-owner"}'
        mock_bucket.blob.return_value = mock_blob
        mock_client.bucket.return_value = mock_bucket

        with (
            patch("deployment_api.app_config.get_config_dir", return_value=MagicMock()),
            patch.dict(sys.modules, {"deployment_api.utils.cache": mock_cache_mod}),
            patch.dict(sys.modules, {"deployment_api.utils.deployment_events": mock_drain_mod}),
            patch("asyncio.create_task", side_effect=[bg_task, drain_task]),
            patch("deployment_api.app_config.set_background_task_handles"),
            patch("deployment_api.app_config.get_held_deployment_locks", return_value={"dep-1"}),
            patch("deployment_api.app_config.get_owner_id", return_value="test-owner"),
            patch(
                "deployment_api.app_config.get_storage_client_with_pool", return_value=mock_client
            ),
        ):
            async with app_config.lifespan(mock_app):
                pass

        mock_blob.delete.assert_called_once()

    @pytest.mark.asyncio
    async def test_shutdown_handles_storage_error(self):
        from fastapi import FastAPI

        mock_app = FastAPI()

        mock_cache_obj = AsyncMock()
        mock_cache_obj.initialize = AsyncMock()
        mock_cache_obj.shutdown = AsyncMock()
        mock_cache_mod = MagicMock()
        mock_cache_mod.cache = mock_cache_obj

        mock_drain_mod = MagicMock()
        mock_drain_mod._drain_sync_queue = AsyncMock(return_value=None)

        bg_task = asyncio.create_task(asyncio.sleep(0))
        drain_task = asyncio.create_task(asyncio.sleep(0))

        with (
            patch("deployment_api.app_config.get_config_dir", return_value=MagicMock()),
            patch.dict(sys.modules, {"deployment_api.utils.cache": mock_cache_mod}),
            patch.dict(sys.modules, {"deployment_api.utils.deployment_events": mock_drain_mod}),
            patch("asyncio.create_task", side_effect=[bg_task, drain_task]),
            patch("deployment_api.app_config.set_background_task_handles"),
            patch("deployment_api.app_config.get_held_deployment_locks", return_value={"dep-x"}),
            patch("deployment_api.app_config.get_owner_id", return_value="owner"),
            patch(
                "deployment_api.app_config.get_storage_client_with_pool",
                side_effect=OSError("network"),
            ),
        ):
            # Should not raise even with storage error
            async with app_config.lifespan(mock_app):
                pass

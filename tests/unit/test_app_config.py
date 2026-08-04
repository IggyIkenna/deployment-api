"""
Unit tests for app_config module.

Tests get_config_dir and get_ui_dist_dir.
"""

from pathlib import Path
from unittest.mock import patch

import pytest

import deployment_api.app_config as app_config


class TestGetConfigDir:
    """Tests for get_config_dir function."""

    def test_returns_path_when_pm_configs_exists(self, tmp_path):
        # pm-configs/ bundled at repo_root — simulate Docker / local dev via symlink
        repo_root = tmp_path
        (repo_root / "pm-configs").mkdir()
        fake_api_file = tmp_path / "deployment_api" / "app_config.py"
        fake_api_file.parent.mkdir(parents=True, exist_ok=True)
        fake_api_file.touch()

        with patch.object(app_config, "__file__", str(fake_api_file)):
            result = app_config.get_config_dir()
            assert result == repo_root / "pm-configs"

    def test_raises_runtime_error_when_configs_missing(self, tmp_path):
        fake_api_file = tmp_path / "deployment_api" / "app_config.py"
        fake_api_file.parent.mkdir(parents=True, exist_ok=True)
        fake_api_file.touch()
        # No pm-configs dir exists, no sibling unified-trading-pm dir either

        with patch.object(app_config, "__file__", str(fake_api_file)):
            with pytest.raises(RuntimeError, match="Could not find operational configs directory"):
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

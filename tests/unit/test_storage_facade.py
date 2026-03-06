"""
Unit tests for storage_facade module.

Tests cover ObjectInfo, _fuse_path, get_gcs_fuse_status, and storage
operations via mocked GCS client.
"""

from datetime import UTC, datetime
from pathlib import Path
from unittest.mock import MagicMock, patch

from deployment_api.utils.storage_facade import (
    GCS_FUSE_MOUNT_BASE,
    ObjectInfo,
    _fuse_path,
    _is_bucket_mounted,
    _use_gcs_fuse,
    get_gcs_fuse_status,
    list_objects,
    object_exists,
)


class TestObjectInfo:
    """Tests for ObjectInfo dataclass."""

    def test_basic_creation(self):
        obj = ObjectInfo(name="test/path/file.parquet")
        assert obj.name == "test/path/file.parquet"
        assert obj.updated is None
        assert obj.size is None

    def test_time_created_defaults_to_updated(self):
        now = datetime.now(UTC)
        obj = ObjectInfo(name="test.parquet", updated=now)
        assert obj.time_created == now

    def test_time_created_explicit(self):
        created = datetime(2024, 1, 1, tzinfo=UTC)
        updated = datetime(2024, 2, 1, tzinfo=UTC)
        obj = ObjectInfo(name="test.parquet", updated=updated, time_created=created)
        assert obj.time_created == created

    def test_size_stored(self):
        obj = ObjectInfo(name="file.parquet", size=12345)
        assert obj.size == 12345


class TestFusePath:
    """Tests for _fuse_path helper."""

    def test_bucket_only(self):
        path = _fuse_path("my-bucket")
        assert path == Path(GCS_FUSE_MOUNT_BASE) / "my-bucket"

    def test_bucket_with_object(self):
        path = _fuse_path("my-bucket", "some/path/file.txt")
        assert str(path).endswith("my-bucket/some/path/file.txt")

    def test_leading_slash_in_object_stripped(self):
        path = _fuse_path("my-bucket", "/some/path/file.txt")
        assert str(path).endswith("my-bucket/some/path/file.txt")
        # Shouldn't have double slash
        assert "//" not in str(path)


class TestUseGcsFuse:
    """Tests for _use_gcs_fuse."""

    @patch("deployment_api.utils.storage_facade.DEPLOYMENT_ENV", "production")
    def test_production_env_returns_true(self):
        assert _use_gcs_fuse() is True

    @patch("deployment_api.utils.storage_facade.DEPLOYMENT_ENV", "development")
    def test_development_env_returns_false(self):
        assert _use_gcs_fuse() is False

    @patch("deployment_api.utils.storage_facade.DEPLOYMENT_ENV", "staging")
    def test_staging_env_returns_false(self):
        assert _use_gcs_fuse() is False


class TestIsBucketMounted:
    """Tests for _is_bucket_mounted."""

    @patch("deployment_api.utils.storage_facade._fuse_path")
    def test_returns_true_when_dir_exists(self, mock_path_fn):
        mock_path = MagicMock()
        mock_path.exists.return_value = True
        mock_path.is_dir.return_value = True
        mock_path_fn.return_value = mock_path
        assert _is_bucket_mounted("my-bucket") is True

    @patch("deployment_api.utils.storage_facade._fuse_path")
    def test_returns_false_when_not_exists(self, mock_path_fn):
        mock_path = MagicMock()
        mock_path.exists.return_value = False
        mock_path_fn.return_value = mock_path
        assert _is_bucket_mounted("my-bucket") is False

    @patch("deployment_api.utils.storage_facade._fuse_path")
    def test_returns_false_when_not_dir(self, mock_path_fn):
        mock_path = MagicMock()
        mock_path.exists.return_value = True
        mock_path.is_dir.return_value = False
        mock_path_fn.return_value = mock_path
        assert _is_bucket_mounted("my-bucket") is False


class TestGetGcsFuseStatus:
    """Tests for get_gcs_fuse_status."""

    @patch("deployment_api.utils.storage_facade.DEPLOYMENT_ENV", "development")
    def test_development_returns_inactive(self):
        status = get_gcs_fuse_status()
        assert status["active"] is False
        assert status["env"] == "development"

    @patch("deployment_api.utils.storage_facade.DEPLOYMENT_ENV", "production")
    @patch("deployment_api.utils.storage_facade._is_bucket_mounted", return_value=False)
    @patch("deployment_api.utils.storage_facade.STATE_BUCKET", "my-state-bucket")
    def test_production_without_mount_inactive(self, mock_mounted):
        status = get_gcs_fuse_status()
        assert status["active"] is False
        assert "production but mount missing" in status["reason"]

    @patch("deployment_api.utils.storage_facade.DEPLOYMENT_ENV", "production")
    @patch("deployment_api.utils.storage_facade._is_bucket_mounted", return_value=True)
    @patch("deployment_api.utils.storage_facade.STATE_BUCKET", "my-state-bucket")
    def test_production_with_mount_active(self, mock_mounted):
        status = get_gcs_fuse_status()
        assert status["active"] is True
        assert "production + mount exists" in status["reason"]


class TestListObjectsViaApi:
    """Tests for list_objects using GCS API (non-FUSE path)."""

    @patch("deployment_api.utils.storage_facade._use_gcs_fuse", return_value=False)
    @patch("deployment_api.utils.storage_facade._is_bucket_mounted", return_value=False)
    @patch("deployment_api.utils.storage_client.get_storage_client")
    def test_returns_object_info_list(self, mock_client_fn, mock_mounted, mock_fuse):
        mock_client = MagicMock()
        mock_bucket = MagicMock()
        mock_blob = MagicMock()
        mock_blob.name = "prefix/file.parquet"
        mock_blob.updated = datetime(2024, 1, 15, tzinfo=UTC)
        mock_blob.size = 1024
        mock_blob.time_created = datetime(2024, 1, 15, tzinfo=UTC)
        mock_bucket.list_blobs.return_value = [mock_blob]
        mock_client.bucket.return_value = mock_bucket
        mock_client_fn.return_value = mock_client

        results = list_objects("test-bucket", "prefix/")
        assert len(results) == 1
        assert results[0].name == "prefix/file.parquet"
        assert results[0].size == 1024

    @patch("deployment_api.utils.storage_facade._use_gcs_fuse", return_value=False)
    @patch("deployment_api.utils.storage_facade._is_bucket_mounted", return_value=False)
    @patch("deployment_api.utils.storage_client.get_storage_client")
    def test_empty_bucket_returns_empty_list(self, mock_client_fn, mock_mounted, mock_fuse):
        mock_client = MagicMock()
        mock_bucket = MagicMock()
        mock_bucket.list_blobs.return_value = []
        mock_client.bucket.return_value = mock_bucket
        mock_client_fn.return_value = mock_client

        results = list_objects("test-bucket", "nonexistent/")
        assert results == []


class TestObjectExistsViaApi:
    """Tests for object_exists using GCS API."""

    @patch("deployment_api.utils.storage_facade._use_gcs_fuse", return_value=False)
    @patch("deployment_api.utils.storage_facade._is_bucket_mounted", return_value=False)
    @patch("deployment_api.utils.storage_client.get_storage_client")
    def test_exists_when_blob_exists(self, mock_client_fn, mock_mounted, mock_fuse):
        mock_client = MagicMock()
        mock_bucket = MagicMock()
        mock_blob = MagicMock()
        mock_blob.exists.return_value = True
        mock_bucket.blob.return_value = mock_blob
        mock_client.bucket.return_value = mock_bucket
        mock_client_fn.return_value = mock_client

        assert object_exists("test-bucket", "path/file.parquet") is True

    @patch("deployment_api.utils.storage_facade._use_gcs_fuse", return_value=False)
    @patch("deployment_api.utils.storage_facade._is_bucket_mounted", return_value=False)
    @patch("deployment_api.utils.storage_client.get_storage_client")
    def test_does_not_exist(self, mock_client_fn, mock_mounted, mock_fuse):
        mock_client = MagicMock()
        mock_bucket = MagicMock()
        mock_blob = MagicMock()
        mock_blob.exists.return_value = False
        mock_bucket.blob.return_value = mock_blob
        mock_client.bucket.return_value = mock_bucket
        mock_client_fn.return_value = mock_client

        assert object_exists("test-bucket", "path/missing.parquet") is False

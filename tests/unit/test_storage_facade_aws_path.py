"""
AWS routing pin for storage_facade + UCI storage client factory.

When CLOUD_PROVIDER=aws:
- deployment_api.utils.storage_client.get_storage_client() MUST return S3StorageClient
  (not GCSStorageClient). Catches the regression where a refactor leaves
  ``from google.cloud import storage`` hardcoded inside the facade.
- storage_facade operations MUST route through the UCI client's list_blobs /
  blob_exists / download_bytes methods — not any GCS-specific code path.

Plan: data_status_comprehensive_test_coverage_2026_05_07 § E.1.
"""

from __future__ import annotations

import os
from unittest.mock import MagicMock, patch

import pytest
from unified_trading_library import StorageClient, clear_client_caches

from deployment_api.utils.storage_client import get_storage_client as _get_storage_client
from deployment_api.utils.storage_facade import (
    list_objects,
    object_exists,
    read_object_bytes,
    write_object_bytes,
)


class TestStorageClientFactoryAwsRouting:
    """Factory dispatch: CLOUD_PROVIDER=aws → S3StorageClient, NOT GCSStorageClient."""

    @pytest.fixture(autouse=True)
    def reset_utl_cache(self) -> None:
        """UTL caches clients per provider key — clear before/after every test."""
        clear_client_caches()
        yield
        clear_client_caches()

    def test_returns_aws_storage_client_type_when_cloud_provider_aws(self) -> None:
        """deployment_api.get_storage_client returns a StorageClient with provider_name='aws' when CLOUD_PROVIDER=aws.

        Supplies fake AWS credentials so boto3 does not attempt the IMDSv2
        metadata call (169.254.169.254) which is blocked by --allow-hosts in CI.
        """
        with patch.dict(
            os.environ,
            {
                "CLOUD_PROVIDER": "aws",
                "AWS_ACCESS_KEY_ID": "testing",
                "AWS_SECRET_ACCESS_KEY": "testing",
                "AWS_DEFAULT_REGION": "us-east-1",
            },
        ):
            clear_client_caches()
            client = _get_storage_client()

        assert isinstance(client, StorageClient)
        assert client.provider_name == "aws", (
            f"Expected provider_name='aws', got {client.provider_name!r}. "
            "This catches a regression where the facade is hardcoded to GCS."
        )

    def test_gcs_storage_client_not_instantiated_when_cloud_provider_aws(self) -> None:
        """GCSStorageClient constructor must never be called when CLOUD_PROVIDER=aws."""
        with (
            patch.dict(
                os.environ,
                {
                    "CLOUD_PROVIDER": "aws",
                    "AWS_ACCESS_KEY_ID": "testing",
                    "AWS_SECRET_ACCESS_KEY": "testing",
                    "AWS_DEFAULT_REGION": "us-east-1",
                },
            ),
            patch("unified_trading_library.cloud_interface.factory.GCSStorageClient") as mock_gcs,
        ):
            clear_client_caches()
            _get_storage_client()

        mock_gcs.assert_not_called()


class TestStorageFacadeAwsDispatch:
    """storage_facade routes through UCI client (not hardcoded GCS) when CLOUD_PROVIDER=aws."""

    @patch("deployment_api.utils.storage_facade.DEPLOYMENT_ENV", "development")
    def test_list_objects_calls_uci_list_blobs_when_aws(self) -> None:
        """list_objects invokes client.list_blobs — the UCI abstraction, not google.cloud.storage."""
        mock_client = MagicMock()
        mock_blob = MagicMock()
        mock_blob.name = "prefix/shard.parquet"
        mock_blob.size = 2048
        mock_client.list_blobs.return_value = [mock_blob]

        with patch(
            "deployment_api.utils.storage_client.get_storage_client", return_value=mock_client
        ):
            results = list_objects("my-s3-bucket", "prefix/")

        mock_client.list_blobs.assert_called_once()
        assert len(results) == 1
        assert results[0].name == "prefix/shard.parquet"
        assert results[0].size == 2048

    @patch("deployment_api.utils.storage_facade.DEPLOYMENT_ENV", "development")
    def test_object_exists_calls_uci_blob_exists_when_aws(self) -> None:
        """object_exists calls client.blob_exists — not any GCS-specific method."""
        mock_client = MagicMock()
        mock_client.blob_exists.return_value = True

        with patch(
            "deployment_api.utils.storage_client.get_storage_client", return_value=mock_client
        ):
            result = object_exists("my-s3-bucket", "path/file.parquet")

        mock_client.blob_exists.assert_called_once_with("my-s3-bucket", "path/file.parquet")
        assert result is True

    @patch("deployment_api.utils.storage_facade.DEPLOYMENT_ENV", "development")
    def test_read_object_bytes_calls_uci_download_bytes_when_aws(self) -> None:
        """read_object_bytes calls client.download_bytes — not any GCS blob API."""
        mock_client = MagicMock()
        mock_client.download_bytes.return_value = b"s3-payload"

        with patch(
            "deployment_api.utils.storage_client.get_storage_client", return_value=mock_client
        ):
            result = read_object_bytes("my-s3-bucket", "file.parquet")

        mock_client.download_bytes.assert_called_once_with("my-s3-bucket", "file.parquet")
        assert result == b"s3-payload"

    @patch("deployment_api.utils.storage_facade.DEPLOYMENT_ENV", "development")
    def test_fuse_path_not_used_in_aws_environment(self) -> None:
        """AWS deploys with DEPLOYMENT_ENV=development; the FUSE path must never be invoked."""
        mock_client = MagicMock()
        mock_client.list_blobs.return_value = []

        with (
            patch("deployment_api.utils.storage_facade._list_objects_fuse") as mock_fuse_fn,
            patch(
                "deployment_api.utils.storage_client.get_storage_client",
                return_value=mock_client,
            ),
        ):
            list_objects("my-s3-bucket", "prefix/")

        mock_fuse_fn.assert_not_called()

    @patch("deployment_api.utils.storage_facade.DEPLOYMENT_ENV", "development")
    def test_list_objects_empty_result_when_bucket_has_no_objects(self) -> None:
        """list_objects returns [] when the UCI client returns an empty blob list (S3 or GCS)."""
        mock_client = MagicMock()
        mock_client.list_blobs.return_value = []

        with patch(
            "deployment_api.utils.storage_client.get_storage_client", return_value=mock_client
        ):
            results = list_objects("my-s3-bucket", "nonexistent/prefix/")

        assert results == []

    @patch("deployment_api.utils.storage_facade.DEPLOYMENT_ENV", "development")
    def test_write_object_bytes_calls_uci_upload_bytes_when_aws(self) -> None:
        """write_object_bytes calls client.upload_bytes — not any GCS bucket/blob API."""
        mock_client = MagicMock()

        with patch(
            "deployment_api.utils.storage_client.get_storage_client", return_value=mock_client
        ):
            write_object_bytes("my-s3-bucket", "path/out.parquet", b"row-data")

        mock_client.upload_bytes.assert_called_once_with(
            "my-s3-bucket", "path/out.parquet", b"row-data"
        )

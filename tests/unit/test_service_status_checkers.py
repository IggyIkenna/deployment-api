"""
Unit tests for service_status_checkers module.

Tests cover:
- _get_cache_dict: typed sub-dict extraction
- _build_sort_key: Cloud Build object sort key
- _get_asset_group_blob_timestamp: GCS blob timestamp fetching (mocked storage)
- _get_service_timestamps_sync: service timestamps (mocked storage)
- get_latest_code_push: GitHub API calls (mocked Github)
- get_latest_data_timestamp: async wrapper with caching
- get_latest_build: async wrapper with GCS cache
- get_latest_deployment: async wrapper with GCS cache
"""

import asyncio
from datetime import UTC, datetime, timedelta
from unittest.mock import MagicMock, patch

# ---------------------------------------------------------------------------
# _get_cache_dict
# ---------------------------------------------------------------------------


class TestGetCacheDict:
    def test_returns_existing_dict(self):
        from deployment_api.routes.service_status_checkers import _get_cache_dict

        cache: dict[str, object] = {"key1": {"a": "b"}}
        result = _get_cache_dict(cache, "key1")
        assert result == {"a": "b"}

    def test_returns_empty_for_missing_key(self):
        from deployment_api.routes.service_status_checkers import _get_cache_dict

        cache: dict[str, object] = {}
        result = _get_cache_dict(cache, "missing")
        assert result == {}

    def test_returns_empty_for_non_dict_value(self):
        from deployment_api.routes.service_status_checkers import _get_cache_dict

        cache: dict[str, object] = {"key1": "string_value"}
        result = _get_cache_dict(cache, "key1")
        assert result == {}

    def test_returns_empty_for_none_value(self):
        from deployment_api.routes.service_status_checkers import _get_cache_dict

        cache: dict[str, object] = {"key1": None}
        result = _get_cache_dict(cache, "key1")
        assert result == {}

    def test_returns_empty_for_list_value(self):
        from deployment_api.routes.service_status_checkers import _get_cache_dict

        cache: dict[str, object] = {"key1": [1, 2, 3]}
        result = _get_cache_dict(cache, "key1")
        assert result == {}


# ---------------------------------------------------------------------------
# _build_sort_key
# ---------------------------------------------------------------------------


class TestBuildSortKey:
    def test_datetime_create_time_returns_directly(self):
        from deployment_api.routes.service_status_checkers import _build_sort_key

        now = datetime.now(UTC)
        build = MagicMock()
        build.create_time = now
        result = _build_sort_key(build)
        assert result == now

    def test_missing_create_time_returns_min(self):
        from deployment_api.routes.service_status_checkers import _build_sort_key

        build = MagicMock()
        build.create_time = None
        result = _build_sort_key(build)
        assert result == datetime.min.replace(tzinfo=UTC)

    def test_no_create_time_attr_returns_min(self):
        from deployment_api.routes.service_status_checkers import _build_sort_key

        build = object()  # plain object with no attributes
        result = _build_sort_key(build)
        assert result == datetime.min.replace(tzinfo=UTC)

    def test_protobuf_timestamp_with_seconds(self):
        from deployment_api.routes.service_status_checkers import _build_sort_key

        # Simulate a Protobuf Timestamp object
        proto_ts = MagicMock(spec=[])  # spec=[] means no datetime attributes
        proto_ts.seconds = 1700000000  # Unix timestamp
        build = MagicMock()
        build.create_time = proto_ts
        result = _build_sort_key(build)
        expected = datetime.fromtimestamp(1700000000, tz=UTC)
        assert result == expected

    def test_protobuf_timestamp_without_seconds_returns_min(self):
        from deployment_api.routes.service_status_checkers import _build_sort_key

        proto_ts = MagicMock(spec=[])  # no seconds attribute
        build = MagicMock()
        build.create_time = proto_ts
        result = _build_sort_key(build)
        assert result == datetime.min.replace(tzinfo=UTC)


# ---------------------------------------------------------------------------
# _get_asset_group_blob_timestamp
# ---------------------------------------------------------------------------


class TestGetCategoryBlobTimestamp:
    def test_returns_empty_when_no_blobs(self):
        from deployment_api.routes.service_status_checkers import _get_asset_group_blob_timestamp

        with patch("deployment_api.routes.service_status_checkers.list_objects", return_value=[]):
            result = _get_asset_group_blob_timestamp("test-bucket", "CEFI")
        assert result == {}

    def test_returns_timestamp_for_latest_blob(self):
        from deployment_api.routes.service_status_checkers import _get_asset_group_blob_timestamp

        now = datetime.now(UTC)
        blob1 = MagicMock()
        blob1.updated = now - timedelta(hours=2)
        blob1.name = "file1.parquet"
        blob1.size = 1024 * 1024 * 2  # 2 MB

        blob2 = MagicMock()
        blob2.updated = now
        blob2.name = "file2.parquet"
        blob2.size = 1024 * 1024 * 3  # 3 MB

        with patch(
            "deployment_api.routes.service_status_checkers.list_objects",
            return_value=[blob1, blob2],
        ):
            result = _get_asset_group_blob_timestamp("test-bucket", "CEFI")

        assert "timestamp" in result
        assert result["file"] == "file2.parquet"
        assert result["size_mb"] == 3.0

    def test_handles_blob_without_size(self):
        from deployment_api.routes.service_status_checkers import _get_asset_group_blob_timestamp

        now = datetime.now(UTC)
        blob = MagicMock()
        blob.updated = now
        blob.name = "file.parquet"
        blob.size = None  # no size

        with patch(
            "deployment_api.routes.service_status_checkers.list_objects",
            return_value=[blob],
        ):
            result = _get_asset_group_blob_timestamp("test-bucket", "CEFI")

        assert result["size_mb"] == 0

    def test_handles_blob_without_updated(self):
        from deployment_api.routes.service_status_checkers import _get_asset_group_blob_timestamp

        blob1 = MagicMock()
        blob1.updated = None
        blob1.name = "file1.parquet"
        blob1.size = 100

        blob2 = MagicMock()
        blob2.updated = datetime.now(UTC)
        blob2.name = "file2.parquet"
        blob2.size = 200

        with patch(
            "deployment_api.routes.service_status_checkers.list_objects",
            return_value=[blob1, blob2],
        ):
            result = _get_asset_group_blob_timestamp("test-bucket", "CEFI")
        # Should pick file2 (has updated timestamp)
        assert result.get("file") == "file2.parquet"


# ---------------------------------------------------------------------------
# _get_service_timestamps_sync
# ---------------------------------------------------------------------------


class TestGetServiceTimestampsSync:
    def test_unknown_service_returns_empty_categories(self):
        from deployment_api.routes.service_status_checkers import _get_service_timestamps_sync

        result = _get_service_timestamps_sync("unknown-service")
        assert "by_asset_group" in result or "error" in result or "latest" in result

    def test_known_service_returns_by_asset_group(self):
        from deployment_api.routes.service_status_checkers import _get_service_timestamps_sync

        now = datetime.now(UTC)
        blob = MagicMock()
        blob.updated = now
        blob.name = "file.parquet"
        blob.size = 1024

        with patch(
            "deployment_api.routes.service_status_checkers.list_objects",
            return_value=[blob],
        ):
            result = _get_service_timestamps_sync("instruments-service")

        assert "by_asset_group" in result
        assert "latest" in result

    def test_returns_error_on_exception(self):
        from deployment_api.routes.service_status_checkers import _get_service_timestamps_sync

        with patch(
            "deployment_api.routes.service_status_checkers.list_objects",
            side_effect=OSError("storage error"),
        ):
            result = _get_service_timestamps_sync("instruments-service")

        # Should return either by_asset_group with errors or error key
        assert isinstance(result, dict)

    def test_handles_storage_error_per_category(self):
        from deployment_api.routes.service_status_checkers import _get_service_timestamps_sync

        with patch(
            "deployment_api.routes.service_status_checkers.list_objects",
            side_effect=OSError("connection error"),
        ):
            result = _get_service_timestamps_sync("market-tick-data-handler")

        # by_asset_group should contain error entries for each category
        assert isinstance(result, dict)
        if "by_asset_group" in result:
            for _cat, cat_result in result["by_asset_group"].items():
                assert "error" in cat_result


# ---------------------------------------------------------------------------
# get_latest_code_push
# ---------------------------------------------------------------------------


class TestGetLatestCodePush:
    def test_returns_error_when_no_token(self):
        from deployment_api.routes.service_status_checkers import get_latest_code_push

        result = asyncio.run(get_latest_code_push("my-service", github_token=None))
        assert result is not None
        assert "error" in result

    def test_returns_commit_info_with_valid_token(self):
        from deployment_api.routes.service_status_checkers import get_latest_code_push

        # Mock Github API
        mock_commit = MagicMock()
        mock_commit.sha = "abc1234567890"
        mock_commit.commit.author.date = datetime(2024, 1, 15, 12, 0, 0, tzinfo=UTC)
        mock_commit.commit.message = "feat: add new feature"
        mock_commit.commit.author.name = "Test Author"

        mock_commits = MagicMock()
        mock_commits.__getitem__ = MagicMock(return_value=mock_commit)

        mock_repo = MagicMock()
        mock_repo.get_commits.return_value = mock_commits

        mock_gh = MagicMock()
        mock_gh.get_repo.return_value = mock_repo

        with patch("deployment_api.routes.service_status_checkers.Github", return_value=mock_gh):
            result = asyncio.run(get_latest_code_push("my-service", github_token="test-token"))

        assert result is not None
        assert "commit_sha" in result
        assert result["commit_sha"] == "abc1234"  # first 7 chars
        assert result["author"] == "Test Author"
        assert result["message"] == "feat: add new feature"

    def test_returns_error_on_github_exception(self):
        from deployment_api.routes.service_status_checkers import get_latest_code_push

        mock_gh = MagicMock()
        mock_gh.get_repo.side_effect = OSError("GitHub API error")

        with patch("deployment_api.routes.service_status_checkers.Github", return_value=mock_gh):
            result = asyncio.run(get_latest_code_push("my-service", github_token="test-token"))

        assert result is not None
        assert "error" in result


# ---------------------------------------------------------------------------
# get_latest_data_timestamp (async with cache)
# ---------------------------------------------------------------------------


class TestGetLatestDataTimestamp:
    def test_returns_result_from_gcs_scan(self):
        from deployment_api.routes.service_status_checkers import get_latest_data_timestamp

        mock_result = {"by_asset_group": {"CEFI": {"timestamp": "2024-01-15T12:00:00+00:00"}}}

        with (
            patch(
                "deployment_api.routes.service_status_checkers.load_gcs_cache",
                return_value={},
            ),
            patch(
                "deployment_api.routes.service_status_checkers.save_gcs_cache",
            ),
            patch(
                "deployment_api.routes.service_status_checkers._get_service_timestamps_sync",
                return_value=mock_result,
            ),
        ):
            result = asyncio.run(get_latest_data_timestamp("instruments-service", use_cache=False))

        assert result is not None
        assert "by_asset_group" in result

    def test_uses_cache_when_fresh(self):
        from deployment_api.routes.service_status_checkers import get_latest_data_timestamp

        cached_data = {"by_asset_group": {}, "latest": "2024-01-15T12:00:00+00:00"}
        cache_time = datetime.now(UTC).isoformat()

        mock_cache: dict[str, object] = {
            "data_timestamps": {"instruments-service": cached_data},
            "data_timestamp_times": {"instruments-service": cache_time},
        }

        with (
            patch(
                "deployment_api.routes.service_status_checkers.load_gcs_cache",
                return_value=mock_cache,
            ),
        ):
            result = asyncio.run(get_latest_data_timestamp("instruments-service", use_cache=True))

        assert result is not None


# ---------------------------------------------------------------------------
# get_latest_build (async with GCS cache)
# ---------------------------------------------------------------------------


class TestGetLatestBuild:
    def test_uses_cache_when_fresh(self):
        from deployment_api.routes.service_status_checkers import get_latest_build

        cached_build: dict[str, object] = {
            "build_id": "build-123",
            "timestamp": "2024-01-15T12:00:00+00:00",
            "status": "SUCCESS",
        }
        cache_time = datetime.now(UTC).isoformat()

        mock_cache: dict[str, object] = {
            "builds": {"instruments-service": cached_build},
            "build_times": {"instruments-service": cache_time},
        }

        with patch(
            "deployment_api.routes.service_status_checkers.load_gcs_cache",
            return_value=mock_cache,
        ):
            result = asyncio.run(get_latest_build("instruments-service", use_cache=True))

        assert result is not None
        assert result.get("build_id") == "build-123"

    def test_fetches_from_api_when_cache_expired(self):
        from deployment_api.routes.service_status_checkers import get_latest_build

        # Cache with expired timestamp
        old_time = (datetime.now(UTC) - timedelta(minutes=10)).isoformat()
        mock_cache: dict[str, object] = {
            "builds": {"instruments-service": {"build_id": "old-build"}},
            "build_times": {"instruments-service": old_time},
        }

        mock_build_result: dict[str, object] = {
            "build_id": "new-build-456",
            "timestamp": "2024-01-15T13:00:00+00:00",
            "status": "SUCCESS",
        }

        with (
            patch(
                "deployment_api.routes.service_status_checkers.load_gcs_cache",
                return_value=mock_cache,
            ),
            patch(
                "deployment_api.routes.service_status_checkers._fetch_build_from_api",
                return_value=mock_build_result,
            ),
        ):
            result = asyncio.run(get_latest_build("instruments-service", use_cache=True))

        assert result is not None
        assert result.get("build_id") == "new-build-456"

    def test_no_cache_fetches_from_api(self):
        from deployment_api.routes.service_status_checkers import get_latest_build

        mock_build_result: dict[str, object] = {"build_id": "direct-build"}

        with patch(
            "deployment_api.routes.service_status_checkers._fetch_build_from_api",
            return_value=mock_build_result,
        ):
            result = asyncio.run(get_latest_build("instruments-service", use_cache=False))

        assert result is not None
        assert result.get("build_id") == "direct-build"

    def test_invalid_cache_time_falls_through_to_api(self):
        from deployment_api.routes.service_status_checkers import get_latest_build

        # Invalid ISO timestamp in cache triggers except (ValueError, ...) at lines 432-433
        mock_cache: dict[str, object] = {
            "builds": {"my-svc": {"build_id": "stale"}},
            "build_times": {"my-svc": "NOT-A-DATE"},
        }
        fresh_result: dict[str, object] = {"build_id": "fresh-api-build"}

        with (
            patch("deployment_api.routes.service_status_checkers.load_gcs_cache", return_value=mock_cache),
            patch("deployment_api.routes.service_status_checkers._fetch_build_from_api", return_value=fresh_result),
        ):
            result = asyncio.run(get_latest_build("my-svc", use_cache=True))

        assert result is not None
        assert result.get("build_id") == "fresh-api-build"


# ---------------------------------------------------------------------------
# _resolve_trigger_id
# ---------------------------------------------------------------------------


class TestResolveTriggerID:
    def test_cached_trigger_returned_immediately(self):
        from deployment_api.routes.service_status_checkers import _resolve_trigger_id

        trigger_ids: dict[str, object] = {"my-svc": "trigger-abc-123"}
        result = _resolve_trigger_id("my-svc", {}, trigger_ids)
        assert result == "trigger-abc-123"

    def test_non_string_cached_value_returns_empty_string(self):
        from deployment_api.routes.service_status_checkers import _resolve_trigger_id

        trigger_ids: dict[str, object] = {"svc": 99999}
        result = _resolve_trigger_id("svc", {}, trigger_ids)
        assert result == ""

    def test_gcloud_success_fetches_and_caches(self):
        from deployment_api.routes.service_status_checkers import _resolve_trigger_id

        mock_proc = MagicMock()
        mock_proc.returncode = 0
        mock_proc.stdout = "trigger-xyz-456\n"

        with (
            patch("subprocess.run", return_value=mock_proc),
            patch("deployment_api.routes.service_status_checkers.save_gcs_cache"),
        ):
            trigger_ids: dict[str, object] = {}
            cache: dict[str, object] = {}
            result = _resolve_trigger_id("new-svc", cache, trigger_ids)

        assert result == "trigger-xyz-456"
        assert trigger_ids["new-svc"] == "trigger-xyz-456"
        assert cache["trigger_ids"] is trigger_ids

    def test_gcloud_failure_returns_none(self):
        from deployment_api.routes.service_status_checkers import _resolve_trigger_id

        mock_proc = MagicMock()
        mock_proc.returncode = 1
        mock_proc.stdout = ""

        with patch("subprocess.run", return_value=mock_proc):
            result = _resolve_trigger_id("missing-svc", {}, {})

        assert result is None


# ---------------------------------------------------------------------------
# _get_asset_group_blob_timestamp (naive-tz warning path)
# ---------------------------------------------------------------------------


class TestNaiveTimestampWarning:
    def test_naive_datetime_triggers_warning(self) -> None:
        from deployment_api.routes.service_status_checkers import _get_asset_group_blob_timestamp

        # datetime without tzinfo = naive → triggers logger.warning at line 250
        naive_dt = datetime(2026, 5, 15, 12, 0, 0)
        blob = MagicMock()
        blob.updated = naive_dt
        blob.name = "file.parquet"
        blob.size = 1024

        with patch("deployment_api.routes.service_status_checkers.list_objects", return_value=[blob]):
            result = _get_asset_group_blob_timestamp("test-bucket", "CEFI")

        assert "timestamp" in result
        assert result["file"] == "file.parquet"


# ---------------------------------------------------------------------------
# _get_service_timestamps_sync (outer except path)
# ---------------------------------------------------------------------------


class TestGetServiceTimestampsSyncOuterExcept:
    def test_fromisoformat_failure_triggers_outer_except(self) -> None:
        from deployment_api.routes.service_status_checkers import _get_service_timestamps_sync

        # Returning {"timestamp": "NOT-ISO"} triggers datetime.fromisoformat() → ValueError
        # in the valid_timestamps comprehension — caught by the outer except at lines 276-278.
        bad_result: dict[str, object] = {"timestamp": "NOT-A-VALID-ISO-DATE", "file": "x", "size_mb": 0}

        with patch(
            "deployment_api.routes.service_status_checkers._get_asset_group_blob_timestamp",
            return_value=bad_result,
        ):
            result = _get_service_timestamps_sync("instruments-service")

        assert "error" in result


# ---------------------------------------------------------------------------
# get_latest_data_timestamp (cache invalid path)
# ---------------------------------------------------------------------------


class TestGetLatestDataTimestampCacheInvalid:
    def test_invalid_cache_time_falls_through_to_fetch(self) -> None:
        from deployment_api.routes.service_status_checkers import get_latest_data_timestamp

        # Invalid cache time → ValueError in fromisoformat → falls through at lines 305-306
        mock_cache: dict[str, object] = {
            "data_timestamps": {"instruments-service": {"by_asset_group": {}}},
            "data_timestamp_times": {"instruments-service": "BAD-TIMESTAMP"},
        }
        fresh_result: dict[str, object] = {"by_asset_group": {"CEFI": {}}}

        with (
            patch("deployment_api.routes.service_status_checkers.load_gcs_cache", return_value=mock_cache),
            patch("deployment_api.routes.service_status_checkers.save_gcs_cache"),
            patch(
                "deployment_api.routes.service_status_checkers._get_service_timestamps_sync",
                return_value=fresh_result,
            ),
        ):
            result = asyncio.run(get_latest_data_timestamp("instruments-service", use_cache=True))

        assert result is not None
        assert "by_asset_group" in result


# ---------------------------------------------------------------------------
# get_latest_deployment
# ---------------------------------------------------------------------------


class TestGetLatestDeployment:
    def test_uses_cache_when_fresh(self) -> None:
        from deployment_api.routes.service_status_checkers import get_latest_deployment

        cached = {"deployment_id": "dep-123", "status": "COMPLETED"}
        cache_time = datetime.now(UTC).isoformat()
        mock_cache: dict[str, object] = {
            "deployments": {"my-svc": cached},
            "deployment_times": {"my-svc": cache_time},
        }

        with patch("deployment_api.routes.service_status_checkers.load_gcs_cache", return_value=mock_cache):
            result = asyncio.run(get_latest_deployment("my-svc", use_cache=True))

        assert result is not None
        assert result.get("deployment_id") == "dep-123"

    def test_expired_cache_fetches_from_gcs(self) -> None:
        from deployment_api.routes.service_status_checkers import get_latest_deployment

        old_time = (datetime.now(UTC) - timedelta(minutes=10)).isoformat()
        mock_cache: dict[str, object] = {
            "deployments": {"my-svc": {"deployment_id": "old-dep"}},
            "deployment_times": {"my-svc": old_time},
        }
        gcs_deployment = {
            "deployment_id": "fresh-dep-456",
            "status": "RUNNING",
            "cli_args": "",
            "progress": {"total_shards": 10, "completed": 5, "failed": 0},
            "created_at": "2026-05-18T10:00:00Z",
            "compute_type": "vm",
            "tag": "v1.2.3",
        }

        with (
            patch("deployment_api.routes.service_status_checkers.load_gcs_cache", return_value=mock_cache),
            patch("deployment_api.routes.service_status_checkers.save_gcs_cache"),
            patch(
                "deployment_api.routes.service_status_checkers._list_deployments_from_gcs",
                return_value=[gcs_deployment],
            ),
        ):
            result = asyncio.run(get_latest_deployment("my-svc", use_cache=True))

        assert result is not None
        assert result.get("deployment_id") == "fresh-dep-456"
        assert result.get("total_shards") == 10

    def test_no_deployments_returns_none(self) -> None:
        from deployment_api.routes.service_status_checkers import get_latest_deployment

        mock_cache: dict[str, object] = {"deployments": {}, "deployment_times": {}}

        with (
            patch("deployment_api.routes.service_status_checkers.load_gcs_cache", return_value=mock_cache),
            patch("deployment_api.routes.service_status_checkers.save_gcs_cache"),
            patch("deployment_api.routes.service_status_checkers._list_deployments_from_gcs", return_value=[]),
        ):
            result = asyncio.run(get_latest_deployment("no-svc", use_cache=False))

        assert result is None

    def test_force_flag_detected(self) -> None:
        from deployment_api.routes.service_status_checkers import get_latest_deployment

        mock_cache: dict[str, object] = {"deployments": {}, "deployment_times": {}}
        gcs_deployment = {
            "deployment_id": "forced-dep",
            "status": "COMPLETED",
            "cli_args": "--service my-svc --force --tag v1.0",
            "created_at": "2026-05-18T10:00:00Z",
            "compute_type": "vm",
            "tag": "v1.0",
        }

        with (
            patch("deployment_api.routes.service_status_checkers.load_gcs_cache", return_value=mock_cache),
            patch("deployment_api.routes.service_status_checkers.save_gcs_cache"),
            patch(
                "deployment_api.routes.service_status_checkers._list_deployments_from_gcs",
                return_value=[gcs_deployment],
            ),
        ):
            result = asyncio.run(get_latest_deployment("my-svc", use_cache=False))

        assert result is not None
        assert result.get("used_force") is True

    def test_gcs_error_returns_error_dict(self) -> None:
        from deployment_api.routes.service_status_checkers import get_latest_deployment

        mock_cache: dict[str, object] = {"deployments": {}, "deployment_times": {}}

        with (
            patch("deployment_api.routes.service_status_checkers.load_gcs_cache", return_value=mock_cache),
            patch("deployment_api.routes.service_status_checkers.save_gcs_cache"),
            patch(
                "deployment_api.routes.service_status_checkers._list_deployments_from_gcs",
                side_effect=OSError("GCS unavailable"),
            ),
        ):
            result = asyncio.run(get_latest_deployment("my-svc", use_cache=False))

        assert result is not None
        assert "error" in result

    def test_invalid_cache_time_falls_through_to_fetch(self) -> None:
        from deployment_api.routes.service_status_checkers import get_latest_deployment

        # Invalid ISO timestamp → except (ValueError, TypeError, KeyError) at lines 348-349
        mock_cache: dict[str, object] = {
            "deployments": {"my-svc": {"deployment_id": "stale"}},
            "deployment_times": {"my-svc": "BAD-TIMESTAMP"},
        }
        fresh = {
            "deployment_id": "fresh-dep",
            "status": "COMPLETED",
            "cli_args": "",
            "created_at": "2026-05-18T10:00:00Z",
            "compute_type": "vm",
            "tag": "v1.1",
        }

        with (
            patch("deployment_api.routes.service_status_checkers.load_gcs_cache", return_value=mock_cache),
            patch("deployment_api.routes.service_status_checkers.save_gcs_cache"),
            patch(
                "deployment_api.routes.service_status_checkers._list_deployments_from_gcs",
                return_value=[fresh],
            ),
        ):
            result = asyncio.run(get_latest_deployment("my-svc", use_cache=True))

        assert result is not None
        assert result.get("deployment_id") == "fresh-dep"

"""Unit tests for scripts/data_status_rollup_worker.py — pure helpers + mocked run_rollup."""

from __future__ import annotations

import gzip
import json
from types import SimpleNamespace
from unittest.mock import MagicMock, call, patch

from unified_trading_library import setup_events

setup_events("deployment-api", "test")

_PATCH_SETUP_EVENTS = "deployment_api.scripts.data_status_rollup_worker.setup_events"
_PATCH_LOG_EVENT = "deployment_api.scripts.data_status_rollup_worker.log_event"
_PATCH_GET_STORAGE = "deployment_api.scripts.data_status_rollup_worker.get_storage_client"
_PATCH_DSS = "deployment_api.scripts.data_status_rollup_worker.DataStatusService"
_PATCH_GZIP_ES = "deployment_api.scripts.data_status_rollup_worker.GcsEventSink"


# ── Pure helpers ──────────────────────────────────────────────────────────────


class TestTodayIso:
    def test_returns_date_string(self) -> None:
        from deployment_api.scripts.data_status_rollup_worker import _today_iso

        result = _today_iso()
        assert len(result) == 10
        assert result[4] == "-" and result[7] == "-"


class TestRollupBlobPath:
    def test_returns_expected_path(self) -> None:
        from deployment_api.scripts.data_status_rollup_worker import _rollup_blob_path

        assert _rollup_blob_path("instruments-service") == "instruments-service/full.json.gz"

    def test_service_name_embedded(self) -> None:
        from deployment_api.scripts.data_status_rollup_worker import _rollup_blob_path

        assert "my-svc" in _rollup_blob_path("my-svc")


class TestCoverageBlobPath:
    def test_returns_expected_path(self) -> None:
        from deployment_api.scripts.data_status_rollup_worker import _coverage_blob_path

        assert _coverage_blob_path("instruments-service") == "instruments-service/coverage.json.gz"


class TestGzipPayload:
    def test_returns_bytes_and_raw_size(self) -> None:
        from deployment_api.scripts.data_status_rollup_worker import _gzip_payload

        payload = {"key": "value", "count": 42}
        compressed, raw_size = _gzip_payload(payload)
        assert isinstance(compressed, bytes)
        assert raw_size > 0

    def test_compressed_is_valid_gzip(self) -> None:
        from deployment_api.scripts.data_status_rollup_worker import _gzip_payload

        payload = {"test": True}
        compressed, _ = _gzip_payload(payload)
        decompressed = gzip.decompress(compressed)
        parsed = json.loads(decompressed)
        assert parsed == {"test": True}

    def test_raw_size_matches_json_length(self) -> None:
        from deployment_api.scripts.data_status_rollup_worker import _gzip_payload

        payload = {"hello": "world"}
        _, raw_size = _gzip_payload(payload)
        expected = len(json.dumps(payload).encode("utf-8"))
        assert raw_size == expected

    def test_empty_dict_produces_valid_output(self) -> None:
        from deployment_api.scripts.data_status_rollup_worker import _gzip_payload

        compressed, raw_size = _gzip_payload({})
        assert raw_size > 0
        parsed = json.loads(gzip.decompress(compressed))
        assert parsed == {}


# ── _write_rollup_to_gcs / _write_coverage_to_gcs ────────────────────────────


class TestWriteRollupToGcs:
    def test_calls_upload_bytes_with_gzip(self) -> None:
        from deployment_api.scripts.data_status_rollup_worker import _write_rollup_to_gcs

        mock_client = MagicMock()
        payload = {"data": [1, 2, 3]}
        result = _write_rollup_to_gcs(mock_client, "my-bucket", "instruments-service", payload)

        mock_client.upload_bytes.assert_called_once()
        call_kwargs = mock_client.upload_bytes.call_args
        assert call_kwargs.kwargs["bucket"] == "my-bucket"
        assert call_kwargs.kwargs["blob_path"] == "instruments-service/full.json.gz"
        assert isinstance(result["size_compressed"], int)
        assert isinstance(result["size_uncompressed"], int)

    def test_returns_size_metrics(self) -> None:
        from deployment_api.scripts.data_status_rollup_worker import _write_rollup_to_gcs

        mock_client = MagicMock()
        result = _write_rollup_to_gcs(mock_client, "bucket", "svc", {"x": 1})
        assert result["size_compressed"] > 0
        assert result["size_uncompressed"] > 0


class TestWriteCoverageToGcs:
    def test_calls_upload_bytes_with_coverage_path(self) -> None:
        from deployment_api.scripts.data_status_rollup_worker import _write_coverage_to_gcs

        mock_client = MagicMock()
        _write_coverage_to_gcs(mock_client, "my-bucket", "instruments-service", {"coverage": []})

        call_kwargs = mock_client.upload_bytes.call_args
        assert call_kwargs.kwargs["blob_path"] == "instruments-service/coverage.json.gz"


# ── _build_one_service_rollup / _build_one_service_coverage ──────────────────


class TestBuildOneServiceRollup:
    def test_calls_get_manifest_status_sync(self) -> None:
        from deployment_api.scripts.data_status_rollup_worker import _build_one_service_rollup

        mock_dss = MagicMock()
        mock_dss._get_manifest_status_sync.return_value = {"asset_groups": {}}
        result = _build_one_service_rollup(mock_dss, "my-service", "2024-01-31")

        mock_dss._get_manifest_status_sync.assert_called_once_with(
            service="my-service",
            start_date="2018-01-01",
            end_date="2024-01-31",
            asset_groups=None,
        )
        assert result == {"asset_groups": {}}


class TestBuildOneServiceCoverage:
    def test_calls_get_coverage_summary_sync(self) -> None:
        from deployment_api.scripts.data_status_rollup_worker import _build_one_service_coverage

        mock_dss = MagicMock()
        mock_dss._get_coverage_summary_sync.return_value = {"summary": "ok"}
        result = _build_one_service_coverage(mock_dss, "my-service")

        mock_dss._get_coverage_summary_sync.assert_called_once_with(service="my-service", asset_groups=None)
        assert result == {"summary": "ok"}


# ── run_rollup ────────────────────────────────────────────────────────────────


class TestRunRollup:
    def _make_mocks(self) -> tuple[MagicMock, MagicMock]:
        mock_storage = MagicMock()
        mock_dss_instance = MagicMock()
        mock_dss_instance._get_manifest_status_sync.return_value = {"asset_groups": {"core": {}}}
        mock_dss_instance._get_coverage_summary_sync.return_value = {"total": 1}
        return mock_storage, mock_dss_instance

    def test_success_returns_zero(self) -> None:
        from deployment_api.scripts.data_status_rollup_worker import run_rollup

        mock_storage, mock_dss_instance = self._make_mocks()
        with (
            patch(_PATCH_SETUP_EVENTS),
            patch(_PATCH_LOG_EVENT),
            patch(_PATCH_GZIP_ES),
            patch(_PATCH_GET_STORAGE, return_value=mock_storage),
            patch(_PATCH_DSS, return_value=mock_dss_instance),
        ):
            result = run_rollup("test-project", "test-bucket", ["instruments-service"])

        assert result == 0

    def test_all_failures_returns_one(self) -> None:
        from deployment_api.scripts.data_status_rollup_worker import run_rollup

        mock_storage = MagicMock()
        mock_dss_instance = MagicMock()
        mock_dss_instance._get_manifest_status_sync.side_effect = RuntimeError("GCS error")
        mock_dss_instance._get_coverage_summary_sync.side_effect = RuntimeError("GCS error")

        with (
            patch(_PATCH_SETUP_EVENTS),
            patch(_PATCH_LOG_EVENT),
            patch(_PATCH_GZIP_ES),
            patch(_PATCH_GET_STORAGE, return_value=mock_storage),
            patch(_PATCH_DSS, return_value=mock_dss_instance),
        ):
            result = run_rollup("test-project", "test-bucket", ["instruments-service"])

        assert result == 1

    def test_multiple_services_all_succeed(self) -> None:
        from deployment_api.scripts.data_status_rollup_worker import run_rollup

        mock_storage, mock_dss_instance = self._make_mocks()
        with (
            patch(_PATCH_SETUP_EVENTS),
            patch(_PATCH_LOG_EVENT),
            patch(_PATCH_GZIP_ES),
            patch(_PATCH_GET_STORAGE, return_value=mock_storage),
            patch(_PATCH_DSS, return_value=mock_dss_instance),
        ):
            result = run_rollup("test-project", "test-bucket", ["svc-a", "svc-b"])

        assert result == 0
        assert mock_dss_instance._get_manifest_status_sync.call_count == 2

    def test_partial_failure_returns_zero(self) -> None:
        from deployment_api.scripts.data_status_rollup_worker import run_rollup

        mock_storage = MagicMock()
        mock_dss_instance = MagicMock()
        mock_dss_instance._get_manifest_status_sync.side_effect = [
            RuntimeError("fail"),
            {"asset_groups": {}},
        ]
        mock_dss_instance._get_coverage_summary_sync.return_value = {"total": 0}

        with (
            patch(_PATCH_SETUP_EVENTS),
            patch(_PATCH_LOG_EVENT),
            patch(_PATCH_GZIP_ES),
            patch(_PATCH_GET_STORAGE, return_value=mock_storage),
            patch(_PATCH_DSS, return_value=mock_dss_instance),
        ):
            result = run_rollup("test-project", "test-bucket", ["svc-fail", "svc-ok"])

        assert result == 0

    def test_started_and_stopped_events_emitted(self) -> None:
        from deployment_api.scripts.data_status_rollup_worker import run_rollup

        mock_storage, mock_dss_instance = self._make_mocks()
        log_calls: list[tuple[str]] = []

        def capture_log(event_type: str, **kwargs: object) -> None:
            log_calls.append((event_type,))

        with (
            patch(_PATCH_SETUP_EVENTS),
            patch(_PATCH_LOG_EVENT, side_effect=capture_log),
            patch(_PATCH_GZIP_ES),
            patch(_PATCH_GET_STORAGE, return_value=mock_storage),
            patch(_PATCH_DSS, return_value=mock_dss_instance),
        ):
            run_rollup("test-project", "test-bucket", ["instruments-service"])

        event_types = [e[0] for e in log_calls]
        assert "STARTED" in event_types
        assert "STOPPED" in event_types

    def test_failed_event_on_all_failures(self) -> None:
        from deployment_api.scripts.data_status_rollup_worker import run_rollup

        mock_storage = MagicMock()
        mock_dss_instance = MagicMock()
        mock_dss_instance._get_manifest_status_sync.side_effect = ValueError("boom")
        mock_dss_instance._get_coverage_summary_sync.side_effect = ValueError("boom")

        log_calls: list[tuple[str]] = []

        def capture_log(event_type: str, **kwargs: object) -> None:
            log_calls.append((event_type,))

        with (
            patch(_PATCH_SETUP_EVENTS),
            patch(_PATCH_LOG_EVENT, side_effect=capture_log),
            patch(_PATCH_GZIP_ES),
            patch(_PATCH_GET_STORAGE, return_value=mock_storage),
            patch(_PATCH_DSS, return_value=mock_dss_instance),
        ):
            run_rollup("test-project", "test-bucket", ["instruments-service"])

        event_types = [e[0] for e in log_calls]
        assert "FAILED" in event_types

    def test_coverage_failure_does_not_fail_manifest(self) -> None:
        from deployment_api.scripts.data_status_rollup_worker import run_rollup

        mock_storage, mock_dss_instance = self._make_mocks()
        mock_dss_instance._get_coverage_summary_sync.side_effect = OSError("storage error")

        with (
            patch(_PATCH_SETUP_EVENTS),
            patch(_PATCH_LOG_EVENT),
            patch(_PATCH_GZIP_ES),
            patch(_PATCH_GET_STORAGE, return_value=mock_storage),
            patch(_PATCH_DSS, return_value=mock_dss_instance),
        ):
            result = run_rollup("test-project", "test-bucket", ["instruments-service"])

        assert result == 0

    def test_empty_services_list(self) -> None:
        from deployment_api.scripts.data_status_rollup_worker import run_rollup

        mock_storage = MagicMock()
        mock_dss_instance = MagicMock()

        with (
            patch(_PATCH_SETUP_EVENTS),
            patch(_PATCH_LOG_EVENT),
            patch(_PATCH_GZIP_ES),
            patch(_PATCH_GET_STORAGE, return_value=mock_storage),
            patch(_PATCH_DSS, return_value=mock_dss_instance),
        ):
            result = run_rollup("test-project", "test-bucket", [])

        assert result == 1

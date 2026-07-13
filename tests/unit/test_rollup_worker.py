"""Unit tests for scripts/data_status_rollup_worker.py — pure helpers + mocked run_rollup."""

from __future__ import annotations

import gzip
import json
from unittest.mock import MagicMock, patch

from unified_trading_library import setup_events

setup_events("deployment-api", "test")

_PATCH_LOG_EVENT = "deployment_api.scripts.data_status_rollup_worker.log_event"
_PATCH_GET_STORAGE = "deployment_api.scripts.data_status_rollup_worker.get_storage_client"
_PATCH_DSS = "deployment_api.scripts.data_status_rollup_worker.DataStatusService"


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


# ── _set_child_memory_rlimit ──────────────────────────────────────────────────


class TestSetChildMemoryRlimit:
    def test_calls_setrlimit_with_same_soft_and_hard(self) -> None:
        from deployment_api.scripts.data_status_rollup_worker import _set_child_memory_rlimit

        with patch("deployment_api.scripts.data_status_rollup_worker.resource") as mock_resource:
            mock_resource.RLIMIT_AS = "RLIMIT_AS_SENTINEL"
            _set_child_memory_rlimit(1234)

        mock_resource.setrlimit.assert_called_once_with("RLIMIT_AS_SENTINEL", (1234, 1234))


# ── _child_build_and_write_service ───────────────────────────────────────────

_PATCH_SET_RLIMIT = "deployment_api.scripts.data_status_rollup_worker._set_child_memory_rlimit"


class TestChildBuildAndWriteService:
    def _make_mocks(self) -> tuple[MagicMock, MagicMock]:
        mock_storage = MagicMock()
        mock_dss_instance = MagicMock()
        mock_dss_instance._get_manifest_status_sync.return_value = {"asset_groups": {"core": {}}}
        mock_dss_instance._get_coverage_summary_sync.return_value = {"total": 1}
        return mock_storage, mock_dss_instance

    def test_full_success(self) -> None:
        from deployment_api.scripts.data_status_rollup_worker import _child_build_and_write_service

        mock_storage, mock_dss_instance = self._make_mocks()
        result_queue: MagicMock = MagicMock()
        with (
            patch(_PATCH_SET_RLIMIT),
            patch(_PATCH_GET_STORAGE, return_value=mock_storage),
            patch(_PATCH_DSS, return_value=mock_dss_instance),
        ):
            _child_build_and_write_service("proj", "bucket", "instruments-service", "2024-01-31", 999, result_queue)

        result = result_queue.put.call_args.args[0]
        assert result["manifest_ok"] is True
        assert result["coverage_ok"] is True
        assert result["service"] == "instruments-service"

    def test_manifest_failure_does_not_block_coverage(self) -> None:
        from deployment_api.scripts.data_status_rollup_worker import _child_build_and_write_service

        mock_storage, mock_dss_instance = self._make_mocks()
        mock_dss_instance._get_manifest_status_sync.side_effect = RuntimeError("boom")
        result_queue: MagicMock = MagicMock()
        with (
            patch(_PATCH_SET_RLIMIT),
            patch(_PATCH_GET_STORAGE, return_value=mock_storage),
            patch(_PATCH_DSS, return_value=mock_dss_instance),
        ):
            _child_build_and_write_service("proj", "bucket", "svc", "2024-01-31", 999, result_queue)

        result = result_queue.put.call_args.args[0]
        assert result["manifest_ok"] is False
        assert "boom" in result["manifest_error"]
        assert result["coverage_ok"] is True

    def test_coverage_failure_does_not_fail_manifest(self) -> None:
        from deployment_api.scripts.data_status_rollup_worker import _child_build_and_write_service

        mock_storage, mock_dss_instance = self._make_mocks()
        mock_dss_instance._get_coverage_summary_sync.side_effect = OSError("storage error")
        result_queue: MagicMock = MagicMock()
        with (
            patch(_PATCH_SET_RLIMIT),
            patch(_PATCH_GET_STORAGE, return_value=mock_storage),
            patch(_PATCH_DSS, return_value=mock_dss_instance),
        ):
            _child_build_and_write_service("proj", "bucket", "svc", "2024-01-31", 999, result_queue)

        result = result_queue.put.call_args.args[0]
        assert result["manifest_ok"] is True
        assert result["coverage_ok"] is False

    def test_rlimit_failure_does_not_abort_the_service(self) -> None:
        # A host where RLIMIT_AS can't be set (observed on macOS dev boxes; real
        # Cloud Run/Linux sets it fine — verified separately) must still let the
        # service's actual compute proceed, just unprotected by the ceiling.
        from deployment_api.scripts.data_status_rollup_worker import _child_build_and_write_service

        mock_storage, mock_dss_instance = self._make_mocks()
        result_queue: MagicMock = MagicMock()
        with (
            patch(_PATCH_SET_RLIMIT, side_effect=ValueError("current limit exceeds maximum limit")),
            patch(_PATCH_GET_STORAGE, return_value=mock_storage),
            patch(_PATCH_DSS, return_value=mock_dss_instance),
        ):
            _child_build_and_write_service("proj", "bucket", "svc", "2024-01-31", 999, result_queue)

        result = result_queue.put.call_args.args[0]
        assert result["manifest_ok"] is True
        assert result["coverage_ok"] is True

    def test_init_failure_reports_error_and_returns_early(self) -> None:
        from deployment_api.scripts.data_status_rollup_worker import _child_build_and_write_service

        result_queue: MagicMock = MagicMock()
        with (
            patch(_PATCH_SET_RLIMIT),
            patch(_PATCH_GET_STORAGE, side_effect=RuntimeError("no creds")),
            patch(_PATCH_DSS),
        ):
            _child_build_and_write_service("proj", "bucket", "svc", "2024-01-31", 999, result_queue)

        result = result_queue.put.call_args.args[0]
        assert result["manifest_ok"] is False
        assert result["coverage_ok"] is False
        assert "no creds" in result["error"]


# ── _run_service_isolated ─────────────────────────────────────────────────────

_PATCH_MULTIPROCESSING = "deployment_api.scripts.data_status_rollup_worker.multiprocessing"


class TestRunServiceIsolated:
    def test_reads_result_from_queue_on_clean_exit(self) -> None:
        from deployment_api.scripts.data_status_rollup_worker import _run_service_isolated

        mock_process = MagicMock()
        mock_process.is_alive.return_value = False
        mock_process.exitcode = 0
        mock_queue = MagicMock()
        mock_queue.empty.return_value = False
        mock_queue.get.return_value = {"service": "svc", "manifest_ok": True, "coverage_ok": True}
        mock_ctx = MagicMock()
        mock_ctx.Queue.return_value = mock_queue
        mock_ctx.Process.return_value = mock_process

        with patch(_PATCH_MULTIPROCESSING) as mock_mp:
            mock_mp.get_context.return_value = mock_ctx
            result = _run_service_isolated("proj", "bucket", "svc", "2024-01-31")

        assert result == {"service": "svc", "manifest_ok": True, "coverage_ok": True}
        mock_process.start.assert_called_once()

    def test_synthesizes_failure_when_child_dies_without_reporting(self) -> None:
        # e.g. OOM-killed before it could reach result_queue.put.
        from deployment_api.scripts.data_status_rollup_worker import _run_service_isolated

        mock_process = MagicMock()
        mock_process.is_alive.return_value = False
        mock_process.exitcode = -9
        mock_queue = MagicMock()
        mock_queue.empty.return_value = True
        mock_ctx = MagicMock()
        mock_ctx.Queue.return_value = mock_queue
        mock_ctx.Process.return_value = mock_process

        with patch(_PATCH_MULTIPROCESSING) as mock_mp:
            mock_mp.get_context.return_value = mock_ctx
            result = _run_service_isolated("proj", "bucket", "svc", "2024-01-31")

        assert result["manifest_ok"] is False
        assert result["coverage_ok"] is False
        assert "-9" in result["error"]

    def test_terminates_and_reports_timeout_when_still_alive(self) -> None:
        from deployment_api.scripts.data_status_rollup_worker import _run_service_isolated

        mock_process = MagicMock()
        mock_process.is_alive.return_value = True
        mock_queue = MagicMock()
        mock_ctx = MagicMock()
        mock_ctx.Queue.return_value = mock_queue
        mock_ctx.Process.return_value = mock_process

        with patch(_PATCH_MULTIPROCESSING) as mock_mp:
            mock_mp.get_context.return_value = mock_ctx
            result = _run_service_isolated("proj", "bucket", "svc", "2024-01-31")

        mock_process.terminate.assert_called_once()
        assert result["manifest_ok"] is False
        assert "timed out" in result["error"]


# ── run_rollup ────────────────────────────────────────────────────────────────

_PATCH_RUN_ISOLATED = "deployment_api.scripts.data_status_rollup_worker._run_service_isolated"


def _isolated_result(service: str, *, manifest_ok: bool = True, coverage_ok: bool = True) -> dict[str, object]:
    return {
        "service": service,
        "manifest_ok": manifest_ok,
        "coverage_ok": coverage_ok,
        "manifest_elapsed_s": 1.0,
        "coverage_elapsed_s": 1.0,
        "size_compressed": 10,
        "size_uncompressed": 20,
        "coverage_size_compressed": 5,
        "asset_groups_n": 1,
        "manifest_error": None,
        "coverage_error": None,
        "error": None,
    }


class TestRunRollup:
    def test_success_returns_zero(self) -> None:
        from deployment_api.scripts.data_status_rollup_worker import run_rollup

        with (
            patch(_PATCH_LOG_EVENT),
            patch(_PATCH_RUN_ISOLATED, return_value=_isolated_result("instruments-service")),
        ):
            result = run_rollup("test-project", "test-bucket", ["instruments-service"])

        assert result == 0

    def test_all_failures_returns_one(self) -> None:
        from deployment_api.scripts.data_status_rollup_worker import run_rollup

        failed = _isolated_result("instruments-service", manifest_ok=False, coverage_ok=False)
        failed["manifest_error"] = "GCS error"
        failed["coverage_error"] = "GCS error"

        with (
            patch(_PATCH_LOG_EVENT),
            patch(_PATCH_RUN_ISOLATED, return_value=failed),
        ):
            result = run_rollup("test-project", "test-bucket", ["instruments-service"])

        assert result == 1

    def test_multiple_services_all_succeed(self) -> None:
        from deployment_api.scripts.data_status_rollup_worker import run_rollup

        with (
            patch(_PATCH_LOG_EVENT),
            patch(_PATCH_RUN_ISOLATED, side_effect=lambda *a, **_kw: _isolated_result(a[2])),
        ):
            result = run_rollup("test-project", "test-bucket", ["svc-a", "svc-b"])

        assert result == 0

    def test_partial_failure_returns_zero(self) -> None:
        from deployment_api.scripts.data_status_rollup_worker import run_rollup

        def fake_isolated(project_id: str, bucket: str, service: str, end_date: str) -> dict[str, object]:
            if service == "svc-fail":
                return _isolated_result(service, manifest_ok=False, coverage_ok=False)
            return _isolated_result(service)

        with (
            patch(_PATCH_LOG_EVENT),
            patch(_PATCH_RUN_ISOLATED, side_effect=fake_isolated),
        ):
            result = run_rollup("test-project", "test-bucket", ["svc-fail", "svc-ok"])

        assert result == 0

    def test_a_doomed_service_does_not_block_the_next_one(self) -> None:
        # The whole point of the isolation: MTDS-style failure must not prevent
        # market-data-processing-service (queued right after it) from being attempted.
        from deployment_api.scripts.data_status_rollup_worker import run_rollup

        calls: list[str] = []

        def fake_isolated(project_id: str, bucket: str, service: str, end_date: str) -> dict[str, object]:
            calls.append(service)
            if service == "market-tick-data-service":
                return _isolated_result(service, manifest_ok=False, coverage_ok=False)
            return _isolated_result(service)

        with (
            patch(_PATCH_LOG_EVENT),
            patch(_PATCH_RUN_ISOLATED, side_effect=fake_isolated),
        ):
            result = run_rollup(
                "test-project",
                "test-bucket",
                ["instruments-service", "market-tick-data-service", "market-data-processing-service"],
            )

        assert calls == ["instruments-service", "market-tick-data-service", "market-data-processing-service"]
        assert result == 0

    def test_service_processed_event_emitted(self) -> None:
        from deployment_api.scripts.data_status_rollup_worker import run_rollup

        log_calls: list[tuple[str]] = []

        def capture_log(event_type: str, **kwargs: object) -> None:
            log_calls.append((event_type,))

        with (
            patch(_PATCH_LOG_EVENT, side_effect=capture_log),
            patch(_PATCH_RUN_ISOLATED, return_value=_isolated_result("instruments-service")),
        ):
            run_rollup("test-project", "test-bucket", ["instruments-service"])

        event_types = [e[0] for e in log_calls]
        assert "SERVICE_PROCESSED" in event_types

    def test_service_failed_event_on_all_failures(self) -> None:
        from deployment_api.scripts.data_status_rollup_worker import run_rollup

        failed = _isolated_result("instruments-service", manifest_ok=False, coverage_ok=False)
        failed["manifest_error"] = "boom"
        failed["coverage_error"] = "boom"
        log_calls: list[tuple[str]] = []

        def capture_log(event_type: str, **kwargs: object) -> None:
            log_calls.append((event_type,))

        with (
            patch(_PATCH_LOG_EVENT, side_effect=capture_log),
            patch(_PATCH_RUN_ISOLATED, return_value=failed),
        ):
            run_rollup("test-project", "test-bucket", ["instruments-service"])

        event_types = [e[0] for e in log_calls]
        assert "SERVICE_FAILED" in event_types

    def test_coverage_failure_does_not_fail_manifest(self) -> None:
        from deployment_api.scripts.data_status_rollup_worker import run_rollup

        result_val = _isolated_result("instruments-service", coverage_ok=False)
        result_val["coverage_error"] = "storage error"

        with (
            patch(_PATCH_LOG_EVENT),
            patch(_PATCH_RUN_ISOLATED, return_value=result_val),
        ):
            result = run_rollup("test-project", "test-bucket", ["instruments-service"])

        assert result == 0

    def test_empty_services_list(self) -> None:
        from deployment_api.scripts.data_status_rollup_worker import run_rollup

        with patch(_PATCH_LOG_EVENT), patch(_PATCH_RUN_ISOLATED):
            result = run_rollup("test-project", "test-bucket", [])

        assert result == 1

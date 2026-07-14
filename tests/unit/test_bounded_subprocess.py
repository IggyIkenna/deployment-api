"""Unit tests for deployment_api/utils/bounded_subprocess.py.

Mirrors the mocking style already proven in ``test_rollup_worker.py``'s
``TestRunServiceIsolated`` (patch the module-level ``multiprocessing`` name
so no real process is ever spawned in the test suite).
"""

from __future__ import annotations

from unittest.mock import MagicMock, patch

import pytest

from deployment_api.utils.bounded_subprocess import (
    BoundedSubprocessError,
    _child_entrypoint,
    _set_child_memory_rlimit,
    run_bounded,
)

_PATCH_MULTIPROCESSING = "deployment_api.utils.bounded_subprocess.multiprocessing"


def _sample_fn(a: int, b: int) -> int:
    return a + b


class TestSetChildMemoryRlimit:
    def test_calls_setrlimit_with_same_soft_and_hard(self) -> None:
        with patch("deployment_api.utils.bounded_subprocess.resource") as mock_resource:
            mock_resource.RLIMIT_AS = "RLIMIT_AS_SENTINEL"
            _set_child_memory_rlimit(4096)

        mock_resource.setrlimit.assert_called_once_with("RLIMIT_AS_SENTINEL", (4096, 4096))


class TestChildEntrypoint:
    def test_success_puts_ok_result(self) -> None:
        result_queue: MagicMock = MagicMock()
        with patch("deployment_api.utils.bounded_subprocess._set_child_memory_rlimit"):
            _child_entrypoint(_sample_fn, (2, 3), {}, 1024, result_queue)

        result = result_queue.put.call_args.args[0]
        assert result == {"ok": True, "value": 5}

    def test_target_exception_puts_error_result(self) -> None:
        def _boom() -> None:
            raise RuntimeError("kaboom")

        result_queue: MagicMock = MagicMock()
        with patch("deployment_api.utils.bounded_subprocess._set_child_memory_rlimit"):
            _child_entrypoint(_boom, (), {}, 1024, result_queue)

        result = result_queue.put.call_args.args[0]
        assert result["ok"] is False
        assert "kaboom" in result["error"]

    def test_rlimit_failure_does_not_abort_the_call(self) -> None:
        # A host where RLIMIT_AS can't be set (observed on macOS dev boxes;
        # real Cloud Run/Linux sets it fine) must still let the callable run,
        # just unprotected by the ceiling.
        result_queue: MagicMock = MagicMock()
        with patch(
            "deployment_api.utils.bounded_subprocess._set_child_memory_rlimit",
            side_effect=ValueError("current limit exceeds maximum limit"),
        ):
            _child_entrypoint(_sample_fn, (2, 3), {}, 1024, result_queue)

        result = result_queue.put.call_args.args[0]
        assert result == {"ok": True, "value": 5}


class TestRunBounded:
    def test_reads_result_from_queue_on_clean_exit(self) -> None:
        mock_process = MagicMock()
        mock_process.is_alive.return_value = False
        mock_process.exitcode = 0
        mock_queue = MagicMock()
        mock_queue.empty.return_value = False
        mock_queue.get.return_value = {"ok": True, "value": 42}
        mock_ctx = MagicMock()
        mock_ctx.Queue.return_value = mock_queue
        mock_ctx.Process.return_value = mock_process

        with patch(_PATCH_MULTIPROCESSING) as mock_mp:
            mock_mp.get_context.return_value = mock_ctx
            result = run_bounded(_sample_fn, 1, 2, rlimit_bytes=1024, timeout_s=5.0, name="test-child")

        assert result == 42
        mock_process.start.assert_called_once()
        mock_process.join.assert_any_call(timeout=5.0)

    def test_target_error_raises_bounded_subprocess_error(self) -> None:
        mock_process = MagicMock()
        mock_process.is_alive.return_value = False
        mock_process.exitcode = 1
        mock_queue = MagicMock()
        mock_queue.empty.return_value = False
        mock_queue.get.return_value = {"ok": False, "error": "MemoryError: boom"}
        mock_ctx = MagicMock()
        mock_ctx.Queue.return_value = mock_queue
        mock_ctx.Process.return_value = mock_process

        with patch(_PATCH_MULTIPROCESSING) as mock_mp:
            mock_mp.get_context.return_value = mock_ctx
            with pytest.raises(BoundedSubprocessError, match="MemoryError"):
                run_bounded(_sample_fn, 1, 2, rlimit_bytes=1024, timeout_s=5.0)

    def test_synthesizes_failure_when_child_dies_without_reporting(self) -> None:
        # e.g. OOM-killed before it could reach result_queue.put.
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
            with pytest.raises(BoundedSubprocessError, match="-9"):
                run_bounded(_sample_fn, 1, 2, rlimit_bytes=1024, timeout_s=5.0)

    def test_terminates_and_raises_on_timeout(self) -> None:
        mock_process = MagicMock()
        mock_process.is_alive.return_value = True
        mock_queue = MagicMock()
        mock_ctx = MagicMock()
        mock_ctx.Queue.return_value = mock_queue
        mock_ctx.Process.return_value = mock_process

        with patch(_PATCH_MULTIPROCESSING) as mock_mp:
            mock_mp.get_context.return_value = mock_ctx
            with pytest.raises(BoundedSubprocessError, match="timed out"):
                run_bounded(_sample_fn, 1, 2, rlimit_bytes=1024, timeout_s=1.0)

        mock_process.terminate.assert_called_once()

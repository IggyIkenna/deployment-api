"""Generic resource-bounded child-process runner.

Factored out of the isolation pattern first shipped in commit ``8d260ad``
(``deployment_api/scripts/data_status_rollup_worker.py`` /
``_run_service_isolated`` + ``_child_build_and_write_service``) so any other
request path that risks a large, hard-to-bound memory allocation can reuse
the exact same defense: run the risky compute in its own throwaway
``multiprocessing.get_context("spawn")`` child process, capped with
``resource.setrlimit(RLIMIT_AS, ...)``, so an estimate that undershoots
raises a catchable error INSIDE the child instead of the platform OOM-killer
taking down the parent gunicorn worker (and every other request sharing its
process/container).

``target`` MUST be a module-level (picklable-by-reference) callable — under
the ``spawn`` start method, closures / bound methods / lambdas do not
pickle reliably. The established pattern (mirrored here) is: the callable
reconstructs whatever service object it needs FRESH inside the child (cheap
— no GCS/network at construction time) rather than the caller trying to
ship a live instance across the process boundary.
"""

from __future__ import annotations

import contextlib
import logging
import multiprocessing
import resource
from collections.abc import Callable
from typing import cast

logger = logging.getLogger(__name__)

# Wall-clock backstop in addition to the RLIMIT_AS ceiling — a child that
# hangs (not necessarily OOMing) still must not block the caller forever.
DEFAULT_TIMEOUT_S = 120.0


class BoundedSubprocessError(RuntimeError):
    """The child process failed to produce a result.

    Covers a caught ``MemoryError`` from the rlimit ceiling, any other
    exception the target callable raised, a hard OOM-kill (no exception
    possible — the child just vanishes), or a wall-clock timeout. The
    caller decides what to do next (e.g. serve a stale cached fallback, or
    return a structured "narrow your request" error) rather than letting a
    doomed allocation take the whole process down.
    """


def _set_child_memory_rlimit(limit_bytes: int) -> None:
    """Cap this process's virtual address space.

    Once exceeded, the allocator fails and Python raises a catchable
    ``MemoryError`` instead of the process silently growing until the
    platform's OOM killer terminates the whole container.
    """
    resource.setrlimit(resource.RLIMIT_AS, (limit_bytes, limit_bytes))


def _child_entrypoint(
    fn: Callable[..., object],
    args: tuple[object, ...],
    kwargs: dict[str, object],
    rlimit_bytes: int,
    result_queue: multiprocessing.Queue[dict[str, object]],
) -> None:
    """Spawned-child entrypoint: set the rlimit, run ``fn``, always report back.

    Always puts exactly one result dict on the queue (even on failure) so
    the parent never blocks past its join timeout waiting for a message
    that will never arrive.
    """
    try:
        _set_child_memory_rlimit(rlimit_bytes)
    except (ValueError, OSError) as e:  # best-effort guard: proceed unprotected rather than abort the call
        logger.warning("bounded_subprocess: could not set memory rlimit (%s) — proceeding unprotected", e)

    try:
        value = fn(*args, **kwargs)
    except Exception as e:  # must never escape uncaught; the parent needs a result either way
        with contextlib.suppress(Exception):
            result_queue.put({"ok": False, "error": f"{type(e).__name__}: {e}"})
        return

    with contextlib.suppress(Exception):
        result_queue.put({"ok": True, "value": value})


def run_bounded[T](
    fn: Callable[..., T],
    *args: object,
    rlimit_bytes: int,
    timeout_s: float = DEFAULT_TIMEOUT_S,
    name: str = "bounded-child",
    **kwargs: object,
) -> T:
    """Run ``fn(*args, **kwargs)`` in a spawned child capped at ``rlimit_bytes``.

    Raises :class:`BoundedSubprocessError` if the child times out, is
    OOM-killed / crashes before reporting a result, or the callable raised
    (including a rlimit-triggered ``MemoryError``) — the parent process and
    every other request it is serving survives regardless of which of these
    happened.
    """
    ctx = multiprocessing.get_context("spawn")
    result_queue: multiprocessing.Queue[dict[str, object]] = ctx.Queue()
    process = ctx.Process(
        target=_child_entrypoint,
        args=(fn, args, kwargs, rlimit_bytes, result_queue),
        daemon=True,
        name=name,
    )
    process.start()
    process.join(timeout=timeout_s)

    if process.is_alive():
        process.terminate()
        process.join(timeout=10)
        if process.is_alive():
            process.kill()
        raise BoundedSubprocessError(f"{name} timed out after {timeout_s}s")

    if not result_queue.empty():
        result = result_queue.get()
        if result.get("ok"):
            return cast(T, result["value"])
        raise BoundedSubprocessError(str(result.get("error", "unknown child failure")))

    raise BoundedSubprocessError(
        f"{name} exited with code {process.exitcode} before reporting a result (likely OOM-killed)"
    )

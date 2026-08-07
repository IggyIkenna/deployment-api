"""Shared in-flight guard for the heavy "cockpit" dashboard rollup handlers.

``deployment_api_sigabrt_crash_loop_2026_07_24.md``'s OOM-kill sub-issue traced every
confirmed ``Container terminated on signal 9`` event to a cold multi-panel "cockpit"
dashboard-load burst (``/api/repo-ci/overview`` + ``/api/health/overview`` +
``/api/vm-deployments``), and the ``130c3a2`` memory-profile instrumentation
(``utils/request_memory_profiling.py``) — live in production since 2026-08-04 —
confirmed the attribution: ``repo_ci.get_overview`` peaks at 0.36-2.9 GiB per call and
``health_overview.get_health_overview`` at ~2.6 GiB, with the container sitting only
0.3%-3.8% under its 16384 MiB ceiling when the platform OOM-kills it.

Neither handler has any cross-request guard (only ``repo_ci``'s per-call
``asyncio.Semaphore(_REPO_CONCURRENCY)`` bounding intra-call GitHub API fan-out), so a
cockpit reload that fires both endpoints simultaneously lets two multi-GiB builds stack
on top of the ~5 GiB baseline and the platform kills the whole container. This module is
the shared backstop: ONE build slot per worker (2 workers -> 2 container-wide), with a
hard inflight cap that sheds excess with ``503 + Retry-After`` instead of queueing
unboundedly — mirroring ``routes/data_status/_deploy_turbo.py``'s drilldown guard (its
``_DRILLDOWN_BUILD_SLOTS=1`` rationale: a per-worker semaphore means the container-wide
ceiling is ``slots * WORKERS``; keep slots = 1 so the bound stays sane if ``WORKERS``
changes) and ``services/catalogue_lifecycle.py``'s ``_build_semaphore`` pattern.
"""

from __future__ import annotations

import asyncio
import logging
from collections.abc import AsyncIterator
from contextlib import asynccontextmanager

from fastapi import HTTPException

logger = logging.getLogger(__name__)

# One heavy build at a time per worker. ``WORKERS=2`` in the Cloud Run deploy, so the
# container-wide ceiling is 2 concurrent builds — the level that fits under the 16 GiB
# limit given a ~2.9 GiB per-build peak (see module docstring). If ``WORKERS`` changes,
# keep SLOTS * WORKERS <= 2 (or re-measure the per-build peak).
_COCKPIT_BUILD_SLOTS = 1
# Hard cap on building+queued so a burst fails fast (503 + Retry-After) rather than
# piling an unbounded wait queue that holds RAM waiting for a slot.
_COCKPIT_MAX_INFLIGHT = 3

_cockpit_build_semaphore = asyncio.Semaphore(_COCKPIT_BUILD_SLOTS)
_cockpit_inflight = 0


@asynccontextmanager
async def cockpit_build_slot(handler: str) -> AsyncIterator[None]:
    """Bound concurrent heavy cockpit builds; shed with 503 + Retry-After when saturated.

    A cockpit page-load fans out several heavy rollups at once; without a cross-handler
    cap each holds 0.4-2.9 GiB and the container OOM-kills. Acquire the slot BEFORE any
    heavy work; the 503 is raised before queueing so a burst degrades gracefully.
    """
    global _cockpit_inflight
    # Shed BEFORE queueing when too many builds are already in flight. (No ``await``
    # between the check and the increment — asyncio is cooperative, so this read-modify
    # is atomic, same as ``_deploy_turbo._drilldown_inflight``.)
    if _cockpit_inflight >= _COCKPIT_MAX_INFLIGHT:
        logger.warning("cockpit build at capacity (%d in flight); shedding %s", _cockpit_inflight, handler)
        raise HTTPException(
            status_code=503,
            detail=("Cockpit overview is at capacity (too many concurrent builds). Retry shortly."),
            headers={"Retry-After": "5"},
        )
    _cockpit_inflight += 1
    try:
        # ``_COCKPIT_BUILD_SLOTS`` concurrent builds max (bounds peak RAM); the rest wait
        # here cooperatively without blocking the event loop.
        async with _cockpit_build_semaphore:
            yield
    finally:
        _cockpit_inflight -= 1


__all__ = ["cockpit_build_slot"]

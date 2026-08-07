"""Unit tests for the shared cockpit-build guard (``utils/cockpit_build_guard.py``).

The guard bounds concurrent heavy "cockpit" dashboard rollups
(``repo_ci.get_overview`` + ``health_overview.get_health_overview``) that peak at
0.4-2.9 GiB per call and OOM-kill the 16 GiB container when they stack — the live
``memory-profile`` attribution surfaced on 2026-08-06 from the ``130c3a2``
instrumentation (deployment_api_sigabrt_crash_loop_2026_07_24.md OOM sub-issue).
Mirrors ``routes/data_status/_deploy_turbo.py``'s drilldown guard test shape.
"""

from __future__ import annotations

import asyncio

import pytest
from fastapi import HTTPException

from deployment_api.utils import cockpit_build_guard as guard


def _fresh_state() -> None:
    """Reset module guard state + rebind the semaphore to the current test's loop.

    The module-level ``asyncio.Semaphore`` is ``_LoopBoundMixin``-bound to the first
    loop that uses it; pytest-asyncio (``asyncio_mode=auto``) hands each test its own
    loop, so a fresh semaphore per test avoids the cross-loop RuntimeError.
    """
    guard._cockpit_inflight = 0
    guard._cockpit_build_semaphore = asyncio.Semaphore(guard._COCKPIT_BUILD_SLOTS)  # pyright: ignore[reportPrivateUsage]


class TestCockpitBuildGuard:
    async def test_normal_path_acquires_and_releases(self) -> None:
        _fresh_state()
        async with guard.cockpit_build_slot("repo_ci.get_overview"):
            assert guard._cockpit_inflight == 1  # pyright: ignore[reportPrivateUsage]
        assert guard._cockpit_inflight == 0  # pyright: ignore[reportPrivateUsage]

    async def test_sheds_with_503_when_at_capacity(self) -> None:
        _fresh_state()
        guard._cockpit_inflight = guard._COCKPIT_MAX_INFLIGHT  # pyright: ignore[reportPrivateUsage]
        with pytest.raises(HTTPException) as exc_info:
            async with guard.cockpit_build_slot("repo_ci.get_overview"):
                pass  # pragma: no cover — the shed path raises before the body runs
        assert exc_info.value.status_code == 503
        assert exc_info.value.headers == {"Retry-After": "5"}
        assert guard._cockpit_inflight == guard._COCKPIT_MAX_INFLIGHT  # pyright: ignore[reportPrivateUsage]

    async def test_semaphore_bounds_concurrent_builds(self) -> None:
        _fresh_state()
        held = [await guard._cockpit_build_semaphore.acquire() for _ in range(guard._COCKPIT_BUILD_SLOTS)]  # pyright: ignore[reportPrivateUsage]
        assert all(held), "test setup: expected to acquire every guard slot"
        try:
            # One build already in flight below the shed threshold (inflight=1 < MAX) —
            # a second must wait on the semaphore rather than proceed concurrently.
            guard._cockpit_inflight = 1  # pyright: ignore[reportPrivateUsage]
            cm = guard.cockpit_build_slot("health_overview.get_health_overview")
            with pytest.raises(asyncio.TimeoutError):
                await asyncio.wait_for(cm.__aenter__(), timeout=0.05)
        finally:
            for _ in held:
                guard._cockpit_build_semaphore.release()  # pyright: ignore[reportPrivateUsage]

    async def test_inflight_decrements_after_exception_inside_body(self) -> None:
        _fresh_state()
        with pytest.raises(RuntimeError):
            async with guard.cockpit_build_slot("repo_ci.get_overview"):
                raise RuntimeError("boom")
        assert guard._cockpit_inflight == 0  # pyright: ignore[reportPrivateUsage]

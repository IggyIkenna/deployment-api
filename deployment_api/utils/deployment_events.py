"""
Deployment events: notify UI when deployment state is updated (low-latency push).
"""

import asyncio
import contextlib
import logging
from collections import defaultdict
from collections.abc import AsyncGenerator

logger = logging.getLogger(__name__)
_sse_queues: dict[str, set[asyncio.Queue[str]]] = defaultdict(set)
_lock = asyncio.Lock()
_sync_queue: asyncio.Queue[str] | None = None


def _get_sync_queue():
    global _sync_queue
    if _sync_queue is None:
        try:
            _sync_queue = asyncio.Queue()
        except RuntimeError:
            _sync_queue = None
    return _sync_queue


async def subscribe(deployment_id: str) -> AsyncGenerator[str]:
    q: asyncio.Queue[str] = asyncio.Queue()
    async with _lock:
        _sse_queues[deployment_id].add(q)
    try:
        while True:
            msg: str = await q.get()
            yield msg
    finally:
        async with _lock:
            _sse_queues[deployment_id].discard(q)
            if not _sse_queues[deployment_id]:
                del _sse_queues[deployment_id]


async def _broadcast(deployment_id: str):
    async with _lock:
        queues = list(_sse_queues.get(deployment_id, []))
    for q in queues:
        with contextlib.suppress(asyncio.QueueFull):
            q.put_nowait("updated")


async def notify_deployment_updated(deployment_id: str):
    try:
        from deployment_api.routes.deployment_caching import invalidate_deployment_state_cache

        await invalidate_deployment_state_cache(deployment_id)
    except (OSError, ValueError, RuntimeError) as e:
        logger.debug("Cache invalidation on notify: %s", e)
    await _broadcast(deployment_id)


def notify_deployment_updated_sync(deployment_id: str):
    try:
        _q = _get_sync_queue()
        if _q is not None:
            _q.put_nowait(deployment_id)
    except (AttributeError, RuntimeError) as e:
        logger.debug("Sync queue put: %s", e)
    try:
        from unified_trading_library import get_queue_client  # noqa: imports-inside-functions

        get_queue_client().publish("deployment:updated", deployment_id.encode())
    except (OSError, ValueError, RuntimeError) as e:
        logger.debug("Queue publish: %s", e)


async def drain_sync_queue():
    while True:
        try:
            _q = _get_sync_queue()
            if _q is None:
                await asyncio.sleep(1.0)
                continue
            # ``wait_for(.., timeout=1.0)`` lets the loop check periodically
            # whether the queue has been re-bound (e.g. after a fresh
            # ``get_event_loop`` in tests) without blocking forever.
            # ``TimeoutError`` here is the normal idle path — there's just
            # no item yet — NOT an error worth logging at WARNING. Pre-fix
            # this emitted ~1 warning/sec and flooded the deployment-api
            # log even when the API was completely idle.
            try:
                deployment_id = await asyncio.wait_for(_q.get(), timeout=1.0)
            except TimeoutError:
                continue
            await _broadcast(deployment_id)
            try:
                from deployment_api.routes.deployment_caching import (
                    invalidate_deployment_state_cache,
                )

                await invalidate_deployment_state_cache(deployment_id)
            except (OSError, ValueError, RuntimeError) as e:
                logger.warning("Unexpected error during operation: %s", e, exc_info=True)
                pass
        except (OSError, ValueError, RuntimeError) as e:
            logger.warning("Drain sync queue: %s", e)
            await asyncio.sleep(1.0)

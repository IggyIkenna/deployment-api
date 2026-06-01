"""Per-endpoint rate-limit FastAPI dependency factory.

Provides endpoint-level tightening on top of the global RateLimitMiddleware
(60 req/min). Use for high-value or expensive endpoints that should be
capped below the global limit.

Usage:
    from deployment_api.rate_limiting import endpoint_rate_limit

    @router.get("/expensive")
    async def expensive_handler(
        _rl: None = Depends(endpoint_rate_limit(10)),  # 10/min per IP
    ) -> dict[str, object]:
        ...

The dependency is a no-op when DISABLE_AUTH=true (test mode).
"""

from __future__ import annotations

import time
from collections import deque
from collections.abc import Callable, Coroutine

from fastapi import HTTPException, Request

from deployment_api.auth import DISABLE_AUTH

_WINDOW_SECS = 60.0

# Registry: (client_ip, endpoint_key) → sliding-window timestamps
_ENDPOINT_WINDOWS: dict[tuple[str, str], deque[float]] = {}


def endpoint_rate_limit(
    requests_per_minute: int,
) -> Callable[..., Coroutine[object, object, None]]:
    """FastAPI dependency factory: enforce a per-endpoint per-IP rate limit.

    Args:
        requests_per_minute: Max calls allowed per IP per 60-second window.

    Returns:
        A FastAPI dependency that raises HTTP 429 on excess.
    """

    async def _check(request: Request) -> None:
        if DISABLE_AUTH:
            return

        client_ip = request.client.host if request.client else "unknown"
        endpoint_key = request.url.path
        bucket_key = (client_ip, endpoint_key)
        now = time.time()
        cutoff = now - _WINDOW_SECS

        if bucket_key not in _ENDPOINT_WINDOWS:
            _ENDPOINT_WINDOWS[bucket_key] = deque()
        bucket = _ENDPOINT_WINDOWS[bucket_key]

        while bucket and bucket[0] < cutoff:
            bucket.popleft()

        if len(bucket) >= requests_per_minute:
            raise HTTPException(
                status_code=429,
                detail=f"Rate limit exceeded: max {requests_per_minute} requests/minute for this endpoint",
                headers={"Retry-After": "60"},
            )

        bucket.append(now)

    return _check

"""
Middleware configuration for the FastAPI application.

Handles CORS setup and other middleware configurations.
"""

import threading
import time
import uuid
from collections import deque
from collections.abc import Awaitable, Callable

from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from starlette.middleware.base import BaseHTTPMiddleware
from starlette.requests import Request
from starlette.responses import JSONResponse, Response
from starlette.types import ASGIApp

from deployment_api import settings
from deployment_api.metrics import PROCESSING_LATENCY, RECORDS_PROCESSED

_RATE_LIMIT_EXEMPT_PREFIXES = (
    "/health",
    "/api/health",
    "/readiness",
    "/api/readiness",
    "/metrics",
)

_rate_limit_lock = threading.Lock()
_rate_limit_window: dict[str, deque[float]] = {}

_RequestResponseEndpoint = Callable[[Request], Awaitable[Response]]


class RateLimiterMiddleware(BaseHTTPMiddleware):
    """Per-IP sliding-window rate limiter.

    Allows up to `max_requests` requests per `window_seconds`. Health and
    metrics endpoints are exempt. Returns HTTP 429 with Retry-After header
    when a client exceeds the limit.
    """

    def __init__(
        self,
        app: ASGIApp,
        max_requests: int = 60,
        window_seconds: float = 60.0,
    ) -> None:
        super().__init__(app)
        self._max_requests = max_requests
        self._window_seconds = window_seconds

    def _get_client_ip(self, request: Request) -> str:
        forwarded_for = request.headers.get("X-Forwarded-For")
        if forwarded_for:
            return forwarded_for.split(",")[0].strip()
        return request.client.host if request.client else "unknown"

    async def dispatch(self, request: Request, call_next: _RequestResponseEndpoint) -> Response:
        path = request.url.path
        if any(
            path == prefix or path.startswith(prefix + "/")
            for prefix in _RATE_LIMIT_EXEMPT_PREFIXES
        ):
            return await call_next(request)

        client_ip = self._get_client_ip(request)
        now = time.monotonic()
        cutoff = now - self._window_seconds

        with _rate_limit_lock:
            if client_ip not in _rate_limit_window:
                _rate_limit_window[client_ip] = deque()
            timestamps = _rate_limit_window[client_ip]
            while timestamps and timestamps[0] < cutoff:
                timestamps.popleft()
            if len(timestamps) >= self._max_requests:
                retry_after = int(self._window_seconds - (now - timestamps[0])) + 1
                return JSONResponse(
                    status_code=429,
                    content={"detail": "Too Many Requests", "retry_after_seconds": retry_after},
                    headers={"Retry-After": str(retry_after)},
                )
            timestamps.append(now)

        return await call_next(request)


class CorrelationIdMiddleware(BaseHTTPMiddleware):
    """Propagate or generate X-Correlation-ID for every request."""

    async def dispatch(self, request: Request, call_next: _RequestResponseEndpoint) -> Response:
        correlation_id = request.headers.get("X-Correlation-ID", str(uuid.uuid4()))
        request.state.correlation_id = correlation_id
        response = await call_next(request)
        response.headers["X-Correlation-ID"] = correlation_id
        return response


class PrometheusMiddleware(BaseHTTPMiddleware):
    """ASGI middleware that records request counts and latency into Prometheus metrics.

    Uses the existing RECORDS_PROCESSED Counter and PROCESSING_LATENCY Histogram
    from deployment_api.metrics — no additional Prometheus dependencies required.
    """

    def __init__(self, app: ASGIApp, service_name: str = "deployment-api") -> None:
        super().__init__(app)
        self.service_name = service_name

    async def dispatch(self, request: Request, call_next: _RequestResponseEndpoint) -> Response:
        start = time.perf_counter()
        response = await call_next(request)
        duration = time.perf_counter() - start
        status = "success" if response.status_code < 500 else "error"
        RECORDS_PROCESSED.labels(status=status).inc()
        PROCESSING_LATENCY.observe(duration)
        return response


_RATE_LIMIT_EXEMPT_PREFIXES = ("/health", "/metrics", "/infra/health")
_RATE_LIMIT_WINDOW_SECS = 60.0


class RateLimitMiddleware(BaseHTTPMiddleware):
    """Sliding-window per-IP rate limiter.

    Exempt paths: /health*, /metrics, /infra/health.
    Default: 60 requests / 60 seconds per client IP.
    Returns HTTP 429 with Retry-After header on exceed.
    """

    def __init__(self, app: ASGIApp, requests_per_minute: int = 60) -> None:
        super().__init__(app)
        self.limit = requests_per_minute
        self._windows: dict[str, deque[float]] = {}

    async def dispatch(self, request: Request, call_next: _RequestResponseEndpoint) -> Response:
        path: str = request.url.path
        if any(path.startswith(p) for p in _RATE_LIMIT_EXEMPT_PREFIXES):
            return await call_next(request)

        client_ip: str = request.client.host if request.client else "unknown"
        now = time.time()
        cutoff = now - _RATE_LIMIT_WINDOW_SECS

        if client_ip not in self._windows:
            self._windows[client_ip] = deque()
        bucket = self._windows[client_ip]

        while bucket and bucket[0] < cutoff:
            bucket.popleft()

        if len(bucket) >= self.limit:
            _body = (
                '{"error":{"code":"RATE_LIMITED","message":"Too many requests"},"retry_after":60}'
            )
            return Response(
                content=_body,
                status_code=429,
                media_type="application/json",
                headers={"Retry-After": "60"},
            )

        bucket.append(now)
        return await call_next(request)


def configure_middleware(app: FastAPI) -> None:
    """Configure middleware for the FastAPI application."""
    # Build CORS allowed origins from config
    _api_port = settings.API_PORT
    _frontend_port = settings.FRONTEND_PORT

    # Production domains
    _cors_origins_env = settings.CORS_ALLOWED_ORIGINS
    production_origins = _cors_origins_env.split(",") if _cors_origins_env else []

    # Development origins (only in development mode)
    dev_origins: list[str] = []
    if settings.DEPLOYMENT_ENV == "development":
        _static_dev_origins = [o.strip() for o in settings.CORS_DEV_ORIGINS.split(",") if o.strip()]
        dev_origins = [
            *_static_dev_origins,
            f"http://localhost:{_frontend_port}",
            f"http://127.0.0.1:{_frontend_port}",
            f"http://localhost:{_api_port}",
        ]

    allowed_origins = production_origins + dev_origins

    # Only allow specific Cloud Run services (not wildcard)
    # Configure this via CORS_ALLOWED_CLOUD_RUN env var
    allowed_cloud_run = settings.CORS_ALLOWED_CLOUD_RUN
    origin_regex = None
    if allowed_cloud_run:
        # Example: "deployment-dashboard,execution-service" -> regex for those specific services
        services = allowed_cloud_run.split(",")
        if services:
            pattern = "|".join(f"{service.strip()}-[a-z0-9]{{10}}" for service in services)
            origin_regex = rf"https://({pattern})-[a-z0-9]{{10}}\.run\.app"

    # Configure CORS with stricter settings
    if origin_regex:
        app.add_middleware(
            CORSMiddleware,
            allow_origins=allowed_origins,
            allow_credentials=True,
            allow_methods=["GET", "POST", "PUT", "DELETE", "OPTIONS"],
            allow_headers=["Authorization", "Content-Type", "X-API-Key", "X-Request-ID"],
            max_age=3600,
            allow_origin_regex=origin_regex,
        )
    else:
        app.add_middleware(
            CORSMiddleware,
            allow_origins=allowed_origins,
            allow_credentials=True,
            allow_methods=["GET", "POST", "PUT", "DELETE", "OPTIONS"],
            allow_headers=["Authorization", "Content-Type", "X-API-Key", "X-Request-ID"],
            max_age=3600,
        )

    app.add_middleware(RateLimiterMiddleware)

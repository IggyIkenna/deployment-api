"""Unit tests for the RateLimitMiddleware (per-IP sliding-window, 60 req/min)."""

from __future__ import annotations

import pytest
from fastapi import FastAPI
from fastapi.testclient import TestClient

from deployment_api.middleware import RateLimitMiddleware


def _make_app(limit: int = 3) -> FastAPI:
    """Minimal FastAPI app with a low-limit rate limiter for test isolation."""
    app = FastAPI()
    app.add_middleware(RateLimitMiddleware, requests_per_minute=limit)

    @app.get("/api/test")
    async def test_route() -> dict[str, str]:
        return {"ok": "true"}

    @app.get("/health")
    async def health() -> dict[str, str]:
        return {"status": "ok"}

    return app


@pytest.fixture
def client() -> TestClient:
    return TestClient(_make_app(limit=3), raise_server_exceptions=False)


def test_normal_request_succeeds(client: TestClient) -> None:
    resp = client.get("/api/test")
    assert resp.status_code == 200


def test_within_limit_all_succeed(client: TestClient) -> None:
    for _ in range(3):
        resp = client.get("/api/test")
        assert resp.status_code == 200


def test_exceeding_limit_returns_429(client: TestClient) -> None:
    for _ in range(3):
        client.get("/api/test")
    resp = client.get("/api/test")  # 4th request → exceeds limit=3
    assert resp.status_code == 429


def test_429_response_contains_rate_limited_error(client: TestClient) -> None:
    for _ in range(3):
        client.get("/api/test")
    resp = client.get("/api/test")
    body = resp.json()
    assert body["error"]["code"] == "RATE_LIMITED"


def test_429_response_has_retry_after_header(client: TestClient) -> None:
    for _ in range(3):
        client.get("/api/test")
    resp = client.get("/api/test")
    assert resp.headers.get("retry-after") == "60"


def test_health_endpoint_is_exempt_from_rate_limiting() -> None:
    """Health check should never be rate-limited (load balancer probes)."""
    app = _make_app(limit=1)
    client = TestClient(app, raise_server_exceptions=False)
    # First request uses the 1 allowed slot on /api/test
    client.get("/api/test")
    # Health endpoint must still respond 200 even after limit consumed
    for _ in range(5):
        resp = client.get("/health")
        assert resp.status_code == 200

"""Unit tests for deployment_api.rate_limiting.endpoint_rate_limit."""

from __future__ import annotations

import asyncio
from collections.abc import Generator
from unittest.mock import MagicMock, patch

import pytest
from fastapi import HTTPException

from deployment_api.rate_limiting import _ENDPOINT_WINDOWS, endpoint_rate_limit

_PATCH_DISABLE_AUTH = "deployment_api.rate_limiting.DISABLE_AUTH"


def _make_request(ip: str = "1.2.3.4", path: str = "/api/test") -> MagicMock:
    req = MagicMock()
    req.client.host = ip
    req.url.path = path
    return req


@pytest.fixture(autouse=True)
def clear_windows() -> Generator[None]:
    """Ensure each test starts with a clean rate-limit window registry."""
    _ENDPOINT_WINDOWS.clear()
    yield
    _ENDPOINT_WINDOWS.clear()


class TestEndpointRateLimitBasic:
    def test_allows_requests_within_limit(self) -> None:
        check = endpoint_rate_limit(3)
        req = _make_request()
        with patch(_PATCH_DISABLE_AUTH, False):
            for _ in range(3):
                asyncio.get_event_loop().run_until_complete(check(req))

    def test_raises_429_when_limit_exceeded(self) -> None:
        check = endpoint_rate_limit(3)
        req = _make_request()
        with patch(_PATCH_DISABLE_AUTH, False):
            for _ in range(3):
                asyncio.get_event_loop().run_until_complete(check(req))
            with pytest.raises(HTTPException) as exc_info:
                asyncio.get_event_loop().run_until_complete(check(req))
        assert exc_info.value.status_code == 429
        assert "3" in exc_info.value.detail
        assert exc_info.value.headers == {"Retry-After": "60"}

    def test_429_detail_includes_limit(self) -> None:
        check = endpoint_rate_limit(5)
        req = _make_request()
        with patch(_PATCH_DISABLE_AUTH, False):
            for _ in range(5):
                asyncio.get_event_loop().run_until_complete(check(req))
            with pytest.raises(HTTPException) as exc_info:
                asyncio.get_event_loop().run_until_complete(check(req))
        assert "5" in exc_info.value.detail
        assert "requests/minute" in exc_info.value.detail


class TestEndpointRateLimitDisabledAuth:
    def test_no_op_when_disable_auth_true(self) -> None:
        check = endpoint_rate_limit(1)
        req = _make_request()
        with patch(_PATCH_DISABLE_AUTH, True):
            for _ in range(20):
                asyncio.get_event_loop().run_until_complete(check(req))

    def test_buckets_not_written_when_disabled(self) -> None:
        check = endpoint_rate_limit(3)
        req = _make_request(ip="5.5.5.5", path="/api/never-tracked")
        with patch(_PATCH_DISABLE_AUTH, True):
            asyncio.get_event_loop().run_until_complete(check(req))
        assert ("5.5.5.5", "/api/never-tracked") not in _ENDPOINT_WINDOWS


class TestEndpointRateLimitIsolation:
    def test_different_ips_have_independent_windows(self) -> None:
        check = endpoint_rate_limit(2)
        req_a = _make_request(ip="10.0.0.1", path="/api/x")
        req_b = _make_request(ip="10.0.0.2", path="/api/x")
        with patch(_PATCH_DISABLE_AUTH, False):
            for _ in range(2):
                asyncio.get_event_loop().run_until_complete(check(req_a))
            for _ in range(2):
                asyncio.get_event_loop().run_until_complete(check(req_b))
            with pytest.raises(HTTPException) as exc_info:
                asyncio.get_event_loop().run_until_complete(check(req_a))
        assert exc_info.value.status_code == 429
        asyncio.get_event_loop().run_until_complete(check(req_b))

    def test_different_paths_have_independent_windows(self) -> None:
        check = endpoint_rate_limit(2)
        req_p1 = _make_request(ip="1.2.3.4", path="/api/endpoint-one")
        req_p2 = _make_request(ip="1.2.3.4", path="/api/endpoint-two")
        with patch(_PATCH_DISABLE_AUTH, False):
            for _ in range(2):
                asyncio.get_event_loop().run_until_complete(check(req_p1))
            with pytest.raises(HTTPException):
                asyncio.get_event_loop().run_until_complete(check(req_p1))
            for _ in range(2):
                asyncio.get_event_loop().run_until_complete(check(req_p2))

    def test_missing_client_uses_unknown_ip(self) -> None:
        check = endpoint_rate_limit(1)
        req = MagicMock()
        req.client = None
        req.url.path = "/api/no-client"
        with patch(_PATCH_DISABLE_AUTH, False):
            asyncio.get_event_loop().run_until_complete(check(req))
            with pytest.raises(HTTPException):
                asyncio.get_event_loop().run_until_complete(check(req))
        assert ("unknown", "/api/no-client") in _ENDPOINT_WINDOWS

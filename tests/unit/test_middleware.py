"""
Unit tests for middleware module.

Tests configure_middleware with various CORS settings including
production origins, dev origins, and cloud run regex patterns.
Also tests RateLimiterMiddleware per-IP sliding-window enforcement.
"""

import time
from unittest.mock import MagicMock, patch

from fastapi import FastAPI
from starlette.responses import PlainTextResponse
from starlette.testclient import TestClient

from deployment_api.middleware import (
    RateLimiterMiddleware,
    _rate_limit_window,
    configure_middleware,
)


def _make_rate_limited_app(max_requests: int = 3, window_seconds: float = 60.0) -> TestClient:
    app = FastAPI()
    app.add_middleware(
        RateLimiterMiddleware, max_requests=max_requests, window_seconds=window_seconds
    )

    @app.get("/api/data")
    async def data():
        return {"ok": True}

    @app.get("/health")
    async def health():
        return {"status": "ok"}

    @app.get("/api/health")
    async def api_health():
        return {"status": "ok"}

    @app.get("/metrics")
    async def metrics():
        return PlainTextResponse("# metrics")

    return TestClient(app, raise_server_exceptions=True)


class TestRateLimiterMiddleware:
    """Tests for per-IP sliding-window rate limiting."""

    def setup_method(self):
        _rate_limit_window.clear()

    def test_requests_within_limit_pass(self):
        client = _make_rate_limited_app(max_requests=5)
        for _ in range(5):
            r = client.get("/api/data")
            assert r.status_code == 200

    def test_request_exceeding_limit_returns_429(self):
        client = _make_rate_limited_app(max_requests=3)
        for _ in range(3):
            client.get("/api/data")
        r = client.get("/api/data")
        assert r.status_code == 429
        body = r.json()
        assert body["detail"] == "Too Many Requests"
        assert "retry_after_seconds" in body
        assert r.headers["Retry-After"]

    def test_health_endpoint_is_exempt(self):
        client = _make_rate_limited_app(max_requests=1)
        for _ in range(5):
            r = client.get("/health")
            assert r.status_code == 200

    def test_api_health_endpoint_is_exempt(self):
        client = _make_rate_limited_app(max_requests=1)
        for _ in range(5):
            r = client.get("/api/health")
            assert r.status_code == 200

    def test_metrics_endpoint_is_exempt(self):
        client = _make_rate_limited_app(max_requests=1)
        for _ in range(5):
            r = client.get("/metrics")
            assert r.status_code == 200

    def test_window_expiry_resets_count(self):
        client = _make_rate_limited_app(max_requests=2, window_seconds=0.05)
        for _ in range(2):
            client.get("/api/data")
        # After a short sleep the window resets
        time.sleep(0.1)
        r = client.get("/api/data")
        assert r.status_code == 200


class TestConfigureMiddlewareNoCloudRun:
    """Tests when CORS_ALLOWED_CLOUD_RUN is not set."""

    @patch("deployment_api.middleware.settings")
    def test_adds_cors_middleware_without_regex(self, mock_settings):
        mock_settings.API_PORT = 8080
        mock_settings.FRONTEND_PORT = 5173
        mock_settings.CORS_ALLOWED_ORIGINS = ""
        mock_settings.DEPLOYMENT_ENV = "production"
        mock_settings.CORS_ALLOWED_CLOUD_RUN = ""

        app = MagicMock()
        configure_middleware(app)

        # 2 calls: CORS + RateLimiterMiddleware
        assert app.add_middleware.call_count == 2
        cors_call = app.add_middleware.call_args_list[0]
        assert "allow_credentials" in cors_call.kwargs
        assert cors_call.kwargs["allow_credentials"] is True

    @patch("deployment_api.middleware.settings")
    def test_development_adds_dev_origins(self, mock_settings):
        mock_settings.API_PORT = 8080
        mock_settings.FRONTEND_PORT = 5173
        mock_settings.CORS_ALLOWED_ORIGINS = ""
        mock_settings.DEPLOYMENT_ENV = "development"
        mock_settings.CORS_ALLOWED_CLOUD_RUN = ""

        app = MagicMock()
        configure_middleware(app)

        cors_call = app.add_middleware.call_args_list[0]
        allow_origins = cors_call.kwargs.get("allow_origins", [])
        assert any("localhost" in o for o in allow_origins)

    @patch("deployment_api.middleware.settings")
    def test_production_origins_from_settings(self, mock_settings):
        mock_settings.API_PORT = 8080
        mock_settings.FRONTEND_PORT = 5173
        mock_settings.CORS_ALLOWED_ORIGINS = "https://example.com,https://app.example.com"
        mock_settings.DEPLOYMENT_ENV = "production"
        mock_settings.CORS_ALLOWED_CLOUD_RUN = ""

        app = MagicMock()
        configure_middleware(app)

        cors_call = app.add_middleware.call_args_list[0]
        allow_origins = cors_call.kwargs.get("allow_origins", [])
        assert "https://example.com" in allow_origins
        assert "https://app.example.com" in allow_origins

    @patch("deployment_api.middleware.settings")
    def test_production_no_dev_origins(self, mock_settings):
        mock_settings.API_PORT = 8080
        mock_settings.FRONTEND_PORT = 5173
        mock_settings.CORS_ALLOWED_ORIGINS = ""
        mock_settings.DEPLOYMENT_ENV = "production"
        mock_settings.CORS_ALLOWED_CLOUD_RUN = ""

        app = MagicMock()
        configure_middleware(app)

        cors_call = app.add_middleware.call_args_list[0]
        allow_origins = cors_call.kwargs.get("allow_origins", [])
        assert len(allow_origins) == 0


class TestConfigureMiddlewareWithCloudRun:
    """Tests when CORS_ALLOWED_CLOUD_RUN is set."""

    @patch("deployment_api.middleware.settings")
    def test_adds_cors_middleware_with_regex(self, mock_settings):
        mock_settings.API_PORT = 8080
        mock_settings.FRONTEND_PORT = 5173
        mock_settings.CORS_ALLOWED_ORIGINS = ""
        mock_settings.DEPLOYMENT_ENV = "production"
        mock_settings.CORS_ALLOWED_CLOUD_RUN = "my-service,other-service"

        app = MagicMock()
        configure_middleware(app)

        # 2 calls: CORS + RateLimiterMiddleware
        assert app.add_middleware.call_count == 2
        cors_call = app.add_middleware.call_args_list[0]
        assert "allow_origin_regex" in cors_call.kwargs
        regex = cors_call.kwargs["allow_origin_regex"]
        assert "my-service" in regex
        assert "other-service" in regex

    @patch("deployment_api.middleware.settings")
    def test_single_cloud_run_service_regex(self, mock_settings):
        mock_settings.API_PORT = 8080
        mock_settings.FRONTEND_PORT = 5173
        mock_settings.CORS_ALLOWED_ORIGINS = ""
        mock_settings.DEPLOYMENT_ENV = "production"
        mock_settings.CORS_ALLOWED_CLOUD_RUN = "deployment-api"

        app = MagicMock()
        configure_middleware(app)

        cors_call = app.add_middleware.call_args_list[0]
        regex = cors_call.kwargs["allow_origin_regex"]
        assert "deployment-api" in regex
        assert "run\\.app" in regex or "run.app" in regex

    @patch("deployment_api.middleware.settings")
    def test_allowed_methods_always_set(self, mock_settings):
        mock_settings.API_PORT = 8080
        mock_settings.FRONTEND_PORT = 5173
        mock_settings.CORS_ALLOWED_ORIGINS = ""
        mock_settings.DEPLOYMENT_ENV = "production"
        mock_settings.CORS_ALLOWED_CLOUD_RUN = ""

        app = MagicMock()
        configure_middleware(app)

        cors_call = app.add_middleware.call_args_list[0]
        methods = cors_call.kwargs.get("allow_methods", [])
        assert "GET" in methods
        assert "POST" in methods
        assert "DELETE" in methods

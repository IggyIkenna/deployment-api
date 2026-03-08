"""
Middleware configuration for the FastAPI application.

Handles CORS setup and other middleware configurations.
"""

from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware

from deployment_api import settings


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

"""Middleware configuration."""

from starlette.middleware.cors import CORSMiddleware

from deployment_api.settings import CORS_ALLOWED_ORIGINS


def configure_middleware(app: object) -> None:
    """Add CORS middleware to the FastAPI app."""
    app.add_middleware(
        CORSMiddleware,
        allow_origins=[o.strip() for o in CORS_ALLOWED_ORIGINS if o.strip()],
        allow_credentials=True,
        allow_methods=["*"],
        allow_headers=["*"],
    )

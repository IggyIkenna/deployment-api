"""Health check routes."""

from fastapi import APIRouter

router = APIRouter(tags=["health"])


@router.get("/")
async def root() -> dict[str, str]:
    """Root endpoint."""
    return {"service": "deployment-api", "status": "ok"}


@router.get("/api/health")
async def health() -> dict[str, str]:
    """Health check endpoint."""
    return {"status": "healthy"}

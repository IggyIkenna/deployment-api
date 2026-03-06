"""
Infrastructure Health Route

Exposes GET /infra/health — Layer 2 infra verification check.
Verifies GCS buckets, PubSub topics, and Secret Manager entries are accessible
before deployment proceeds.

Unauthenticated: this endpoint is intentionally public so that orchestrators
and CI/CD pipelines can poll it without an API key.
"""

from __future__ import annotations

import logging
import sys
from pathlib import Path

from fastapi import APIRouter

logger = logging.getLogger(__name__)

router = APIRouter()


def _get_verify_infra():
    """
    Lazy import of verify_infra from deployment-service scripts.

    deployment-service scripts/ is mounted at /app/deployment_service_scripts
    in the Docker image, or available via PYTHONPATH when running locally
    with both repos checked out side-by-side.

    Falls back to a direct path insert for local development.
    """
    # Try standard import first (deployed image where scripts are on PYTHONPATH)
    try:
        import deployment_service.scripts.verify_infra as verify_infra  # type: ignore[import]

        return verify_infra
    except ModuleNotFoundError:
        pass

    # Local development fallback: adjacent repo checkout
    scripts_dir = Path(__file__).parent.parent.parent.parent / "deployment-service" / "scripts"
    if scripts_dir.exists() and str(scripts_dir) not in sys.path:
        sys.path.insert(0, str(scripts_dir))
    try:
        import verify_infra  # type: ignore[import]

        return verify_infra
    except ModuleNotFoundError:
        return None


@router.get("/infra/health")
async def infra_health() -> dict[str, object]:
    """
    Layer 2 infra health check.

    Verifies that required GCS buckets, PubSub topics, and Secret Manager
    entries are accessible. Run after Terraform apply, before Layer 3 smoke tests.

    Returns:
        JSON with status ("ok" | "degraded" | "error"), check details, and errors.
    """
    verify_infra = _get_verify_infra()
    if verify_infra is None:
        logger.warning("[INFRA-HEALTH] verify_infra module not found — returning skip status")
        return {
            "status": "skip",
            "message": "verify_infra module not available (deployment-service not on PYTHONPATH)",
            "checks": {},
            "errors": [],
        }

    try:
        from unified_config_interface import UnifiedCloudConfig

        config = UnifiedCloudConfig()
        project_id: str = str(config.project_id) if hasattr(config, "project_id") else ""
        result = verify_infra.run_verification(project_id=project_id)
        status = "ok" if result.passed else "degraded"
        return {
            "status": status,
            "project_id": result.project_id,
            "timestamp": result.timestamp,
            "summary": result.summary,
            "checks": [c.to_dict() for c in result.checks],
            "errors": [c.error for c in result.checks if c.status == "error"],
        }
    except Exception as exc:
        logger.error("[INFRA-HEALTH] Verification failed with exception: %s", exc)
        return {
            "status": "error",
            "error": str(exc),
            "checks": {},
            "errors": [str(exc)],
        }

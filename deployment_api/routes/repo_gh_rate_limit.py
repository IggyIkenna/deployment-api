"""
GitHub rate-limit budget endpoint.

GET /api/repos/gh-rate-limit  — the shared per-user REST budget for the dashboard.

The whole fleet signs GitHub REST calls with ONE shared PAT (per-user 5000/hr core
budget); when it is exhausted every caller 403s. This endpoint surfaces the live
budget so the UI can warn before exhaustion. The `/rate_limit` endpoint is itself
FREE — it never decrements the budget — so this route is never cached and never
sends a conditional `If-None-Match`.

Plan: ci_dashboard_deployment_ui_2026_06_10.md Phase 1.
"""

from __future__ import annotations

import logging
from datetime import UTC, datetime, timedelta

import aiohttp
from fastapi import APIRouter

from deployment_api.deployment_api_config import DeploymentApiConfig
from deployment_api.settings import gcp_project_id as default_project_id

from ._repo_ci_github import gh_app_rate_limit, gh_rate_limit, resolve_app_credentials, resolve_gh_token

logger = logging.getLogger(__name__)
router = APIRouter()


def _mock_rate_limit() -> dict[str, object]:
    # Relative-to-now (not a hardcoded date) so this fixture never goes stale again — a
    # frozen absolute date/reset silently trains the UI on an old shape (see
    # unified-trading-pm/plans/active/issues/deployment_api_live_mock_parity_2026_07_17.md).
    now = datetime.now(UTC)
    core_reset = int((now + timedelta(hours=1)).timestamp())
    search_reset = int((now + timedelta(minutes=1)).timestamp())
    return {
        "fetched_at": now.strftime("%Y-%m-%dT%H:%M:%SZ"),
        "resources": {
            "core": {"limit": 5000, "remaining": 4821, "used": 179, "reset": core_reset},
            "graphql": {"limit": 5000, "remaining": 5000, "used": 0, "reset": core_reset},
            "search": {"limit": 30, "remaining": 30, "used": 0, "reset": search_reset},
        },
        # The GitHub App ("uts-ci-poller") pool is a SEPARATE 5000/hr budget the fleet's
        # CI pollers now draw from — surfaced alongside the PAT so the operator sees both.
        "app": {
            "resources": {
                "core": {"limit": 5000, "remaining": 4990, "used": 10, "reset": core_reset},
                "graphql": {"limit": 5000, "remaining": 5000, "used": 0, "reset": core_reset},
            },
        },
    }


@router.get("/gh-rate-limit")
async def get_gh_rate_limit() -> dict[str, object]:
    """Return the shared GitHub PAT's REST budget (core/graphql/search).

    Also surfaces the GitHub App installation-token pool (``app``) when its creds are
    configured in Secret Manager — a SEPARATE 5000/hr budget the fleet's CI pollers
    use. Both /rate_limit reads are FREE (they do not count against either budget).
    The App block is best-effort: absent creds / mint failure simply omit it, never
    breaking the PAT measurement.
    """
    cfg = DeploymentApiConfig()
    if cfg.is_mock_mode():
        return _mock_rate_limit()

    project_id: str = default_project_id or ""
    token = await resolve_gh_token(project_id)
    async with aiohttp.ClientSession() as session:
        result = await gh_rate_limit(session, token)
        app_creds = await resolve_app_credentials(project_id)
        if app_creds is not None:
            app_block = await gh_app_rate_limit(session, *app_creds)
            if app_block is not None:
                result["app"] = app_block
    return result

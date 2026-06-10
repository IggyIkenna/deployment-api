"""Fleet git-health proxy — surfaces the agent-orchestrator's /api/fleet/git-health
in the deployment-ui single pane (operator decision v2, 2026-06-10).

The orchestrator owns slot/host git-status; deployment-ui is the single devops pane,
so deployment-api proxies the orchestrator endpoint server-side (keeps the orchestrator
token server-side; avoids the browser → orchestrator cross-origin/mixed-content hop).

Honest degradation (shard-level isolation): if the orchestrator is unreachable or no
token is configured, `available=False` + a reason, never fabricated fleet data. The
`orchestrator_url` is always returned so the UI can deep-link to the orchestrator's own
Fleet Git-Health page — the authoritative git-health surface (operator: git-health
click-through goes to the AO UI).

Plan: unified-trading-pm/plans/active/fleet_git_health_orchestrator_2026_06_10.md (Phase 2
single-pane integration) + monitoring_control_plane_master_2026_06_10.md (operator v2).
"""

from __future__ import annotations

import asyncio
import logging
from typing import cast

import aiohttp
from unified_trading_library import get_secret_client

from deployment_api.routes._repo_ci_types import FleetGitHealthProxyDict

logger = logging.getLogger(__name__)

# Read-only proxy target — the agent-orchestrator public API (codex
# agent-orchestrator-overview.md). Not a secret; a fixed default so the proxy works
# out-of-the-box. Override path is the Secret-Manager token below + (future) a config field.
_ORCHESTRATOR_API_URL = "https://api.agent-orchestrator.odum-research.com"
_ORCHESTRATOR_TOKEN_SECRET = "ORCHESTRATOR_API_TOKEN"
_TIMEOUT_SECONDS = 10.0

_token_cache: str | None = None


async def _resolve_orchestrator_token(project_id: str) -> str | None:
    """Orchestrator API token from Secret Manager (cached; rotation = restart).

    Returns None when the secret is absent/unreadable — the caller degrades honestly
    rather than failing the whole dashboard.
    """
    global _token_cache
    if _token_cache:
        return _token_cache

    def _fetch() -> str | None:
        return get_secret_client(project_id=project_id).get_secret(_ORCHESTRATOR_TOKEN_SECRET)

    token = await asyncio.to_thread(_fetch)
    if token:
        _token_cache = token
    return token


async def fetch_fleet_git_health(project_id: str) -> FleetGitHealthProxyDict:
    """Proxy the orchestrator fleet git-health, degrading honestly on any failure."""
    token = await _resolve_orchestrator_token(project_id)
    if not token:
        return {
            "available": False,
            "reason": f"no orchestrator token configured (Secret Manager {_ORCHESTRATOR_TOKEN_SECRET})",
            "orchestrator_url": _ORCHESTRATOR_API_URL,
            "data": None,
        }

    url = f"{_ORCHESTRATOR_API_URL}/api/fleet/git-health?scope=fleet"
    try:
        async with (
            aiohttp.ClientSession() as session,
            session.get(
                url,
                headers={"Authorization": f"Bearer {token}"},
                timeout=aiohttp.ClientTimeout(total=_TIMEOUT_SECONDS),
            ) as resp,
        ):
            if resp.status != 200:
                return {
                    "available": False,
                    "reason": f"orchestrator returned HTTP {resp.status}",
                    "orchestrator_url": _ORCHESTRATOR_API_URL,
                    "data": None,
                }
            payload = cast(object, await resp.json())  # noqa: qg-raw-json
            data = cast(dict[str, object], payload) if isinstance(payload, dict) else None
            return {
                "available": data is not None,
                "reason": "" if data is not None else "orchestrator returned a non-object payload",
                "orchestrator_url": _ORCHESTRATOR_API_URL,
                "data": data,
            }
    except (TimeoutError, aiohttp.ClientError) as exc:
        logger.warning("fleet git-health proxy failed: %s", exc)
        return {
            "available": False,
            "reason": f"orchestrator unreachable: {exc}",
            "orchestrator_url": _ORCHESTRATOR_API_URL,
            "data": None,
        }

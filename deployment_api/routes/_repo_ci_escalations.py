"""Gap-4 escalation proxy — surfaces the agent-orchestrator's /api/escalations/active
in the deployment-ui Repos-CI board (ci_pipeline_self_healing_gaps_2026_06_11 G4).

The orchestrator owns the per-repo active-worker assignment for stuck PRs.
deployment-ui reads this to render repos under active recovery as working/pending
instead of just stuck, so the operator can tell "agent has this" from "parked".

Honest degradation (shard-level isolation): if the orchestrator is unreachable or no
token is configured, available=False + a reason. The board shows no agent-state annotation
rather than failing — the signal is supplementary to the Repos-CI overview, not critical.
"""

from __future__ import annotations

import asyncio
import logging
from typing import Any, cast

import aiohttp
from unified_trading_library import get_secret_client

from deployment_api.routes._repo_ci_types import EscalationsProxyDict

logger = logging.getLogger(__name__)

_ORCHESTRATOR_API_URL = "https://api.agent-orchestrator.odum-research.com"
_ORCHESTRATOR_TOKEN_SECRET = "ORCHESTRATOR_API_TOKEN"
_TIMEOUT_SECONDS = 8.0

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


async def fetch_active_escalations(project_id: str) -> EscalationsProxyDict:
    """Proxy the orchestrator's active escalations, degrading honestly on failure."""
    token = await _resolve_orchestrator_token(project_id)
    if not token:
        return {
            "available": False,
            "reason": f"no orchestrator token configured (Secret Manager {_ORCHESTRATOR_TOKEN_SECRET})",
            "escalations": [],
        }

    url = f"{_ORCHESTRATOR_API_URL}/api/escalations/active"
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
                    "escalations": [],
                }
            payload = cast(object, await resp.json())  # noqa: qg-raw-json
            if not isinstance(payload, list):
                return {
                    "available": False,
                    "reason": "orchestrator returned a non-list payload",
                    "escalations": [],
                }
            return {
                "available": True,
                "reason": "",
                "escalations": cast(list[dict[str, Any]], payload),
            }
    except (TimeoutError, aiohttp.ClientError) as exc:
        logger.warning("escalations proxy failed: %s", exc)
        return {
            "available": False,
            "reason": f"orchestrator unreachable: {exc}",
            "escalations": [],
        }

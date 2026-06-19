"""Infra-VM health + VM census proxies — surfaces the agent-orchestrator's /api/fleet/summary
in the deployment-ui single pane (plan: deployment_ui_monitoring_pane_2026_06_19.md INFRA P1).

Two endpoints derived from the same upstream:
  GET /api/repo-ci/fleet/infra-vm-health  — raw per-VM liveness summary
  GET /api/repo-ci/fleet/vm-census        — census roll-up: healthy / stale / error counts

The orchestrator owns the fleet registry; deployment-api proxies server-side to keep the
ORCHESTRATOR_API_TOKEN out of the browser and avoid cross-origin hops. Honest degradation
on any failure: available=False + a reason. orchestrator_url always returned for AO deep-links.
"""

from __future__ import annotations

import asyncio
import logging
from typing import cast

import aiohttp
from unified_trading_library import get_secret_client

from deployment_api.routes._repo_ci_types import InfraVmHealthProxyDict, VmCensusEntryDict, VmCensusProxyDict

logger = logging.getLogger(__name__)

_ORCHESTRATOR_API_URL = "https://api.agent-orchestrator.odum-research.com"
_ORCHESTRATOR_TOKEN_SECRET = "ORCHESTRATOR_API_TOKEN"
_TIMEOUT_SECONDS = 10.0

_token_cache: str | None = None


async def _resolve_orchestrator_token(project_id: str) -> str | None:
    global _token_cache
    if _token_cache:
        return _token_cache

    def _fetch() -> str | None:
        return get_secret_client(project_id=project_id).get_secret(_ORCHESTRATOR_TOKEN_SECRET)

    token = await asyncio.to_thread(_fetch)
    if token:
        _token_cache = token
    return token


async def _fetch_fleet_summary_raw(project_id: str) -> tuple[dict[str, object] | None, str]:
    """Fetch AO /api/fleet/summary. Returns (payload, error_reason)."""
    token = await _resolve_orchestrator_token(project_id)
    if not token:
        return None, f"no orchestrator token configured (Secret Manager {_ORCHESTRATOR_TOKEN_SECRET})"

    url = f"{_ORCHESTRATOR_API_URL}/api/fleet/summary"
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
                return None, f"orchestrator returned HTTP {resp.status}"
            payload = cast(object, await resp.json())  # noqa: qg-raw-json
            if not isinstance(payload, dict):
                return None, "orchestrator returned a non-object payload"
            return cast(dict[str, object], payload), ""
    except (TimeoutError, aiohttp.ClientError) as exc:
        logger.warning("infra-vm proxy failed: %s", exc)
        return None, f"orchestrator unreachable: {exc}"


async def fetch_infra_vm_health(project_id: str) -> InfraVmHealthProxyDict:
    """Proxy the AO fleet summary for the infra-VM health tile."""
    data, reason = await _fetch_fleet_summary_raw(project_id)
    return {
        "available": data is not None,
        "reason": reason,
        "orchestrator_url": _ORCHESTRATOR_API_URL,
        "data": data,
    }


def _classify_vm_entry(vm: object) -> VmCensusEntryDict:
    """Extract a census entry from one element of the AO /api/fleet/summary vms list."""
    if not isinstance(vm, dict):
        return {
            "id": "unknown",
            "label": "unknown",
            "url": "",
            "status": "error",
            "last_heartbeat_seconds_ago": None,
            "error": "malformed VM entry from AO",
        }
    stale = bool(vm.get("stale", False))
    error = vm.get("error")
    if error:
        status = "error"
    elif stale:
        status = "stale"
    else:
        status = "healthy"
    heartbeat_raw = vm.get("last_heartbeat_seconds_ago")
    heartbeat = int(heartbeat_raw) if isinstance(heartbeat_raw, (int, float)) else None
    return {
        "id": str(vm.get("id", "")),
        "label": str(vm.get("label", "")),
        "url": str(vm.get("url", "")),
        "status": status,
        "last_heartbeat_seconds_ago": heartbeat,
        "error": str(error) if error else None,
    }


async def fetch_vm_census(project_id: str) -> VmCensusProxyDict:
    """Derive a running-vs-stale-vs-error census from the AO fleet summary."""
    data, reason = await _fetch_fleet_summary_raw(project_id)
    if data is None:
        return {
            "available": False,
            "reason": reason,
            "orchestrator_url": _ORCHESTRATOR_API_URL,
            "total": 0,
            "healthy": 0,
            "stale": 0,
            "error": 0,
            "vms": [],
        }

    raw_vms = data.get("vms", [])
    entries: list[VmCensusEntryDict] = [_classify_vm_entry(vm) for vm in (raw_vms if isinstance(raw_vms, list) else [])]
    healthy = sum(1 for e in entries if e["status"] == "healthy")
    stale = sum(1 for e in entries if e["status"] == "stale")
    error = sum(1 for e in entries if e["status"] == "error")
    return {
        "available": True,
        "reason": "",
        "orchestrator_url": _ORCHESTRATOR_API_URL,
        "total": len(entries),
        "healthy": healthy,
        "stale": stale,
        "error": error,
        "vms": entries,
    }

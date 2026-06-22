# Epic: monitoring_control_plane_master_2026_06_10
# Lifecycle: permanent
"""Infra-VM-health proxy — surfaces the agent-orchestrator's /api/fleet/summary in the
deployment-ui FleetInfra tile (plan: deployment_ui_monitoring_pane_2026_06_19.md).

The orchestrator owns the per-VM slot/backlog/watchdog state; deployment-ui is the single
devops pane, so deployment-api proxies the orchestrator endpoint server-side — keeping the
orchestrator token server-side and avoiding the browser → orchestrator cross-origin /
mixed-content hop (same shape as ``_repo_ci_fleet.py``'s git-health proxy).

Honest degradation (shard-level isolation): if the orchestrator is unreachable or no token
is configured, ``available=False`` + an empty ``vms`` list, never fabricated fleet data.
``orchestrator_url`` is always returned so the UI can deep-link to the orchestrator's own
fleet page.
"""

from __future__ import annotations

import asyncio
import logging
from typing import cast

import aiohttp
from unified_trading_library import get_secret_client

from deployment_api.routes._fleet_types import (
    AlertSeverity,
    InfraVmAlert,
    InfraVmHealthResponse,
    InfraVmSlot,
    InfraVmSummary,
    InfraVmWatchdog,
    VmRole,
)

logger = logging.getLogger(__name__)

# Read-only proxy target — the agent-orchestrator public API (codex
# agent-orchestrator-overview.md). Not a secret; a fixed default so the proxy works
# out-of-the-box. Override path is the Secret-Manager token below.
_ORCHESTRATOR_API_URL = "https://api.agent-orchestrator.odum-research.com"
_ORCHESTRATOR_TOKEN_SECRET = "ORCHESTRATOR_API_TOKEN"
_TIMEOUT_SECONDS = 10.0

_token_cache: str | None = None

_VALID_ROLES = frozenset({"epic", "planning", "unknown"})
_VALID_SEVERITIES = frozenset({"info", "warn", "crit"})


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


def _coerce_int(value: object, default: int = 0) -> int:
    """Best-effort int coercion for an untyped JSON value (bool excluded → 0)."""
    if isinstance(value, bool):
        return default
    if isinstance(value, int):
        return value
    if isinstance(value, float):
        return int(value)
    return default


def _coerce_opt_int(value: object) -> int | None:
    """Best-effort int|None coercion (None stays None; bool → None)."""
    if value is None or isinstance(value, bool):
        return None
    if isinstance(value, (int, float)):
        return int(value)
    return None


def _parse_watchdog(raw: object) -> InfraVmWatchdog | None:
    """Map the orchestrator ``WatchdogHealth`` payload → :class:`InfraVmWatchdog`."""
    if not isinstance(raw, dict):
        return None
    wd = cast(dict[str, object], raw)
    return InfraVmWatchdog(
        enabled=bool(wd.get("enabled", False)),
        kills_today=_coerce_int(wd.get("kills_today")),
        daily_cap=_coerce_int(wd.get("daily_cap")),
        dormant=bool(wd.get("dormant", False)),
        flapping=bool(wd.get("flapping", False)),
    )


def _parse_alerts(raw: object) -> list[InfraVmAlert]:
    """Map the orchestrator ``VmAlert[]`` payload → :class:`InfraVmAlert` list."""
    if not isinstance(raw, list):
        return []
    out: list[InfraVmAlert] = []
    for item in cast(list[object], raw):
        if not isinstance(item, dict):
            continue
        alert = cast(dict[str, object], item)
        severity = str(alert.get("severity", "info"))
        if severity not in _VALID_SEVERITIES:
            severity = "info"
        out.append(
            InfraVmAlert(
                severity=cast(AlertSeverity, severity),  # constrained to _VALID_SEVERITIES above
                kind=str(alert.get("kind", "")),
                detail=str(alert.get("detail", "")),
            )
        )
    return out


def _parse_summary(raw: object) -> InfraVmSummary | None:
    """Map the orchestrator ``VmSummary`` payload → :class:`InfraVmSummary` (the subset
    the FleetInfra tile renders). None when the entry has no summary (VM unreachable)."""
    if not isinstance(raw, dict):
        return None
    s = cast(dict[str, object], raw)
    role = str(s.get("role", "unknown"))
    if role not in _VALID_ROLES:
        role = "unknown"
    label_raw = s.get("label")
    return InfraVmSummary(
        vm_id=str(s.get("vm_id", "")),
        role=cast(VmRole, role),  # constrained to _VALID_ROLES above
        label=str(label_raw) if label_raw is not None else None,
        slots_total=_coerce_int(s.get("slots_total")),
        slots_working=_coerce_int(s.get("slots_working")),
        slots_idle=_coerce_int(s.get("slots_idle")),
        slots_stale=_coerce_int(s.get("slots_stale")),
        slots_paused=_coerce_int(s.get("slots_paused")),
        slots_blocked=_coerce_int(s.get("slots_blocked")),
        backlog_total=_coerce_int(s.get("backlog_total")),
        backlog_queued=_coerce_int(s.get("backlog_queued")),
        alerts=_parse_alerts(s.get("alerts")),
        watchdog=_parse_watchdog(s.get("watchdog")),
    )


def _parse_slot(raw: object) -> InfraVmSlot | None:
    """Map one orchestrator fleet-summary VM entry → :class:`InfraVmSlot`."""
    if not isinstance(raw, dict):
        return None
    entry = cast(dict[str, object], raw)
    vm_id = str(entry.get("id", ""))
    error_raw = entry.get("error")
    summary = _parse_summary(entry.get("summary"))
    return InfraVmSlot(
        id=vm_id,
        label=str(entry.get("label", vm_id)),
        url=str(entry.get("url", "")),
        # A VM is "available" iff it returned a usable summary (the orchestrator sets
        # error+summary=None for an unreachable backend) — derive it, don't trust a field.
        available=summary is not None,
        error=str(error_raw) if error_raw is not None else None,
        stale=bool(entry.get("stale", False)),
        last_heartbeat_seconds_ago=_coerce_opt_int(entry.get("last_heartbeat_seconds_ago")),
        summary=summary,
    )


def _map_fleet_summary(payload: dict[str, object]) -> list[InfraVmSlot]:
    """Map the orchestrator ``{vms: [...]}`` envelope → :class:`InfraVmSlot` list."""
    raw_vms = payload.get("vms")
    if not isinstance(raw_vms, list):
        return []
    slots: list[InfraVmSlot] = []
    for item in cast(list[object], raw_vms):
        slot = _parse_slot(item)
        if slot is not None:
            slots.append(slot)
    return slots


def _degraded(reason: str) -> InfraVmHealthResponse:
    """An honest degraded response — always carries the orchestrator deep-link URL."""
    logger.warning("infra-vm-health proxy degraded: %s", reason)
    return InfraVmHealthResponse(
        available=False,
        orchestrator_url=_ORCHESTRATOR_API_URL,
        vms=[],
    )


async def fetch_infra_vm_health(project_id: str) -> InfraVmHealthResponse:
    """Proxy the orchestrator fleet summary, degrading honestly on any failure."""
    token = await _resolve_orchestrator_token(project_id)
    if not token:
        return _degraded(f"no orchestrator token configured (Secret Manager {_ORCHESTRATOR_TOKEN_SECRET})")

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
                return _degraded(f"orchestrator returned HTTP {resp.status}")
            payload = cast(object, await resp.json())  # noqa: qg-raw-json
            if not isinstance(payload, dict):
                return _degraded("orchestrator returned a non-object payload")
            return InfraVmHealthResponse(
                available=True,
                orchestrator_url=_ORCHESTRATOR_API_URL,
                vms=_map_fleet_summary(cast(dict[str, object], payload)),
            )
    except (TimeoutError, aiohttp.ClientError) as exc:
        return _degraded(f"orchestrator unreachable: {exc}")


__all__ = ["fetch_infra_vm_health"]

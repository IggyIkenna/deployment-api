# Epic: observability_master
# Lifecycle: permanent
"""GET /api/health/overview — the single "is everything healthy right now?" rollup.

The cockpit's one-call health envelope. Aggregates the EXISTING monitoring signals into
one tile list (NO new data sources — pure reuse of the shipped route helpers):

* **fleet** — VM census running / zombie / OOM / stopped (``fleet`` route's census builder).
* **consolidator** — per-asset_group manifest-consolidator posture (``health_consolidator``).
* **coverage** — data-status coverage health (the precomputed ``coverage.json.gz`` rollup;
  stale/missing → ``unknown``, never a heavy on-demand compute on a health ping).
* **alerts** — open-alert counts by class from the unified ledger (``unified_alerts`` →
  ``load_alerts_payload``: CI + vm_down + consolidator_down + git_health + worker_liveness).
* **gh_budget** — the shared GitHub PAT REST budget (``repo_gh_rate_limit``); doubles as the
  **GH Actions / billing** surface (Phase-4 P3 — minutes/budget headroom).
* **cost** — today's total cloud spend (``cost_daily``) + a GCP-billing-threshold flag.

Rollup: each tile is ``ok|degraded|critical|unknown``; ``overall`` = the WORST tile
(any critical → critical, else any degraded → degraded, else ok; ``unknown`` does not
escalate past ``degraded``). Every tile carries a ``detail_href`` deep-link to its existing
drill-down endpoint so the UI can click through.

Honest degradation: each tile is computed in isolation — one failing source yields that
tile ``unknown`` (logged), the rollup still returns 200. Read-only; never a 5xx.

Plan: unified-trading-pm/plans/active/unified_deployment_health_cockpit_2026_06_23.md
Phase 1 (rollup) + Phase 4 P3 (GH-Actions / billing tile).
SSOT: codex/05-infrastructure/deployment-observability.md.
"""

from __future__ import annotations

import logging
from datetime import UTC, datetime
from typing import cast

import aiohttp
from fastapi import APIRouter
from pydantic import BaseModel, Field

from deployment_api.deployment_api_config import DeploymentApiConfig

router = APIRouter()
logger = logging.getLogger(__name__)

_cfg = DeploymentApiConfig()

# A GCP daily-spend threshold (USD) above which the cost tile flags "degraded". The
# GH-Actions billing-wall concern (issue github_actions_billing_wall_2026_06_11.md)
# rides the gh_budget tile (REST + Actions minutes share the same per-user PAT budget).
_DAILY_COST_WARN_USD = 200.0
_DAILY_COST_CRIT_USD = 500.0
# GitHub REST core budget headroom thresholds (fraction of limit remaining).
_GH_REMAINING_WARN_FRAC = 0.20
_GH_REMAINING_CRIT_FRAC = 0.05


class HealthTile(BaseModel):  # CORRECT-LOCAL: FastAPI API contract model
    """One health tile in the overview rollup."""

    id: str  # "fleet" | "consolidator" | "coverage" | "alerts" | "gh_budget" | "cost"
    label: str
    status: str  # "ok" | "degraded" | "critical" | "unknown"
    value: str  # a short human summary (e.g. "12 running, 1 OOM")
    detail_href: str  # deep-link to the existing drill-down endpoint


class HealthOverviewResponse(BaseModel):  # CORRECT-LOCAL: FastAPI API contract model
    """GET /api/health/overview response — overall + the tile rollup."""

    generated_at: str  # ISO-8601 UTC
    overall: str  # "ok" | "degraded" | "critical"
    tiles: list[HealthTile] = Field(default_factory=list)


def _rollup_overall(tiles: list[HealthTile]) -> str:
    """Worst-tile rollup: any critical → critical, else any degraded → degraded, else ok.

    ``unknown`` is treated as ``degraded`` for the overall (a source we can't read is a
    degraded posture, not a healthy one) but never escalates to ``critical``.
    """
    statuses = {t.status for t in tiles}
    if "critical" in statuses:
        return "critical"
    if "degraded" in statuses or "unknown" in statuses:
        return "degraded"
    return "ok"


# ---------------------------------------------------------------------------
# Per-tile builders (each isolated — a failure degrades only its own tile)
# ---------------------------------------------------------------------------


def _fleet_tile(now: datetime) -> HealthTile:
    """VM census tile — running / zombie / OOM / stopped (reuses the fleet census builder)."""
    from deployment_api.routes import fleet
    from deployment_api.routes._fleet_census import build_vm_census, load_watchdog_census
    from deployment_api.routes._fleet_mocks import mock_vm_census

    try:
        if _cfg.is_mock_mode():
            census = mock_vm_census()
        else:
            project_id = _cfg.gcp_project_id or ""
            if not project_id:
                census = build_vm_census({}, now)
            else:
                watchdog = load_watchdog_census(project_id, now)
                details = fleet.get_vm_instance_details(project_id)
                census = build_vm_census(details, now, watchdog)
    except (OSError, ValueError, RuntimeError) as exc:
        logger.warning("health-overview: fleet tile failed: %s", exc)
        return HealthTile(
            id="fleet",
            label="Fleet VMs",
            status="unknown",
            value="census unavailable",
            detail_href="/api/fleet/vm-census",
        )
    if census.oom > 0 or census.zombie > 0:
        status = "critical"
    elif census.stopped > census.running:
        status = "degraded"
    else:
        status = "ok"
    value = f"{census.running} running, {census.zombie} zombie, {census.oom} OOM, {census.stopped} stopped"
    return HealthTile(id="fleet", label="Fleet VMs", status=status, value=value, detail_href="/api/fleet/vm-census")


def _consolidator_tile() -> HealthTile:
    """Manifest-consolidator tile — worst per-AG posture (reuses health_consolidator)."""
    from deployment_api.routes import health_consolidator as hc

    try:
        resp = hc.get_consolidator_health()
    except (OSError, ValueError, RuntimeError) as exc:
        logger.warning("health-overview: consolidator tile failed: %s", exc)
        return HealthTile(
            id="consolidator",
            label="Manifest Consolidator",
            status="unknown",
            value="unavailable",
            detail_href="/api/health/consolidator",
        )
    crit = [e.asset_group for e in resp.asset_groups if e.status == "critical"]
    value = f"{len(resp.asset_groups)} asset_groups; {len(crit)} critical"
    if crit:
        value = f"DOWN for: {', '.join(crit)}"
    return HealthTile(
        id="consolidator",
        label="Manifest Consolidator",
        status=resp.overall,
        value=value,
        detail_href="/api/health/consolidator",
    )


def _coverage_tile() -> HealthTile:
    """Data-status coverage tile — the precomputed coverage rollup (no heavy compute).

    Reads the fresh ``coverage.json.gz`` rollup for the canonical instruments-service via
    ``read_coverage_rollup_if_fresh``. A stale/missing rollup is honest ``unknown`` (the
    health ping never triggers the multi-minute on-demand coverage compute).
    """
    if _cfg.is_mock_mode():
        return HealthTile(
            id="coverage",
            label="Data Coverage",
            status="ok",
            value="5 asset_groups tracked",
            detail_href="/api/data-status/coverage-summary",
        )
    try:
        from deployment_api.services.data_status.rollup_cache import read_coverage_rollup_if_fresh

        rollup = read_coverage_rollup_if_fresh("instruments-service")
    except (OSError, ValueError, RuntimeError) as exc:
        logger.warning("health-overview: coverage tile failed: %s", exc)
        rollup = None
    if rollup is None:
        return HealthTile(
            id="coverage",
            label="Data Coverage",
            status="unknown",
            value="rollup stale/missing",
            detail_href="/api/data-status/coverage-summary",
        )
    ag_obj = rollup.get("asset_groups")
    ag_count = len(cast("dict[str, object]", ag_obj)) if isinstance(ag_obj, dict) else 0
    status = "ok" if ag_count > 0 else "degraded"
    return HealthTile(
        id="coverage",
        label="Data Coverage",
        status=status,
        value=f"{ag_count} asset_groups tracked",
        detail_href="/api/data-status/coverage-summary",
    )


async def _alerts_tile() -> HealthTile:
    """Open-alert counts by class tile (reuses the unified alert ledger)."""
    from deployment_api.routes._repo_ci_alerts import load_alerts_payload
    from deployment_api.routes._repo_ci_mocks import _mock_alerts  # pyright: ignore[reportPrivateUsage]

    try:
        payload = _mock_alerts() if _cfg.is_mock_mode() else await load_alerts_payload()
    except (OSError, ValueError, RuntimeError) as exc:
        logger.warning("health-overview: alerts tile failed: %s", exc)
        return HealthTile(
            id="alerts",
            label="Open Alerts",
            status="unknown",
            value="ledger unavailable",
            detail_href="/api/alerts",
        )
    alerts = payload["alerts"]
    crit = sum(1 for a in alerts if (a.get("severity") or "") == "CRITICAL")
    warn = sum(1 for a in alerts if (a.get("severity") or "") == "WARNING")
    by_class: dict[str, int] = {}
    for a in alerts:
        cls = a.get("alert_class") or a.get("kind") or "ci"
        by_class[cls] = by_class.get(cls, 0) + 1
    if crit > 0:
        status = "critical"
    elif warn > 0:
        status = "degraded"
    else:
        status = "ok"
    value = f"{len(alerts)} open ({crit} crit, {warn} warn)"
    return HealthTile(id="alerts", label="Open Alerts", status=status, value=value, detail_href="/api/alerts")


async def _gh_budget_tile() -> HealthTile:
    """GitHub PAT REST + Actions budget tile (reuses repo_gh_rate_limit).

    The shared per-user PAT budget (REST core) is the proxy for the GH-Actions billing-wall
    concern — when it nears exhaustion the fleet's CI/Actions calls 403. Surfaces headroom so
    the operator sees the wall before it bites (issue github_actions_billing_wall_2026_06_11).
    """
    from deployment_api.routes import repo_gh_rate_limit as ghrl

    try:
        if _cfg.is_mock_mode():
            data = ghrl._mock_rate_limit()  # pyright: ignore[reportPrivateUsage]
        else:
            from deployment_api.routes._repo_ci_github import gh_rate_limit, resolve_gh_token
            from deployment_api.settings import gcp_project_id as default_project_id

            project_id = default_project_id or ""
            token = await resolve_gh_token(project_id)
            async with aiohttp.ClientSession() as session:
                data = await gh_rate_limit(session, token)
    except (OSError, ValueError, RuntimeError, aiohttp.ClientError) as exc:
        logger.warning("health-overview: gh_budget tile failed: %s", exc)
        return HealthTile(
            id="gh_budget",
            label="GitHub Budget",
            status="unknown",
            value="rate-limit unavailable",
            detail_href="/api/repos/gh-rate-limit",
        )
    core = _extract_core(data)
    if core is None:
        return HealthTile(
            id="gh_budget",
            label="GitHub Budget",
            status="unknown",
            value="no core budget in response",
            detail_href="/api/repos/gh-rate-limit",
        )
    limit, remaining = core
    frac = remaining / limit if limit > 0 else 1.0
    if frac <= _GH_REMAINING_CRIT_FRAC:
        status = "critical"
    elif frac <= _GH_REMAINING_WARN_FRAC:
        status = "degraded"
    else:
        status = "ok"
    return HealthTile(
        id="gh_budget",
        label="GitHub Budget",
        status=status,
        value=f"{remaining}/{limit} REST core remaining",
        detail_href="/api/repos/gh-rate-limit",
    )


def _extract_core(data: dict[str, object]) -> tuple[int, int] | None:
    """Pull (limit, remaining) from a /rate_limit ``resources.core`` block."""
    resources = data.get("resources")
    if not isinstance(resources, dict):
        return None
    core = cast("dict[str, object]", resources).get("core")
    if not isinstance(core, dict):
        return None
    core_d = cast("dict[str, object]", core)
    try:
        return int(str(core_d.get("limit", 0))), int(str(core_d.get("remaining", 0)))
    except (ValueError, TypeError):
        return None


def _cost_tile(now: datetime) -> HealthTile:
    """Today's cloud spend tile + GCP billing-threshold flag (reuses cost_daily)."""
    from deployment_api.routes import cost_daily

    date = now.strftime("%Y-%m-%d")
    try:
        if _cfg.is_mock_mode():
            resp = cost_daily._mock_response(date)  # pyright: ignore[reportPrivateUsage]
        else:
            bucket = cost_daily._cost_bucket()  # pyright: ignore[reportPrivateUsage]
            from unified_trading_library import get_storage_client

            storage = get_storage_client(project_id=_cfg.gcp_project_id)
            prefix = f"cost_summary/{date}/"
            blobs = list(storage.list_blobs(bucket=bucket, prefix=prefix))  # pyright: ignore[reportUnknownVariableType,reportUnknownMemberType,reportUnknownArgumentType]
            vm_rows: list[cost_daily.VmCostRow] = []
            for blob in blobs:
                blob_name: str = blob.name  # pyright: ignore[reportUnknownVariableType,reportUnknownMemberType]
                if not blob_name.endswith(".jsonl"):
                    continue
                raw: bytes = storage.download_bytes(bucket=bucket, blob_path=blob_name)  # pyright: ignore[reportAttributeAccessIssue,reportUnknownVariableType,reportUnknownMemberType]
                parsed = cost_daily._parse_blob(raw, blob_name)  # pyright: ignore[reportPrivateUsage]
                if parsed is not None:
                    vm_rows.append(parsed)
            resp = cost_daily._aggregate(vm_rows, date)  # pyright: ignore[reportPrivateUsage]
    except (OSError, ValueError, RuntimeError) as exc:
        logger.warning("health-overview: cost tile failed: %s", exc)
        return HealthTile(
            id="cost",
            label="Daily Cost",
            status="unknown",
            value="cost data unavailable",
            detail_href="/api/costs/daily",
        )
    total = resp.total_usd
    if total >= _DAILY_COST_CRIT_USD:
        status = "critical"
    elif total >= _DAILY_COST_WARN_USD:
        status = "degraded"
    else:
        status = "ok"
    return HealthTile(
        id="cost",
        label="Daily Cost",
        status=status,
        value=f"${total:.2f} today ({len(resp.by_vm)} VMs)",
        detail_href="/api/costs/daily",
    )


@router.get("/health/overview", response_model=HealthOverviewResponse)
async def get_health_overview() -> HealthOverviewResponse:
    """One-call "is everything healthy right now?" rollup across the existing signals.

    Tiles: fleet VM census, manifest-consolidator posture, data-status coverage, open
    alerts by class, GitHub/Actions budget headroom, today's cost. ``overall`` is the worst
    tile status. Each tile degrades to ``unknown`` in isolation on a source failure — the
    rollup always returns 200 (read-only monitoring).
    """
    now = datetime.now(UTC)
    tiles: list[HealthTile] = [
        _fleet_tile(now),
        _consolidator_tile(),
        _coverage_tile(),
        await _alerts_tile(),
        await _gh_budget_tile(),
        _cost_tile(now),
    ]
    return HealthOverviewResponse(
        generated_at=now.isoformat(),
        overall=_rollup_overall(tiles),
        tiles=tiles,
    )


__all__ = [
    "HealthOverviewResponse",
    "HealthTile",
    "build_overview_rollup",
    "router",
]


def build_overview_rollup(tiles: list[HealthTile], now: datetime) -> HealthOverviewResponse:
    """Pure rollup helper (testable without I/O): wrap tiles + worst-overall."""
    return HealthOverviewResponse(
        generated_at=now.isoformat(),
        overall=_rollup_overall(tiles),
        tiles=tiles,
    )

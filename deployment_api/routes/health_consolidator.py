# Epic: observability_master
# Lifecycle: permanent
"""GET /api/health/consolidator — manifest-consolidator health drill-down per asset_group.

Replaces today's binary up/down (``CONSOLIDATOR_DOWN`` alert / ``assert_consolidator_healthy``
raise) with a per-asset_group posture the cockpit Health pane can render: for each
asset_group's ``raw_tick_data`` bucket we report the consolidated ``_index/
availability_index.parquet`` heartbeat age (the consolidator touches its mtime every
cycle, incl. no-op cycles), whether per-VM shards exist behind a stale/missing index
(= the consolidator is BEHIND or DOWN, the recovery-merge fallback would activate), and
the derived health status.

Reuse: the SAME ``unified_trading_library.manifest_writer`` internals the consolidator
liveness contract uses — ``consolidated_blob_age_sec`` (the heartbeat), ``per_vm_shards_exist``
(the stale-vs-empty discriminator), ``resolve_consolidated_staleness_sec`` (the budget).
NO new GCS-walk; one metadata read (``blob.reload()``) + one cheap shard-list per AG.

Honest degradation: a per-AG read failure yields ``status="unknown"`` for that AG (logged),
never a 5xx — this is a read-only monitoring endpoint.

Plan: unified-trading-pm/plans/active/unified_deployment_health_cockpit_2026_06_23.md Phase 1.
SSOT: codex/05-infrastructure/manifest-consolidator-ssot.md +
codex/05-infrastructure/deployment-observability.md.
"""

from __future__ import annotations

import logging
from datetime import UTC, datetime, timedelta

from fastapi import APIRouter
from pydantic import BaseModel, Field
from unified_trading_library import (
    AssetGroup,
    consolidated_blob_age_sec,
    get_storage_client,
    per_vm_shards_exist,
    resolve_bucket_name,
    resolve_consolidated_staleness_sec,
)

from deployment_api.deployment_api_config import DeploymentApiConfig

router = APIRouter()
logger = logging.getLogger(__name__)

_cfg = DeploymentApiConfig()

# The asset_groups whose raw_tick_data buckets carry an availability_index the
# consolidator maintains (the canonical lowercase set, UAC AssetGroup literals).
_ASSET_GROUPS: tuple[AssetGroup, ...] = ("cefi", "defi", "tradfi", "sports", "prediction")


class ConsolidatorAgHealth(BaseModel):  # CORRECT-LOCAL: FastAPI API contract model
    """Per-asset_group manifest-consolidator posture."""

    asset_group: str
    bucket: str
    status: str  # "ok" | "degraded" | "critical" | "unknown"
    index_age_seconds: float | None = None  # heartbeat age of the consolidated index
    staleness_budget_seconds: int
    per_vm_shard_fallback_active: bool  # stale/missing index WHILE shards exist → recovery merge
    last_successful_run_at: str | None = None  # ISO-8601, derived from index mtime
    detail: str


class ConsolidatorHealthResponse(BaseModel):  # CORRECT-LOCAL: FastAPI API contract model
    """GET /api/health/consolidator response — per-AG consolidator drill-down."""

    generated_at: str  # ISO-8601 UTC
    overall: str  # worst per-AG status (ok|degraded|critical|unknown)
    asset_groups: list[ConsolidatorAgHealth] = Field(default_factory=list)


def _status_rank(status: str) -> int:
    """Order statuses worst-first for the overall rollup."""
    return {"critical": 0, "degraded": 1, "unknown": 2, "ok": 3}.get(status, 2)


def _classify_ag(age: float | None, budget: int, shards_exist: bool) -> tuple[str, bool, str]:
    """Derive (status, fallback_active, detail) for one asset_group.

    * Index FRESH (age <= budget) → ok.
    * Index STALE/MISSING **and** per-VM shards exist → the consolidator is behind/down
      and the read path would fall back to the OOM-prone recovery merge → critical.
    * Index STALE/MISSING but **no** shards → a genuinely empty / never-written bucket,
      not an outage → degraded (nothing to consolidate yet).
    """
    if age is not None and age <= budget:
        return "ok", False, f"index heartbeat {age:.0f}s old (<= {budget}s budget)"
    if shards_exist:
        age_str = f"{age:.0f}s" if age is not None else "missing"
        return (
            "critical",
            True,
            f"index {age_str} (> {budget}s budget) while per-VM shards exist — consolidator behind/DOWN",
        )
    age_str = f"{age:.0f}s old" if age is not None else "missing"
    return "degraded", False, f"index {age_str}; no per-VM shards — genuinely empty bucket, not an outage"


def _ag_health(asset_group: AssetGroup, budget: int, now: datetime) -> ConsolidatorAgHealth:
    """Build the consolidator posture for one asset_group (honest per-AG degradation)."""
    try:
        bucket = resolve_bucket_name(cloud="gcp", kind="raw_tick_data", asset_group=asset_group)
    except (OSError, ValueError) as exc:
        logger.warning("consolidator-health: bucket resolution failed for %s: %s", asset_group, exc)
        return ConsolidatorAgHealth(
            asset_group=asset_group,
            bucket="",
            status="unknown",
            staleness_budget_seconds=budget,
            per_vm_shard_fallback_active=False,
            detail=f"bucket resolution failed: {exc}",
        )
    try:
        client = get_storage_client()
        age = consolidated_blob_age_sec(client, bucket)
        shards_exist = age is None or age > budget
        # Only pay for the shard-list when the index looks stale/missing (the discriminator).
        shards_present = per_vm_shards_exist(client, bucket, exclude_self=True) if shards_exist else False
        status, fallback, detail = _classify_ag(age, budget, shards_present)
        last_run: str | None = None
        if age is not None:
            last_run = (now - timedelta(seconds=age)).isoformat()
        return ConsolidatorAgHealth(
            asset_group=asset_group,
            bucket=bucket,
            status=status,
            index_age_seconds=round(age, 1) if age is not None else None,
            staleness_budget_seconds=budget,
            per_vm_shard_fallback_active=fallback,
            last_successful_run_at=last_run,
            detail=detail,
        )
    except (OSError, ValueError, RuntimeError) as exc:
        logger.warning("consolidator-health: read failed for %s (%s): %s", asset_group, bucket, exc)
        return ConsolidatorAgHealth(
            asset_group=asset_group,
            bucket=bucket,
            status="unknown",
            staleness_budget_seconds=budget,
            per_vm_shard_fallback_active=False,
            detail=f"consolidator read failed: {exc}",
        )


def build_consolidator_health(ag_entries: list[ConsolidatorAgHealth], now: datetime) -> ConsolidatorHealthResponse:
    """Roll per-AG postures into the response with a worst-first overall."""
    overall = "ok"
    if ag_entries:
        overall = min((e.status for e in ag_entries), key=_status_rank)
    return ConsolidatorHealthResponse(
        generated_at=now.isoformat(),
        overall=overall,
        asset_groups=ag_entries,
    )


def _mock_response(now: datetime) -> ConsolidatorHealthResponse:
    """Representative mock consolidator health (mock mode — no GCS access)."""
    budget = 86400
    entries = [
        ConsolidatorAgHealth(
            asset_group="cefi",
            bucket="raw-tick-data-cefi-mock",
            status="ok",
            index_age_seconds=42.0,
            staleness_budget_seconds=budget,
            per_vm_shard_fallback_active=False,
            last_successful_run_at=now.isoformat(),
            detail="index heartbeat 42s old (<= 86400s budget)",
        ),
        ConsolidatorAgHealth(
            asset_group="defi",
            bucket="raw-tick-data-defi-mock",
            status="critical",
            index_age_seconds=90000.0,
            staleness_budget_seconds=budget,
            per_vm_shard_fallback_active=True,
            last_successful_run_at=None,
            detail="index 90000s (> 86400s budget) while per-VM shards exist — consolidator behind/DOWN",
        ),
    ]
    return build_consolidator_health(entries, now)


@router.get("/health/consolidator", response_model=ConsolidatorHealthResponse)
def get_consolidator_health() -> ConsolidatorHealthResponse:
    """Per-asset_group manifest-consolidator health drill-down.

    For each asset_group's ``raw_tick_data`` bucket: the consolidated availability-index
    heartbeat age, whether the per-VM shard recovery-merge fallback is active (stale index
    + shards present = consolidator behind/down), the derived health status, and the last
    successful run timestamp. Read-only; degrades to ``status="unknown"`` per-AG on a read
    failure, never a 5xx.
    """
    now = datetime.now(UTC)
    if _cfg.is_mock_mode():
        return _mock_response(now)
    budget = resolve_consolidated_staleness_sec()
    entries = [_ag_health(ag, budget, now) for ag in _ASSET_GROUPS]
    return build_consolidator_health(entries, now)


__all__ = [
    "ConsolidatorAgHealth",
    "ConsolidatorHealthResponse",
    "build_consolidator_health",
    "router",
]

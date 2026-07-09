# Epic: observability_master
# Lifecycle: permanent
"""GET /api/health/consolidator — manifest-consolidator health drill-down per asset_group.

Replaces today's binary up/down (``CONSOLIDATOR_DOWN`` alert / ``assert_consolidator_healthy``
raise) with a per-asset_group posture the cockpit Health pane can render: for each
asset_group's ``market-data`` bucket we report the consolidated ``_index/
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
    per_vm_shard_backlog,
    per_vm_shards_exist,
    read_availability_index,
    resolve_bucket_name,
    resolve_consolidated_staleness_sec,
)

from deployment_api.deployment_api_config import DeploymentApiConfig

router = APIRouter()
logger = logging.getLogger(__name__)

_cfg = DeploymentApiConfig()

# The asset_groups whose market-data buckets carry an availability_index the
# consolidator maintains (the canonical lowercase set, UAC AssetGroup literals).
_ASSET_GROUPS: tuple[AssetGroup, ...] = ("cefi", "defi", "tradfi", "sports", "prediction")

# Per-asset_group market-data bucket KIND. cefi/defi/tradfi/sports live under the shared
# ``market-data`` kind (the ``market-data-tick-<ag>-...`` buckets); prediction has its own
# dedicated flat key ``market-data-tick-prediction`` (``market-data-tick-pred-...``), so the
# shared ``market-data`` kind has no ``prediction`` entry. Resolve the right kind per AG.
_MARKET_DATA_KIND: dict[str, str] = {"prediction": "market-data-tick-prediction"}


def _market_data_kind(asset_group: str) -> str:
    """Bucket kind for an asset_group's market-data store (prediction has a dedicated key)."""
    return _MARKET_DATA_KIND.get(asset_group, "market-data")


class ConsolidatorAgHealth(BaseModel):  # CORRECT-LOCAL: FastAPI API contract model
    """Per-asset_group manifest-consolidator posture."""

    asset_group: str
    bucket: str
    status: str  # "ok" | "degraded" | "critical" | "unknown"
    index_age_seconds: float | None = None  # heartbeat age of the consolidated index
    staleness_budget_seconds: int
    per_vm_shard_fallback_active: bool  # stale/missing index WHILE shards exist → recovery merge
    last_successful_run_at: str | None = None  # ISO-8601, derived from index mtime
    pending_shard_count: int | None = None  # per-VM shards written since the last merge (backlog)
    total_shard_count: int | None = None  # per-VM shards present (fan-in width)
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


def _ag_health(
    asset_group: AssetGroup, budget: int, now: datetime, *, include_backlog: bool = False
) -> ConsolidatorAgHealth:
    """Build the consolidator posture for one asset_group (honest per-AG degradation).

    ``include_backlog=True`` also counts the per-VM shard backlog (shards written since
    the last merge → not yet absorbed) via ONE extra prefix list. It is opt-in: the
    Consolidators-tab endpoint sets it; the per-deployment ``/freshness`` reuse (via
    ``consolidator_posture``) leaves it off so that hotter path pays no extra list.
    """
    try:
        bucket = resolve_bucket_name(cloud="gcp", kind=_market_data_kind(asset_group), asset_group=asset_group)
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
        index_mtime = (now - timedelta(seconds=age)) if age is not None else None
        pending_count: int | None = None
        total_count: int | None = None
        if include_backlog:
            # ONE prefix list gives BOTH the backlog counts AND shard existence.
            pending_count, total_count = per_vm_shard_backlog(client, bucket, index_mtime)
            shards_present = total_count > 0
        else:
            # Only pay for the shard-list when the index looks stale/missing (the discriminator).
            shards_present = per_vm_shards_exist(client, bucket, exclude_self=True) if shards_exist else False
        status, fallback, detail = _classify_ag(age, budget, shards_present)
        last_run = index_mtime.isoformat() if index_mtime is not None else None
        return ConsolidatorAgHealth(
            asset_group=asset_group,
            bucket=bucket,
            status=status,
            index_age_seconds=round(age, 1) if age is not None else None,
            staleness_budget_seconds=budget,
            per_vm_shard_fallback_active=fallback,
            last_successful_run_at=last_run,
            pending_shard_count=pending_count,
            total_shard_count=total_count,
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


def consolidator_posture(asset_group: AssetGroup, now: datetime) -> ConsolidatorAgHealth:
    """Public per-asset_group manifest-index posture (index age / fallback / last run).

    The availability-index heartbeat IS the manifest-derived freshness for an
    asset_group's owned shards, so the per-deployment freshness endpoint
    (``/api/deployments/{id}/freshness``) reuses this rather than re-walking the
    manifest. Uses the canonical consolidated-staleness budget.
    """
    return _ag_health(asset_group, resolve_consolidated_staleness_sec(), now)


def object_delta_for_bucket(bucket: str) -> tuple[int | None, str]:
    """Object-count delta = a manifest LOOKUP off the consolidated index (no new bucket walk).

    Reads the SAME consolidated ``availability_index`` blob ``consolidator_posture`` already
    resolved (``read_availability_index`` hits the process-level index cache health_consolidator
    just warmed), sums ``row_count``-else-``instrument_count`` for ``capture_status="captured"``
    rows per written date, and diffs the two most recent written dates. This is the authoritative
    write-truth signal for WS-D's composite health (D.1) — objects that actually landed, not the
    log-scraped ``rows_out`` hint. Honest degradation: any read failure or <2 distinct written
    dates yields ``(None, <reason>)``, never a false zero.

    Lives here (bucket-only, not deployment-id-scoped) rather than in the per-deployment
    ``/freshness`` route so both that route AND the composite-health `stalled` classifier
    (``object_delta_for_asset_group`` below — batched ONE call per distinct asset_group per
    census cycle, not once per VM entry) can share the same manifest read without a circular
    import between ``deployment_freshness`` and ``deployments_inventory``.
    """
    try:
        index = read_availability_index(bucket, columns=["date", "row_count", "instrument_count", "capture_status"])
    except (OSError, ValueError, RuntimeError) as exc:
        return None, f"manifest read failed: {exc}"
    if index.empty:
        return None, "manifest index is empty"
    captured = index[index["capture_status"] == "captured"]
    if captured.empty:
        return None, "no captured rows in manifest index"
    counts = captured["row_count"].where(captured["row_count"] > 0, captured["instrument_count"])
    by_date = counts.groupby(captured["date"]).sum().sort_index()
    if len(by_date) < 2:
        return None, f"only {len(by_date)} distinct written date(s) in manifest — nothing to diff yet"
    latest_date, prior_date = by_date.index[-1], by_date.index[-2]
    delta = int(by_date.iloc[-1] - by_date.iloc[-2])
    return delta, f"{latest_date} object count {by_date.iloc[-1]:.0f} vs {prior_date} {by_date.iloc[-2]:.0f}"


def object_delta_for_asset_group(asset_group: str, now: datetime) -> tuple[int | None, str]:
    """Object-count delta for an asset_group's market-data bucket — keyed by asset_group ALONE.

    A thin combinator over ``consolidator_posture`` (bucket resolution) + ``object_delta_for_bucket``,
    so a caller that needs this per DISTINCT asset_group (not per specific deployment_id) — e.g. the
    composite-health `stalled` classifier looping many VM entries that share an asset_group — can
    batch it exactly once per asset_group per cycle instead of re-deriving it per VM.
    """
    if asset_group not in _ASSET_GROUPS:
        return None, f"asset_group {asset_group!r} has no availability-index to read freshness from"
    posture = consolidator_posture(asset_group, now)  # type: ignore[arg-type]  # validated against _ASSET_GROUPS above
    if not posture.bucket:
        return None, posture.detail
    return object_delta_for_bucket(posture.bucket)


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
            bucket="market-data-cefi-mock",
            status="ok",
            index_age_seconds=42.0,
            staleness_budget_seconds=budget,
            per_vm_shard_fallback_active=False,
            last_successful_run_at=now.isoformat(),
            pending_shard_count=2,  # small in-flight backlog is normal (~1 merge cycle)
            total_shard_count=6,
            detail="index heartbeat 42s old (<= 86400s budget)",
        ),
        ConsolidatorAgHealth(
            asset_group="defi",
            bucket="market-data-defi-mock",
            status="critical",
            index_age_seconds=90000.0,
            staleness_budget_seconds=budget,
            per_vm_shard_fallback_active=True,
            last_successful_run_at=None,
            pending_shard_count=47,  # consolidator behind → large unabsorbed backlog
            total_shard_count=48,
            detail="index 90000s (> 86400s budget) while per-VM shards exist — consolidator behind/DOWN",
        ),
    ]
    return build_consolidator_health(entries, now)


@router.get("/health/consolidator", response_model=ConsolidatorHealthResponse)
def get_consolidator_health() -> ConsolidatorHealthResponse:
    """Per-asset_group manifest-consolidator health drill-down.

    For each asset_group's ``market-data`` bucket: the consolidated availability-index
    heartbeat age, whether the per-VM shard recovery-merge fallback is active (stale index
    + shards present = consolidator behind/down), the derived health status, and the last
    successful run timestamp. Read-only; degrades to ``status="unknown"`` per-AG on a read
    failure, never a 5xx.
    """
    now = datetime.now(UTC)
    if _cfg.is_mock_mode():
        return _mock_response(now)
    budget = resolve_consolidated_staleness_sec()
    entries = [_ag_health(ag, budget, now, include_backlog=True) for ag in _ASSET_GROUPS]
    return build_consolidator_health(entries, now)


__all__ = [
    "ConsolidatorAgHealth",
    "ConsolidatorHealthResponse",
    "build_consolidator_health",
    "consolidator_posture",
    "router",
]

# Epic: observability_master
# Lifecycle: permanent
"""GET /api/deployments/{deployment_id}/freshness — per-deployment data freshness.

Health (a liveness ping) is NOT data freshness. The cockpit needs to answer "is the
data this deployment is RESPONSIBLE FOR actually fresh?" — attributed PER deployment,
not guessed from an in-memory ``make_health_router(data_freshness=...)`` callback.

The binding *deployment → the shard-set it owns* is the deployment-service resolver
``responsibility_for_deployment`` (a pure derivation off the classified
service+asset_group+umbrella). The per-shard freshness SSOT is the availability
**manifest**, whose consolidated ``_index`` heartbeat age IS the manifest-derived
freshness for an asset_group's owned shards — so this endpoint REUSES the shipped
consolidator posture reader (``health_consolidator.consolidator_posture``) rather than
re-walking the manifest. A deployment that owns no shards (gateway / control-plane)
resolves to ``ShardResponsibilityKind.NONE`` and renders ``liveness_only`` — never a
false "fresh".

Doc / contract: ``ShardResponsibility`` (UAC ``canonical/crosscutting/lifecycle_class``)
+ ``deployment_service.deployment_cluster_registry.responsibility_for_deployment``.
SSOT: ``plans/active/unified_deployment_health_cockpit_2026_06_23.md`` Phase 4.5.
"""

from __future__ import annotations

import logging
from datetime import UTC, datetime

from deployment_service.deployment_classification import UnclassifiedDeploymentError
from deployment_service.deployment_cluster_registry import responsibility_for_deployment
from fastapi import APIRouter, HTTPException
from pydantic import BaseModel
from unified_api_contracts import ShardResponsibilityKind
from unified_trading_library import read_availability_index

from deployment_api.deployment_api_config import DeploymentApiConfig
from deployment_api.routes.deployments_inventory import classify_vm_target
from deployment_api.routes.health_consolidator import consolidator_posture

router = APIRouter()
logger = logging.getLogger(__name__)

_cfg = DeploymentApiConfig()

# The asset_groups whose market-data buckets carry an availability_index (mirrors
# health_consolidator._ASSET_GROUPS — the manifest-freshness reader only knows these).
_FRESHNESS_ASSET_GROUPS = frozenset({"cefi", "defi", "tradfi", "sports", "prediction"})


class DeploymentFreshness(BaseModel):  # CORRECT-LOCAL: FastAPI API contract model
    """Per-deployment data-freshness posture (manifest-derived, not a health ping)."""

    deployment_id: str
    # ShardResponsibilityKind value: asset_group_capture | manifest_consolidation | strategy_shard | none
    responsibility: str
    asset_group: str | None
    mode: str | None = None
    freshness_status: str  # "fresh" | "stale" | "liveness_only" | "unknown"
    index_age_seconds: float | None = None
    staleness_budget_seconds: int | None = None
    per_vm_shard_fallback_active: bool = False
    oldest_available_at: str | None = None
    # Object-count delta (WS-D D.1 `object_delta` — the AUTHORITATIVE write-truth signal,
    # vs the log-scraped `rows_out` hint): captured-row count for the manifest's most recent
    # written date minus the prior written date, for this asset_group. A manifest LOOKUP
    # (read_availability_index reads the SAME consolidated index blob already resolved via
    # `consolidator_posture` — zero new bucket walks). None when there's <2 distinct written
    # dates to diff (empty / freshly-seeded index) or a manifest read fails (honest degradation).
    object_delta: int | None = None
    object_delta_detail: str = ""
    detail: str = ""


# consolidator status → freshness_status (the manifest-index heartbeat IS the freshness signal).
_FRESHNESS_BY_STATUS: dict[str, str] = {
    "ok": "fresh",
    "degraded": "stale",
    "critical": "stale",
    "unknown": "unknown",
}


def _mock_freshness(deployment_id: str) -> DeploymentFreshness:
    """Representative mock freshness (mock mode — no GCS access)."""
    if "gateway" in deployment_id or "api" in deployment_id:
        return DeploymentFreshness(
            deployment_id=deployment_id,
            responsibility=ShardResponsibilityKind.NONE.value,
            asset_group=None,
            freshness_status="liveness_only",
            detail="liveness-only (gateway / control-plane — no data-freshness obligation)",
        )
    return DeploymentFreshness(
        deployment_id=deployment_id,
        responsibility=ShardResponsibilityKind.ASSET_GROUP_CAPTURE.value,
        asset_group="cefi",
        freshness_status="fresh",
        index_age_seconds=42.0,
        staleness_budget_seconds=86400,
        per_vm_shard_fallback_active=False,
        oldest_available_at="2026-06-24T06:55:00+00:00",
        object_delta=128,
        object_delta_detail="2026-06-24 object count 4128 vs 2026-06-23 4000",
        detail="index heartbeat 42s old (<= 86400s budget)",
    )


def _object_delta_for_bucket(bucket: str) -> tuple[int | None, str]:
    """Object-count delta = a manifest LOOKUP off the consolidated index (no new bucket walk).

    Reads the SAME consolidated ``availability_index`` blob ``consolidator_posture`` already
    resolved (``read_availability_index`` hits the process-level index cache health_consolidator
    just warmed), sums ``row_count``-else-``instrument_count`` for ``capture_status="captured"``
    rows per written date, and diffs the two most recent written dates. This is the authoritative
    write-truth signal for WS-D's composite health (D.1) — objects that actually landed, not the
    log-scraped ``rows_out`` hint. Honest degradation: any read failure or <2 distinct written
    dates yields ``(None, <reason>)``, never a false zero.
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


def compute_freshness(deployment_id: str, now: datetime) -> DeploymentFreshness:
    """Resolve a deployment's responsibility then read its owned shards' manifest freshness.

    ``NONE`` → liveness-only (no false fresh). A capture / consolidation / strategy
    responsibility → the asset_group's consolidated-index posture (the manifest-derived
    freshness). An asset_group outside the known availability-index set → ``unknown``.
    """
    target = classify_vm_target(deployment_id)
    responsibility = responsibility_for_deployment(target)
    kind = responsibility.kind

    if kind == ShardResponsibilityKind.NONE:
        return DeploymentFreshness(
            deployment_id=deployment_id,
            responsibility=kind.value,
            asset_group=None,
            freshness_status="liveness_only",
            detail="liveness-only (gateway / control-plane — no data-freshness obligation)",
        )

    asset_group = responsibility.asset_group
    mode = responsibility.mode or None
    if not asset_group or asset_group not in _FRESHNESS_ASSET_GROUPS:
        return DeploymentFreshness(
            deployment_id=deployment_id,
            responsibility=kind.value,
            asset_group=asset_group or None,
            mode=mode,
            freshness_status="unknown",
            detail=f"asset_group {asset_group!r} has no availability-index to read freshness from",
        )

    posture = consolidator_posture(asset_group, now)  # type: ignore[arg-type]  # validated against the AssetGroup literal set above
    object_delta, object_delta_detail = _object_delta_for_bucket(posture.bucket) if posture.bucket else (None, "")
    return DeploymentFreshness(
        deployment_id=deployment_id,
        responsibility=kind.value,
        asset_group=asset_group,
        mode=mode,
        freshness_status=_FRESHNESS_BY_STATUS.get(posture.status, "unknown"),
        index_age_seconds=posture.index_age_seconds,
        staleness_budget_seconds=posture.staleness_budget_seconds,
        per_vm_shard_fallback_active=posture.per_vm_shard_fallback_active,
        oldest_available_at=posture.last_successful_run_at,
        object_delta=object_delta,
        object_delta_detail=object_delta_detail,
        detail=posture.detail,
    )


@router.get("/deployments/{deployment_id}/freshness", response_model=DeploymentFreshness)
def get_deployment_freshness(deployment_id: str) -> DeploymentFreshness:
    """Manifest-derived data freshness for the shards a deployment is responsible for.

    Classifies the deployment → ``ShardResponsibility`` → reads the owned asset_group's
    consolidated availability-index freshness. ``NONE`` (gateway / control-plane) →
    ``liveness_only`` (never a false fresh). Read-only; an unclassifiable id → 404.
    """
    if _cfg.is_mock_mode():
        return _mock_freshness(deployment_id)
    now = datetime.now(UTC)
    try:
        return compute_freshness(deployment_id, now)
    except UnclassifiedDeploymentError as exc:
        raise HTTPException(status_code=404, detail=f"unclassifiable deployment {deployment_id!r}: {exc}") from exc


__all__ = ["DeploymentFreshness", "compute_freshness", "get_deployment_freshness", "router"]

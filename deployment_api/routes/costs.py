"""Cost observability API — GET /api/costs/{summary,breakdown,timeseries}.

Comprehensive cross-cloud billing for the deployment-UI /ops/costs page, served from the
real GCP BigQuery + AWS CUR/Athena exports (GitHub is a labelled dummy until a PAT lands).
Replaces the narrow self-reported `cost_summary` pipeline that previously backed this page.

SSOT for the backends + IAM: codex/05-infrastructure/billing-cost-observability.md
"""

from __future__ import annotations

import asyncio
import logging
from datetime import date
from typing import Literal

from fastapi import APIRouter, Depends, HTTPException, Query

from deployment_api.deployment_api_config import DeploymentApiConfig
from deployment_api.rate_limiting import endpoint_rate_limit
from deployment_api.scripts.cost_snapshot_worker import DEFAULT_LOOKBACK_DAYS, run_snapshot
from deployment_api.services.cost_observability import (
    BreakdownResponse,
    CostObservabilityService,
    SummaryResponse,
    TimeseriesResponse,
)
from deployment_api.services.cost_observability.snapshot import SNAPSHOT_CLOUDS

router = APIRouter()
logger = logging.getLogger(__name__)

# Module-level singleton so the cache persists across requests (and is shared with the
# health-overview cost tile, which imports it).
cost_service = CostObservabilityService()

Dimension = Literal["service", "resource", "bucket", "region", "day", "sku", "zone", "label"]
CloudFilter = Literal["all", "gcp", "aws", "github"]
# Which GCP business label the `label` dimension groups by (GCP-only; AWS/GitHub → "(unlabeled)").
LabelKey = Literal["purpose", "category", "venue", "asset_group"]

# Longest selectable window — mirrors the service's own `_MAX_DAYS` clamp and the `days` param's
# `le=366`, so an explicit range can't out-scan what a trailing window already allows.
MAX_RANGE_DAYS = 366

_START_DESC = "Range start (inclusive, YYYY-MM-DD). Give BOTH start_date and end_date to override `days`."
_END_DESC = "Range end (inclusive, YYYY-MM-DD). Give BOTH start_date and end_date to override `days`."


def _resolve_range(start_date: date | None, end_date: date | None) -> tuple[date | None, date | None]:
    """Validate an optional explicit window, 400-ing on anything ambiguous or unbounded.

    The service normalizes silently (a defensive clamp for direct callers); the HTTP surface is the
    LOUD gate, because a request that quietly returns a different window than the one asked for is
    indistinguishable — to the operator reading the page — from real spend moving.

    * both-or-neither — a half-open range is ambiguous (is the other end today, or the first day of
      the export?), so it's rejected rather than guessed;
    * start after end is rejected rather than swapped — it means the caller built the range wrong,
      and silently reversing it hides that bug;
    * over-long spans are rejected rather than clamped, for the same reason.
    """
    if start_date is None and end_date is None:
        return None, None
    if start_date is None or end_date is None:
        raise HTTPException(
            status_code=400,
            detail={
                "code": "COST_RANGE_INCOMPLETE",
                "message": "start_date and end_date must be provided together (or use `days`).",
            },
        )
    if start_date > end_date:
        raise HTTPException(
            status_code=400,
            detail={
                "code": "COST_RANGE_INVERTED",
                "message": f"start_date ({start_date.isoformat()}) is after end_date ({end_date.isoformat()}).",
            },
        )
    span = (end_date - start_date).days + 1
    if span > MAX_RANGE_DAYS:
        raise HTTPException(
            status_code=400,
            detail={
                "code": "COST_RANGE_TOO_LONG",
                "message": f"Range spans {span} days; the maximum is {MAX_RANGE_DAYS}.",
            },
        )
    return start_date, end_date


@router.get("/costs/summary", response_model=SummaryResponse)
def get_cost_summary(
    days: int = Query(30, ge=1, le=366, description="Window length in days (ending today)"),
    start_date: date | None = Query(None, description=_START_DESC),
    end_date: date | None = Query(None, description=_END_DESC),
    refresh: bool = Query(False, description="Bypass the cache and re-query the exports"),
    _rl: None = Depends(endpoint_rate_limit(30)),
) -> SummaryResponse:
    """Top-line total, per-cloud totals with deltas, and daily sparkline series."""
    start, end = _resolve_range(start_date, end_date)
    try:
        return cost_service.summarize(days, start_date=start, end_date=end, force=refresh)
    except Exception as exc:
        logger.exception("cost summary failed")
        raise HTTPException(
            status_code=502, detail={"code": "COST_QUERY_FAILED", "message": "Cost data unavailable"}
        ) from exc


@router.get("/costs/breakdown", response_model=BreakdownResponse)
def get_cost_breakdown(
    dimension: Dimension = Query("service", description="How to slice the spend"),
    cloud: CloudFilter = Query("all", description="Filter to one cloud"),
    days: int = Query(30, ge=1, le=366),
    start_date: date | None = Query(None, description=_START_DESC),
    end_date: date | None = Query(None, description=_END_DESC),
    label_key: LabelKey = Query("purpose", description="GCP label to group by when dimension=label"),
    refresh: bool = Query(False),
    _rl: None = Depends(endpoint_rate_limit(30)),
) -> BreakdownResponse:
    """Spend grouped by service / resource / bucket / region / day / sku / zone / label."""
    start, end = _resolve_range(start_date, end_date)
    try:
        return cost_service.breakdown(
            dimension, cloud, days, label_key=label_key, start_date=start, end_date=end, force=refresh
        )
    except Exception as exc:
        logger.exception("cost breakdown failed")
        raise HTTPException(
            status_code=502, detail={"code": "COST_QUERY_FAILED", "message": "Cost data unavailable"}
        ) from exc


@router.get("/costs/timeseries", response_model=TimeseriesResponse)
def get_cost_timeseries(
    days: int = Query(30, ge=1, le=366),
    cloud: CloudFilter = Query("all"),
    start_date: date | None = Query(None, description=_START_DESC),
    end_date: date | None = Query(None, description=_END_DESC),
    refresh: bool = Query(False),
    _rl: None = Depends(endpoint_rate_limit(30)),
) -> TimeseriesResponse:
    """Daily spend per cloud for the stacked trend chart."""
    start, end = _resolve_range(start_date, end_date)
    try:
        return cost_service.timeseries(days, cloud, start_date=start, end_date=end, force=refresh)
    except Exception as exc:
        logger.exception("cost timeseries failed")
        raise HTTPException(
            status_code=502, detail={"code": "COST_QUERY_FAILED", "message": "Cost data unavailable"}
        ) from exc


@router.post("/costs/snapshot-run")
async def run_cost_snapshot(
    clouds: list[str] | None = Query(None, description="Clouds to snapshot (default: all present)"),
) -> dict[str, object]:
    """Compute + write the per-cloud cost parquet snapshot to the state bucket (Cloud Scheduler target).

    Runs IN the gen1 deployment-api service — the same reason data-status uses an in-service endpoint
    (Cloud Run Jobs are gen2-only and native pyarrow compute can crash there). Synchronous (the caller
    is the scheduler with a long attempt-deadline, not the UI). Each per-cloud write is overwrite-by-name
    (idempotent); a partial failure returns ``status="partial"`` and the next tick recomputes. The AWS
    slice needs AWS creds on the service (see billing-cost-observability.md) — it degrades to a skipped
    cloud when absent, never failing GCP/GitHub.
    """
    cfg = DeploymentApiConfig()
    project = cfg.require_gcp_project_id()
    bucket = cfg.effective_state_bucket
    cloud_list = list(clouds) if clouds else list(SNAPSHOT_CLOUDS)

    logger.info("cost snapshot: clouds=%s -> gs://%s/cost-snapshots/", cloud_list, bucket)
    rc = await asyncio.to_thread(run_snapshot, project, bucket, cloud_list, DEFAULT_LOOKBACK_DAYS)
    return {"status": "ok" if rc == 0 else "partial", "clouds": cloud_list, "exit_code": rc, "bucket": bucket}

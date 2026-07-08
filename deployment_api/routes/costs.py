"""Cost observability API — GET /api/costs/{summary,breakdown,timeseries}.

Comprehensive cross-cloud billing for the deployment-UI /ops/costs page, served from the
real GCP BigQuery + AWS CUR/Athena exports (GitHub is a labelled dummy until a PAT lands).
Replaces the narrow self-reported `cost_summary` pipeline that previously backed this page.

SSOT for the backends + IAM: codex/05-infrastructure/billing-cost-observability.md
"""

from __future__ import annotations

import logging
from typing import Literal

from fastapi import APIRouter, Depends, HTTPException, Query

from deployment_api.rate_limiting import endpoint_rate_limit
from deployment_api.services.cost_observability import (
    BreakdownResponse,
    CostObservabilityService,
    SummaryResponse,
    TimeseriesResponse,
)

router = APIRouter()
logger = logging.getLogger(__name__)

# Module-level singleton so the cache persists across requests (and is shared with the
# health-overview cost tile, which imports it).
cost_service = CostObservabilityService()

Dimension = Literal["service", "resource", "bucket", "region", "day"]
CloudFilter = Literal["all", "gcp", "aws", "github"]


@router.get("/costs/summary", response_model=SummaryResponse)
def get_cost_summary(
    days: int = Query(30, ge=1, le=366, description="Window length in days (ending today)"),
    refresh: bool = Query(False, description="Bypass the cache and re-query the exports"),
    _rl: None = Depends(endpoint_rate_limit(30)),
) -> SummaryResponse:
    """Top-line total, per-cloud totals with deltas, and daily sparkline series."""
    try:
        return cost_service.summarize(days, force=refresh)
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
    refresh: bool = Query(False),
    _rl: None = Depends(endpoint_rate_limit(30)),
) -> BreakdownResponse:
    """Spend grouped by service / resource / bucket / region / day."""
    try:
        return cost_service.breakdown(dimension, cloud, days, force=refresh)
    except Exception as exc:
        logger.exception("cost breakdown failed")
        raise HTTPException(
            status_code=502, detail={"code": "COST_QUERY_FAILED", "message": "Cost data unavailable"}
        ) from exc


@router.get("/costs/timeseries", response_model=TimeseriesResponse)
def get_cost_timeseries(
    days: int = Query(30, ge=1, le=366),
    cloud: CloudFilter = Query("all"),
    refresh: bool = Query(False),
    _rl: None = Depends(endpoint_rate_limit(30)),
) -> TimeseriesResponse:
    """Daily spend per cloud for the stacked trend chart."""
    try:
        return cost_service.timeseries(days, cloud, force=refresh)
    except Exception as exc:
        logger.exception("cost timeseries failed")
        raise HTTPException(
            status_code=502, detail={"code": "COST_QUERY_FAILED", "message": "Cost data unavailable"}
        ) from exc

"""Artifact pipeline API — GET /api/artifacts/{running,deploys,builds,images,health}.

The deployment estate's final stage end-to-end: what's actually running (with drift flags), the
deploy timeline, the build pipeline (both clouds, both lanes, structured failure), the registry
inventory, and measured pipeline health. Served from the live cloud APIs behind a small TTL cache
(the cost-observability shape), so the page is cheap and OOM-safe. Unknowns render as an explicit
"unknown + why", never a fabricated green.

SSOT: plans/active/artifact_pipeline_observability_2026_07_17.md
"""

from __future__ import annotations

import logging
from datetime import date
from typing import Literal

from fastapi import APIRouter, Depends, HTTPException, Query

from deployment_api.rate_limiting import endpoint_rate_limit
from deployment_api.services.artifact_pipeline import ArtifactPipelineService, BuildsResponse, DeploysResponse

router = APIRouter()
logger = logging.getLogger(__name__)

# Module-level singleton so the TTL cache persists across requests.
artifact_service = ArtifactPipelineService()

CloudFilter = Literal["all", "gcp", "aws"]
LaneFilter = Literal["all", "image", "tarball"]
BuildStatusFilter = Literal["all", "failed"]
DeployChangeFilter = Literal["all", "code", "live", "fail"]

# Longest selectable window — mirrors the service's `_MAX_DAYS` clamp and the `days` param `le=366`.
MAX_RANGE_DAYS = 366

_START_DESC = "Range start (inclusive, YYYY-MM-DD). Give BOTH start_date and end_date to override `days`."
_END_DESC = "Range end (inclusive, YYYY-MM-DD). Give BOTH start_date and end_date to override `days`."


def _resolve_range(start_date: date | None, end_date: date | None) -> tuple[date | None, date | None]:
    """Validate an optional explicit window, 400-ing on anything ambiguous or unbounded.

    The service normalizes silently (a defensive clamp for direct callers); the HTTP surface is the
    LOUD gate — a request that quietly returns a different window than the one asked for is
    indistinguishable, to the operator reading the page, from the estate actually changing.
    """
    if start_date is None and end_date is None:
        return None, None
    if start_date is None or end_date is None:
        raise HTTPException(
            status_code=400,
            detail={
                "code": "ARTIFACT_RANGE_INCOMPLETE",
                "message": "start_date and end_date must be provided together (or use `days`).",
            },
        )
    if start_date > end_date:
        raise HTTPException(
            status_code=400,
            detail={
                "code": "ARTIFACT_RANGE_INVERTED",
                "message": f"start_date ({start_date.isoformat()}) is after end_date ({end_date.isoformat()}).",
            },
        )
    span = (end_date - start_date).days + 1
    if span > MAX_RANGE_DAYS:
        raise HTTPException(
            status_code=400,
            detail={
                "code": "ARTIFACT_RANGE_TOO_LONG",
                "message": f"Range spans {span} days; the maximum is {MAX_RANGE_DAYS}.",
            },
        )
    return start_date, end_date


@router.get("/artifacts/builds", response_model=BuildsResponse)
def get_artifact_builds(
    days: int = Query(30, ge=1, le=366, description="Window length in days (ending today)"),
    cloud: CloudFilter = Query("all", description="Filter to one cloud"),
    lane: LaneFilter = Query("all", description="Filter to one lane (image / tarball)"),
    status: BuildStatusFilter = Query("all", description="Filter to failed builds only"),
    start_date: date | None = Query(None, description=_START_DESC),
    end_date: date | None = Query(None, description=_END_DESC),
    refresh: bool = Query(False, description="Bypass the cache and re-query the build APIs"),
    _rl: None = Depends(endpoint_rate_limit(30)),
) -> BuildsResponse:
    """The Pipeline view: both clouds, both lanes, structured failure, dup / cross-lane flags."""
    start, end = _resolve_range(start_date, end_date)
    try:
        return artifact_service.builds(days, cloud, lane, status, start_date=start, end_date=end, force=refresh)
    except Exception as exc:
        logger.exception("artifact builds failed")
        raise HTTPException(
            status_code=502, detail={"code": "ARTIFACT_QUERY_FAILED", "message": "Artifact data unavailable"}
        ) from exc


@router.get("/artifacts/deploys", response_model=DeploysResponse)
def get_artifact_deploys(
    days: int = Query(30, ge=1, le=366, description="Window length in days (ending today)"),
    cloud: CloudFilter = Query("all", description="Filter to one cloud"),
    change: DeployChangeFilter = Query(
        "all", description="Filter: 'code' hides config-only churn, 'live' = serving now, 'fail' = never went ready"
    ),
    start_date: date | None = Query(None, description=_START_DESC),
    end_date: date | None = Query(None, description=_END_DESC),
    refresh: bool = Query(False, description="Bypass the cache and re-query the deploy APIs"),
    _rl: None = Depends(endpoint_rate_limit(30)),
) -> DeploysResponse:
    """The Deploy timeline view: every Cloud Run revision, its change-type, held-for, deployer."""
    start, end = _resolve_range(start_date, end_date)
    try:
        return artifact_service.deploys(days, cloud, change, start_date=start, end_date=end, force=refresh)
    except Exception as exc:
        logger.exception("artifact deploys failed")
        raise HTTPException(
            status_code=502, detail={"code": "ARTIFACT_QUERY_FAILED", "message": "Artifact data unavailable"}
        ) from exc

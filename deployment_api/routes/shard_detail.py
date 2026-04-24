"""Unified shard-detail + venue-detail routes.

Exposes ``GET /api/data-status/shard-detail`` (single-entry endpoint for
the Data Status drilldown modal) and ``GET /api/data-status/venue-detail``
(Instrument Breakdown card — DeFi-aware).

Routes live in their own module so the concurrent edits to
``data_status.py`` do not block adding the new endpoints.  ``main.py``
mounts this router alongside the existing data_status router under the
same ``/api/data-status`` prefix so URLs are unified.
"""

from __future__ import annotations

import logging
from datetime import date

from fastapi import APIRouter, HTTPException, Query
from fastapi.concurrency import run_in_threadpool

from deployment_api.services.shard_detail import fetch_venue_detail, get_shard_detail
from deployment_api.types.shard_detail import ShardDetailResponse, VenueDetailResponse

logger = logging.getLogger(__name__)

router = APIRouter()


@router.get("/shard-detail", response_model=ShardDetailResponse)
async def get_shard_detail_route(
    service: str = Query(
        ..., description="Service name (market-tick-data-service, instruments-service, …)"
    ),
    category: str = Query(
        ..., description="Category (CEFI / TRADFI / DEFI / SPORTS / PREDICTION / INSTRUMENTS)"
    ),
    instrument_type: str = Query(..., description="Instrument type"),
    data_type: str = Query(..., description="Data type"),
    day: date = Query(..., description="Day (YYYY-MM-DD)"),
    venue: str | None = Query(None, description="Venue (or composite PROTOCOL-CHAIN for DeFi)"),
    underlying: str | None = Query(
        None, description="Underlying root for grouped bundles (BTC, ETH, …)"
    ),
    instrument_id: str | None = Query(None, description="Per-symbol leaf identifier"),
) -> ShardDetailResponse:
    """Unified drill-down endpoint for one shard coordinate.

    Returns schema + GCS metadata + sample rows + payload branched by
    ``shard_class`` (grouped / per_symbol / reference / fixtures) +
    download URLs (1h signed parquet URL + CSV projection).
    """
    try:
        return await run_in_threadpool(
            get_shard_detail,
            service=service,
            category=category,
            instrument_type=instrument_type,
            data_type=data_type,
            day=day.isoformat(),
            venue=venue,
            underlying=underlying,
            instrument_id=instrument_id,
        )
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc
    except (OSError, RuntimeError):
        logger.exception("shard-detail failed for %s/%s/%s", service, category, venue)
        raise HTTPException(
            status_code=500, detail="Internal server error. Check server logs."
        ) from None


@router.get("/venue-detail", response_model=VenueDetailResponse)
async def get_venue_detail_route(
    service: str = Query(..., description="Service name"),
    category: str = Query(..., description="Category (DEFI branches on chain vs composite venue)"),
    venue: str = Query(
        ..., description="Venue — CeFi venue, DeFi chain, or composite PROTOCOL-CHAIN"
    ),
) -> VenueDetailResponse:
    """Venue-scoped drill-down — understands DeFi chain and protocol-chain composites."""
    try:
        return await run_in_threadpool(
            fetch_venue_detail,
            service=service,
            category=category,
            venue=venue,
        )
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc
    except (OSError, RuntimeError):
        logger.exception("venue-detail failed for %s/%s/%s", service, category, venue)
        raise HTTPException(
            status_code=500, detail="Internal server error. Check server logs."
        ) from None

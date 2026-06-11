"""Core data-status routes — status / missing-shards / last-updated / manifest / coverage-summary.

Split from ``routes/data_status.py`` (pure code motion; plan:
``codex_violations_ratchet_to_five_2026_06_10.md`` Phase-1 P2). Routes
register on the package facade's shared ``router``; patched module-level
collaborators (``_cfg`` / service singletons) are resolved through the
facade module (``_ds``) at call time so the existing test patch surface
``deployment_api.routes.data_status.<name>`` keeps intercepting.
"""

import logging
from typing import Literal

from fastapi import HTTPException, Query, Request

import deployment_api.routes.data_status as _ds
from deployment_api.routes.data_status import router
from deployment_api.services.data_status_mock import build_mock_turbo_response

logger = logging.getLogger(__name__)


@router.get("")
async def get_data_status(
    request: Request,
    service: str = Query(..., description="Service name"),
    start_date: str = Query(..., description="Start date (YYYY-MM-DD)"),
    end_date: str = Query(..., description="End date (YYYY-MM-DD)"),
    asset_group: list[str] | None = Query(None, description="Filter by asset group"),
    venue: list[str] | None = Query(None, description="Filter by venue"),
    show_missing: bool = Query(False, description="Include list of missing dates"),
    check_venues: bool = Query(False, description="Check venue coverage inside parquet files"),
    check_data_types: bool = Query(False, description="Check per data_type completion"),
    check_feature_groups: bool = Query(False, description="Check per feature_group completion"),
    check_timeframes: bool = Query(False, description="Check per timeframe completion"),
    force_refresh: bool = Query(False, description="Skip cache and fetch fresh data"),
    mode: str = Query("batch", description="Data path mode: 'batch' or 'live'"),
):
    """
    Get data completion status for a service across a date range.

    Returns completion percentages broken down by asset group and venue,
    with optional list of missing dates.
    """
    if _ds._cfg.is_mock_mode():  # pyright: ignore[reportPrivateUsage]
        sources: list[dict[str, object]] = []
        return {
            "status": "ok",
            "service": service,
            "start_date": start_date,
            "end_date": end_date,
            "sources": sources,
            "mock": True,
        }
    try:
        result = await _ds.data_status_service.run_data_status_cli(
            service=service,
            start_date=start_date,
            end_date=end_date,
            asset_groups=asset_group,
            venues=venue,
            show_missing=show_missing,
            check_venues=check_venues,
            check_data_types=check_data_types,
            check_feature_groups=check_feature_groups,
            check_timeframes=check_timeframes,
            mode=mode,
        )

        if "error" in result:
            raise HTTPException(status_code=500, detail=result["error"])

        return result

    except (OSError, ValueError, RuntimeError) as e:
        logger.exception("Error in get_data_status")
        if isinstance(e, HTTPException):
            raise
        raise HTTPException(status_code=500, detail="Internal server error. Check server logs.") from e


@router.post("/missing-shards")
async def calculate_missing_shards(
    request: Request,
    service: str = Query(..., description="Service name"),
    start_date: str = Query(..., description="Start date (YYYY-MM-DD)"),
    end_date: str = Query(..., description="End date (YYYY-MM-DD)"),
    asset_group: list[str] | None = Query(None, description="Filter by asset group"),
    venue: list[str] | None = Query(None, description="Filter by venue"),
    mode: str = Query("batch", description="Data path mode: 'batch' or 'live'"),
):
    """Calculate missing shards for a service over a date range."""
    try:
        result = await _ds.data_status_service.calculate_missing_shards(
            service=service,
            start_date=start_date,
            end_date=end_date,
            asset_groups=asset_group,
            venues=venue,
            mode=mode,
        )

        if "error" in result:
            raise HTTPException(status_code=500, detail=result["error"])

        return result

    except (OSError, ValueError, RuntimeError) as e:
        logger.exception("Error in calculate_missing_shards")
        if isinstance(e, HTTPException):
            raise
        raise HTTPException(status_code=500, detail="Internal server error. Check server logs.") from e


@router.get("/last-updated")
async def get_last_updated(
    service: str = Query(..., description="Service name"),
    asset_group: list[str] | None = Query(None, description="Filter by asset group"),
):
    """Get last updated information for a service."""
    try:
        result = await _ds.data_status_service.get_last_updated_info(
            service=service,
            asset_groups=asset_group,
        )

        if "error" in result:
            raise HTTPException(status_code=500, detail=result["error"])

        return result

    except (OSError, ValueError, RuntimeError) as e:
        logger.exception("Error in get_last_updated")
        if isinstance(e, HTTPException):
            raise
        raise HTTPException(status_code=500, detail="Internal server error. Check server logs.") from e


@router.get("/manifest")
async def get_data_status_manifest(
    service: str = Query(..., description="Service name"),
    start_date: str = Query(..., description="Start date (YYYY-MM-DD)"),
    end_date: str = Query(..., description="End date (YYYY-MM-DD)"),
    asset_group: list[str] | None = Query(None, description="Filter by asset group"),
    secondary_axis: str | None = Query(
        None,
        description=(
            "Secondary axis the UI wants to slice the cell grid by "
            "(e.g. 'chain', 'league_id', 'job_id'). Echoed back in the "
            "response so the UI knows which slice it got."
        ),
    ),
    league_id: str | None = Query(
        None,
        description="Filter sports manifest rows to one canonical league_id (e.g. 'EPL').",
    ),
    fixture_id: str | None = Query(
        None,
        description="Filter sports manifest rows to one fixture_id (display-axis drill-down).",
    ),
    canonical_question_group: str | None = Query(
        None,
        description=("Filter prediction manifest rows to one canonical_question_group (e.g. 'BTC_UP_DOWN_HOURLY')."),
    ),
    job_id: str | None = Query(
        None,
        description=(
            "Filter ML / strategy / execution manifest rows to one experiment-run "
            "job_id (typically 'RUN_TS-experiment_name')."
        ),
    ),
    chain: str | None = Query(
        None,
        description="Filter DeFi manifest rows to one chain (e.g. 'ETHEREUM', 'ARBITRUM').",
    ),
    cloud: Literal["gcp", "aws"] = Query("gcp", description="Cloud provider for bucket reads"),
):
    """Get data status from manifest availability indices (fastest path).

    Plan: ``data_status_multi_axis_shard_propagation_2026_05_06.md`` Phase 2.
    Optional ``secondary_axis`` + filter params let the UI drill into a single
    league / canonical_question_group / job_id / chain / fixture_id slice.
    """
    if _ds._cfg.is_mock_mode():  # pyright: ignore[reportPrivateUsage]
        # Phase-C honest-coverage: return a realistic v5-shaped payload so
        # the UI's Category Breakdown, 4-state heatmap, "Show only
        # failures" filter, and drill-down retry button all render in
        # local dev. Without this the mock response was ``categories: {}``
        # and none of the new surfaces were reachable via Playwright.
        response = build_mock_turbo_response(
            service=service,
            start_date=start_date,
            end_date=end_date,
            asset_groups=asset_group,
        )
        if secondary_axis:
            response["secondary_axis"] = secondary_axis
        return response
    try:
        result = await _ds.data_status_service.get_manifest_status(
            service=service,
            start_date=start_date,
            end_date=end_date,
            asset_groups=asset_group,
            cloud=cloud,
            secondary_axis=secondary_axis,
            league_id=league_id,
            fixture_id=fixture_id,
            canonical_question_group=canonical_question_group,
            job_id=job_id,
            chain=chain,
        )
        if "error" in result:
            raise HTTPException(status_code=500, detail=result["error"])
        return result
    except (OSError, ValueError, RuntimeError) as e:
        logger.exception("Error in get_data_status_manifest")
        if isinstance(e, HTTPException):
            raise
        raise HTTPException(status_code=500, detail="Internal server error. Check server logs.") from e


@router.get("/coverage-summary")
async def get_coverage_summary(
    service: str = Query("instruments-service", description="Service name"),
    asset_groups: str | None = Query(None, description="Comma-separated asset groups (e.g. CEFI,DEFI)"),
    cloud: Literal["gcp", "aws"] = Query("gcp", description="Cloud provider for bucket reads"),
):
    """Get coverage summary with shard counts and latest-day instrument totals."""
    if _ds._cfg.is_mock_mode():  # pyright: ignore[reportPrivateUsage]
        return {
            "service": service,
            "asset_groups": {},
            "totals": {
                "shards": 0,
                "instrument_rows": 0,
                "dates_across_asset_groups": 0,
                "latest_day_instruments": 0,
            },
            "mock": True,
        }
    try:
        ag_list = asset_groups.split(",") if asset_groups else None
        result = await _ds.data_status_service.get_coverage_summary(
            service=service,
            asset_groups=ag_list,
            cloud=cloud,
        )
        return result
    except (OSError, ValueError, RuntimeError) as e:
        logger.exception("Error in get_coverage_summary")
        raise HTTPException(status_code=500, detail="Internal server error. Check server logs.") from e

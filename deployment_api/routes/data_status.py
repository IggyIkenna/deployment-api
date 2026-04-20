"""
Data Status API Routes - Thin handlers only.

Endpoint for checking data completion status across services.
Business logic delegated to service layer modules.
"""

import logging

from fastapi import APIRouter, HTTPException, Query, Request
from fastapi.responses import Response

from deployment_api.deployment_api_config import DeploymentApiConfig
from deployment_api.services import DataAnalyticsService, DataQueryService, DataStatusService
from deployment_api.services.data_status_drilldown import (
    DEFAULT_INSTRUMENT_LIMIT,
    MAX_INSTRUMENT_LIMIT,
    build_csv_export,
    clear_drilldown_cache,
    compute_bucket_counts,
    get_schema_for_shard,
    get_shard_info,
    list_instruments_for_shard,
    preview_bundle_symbols,
)
from deployment_api.services.data_status_mock import (
    build_mock_shard_instruments,
    build_mock_turbo_response,
)

_cfg = DeploymentApiConfig()

logger = logging.getLogger(__name__)

router = APIRouter()

# Initialize service instances
data_status_service = DataStatusService()
data_query_service = DataQueryService()
data_analytics_service = DataAnalyticsService()


@router.get("")
async def get_data_status(
    request: Request,
    service: str = Query(..., description="Service name"),
    start_date: str = Query(..., description="Start date (YYYY-MM-DD)"),
    end_date: str = Query(..., description="End date (YYYY-MM-DD)"),
    category: list[str] | None = Query(None, description="Filter by category"),
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

    Returns completion percentages broken down by category and venue,
    with optional list of missing dates.
    """
    if _cfg.is_mock_mode():
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
        result = await data_status_service.run_data_status_cli(
            service=service,
            start_date=start_date,
            end_date=end_date,
            categories=category,
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
        raise HTTPException(
            status_code=500, detail="Internal server error. Check server logs."
        ) from e


@router.post("/missing-shards")
async def calculate_missing_shards(
    request: Request,
    service: str = Query(..., description="Service name"),
    start_date: str = Query(..., description="Start date (YYYY-MM-DD)"),
    end_date: str = Query(..., description="End date (YYYY-MM-DD)"),
    category: list[str] | None = Query(None, description="Filter by category"),
    venue: list[str] | None = Query(None, description="Filter by venue"),
    mode: str = Query("batch", description="Data path mode: 'batch' or 'live'"),
):
    """Calculate missing shards for a service over a date range."""
    try:
        result = await data_status_service.calculate_missing_shards(
            service=service,
            start_date=start_date,
            end_date=end_date,
            categories=category,
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
        raise HTTPException(
            status_code=500, detail="Internal server error. Check server logs."
        ) from e


@router.get("/last-updated")
async def get_last_updated(
    service: str = Query(..., description="Service name"),
    category: list[str] | None = Query(None, description="Filter by category"),
):
    """Get last updated information for a service."""
    try:
        result = await data_status_service.get_last_updated_info(
            service=service,
            categories=category,
        )

        if "error" in result:
            raise HTTPException(status_code=500, detail=result["error"])

        return result

    except (OSError, ValueError, RuntimeError) as e:
        logger.exception("Error in get_last_updated")
        if isinstance(e, HTTPException):
            raise
        raise HTTPException(
            status_code=500, detail="Internal server error. Check server logs."
        ) from e


@router.get("/manifest")
async def get_data_status_manifest(
    service: str = Query(..., description="Service name"),
    start_date: str = Query(..., description="Start date (YYYY-MM-DD)"),
    end_date: str = Query(..., description="End date (YYYY-MM-DD)"),
    category: list[str] | None = Query(None, description="Filter by category"),
):
    """Get data status from manifest availability indices (fastest path)."""
    if _cfg.is_mock_mode():
        # Phase-C honest-coverage: return a realistic v5-shaped payload so
        # the UI's Category Breakdown, 4-state heatmap, "Show only
        # failures" filter, and drill-down retry button all render in
        # local dev. Without this the mock response was ``categories: {}``
        # and none of the new surfaces were reachable via Playwright.
        return build_mock_turbo_response(
            service=service,
            start_date=start_date,
            end_date=end_date,
            categories=category,
        )
    try:
        result = await data_status_service.get_manifest_status(
            service=service,
            start_date=start_date,
            end_date=end_date,
            categories=category,
        )
        if "error" in result:
            raise HTTPException(status_code=500, detail=result["error"])
        return result
    except (OSError, ValueError, RuntimeError) as e:
        logger.exception("Error in get_data_status_manifest")
        if isinstance(e, HTTPException):
            raise
        raise HTTPException(
            status_code=500, detail="Internal server error. Check server logs."
        ) from e


@router.get("/coverage-summary")
async def get_coverage_summary(
    service: str = Query("instruments-service", description="Service name"),
    categories: str | None = Query(None, description="Comma-separated categories"),
):
    """Get coverage summary with shard counts and latest-day instrument totals."""
    if _cfg.is_mock_mode():
        return {
            "service": service,
            "categories": {},
            "totals": {
                "shards": 0,
                "instrument_rows": 0,
                "dates_across_categories": 0,
                "latest_day_instruments": 0,
            },
            "mock": True,
        }
    try:
        cat_list = categories.split(",") if categories else None
        result = await data_status_service.get_coverage_summary(
            service=service,
            categories=cat_list,
        )
        return result
    except (OSError, ValueError, RuntimeError) as e:
        logger.exception("Error in get_coverage_summary")
        raise HTTPException(
            status_code=500, detail="Internal server error. Check server logs."
        ) from e


@router.get("/turbo")
async def get_data_status_turbo(
    request: Request,
    service: str = Query(..., description="Service name"),
    start_date: str = Query(..., description="Start date (YYYY-MM-DD)"),
    end_date: str = Query(..., description="End date (YYYY-MM-DD)"),
    category: list[str] | None = Query(None, description="Filter by category"),
    venue: list[str] | None = Query(None, description="Filter by venue"),
    include_sub_dimensions: bool = Query(False, description="Include sub-dimension breakdown"),
    include_instrument_types: bool = Query(False, description="Include instrument type breakdown"),
    include_file_counts: bool = Query(False, description="Include per-date file counts"),
    include_dates_list: bool = Query(False, description="Include sorted list of dates found"),
):
    """Get data status with turbo mode caching (5-minute cache TTL)."""
    if _cfg.is_mock_mode():
        # Same v5-shaped mock as /manifest — keeps the Phase-C UI surfaces
        # (capture_status_counts, failure_rate_by_dimension, attempt vs
        # capture split) identically reachable whether the UI prefers
        # /turbo or /manifest. Ignores venue/include_* filters in mock —
        # the seed data is enough to drive every UI flow.
        _ = (include_sub_dimensions, include_instrument_types, include_file_counts)
        _ = (include_dates_list, venue)
        return build_mock_turbo_response(
            service=service,
            start_date=start_date,
            end_date=end_date,
            categories=category,
        )
    try:
        # Use manifest reader directly (faster, no CLI subprocess,
        # returns league breakdowns for sports venues).
        async def _manifest_source(
            service: str,
            start_date: str,
            end_date: str,
            categories: list[str] | None = None,
            **_kw: object,
        ) -> dict[str, object]:
            return await data_status_service.get_manifest_status(
                service=service,
                start_date=start_date,
                end_date=end_date,
                categories=categories,
            )

        result = await data_analytics_service.get_data_status_turbo(
            service=service,
            start_date=start_date,
            end_date=end_date,
            from_data_status_service=_manifest_source,
            categories=category,
            venues=venue,
        )

        if "error" in result:
            raise HTTPException(status_code=500, detail=result["error"])

        return result

    except (OSError, ValueError, RuntimeError) as e:
        logger.exception("Error in get_data_status_turbo")
        if isinstance(e, HTTPException):
            raise
        raise HTTPException(
            status_code=500, detail="Internal server error. Check server logs."
        ) from e


@router.get("/turbo/stats")
async def get_turbo_cache_stats():
    """Get turbo mode cache statistics."""
    try:
        return await data_analytics_service.get_cache_stats()
    except (OSError, ValueError, RuntimeError) as e:
        logger.exception("Error in get_turbo_cache_stats")
        raise HTTPException(
            status_code=500, detail="Internal server error. Check server logs."
        ) from e


@router.post("/turbo/clear")
async def clear_turbo_cache():
    """Clear the turbo mode cache."""
    try:
        from deployment_api.services.data_status_service import clear_index_cache

        clear_index_cache()
        return await data_analytics_service.clear_cache()
    except (OSError, ValueError, RuntimeError) as e:
        logger.exception("Error in clear_turbo_cache")
        raise HTTPException(
            status_code=500, detail="Internal server error. Check server logs."
        ) from e


@router.get("/venue-filters")
async def get_venue_filters(service: str = Query(..., description="Service name")):
    """Get available venue filters for a service."""
    try:
        result = await data_query_service.get_venue_filters(service)

        if "error" in result:
            raise HTTPException(status_code=500, detail=result["error"])

        return result

    except (OSError, ValueError, RuntimeError) as e:
        logger.exception("Error in get_venue_filters")
        if isinstance(e, HTTPException):
            raise
        raise HTTPException(
            status_code=500, detail="Internal server error. Check server logs."
        ) from e


@router.get("/list-files")
async def list_files_in_path(
    bucket_name: str = Query(..., description="GCS bucket name"),
    path: str = Query("", description="Path within bucket"),
    max_results: int = Query(100, description="Maximum number of results"),
    show_dirs: bool = Query(False, description="Include directory-like prefixes"),
):
    """List files in a specific GCS bucket path."""
    try:
        result = await data_query_service.list_files_in_path(
            bucket_name=bucket_name,
            path=path,
            max_results=max_results,
            show_dirs=show_dirs,
        )

        if "error" in result:
            raise HTTPException(status_code=500, detail=result["error"])

        return result

    except (OSError, PermissionError) as e:
        if isinstance(e, HTTPException):
            raise
        logger.exception("Error in list_files_in_path")
        raise HTTPException(
            status_code=500, detail="Internal server error. Check server logs."
        ) from e


@router.get("/instruments")
async def get_instruments_list(
    category: str = Query(..., description="Category (cefi, tradfi, defi)"),
    venue: str | None = Query(None, description="Filter by venue"),
    instrument_type: str | None = Query(None, description="Filter by instrument type"),
    limit: int = Query(100, description="Maximum number of instruments"),
):
    """Get list of instruments for a category."""
    try:
        result = await data_query_service.get_instruments_list(
            category=category,
            venue=venue,
            instrument_type=instrument_type,
            limit=limit,
        )

        if "error" in result:
            raise HTTPException(status_code=500, detail=result["error"])

        return result

    except (OSError, ValueError, RuntimeError) as e:
        logger.exception("Error in get_instruments_list")
        if isinstance(e, HTTPException):
            raise
        raise HTTPException(
            status_code=500, detail="Internal server error. Check server logs."
        ) from e


@router.get("/instrument-availability")
async def get_instrument_availability(
    venue: str = Query(..., description="Venue name"),
    instrument_type: str = Query(..., description="Instrument type"),
    instrument: str = Query(..., description="Instrument symbol"),
    start_date: str = Query(..., description="Start date (YYYY-MM-DD)"),
    end_date: str = Query(..., description="End date (YYYY-MM-DD)"),
    data_type: str | None = Query(None, description="Specific data type to check"),
    available_from: str | None = Query(None, description="Instrument availability start"),
    available_to: str | None = Query(None, description="Instrument availability end"),
):
    """Check instrument availability over a date range."""
    try:
        result = await data_query_service.get_instrument_availability(
            venue=venue,
            instrument_type=instrument_type,
            instrument=instrument,
            start_date=start_date,
            end_date=end_date,
            data_type=data_type,
            available_from=available_from,
            available_to=available_to,
        )

        if "error" in result:
            raise HTTPException(status_code=500, detail=result["error"])

        return result

    except (OSError, ValueError, RuntimeError) as e:
        logger.exception("Error in get_instrument_availability")
        if isinstance(e, HTTPException):
            raise
        raise HTTPException(
            status_code=500, detail="Internal server error. Check server logs."
        ) from e


@router.post("/analyze")
async def analyze_data_patterns(
    service: str = Query(..., description="Service name"),
    start_date: str = Query(..., description="Start date (YYYY-MM-DD)"),
    end_date: str = Query(..., description="End date (YYYY-MM-DD)"),
    category: list[str] | None = Query(None, description="Filter by category"),
):
    """Analyze data patterns and trends for a service."""
    try:
        # First get the data status
        data_status_result = await data_status_service.run_data_status_cli(
            service=service,
            start_date=start_date,
            end_date=end_date,
            categories=category,
            show_missing=True,
        )

        if "error" in data_status_result:
            raise HTTPException(status_code=500, detail=data_status_result["error"])

        # Then analyze it
        result = await data_analytics_service.analyze_data_patterns(
            service=service,
            data_status_result=data_status_result,
        )

        if "error" in result:
            raise HTTPException(status_code=500, detail=result["error"])

        return result

    except (OSError, ValueError, RuntimeError) as e:
        logger.exception("Error in analyze_data_patterns")
        if isinstance(e, HTTPException):
            raise
        raise HTTPException(
            status_code=500, detail="Internal server error. Check server logs."
        ) from e


@router.post("/multi-service")
async def get_multi_service_status(
    services: list[str] = Query(..., description="List of service names"),
    start_date: str = Query(..., description="Start date (YYYY-MM-DD)"),
    end_date: str = Query(..., description="End date (YYYY-MM-DD)"),
    category: list[str] | None = Query(None, description="Filter by category"),
):
    """Get aggregated data status across multiple services."""
    try:
        result = await data_analytics_service.aggregate_multi_service_status(
            services=services,
            start_date=start_date,
            end_date=end_date,
            from_data_status_service=data_status_service.run_data_status_cli,
            categories=category,
        )

        return result

    except (OSError, ValueError, RuntimeError) as e:
        logger.exception("Error in get_multi_service_status")
        if isinstance(e, HTTPException):
            raise
        raise HTTPException(
            status_code=500, detail="Internal server error. Check server logs."
        ) from e


# ---------------------------------------------------------------------------
# Drill-down endpoints (schema, per-day instruments, CSV download,
# bucket counts for Polymarket-style named/OTHER split).
# ---------------------------------------------------------------------------


@router.get("/schema")
async def get_schema(
    service: str = Query(..., description="Service name (unused, kept for symmetry)"),
    category: str = Query(..., description="Category (cefi/tradfi/defi/sports/prediction)"),
    instrument_type: str = Query(..., description="Instrument type"),
    data_type: str = Query(..., description="Data type"),
    venue: str | None = Query(None, description="Venue for venue-specific overrides"),
):
    """Return the SchemaContract columns for a (category, instrument_type, data_type)
    tuple, honouring venue-specific overrides (UNISWAP_V2/V3/V4 etc.).

    Falls back gracefully when no contract is registered — returns
    ``registered: false`` so the UI can fall back to a raw-column projection.
    """
    try:
        return get_schema_for_shard(
            category=category,
            instrument_type=instrument_type,
            data_type=data_type,
            venue=venue,
        )
    except (ValueError, RuntimeError) as e:
        logger.exception("Error in get_schema")
        raise HTTPException(
            status_code=500, detail="Internal server error. Check server logs."
        ) from e


@router.get("/instruments-for-shard")
async def get_instruments_for_shard(
    service: str = Query(..., description="Service name"),
    category: str = Query(..., description="Category"),
    venue: str = Query(..., description="Venue"),
    day: str = Query(..., description="Day (YYYY-MM-DD)"),
    instrument_type: str = Query(..., description="Instrument type"),
    data_type: str = Query(..., description="Data type"),
    limit: int = Query(
        DEFAULT_INSTRUMENT_LIMIT,
        ge=1,
        le=MAX_INSTRUMENT_LIMIT,
        description="Page size (default 50, max 500)",
    ),
    offset: int = Query(0, ge=0, description="Page offset (default 0)"),
    search: str | None = Query(
        None,
        description=(
            "Case-insensitive substring match on instrument_id. "
            "Ignored for per_underlying bundles. Capped at 100 matches."
        ),
    ),
):
    """List the instrument_ids in a single (day, venue, instrument_type,
    data_type) shard.

    Response shape:
    ``{instruments, total_count, limit, offset, has_more, bundling, search, ...}``.

    ``bundling`` is ``per_symbol``, ``per_underlying`` (options_chain /
    futures_chain / combo), or ``per_condition_id`` (Polymarket OTHER).
    The UI uses this to explain to the user that selecting one root
    downloads the whole bundle parquet for that root.

    For ``per_underlying`` shards search is a no-op — there is one entry
    per underlying and the user is choosing a whole bundle by its root.
    ``total_count`` always reflects the post-search, pre-pagination count
    so the UI can decide whether to show "Load more" / paginator.
    """
    if _cfg.is_mock_mode():
        # Phase-C: return three mock instruments (captured / empty /
        # failed) so the drill-down modal has every capture_status badge
        # on screen and the Retry button on the attempted_failed row is
        # exercised against the mock retryFailedShard round-trip.
        _ = (limit, offset, search)
        return build_mock_shard_instruments(
            service=service,
            category=category,
            venue=venue,
            day=day,
            instrument_type=instrument_type,
            data_type=data_type,
        )
    try:
        return list_instruments_for_shard(
            service=service,
            category=category,
            venue=venue,
            day=day,
            instrument_type=instrument_type,
            data_type=data_type,
            limit=limit,
            offset=offset,
            search=search,
        )
    except (OSError, ValueError, RuntimeError) as e:
        logger.exception("Error in get_instruments_for_shard")
        raise HTTPException(
            status_code=500, detail="Internal server error. Check server logs."
        ) from e


@router.get("/bundle-preview")
async def get_bundle_preview(
    service: str = Query(..., description="Service name"),
    category: str = Query(..., description="Category"),
    venue: str = Query(..., description="Venue"),
    day: str = Query(..., description="Day (YYYY-MM-DD)"),
    instrument_type: str = Query(..., description="Instrument type"),
    data_type: str = Query(..., description="Data type"),
    limit: int = Query(20, ge=1, le=200, description="Max preview symbols"),
):
    """Return the first N symbol-column values inside a per_underlying bundle.

    Meant for the "Preview symbols inside" expander on the Instruments modal
    so the user can eyeball what's in an options_chain / futures_chain /
    combo parquet before downloading it. Returns ``symbols: []`` with an
    explanatory ``message`` for non-bundled shards.
    """
    try:
        return preview_bundle_symbols(
            service=service,
            category=category,
            venue=venue,
            day=day,
            instrument_type=instrument_type,
            data_type=data_type,
            limit=limit,
        )
    except (OSError, ValueError, RuntimeError) as e:
        logger.exception("Error in get_bundle_preview")
        raise HTTPException(
            status_code=500, detail="Internal server error. Check server logs."
        ) from e


@router.get("/bucket-counts")
async def get_bucket_counts(
    service: str = Query(..., description="Service name"),
    category: str = Query(..., description="Category"),
    venue: str = Query(..., description="Venue"),
    day: str | None = Query(
        None,
        description=(
            "Day to sample (YYYY-MM-DD). Optional — defaults to yesterday UTC "
            "so the venue badge can render without the caller having to "
            "resolve a specific day."
        ),
    ),
    data_type: str | None = Query(
        None,
        description=(
            "Data type. Optional — defaults to the first data_type registered "
            "for the shard (INSTRUMENT_DEFINITION for instruments-service, "
            "trades for market-tick-data-service, etc.)."
        ),
    ),
):
    """Return named_market_count + other_market_count for a venue.

    ``named_market_count`` = distinct instrument_types under the venue that
    are not ``OTHER``. ``other_market_count`` = distinct symbol-column
    values inside the OTHER bundle parquet (conditionIds for Polymarket).

    ``day`` and ``data_type`` are optional per the audit checklist Appendix B
    so the badge renders per-venue, not per-day+type. When omitted the server
    resolves sensible defaults (yesterday UTC; service-default data_type).
    """
    try:
        resolved_day = day or _default_bucket_counts_day()
        resolved_data_type = data_type or _default_data_type_for_service(service)
        result = compute_bucket_counts(
            service=service,
            category=category,
            venue=venue,
            day=resolved_day,
            data_type=resolved_data_type,
        )
        return {
            **result,
            "venue": venue,
            "day": resolved_day,
            "data_type": resolved_data_type,
            "count": int(result.get("named_market_count", 0))
            + int(result.get("other_market_count", 0)),
        }
    except (OSError, ValueError, RuntimeError) as e:
        logger.exception("Error in get_bucket_counts")
        raise HTTPException(
            status_code=500, detail="Internal server error. Check server logs."
        ) from e


def _default_bucket_counts_day() -> str:
    """Return yesterday UTC as YYYY-MM-DD (the badge's default sample day)."""
    from datetime import UTC, datetime, timedelta

    return (datetime.now(UTC) - timedelta(days=1)).strftime("%Y-%m-%d")


# Per-service default data_type — used when the caller omits data_type on
# /bucket-counts so the badge can render without forcing the UI to resolve
# a shard-specific type up front.
_DEFAULT_DATA_TYPE_BY_SERVICE: dict[str, str] = {
    "instruments-service": "INSTRUMENT_DEFINITION",
    "market-tick-data-service": "trades",
    "market-tick-data-handler": "trades",
    "market-data-processing-service": "trades",
    "features-onchain-service": "onchain",
    "features-sports-service": "sports",
    "features-delta-one-service": "delta_one",
    "features-volatility-service": "volatility",
    "features-multi-timeframe-service": "multi_timeframe",
    "features-cross-instrument-service": "cross_instrument",
    "features-commodity-service": "commodity",
    "features-calendar-service": "calendar",
    "ml-training-service": "training",
    "ml-inference-service": "inference",
    "strategy-service": "strategy",
    "execution-service": "execution",
}


def _default_data_type_for_service(service: str) -> str:
    """Return the default data_type for a service when the caller omits it."""
    return _DEFAULT_DATA_TYPE_BY_SERVICE.get(service, "trades")


@router.get("/download-csv")
async def download_csv(
    service: str = Query(..., description="Service name"),
    category: str = Query(..., description="Category"),
    venue: str = Query(..., description="Venue"),
    day: str = Query(..., description="Day (YYYY-MM-DD)"),
    instrument_type: str = Query(..., description="Instrument type"),
    data_type: str = Query(..., description="Data type"),
    instrument_ids: str = Query("", description="Comma-separated instrument IDs (empty = all)"),
):
    """Stream a CSV of the selected instruments for one shard.

    Empty ``instrument_ids`` means "download the full shard". The server
    caps output at 500k rows — larger requests get a 413 advising
    BigQuery external tables.
    """
    ids = [s.strip() for s in instrument_ids.split(",") if s.strip()] if instrument_ids else []
    try:
        csv_text, row_count, filename = build_csv_export(
            service=service,
            category=category,
            venue=venue,
            day=day,
            instrument_type=instrument_type,
            data_type=data_type,
            instrument_ids=ids,
        )
    except ValueError as e:
        # Row-cap exceeded.
        raise HTTPException(status_code=413, detail=str(e)) from e
    except (OSError, RuntimeError) as e:
        logger.exception("Error in download_csv")
        raise HTTPException(
            status_code=500, detail="Internal server error. Check server logs."
        ) from e

    headers = {
        "Content-Disposition": f'attachment; filename="{filename}"',
        "X-Row-Count": str(row_count),
    }
    return Response(content=csv_text, media_type="text/csv; charset=utf-8", headers=headers)


@router.get("/shard-info")
async def get_shard_info_endpoint(
    service: str = Query(..., description="Service name"),
    category: str = Query(..., description="Category"),
    venue: str = Query(..., description="Venue"),
    day: str = Query(..., description="Day (YYYY-MM-DD)"),
    data_type: str = Query(..., description="Data type"),
):
    """Return the instrument_types present on a venue+day+data_type shard.

    The UI uses this to resolve the ``instrument_type`` axis before opening
    the Instruments modal — avoids guessing ``data_type`` as the
    instrument_type for venues that actually shard by it (e.g. DERIBIT's
    ``options_chain`` vs ``perpetual``).

    Response:
    ``{instrument_types: [{name, bundling}, ...], recommended_instrument_type}``.
    """
    try:
        return get_shard_info(
            service=service,
            category=category,
            venue=venue,
            day=day,
            data_type=data_type,
        )
    except (OSError, ValueError, RuntimeError) as e:
        logger.exception("Error in get_shard_info_endpoint")
        raise HTTPException(
            status_code=500, detail="Internal server error. Check server logs."
        ) from e


@router.post("/drilldown/clear-cache")
async def clear_drilldown_cache_endpoint():
    """Reset the drill-down TTL cache (schema / instruments / bucket counts)."""
    clear_drilldown_cache()
    return {"status": "ok"}

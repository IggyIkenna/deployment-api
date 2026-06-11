"""Query / instruments / schema / analytics data-status routes.

Split from ``routes/data_status.py`` (pure code motion; plan:
``codex_violations_ratchet_to_five_2026_06_10.md`` Phase-1 P2). Routes
register on the package facade's shared ``router``; patched module-level
collaborators are resolved through the facade module (``_ds``) at call
time so the existing test patch surface keeps intercepting.
"""

import logging
from datetime import UTC, datetime, timedelta

from fastapi import HTTPException, Query

import deployment_api.routes.data_status as _ds
from deployment_api.routes.data_status import router
from deployment_api.services.data_status_drilldown import (
    DEFAULT_INSTRUMENT_LIMIT,
    MAX_INSTRUMENT_LIMIT,
)
from deployment_api.services.data_status_mock import build_mock_shard_instruments

logger = logging.getLogger(__name__)


@router.get("/venue-filters")
async def get_venue_filters(service: str = Query(..., description="Service name")):
    """Get available venue filters for a service."""
    try:
        result = await _ds.data_query_service.get_venue_filters(service)

        if "error" in result:
            raise HTTPException(status_code=500, detail=result["error"])

        return result

    except (OSError, ValueError, RuntimeError) as e:
        logger.exception("Error in get_venue_filters")
        if isinstance(e, HTTPException):
            raise
        raise HTTPException(status_code=500, detail="Internal server error. Check server logs.") from e


@router.get("/list-files")
async def list_files_in_path(
    bucket_name: str = Query(..., description="GCS bucket name"),
    path: str = Query("", description="Path within bucket"),
    max_results: int = Query(100, description="Maximum number of results"),
    show_dirs: bool = Query(False, description="Include directory-like prefixes"),
):
    """List files in a specific GCS bucket path."""
    try:
        result = await _ds.data_query_service.list_files_in_path(
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
        raise HTTPException(status_code=500, detail="Internal server error. Check server logs.") from e


@router.get("/instruments")
async def get_instruments_list(
    asset_group: str = Query(..., description="Asset group (cefi, tradfi, defi)"),
    venue: str | None = Query(None, description="Filter by venue"),
    instrument_type: str | None = Query(None, description="Filter by instrument type"),
    limit: int = Query(100, description="Maximum number of instruments"),
):
    """Get list of instruments for an asset group."""
    try:
        result = await _ds.data_query_service.get_instruments_list(
            asset_group=asset_group,
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
        raise HTTPException(status_code=500, detail="Internal server error. Check server logs.") from e


@router.get("/instruments/search")
async def search_instruments(
    query: str = Query(..., description="Case-insensitive substring; whitespace = AND-match across tokens"),
    asset_group: str | None = Query(
        None,
        description=("Single asset group (cefi/tradfi/defi/sports/prediction). Omit to search all five."),
    ),
    limit: int = Query(50, description="Max matches returned (truncation flag in response)"),
):
    """Canonical-symbol search across one or all categories.

    Returns ``{matches: [{canonical_id, asset_group, venue, instrument_type}, ...]}``
    sorted by canonical_id for deterministic UI rendering. Empty query returns
    an empty match list (we don't dump the entire registry by default).

    Powers the cross-asset-group instruments search field in the deployment-ui
    Data Status panel — institutional-grade equivalent of the SPORTS-only
    league_id search that already exists.
    """
    try:
        return await _ds.data_query_service.search_instruments(
            query=query,
            asset_group=asset_group,
            limit=limit,
        )
    except (OSError, ValueError, RuntimeError) as e:
        logger.exception("Error in search_instruments")
        raise HTTPException(status_code=500, detail="Internal server error. Check server logs.") from e


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
        result = await _ds.data_query_service.get_instrument_availability(
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
        raise HTTPException(status_code=500, detail="Internal server error. Check server logs.") from e


@router.post("/analyze")
async def analyze_data_patterns(
    service: str = Query(..., description="Service name"),
    start_date: str = Query(..., description="Start date (YYYY-MM-DD)"),
    end_date: str = Query(..., description="End date (YYYY-MM-DD)"),
    asset_group: list[str] | None = Query(None, description="Filter by asset group"),
):
    """Analyze data patterns and trends for a service."""
    try:
        # First get the data status
        data_status_result = await _ds.data_status_service.run_data_status_cli(
            service=service,
            start_date=start_date,
            end_date=end_date,
            asset_groups=asset_group,
            show_missing=True,
        )

        if "error" in data_status_result:
            raise HTTPException(status_code=500, detail=data_status_result["error"])

        # Then analyze it
        result = await _ds.data_analytics_service.analyze_data_patterns(
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
        raise HTTPException(status_code=500, detail="Internal server error. Check server logs.") from e


@router.post("/multi-service")
async def get_multi_service_status(
    services: list[str] = Query(..., description="List of service names"),
    start_date: str = Query(..., description="Start date (YYYY-MM-DD)"),
    end_date: str = Query(..., description="End date (YYYY-MM-DD)"),
    asset_group: list[str] | None = Query(None, description="Filter by asset group"),
):
    """Get aggregated data status across multiple services."""
    try:
        result = await _ds.data_analytics_service.aggregate_multi_service_status(
            services=services,
            start_date=start_date,
            end_date=end_date,
            from_data_status_service=_ds.data_status_service.run_data_status_cli,
            asset_groups=asset_group,
        )

        return result

    except (OSError, ValueError, RuntimeError) as e:
        logger.exception("Error in get_multi_service_status")
        if isinstance(e, HTTPException):
            raise
        raise HTTPException(status_code=500, detail="Internal server error. Check server logs.") from e


# ---------------------------------------------------------------------------
# Drill-down endpoints (schema, per-day instruments, CSV download,
# bucket counts for Polymarket-style named/OTHER split).
# ---------------------------------------------------------------------------


@router.get("/schema")
async def get_schema(
    service: str = Query(..., description="Service name — used for parquet projection fallback"),
    asset_group: str = Query(..., description="Asset group (cefi/tradfi/defi/sports/prediction)"),
    instrument_type: str = Query(..., description="Instrument type"),
    data_type: str = Query(..., description="Data type"),
    venue: str | None = Query(None, description="Venue for venue-specific overrides"),
    day: str | None = Query(None, description="Day (YYYY-MM-DD) for parquet projection fallback"),
    chain: str | None = Query(None, description="DeFi chain (ETHEREUM, ARBITRUM, ...)"),
    instrument_id: str | None = Query(None, description="Per-instrument leaf shard"),
    league_id: str | None = Query(None, description="Sports league_id (per-league leaf)"),
    fixture_id: str | None = Query(None, description="Sports fixture_id (per-fixture leaf)"),
    canonical_question_group: str | None = Query(
        None, description="Prediction canonical_question_group (per-group leaf)"
    ),
    job_id: str | None = Query(None, description="ML / strategy / execution experiment-run id"),
    model_family: str | None = Query(None, description="ML model_family"),
    training_period: str | None = Query(None, description="ML training_period"),
    strategy_id: str | None = Query(None, description="strategy-service strategy_id"),
    instruction_type: str | None = Query(None, description="execution-service instruction_type"),
    feature_group: str | None = Query(None, description="features-* feature_group"),
    timeframe: str | None = Query(None, description="features-* / multi-timeframe timeframe"),
):
    """Return the leaf-shard SchemaContract columns for the requested axis tuple.

    Plan: data_status_multi_axis_shard_propagation_2026_05_06.md Phase 3.
    All v7 multi-axis params are optional — populated ones narrow the
    parquet-projection probe to the deepest shard the operator clicked
    on. Each leaf parquet has its own column shape (CeFi spot
    per-instrument vs DeFi options-chain bundle vs sports per-league
    fixture parquet differ); the response is ALWAYS at the deepest level
    the kwargs identify, never aggregated. Resolution order documented
    on ``get_schema_for_shard`` (registry → catalogue synthesis →
    parquet projection → honest absence).
    """
    bucket: str | None = None
    if service:
        try:
            bucket = _ds.build_bucket_name(service, asset_group)
        except ValueError:
            bucket = None
    try:
        return _ds.get_schema_for_shard(
            asset_group=asset_group,
            instrument_type=instrument_type,
            data_type=data_type,
            venue=venue,
            service=service,
            bucket=bucket,
            day=day,
            chain=chain,
            instrument_id=instrument_id,
            league_id=league_id,
            fixture_id=fixture_id,
            canonical_question_group=canonical_question_group,
            job_id=job_id,
            model_family=model_family,
            training_period=training_period,
            strategy_id=strategy_id,
            instruction_type=instruction_type,
            feature_group=feature_group,
            timeframe=timeframe,
        )
    except (ValueError, RuntimeError) as e:
        logger.exception("Error in get_schema")
        raise HTTPException(status_code=500, detail="Internal server error. Check server logs.") from e


@router.get("/instruments-for-shard")
async def get_instruments_for_shard(
    service: str = Query(..., description="Service name"),
    asset_group: str = Query(..., description="Asset group"),
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
    if _ds._cfg.is_mock_mode():  # pyright: ignore[reportPrivateUsage]
        # Phase-C: return three mock instruments (captured / empty /
        # failed) so the drill-down modal has every capture_status badge
        # on screen and the Retry button on the attempted_failed row is
        # exercised against the mock retryFailedShard round-trip.
        _ = (limit, offset, search)
        return build_mock_shard_instruments(
            service=service,
            asset_group=asset_group,
            venue=venue,
            day=day,
            instrument_type=instrument_type,
            data_type=data_type,
        )
    try:
        return _ds.list_instruments_for_shard(
            service=service,
            asset_group=asset_group,
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
        raise HTTPException(status_code=500, detail="Internal server error. Check server logs.") from e


@router.get("/bundle-preview")
async def get_bundle_preview(
    service: str = Query(..., description="Service name"),
    asset_group: str = Query(..., description="Asset group"),
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
        return _ds.preview_bundle_symbols(
            service=service,
            asset_group=asset_group,
            venue=venue,
            day=day,
            instrument_type=instrument_type,
            data_type=data_type,
            limit=limit,
        )
    except (OSError, ValueError, RuntimeError) as e:
        logger.exception("Error in get_bundle_preview")
        raise HTTPException(status_code=500, detail="Internal server error. Check server logs.") from e


@router.get("/bucket-counts")
async def get_bucket_counts(
    service: str = Query(..., description="Service name"),
    asset_group: str = Query(..., description="Asset group"),
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
        result = _ds.compute_bucket_counts(
            service=service,
            asset_group=asset_group,
            venue=venue,
            day=resolved_day,
            data_type=resolved_data_type,
        )
        return {
            **result,
            "venue": venue,
            "day": resolved_day,
            "data_type": resolved_data_type,
            "count": int(result.get("named_market_count", 0)) + int(result.get("other_market_count", 0)),
        }
    except (OSError, ValueError, RuntimeError) as e:
        logger.exception("Error in get_bucket_counts")
        raise HTTPException(status_code=500, detail="Internal server error. Check server logs.") from e


def _default_bucket_counts_day() -> str:
    """Return yesterday UTC as YYYY-MM-DD (the badge's default sample day)."""
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
    # ml-training-service + ml-inference-service consolidated into ml-service (2026-05-21)
    "ml-service": "training",
    "strategy-service": "strategy",
    "execution-service": "execution",
}


def _default_data_type_for_service(service: str) -> str:
    """Return the default data_type for a service when the caller omits it."""
    return _DEFAULT_DATA_TYPE_BY_SERVICE.get(service, "trades")

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
    MTDS_SHARD_SERVICES,
    build_bucket_name,
    build_csv_export,
    build_fixture_breakdown,
    build_fixture_download,
    build_fixtures_csv_export,
    build_instruments_shard_csv_export,
    build_mtds_shard_csv_export,
    build_pool_breakdown,
    clear_drilldown_cache,
    compute_bucket_counts,
    get_schema_for_shard,
    get_shard_info,
    list_instruments_for_shard,
    lookup_capture_status_for_shard,
    preview_bundle_symbols,
)
from deployment_api.services.data_status_hierarchical import (
    get_hierarchical_drilldown,
    list_supported_pairs,
)
from deployment_api.services.data_status_mock import (
    build_mock_shard_instruments,
    build_mock_turbo_response,
)
from deployment_api.services.deploy_missing import (
    DeployMissingError,
    build_deploy_missing_preview,
)
from deployment_api.services.deploy_missing import (
    list_supported_services as deploy_missing_supported_services,
)

_cfg = DeploymentApiConfig()

logger = logging.getLogger(__name__)

router = APIRouter()

# Initialize service instances
data_status_service = DataStatusService()
data_query_service = DataQueryService()
data_analytics_service = DataAnalyticsService()


# ---------------------------------------------------------------------------
# Capture-status download response branching (multi-axis drilldown).
#
# Plan: data_status_multi_axis_shard_propagation_2026_05_06.plan.md Phase 3.
# The download endpoints below check the manifest's capture_status BEFORE
# attempting to read the parquet. The branch dictates HTTP status + body
# shape so the operator can tell:
#
#   captured + parquet OK   → 200 + CSV body                    (today)
#   empty_confirmed         → 200 + header-only CSV explaining the
#                             honest empty (source returned 0 rows).
#                             X-Capture-Status: empty_confirmed.
#   attempted_failed        → 200 + header-only CSV with the error
#                             reason. X-Capture-Status: attempted_failed.
#   captured + 0 byte read  → 502 + body explaining the path drift.
#                             X-Capture-Status: path_drift.
#                             Real bug class — manifest claims captured
#                             but the downloader can't find the parquet.
#   never_attempted         → 404 + body. X-Capture-Status: never_attempted.
# ---------------------------------------------------------------------------


def _empty_confirmed_csv_body(
    *,
    service: str,
    asset_group: str,
    day: str,
    error_reason: str,
    attempted_at: str,
) -> str:
    """Header-only CSV for empty_confirmed shards.

    Operators landing on this from the download button need to know
    immediately that the empty body is HONEST (source returned no
    rows), not a bug. We emit a CSV with one informative comment row
    so opening the file in any tool surfaces the message. The
    X-Capture-Status header carries the machine-readable signal.
    """
    return (
        "# CAPTURE_STATUS: empty_confirmed\n"
        f"# service: {service}\n"
        f"# asset_group: {asset_group}\n"
        f"# day: {day}\n"
        f"# attempted_at: {attempted_at}\n"
        f"# error_reason: {error_reason or '(none — source returned 0 rows)'}\n"
        "# This is an HONEST empty: the adapter ran successfully and the\n"
        "# upstream source returned zero rows for this shard. NaN downstream\n"
        "# is fine — tree-based ML and rank-based allocators handle missing\n"
        "# data natively. The denominator-clip rules in CLAUDE.md (sports\n"
        "# SOURCE_COVERAGE_START / VIX 15m layering / etc.) live here.\n"
    )


def _attempted_failed_csv_body(
    *,
    service: str,
    asset_group: str,
    day: str,
    error_reason: str,
    attempted_at: str,
) -> str:
    """Header-only CSV for attempted_failed shards."""
    return (
        "# CAPTURE_STATUS: attempted_failed\n"
        f"# service: {service}\n"
        f"# asset_group: {asset_group}\n"
        f"# day: {day}\n"
        f"# attempted_at: {attempted_at}\n"
        f"# error_reason: {error_reason or '(unclassified)'}\n"
        "# The adapter ran and raised. error_reason carries the classified\n"
        "# failure category. Re-run the orchestrator for this shard from the\n"
        "# Data Status tab; the manifest will flip back to captured on retry\n"
        "# success.\n"
    )


def _capture_status_response(
    *,
    service: str,
    asset_group: str,
    day: str,
    capture_meta: dict[str, str],
    filename_stem: str,
) -> Response | None:
    """Return a Response when the capture_status branches off the happy path.

    Returns None when status == ``captured`` so the caller falls through
    to its normal parquet-read code path. For every other status,
    returns the appropriate Response with X-Capture-Status header set.

    This is a pure response builder — does NOT touch the parquet.
    """
    status = capture_meta.get("status", "captured")
    error_reason = capture_meta.get("error_reason", "")
    attempted_at = capture_meta.get("attempted_at", "")

    if status == "captured":
        return None

    headers = {"X-Capture-Status": status}

    if status == "never_attempted":
        raise HTTPException(
            status_code=404,
            detail=(
                f"No manifest row for service={service}, asset_group={asset_group}, "
                f"day={day} — the adapter never ran for this shard. Trigger a "
                f"backfill from the Data Status tab."
            ),
            headers=headers,
        )

    if status == "empty_confirmed":
        body = _empty_confirmed_csv_body(
            service=service,
            asset_group=asset_group,
            day=day,
            error_reason=error_reason,
            attempted_at=attempted_at,
        )
        headers["Content-Disposition"] = (
            f'attachment; filename="{filename_stem}.empty_confirmed.csv"'
        )
        headers["X-Row-Count"] = "0"
        return Response(content=body, media_type="text/csv; charset=utf-8", headers=headers)

    if status == "attempted_failed":
        body = _attempted_failed_csv_body(
            service=service,
            asset_group=asset_group,
            day=day,
            error_reason=error_reason,
            attempted_at=attempted_at,
        )
        headers["Content-Disposition"] = (
            f'attachment; filename="{filename_stem}.attempted_failed.csv"'
        )
        headers["X-Row-Count"] = "0"
        return Response(content=body, media_type="text/csv; charset=utf-8", headers=headers)

    # Unknown status — treat as never_attempted but signal the surprise.
    raise HTTPException(
        status_code=500,
        detail=f"Unknown capture_status {status!r} from manifest",
        headers=headers,
    )


def _path_drift_response(
    *,
    service: str,
    asset_group: str,
    day: str,
    expected_path_hint: str,
) -> HTTPException:
    """Build the 502 response for the captured-but-0-rows path-drift case.

    Caller raises this when ``capture_status=captured`` per the manifest
    but the downloader's parquet probe returned 0 rows. Distinct from
    a 404 (manifest had nothing) and from a 200-empty (honest empty);
    the 502 deliberately signals "we believed we had data but the
    downloader can't deliver it" — operator-actionable bug.
    """
    return HTTPException(
        status_code=502,
        detail=(
            f"Manifest says captured for service={service}, "
            f"asset_group={asset_group}, day={day}, but the downloader read 0 "
            f"rows from {expected_path_hint or 'the expected path'}. Likely "
            f"writer/downloader path-template drift, chain-bundle / "
            f"instrument_type / hive-vocab axis drift, or a per-league / "
            f"per-job_id sub-partition the downloader doesn't walk into. Run "
            f"the phantom audit (see codex/02-data/availability-manifest-and-"
            f"data-status.md § Phantom audit — re-runnable recipe)."
        ),
        headers={"X-Capture-Status": "path_drift"},
    )


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
        raise HTTPException(
            status_code=500, detail="Internal server error. Check server logs."
        ) from e


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
        result = await data_status_service.calculate_missing_shards(
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
        raise HTTPException(
            status_code=500, detail="Internal server error. Check server logs."
        ) from e


@router.get("/last-updated")
async def get_last_updated(
    service: str = Query(..., description="Service name"),
    asset_group: list[str] | None = Query(None, description="Filter by asset group"),
):
    """Get last updated information for a service."""
    try:
        result = await data_status_service.get_last_updated_info(
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
        raise HTTPException(
            status_code=500, detail="Internal server error. Check server logs."
        ) from e


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
        description=(
            "Filter prediction manifest rows to one canonical_question_group "
            "(e.g. 'BTC_UP_DOWN_HOURLY')."
        ),
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
):
    """Get data status from manifest availability indices (fastest path).

    Plan: ``data_status_multi_axis_shard_propagation_2026_05_06.plan.md`` Phase 2.
    Optional ``secondary_axis`` + filter params let the UI drill into a single
    league / canonical_question_group / job_id / chain / fixture_id slice.
    """
    if _cfg.is_mock_mode():
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
        result = await data_status_service.get_manifest_status(
            service=service,
            start_date=start_date,
            end_date=end_date,
            asset_groups=asset_group,
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
        raise HTTPException(
            status_code=500, detail="Internal server error. Check server logs."
        ) from e


@router.get("/coverage-summary")
async def get_coverage_summary(
    service: str = Query("instruments-service", description="Service name"),
    asset_groups: str | None = Query(
        None, description="Comma-separated asset groups (e.g. CEFI,DEFI)"
    ),
):
    """Get coverage summary with shard counts and latest-day instrument totals."""
    if _cfg.is_mock_mode():
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
        result = await data_status_service.get_coverage_summary(
            service=service,
            asset_groups=ag_list,
        )
        return result
    except (OSError, ValueError, RuntimeError) as e:
        logger.exception("Error in get_coverage_summary")
        raise HTTPException(
            status_code=500, detail="Internal server error. Check server logs."
        ) from e


@router.get("/drilldown/{service}/{asset_group}")
async def get_data_status_drilldown(
    service: str,
    asset_group: str,
    start_date: str = Query(..., description="ISO YYYY-MM-DD window lower bound"),
    end_date: str = Query(..., description="ISO YYYY-MM-DD window upper bound"),
    chain: str | None = Query(None, description="DEFI chain filter"),
    venue: str | None = Query(None, description="Venue filter"),
    data_type: str | None = Query(None, description="Data type filter"),
    instrument_type: str | None = Query(None, description="Instrument type filter"),
    instrument_id: str | None = Query(None, description="Instrument id filter"),
    league_id: str | None = Query(None, description="Sports league id filter"),
    feature_group: str | None = Query(None, description="Features feature_group filter"),
    timeframe: str | None = Query(None, description="Timeframe filter"),
    canonical_question_group: str | None = Query(
        None, description="Prediction canonical_question_group filter"
    ),
    expand_to_depth: int = Query(2, ge=0, le=10, description="Levels to materialise eagerly"),
    child_offset: int = Query(
        0,
        ge=0,
        description=(
            "Pagination offset into the top-level children list. "
            "Combine with ``child_limit`` to fetch the next page when a "
            "venue has thousands of instruments."
        ),
    ),
    child_limit: int | None = Query(
        None,
        ge=1,
        le=10_000,
        description=(
            "Max top-level children returned. ``None`` = no slice "
            "(return all up to the per-node cap); positive int paginates."
        ),
    ),
):
    """Hierarchical shard-atom drill-down per the codex per-asset_group matrix.

    Plan: ``data_status_drilldown_shard_atom_alignment_2026_05_07`` Phase 1.
    Replaces the flat ``venue -> instrument_type -> day`` panel with a tree
    shaped per ``unified_api_contracts.registry.data_status_axis_matrix``.
    Lazy-load via filter query params: each progressively-deeper request
    returns only the matched subtree.
    """
    if _cfg.is_mock_mode():
        return {
            "service": service,
            "asset_group": asset_group,
            "axes": [],
            "tree": [],
            "totals": {
                "captured": 0,
                "empty_confirmed": 0,
                "attempted_failed": 0,
                "total": 0,
                "completion_pct": 0.0,
            },
            "filtered_by": {},
            "mock": True,
        }
    raw_filters: dict[str, str | None] = {
        "chain": chain,
        "venue": venue,
        "data_type": data_type,
        "instrument_type": instrument_type,
        "instrument_id": instrument_id,
        "league_id": league_id,
        "feature_group": feature_group,
        "timeframe": timeframe,
        "canonical_question_group": canonical_question_group,
    }
    filters: dict[str, str] = {k: v for k, v in raw_filters.items() if v is not None}
    try:
        return get_hierarchical_drilldown(
            service=service,
            asset_group=asset_group,
            window_start=start_date,
            window_end=end_date,
            filters=filters,
            expand_to_depth=expand_to_depth,
            child_offset=child_offset,
            child_limit=child_limit,
        )
    except (OSError, ValueError, RuntimeError) as e:
        logger.exception("drilldown(service=%s, asset_group=%s) failed", service, asset_group)
        raise HTTPException(status_code=500, detail=f"hierarchical drilldown failed: {e!s}") from e


@router.post("/deploy-missing-preview")
async def post_deploy_missing_preview(
    request: dict[str, object],
):
    """Build the surgical-recovery shell command for ONE leaf shard.

    Phase 3 ships preview-mode (operator copies + runs the command from
    their authenticated terminal); auto-launch is a follow-up plan
    pending security review of the deployment-api -> gcloud perm
    boundary. Body shape::

        {
            "service": "market-tick-data-service",
            "asset_group": "defi",
            "row_key": {"venue": "AAVEV3-ARBITRUM",
                        "data_type": "lending_indices",
                        "instrument_id": "USDC",
                        "day": "2024-03-04"}
        }
    """
    service = str(request.get("service", ""))
    asset_group = str(request.get("asset_group", ""))
    raw_row_key = request.get("row_key", {})
    mode = str(request.get("mode", "preview"))
    if not isinstance(raw_row_key, dict):
        raise HTTPException(status_code=400, detail="row_key must be an object")
    row_key: dict[str, str] = {str(k): str(v) for k, v in raw_row_key.items()}
    if not service or not asset_group:
        raise HTTPException(status_code=400, detail="service and asset_group are required")
    try:
        preview = build_deploy_missing_preview(
            service=service,
            asset_group=asset_group,
            row_key=row_key,
            mode=mode,
        )
    except DeployMissingError as e:
        raise HTTPException(status_code=400, detail=str(e)) from e
    return preview.to_dict()


@router.get("/deploy-missing-services")
async def get_deploy_missing_services():
    """List services with a registered launcher script for Deploy-Missing.

    UI consumers call this to decide which leaves render the
    Deploy-Missing button vs a "manual recovery" placeholder.
    """
    return {"services": deploy_missing_supported_services()}


@router.get("/drilldown-pairs")
async def get_drilldown_supported_pairs():
    """List all ``(service, asset_group)`` pairs the drill-down endpoint supports.

    UI consumers introspect this to decide which services render the
    hierarchical drill-down vs the legacy flat panel during the Phase 2
    migration.
    """
    return {"pairs": list_supported_pairs()}


@router.get("/turbo")
async def get_data_status_turbo(
    request: Request,
    service: str = Query(..., description="Service name"),
    start_date: str = Query(..., description="Start date (YYYY-MM-DD)"),
    end_date: str = Query(..., description="End date (YYYY-MM-DD)"),
    asset_group: list[str] | None = Query(None, description="Filter by asset group"),
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
            asset_groups=asset_group,
        )
    try:
        # Use manifest reader directly (faster, no CLI subprocess,
        # returns league breakdowns for sports venues).
        async def _manifest_source(
            service: str,
            start_date: str,
            end_date: str,
            asset_groups: list[str] | None = None,
            **_kw: object,
        ) -> dict[str, object]:
            return await data_status_service.get_manifest_status(
                service=service,
                start_date=start_date,
                end_date=end_date,
                asset_groups=asset_groups,
            )

        result = await data_analytics_service.get_data_status_turbo(
            service=service,
            start_date=start_date,
            end_date=end_date,
            from_data_status_service=_manifest_source,
            asset_groups=asset_group,
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
    """Clear ALL data-status caches.

    Four cache layers exist; UI staleness occurs when any are skipped:
      1. data_analytics_service._turbo_cache — turbo response cache
      2. data_status_service._INDEX_CACHE — manifest parquet cache (5-min TTL)
      3. data_status_drilldown._cache — per-(date, venue, data_type) drilldown cache (5-min TTL)
      4. DataStatusService._REF_DATA_CACHE — upstream-expected-dates cache (5-min TTL, class-level)

    Reference incident 2026-05-05: PREDICTION fanout completed and canonical
    showed 100% by date, but /turbo kept returning stale 89.05% because layers
    3 and 4 were not cleared. clear_drilldown_cache was imported at module top
    but never invoked from this endpoint.
    """
    try:
        from deployment_api.services.data_status_service import (
            clear_index_cache,
            clear_rollup_cache,
        )

        clear_index_cache()
        clear_rollup_cache()
        clear_drilldown_cache()
        DataStatusService._REF_DATA_CACHE.clear()
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
    asset_group: str = Query(..., description="Asset group (cefi, tradfi, defi)"),
    venue: str | None = Query(None, description="Filter by venue"),
    instrument_type: str | None = Query(None, description="Filter by instrument type"),
    limit: int = Query(100, description="Maximum number of instruments"),
):
    """Get list of instruments for an asset group."""
    try:
        result = await data_query_service.get_instruments_list(
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
        raise HTTPException(
            status_code=500, detail="Internal server error. Check server logs."
        ) from e


@router.get("/instruments/search")
async def search_instruments(
    query: str = Query(
        ..., description="Case-insensitive substring; whitespace = AND-match across tokens"
    ),
    asset_group: str | None = Query(
        None,
        description=(
            "Single asset group (cefi/tradfi/defi/sports/prediction). Omit to search all five."
        ),
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
        return await data_query_service.search_instruments(
            query=query,
            asset_group=asset_group,
            limit=limit,
        )
    except (OSError, ValueError, RuntimeError) as e:
        logger.exception("Error in search_instruments")
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
    asset_group: list[str] | None = Query(None, description="Filter by asset group"),
):
    """Analyze data patterns and trends for a service."""
    try:
        # First get the data status
        data_status_result = await data_status_service.run_data_status_cli(
            service=service,
            start_date=start_date,
            end_date=end_date,
            asset_groups=asset_group,
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
    asset_group: list[str] | None = Query(None, description="Filter by asset group"),
):
    """Get aggregated data status across multiple services."""
    try:
        result = await data_analytics_service.aggregate_multi_service_status(
            services=services,
            start_date=start_date,
            end_date=end_date,
            from_data_status_service=data_status_service.run_data_status_cli,
            asset_groups=asset_group,
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

    Plan: data_status_multi_axis_shard_propagation_2026_05_06.plan.md Phase 3.
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
            bucket = build_bucket_name(service, asset_group)
        except ValueError:
            bucket = None
    try:
        return get_schema_for_shard(
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
        raise HTTPException(
            status_code=500, detail="Internal server error. Check server logs."
        ) from e


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
    if _cfg.is_mock_mode():
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
        return list_instruments_for_shard(
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
        raise HTTPException(
            status_code=500, detail="Internal server error. Check server logs."
        ) from e


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
        return preview_bundle_symbols(
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
        raise HTTPException(
            status_code=500, detail="Internal server error. Check server logs."
        ) from e


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
        result = compute_bucket_counts(
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
    asset_group: str = Query(..., description="Asset group"),
    venue: str = Query(..., description="Venue"),
    day: str = Query(..., description="Day (YYYY-MM-DD)"),
    instrument_type: str = Query(..., description="Instrument type"),
    data_type: str = Query(..., description="Data type"),
    instrument_ids: str = Query("", description="Comma-separated instrument IDs (empty = all)"),
    chain: str | None = Query(None, description="DeFi chain leaf-axis filter"),
    league_id: str | None = Query(None, description="Sports league_id leaf-axis filter"),
    job_id: str | None = Query(
        None, description="ML / strategy / execution job_id leaf-axis filter"
    ),
):
    """Stream a CSV of the selected instruments for one shard.

    Reads the manifest's capture_status for the requested axis tuple
    FIRST and branches before touching the parquet — see
    ``_capture_status_response`` for the full empty_confirmed /
    attempted_failed / never_attempted / path_drift behaviour.

    Empty ``instrument_ids`` means "download the full shard". The server
    caps output at 500k rows — larger requests get a 413 advising
    BigQuery external tables.
    """
    capture_meta = lookup_capture_status_for_shard(
        service=service,
        asset_group=asset_group,
        day=day,
        venue=venue,
        instrument_type=instrument_type,
        data_type=data_type,
        chain=chain,
        league_id=league_id,
        job_id=job_id,
    )
    branched = _capture_status_response(
        service=service,
        asset_group=asset_group,
        day=day,
        capture_meta=capture_meta,
        filename_stem=f"{service}_{asset_group}_{venue}_{day}",
    )
    if branched is not None:
        return branched

    ids = [s.strip() for s in instrument_ids.split(",") if s.strip()] if instrument_ids else []
    try:
        csv_text, row_count, filename = build_csv_export(
            service=service,
            asset_group=asset_group,
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

    if row_count == 0:
        # Manifest said captured, parquet read returned 0 rows → path drift.
        raise _path_drift_response(
            service=service,
            asset_group=asset_group,
            day=day,
            expected_path_hint=filename,
        )

    headers = {
        "Content-Disposition": f'attachment; filename="{filename}"',
        "X-Row-Count": str(row_count),
        "X-Capture-Status": "captured",
    }
    return Response(content=csv_text, media_type="text/csv; charset=utf-8", headers=headers)


@router.get("/download-fixtures-csv")
async def download_fixtures_csv(
    day: str = Query(..., description="Day (YYYY-MM-DD)"),
    league_id: str = Query(..., description="Canonical league_id (e.g. EPL)"),
):
    """Stream a CSV of all fixtures for one (day, league) slice.

    Sports FIXTURES are stored as a single per-day parquet at
    ``sports_reference/by_date/day={day}/entity=fixtures/fixtures.parquet``
    keyed by API-Football numeric league id. This endpoint filters by
    canonical ``league_id`` via UAC and streams the matching rows as CSV.

    Reads the manifest capture_status for this (instruments-service,
    sports, day, league_id) FIRST and branches — empty_confirmed leagues
    (paused window per UAC ``KNOWN_COVERAGE_GAPS`` or pre-source-launch
    per ``SOURCE_COVERAGE_START``) return a header-only CSV explaining
    the honest empty rather than a confusing zero-row download.
    """
    capture_meta = lookup_capture_status_for_shard(
        service="instruments-service",
        asset_group="sports",
        day=day,
        data_type="fixtures",
        league_id=league_id,
    )
    branched = _capture_status_response(
        service="instruments-service",
        asset_group="sports",
        day=day,
        capture_meta=capture_meta,
        filename_stem=f"fixtures_{league_id}_{day}",
    )
    if branched is not None:
        return branched

    try:
        csv_text, row_count, filename = build_fixtures_csv_export(
            day=day,
            league_id=league_id,
        )
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e)) from e
    except (OSError, RuntimeError) as e:
        logger.exception("Error in download_fixtures_csv")
        raise HTTPException(
            status_code=500, detail="Internal server error. Check server logs."
        ) from e

    if row_count == 0:
        # Manifest claimed captured for this (day, league_id) but the
        # parquet probe returned 0 rows. Distinct from the honest-empty
        # branch above — file as path drift so the operator notices.
        raise _path_drift_response(
            service="instruments-service",
            asset_group="sports",
            day=day,
            expected_path_hint=filename,
        )

    headers = {
        "Content-Disposition": f'attachment; filename="{filename}"',
        "X-Row-Count": str(row_count),
        "X-Capture-Status": "captured",
    }
    return Response(content=csv_text, media_type="text/csv; charset=utf-8", headers=headers)


@router.get("/download-shard-csv")
async def download_shard_csv(
    service: str = Query(..., description="Service name (e.g. instruments-service)"),
    asset_group: str = Query(..., description="Asset group (CEFI, TRADFI, DEFI)"),
    venue: str = Query(..., description="Venue name (e.g. BINANCE-SPOT)"),
    date: str = Query(..., description="Day (YYYY-MM-DD)"),
    chain: str | None = Query(None, description="DeFi chain leaf-axis filter"),
    league_id: str | None = Query(None, description="Sports league_id leaf-axis filter"),
    job_id: str | None = Query(
        None, description="ML / strategy / execution job_id leaf-axis filter"
    ),
):
    """Stream a CSV for one (service, asset group, venue, date) shard or catalog.

    Reads the manifest's capture_status FIRST and branches before
    touching the parquet — empty_confirmed and attempted_failed return
    200 with header-only bodies explaining the status; never_attempted
    returns 404; captured-but-zero-rows returns 502 (path drift) so the
    operator distinguishes "honest empty" from "downloader broken".
    See ``_capture_status_response`` for the full contract.

    Routing for the captured happy path:
    * instruments-service / corporate-actions — reads the per-(venue, day)
      ``instruments.parquet`` bundle and returns its rows as CSV.
    * market-tick-data-service / market-data-processing-service — reads the
      availability manifest catalog filtered to ``(venue, date)`` and returns
      instrument capture metadata as CSV (tick data itself is too large).
    """
    capture_meta = lookup_capture_status_for_shard(
        service=service,
        asset_group=asset_group,
        day=date,
        venue=venue,
        chain=chain,
        league_id=league_id,
        job_id=job_id,
    )
    branched = _capture_status_response(
        service=service,
        asset_group=asset_group,
        day=date,
        capture_meta=capture_meta,
        filename_stem=f"{service}_{asset_group}_{venue}_{date}",
    )
    if branched is not None:
        return branched

    try:
        svc = service.lower()
        if svc in MTDS_SHARD_SERVICES:
            csv_text, row_count, filename = build_mtds_shard_csv_export(
                service=service,
                asset_group=asset_group,
                venue=venue,
                date=date,
            )
        else:
            csv_text, row_count, filename = build_instruments_shard_csv_export(
                service=service,
                asset_group=asset_group,
                venue=venue,
                date=date,
            )
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e)) from e
    except (OSError, RuntimeError) as e:
        logger.exception("Error in download_shard_csv")
        raise HTTPException(
            status_code=500, detail="Internal server error. Check server logs."
        ) from e

    # Manifest said captured but parquet read returned 0 rows → path drift.
    # Distinct from the honest empty / never_attempted branches handled
    # above — the operator needs to know we believed we had data and
    # couldn't deliver it.
    if row_count == 0 and not csv_text:
        raise _path_drift_response(
            service=service,
            asset_group=asset_group,
            day=date,
            expected_path_hint=filename,
        )

    headers = {
        "Content-Disposition": f'attachment; filename="{filename}"',
        "X-Row-Count": str(row_count),
        "X-Capture-Status": "captured",
    }
    return Response(content=csv_text, media_type="text/csv; charset=utf-8", headers=headers)


@router.get("/fixtures/breakdown")
async def get_fixture_breakdown(
    day: str = Query(..., description="Day (YYYY-MM-DD)"),
    league_id: str = Query(..., description="Canonical league_id (e.g. EPL)"),
):
    """Per-fixture coverage breakdown for one (day, league_id).

    Sports is fixture-anchored — this endpoint exposes the fixture leaf one
    level below the existing per-(league, day) drilldown. For every fixture
    on the day, returns per-entity capture status so the UI can render
    green/amber/red badges per fixture and wire per-fixture download links.

    Response shape documented on ``build_fixture_breakdown``.
    """
    try:
        return build_fixture_breakdown(day=day, league_id=league_id)
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e)) from e
    except (OSError, RuntimeError) as e:
        logger.exception("Error in get_fixture_breakdown")
        raise HTTPException(
            status_code=500, detail="Internal server error. Check server logs."
        ) from e


@router.get("/pools/breakdown")
async def get_pool_breakdown(
    day: str = Query(..., description="Day (YYYY-MM-DD)"),
    venue: str = Query(..., description="DeFi venue (e.g. UNISWAP_V3, AAVE_V3, LIDO)"),
    chain: str = Query(..., description="Chain (e.g. ETHEREUM, ARBITRUM, POLYGON, BASE)"),
):
    """Per-pool coverage breakdown for one (day, venue, chain) DeFi shard.

    Institutional equivalent of ``/fixtures/breakdown`` for chain-native data.
    Walks every ``(instrument_type, data_type)`` partition under the
    ``venue=<VENUE>-<CHAIN>/`` GCS prefix, extracts the canonical pool / asset
    identifier from each parquet, and returns a per-pool coverage map showing
    which data_types captured each pool.

    Response shape documented on ``build_pool_breakdown``.
    """
    try:
        return build_pool_breakdown(day=day, venue=venue, chain=chain)
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e)) from e
    except (OSError, RuntimeError) as e:
        logger.exception("Error in get_pool_breakdown")
        raise HTTPException(
            status_code=500, detail="Internal server error. Check server logs."
        ) from e


@router.get("/fixtures/download")
async def download_fixture_payload(
    fixture_id: str = Query(..., description="Canonical fixture_id"),
    day: str = Query(..., description="Kickoff day (YYYY-MM-DD) — used to locate parquets"),
    format: str = Query("csv", description="'csv' or 'json'"),
):
    """Stream a per-fixture union download across every fixture-scoped entity.

    Reads the 8 per-day fixture-scoped entity parquets, filters each to
    ``fixture_id``, and returns either:

    - **CSV**: denormalised — one leading ``entity`` column + each entity's
      columns concatenated (union-of-columns, missing values blank).
    - **JSON**: ``{fixture_id, day, coverage: {...}, entities: {...}}`` —
      each entity value is either a ``{capture_status, rows}`` dict (when
      captured) or a ``{capture_status}`` sentinel.
    """
    try:
        body_text, row_count, filename, media_type = build_fixture_download(
            fixture_id=fixture_id,
            day=day,
            fmt=format,
        )
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e)) from e
    except FileNotFoundError as e:
        raise HTTPException(status_code=404, detail=str(e)) from e
    except (OSError, RuntimeError) as e:
        logger.exception("Error in download_fixture_payload")
        raise HTTPException(
            status_code=500, detail="Internal server error. Check server logs."
        ) from e

    headers = {
        "Content-Disposition": f'attachment; filename="{filename}"',
        "X-Row-Count": str(row_count),
    }
    return Response(content=body_text, media_type=media_type, headers=headers)


@router.get("/shard-info")
async def get_shard_info_endpoint(
    service: str = Query(..., description="Service name"),
    asset_group: str = Query(..., description="Asset group"),
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
            asset_group=asset_group,
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

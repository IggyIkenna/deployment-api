"""
Data Batch Processing API Routes

Batch processing endpoints for data status checks with optimized querying
and result aggregation.
"""

import logging
import re
from concurrent.futures import ThreadPoolExecutor, as_completed
from typing import cast
from datetime import UTC, datetime

from fastapi import APIRouter, HTTPException, Query, Request

from deployment_api.utils.path_combinatorics import get_path_combinatorics
from deployment_api.utils.storage_facade import (
    list_objects,
)

from .batch_cache_manager import (
    check_cache_for_result,
    store_result_in_cache,
)

# Import our extracted modules
from .batch_config_utils import (
    BUCKET_MAPPING,
    LIVE_PATH_PREFIX,
    SERVICE_CONFIG,
    generate_date_range_and_year_months,
    get_expected_dates_for_venue,
    get_expected_venues_for_category,
    is_venue_expected,
    load_expected_start_dates,
    load_venue_data_types,
)
from .batch_query_engine import (
    get_expected_dates_for_category,
    query_generic_prefixes_for_category,
    query_specific_prefixes_for_category,
)
from .batch_result_processor import (
    build_final_response,
    calculate_overall_file_counts,
    calculate_venue_weighted_totals,
    update_category_completion_percentages,
)

logger = logging.getLogger(__name__)

router = APIRouter()


@router.get("/last-updated/batch")
async def get_last_updated_batch(
    request: Request,
    services: list[str] | None = Query(None, description="List of services (default: all known)"),
):
    """
    Batch endpoint to get last updated timestamps for multiple services at once.

    Much faster than calling /last-updated for each service individually.
    """
    services_to_check = services or list(BUCKET_MAPPING.keys())

    results = {}

    def check_bucket_latest(service: str, cat: str, bucket_name: str) -> dict:
        """Check latest file in a bucket. Uses storage facade (FUSE when production)."""
        try:
            blobs = list_objects(bucket_name, "", max_results=10)

            if not blobs:
                return {"service": service, "category": cat, "latest": None}

            # Find most recent
            latest_blob = max(blobs, key=lambda b: (b.updated or b.time_created) or datetime.min.replace(tzinfo=UTC))
            return {
                "service": service,
                "category": cat,
                "latest": (latest_blob.updated.isoformat() if latest_blob.updated else None),
            }
        except (OSError, ValueError, RuntimeError) as e:
            return {"service": service, "category": cat, "error": str(e)}

    # Build all tasks
    tasks = []
    for svc in services_to_check:
        if svc in BUCKET_MAPPING:
            for cat, bucket in BUCKET_MAPPING[svc].items():
                tasks.append((svc, cat, bucket))

    # Parallel check (max 15 concurrent to avoid overwhelming GCS)
    with ThreadPoolExecutor(max_workers=min(15, len(tasks))) as executor:
        futures = {executor.submit(check_bucket_latest, svc, cat, bucket): (svc, cat) for svc, cat, bucket in tasks}
        for future in as_completed(futures):
            result = future.result()
            svc = result["service"]
            if svc not in results:
                results[svc] = {"categories": {}}
            results[svc]["categories"][result["category"]] = {
                "latest": result.get("latest"),
                "error": result.get("error"),
            }

    return {"services": results}


async def get_data_status_turbo_impl(
    service: str,
    start_date: str,
    end_date: str,
    category: list[str] | None = None,
    venue: list[str] | None = None,
    folder: list[str] | None = None,
    data_type: list[str] | None = None,
    include_sub_dimensions: bool = False,
    include_instrument_types: bool = False,
    include_file_counts: bool = False,
    include_dates_list: bool = False,
    full_dates_list: bool = False,
    check_upstream_availability: bool = True,
    first_day_of_month_only: bool = False,
    freshness_date: str | None = None,
    _upstream_dates: dict[str, dict[str, set]] | None = None,
    mode: str = "batch",
):
    """
    Internal implementation of TURBO data status check.

    This function has normal Python defaults (not Query objects) so it can be
    called directly from other modules (e.g., deployments.py, tests).

    For the FastAPI endpoint with Query parameter docs, see get_data_status_turbo().
    """
    logger = logging.getLogger(__name__)

    if mode not in ("batch", "live"):
        return {"error": f"Invalid mode: {mode}. Use 'batch' or 'live'."}
    path_prefix = LIVE_PATH_PREFIX if mode == "live" else ""

    # Parse freshness_date into datetime for comparison in check_category
    # Supports both YYYY-MM-DD and YYYY-MM-DDTHH:MM:SS formats
    freshness_date_dt = None
    if freshness_date:
        for fmt in ("%Y-%m-%dT%H:%M:%S", "%Y-%m-%dT%H:%M", "%Y-%m-%d"):
            try:
                freshness_date_dt = datetime.strptime(freshness_date, fmt).replace(tzinfo=UTC)
                break
            except ValueError as e:
                logger.debug("Skipping item due to %s: %s", type(e).__name__, e)
                continue
        if freshness_date_dt is None:
            logger.warning("[TURBO] Invalid freshness_date format: %s", freshness_date)
            freshness_date_dt = datetime.strptime(freshness_date[:10], "%Y-%m-%d").replace(tzinfo=UTC)

    # Check cache first
    cached_result = check_cache_for_result(
        service=service,
        start_date=start_date,
        end_date=end_date,
        category=category,
        venue=venue,
        folder=folder,
        data_type=data_type,
        include_sub_dimensions=include_sub_dimensions,
        include_instrument_types=include_instrument_types,
        include_file_counts=include_file_counts,
        check_upstream_availability=check_upstream_availability,
        first_day_of_month_only=first_day_of_month_only,
        freshness_date=freshness_date,
        mode=mode,
        include_dates_list=include_dates_list,
        full_dates_list=full_dates_list,
    )

    if cached_result is not None:
        return cached_result

    # For market-data-processing-service, fetch upstream (market-tick-data-handler) data
    # to determine which dates are actually available as candidates for processing.
    upstream_dates = _upstream_dates  # May be pre-computed
    if service == "market-data-processing-service" and check_upstream_availability and upstream_dates is None:
        logger.info("[TURBO] Fetching upstream market-tick-data-handler data for cascading expected dates")
        # Fetch tick handler data status (use cache if available, but don't recurse)
        tick_result = await get_data_status_turbo_impl(
            service="market-tick-data-handler",
            start_date=start_date,
            end_date=end_date,
            category=category,
            venue=venue,
            include_sub_dimensions=True,  # Need venue breakdown
            include_instrument_types=False,
            include_file_counts=False,
            include_dates_list=True,  # Need actual dates found
            full_dates_list=True,
            check_upstream_availability=False,  # Don't recurse further
            _upstream_dates=None,
            mode=mode,
        )

        # Extract dates per category/venue from tick handler result
        # Structure: {category: {venue: set(dates)}}
        upstream_dates = {}
        for cat_name, cat_data in tick_result.get("categories") or {}.items():
            if not isinstance(cat_data, dict):
                continue
            upstream_dates[cat_name] = {}
            # Category-level dates (for venues without breakdown)
            cat_dates = set(cat_data.get("dates_found_list") or [])
            upstream_dates[cat_name]["__category__"] = cat_dates

            # Venue-level dates
            for venue_name, venue_data in cat_data.get("venues") or {}.items():
                if isinstance(venue_data, dict):
                    venue_dates = set(venue_data.get("dates_found_list") or [])
                    upstream_dates[cat_name][venue_name] = venue_dates

        logger.info(
            "[TURBO] Upstream dates extracted: %s venue-date combinations",
            sum(len(v) for c in upstream_dates.values() for v in c.values()),
        )

    if service not in BUCKET_MAPPING:
        return {"error": f"Service {service} not supported for turbo mode. Supported: {list(BUCKET_MAPPING.keys())}"}

    categories = category if category else list(BUCKET_MAPPING[service].keys())
    config = SERVICE_CONFIG[service]

    # Load expected start dates config for filtering
    expected_start_dates_config = load_expected_start_dates()

    # Load venue data types config for expected vs actual tracking
    venue_data_types_config = load_venue_data_types()

    # Generate ALL dates in range (before category filtering) for year-month prefixes
    all_dates, year_months = generate_date_range_and_year_months(start_date, end_date, first_day_of_month_only)

    # Use PathCombinatorics for highly optimized parallel specific queries
    path_combinatorics = get_path_combinatorics()

    # Guard: prevent 503 timeout for very large requests
    max_estimated_checks = 35_000
    if service == "instruments-service" and include_sub_dimensions:
        end_dt = datetime.strptime(end_date, "%Y-%m-%d").replace(tzinfo=UTC)
        start_dt = datetime.strptime(start_date, "%Y-%m-%d").replace(tzinfo=UTC)
        total_venues = sum(len(path_combinatorics.get_all_venues_for_category(cat)) for cat in categories)
        days = (end_dt - start_dt).days + 1
        estimated = days * total_venues
        if estimated > max_estimated_checks:
            raise HTTPException(
                status_code=400,
                detail=(
                    f"Request too large: {days} days x {total_venues} venues x {len(categories)} categories "
                    f"= ~{estimated:,} GCS checks (limit {max_estimated_checks:,}). "
                    "Narrow the date range (e.g. last 6-12 months), select specific categories, or add venue filter."
                ),
            )

    _date_re = re.compile(cast(str, config["date_pattern"]))
    _sub_re = re.compile(cast(str, config["sub_pattern"])) if "sub_pattern" in config else None
    sub_dimension_name = cast(str | None, config.get("sub_dimension"))
    results = {}

    # Determine if we should use flat listing (much faster for large date ranges)
    _use_flat_listing = len(year_months) > 6  # More than 6 months = use flat listing

    def check_category(cat: str) -> dict:
        """Check all date directories for a category using optimized queries."""
        bucket_name = BUCKET_MAPPING[service].get(cat)
        if not bucket_name:
            return {"category": cat, "error": f"No bucket for category {cat}"}

        # Get category-specific expected dates (respects category_start from config)
        expected_dates_for_cat = get_expected_dates_for_category(all_dates, expected_start_dates_config, service, cat)

        try:
            # Choose query strategy based on service and filters
            if service in ["market-tick-data-handler", "market-data-processing-service"] and path_combinatorics:
                # Use specific PathCombinatorics queries for market data services
                query_result = query_specific_prefixes_for_category(
                    service=service,
                    cat=cat,
                    dates_to_check=expected_dates_for_cat,
                    venue=venue,
                    folder=folder,
                    data_type=data_type,
                    path_prefix=path_prefix,
                    expected_start_dates_config=expected_start_dates_config,
                    all_dates=all_dates,
                    upstream_avail_dates=upstream_dates,
                )
            else:
                # Use generic combinatorics queries for other services
                query_result = query_generic_prefixes_for_category(
                    service=service,
                    cat=cat,
                    dates_to_check=expected_dates_for_cat,
                    venue=venue,
                    path_prefix=path_prefix,
                )

            if "error" in query_result:
                return query_result

            found_dates = query_result["found_dates"]
            venue_data = query_result["venue_data"]
            sub_dimension_data = query_result["sub_dimension_data"]
            query_result["inst_type_data"]
            query_result["venue_data_types"]
            query_result["venue_folders"]
            query_result.get("timeframe_data") or {}
            query_result.get("venue_timeframes") or {}
            query_result.get("venue_date_blob_timestamps") or {}

            # Build result dictionary with found data
            result: dict[str, object] = {
                "category": cat,
                "bucket": bucket_name,
                "dates_found": len(found_dates),
                "dates_expected": len(expected_dates_for_cat),
                "completion_pct": round(len(found_dates) / len(expected_dates_for_cat) * 100, 1)
                if expected_dates_for_cat
                else 0,
            }

            # Add dates list if requested
            if include_dates_list:
                dates_found_list = sorted(found_dates)
                dates_expected_list = sorted(expected_dates_for_cat)
                dates_missing_list = sorted(expected_dates_for_cat - found_dates)

                result.update(
                    {
                        "dates_found_list": dates_found_list,
                        "dates_expected_list": dates_expected_list,
                        "dates_missing_list": dates_missing_list,
                    }
                )

            # Add sub-dimension data if requested
            if include_sub_dimensions and sub_dimension_data:
                result[sub_dimension_name or "sub_dimension"] = {
                    name: {
                        "dates_found": len(dates),
                        "dates_found_list": sorted(dates) if include_dates_list else None,
                    }
                    for name, dates in sub_dimension_data.items()
                }

            # Add venue data if available
            if venue_data:
                venues_dict = {}
                for venue_name, venue_dates in venue_data.items():
                    # Get expected dates for this venue
                    venue_expected_dates = get_expected_dates_for_venue(
                        all_dates,
                        expected_start_dates_config,
                        service,
                        cat,
                        venue_name,
                        upstream_avail_dates=upstream_dates,
                    )

                    venue_dict: dict[str, object] = {
                        "dates_found": len(venue_dates),
                        "dates_expected": len(venue_expected_dates),
                        "dates_expected_venue": len(venue_expected_dates),
                        "is_expected": is_venue_expected(venue_data_types_config, cat, venue_name),
                    }

                    if venue_expected_dates:
                        venue_dict["completion_pct"] = round(len(venue_dates) / len(venue_expected_dates) * 100, 1)
                    else:
                        venue_dict["completion_pct"] = 0

                    if include_dates_list:
                        venue_dict["dates_found_list"] = sorted(venue_dates)
                        venue_dict["dates_missing_list"] = sorted(venue_expected_dates - venue_dates)

                    venues_dict[venue_name] = venue_dict

                result["venues"] = venues_dict

                # Add venue summary
                expected_venues = get_expected_venues_for_category(venue_data_types_config, cat)
                actual_venues = set(venue_data.keys())
                result["venue_summary"] = {
                    "expected": sorted(expected_venues),
                    "found": sorted(actual_venues),
                    "missing": sorted(expected_venues - actual_venues),
                    "unexpected": sorted(actual_venues - expected_venues),
                    "expected_but_missing": sorted(expected_venues - actual_venues),
                }

            return result

        except (OSError, ValueError, RuntimeError) as e:
            logger.error("Error checking %s: %s", bucket_name, e)
            return {"category": cat, "bucket": bucket_name, "error": str(e)}

    # Parallel check across categories
    with ThreadPoolExecutor(max_workers=len(categories)) as executor:
        futures = {executor.submit(check_category, cat): cat for cat in categories}
        for future in as_completed(futures):
            result = future.result()
            results[result["category"]] = result

    # Calculate overall totals using extracted functions
    total_venue_expected, total_venue_found, expected_missing, unexpected_missing = calculate_venue_weighted_totals(
        results, all_dates, expected_start_dates_config, service, upstream_dates
    )

    # Update category completion percentages to be venue-weighted
    update_category_completion_percentages(results, all_dates, expected_start_dates_config, service, upstream_dates)

    # Calculate category-level totals for reference
    total_expected_category = sum(r.get("dates_expected", 0) for r in results.values() if "error" not in r)
    total_found_category = sum(r.get("dates_found", 0) for r in results.values() if "error" not in r)

    # Calculate overall file counts if requested
    overall_file_counts = calculate_overall_file_counts(results, include_file_counts)

    # Build final response
    response = build_final_response(
        service=service,
        start_date=start_date,
        end_date=end_date,
        first_day_of_month_only=first_day_of_month_only,
        sub_dimension_name=sub_dimension_name,
        include_sub_dimensions=include_sub_dimensions,
        include_file_counts=include_file_counts,
        all_dates=all_dates,
        total_venue_expected=total_venue_expected,
        total_venue_found=total_venue_found,
        total_expected_category=total_expected_category,
        total_found_category=total_found_category,
        expected_missing=expected_missing,
        unexpected_missing=unexpected_missing,
        results=results,
        overall_file_counts=overall_file_counts,
    )

    # Store in cache and return (potentially truncated) response
    response = store_result_in_cache(
        response=response,
        service=service,
        start_date=start_date,
        end_date=end_date,
        category=category,
        venue=venue,
        folder=folder,
        data_type=data_type,
        include_sub_dimensions=include_sub_dimensions,
        include_instrument_types=include_instrument_types,
        include_file_counts=include_file_counts,
        check_upstream_availability=check_upstream_availability,
        first_day_of_month_only=first_day_of_month_only,
        freshness_date=freshness_date,
        mode=mode,
        include_dates_list=include_dates_list,
        full_dates_list=full_dates_list,
    )

    return response

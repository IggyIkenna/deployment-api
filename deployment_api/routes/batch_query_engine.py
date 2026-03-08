"""
Query engine functions for batch data processing.

Contains specialized and generic query engines for retrieving data
from cloud storage with optimized parallel processing.
"""

import logging
from concurrent.futures import ThreadPoolExecutor
from datetime import UTC, datetime

from deployment_api.utils.path_combinatorics import get_path_combinatorics
from deployment_api.utils.storage_facade import list_objects

from .batch_config_utils import (
    BUCKET_MAPPING,
    get_category_start_date,
)

logger = logging.getLogger(__name__)


def query_specific_prefixes_for_category(  # noqa: C901
    service: str,
    cat: str,
    dates_to_check: set,
    venue: list[str] | None,
    folder: list[str] | None,
    data_type: list[str] | None,
    path_prefix: str,
    expected_start_dates_config: dict,
    all_dates: set,
    upstream_avail_dates: dict[str, dict[str, set]] | None = None,
) -> dict:
    """
    Use PathCombinatorics to query specific GCS prefixes in parallel.

    This is MUCH faster than hierarchical scanning when we know the exact
    combinatorics of (data_type, folder, venue) that are valid.

    Returns:
        Dict with found_dates, venue_data, sub_dimension_data, etc.
    """
    bucket_name = BUCKET_MAPPING[service].get(cat)
    if not bucket_name:
        return {"error": f"No bucket for category {cat}"}

    # Get all valid combinatorics for this category (and venue/folder/data_type filters if specified)  # noqa: E501
    # Pass service for timeframe expansion (market-data-processing-service)
    path_combinatorics = get_path_combinatorics()
    combos = path_combinatorics.get_combinatorics(
        category=cat,
        venues=venue,  # Apply venue filter if specified
        folders=folder,  # Apply folder/instrument type filter if specified
        data_types=data_type,  # Apply data type filter if specified
        service=service,  # For timeframe expansion
    )

    if not combos:
        logger.warning("[TURBO] No valid combinatorics for %s with venue=%s", cat, venue)
        return {
            "found_dates": set(),
            "venue_data": {},
            "sub_dimension_data": {},  # data_type -> dates
            "inst_type_data": {},
            "venue_data_types": {},
            "venue_folders": {},
            "timeframe_data": {},  # For market-data-processing-service
        }

    # Build all specific prefixes to query
    # Group by date for efficient parallel execution
    prefixes_by_date = {}  # date -> list of prefixes
    for date_str in dates_to_check:
        prefixes = []
        # Check if this date is inside a tick data window
        in_tick_window = path_combinatorics.is_in_tick_window(date_str)
        for combo in combos:
            # Filter out combos that started after this date
            if combo.start_date:
                try:
                    start_dt = datetime.strptime(combo.start_date, "%Y-%m-%d").replace(tzinfo=UTC)
                    if datetime.strptime(date_str, "%Y-%m-%d").replace(tzinfo=UTC) < start_dt:
                        continue
                except ValueError as e:
                    logger.debug("Suppressed %s during operation: %s", type(e).__name__, e)
                    pass
            # Skip tick_window_only data types outside tick windows
            if combo.tick_window_only and not in_tick_window:
                continue
            base_prefix = path_combinatorics._get_base_prefix(service)
            prefix = combo.to_gcs_prefix(date_str, base_prefix)
            prefixes.append((prefix, combo))
        if prefixes:
            prefixes_by_date[date_str] = prefixes

    # Execute parallel queries for all prefixes
    # FLAT tracking
    found_dates = set()
    venue_data = {}  # venue -> set of dates
    sub_dimension_data = {}  # data_type -> set of dates
    inst_type_data = {}  # folder -> set of dates
    timeframe_data = {}  # timeframe -> set of dates (for market-data-processing-service)

    # NESTED tracking for full dimensional breakdown
    venue_data_types = {}  # venue -> {data_type -> set of dates}
    venue_folders = {}  # venue -> {folder -> set of dates}
    venue_timeframes = {}  # venue -> {timeframe -> set of dates} (for market-data-processing-service)  # noqa: E501

    # Blob timestamp tracking for verification classification
    # Maps venue -> {date_str -> oldest blob.updated datetime}
    # Uses min so if ANY file under a prefix is stale, the group is stale
    venue_date_blob_timestamps = {}  # venue -> {date -> blob.updated}

    def check_prefix(prefix: str, combo) -> tuple:
        """Check if prefix has any data, return (has_data, combo, oldest_blob_updated).

        Lists up to 50 blobs under the prefix (FUSE when production).
        Using min ensures that if ANY file under the prefix is stale,
        the whole group is treated as stale for freshness comparison.
        """
        try:
            effective_prefix = (path_prefix + prefix) if path_prefix else prefix
            blobs = list_objects(bucket_name, effective_prefix, max_results=50)
            if blobs:
                oldest = min(
                    (b.updated for b in blobs if b.updated is not None),
                    default=None,
                )
                return (True, combo, oldest)
            return (False, combo, None)
        except (OSError, ValueError, RuntimeError) as e:
            logger.debug("Prefix check failed for %s: %s", prefix, e)
            return (False, combo, None)

    # Process each date in parallel
    total_queries = sum(len(p) for p in prefixes_by_date.values())
    logger.info(
        "[TURBO] Querying %s specific prefixes for %s (%s dates, %s combos per date)",
        total_queries,
        cat,
        len(prefixes_by_date),
        len(combos),
    )

    # Use ThreadPoolExecutor for parallel GCS queries
    with ThreadPoolExecutor(max_workers=100) as executor:
        all_futures = []
        for date_str, prefix_combos in prefixes_by_date.items():
            for prefix, combo in prefix_combos:
                future = executor.submit(check_prefix, prefix, combo)
                all_futures.append((future, date_str))

        for future, date_str in all_futures:
            try:
                has_data, combo, blob_updated = future.result(timeout=30)
                if has_data:
                    found_dates.add(date_str)

                    # Track by venue
                    if combo.venue not in venue_data:
                        venue_data[combo.venue] = set()
                    venue_data[combo.venue].add(date_str)

                    # Track by data_type (sub_dimension)
                    if combo.data_type not in sub_dimension_data:
                        sub_dimension_data[combo.data_type] = set()
                    sub_dimension_data[combo.data_type].add(date_str)

                    # Track by folder (instrument_type)
                    if combo.folder not in inst_type_data:
                        inst_type_data[combo.folder] = set()
                    inst_type_data[combo.folder].add(date_str)

                    # NESTED: venue -> data_type -> dates
                    if combo.venue not in venue_data_types:
                        venue_data_types[combo.venue] = {}
                    if combo.data_type not in venue_data_types[combo.venue]:
                        venue_data_types[combo.venue][combo.data_type] = set()
                    venue_data_types[combo.venue][combo.data_type].add(date_str)

                    # NESTED: venue -> folder -> dates
                    if combo.venue not in venue_folders:
                        venue_folders[combo.venue] = {}
                    if combo.folder not in venue_folders[combo.venue]:
                        venue_folders[combo.venue][combo.folder] = set()
                    venue_folders[combo.venue][combo.folder].add(date_str)

                    # Timeframe tracking (for market-data-processing-service)
                    if combo.timeframe:
                        # Flat: timeframe -> dates
                        if combo.timeframe not in timeframe_data:
                            timeframe_data[combo.timeframe] = set()
                        timeframe_data[combo.timeframe].add(date_str)

                        # NESTED: venue -> timeframe -> dates
                        if combo.venue not in venue_timeframes:
                            venue_timeframes[combo.venue] = {}
                        if combo.timeframe not in venue_timeframes[combo.venue]:
                            venue_timeframes[combo.venue][combo.timeframe] = set()
                        venue_timeframes[combo.venue][combo.timeframe].add(date_str)

                    # Track blob timestamps for verification (use min = oldest)
                    # If ANY file under this venue+date is stale, treat group as stale
                    if blob_updated is not None:
                        if combo.venue not in venue_date_blob_timestamps:
                            venue_date_blob_timestamps[combo.venue] = {}
                        existing_ts = venue_date_blob_timestamps[combo.venue].get(date_str)
                        if existing_ts is None or blob_updated < existing_ts:
                            venue_date_blob_timestamps[combo.venue][date_str] = blob_updated
            except (OSError, ValueError, RuntimeError) as e:
                logger.debug("Query failed: %s", e)

    logger.info(
        "[TURBO] Found data in %s dates, %s venues, %s data_types, %s folders, %s timeframes",
        len(found_dates),
        len(venue_data),
        len(sub_dimension_data),
        len(inst_type_data),
        len(timeframe_data),
    )

    return {
        "found_dates": found_dates,
        "venue_data": venue_data,
        "sub_dimension_data": sub_dimension_data,
        "inst_type_data": inst_type_data,
        # NESTED for per-venue breakdown
        "venue_data_types": venue_data_types,  # venue -> {data_type -> dates}
        "venue_folders": venue_folders,  # venue -> {folder -> dates}
        # Timeframe tracking (for market-data-processing-service)
        "timeframe_data": timeframe_data,  # timeframe -> dates
        "venue_timeframes": venue_timeframes,  # venue -> {timeframe -> dates}
        # Blob timestamps for verification classification
        "venue_date_blob_timestamps": venue_date_blob_timestamps,
    }


def query_generic_prefixes_for_category(  # noqa: C901
    service: str,
    cat: str,
    dates_to_check: set,
    venue: list[str] | None,
    path_prefix: str,
) -> dict:
    """
    Use generic service combinatorics to query GCS prefixes in parallel.

    This is the generalized version of query_specific_prefixes_for_category,
    supporting ALL services by loading dimensions from their sharding configs:
      - instruments-service: category x venue x date
      - features-delta-one-service: category x feature_group x date
      - features-onchain-service: category x feature_group x date
      - features-volatility-service: category x feature_group x date
      - features-calendar-service: feature_type x date
      - corporate-actions: category x date

    Instead of hierarchical directory scanning, this queries exact known paths
    with list_blobs(prefix=..., max_results=1) for O(1) existence checks.

    Returns:
        Dict with found_dates, venue_data, sub_dimension_data, etc.
    """
    bucket_name = BUCKET_MAPPING[service].get(cat)
    if not bucket_name:
        return {"error": f"No bucket for category {cat}"}

    path_combinatorics = get_path_combinatorics()

    # Build all prefixes to check
    all_entries = []  # (date_str, prefix, sub_dim_value)
    for date_str in dates_to_check:
        entries = path_combinatorics.get_service_prefixes_for_date(
            service=service,
            category=cat,
            date_str=date_str,
            venue_filter=venue,  # Apply venue filter for instruments-service
        )
        for prefix, sub_dim_value in entries:
            all_entries.append((date_str, prefix, sub_dim_value))

    if not all_entries:
        logger.warning("[TURBO] No combinatoric prefixes for %s/%s", cat, service)
        return {
            "found_dates": set(),
            "venue_data": {},
            "sub_dimension_data": {},
            "inst_type_data": {},
            "venue_data_types": {},
            "venue_folders": {},
            "timeframe_data": {},
            "venue_timeframes": {},
            "venue_date_blob_timestamps": {},
        }

    # Track results
    found_dates = set()
    sub_dimension_data = {}  # sub_dim_value -> set of dates
    venue_data = {}  # venue -> set of dates (for instruments-service)
    venue_date_blob_timestamps = {}  # venue -> {date -> blob.updated}

    def check_prefix_generic(prefix: str) -> tuple:
        """Quick existence check for a prefix, returns (has_data, oldest_blob_updated).

        Lists up to 50 blobs (FUSE when production) and returns min blob.updated.
        Using min ensures that if ANY file under the prefix is stale, the whole group is stale.
        """
        try:
            effective_prefix = (path_prefix + prefix) if path_prefix else prefix
            blobs = list_objects(bucket_name, effective_prefix, max_results=50)
            if blobs:
                oldest = min(
                    (b.updated for b in blobs if b.updated is not None),
                    default=None,
                )
                return (True, oldest)
            return (False, None)
        except (OSError, ValueError, RuntimeError) as e:
            logger.debug("Prefix check failed for %s: %s", prefix, e)
            return (False, None)

    logger.info(
        "[TURBO] Querying %s generic combinatoric prefixes for %s/%s (%s dates)",
        len(all_entries),
        cat,
        service,
        len(dates_to_check),
    )

    # Execute all checks in parallel
    with ThreadPoolExecutor(max_workers=100) as executor:
        futures = []
        for date_str, prefix, sub_dim_value in all_entries:
            future = executor.submit(check_prefix_generic, prefix)
            futures.append((future, date_str, sub_dim_value))

        for future, date_str, sub_dim_value in futures:
            try:
                has_data, blob_updated = future.result(timeout=30)
                if has_data:
                    found_dates.add(date_str)

                    # Track by sub-dimension (feature_group, venue, feature_type)
                    if sub_dim_value:
                        if sub_dim_value not in sub_dimension_data:
                            sub_dimension_data[sub_dim_value] = set()
                        sub_dimension_data[sub_dim_value].add(date_str)

                    # For instruments-service, sub_dim IS the venue
                    if service == "instruments-service" and sub_dim_value:
                        if sub_dim_value not in venue_data:
                            venue_data[sub_dim_value] = set()
                        venue_data[sub_dim_value].add(date_str)

                    # Track blob timestamps for verification (use min = oldest)
                    # If ANY file under this group is stale, treat group as stale
                    venue_key = sub_dim_value or "_all"
                    if blob_updated is not None:
                        if venue_key not in venue_date_blob_timestamps:
                            venue_date_blob_timestamps[venue_key] = {}
                        existing_ts = venue_date_blob_timestamps[venue_key].get(date_str)
                        if existing_ts is None or blob_updated < existing_ts:
                            venue_date_blob_timestamps[venue_key][date_str] = blob_updated
            except (OSError, ValueError, RuntimeError) as e:
                logger.debug("Generic prefix query failed: %s", e)

    logger.info(
        "[TURBO] Generic combinatorics found data in %s dates, %s sub-dimensions for %s/%s",
        len(found_dates),
        len(sub_dimension_data),
        cat,
        service,
    )

    return {
        "found_dates": found_dates,
        "venue_data": venue_data,
        "sub_dimension_data": sub_dimension_data,
        "inst_type_data": {},  # Not applicable for generic services
        "venue_data_types": {},
        "venue_folders": {},
        "timeframe_data": {},
        "venue_timeframes": {},
        "venue_date_blob_timestamps": venue_date_blob_timestamps,
    }


def get_expected_dates_for_category(
    all_dates: set,
    expected_start_dates_config: dict,
    service: str,
    cat: str,
) -> set:
    """Get expected dates for a category, respecting category_start from config."""
    cat_start = get_category_start_date(expected_start_dates_config, service, cat)
    if not cat_start:
        return all_dates  # No start date configured, use all dates
    # Filter to dates >= category_start
    return {d for d in all_dates if d >= cat_start}

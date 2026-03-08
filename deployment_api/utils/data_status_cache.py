"""
In-memory cache for data status results.

Caches the full (non-truncated) data status results with a TTL (default 1800s = 30 minutes).
This allows deploy_missing_only to reuse results from a recent data status check
instead of re-scanning GCS (~90s).

Design decisions:
- Cache stores FULL results (full_dates_list=True) always
- When truncated results requested, we truncate from cache
- 3-minute TTL balances freshness vs performance (deployments take 3-4+ minutes anyway)
- In-memory only - no cross-machine sharing, clears on restart
- Thread-safe with locks
- Cache key includes: service, start_date, end_date, category, include_sub_dimensions, etc.
  So different date ranges = different cache entries = fresh scans
"""

import copy
import hashlib
import json
import logging
import threading
from datetime import UTC, datetime
from typing import cast

from deployment_api.settings import DATA_STATUS_CACHE_TTL_SECONDS

logger = logging.getLogger(__name__)

# Cache TTL in seconds (default 1800s = 30 minutes)
# Data rarely changes, and expensive queries should be cached longer
# Use Clear Cache button or DATA_STATUS_CACHE_TTL_SECONDS env var to adjust
CACHE_TTL_SECONDS = DATA_STATUS_CACHE_TTL_SECONDS

# Cache storage: key -> (result, timestamp)
_cache: dict[str, tuple[dict[str, object], datetime]] = {}
_cache_lock = threading.Lock()


def _make_cache_key(
    service: str,
    start_date: str,
    end_date: str,
    category: list[str] | None,
    venue: list[str] | None = None,
    folder: list[str] | None = None,
    data_type: list[str] | None = None,
    include_sub_dimensions: bool = False,
    include_instrument_types: bool = False,
    include_file_counts: bool = False,
    check_upstream_availability: bool = False,
    first_day_of_month_only: bool = False,
    freshness_date: str | None = None,
    mode: str = "batch",
) -> str:
    """
    Generate a cache key from query parameters.

    Note: include_dates_list and full_dates_list are NOT part of the key
    because we always store full results and truncate on read.

    Note: check_upstream_availability IS part of the key because it fundamentally
    changes the expected dates calculation for market-data-processing-service.

    Note: venue, folder, data_type ARE part of the key because filtering changes results.

    Note: first_day_of_month_only IS part of the key because it filters dates.

    Note: freshness_date IS part of the key because it changes what counts as "found".

    Note: mode (batch | live) IS part of the key because live uses different GCS paths.
    """
    key_data = {
        "service": service,
        "start_date": start_date,
        "end_date": end_date,
        "category": sorted(category) if category else None,
        "venue": sorted(venue) if venue else None,
        "folder": sorted(folder) if folder else None,
        "data_type": sorted(data_type) if data_type else None,
        "include_sub_dimensions": include_sub_dimensions,
        "include_instrument_types": include_instrument_types,
        "include_file_counts": include_file_counts,
        "check_upstream_availability": check_upstream_availability,
        "first_day_of_month_only": first_day_of_month_only,
        "freshness_date": freshness_date,
        "mode": mode,
    }
    key_str = json.dumps(key_data, sort_keys=True)
    return hashlib.md5(key_str.encode()).hexdigest()


def get_cached_result(
    service: str,
    start_date: str,
    end_date: str,
    category: list[str] | None,
    venue: list[str] | None = None,
    folder: list[str] | None = None,
    data_type: list[str] | None = None,
    include_sub_dimensions: bool = False,
    include_instrument_types: bool = False,
    include_file_counts: bool = False,
    check_upstream_availability: bool = False,
    first_day_of_month_only: bool = False,
    freshness_date: str | None = None,
    mode: str = "batch",
) -> dict[str, object] | None:
    """
    Get cached data status result if available and not expired.

    Returns the full (non-truncated) result, or None if cache miss/expired.
    """
    key = _make_cache_key(
        service,
        start_date,
        end_date,
        category,
        venue,
        folder,
        data_type,
        include_sub_dimensions,
        include_instrument_types,
        include_file_counts,
        check_upstream_availability,
        first_day_of_month_only,
        freshness_date,
        mode,
    )

    with _cache_lock:
        if key not in _cache:
            logger.debug(
                "[DATA_STATUS_CACHE] MISS (not found): %s %s-%s", service, start_date, end_date
            )
            return None

        result, cached_at = _cache[key]
        age_seconds = (datetime.now(UTC) - cached_at).total_seconds()

        if age_seconds > CACHE_TTL_SECONDS:
            # Expired - remove from cache
            del _cache[key]
            logger.info(
                "[DATA_STATUS_CACHE] MISS (expired, age=%.1fs): %s %s-%s",
                age_seconds,
                service,
                start_date,
                end_date,
            )
            return None

        logger.info(
            "[DATA_STATUS_CACHE] HIT (age=%.1fs): %s %s-%s",
            age_seconds,
            service,
            start_date,
            end_date,
        )
        return result


def set_cached_result(
    service: str,
    start_date: str,
    end_date: str,
    category: list[str] | None,
    venue: list[str] | None = None,
    folder: list[str] | None = None,
    data_type: list[str] | None = None,
    include_sub_dimensions: bool = False,
    include_instrument_types: bool = False,
    include_file_counts: bool = False,
    result: dict[str, object] | None = None,
    check_upstream_availability: bool = False,
    first_day_of_month_only: bool = False,
    freshness_date: str | None = None,
    mode: str = "batch",
) -> None:
    """
    Store data status result in cache.

    The result should be the FULL (non-truncated) result with include_dates_list=True
    and full_dates_list=True.
    """
    key = _make_cache_key(
        service,
        start_date,
        end_date,
        category,
        venue,
        folder,
        data_type,
        include_sub_dimensions,
        include_instrument_types,
        include_file_counts,
        check_upstream_availability,
        first_day_of_month_only,
        freshness_date,
        mode,
    )

    with _cache_lock:
        if result is not None:
            _cache[key] = (result, datetime.now(UTC))
            logger.info(
                "[DATA_STATUS_CACHE] STORED: %s %s-%s (TTL=%ss)",
                service,
                start_date,
                end_date,
                CACHE_TTL_SECONDS,
            )


def truncate_dates_list(result: dict[str, object], max_items: int = 50) -> dict[str, object]:  # noqa: C901
    """
    Truncate dates lists in result for UI display.

    Takes a full result and returns a copy with truncated date lists:
    - If len <= max_items: keep as-is
    - If len > max_items: keep first 25 and last 25, mark as truncated

    This is applied recursively to categories and venues.
    """
    truncated = copy.deepcopy(result)
    half = max_items // 2

    def truncate_list_in_dict(
        d: dict[str, object], list_key: str, tail_key: str, truncated_key: str
    ):
        """Truncate a dates list in a dict."""
        if list_key not in d:
            return

        dates = d[list_key]
        if not isinstance(dates, list):
            return

        if len(dates) <= max_items:
            return

        # Truncate: first 25 + last 25
        d[list_key] = dates[:half]
        d[tail_key] = dates[-half:]
        d[truncated_key] = True

    # Truncate category-level dates
    cats_raw: object = truncated.get("categories") or {}
    for _cat_name, cat_data in (
        cast(dict[str, object], cats_raw) if isinstance(cats_raw, dict) else {}
    ).items():
        if isinstance(cat_data, dict):
            truncate_list_in_dict(
                cat_data,
                "dates_found_list",
                "dates_found_list_tail",
                "dates_found_truncated",
            )
            truncate_list_in_dict(
                cat_data,
                "dates_missing_list",
                "dates_missing_list_tail",
                "dates_missing_truncated",
            )

            # Truncate venue-level dates
            for _venue_name, venue_data in (cat_data.get("venues") or {}).items():
                if isinstance(venue_data, dict):
                    truncate_list_in_dict(
                        venue_data,
                        "dates_found_list",
                        "dates_found_list_tail",
                        "dates_found_truncated",
                    )
                    truncate_list_in_dict(
                        venue_data,
                        "dates_missing_list",
                        "dates_missing_list_tail",
                        "dates_missing_truncated",
                    )

                    # Truncate data_type-level dates
                    for _dt_name, dt_data in (venue_data.get("data_types") or {}).items():
                        if isinstance(dt_data, dict):
                            truncate_list_in_dict(
                                dt_data,
                                "dates_found_list",
                                "dates_found_list_tail",
                                "dates_found_truncated",
                            )
                            truncate_list_in_dict(
                                dt_data,
                                "dates_missing_list",
                                "dates_missing_list_tail",
                                "dates_missing_truncated",
                            )

    return truncated


def clear_cache() -> int:
    """Clear all cached results. Returns number of entries cleared."""
    with _cache_lock:
        count = len(_cache)
        _cache.clear()
        logger.info("[DATA_STATUS_CACHE] CLEARED: %s entries", count)
        return count


def get_cache_stats() -> dict[str, object]:
    """Get cache statistics."""
    with _cache_lock:
        now = datetime.now(UTC)
        entries_detail: list[object] = []
        stats: dict[str, object] = {
            "entries": len(_cache),
            "ttl_seconds": CACHE_TTL_SECONDS,
            "entries_detail": entries_detail,
        }
        for key, (result, cached_at) in _cache.items():
            age = (now - cached_at).total_seconds()
            entries_detail.append(
                {
                    "key": key[:12] + "...",
                    "service": result.get("service", result.get("config_path", "unknown")),
                    "age_seconds": round(age, 1),
                    "expires_in_seconds": round(max(0, CACHE_TTL_SECONDS - age), 1),
                }
            )
        return stats


# ============================================================================
# Execution Services Cache Functions
# ============================================================================


def _make_exec_cache_key(
    config_path: str,
    start_date: str | None,
    end_date: str | None,
) -> str:
    """Generate cache key for execution-service data status."""
    key_data = {
        "type": "execution-service",
        "config_path": config_path,
        "start_date": start_date,
        "end_date": end_date,
    }
    key_str = json.dumps(key_data, sort_keys=True)
    return hashlib.md5(key_str.encode()).hexdigest()


def get_exec_cached_result(
    config_path: str,
    start_date: str | None,
    end_date: str | None,
) -> dict[str, object] | None:
    """
    Get cached execution-service data status result if available and not expired.
    """
    key = _make_exec_cache_key(config_path, start_date, end_date)

    with _cache_lock:
        if key not in _cache:
            logger.debug("[EXEC_CACHE] MISS (not found): %s", config_path)
            return None

        result, cached_at = _cache[key]
        age_seconds = (datetime.now(UTC) - cached_at).total_seconds()

        if age_seconds > CACHE_TTL_SECONDS:
            del _cache[key]
            logger.info("[EXEC_CACHE] MISS (expired, age=%.1fs): %s", age_seconds, config_path)
            return None

        logger.info("[EXEC_CACHE] HIT (age=%.1fs): %s", age_seconds, config_path)
        return result


def set_exec_cached_result(
    config_path: str,
    start_date: str | None,
    end_date: str | None,
    result: dict[str, object],
) -> None:
    """Store execution-service data status result in cache."""
    key = _make_exec_cache_key(config_path, start_date, end_date)

    with _cache_lock:
        _cache[key] = (result, datetime.now(UTC))
        logger.info("[EXEC_CACHE] STORED: %s (TTL=%ss)", config_path, CACHE_TTL_SECONDS)

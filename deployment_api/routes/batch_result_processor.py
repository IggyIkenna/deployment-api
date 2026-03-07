"""
Result processing and aggregation for batch data processing.

Contains functions for calculating overall completion percentages,
venue-weighted statistics, and building the final response.
"""

import logging

from .batch_config_utils import get_expected_dates_for_venue

logger = logging.getLogger(__name__)


def calculate_overall_file_counts(
    results: dict[str, object], include_file_counts: bool
) -> dict[str, object] | None:
    """Calculate overall file counts across all categories.

    Args:
        results: Category results dictionary
        include_file_counts: Whether file counts were requested

    Returns:
        Overall file counts dict or None if not requested/unavailable
    """
    if not include_file_counts:
        return None

    total_files_all = 0
    total_dates_with_files = 0
    for cat_result in results.values():
        if "error" in cat_result:
            continue
        file_counts = cat_result.get("file_counts") or {}
        total_files_all += file_counts.get("total_files", 0)
        total_dates_with_files += file_counts.get("dates_with_file_counts", 0)

    if total_files_all > 0:
        return {
            "total_files": total_files_all,
            "dates_with_file_counts": total_dates_with_files,
            "avg_files_per_date": (
                round(total_files_all / total_dates_with_files, 1)
                if total_dates_with_files > 0
                else 0
            ),
        }
    return None


def calculate_venue_weighted_totals(
    results: dict[str, object],
    all_dates: set[str],
    expected_start_dates_config: dict[str, object],
    service: str,
    upstream_dates: dict[str, dict[str, set]] | None = None,
) -> tuple[int, int, int, int]:
    """Calculate venue-weighted totals across all categories.

    Args:
        results: Category results dictionary
        all_dates: Set of all dates in the range
        expected_start_dates_config: Expected start dates configuration
        service: Service name
        upstream_dates: Upstream availability data for cascading

    Returns:
        Tuple of (total_venue_expected, total_venue_found, expected_missing, unexpected_missing)
    """
    total_venue_expected = 0
    total_venue_found = 0
    expected_missing = 0  # Missing data that SHOULD exist (date >= venue start)
    unexpected_missing = 0  # Kept for backwards compat but always 0 now

    for cat_name, cat_result in results.items():
        if "error" in cat_result:
            continue
        venue_summary = cat_result.get("venue_summary") or {}
        venues = cat_result.get("venues") or {}
        cat_dates_expected = cat_result.get("dates_expected", 0)
        cat_dates_found = cat_result.get("dates_found", 0)

        if venues:
            for _venue_name, venue_info in venues.items():
                # Use dimension-weighted values when available (accounts for
                # multiple expected data_types/folders per venue).
                # Falls back to raw venue dates for services without sub-dimensions.
                venue_expected = venue_info.get(
                    "_dim_weighted_expected",
                    venue_info.get(
                        "dates_expected_venue",
                        venue_info.get("dates_expected", 0),
                    ),
                )
                venue_found = venue_info.get(
                    "_dim_weighted_found",
                    venue_info.get("dates_found", 0),
                )
                is_expected = venue_info.get("is_expected", True)

                # Only count EXPECTED venues in the overall totals
                # Bonus venues exist but shouldn't affect the completion percentage
                if is_expected:
                    total_venue_expected += venue_expected
                    total_venue_found += venue_found

                    # Calculate EXPECTED missing: data that SHOULD exist but doesn't
                    # This only counts dates >= venue start date
                    venue_missing = venue_expected - venue_found
                    if venue_missing > 0:
                        expected_missing += venue_missing

            # Also count expected venues that have no data at all.
            # Use pre-computed dimension-weighted expected if available
            # (ensures consistency with dimension-weighted present venues).
            missing_dim_exp = cat_result.get("_missing_venue_dim_expected")
            if missing_dim_exp is not None and missing_dim_exp > 0:
                expected_missing += missing_dim_exp
                total_venue_expected += missing_dim_exp
            else:
                # Fallback: use raw venue-specific expected dates
                for missing_venue in venue_summary.get("expected_but_missing") or []:
                    venue_specific_expected = get_expected_dates_for_venue(
                        all_dates,
                        expected_start_dates_config,
                        service,
                        cat_name,
                        missing_venue,
                        upstream_avail_dates=upstream_dates,
                    )
                    venue_expected_count = len(venue_specific_expected)
                    if venue_expected_count > 0:
                        expected_missing += venue_expected_count
                        total_venue_expected += venue_expected_count
        else:
            # No venue breakdown - use category-level data
            # This happens when include_sub_dimensions=False (default)
            total_venue_expected += cat_dates_expected
            total_venue_found += cat_dates_found
            cat_missing = cat_dates_expected - cat_dates_found
            if cat_missing > 0:
                expected_missing += cat_missing

    return total_venue_expected, total_venue_found, expected_missing, unexpected_missing


def update_category_completion_percentages(
    results: dict[str, object],
    all_dates: set[str],
    expected_start_dates_config: dict[str, object],
    service: str,
    upstream_dates: dict[str, dict[str, set]] | None = None,
) -> None:
    """Update category-level completion percentages to be venue-weighted.

    This ensures "100%" only shows when ALL expected venues have ALL dates.

    Args:
        results: Category results dictionary (modified in place)
        all_dates: Set of all dates in the range
        expected_start_dates_config: Expected start dates configuration
        service: Service name
        upstream_dates: Upstream availability data for cascading
    """
    for cat_name, cat_result in results.items():
        if "error" in cat_result:
            continue
        venues = cat_result.get("venues") or {}
        if not venues:
            # No venue breakdown, keep date-level calculation
            continue

        # Calculate venue-weighted completion for this category
        # Only count EXPECTED venues - bonus venues shouldn't affect completion %
        cat_venue_expected = 0
        cat_venue_found = 0
        for _venue_name, venue_info in venues.items():
            if not venue_info.get("is_expected", True):
                continue  # Skip bonus venues
            # Use dimension-weighted values when available (accounts for
            # multiple expected data_types/folders per venue)
            v_expected = venue_info.get(
                "_dim_weighted_expected",
                venue_info.get(
                    "dates_expected_venue",
                    venue_info.get("dates_expected", 0),
                ),
            )
            v_found = venue_info.get(
                "_dim_weighted_found",
                venue_info.get("dates_found", 0),
            )
            cat_venue_expected += v_expected
            cat_venue_found += v_found

        # Add missing expected venues (they have 0 found but should count as expected)
        # Use pre-computed dimension-weighted expected if available
        venue_summary = cat_result.get("venue_summary") or {}
        missing_dim_exp = cat_result.get("_missing_venue_dim_expected")
        if missing_dim_exp is not None and missing_dim_exp > 0:
            cat_venue_expected += missing_dim_exp
        else:
            for missing_venue in venue_summary.get("expected_but_missing") or []:
                venue_specific_expected = get_expected_dates_for_venue(
                    all_dates,
                    expected_start_dates_config,
                    service,
                    cat_name,
                    missing_venue,
                    upstream_avail_dates=upstream_dates,
                )
                cat_venue_expected += len(venue_specific_expected)

        # Update completion_pct to venue-weighted value
        if cat_venue_expected > 0:
            cat_result["completion_pct"] = round(cat_venue_found / cat_venue_expected * 100, 1)
            cat_result["venue_weighted"] = True
            cat_result["venue_dates_found"] = cat_venue_found
            cat_result["venue_dates_expected"] = cat_venue_expected


def build_final_response(
    service: str,
    start_date: str,
    end_date: str,
    first_day_of_month_only: bool,
    sub_dimension_name: str | None,
    include_sub_dimensions: bool,
    include_file_counts: bool,
    all_dates: set,
    total_venue_expected: int,
    total_venue_found: int,
    total_expected_category: int,
    total_found_category: int,
    expected_missing: int,
    unexpected_missing: int,
    results: dict[str, object],
    overall_file_counts: dict[str, object] | None,
) -> dict[str, object]:
    """Build the final response dictionary.

    Args:
        All the calculated totals and metadata needed for the response

    Returns:
        Complete response dictionary
    """
    # Calculate overall percentage - use venue-weighted if available, else category-level
    # This handles services without venue breakdown (e.g., market-data-processing-service)
    if total_venue_expected > 0:
        overall_pct = total_venue_found / total_venue_expected * 100
        # total_missing = expected_missing (only counts dates >= venue start)
        total_missing = expected_missing
    else:
        # No venue data - fall back to category-level calculation
        overall_pct = (
            (total_found_category / total_expected_category * 100) if total_expected_category else 0
        )
        total_missing = total_expected_category - total_found_category

    response = {
        "service": service,
        "date_range": {"start": start_date, "end": end_date, "days": len(all_dates)},
        "mode": "turbo",
        "first_day_of_month_only": first_day_of_month_only,
        "sub_dimension": sub_dimension_name if include_sub_dimensions else None,
        "include_file_counts": include_file_counts,
        "overall_completion_pct": round(overall_pct, 1),
        "overall_dates_found": total_venue_found,
        "overall_dates_expected": total_venue_expected,
        # Category-level totals for reference (not venue-weighted)
        "overall_dates_found_category": total_found_category,
        "overall_dates_expected_category": total_expected_category,
        "total_missing": total_missing,
        "unexpected_missing": unexpected_missing,
        "expected_missing": expected_missing,
        "categories": results,
    }

    # Add overall file counts if available
    if overall_file_counts:
        response["overall_file_counts"] = overall_file_counts

    return response

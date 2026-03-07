"""
Configuration utilities and helper functions for batch data processing.

Contains functions for loading configurations, managing expected dates,
data types, venues, and service configurations.
"""

import logging
from datetime import UTC, datetime, timedelta
from pathlib import Path

import yaml

from deployment_api.settings import GCP_PROJECT_ID as _PID

logger = logging.getLogger(__name__)

# Live persistence path prefix: live data sink writes under this prefix in same buckets.
# See codex 04-architecture/batch-live-symmetry.md and deployment-topology-diagrams.md.
LIVE_PATH_PREFIX = "live/"

# Service -> bucket mapping (uses storage facade)
BUCKET_MAPPING = {
    "instruments-service": {
        "CEFI": f"instruments-store-cefi-{_PID}",
        "DEFI": f"instruments-store-defi-{_PID}",
        "TRADFI": f"instruments-store-tradfi-{_PID}",
    },
    "market-tick-data-handler": {
        "CEFI": f"market-data-tick-cefi-{_PID}",
        "DEFI": f"market-data-tick-defi-{_PID}",
        "TRADFI": f"market-data-tick-tradfi-{_PID}",
    },
    "market-data-processing-service": {
        # NOTE: Processing service writes to tick buckets with /processed_candles/ prefix
        "CEFI": f"market-data-tick-cefi-{_PID}",
        "DEFI": f"market-data-tick-defi-{_PID}",
        "TRADFI": f"market-data-tick-tradfi-{_PID}",
    },
    "features-delta-one-service": {
        "CEFI": f"features-delta-one-cefi-{_PID}",
        "DEFI": f"features-delta-one-defi-{_PID}",
        "TRADFI": f"features-delta-one-tradfi-{_PID}",
    },
    "features-calendar-service": {
        # Calendar features are UNIVERSAL - use single bucket (CEFI)
        # Temporal patterns, economic events, macro indicators don't vary by category
        "CEFI": f"features-calendar-cefi-{_PID}",
    },
    "features-onchain-service": {
        "CEFI": f"features-onchain-cefi-{_PID}",
        "DEFI": f"features-onchain-defi-{_PID}",
    },
    "features-volatility-service": {
        "CEFI": f"features-volatility-cefi-{_PID}",
        "DEFI": f"features-volatility-defi-{_PID}",
        "TRADFI": f"features-volatility-tradfi-{_PID}",
    },
    "corporate-actions": {
        # Corporate actions are TRADFI-only (uses instruments-store bucket)
        "TRADFI": f"instruments-store-tradfi-{_PID}",
    },
}

# Service configurations: prefix, date pattern, and optional sub-dimensions
# sub_dimension: primary breakdown (data_type, venue, feature_group)
# venue_extraction: secondary venue breakdown for services that shard by venue
#   - "from_filename": extract venue from filenames (e.g., BINANCE-FUTURES:...)
#   - "from_directory": extract venue from directory path (e.g., .../venue/file.parquet)
SERVICE_CONFIG = {
    "instruments-service": {
        "prefix_template": "instrument_availability/by_date/day={year_month}",
        "date_pattern": r"day=(\d{4}-\d{2}-\d{2})",
        "sub_dimension": "venue",
        "sub_pattern": r"venue=([^/]+)",
    },
    "market-tick-data-handler": {
        "prefix_template": "raw_tick_data/by_date/day={year_month}",
        "date_pattern": r"day=(\d{4}-\d{2}-\d{2})",
        "sub_dimension": "data_type",
        "sub_pattern": r"data_type=([^/]+)",
        # Instrument type breakdown: directory level below data_type
        "supports_instrument_types": True,
        # Venue breakdown: fast directory listing
        # Path: .../data_type={dt}/instrument_type={folder}/venue={venue}/instrument_key={instrument}.parquet
        "venue_extraction": "from_path_parsing",
    },
    "market-data-processing-service": {
        "prefix_template": "processed_candles/by_date/day={year_month}",
        "date_pattern": r"day=(\d{4}-\d{2}-\d{2})",
        # Primary sub-dimension: timeframe (first level after date)
        "sub_dimension": "timeframe",
        "sub_pattern": r"timeframe=([^/]+)",
        # Instrument type and venue are deeper in the hierarchy
        # Path: .../day={date}/timeframe={tf}/data_type={dt}/{instrument_id}.parquet (flat)
        "supports_instrument_types": True,
        # Venue extraction: parse from path (same as market-tick-data-handler)
        "venue_extraction": "from_path_parsing",
    },
    "features-delta-one-service": {
        "prefix_template": "by_date/day={year_month}",
        "date_pattern": r"day=(\d{4}-\d{2}-\d{2})",
        "sub_dimension": "feature_group",
        "sub_pattern": r"feature_group=([^/]+)",
    },
    "features-calendar-service": {
        # GCS path: calendar/category={type}/by_date/day={date}/features.parquet
        # feature_type = temporal, scheduled_events, macro
        "prefix_template": "calendar/",
        "date_pattern": r"by_date/day=(\d{4}-\d{2}-\d{2})",
        # sub_dimension is feature_type (temporal, scheduled_events, macro)
        "sub_dimension": "feature_type",
        "sub_pattern": r"calendar/([^/]+)/by_date",
    },
    "features-onchain-service": {
        "prefix_template": "by_date/day={year_month}",
        "date_pattern": r"day=(\d{4}-\d{2}-\d{2})",
        "sub_dimension": "feature_group",
        "sub_pattern": r"feature_group=([^/]+)",
    },
    "features-volatility-service": {
        "prefix_template": "by_date/day={year_month}",
        "date_pattern": r"day=(\d{4}-\d{2}-\d{2})",
        "sub_dimension": "feature_group",
        "sub_pattern": r"feature_group=([^/]+)",
    },
    "corporate-actions": {
        # Corporate actions stored in instruments-store-tradfi bucket
        "prefix_template": "corporate_actions/by_date/day={year_month}",
        "date_pattern": r"day=(\d{4}-\d{2}-\d{2})",
    },
}


def load_expected_start_dates() -> dict[str, dict[str, object]]:
    """Load expected start dates from configs/expected_start_dates.yaml."""
    config_path = Path(__file__).parent.parent.parent / "configs" / "expected_start_dates.yaml"
    if not config_path.exists():
        logger.warning("Expected start dates config not found: %s", config_path)
        return {}
    with open(config_path) as f:
        return yaml.safe_load(f) or {}


def load_venue_data_types() -> dict[str, dict[str, object]]:
    """Load venue-specific data type expectations from venue_data_types.yaml."""
    config_path = Path(__file__).parent.parent.parent / "configs" / "venue_data_types.yaml"
    if not config_path.exists():
        logger.warning("Venue data types config not found: %s", config_path)
        return {}
    with open(config_path) as f:
        return yaml.safe_load(f) or {}


def get_expected_venues_for_category(venue_config: dict[str, object], category: str) -> set[str]:
    """Get the set of expected venues for a category from venue_data_types.yaml."""
    cat_config = venue_config.get(category, {})
    venues = cat_config.get("venues") or {}
    return set(venues.keys())


def is_venue_expected(venue_config: dict[str, object], category: str, venue: str) -> bool:
    """Check if a venue is expected for a category."""
    expected_venues = get_expected_venues_for_category(venue_config, category)
    return venue in expected_venues


def get_expected_data_types_for_venue(
    venue_config: dict[str, object], category: str, venue: str
) -> list[str]:
    """Get expected data_types for a specific venue from venue_data_types.yaml."""
    cat_config = venue_config.get(category, {})
    venues = cat_config.get("venues") or {}
    venue_cfg = venues.get(venue, {})
    return venue_cfg.get("data_types") or []


def get_expected_instrument_types_for_venue(
    venue_config: dict[str, object], category: str, venue: str
) -> list[str]:
    """Get expected instrument_types for a specific venue from venue_data_types.yaml."""
    cat_config = venue_config.get(category, {})
    venues = cat_config.get("venues") or {}
    venue_cfg = venues.get(venue, {})
    return venue_cfg.get("instrument_types") or []


def get_data_type_start_date(
    expected_dates_config: dict[str, object],
    service: str,
    category: str,
    venue: str,
    data_type: str,
) -> str | None:
    """Get data_type-specific start date for a venue.

    Returns the start date for a specific data_type within a venue.
    Falls back to venue start date if no data_type-specific date is configured.
    Returns None if the data_type is not available for this venue.
    """
    service_config = expected_dates_config.get(service, {})
    category_config = service_config.get(category, {})
    if isinstance(category_config, dict):
        # Check for data_type-specific start dates
        dt_start_dates = category_config.get("data_type_start_dates") or {}
        venue_dt_config = dt_start_dates.get(venue, {})
        if data_type in venue_dt_config:
            # Explicit config for this data_type (could be null = not available)
            return venue_dt_config.get(data_type)
        # Fall back to venue start date
        venues = category_config.get("venues") or {}
        if venue in venues:
            return venues[venue]
        return category_config.get("category_start")
    return None


def is_data_type_available_for_venue(
    expected_dates_config: dict[str, object],
    service: str,
    category: str,
    venue: str,
    data_type: str,
) -> bool:
    """Check if a data_type is available at all for a venue.

    Returns False if the data_type has a null start date (explicitly unavailable).
    """
    service_config = expected_dates_config.get(service, {})
    category_config = service_config.get(category, {})
    if isinstance(category_config, dict):
        dt_start_dates = category_config.get("data_type_start_dates") or {}
        venue_dt_config = dt_start_dates.get(venue, {})
        if data_type in venue_dt_config:
            # Explicitly configured - check if it's null (not available)
            return venue_dt_config.get(data_type) is not None
    # Not explicitly configured, assume available
    return True


def get_category_start_date(
    expected_dates_config: dict[str, object], service: str, category: str
) -> str | None:
    """Get the expected start date for a service/category."""
    service_config = expected_dates_config.get(service, {})
    category_config = service_config.get(category, {})
    if isinstance(category_config, dict):
        return category_config.get("category_start")
    return None


def get_venue_start_date(
    expected_dates_config: dict[str, object], service: str, category: str, venue: str
) -> str | None:
    """Get venue-specific start date, falling back to category start.

    Looks up venue-level start dates from expected_start_dates.yaml.
    This allows accurate completion % calculation per venue.
    """
    service_config = expected_dates_config.get(service, {})
    category_config = service_config.get(category, {})
    if isinstance(category_config, dict):
        venues = category_config.get("venues") or {}
        if venue in venues:
            return venues[venue]
        return category_config.get("category_start")
    return None


def get_expected_dates_for_venue(
    all_dates: set[str],
    expected_dates_config: dict[str, object],
    service: str,
    category: str,
    venue: str,
    upstream_avail_dates: dict[str, dict[str, set]] | None = None,
) -> set:
    """Get expected dates for a specific venue based on its start date.

    For market-data-processing-service with upstream_avail_dates:
        Expected dates = upstream dates that exist for this venue
        (i.e., only expect processing data where tick data exists)

    For other services or without upstream data:
        Expected dates = all dates >= venue start date
    """
    # For market-data-processing-service, use upstream availability as baseline
    if service == "market-data-processing-service" and upstream_avail_dates is not None:
        cat_upstream = upstream_avail_dates.get(category, {})
        # Try venue-specific dates first, fall back to category-level
        venue_upstream = cat_upstream.get(venue, cat_upstream.get("__category__", set()))
        if venue_upstream:
            # Expected = upstream dates that are in our date range
            return venue_upstream & all_dates

    # Default: use venue start date
    venue_start = get_venue_start_date(expected_dates_config, service, category, venue)
    if not venue_start:
        # No venue or category start date configured, use all dates
        return all_dates
    # Filter to dates >= venue_start
    return {d for d in all_dates if d >= venue_start}


def generate_date_range_and_year_months(
    start_date: str, end_date: str, first_day_of_month_only: bool = False
) -> tuple[set, set]:
    """Generate all dates in range and year-month prefixes.

    Args:
        start_date: Start date in YYYY-MM-DD format
        end_date: End date in YYYY-MM-DD format
        first_day_of_month_only: Only include first day of each month

    Returns:
        Tuple of (all_dates set, year_months set)
    """
    start_dt = datetime.strptime(start_date, "%Y-%m-%d").replace(tzinfo=UTC)
    end_dt = datetime.strptime(end_date, "%Y-%m-%d").replace(tzinfo=UTC)
    all_dates = set()
    year_months = set()
    current = start_dt
    while current <= end_dt:
        date_str = current.strftime("%Y-%m-%d")
        all_dates.add(date_str)
        year_months.add(current.strftime("%Y-%m"))
        current += timedelta(days=1)

    # Filter to first day of month only if requested
    # Useful for TARDIS free tier (no API key needed for first-day-of-month data)
    if first_day_of_month_only:
        original_count = len(all_dates)
        all_dates = {d for d in all_dates if d.endswith("-01")}
        logger.info(
            "[TURBO] First-day-of-month filter: %s -> %s dates", original_count, len(all_dates)
        )

    return all_dates, year_months

"""
Data query service for file listing and path operations.

Handles GCS bucket operations, file listing, venue filtering,
and instrument availability queries.
"""

import logging
from datetime import UTC, datetime, timedelta
from typing import cast

from deployment_api.settings import gcp_project_id as _pid
from deployment_api.utils.storage_facade import (
    ObjectInfo,
    list_objects,
    list_prefixes,
    object_exists,
)

logger = logging.getLogger(__name__)


class DataQueryService:
    """
    Query service for data operations.

    This service handles:
    - GCS bucket file listing
    - Path-based data queries
    - Venue filtering operations
    - Instrument availability checks
    """

    def __init__(self, project_id: str | None = None):
        """Initialize data query service."""
        self.project_id = project_id or _pid

    def build_bucket_name(self, prefix: str, category: str) -> str:
        """Build a GCS bucket name: {prefix}-{category_lower}-{project_id}."""
        return f"{prefix}-{category.lower()}-{self.project_id}"

    async def list_files_in_path(
        self,
        bucket_name: str,
        path: str = "",
        max_results: int = 100,
        show_dirs: bool = False,
    ) -> dict[str, object]:
        """
        List files in a specific GCS bucket path.

        Args:
            bucket_name: GCS bucket name
            path: Path within bucket (optional)
            max_results: Maximum number of results to return
            show_dirs: Whether to include directory-like prefixes

        Returns:
            Dictionary containing file listing results
        """
        try:
            logger.info("Listing files in %s/%s", bucket_name, path)

            files: list[dict[str, object]] = []
            directories: set[str] = set()
            truncated: bool = False

            # List objects in the path
            objects: list[ObjectInfo] = list_objects(bucket_name, path, max_results=max_results * 2)

            for obj in objects:
                obj_path: str = obj.name
                # Skip the path itself if it matches exactly
                if obj_path == path:
                    continue

                # Extract relative path from the prefix
                if path and not obj_path.startswith(path):
                    continue

                relative_path = obj_path[len(path) :] if path else obj_path
                relative_path = relative_path.lstrip("/")

                # Check if this looks like a directory
                if "/" in relative_path:
                    # Extract directory name
                    dir_name = relative_path.split("/")[0]
                    full_dir_path = f"{path}/{dir_name}".strip("/") if path else dir_name
                    directories.add(full_dir_path)
                else:
                    # It's a file
                    files.append(
                        {
                            "name": relative_path,
                            "full_path": obj_path,
                            "size": None,  # Would need GCS metadata call to get size
                            "type": "file",
                        }
                    )

            # Limit results
            if len(files) > max_results:
                files = files[:max_results]
                truncated = True

            result: dict[str, object] = {
                "bucket": bucket_name,
                "path": path,
                "files": files,
                "directories": [{"name": d, "type": "directory"} for d in sorted(directories)],
                "total_count": len(files) + len(directories),
                "truncated": truncated,
            }

            return result

        except (OSError, ValueError, RuntimeError) as e:
            logger.error("Error listing files in %s/%s: %s", bucket_name, path, e)
            return {"error": str(e)}

    async def get_venue_filters(self, service: str) -> dict[str, object]:
        """
        Get available venue filters for a service.

        Args:
            service: Service name to get venues for

        Returns:
            Dictionary containing available venues by category
        """
        # Service to bucket mappings
        service_mappings = {
            "market-tick-data-handler": {
                "prefix": "market-data",
                "categories": ["cefi", "tradfi", "defi"],
            },
            "market-data-processing-service": {
                "prefix": "processed-market-data",
                "categories": ["cefi", "tradfi", "defi"],
            },
            "instruments-service": {
                "prefix": "instruments",
                "categories": ["cefi", "tradfi", "defi"],
            },
            "features-equity-service": {
                "prefix": "features-equity",
                "categories": ["tradfi"],
            },
            "features-derivatives-service": {
                "prefix": "features-derivatives",
                "categories": ["cefi"],
            },
            "features-defi-service": {
                "prefix": "features-defi",
                "categories": ["defi"],
            },
        }

        mapping = service_mappings.get(service)
        if not mapping:
            return {"error": f"Unknown service: {service}"}

        categories_result: dict[str, dict[str, object]] = {}
        venue_filters: dict[str, object] = {
            "service": service,
            "categories": categories_result,
        }

        for category in cast(list[str], mapping.get("categories") or []):
            try:
                bucket_name = self.build_bucket_name(
                    cast(str, mapping.get("prefix") or ""), category
                )
                venues: list[str] = []

                # List prefixes to find venue directories
                # Assuming structure: bucket/venue/date/... or bucket/date/venue/...
                prefixes = list_prefixes(bucket_name, "")

                # Extract venue names from prefixes
                for prefix in prefixes[:50]:  # Limit to avoid huge responses
                    # Remove trailing slash
                    clean_prefix = prefix.rstrip("/")

                    # Extract potential venue name
                    # This is heuristic - actual structure may vary
                    parts = clean_prefix.split("/")
                    if parts:
                        venue_name = parts[0]
                        if venue_name and venue_name not in venues:
                            venues.append(venue_name)

                categories_result[category] = {
                    "venues": sorted(venues),
                    "count": len(venues),
                }

            except (OSError, ValueError, RuntimeError) as e:
                logger.debug("Error getting venues for %s: %s", category, e)
                categories_result[category] = {
                    "error": str(e),
                    "venues": [],
                    "count": 0,
                }

        return venue_filters

    async def get_instruments_list(
        self,
        category: str,
        venue: str | None = None,
        instrument_type: str | None = None,
        limit: int = 100,
    ) -> dict[str, object]:
        """
        Get list of instruments for a category.

        Args:
            category: Category (cefi, tradfi, defi)
            venue: Optional venue filter
            instrument_type: Optional instrument type filter
            limit: Maximum number of instruments to return

        Returns:
            Dictionary containing instruments list
        """
        try:
            # Map to instruments bucket
            bucket_name = self.build_bucket_name("instruments", category)

            # Build path based on filters
            path = ""
            if venue:
                path = f"{venue}/"
            if instrument_type:
                # Map instrument type to folder structure
                type_mapping = {
                    "SPOT": "spot_pairs",
                    "PERPETUAL": "perps",
                    "FUTURE": "futures",
                    "OPTION": "options",
                    "EQUITY": "equities",
                    "BOND": "bonds",
                    "ETF": "etfs",
                }
                folder = type_mapping.get(instrument_type.upper())
                if folder:
                    path += f"{folder}/"

            # List files to find instruments
            objects_list: list[ObjectInfo] = list_objects(bucket_name, path, max_results=limit * 2)

            instruments: list[str] = []
            truncated_instr: bool = False
            for obj in objects_list:
                # Extract instrument name from path
                # Assuming structure like: venue/type/instrument_name.parquet
                parts = obj.name.split("/")
                if len(parts) >= 2:
                    filename = parts[-1]
                    # Remove file extension
                    instrument_name = filename.rsplit(".", 1)[0] if "." in filename else filename

                    if instrument_name and instrument_name not in instruments:
                        instruments.append(instrument_name)

                        if len(instruments) >= limit:
                            truncated_instr = True
                            break

            instruments_result: dict[str, object] = {
                "category": category,
                "venue": venue,
                "instrument_type": instrument_type,
                "instruments": sorted(instruments),
                "total_count": len(instruments),
                "truncated": truncated_instr,
            }

            return instruments_result

        except (OSError, ValueError, RuntimeError) as e:
            logger.error("Error getting instruments list: %s", e)
            return {"error": str(e)}

    async def get_instrument_availability(  # noqa: C901
        self,
        venue: str,
        instrument_type: str,
        instrument: str,
        start_date: str,
        end_date: str,
        data_type: str | None = None,
        available_from: str | None = None,
        available_to: str | None = None,
    ) -> dict[str, object]:
        """
        Check instrument availability over a date range.

        Args:
            venue: Venue name
            instrument_type: Type of instrument
            instrument: Instrument symbol
            start_date: Start date (YYYY-MM-DD)
            end_date: End date (YYYY-MM-DD)
            data_type: Optional data type to check
            available_from: Instrument availability start date
            available_to: Instrument availability end date

        Returns:
            Dictionary containing availability analysis
        """
        try:
            # Determine category from venue
            venue_upper = venue.upper()
            if any(
                cefi in venue_upper for cefi in ["BINANCE", "BYBIT", "OKX", "DERIBIT", "BITMEX"]
            ):
                category = "CEFI"
            elif any(tradfi in venue_upper for tradfi in ["NYSE", "NASDAQ", "CME", "CBOE", "ICE"]):
                category = "TRADFI"
            elif any(defi in venue_upper for defi in ["UNISWAP", "AAVE", "CURVE", "BALANCER"]):
                category = "DEFI"
            else:
                return {"error": f"Could not determine category for venue: {venue}"}

            # Bucket mapping
            market_data_buckets = {
                "CEFI": self.build_bucket_name("market-data", "cefi"),
                "TRADFI": self.build_bucket_name("market-data", "tradfi"),
                "DEFI": self.build_bucket_name("market-data", "defi"),
            }

            bucket_name = market_data_buckets.get(category)
            if not bucket_name:
                return {"error": f"No bucket mapping for category: {category}"}

            # Parse dates
            try:
                start_dt = datetime.strptime(start_date, "%Y-%m-%d").replace(tzinfo=UTC)
                end_dt = datetime.strptime(end_date, "%Y-%m-%d").replace(tzinfo=UTC)
            except ValueError as e:
                return {"error": f"Invalid date format: {e}"}

            # Handle availability window
            instrument_available_from = None
            instrument_available_to = None

            if available_from:
                try:
                    if "T" in available_from:
                        instrument_available_from = datetime.fromisoformat(
                            available_from.replace("Z", "+00:00")
                        )
                    else:
                        instrument_available_from = datetime.strptime(
                            available_from, "%Y-%m-%d"
                        ).replace(tzinfo=UTC)
                except (ValueError, TypeError):
                    logger.warning("Could not parse available_from: %s", available_from)

            if available_to:
                try:
                    if "T" in available_to:
                        instrument_available_to = datetime.fromisoformat(
                            available_to.replace("Z", "+00:00")
                        )
                    else:
                        instrument_available_to = datetime.strptime(
                            available_to, "%Y-%m-%d"
                        ).replace(tzinfo=UTC)
                except (ValueError, TypeError):
                    logger.warning("Could not parse available_to: %s", available_to)

            # Calculate effective date range
            effective_start = start_dt
            effective_end = end_dt

            if instrument_available_from and instrument_available_from > effective_start:
                effective_start = instrument_available_from
            if instrument_available_to and instrument_available_to < effective_end:
                effective_end = instrument_available_to

            # Default data types by category
            if not data_type:
                if category == "CEFI":
                    data_types = ["trades", "book_snapshot_5"]
                elif category == "TRADFI":
                    data_types = ["trades", "ohlcv_1m", "tbbo"]
                elif category == "DEFI":
                    data_types = ["swaps", "rate_indices"]
                else:
                    data_types = ["trades"]
            else:
                data_types = [data_type]

            # Check availability for each date and data type
            daily_availability: dict[str, dict[str, bool]] = {}

            # Check each day in range
            current_dt = effective_start
            while current_dt <= effective_end:
                date_str = current_dt.strftime("%Y-%m-%d")
                daily_availability[date_str] = {}

                for dt in data_types:
                    # Build expected path for this instrument/date/data_type
                    # Structure varies by category, this is simplified
                    expected_path = (
                        f"{venue}/{instrument_type.lower()}/{instrument}/{date_str}/{dt}"
                    )

                    # Check if data exists
                    exists = object_exists(bucket_name, expected_path)
                    daily_availability[date_str][dt] = exists

                current_dt += timedelta(days=1)

            # Calculate summary statistics
            total_days = len(daily_availability)
            available_days = 0

            for date_data in daily_availability.values():
                if any(date_data.values()):  # At least one data type available
                    available_days += 1

            summary: dict[str, object] = {
                "total_days": total_days,
                "available_days": available_days,
                "missing_days": total_days - available_days,
                "availability_rate": (available_days / total_days * 100) if total_days > 0 else 0.0,
            }

            availability: dict[str, object] = {
                "venue": venue,
                "instrument_type": instrument_type,
                "instrument": instrument,
                "date_range": {"start": start_date, "end": end_date},
                "effective_range": {
                    "start": effective_start.strftime("%Y-%m-%d"),
                    "end": effective_end.strftime("%Y-%m-%d"),
                },
                "data_types": data_types,
                "daily_availability": daily_availability,
                "summary": summary,
            }

            return availability

        except (OSError, ValueError, RuntimeError) as e:
            logger.error("Error checking instrument availability: %s", e)
            return {"error": str(e)}

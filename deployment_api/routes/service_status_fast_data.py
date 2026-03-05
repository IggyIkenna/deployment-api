"""
Fast Data Status Functions

Optimized functions for quickly getting data timestamps using date-partitioned lookups.
"""

import asyncio
import logging
import re
from datetime import UTC, datetime
from typing import cast

logger = logging.getLogger(__name__)

# Service to GCS bucket mapping (imported from checkers module for consistency)
from .service_status_checkers import SERVICE_OUTPUT_BUCKETS


async def get_latest_data_timestamp_fast(service: str) -> dict[str, object] | None:
    """
    FAST version: Get most recent data timestamp by checking latest date folder.

    For date-partitioned data (day=YYYY-MM-DD), lists date folders and checks
    the most recent one for actual file timestamps. Uses storage facade (FUSE).
    """
    from deployment_api.utils.storage_facade import list_objects, list_prefixes

    def _get_timestamps_sync():
        try:
            buckets = SERVICE_OUTPUT_BUCKETS.get(service, {})

            if not buckets:
                return {"latest": None, "by_category": {}}

            results = {}
            for category, bucket_name in buckets.items():
                try:
                    # Strategy: Find most recent date folder, then check files there
                    prefixes_to_try = [
                        "raw_tick_data/by_date/",  # market-tick-data-handler
                        "instruments/",  # instruments-service
                        "",  # root level
                    ]

                    latest_timestamp = None

                    for prefix in prefixes_to_try:
                        # List prefixes (folders) via storage facade
                        prefixes_found = list_prefixes(bucket_name, prefix)

                        # Look for day=YYYY-MM-DD pattern in prefixes
                        date_prefixes = [p for p in prefixes_found if re.search(r"day=\d{4}-\d{2}-\d{2}", p)]

                        if date_prefixes:
                            date_prefixes.sort(reverse=True)
                            most_recent_prefix = date_prefixes[0]

                            # Get files in most recent date folder
                            recent_objs = list_objects(bucket_name, most_recent_prefix, max_results=10)
                            if recent_objs:
                                latest_obj = max(
                                    recent_objs,
                                    key=lambda o: o.updated if o.updated else datetime.min.replace(tzinfo=UTC),
                                )
                                latest_timestamp = latest_obj.updated
                            break
                        else:
                            # No date folders, check actual files at this prefix
                            objs = list_objects(bucket_name, prefix, max_results=500)
                            if objs:
                                latest_obj = max(
                                    objs,
                                    key=lambda o: o.updated if o.updated else datetime.min.replace(tzinfo=UTC),
                                )
                                latest_timestamp = latest_obj.updated
                                break

                    if latest_timestamp:
                        results[category] = {
                            "timestamp": latest_timestamp.isoformat(),
                        }
                    else:
                        results[category] = {"timestamp": None}

                except (OSError, ValueError, RuntimeError) as e:
                    logger.warning("Error checking %s/%s: %s", category, bucket_name, e)
                    results[category] = {"error": str(e)[:50]}

            valid_timestamps = [datetime.fromisoformat(r["timestamp"]) for r in results.values() if r.get("timestamp")]

            return {
                "by_category": results,
                "latest": (max(valid_timestamps).isoformat() if valid_timestamps else None),
            }
        except (OSError, ValueError, RuntimeError) as e:
            return {"error": str(e)[:100]}

    return cast(dict[str, object] | None, cast(object, await asyncio.to_thread(_get_timestamps_sync)))

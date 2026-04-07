"""
Data status business logic service.

Handles core data status operations including CLI integration,
missing shards calculation, and status aggregation.
"""

import asyncio
import json
import logging
import sys
import time
from typing import ClassVar, cast

import pandas as pd
from unified_api_contracts import VenueMapping
from unified_api_contracts.internal import MarketCategory
from unified_trading_library import read_availability_index

from deployment_api.settings import gcp_project_id as _pid
from deployment_api.utils.storage_facade import list_objects

logger = logging.getLogger(__name__)

# Cache for availability index reads — avoids repeated GCS downloads
_INDEX_CACHE: dict[str, tuple[float, pd.DataFrame]] = {}
_INDEX_CACHE_TTL = 300  # 5 minutes


def _read_index_cached(bucket: str) -> pd.DataFrame:
    """Read availability index with 5-minute TTL cache."""
    now = time.monotonic()
    cached = _INDEX_CACHE.get(bucket)
    if cached and (now - cached[0]) < _INDEX_CACHE_TTL:
        return cached[1]
    idx = read_availability_index(bucket)
    _INDEX_CACHE[bucket] = (now, idx)
    return idx


def clear_index_cache() -> None:
    """Clear the availability index cache."""
    _INDEX_CACHE.clear()


def _clamp_to_venue_starts(filtered: pd.DataFrame, start_date: str) -> str:
    """Clamp start date forward to the latest venue launch date."""
    effective_start = start_date
    if "venue" not in filtered.columns or filtered.empty:
        return effective_start
    venue_mapping = VenueMapping()
    for v in filtered["venue"].unique():
        vs = venue_mapping.get_venue_start_date(v)
        if not vs and ":" in v:
            vs = venue_mapping.get_venue_start_date(v.split(":")[0])
        if vs:
            effective_start = max(effective_start, vs)
    return effective_start


class DataStatusService:
    """
    Business logic service for data status operations.

    This service handles:
    - CLI wrapper for data status commands
    - Missing shards calculation
    - Data completeness validation
    - Cross-service status aggregation
    """

    def __init__(self, project_id: str | None = None):
        """Initialize data status service."""
        self.project_id = project_id or _pid

    def build_bucket_name(self, prefix: str, category: str) -> str:
        """Build a GCS bucket name: {prefix}-{category_lower}-{project_id}."""
        return f"{prefix}-{category.lower()}-{self.project_id}"

    def _build_cli_cmd(
        self,
        service: str,
        start_date: str,
        end_date: str,
        categories: list[str] | None,
        venues: list[str] | None,
        show_missing: bool,
        check_venues: bool,
        check_data_types: bool,
        check_feature_groups: bool,
        check_timeframes: bool,
        mode: str,
    ) -> list[str]:
        """Build the data-status CLI command list."""
        cmd = [
            sys.executable,
            "-m",
            "deployment_service",
            "data-status",
            "-s",
            service,
            "--start-date",
            start_date,
            "--end-date",
            end_date,
            "--output",
            "json",
            "--mode",
            mode,
        ]
        for cat in categories or []:
            cmd.extend(["-c", cat])
        for venue in venues or []:
            cmd.extend(["-v", venue])
        if show_missing:
            cmd.append("--show-missing")
        if check_venues:
            cmd.append("--check-venues")
        elif check_feature_groups:
            cmd.append("--check-feature-groups")
        elif check_timeframes:
            cmd.append("--check-timeframes")
        elif service in ["market-tick-data-handler", "market-data-processing-service"]:
            cmd.append("--fast")
        if check_data_types:
            cmd.append("--check-data-types")
        return cmd

    async def run_data_status_cli(
        self,
        service: str,
        start_date: str,
        end_date: str,
        categories: list[str] | None = None,
        venues: list[str] | None = None,
        show_missing: bool = False,
        check_venues: bool = False,
        check_data_types: bool = False,
        check_feature_groups: bool = False,
        check_timeframes: bool = False,
        mode: str = "batch",
    ) -> dict[str, object]:
        """
        Run data-status CLI command and return parsed JSON output.

        Returns parsed JSON output from CLI command.
        """
        cmd = self._build_cli_cmd(
            service,
            start_date,
            end_date,
            categories,
            venues,
            show_missing,
            check_venues,
            check_data_types,
            check_feature_groups,
            check_timeframes,
            mode,
        )
        logger.info("Running CLI: %s", " ".join(cmd))

        try:
            process = await asyncio.create_subprocess_exec(
                *cmd,
                stdout=asyncio.subprocess.PIPE,
                stderr=asyncio.subprocess.PIPE,
                cwd=None,
            )
            stdout, stderr = await process.communicate()

            if process.returncode != 0:
                error_msg = f"CLI command failed with code {process.returncode}: {stderr.decode()}"
                logger.error(error_msg)
                return {"error": error_msg, "stderr": stderr.decode()}

            try:
                result = cast(dict[str, object], json.loads(stdout.decode()))
                return result
            except json.JSONDecodeError as e:
                logger.error("Failed to parse CLI JSON output: %s", e)
                return {"error": f"Invalid JSON output: {e}", "raw_output": stdout.decode()}

        except (OSError, ValueError, RuntimeError) as e:
            logger.error("Error running CLI command: %s", e)
            return {"error": str(e)}

    def _tally_missing_venues(
        self,
        date_info: dict[str, object],
        missing_by_venue: dict[str, int],
        missing_by_category: dict[str, int],
    ) -> int:
        """Count missing venues in a date entry and update tallies in-place."""
        missing_count = 0
        venues_raw: object = date_info.get("venues")
        if not (venues_raw and isinstance(venues_raw, list)):
            return 0
        for venue_info_raw in cast(list[object], venues_raw):
            if not isinstance(venue_info_raw, dict):
                continue
            venue_info = cast(dict[str, object], venue_info_raw)
            venue_name_raw: object = venue_info.get("venue")
            venue_name = venue_name_raw if isinstance(venue_name_raw, str) else ""
            if venue_info.get("status") == "missing":
                missing_count += 1
                missing_by_venue[venue_name] = missing_by_venue.get(venue_name, 0) + 1
                cat_raw: object = venue_info.get("category", "unknown")
                category = cat_raw if isinstance(cat_raw, str) else "unknown"
                missing_by_category[category] = missing_by_category.get(category, 0) + 1
        return missing_count

    # ── Venue aliases (mirrors deployment-service ManifestReader._VENUE_ALIASES) ──
    _VENUE_ALIASES: ClassVar[dict[str, str]] = {
        "OKX": "OKX-SPOT",
        "COINBASE": "COINBASE-SPOT",
    }

    # ── Bucket resolution (mirrors deployment-service ManifestReader) ──
    _BUCKET_TEMPLATES: ClassVar[dict[str, str]] = {
        "instruments-service": "instruments-store-{cat}-{pid}",
        "corporate-actions": "instruments-store-{cat}-{pid}",
        "market-tick-data-service": "market-data-tick-{cat}-{pid}",
        "market-data-processing-service": "market-data-tick-{cat}-{pid}",
        "features-delta-one-service": "features-delta-one-{cat}-{pid}",
        "features-volatility-service": "features-volatility-{cat}-{pid}",
        "features-onchain-service": "features-onchain-{pid}",
        "features-sports-service": "features-sports-{cat}-{pid}",
    }

    async def calculate_missing_shards(
        self,
        service: str,
        start_date: str,
        end_date: str,
        categories: list[str] | None = None,
        venues: list[str] | None = None,
        mode: str = "batch",
    ) -> dict[str, object]:
        """Calculate missing shards by reading manifest indices directly.

        Uses read_availability_index (same as deployment-service CLI) instead
        of shelling out to the data-status CLI subprocess.
        Runs in a thread to avoid blocking the async event loop.
        """
        return await asyncio.to_thread(
            self._calculate_missing_shards_sync,
            service,
            start_date,
            end_date,
            categories,
            venues,
        )

    def _calculate_missing_shards_sync(
        self,
        service: str,
        start_date: str,
        end_date: str,
        categories: list[str] | None = None,
        venues: list[str] | None = None,
    ) -> dict[str, object]:
        """Synchronous implementation of missing shard calculation."""
        try:
            cat_list = categories or [str(c) for c in MarketCategory]
            missing_by_date: dict[str, int] = {}
            missing_by_category: dict[str, int] = {}
            total_missing = 0
            total_days_checked = 0

            for cat in cat_list:
                cat_result = self._scan_category_manifest(service, cat, start_date, end_date)
                if not cat_result:
                    continue
                for md in cat_result["missing"]:
                    missing_by_date[md] = missing_by_date.get(md, 0) + 1
                missing_by_category[cat] = len(cat_result["missing"])
                total_missing += len(cat_result["missing"])
                total_days_checked += cat_result["days_checked"]

            days_total = max(1, total_days_checked)
            days_complete = days_total - len(missing_by_date)
            completion = round(days_complete / days_total * 100, 2)

            return {
                "service": service,
                "date_range": {"start": start_date, "end": end_date},
                "total_missing": total_missing,
                "missing_by_date": missing_by_date,
                "missing_by_venue": {},
                "missing_by_category": missing_by_category,
                "summary": {
                    "total_days_checked": total_days_checked,
                    "days_with_missing": len(missing_by_date),
                    "venues_with_missing": 0,
                    "categories_with_missing": len(missing_by_category),
                    "completion_rate": completion,
                },
            }
        except Exception as e:
            logger.exception("Error calculating missing shards")
            return {"error": str(e)}

    def _scan_category_manifest(
        self,
        service: str,
        cat: str,
        start_date: str,
        end_date: str,
    ) -> dict[str, list[str] | int] | None:
        """Read manifest index for one category and return missing dates."""
        template = self._BUCKET_TEMPLATES.get(service)
        if not template:
            return None
        bucket = template.format(cat=cat.lower(), pid=self.project_id)

        try:
            index = _read_index_cached(bucket)
        except Exception:
            logger.debug("No manifest index in %s", bucket)
            return None

        if index.empty:
            return None

        mask = (index["date"] >= start_date) & (index["date"] <= end_date)
        if "service_name" in index.columns:
            mask = mask & (index["service_name"] == service)
        filtered = index.loc[mask].copy()

        # Fold bare venue aliases
        if "venue" in filtered.columns and not filtered.empty:
            filtered["venue"] = filtered["venue"].replace(self._VENUE_ALIASES)

        effective_start = _clamp_to_venue_starts(filtered, start_date)
        all_dates = pd.date_range(effective_start, end_date, freq="D")
        found_dates = set(filtered["date"].unique())
        missing = [
            d.strftime("%Y-%m-%d") for d in all_dates if d.strftime("%Y-%m-%d") not in found_dates
        ]
        if not missing:
            return None
        return {"missing": missing, "days_checked": len(all_dates)}

    def _calculate_completion_rate(self, data_status_result: dict[str, object]) -> float:
        """
        Calculate completion rate from data status result.

        Args:
            data_status_result: Result from data status CLI

        Returns:
            Completion rate as percentage (0.0-100.0)
        """
        if "dates" not in data_status_result:
            return 0.0

        total_checks = 0
        completed_checks = 0

        dates_raw: object = data_status_result.get("dates")
        if not isinstance(dates_raw, list):
            return 0.0

        for date_info_raw in cast(list[object], dates_raw):
            if not isinstance(date_info_raw, dict):
                continue
            date_info = cast(dict[str, object], date_info_raw)
            venues_raw: object = date_info.get("venues")
            if not isinstance(venues_raw, list):
                continue
            for venue_info_raw in cast(list[object], venues_raw):
                if not isinstance(venue_info_raw, dict):
                    continue
                venue_info = cast(dict[str, object], venue_info_raw)
                total_checks += 1
                if venue_info.get("status") != "missing":
                    completed_checks += 1

        if total_checks == 0:
            return 0.0

        return (completed_checks / total_checks) * 100.0

    async def get_coverage_summary(
        self,
        service: str = "instruments-service",
        categories: list[str] | None = None,
    ) -> dict[str, object]:
        """Return shard counts and latest-day instrument totals per category.

        Reads availability indices directly (same as calculate_missing_shards)
        and aggregates into the shape the deployment-ui expects.
        """
        return await asyncio.to_thread(self._get_coverage_summary_sync, service, categories)

    def _get_coverage_summary_sync(
        self,
        service: str,
        categories: list[str] | None = None,
    ) -> dict[str, object]:
        """Synchronous coverage summary implementation."""
        cat_list = categories or [str(c) for c in MarketCategory]
        result_categories: dict[str, object] = {}
        total_shards = 0
        total_instrument_rows = 0
        all_dates: set[str] = set()
        total_latest_day_instruments = 0

        for cat in cat_list:
            template = self._BUCKET_TEMPLATES.get(service)
            if not template:
                continue
            bucket = template.format(cat=cat.lower(), pid=self.project_id)
            try:
                index = _read_index_cached(bucket)
            except Exception:
                logger.debug("No manifest index in %s", bucket)
                continue

            if index.empty:
                continue

            # Fold bare venue aliases
            if "venue" in index.columns:
                index = index.copy()
                index["venue"] = index["venue"].replace(self._VENUE_ALIASES)

            shards = len(index)
            unique_dates = sorted(index["date"].unique()) if "date" in index.columns else []
            unique_venues_list = sorted(index["venue"].unique()) if "venue" in index.columns else []
            date_range: dict[str, str] | None = None
            if unique_dates:
                date_range = {"start": str(unique_dates[0]), "end": str(unique_dates[-1])}

            # Latest day instrument counts
            latest_day: str | None = str(unique_dates[-1]) if unique_dates else None
            latest_day_instruments: dict[str, int] = {}
            latest_day_total = 0
            if latest_day and "date" in index.columns:
                latest = index[index["date"] == latest_day]
                latest_day_total = len(latest)
                if "venue" in latest.columns:
                    for v in latest["venue"].unique():
                        latest_day_instruments[str(v)] = int((latest["venue"] == v).sum())

            instrument_rows = shards  # each row in the index is an instrument-date shard

            result_categories[cat] = {
                "total_shards": shards,
                "total_instrument_rows": instrument_rows,
                "unique_dates": len(unique_dates),
                "unique_venues": len(unique_venues_list),
                "date_range": date_range,
                "latest_day": latest_day,
                "latest_day_instruments": latest_day_instruments,
                "latest_day_total": latest_day_total,
            }

            total_shards += shards
            total_instrument_rows += instrument_rows
            all_dates.update(str(d) for d in unique_dates)
            total_latest_day_instruments += latest_day_total

        return {
            "service": service,
            "categories": result_categories,
            "totals": {
                "shards": total_shards,
                "instrument_rows": total_instrument_rows,
                "dates_across_categories": len(all_dates),
                "latest_day_instruments": total_latest_day_instruments,
            },
        }

    async def get_manifest_status(
        self,
        service: str,
        start_date: str,
        end_date: str,
        categories: list[str] | None = None,
    ) -> dict[str, object]:
        """Return data status from manifest indices in TurboDataStatusResponse shape."""
        return await asyncio.to_thread(
            self._get_manifest_status_sync, service, start_date, end_date, categories
        )

    def _get_manifest_status_sync(
        self,
        service: str,
        start_date: str,
        end_date: str,
        categories: list[str] | None = None,
    ) -> dict[str, object]:
        """Synchronous manifest status — returns TurboDataStatusResponse shape."""
        cat_list = categories or [str(c) for c in MarketCategory]
        all_dates_range = pd.date_range(start_date, end_date, freq="D")
        all_date_strs = [d.strftime("%Y-%m-%d") for d in all_dates_range]
        total_days = len(all_dates_range)
        result_categories: dict[str, object] = {}
        overall_found = 0
        overall_expected = 0

        venue_mapping = VenueMapping()

        for cat in cat_list:
            cat_result = self._build_manifest_category(
                service,
                cat,
                start_date,
                end_date,
                all_date_strs,
                total_days,
                venue_mapping,
            )
            result_categories[cat] = cat_result
            overall_found += cat_result["_venue_found"]
            overall_expected += cat_result["_venue_expected"]
            # Remove internal counters from output
            del cat_result["_venue_found"]
            del cat_result["_venue_expected"]

        overall_pct = round(overall_found / max(1, overall_expected) * 100, 2)

        return {
            "service": service,
            "date_range": {"start": start_date, "end": end_date, "days": total_days},
            "mode": "turbo",
            "sub_dimension": "venue",
            "overall_completion_pct": overall_pct,
            "overall_dates_found": overall_found,
            "overall_dates_expected": overall_expected,
            "categories": result_categories,
        }

    def _build_venue_breakdown(
        self,
        filtered: pd.DataFrame,
        start_date: str,
        end_date: str,
        venue_mapping: VenueMapping,
        cat_found: int,
        total_days: int,
    ) -> tuple[dict[str, object], int, int]:
        """Build per-venue stats from filtered index data."""
        if "venue" not in filtered.columns or filtered.empty:
            return {}, cat_found, total_days

        venues_dict: dict[str, object] = {}
        venue_found_total = 0
        venue_expected_total = 0
        for v in sorted(str(x) for x in filtered["venue"].unique()):
            v_dates = {str(d) for d in filtered[filtered["venue"] == v]["date"].unique()}
            vs = venue_mapping.get_venue_start_date(v)
            if not vs and ":" in v:
                vs = venue_mapping.get_venue_start_date(v.split(":")[0])
            if not vs and v_dates:
                vs = min(v_dates)
            eff_start = max(start_date, vs) if vs else start_date
            v_all_dates = [
                d.strftime("%Y-%m-%d") for d in pd.date_range(eff_start, end_date, freq="D")
            ]
            expected = len(v_all_dates)
            found = len(v_dates)
            v_missing = sorted(set(v_all_dates) - v_dates)
            venues_dict[v] = {
                "dates_found": found,
                "dates_expected": expected,
                "dates_expected_venue": expected,
                "dates_missing": len(v_missing),
                "missing_dates": v_missing,
                "completion_pct": round(found / max(1, expected) * 100, 2),
                "venue_start_date": vs,
            }
            venue_found_total += found
            venue_expected_total += expected
        return venues_dict, venue_found_total, venue_expected_total

    def _build_manifest_category(
        self,
        service: str,
        cat: str,
        start_date: str,
        end_date: str,
        all_date_strs: list[str],
        total_days: int,
        venue_mapping: VenueMapping,
    ) -> dict[str, object]:
        """Build a single category entry for manifest status."""
        template = self._BUCKET_TEMPLATES.get(service)
        empty: dict[str, object] = {
            "category": cat,
            "bucket": "",
            "prefixes_queried": 0,
            "dates_found": 0,
            "dates_expected": total_days,
            "dates_missing": total_days,
            "completion_pct": 0.0,
            "missing_dates": all_date_strs,
            "venues": {},
            "_venue_found": 0,
            "_venue_expected": total_days,
        }
        if not template:
            return empty

        bucket = template.format(cat=cat.lower(), pid=self.project_id)
        empty["bucket"] = bucket
        try:
            index = _read_index_cached(bucket)
        except Exception:
            logger.debug("No manifest index in %s", bucket)
            return empty

        if index.empty:
            return empty

        mask = (index["date"] >= start_date) & (index["date"] <= end_date)
        if "service_name" in index.columns:
            mask = mask & (index["service_name"] == service)
        filtered = index.loc[mask].copy()

        # Fold bare venue aliases (e.g. "OKX" → "OKX-SPOT", "COINBASE" → "COINBASE-SPOT")
        if "venue" in filtered.columns and not filtered.empty:
            filtered["venue"] = filtered["venue"].replace(self._VENUE_ALIASES)

        cat_found_dates = (
            {str(d) for d in filtered["date"].unique()} if not filtered.empty else set()
        )
        cat_missing = sorted(set(all_date_strs) - cat_found_dates)
        cat_found = len(cat_found_dates)

        # Per-venue breakdown
        venues_dict, venue_found_total, venue_expected_total = self._build_venue_breakdown(
            filtered,
            start_date,
            end_date,
            venue_mapping,
            cat_found,
            total_days,
        )

        # Use venue-weighted completion when venues exist
        if venues_dict:
            cat_pct = round(venue_found_total / max(1, venue_expected_total) * 100, 2)
        else:
            cat_pct = round(cat_found / max(1, total_days) * 100, 2)

        return {
            "category": cat,
            "bucket": bucket,
            "prefixes_queried": 0,
            "dates_found": cat_found,
            "dates_expected": total_days,
            "dates_missing": len(cat_missing),
            "completion_pct": cat_pct,
            "venue_weighted": bool(venues_dict),
            "venue_dates_found": venue_found_total,
            "venue_dates_expected": venue_expected_total,
            "missing_dates": cat_missing,
            "venues": venues_dict,
            "_venue_found": venue_found_total,
            "_venue_expected": venue_expected_total,
        }

    async def get_last_updated_info(
        self,
        service: str,
        categories: list[str] | None = None,
    ) -> dict[str, object]:
        """
        Get last updated information for a service.

        Args:
            service: Service name to check
            categories: Optional list of categories to filter

        Returns:
            Dictionary containing last updated information
        """
        # Service to bucket prefix mapping
        bucket_prefixes = {
            "market-tick-data-handler": "market-data",
            "market-data-processing-service": "processed-market-data",
            "instruments-service": "instruments",
            "features-equity-service": "features-equity",
            "features-derivatives-service": "features-derivatives",
            "features-defi-service": "features-defi",
        }

        prefix = bucket_prefixes.get(service)
        if not prefix:
            return {"error": f"Unknown service: {service}"}

        # Default categories if none specified
        if not categories:
            categories = [cat.value.lower() for cat in MarketCategory]

        categories_info: dict[str, object] = {}
        last_updated_info: dict[str, object] = {
            "service": service,
            "categories": categories_info,
            "overall_last_updated": None,
        }

        for category in categories:
            try:
                bucket_name = self.build_bucket_name(prefix, category)

                # Check if bucket has any recent activity
                # Use the most recent object in the bucket as proxy
                objects = list_objects(bucket_name, "", max_results=10)

                if objects:
                    # Get the most recently created object
                    # This is a simplified approach - in production you might want
                    # to check specific paths or use bucket metadata
                    categories_info[category] = {
                        "status": "active",
                        "object_count": len(objects),
                        "sample_paths": objects[:5],  # First 5 as examples
                    }
                else:
                    categories_info[category] = {
                        "status": "empty",
                        "object_count": 0,
                    }

            except (OSError, ValueError, RuntimeError) as e:
                logger.debug("Error checking category %s: %s", category, e)
                categories_info[category] = {
                    "status": "error",
                    "error": str(e),
                }

        return last_updated_info

    async def validate_data_completeness(
        self,
        service: str,
        date: str,
        categories: list[str] | None = None,
        venues: list[str] | None = None,
    ) -> dict[str, object]:
        """
        Validate data completeness for a specific date.

        Args:
            service: Service name to validate
            date: Date in YYYY-MM-DD format
            categories: Optional list of categories to check
            venues: Optional list of venues to check

        Returns:
            Validation result with completeness details
        """
        # Get data status for single day
        result = await self.run_data_status_cli(
            service=service,
            start_date=date,
            end_date=date,
            categories=categories,
            venues=venues,
            show_missing=True,
        )

        if "error" in result:
            return result

        # Analyze completeness
        missing_venues: list[str] = []
        validation_errors: list[object] = []
        is_complete = True
        total_venues = 0
        completed_venues = 0

        dates_val: object = result.get("dates")
        if dates_val and isinstance(dates_val, list):
            dates_list = cast(list[object], dates_val)
            if dates_list and isinstance(dates_list[0], dict):
                date_data = cast(dict[str, object], dates_list[0])  # Single date

                venues_val: object = date_data.get("venues")
                if venues_val and isinstance(venues_val, list):
                    venues_list = cast(list[object], venues_val)
                    total_venues = len(venues_list)

                    for venue_info_raw in venues_list:
                        if not isinstance(venue_info_raw, dict):
                            continue
                        venue_info = cast(dict[str, object], venue_info_raw)
                        vname_raw: object = venue_info.get("venue", "unknown")
                        venue_name = vname_raw if isinstance(vname_raw, str) else "unknown"
                        status_raw: object = venue_info.get("status")
                        status = status_raw if isinstance(status_raw, str) else ""

                        if status == "missing":
                            is_complete = False
                            missing_venues.append(venue_name)
                        elif status == "error":
                            err_raw: object = venue_info.get("error", "Unknown error")
                            validation_errors.append(
                                {
                                    "venue": venue_name,
                                    "error": err_raw
                                    if isinstance(err_raw, str)
                                    else "Unknown error",
                                }
                            )
                        else:
                            completed_venues += 1

        completion_rate = (completed_venues / total_venues * 100) if total_venues > 0 else 0.0

        validation: dict[str, object] = {
            "service": service,
            "date": date,
            "is_complete": is_complete,
            "total_venues": total_venues,
            "completed_venues": completed_venues,
            "missing_venues": missing_venues,
            "errors": validation_errors,
            "completion_rate": completion_rate,
        }

        return validation

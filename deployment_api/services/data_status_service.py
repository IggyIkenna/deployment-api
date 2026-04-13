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
from unified_api_contracts import (
    VenueMapping,
    get_expected_data_types_for_venue,
    get_venue_data_type_start_date,
)
from unified_api_contracts.internal import MarketCategory
from unified_api_contracts.sports import (
    get_all_prediction_league_ids,
    get_league_fixture_calendar,
)
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

    # Categories whose bucket name doesn't follow the template pattern
    _BUCKET_CATEGORY_OVERRIDES: ClassVar[dict[tuple[str, str], str]] = {
        ("market-tick-data-service", "gas-fees"): "gas-fees-{pid}",
        ("market-tick-data-service", "evm-defi"): "evm-defi-{pid}",
        ("market-tick-data-service", "solana-defi"): "solana-defi-{pid}",
        ("market-tick-data-service", "dex-pools"): "dex-pools-{pid}",
        ("market-tick-data-service", "dex-swaps"): "dex-swaps-{pid}",
        ("market-tick-data-service", "lending-indices"): "lending-indices-{pid}",
        ("market-tick-data-service", "liquidations"): "liquidations-{pid}",
        ("market-tick-data-service", "lst-rates"): "lst-rates-{pid}",
        ("market-tick-data-service", "oracle-prices"): "oracle-prices-{pid}",
        ("market-tick-data-service", "perp-funding"): "perp-funding-{pid}",
    }

    # DeFi sub-dimension bucket keys for MTDS — merged into DEFI category
    _MTDS_DEFI_SUB_DIMENSIONS: ClassVar[list[str]] = [
        "gas-fees",
        "evm-defi",
        "solana-defi",
        "dex-pools",
        "dex-swaps",
        "lending-indices",
        "liquidations",
        "lst-rates",
        "oracle-prices",
        "perp-funding",
    ]

    def _read_defi_merged_index(self, service: str, cat: str) -> pd.DataFrame:
        """Read availability index, merging sub-dimension buckets for MTDS DEFI.

        For market-tick-data-service + DEFI category, reads the main DEFI bucket
        AND all sub-dimension buckets (gas-fees, dex-swaps, etc.), concatenating
        them so venues from sub-dimensions appear under DEFI in the UI.

        Each row is tagged with ``_defi_source`` so the category builder can
        produce a per-sub-dimension breakdown.
        """
        template = self._BUCKET_TEMPLATES.get(service)
        if not template:
            return pd.DataFrame()

        override = self._BUCKET_CATEGORY_OVERRIDES.get((service, cat.lower()))
        main_bucket = (
            override.format(pid=self.project_id)
            if override
            else template.format(cat=cat.lower(), pid=self.project_id)
        )

        frames: list[pd.DataFrame] = []
        try:
            idx = _read_index_cached(main_bucket)
            if not idx.empty:
                idx = idx.copy()
                idx["_defi_source"] = ""
                frames.append(idx)
        except Exception:
            logger.debug("No manifest index in %s", main_bucket)

        # Merge sub-dimension buckets for MTDS DEFI
        if service == "market-tick-data-service" and cat.lower() == "defi":
            for sub_dim in self._MTDS_DEFI_SUB_DIMENSIONS:
                sub_override = self._BUCKET_CATEGORY_OVERRIDES.get((service, sub_dim))
                if not sub_override:
                    continue
                sub_bucket = sub_override.format(pid=self.project_id)
                try:
                    sub_idx = _read_index_cached(sub_bucket)
                    if not sub_idx.empty:
                        sub_idx = sub_idx.copy()
                        sub_idx["_defi_source"] = sub_dim
                        frames.append(sub_idx)
                except Exception:
                    logger.debug("No sub-dimension index in %s", sub_bucket)

        if not frames:
            return pd.DataFrame()
        return pd.concat(frames, ignore_index=True)

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
        index = self._read_defi_merged_index(service, cat)
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
            index = self._read_defi_merged_index(service, cat)
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

    # Sports reference venues — fixture-dependent, not every-calendar-day expected.
    # Expected dates = dates where ANY sports reference entity has data (fixture calendar).
    _SPORTS_REFERENCE_PREFIXES: ClassVar[tuple[str, ...]] = (
        "FOOTYSTATS_",
        "TRANSFERMARKT_",
        "UNDERSTAT_",
        "SFI_",
        "API_FOOTBALL_",
    )

    def _is_sports_reference_venue(self, venue: str) -> bool:
        """Check if a venue is a sports reference entity (fixture-dependent)."""
        return venue.startswith(self._SPORTS_REFERENCE_PREFIXES)

    # ── Reference-data-driven expected dates ──────────────────────────────

    # Cache for instruments-service reference data
    _REF_DATA_CACHE: ClassVar[dict[str, tuple[float, dict[str, set[str]]]]] = {}
    _REF_DATA_CACHE_TTL = 300  # 5 minutes

    def _get_reference_expected_dates(
        self,
        category: str,
        start_date: str,
        end_date: str,
    ) -> dict[str, set[str]]:
        """Read instruments-service availability index to get per-venue expected dates.

        Returns {venue: {date_str, ...}} — the set of dates where instruments-service
        has reference data for that venue.  Market data services should only be
        expected to have data on dates where instruments actually exist.
        """
        cache_key = f"{category}:{start_date}:{end_date}"
        now = time.monotonic()
        cached = self._REF_DATA_CACHE.get(cache_key)
        if cached and (now - cached[0]) < self._REF_DATA_CACHE_TTL:
            return cached[1]

        inst_template = self._BUCKET_TEMPLATES.get("instruments-service", "")
        bucket = inst_template.format(cat=category.lower(), pid=self.project_id)
        result: dict[str, set[str]] = {}
        try:
            idx = _read_index_cached(bucket)
            if idx.empty:
                return result
            mask = (idx["date"] >= start_date) & (idx["date"] <= end_date)
            filtered = idx.loc[mask]
            if "venue" in filtered.columns:
                for v in filtered["venue"].unique():
                    v_str = str(v)
                    v_dates = {
                        str(d) for d in filtered.loc[filtered["venue"] == v, "date"].unique()
                    }
                    result[v_str] = v_dates
        except Exception:
            logger.debug("No instruments index for %s", bucket)

        self._REF_DATA_CACHE[cache_key] = (now, result)
        return result

    # Services whose expected-date denominator comes from instruments-service
    # reference data rather than calendar trading days.
    _REFERENCE_DRIVEN_SERVICES: ClassVar[frozenset[str]] = frozenset(
        {
            "market-tick-data-service",
            "market-data-processing-service",
            "features-delta-one-service",
            "features-volatility-service",
            "features-onchain-service",
            "features-sports-service",
            "features-calendar-service",
            "features-multi-timeframe-service",
            "features-cross-instrument-service",
            "features-commodity-service",
        }
    )

    def _build_venue_breakdown(
        self,
        filtered: pd.DataFrame,
        start_date: str,
        end_date: str,
        venue_mapping: VenueMapping,
        cat_found: int,
        total_days: int,
        service: str = "",
        category: str = "",
    ) -> tuple[dict[str, object], int, int]:
        """Build per-venue stats from filtered index data.

        Includes data_type sub-dimension for services that write per-data-type
        manifest entries (e.g. market-tick-data-service).

        **Reference-data-driven denominator**: For market-data and feature
        services, the expected-date set comes from instruments-service (i.e.
        "dates where reference data exists for this venue"), NOT from calendar
        trading days.  This means coverage = "missing market data given
        available instruments/fixtures".

        For sports reference venues the denominator is the fixture calendar.
        """
        if "venue" not in filtered.columns or filtered.empty:
            return {}, cat_found, total_days

        has_data_type = (
            "data_type" in filtered.columns
            and not filtered["data_type"].isna().all()
            and (filtered["data_type"].str.len() > 0).any()
        )

        # Reference-data-driven expected dates from instruments-service
        use_ref_denominator = service in self._REFERENCE_DRIVEN_SERVICES and category
        ref_dates: dict[str, set[str]] = {}
        if use_ref_denominator:
            ref_dates = self._get_reference_expected_dates(category, start_date, end_date)

        # Build fixture calendar — union of dates across all sports reference
        # venues in this category.  Used as the expected-date set for each
        # individual sports reference venue instead of all calendar days.
        all_venues = [str(x) for x in filtered["venue"].unique()]
        sports_ref_venues = [v for v in all_venues if self._is_sports_reference_venue(v)]
        fixture_calendar: set[str] | None = None
        if sports_ref_venues:
            sports_mask = filtered["venue"].isin(sports_ref_venues)
            fixture_calendar = {str(d) for d in filtered.loc[sports_mask, "date"].unique()}

        venues_dict: dict[str, object] = {}
        venue_found_total = 0
        venue_expected_total = 0
        for v in sorted(all_venues):
            v_mask = filtered["venue"] == v
            v_df = filtered[v_mask]
            v_dates_all = {str(d) for d in v_df["date"].unique()}
            vs = venue_mapping.get_venue_start_date(v)
            if not vs and ":" in v:
                vs = venue_mapping.get_venue_start_date(v.split(":")[0])
            if not vs and v_dates_all:
                vs = min(v_dates_all)
            eff_start = max(start_date, vs) if vs else start_date

            v_all_dates = self._resolve_expected_dates(
                v,
                eff_start,
                end_date,
                fixture_calendar,
                ref_dates,
                venue_mapping,
            )
            venue_entry = self._build_single_venue_entry(
                v_df,
                v,
                vs,
                eff_start,
                end_date,
                v_dates_all,
                v_all_dates,
                has_data_type,
                venue_mapping,
            )

            venues_dict[v] = venue_entry
            venue_found_total += int(venue_entry["dates_found"])
            venue_expected_total += int(venue_entry["dates_expected"])
        return venues_dict, venue_found_total, venue_expected_total

    def _resolve_expected_dates(
        self,
        venue: str,
        eff_start: str,
        end_date: str,
        fixture_calendar: set[str] | None,
        ref_dates: dict[str, set[str]],
        venue_mapping: VenueMapping,
    ) -> set[str]:
        """Resolve the expected-date set for a venue.

        Priority:
        1. Sports reference venues → fixture calendar
        2. Reference-driven services → instruments-service dates
        3. Fallback → calendar trading days
        """
        if self._is_sports_reference_venue(venue) and fixture_calendar is not None:
            return {d for d in fixture_calendar if d >= eff_start}
        if venue in ref_dates:
            return {d for d in ref_dates[venue] if d >= eff_start}
        return set(venue_mapping.get_expected_trading_dates(venue, eff_start, end_date))

    def _build_single_venue_entry(
        self,
        v_df: pd.DataFrame,
        venue: str,
        venue_start: str | None,
        eff_start: str,
        end_date: str,
        v_dates_all: set[str],
        v_all_dates: set[str],
        has_data_type: bool,
        venue_mapping: VenueMapping,
    ) -> dict[str, object]:
        """Build stats dict for a single venue."""
        v_dates = v_dates_all & v_all_dates
        expected = len(v_all_dates)
        found = len(v_dates)
        v_missing = sorted(v_all_dates - v_dates)
        v_found_sorted = sorted(v_dates)

        venue_entry: dict[str, object] = {
            "dates_found": found,
            "dates_expected": expected,
            "dates_expected_venue": expected,
            "dates_missing": len(v_missing),
            "missing_dates": v_missing,
            "dates_found_list": v_found_sorted,
            "dates_missing_list": v_missing,
            "completion_pct": round(found / max(1, expected) * 100, 2),
            "venue_start_date": venue_start,
        }

        if has_data_type:
            dt_breakdown = self._build_data_type_breakdown(
                v_df,
                venue,
                eff_start,
                end_date,
                venue_mapping,
            )
            if dt_breakdown:
                venue_entry["data_types"] = dt_breakdown

        league_breakdown = self._build_league_breakdown(v_df, eff_start, end_date)
        if league_breakdown:
            venue_entry["leagues"] = league_breakdown

        return venue_entry

    def _build_data_type_breakdown(
        self,
        venue_df: pd.DataFrame,
        venue: str,
        start_date: str,
        end_date: str,
        venue_mapping: VenueMapping,
    ) -> dict[str, object]:
        """Build per-data-type stats for a single venue.

        Uses UAC get_expected_data_types_for_venue() to know which data types
        should exist, and get_venue_data_type_start_date() for per-data-type ranges.
        """
        if "data_type" not in venue_df.columns:
            return {}

        # Get expected data types from UAC
        expected_dts = get_expected_data_types_for_venue(venue)
        # Also include data types actually present in the index (may have extras)
        present_dts = {str(dt) for dt in venue_df["data_type"].unique() if dt and str(dt).strip()}
        all_dts = sorted(set(expected_dts) | present_dts)

        if not all_dts:
            return {}

        dt_dict: dict[str, object] = {}
        for dt in all_dts:
            dt_df = venue_df[venue_df["data_type"] == dt]
            dt_dates = {str(d) for d in dt_df["date"].unique()} if not dt_df.empty else set()

            # Per-data-type start date from UAC
            dt_start = get_venue_data_type_start_date(venue, dt)
            dt_eff_start = max(start_date, dt_start) if dt_start else start_date
            dt_expected_list = venue_mapping.get_expected_trading_dates(
                venue,
                dt_eff_start,
                end_date,
            )
            dt_expected = set(dt_expected_list)
            dt_found = dt_dates & dt_expected
            dt_missing_dates = sorted(dt_expected - dt_found)

            dt_dict[dt] = {
                "dates_found": len(dt_found),
                "dates_expected": len(dt_expected),
                "dates_missing": len(dt_missing_dates),
                "completion_pct": round(
                    len(dt_found) / max(1, len(dt_expected)) * 100,
                    2,
                ),
                "start_date": dt_start,
                "is_expected": dt in expected_dts,
            }
        return dt_dict

    def _build_league_breakdown(
        self,
        venue_df: pd.DataFrame,
        start_date: str,
        end_date: str,
    ) -> dict[str, object]:
        """Build per-league stats for a sports venue.

        Uses UAC league fixture calendar as the per-league denominator.
        Leagues in UAC's prediction registry but absent from manifest
        appear at 0%.
        """
        if "league_id" not in venue_df.columns:
            return {}

        leagues_in_data = {lid for lid in venue_df["league_id"].unique() if lid}

        # Include all prediction leagues so newly-added ones show 0%
        all_league_ids = set(get_all_prediction_league_ids())
        all_leagues = sorted(leagues_in_data | all_league_ids)

        if not all_leagues:
            return {}

        league_dict: dict[str, object] = {}
        for lid in all_leagues:
            expected_dates = get_league_fixture_calendar(lid, start_date, end_date)
            expected_count = max(1, len(expected_dates)) if expected_dates else 1

            if lid in leagues_in_data:
                l_df = venue_df[venue_df["league_id"] == lid]
                found_dates = {str(d) for d in l_df["date"].unique()}
                found_count = len(found_dates)
                missing_dates = sorted(d for d in expected_dates if d not in found_dates)
            else:
                found_count = 0
                found_dates = set()
                missing_dates = sorted(expected_dates) if expected_dates else []

            league_dict[lid] = {
                "dates_found": found_count,
                "dates_expected": expected_count,
                "dates_missing": len(missing_dates),
                "missing_dates": missing_dates,
                "dates_found_list": sorted(found_dates),
                "completion_pct": round(
                    found_count / max(1, expected_count) * 100,
                    2,
                ),
            }
        return league_dict

    def _build_defi_sub_dimension_breakdown(
        self,
        filtered: pd.DataFrame,
        start_date: str,
        end_date: str,
    ) -> dict[str, object]:
        """Build per-sub-dimension stats for DEFI category.

        Groups rows by ``_defi_source`` (gas-fees, dex-pools, etc.) and produces
        per-sub-dimension stats with venues, found/expected, and completion_pct.
        The "main" DEFI bucket rows (source="") are shown as "defi-core".
        """
        if "_defi_source" not in filtered.columns:
            return {}

        # All known sub-dims plus any that appeared in the data
        all_sources = set(self._MTDS_DEFI_SUB_DIMENSIONS)
        data_sources = {str(s) for s in filtered["_defi_source"].unique() if s}
        all_sources |= data_sources

        sub_dim_dict: dict[str, object] = {}
        for src in sorted(all_sources):
            src_mask = filtered["_defi_source"] == src
            src_df = filtered[src_mask]
            src_dates = {str(d) for d in src_df["date"].unique()} if not src_df.empty else set()
            src_venues = sorted(src_df["venue"].unique()) if not src_df.empty else []

            # Expected = dates where ANY venue in this sub-dim has data
            all_dates_range = pd.date_range(start_date, end_date, freq="D")
            expected_dates = {d.strftime("%Y-%m-%d") for d in all_dates_range}
            found_dates = src_dates & expected_dates
            missing_dates = sorted(expected_dates - found_dates)

            sub_dim_dict[src] = {
                "dates_found": len(found_dates),
                "dates_expected": len(expected_dates),
                "dates_missing": len(missing_dates),
                "completion_pct": round(
                    len(found_dates) / max(1, len(expected_dates)) * 100,
                    2,
                ),
                "venues": src_venues,
                "venue_count": len(src_venues),
            }

        # Include "defi-core" for the main bucket rows
        core_mask = filtered["_defi_source"] == ""
        if core_mask.any():
            core_df = filtered[core_mask]
            core_dates = {str(d) for d in core_df["date"].unique()}
            core_venues = sorted(core_df["venue"].unique())
            all_dates_range = pd.date_range(start_date, end_date, freq="D")
            expected_dates = {d.strftime("%Y-%m-%d") for d in all_dates_range}
            found_dates = core_dates & expected_dates

            sub_dim_dict["defi-core"] = {
                "dates_found": len(found_dates),
                "dates_expected": len(expected_dates),
                "dates_missing": len(expected_dates - found_dates),
                "completion_pct": round(
                    len(found_dates) / max(1, len(expected_dates)) * 100,
                    2,
                ),
                "venues": core_venues,
                "venue_count": len(core_venues),
            }

        return sub_dim_dict

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

        # Resolve the main bucket name (for display in the response)
        override = self._BUCKET_CATEGORY_OVERRIDES.get((service, cat.lower()))
        bucket = (
            override.format(pid=self.project_id)
            if override
            else template.format(cat=cat.lower(), pid=self.project_id)
        )

        index = self._read_defi_merged_index(service, cat)
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

        # Per-venue breakdown (includes data_type sub-dimension for multi-data-type services)
        venues_dict, venue_found_total, venue_expected_total = self._build_venue_breakdown(
            filtered,
            start_date,
            end_date,
            venue_mapping,
            cat_found,
            total_days,
            service=service,
            category=cat,
        )

        # Use venue-weighted completion when venues exist
        if venues_dict:
            cat_pct = round(venue_found_total / max(1, venue_expected_total) * 100, 2)
        else:
            cat_pct = round(cat_found / max(1, total_days) * 100, 2)

        # DeFi sub-dimension breakdown (gas-fees, dex-pools, lending-indices, etc.)
        defi_sub_dims: dict[str, object] = {}
        if (
            "_defi_source" in filtered.columns
            and service == "market-tick-data-service"
            and cat.lower() == "defi"
        ):
            defi_sub_dims = self._build_defi_sub_dimension_breakdown(
                filtered,
                start_date,
                end_date,
            )

        cat_found_sorted = sorted(cat_found_dates)
        result: dict[str, object] = {
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
            "dates_found_list": cat_found_sorted,
            "dates_missing_list": cat_missing,
            "venues": venues_dict,
            "_venue_found": venue_found_total,
            "_venue_expected": venue_expected_total,
        }
        if defi_sub_dims:
            result["defi_sub_dimensions"] = defi_sub_dims
        return result

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

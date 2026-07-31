"""Missing-shards calculation against the availability manifest.

Split out of the 6,663-line ``data_status_service.py`` god-module
(codex ratchet plan 2026-06-10). The facade module re-exports every
public + legacy-underscore name, so callers keep importing from
``deployment_api.services.data_status_service``.
"""

import asyncio
import logging
from typing import cast

import pandas as pd
from unified_api_contracts.internal import MarketCategory

logger = logging.getLogger(__name__)

from deployment_api.services.data_status.coverage import CoverageStatusMixin
from deployment_api.services.data_status.frame_utils import clamp_to_venue_starts


class MissingShardsMixin(CoverageStatusMixin):
    """calculate_missing_shards + manifest scanning / completion rate.

    The data_status mixins form a single linear inheritance chain
    (cli -> defi -> sports -> breakdowns_domain -> breakdowns_core ->
    venue_resolution -> coverage -> missing_shards -> manifest_category_builder ->
    manifest) so that
    every cross-group ``self._method`` reference resolves statically
    under basedpyright strict. ``DataStatusService`` composes the top of
    the chain and is the ONLY public entry point — import it from
    ``deployment_api.services.data_status_service`` (the facade).
    """

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

    async def calculate_missing_shards(
        self,
        service: str,
        start_date: str,
        end_date: str,
        asset_groups: list[str] | None = None,
        venues: list[str] | None = None,
        mode: str = "batch",
        cloud: str = "gcp",
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
            asset_groups,
            venues,
            cloud,
        )

    def _calculate_missing_shards_sync(
        self,
        service: str,
        start_date: str,
        end_date: str,
        asset_groups: list[str] | None = None,
        venues: list[str] | None = None,
        cloud: str = "gcp",
    ) -> dict[str, object]:
        """Synchronous implementation of missing shard calculation."""
        try:
            cat_list = asset_groups or [str(c) for c in MarketCategory]
            missing_by_date: dict[str, int] = {}
            missing_by_category: dict[str, int] = {}
            total_missing = 0
            total_days_checked = 0

            for cat in cat_list:
                cat_result = self._scan_category_manifest(service, cat, start_date, end_date, cloud=cloud)
                if not cat_result:
                    continue
                for md in cat_result["missing"]:  # pyright: ignore[reportGeneralTypeIssues,reportUnknownVariableType]
                    missing_by_date[md] = missing_by_date.get(md, 0) + 1  # pyright: ignore[reportUnknownArgumentType]
                missing_by_category[cat] = len(cat_result["missing"])  # pyright: ignore[reportArgumentType]
                total_missing += len(cat_result["missing"])  # pyright: ignore[reportArgumentType]
                total_days_checked += cat_result["days_checked"]  # pyright: ignore[reportOperatorIssue,reportUnknownVariableType]

            days_total = max(1, total_days_checked)  # pyright: ignore[reportUnknownArgumentType,reportUnknownVariableType]
            days_complete = days_total - len(missing_by_date)  # pyright: ignore[reportUnknownVariableType]
            completion = round(days_complete / days_total * 100, 2)  # pyright: ignore[reportUnknownArgumentType]

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
        cloud: str = "gcp",
    ) -> dict[str, list[str] | int] | None:
        """Read manifest index for one category and return missing dates."""
        # Skip categories that don't apply to this service
        allowed = self._SERVICE_CATEGORY_RESTRICTIONS.get(service)
        if allowed and cat.upper() not in allowed:
            return None

        index = self._read_defi_merged_index(service, cat, cloud=cloud)
        if index.empty:
            return None

        mask = (index["date"] >= start_date) & (index["date"] <= end_date)
        if "service_name" in index.columns:
            mask = mask & (index["service_name"] == service)
        filtered = index.loc[mask].copy()

        # Fold bare venue aliases
        if "venue" in filtered.columns and not filtered.empty:
            filtered["venue"] = filtered["venue"].replace(self._VENUE_ALIASES)

        effective_start = clamp_to_venue_starts(filtered, start_date)
        all_dates = pd.date_range(effective_start, end_date, freq="D")
        found_dates = set(filtered["date"].unique())  # pyright: ignore[reportAny]
        missing = [d.strftime("%Y-%m-%d") for d in all_dates if d.strftime("%Y-%m-%d") not in found_dates]
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

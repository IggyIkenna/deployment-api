"""
Execution Services Data Status

Functions for checking execution services data status and missing shards.
"""

import asyncio
import logging
import re
from collections import defaultdict
from datetime import UTC, datetime, timedelta
from typing import cast

logger = logging.getLogger(__name__)


async def get_execution_service_data_status(
    config_path: str,
    start_date: str | None = None,
    end_date: str | None = None,
) -> dict[str, object]:
    """
    Get execution-service data status by checking configs vs results.

    Turbo-compatible: Uses fast GCS directory listing.
    Caching: Results cached for 60s (configurable via DATA_STATUS_CACHE_TTL_SECONDS).

    Logic:
    - Configs: configs/{version}/{strategy}/{mode}/{timeframe}/{config_file}.json
    - Results: results/{any_date}/{strategy}_{mode}_{timeframe}_{version}/...

    For each config, checks if corresponding results directory exists.
    Groups hierarchically: strategy -> mode -> timeframe -> configs

    This enables diagnostic drilling:
    - Which strategies are failing?
    - Within a strategy, which modes (SCE vs HUF)?
    - Within a mode, which timeframes (5M vs 15M)?
    - Finally, which specific config files (algo params) are missing?

    Args:
        config_path: Cloud path to configs (e.g., gs://execution-store.../configs/V1)
        start_date: Optional start date filter (YYYY-MM-DD) for results check
        end_date: Optional end date filter (YYYY-MM-DD) for results check
    """
    from deployment_api.utils.data_status_cache import (
        get_exec_cached_result,
        set_exec_cached_result,
    )
    from deployment_api.utils.storage_facade import list_objects, list_prefixes

    # Check cache first
    cached = get_exec_cached_result(config_path, start_date, end_date)
    if cached is not None:
        return cached

    def _get_status_sync():
        try:
            # Parse config_path to get bucket and prefix
            if not config_path.startswith("gs://"):
                return {"error": "config_path must start with gs://"}

            path_parts = config_path[5:].split("/", 1)
            bucket_name = path_parts[0]
            config_prefix = path_parts[1] if len(path_parts) > 1 else ""

            # Ensure prefix ends with /
            if config_prefix and not config_prefix.endswith("/"):
                config_prefix += "/"

            # Extract version from config path (e.g., "configs/V1/" -> "V1")
            version_match = re.search(r"/([Vv]\d+)/?$", config_prefix)
            version = version_match.group(1) if version_match else "V1"

            # Step 1: List all configs under the config path (storage facade)
            configs = []
            config_objs = list_objects(bucket_name, config_prefix, max_results=10000)

            for obj in config_objs:
                if obj.name.endswith(".json"):
                    # Parse path: configs/V1/{strategy}/{mode}/{timeframe}/{config}.json
                    rel_path = obj.name[len(config_prefix) :] if obj.name.startswith(config_prefix) else obj.name
                    parts = rel_path.split("/")

                    if len(parts) >= 4:
                        strategy = parts[0]
                        mode = parts[1]
                        timeframe = parts[2]
                        config_file = parts[3] if len(parts) > 3 else parts[-1]

                        # Extract algo name from config file (e.g., ADAPTIVE_TWAP from ADAPTIVE_TWAP_horizon_secs120_...)
                        algo_match = re.match(
                            r"^([A-Z_]+?)_(?:horizon|profile|display|clip|urgency|num_|participation|lambda|sigma|passive)",
                            config_file,
                        )
                        algo_name = algo_match.group(1) if algo_match else config_file.split("_")[0]

                        # Build expected result strategy_id: {strategy}_{mode}_{timeframe}_{version}
                        result_strategy_id = f"{strategy}_{mode}_{timeframe}_{version}"

                        configs.append(
                            {
                                "path": obj.name,
                                "strategy": strategy,
                                "mode": mode,
                                "timeframe": timeframe,
                                "config_file": config_file,
                                "algo_name": algo_name,
                                "result_strategy_id": result_strategy_id,
                            }
                        )

            logger.info("Found %s configs under %s", len(configs), config_path)

            # Step 2: List all result strategy_ids from results/
            # Structure: results/{date}/{strategy_id}/...
            # We use delimiter to get unique strategy_ids across all dates

            results_prefix = "results/"
            existing_result_strategy_ids = set()
            result_dates_by_strategy = defaultdict(set)

            # List all date directories under results/ (storage facade)
            date_prefixes = list_prefixes(bucket_name, results_prefix)

            # Parse date range filter
            filter_start = datetime.strptime(start_date, "%Y-%m-%d").replace(tzinfo=UTC).date() if start_date else None
            filter_end = datetime.strptime(end_date, "%Y-%m-%d").replace(tzinfo=UTC).date() if end_date else None

            for date_prefix in date_prefixes:
                # Extract date from prefix: results/2023-05-23/
                date_match = re.search(r"results/(\d{4}-\d{2}-\d{2})/", date_prefix)
                if date_match:
                    date_str = date_match.group(1)
                    result_date = datetime.strptime(date_str, "%Y-%m-%d").replace(tzinfo=UTC).date()

                    # Apply date filter if specified
                    if filter_start and result_date < filter_start:
                        continue
                    if filter_end and result_date > filter_end:
                        continue

                    # List strategy_ids under this date (storage facade)
                    strategy_prefixes = list_prefixes(bucket_name, date_prefix)

                    for strategy_prefix in strategy_prefixes:
                        # Extract strategy_id: results/2023-05-23/CEFI_BTC_momentum-macd_SCE_5M_V1/
                        strategy_match = re.search(r"results/\d{4}-\d{2}-\d{2}/([^/]+)/", strategy_prefix)
                        if strategy_match:
                            strategy_id = strategy_match.group(1)
                            existing_result_strategy_ids.add(strategy_id)
                            result_dates_by_strategy[strategy_id].add(date_str)

            logger.info("Found %s unique result strategy_ids", len(existing_result_strategy_ids))

            # Step 3: Build hierarchical status: strategy -> mode -> timeframe -> configs
            # This enables drilling down to diagnose issues

            hierarchy = defaultdict(lambda: defaultdict(lambda: defaultdict(list)))

            for config in configs:
                strategy = config["strategy"]
                mode = config["mode"]
                timeframe = config["timeframe"]
                result_strategy_id = config["result_strategy_id"]

                has_results = result_strategy_id in existing_result_strategy_ids
                result_dates = sorted(result_dates_by_strategy.get(result_strategy_id, []))

                hierarchy[strategy][mode][timeframe].append(
                    {
                        "config_file": config["config_file"],
                        "algo_name": config["algo_name"],
                        "result_strategy_id": result_strategy_id,
                        "has_results": has_results,
                        "result_dates": result_dates,
                    }
                )

            # Step 4: Build response with hierarchical breakdown and summaries
            strategies = []
            total_configs = 0
            total_with_results = 0

            # Also build flat breakdown by attribute for filtering
            breakdown_by_mode = defaultdict(lambda: {"total": 0, "with_results": 0, "missing": []})
            breakdown_by_timeframe = defaultdict(lambda: {"total": 0, "with_results": 0, "missing": []})
            breakdown_by_algo = defaultdict(lambda: {"total": 0, "with_results": 0, "missing": []})

            for strategy_name in sorted(hierarchy.keys()):
                modes_data = hierarchy[strategy_name]
                strategy_configs = 0
                strategy_with_results = 0
                strategy_result_dates = set()

                modes = []
                for mode_name in sorted(modes_data.keys()):
                    timeframes_data = modes_data[mode_name]
                    mode_configs = 0
                    mode_with_results = 0

                    timeframes = []
                    for timeframe_name in sorted(timeframes_data.keys()):
                        configs_list = timeframes_data[timeframe_name]
                        tf_total = len(configs_list)
                        tf_with_results = sum(1 for c in configs_list if c["has_results"])
                        tf_missing = [c for c in configs_list if not c["has_results"]]

                        # Collect result dates
                        for c in configs_list:
                            strategy_result_dates.update(c["result_dates"])

                        timeframes.append(
                            {
                                "timeframe": timeframe_name,
                                "total": tf_total,
                                "with_results": tf_with_results,
                                "completion_pct": (round(tf_with_results / tf_total * 100, 1) if tf_total > 0 else 0),
                                "missing_configs": [
                                    {
                                        "config_file": c["config_file"],
                                        "algo_name": c["algo_name"],
                                    }
                                    for c in tf_missing
                                ],
                                "configs": configs_list,
                            }
                        )

                        mode_configs += tf_total
                        mode_with_results += tf_with_results

                        # Update timeframe breakdown
                        breakdown_by_timeframe[timeframe_name]["total"] += tf_total
                        breakdown_by_timeframe[timeframe_name]["with_results"] += tf_with_results
                        breakdown_by_timeframe[timeframe_name]["missing"].extend(
                            f"{strategy_name}/{mode_name}/{c['config_file']}" for c in tf_missing
                        )

                        # Update algo breakdown
                        for c in configs_list:
                            breakdown_by_algo[c["algo_name"]]["total"] += 1
                            if c["has_results"]:
                                breakdown_by_algo[c["algo_name"]]["with_results"] += 1
                            else:
                                breakdown_by_algo[c["algo_name"]]["missing"].append(
                                    f"{strategy_name}/{mode_name}/{timeframe_name}/{c['config_file']}"
                                )

                    modes.append(
                        {
                            "mode": mode_name,
                            "total": mode_configs,
                            "with_results": mode_with_results,
                            "completion_pct": (
                                round(mode_with_results / mode_configs * 100, 1) if mode_configs > 0 else 0
                            ),
                            "timeframes": timeframes,
                        }
                    )

                    strategy_configs += mode_configs
                    strategy_with_results += mode_with_results

                    # Update mode breakdown
                    breakdown_by_mode[mode_name]["total"] += mode_configs
                    breakdown_by_mode[mode_name]["with_results"] += mode_with_results

                strategies.append(
                    {
                        "strategy": strategy_name,
                        "total": strategy_configs,
                        "with_results": strategy_with_results,
                        "completion_pct": (
                            round(strategy_with_results / strategy_configs * 100, 1) if strategy_configs > 0 else 0
                        ),
                        "result_dates": sorted(strategy_result_dates),
                        "result_date_count": len(strategy_result_dates),
                        "modes": modes,
                    }
                )

                total_configs += strategy_configs
                total_with_results += strategy_with_results

            # Build breakdown summaries with completion %
            def build_breakdown_summary(breakdown):
                return {
                    name: {
                        "total": data["total"],
                        "with_results": data["with_results"],
                        "missing_count": data["total"] - data["with_results"],
                        "completion_pct": (
                            round(data["with_results"] / data["total"] * 100, 1) if data["total"] > 0 else 0
                        ),
                        "missing_samples": data["missing"][:5],  # First 5 for preview
                    }
                    for name, data in sorted(breakdown.items())
                }

            return {
                "config_path": config_path,
                "version": version,
                "total_configs": total_configs,
                "configs_with_results": total_with_results,
                "missing_count": total_configs - total_with_results,
                "completion_pct": (round(total_with_results / total_configs * 100, 1) if total_configs > 0 else 0),
                "strategy_count": len(strategies),
                "strategies": strategies,
                # Flat breakdowns for quick diagnostics
                "breakdown_by_mode": build_breakdown_summary(breakdown_by_mode),
                "breakdown_by_timeframe": build_breakdown_summary(breakdown_by_timeframe),
                "breakdown_by_algo": build_breakdown_summary(breakdown_by_algo),
                "date_filter": (
                    {
                        "start": start_date,
                        "end": end_date,
                    }
                    if start_date or end_date
                    else None
                ),
            }

        except (OSError, ValueError, RuntimeError) as e:
            logger.exception("Error getting execution-service data status: %s", e)
            return {"error": str(e)[:200]}

    result = cast(dict[str, object], cast(object, await asyncio.to_thread(_get_status_sync)))

    # Cache successful results (not errors)
    if "error" not in result:
        set_exec_cached_result(config_path, start_date, end_date, result)

    return result


async def calculate_execution_missing_shards(
    config_path: str,
    start_date: str,
    end_date: str,
    strategy: str | None = None,
    mode: str | None = None,
    timeframe: str | None = None,
    algo: str | None = None,
) -> dict[str, object]:
    """
    Calculate missing config x date shards for execution-service.

    For each config, finds dates without results within the specified range.
    Returns missing shards that need to be deployed.

    Args:
        config_path: GCS config path (e.g., gs://execution-store.../configs/V1)
        start_date: Start date (YYYY-MM-DD)
        end_date: End date (YYYY-MM-DD)
        strategy: Optional filter by strategy name
        mode: Optional filter by mode (SCE/HUF)
        timeframe: Optional filter by timeframe (5M, 15M, etc.)
        algo: Optional filter by algorithm name
    """
    from deployment_api.utils.storage_facade import list_objects, list_prefixes

    def _calculate_missing_sync():
        try:
            # Parse config_path to get bucket and prefix
            if not config_path.startswith("gs://"):
                return {"error": "config_path must start with gs://"}

            path_parts = config_path[5:].split("/", 1)
            bucket_name = path_parts[0]
            config_prefix = path_parts[1] if len(path_parts) > 1 else ""

            if config_prefix and not config_prefix.endswith("/"):
                config_prefix += "/"

            # Extract version from config path
            version_match = re.search(r"/([Vv]\d+)/?$", config_prefix)
            version = version_match.group(1) if version_match else "V1"

            # Generate all expected dates from date range
            filter_start = datetime.strptime(start_date, "%Y-%m-%d").replace(tzinfo=UTC).date()
            filter_end = datetime.strptime(end_date, "%Y-%m-%d").replace(tzinfo=UTC).date()
            all_expected_dates = set()
            current = filter_start
            while current <= filter_end:
                all_expected_dates.add(current.strftime("%Y-%m-%d"))
                current += timedelta(days=1)

            # Step 1: List all configs (storage facade)
            configs = []
            config_objs = list_objects(bucket_name, config_prefix, max_results=10000)

            for obj in config_objs:
                if obj.name.endswith(".json"):
                    rel_path = obj.name[len(config_prefix) :] if obj.name.startswith(config_prefix) else obj.name
                    parts = rel_path.split("/")

                    if len(parts) >= 4:
                        cfg_strategy = parts[0]
                        cfg_mode = parts[1]
                        cfg_timeframe = parts[2]
                        config_file = parts[3] if len(parts) > 3 else parts[-1]

                        # Apply filters
                        if strategy and cfg_strategy != strategy:
                            continue
                        if mode and cfg_mode != mode:
                            continue
                        if timeframe and cfg_timeframe != timeframe:
                            continue

                        # Extract algo name
                        algo_match = re.match(
                            r"^([A-Z_]+?)_(?:horizon|profile|display|clip|urgency|num_|participation|lambda|sigma|passive)",
                            config_file,
                        )
                        algo_name = algo_match.group(1) if algo_match else config_file.split("_")[0]

                        # Apply algo filter
                        if algo and algo_name != algo:
                            continue

                        result_strategy_id = f"{cfg_strategy}_{cfg_mode}_{cfg_timeframe}_{version}"

                        configs.append(
                            {
                                "path": f"gs://{bucket_name}/{obj.name}",
                                "strategy": cfg_strategy,
                                "mode": cfg_mode,
                                "timeframe": cfg_timeframe,
                                "config_file": config_file,
                                "algo_name": algo_name,
                                "result_strategy_id": result_strategy_id,
                            }
                        )

            logger.info("[EXEC-MISSING] Found %s configs after filters", len(configs))

            # Step 2: List all result strategy_ids with their dates (storage facade)
            results_prefix = "results/"
            result_dates_by_strategy = defaultdict(set)

            date_prefixes = list_prefixes(bucket_name, results_prefix)

            for date_prefix in date_prefixes:
                date_match = re.search(r"results/(\d{4}-\d{2}-\d{2})/", date_prefix)
                if date_match:
                    date_str = date_match.group(1)
                    result_date = datetime.strptime(date_str, "%Y-%m-%d").replace(tzinfo=UTC).date()

                    if result_date < filter_start or result_date > filter_end:
                        continue

                    strategy_prefixes = list_prefixes(bucket_name, date_prefix)

                    for strategy_prefix in strategy_prefixes:
                        strategy_match = re.search(r"results/\d{4}-\d{2}-\d{2}/([^/]+)/", strategy_prefix)
                        if strategy_match:
                            strategy_id = strategy_match.group(1)
                            result_dates_by_strategy[strategy_id].add(date_str)

            # Step 3: Calculate missing shards for each config
            missing_shards = []
            breakdown_by_strategy = defaultdict(int)
            breakdown_by_mode = defaultdict(int)
            breakdown_by_timeframe = defaultdict(int)
            breakdown_by_algo = defaultdict(int)
            breakdown_by_date = defaultdict(int)

            for config in configs:
                result_strategy_id = config["result_strategy_id"]
                existing_dates = result_dates_by_strategy.get(result_strategy_id, set())
                missing_dates = all_expected_dates - existing_dates

                for date_str in sorted(missing_dates):
                    missing_shards.append(
                        {
                            "config_gcs": config["path"],
                            "date": date_str,
                            "strategy": config["strategy"],
                            "mode": config["mode"],
                            "timeframe": config["timeframe"],
                            "algo": config["algo_name"],
                        }
                    )
                    breakdown_by_strategy[config["strategy"]] += 1
                    breakdown_by_mode[config["mode"]] += 1
                    breakdown_by_timeframe[config["timeframe"]] += 1
                    breakdown_by_algo[config["algo_name"]] += 1
                    breakdown_by_date[date_str] += 1

            logger.info("[EXEC-MISSING] Calculated %s missing shards", len(missing_shards))

            return {
                "missing_shards": missing_shards,
                "total_missing": len(missing_shards),
                "total_configs": len(configs),
                "total_dates": len(all_expected_dates),
                "breakdown": {
                    "by_strategy": dict(sorted(breakdown_by_strategy.items())),
                    "by_mode": dict(sorted(breakdown_by_mode.items())),
                    "by_timeframe": dict(sorted(breakdown_by_timeframe.items())),
                    "by_algo": dict(sorted(breakdown_by_algo.items())),
                    "by_date": dict(sorted(breakdown_by_date.items())),
                },
                "filters": {
                    "config_path": config_path,
                    "start_date": start_date,
                    "end_date": end_date,
                    "strategy": strategy,
                    "mode": mode,
                    "timeframe": timeframe,
                    "algo": algo,
                },
            }

        except (OSError, ValueError, RuntimeError) as e:
            logger.exception("Error calculating execution missing shards: %s", e)
            return {"error": str(e)[:200]}

    return cast(dict[str, object], cast(object, await asyncio.to_thread(_calculate_missing_sync)))

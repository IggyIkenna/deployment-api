"""Supplemental tests for service_status_execution.py — covers uncovered helpers:
_list_exec_configs, _collect_result_strategy_ids, _build_hierarchy_from_configs,
_summarize_hierarchy, _list_missing_configs_filtered, _collect_result_dates_in_range.
"""

from __future__ import annotations

from collections import defaultdict
from datetime import date
from types import SimpleNamespace
from typing import cast
from unittest.mock import patch

from unified_trading_library import setup_events

setup_events("deployment-api", "test")

_PATCH_LIST_OBJECTS = "deployment_api.utils.storage_facade.list_objects"
_PATCH_LIST_PREFIXES = "deployment_api.utils.storage_facade.list_prefixes"


# ── _list_exec_configs ────────────────────────────────────────────────────────


class TestListExecConfigs:
    def test_empty_storage_returns_empty_list(self) -> None:
        from deployment_api.routes.service_status_execution import _list_exec_configs

        with patch(_PATCH_LIST_OBJECTS, return_value=[]):
            result = _list_exec_configs("bucket", "configs/V1/", "V1")
        assert result == []

    def test_valid_json_object_included(self) -> None:
        from deployment_api.routes.service_status_execution import _list_exec_configs

        obj = SimpleNamespace(name="configs/V1/SCE/live/5M/TWAP_horizon.json")
        with patch(_PATCH_LIST_OBJECTS, return_value=[obj]):
            result = _list_exec_configs("bucket", "configs/V1/", "V1")
        assert len(result) == 1
        assert result[0]["strategy"] == "SCE"
        assert result[0]["mode"] == "live"
        assert result[0]["timeframe"] == "5M"

    def test_non_json_file_skipped(self) -> None:
        from deployment_api.routes.service_status_execution import _list_exec_configs

        obj = SimpleNamespace(name="configs/V1/SCE/live/5M/TWAP.yaml")
        with patch(_PATCH_LIST_OBJECTS, return_value=[obj]):
            result = _list_exec_configs("bucket", "configs/V1/", "V1")
        assert result == []

    def test_short_path_skipped(self) -> None:
        from deployment_api.routes.service_status_execution import _list_exec_configs

        obj = SimpleNamespace(name="configs/V1/SCE/live.json")
        with patch(_PATCH_LIST_OBJECTS, return_value=[obj]):
            result = _list_exec_configs("bucket", "configs/V1/", "V1")
        assert result == []

    def test_strategy_filter_applied(self) -> None:
        from deployment_api.routes.service_status_execution import _list_exec_configs

        obj_sce = SimpleNamespace(name="configs/V1/SCE/live/5M/TWAP.json")
        obj_huf = SimpleNamespace(name="configs/V1/HUF/live/5M/TWAP.json")
        with patch(_PATCH_LIST_OBJECTS, return_value=[obj_sce, obj_huf]):
            result = _list_exec_configs("bucket", "configs/V1/", "V1", strategy_filter="SCE")
        assert len(result) == 1
        assert result[0]["strategy"] == "SCE"

    def test_mode_filter_applied(self) -> None:
        from deployment_api.routes.service_status_execution import _list_exec_configs

        obj_live = SimpleNamespace(name="configs/V1/SCE/live/5M/TWAP.json")
        obj_paper = SimpleNamespace(name="configs/V1/SCE/paper/5M/TWAP.json")
        with patch(_PATCH_LIST_OBJECTS, return_value=[obj_live, obj_paper]):
            result = _list_exec_configs("bucket", "configs/V1/", "V1", mode_filter="live")
        assert all(r["mode"] == "live" for r in result)

    def test_include_gs_prefix(self) -> None:
        from deployment_api.routes.service_status_execution import _list_exec_configs

        obj = SimpleNamespace(name="configs/V1/SCE/live/5M/TWAP.json")
        with patch(_PATCH_LIST_OBJECTS, return_value=[obj]):
            result = _list_exec_configs("bucket", "configs/V1/", "V1", include_gs_prefix=True)
        assert result[0]["path"].startswith("gs://")

    def test_result_strategy_id_format(self) -> None:
        from deployment_api.routes.service_status_execution import _list_exec_configs

        obj = SimpleNamespace(name="configs/V1/SCE/live/5M/TWAP.json")
        with patch(_PATCH_LIST_OBJECTS, return_value=[obj]):
            result = _list_exec_configs("bucket", "configs/V1/", "V1")
        assert result[0]["result_strategy_id"] == "SCE_live_5M_V1"


# ── _collect_result_strategy_ids ──────────────────────────────────────────────


class TestCollectResultStrategyIds:
    def test_empty_storage_returns_empty_sets(self) -> None:
        from deployment_api.routes.service_status_execution import _collect_result_strategy_ids

        with patch(_PATCH_LIST_PREFIXES, return_value=[]):
            ids, dates = _collect_result_strategy_ids("bucket", None, None)
        assert ids == set()
        assert dict(dates) == {}

    def test_valid_result_prefix_extracted(self) -> None:
        from deployment_api.routes.service_status_execution import _collect_result_strategy_ids

        with patch(
            _PATCH_LIST_PREFIXES,
            side_effect=[
                ["results/2024-01-01/"],
                ["results/2024-01-01/SCE_live_5M_V1/"],
            ],
        ):
            ids, dates = _collect_result_strategy_ids("bucket", None, None)
        assert "SCE_live_5M_V1" in ids
        assert "2024-01-01" in dates["SCE_live_5M_V1"]

    def test_date_filter_applied(self) -> None:
        from datetime import date

        from deployment_api.routes.service_status_execution import _collect_result_strategy_ids

        with patch(
            _PATCH_LIST_PREFIXES,
            side_effect=[
                ["results/2024-01-01/", "results/2024-02-01/"],
                ["results/2024-02-01/SCE_live_5M_V1/"],
            ],
        ):
            filter_start = date(2024, 2, 1)
            ids, dates = _collect_result_strategy_ids("bucket", filter_start, None)
        assert "SCE_live_5M_V1" in ids

    def test_invalid_date_prefix_skipped(self) -> None:
        from deployment_api.routes.service_status_execution import _collect_result_strategy_ids

        with patch(
            _PATCH_LIST_PREFIXES,
            side_effect=[
                ["results/not-a-date/"],
            ],
        ):
            ids, dates = _collect_result_strategy_ids("bucket", None, None)
        assert ids == set()


# ── _build_hierarchy_from_configs ─────────────────────────────────────────────


class TestBuildHierarchyFromConfigs:
    def test_builds_correct_hierarchy(self) -> None:
        from deployment_api.routes.service_status_execution import _build_hierarchy_from_configs

        configs = [
            {
                "strategy": "SCE",
                "mode": "live",
                "timeframe": "5M",
                "config_file": "TWAP.json",
                "algo_name": "TWAP",
                "result_strategy_id": "SCE_live_5M_V1",
            }
        ]
        existing_ids: set[str] = {"SCE_live_5M_V1"}
        dates_by_strategy: defaultdict[str, set[str]] = defaultdict(set)
        dates_by_strategy["SCE_live_5M_V1"] = {"2024-01-01"}

        hierarchy = _build_hierarchy_from_configs(configs, existing_ids, dates_by_strategy)
        assert "SCE" in hierarchy
        assert "live" in hierarchy["SCE"]
        assert "5M" in hierarchy["SCE"]["live"]
        entries = hierarchy["SCE"]["live"]["5M"]
        assert len(entries) == 1
        assert entries[0]["has_results"] is True
        assert "2024-01-01" in cast(list[str], entries[0]["result_dates"])

    def test_missing_results_marked_false(self) -> None:
        from deployment_api.routes.service_status_execution import _build_hierarchy_from_configs

        configs = [
            {
                "strategy": "SCE",
                "mode": "live",
                "timeframe": "5M",
                "config_file": "TWAP.json",
                "algo_name": "TWAP",
                "result_strategy_id": "SCE_live_5M_V1",
            }
        ]
        existing_ids: set[str] = set()
        dates_by_strategy: defaultdict[str, set[str]] = defaultdict(set)

        hierarchy = _build_hierarchy_from_configs(configs, existing_ids, dates_by_strategy)
        entries = hierarchy["SCE"]["live"]["5M"]
        assert entries[0]["has_results"] is False


# ── _summarize_hierarchy ──────────────────────────────────────────────────────


class TestSummarizeHierarchy:
    def _make_hierarchy(
        self,
        strategy: str,
        mode: str,
        timeframe: str,
        has_results: bool,
        result_dates: list[str] | None = None,
    ) -> defaultdict:
        hierarchy: defaultdict = defaultdict(lambda: defaultdict(lambda: defaultdict(list)))
        entry = {
            "config_file": "TWAP.json",
            "algo_name": "TWAP",
            "result_strategy_id": f"{strategy}_{mode}_{timeframe}_V1",
            "has_results": has_results,
            "result_dates": result_dates or [],
        }
        hierarchy[strategy][mode][timeframe].append(entry)
        return hierarchy

    def test_single_config_with_results(self) -> None:
        from deployment_api.routes.service_status_execution import _summarize_hierarchy

        hierarchy = self._make_hierarchy("SCE", "live", "5M", True, ["2024-01-01"])
        strategies, total, with_results, bd_mode, bd_tf, bd_algo = _summarize_hierarchy(hierarchy)

        assert total == 1
        assert with_results == 1
        assert len(strategies) == 1
        s = strategies[0]
        assert s["strategy"] == "SCE"
        assert s["completion_pct"] == 100.0
        assert "2024-01-01" in cast(list[str], s["result_dates"])

    def test_single_config_without_results(self) -> None:
        from deployment_api.routes.service_status_execution import _summarize_hierarchy

        hierarchy = self._make_hierarchy("SCE", "live", "5M", False)
        _, total, with_results, _, _, _ = _summarize_hierarchy(hierarchy)
        assert total == 1
        assert with_results == 0

    def test_breakdown_by_mode_populated(self) -> None:
        from deployment_api.routes.service_status_execution import _summarize_hierarchy

        hierarchy = self._make_hierarchy("SCE", "live", "5M", True)
        _, _, _, bd_mode, _, _ = _summarize_hierarchy(hierarchy)
        assert "live" in bd_mode
        entry = cast(dict[str, object], bd_mode["live"])
        assert entry["total"] == 1
        assert entry["with_results"] == 1

    def test_breakdown_by_timeframe_populated(self) -> None:
        from deployment_api.routes.service_status_execution import _summarize_hierarchy

        hierarchy = self._make_hierarchy("SCE", "live", "5M", False)
        _, _, _, _, bd_tf, _ = _summarize_hierarchy(hierarchy)
        assert "5M" in bd_tf

    def test_breakdown_by_algo_populated(self) -> None:
        from deployment_api.routes.service_status_execution import _summarize_hierarchy

        hierarchy = self._make_hierarchy("SCE", "live", "5M", True)
        _, _, _, _, _, bd_algo = _summarize_hierarchy(hierarchy)
        assert "TWAP" in bd_algo

    def test_multiple_strategies_sorted(self) -> None:
        from deployment_api.routes.service_status_execution import _summarize_hierarchy

        hierarchy = self._make_hierarchy("ZZZ", "live", "5M", True)
        hierarchy["AAA"]["live"]["5M"].append(
            {
                "config_file": "X.json",
                "algo_name": "X",
                "result_strategy_id": "AAA_live_5M_V1",
                "has_results": False,
                "result_dates": [],
            }
        )
        strategies, _, _, _, _, _ = _summarize_hierarchy(hierarchy)
        names = [cast(str, s["strategy"]) for s in strategies]
        assert names == sorted(names)

    def test_zero_total_configs_gives_zero_completion(self) -> None:
        from deployment_api.routes.service_status_execution import _summarize_hierarchy

        hierarchy: defaultdict = defaultdict(lambda: defaultdict(lambda: defaultdict(list)))
        strategies, total, _, _, _, _ = _summarize_hierarchy(hierarchy)
        assert total == 0
        assert strategies == []

    def test_missing_samples_limited_in_timeframe(self) -> None:
        from deployment_api.routes.service_status_execution import _summarize_hierarchy

        hierarchy: defaultdict = defaultdict(lambda: defaultdict(lambda: defaultdict(list)))
        for i in range(10):
            hierarchy["SCE"]["live"]["5M"].append(
                {
                    "config_file": f"TWAP_{i}.json",
                    "algo_name": "TWAP",
                    "result_strategy_id": f"SCE_live_5M_V1_{i}",
                    "has_results": False,
                    "result_dates": [],
                }
            )
        strategies, total, with_results, _, _, _ = _summarize_hierarchy(hierarchy)
        assert total == 10
        assert with_results == 0
        # missing_configs in timeframe entry
        modes = cast(list[dict[str, object]], strategies[0]["modes"])
        timeframes = cast(list[dict[str, object]], modes[0]["timeframes"])
        missing = cast(list[object], timeframes[0]["missing_configs"])
        assert len(missing) == 10


# ── _list_missing_configs_filtered ────────────────────────────────────────────


class TestListMissingConfigsFiltered:
    def test_no_configs_returns_empty(self) -> None:
        from deployment_api.routes.service_status_execution import _list_missing_configs_filtered

        with patch(_PATCH_LIST_OBJECTS, return_value=[]):
            result = _list_missing_configs_filtered("bucket", "configs/V1/", "V1", None, None, None, None)
        assert result == []

    def test_valid_json_config_included(self) -> None:
        from deployment_api.routes.service_status_execution import _list_missing_configs_filtered

        obj = SimpleNamespace(name="configs/V1/SCE/live/5M/TWAP_horizon.json")
        with patch(_PATCH_LIST_OBJECTS, return_value=[obj]):
            result = _list_missing_configs_filtered("bucket", "configs/V1/", "V1", None, None, None, None)
        assert len(result) == 1
        assert result[0]["strategy"] == "SCE"

    def test_strategy_filter_applied(self) -> None:
        from deployment_api.routes.service_status_execution import _list_missing_configs_filtered

        obj_sce = SimpleNamespace(name="configs/V1/SCE/live/5M/TWAP.json")
        obj_huf = SimpleNamespace(name="configs/V1/HUF/live/5M/TWAP.json")
        with patch(_PATCH_LIST_OBJECTS, return_value=[obj_sce, obj_huf]):
            result = _list_missing_configs_filtered("bucket", "configs/V1/", "V1", "SCE", None, None, None)
        assert len(result) == 1
        assert result[0]["strategy"] == "SCE"


# ── _collect_result_dates_in_range ────────────────────────────────────────────


class TestCollectResultDatesInRange:
    def test_no_prefixes_returns_empty(self) -> None:
        from deployment_api.routes.service_status_execution import _collect_result_dates_in_range

        with patch(_PATCH_LIST_PREFIXES, return_value=[]):
            result = _collect_result_dates_in_range("bucket", date(2024, 1, 1), date(2024, 1, 31))
        assert dict(result) == {}

    def test_date_in_range_included(self) -> None:
        from deployment_api.routes.service_status_execution import _collect_result_dates_in_range

        with patch(
            _PATCH_LIST_PREFIXES,
            side_effect=[
                ["results/2024-01-15/"],
                ["results/2024-01-15/SCE_live_5M_V1/"],
            ],
        ):
            result = _collect_result_dates_in_range("bucket", date(2024, 1, 1), date(2024, 1, 31))
        assert "2024-01-15" in result["SCE_live_5M_V1"]

    def test_date_outside_range_excluded(self) -> None:
        from deployment_api.routes.service_status_execution import _collect_result_dates_in_range

        with patch(
            _PATCH_LIST_PREFIXES,
            return_value=["results/2024-03-01/"],
        ):
            result = _collect_result_dates_in_range("bucket", date(2024, 1, 1), date(2024, 1, 31))
        assert dict(result) == {}

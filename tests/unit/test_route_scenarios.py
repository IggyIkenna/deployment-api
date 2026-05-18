"""Unit tests for routes/scenarios.py — Phases 7.A + 7.C scenario endpoints."""

from __future__ import annotations

from unified_trading_library import setup_events

setup_events("deployment-api", "test")


class TestListScenarios:
    """Tests for GET /api/scenarios/list."""

    def test_list_all_returns_all_scenarios(self) -> None:
        import asyncio

        from deployment_api.routes.scenarios import list_scenarios

        result = asyncio.run(list_scenarios(asset_group=None))
        assert isinstance(result, dict)
        assert "total" in result
        assert "scenarios" in result
        assert result["asset_group_filter"] is None
        scenarios = result["scenarios"]
        assert isinstance(scenarios, list)
        assert result["total"] == len(scenarios)
        assert result["total"] >= 10

    def test_list_cefi_filter(self) -> None:
        import asyncio

        from deployment_api.routes.scenarios import list_scenarios

        result = asyncio.run(list_scenarios(asset_group="cefi"))
        scenarios = result["scenarios"]
        assert len(scenarios) >= 1
        for s in scenarios:
            assert "cefi" in s["asset_groups"]

    def test_list_defi_filter(self) -> None:
        import asyncio

        from deployment_api.routes.scenarios import list_scenarios

        result = asyncio.run(list_scenarios(asset_group="defi"))
        scenarios = result["scenarios"]
        assert len(scenarios) >= 1
        for s in scenarios:
            assert "defi" in s["asset_groups"]

    def test_unknown_asset_group_returns_empty(self) -> None:
        import asyncio

        from deployment_api.routes.scenarios import list_scenarios

        result = asyncio.run(list_scenarios(asset_group="unknown_group"))
        assert result["total"] == 0
        assert result["scenarios"] == []

    def test_scenario_item_shape(self) -> None:
        import asyncio

        from deployment_api.routes.scenarios import list_scenarios

        result = asyncio.run(list_scenarios(asset_group=None))
        s = result["scenarios"][0]
        assert isinstance(s["scenario_id"], str)
        assert isinstance(s["category"], str)
        assert isinstance(s["layer"], str)
        assert isinstance(s["asset_groups"], list)
        assert isinstance(s["description"], str)
        assert isinstance(s["expected_outcome_count"], int)

    def test_results_sorted_by_scenario_id(self) -> None:
        import asyncio

        from deployment_api.routes.scenarios import list_scenarios

        result = asyncio.run(list_scenarios(asset_group=None))
        ids = [s["scenario_id"] for s in result["scenarios"]]
        assert ids == sorted(ids)


class TestToListItem:
    """Tests for _to_list_item helper."""

    def test_str_fields_are_strings(self) -> None:
        from unified_api_contracts import SCENARIO_REGISTRY

        from deployment_api.routes.scenarios import _to_list_item

        for s in SCENARIO_REGISTRY.values():
            item = _to_list_item(s)
            assert isinstance(item.category, str)
            assert isinstance(item.layer, str)
            assert isinstance(item.asset_groups, list)
            assert item.asset_groups == sorted(item.asset_groups)
            assert item.expected_outcome_count == len(s.expected_outcomes)
            break


class TestGetScenarioMatrix:
    """Tests for GET /api/scenarios/matrix/{archetype}."""

    def test_known_archetype_returns_scenarios(self) -> None:
        import asyncio

        from deployment_api.routes.scenarios import get_scenario_matrix

        result = asyncio.run(get_scenario_matrix("carry_staked_basis"))
        assert result["archetype"] == "carry_staked_basis"
        assert isinstance(result["total"], int)
        assert isinstance(result["scenarios"], list)

    def test_matrix_scenarios_have_list_item_shape(self) -> None:
        import asyncio

        from deployment_api.routes.scenarios import get_scenario_matrix

        result = asyncio.run(get_scenario_matrix("carry_staked_basis"))
        for s in result["scenarios"]:
            assert "scenario_id" in s
            assert "category" in s
            assert "asset_groups" in s

    def test_unknown_archetype_raises_400(self) -> None:
        import asyncio

        import pytest
        from fastapi import HTTPException

        from deployment_api.routes.scenarios import get_scenario_matrix

        with pytest.raises(HTTPException) as exc_info:
            asyncio.run(get_scenario_matrix("nonexistent_archetype"))
        assert exc_info.value.status_code == 400

    def test_matrix_scenarios_sorted_by_id(self) -> None:
        import asyncio

        from unified_api_contracts import CUTOVER_ARCHETYPES

        from deployment_api.routes.scenarios import get_scenario_matrix

        for archetype in CUTOVER_ARCHETYPES:
            result = asyncio.run(get_scenario_matrix(archetype))
            ids = [s["scenario_id"] for s in result["scenarios"]]
            assert ids == sorted(ids)


class TestGetScenarioReport:
    """Tests for GET /api/scenarios/report/{run_id}."""

    def test_any_run_id_raises_501(self) -> None:
        import asyncio

        import pytest
        from fastapi import HTTPException

        from deployment_api.routes.scenarios import get_scenario_report

        with pytest.raises(HTTPException) as exc_info:
            asyncio.run(get_scenario_report("any-run-id"))
        assert exc_info.value.status_code == 501

    def test_501_detail_mentions_phase_2c(self) -> None:
        import asyncio

        import pytest
        from fastapi import HTTPException

        from deployment_api.routes.scenarios import get_scenario_report

        with pytest.raises(HTTPException) as exc_info:
            asyncio.run(get_scenario_report("test-run-123"))
        detail = exc_info.value.detail
        assert isinstance(detail, dict)
        assert "Phase 2.C" in detail["reason"]
        assert detail["run_id"] == "test-run-123"

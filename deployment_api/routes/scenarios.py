"""Scenario registry API routes — Phase 7.A of simulation_scenarios_topology_price_shocks_2026_05_09.md."""

from __future__ import annotations

from fastapi import APIRouter, Query
from pydantic import BaseModel
from unified_api_contracts import (
    SCENARIO_REGISTRY,
    ScenarioOverlay,
)

router = APIRouter()


class ScenarioListItem(BaseModel):
    scenario_id: str
    category: str
    layer: str
    asset_groups: list[str]
    description: str
    expected_outcome_count: int


def _to_list_item(s: ScenarioOverlay) -> ScenarioListItem:
    return ScenarioListItem(
        scenario_id=s.scenario_id,
        category=str(s.category),
        layer=str(s.layer),
        asset_groups=sorted(s.asset_groups),
        description=s.description,
        expected_outcome_count=len(s.expected_outcomes),
    )


@router.get("/list")
async def list_scenarios(
    asset_group: str | None = Query(
        default=None,
        description="Filter by asset_group (cefi/defi/tradfi/sports/prediction)",
    ),
) -> dict[str, object]:
    """Return the UAC scenario registry, optionally filtered by asset_group."""
    scenarios = list(SCENARIO_REGISTRY.values())
    if asset_group is not None:
        scenarios = [s for s in scenarios if asset_group in s.asset_groups]
    return {
        "total": len(scenarios),
        "asset_group_filter": asset_group,
        "scenarios": [_to_list_item(s).model_dump() for s in sorted(scenarios, key=lambda s: s.scenario_id)],
    }

"""Scenario registry API routes — Phases 7.A + 7.C of simulation_scenarios_topology_price_shocks_2026_05_09.md."""

from __future__ import annotations

from fastapi import APIRouter, HTTPException, Query
from pydantic import BaseModel
from unified_api_contracts import (
    CUTOVER_ARCHETYPES,
    SCENARIO_ARCHETYPE_MATRIX,
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


@router.get("/matrix/{archetype}")
async def get_scenario_matrix(archetype: str) -> dict[str, object]:
    """Return scenarios in the regression matrix for the given archetype.

    Only archetypes declared in CUTOVER_ARCHETYPES are valid. Each entry
    in the response is enriched with the full ScenarioListItem from the registry.
    """
    if archetype not in CUTOVER_ARCHETYPES:
        raise HTTPException(
            status_code=400,
            detail={
                "message": f"Unknown archetype {archetype!r}",
                "valid_archetypes": sorted(CUTOVER_ARCHETYPES),
            },
        )
    scenario_ids = SCENARIO_ARCHETYPE_MATRIX[archetype]
    scenarios = [
        _to_list_item(SCENARIO_REGISTRY[sid]).model_dump() for sid in sorted(scenario_ids) if sid in SCENARIO_REGISTRY
    ]
    return {
        "archetype": archetype,
        "total": len(scenarios),
        "scenarios": scenarios,
    }


@router.get("/report/{run_id}")
async def get_scenario_report(run_id: str) -> dict[str, object]:
    """Return the ScenarioReport for a completed scenario run.

    Phase 7.C — GCS parquet read path. Currently returns 501 because
    ScenarioReportEmitter (Phase 2.C of the simulation plan) is DEFERRED
    per compressed pre-cutover scope; the pre-cutover harness uses
    in-memory + JSONL only. Wire GCS path here when Phase 2.C ships.
    """
    raise HTTPException(
        status_code=501,
        detail={
            "message": f"Report for run_id={run_id!r} not available",
            "reason": "ScenarioReportEmitter (Phase 2.C) deferred; pre-cutover harness uses in-memory + JSONL",
            "run_id": run_id,
        },
    )

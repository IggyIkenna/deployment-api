"""Fixtures browser API — catalogue fixtures grouped by league then day.

Sibling to ``routes/fixtures.py``'s ``/fixtures/upcoming`` (a flat forward-
window list): this endpoint groups fixtures in a bounded window around today
by ``league_id`` then UTC day, for the data-status fixtures-browser drilldown
(operator request, P9,
``plans/active/data_status_page_ux_and_canonicalisation_2026_07_16.md``).
Mirrors ``routes/fixtures.py``'s mock-mode-aware shape exactly.
"""

from __future__ import annotations

from fastapi import APIRouter, Query

from deployment_api.deployment_api_config import DeploymentApiConfig
from deployment_api.services.fixtures_browser import list_fixtures_by_league_and_day

_cfg = DeploymentApiConfig()

router = APIRouter(prefix="/fixtures", tags=["Fixtures"])


@router.get("/browse")
async def get_fixtures_browse(
    days_back: int = Query(7, ge=0, le=60, description="Backward window from today (UTC), inclusive"),
    days_forward: int = Query(30, ge=0, le=60, description="Forward window from today (UTC), inclusive"),
    league_id: str | None = Query(None, description="Optional canonical league_id filter"),
) -> dict[str, object]:
    """Return catalogue fixtures in [today-days_back, today+days_forward], grouped league -> day."""
    if _cfg.is_mock_mode():
        return {"leagues": {}, "mock": True}
    grouped = list_fixtures_by_league_and_day(
        window_days_back=days_back,
        window_days_forward=days_forward,
        league_id=league_id,
    )
    return {"leagues": grouped}

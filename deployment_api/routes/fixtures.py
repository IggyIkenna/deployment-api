"""Upcoming fixtures API — reads rolling-window sports_reference parquets."""

from __future__ import annotations

from fastapi import APIRouter, Query

from deployment_api.deployment_api_config import DeploymentApiConfig
from deployment_api.services.upcoming_fixtures import list_upcoming_fixtures

_cfg = DeploymentApiConfig()

router = APIRouter(prefix="/fixtures", tags=["Fixtures"])


@router.get("/upcoming")
async def get_upcoming_fixtures(
    days: int = Query(7, ge=1, le=31, description="Forward window from today (UTC), inclusive"),
    league_id: str | None = Query(None, description="Optional canonical league_id filter"),
) -> dict[str, object]:
    """Return fixtures from GCS parquets for [today, today+days], sorted by kickoff."""
    if _cfg.is_mock_mode():
        return {"fixtures": [], "mock": True}
    fixtures = list_upcoming_fixtures(days=days, league_id=league_id)
    return {"fixtures": list(fixtures)}

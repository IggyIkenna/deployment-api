"""Upcoming fixtures API — reads rolling-window sports_reference parquets."""

from __future__ import annotations

from fastapi import APIRouter, Query

from deployment_api.deployment_api_config import DeploymentApiConfig
from deployment_api.services.upcoming_fixtures import (
    league_names_for_fixtures,
    list_upcoming_fixtures,
)

_cfg = DeploymentApiConfig()

router = APIRouter(prefix="/fixtures", tags=["Fixtures"])


@router.get("/upcoming")
async def get_upcoming_fixtures(
    days: int = Query(7, ge=1, le=31, description="Forward window from today (UTC), inclusive"),
    league_id: str | None = Query(
        None, description="Optional league filter — case-insensitive substring on raw id or human name"
    ),
) -> dict[str, object]:
    """Return fixtures from GCS parquets for [today, today+days], sorted by kickoff.

    ``league_names`` maps each present raw ``league_id`` -> its human
    ``display_name`` (UAC data); unresolved ids are omitted so the UI falls
    back to the raw id (honest-absence), mirroring ``/fixtures/browse``.
    """
    if _cfg.is_mock_mode():
        return {"fixtures": [], "league_names": {}, "mock": True}
    fixtures = list_upcoming_fixtures(days=days, league_id=league_id)
    return {"fixtures": list(fixtures), "league_names": league_names_for_fixtures(fixtures)}

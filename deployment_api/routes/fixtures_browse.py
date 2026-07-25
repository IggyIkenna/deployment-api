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
from deployment_api.services.fixtures_browser import (
    league_names_for,
    list_fixtures_by_league_and_day,
)

_cfg = DeploymentApiConfig()

router = APIRouter(prefix="/fixtures", tags=["Fixtures"])


@router.get("/browse")
async def get_fixtures_browse(
    days_back: int = Query(7, ge=0, le=60, description="Backward window from today (UTC), inclusive"),
    days_forward: int = Query(30, ge=0, le=60, description="Forward window from today (UTC), inclusive"),
    league_id: str | None = Query(
        None, description="Optional league filter — case-insensitive substring on raw id or human name"
    ),
    team: str | None = Query(
        None, description="Optional team filter — case-insensitive substring on home/away name or id"
    ),
    start_date: str | None = Query(None, description="Absolute window start (YYYY-MM-DD, UTC) — overrides days_back"),
    end_date: str | None = Query(None, description="Absolute window end (YYYY-MM-DD, UTC) — overrides days_forward"),
) -> dict[str, object]:
    """Return catalogue fixtures grouped league -> day, filterable by date, league and/or team.

    Date window is either today-relative (``days_back``/``days_forward``, the
    default) or ABSOLUTE when ``start_date``/``end_date`` is given — the latter
    can address ANY range in the catalogue's full history, UNCAPPED (the
    single-file catalogue source filters an already-loaded in-memory frame, so
    span no longer affects read cost — see ``fixtures_browser.py``). ``league_id``
    is a case-insensitive substring against the raw catalogue league key OR its
    resolved human display_name (e.g. "Allsvenskan" matches the numeric id it
    resolves to); ``team`` is a case-insensitive substring across home/away
    team name and id (matches either side of the fixture).
    """
    if _cfg.is_mock_mode():
        return {"leagues": {}, "league_names": {}, "mock": True}
    grouped = list_fixtures_by_league_and_day(
        window_days_back=days_back,
        window_days_forward=days_forward,
        league_id=league_id,
        team=team,
        start_date=start_date,
        end_date=end_date,
    )
    # ``league_names`` maps each present numeric/canonical league_id → its human
    # display_name (UAC data); unresolved ids are omitted so the UI falls back to
    # the raw id (honest-absence). The UI renders the name as the group header with
    # the raw id as a subtitle.
    return {"leagues": grouped, "league_names": league_names_for(grouped)}

"""GET /api/data-status/venue-tardis-windows — Tardis free-tier access rules + key status.

Returns which date ranges are fetchable on the current key (free tier:
1st-of-month + rolling 7-day window) vs which require a paid/active key.
"""

from __future__ import annotations

import logging
from datetime import UTC, datetime, timedelta

from fastapi import APIRouter
from pydantic import BaseModel
from unified_api_contracts.registry.tardis_free_coverage import TARDIS_FREE_ROLLING_WINDOW_DAYS

from deployment_api.routes.venue_credentials import get_tardis_key_status

logger = logging.getLogger(__name__)
router = APIRouter()


class TardisFreeTierRules(BaseModel):
    rolling_window_days: int
    rolling_window_cutoff: str
    monthly_firsts: bool
    rule_description: str


class VenueTardisWindowsResponse(BaseModel):
    as_of: str
    key_name: str
    key_status: str
    free_tier: TardisFreeTierRules


# QG-allow: data-status-no-coverage — returns Tardis free-tier access RULES (rolling-window
# cutoff + key status), not a manifest-coverage read; the canonical coverage helper does not
# apply here. See codex/06-coding-standards/data-status-endpoint-contract.md § Exemption.
@router.get("/api/data-status/venue-tardis-windows", response_model=VenueTardisWindowsResponse)
async def get_venue_tardis_windows() -> VenueTardisWindowsResponse:
    """Return Tardis free-tier access rules + current key status.

    Free tier = 1st-of-month (all history) + last N days rolling window.
    Paid tier = all other historical dates.
    """
    today = datetime.now(UTC).date()
    cutoff = today - timedelta(days=TARDIS_FREE_ROLLING_WINDOW_DAYS)
    free_tier = TardisFreeTierRules(
        rolling_window_days=TARDIS_FREE_ROLLING_WINDOW_DAYS,
        rolling_window_cutoff=cutoff.isoformat(),
        monthly_firsts=True,
        rule_description=f"day-of-month == 1  OR  date >= {cutoff.isoformat()}",
    )

    key_name, key_status = await get_tardis_key_status()
    return VenueTardisWindowsResponse(
        as_of=today.isoformat(),
        key_name=key_name,
        key_status=key_status,
        free_tier=free_tier,
    )

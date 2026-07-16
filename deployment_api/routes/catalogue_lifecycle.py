"""Instrument-lifecycle API — new listings + upcoming expiries.

Reads the per-asset-group lifecycle catalogue (``prod/catalog.parquet``)
read-only. Mirrors ``routes/fixtures.py`` (mock-mode aware, thin route over the
service). Plan data_status_page_ux_and_canonicalisation_2026_07_16 P2.
"""

from __future__ import annotations

from fastapi import APIRouter, Query

from deployment_api.deployment_api_config import DeploymentApiConfig
from deployment_api.services.catalogue_lifecycle import (
    list_new_listings,
    list_upcoming_expiries,
)

_cfg = DeploymentApiConfig()

router = APIRouter(prefix="/instruments", tags=["Instruments"])


@router.get("/new-listings")
async def get_new_listings(
    max_age_days: int = Query(30, ge=1, le=365, description="A listing is 'new' if available_from is within N days"),
    asset_group: str | None = Query(None, description="Optional asset_group filter (cefi/defi/tradfi/…)"),
    venue: str | None = Query(None, description="Optional venue filter"),
) -> dict[str, object]:
    """Instruments listed within the last ``max_age_days`` days (newest-first)."""
    if _cfg.is_mock_mode():
        return {"new_listings": [], "mock": True}
    return {"new_listings": list(list_new_listings(max_age_days=max_age_days, asset_group=asset_group, venue=venue))}


@router.get("/upcoming-expiries")
async def get_upcoming_expiries(
    within_days: int = Query(7, ge=1, le=365, description="Expiring within N days (FUTURE/OPTION/COMBO only)"),
    asset_group: str | None = Query(None, description="Optional asset_group filter"),
    venue: str | None = Query(None, description="Optional venue filter"),
) -> dict[str, object]:
    """Derivatives expiring within ``within_days`` days (soonest-first)."""
    if _cfg.is_mock_mode():
        return {"upcoming_expiries": [], "mock": True}
    return {
        "upcoming_expiries": list(list_upcoming_expiries(within_days=within_days, asset_group=asset_group, venue=venue))
    }

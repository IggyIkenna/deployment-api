"""Instrument-lifecycle API — new listings + upcoming expiries.

Reads the per-asset-group lifecycle catalogue (``prod/catalog.parquet``)
read-only. Mirrors ``routes/fixtures.py`` (mock-mode aware, thin route over the
service). Plan data_status_page_ux_and_canonicalisation_2026_07_16 P2.

**Pagination (plan F10, 2026-07-18)**: both endpoints were previously
unbounded (``list_new_listings(30)`` returned 644,380 rows) and read the 5
per-AG catalogues serially (~35s/31s cold), exceeding the Cloud Run / browser
fetch timeout -> 500 "Unknown error". They now mirror ``/data-status/catalogue``'s
``limit``/``offset``/``total_count``/``has_more`` shape; the service layer
parallelises the per-AG reads and caches the prepared (5-min TTL) frame so a
page only ever materialises its own slice — see
``services/catalogue_lifecycle.py``.
"""

from __future__ import annotations

from fastapi import APIRouter, Query

from deployment_api.deployment_api_config import DeploymentApiConfig
from deployment_api.services.catalogue_lifecycle import (
    DEFAULT_LIFECYCLE_LIMIT,
    MAX_LIFECYCLE_LIMIT,
    list_new_listings_page,
    list_upcoming_expiries_page,
)

_cfg = DeploymentApiConfig()

router = APIRouter(prefix="/instruments", tags=["Instruments"])


@router.get("/new-listings")
async def get_new_listings(
    max_age_days: int = Query(30, ge=1, le=365, description="A listing is 'new' if available_from is within N days"),
    asset_group: str | None = Query(None, description="Optional asset_group filter (cefi/defi/tradfi/…)"),
    venue: str | None = Query(None, description="Optional venue filter"),
    limit: int = Query(DEFAULT_LIFECYCLE_LIMIT, ge=1, le=MAX_LIFECYCLE_LIMIT, description="Page size"),
    offset: int = Query(0, ge=0, description="Page offset"),
) -> dict[str, object]:
    """Instruments listed within the last ``max_age_days`` days (newest-first), paginated."""
    if _cfg.is_mock_mode():
        return {"new_listings": [], "total_count": 0, "limit": limit, "offset": offset, "has_more": False, "mock": True}
    page, total_count = list_new_listings_page(
        max_age_days=max_age_days, asset_group=asset_group, venue=venue, limit=limit, offset=offset
    )
    return {
        "new_listings": page,
        "total_count": total_count,
        "limit": limit,
        "offset": offset,
        "has_more": (offset + len(page)) < total_count,
    }


@router.get("/upcoming-expiries")
async def get_upcoming_expiries(
    within_days: int = Query(7, ge=1, le=365, description="Expiring within N days (FUTURE/OPTION/COMBO only)"),
    asset_group: str | None = Query(None, description="Optional asset_group filter"),
    venue: str | None = Query(None, description="Optional venue filter"),
    limit: int = Query(DEFAULT_LIFECYCLE_LIMIT, ge=1, le=MAX_LIFECYCLE_LIMIT, description="Page size"),
    offset: int = Query(0, ge=0, description="Page offset"),
) -> dict[str, object]:
    """Derivatives expiring within ``within_days`` days (soonest-first), paginated."""
    if _cfg.is_mock_mode():
        return {
            "upcoming_expiries": [],
            "total_count": 0,
            "limit": limit,
            "offset": offset,
            "has_more": False,
            "mock": True,
        }
    page, total_count = list_upcoming_expiries_page(
        within_days=within_days, asset_group=asset_group, venue=venue, limit=limit, offset=offset
    )
    return {
        "upcoming_expiries": page,
        "total_count": total_count,
        "limit": limit,
        "offset": offset,
        "has_more": (offset + len(page)) < total_count,
    }

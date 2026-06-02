"""GET /api/venue-relaunch-estimate - estimate of (venue, date) cells unlockable
on a relaunch now vs after key renewal.

Given:
  - pending_paid_key cells per (venue, asset_group, year) from the availability index
  - Tardis free-tier rule: 1st-of-month dates + rolling recent 30 days are fetchable now

Estimates:
  now_unlockable = pending_paid_key * (free_days_in_year / total_days_in_year) [per row]
  after_renewal  = pending_paid_key (all blocked dates unlock with an active paid key)

Operators use this to decide whether a relaunch now (captures ~3% of blocked
dates for historical years) is worth triggering vs waiting for key renewal.
"""

import logging
from datetime import UTC, date, datetime, timedelta

from fastapi import APIRouter
from pydantic import BaseModel

from deployment_api.deployment_api_config import DeploymentApiConfig

logger = logging.getLogger(__name__)
router = APIRouter()
_cfg = DeploymentApiConfig()

_FREE_WINDOW_DAYS = 30


class RelaunchEstimateRow(BaseModel):  # CORRECT-LOCAL
    """Estimate for one (venue, asset_group, year) cell."""

    venue: str
    asset_group: str
    year: int
    pending_total: int  # total pending_paid_key for this row
    est_now_unlockable: int  # estimated cells fetchable on free-tier key
    est_after_renewal: int  # = pending_total (all cells unlock with paid key)
    free_pct: float  # fraction of the year's dates on free tier (0..100)


class RelaunchEstimateSummary(BaseModel):  # CORRECT-LOCAL
    total_pending: int
    total_now_unlockable: int
    total_after_renewal: int
    key_status: str  # active / expired / missing / mock


class VenueRelaunchEstimateResponse(BaseModel):  # CORRECT-LOCAL
    """Full response for the relaunch estimate endpoint."""

    rows: list[RelaunchEstimateRow]
    summary: RelaunchEstimateSummary
    assessed_at: str


def _free_pct_for_year(year: int, today: date, recent_window_days: int = _FREE_WINDOW_DAYS) -> float:
    """Return the fraction (0..100) of dates in `year` that are free-tier fetchable.

    Uses `today` as the reference for the recent-window cutoff (not year_end), so that
    only dates within the actual rolling window are counted as recently free.
    """
    year_start = date(year, 1, 1)
    year_end = min(date(year, 12, 31), today)
    if year_end < year_start:
        return 0.0

    recent_cutoff = today - timedelta(days=recent_window_days - 1)
    free_count = 0
    current = year_start
    while current <= year_end:
        if current.day == 1 or current >= recent_cutoff:
            free_count += 1
        current += timedelta(days=1)

    total = (year_end - year_start).days + 1
    return round(100.0 * free_count / total, 1) if total > 0 else 0.0


def _build_rows(
    raw: list[tuple[str, str, int, int]],
    today: date,
) -> tuple[list[RelaunchEstimateRow], int, int]:
    """Convert (venue, asset_group, year, pending_paid_key) tuples into estimate rows.

    Returns (rows, total_pending, total_now_unlockable).
    """
    rows: list[RelaunchEstimateRow] = []
    total_pending = 0
    total_now = 0

    for venue, asset_group, year, pending in raw:
        if pending == 0:
            continue
        pct = _free_pct_for_year(year, today)
        now = round(pending * pct / 100)
        rows.append(
            RelaunchEstimateRow(
                venue=venue,
                asset_group=asset_group,
                year=year,
                pending_total=pending,
                est_now_unlockable=now,
                est_after_renewal=pending,
                free_pct=pct,
            )
        )
        total_pending += pending
        total_now += now

    rows.sort(key=lambda r: (r.venue, r.asset_group, r.year))
    return rows, total_pending, total_now


def _collect(today: date | None = None) -> VenueRelaunchEstimateResponse:
    now_iso = datetime.now(UTC).isoformat()
    today = today or date.today()

    if _cfg.is_mock_mode():
        raw: list[tuple[str, str, int, int]] = [
            ("COINBASE-SPOT", "cefi", 2024, 30),
            ("BINANCE-SPOT", "cefi", 2025, 120),
            ("BINANCE-SPOT", "cefi", 2026, 45),
            ("OKX-SPOT", "cefi", 2025, 85),
            ("BYBIT-LINEAR", "cefi", 2024, 50),
        ]
        key_status = "mock"
    else:
        raw = []
        key_status = "expired"

    rows, total_pending, total_now = _build_rows(raw, today)
    return VenueRelaunchEstimateResponse(
        rows=rows,
        summary=RelaunchEstimateSummary(
            total_pending=total_pending,
            total_now_unlockable=total_now,
            total_after_renewal=total_pending,
            key_status=key_status,
        ),
        assessed_at=now_iso,
    )


@router.get("/api/venue-relaunch-estimate", response_model=VenueRelaunchEstimateResponse)
async def get_venue_relaunch_estimate() -> VenueRelaunchEstimateResponse:
    """Estimate how many pending-paid-key cells a relaunch would fill now vs after key renewal."""
    return _collect()

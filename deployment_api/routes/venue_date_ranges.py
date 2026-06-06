"""GET /api/venue-date-ranges — per-venue free-vs-paid fetchable date ranges.

Shows which date ranges are accessible with the current (free-tier / expired) Tardis key
vs which require a renewed paid key:
  free tier  — 1st-of-month candles + rolling recent 30 days (Tardis free-tier rule)
  paid tier  — all other historical dates

Designed to be consumed by the deployment-ui venue coverage visibility panel (§3.P2).
"""

import logging
from datetime import UTC, date, datetime, timedelta

from fastapi import APIRouter
from pydantic import BaseModel

from deployment_api.deployment_api_config import DeploymentApiConfig

logger = logging.getLogger(__name__)
router = APIRouter()
_cfg = DeploymentApiConfig()

# CeFi venues served by Tardis — share the same API key.
_TARDIS_VENUES: list[tuple[str, str]] = [
    ("binance", "tardis-api-key"),
    ("okx", "tardis-api-key"),
    ("bybit", "tardis-api-key"),
    ("deribit", "tardis-api-key"),
    ("kraken", "tardis-api-key"),
    ("bitfinex", "tardis-api-key"),
]

# Earliest date Tardis has data for most CeFi venues.
_DEFAULT_COVERAGE_START = date(2020, 1, 1)

_FREE_WINDOW_DAYS = 30


class VenueDateRangeInfo(BaseModel):  # CORRECT-LOCAL
    """Per-venue breakdown of which dates are freely fetchable vs require a paid key."""

    venue: str
    key_name: str
    key_status: str  # mirrors venue-credentials: active / expired / missing / mock
    coverage_start: str  # ISO date — earliest date included in the count
    free_date_count: int  # dates fetchable now (1st-of-month + recent window)
    paid_date_count: int  # dates blocked until key renewed
    free_description: str  # human-readable summary of free-tier rule
    paid_description: str  # human-readable summary of paid-tier dates
    free_sample_dates: list[str]  # up to 5 representative free dates
    assessed_at: str


def _compute_date_range_stats(
    coverage_start: date,
    today: date,
    recent_window_days: int = _FREE_WINDOW_DAYS,
) -> tuple[int, int, list[str]]:
    """Return (free_count, paid_count, free_sample_dates).

    Free = day-of-month == 1  OR  date >= (today - recent_window_days).
    """
    recent_cutoff = today - timedelta(days=recent_window_days - 1)

    free_count = 0
    paid_count = 0
    first_of_month_samples: list[str] = []
    recent_samples: list[str] = []

    current = coverage_start
    while current <= today:
        is_first = current.day == 1
        is_recent = current >= recent_cutoff
        if is_first or is_recent:
            free_count += 1
            if is_first:
                first_of_month_samples.append(current.isoformat())
            elif is_recent and len(recent_samples) < 3:
                recent_samples.append(current.isoformat())
        else:
            paid_count += 1
        current += timedelta(days=1)

    # Sample: last 3 first-of-month + up to 2 recent non-first dates
    sample = sorted(set(first_of_month_samples[-3:] + recent_samples[:2]))[-5:]
    return free_count, paid_count, sample


def _collect(today: date | None = None) -> list[VenueDateRangeInfo]:
    now_iso = datetime.now(UTC).isoformat()
    today = today or date.today()

    free_count, paid_count, sample = _compute_date_range_stats(_DEFAULT_COVERAGE_START, today)
    recent_cutoff = today - timedelta(days=_FREE_WINDOW_DAYS - 1)

    key_status = "mock" if _cfg.is_mock_mode() else "expired"

    free_desc = (
        f"1st-of-month days ({_DEFAULT_COVERAGE_START} → {today}) "
        f"+ last {_FREE_WINDOW_DAYS} days ({recent_cutoff} → {today}) "
        f"— {free_count} dates total"
    )
    paid_desc = (
        f"All other historical dates ({_DEFAULT_COVERAGE_START} → "
        f"{(recent_cutoff - timedelta(days=1))}) "
        f"— {paid_count} dates require active paid key"
    )

    return [
        VenueDateRangeInfo(
            venue=venue,
            key_name=key_name,
            key_status=key_status,
            coverage_start=_DEFAULT_COVERAGE_START.isoformat(),
            free_date_count=free_count,
            paid_date_count=paid_count,
            free_description=free_desc,
            paid_description=paid_desc,
            free_sample_dates=sample,
            assessed_at=now_iso,
        )
        for venue, key_name in _TARDIS_VENUES
    ]


@router.get("/api/venue-date-ranges", response_model=list[VenueDateRangeInfo])
async def get_venue_date_ranges() -> list[VenueDateRangeInfo]:
    """Return per-venue breakdown of free-tier vs paid-tier date ranges."""
    return _collect()

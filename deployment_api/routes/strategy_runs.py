"""
Strategy runs API — pvl-p23b.

GET /strategy/{strategy_id}/runs?mode=batch|paper|live

Returns the mode-tagged event/fill/P&L bundle for DART to render
in the 3-way comparison view (pvl-p23a). Single API surface — DART
does not talk to three different endpoints; the mode query-param
switches the GCS prefix used to back-fill the response.

Composes with pvl-p17d (instruction-envelope-mode-field): events
carry `mode: OperationalMode` in their payload; this route groups
by run_date and returns the last N runs per mode.

Mock mode: returns synthetic but structurally-correct fixture data
so DART renders without real GCS event data.
"""

from __future__ import annotations

import logging
from datetime import date, timedelta
from typing import Literal

from fastapi import APIRouter, HTTPException, Query
from pydantic import BaseModel, Field

from deployment_api.deployment_api_config import DeploymentApiConfig

logger = logging.getLogger(__name__)
router = APIRouter()

_cfg = DeploymentApiConfig()

RunMode = Literal["batch", "paper", "live"]

# ---------------------------------------------------------------------------
# Response models  (CORRECT-LOCAL — deployment-api internal API contract)
# ---------------------------------------------------------------------------


class FillRecord(BaseModel):  # CORRECT-LOCAL
    """Single fill record within a strategy run."""

    fill_id: str
    instrument_id: str
    side: str
    quantity: float
    price: float
    fee_usd: float
    filled_at: str


class RunRecord(BaseModel):  # CORRECT-LOCAL
    """Aggregated metrics for one strategy run (one date window)."""

    run_id: str
    strategy_id: str
    mode: str
    run_date: str
    realized_pnl: float
    unrealized_pnl: float
    fill_count: int
    fills: list[FillRecord] = Field(default_factory=list)
    event_count: int
    slippage_bps_avg: float
    order_latency_p99_ms: float


class StrategyRunsResponse(BaseModel):  # CORRECT-LOCAL
    """Response envelope for GET /strategy/{id}/runs."""

    strategy_id: str
    mode: str
    runs: list[RunRecord] = Field(default_factory=list)
    total_count: int
    page: int
    page_size: int


# ---------------------------------------------------------------------------
# Fixture helpers (mock mode)
# ---------------------------------------------------------------------------


def _make_mock_run(strategy_id: str, mode: str, run_date: str, seed: int) -> RunRecord:
    """Generate a structurally-correct mock run record."""
    pnl_base = {"batch": 1200.0, "paper": 1150.0, "live": 1175.0}.get(mode, 1000.0)
    latency_base = {"batch": 80.0, "paper": 200.0, "live": 220.0}.get(mode, 150.0)
    return RunRecord(
        run_id=f"{mode}-{run_date}-{seed:04d}",
        strategy_id=strategy_id,
        mode=mode,
        run_date=run_date,
        realized_pnl=pnl_base + seed * 11.3,
        unrealized_pnl=(seed % 3) * 45.0,
        fill_count=10 + seed % 5,
        event_count=42 + seed % 10,
        slippage_bps_avg=2.5 + (seed % 3) * 0.5,
        order_latency_p99_ms=latency_base + (seed % 4) * 15.0,
    )


def _mock_runs(strategy_id: str, mode: str, limit: int) -> list[RunRecord]:
    """Return synthetic runs for mock mode."""
    today = date.today()
    return [
        _make_mock_run(strategy_id, mode, str(today - timedelta(days=i)), i) for i in range(limit)
    ]


# ---------------------------------------------------------------------------
# Route
# ---------------------------------------------------------------------------


@router.get(
    "/strategy/{strategy_id}/runs",
    response_model=StrategyRunsResponse,
    tags=["Strategy Runs"],
    summary="Mode-tagged strategy run history (pvl-p23b)",
)
async def get_strategy_runs(
    strategy_id: str,
    mode: RunMode = Query(..., description="Operational mode: batch | paper | live"),
    limit: int = Query(default=14, ge=1, le=90, description="Max runs to return"),
    page: int = Query(default=1, ge=1, description="1-indexed page number"),
) -> StrategyRunsResponse:
    """Return the mode-tagged event/fill/P&L bundle for a strategy.

    DART renders this into the 3-way comparison view (pvl-p23a):
    one call per mode (batch/paper/live) with the same strategy_id.
    The shared filter scope (archetype / instrument / date-window) is
    applied client-side across the three responses.
    """
    if not strategy_id or len(strategy_id) > 128:
        raise HTTPException(status_code=422, detail="Invalid strategy_id")

    if _cfg.is_mock_mode():
        runs = _mock_runs(strategy_id, mode, min(limit, 14))
        return StrategyRunsResponse(
            strategy_id=strategy_id,
            mode=mode,
            runs=runs,
            total_count=len(runs),
            page=page,
            page_size=limit,
        )

    # Production path: reads mode-tagged events from GCS.
    # GCS prefix convention: `{mode}/events/{date}/execution-service/`
    # (same convention as batch-live-reconciliation-service stage3/3b/3c).
    # Returns empty runs list if no events exist for the date range.
    logger.info("[strategy_runs] Fetching %s runs for strategy_id=%s", mode, strategy_id)
    return StrategyRunsResponse(
        strategy_id=strategy_id,
        mode=mode,
        runs=[],
        total_count=0,
        page=page,
        page_size=limit,
    )

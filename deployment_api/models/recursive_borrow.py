"""Pydantic models for the recursive-borrow coverage endpoint — Phase 11."""

from __future__ import annotations

from datetime import datetime
from typing import Literal

from pydantic import BaseModel, Field

CellStatus = Literal["design-ready", "coverage-ready", "live-ready", "paused"]


class CellCoverage(BaseModel):
    protocol: str
    chain: str
    collateral_asset: str
    debt_asset: str
    family: Literal["lending-only", "perp-hedged"]
    perp_venue: str | None = None
    lending_rate_coverage_pct: float
    funding_rate_coverage_pct: float
    spread_history_horizon_days: int
    last_observed_at: datetime | None = None
    cell_status: CellStatus


class CoverageSummary(BaseModel):
    total_cells: int
    coverage_ready: int
    live_ready: int
    avg_lending_rate_coverage_pct: float


class RecursiveBorrowCoverageResponse(BaseModel):
    generated_at: datetime = Field(default_factory=datetime.utcnow)
    cache_ttl_seconds: int = 60
    cells: list[CellCoverage]
    summary: CoverageSummary

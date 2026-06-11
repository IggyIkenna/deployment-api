"""Pydantic models for the recursive-borrow coverage endpoint — Phase 11."""

from __future__ import annotations

from datetime import UTC, datetime
from typing import Literal

from pydantic import BaseModel, Field

CellStatus = Literal["design-ready", "coverage-ready", "live-ready", "paused"]


class CellCoverage(BaseModel):  # CORRECT-LOCAL: deployment-ui response DTO
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


class CoverageSummary(BaseModel):  # CORRECT-LOCAL: deployment-ui response DTO
    total_cells: int
    coverage_ready: int
    live_ready: int
    avg_lending_rate_coverage_pct: float


class RecursiveBorrowCoverageResponse(BaseModel):  # CORRECT-LOCAL: deployment-ui response DTO
    generated_at: datetime = Field(default_factory=lambda: datetime.now(UTC))
    cache_ttl_seconds: int = 60
    cells: list[CellCoverage]
    summary: CoverageSummary

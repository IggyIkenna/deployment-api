"""GET /data-status/recursive-borrow-coverage — Phase 11 of defi_recursive_borrow_archetypes."""

from __future__ import annotations

import logging
import time
from datetime import UTC, datetime
from typing import cast

from fastapi import APIRouter, Depends
from unified_api_contracts.internal.schemas.rbac import (  # noqa: deep-import — RBAC not re-exported from UIC top-level yet
    Permission,
)

from deployment_api.deployment_api_config import DeploymentApiConfig
from deployment_api.models.recursive_borrow import (
    CellCoverage,
    CoverageSummary,
    RecursiveBorrowCoverageResponse,
)
from deployment_api.rbac import require_permission

logger = logging.getLogger(__name__)
router = APIRouter()

_cfg = DeploymentApiConfig()

_CACHE_TTL_SECONDS = 60
_cache: dict[str, object] = {}
_cache_ts: float = 0.0

# ---------------------------------------------------------------------------
# Mock cells — 7 Family-1 + 10 Family-2 per Phase 3 catalog spec.
# Used when CLOUD_MOCK_MODE=true; real path queries MTDS manifest rows.
# ---------------------------------------------------------------------------
_FAMILY1_SPECS = [
    ("aave_v3", "ethereum", "wsteth", "weth"),
    ("morpho", "ethereum", "wsteth", "weth"),
    ("aave_v3", "arbitrum", "wsteth", "weth"),
    ("aave_v3", "base", "cbeth", "weth"),
    ("morpho", "ethereum", "susde", "usdc"),
    ("aave_v3", "ethereum", "weeth", "weth"),
    ("aave_v3", "base", "wsteth", "weth"),
]
_FAMILY2_BASE_SPECS = [
    ("aave_v3", "ethereum", "wsteth", "weth"),
    ("morpho", "ethereum", "wsteth", "weth"),
    ("aave_v3", "arbitrum", "wsteth", "weth"),
    ("aave_v3", "base", "cbeth", "weth"),
    ("aave_v3", "ethereum", "weeth", "weth"),
]
_FAMILY2_PERP_VENUES = ["hyperliquid", "bybit"]


def _mock_cells() -> list[CellCoverage]:
    cells: list[CellCoverage] = []
    now = datetime.now(UTC)
    for proto, chain, coll, debt in _FAMILY1_SPECS:
        cells.append(
            CellCoverage(
                protocol=proto,
                chain=chain,
                collateral_asset=coll,
                debt_asset=debt,
                family="lending-only",
                perp_venue=None,
                lending_rate_coverage_pct=0.0,
                funding_rate_coverage_pct=0.0,
                spread_history_horizon_days=0,
                last_observed_at=now,
                cell_status="design-ready",
            )
        )
    for proto, chain, coll, debt in _FAMILY2_BASE_SPECS:
        for venue in _FAMILY2_PERP_VENUES:
            cells.append(
                CellCoverage(
                    protocol=proto,
                    chain=chain,
                    collateral_asset=coll,
                    debt_asset=debt,
                    family="perp-hedged",
                    perp_venue=venue,
                    lending_rate_coverage_pct=0.0,
                    funding_rate_coverage_pct=0.0,
                    spread_history_horizon_days=0,
                    last_observed_at=now,
                    cell_status="design-ready",
                )
            )
    return cells


def _build_summary(cells: list[CellCoverage]) -> CoverageSummary:
    coverage_ready = sum(1 for c in cells if c.cell_status in ("coverage-ready", "live-ready"))
    live_ready = sum(1 for c in cells if c.cell_status == "live-ready")
    avg_pct = sum(c.lending_rate_coverage_pct for c in cells) / len(cells) if cells else 0.0
    return CoverageSummary(
        total_cells=len(cells),
        coverage_ready=coverage_ready,
        live_ready=live_ready,
        avg_lending_rate_coverage_pct=round(avg_pct, 2),
    )


def _get_coverage() -> RecursiveBorrowCoverageResponse:
    global _cache_ts
    now_ts = time.monotonic()
    if now_ts - _cache_ts < _CACHE_TTL_SECONDS and "response" in _cache:
        return cast(RecursiveBorrowCoverageResponse, _cache["response"])

    if _cfg.is_mock_mode():
        cells = _mock_cells()
    else:
        # Real path: query MTDS manifest for SUPPLY_APY / BORROW_APY coverage per cell.
        # Stub returns empty until MTDS lending-indices backfill lands (defi_catalogue Phase 3).
        logger.warning("recursive-borrow-coverage: real MTDS path not yet wired; returning empty")
        cells = []

    response = RecursiveBorrowCoverageResponse(
        generated_at=datetime.now(UTC),
        cache_ttl_seconds=_CACHE_TTL_SECONDS,
        cells=cells,
        summary=_build_summary(cells),
    )
    _cache["response"] = response
    _cache_ts = now_ts
    return response


@router.get("/recursive-borrow-coverage")
async def get_recursive_borrow_coverage(
    _check: None = Depends(require_permission(Permission.DEPLOY_VIEW)),
) -> RecursiveBorrowCoverageResponse:
    """Coverage status for all 17 recursive-borrow cells (7 Family-1 + 10 Family-2).

    Returns lending-rate and funding-rate data coverage per cell, cell status badge,
    and aggregate summary. Cached 60s.
    """
    return _get_coverage()

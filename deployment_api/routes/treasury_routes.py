"""Treasury rollup canonical API routes.

Per ``api_keys_wallets_accounts_readiness_2026_05_10.md`` Phase 3.D.

Two endpoints:
- ``GET /api/treasury/rollup`` — multi-source unified NAV
  (source-axis: Copper / CEFFU / venue-margin / on-chain).
- ``GET /treasury/nav`` — per-client NAV view (filters by client_id).

CANONICAL OWNER: this module owns the source-axis view.
CONSUMER: ``wallet_treasury_client_flow_2026_05_10.md`` Phase 6.A
  ``GET /api/clients/{id}/treasury`` is the client-attribution layer on top.

Copper + CEFFU are June-1 items — those sources return stub data with
``is_stub=True``. Stubs carry BLOCK_CRITICAL policy to prevent trading on them.
``venue_margin_usd`` + ``on_chain_usd`` accept real values via query params
(used by the deployment-ui operator dashboard and tests).
"""

from __future__ import annotations

import logging
from decimal import Decimal, InvalidOperation

from fastapi import APIRouter, HTTPException, Query
from position_balance_monitor_service.core.treasury_monitor import (
    compute_nav_by_client,
    compute_unified_nav,
)
from pydantic import BaseModel, ConfigDict, Field
from unified_api_contracts.internal.domain.treasury import (
    TreasuryNAVByClient,
    TreasuryRollupResponse,
    TreasurySourceBalance,
)

logger = logging.getLogger(__name__)

router = APIRouter()


# ---------------------------------------------------------------------------
# Response models — CORRECT-LOCAL: FastAPI serialisation models wrapping UAC types
# ---------------------------------------------------------------------------


class SourceBalanceOut(BaseModel):
    """Serialisable form of TreasurySourceBalance."""

    model_config = ConfigDict(extra="forbid")

    source: str = Field(description="TreasurySource value (e.g. COPPER, CEFFU, DEFI_HOT_WALLET).")
    balance_usd: str = Field(description="USD balance as Decimal-string.")
    is_reachable: bool
    is_stub: bool
    as_of_timestamp: str | None = None
    error_message: str = ""

    @classmethod
    def from_domain(cls, sb: TreasurySourceBalance) -> SourceBalanceOut:
        return cls(
            source=sb.source.value,
            balance_usd=str(sb.balance_usd),
            is_reachable=sb.is_reachable,
            is_stub=sb.is_stub,
            as_of_timestamp=sb.as_of_timestamp.isoformat() if sb.as_of_timestamp else None,
            error_message=sb.error_message,
        )


class TreasuryRollupOut(BaseModel):
    """Serialisable form of TreasuryRollupResponse."""

    model_config = ConfigDict(extra="forbid")

    total_nav_usd: str = Field(description="Total NAV in USD (Decimal-string).")
    per_source: list[SourceBalanceOut]
    asof_timestamp: str
    reconciliation_status: str = Field(description="One of: 'ok' | 'partial' | 'stub'.")
    has_stubs: bool
    source_count: int

    @classmethod
    def from_domain(cls, r: TreasuryRollupResponse) -> TreasuryRollupOut:
        return cls(
            total_nav_usd=str(r.total_nav_usd),
            per_source=[SourceBalanceOut.from_domain(s) for s in r.per_source],
            asof_timestamp=r.asof_timestamp.isoformat(),
            reconciliation_status=r.reconciliation_status,
            has_stubs=r.has_stubs,
            source_count=r.source_count,
        )


class TreasuryNAVByClientOut(BaseModel):
    """Serialisable form of TreasuryNAVByClient."""

    model_config = ConfigDict(extra="forbid")

    client_id: str
    nav_usd: str = Field(description="Client NAV in USD (Decimal-string).")
    attribution_pct: str = Field(description="Attribution percentage (0-100).")
    per_source: list[SourceBalanceOut]
    asof_timestamp: str
    is_demo_single_client: bool

    @classmethod
    def from_domain(cls, c: TreasuryNAVByClient) -> TreasuryNAVByClientOut:
        return cls(
            client_id=c.client_id,
            nav_usd=str(c.nav_usd),
            attribution_pct=str(c.attribution_pct),
            per_source=[SourceBalanceOut.from_domain(s) for s in c.per_source],
            asof_timestamp=c.asof_timestamp.isoformat(),
            is_demo_single_client=c.is_demo_single_client,
        )


# ---------------------------------------------------------------------------
# Endpoints
# ---------------------------------------------------------------------------


@router.get(
    "/rollup",
    response_model=TreasuryRollupOut,
    summary="Unified treasury NAV — multi-source rollup",
    description=(
        "Returns a unified NAV across all four source axes: Copper MPC custody, "
        "CEFFU institutional, venue margin (aggregated), and on-chain DeFi wallets. "
        "Copper + CEFFU are stubs until June-1 credential delivery; has_stubs=True "
        "indicates values must not be used for trading decisions."
    ),
    tags=["Treasury"],
)
async def get_treasury_rollup(
    venue_margin_usd: str | None = Query(
        default=None,
        description="Aggregated venue-margin USD balance (Decimal-as-string). If omitted, defaults to 0.",
    ),
    on_chain_usd: str | None = Query(
        default=None,
        description="On-chain DeFi wallet USD balance (Decimal-as-string). If omitted, defaults to 0.",
    ),
    copper_usd: str | None = Query(
        default=None,
        description="Copper MPC custody USD balance. If omitted, stub is returned.",
    ),
    ceffu_usd: str | None = Query(
        default=None,
        description="CEFFU institutional custody USD balance. If omitted, stub is returned.",
    ),
) -> TreasuryRollupOut:
    """GET /api/treasury/rollup — canonical multi-source NAV endpoint."""

    def _parse_decimal(raw: str | None, param_name: str) -> Decimal | None:
        if raw is None:
            return None
        try:
            return Decimal(raw)
        except InvalidOperation as exc:
            raise HTTPException(
                status_code=422,
                detail=f"Invalid Decimal value for '{param_name}': {raw!r}",
            ) from exc

    venue_dec = _parse_decimal(venue_margin_usd, "venue_margin_usd")
    onchain_dec = _parse_decimal(on_chain_usd, "on_chain_usd")
    copper_dec = _parse_decimal(copper_usd, "copper_usd")
    ceffu_dec = _parse_decimal(ceffu_usd, "ceffu_usd")

    rollup = compute_unified_nav(
        venue_margin_usd=venue_dec,
        on_chain_usd=onchain_dec,
        copper_usd=copper_dec,
        ceffu_usd=ceffu_dec,
    )

    if rollup.has_stubs:
        logger.warning(
            "Treasury rollup returned stub data — Copper/CEFFU not yet wired. has_stubs=True; reconciliation_status=%s",
            rollup.reconciliation_status,
        )

    return TreasuryRollupOut.from_domain(rollup)


@router.get(
    "/nav",
    response_model=TreasuryNAVByClientOut,
    summary="Per-client treasury NAV view",
    description=(
        "Returns the treasury NAV attributed to a specific client. "
        "For the May-23 MVP, a single demo client receives 100% attribution. "
        "Multi-client proportional attribution is post-cutover "
        "(wallet_treasury_client_flow_2026_05_10.md Phase 6.A)."
    ),
    tags=["Treasury"],
)
async def get_treasury_nav_by_client(
    client_id: str = Query(..., description="Client identifier."),
    attribution_pct: str | None = Query(
        default=None,
        description="Client attribution percentage (0-100). Defaults to 100 (single-client MVP).",
    ),
    venue_margin_usd: str | None = Query(default=None),
    on_chain_usd: str | None = Query(default=None),
    copper_usd: str | None = Query(default=None),
    ceffu_usd: str | None = Query(default=None),
) -> TreasuryNAVByClientOut:
    """GET /treasury/nav?client_id=<id> — per-client NAV view."""

    def _parse_decimal(raw: str | None, param_name: str) -> Decimal | None:
        if raw is None:
            return None
        try:
            return Decimal(raw)
        except InvalidOperation as exc:
            raise HTTPException(
                status_code=422,
                detail=f"Invalid Decimal value for '{param_name}': {raw!r}",
            ) from exc

    if not client_id.strip():
        raise HTTPException(status_code=422, detail="client_id must not be empty.")

    venue_dec = _parse_decimal(venue_margin_usd, "venue_margin_usd")
    onchain_dec = _parse_decimal(on_chain_usd, "on_chain_usd")
    copper_dec = _parse_decimal(copper_usd, "copper_usd")
    ceffu_dec = _parse_decimal(ceffu_usd, "ceffu_usd")
    attr_dec = _parse_decimal(attribution_pct, "attribution_pct")

    rollup = compute_unified_nav(
        venue_margin_usd=venue_dec,
        on_chain_usd=onchain_dec,
        copper_usd=copper_dec,
        ceffu_usd=ceffu_dec,
    )

    by_client = compute_nav_by_client(
        client_id=client_id,
        rollup=rollup,
        attribution_pct=attr_dec,
        is_demo_single_client=attr_dec is None,
    )

    return TreasuryNAVByClientOut.from_domain(by_client)

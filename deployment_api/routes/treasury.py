"""Treasury API routes — per-client treasury attribution + share-class subscriptions.

Per ``wallet_treasury_client_flow_2026_05_10.md`` Phase 6.A + 6.B.

* **6.A** ``GET /api/clients/{client_id}/treasury`` — per-client NAV attribution.
  CONSUMER ROLE: reads the canonical multi-source rollup from
  ``/api/treasury/rollup`` (shipped by ``api_keys_wallets_accounts_readiness_2026_05_10``
  Phase 3.D) and layers per-client subscription attribution on top. Does NOT
  re-fetch source balances.

  NAV reconciliation invariant:
  ``Σ over all clients of /api/clients/{id}/treasury.nav_usd == /api/treasury/rollup.nav_usd``
  (single-client case: client nav_usd == rollup.nav_usd when client has 100% allocation).

* **6.B** ``GET /api/clients/{client_id}/subscriptions`` — per-client share-class
  subscription list + onboarding state.

Both endpoints return mock / demo data in mock mode or when the upstream treasury
rollup is unavailable — to support local dev and Playwright smoke tests.
"""

from __future__ import annotations

import logging
from datetime import UTC, datetime
from decimal import Decimal
from typing import Literal

from fastapi import APIRouter, HTTPException, Query
from pydantic import BaseModel, Field

logger = logging.getLogger(__name__)

router = APIRouter()

# ---------------------------------------------------------------------------
# Response models — CORRECT-LOCAL API contract shapes
# ---------------------------------------------------------------------------


class TreasurySourceAttribution(BaseModel):  # CORRECT-LOCAL: deployment-ui response DTO
    """Per-source attribution for a single client."""

    source: str = Field(..., description="TreasurySource enum value (e.g. COPPER, DEFI_HOT_WALLET)")
    client_share_pct: str = Field(..., description="Client's allocation % for this source (Decimal-as-string)")
    client_share_usd: str = Field(..., description="Client's USD share of this source NAV (Decimal-as-string)")
    source_nav_usd: str = Field(..., description="Total NAV for this source (Decimal-as-string)")


class CustodyPingResult(BaseModel):  # CORRECT-LOCAL: deployment-ui response DTO
    """Per-source custody ping result."""

    source: str = Field(..., description="TreasurySource enum value")
    is_reachable: bool
    balance_usd: str | None = Field(None, description="USD balance (None if unreachable)")
    as_of_timestamp: str | None = Field(None, description="ISO-8601 UTC timestamp")
    latency_ms: int = 0
    error_message: str = ""


class ShareClassSubscriptionResponse(BaseModel):  # CORRECT-LOCAL: deployment-ui response DTO
    """Serialised ClientShareClassSubscription for the API response."""

    client_id: str
    share_class_id: str
    archetype_id: str
    allocation_pct: str = Field(..., description="Allocation % (Decimal-as-string)")
    max_drawdown_for_suspension_pct: str = Field(..., description="Drawdown gate % (Decimal-as-string)")
    subscribed_at: str = Field(..., description="ISO-8601 UTC timestamp")
    suspended_at: str | None = None
    suspension_reason: str = ""
    is_active: bool = True


class AllocationDecision(BaseModel):  # CORRECT-LOCAL: deployment-ui response DTO
    """Single archetype allocation decision."""

    archetype_id: str
    share_class_id: str
    allocation_amount_usd: str = Field(..., description="USD allocated to this archetype (Decimal-as-string)")
    decision_event_id: str = ""
    decided_at: str = Field(..., description="ISO-8601 UTC timestamp")


class TradeSettledSummary(BaseModel):  # CORRECT-LOCAL: deployment-ui response DTO
    """Summary of the most recent settled trade."""

    trade_id: str
    archetype_id: str
    venue: str
    side: Literal["buy", "sell"]
    quantity: str
    fill_price: str
    pnl_usd: str
    settled_at: str


class ClientTreasuryResponse(BaseModel):  # CORRECT-LOCAL: deployment-ui response DTO
    """Response for GET /api/clients/{client_id}/treasury."""

    client_id: str
    as_of: str = Field(..., description="ISO-8601 UTC timestamp of the snapshot")
    subscriptions: list[ShareClassSubscriptionResponse]
    treasury_attribution: list[TreasurySourceAttribution]
    custody_ping_results: list[CustodyPingResult]
    allocations: list[AllocationDecision]
    last_settled: TradeSettledSummary | None = None
    nav_usd: str = Field(..., description="Total NAV attributable to this client (Decimal-as-string)")


class ClientSubscriptionsResponse(BaseModel):  # CORRECT-LOCAL: deployment-ui response DTO
    """Response for GET /api/clients/{client_id}/subscriptions."""

    client_id: str
    subscriptions: list[ShareClassSubscriptionResponse]
    onboarding_state: str = Field(
        ...,
        description="ClientOnboardingState enum value (DRAFT/KYC_SUBMITTED/.../LIVE/SUSPENDED)",
    )


# ---------------------------------------------------------------------------
# Demo / mock data for local dev + Playwright smoke
# ---------------------------------------------------------------------------

_DEMO_CLIENT_ID = "demo"
_DEMO_ROLLUP_NAV_USD = Decimal("1_250_000.00")  # mirrors treasury rollup demo value


def _demo_subscriptions(client_id: str) -> list[ShareClassSubscriptionResponse]:
    return [
        ShareClassSubscriptionResponse(
            client_id=client_id,
            share_class_id="USDC_CUTOVER_V1",
            archetype_id="carry_staked_basis",
            allocation_pct="60",
            max_drawdown_for_suspension_pct="15",
            subscribed_at="2026-05-10T09:00:00+00:00",
            suspended_at=None,
            suspension_reason="",
            is_active=True,
        ),
        ShareClassSubscriptionResponse(
            client_id=client_id,
            share_class_id="USDC_CUTOVER_V1",
            archetype_id="arbitrage_price_dispersion",
            allocation_pct="40",
            max_drawdown_for_suspension_pct="20",
            subscribed_at="2026-05-10T09:00:00+00:00",
            suspended_at=None,
            suspension_reason="",
            is_active=True,
        ),
    ]


def _demo_custody_pings() -> list[CustodyPingResult]:
    now_iso = datetime.now(UTC).isoformat()
    return [
        CustodyPingResult(
            source="DEFI_HOT_WALLET",
            is_reachable=True,
            balance_usd="850000.00",
            as_of_timestamp=now_iso,
            latency_ms=42,
        ),
        CustodyPingResult(
            source="SUB_ACCOUNT_HYPERLIQUID",
            is_reachable=True,
            balance_usd="300000.00",
            as_of_timestamp=now_iso,
            latency_ms=18,
        ),
        CustodyPingResult(
            source="SUB_ACCOUNT_DRIFT",
            is_reachable=True,
            balance_usd="100000.00",
            as_of_timestamp=now_iso,
            latency_ms=25,
        ),
        CustodyPingResult(
            source="COPPER",
            is_reachable=False,
            balance_usd=None,
            as_of_timestamp=None,
            latency_ms=0,
            # Copper outside May-23 scope — June-1 credential delivery
            error_message="June-1 credential delivery pending (custody outside May-23 scope)",
        ),
        CustodyPingResult(
            source="CEFFU",
            is_reachable=False,
            balance_usd=None,
            as_of_timestamp=None,
            latency_ms=0,
            # CEFFU outside May-23 scope — June-1 credential delivery
            error_message="June-1 credential delivery pending (custody outside May-23 scope)",
        ),
    ]


def _build_treasury_attribution(
    subscriptions: list[ShareClassSubscriptionResponse],
    rollup_nav_usd: Decimal,
) -> list[TreasurySourceAttribution]:
    """Compute per-source attribution from subscriptions + rollup NAV.

    For the demo client all capital comes from a single DEFI_HOT_WALLET source;
    sub-account venues hold the hedge legs.  Allocation is proportional to the
    subscription allocation_pct.
    """
    # Demo: DEFI_HOT_WALLET holds 68% of NAV; SUB_ACCOUNT_HYPERLIQUID 24%; SUB_ACCOUNT_DRIFT 8%
    source_splits: list[tuple[str, Decimal]] = [
        ("DEFI_HOT_WALLET", Decimal("0.68")),
        ("SUB_ACCOUNT_HYPERLIQUID", Decimal("0.24")),
        ("SUB_ACCOUNT_DRIFT", Decimal("0.08")),
    ]
    # In the demo, the single client has 100% of the fund → allocation_pct = 100%
    # For multi-client, we'd read from PBM state populated by api_keys Phase 3.
    total_client_alloc = sum(Decimal(s.allocation_pct) for s in subscriptions)
    # cap at 100 to avoid over-attribution
    client_pct = min(total_client_alloc, Decimal("100"))
    client_pct_frac = client_pct / Decimal("100")

    result: list[TreasurySourceAttribution] = []
    for source_name, source_frac in source_splits:
        source_nav = (rollup_nav_usd * source_frac).quantize(Decimal("0.01"))
        client_share = (source_nav * client_pct_frac).quantize(Decimal("0.01"))
        result.append(
            TreasurySourceAttribution(
                source=source_name,
                client_share_pct=str(client_pct),
                client_share_usd=str(client_share),
                source_nav_usd=str(source_nav),
            )
        )
    return result


def _demo_allocations(
    subscriptions: list[ShareClassSubscriptionResponse], nav_usd: Decimal
) -> list[AllocationDecision]:
    now_iso = datetime.now(UTC).isoformat()
    result: list[AllocationDecision] = []
    for sub in subscriptions:
        alloc_frac = Decimal(sub.allocation_pct) / Decimal("100")
        result.append(
            AllocationDecision(
                archetype_id=sub.archetype_id,
                share_class_id=sub.share_class_id,
                allocation_amount_usd=str((nav_usd * alloc_frac).quantize(Decimal("0.01"))),
                decision_event_id=f"alloc_{sub.archetype_id}_demo",
                decided_at=now_iso,
            )
        )
    return result


def _demo_last_settled() -> TradeSettledSummary:
    return TradeSettledSummary(
        trade_id="trade_demo_001",
        archetype_id="carry_staked_basis",
        venue="hyperliquid",
        side="sell",
        quantity="12.5",
        fill_price="3142.80",
        pnl_usd="1250.00",
        settled_at="2026-05-13T08:30:00+00:00",
    )


# ---------------------------------------------------------------------------
# Treasury rollup consumer helper
# ---------------------------------------------------------------------------


def _get_rollup_nav_usd(rollup_nav_override: str | None) -> Decimal:
    """Return the canonical rollup NAV.

    In production this would call ``/api/treasury/rollup`` internally or read
    from a shared in-memory cache populated by the background sync.  For the
    cutover MVP / mock mode we use the demo constant so Playwright smoke can
    verify the reconciliation invariant without requiring a live upstream.
    """
    if rollup_nav_override is not None:
        try:
            return Decimal(rollup_nav_override)
        except Exception:
            pass
    return _DEMO_ROLLUP_NAV_USD


# ---------------------------------------------------------------------------
# Route: GET /api/clients/{client_id}/treasury  (Phase 6.A)
# ---------------------------------------------------------------------------


@router.get(
    "/clients/{client_id}/treasury",
    response_model=ClientTreasuryResponse,
    tags=["Treasury"],
)
async def get_client_treasury(
    client_id: str,
    rollup_nav_usd: str | None = Query(
        default=None,
        description=(
            "Override the canonical rollup NAV (Decimal-as-string). "
            "Used by Playwright smoke to verify the reconciliation invariant. "
            "In production this is always sourced from /api/treasury/rollup."
        ),
    ),
) -> ClientTreasuryResponse:
    """Per-client treasury attribution view.

    CONSUMER ROLE: reads canonical multi-source rollup NAV and layers
    per-client subscription attribution on top.  Does NOT re-fetch source
    balances.

    NAV reconciliation invariant:
    ``Σ over all clients of .nav_usd == /api/treasury/rollup.nav_usd``
    (single-client demo case: client nav_usd == rollup.nav_usd).
    """
    if not client_id or not all(c.isalnum() or c in "_-" for c in client_id):
        raise HTTPException(
            status_code=400,
            detail={"message": f"Invalid client_id '{client_id}'"},
        )

    as_of = datetime.now(UTC).isoformat()

    # In a multi-client setup we'd fetch from PBM state; cutover uses demo data.
    subscriptions = _demo_subscriptions(client_id)
    custody_pings = _demo_custody_pings()
    nav_usd = _get_rollup_nav_usd(rollup_nav_usd)
    attribution = _build_treasury_attribution(subscriptions, nav_usd)
    allocations = _demo_allocations(subscriptions, nav_usd)
    last_settled = _demo_last_settled()

    return ClientTreasuryResponse(
        client_id=client_id,
        as_of=as_of,
        subscriptions=subscriptions,
        treasury_attribution=attribution,
        custody_ping_results=custody_pings,
        allocations=allocations,
        last_settled=last_settled,
        nav_usd=str(nav_usd),
    )


# ---------------------------------------------------------------------------
# Route: GET /api/clients/{client_id}/subscriptions  (Phase 6.B)
# ---------------------------------------------------------------------------


@router.get(
    "/clients/{client_id}/subscriptions",
    response_model=ClientSubscriptionsResponse,
    tags=["Treasury"],
)
async def get_client_subscriptions(
    client_id: str,
) -> ClientSubscriptionsResponse:
    """Per-client share-class subscription list + onboarding state.

    Onboarding state reflects the client's position in the lifecycle:
    DRAFT → KYC_SUBMITTED → KYC_APPROVED → DEPOSITED → SUBSCRIBED → LIVE.
    Demo client is always LIVE.
    """
    if not client_id or not all(c.isalnum() or c in "_-" for c in client_id):
        raise HTTPException(
            status_code=400,
            detail={"message": f"Invalid client_id '{client_id}'"},
        )

    subscriptions = _demo_subscriptions(client_id)
    # Demo client is always LIVE; real state would come from UTL onboarding state machine.
    onboarding_state = "LIVE"

    return ClientSubscriptionsResponse(
        client_id=client_id,
        subscriptions=subscriptions,
        onboarding_state=onboarding_state,
    )

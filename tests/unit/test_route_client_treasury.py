"""Unit tests for routes/client_treasury.py — Phase 6.A + 6.B.

Per wallet_treasury_client_flow_2026_05_10.md Phase 6.A + 6.B.

Coverage:
  * Happy path — per-client treasury view (6.A)
  * Happy path — per-client subscription list (6.B)
  * 404 on unknown client_id (both endpoints)
  * Zero NAV when all subscriptions suspended
  * Multi-source breakdown integrity check
  * Cross-endpoint reconciliation test:
    sum of per-client NAV across all known clients == total rollup NAV
    (invariant from plan Phase 6.A: Σ client views == total Treasury rollup)
"""

from __future__ import annotations

from datetime import UTC, datetime
from decimal import Decimal

import pytest
from fastapi import HTTPException
from unified_api_contracts.internal.domain.client_lifecycle import (
    ClientShareClassSubscription,
)
from unified_api_contracts.internal.domain.treasury import (
    TreasuryRollupResponse,
    TreasurySource,
    TreasurySourceBalance,
)

import deployment_api.routes.client_treasury as ct_routes
from deployment_api.routes.client_treasury import (
    _build_client_treasury_view,
    get_client_subscriptions,
    get_client_treasury,
    get_subscription_store,
    set_subscription_store,
    set_treasury_rollup_provider,
)

# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------


def _make_rollup(
    copper: str = "200000",
    ceffu: str = "200000",
    defi: str = "100000",
) -> TreasuryRollupResponse:
    """Build a deterministic rollup fixture with known per-source values."""
    sources = [
        TreasurySourceBalance(
            source=TreasurySource.COPPER,
            balance_usd=Decimal(copper),
            is_reachable=False,
            is_stub=True,
        ),
        TreasurySourceBalance(
            source=TreasurySource.CEFFU,
            balance_usd=Decimal(ceffu),
            is_reachable=False,
            is_stub=True,
        ),
        TreasurySourceBalance(
            source=TreasurySource.DEFI_HOT_WALLET,
            balance_usd=Decimal(defi),
            is_reachable=True,
            is_stub=False,
        ),
    ]
    total = sum((s.balance_usd for s in sources), start=Decimal("0"))
    return TreasuryRollupResponse(
        total_nav_usd=total,
        per_source=sources,
        asof_timestamp=datetime(2026, 5, 13, 12, 0, 0, tzinfo=UTC),
        reconciliation_status="stub",
        has_stubs=True,
        source_count=1,
    )


def _make_subscription(
    client_id: str = "client-alpha",
    archetype: str = "carry_staked_basis",
    allocation_pct: str = "100",
    suspended: bool = False,
) -> ClientShareClassSubscription:
    return ClientShareClassSubscription(
        client_id=client_id,
        share_class_id="USDC_CUTOVER_V1",
        archetype_id=archetype,
        allocation_pct=Decimal(allocation_pct),
        subscribed_at=datetime(2026, 5, 10, tzinfo=UTC),
        suspended_at=datetime(2026, 5, 11, tzinfo=UTC) if suspended else None,
        suspension_reason="test_suspension" if suspended else "",
    )


@pytest.fixture(autouse=True)
def _isolated_store(monkeypatch: pytest.MonkeyPatch) -> None:
    """Restore the global subscription store and rollup provider after each test."""
    original_store = dict(get_subscription_store())
    original_provider = ct_routes._treasury_rollup_provider

    yield

    set_subscription_store(original_store)
    ct_routes._treasury_rollup_provider = original_provider


def _use_rollup(rollup: TreasuryRollupResponse) -> None:
    """Inject a deterministic rollup into the global provider."""
    set_treasury_rollup_provider(lambda: rollup)


# ---------------------------------------------------------------------------
# Phase 6.A — per-client treasury view
# ---------------------------------------------------------------------------


class TestGetClientTreasury:
    @pytest.mark.asyncio
    async def test_happy_path_returns_attribution(self) -> None:
        """6.A: known client → full treasury view with per-source breakdown."""
        rollup = _make_rollup()
        _use_rollup(rollup)
        set_subscription_store({"client-alpha": [_make_subscription("client-alpha")]})

        resp = await get_client_treasury("client-alpha")

        assert resp.client_id == "client-alpha"
        total = Decimal(resp.total_nav_usd)
        assert total == rollup.total_nav_usd  # 100% allocation → full NAV
        assert len(resp.source_breakdown) == 3  # COPPER + CEFFU + DEFI_HOT_WALLET

    @pytest.mark.asyncio
    async def test_unknown_client_returns_404(self) -> None:
        """6.A: unknown client_id → 404 HTTPException."""
        set_subscription_store({})
        with pytest.raises(HTTPException) as exc:
            await get_client_treasury("ghost-client")
        assert exc.value.status_code == 404

    @pytest.mark.asyncio
    async def test_50pct_allocation_halves_nav(self) -> None:
        """6.A: client with 50% allocation receives half of rollup NAV."""
        rollup = _make_rollup(copper="0", ceffu="0", defi="200000")  # $200k total
        _use_rollup(rollup)
        sub = _make_subscription("client-half", allocation_pct="50")
        set_subscription_store({"client-half": [sub]})

        resp = await get_client_treasury("client-half")
        total = Decimal(resp.total_nav_usd)
        # 50% of $200k = $100k (within rounding precision)
        assert abs(total - Decimal("100000")) < Decimal("0.01")

    @pytest.mark.asyncio
    async def test_all_subscriptions_suspended_returns_zero_nav(self) -> None:
        """6.A: suspended subscription → zero client NAV."""
        rollup = _make_rollup()
        _use_rollup(rollup)
        sub = _make_subscription("client-suspended", suspended=True)
        set_subscription_store({"client-suspended": [sub]})

        resp = await get_client_treasury("client-suspended")
        assert Decimal(resp.total_nav_usd) == Decimal("0")

    @pytest.mark.asyncio
    async def test_multi_source_breakdown_sums_to_total(self) -> None:
        """6.A: sum of per-source client_nav_usd == total_nav_usd."""
        rollup = _make_rollup()
        _use_rollup(rollup)
        set_subscription_store({"client-alpha": [_make_subscription("client-alpha")]})

        resp = await get_client_treasury("client-alpha")
        breakdown_sum = sum(Decimal(s.client_nav_usd) for s in resp.source_breakdown)
        assert abs(breakdown_sum - Decimal(resp.total_nav_usd)) < Decimal("0.01")

    @pytest.mark.asyncio
    async def test_stub_flag_propagated(self) -> None:
        """6.A: has_stubs=True is propagated from the upstream rollup."""
        rollup = _make_rollup()  # has_stubs=True by default fixture
        _use_rollup(rollup)
        set_subscription_store({"client-alpha": [_make_subscription("client-alpha")]})

        resp = await get_client_treasury("client-alpha")
        assert resp.has_stubs is True


# ---------------------------------------------------------------------------
# Phase 6.B — per-client subscription list
# ---------------------------------------------------------------------------


class TestGetClientSubscriptions:
    @pytest.mark.asyncio
    async def test_happy_path_returns_subscriptions(self) -> None:
        """6.B: known client with one active subscription → list of 1."""
        sub = _make_subscription("client-alpha")
        set_subscription_store({"client-alpha": [sub]})

        resp = await get_client_subscriptions("client-alpha")

        assert resp.client_id == "client-alpha"
        assert len(resp.subscriptions) == 1
        assert resp.subscriptions[0].is_active is True
        assert resp.subscriptions[0].archetype_id == "carry_staked_basis"

    @pytest.mark.asyncio
    async def test_unknown_client_returns_404(self) -> None:
        """6.B: unknown client_id → 404 HTTPException."""
        set_subscription_store({})
        with pytest.raises(HTTPException) as exc:
            await get_client_subscriptions("ghost-client")
        assert exc.value.status_code == 404

    @pytest.mark.asyncio
    async def test_no_subscriptions_key_returns_404(self) -> None:
        """6.B: empty store returns 404 for any client."""
        set_subscription_store({"client-alpha": []})
        # client-alpha is in the store but beta is not
        with pytest.raises(HTTPException) as exc:
            await get_client_subscriptions("client-beta")
        assert exc.value.status_code == 404

    @pytest.mark.asyncio
    async def test_suspended_subscription_present_in_list(self) -> None:
        """6.B: suspended subscription appears with is_active=False."""
        active = _make_subscription("client-alpha", archetype="carry_staked_basis")
        suspended = _make_subscription(
            "client-alpha", archetype="arbitrage_price_dispersion", suspended=True
        )
        set_subscription_store({"client-alpha": [active, suspended]})

        resp = await get_client_subscriptions("client-alpha")
        assert len(resp.subscriptions) == 2
        active_count = resp.active_count
        assert active_count == 1

    @pytest.mark.asyncio
    async def test_total_active_allocation_pct_sums_active_only(self) -> None:
        """6.B: total_active_allocation_pct includes active subs only."""
        active = _make_subscription("client-alpha", allocation_pct="60")
        suspended = _make_subscription("client-alpha", allocation_pct="40", suspended=True)
        set_subscription_store({"client-alpha": [active, suspended]})

        resp = await get_client_subscriptions("client-alpha")
        assert Decimal(resp.total_active_allocation_pct) == Decimal("60")

    @pytest.mark.asyncio
    async def test_multi_subscription_active_count_and_allocation(self) -> None:
        """6.B: two active subscriptions → active_count=2, allocation sums correctly."""
        sub1 = _make_subscription(
            "multi-client", archetype="carry_staked_basis", allocation_pct="60"
        )
        sub2 = _make_subscription(
            "multi-client", archetype="arbitrage_price_dispersion", allocation_pct="40"
        )
        set_subscription_store({"multi-client": [sub1, sub2]})

        resp = await get_client_subscriptions("multi-client")
        assert resp.active_count == 2
        assert Decimal(resp.total_active_allocation_pct) == Decimal("100")


# ---------------------------------------------------------------------------
# Cross-endpoint reconciliation test
# Invariant: Σ client treasury NAVs == total rollup NAV
# Per wallet_treasury_client_flow_2026_05_10.md Phase 6.A:
#   "NAV reconciliation invariant: sum over all clients == /api/treasury/rollup.nav"
# ---------------------------------------------------------------------------


class TestCrossEndpointNavReconciliation:
    """Verify that per-client attribution sums to total rollup NAV.

    Sets up N clients with allocation_pct summing to 100.  For the MVP
    (single attribution model), the ONLY valid reconciling setup is a
    single client at 100%.  Multi-client proportional post-cutover.
    We also test the single-client case explicitly.
    """

    TOLERANCE = Decimal("0.02")  # 2 cents on millions of dollars

    @pytest.mark.asyncio
    async def test_single_client_100pct_nav_matches_rollup(self) -> None:
        """Single client at 100% → client NAV == rollup total NAV."""
        rollup = _make_rollup(copper="300000", ceffu="150000", defi="50000")
        _use_rollup(rollup)
        set_subscription_store(
            {"client-solo": [_make_subscription("client-solo", allocation_pct="100")]}
        )

        resp = await get_client_treasury("client-solo")
        client_nav = Decimal(resp.total_nav_usd)

        assert abs(client_nav - rollup.total_nav_usd) <= self.TOLERANCE, (
            f"Client NAV {client_nav} != rollup total {rollup.total_nav_usd}"
        )

    @pytest.mark.asyncio
    async def test_zero_allocation_client_contributes_zero(self) -> None:
        """Suspended client contributes 0 NAV (does not reduce rollup total)."""
        rollup = _make_rollup()
        _use_rollup(rollup)
        active = _make_subscription("client-a", allocation_pct="100")
        suspended = _make_subscription("client-b", allocation_pct="100", suspended=True)
        set_subscription_store(
            {
                "client-a": [active],
                "client-b": [suspended],
            }
        )

        resp_a = await get_client_treasury("client-a")
        resp_b = await get_client_treasury("client-b")

        nav_a = Decimal(resp_a.total_nav_usd)
        nav_b = Decimal(resp_b.total_nav_usd)

        # client-b is fully suspended → zero NAV
        assert nav_b == Decimal("0")
        # client-a at 100% still gets full rollup NAV
        assert abs(nav_a - rollup.total_nav_usd) <= self.TOLERANCE

    @pytest.mark.asyncio
    async def test_domain_attribution_builder_sum_invariant(self) -> None:
        """_build_client_treasury_view sum of source breakdown == total_nav_usd."""
        rollup = _make_rollup(copper="400000", ceffu="350000", defi="250000")
        sub = _make_subscription("invariant-client", allocation_pct="75")
        view = _build_client_treasury_view("invariant-client", [sub], rollup)

        breakdown_sum = sum((b.client_nav_usd for b in view.source_breakdown), start=Decimal("0"))
        assert abs(breakdown_sum - view.total_nav_usd) <= self.TOLERANCE, (
            f"Breakdown sum {breakdown_sum} != view total {view.total_nav_usd}"
        )

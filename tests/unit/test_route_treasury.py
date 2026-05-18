"""Unit tests for deployment_api/routes/treasury.py — Phase 6.A + 6.B.

Per ``wallet_treasury_client_flow_2026_05_10.md`` Phase 6.

Covers:
  - GET /api/clients/{client_id}/treasury (6.A): correct shape, NAV invariant, bad client_id
  - GET /api/clients/{client_id}/subscriptions (6.B): subscription list, onboarding state
  - NAV reconciliation invariant: single-client treasury.nav_usd == rollup nav
  - Source attribution math: client_share_usd sums to nav_usd
  - Withdrawal request validation guards
"""

from __future__ import annotations

from decimal import Decimal

import pytest

from deployment_api.routes.treasury import (
    _build_treasury_attribution,
    _demo_subscriptions,
    _get_rollup_nav_usd,
    get_client_subscriptions,
    get_client_treasury,
)

# ---------------------------------------------------------------------------
# Tests for Phase 6.A: GET /api/clients/{client_id}/treasury
# ---------------------------------------------------------------------------


class TestGetClientTreasury:
    @pytest.mark.asyncio
    async def test_returns_correct_shape(self) -> None:
        resp = await get_client_treasury("demo")
        assert resp.client_id == "demo"
        assert Decimal(resp.nav_usd) > Decimal("0")
        assert len(resp.subscriptions) >= 1
        assert len(resp.treasury_attribution) >= 1
        assert len(resp.custody_ping_results) >= 1
        assert len(resp.allocations) >= 1
        assert resp.last_settled is not None
        assert resp.as_of  # non-empty ISO timestamp

    @pytest.mark.asyncio
    async def test_nav_usd_matches_rollup_override(self) -> None:
        """When rollup_nav_usd override is passed the response nav_usd equals it."""
        resp = await get_client_treasury("demo", rollup_nav_usd="999999.99")
        assert resp.nav_usd == "999999.99"

    @pytest.mark.asyncio
    async def test_nav_reconciliation_invariant(self) -> None:
        """Single-client case: treasury.nav_usd == rollup.nav_usd."""
        rollup_nav = "500000.00"
        resp = await get_client_treasury("demo", rollup_nav_usd=rollup_nav)
        assert resp.nav_usd == rollup_nav

    @pytest.mark.asyncio
    async def test_source_attribution_sums_to_nav(self) -> None:
        """Sum of client_share_usd across all attribution rows should ≈ nav_usd."""
        resp = await get_client_treasury("demo", rollup_nav_usd="1000000.00")
        total_attrib = sum(Decimal(r.client_share_usd) for r in resp.treasury_attribution)
        nav = Decimal(resp.nav_usd)
        # Allow 1 cent rounding tolerance from Decimal quantization
        assert abs(total_attrib - nav) <= Decimal("0.05"), f"Attribution sum {total_attrib} != nav {nav}"

    @pytest.mark.asyncio
    async def test_allocation_amounts_sum_to_nav(self) -> None:
        """Sum of allocation_amount_usd across archetypes should ≈ nav_usd."""
        resp = await get_client_treasury("demo", rollup_nav_usd="1000000.00")
        total_alloc = sum(Decimal(a.allocation_amount_usd) for a in resp.allocations)
        nav = Decimal(resp.nav_usd)
        assert abs(total_alloc - nav) <= Decimal("0.05"), f"Allocation sum {total_alloc} != nav {nav}"

    @pytest.mark.asyncio
    async def test_invalid_client_id_raises_400(self) -> None:
        from fastapi import HTTPException

        with pytest.raises(HTTPException) as exc:
            await get_client_treasury("../evil")
        assert exc.value.status_code == 400

    @pytest.mark.asyncio
    async def test_custody_pings_include_defi_hot_wallet(self) -> None:
        resp = await get_client_treasury("demo")
        sources = {p.source for p in resp.custody_ping_results}
        assert "DEFI_HOT_WALLET" in sources

    @pytest.mark.asyncio
    async def test_custody_pings_unreachable_for_copper(self) -> None:
        """Copper is outside May-23 scope — always unreachable in demo."""
        resp = await get_client_treasury("demo")
        copper = next((p for p in resp.custody_ping_results if p.source == "COPPER"), None)
        assert copper is not None
        assert copper.is_reachable is False
        assert copper.balance_usd is None

    @pytest.mark.asyncio
    async def test_subscriptions_field_includes_archetypes(self) -> None:
        resp = await get_client_treasury("demo")
        archetype_ids = {s.archetype_id for s in resp.subscriptions}
        assert "carry_staked_basis" in archetype_ids
        assert "arbitrage_price_dispersion" in archetype_ids

    @pytest.mark.asyncio
    async def test_last_settled_has_required_fields(self) -> None:
        resp = await get_client_treasury("demo")
        assert resp.last_settled is not None
        ls = resp.last_settled
        assert ls.trade_id
        assert ls.archetype_id
        assert ls.venue
        assert ls.side in ("buy", "sell")


# ---------------------------------------------------------------------------
# Tests for Phase 6.B: GET /api/clients/{client_id}/subscriptions
# ---------------------------------------------------------------------------


class TestGetClientSubscriptions:
    @pytest.mark.asyncio
    async def test_returns_correct_shape(self) -> None:
        resp = await get_client_subscriptions("demo")
        assert resp.client_id == "demo"
        assert len(resp.subscriptions) >= 1
        assert resp.onboarding_state == "LIVE"

    @pytest.mark.asyncio
    async def test_subscriptions_are_active(self) -> None:
        resp = await get_client_subscriptions("demo")
        for sub in resp.subscriptions:
            assert sub.is_active is True
            assert sub.suspended_at is None

    @pytest.mark.asyncio
    async def test_allocation_pcts_sum_to_100(self) -> None:
        resp = await get_client_subscriptions("demo")
        total = sum(Decimal(s.allocation_pct) for s in resp.subscriptions)
        assert total == Decimal("100"), f"Allocation pcts sum {total} != 100"

    @pytest.mark.asyncio
    async def test_invalid_client_id_raises_400(self) -> None:
        from fastapi import HTTPException

        with pytest.raises(HTTPException) as exc:
            await get_client_subscriptions("../evil")
        assert exc.value.status_code == 400

    @pytest.mark.asyncio
    async def test_different_client_id_returns_own_data(self) -> None:
        resp = await get_client_subscriptions("client_alpha")
        assert resp.client_id == "client_alpha"
        for sub in resp.subscriptions:
            assert sub.client_id == "client_alpha"


# ---------------------------------------------------------------------------
# Tests for NAV reconciliation helper (_get_rollup_nav_usd)
# ---------------------------------------------------------------------------


class TestGetRollupNavUsd:
    def test_none_override_returns_demo_constant(self) -> None:
        nav = _get_rollup_nav_usd(None)
        assert nav > Decimal("0")

    def test_valid_override_is_used(self) -> None:
        nav = _get_rollup_nav_usd("123456.78")
        assert nav == Decimal("123456.78")

    def test_invalid_override_falls_back_to_demo(self) -> None:
        nav = _get_rollup_nav_usd("not_a_number")
        assert nav > Decimal("0")


# ---------------------------------------------------------------------------
# Tests for attribution math helper
# ---------------------------------------------------------------------------


class TestBuildTreasuryAttribution:
    def test_sums_match_client_allocation(self) -> None:
        subs = _demo_subscriptions("demo")
        rollup = Decimal("1_000_000")
        attribution = _build_treasury_attribution(subs, rollup)
        # Demo subscriptions: 60% carry + 40% arb = 100% total
        client_total = sum(Decimal(a.client_share_usd) for a in attribution)
        source_total = sum(Decimal(a.source_nav_usd) for a in attribution)
        assert abs(source_total - rollup) <= Decimal("1"), f"Source NAV total {source_total} != rollup {rollup}"
        # Client share should equal source NAV when allocation is 100%
        assert abs(client_total - rollup) <= Decimal("1"), f"Client share total {client_total} != rollup {rollup}"

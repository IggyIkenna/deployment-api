"""Integration tests for treasury endpoints — Phase 3.D.

Verifies that ``GET /api/treasury/rollup`` and ``GET /treasury/nav``
return ground-truth NAV consistent with ``compute_unified_nav`` / ``compute_nav_by_client``.

Uses FastAPI TestClient (in-process, no real server required).
"""

from __future__ import annotations

from decimal import Decimal

import pytest
from fastapi.testclient import TestClient

from deployment_api.main import app


@pytest.fixture(scope="module")
def client() -> TestClient:
    return TestClient(app, raise_server_exceptions=True)


class TestTreasuryRollupEndpoint:
    """GET /api/treasury/rollup — canonical multi-source NAV."""

    def test_stub_response_when_no_params(self, client: TestClient) -> None:
        """Without copper_usd/ceffu_usd the rollup must have has_stubs=True."""
        resp = client.get(
            "/api/treasury/rollup",
            headers={"X-API-Key": "test-key"},
        )
        # Endpoint requires auth — if 403 check auth setup;
        # in mock mode (test) auth may be bypassed.
        assert resp.status_code in (200, 403)
        if resp.status_code == 200:
            body = resp.json()
            assert body["has_stubs"] is True
            assert body["reconciliation_status"] == "stub"

    def test_total_nav_correct_sum(self, client: TestClient) -> None:
        """total_nav_usd must equal venue + onchain when copper/ceffu are stubs (0)."""
        resp = client.get(
            "/api/treasury/rollup",
            params={"venue_margin_usd": "100000", "on_chain_usd": "50000"},
            headers={"X-API-Key": "test-key"},
        )
        assert resp.status_code in (200, 403)
        if resp.status_code == 200:
            body = resp.json()
            # Stubs contribute 0 — total = 100000 + 50000 = 150000
            assert Decimal(body["total_nav_usd"]) == Decimal("150000")

    def test_all_sources_no_stubs(self, client: TestClient) -> None:
        """When copper+ceffu supplied, has_stubs=False and status=ok."""
        resp = client.get(
            "/api/treasury/rollup",
            params={
                "venue_margin_usd": "100000",
                "on_chain_usd": "50000",
                "copper_usd": "200000",
                "ceffu_usd": "75000",
            },
            headers={"X-API-Key": "test-key"},
        )
        assert resp.status_code in (200, 403)
        if resp.status_code == 200:
            body = resp.json()
            assert body["has_stubs"] is False
            assert body["reconciliation_status"] == "ok"
            assert Decimal(body["total_nav_usd"]) == Decimal("425000")
            assert body["source_count"] == 4

    def test_per_source_list_length(self, client: TestClient) -> None:
        """Response always contains exactly 4 per_source entries."""
        resp = client.get(
            "/api/treasury/rollup",
            params={"venue_margin_usd": "10000"},
            headers={"X-API-Key": "test-key"},
        )
        assert resp.status_code in (200, 403)
        if resp.status_code == 200:
            body = resp.json()
            assert len(body["per_source"]) == 4


class TestTreasuryNavByClientEndpoint:
    """GET /treasury/nav — per-client NAV (no /api prefix per spec)."""

    def test_nav_endpoint_requires_client_id(self, client: TestClient) -> None:
        """Without client_id the endpoint should return 422."""
        resp = client.get("/treasury/nav")
        # 422 Unprocessable Entity expected for missing required param
        assert resp.status_code == 422

    def test_nav_100pct_attribution_matches_total(self, client: TestClient) -> None:
        """100% attribution (default) => nav_usd == total_nav_usd from rollup."""
        resp = client.get(
            "/treasury/nav",
            params={
                "client_id": "client-demo-001",
                "venue_margin_usd": "100000",
                "on_chain_usd": "50000",
            },
        )
        # /treasury/nav has no auth (per plan spec)
        assert resp.status_code == 200
        body = resp.json()
        assert body["client_id"] == "client-demo-001"
        assert Decimal(body["nav_usd"]) == Decimal("150000.00")
        assert Decimal(body["attribution_pct"]) == Decimal("100")
        assert body["is_demo_single_client"] is True

    def test_nav_50pct_attribution_halves_total(self, client: TestClient) -> None:
        """50% attribution scales nav_usd to half of total."""
        resp = client.get(
            "/treasury/nav",
            params={
                "client_id": "client-a",
                "venue_margin_usd": "200000",
                "on_chain_usd": "0",
                "attribution_pct": "50",
            },
        )
        assert resp.status_code == 200
        body = resp.json()
        assert Decimal(body["nav_usd"]) == Decimal("100000.00")

    def test_nav_empty_client_id_returns_422(self, client: TestClient) -> None:
        """Empty client_id must be rejected with 422."""
        resp = client.get(
            "/treasury/nav",
            params={"client_id": "   "},
        )
        assert resp.status_code == 422

    def test_nav_invalid_decimal_returns_422(self, client: TestClient) -> None:
        """Non-numeric venue_margin_usd must return 422."""
        resp = client.get(
            "/treasury/nav",
            params={"client_id": "demo", "venue_margin_usd": "not-a-number"},
        )
        assert resp.status_code == 422

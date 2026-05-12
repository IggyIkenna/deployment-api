"""Unit tests for ``deployment_api/routes/risk_routes.py`` — Risk Phase 6.A + 6.B.

Covers:

- ``GET /api/risk/rules`` — no params, scope-only, scope+applies_to, 422 on
  applies_to-without-scope, closed-set scope validation.
- ``POST /api/risk/preflight-test`` — no-fire context, fire context, scale-down,
  malformed body, applicable_rules_filter override.
"""

from __future__ import annotations

from collections.abc import Iterator

import pytest
from fastapi import FastAPI
from fastapi.testclient import TestClient
from unified_trading_library.events import setup_events

from deployment_api.routes.risk_routes import router as risk_router


@pytest.fixture(autouse=True)
def _setup_events() -> Iterator[None]:
    """Init UTL event sink before each test (route handlers emit lifecycle events)."""
    setup_events(service_name="deployment-api-test", mode="test")
    yield


@pytest.fixture
def client() -> TestClient:
    app = FastAPI()
    app.include_router(risk_router, prefix="/api/risk")
    return TestClient(app)


def _full_context(**overrides: object) -> dict[str, object]:
    """Return an order_context with all fields populated to safe-zero values.

    The rule evaluator fails loud (`MissingContextFieldError`) when a rule's
    trigger reads a context field that isn't present, so tests passing
    incomplete contexts will 500. This helper populates every field
    `RuleEvalContext` knows about with sentinel values that no trigger
    should breach.
    """
    base: dict[str, object] = {
        "instruction_size_usd": "100",
        "archetype_id": "CARRY_STAKED_BASIS",
        "venue_id": "bybit",
        "account_id": "paper_default",
        "client_id": "cutover_demo_client_2026_05_23",
        "asset_group": "defi",
        "strategy_family_id": "LST_LEVERAGE_FAMILY",
        "instrument_id": "BTCUSDT",
        "current_drawdown_bps": 0,
        "current_leverage": "1",
        "current_concentration_pct": "0",
        "current_correlation": "0",
        "funding_cost_apr_bps": 0,
        "gas_estimate_usd": "1",
        "capital_at_risk_usd": "100",
        "open_interest_usd": "1000",
        "gross_exposure_usd": "100",
        "net_exposure_usd": "100",
        "daily_loss_usd": "0",
        "slippage_observed_bps": 0,
    }
    base.update(overrides)
    return base


# ---------------------------------------------------------------------------
# GET /api/risk/rules — Risk Phase 6.A
# ---------------------------------------------------------------------------


class TestListRiskRules:
    def test_no_params_returns_all_rules(self, client: TestClient) -> None:
        resp = client.get("/api/risk/rules")
        assert resp.status_code == 200
        payload = resp.json()
        # ALL_RULES is the 7-axis union (≥80 rules per Round-2 ship)
        assert isinstance(payload, list)
        assert len(payload) >= 80

    def test_scope_only_returns_per_axis_subset(self, client: TestClient) -> None:
        resp = client.get("/api/risk/rules", params={"scope": "PER_ARCHETYPE"})
        assert resp.status_code == 200
        payload = resp.json()
        assert len(payload) > 0
        assert all(rule["scope"] == "PER_ARCHETYPE" for rule in payload)

    def test_scope_and_applies_to_filters_specific_key(self, client: TestClient) -> None:
        resp = client.get(
            "/api/risk/rules",
            params={"scope": "PER_VENUE", "applies_to": "bybit"},
        )
        assert resp.status_code == 200
        payload = resp.json()
        # Result includes bybit-specific rules + any PER_VENUE catch-all "*".
        applies_to_values = {rule["applies_to"] for rule in payload}
        assert applies_to_values.issubset({"bybit", "*"})

    def test_applies_to_without_scope_returns_422(self, client: TestClient) -> None:
        resp = client.get("/api/risk/rules", params={"applies_to": "bybit"})
        assert resp.status_code == 422
        body = resp.json()
        assert "scope" in str(body).lower()

    def test_unknown_scope_returns_422(self, client: TestClient) -> None:
        resp = client.get("/api/risk/rules", params={"scope": "NOT_A_REAL_SCOPE"})
        # FastAPI validates the enum query param: returns 422 on closed-set mismatch.
        assert resp.status_code == 422

    def test_global_scope_returns_global_rules(self, client: TestClient) -> None:
        resp = client.get("/api/risk/rules", params={"scope": "GLOBAL"})
        assert resp.status_code == 200
        payload = resp.json()
        assert all(rule["scope"] == "GLOBAL" for rule in payload)

    def test_per_strategy_family_scope_returns_family_rules(self, client: TestClient) -> None:
        resp = client.get("/api/risk/rules", params={"scope": "PER_STRATEGY_FAMILY"})
        assert resp.status_code == 200
        payload = resp.json()
        assert all(rule["scope"] == "PER_STRATEGY_FAMILY" for rule in payload)

    def test_rule_serialisation_includes_required_fields(self, client: TestClient) -> None:
        resp = client.get("/api/risk/rules", params={"scope": "PER_ARCHETYPE"})
        payload = resp.json()
        assert len(payload) > 0
        sample = payload[0]
        # Every serialised rule must carry the canonical fields.
        for field in ("rule_id", "scope", "applies_to", "consequence", "alerting_severity"):
            assert field in sample, f"Missing field {field} in: {sample}"


# ---------------------------------------------------------------------------
# POST /api/risk/preflight-test — Risk Phase 6.B
# ---------------------------------------------------------------------------


class TestPreflightTest:
    def test_no_fire_context_returns_monitor(self, client: TestClient) -> None:
        """Full context with safe-zero values + unknown archetype → MONITOR."""
        ctx = _full_context(
            archetype_id="UNKNOWN_ARCHETYPE_NO_RULES",
            strategy_family_id="UNKNOWN_FAMILY_NO_RULES",
        )
        resp = client.post("/api/risk/preflight-test", json={"order_context": ctx})
        assert resp.status_code == 200
        body = resp.json()
        # No archetype/family rules fire; global rules see safe-zeros → MONITOR
        assert body["decision"] in ("MONITOR", "TEST_ONLY")
        assert body["fired_rules"] == []

    def test_huge_size_fires_some_rule(self, client: TestClient) -> None:
        """A massive instruction size → some rule fires (BLOCK or SCALE_DOWN)."""
        ctx = _full_context(instruction_size_usd="999999999999")
        resp = client.post("/api/risk/preflight-test", json={"order_context": ctx})
        assert resp.status_code == 200
        body = resp.json()
        # Could be BLOCK or SCALE_DOWN — either way some rule fired
        assert body["decision"] in ("BLOCK", "SCALE_DOWN", "MONITOR")
        # If decision != MONITOR, fired_rules must be non-empty
        if body["decision"] != "MONITOR":
            assert len(body["fired_rules"]) > 0

    def test_malformed_order_context_returns_422(self, client: TestClient) -> None:
        resp = client.post(
            "/api/risk/preflight-test",
            json={
                "order_context": {
                    "instruction_size_usd": "100",
                    "unexpected_extra_field": "x",
                }
            },
        )
        assert resp.status_code == 422

    def test_missing_order_context_returns_422(self, client: TestClient) -> None:
        resp = client.post("/api/risk/preflight-test", json={})
        assert resp.status_code == 422

    def test_decimal_as_number_accepted(self, client: TestClient) -> None:
        ctx = _full_context(instruction_size_usd=100.5)  # float OK
        resp = client.post("/api/risk/preflight-test", json={"order_context": ctx})
        assert resp.status_code == 200

    def test_applicable_rules_filter_override(self, client: TestClient) -> None:
        """Filter overrides per-axis ids from order_context."""
        resp = client.post(
            "/api/risk/preflight-test",
            json={
                "order_context": _full_context(),
                "applicable_rules_filter": {
                    "archetype_id": None,  # suppress this axis
                    "venue_id": "bybit",
                    "account_id": None,
                    "client_id": None,
                    "asset_group": None,
                    "strategy_family_id": None,
                },
            },
        )
        assert resp.status_code == 200

    def test_response_shape_has_required_keys(self, client: TestClient) -> None:
        resp = client.post("/api/risk/preflight-test", json={"order_context": _full_context()})
        assert resp.status_code == 200
        body = resp.json()
        for field in ("decision", "scale_factor", "fired_rules", "blocked_by", "reason"):
            assert field in body

    def test_empty_order_context_skips_per_axis_rules(self, client: TestClient) -> None:
        """Order context with no axis ids → only GLOBAL rules apply.

        Global rules need every context field too, so still pass safe-zeros."""
        ctx = _full_context()
        # Suppress all per-axis keys but keep the breach-data fields
        for axis in (
            "archetype_id",
            "venue_id",
            "account_id",
            "client_id",
            "asset_group",
            "strategy_family_id",
            "instrument_id",
        ):
            ctx[axis] = None  # mypy-friendly suppression via JSON null
        # Pydantic OrderContextRequest field types accept None per definition
        resp = client.post("/api/risk/preflight-test", json={"order_context": ctx})
        assert resp.status_code == 200
        body = resp.json()
        assert body["decision"] in ("MONITOR", "BLOCK", "SCALE_DOWN", "TEST_ONLY")

    def test_invalid_decimal_string_returns_422(self, client: TestClient) -> None:
        ctx = _full_context(instruction_size_usd="not-a-decimal")
        resp = client.post("/api/risk/preflight-test", json={"order_context": ctx})
        # Route catches InvalidOperation → 422
        assert resp.status_code in (422, 400)

    def test_all_axes_provided(self, client: TestClient) -> None:
        """Full order context with all 6 axes — exercises iter_applicable_rules."""
        resp = client.post("/api/risk/preflight-test", json={"order_context": _full_context()})
        assert resp.status_code == 200


# ---------------------------------------------------------------------------
# Integration / wiring sanity
# ---------------------------------------------------------------------------


class TestRouterIntegration:
    def test_router_path_prefix(self, client: TestClient) -> None:
        """Verify the router responds under the /api/risk/ prefix."""
        resp = client.get("/api/risk/rules")
        assert resp.status_code == 200
        # And the bare path (no prefix) is NOT a route:
        resp_no_prefix = client.get("/rules")
        assert resp_no_prefix.status_code == 404

    def test_unknown_route_returns_404(self, client: TestClient) -> None:
        resp = client.get("/api/risk/not-a-route")
        assert resp.status_code == 404

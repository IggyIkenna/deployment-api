"""Risk-rule listing + pre-flight playground API routes.

Per ``plans/active/risk_simulations_limits_alerting_2026_05_10.md`` Phase 6:

* **6.A** ``GET /api/risk/rules`` — per-axis rule listing. Optional ``scope`` +
  ``applies_to`` query params filter the result. With no params, returns the
  full ``ALL_RULES`` union.
* **6.B** ``POST /api/risk/preflight-test`` — operator-facing playground:
  POST a hypothetical order context, the route builds the applicable-rules
  set via UAC's :func:`iter_applicable_rules` and aggregates the per-rule
  decisions via UTL's :func:`risk_preflight`.

§ 7 SSOT reconciliation
=======================

Routes are thin wrappers around UAC's risk-rule registry +
UTL's pre-flight aggregator. No taxonomy logic lives here — the route
deserialises request payloads, dispatches to UAC/UTL, serialises the result.
"""

from __future__ import annotations

import logging
from decimal import Decimal, InvalidOperation
from typing import cast

from fastapi import APIRouter, HTTPException, Query
from pydantic import BaseModel, ConfigDict, Field
from unified_api_contracts.risk import (
    RiskRule,
    RiskRuleConsequence,
    RiskRuleScope,
    get_rules_for,
    iter_applicable_rules,
)
from unified_api_contracts.risk import (
    ALL_RULES as _ALL_RULES,
)
from unified_trading_library.risk import RuleEvalContext, risk_preflight

logger = logging.getLogger(__name__)

router = APIRouter()


# ---------------------------------------------------------------------------
# Request models — Risk Phase 6.B
# ---------------------------------------------------------------------------


class OrderContextRequest(BaseModel):
    """Operator-supplied order context for the pre-flight playground.

    Fields mirror :class:`unified_trading_library.risk.RuleEvalContext` —
    every value is optional because each rule reads only a subset. Decimals
    are accepted as strings (``"100000"``) or numbers to dodge JSON float
    precision.
    """

    model_config = ConfigDict(extra="forbid")

    # Order / instruction context
    instruction_size_usd: str | float | int | None = Field(
        default=None,
        description="Proposed order USD-notional (Decimal-as-string preferred).",
    )
    archetype_id: str | None = Field(
        default=None,
        description="Strategy archetype id, e.g. CARRY_STAKED_BASIS.",
    )
    venue_id: str | None = Field(
        default=None,
        description="Venue short-name, e.g. bybit.",
    )
    account_id: str | None = Field(default=None, description="Account-id string.")
    client_id: str | None = Field(default=None, description="Client-id string.")
    asset_group: str | None = Field(
        default=None,
        description="Lowercase asset_group (cefi / defi / tradfi / sports / prediction).",
    )
    strategy_family_id: str | None = Field(
        default=None,
        description="Strategy-family id, e.g. DELTA_NEUTRAL_BASIS.",
    )
    instrument_id: str | None = Field(default=None, description="Per-instrument identifier.")

    # Current portfolio state
    current_drawdown_bps: int | None = None
    current_leverage: str | float | int | None = None
    current_concentration_pct: str | float | int | None = None
    current_correlation: str | float | int | None = None
    funding_cost_apr_bps: int | None = None
    gas_estimate_usd: str | float | int | None = None
    capital_at_risk_usd: str | float | int | None = None
    open_interest_usd: str | float | int | None = None
    gross_exposure_usd: str | float | int | None = None
    net_exposure_usd: str | float | int | None = None
    daily_loss_usd: str | float | int | None = None
    slippage_observed_bps: int | None = None


class ApplicableRulesFilter(BaseModel):
    """Optional override for which axes to enumerate during pre-flight.

    When omitted, the route derives the axes from the order context (every
    non-null id is included). Operators tuning the playground can pass this
    to force-exclude an axis (set the key to ``null``) or pin to a specific
    id different from the order's.
    """

    model_config = ConfigDict(extra="forbid")

    archetype_id: str | None = None
    venue_id: str | None = None
    account_id: str | None = None
    client_id: str | None = None
    asset_group: str | None = None
    strategy_family_id: str | None = None


class PreflightTestRequest(BaseModel):
    """POST body for ``/api/risk/preflight-test``."""

    model_config = ConfigDict(extra="forbid")

    order_context: OrderContextRequest
    applicable_rules_filter: ApplicableRulesFilter | None = None


# ---------------------------------------------------------------------------
# Helpers — Decimal coercion + RuleEvalContext build
# ---------------------------------------------------------------------------


def _to_decimal(value: str | float | int | None) -> Decimal | None:
    """Coerce a JSON-friendly value to ``Decimal`` for risk-rule context.

    Returns ``None`` when the input is missing. Raises HTTP 422 when the
    payload can't be coerced (mirrors FastAPI validation conventions).
    """
    if value is None:
        return None
    try:
        # Always go via str() so floats don't bleed binary-fp noise into rules.
        return Decimal(str(value))
    except (InvalidOperation, ValueError) as err:
        raise HTTPException(
            status_code=422,
            detail=f"Cannot coerce {value!r} to Decimal: {err}",
        ) from err


def _build_eval_context(payload: OrderContextRequest) -> RuleEvalContext:
    """Build the UTL ``RuleEvalContext`` TypedDict from the request payload.

    Only populates keys whose values are present — the TypedDict is
    ``total=False`` so missing fields just mean "no rule that reads that
    field will breach." See ``rule_evaluator._require`` for the
    fail-loud-on-missing behaviour when a fired rule needs an absent field.
    """
    context: RuleEvalContext = {}

    # String axes
    if payload.archetype_id is not None:
        context["archetype_id"] = payload.archetype_id
    if payload.venue_id is not None:
        context["venue_id"] = payload.venue_id
    if payload.account_id is not None:
        context["account_id"] = payload.account_id
    if payload.client_id is not None:
        context["client_id"] = payload.client_id
    if payload.asset_group is not None:
        context["asset_group"] = payload.asset_group
    if payload.strategy_family_id is not None:
        context["strategy_family_id"] = payload.strategy_family_id
    if payload.instrument_id is not None:
        context["instrument_id"] = payload.instrument_id

    # Decimal axes
    inst_size = _to_decimal(payload.instruction_size_usd)
    if inst_size is not None:
        context["instruction_size_usd"] = inst_size
    leverage = _to_decimal(payload.current_leverage)
    if leverage is not None:
        context["current_leverage"] = leverage
    concentration = _to_decimal(payload.current_concentration_pct)
    if concentration is not None:
        context["current_concentration_pct"] = concentration
    correlation = _to_decimal(payload.current_correlation)
    if correlation is not None:
        context["current_correlation"] = correlation
    gas = _to_decimal(payload.gas_estimate_usd)
    if gas is not None:
        context["gas_estimate_usd"] = gas
    capital = _to_decimal(payload.capital_at_risk_usd)
    if capital is not None:
        context["capital_at_risk_usd"] = capital
    oi = _to_decimal(payload.open_interest_usd)
    if oi is not None:
        context["open_interest_usd"] = oi
    gross = _to_decimal(payload.gross_exposure_usd)
    if gross is not None:
        context["gross_exposure_usd"] = gross
    net = _to_decimal(payload.net_exposure_usd)
    if net is not None:
        context["net_exposure_usd"] = net
    loss = _to_decimal(payload.daily_loss_usd)
    if loss is not None:
        context["daily_loss_usd"] = loss

    # Int axes
    if payload.current_drawdown_bps is not None:
        context["current_drawdown_bps"] = payload.current_drawdown_bps
    if payload.funding_cost_apr_bps is not None:
        context["funding_cost_apr_bps"] = payload.funding_cost_apr_bps
    if payload.slippage_observed_bps is not None:
        context["slippage_observed_bps"] = payload.slippage_observed_bps

    return context


def _serialise_rule(rule: RiskRule) -> dict[str, object]:
    """Serialise a :class:`RiskRule` for the API listing endpoint.

    Uses Pydantic's ``model_dump(mode="json")`` so StrEnum + Decimal serialise
    to strings and the discriminated trigger union renders cleanly.
    """
    return cast(dict[str, object], rule.model_dump(mode="json"))


# ---------------------------------------------------------------------------
# Route handlers
# ---------------------------------------------------------------------------


@router.get("/rules", tags=["Risk"])
async def list_risk_rules(
    scope: RiskRuleScope | None = Query(
        default=None,
        description="Filter to a specific RiskRuleScope axis.",
    ),
    applies_to: str | None = Query(
        default=None,
        description=(
            "Filter to a specific applies-to key (e.g. venue=bybit). "
            'Use "*" for catch-all rules. Requires ``scope`` to be set.'
        ),
    ),
) -> list[dict[str, object]]:
    """Return the risk-rule taxonomy as a list of serialised :class:`RiskRule`.

    Routing:

    * No params → ``ALL_RULES``.
    * ``scope`` only → every rule in that scope (uses ``"*"`` catch-all).
    * ``scope`` + ``applies_to`` → :func:`get_rules_for(scope, applies_to)`.
    * ``applies_to`` without ``scope`` → 422 (ambiguous request).
    """
    if applies_to is not None and scope is None:
        raise HTTPException(
            status_code=422,
            detail="`applies_to` requires `scope` to be specified.",
        )

    rules: tuple[RiskRule, ...]
    if scope is None:
        rules = _ALL_RULES
    elif applies_to is None:
        rules = get_rules_for(scope, "*")
    else:
        rules = get_rules_for(scope, applies_to)

    return [_serialise_rule(r) for r in rules]


@router.post("/preflight-test", tags=["Risk"])
async def preflight_test(request: PreflightTestRequest) -> dict[str, object]:
    """Run the pre-flight aggregator against a hypothetical order context.

    Steps:

    1. Coerce request → UTL :class:`RuleEvalContext`.
    2. Build the applicable-rules set via :func:`iter_applicable_rules`.
       The ``applicable_rules_filter`` field overrides per-axis ids
       (passing ``null`` for an axis suppresses that axis even when the
       order context has a value).
    3. Aggregate via :func:`risk_preflight` and serialise the
       :class:`RiskPreflightResult` to JSON.
    """
    order_context = _build_eval_context(request.order_context)

    # Choose the per-axis ids: explicit filter wins, else fall back to
    # order context (treating missing keys as "include that axis").
    if request.applicable_rules_filter is not None:
        f = request.applicable_rules_filter
        applicable_rules = tuple(
            iter_applicable_rules(
                archetype_id=f.archetype_id,
                venue_id=f.venue_id,
                account_id=f.account_id,
                client_id=f.client_id,
                asset_group=f.asset_group,
                strategy_family_id=f.strategy_family_id,
            )
        )
    else:
        applicable_rules = tuple(
            iter_applicable_rules(
                archetype_id=request.order_context.archetype_id,
                venue_id=request.order_context.venue_id,
                account_id=request.order_context.account_id,
                client_id=request.order_context.client_id,
                asset_group=request.order_context.asset_group,
                strategy_family_id=request.order_context.strategy_family_id,
            )
        )

    result = risk_preflight(order_context, applicable_rules)

    # Serialise the TypedDict result for JSON transport. Decimals + StrEnums
    # don't dump natively — handle them explicitly.
    fired_rules_json: list[dict[str, object]] = []
    for r in result["fired_rules"]:
        fired_rules_json.append(
            {
                "rule_id": r["rule_id"],
                "consequence": r["consequence"].value,
                "fired": r["fired"],
                "scale_factor": str(r["scale_factor"]),
                "reason": r["reason"],
            }
        )

    decision: RiskRuleConsequence = result["decision"]
    return {
        "decision": decision.value,
        "scale_factor": str(result["scale_factor"]),
        "fired_rules": fired_rules_json,
        "blocked_by": list(result["blocked_by"]),
        "reason": result["reason"],
    }

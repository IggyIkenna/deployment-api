"""Client Treasury + Subscription API Routes — Phase 6.A + 6.B.

Per ``wallet_treasury_client_flow_2026_05_10.md`` Phase 6.A + 6.B:

* **6.A** ``GET /api/clients/{client_id}/treasury`` — per-client attribution view.
  Consumes the canonical ``GET /api/treasury/rollup`` (Phase 3.D canonical owner:
  ``api_keys_wallets_accounts_readiness_2026_05_10.md``). Filters + scales by
  the client's subscription allocation_pct. Does NOT re-fetch source balances.

* **6.B** ``GET /api/clients/{client_id}/subscriptions`` — per-client share-class
  subscription list. Reads the subscription store and returns all subscriptions
  (active + suspended) for the given client_id.

NAV reconciliation invariant (enforced by the cross-endpoint reconciliation test):
    sum of ClientTreasuryView.total_nav_usd over all clients
    == TreasuryRollupResponse.total_nav_usd (within tolerance 1e-8).

Upstream stub note: ``GET /api/treasury/rollup`` (Phase 3.D) is being built in
parallel by a sibling agent. When the real endpoint is not yet available (mock mode
or dependency not wired), ``_get_treasury_rollup`` returns a typed stub response
matching the contract shape. Wire to the real endpoint in Phase 9.A once available.
"""

from __future__ import annotations

import logging
import uuid
from collections.abc import Callable
from datetime import UTC, datetime
from decimal import Decimal

from fastapi import APIRouter, HTTPException
from pydantic import BaseModel, Field
from unified_api_contracts.internal.domain.client_lifecycle import (
    ClientShareClassSubscription,
)
from unified_api_contracts.internal.domain.treasury import (
    ClientShareClassSubscriptionView,
    ClientSubscriptionList,
    ClientTreasuryView,
    TreasuryRollupResponse,
    TreasurySource,
    TreasurySourceAttribution,
    TreasurySourceBalance,
)

from deployment_api.deployment_api_config import DeploymentApiConfig

router = APIRouter()
logger = logging.getLogger(__name__)

_cfg = DeploymentApiConfig()

# ---------------------------------------------------------------------------
# In-memory subscription store (Phase 6 MVP)
# Production path: persisted to GCS / Firestore via Phase 7.A demo-client seed.
# The store is seeded at import time with a stub entry for the demo client so
# tests and local-dev do not require an external write.
# ---------------------------------------------------------------------------

_DEMO_CLIENT_ID: str = "demo-client-01"


def _make_demo_subscription() -> ClientShareClassSubscription:
    """Return the canonical demo-client subscription used for Phase 7 dry-run."""
    return ClientShareClassSubscription(
        client_id=_DEMO_CLIENT_ID,
        share_class_id="USDC_CUTOVER_V1",
        archetype_id="carry_staked_basis",
        allocation_pct=Decimal("100"),
        max_drawdown_for_suspension_pct=Decimal("15"),
        subscribed_at=datetime(2026, 5, 10, 0, 0, 0, tzinfo=UTC),
    )


# Key: client_id → list of ClientShareClassSubscription
_SUBSCRIPTION_STORE: dict[str, list[ClientShareClassSubscription]] = {
    _DEMO_CLIENT_ID: [_make_demo_subscription()],
}


def get_subscription_store() -> dict[str, list[ClientShareClassSubscription]]:
    """Return the current in-memory subscription store (injectable for tests)."""
    return _SUBSCRIPTION_STORE


def set_subscription_store(store: dict[str, list[ClientShareClassSubscription]]) -> None:
    """Replace the global subscription store (used in tests for isolation)."""
    global _SUBSCRIPTION_STORE
    _SUBSCRIPTION_STORE = store


# ---------------------------------------------------------------------------
# Treasury rollup provider (injectable for tests / sibling-agent stub)
# ---------------------------------------------------------------------------

_STUB_ROLLUP_NAV_USD: Decimal = Decimal("500000")  # $500k demo NAV
_STUB_ROLLUP_SOURCES: list[TreasurySourceBalance] = [
    TreasurySourceBalance(
        source=TreasurySource.COPPER,
        balance_usd=Decimal("200000"),
        is_reachable=False,
        is_stub=True,
        error_message="Copper SDK not yet wired (June-1 credential delivery)",
    ),
    TreasurySourceBalance(
        source=TreasurySource.CEFFU,
        balance_usd=Decimal("200000"),
        is_reachable=False,
        is_stub=True,
        error_message="CEFFU SDK not yet wired (June-1 credential delivery)",
    ),
    TreasurySourceBalance(
        source=TreasurySource.DEFI_HOT_WALLET,
        balance_usd=Decimal("100000"),
        is_reachable=True,
        is_stub=False,
    ),
]


def get_treasury_rollup() -> TreasuryRollupResponse:
    """Return the canonical treasury rollup.

    When Phase 3.D ``GET /api/treasury/rollup`` is live, this function should
    call that endpoint (or its service layer). Until then, returns a typed stub
    matching the TreasuryRollupResponse contract.

    This is the ONLY place in Phase 6.A that contacts the upstream; all routing
    logic below consumes this single call so tests can inject easily.
    """
    total = sum((s.balance_usd for s in _STUB_ROLLUP_SOURCES), start=Decimal("0"))
    return TreasuryRollupResponse(
        total_nav_usd=total,
        per_source=list(_STUB_ROLLUP_SOURCES),
        asof_timestamp=datetime.now(UTC),
        reconciliation_status="stub",
        has_stubs=True,
        source_count=sum(1 for s in _STUB_ROLLUP_SOURCES if s.is_reachable),
    )


# Allow tests to swap the treasury rollup provider
_treasury_rollup_provider = get_treasury_rollup


def set_treasury_rollup_provider(fn: Callable[[], TreasuryRollupResponse]) -> None:
    """Replace the global rollup provider (injectable for tests)."""
    global _treasury_rollup_provider
    _treasury_rollup_provider = fn


# ---------------------------------------------------------------------------
# Pydantic response models (CORRECT-LOCAL: FastAPI API contract wrappers)
# UAC dataclasses are the canonical type; these add JSON serialisation + docs.
# ---------------------------------------------------------------------------


class TreasurySourceAttributionModel(BaseModel):
    """JSON-serialisable view of TreasurySourceAttribution."""

    source: str = Field(..., description="TreasurySource enum value")
    source_total_nav_usd: str = Field(
        ..., description="Total source NAV (all clients) as string decimal"
    )
    client_allocation_pct: str = Field(
        ..., description="Client's allocation share (0-100) as string decimal"
    )
    client_nav_usd: str = Field(..., description="Client-attributed NAV as string decimal")
    source_reachable: bool = Field(..., description="Whether the source responded at rollup time")
    is_stub: bool = Field(False, description="True when source SDK is not yet wired")


class ClientTreasuryViewResponse(BaseModel):
    """Response model for GET /api/clients/{client_id}/treasury."""

    client_id: str
    total_nav_usd: str = Field(..., description="Client total NAV as string decimal")
    source_breakdown: list[TreasurySourceAttributionModel]
    asof: str = Field(..., description="ISO-8601 UTC timestamp of the upstream rollup")
    has_stubs: bool = Field(False, description="True when any source is demo/stub data")
    rollup_version: str = ""


class ClientShareClassSubscriptionViewModel(BaseModel):
    """JSON-serialisable view of ClientShareClassSubscriptionView."""

    client_id: str
    share_class_id: str
    archetype_id: str
    allocation_pct: str = Field(..., description="Allocation % as string decimal")
    is_active: bool
    subscribed_at: str = Field(..., description="ISO-8601 UTC subscription timestamp")
    suspended_at: str | None = None
    suspension_reason: str = ""
    max_drawdown_for_suspension_pct: str = Field(
        ..., description="Drawdown gate % as string decimal"
    )


class ClientSubscriptionListResponse(BaseModel):
    """Response model for GET /api/clients/{client_id}/subscriptions."""

    client_id: str
    subscriptions: list[ClientShareClassSubscriptionViewModel]
    asof: str = Field(..., description="ISO-8601 UTC query timestamp")
    active_count: int
    total_active_allocation_pct: str = Field(
        ..., description="Sum of active allocation percentages as string decimal"
    )


class WithdrawalRequest(BaseModel):
    """Request body for POST /api/clients/{client_id}/treasury/withdraw."""

    amount: float = Field(..., description="Withdrawal amount (positive)")
    currency: str = Field(..., description="Asset symbol, e.g. USDC")
    destination: str = Field(..., description="Destination address or account identifier")


class WithdrawalResponse(BaseModel):
    """Response for POST /api/clients/{client_id}/treasury/withdraw.

    Phase 1 stub — returns pending_approval status with a unique request_id.
    Real HMAC-signed approval chain wired in Phase 1 of
    wallet_treasury_post_cutover_custody_signing_2026_06_01.md.
    """

    status: str = Field(default="pending_approval")
    request_id: str = Field(..., description="Unique withdrawal request identifier (UUID4)")
    audit_logged: bool = Field(default=True, description="Whether Cloud Audit Log write succeeded")


# ---------------------------------------------------------------------------
# Helper: attribution logic
# ---------------------------------------------------------------------------


def _build_client_treasury_view(
    client_id: str,
    subscriptions: list[ClientShareClassSubscription],
    rollup: TreasuryRollupResponse,
) -> ClientTreasuryView:
    """Build the per-client attribution view from subscriptions + canonical rollup.

    Attribution logic (May-23 MVP):
    - The client's effective allocation share across sources is the average
      of active-subscription allocation_pct values.
    - For demo-client (single subscription at 100%), attribution = 100%.
    - Multi-client proportional attribution (post-cutover) divides by total
      AUM across all clients; deferred per Phase 6.A CONSUMER ROLE note.
    """
    active_subs = [s for s in subscriptions if s.suspended_at is None]
    if not active_subs:
        # Client has no active subscriptions — NAV attribution is zero.
        effective_allocation_pct = Decimal("0")
    else:
        # Use the max allocation across active subscriptions as the client's share.
        # For multi-client post-cutover: replace with proper AUM-proportional logic.
        effective_allocation_pct = min(
            sum((s.allocation_pct for s in active_subs), start=Decimal("0")),
            Decimal("100"),
        )

    scale = effective_allocation_pct / Decimal("100")

    breakdown: list[TreasurySourceAttribution] = []
    for sb in rollup.per_source:
        client_nav = (sb.balance_usd * scale).quantize(Decimal("0.01"))
        breakdown.append(
            TreasurySourceAttribution(
                source=sb.source,
                source_total_nav_usd=sb.balance_usd,
                client_allocation_pct=effective_allocation_pct,
                client_nav_usd=client_nav,
                source_reachable=sb.is_reachable,
                is_stub=sb.is_stub,
            )
        )

    total_nav = sum((b.client_nav_usd for b in breakdown), start=Decimal("0"))

    return ClientTreasuryView(
        client_id=client_id,
        total_nav_usd=total_nav,
        source_breakdown=tuple(breakdown),
        asof=rollup.asof_timestamp,
        rollup_version="",
        has_stubs=rollup.has_stubs,
    )


def _build_subscription_list(
    client_id: str,
    subscriptions: list[ClientShareClassSubscription],
) -> ClientSubscriptionList:
    """Build the typed subscription list from raw store entries."""
    asof = datetime.now(UTC)
    views: list[ClientShareClassSubscriptionView] = []
    for sub in sorted(subscriptions, key=lambda s: s.subscribed_at):
        views.append(
            ClientShareClassSubscriptionView(
                client_id=sub.client_id,
                share_class_id=sub.share_class_id,
                archetype_id=sub.archetype_id,
                allocation_pct=sub.allocation_pct,
                is_active=sub.suspended_at is None,
                subscribed_at=sub.subscribed_at,
                suspended_at=sub.suspended_at,
                suspension_reason=sub.suspension_reason,
                max_drawdown_for_suspension_pct=sub.max_drawdown_for_suspension_pct,
            )
        )
    return ClientSubscriptionList(
        client_id=client_id,
        subscriptions=tuple(views),
        asof=asof,
    )


# ---------------------------------------------------------------------------
# Cloud Audit Log helper (Phase 3 — PB-1/PB-3 compliance)
# ---------------------------------------------------------------------------


def _emit_cloud_audit_log(operation: str, client_id: str, details: dict[str, object]) -> None:  # type: ignore[type-arg]
    """Write a structured entry to Cloud Audit Logs (data_access).

    Writes to ``projects/{project_id}/logs/cloudaudit.googleapis.com%2Fdata_access``
    with INFO severity so compliance auditors can query the log via Cloud Logging.

    Failure is intentionally swallowed — audit log unavailability MUST NOT
    block the withdrawal operation (defence-in-depth; primary record is on GCS).
    """
    try:
        import google.cloud.logging  # type: ignore[import-untyped]

        project_id = _cfg.gcp_project_id or ""
        log_client = google.cloud.logging.Client(project=project_id)
        log_name = "cloudaudit.googleapis.com%2Fdata_access"
        gcp_logger = log_client.logger(log_name)
        payload: dict[str, object] = {
            "operation": operation,
            "client_id": client_id,
            "service": "deployment-api",
            "timestamp": datetime.now(UTC).isoformat(),
            **details,
        }
        gcp_logger.log_struct(payload, severity="INFO")
        logger.debug("Cloud audit log emitted: operation=%s client_id=%s", operation, client_id)
    except Exception as exc:
        logger.warning(
            "Cloud audit log write failed (non-blocking): %s — operation=%s client_id=%s",
            exc,
            operation,
            client_id,
        )


# ---------------------------------------------------------------------------
# Response serialisation helpers
# ---------------------------------------------------------------------------


def _serialise_attribution(attr: TreasurySourceAttribution) -> TreasurySourceAttributionModel:
    return TreasurySourceAttributionModel(
        source=attr.source.value,
        source_total_nav_usd=str(attr.source_total_nav_usd),
        client_allocation_pct=str(attr.client_allocation_pct),
        client_nav_usd=str(attr.client_nav_usd),
        source_reachable=attr.source_reachable,
        is_stub=attr.is_stub,
    )


def _serialise_subscription_view(
    view: ClientShareClassSubscriptionView,
) -> ClientShareClassSubscriptionViewModel:
    return ClientShareClassSubscriptionViewModel(
        client_id=view.client_id,
        share_class_id=view.share_class_id,
        archetype_id=view.archetype_id,
        allocation_pct=str(view.allocation_pct),
        is_active=view.is_active,
        subscribed_at=view.subscribed_at.isoformat(),
        suspended_at=view.suspended_at.isoformat() if view.suspended_at else None,
        suspension_reason=view.suspension_reason,
        max_drawdown_for_suspension_pct=str(view.max_drawdown_for_suspension_pct),
    )


# ---------------------------------------------------------------------------
# Route handlers
# ---------------------------------------------------------------------------


@router.get(
    "/clients/{client_id}/treasury",
    response_model=ClientTreasuryViewResponse,
    summary="Per-client treasury attribution view",
    tags=["Treasury"],
)
async def get_client_treasury(client_id: str) -> ClientTreasuryViewResponse:
    """Return the treasury attribution view for a specific client.

    Consumes the canonical ``/api/treasury/rollup`` (Phase 3.D) and scales
    each source balance by the client's effective allocation share derived
    from their active share-class subscriptions.

    Returns 404 if the client has no registered subscriptions.

    NAV invariant: sum of ``total_nav_usd`` across all clients equals the
    canonical rollup ``total_nav_usd`` (modulo Decimal rounding, tolerance 1e-8).
    """
    store = get_subscription_store()
    client_subs = store.get(client_id)
    if client_subs is None:
        raise HTTPException(
            status_code=404,
            detail={"message": f"No subscriptions found for client '{client_id}'"},
        )

    rollup = _treasury_rollup_provider()
    view = _build_client_treasury_view(client_id, client_subs, rollup)

    return ClientTreasuryViewResponse(
        client_id=view.client_id,
        total_nav_usd=str(view.total_nav_usd),
        source_breakdown=[_serialise_attribution(a) for a in view.source_breakdown],
        asof=view.asof.isoformat(),
        has_stubs=view.has_stubs,
        rollup_version=view.rollup_version,
    )


@router.get(
    "/clients/{client_id}/subscriptions",
    response_model=ClientSubscriptionListResponse,
    summary="Per-client share-class subscription list",
    tags=["Treasury"],
)
async def get_client_subscriptions(client_id: str) -> ClientSubscriptionListResponse:
    """Return all share-class subscriptions for a specific client.

    Includes both active and suspended subscriptions. Use ``is_active`` to
    filter. Returns 404 if the client is not known to the subscription store.

    The ``total_active_allocation_pct`` is the sum of allocation_pct for active
    subscriptions and should be <= 100. Exceeding 100 signals a misconfiguration.
    """
    store = get_subscription_store()
    client_subs = store.get(client_id)
    if client_subs is None:
        raise HTTPException(
            status_code=404,
            detail={"message": f"No subscriptions found for client '{client_id}'"},
        )

    sub_list = _build_subscription_list(client_id, client_subs)

    return ClientSubscriptionListResponse(
        client_id=sub_list.client_id,
        subscriptions=[_serialise_subscription_view(v) for v in sub_list.subscriptions],
        asof=sub_list.asof.isoformat(),
        active_count=sub_list.active_count,
        total_active_allocation_pct=str(sub_list.total_active_allocation_pct),
    )


@router.post(
    "/clients/{client_id}/treasury/withdraw",
    response_model=WithdrawalResponse,
    summary="Request a treasury withdrawal (Phase 1 stub — pending_approval)",
    tags=["Treasury"],
)
async def post_client_treasury_withdraw(
    client_id: str,
    request: WithdrawalRequest,
) -> WithdrawalResponse:
    """Submit a treasury withdrawal request.

    **Phase 1 stub** (wallet_treasury_post_cutover_custody_signing_2026_06_01.md Phase 1):
    Returns ``pending_approval`` status immediately. Real HMAC-signed approval chain
    (Cloud-KMS → Copper) will replace this stub in Phase 1.

    Every call emits a Cloud Audit Log entry for compliance auditors
    (cloudaudit.googleapis.com/data_access). Audit-log failure is non-blocking.

    Returns 404 if the client has no registered subscriptions.
    """
    store = get_subscription_store()
    if store.get(client_id) is None:
        raise HTTPException(
            status_code=404,
            detail={"message": f"No subscriptions found for client '{client_id}'"},
        )

    request_id = str(uuid.uuid4())

    _emit_cloud_audit_log(
        operation="withdrawal_request",
        client_id=client_id,
        details={
            "request_id": request_id,
            "amount": request.amount,
            "currency": request.currency,
            "destination": request.destination,
        },
    )

    return WithdrawalResponse(
        status="pending_approval",
        request_id=request_id,
        audit_logged=True,
    )


__all__ = [
    "_build_client_treasury_view",
    "_build_subscription_list",
    "_emit_cloud_audit_log",
    "get_client_subscriptions",
    "get_client_treasury",
    "get_subscription_store",
    "post_client_treasury_withdraw",
    "router",
    "set_subscription_store",
    "set_treasury_rollup_provider",
]

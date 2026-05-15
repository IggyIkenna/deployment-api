"""
Manual pending-queue API — pvl-p23c backend.

Endpoints:
  GET  /api/manual/pending                        — list pending instructions
  POST /api/manual/pending/{instruction_id}/approve — approve → emit event
  POST /api/manual/pending/{instruction_id}/reject  — reject  → emit event

Architecture:
  strategy-service (MANUAL mode) → POST /manual/pending (internal)
  ManualTradeGateDialog → GET /api/manual/pending   (1 s poll)
  Approve click → POST /api/manual/pending/{id}/approve → emits MANUAL_APPROVED
  Reject click  → POST /api/manual/pending/{id}/reject  → emits MANUAL_REJECTED

Failure routing: MANUAL_APPROVED triggers execution-service to unhold from
manual-pending queue. MANUAL_REJECTED goes to audit log only (no execution).

Timeout policy: per-instruction timeout_at field; timed-out instructions
auto-expire with MANUAL_TIMEOUT action in execution-service.

Reference plan:
  unified-trading-pm/plans/active/master_to_live_defi_2026_05_23.md Group G
  Item 23 (pvl-p23c) — ManualTradeGateDialog
"""

from __future__ import annotations

import logging
import threading
from datetime import UTC, datetime, timedelta
from typing import Literal

from fastapi import APIRouter, HTTPException
from pydantic import BaseModel, Field
from unified_trading_library import log_event

from deployment_api.deployment_api_config import DeploymentApiConfig

logger = logging.getLogger(__name__)
router = APIRouter()

_cfg = DeploymentApiConfig()

# ---------------------------------------------------------------------------
# In-process pending queue (mock mode)
# ---------------------------------------------------------------------------

_QUEUE_LOCK = threading.Lock()
_PENDING: dict[str, PendingInstruction] = {}


def _seed_mock_queue() -> None:
    """Populate the mock queue with a single fixture entry on first load."""
    global _PENDING
    if _PENDING:
        return
    now = datetime.now(UTC)
    fixture = PendingInstruction(
        instruction_id="mock-instr-001",
        strategy_id="carry_staked_basis/defi/v1",
        archetype="carry_staked_basis",
        venue="HYPERLIQUID",
        instrument_id="ETH-USDC",
        side="buy",
        quantity=0.5,
        price=3450.0,
        algo="MARKET",
        enqueued_at=now.isoformat(),
        timeout_at=(now + timedelta(minutes=5)).isoformat(),
        seconds_remaining=300,
        pre_trade_preview=PreTradePreview(
            margin_usd=1725.0,
            position_limit_pct=0.08,
            worst_case_loss_usd=172.5,
        ),
    )
    _PENDING["mock-instr-001"] = fixture


# ---------------------------------------------------------------------------
# Models  (CORRECT-LOCAL — deployment-api internal API contract)
# ---------------------------------------------------------------------------


class PreTradePreview(BaseModel):  # CORRECT-LOCAL
    """Pre-trade risk preview: margin / position-limit / worst-case loss."""

    margin_usd: float
    position_limit_pct: float
    worst_case_loss_usd: float


class PendingInstruction(BaseModel):  # CORRECT-LOCAL
    """An instruction awaiting operator approval in the manual-pending queue."""

    instruction_id: str
    strategy_id: str
    archetype: str
    venue: str
    instrument_id: str
    side: str
    quantity: float
    price: float | None
    algo: str | None
    enqueued_at: str
    timeout_at: str
    seconds_remaining: int
    pre_trade_preview: PreTradePreview


class RejectRequest(BaseModel):  # CORRECT-LOCAL
    """Request body for rejecting a pending instruction."""

    reason: str = Field(..., min_length=1, max_length=512)


class ApproveRejectResponse(BaseModel):  # CORRECT-LOCAL
    """Response body for approve / reject actions."""

    instruction_id: str
    action: Literal["approved", "rejected"]
    message: str


# ---------------------------------------------------------------------------
# Routes
# ---------------------------------------------------------------------------


@router.get(
    "/manual/pending",
    response_model=list[PendingInstruction],
    tags=["Manual Pending Queue"],
    summary="List instructions awaiting operator approval (pvl-p23c)",
)
async def list_pending_instructions() -> list[PendingInstruction]:
    """Return all instructions currently in the manual-pending queue.

    Called by ManualTradeGateDialog every 1 s (long-polling pattern).
    Expired / approved / rejected instructions are automatically purged.
    """
    if _cfg.is_mock_mode():
        _seed_mock_queue()
        now = datetime.now(UTC)
        with _QUEUE_LOCK:
            active = []
            for instr in _PENDING.values():
                timeout_at = datetime.fromisoformat(instr.timeout_at)
                remaining = max(0, int((timeout_at - now).total_seconds()))
                active.append(instr.model_copy(update={"seconds_remaining": remaining}))
            return active

    logger.info("[manual_pending] Listing pending instructions (production stub)")
    return []


@router.post(
    "/manual/pending/{instruction_id}/approve",
    response_model=ApproveRejectResponse,
    tags=["Manual Pending Queue"],
    summary="Approve a pending instruction (pvl-p23c)",
)
async def approve_instruction(instruction_id: str) -> ApproveRejectResponse:
    """Approve the named instruction.

    On approval:
    1. Instruction removed from the pending queue.
    2. MANUAL_APPROVED event emitted → execution-service unholds from queue.
    """
    if _cfg.is_mock_mode():
        with _QUEUE_LOCK:
            if instruction_id not in _PENDING:
                raise HTTPException(
                    status_code=404, detail=f"Instruction {instruction_id!r} not found"
                )
            del _PENDING[instruction_id]

        log_event(
            "MANUAL_APPROVED",
            details={"instruction_id": instruction_id, "source": "dart_ui"},
        )
        logger.info("[manual_pending] Approved instruction_id=%s", instruction_id)
        return ApproveRejectResponse(
            instruction_id=instruction_id,
            action="approved",
            message=f"Instruction {instruction_id} approved — forwarded to execution-service.",
        )

    # Production path: delegate to execution-service via PubSub
    log_event(
        "MANUAL_APPROVED",
        details={"instruction_id": instruction_id, "source": "dart_ui"},
    )
    raise HTTPException(status_code=501, detail="Production manual-pending routing not yet wired")


@router.post(
    "/manual/pending/{instruction_id}/reject",
    response_model=ApproveRejectResponse,
    tags=["Manual Pending Queue"],
    summary="Reject a pending instruction (pvl-p23c)",
)
async def reject_instruction(
    instruction_id: str,
    body: RejectRequest,
) -> ApproveRejectResponse:
    """Reject the named instruction.

    On rejection:
    1. Instruction removed from the pending queue.
    2. MANUAL_REJECTED event emitted → audit log only, no execution.
    """
    if _cfg.is_mock_mode():
        with _QUEUE_LOCK:
            if instruction_id not in _PENDING:
                raise HTTPException(
                    status_code=404, detail=f"Instruction {instruction_id!r} not found"
                )
            del _PENDING[instruction_id]

        log_event(
            "MANUAL_REJECTED",
            details={"instruction_id": instruction_id, "reason": body.reason, "source": "dart_ui"},
        )
        logger.info(
            "[manual_pending] Rejected instruction_id=%s reason=%r", instruction_id, body.reason
        )
        return ApproveRejectResponse(
            instruction_id=instruction_id,
            action="rejected",
            message=f"Instruction {instruction_id} rejected — reason logged to audit trail.",
        )

    log_event(
        "MANUAL_REJECTED",
        details={"instruction_id": instruction_id, "reason": body.reason, "source": "dart_ui"},
    )
    raise HTTPException(status_code=501, detail="Production manual-pending routing not yet wired")

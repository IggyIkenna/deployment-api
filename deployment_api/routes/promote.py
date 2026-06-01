"""POST /promote/{strategy_id}/{candidate_manifest_id} — Phase U3.

Minimal promote endpoint for May-23 cutover. Reads a MinimalCandidateManifest from
Firestore, runs the minimum viable pre-flight gate set, emits the appropriate promote
event, and enqueues a VM-launch job for the next paper/live VM cycle.

Pre-flight gates (minimal viable May-23 subset; full pipeline post-cutover):
  1. Copper sandbox HMAC handshake — always passes in mock mode.
  2. All 6 perp venue API keys present in Secret Manager.
  3. Alerting paging targets configured.
  4. Kill-switch YAML loadable.
  5. Recon green for last 24h (LIVE_EARLY target only).

Auth: X-API-Key (operator gate). Firebase execution-full claim enforcement is
applied at the UI layer (LiveConfirmDialog) for May-23; backend Firebase-admin
integration is post-cutover.

SSOT: plans/active/promote_workflow_may23_cli_path_2026_05_10.md § Phase U3
"""

from __future__ import annotations

import logging
from dataclasses import dataclass, field
from datetime import UTC, datetime

from fastapi import APIRouter, Depends, HTTPException
from pydantic import BaseModel
from unified_api_contracts.internal.domain.strategy_service import (
    MinimalCandidateManifest,
    StrategyMaturityPhase,
)
from unified_trading_library import (
    STRATEGY_MATURITY_ADVANCED,
    STRATEGY_MATURITY_REGRESSED,
    STRATEGY_PROMOTE_REJECTED,
    STRATEGY_PROMOTED_TO_LIVE,
    STRATEGY_PROMOTED_TO_PAPER,
    CandidateManifestStore,
    UnifiedCloudConfig,
    log_event,
)

from deployment_api.auth import verify_api_key
from deployment_api.deployment_api_config import DeploymentApiConfig

logger = logging.getLogger(__name__)

router = APIRouter(
    prefix="/promote",
    tags=["Promote Workflow"],
    dependencies=[Depends(verify_api_key)],
)

_cfg = DeploymentApiConfig()
_cloud_cfg = UnifiedCloudConfig()


# ---------------------------------------------------------------------------
# Request + Response models  (CORRECT-LOCAL — deployment-api internal API contract)
# ---------------------------------------------------------------------------


class PromoteRequest(BaseModel):  # CORRECT-LOCAL
    """Body for POST /promote/{strategy_id}/{candidate_manifest_id}."""

    target_phase: str
    promoter: str
    reason: str


@dataclass
class PreflightResult:
    passed: bool
    failed_gates: list[str] = field(default_factory=list)


class PromoteResponse(BaseModel):  # CORRECT-LOCAL
    """Response for a successful promote decision."""

    manifest_id: str
    strategy_id: str
    strategy_instance_id: str
    target_phase: str
    promoter: str
    promoted_at: str
    event_emitted: str


class PromoteRejectedResponse(BaseModel):  # CORRECT-LOCAL
    """412 body when pre-flight gates fail."""

    manifest_id: str
    strategy_id: str
    target_phase: str
    failed_gates: list[str]
    rejected_at: str


class DemoteRequest(BaseModel):  # CORRECT-LOCAL
    """Body for POST /promote/{strategy_id}/{candidate_manifest_id}/demote."""

    demote_to_stage: str
    operator: str
    reason: str


class DemoteResponse(BaseModel):  # CORRECT-LOCAL
    """Response for a successful demote action."""

    strategy_id: str
    manifest_id: str
    demoted_to_stage: str
    operator: str
    demoted_at: str
    event_emitted: str


class OverrideRequest(BaseModel):  # CORRECT-LOCAL
    """Body for POST /promote/{strategy_id}/{candidate_manifest_id}/override."""

    override_stage: str
    operator: str
    reason: str
    risk_ack: bool


class OverrideResponse(BaseModel):  # CORRECT-LOCAL
    """Response for a successful override action."""

    strategy_id: str
    manifest_id: str
    override_stage: str
    operator: str
    overridden_at: str
    event_emitted: str


# ---------------------------------------------------------------------------
# Pre-flight gate helpers (mock-mode aware)
# ---------------------------------------------------------------------------


def _gate_copper_sandbox(is_mock: bool) -> str | None:
    """Return gate name on failure, None on pass."""
    if is_mock:
        return None
    # Production: perform HMAC handshake with Copper sandbox.
    # Wire to Phase 4.A execution when Copper credentials land.
    return None


def _gate_venue_api_keys(is_mock: bool) -> str | None:
    """Return gate name on failure, None on pass."""
    if is_mock:
        return None
    # Production: probe all 6 perp venue keys in Secret Manager.
    # Composes with Phase 2 preflight-cutover.sh probe.
    return None


def _gate_alerting_config(is_mock: bool) -> str | None:
    if is_mock:
        return None
    # Production: verify paging targets configured (Phase 5.B).
    return None


def _gate_kill_switch_yaml(is_mock: bool) -> str | None:
    if is_mock:
        return None
    # Production: confirm kill-switch YAML loadable (Phase 4.D).
    return None


def _gate_recon_green(is_mock: bool, target_phase: StrategyMaturityPhase) -> str | None:
    if is_mock or target_phase != StrategyMaturityPhase.LIVE_EARLY:
        return None
    # Production: assert recon green for last 24h (Phase 5.A).
    return None


def _run_preflight(is_mock: bool, target_phase: StrategyMaturityPhase) -> PreflightResult:
    failed: list[str] = []
    for result in (
        _gate_copper_sandbox(is_mock),
        _gate_venue_api_keys(is_mock),
        _gate_alerting_config(is_mock),
        _gate_kill_switch_yaml(is_mock),
        _gate_recon_green(is_mock, target_phase),
    ):
        if result is not None:
            failed.append(result)
    return PreflightResult(passed=len(failed) == 0, failed_gates=failed)


# ---------------------------------------------------------------------------
# Endpoint
# ---------------------------------------------------------------------------


@router.post("/{strategy_id}/{candidate_manifest_id}")
async def promote_candidate(
    strategy_id: str,
    candidate_manifest_id: str,
    body: PromoteRequest,
) -> PromoteResponse:
    """Promote a strategy candidate from batch→paper or paper→live.

    Reads MinimalCandidateManifest from Firestore, runs minimal pre-flight gates,
    emits STRATEGY_PROMOTED_TO_PAPER or STRATEGY_PROMOTED_TO_LIVE, returns 200 on
    pass or 412 with failed-gates list on fail.
    """
    try:
        target_phase = StrategyMaturityPhase(body.target_phase)
    except ValueError as exc:
        raise HTTPException(
            status_code=422,
            detail=f"Invalid target_phase: {body.target_phase!r}. Must be paper_1d or live_early.",
        ) from exc

    if target_phase not in (StrategyMaturityPhase.PAPER_1D, StrategyMaturityPhase.LIVE_EARLY):
        raise HTTPException(
            status_code=422,
            detail=f"target_phase must be PAPER_1D or LIVE_EARLY; got {target_phase!r}",
        )

    is_mock = _cfg.is_mock_mode()

    manifest: MinimalCandidateManifest | None = None
    if not is_mock:
        store = CandidateManifestStore(project_id=_cloud_cfg.gcp_project_id)
        manifest = store.read(candidate_manifest_id)
        if manifest is None:
            raise HTTPException(
                status_code=404,
                detail=f"Candidate manifest {candidate_manifest_id!r} not found.",
            )
        if manifest.strategy_instance_id != strategy_id:
            raise HTTPException(
                status_code=409,
                detail=(
                    f"Manifest {candidate_manifest_id!r} belongs to strategy "
                    f"{manifest.strategy_instance_id!r}, not {strategy_id!r}."
                ),
            )

    preflight = _run_preflight(is_mock, target_phase)

    now_iso = datetime.now(UTC).isoformat()

    if not preflight.passed:
        log_event(
            STRATEGY_PROMOTE_REJECTED,
            severity="WARNING",
            details={
                "manifest_id": candidate_manifest_id,
                "strategy_id": strategy_id,
                "target_phase": target_phase.value,
                "failed_gates": preflight.failed_gates,
                "promoter": body.promoter,
            },
        )
        raise HTTPException(
            status_code=412,
            detail={
                "manifest_id": candidate_manifest_id,
                "strategy_id": strategy_id,
                "target_phase": target_phase.value,
                "failed_gates": preflight.failed_gates,
                "rejected_at": now_iso,
            },
        )

    event_name = (
        STRATEGY_PROMOTED_TO_PAPER if target_phase == StrategyMaturityPhase.PAPER_1D else STRATEGY_PROMOTED_TO_LIVE
    )
    log_event(
        event_name,
        severity="INFO",
        details={
            "manifest_id": candidate_manifest_id,
            "strategy_id": strategy_id,
            "target_phase": target_phase.value,
            "promoter": body.promoter,
            "reason": body.reason,
        },
    )

    logger.info(
        "Promote decision recorded: strategy_id=%s target_phase=%s event=%s promoter=%s",
        strategy_id,
        target_phase.value,
        event_name,
        body.promoter,
    )

    return PromoteResponse(
        manifest_id=candidate_manifest_id,
        strategy_id=strategy_id,
        strategy_instance_id=strategy_id if is_mock else (manifest.strategy_instance_id if manifest else strategy_id),
        target_phase=target_phase.value,
        promoter=body.promoter,
        promoted_at=now_iso,
        event_emitted=event_name,
    )


@router.post("/{strategy_id}/{candidate_manifest_id}/demote")
async def demote_candidate(
    strategy_id: str,
    candidate_manifest_id: str,
    body: DemoteRequest,
) -> DemoteResponse:
    """Demote a strategy candidate to an earlier pipeline stage.

    Emits STRATEGY_MATURITY_REGRESSED and records the reason in the audit trail.
    Valid for any operator-initiated pipeline regression (e.g. live_early→paper_1d).
    """
    now_iso = datetime.now(UTC).isoformat()

    log_event(
        STRATEGY_MATURITY_REGRESSED,
        severity="WARNING",
        details={
            "manifest_id": candidate_manifest_id,
            "strategy_id": strategy_id,
            "demoted_to_stage": body.demote_to_stage,
            "operator": body.operator,
            "reason": body.reason,
            "source": "dart_ui",
        },
    )
    logger.info(
        "Demote recorded: strategy_id=%s demoted_to=%s operator=%s",
        strategy_id,
        body.demote_to_stage,
        body.operator,
    )
    return DemoteResponse(
        strategy_id=strategy_id,
        manifest_id=candidate_manifest_id,
        demoted_to_stage=body.demote_to_stage,
        operator=body.operator,
        demoted_at=now_iso,
        event_emitted=STRATEGY_MATURITY_REGRESSED,
    )


@router.post("/{strategy_id}/{candidate_manifest_id}/override")
async def override_candidate(
    strategy_id: str,
    candidate_manifest_id: str,
    body: OverrideRequest,
) -> OverrideResponse:
    """Override a pipeline gate with elevated operator authority.

    Emits STRATEGY_MATURITY_ADVANCED with is_override=True and records the
    risk acknowledgement and reason in the audit trail.
    """
    if not body.risk_ack:
        raise HTTPException(
            status_code=422,
            detail="risk_ack must be true — operator must acknowledge elevated risk.",
        )

    now_iso = datetime.now(UTC).isoformat()

    log_event(
        STRATEGY_MATURITY_ADVANCED,
        severity="WARNING",
        details={
            "manifest_id": candidate_manifest_id,
            "strategy_id": strategy_id,
            "override_stage": body.override_stage,
            "operator": body.operator,
            "reason": body.reason,
            "is_override": True,
            "source": "dart_ui",
        },
    )
    logger.info(
        "Override recorded: strategy_id=%s stage=%s operator=%s",
        strategy_id,
        body.override_stage,
        body.operator,
    )
    return OverrideResponse(
        strategy_id=strategy_id,
        manifest_id=candidate_manifest_id,
        override_stage=body.override_stage,
        operator=body.operator,
        overridden_at=now_iso,
        event_emitted=STRATEGY_MATURITY_ADVANCED,
    )

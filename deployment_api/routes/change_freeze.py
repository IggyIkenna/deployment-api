"""
Change-freeze panel — GET /api/change-freeze/status.

Reads the Firestore verdict-store (``change_freeze_verdicts``, one document per ``check_type`` —
``PROD_DEPLOY`` / ``AUTONOMOUS``) written by unified-trading-pm's
``.github/workflows/change-freeze-check.yml`` every time that reusable workflow runs (invoked by
cloud-build-router.yml / cloud-build-router-aws.yml / freeze-deferred-build-replay.yml /
overnight-agent-orchestrator.yml).

**Read-only proxy — this route NEVER re-evaluates the recurrence/DST calendar logic itself.** The
SSOT evaluator is the inline bash in ``change-freeze-check.yml`` (6 recurrence types + DST notes,
``plans/ops/change-freeze-calendar.csv``); reimplementing that in TypeScript would risk drifting
from the real gate — the exact trap flagged when this panel (G5) was first scoped. The panel
surfaces the workflow's stored pass/fail verdict, not a second implementation.

Plan: unified-trading-pm/plans/active/monitoring_control_plane_master_2026_06_10.md (G5 item).
Operator decision 2026-07-27: build the real Firestore verdict-store generalisation, not a
faithful-port workaround.
"""

from __future__ import annotations

from typing import TypedDict, cast

from fastapi import APIRouter

from deployment_api.deployment_api_config import DeploymentApiConfig
from deployment_api.settings import gcp_project_id as default_project_id

from ._repo_ci_types import _now_iso  # pyright: ignore[reportPrivateUsage]
from ._verdict_store_reader import CHANGE_FREEZE_COLLECTION, get_all_verdicts_with_status

router = APIRouter(prefix="/api/change-freeze", tags=["Change Freeze"])

# The two check_type keys change-freeze-check.yml writes (mirrors its `inputs.check_type` values).
CHECK_TYPES: tuple[str, ...] = ("PROD_DEPLOY", "AUTONOMOUS")


class FreezeVerdictDict(TypedDict):  # CORRECT-LOCAL: deployment-api response shape, not a domain contract
    """One check_type's stored verdict doc, shaped for the UI banner."""

    verdict: str  # "CLEAR" | "BLOCKED" | "UNKNOWN" (never checked yet)
    reason: str | None  # the blocking window's description when verdict == "BLOCKED"
    checked_at: str | None


class ChangeFreezeStatusDict(TypedDict):  # CORRECT-LOCAL: deployment-api response shape, not a domain contract
    generated_at: str
    source: str  # "firestore" | "unavailable" | "mock"
    checks: dict[str, FreezeVerdictDict]


def _shape_doc(doc: dict[str, object]) -> FreezeVerdictDict:
    verdict = doc.get("verdict")
    reasons = doc.get("reasons")
    checked_at = doc.get("checked_at")
    reason = None
    if isinstance(reasons, list) and reasons:
        reason = str(cast("object", reasons[0]))  # pyright: ignore[reportUnknownArgumentType]
    return FreezeVerdictDict(
        verdict=str(verdict) if isinstance(verdict, str) and verdict else "UNKNOWN",
        reason=reason,
        checked_at=str(checked_at) if isinstance(checked_at, str) else None,
    )


def _mock_status() -> ChangeFreezeStatusDict:
    """Deterministic fixture for mock mode / UI dev / the playwright regression spec — one CLEAR,
    one BLOCKED so the banner's both states are exercised."""
    return ChangeFreezeStatusDict(
        generated_at="2026-07-27T12:00:00+00:00",
        source="mock",
        checks={
            "PROD_DEPLOY": FreezeVerdictDict(
                verdict="BLOCKED",
                reason="US market open volatility window (freeze-us-open): 13:25 to 13:55 UTC",
                checked_at="2026-07-27T13:30:00Z",
            ),
            "AUTONOMOUS": FreezeVerdictDict(verdict="CLEAR", reason=None, checked_at="2026-07-27T13:30:00Z"),
        },
    )


@router.get("/status")
async def get_change_freeze_status() -> ChangeFreezeStatusDict:
    """Standing freeze-window banner state for both check types (PROD_DEPLOY / AUTONOMOUS)."""
    cfg = DeploymentApiConfig()
    if cfg.is_mock_mode():
        return _mock_status()

    project_id = default_project_id or ""
    docs, available = get_all_verdicts_with_status(CHANGE_FREEZE_COLLECTION, project_id=project_id)
    source = "firestore" if available else "unavailable"
    # Always report BOTH known check_types (even when Firestore has no doc for one yet) so the UI
    # never has to guess which keys might exist — an absent doc renders verdict="UNKNOWN", honestly.
    checks = {check_type: _shape_doc(docs.get(check_type, {})) for check_type in CHECK_TYPES}
    return ChangeFreezeStatusDict(generated_at=_now_iso(), source=source, checks=checks)

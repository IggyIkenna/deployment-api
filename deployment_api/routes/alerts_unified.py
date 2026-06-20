"""
Unified alert ledger — GET /api/alerts — superset of /api/repo-ci/alerts.

Merges CI/workflow alerts (from the CICD events GCS bucket) with infra-domain
alerts: VM-down, consolidator-down, git-health-guard, worker-liveness, and
data-pipeline alerts.  Each entry carries a ``domain`` discriminator so the UI
can filter/group by alert class.

Only the CI domain is populated in the live path (the CICD events ledger).
Other domains are stub-ready: their watcher emitters will write to the unified
store in a follow-up plan.  Mock mode includes entries for all domains so the
Playwright suite exercises the full unified surface.

Plan: deployment_ui_monitoring_pane_2026_06_19.md § INFRA P1 (unify alert ledger).
"""

from __future__ import annotations

import datetime as dt
from typing import TypedDict

from fastapi import APIRouter

from deployment_api.deployment_api_config import DeploymentApiConfig

from ._repo_ci_alerts import AlertEntryDict, AlertStreamDict, derive_streams, load_alerts_payload

router = APIRouter(prefix="/api/alerts", tags=["Alerts"])

_MAX_ITEMS = 400


class UnifiedAlertEntryDict(TypedDict):  # CORRECT-LOCAL: unified-alerts GCS payload shape (internal)
    """One entry in the unified alert ledger (CI or infra-domain alert)."""

    kind: str
    timestamp: str
    repo: str
    workflow_name: str
    severity: str | None
    conclusion: str | None
    message: str | None
    run_url: str | None
    domain: str  # "ci" | "vm" | "consolidator" | "git-health" | "worker-liveness" | "data-pipeline"


class UnifiedAlertsPayloadDict(TypedDict):  # CORRECT-LOCAL: unified-alerts GCS payload shape (internal)
    """GET /api/alerts response."""

    generated_at: str
    source: str
    alerts: list[UnifiedAlertEntryDict]
    streams: list[AlertStreamDict]  # CI streams (current vs previous per repo/workflow)


def _now_iso() -> str:
    return dt.datetime.now(dt.UTC).isoformat()


def _ci_to_unified(entry: AlertEntryDict, *, domain: str = "ci") -> UnifiedAlertEntryDict:
    return UnifiedAlertEntryDict(
        kind=entry["kind"],
        timestamp=entry["timestamp"],
        repo=entry["repo"],
        workflow_name=entry["workflow_name"],
        severity=entry["severity"],
        conclusion=entry["conclusion"],
        message=entry["message"],
        run_url=entry["run_url"],
        domain=domain,
    )


# ---------------------------------------------------------------------------
# Mock data — CI + all infra domains so Playwright can assert the full surface.
# ---------------------------------------------------------------------------

_MOCK_ENTRIES: list[UnifiedAlertEntryDict] = [
    # CI domain — mirrors _mock_alerts() in _repo_ci_mocks.py for test parity
    UnifiedAlertEntryDict(
        kind="alert",
        timestamp="2026-06-10T12:10:00Z",
        repo="unified-trading-pm",
        workflow_name="ci-status-update",
        severity="CRITICAL",
        conclusion="failure",
        message="CI REGRESSION: deployment-api is now FAILING (was MAIN_GREEN)",
        run_url="https://github.com/IggyIkenna/unified-trading-pm/actions/runs/1",
        domain="ci",
    ),
    UnifiedAlertEntryDict(
        kind="alert",
        timestamp="2026-06-10T12:50:00Z",
        repo="unified-trading-pm",
        workflow_name="ci-status-update",
        severity="INFO",
        conclusion="success",
        message="RESOLVED: deployment-api recovered (FAILING -> MAIN_GREEN)",
        run_url="https://github.com/IggyIkenna/unified-trading-pm/actions/runs/2",
        domain="ci",
    ),
    UnifiedAlertEntryDict(
        kind="alert",
        timestamp="2026-06-10T13:05:00Z",
        repo="execution-service",
        workflow_name="quality-gates-v2",
        severity="CRITICAL",
        conclusion="failure",
        message="quality-gates-v2 FAILED on main",
        run_url="https://github.com/IggyIkenna/execution-service/actions/runs/4",
        domain="ci",
    ),
    # VM domain
    UnifiedAlertEntryDict(
        kind="alert",
        timestamp="2026-06-10T11:00:00Z",
        repo="",
        workflow_name="vm-zombie-watchdog",
        severity="WARNING",
        conclusion=None,
        message="VM agent-orchestrator-vm-1 heartbeat stale (>10 min) — possible OOM or network partition",
        run_url=None,
        domain="vm",
    ),
    # Consolidator domain
    UnifiedAlertEntryDict(
        kind="alert",
        timestamp="2026-06-10T10:30:00Z",
        repo="",
        workflow_name="manifest-consolidator",
        severity="WARNING",
        conclusion=None,
        message="Manifest consolidator stale — last run >2h ago for bucket unified-trading-manifests-prd",
        run_url=None,
        domain="consolidator",
    ),
    # Git-health domain
    UnifiedAlertEntryDict(
        kind="alert",
        timestamp="2026-06-10T11:30:00Z",
        repo="execution-service",
        workflow_name="git-health-guard",
        severity="WARNING",
        conclusion=None,
        message="Slot 3 · laptop: execution-service behind origin/live-defi-rollout by 12 commits",
        run_url=None,
        domain="git-health",
    ),
    # Worker-liveness domain
    UnifiedAlertEntryDict(
        kind="alert",
        timestamp="2026-06-10T12:00:00Z",
        repo="",
        workflow_name="worker-liveness-watchdog",
        severity="WARNING",
        conclusion=None,
        message="Worker slot-5 · vm-planning: heartbeat silent >900s — terminated + AutoSpawn queued",
        run_url=None,
        domain="worker-liveness",
    ),
    # Data-pipeline domain
    UnifiedAlertEntryDict(
        kind="alert",
        timestamp="2026-06-10T09:00:00Z",
        repo="",
        workflow_name="data-pipeline-monitor",
        severity="WARNING",
        conclusion=None,
        message="MTDS defi / hyperliquid: 0 shards captured for 2026-06-09 (expected >50)",
        run_url=None,
        domain="data-pipeline",
    ),
]


def _mock_unified_alerts() -> UnifiedAlertsPayloadDict:
    ordered = sorted(_MOCK_ENTRIES, key=lambda e: e["timestamp"], reverse=True)
    ci_base: list[AlertEntryDict] = [
        AlertEntryDict(
            kind=e["kind"],
            timestamp=e["timestamp"],
            repo=e["repo"],
            workflow_name=e["workflow_name"],
            severity=e["severity"],
            conclusion=e["conclusion"],
            message=e["message"],
            run_url=e["run_url"],
        )
        for e in _MOCK_ENTRIES
        if e["domain"] == "ci"
    ]
    return UnifiedAlertsPayloadDict(
        generated_at=_now_iso(),
        source="mock",
        alerts=ordered[:_MAX_ITEMS],
        streams=derive_streams(ci_base),
    )


# ---------------------------------------------------------------------------
# Live path
# ---------------------------------------------------------------------------


async def load_unified_alerts_payload() -> UnifiedAlertsPayloadDict:
    """Merge CI alerts from the CICD events ledger with infra-domain alerts.

    Currently: CI domain populated from GCS; all other domains are stubs (empty
    until the respective watcher-emitters write to the unified store in a
    follow-up plan).  Streams are derived from CI entries only (repo+workflow
    lifecycle pairs).
    """
    ci_payload = await load_alerts_payload()
    ci_unified: list[UnifiedAlertEntryDict] = [
        _ci_to_unified(e) for e in ci_payload["alerts"]
    ]
    ci_unified.sort(key=lambda e: e["timestamp"], reverse=True)
    return UnifiedAlertsPayloadDict(
        generated_at=ci_payload["generated_at"],
        source=ci_payload["source"],
        alerts=ci_unified[:_MAX_ITEMS],
        streams=ci_payload["streams"],
    )


# ---------------------------------------------------------------------------
# Route
# ---------------------------------------------------------------------------


@router.get("")
async def get_unified_alerts() -> UnifiedAlertsPayloadDict:
    """Unified alert ledger — superset of /api/repo-ci/alerts.

    Domains: ci (CICD events GCS ledger), vm, consolidator, git-health,
    worker-liveness, data-pipeline.  The ``domain`` field on every entry
    lets the UI filter/group by alert class.
    """
    cfg = DeploymentApiConfig()
    if cfg.is_mock_mode():
        return _mock_unified_alerts()
    return await load_unified_alerts_payload()

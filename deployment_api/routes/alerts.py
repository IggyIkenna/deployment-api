"""
GET /api/alerts — unified alert ledger (CI + infra).

Superset of /api/repo-ci/alerts: returns all Slack CI alerts, CI workflow events,
AND infra alerts emitted by agent-orchestrator (slot-stale, worker-polling-dead, etc.)
that are written to gs://unified-trading-cicd-events/infra/alerts/{date}/alerts.jsonl.

Plan: deployment_ui_monitoring_pane_2026_06_19.md (task 004).
"""

from __future__ import annotations

import datetime as dt

from fastapi import APIRouter

from deployment_api.deployment_api_config import DeploymentApiConfig

from ._alerts import UnifiedAlertsPayloadDict, load_unified_alerts_payload

router = APIRouter(prefix="/api/alerts", tags=["Alerts"])


def _mock_unified_alerts() -> UnifiedAlertsPayloadDict:
    return UnifiedAlertsPayloadDict(
        generated_at=dt.datetime.now(dt.UTC).isoformat(),
        source="mock",
        alerts=[],
        streams=[],
        infra_alert_count=0,
    )


@router.get("")
async def get_unified_alerts() -> UnifiedAlertsPayloadDict:
    """Unified alert ledger: CI alerts + infra alerts (VM-down, worker-liveness, git-health).

    Returns all alert classes in a single payload so the deployment-ui Monitoring Pane
    shows the full operational picture without toggling between dashboards.
    """
    cfg = DeploymentApiConfig()
    if cfg.is_mock_mode():
        return _mock_unified_alerts()
    return await load_unified_alerts_payload()

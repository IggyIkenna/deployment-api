"""
GET /api/alerts — unified alert ledger (all alert classes, not just CI/CD).

Non-CI watchers (VM-down, consolidator-down, git-health-guard, worker-liveness) persist
alerts via `_persist_to_gcs()` in agent-orchestrator/server/notifications/slack.py.
They write to the same `cicd/alerts/{date}/alerts.jsonl` GCS store as `notify-slack.yml`,
but include an `alert_class` field ("worker_liveness", "git_health", "vm_down",
"consolidator_down") that `_parse_line()` maps to the entry `kind`.

The `alerts` list includes all non-"event" kinds (CI "alert" + all non-CI kinds).
The `streams` list groups all kinds into (repo, workflow) lifecycle pairs.

Plan: deployment_ui_monitoring_pane_2026_06_19.md (unified ledger UI P1).
"""

from __future__ import annotations

from fastapi import APIRouter

from deployment_api.deployment_api_config import DeploymentApiConfig

from ._repo_ci_alerts import AlertsPayloadDict, load_alerts_payload
from ._repo_ci_mocks import _mock_alerts

router = APIRouter()


@router.get("/alerts")
async def get_unified_alerts() -> AlertsPayloadDict:
    """Return all persisted alerts across all alert classes.

    Shape is a superset of /api/repo-ci/alerts: same fields, extensible `kind`
    (currently "alert" | "event" for CI/CD; INFRA P1 adds non-CI kinds).
    """
    cfg = DeploymentApiConfig()
    if cfg.is_mock_mode():
        return _mock_alerts()
    return await load_alerts_payload()

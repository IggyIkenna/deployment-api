"""
GET /api/alerts — unified alert ledger (all alert classes, not just CI/CD).

Currently surfaces CI/CD alerts only (same backend as /api/repo-ci/alerts).
When INFRA P1 lands (alert_quality_overhaul_2026_06_18.md), non-CI watchers
(VM-down, consolidator-down, git-health-guard, worker-liveness, data-pipeline)
will emit into the shared store and new `kind` values will appear in the response
(e.g. "vm_down", "consolidator_down", "git_health", "worker_liveness").

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

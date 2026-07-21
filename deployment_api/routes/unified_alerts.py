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

from fastapi import APIRouter, Query

from deployment_api.deployment_api_config import DeploymentApiConfig

from ._repo_ci_alerts import (
    _DEFAULT_DAYS,
    _DEFAULT_PAGE_SIZE,
    _MAX_DAYS,
    _MAX_PAGE_SIZE,
    AlertsPayloadDict,
    load_alerts_payload,
)
from ._repo_ci_mocks import _mock_alerts

router = APIRouter()


@router.get("/alerts")
async def get_unified_alerts(
    days: int = Query(_DEFAULT_DAYS, ge=1, le=_MAX_DAYS),
    offset: int = Query(0, ge=0),
    limit: int = Query(_DEFAULT_PAGE_SIZE, ge=1, le=_MAX_PAGE_SIZE),
) -> AlertsPayloadDict:
    """Return persisted alerts across all alert classes, paginated over a bounded day-window.

    Shape is a superset of /api/repo-ci/alerts: same fields, extensible `kind`
    (currently "alert" | "event" for CI/CD; INFRA P1 adds non-CI kinds). `days`/`offset`/`limit`
    let Plan B's date-range filter query past the default window without a silent truncation —
    see `capped` in the response.
    """
    cfg = DeploymentApiConfig()
    if cfg.is_mock_mode():
        return _mock_alerts()
    return await load_alerts_payload(days=days, offset=offset, limit=limit)

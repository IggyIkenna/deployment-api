"""
GET /api/alerts — unified alert ledger (all alert classes).

Merges two sources:
  1. CI/CD alerts from gs://{CICD_EVENTS_BUCKET}/cicd/alerts/ and cicd/events/
     (same backend as /api/repo-ci/alerts, kind="alert"|"event").
  2. Non-CI ops alerts from gs://{CICD_EVENTS_BUCKET}/ops/alerts/ written by
     agent-orchestrator and other ops watchers (kind="git_health"|"worker_liveness"|
     "vm_down"|"consolidator_down"|"data_pipeline").

The merged response is a superset of /api/repo-ci/alerts with the same
AlertsPayloadDict shape; streams derive from the full merged entry set.

Plan: deployment_ui_monitoring_pane_2026_06_19.md (unified ledger UI P1 + INFRA P1).
"""

from __future__ import annotations

import datetime as dt

from fastapi import APIRouter

from deployment_api.deployment_api_config import DeploymentApiConfig

from ._non_ci_alerts import load_non_ci_alerts
from ._repo_ci_alerts import AlertEntryDict, AlertsPayloadDict, derive_streams, load_alerts_payload
from ._repo_ci_mocks import _mock_alerts

router = APIRouter()

_MAX_ITEMS = 400


@router.get("/alerts")
async def get_unified_alerts() -> AlertsPayloadDict:
    """Return all persisted alerts across all alert classes (CI/CD + ops).

    Shape is a superset of /api/repo-ci/alerts: same AlertsPayloadDict fields,
    extensible `kind` (CI/CD: "alert"|"event"; ops: "git_health"|"worker_liveness"|
    "vm_down"|"consolidator_down"|"data_pipeline").

    Streams derive from the merged entry set so CI and non-CI events appear in
    the same per-(repo/source, workflow) lifecycle view.
    """
    cfg = DeploymentApiConfig()
    if cfg.is_mock_mode():
        return _mock_alerts()
    ci_payload = await load_alerts_payload()
    non_ci_entries = await load_non_ci_alerts()

    # Merge: CI entries (alerts + events) + non-CI ops entries, newest-first.
    ci_entries: list[AlertEntryDict] = ci_payload["alerts"]
    all_entries: list[AlertEntryDict] = ci_entries + non_ci_entries
    all_entries.sort(key=lambda e: e["timestamp"], reverse=True)
    all_entries = all_entries[:_MAX_ITEMS]

    return AlertsPayloadDict(
        generated_at=dt.datetime.now(dt.UTC).isoformat(),
        source="live",
        alerts=all_entries,
        streams=derive_streams(all_entries),
    )

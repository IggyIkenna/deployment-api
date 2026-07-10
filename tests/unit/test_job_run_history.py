"""Unit tests for the WS-D #11 Cloud Run job run-history in the detail popover.

Credential-free: the executions client is mocked. Pins the contract — a non-job kind gets no
history; a Cloud Run job maps its executions to the run_history vector (name/status/times/duration).
"""

from __future__ import annotations

import os

os.environ.setdefault("CLOUD_MOCK_MODE", "false")
os.environ.setdefault("CLOUD_PROVIDER", "local")
os.environ.setdefault("GCP_PROJECT_ID", "test-project")
os.environ.setdefault("DISABLE_AUTH", "true")

from unittest.mock import patch

from deployment_api.routes import deployments_inventory as inv
from deployment_api.routes._cloud_run_executions import ExecutionRecord
from deployment_api.routes.deployments_inventory import (  # pyright: ignore[reportPrivateUsage]
    DeploymentItem,
    _job_run_history,
)


def _item(kind: str, cloud: str = "GCP") -> DeploymentItem:
    return DeploymentItem(
        name="prd-x", kind=kind, umbrella="BATCH", cloud=cloud, service="x", asset_group="cefi", status="succeeded"
    )


def test_run_history_empty_for_non_job_kinds() -> None:
    assert _job_run_history(_item("VM")) == []
    assert _job_run_history(_item("CLOUD_RUN_SERVICE")) == []
    assert _job_run_history(_item("CLOUD_RUN_JOB", cloud="AWS")) == []  # AWS Batch job, not GCP Cloud Run


def test_run_history_maps_executions_for_a_gcp_job() -> None:
    records = [
        ExecutionRecord(
            name="ex1",
            status="succeeded",
            started_at="2026-07-10T10:00:00+00:00",
            completed_at="2026-07-10T10:05:00+00:00",
            duration_seconds=300.0,
        ),
        ExecutionRecord(
            name="ex2",
            status="failed",
            started_at="2026-07-10T09:00:00+00:00",
            completed_at=None,
            duration_seconds=None,
        ),
    ]
    # Replace the whole _cfg reference with a mock (the pydantic config instance can't have its
    # methods patched in place) so is_mock_mode()==False + a project id resolve deterministically,
    # regardless of the shared config singleton's mock-mode state under xdist.
    with (
        patch.object(inv, "list_job_executions", return_value=records),
        patch.object(inv, "_cfg") as mock_cfg,
    ):
        mock_cfg.is_mock_mode.return_value = False
        mock_cfg.require_gcp_project_id.return_value = "p"
        history = _job_run_history(_item("CLOUD_RUN_JOB"))
    assert [h["name"] for h in history] == ["ex1", "ex2"]
    assert history[0]["status"] == "succeeded"
    assert history[0]["duration_seconds"] == 300.0
    assert history[1]["completed_at"] is None

"""Unit tests for monitor_scheduled routes — Phase D.2 (deploy-missing) + D.3 (pause/resume).

Tests run in mock mode (CLOUD_MOCK_MODE=true) so no real gcloud calls are made.
"""

from __future__ import annotations

from unittest.mock import patch

import pytest
from fastapi.testclient import TestClient

from deployment_api.main import app
from deployment_api.routes.monitor_scheduled import (
    _build_gcloud_create_cmd,
    _build_gcloud_pause_cmd,
    _build_gcloud_resume_cmd,
    _resolve_cloud_target,
    _resolve_env_tier,
)

_client = TestClient(app, raise_server_exceptions=True)

_AUTH = {"X-API-Key": "test-api-key"}


@pytest.fixture(autouse=True)
def _mock_auth(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(
        "deployment_api.firebase_auth.verify_any_auth",
        lambda: None,
    )


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def test_resolve_cloud_target_gcp() -> None:
    from unified_api_contracts.canonical.crosscutting.cloud_target import CloudTarget

    assert _resolve_cloud_target("gcp") == CloudTarget.GCP
    assert _resolve_cloud_target("GCP") == CloudTarget.GCP


def test_resolve_cloud_target_aws() -> None:
    from unified_api_contracts.canonical.crosscutting.cloud_target import CloudTarget

    assert _resolve_cloud_target("aws") == CloudTarget.AWS


def test_resolve_cloud_target_fallback() -> None:
    from unified_api_contracts.canonical.crosscutting.cloud_target import CloudTarget

    assert _resolve_cloud_target("unknown") == CloudTarget.GCP


def test_resolve_env_tier_staging(monkeypatch: pytest.MonkeyPatch) -> None:
    from unified_api_contracts.canonical.crosscutting.environment_tier import EnvironmentTier

    monkeypatch.setattr("deployment_api.routes.monitor_scheduled._settings.DEPLOYMENT_ENV", "staging")
    assert _resolve_env_tier() == EnvironmentTier.STAGING


def test_resolve_env_tier_prod(monkeypatch: pytest.MonkeyPatch) -> None:
    from unified_api_contracts.canonical.crosscutting.environment_tier import EnvironmentTier

    monkeypatch.setattr("deployment_api.routes.monitor_scheduled._settings.DEPLOYMENT_ENV", "production")
    assert _resolve_env_tier() == EnvironmentTier.PROD


def test_resolve_env_tier_dev(monkeypatch: pytest.MonkeyPatch) -> None:
    from unified_api_contracts.canonical.crosscutting.environment_tier import EnvironmentTier

    monkeypatch.setattr("deployment_api.routes.monitor_scheduled._settings.DEPLOYMENT_ENV", "development")
    assert _resolve_env_tier() == EnvironmentTier.DEV


# ---------------------------------------------------------------------------
# gcloud command builders
# ---------------------------------------------------------------------------


def test_build_gcloud_create_cmd_contains_schedule() -> None:
    from unified_api_contracts.canonical.crosscutting.environment_tier import EnvironmentTier
    from unified_api_contracts.canonical.crosscutting.lifecycle_class import LifecycleClass
    from unified_api_contracts.canonical.crosscutting.scheduler_registry import SchedulerSpec, SchedulerTargetKind

    spec = SchedulerSpec(
        name="test-job",
        lifecycle_class=LifecycleClass.SCHEDULED_RECURRING,
        schedule_cron="*/15 * * * *",
        target_kind=SchedulerTargetKind.CLOUD_RUN,
        target_ref="instruments-service",
        asset_group="cefi",
        owning_plan="test-plan",
        expected_max_runtime_seconds=600,
        max_consecutive_failures_before_page=3,
        env_tiers=frozenset({EnvironmentTier.STAGING}),
    )
    cmd = _build_gcloud_create_cmd(spec, "my-project", "us-central1")
    assert "gcloud" in cmd
    assert "scheduler" in cmd
    assert "test-job" in cmd
    assert "*/15 * * * *" in cmd
    assert "my-project" in cmd


def test_build_gcloud_pause_cmd() -> None:
    cmd = _build_gcloud_pause_cmd("cefi-ohlcv-15m", "my-project", "us-central1")
    assert "pause" in cmd
    assert "cefi-ohlcv-15m" in cmd
    assert "my-project" in cmd


def test_build_gcloud_resume_cmd() -> None:
    cmd = _build_gcloud_resume_cmd("cefi-ohlcv-15m", "my-project", "us-central1")
    assert "resume" in cmd
    assert "cefi-ohlcv-15m" in cmd


# ---------------------------------------------------------------------------
# POST /api/monitor/scheduled/deploy-missing — mock mode (dry_run=true)
# ---------------------------------------------------------------------------


def test_deploy_missing_dry_run_returns_preview() -> None:
    resp = _client.post(
        "/api/monitor/scheduled/deploy-missing",
        params={"cloud": "gcp", "dry_run": "true"},
        headers=_AUTH,
    )
    assert resp.status_code == 200
    body = resp.json()
    assert "results" in body
    assert "total_checked" in body
    for entry in body["results"]:
        assert entry["status"] in {"preview", "already_deployed", "deployed", "failed"}


def test_deploy_missing_mock_mode_no_gcloud_calls() -> None:
    with patch("deployment_api.routes.monitor_scheduled._gcloud_job_exists") as mock_exists:
        with patch("deployment_api.routes.monitor_scheduled._run_gcloud_cmd") as mock_run:
            resp = _client.post(
                "/api/monitor/scheduled/deploy-missing",
                params={"cloud": "gcp", "dry_run": "true"},
                headers=_AUTH,
            )
    assert resp.status_code == 200
    mock_exists.assert_not_called()
    mock_run.assert_not_called()


def test_deploy_missing_response_schema() -> None:
    resp = _client.post(
        "/api/monitor/scheduled/deploy-missing",
        params={"dry_run": "true"},
        headers=_AUTH,
    )
    assert resp.status_code == 200
    body = resp.json()
    assert isinstance(body["total_checked"], int)
    assert isinstance(body["deployed_count"], int)
    assert isinstance(body["skipped_count"], int)
    assert isinstance(body["failed_count"], int)
    assert isinstance(body["cloud"], str)
    assert isinstance(body["env"], str)
    assert isinstance(body["executed_at"], str)


# ---------------------------------------------------------------------------
# POST /api/monitor/scheduled/{name}/pause — dry_run
# ---------------------------------------------------------------------------


def test_pause_scheduler_dry_run() -> None:
    resp = _client.post(
        "/api/monitor/scheduled/cefi-ohlcv-15m/pause",
        params={"dry_run": "true"},
        headers=_AUTH,
    )
    assert resp.status_code == 200
    body = resp.json()
    assert body["name"] == "cefi-ohlcv-15m"
    assert body["action"] == "pause"
    assert body["status"] == "preview"
    assert "gcloud" in body["command"]
    assert "pause" in body["command"]


def test_pause_scheduler_aws_dry_run() -> None:
    resp = _client.post(
        "/api/monitor/scheduled/cefi-ohlcv-15m/pause",
        params={"cloud": "aws", "dry_run": "true"},
        headers=_AUTH,
    )
    assert resp.status_code == 200
    body = resp.json()
    assert "disable-rule" in body["command"]


# ---------------------------------------------------------------------------
# POST /api/monitor/scheduled/{name}/resume — dry_run
# ---------------------------------------------------------------------------


def test_resume_scheduler_dry_run() -> None:
    resp = _client.post(
        "/api/monitor/scheduled/cefi-ohlcv-15m/resume",
        params={"dry_run": "true"},
        headers=_AUTH,
    )
    assert resp.status_code == 200
    body = resp.json()
    assert body["action"] == "resume"
    assert body["status"] == "preview"
    assert "resume" in body["command"]


def test_resume_scheduler_aws_dry_run() -> None:
    resp = _client.post(
        "/api/monitor/scheduled/cefi-ohlcv-15m/resume",
        params={"cloud": "aws", "dry_run": "true"},
        headers=_AUTH,
    )
    assert resp.status_code == 200
    body = resp.json()
    assert "enable-rule" in body["command"]

"""Unit tests for monitor_live routes — Phase E.2 lifecycle actions.

Tests run in mock mode (CLOUD_MOCK_MODE=true) so no real cloud calls are made.
"""

from __future__ import annotations

import pytest
from fastapi.testclient import TestClient

from deployment_api.main import app
from deployment_api.routes.monitor_live import (
    _build_cluster_action_cmd,
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
# Helper unit tests
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

    monkeypatch.setattr("deployment_api.routes.monitor_live._settings.DEPLOYMENT_ENV", "staging")
    assert _resolve_env_tier() == EnvironmentTier.STAGING


def test_resolve_env_tier_prod(monkeypatch: pytest.MonkeyPatch) -> None:
    from unified_api_contracts.canonical.crosscutting.environment_tier import EnvironmentTier

    monkeypatch.setattr("deployment_api.routes.monitor_live._settings.DEPLOYMENT_ENV", "production")
    assert _resolve_env_tier() == EnvironmentTier.PROD


# ---------------------------------------------------------------------------
# _build_cluster_action_cmd
# ---------------------------------------------------------------------------


def _make_cloud_run_spec() -> object:
    from unified_api_contracts.canonical.crosscutting.cloud_target import CloudTarget
    from unified_api_contracts.canonical.crosscutting.environment_tier import EnvironmentTier
    from unified_api_contracts.canonical.crosscutting.lifecycle_class import LifecycleClass
    from unified_api_contracts.canonical.crosscutting.live_cluster_registry import (
        LiveClusterDeploymentKind,
        LiveClusterSpec,
    )

    return LiveClusterSpec(
        name="mtds-live-binance",
        lifecycle_class=LifecycleClass.LONG_LIVED_LIVE,
        cloud_target=CloudTarget.GCP,
        environment_tier=EnvironmentTier.STAGING,
        deployment_kind=LiveClusterDeploymentKind.CLOUD_RUN,
        target_ref="mtds-live-binance",
        asset_group="cefi",
        archetype_owners=("carry_staked_basis",),
        health_endpoint="/health",
        expected_replicas=1,
        drain_timeout=60,
    )


def test_build_cmd_cloud_run_stop() -> None:
    spec = _make_cloud_run_spec()
    cmd = _build_cluster_action_cmd(spec, "stop", "my-project")
    assert "gcloud" in cmd
    assert "min-instances=0" in cmd
    assert "my-project" in cmd


def test_build_cmd_cloud_run_start() -> None:
    spec = _make_cloud_run_spec()
    cmd = _build_cluster_action_cmd(spec, "start", "my-project")
    assert "gcloud" in cmd
    assert "min-instances=1" in cmd


def test_build_cmd_cloud_run_restart() -> None:
    spec = _make_cloud_run_spec()
    cmd = _build_cluster_action_cmd(spec, "restart", "my-project")
    assert "gcloud" in cmd
    assert "no-traffic" in cmd


def test_build_cmd_cloud_run_drain() -> None:
    spec = _make_cloud_run_spec()
    cmd = _build_cluster_action_cmd(spec, "drain", "my-project")
    assert "update-traffic" in cmd
    assert "LATEST=0" in cmd


def test_build_cmd_cloud_run_pause() -> None:
    spec = _make_cloud_run_spec()
    cmd = _build_cluster_action_cmd(spec, "pause", "my-project")
    assert "min-instances=0" in cmd


# ---------------------------------------------------------------------------
# POST /api/monitor/live/{name}/{action} — dry_run (mock mode)
# ---------------------------------------------------------------------------


@pytest.mark.parametrize("action", ["start", "stop", "pause", "restart", "drain"])
def test_lifecycle_action_404_for_unknown_cluster(action: str) -> None:
    resp = _client.post(
        f"/api/monitor/live/nonexistent-cluster/{action}",
        params={"dry_run": "true"},
        headers=_AUTH,
    )
    assert resp.status_code == 404


@pytest.mark.parametrize("action", ["start", "stop", "pause", "restart", "drain"])
def test_lifecycle_action_preview_for_known_cluster(action: str, monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr("deployment_api.routes.monitor_live._settings.DEPLOYMENT_ENV", "staging")
    resp = _client.post(
        f"/api/monitor/live/mtds-live-binance/{action}",
        params={"dry_run": "true", "cloud": "gcp"},
        headers=_AUTH,
    )
    assert resp.status_code == 200
    body = resp.json()
    assert body["name"] == "mtds-live-binance"
    assert body["action"] == action
    assert body["status"] == "preview"
    assert body["command"]
    assert "executed_at" in body


def test_start_dry_run_contains_min_instances(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr("deployment_api.routes.monitor_live._settings.DEPLOYMENT_ENV", "staging")
    resp = _client.post(
        "/api/monitor/live/mtds-live-binance/start",
        params={"dry_run": "true", "cloud": "gcp"},
        headers=_AUTH,
    )
    body = resp.json()
    assert "min-instances" in body["command"]


def test_stop_dry_run_contains_min_instances_0(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr("deployment_api.routes.monitor_live._settings.DEPLOYMENT_ENV", "staging")
    resp = _client.post(
        "/api/monitor/live/mtds-live-binance/stop",
        params={"dry_run": "true", "cloud": "gcp"},
        headers=_AUTH,
    )
    body = resp.json()
    assert "min-instances=0" in body["command"]


def test_drain_dry_run_contains_update_traffic(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr("deployment_api.routes.monitor_live._settings.DEPLOYMENT_ENV", "staging")
    resp = _client.post(
        "/api/monitor/live/mtds-live-binance/drain",
        params={"dry_run": "true", "cloud": "gcp"},
        headers=_AUTH,
    )
    body = resp.json()
    assert "update-traffic" in body["command"]


def test_aws_execution_stop_dry_run(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr("deployment_api.routes.monitor_live._settings.DEPLOYMENT_ENV", "staging")
    resp = _client.post(
        "/api/monitor/live/execution-live-aws/stop",
        params={"dry_run": "true", "cloud": "aws"},
        headers=_AUTH,
    )
    assert resp.status_code == 200
    body = resp.json()
    assert "ecs" in body["command"] or "aws" in body["command"]
    assert "desired-count=0" in body["command"]

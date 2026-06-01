"""Unit tests for GET /api/health/detailed endpoint.

Tests per-component status (GCS, Pub/Sub, Secret Manager, deployment-events)
in both mock mode and production mode (component up/down states).
"""

from __future__ import annotations

from unittest.mock import MagicMock, patch

from fastapi import FastAPI
from fastapi.testclient import TestClient

from deployment_api.health_routes import (
    _check_deployment_events,
    _check_gcs,
    _check_pubsub,
    _check_secret_manager,
)
from deployment_api.health_routes import (
    router as health_router,
)


def _make_client() -> TestClient:
    app = FastAPI()
    app.include_router(health_router)
    return TestClient(app, raise_server_exceptions=False)


# ---------------------------------------------------------------------------
# Endpoint contract
# ---------------------------------------------------------------------------


class TestDetailedHealthEndpoint:
    def test_returns_200(self) -> None:
        with patch("deployment_api.health_routes._cloud_cfg") as mock_cfg:
            mock_cfg.is_mock_mode.return_value = True
            client = _make_client()
            resp = client.get("/api/health/detailed")
        assert resp.status_code == 200

    def test_response_has_required_keys(self) -> None:
        with patch("deployment_api.health_routes._cloud_cfg") as mock_cfg:
            mock_cfg.is_mock_mode.return_value = True
            client = _make_client()
            body = client.get("/api/health/detailed").json()
        assert "status" in body
        assert "components" in body
        assert "version" in body
        assert "checked_at" in body
        assert "mock_mode" in body

    def test_components_has_four_keys(self) -> None:
        with patch("deployment_api.health_routes._cloud_cfg") as mock_cfg:
            mock_cfg.is_mock_mode.return_value = True
            client = _make_client()
            body = client.get("/api/health/detailed").json()
        comps = body["components"]
        assert "gcs" in comps
        assert "pubsub" in comps
        assert "secret_manager" in comps
        assert "deployment_events" in comps

    def test_mock_mode_all_components_up(self) -> None:
        with patch("deployment_api.health_routes._cloud_cfg") as mock_cfg:
            mock_cfg.is_mock_mode.return_value = True
            client = _make_client()
            body = client.get("/api/health/detailed").json()
        comps = body["components"]
        for name, comp in comps.items():
            assert comp["status"] == "up", f"{name} not up in mock mode"

    def test_mock_mode_status_healthy(self) -> None:
        with patch("deployment_api.health_routes._cloud_cfg") as mock_cfg:
            mock_cfg.is_mock_mode.return_value = True
            client = _make_client()
            body = client.get("/api/health/detailed").json()
        assert body["status"] == "healthy"
        assert body["mock_mode"] is True

    def test_all_up_status_healthy(self) -> None:
        up_result = {"status": "up", "detail": None}
        with (
            patch("deployment_api.health_routes._cloud_cfg") as mock_cfg,
            patch("deployment_api.health_routes._check_gcs", return_value=up_result),
            patch("deployment_api.health_routes._check_pubsub", return_value=up_result),
            patch("deployment_api.health_routes._check_secret_manager", return_value=up_result),
            patch("deployment_api.health_routes._check_deployment_events", return_value=up_result),
        ):
            mock_cfg.is_mock_mode.return_value = False
            mock_cfg.gcp_project_id = "test-project"
            client = _make_client()
            body = client.get("/api/health/detailed").json()
        assert body["status"] == "healthy"
        assert body["mock_mode"] is False

    def test_one_component_down_yields_degraded(self) -> None:
        up = {"status": "up", "detail": None}
        down = {"status": "down", "detail": "connection refused"}
        with (
            patch("deployment_api.health_routes._cloud_cfg") as mock_cfg,
            patch("deployment_api.health_routes._check_gcs", return_value=down),
            patch("deployment_api.health_routes._check_pubsub", return_value=up),
            patch("deployment_api.health_routes._check_secret_manager", return_value=up),
            patch("deployment_api.health_routes._check_deployment_events", return_value=up),
        ):
            mock_cfg.is_mock_mode.return_value = False
            mock_cfg.gcp_project_id = "test-project"
            client = _make_client()
            body = client.get("/api/health/detailed").json()
        assert body["status"] == "degraded"
        assert body["components"]["gcs"]["status"] == "down"

    def test_component_detail_preserved_in_response(self) -> None:
        up = {"status": "up", "detail": None}
        down = {"status": "down", "detail": "ECONNREFUSED: 127.0.0.1:8085"}
        with (
            patch("deployment_api.health_routes._cloud_cfg") as mock_cfg,
            patch("deployment_api.health_routes._check_gcs", return_value=up),
            patch("deployment_api.health_routes._check_pubsub", return_value=down),
            patch("deployment_api.health_routes._check_secret_manager", return_value=up),
            patch("deployment_api.health_routes._check_deployment_events", return_value=up),
        ):
            mock_cfg.is_mock_mode.return_value = False
            mock_cfg.gcp_project_id = "test-project"
            client = _make_client()
            body = client.get("/api/health/detailed").json()
        assert body["components"]["pubsub"]["detail"] == "ECONNREFUSED: 127.0.0.1:8085"


# ---------------------------------------------------------------------------
# Per-component probe functions
# ---------------------------------------------------------------------------


class TestCheckGcs:
    def test_up_when_storage_client_succeeds(self) -> None:
        with patch("unified_trading_library.get_storage_client", return_value=MagicMock()):
            result = _check_gcs()
        assert result["status"] == "up"
        assert result["detail"] is None

    def test_down_when_exception_raised(self) -> None:
        with patch("unified_trading_library.get_storage_client", side_effect=RuntimeError("GCS offline")):
            result = _check_gcs()
        assert result["status"] == "down"
        assert "GCS offline" in str(result["detail"])


class TestCheckPubsub:
    def test_up_when_subscriber_succeeds(self) -> None:
        mock_sub = MagicMock()
        mock_sub.list_subscriptions.return_value = iter([])
        with patch("google.cloud.pubsub_v1.SubscriberClient", return_value=mock_sub):
            with patch("deployment_api.health_routes._cloud_cfg") as mock_cfg:
                mock_cfg.gcp_project_id = "test-proj"
                result = _check_pubsub()
        assert result["status"] == "up"

    def test_down_when_subscriber_raises(self) -> None:
        with patch("google.cloud.pubsub_v1.SubscriberClient", side_effect=Exception("pubsub down")):
            with patch("deployment_api.health_routes._cloud_cfg") as mock_cfg:
                mock_cfg.gcp_project_id = "test-proj"
                result = _check_pubsub()
        assert result["status"] == "down"
        assert "pubsub down" in str(result["detail"])


class TestCheckSecretManager:
    def test_up_when_client_succeeds(self) -> None:
        mock_client = MagicMock()
        mock_client.list_secrets.return_value = iter([])
        with patch("google.cloud.secretmanager.SecretManagerServiceClient", return_value=mock_client):
            with patch("deployment_api.health_routes._cloud_cfg") as mock_cfg:
                mock_cfg.gcp_project_id = "test-proj"
                result = _check_secret_manager()
        assert result["status"] == "up"

    def test_down_when_client_raises(self) -> None:
        with patch(
            "google.cloud.secretmanager.SecretManagerServiceClient",
            side_effect=Exception("secret manager offline"),
        ):
            with patch("deployment_api.health_routes._cloud_cfg") as mock_cfg:
                mock_cfg.gcp_project_id = "test-proj"
                result = _check_secret_manager()
        assert result["status"] == "down"
        assert "secret manager offline" in str(result["detail"])


class TestCheckDeploymentEvents:
    def test_up_when_topic_found(self) -> None:
        mock_pub = MagicMock()
        mock_pub.topic_path.return_value = "projects/test-proj/topics/deployment-api-events"
        mock_pub.get_topic.return_value = MagicMock()
        with patch("google.cloud.pubsub_v1.PublisherClient", return_value=mock_pub):
            with patch("deployment_api.health_routes._cloud_cfg") as mock_cfg:
                mock_cfg.gcp_project_id = "test-proj"
                result = _check_deployment_events()
        assert result["status"] == "up"

    def test_down_when_topic_missing(self) -> None:
        with patch(
            "google.cloud.pubsub_v1.PublisherClient",
            side_effect=Exception("topic not found"),
        ):
            with patch("deployment_api.health_routes._cloud_cfg") as mock_cfg:
                mock_cfg.gcp_project_id = "test-proj"
                result = _check_deployment_events()
        assert result["status"] == "down"
        assert "topic not found" in str(result["detail"])

"""Unit tests for POST /promote/{strategy_id}/{candidate_manifest_id} — Phase U3.

Covers:
- 200 OK for PAPER_1D promote in mock mode.
- 200 OK for LIVE_EARLY promote in mock mode.
- Correct event_emitted in response.
- 422 on invalid target_phase.
- 412 never fires in mock mode (all gates pass).
- Response schema has all required fields.

Tests mount the router on a minimal FastAPI app (no lifespan stack) and force
mock mode via CLOUD_MOCK_MODE=true so no real GCS / Firestore deps are needed.
"""

from __future__ import annotations

from unittest.mock import patch

import pytest
from fastapi import FastAPI
from fastapi.testclient import TestClient

from deployment_api.routes.promote import router

_PATCH_MOCK = "deployment_api.deployment_api_config.DeploymentApiConfig.is_mock_mode"


@pytest.fixture
def mock_client() -> TestClient:
    """Minimal FastAPI app with promote router in mock mode."""
    with patch(_PATCH_MOCK, return_value=True):
        app = FastAPI()
        app.include_router(router)
        return TestClient(app)


_PAPER_BODY = {
    "target_phase": "paper_1d",
    "promoter": "test_operator",
    "reason": "passing backtest thresholds",
}

_LIVE_BODY = {
    "target_phase": "live_early",
    "promoter": "test_operator",
    "reason": "paper trading profitable for 7 days",
}


class TestPromoteToPaper:
    def test_paper_promote_returns_200(self, mock_client: TestClient) -> None:
        resp = mock_client.post(
            "/promote/carry_staked_basis/manifest-abc-123",
            json=_PAPER_BODY,
        )
        assert resp.status_code == 200

    def test_paper_promote_event_emitted(self, mock_client: TestClient) -> None:
        resp = mock_client.post(
            "/promote/carry_staked_basis/manifest-abc-123",
            json=_PAPER_BODY,
        )
        body = resp.json()
        assert body["event_emitted"] == "STRATEGY_PROMOTED_TO_PAPER"

    def test_paper_promote_target_phase_in_response(self, mock_client: TestClient) -> None:
        resp = mock_client.post(
            "/promote/carry_staked_basis/manifest-abc-123",
            json=_PAPER_BODY,
        )
        body = resp.json()
        assert body["target_phase"] == "paper_1d"
        assert body["strategy_id"] == "carry_staked_basis"


class TestPromoteToLive:
    def test_live_promote_returns_200(self, mock_client: TestClient) -> None:
        resp = mock_client.post(
            "/promote/carry_staked_basis/manifest-xyz-456",
            json=_LIVE_BODY,
        )
        assert resp.status_code == 200

    def test_live_promote_event_emitted(self, mock_client: TestClient) -> None:
        resp = mock_client.post(
            "/promote/carry_staked_basis/manifest-xyz-456",
            json=_LIVE_BODY,
        )
        body = resp.json()
        assert body["event_emitted"] == "STRATEGY_PROMOTED_TO_LIVE"

    def test_live_promote_target_phase_in_response(self, mock_client: TestClient) -> None:
        resp = mock_client.post(
            "/promote/carry_staked_basis/manifest-xyz-456",
            json=_LIVE_BODY,
        )
        body = resp.json()
        assert body["target_phase"] == "live_early"
        assert body["strategy_id"] == "carry_staked_basis"


class TestPromoteValidation:
    def test_invalid_target_phase_returns_422(self, mock_client: TestClient) -> None:
        resp = mock_client.post(
            "/promote/carry_staked_basis/manifest-abc-123",
            json={"target_phase": "smoke", "promoter": "op", "reason": "test"},
        )
        assert resp.status_code == 422

    def test_unknown_target_phase_returns_422(self, mock_client: TestClient) -> None:
        resp = mock_client.post(
            "/promote/carry_staked_basis/manifest-abc-123",
            json={"target_phase": "not_a_phase", "promoter": "op", "reason": "test"},
        )
        assert resp.status_code == 422

    def test_missing_promoter_returns_422(self, mock_client: TestClient) -> None:
        resp = mock_client.post(
            "/promote/carry_staked_basis/manifest-abc-123",
            json={"target_phase": "paper_1d", "reason": "test"},
        )
        assert resp.status_code == 422

    def test_response_schema_has_all_fields(self, mock_client: TestClient) -> None:
        resp = mock_client.post(
            "/promote/carry_staked_basis/manifest-abc-123",
            json=_PAPER_BODY,
        )
        body = resp.json()
        assert "manifest_id" in body
        assert "strategy_id" in body
        assert "strategy_instance_id" in body
        assert "target_phase" in body
        assert "promoter" in body
        assert "promoted_at" in body
        assert "event_emitted" in body

    def test_manifest_id_passthrough(self, mock_client: TestClient) -> None:
        resp = mock_client.post(
            "/promote/carry_staked_basis/manifest-unique-999",
            json=_PAPER_BODY,
        )
        body = resp.json()
        assert body["manifest_id"] == "manifest-unique-999"

    def test_promoter_passthrough(self, mock_client: TestClient) -> None:
        resp = mock_client.post(
            "/promote/carry_staked_basis/manifest-abc-123",
            json=_PAPER_BODY,
        )
        body = resp.json()
        assert body["promoter"] == "test_operator"

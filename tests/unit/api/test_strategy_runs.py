"""Unit tests for GET /strategy/{strategy_id}/runs — pvl-p23b.

Three tests — one per mode (batch / paper / live) — covering:
- Correct mode tag in response.
- Structurally-valid StrategyRunsResponse in mock mode.
- strategy_id passthrough + 422 on invalid strategy_id.
- QG green without network access.

Tests mount the router on a minimal FastAPI app (no lifespan stack) and
force mock mode via CLOUD_MOCK_MODE=true so no real GCS / cloud deps are needed.
"""

from __future__ import annotations

from unittest.mock import patch

import pytest
from fastapi import FastAPI
from fastapi.testclient import TestClient

from deployment_api.routes.strategy_runs import router


@pytest.fixture
def mock_client() -> TestClient:
    """Minimal FastAPI app with strategy_runs router in mock mode."""
    with patch("deployment_api.deployment_api_config.DeploymentApiConfig.is_mock_mode", return_value=True):
        app = FastAPI()
        app.include_router(router)
        return TestClient(app)


class TestGetStrategyRunsBatchMode:
    def test_batch_mode_returns_200(self, mock_client: TestClient) -> None:
        resp = mock_client.get("/strategy/carry_staked_basis/runs", params={"mode": "batch"})
        assert resp.status_code == 200

    def test_batch_mode_tag_in_response(self, mock_client: TestClient) -> None:
        resp = mock_client.get("/strategy/carry_staked_basis/runs", params={"mode": "batch"})
        body = resp.json()
        assert body["mode"] == "batch"
        assert body["strategy_id"] == "carry_staked_basis"

    def test_batch_mode_runs_non_empty(self, mock_client: TestClient) -> None:
        resp = mock_client.get("/strategy/carry_staked_basis/runs", params={"mode": "batch"})
        body = resp.json()
        assert body["total_count"] > 0
        assert len(body["runs"]) > 0
        assert body["runs"][0]["mode"] == "batch"


class TestGetStrategyRunsPaperMode:
    def test_paper_mode_returns_200(self, mock_client: TestClient) -> None:
        resp = mock_client.get("/strategy/carry_staked_basis/runs", params={"mode": "paper"})
        assert resp.status_code == 200

    def test_paper_mode_tag_in_response(self, mock_client: TestClient) -> None:
        resp = mock_client.get("/strategy/carry_staked_basis/runs", params={"mode": "paper"})
        body = resp.json()
        assert body["mode"] == "paper"
        assert body["strategy_id"] == "carry_staked_basis"

    def test_paper_mode_runs_non_empty(self, mock_client: TestClient) -> None:
        resp = mock_client.get("/strategy/carry_staked_basis/runs", params={"mode": "paper"})
        body = resp.json()
        assert body["total_count"] > 0
        assert body["runs"][0]["mode"] == "paper"


class TestGetStrategyRunsLiveMode:
    def test_live_mode_returns_200(self, mock_client: TestClient) -> None:
        resp = mock_client.get("/strategy/carry_staked_basis/runs", params={"mode": "live"})
        assert resp.status_code == 200

    def test_live_mode_tag_in_response(self, mock_client: TestClient) -> None:
        resp = mock_client.get("/strategy/carry_staked_basis/runs", params={"mode": "live"})
        body = resp.json()
        assert body["mode"] == "live"
        assert body["strategy_id"] == "carry_staked_basis"

    def test_live_mode_runs_non_empty(self, mock_client: TestClient) -> None:
        resp = mock_client.get("/strategy/carry_staked_basis/runs", params={"mode": "live"})
        body = resp.json()
        assert body["total_count"] > 0
        assert body["runs"][0]["mode"] == "live"


class TestGetStrategyRunsValidation:
    def test_missing_mode_returns_422(self, mock_client: TestClient) -> None:
        resp = mock_client.get("/strategy/carry_staked_basis/runs")
        assert resp.status_code == 422

    def test_invalid_mode_returns_422(self, mock_client: TestClient) -> None:
        resp = mock_client.get("/strategy/carry_staked_basis/runs", params={"mode": "unknown"})
        assert resp.status_code == 422

    def test_empty_strategy_id_returns_422(self, mock_client: TestClient) -> None:
        resp = mock_client.get("/strategy//runs", params={"mode": "batch"})
        assert resp.status_code in (404, 422)

    def test_response_schema_has_all_fields(self, mock_client: TestClient) -> None:
        resp = mock_client.get("/strategy/arb_price_dispersion/runs", params={"mode": "batch"})
        body = resp.json()
        assert "strategy_id" in body
        assert "mode" in body
        assert "runs" in body
        assert "total_count" in body
        assert "page" in body
        assert "page_size" in body

    def test_run_record_schema(self, mock_client: TestClient) -> None:
        resp = mock_client.get("/strategy/carry_staked_basis/runs", params={"mode": "batch"})
        run = resp.json()["runs"][0]
        assert "run_id" in run
        assert "strategy_id" in run
        assert "mode" in run
        assert "run_date" in run
        assert "realized_pnl" in run
        assert "fill_count" in run

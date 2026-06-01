"""Unit tests for vm_cost_estimate.py — POST /api/vm/cost-estimate."""

from __future__ import annotations

from unittest.mock import patch

import pytest
from fastapi import FastAPI
from fastapi.testclient import TestClient
from unified_trading_library import setup_events

from deployment_api.routes.vm_cost_estimate import router

setup_events("deployment-api", "test")

_PATCH_MOCK = "deployment_api.deployment_api_config.DeploymentApiConfig.is_mock_mode"


@pytest.fixture
def client() -> TestClient:
    with patch(_PATCH_MOCK, return_value=True):
        app = FastAPI()
        app.include_router(router)
        return TestClient(app)


class TestVmCostEstimate:
    def test_returns_200_for_known_machine(self, client: TestClient) -> None:
        r = client.post("/api/vm/cost-estimate", json={"machine_type": "n1-standard-4", "runtime_hours": 2.0})
        assert r.status_code == 200

    def test_response_shape(self, client: TestClient) -> None:
        r = client.post("/api/vm/cost-estimate", json={"machine_type": "n1-standard-4", "runtime_hours": 1.0})
        body = r.json()
        for field in (
            "machine_type",
            "runtime_hours",
            "disk_gb",
            "count",
            "compute_cost_usd",
            "disk_cost_usd",
            "total_cost_usd",
            "hourly_rate_usd",
            "currency",
            "region",
            "dry_run",
            "estimated_at",
            "unknown_machine_type",
        ):
            assert field in body, f"Missing field: {field}"

    def test_compute_cost_matches_rate_times_hours(self, client: TestClient) -> None:
        r = client.post("/api/vm/cost-estimate", json={"machine_type": "n1-standard-4", "runtime_hours": 10.0})
        body = r.json()
        expected_compute = round(body["hourly_rate_usd"] * 10.0, 4)
        assert abs(body["compute_cost_usd"] - expected_compute) < 0.001

    def test_count_multiplies_cost(self, client: TestClient) -> None:
        r1 = client.post(
            "/api/vm/cost-estimate",
            json={"machine_type": "n1-standard-2", "runtime_hours": 5.0, "count": 1},
        )
        r3 = client.post(
            "/api/vm/cost-estimate",
            json={"machine_type": "n1-standard-2", "runtime_hours": 5.0, "count": 3},
        )
        single = r1.json()["total_cost_usd"]
        triple = r3.json()["total_cost_usd"]
        assert abs(triple - single * 3) < 0.01

    def test_unknown_machine_type_flagged(self, client: TestClient) -> None:
        r = client.post("/api/vm/cost-estimate", json={"machine_type": "a100-ultra-gpu", "runtime_hours": 1.0})
        assert r.status_code == 200
        body = r.json()
        assert body["unknown_machine_type"] is True
        assert body["total_cost_usd"] > 0

    def test_dry_run_true_in_mock_mode(self, client: TestClient) -> None:
        r = client.post("/api/vm/cost-estimate", json={"machine_type": "n1-standard-8", "runtime_hours": 3.0})
        assert r.json()["dry_run"] is True

    def test_currency_and_region(self, client: TestClient) -> None:
        r = client.post("/api/vm/cost-estimate", json={"machine_type": "n1-standard-1", "runtime_hours": 1.0})
        body = r.json()
        assert body["currency"] == "USD"
        assert body["region"] == "asia-northeast1"

    def test_invalid_runtime_hours_rejected(self, client: TestClient) -> None:
        r = client.post("/api/vm/cost-estimate", json={"machine_type": "n1-standard-4", "runtime_hours": 0})
        assert r.status_code == 422

    def test_total_equals_compute_plus_disk(self, client: TestClient) -> None:
        r = client.post(
            "/api/vm/cost-estimate",
            json={"machine_type": "n1-standard-4", "runtime_hours": 4.0, "disk_gb": 100},
        )
        body = r.json()
        assert abs(body["total_cost_usd"] - (body["compute_cost_usd"] + body["disk_cost_usd"])) < 0.001

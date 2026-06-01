"""Unit tests for deployment_diff.py — GET /api/deployments/diff."""

from __future__ import annotations

from unittest.mock import patch

import pytest
from fastapi import FastAPI
from fastapi.testclient import TestClient
from unified_trading_library import setup_events

from deployment_api.routes.deployment_diff import router

setup_events("deployment-api", "test")

_PATCH_MOCK = "deployment_api.deployment_api_config.DeploymentApiConfig.is_mock_mode"


@pytest.fixture
def client() -> TestClient:
    with patch(_PATCH_MOCK, return_value=True):
        app = FastAPI()
        app.include_router(router)
        return TestClient(app)


class TestDiffMockMode:
    def test_diff_returns_200(self, client: TestClient) -> None:
        r = client.get("/api/deployments/diff?from_sha=abc123&to_sha=def456")
        assert r.status_code == 200

    def test_diff_response_shape(self, client: TestClient) -> None:
        body = client.get("/api/deployments/diff?from_sha=abc123&to_sha=def456").json()
        assert body["from_sha"] == "abc123"
        assert body["to_sha"] == "def456"
        assert isinstance(body["added"], list)
        assert isinstance(body["removed"], list)
        assert isinstance(body["changed"], list)
        assert isinstance(body["total_changes"], int)

    def test_diff_total_changes_consistent(self, client: TestClient) -> None:
        body = client.get("/api/deployments/diff?from_sha=abc&to_sha=xyz").json()
        expected = len(body["added"]) + len(body["removed"]) + len(body["changed"])
        assert body["total_changes"] == expected

    def test_diff_identical_shas_returns_empty(self, client: TestClient) -> None:
        body = client.get("/api/deployments/diff?from_sha=abc&to_sha=abc").json()
        assert body["total_changes"] == 0
        assert body["added"] == []
        assert body["removed"] == []
        assert body["changed"] == []

    def test_diff_non_identical_shas_has_changes(self, client: TestClient) -> None:
        body = client.get("/api/deployments/diff?from_sha=abc&to_sha=xyz").json()
        assert body["total_changes"] > 0

    def test_diff_entry_has_expected_fields(self, client: TestClient) -> None:
        body = client.get("/api/deployments/diff?from_sha=abc&to_sha=xyz").json()
        all_entries = body["added"] + body["removed"] + body["changed"]
        assert len(all_entries) > 0
        for entry in all_entries:
            assert "service" in entry
            assert "from_version" in entry
            assert "to_version" in entry

    def test_diff_dry_run_flag_set(self, client: TestClient) -> None:
        body = client.get("/api/deployments/diff?from_sha=a&to_sha=b").json()
        assert body["dry_run"] is True

"""Unit tests for GET /api/vm/{vm_name}/events — filtered path-based endpoint."""

from __future__ import annotations

from collections.abc import Generator
from unittest.mock import MagicMock, patch

import pytest
from fastapi import FastAPI
from fastapi.testclient import TestClient
from unified_trading_library import setup_events

from deployment_api.routes.vm_events import router

setup_events("deployment-api", "test")

_PATCH_MOCK = "deployment_api.deployment_api_config.DeploymentApiConfig.is_mock_mode"


@pytest.fixture
def mock_client() -> Generator[TestClient]:
    with patch(_PATCH_MOCK, return_value=True):
        app = FastAPI()
        app.include_router(router, prefix="/vm")
        yield TestClient(app)


@pytest.fixture
def prod_client() -> Generator[TestClient]:
    with patch(_PATCH_MOCK, return_value=False):
        app = FastAPI()
        app.include_router(router, prefix="/vm")
        yield TestClient(app)


class TestFilteredEventsMockMode:
    def test_returns_200(self, mock_client: TestClient) -> None:
        r = mock_client.get("/vm/cefi-backfill-20260515/events")
        assert r.status_code == 200

    def test_has_required_fields(self, mock_client: TestClient) -> None:
        r = mock_client.get("/vm/cefi-backfill-20260515/events")
        body = r.json()
        assert "events" in body
        assert "total_events" in body
        assert "vm_name" in body
        assert "date" in body

    def test_type_filter_started_only(self, mock_client: TestClient) -> None:
        r = mock_client.get("/vm/cefi-backfill-20260515/events", params={"type": "STARTED"})
        assert r.status_code == 200
        body = r.json()
        for evt in body["events"]:
            assert evt["event"] == "STARTED"

    def test_type_filter_unknown_returns_empty(self, mock_client: TestClient) -> None:
        r = mock_client.get("/vm/cefi-backfill-20260515/events", params={"type": "NONEXISTENT"})
        assert r.status_code == 200
        assert r.json()["total_events"] == 0

    def test_limit_respected(self, mock_client: TestClient) -> None:
        r = mock_client.get("/vm/cefi-backfill-20260515/events", params={"limit": 1})
        assert r.status_code == 200
        assert len(r.json()["events"]) <= 1

    def test_unknown_prefix_returns_400(self, mock_client: TestClient) -> None:
        r = mock_client.get("/vm/unknown-prefix-vm/events")
        assert r.status_code == 400


class TestFilteredEventsProdMode:
    def test_returns_empty_when_no_blobs(self, prod_client: TestClient) -> None:
        mock_storage = MagicMock()
        mock_storage.list_blobs.return_value = []
        with patch("deployment_api.routes.vm_events.get_storage_client", return_value=mock_storage):
            r = prod_client.get("/vm/cefi-backfill-20260515/events")
        assert r.status_code == 200
        assert r.json()["total_events"] == 0

    def test_bad_since_returns_400(self, prod_client: TestClient) -> None:
        r = prod_client.get("/vm/cefi-backfill-20260515/events", params={"since": "not-a-date"})
        assert r.status_code == 400


class TestFilterCombinations:
    """Pagination + multi-param filter combos in mock mode."""

    def test_type_and_limit_combined_respects_both(self, mock_client: TestClient) -> None:
        r = mock_client.get(
            "/vm/cefi-backfill-20260515/events",
            params={"type": "STARTED", "limit": 1},
        )
        assert r.status_code == 200
        body = r.json()
        assert len(body["events"]) <= 1
        for evt in body["events"]:
            assert evt["event"] == "STARTED"

    def test_limit_1_returns_at_most_one_event(self, mock_client: TestClient) -> None:
        r = mock_client.get("/vm/cefi-backfill-20260515/events", params={"limit": 1})
        assert r.status_code == 200
        assert len(r.json()["events"]) <= 1

    def test_large_limit_returns_all_available(self, mock_client: TestClient) -> None:
        r_default = mock_client.get("/vm/cefi-backfill-20260515/events")
        r_large = mock_client.get("/vm/cefi-backfill-20260515/events", params={"limit": 5000})
        assert r_default.status_code == 200
        assert r_large.status_code == 200
        assert r_large.json()["total_events"] == r_default.json()["total_events"]

    def test_since_valid_iso_returns_200(self, mock_client: TestClient) -> None:
        r = mock_client.get(
            "/vm/cefi-backfill-20260515/events",
            params={"since": "2026-05-15T00:00:00Z"},
        )
        assert r.status_code == 200
        assert "events" in r.json()

    def test_since_and_type_combined_returns_200(self, mock_client: TestClient) -> None:
        r = mock_client.get(
            "/vm/cefi-backfill-20260515/events",
            params={"since": "2026-05-15T00:00:00Z", "type": "STARTED"},
        )
        assert r.status_code == 200
        body = r.json()
        for evt in body["events"]:
            assert evt["event"] == "STARTED"

    def test_truncated_flag_set_when_limit_cuts_results(self, mock_client: TestClient) -> None:
        r_full = mock_client.get("/vm/cefi-backfill-20260515/events")
        total = r_full.json()["total_events"]
        if total > 1:
            r = mock_client.get("/vm/cefi-backfill-20260515/events", params={"limit": 1})
            assert r.json()["truncated"] is True

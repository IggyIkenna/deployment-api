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

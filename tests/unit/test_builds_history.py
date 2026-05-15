"""Unit tests for GET /api/builds/history."""

from __future__ import annotations

from unittest.mock import patch

import pytest
from fastapi.testclient import TestClient

_PATCH_LOG = "deployment_api.routes.builds_history.log_event"

_HEADERS = {"X-API-Key": "test-key"}


@pytest.fixture(scope="module")
def client() -> TestClient:
    from deployment_api.main import app

    return TestClient(app, raise_server_exceptions=False)


def test_list_all_services_mock_mode(client: TestClient) -> None:
    with patch(_PATCH_LOG):
        resp = client.get("/api/builds/history", headers=_HEADERS)
    assert resp.status_code == 200
    data = resp.json()
    assert data["dry_run"] is True
    assert isinstance(data["entries"], list)
    assert data["total"] == len(data["entries"])
    assert len(data["entries"]) <= 10  # default limit


def test_entries_have_required_fields(client: TestClient) -> None:
    with patch(_PATCH_LOG):
        resp = client.get("/api/builds/history", headers=_HEADERS)
    data = resp.json()
    for entry in data["entries"]:
        assert "service" in entry
        assert "image_tags" in entry
        assert isinstance(entry["image_tags"], list)


def test_tarball_info_structure(client: TestClient) -> None:
    with patch(_PATCH_LOG):
        resp = client.get("/api/builds/history", headers=_HEADERS)
    data = resp.json()
    for entry in data["entries"]:
        if entry.get("tarball") is not None:
            tb = entry["tarball"]
            assert "bucket" in tb
            assert "object_path" in tb
            assert tb["bucket"].startswith("deployment-scripts-")
            assert tb["object_path"].startswith("code/")
            assert tb["object_path"].endswith("-code.tar.gz")


def test_service_filter(client: TestClient) -> None:
    with patch(_PATCH_LOG):
        resp = client.get(
            "/api/builds/history",
            params={"service": "market-tick-data-service"},
            headers=_HEADERS,
        )
    assert resp.status_code == 200
    data = resp.json()
    assert data["total"] == 1
    assert data["entries"][0]["service"] == "market-tick-data-service"


def test_limit_param(client: TestClient) -> None:
    with patch(_PATCH_LOG):
        resp = client.get(
            "/api/builds/history",
            params={"limit": 3},
            headers=_HEADERS,
        )
    assert resp.status_code == 200
    data = resp.json()
    assert len(data["entries"]) <= 3


def test_limit_too_large_returns_422(client: TestClient) -> None:
    resp = client.get(
        "/api/builds/history",
        params={"limit": 999},
        headers=_HEADERS,
    )
    assert resp.status_code == 422


def test_project_id_in_response(client: TestClient) -> None:
    with patch(_PATCH_LOG):
        resp = client.get("/api/builds/history", headers=_HEADERS)
    data = resp.json()
    assert "project_id" in data
    assert isinstance(data["project_id"], str)

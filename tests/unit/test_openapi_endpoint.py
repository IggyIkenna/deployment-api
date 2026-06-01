"""Smoke tests for GET /api/openapi.json — verifies schema is present and valid."""

from __future__ import annotations

import pytest
from fastapi.testclient import TestClient


@pytest.fixture(scope="module")
def client() -> TestClient:
    from deployment_api.main import app

    return TestClient(app, raise_server_exceptions=False)


def test_openapi_returns_200(client: TestClient) -> None:
    resp = client.get("/api/openapi.json")
    assert resp.status_code == 200


def test_openapi_content_type_is_json(client: TestClient) -> None:
    resp = client.get("/api/openapi.json")
    assert "application/json" in resp.headers.get("content-type", "")


def test_openapi_has_required_top_level_keys(client: TestClient) -> None:
    resp = client.get("/api/openapi.json")
    schema = resp.json()
    for key in ("openapi", "info", "paths"):
        assert key in schema, f"Missing top-level key: {key}"


def test_openapi_version_is_3x(client: TestClient) -> None:
    resp = client.get("/api/openapi.json")
    version: str = resp.json().get("openapi", "")
    assert version.startswith("3."), f"Expected OpenAPI 3.x, got: {version}"


def test_openapi_info_has_title_and_version(client: TestClient) -> None:
    resp = client.get("/api/openapi.json")
    info: dict[str, object] = resp.json().get("info", {})
    assert info.get("title")
    assert info.get("version")


def test_openapi_paths_not_empty(client: TestClient) -> None:
    resp = client.get("/api/openapi.json")
    paths: dict[str, object] = resp.json().get("paths", {})
    assert len(paths) > 0, "OpenAPI spec has no paths defined"

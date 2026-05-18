"""Unit tests for GET /api/builds/history."""

from __future__ import annotations

import json
from pathlib import Path
from unittest.mock import AsyncMock, MagicMock, patch

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


# ── _mock_entries direct tests ────────────────────────────────────────────────


def test_mock_entries_invalid_json_manifest(tmp_path: Path) -> None:
    """Lines 109-117: except (json.JSONDecodeError, OSError) path in _mock_entries."""
    from deployment_api.routes.builds_history import _mock_entries

    manifest_dir = tmp_path / "unified-trading-pm"
    manifest_dir.mkdir(parents=True)
    (manifest_dir / "workspace-manifest.json").write_text("NOT VALID JSON }{")

    with patch("deployment_api.routes.builds_history._workspace_root", return_value=tmp_path):
        entries = _mock_entries("test-project", ["my-svc"], 10)

    assert len(entries) == 1
    assert entries[0].service == "my-svc"
    # No deployed version available due to parse failure
    assert entries[0].latest_image_tag is None


def test_mock_entries_with_deployed_versions(tmp_path: Path) -> None:
    """_mock_entries reads deployed_versions from manifest and populates image_tags."""
    from deployment_api.routes.builds_history import _mock_entries

    manifest = {
        "deployed_versions": {
            "production": {
                "strategy-service": "v1.2.3",
            }
        }
    }
    manifest_dir = tmp_path / "unified-trading-pm"
    manifest_dir.mkdir(parents=True)
    (manifest_dir / "workspace-manifest.json").write_text(json.dumps(manifest))

    with patch("deployment_api.routes.builds_history._workspace_root", return_value=tmp_path):
        entries = _mock_entries("my-project", ["strategy-service"], 10)

    assert len(entries) == 1
    assert entries[0].latest_image_tag == "v1.2.3"
    assert entries[0].image_tags == ["v1.2.3"]


def test_mock_entries_missing_manifest(tmp_path: Path) -> None:
    """_mock_entries when workspace manifest file doesn't exist returns entries with no tags."""
    from deployment_api.routes.builds_history import _mock_entries

    with patch("deployment_api.routes.builds_history._workspace_root", return_value=tmp_path):
        entries = _mock_entries("test-project", ["execution-service", "risk-and-exposure-service"], 5)

    assert len(entries) == 2
    assert all(e.latest_image_tag is None for e in entries)


def test_mock_entries_limit_applied(tmp_path: Path) -> None:
    """_mock_entries respects the limit parameter."""
    from deployment_api.routes.builds_history import _mock_entries

    with patch("deployment_api.routes.builds_history._workspace_root", return_value=tmp_path):
        entries = _mock_entries("p", ["svc-a", "svc-b", "svc-c", "svc-d"], 2)

    assert len(entries) == 2


# ── live mode route path ──────────────────────────────────────────────────────


def test_live_mode_calls_live_entries(client: TestClient) -> None:
    """Line 233: entries = await _live_entries(...) when dry_run=False."""
    from deployment_api.routes.builds_history import BuildLineageEntry

    mock_cfg = MagicMock()
    mock_cfg.is_mock_mode.return_value = False
    mock_cfg.gcp_project_id = "live-project"

    live_result = [
        BuildLineageEntry(
            service="execution-service",
            tarball=None,
            image_tags=["v2.0.0"],
            latest_image_tag="v2.0.0",
        )
    ]

    with (
        patch(_PATCH_LOG),
        patch("deployment_api.routes.builds_history._cfg", mock_cfg),
        patch("deployment_api.routes.builds_history._live_entries", new=AsyncMock(return_value=live_result)),
    ):
        resp = client.get("/api/builds/history", headers=_HEADERS)

    assert resp.status_code == 200
    data = resp.json()
    assert data["dry_run"] is False
    assert data["total"] == 1

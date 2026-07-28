"""Unit tests for routes/version_coherence.py — GET /api/version-coherence/overview.

Mirrors test_repo_ci_routes.py's mock-mode TestClient pattern; the real-Firestore path patches the
route module's imported get_all_verdicts_with_status directly (no network, no SDK).
"""

from __future__ import annotations

from unittest.mock import patch

import pytest
from fastapi import FastAPI
from fastapi.testclient import TestClient
from unified_trading_library import setup_events

setup_events("deployment-api", "test")

_PATCH_MOCK_MODE = "deployment_api.routes.version_coherence.DeploymentApiConfig.is_mock_mode"
_PATCH_READER = "deployment_api.routes.version_coherence.get_all_verdicts_with_status"


@pytest.fixture
def client() -> TestClient:
    from deployment_api.routes.version_coherence import router

    app = FastAPI()
    app.include_router(router)
    return TestClient(app, raise_server_exceptions=False)


class TestOverviewMock:
    def test_overview_shape(self, client: TestClient) -> None:
        with patch(_PATCH_MOCK_MODE, return_value=True):
            resp = client.get("/api/version-coherence/overview")
        assert resp.status_code == 200
        body = resp.json()
        assert body["source"] == "mock"
        assert {"generated_at", "source", "repos"} <= set(body)
        assert len(body["repos"]) >= 4
        one = next(iter(body["repos"].values()))
        assert {"verdict", "reasons", "checked_at"} <= set(one)

    def test_every_verdict_class_represented(self, client: TestClient) -> None:
        """The mock fixture is the playwright regression spec's data source — every chip tone must
        be exercised so a rendering regression on any one class is caught."""
        with patch(_PATCH_MOCK_MODE, return_value=True):
            body = client.get("/api/version-coherence/overview").json()
        verdicts = {entry["verdict"] for entry in body["repos"].values()}
        assert verdicts == {"OK", "VERSION_SPLIT", "VESTIGIAL_SCALAR_DRIFT", "DEP_FLOOR_UNSATISFIABLE"}


class TestOverviewFirestore:
    def test_reads_from_verdict_store(self, client: TestClient) -> None:
        fake_docs = {
            "unified-trading-library": {"verdict": "OK", "reasons": [], "checked_at": "2026-07-27T10:00:00Z"},
            "instruments-service": {
                "verdict": "VERSION_SPLIT",
                "reasons": ["instruments-service: version split"],
                "checked_at": "2026-07-27T10:00:00Z",
            },
        }
        with (
            patch(_PATCH_MOCK_MODE, return_value=False),
            patch(_PATCH_READER, return_value=(fake_docs, True)),
        ):
            resp = client.get("/api/version-coherence/overview")
        assert resp.status_code == 200
        body = resp.json()
        assert body["source"] == "firestore"
        assert body["repos"]["unified-trading-library"]["verdict"] == "OK"
        assert body["repos"]["instruments-service"]["verdict"] == "VERSION_SPLIT"
        assert body["repos"]["instruments-service"]["reasons"] == ["instruments-service: version split"]

    def test_unavailable_reports_honest_source_not_fabricated_ok(self, client: TestClient) -> None:
        """When Firestore is unreachable the panel must show 'unavailable', never a fabricated
        per-repo OK — the honest-absence contract every verdict-store reader follows."""
        with (
            patch(_PATCH_MOCK_MODE, return_value=False),
            patch(_PATCH_READER, return_value=({}, False)),
        ):
            resp = client.get("/api/version-coherence/overview")
        body = resp.json()
        assert body["source"] == "unavailable"
        assert body["repos"] == {}

    def test_empty_but_reachable_is_not_mislabeled_unavailable(self, client: TestClient) -> None:
        """Before the first scheduled writer run ever completes the collection is genuinely empty
        but Firestore itself is reachable — that must read 'firestore' (with zero repos), not
        'unavailable' (which would misleadingly suggest an outage)."""
        with (
            patch(_PATCH_MOCK_MODE, return_value=False),
            patch(_PATCH_READER, return_value=({}, True)),
        ):
            resp = client.get("/api/version-coherence/overview")
        body = resp.json()
        assert body["source"] == "firestore"
        assert body["repos"] == {}

    def test_doc_missing_verdict_field_reads_unknown(self, client: TestClient) -> None:
        fake_docs = {"some-repo": {"reasons": []}}  # no "verdict" key
        with (
            patch(_PATCH_MOCK_MODE, return_value=False),
            patch(_PATCH_READER, return_value=(fake_docs, True)),
        ):
            resp = client.get("/api/version-coherence/overview")
        body = resp.json()
        assert body["repos"]["some-repo"]["verdict"] == "UNKNOWN"

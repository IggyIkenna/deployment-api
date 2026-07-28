"""Unit tests for routes/change_freeze.py — GET /api/change-freeze/status.

Mirrors test_version_coherence_routes.py's structure/conventions for the sibling verdict-store
panel (change_freeze_verdicts, keyed by check_type instead of repo).
"""

from __future__ import annotations

from unittest.mock import patch

import pytest
from fastapi import FastAPI
from fastapi.testclient import TestClient
from unified_trading_library import setup_events

setup_events("deployment-api", "test")

_PATCH_MOCK_MODE = "deployment_api.routes.change_freeze.DeploymentApiConfig.is_mock_mode"
_PATCH_READER = "deployment_api.routes.change_freeze.get_all_verdicts_with_status"


@pytest.fixture
def client() -> TestClient:
    from deployment_api.routes.change_freeze import router

    app = FastAPI()
    app.include_router(router)
    return TestClient(app, raise_server_exceptions=False)


class TestStatusMock:
    def test_status_shape(self, client: TestClient) -> None:
        with patch(_PATCH_MOCK_MODE, return_value=True):
            resp = client.get("/api/change-freeze/status")
        assert resp.status_code == 200
        body = resp.json()
        assert body["source"] == "mock"
        assert {"generated_at", "source", "checks"} <= set(body)
        assert set(body["checks"]) == {"PROD_DEPLOY", "AUTONOMOUS"}
        for entry in body["checks"].values():
            assert {"verdict", "reason", "checked_at"} <= set(entry)

    def test_both_blocked_and_clear_represented(self, client: TestClient) -> None:
        """The mock fixture is the playwright regression spec's data source — both banner states
        (active freeze vs clear) must be exercised."""
        with patch(_PATCH_MOCK_MODE, return_value=True):
            body = client.get("/api/change-freeze/status").json()
        verdicts = {entry["verdict"] for entry in body["checks"].values()}
        assert verdicts == {"BLOCKED", "CLEAR"}
        blocked = body["checks"]["PROD_DEPLOY"]
        assert blocked["verdict"] == "BLOCKED"
        assert blocked["reason"]  # a real freeze-window description, not empty


class TestStatusFirestore:
    def test_reads_from_verdict_store(self, client: TestClient) -> None:
        fake_docs = {
            "PROD_DEPLOY": {"verdict": "CLEAR", "reasons": [], "checked_at": "2026-07-27T13:00:00Z"},
            "AUTONOMOUS": {
                "verdict": "BLOCKED",
                "reasons": ["freeze-us-open (w1): 13:25 to 13:55 UTC"],
                "checked_at": "2026-07-27T13:00:00Z",
            },
        }
        with (
            patch(_PATCH_MOCK_MODE, return_value=False),
            patch(_PATCH_READER, return_value=(fake_docs, True)),
        ):
            resp = client.get("/api/change-freeze/status")
        assert resp.status_code == 200
        body = resp.json()
        assert body["source"] == "firestore"
        assert body["checks"]["PROD_DEPLOY"]["verdict"] == "CLEAR"
        assert body["checks"]["PROD_DEPLOY"]["reason"] is None
        assert body["checks"]["AUTONOMOUS"]["verdict"] == "BLOCKED"
        assert body["checks"]["AUTONOMOUS"]["reason"] == "freeze-us-open (w1): 13:25 to 13:55 UTC"

    def test_missing_check_type_doc_reads_unknown_not_dropped(self, client: TestClient) -> None:
        """Both check_types are ALWAYS reported even when Firestore only has a doc for one — the
        UI must never have to guess which keys might exist."""
        fake_docs = {"PROD_DEPLOY": {"verdict": "CLEAR", "reasons": [], "checked_at": "2026-07-27T13:00:00Z"}}
        with (
            patch(_PATCH_MOCK_MODE, return_value=False),
            patch(_PATCH_READER, return_value=(fake_docs, True)),
        ):
            resp = client.get("/api/change-freeze/status")
        body = resp.json()
        assert set(body["checks"]) == {"PROD_DEPLOY", "AUTONOMOUS"}
        assert body["checks"]["AUTONOMOUS"]["verdict"] == "UNKNOWN"

    def test_unavailable_reports_honest_source(self, client: TestClient) -> None:
        with (
            patch(_PATCH_MOCK_MODE, return_value=False),
            patch(_PATCH_READER, return_value=({}, False)),
        ):
            resp = client.get("/api/change-freeze/status")
        body = resp.json()
        assert body["source"] == "unavailable"
        assert body["checks"]["PROD_DEPLOY"]["verdict"] == "UNKNOWN"
        assert body["checks"]["AUTONOMOUS"]["verdict"] == "UNKNOWN"

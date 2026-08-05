"""Unit tests for the watchdog kill-events read route + its query builder."""

from __future__ import annotations

from typing import cast
from unittest.mock import PropertyMock, patch

import pytest
from fastapi import FastAPI
from fastapi.testclient import TestClient

from deployment_api.routes.watchdog_events import router
from deployment_api.services.operational_data_queries import (
    InvalidIdentifierError,
    watchdog_kill_events_sql,
)

_CFG = "deployment_api.deployment_api_config.DeploymentApiConfig"
_PATCH_MOCK = f"{_CFG}.is_mock_mode"
_PATCH_PROJECT_ID = f"{_CFG}.effective_project_id"
_PATCH_REQUIRE_PROJECT_ID = f"{_CFG}.require_gcp_project_id"
_PATCH_RUN_QUERY = "deployment_api.routes.watchdog_events.run_query"


@pytest.fixture
def mock_client():
    with patch(_PATCH_MOCK, return_value=True):
        app = FastAPI()
        app.include_router(router)
        yield TestClient(app)


@pytest.fixture
def prod_client():
    with patch(_PATCH_MOCK, return_value=False):
        app = FastAPI()
        app.include_router(router)
        yield TestClient(app)


class TestWatchdogKillEventsSql:
    def test_valid_hours_and_vm_name(self):
        sql = watchdog_kill_events_sql("proj", hours=24, vm_name="planning")
        assert "vm_name = 'planning'" in sql
        assert "INTERVAL 24 HOUR" in sql
        assert "DATE(ts) >= DATE_SUB(CURRENT_DATE(), INTERVAL 3 DAY)" in sql  # 2 + 24//24

    def test_no_vm_name_omits_filter(self):
        sql = watchdog_kill_events_sql("proj", hours=1, vm_name=None)
        assert "vm_name =" not in sql
        assert "INTERVAL 1 HOUR" in sql
        assert "INTERVAL 2 DAY" in sql

    def test_seven_day_hours_uses_max_day_buffer(self):
        sql = watchdog_kill_events_sql("proj", hours=168, vm_name=None)
        assert "INTERVAL 9 DAY" in sql  # 2 + 168//24 — the partition-day buffer (matches WINDOWS["1wk"]=9)
        assert "INTERVAL 168 HOUR" in sql

    def test_rejects_non_int_hours(self):
        bad_hours = cast(int, "1; DROP TABLE x")  # runtime str, statically int — exercises the isinstance guard
        with pytest.raises(ValueError):
            watchdog_kill_events_sql("proj", hours=bad_hours, vm_name=None)

    def test_rejects_out_of_range_hours(self):
        with pytest.raises(ValueError):
            watchdog_kill_events_sql("proj", hours=0, vm_name=None)
        with pytest.raises(ValueError):
            watchdog_kill_events_sql("proj", hours=169, vm_name=None)

    def test_rejects_injection_shaped_vm_name(self):
        with pytest.raises(InvalidIdentifierError):
            watchdog_kill_events_sql("proj", hours=24, vm_name="bad; DROP TABLE x")


class TestWatchdogKillEventsEndpoint:
    def test_mock_mode_returns_empty(self, mock_client: TestClient):
        r = mock_client.get("/api/watchdog/kill-events")
        assert r.status_code == 200
        assert r.json() == {"hours": 24, "vm_name": None, "rows": []}

    def test_out_of_range_hours_rejected_by_fastapi(self, mock_client: TestClient):
        r = mock_client.get("/api/watchdog/kill-events", params={"hours": 0})
        assert r.status_code == 422

    def test_mock_mode_short_circuits_before_validation(self, mock_client: TestClient):
        """Mock mode never reaches the SQL builder — an injection-shaped vm_name is harmless."""
        r = mock_client.get("/api/watchdog/kill-events", params={"vm_name": "bad; DROP TABLE x"})
        assert r.status_code == 200

    def test_prod_mode_rejects_invalid_vm_name(self, prod_client: TestClient):
        with patch(_PATCH_PROJECT_ID, new_callable=PropertyMock, return_value="test-project"):
            r = prod_client.get("/api/watchdog/kill-events", params={"vm_name": "bad; DROP TABLE x"})
        assert r.status_code == 400

    def test_prod_mode_no_project_degrades_to_empty(self, prod_client: TestClient):
        with patch(_PATCH_PROJECT_ID, new_callable=PropertyMock, return_value=""):
            r = prod_client.get("/api/watchdog/kill-events")
        assert r.status_code == 200
        assert r.json()["rows"] == []

    def test_prod_mode_query_failure_degrades_to_empty(self, prod_client: TestClient):
        with (
            patch(_PATCH_PROJECT_ID, new_callable=PropertyMock, return_value="test-project"),
            patch(_PATCH_REQUIRE_PROJECT_ID, return_value="test-project"),
            patch(_PATCH_RUN_QUERY, side_effect=RuntimeError("bq unavailable")),
        ):
            r = prod_client.get("/api/watchdog/kill-events")
        assert r.status_code == 200
        assert r.json()["rows"] == []

    def test_prod_mode_maps_rows(self, prod_client: TestClient):
        fake_row = {
            "ts": "2026-08-05T10:00:00Z",
            "vm_name": "planning",
            "pid": 4242,
            "slot_id": "3",
            "command": "python worker.py",
            "reason": "memory breach",
            "rss_mb": 2048,
            "limit_mb": 1024,
            "pressure_level": "high",
            "killed": True,
        }
        with (
            patch(_PATCH_PROJECT_ID, new_callable=PropertyMock, return_value="test-project"),
            patch(_PATCH_REQUIRE_PROJECT_ID, return_value="test-project"),
            patch(_PATCH_RUN_QUERY, return_value=[fake_row]),
        ):
            r = prod_client.get("/api/watchdog/kill-events", params={"vm_name": "planning", "hours": "24"})
        assert r.status_code == 200
        body = r.json()
        assert body["vm_name"] == "planning"
        assert body["hours"] == 24
        row = body["rows"][0]
        assert row["vm_name"] == "planning"
        assert row["pid"] == 4242
        assert row["slot_id"] == "3"
        assert row["rss_mb"] == 2048
        assert row["limit_mb"] == 1024
        assert row["pressure_level"] == "high"
        assert row["killed"] is True

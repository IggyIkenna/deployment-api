"""Unit tests for vm_resource_history.py and its query builders."""

from __future__ import annotations

from unittest.mock import PropertyMock, patch

import pytest
from fastapi import FastAPI
from fastapi.testclient import TestClient

from deployment_api.routes.vm_resource_history import router
from deployment_api.services.operational_data_queries import (
    InvalidIdentifierError,
    process_category_breakdown_sql,
    resource_samples_rolling_sql,
)

_CFG = "deployment_api.deployment_api_config.DeploymentApiConfig"
_PATCH_MOCK = f"{_CFG}.is_mock_mode"
_PATCH_PROJECT_ID = f"{_CFG}.effective_project_id"
_PATCH_REQUIRE_PROJECT_ID = f"{_CFG}.require_gcp_project_id"
_PATCH_RUN_QUERY = "deployment_api.routes.vm_resource_history.run_query"


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


class TestRollingWindowSql:
    def test_valid_window_and_vm_name(self):
        sql = resource_samples_rolling_sql("proj", "1h", "vm-1")
        assert "vm_name = 'vm-1'" in sql
        assert "INTERVAL 1 HOUR" in sql
        assert "DATE(ts) >= DATE_SUB(CURRENT_DATE(), INTERVAL 2 DAY)" in sql

    def test_no_vm_name_omits_filter(self):
        sql = resource_samples_rolling_sql("proj", "1wk", None)
        assert "vm_name =" not in sql
        assert "INTERVAL 7 DAY" in sql

    def test_rejects_injection_shaped_vm_name(self):
        with pytest.raises(InvalidIdentifierError):
            resource_samples_rolling_sql("proj", "1h", "bad; DROP TABLE x")

    def test_rejects_vm_name_with_quote(self):
        with pytest.raises(InvalidIdentifierError):
            resource_samples_rolling_sql("proj", "1h", "vm' OR '1'='1")


class TestProcessCategorySql:
    def test_valid(self):
        sql = process_category_breakdown_sql("proj", "4h", "i-0c9b283b31d6b5ca7")
        assert "vm_name = 'i-0c9b283b31d6b5ca7'" in sql
        assert "INTERVAL 4 HOUR" in sql

    def test_rejects_injection_shaped_vm_name(self):
        with pytest.raises(InvalidIdentifierError):
            process_category_breakdown_sql("proj", "4h", "bad; DROP TABLE x")


class TestRollingWindowEndpoint:
    def test_mock_mode_returns_empty(self, mock_client: TestClient):
        r = mock_client.get("/api/vm-resources/rolling", params={"window": "1h"})
        assert r.status_code == 200
        assert r.json() == {"window": "1h", "rows": []}

    def test_invalid_window_rejected_by_fastapi(self, mock_client: TestClient):
        r = mock_client.get("/api/vm-resources/rolling", params={"window": "bogus"})
        assert r.status_code == 422

    def test_mock_mode_short_circuits_before_validation(self, mock_client: TestClient):
        """Mock mode never reaches the SQL builder — an injection-shaped vm_name is harmless."""
        r = mock_client.get("/api/vm-resources/rolling", params={"window": "1h", "vm_name": "bad; DROP TABLE x"})
        assert r.status_code == 200

    def test_prod_mode_rejects_invalid_vm_name(self, prod_client: TestClient):
        with patch(_PATCH_PROJECT_ID, new_callable=PropertyMock, return_value="test-project"):
            r = prod_client.get("/api/vm-resources/rolling", params={"window": "1h", "vm_name": "bad; DROP TABLE x"})
        assert r.status_code == 400

    def test_prod_mode_no_project_degrades_to_empty(self, prod_client: TestClient):
        with patch(_PATCH_PROJECT_ID, new_callable=PropertyMock, return_value=""):
            r = prod_client.get("/api/vm-resources/rolling", params={"window": "1h"})
        assert r.status_code == 200
        assert r.json()["rows"] == []

    def test_prod_mode_query_failure_degrades_to_empty(self, prod_client: TestClient):
        with (
            patch(_PATCH_PROJECT_ID, new_callable=PropertyMock, return_value="test-project"),
            patch(_PATCH_REQUIRE_PROJECT_ID, return_value="test-project"),
            patch(_PATCH_RUN_QUERY, side_effect=RuntimeError("bq unavailable")),
        ):
            r = prod_client.get("/api/vm-resources/rolling", params={"window": "1h"})
        assert r.status_code == 200
        assert r.json()["rows"] == []

    def test_prod_mode_maps_rows(self, prod_client: TestClient):
        fake_row = {
            "vm_name": "vm-1",
            "service": "svc",
            "avg_cpu_pct": 10.0,
            "min_cpu_pct": 5.0,
            "max_cpu_pct": 20.0,
            "p95_cpu_pct": 18.0,
            "avg_mem_pct": 30.0,
            "min_mem_pct": 25.0,
            "max_mem_pct": 40.0,
            "p95_mem_pct": 38.0,
            "avg_disk_pct": 50.0,
            "min_disk_pct": 45.0,
            "max_disk_pct": 55.0,
            "p95_disk_pct": 54.0,
            "sample_count": 42,
        }
        with (
            patch(_PATCH_PROJECT_ID, new_callable=PropertyMock, return_value="test-project"),
            patch(_PATCH_REQUIRE_PROJECT_ID, return_value="test-project"),
            patch(_PATCH_RUN_QUERY, return_value=[fake_row]),
        ):
            r = prod_client.get("/api/vm-resources/rolling", params={"window": "24h"})
        assert r.status_code == 200
        body = r.json()
        assert body["window"] == "24h"
        assert body["rows"][0]["vm_name"] == "vm-1"
        assert body["rows"][0]["sample_count"] == 42


class TestProcessCategoryEndpoint:
    def test_mock_mode_returns_empty(self, mock_client: TestClient):
        r = mock_client.get("/api/vm-resources/process-category", params={"vm_name": "i-abc"})
        assert r.status_code == 200
        assert r.json() == {"vm_name": "i-abc", "window": "1h", "rows": []}

    def test_requires_vm_name(self, mock_client: TestClient):
        r = mock_client.get("/api/vm-resources/process-category")
        assert r.status_code == 422

    def test_prod_mode_rejects_invalid_vm_name(self, prod_client: TestClient):
        with patch(_PATCH_PROJECT_ID, new_callable=PropertyMock, return_value="test-project"):
            r = prod_client.get(
                "/api/vm-resources/process-category", params={"vm_name": "bad; DROP TABLE x", "window": "1h"}
            )
        assert r.status_code == 400

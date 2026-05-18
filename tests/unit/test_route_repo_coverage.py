"""Unit tests for routes/repo_coverage.py — _evaluate_repo_coverage + mock-mode route."""

from __future__ import annotations

from unittest.mock import MagicMock, patch

import pytest
from fastapi import FastAPI
from fastapi.testclient import TestClient
from unified_trading_library import setup_events

setup_events("deployment-api", "test")

_PATCH_DISABLE_AUTH = "deployment_api.rbac.DISABLE_AUTH"
_PATCH_API_CFG = "deployment_api.deployment_api_config.DeploymentApiConfig"


# ── _evaluate_repo_coverage ───────────────────────────────────────────────────


class TestEvaluateRepoCoverage:
    def test_empty_rows_returns_all_false(self) -> None:
        from deployment_api.routes.repo_coverage import _evaluate_repo_coverage

        result = _evaluate_repo_coverage([])
        assert result["all_above_target"] is False
        assert result["snapshot_at"] is None
        assert result["worst_surface"] is None

    def test_all_above_target_returns_true(self) -> None:
        from deployment_api.routes.repo_coverage import _evaluate_repo_coverage

        rows = [
            {"snapshot_at": "2026-05-17", "surface": "unit", "actual_pct": 85.0, "target_pct": 70},
            {"snapshot_at": "2026-05-17", "surface": "integration", "actual_pct": 90.0, "target_pct": 80},
        ]
        result = _evaluate_repo_coverage(rows)
        assert result["all_above_target"] is True
        assert result["snapshot_at"] == "2026-05-17"
        assert result["worst_surface"] is None

    def test_one_below_target_returns_false_with_worst(self) -> None:
        from deployment_api.routes.repo_coverage import _evaluate_repo_coverage

        rows = [
            {"snapshot_at": "2026-05-17", "surface": "unit", "actual_pct": 85.0, "target_pct": 70},
            {"snapshot_at": "2026-05-17", "surface": "service_startup", "actual_pct": 36.1, "target_pct": 100},
        ]
        result = _evaluate_repo_coverage(rows)
        assert result["all_above_target"] is False
        assert result["worst_surface"] == "service_startup"
        assert result["worst_actual_pct"] == 36.1
        assert result["worst_target_pct"] == 100

    def test_worst_surface_is_lowest_actual_pct(self) -> None:
        from deployment_api.routes.repo_coverage import _evaluate_repo_coverage

        rows = [
            {"snapshot_at": "2026-05-17", "surface": "a", "actual_pct": 50.0, "target_pct": 80},
            {"snapshot_at": "2026-05-17", "surface": "b", "actual_pct": 20.0, "target_pct": 80},
            {"snapshot_at": "2026-05-17", "surface": "c", "actual_pct": 40.0, "target_pct": 80},
        ]
        result = _evaluate_repo_coverage(rows)
        assert result["worst_surface"] == "b"
        assert result["worst_actual_pct"] == 20.0

    def test_snapshot_at_from_first_row(self) -> None:
        from deployment_api.routes.repo_coverage import _evaluate_repo_coverage

        rows = [
            {"snapshot_at": "2026-01-01", "surface": "s", "actual_pct": 80.0, "target_pct": 90},
        ]
        result = _evaluate_repo_coverage(rows)
        assert result["snapshot_at"] == "2026-01-01"

    def test_missing_actual_pct_treated_as_zero(self) -> None:
        from deployment_api.routes.repo_coverage import _evaluate_repo_coverage

        rows = [{"snapshot_at": "2026-05-17", "surface": "missing_data", "target_pct": 80}]
        result = _evaluate_repo_coverage(rows)
        assert result["all_above_target"] is False


# ── get_repo_coverage route (mock mode) ──────────────────────────────────────


@pytest.fixture
def client_repo_coverage() -> TestClient:
    from deployment_api.routes.repo_coverage import router

    app = FastAPI()
    app.include_router(router, prefix="/repos")
    with patch(_PATCH_DISABLE_AUTH, True):
        return TestClient(app, raise_server_exceptions=False)


def test_get_repo_coverage_mock_mode_returns_list(client_repo_coverage: TestClient) -> None:
    mock_cfg = MagicMock()
    mock_cfg.is_mock_mode.return_value = True

    with patch(_PATCH_API_CFG, return_value=mock_cfg):
        r = client_repo_coverage.get("/repos/coverage")

    assert r.status_code == 200
    data = r.json()
    assert isinstance(data, list)
    assert len(data) >= 1
    assert all("repo" in item for item in data)
    assert all("all_above_target" in item for item in data)


def test_get_repo_coverage_mock_mode_has_expected_repos(client_repo_coverage: TestClient) -> None:
    mock_cfg = MagicMock()
    mock_cfg.is_mock_mode.return_value = True

    with patch(_PATCH_API_CFG, return_value=mock_cfg):
        r = client_repo_coverage.get("/repos/coverage")

    repos = {item["repo"] for item in r.json()}
    assert "deployment-api" in repos


def test_get_repo_coverage_gcs_failure_returns_no_data(client_repo_coverage: TestClient) -> None:
    mock_cfg = MagicMock()
    mock_cfg.is_mock_mode.return_value = False
    mock_cfg.gcp_project_id = "test-project"

    with (
        patch(_PATCH_API_CFG, return_value=mock_cfg),
        patch("deployment_api.routes.repo_coverage._load_coverage_from_gcs", side_effect=RuntimeError("GCS down")),
    ):
        r = client_repo_coverage.get("/repos/coverage")

    assert r.status_code == 200
    data = r.json()
    assert data[0]["repo"] == "no_data"


def test_get_repo_coverage_with_rows_returns_sorted_results(client_repo_coverage: TestClient) -> None:
    mock_cfg = MagicMock()
    mock_cfg.is_mock_mode.return_value = False
    mock_cfg.gcp_project_id = "test-project"

    mock_rows: dict[str, list[dict[str, object]]] = {
        "z-service": [{"snapshot_at": "2026-05-17", "surface": "unit", "actual_pct": 90.0, "target_pct": 80}],
        "a-service": [{"snapshot_at": "2026-05-17", "surface": "unit", "actual_pct": 50.0, "target_pct": 80}],
    }

    with (
        patch(_PATCH_API_CFG, return_value=mock_cfg),
        patch("deployment_api.routes.repo_coverage._load_coverage_from_gcs", return_value=mock_rows),
    ):
        r = client_repo_coverage.get("/repos/coverage")

    assert r.status_code == 200
    data = r.json()
    repos = [item["repo"] for item in data]
    assert repos == sorted(repos)

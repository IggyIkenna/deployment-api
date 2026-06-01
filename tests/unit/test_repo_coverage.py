"""Unit tests for /api/repos/coverage (Phase 8.E.2)."""

from __future__ import annotations

import os

import pytest
from fastapi.testclient import TestClient

os.environ.setdefault("CLOUD_PROVIDER", "local")
os.environ.setdefault("CLOUD_MOCK_MODE", "true")
os.environ.setdefault("GCP_PROJECT_ID", "test-project")
os.environ.setdefault("GOOGLE_APPLICATION_CREDENTIALS", "")


from deployment_api.main import app
from deployment_api.routes.repo_coverage import _evaluate_repo_coverage

client = TestClient(app, raise_server_exceptions=True)


class TestEvaluateRepoCoverage:
    def test_empty_rows_returns_not_above_target(self) -> None:
        result = _evaluate_repo_coverage([])
        assert result["all_above_target"] is False
        assert result["snapshot_at"] is None
        assert result["worst_surface"] is None

    def test_all_above_target(self) -> None:
        rows = [
            {
                "surface": "service_startup",
                "actual_pct": 100.0,
                "target_pct": 100,
                "snapshot_at": "2026-05-17T06:00:00Z",
            },
            {"surface": "default", "actual_pct": 85.0, "target_pct": 80, "snapshot_at": "2026-05-17T06:00:00Z"},
        ]
        result = _evaluate_repo_coverage(rows)
        assert result["all_above_target"] is True
        assert result["snapshot_at"] == "2026-05-17T06:00:00Z"
        assert result["worst_surface"] is None
        assert result["worst_actual_pct"] is None

    def test_some_below_target_returns_worst(self) -> None:
        rows = [
            {
                "surface": "service_startup",
                "actual_pct": 100.0,
                "target_pct": 100,
                "snapshot_at": "2026-05-17T06:00:00Z",
            },
            {
                "surface": "per_archetype_calculators",
                "actual_pct": 79.5,
                "target_pct": 90,
                "snapshot_at": "2026-05-17T06:00:00Z",
            },
            {"surface": "default", "actual_pct": 60.0, "target_pct": 80, "snapshot_at": "2026-05-17T06:00:00Z"},
        ]
        result = _evaluate_repo_coverage(rows)
        assert result["all_above_target"] is False
        assert result["worst_surface"] == "default"
        assert result["worst_actual_pct"] == pytest.approx(60.0)
        assert result["worst_target_pct"] == 80

    def test_snapshot_at_taken_from_first_row(self) -> None:
        rows = [{"surface": "default", "actual_pct": 90.0, "target_pct": 80, "snapshot_at": "2026-05-16T06:00:00Z"}]
        result = _evaluate_repo_coverage(rows)
        assert result["snapshot_at"] == "2026-05-16T06:00:00Z"

    def test_worst_is_lowest_actual_pct(self) -> None:
        rows = [
            {"surface": "custody_wallet", "actual_pct": 50.0, "target_pct": 100, "snapshot_at": "2026-05-17T06:00:00Z"},
            {
                "surface": "service_startup",
                "actual_pct": 70.0,
                "target_pct": 100,
                "snapshot_at": "2026-05-17T06:00:00Z",
            },
        ]
        result = _evaluate_repo_coverage(rows)
        assert result["worst_surface"] == "custody_wallet"
        assert result["worst_actual_pct"] == pytest.approx(50.0)


class TestRepoCoverageEndpoint:
    def test_mock_mode_returns_list(self) -> None:
        response = client.get("/api/repos/coverage")
        assert response.status_code == 200
        data = response.json()
        assert isinstance(data, list)
        assert len(data) >= 1

    def test_mock_mode_response_schema(self) -> None:
        response = client.get("/api/repos/coverage")
        data = response.json()
        for row in data:
            assert "repo" in row
            assert "all_above_target" in row
            assert "snapshot_at" in row
            assert "worst_surface" in row
            assert "worst_actual_pct" in row
            assert "worst_target_pct" in row

    def test_mock_mode_has_green_and_red_repos(self) -> None:
        response = client.get("/api/repos/coverage")
        data = response.json()
        above_target = [r for r in data if r["all_above_target"]]
        below_target = [r for r in data if not r["all_above_target"]]
        assert len(above_target) >= 1
        assert len(below_target) >= 1

    def test_mock_mode_below_target_has_worst_surface(self) -> None:
        response = client.get("/api/repos/coverage")
        data = response.json()
        for row in data:
            if not row["all_above_target"]:
                assert row["worst_surface"] is not None
                assert row["worst_actual_pct"] is not None
                assert row["worst_target_pct"] is not None

    def test_mock_mode_above_target_no_worst_surface(self) -> None:
        response = client.get("/api/repos/coverage")
        data = response.json()
        for row in data:
            if row["all_above_target"]:
                assert row["worst_surface"] is None
                assert row["worst_actual_pct"] is None
                assert row["worst_target_pct"] is None

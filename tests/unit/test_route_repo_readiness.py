"""Unit tests for routes/repo_readiness.py — pure helpers + mock-mode route."""

from __future__ import annotations

import tempfile
from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest
from fastapi import FastAPI
from fastapi.testclient import TestClient
from unified_trading_library import setup_events

setup_events("deployment-api", "test")

_PATCH_DISABLE_AUTH = "deployment_api.rbac.DISABLE_AUTH"
_PATCH_API_CFG = "deployment_api.deployment_api_config.DeploymentApiConfig"


# ── _check_p0_issue_docs ─────────────────────────────────────────────────────


class TestCheckP0IssueDocs:
    def test_nonexistent_dir_returns_true(self, tmp_path: Path) -> None:
        from deployment_api.routes.repo_readiness import _check_p0_issue_docs

        assert _check_p0_issue_docs(tmp_path / "nonexistent", "my-repo") is True

    def test_no_p0_docs_returns_true(self, tmp_path: Path) -> None:
        from deployment_api.routes.repo_readiness import _check_p0_issue_docs

        (tmp_path / "some_issue.md").write_text("severity: P2\nmy-repo mentioned here\n")
        assert _check_p0_issue_docs(tmp_path, "my-repo") is True

    def test_resolved_p0_issue_returns_true(self, tmp_path: Path) -> None:
        from deployment_api.routes.repo_readiness import _check_p0_issue_docs

        (tmp_path / "issue.md").write_text("severity: P0\nmy-repo\nresolved: true\n")
        assert _check_p0_issue_docs(tmp_path, "my-repo") is True

    def test_open_p0_issue_for_repo_returns_false(self, tmp_path: Path) -> None:
        from deployment_api.routes.repo_readiness import _check_p0_issue_docs

        (tmp_path / "issue.md").write_text("severity: P0\nmy-repo is broken\n")
        assert _check_p0_issue_docs(tmp_path, "my-repo") is False

    def test_p0_issue_not_referencing_repo_returns_true(self, tmp_path: Path) -> None:
        from deployment_api.routes.repo_readiness import _check_p0_issue_docs

        (tmp_path / "issue.md").write_text("severity: P0\nsome-other-repo is broken\n")
        assert _check_p0_issue_docs(tmp_path, "my-repo") is True


# ── _check_no_inflight_refactor_banner ───────────────────────────────────────


class TestCheckNoInflightRefactorBanner:
    def test_nonexistent_dir_returns_true(self, tmp_path: Path) -> None:
        from deployment_api.routes.repo_readiness import _check_no_inflight_refactor_banner

        assert _check_no_inflight_refactor_banner(tmp_path / "nonexistent", "my-repo") is True

    def test_no_banner_returns_true(self, tmp_path: Path) -> None:
        from deployment_api.routes.repo_readiness import _check_no_inflight_refactor_banner

        (tmp_path / "plan.md").write_text("# Normal plan\n\nSome content here.\n")
        assert _check_no_inflight_refactor_banner(tmp_path, "my-repo") is True

    def test_inflight_banner_for_repo_returns_false(self, tmp_path: Path) -> None:
        from deployment_api.routes.repo_readiness import _check_no_inflight_refactor_banner

        (tmp_path / "plan.md").write_text("> **🟡 IN-FLIGHT REFACTOR — my-repo auth migration**\n\n# Plan\n")
        assert _check_no_inflight_refactor_banner(tmp_path, "my-repo") is False

    def test_inflight_banner_for_other_repo_returns_true(self, tmp_path: Path) -> None:
        from deployment_api.routes.repo_readiness import _check_no_inflight_refactor_banner

        (tmp_path / "plan.md").write_text("> **🟡 IN-FLIGHT REFACTOR — other-repo migration**\n\n")
        assert _check_no_inflight_refactor_banner(tmp_path, "my-repo") is True


# ── _evaluate_repo ────────────────────────────────────────────────────────────


class TestEvaluateRepo:
    def _green_snapshot(self) -> dict[str, object]:
        return {"qg_status": "green", "failing_step": None, "snapshot_at": "2026-05-15"}

    def test_fewer_than_5_snapshots_returns_not_enough(self) -> None:
        from deployment_api.routes.repo_readiness import _evaluate_repo

        result = _evaluate_repo("my-repo", [self._green_snapshot()] * 3, None)
        assert result["deploy_ready"] is False
        assert result["reason"] == "not_enough_snapshots"

    def test_exactly_5_green_snapshots_returns_ready(self) -> None:
        from deployment_api.routes.repo_readiness import _evaluate_repo

        result = _evaluate_repo("my-repo", [self._green_snapshot()] * 5, None)
        assert result["deploy_ready"] is True
        assert result["reason"] == "all_criteria_met"

    def test_one_failing_snapshot_returns_snapshot_failing(self) -> None:
        from deployment_api.routes.repo_readiness import _evaluate_repo

        snaps = [self._green_snapshot()] * 4 + [{"qg_status": "red", "failing_step": "lint", "snapshot_at": "x"}]
        result = _evaluate_repo("my-repo", snaps, None)
        assert result["deploy_ready"] is False
        assert "snapshot_failing" in str(result["reason"])

    def test_open_p0_issue_returns_not_ready(self, tmp_path: Path) -> None:
        from deployment_api.routes.repo_readiness import _evaluate_repo

        issues_dir = tmp_path / "issues"
        issues_dir.mkdir()
        (issues_dir / "issue.md").write_text("severity: P0\nmy-repo is broken\n")

        result = _evaluate_repo("my-repo", [self._green_snapshot()] * 5, tmp_path)
        assert result["deploy_ready"] is False
        assert result["reason"] == "open_p0_issue"

    def test_inflight_banner_returns_not_ready(self, tmp_path: Path) -> None:
        from deployment_api.routes.repo_readiness import _evaluate_repo

        (tmp_path / "plan.md").write_text("> **🟡 IN-FLIGHT REFACTOR — my-repo migration**\n\n")

        result = _evaluate_repo("my-repo", [self._green_snapshot()] * 5, tmp_path)
        assert result["deploy_ready"] is False
        assert result["reason"] == "in_flight_refactor"

    def test_empty_snapshots_returns_not_enough(self) -> None:
        from deployment_api.routes.repo_readiness import _evaluate_repo

        result = _evaluate_repo("my-repo", [], None)
        assert result["deploy_ready"] is False
        assert result["reason"] == "not_enough_snapshots"


# ── get_deploy_ready route (mock mode) ────────────────────────────────────────


@pytest.fixture
def client_repo_readiness() -> TestClient:
    from deployment_api.routes.repo_readiness import router

    app = FastAPI()
    app.include_router(router, prefix="/repos")
    with patch(_PATCH_DISABLE_AUTH, True):
        return TestClient(app, raise_server_exceptions=False)


def test_get_deploy_ready_mock_mode_returns_list(client_repo_readiness: TestClient) -> None:
    mock_cfg = MagicMock()
    mock_cfg.is_mock_mode.return_value = True

    with patch(_PATCH_API_CFG, return_value=mock_cfg):
        r = client_repo_readiness.get("/repos/deploy-ready")

    assert r.status_code == 200
    data = r.json()
    assert isinstance(data, list)
    assert len(data) >= 1
    assert all("repo" in item for item in data)
    assert all("deploy_ready" in item for item in data)


def test_get_deploy_ready_mock_mode_has_expected_repos(client_repo_readiness: TestClient) -> None:
    mock_cfg = MagicMock()
    mock_cfg.is_mock_mode.return_value = True

    with patch(_PATCH_API_CFG, return_value=mock_cfg):
        r = client_repo_readiness.get("/repos/deploy-ready")

    repos = {item["repo"] for item in r.json()}
    assert "deployment-api" in repos


def test_get_deploy_ready_gcs_failure_returns_no_data(client_repo_readiness: TestClient) -> None:
    mock_cfg = MagicMock()
    mock_cfg.is_mock_mode.return_value = False
    mock_cfg.gcp_project_id = "test-project"
    mock_cfg.workspace_root = None

    with (
        patch(_PATCH_API_CFG, return_value=mock_cfg),
        patch("deployment_api.routes.repo_readiness._load_snapshots_from_gcs", side_effect=RuntimeError("GCS down")),
    ):
        r = client_repo_readiness.get("/repos/deploy-ready")

    assert r.status_code == 200
    data = r.json()
    assert data[0]["repo"] == "no_data"
    assert data[0]["reason"] == "no_snapshots_in_gcs"


def test_get_deploy_ready_with_snapshots_returns_sorted(client_repo_readiness: TestClient) -> None:
    mock_cfg = MagicMock()
    mock_cfg.is_mock_mode.return_value = False
    mock_cfg.gcp_project_id = "test-project"
    mock_cfg.workspace_root = None

    green = {"qg_status": "green", "failing_step": None, "snapshot_at": "2026-05-15"}
    mock_snaps: dict[str, list[dict[str, object]]] = {
        "z-service": [green] * 5,
        "a-service": [green] * 5,
    }

    with (
        patch(_PATCH_API_CFG, return_value=mock_cfg),
        patch("deployment_api.routes.repo_readiness._load_snapshots_from_gcs", return_value=mock_snaps),
    ):
        r = client_repo_readiness.get("/repos/deploy-ready")

    assert r.status_code == 200
    data = r.json()
    repos = [item["repo"] for item in data]
    assert repos == sorted(repos)
    assert all(item["deploy_ready"] is True for item in data)

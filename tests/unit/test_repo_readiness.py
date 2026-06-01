"""
Unit tests for deployment_api.routes.repo_readiness (B-013).

Covers the three criteria via the pure helper functions:
  1. green-5 case              → deploy_ready=True
  2. failing-snapshot case     → deploy_ready=False, reason includes step name
  3. P0-issue-doc case         → deploy_ready=False, reason=open_p0_issue
  4. in-flight refactor case   → deploy_ready=False, reason=in_flight_refactor
  5. not-enough-snapshots case → deploy_ready=False, reason=not_enough_snapshots
  6. mock mode                 → synthetic list returned without hitting GCS

Plan: deployment_and_qg_strategy_implementation_2026_05_13.md Phase 4.B.
"""

from __future__ import annotations

from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest

from deployment_api.routes.repo_readiness import (
    _check_no_inflight_refactor_banner,
    _check_p0_issue_docs,
    _evaluate_repo,
)


def _green_snapshots(n: int = 5) -> list[dict[str, object]]:
    return [{"qg_status": "green", "failing_step": None}] * n


class TestCheckP0IssueDocs:
    def test_no_issues_dir_returns_true(self, tmp_path: Path) -> None:
        assert _check_p0_issue_docs(tmp_path / "nonexistent", "any-repo") is True

    def test_no_matching_issue_returns_true(self, tmp_path: Path) -> None:
        f = tmp_path / "some_issue_2026_05_14.md"
        f.write_text("severity: P1\nsome-other-repo mentioned here")
        assert _check_p0_issue_docs(tmp_path, "my-repo") is True

    def test_open_p0_issue_for_repo_returns_false(self, tmp_path: Path) -> None:
        f = tmp_path / "critical_issue_2026_05_14.md"
        f.write_text("severity: P0\nmy-repo has a problem")
        assert _check_p0_issue_docs(tmp_path, "my-repo") is False

    def test_resolved_p0_issue_returns_true(self, tmp_path: Path) -> None:
        f = tmp_path / "old_issue_2026_05_14.md"
        f.write_text("severity: P0\nmy-repo affected\nresolved: 2026-05-14")
        assert _check_p0_issue_docs(tmp_path, "my-repo") is True

    def test_p0_issue_for_different_repo_returns_true(self, tmp_path: Path) -> None:
        f = tmp_path / "other_issue_2026_05_14.md"
        f.write_text("severity: P0\nother-repo has a problem")
        assert _check_p0_issue_docs(tmp_path, "my-repo") is True


class TestCheckNoInflightRefactorBanner:
    def test_no_plans_dir_returns_true(self, tmp_path: Path) -> None:
        assert _check_no_inflight_refactor_banner(tmp_path / "nonexistent", "any-repo") is True

    def test_no_banner_returns_true(self, tmp_path: Path) -> None:
        f = tmp_path / "some_plan.md"
        f.write_text("# A plan\n\nNo banners here.\n")
        assert _check_no_inflight_refactor_banner(tmp_path, "my-repo") is True

    def test_inflight_refactor_banner_for_repo_returns_false(self, tmp_path: Path) -> None:
        f = tmp_path / "refactor_plan.md"
        f.write_text("> **🟡 IN-FLIGHT REFACTOR — touching my-repo UAC schema**\n\n# Plan body\n")
        assert _check_no_inflight_refactor_banner(tmp_path, "my-repo") is False

    def test_inflight_refactor_for_different_repo_returns_true(self, tmp_path: Path) -> None:
        f = tmp_path / "other_plan.md"
        f.write_text("> **🟡 IN-FLIGHT REFACTOR — touching other-repo**\n\n# Plan body\n")
        assert _check_no_inflight_refactor_banner(tmp_path, "my-repo") is True

    def test_banner_beyond_30_lines_not_detected(self, tmp_path: Path) -> None:
        f = tmp_path / "deep_plan.md"
        lines = ["# Some header\n"] * 35
        lines.append("> **🟡 IN-FLIGHT REFACTOR — touching my-repo**\n")
        f.write_text("".join(lines))
        # Banner at line 36 is past the 30-line window — should not flag
        assert _check_no_inflight_refactor_banner(tmp_path, "my-repo") is True


class TestEvaluateRepo:
    def test_green_five_snapshots_no_plans_dir(self) -> None:
        result = _evaluate_repo("my-repo", _green_snapshots(5), None)
        assert result["deploy_ready"] is True
        assert result["reason"] == "all_criteria_met"

    def test_not_enough_snapshots_returns_false(self) -> None:
        result = _evaluate_repo("my-repo", _green_snapshots(4), None)
        assert result["deploy_ready"] is False
        assert result["reason"] == "not_enough_snapshots"

    def test_zero_snapshots_returns_false(self) -> None:
        result = _evaluate_repo("my-repo", [], None)
        assert result["deploy_ready"] is False
        assert result["reason"] == "not_enough_snapshots"

    def test_failing_snapshot_returns_false_with_step(self) -> None:
        snapshots: list[dict[str, object]] = [
            {"qg_status": "green", "failing_step": None},
            {"qg_status": "red", "failing_step": "lint"},
            {"qg_status": "green", "failing_step": None},
            {"qg_status": "green", "failing_step": None},
            {"qg_status": "green", "failing_step": None},
        ]
        result = _evaluate_repo("my-repo", snapshots, None)
        assert result["deploy_ready"] is False
        assert "lint" in str(result["reason"])

    def test_p0_issue_doc_blocks_deploy(self, tmp_path: Path) -> None:
        issues_dir = tmp_path / "issues"
        issues_dir.mkdir()
        (issues_dir / "bug_2026_05_14.md").write_text("severity: P0\nmy-repo affected by this bug")
        result = _evaluate_repo("my-repo", _green_snapshots(5), tmp_path)
        assert result["deploy_ready"] is False
        assert result["reason"] == "open_p0_issue"

    def test_inflight_refactor_banner_blocks_deploy(self, tmp_path: Path) -> None:
        (tmp_path / "refactor.md").write_text("> **🟡 IN-FLIGHT REFACTOR — touching my-repo contracts**\n\n# Body\n")
        result = _evaluate_repo("my-repo", _green_snapshots(5), tmp_path)
        assert result["deploy_ready"] is False
        assert result["reason"] == "in_flight_refactor"

    def test_green_all_criteria_with_clean_plans_dir(self, tmp_path: Path) -> None:
        (tmp_path / "clean_plan.md").write_text("# Clean plan\n\nNo banners, no issues.\n")
        (tmp_path / "issues").mkdir()
        result = _evaluate_repo("my-repo", _green_snapshots(5), tmp_path)
        assert result["deploy_ready"] is True
        assert result["reason"] == "all_criteria_met"


class TestGetDeployReadyEndpoint:
    @pytest.mark.asyncio
    async def test_mock_mode_returns_synthetic_list(self) -> None:
        mock_cfg = MagicMock()
        mock_cfg.is_mock_mode.return_value = True
        with patch(
            "deployment_api.deployment_api_config.DeploymentApiConfig",
            return_value=mock_cfg,
        ):
            from deployment_api.routes.repo_readiness import get_deploy_ready

            result = await get_deploy_ready()

        assert isinstance(result, list)
        assert len(result) == 5
        repos = [r["repo"] for r in result]
        assert "deployment-api" in repos
        assert "strategy-service" in repos

    @pytest.mark.asyncio
    async def test_mock_mode_has_mixed_readiness(self) -> None:
        mock_cfg = MagicMock()
        mock_cfg.is_mock_mode.return_value = True
        with patch(
            "deployment_api.deployment_api_config.DeploymentApiConfig",
            return_value=mock_cfg,
        ):
            from deployment_api.routes.repo_readiness import get_deploy_ready

            result = await get_deploy_ready()

        ready = [r for r in result if r["deploy_ready"] is True]
        not_ready = [r for r in result if r["deploy_ready"] is False]
        assert len(ready) >= 1
        assert len(not_ready) >= 1

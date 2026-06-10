"""Unit tests for routes/repo_ci.py — mock-mode payload shape (drives the UI contract).

The mock fixtures intentionally cover EVERY stuck class + stuck-in-SIT + the live
SIT-run panel; the deployment-ui playwright regression spec asserts the same set, so
these tests pin the API side of that contract.
"""

from __future__ import annotations

from unittest.mock import patch

import pytest
from fastapi import FastAPI
from fastapi.testclient import TestClient
from unified_trading_library import setup_events

setup_events("deployment-api", "test")

_PATCH_MOCK_MODE = "deployment_api.routes.repo_ci.DeploymentApiConfig.is_mock_mode"


@pytest.fixture
def client_repo_ci() -> TestClient:
    from deployment_api.routes.repo_ci import router

    app = FastAPI()
    app.include_router(router)
    return TestClient(app, raise_server_exceptions=False)


class TestOverviewMock:
    def test_overview_shape(self, client_repo_ci: TestClient) -> None:
        with patch(_PATCH_MOCK_MODE, return_value=True):
            resp = client_repo_ci.get("/api/repo-ci/overview")
        assert resp.status_code == 200
        body = resp.json()
        assert body["source"] == "mock"
        assert len(body["repos"]) >= 5
        row = body["repos"][0]
        assert {"repo", "repo_type", "ci_status", "branches", "deltas", "open_prs", "sit", "image"} <= set(row)
        assert [b["branch"] for b in row["branches"]] == ["live-defi-rollout", "staging", "main"]

    def test_all_stuck_classes_present(self, client_repo_ci: TestClient) -> None:
        with patch(_PATCH_MOCK_MODE, return_value=True):
            body = client_repo_ci.get("/api/repo-ci/overview").json()
        classes = {pr["stuck_class"] for pr in body["stuck_prs"]}
        assert classes == {"conflicting", "v2_never_reported", "skip_ci_jammed", "failing_check", "automerge_stuck"}

    def test_stuck_in_sit_present(self, client_repo_ci: TestClient) -> None:
        with patch(_PATCH_MOCK_MODE, return_value=True):
            body = client_repo_ci.get("/api/repo-ci/overview").json()
        assert body["stuck_in_sit"] == ["greeks-service"]
        greeks = next(r for r in body["repos"] if r["repo"] == "greeks-service")
        assert greeks["sit"]["stuck_in_sit"] is True
        assert greeks["sit"]["in_breaking_pending"] is True

    def test_sit_last_run_panel(self, client_repo_ci: TestClient) -> None:
        # Alert-parity (operator add 2026-06-10): the last SIT run is ALWAYS visible —
        # per-repo jobs with pass/fail/in-progress, not just a failure alert.
        with patch(_PATCH_MOCK_MODE, return_value=True):
            body = client_repo_ci.get("/api/repo-ci/overview").json()
        sit_run = body["sit_last_run"]
        assert sit_run is not None
        assert sit_run["status"] == "in_progress"
        conclusions = {j["name"]: j["conclusion"] for j in sit_run["jobs"]}
        assert conclusions["sit / unified-trading-library"] == "success"
        assert conclusions["sit / greeks-service"] == "failure"
        assert conclusions["sit / execution-service"] is None  # in progress

    def test_stuck_prs_match_rows(self, client_repo_ci: TestClient) -> None:
        with patch(_PATCH_MOCK_MODE, return_value=True):
            body = client_repo_ci.get("/api/repo-ci/overview").json()
        from_rows = {(pr["repo"], pr["number"]) for row in body["repos"] for pr in row["open_prs"] if pr["stuck_class"]}
        from_panel = {(pr["repo"], pr["number"]) for pr in body["stuck_prs"]}
        assert from_rows == from_panel

    def test_errors_block_present(self, client_repo_ci: TestClient) -> None:
        # Operator add 2026-06-10: a degraded repo surfaces as an errors[] entry, never a
        # silently-dropped row. The mock seeds one so the UI panel + spec have data.
        with patch(_PATCH_MOCK_MODE, return_value=True):
            body = client_repo_ci.get("/api/repo-ci/overview").json()
        assert "errors" in body
        assert isinstance(body["errors"], list)
        assert body["errors"], "mock seeds at least one degraded-repo error"
        err = body["errors"][0]
        assert {"repo", "error"} <= set(err)
        # An errors[] repo must NOT also appear as a rendered row (it was dropped from rows).
        row_repos = {r["repo"] for r in body["repos"]}
        assert err["repo"] not in row_repos


class TestDetailMock:
    def test_detail_shape(self, client_repo_ci: TestClient) -> None:
        with patch(_PATCH_MOCK_MODE, return_value=True):
            resp = client_repo_ci.get("/api/repo-ci/execution-service/detail")
        assert resp.status_code == 200
        body = resp.json()
        assert body["repo"] == "execution-service"
        assert len(body["history"]) == 3  # one per promotion branch
        for branch_history in body["history"]:
            assert branch_history["commits"], "every branch carries SHA history"
            first = branch_history["commits"][0]
            assert {"sha", "message", "author", "committed_at", "v2_conclusion"} <= set(first)

    def test_detail_unknown_repo_falls_back_in_mock(self, client_repo_ci: TestClient) -> None:
        with patch(_PATCH_MOCK_MODE, return_value=True):
            resp = client_repo_ci.get("/api/repo-ci/not-a-repo/detail")
        assert resp.status_code == 200  # mock mode serves the first fixture row


class TestFleetGitHealthMock:
    def test_fleet_git_health_mock_shape(self, client_repo_ci: TestClient) -> None:
        with patch(_PATCH_MOCK_MODE, return_value=True):
            resp = client_repo_ci.get("/api/repo-ci/fleet-git-health")
        assert resp.status_code == 200
        body = resp.json()
        # Always carries the orchestrator deep-link URL (git-health click-through → AO UI).
        assert body["orchestrator_url"].startswith("http")
        assert body["available"] is True
        assert body["reason"] == ""
        data = body["data"]
        assert data is not None
        assert data["summary"]["drift_violations"] == 1
        # The drift-violating repo is flagged + rolled up.
        drift_repos = {d["repo"] for d in data["drift_violations"]}
        assert "execution-service" in drift_repos
        flagged = [
            r["name"]
            for h in data["hosts"]
            for s in h["slots"]
            for r in r_iter(s)
            if r["drift_violation"]
        ]
        assert "execution-service" in flagged


def r_iter(slot: dict[str, object]) -> list[dict[str, object]]:
    repos = slot["repos"]
    assert isinstance(repos, list)
    return repos


class TestFleetGitHealthDegrade:
    def test_proxy_degrades_honestly_without_token(self) -> None:
        """No token configured → available=False + a reason + the deep-link URL, never raises."""
        import asyncio
        from unittest.mock import AsyncMock

        from deployment_api.routes import _repo_ci_fleet

        with patch.object(_repo_ci_fleet, "_resolve_orchestrator_token", new=AsyncMock(return_value=None)):
            result = asyncio.run(_repo_ci_fleet.fetch_fleet_git_health("proj"))
        assert result["available"] is False
        assert result["data"] is None
        assert "token" in result["reason"]
        assert result["orchestrator_url"].startswith("http")

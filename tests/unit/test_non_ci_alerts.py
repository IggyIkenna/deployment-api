"""Unit tests for the non-CI ops alert ledger reader and unified /api/alerts merger."""

from __future__ import annotations

import json
from unittest.mock import AsyncMock, MagicMock, patch

import pytest
from fastapi import FastAPI
from fastapi.testclient import TestClient
from unified_trading_library import setup_events

from deployment_api.routes._non_ci_alerts import _parse_ops_line
from deployment_api.routes._repo_ci_alerts import AlertEntryDict

setup_events("deployment-api", "test")

_PATCH_MOCK_MODE = "deployment_api.routes.unified_alerts.DeploymentApiConfig.is_mock_mode"


# ---------------------------------------------------------------------------
# _parse_ops_line
# ---------------------------------------------------------------------------


def _ops_line(kind: str, severity: str = "WARNING", message: str = "test") -> str:
    return json.dumps(
        {
            "event_type": "ops_alert",
            "timestamp": "2026-06-20T10:00:00Z",
            "kind": kind,
            "source": "agent-orchestrator",
            "severity": severity,
            "message": message,
            "run_url": None,
        }
    )


class TestParseOpsLine:
    def test_git_health_parses_correctly(self) -> None:
        parsed = _parse_ops_line(_ops_line("git_health", "WARNING", "Slot 3 git RED"))
        assert parsed is not None
        assert parsed["kind"] == "git_health"
        assert parsed["severity"] == "WARNING"
        assert parsed["message"] == "Slot 3 git RED"
        assert parsed["workflow_name"] == "ops-git_health"
        assert parsed["repo"] == "agent-orchestrator"
        assert parsed["conclusion"] is None
        assert parsed["run_url"] is None

    def test_worker_liveness_parses_correctly(self) -> None:
        parsed = _parse_ops_line(_ops_line("worker_liveness", "WARNING", "Slot 2 watchdog-killed"))
        assert parsed is not None
        assert parsed["kind"] == "worker_liveness"
        assert parsed["workflow_name"] == "ops-worker_liveness"

    def test_vm_down_parses_correctly(self) -> None:
        parsed = _parse_ops_line(_ops_line("vm_down", "CRITICAL", "VM vm-1 DOWN"))
        assert parsed is not None
        assert parsed["kind"] == "vm_down"
        assert parsed["severity"] == "CRITICAL"

    def test_consolidator_down_parses(self) -> None:
        parsed = _parse_ops_line(_ops_line("consolidator_down", "CRITICAL", "Consolidator stale"))
        assert parsed is not None
        assert parsed["kind"] == "consolidator_down"

    def test_data_pipeline_parses(self) -> None:
        parsed = _parse_ops_line(_ops_line("data_pipeline", "WARNING", "Data pipeline stalled"))
        assert parsed is not None
        assert parsed["kind"] == "data_pipeline"

    def test_junk_lines_return_none(self) -> None:
        assert _parse_ops_line("") is None
        assert _parse_ops_line("not json") is None
        assert _parse_ops_line('{"event_type":"slack_alert"}') is None
        assert _parse_ops_line('{"event_type":"ops_alert"}') is None  # missing kind
        assert _parse_ops_line("[1, 2]") is None

    def test_unknown_event_type_skipped(self) -> None:
        line = json.dumps({"event_type": "github_workflow_event", "kind": "git_health"})
        assert _parse_ops_line(line) is None

    def test_run_url_threaded_through(self) -> None:
        line = json.dumps(
            {
                "event_type": "ops_alert",
                "timestamp": "2026-06-20T10:00:00Z",
                "kind": "vm_down",
                "source": "vm-watchdog",
                "severity": "CRITICAL",
                "message": "VM DOWN",
                "run_url": "https://console.cloud.google.com/vm/vm-1",
            }
        )
        parsed = _parse_ops_line(line)
        assert parsed is not None
        assert parsed["run_url"] == "https://console.cloud.google.com/vm/vm-1"
        assert parsed["repo"] == "vm-watchdog"


# ---------------------------------------------------------------------------
# GET /api/alerts mock mode (unified merger)
# ---------------------------------------------------------------------------


@pytest.fixture
def client_unified() -> TestClient:
    from deployment_api.routes.unified_alerts import router

    app = FastAPI()
    app.include_router(router, prefix="/api")
    return TestClient(app, raise_server_exceptions=False)


class TestUnifiedAlertsMockMode:
    def test_mock_payload_includes_non_ci_kinds(self, client_unified: TestClient) -> None:
        with patch(_PATCH_MOCK_MODE, return_value=True):
            resp = client_unified.get("/api/alerts")
        assert resp.status_code == 200
        body = resp.json()
        assert body["source"] == "mock"
        kinds = {a["kind"] for a in body["alerts"]}
        assert "alert" in kinds
        assert "git_health" in kinds
        assert "worker_liveness" in kinds
        assert "vm_down" in kinds

    def test_mock_payload_newest_first(self, client_unified: TestClient) -> None:
        with patch(_PATCH_MOCK_MODE, return_value=True):
            resp = client_unified.get("/api/alerts")
        body = resp.json()
        timestamps = [a["timestamp"] for a in body["alerts"]]
        assert timestamps == sorted(timestamps, reverse=True)

    def test_mock_streams_worst_current_first(self, client_unified: TestClient) -> None:
        with patch(_PATCH_MOCK_MODE, return_value=True):
            resp = client_unified.get("/api/alerts")
        body = resp.json()
        streams = body["streams"]
        # At least one CRITICAL stream should exist (vm_down or ci CRITICAL).
        critical_streams = [s for s in streams if s["current"]["severity"] == "CRITICAL"]
        assert len(critical_streams) >= 1
        # The first stream must be CRITICAL (worst-first sort).
        assert streams[0]["current"]["severity"] == "CRITICAL"

    def test_mock_non_ci_stream_has_no_previous(self, client_unified: TestClient) -> None:
        with patch(_PATCH_MOCK_MODE, return_value=True):
            resp = client_unified.get("/api/alerts")
        body = resp.json()
        # Non-CI ops alerts each appear once (no lifecycle pair) so previous=None.
        git_stream = next((s for s in body["streams"] if s["workflow_name"] == "ops-git_health"), None)
        assert git_stream is not None
        assert git_stream["previous"] is None
        assert git_stream["count"] == 1


# ---------------------------------------------------------------------------
# Live mode merger (mocked I/O — no real GCS calls)
# ---------------------------------------------------------------------------


class TestUnifiedAlertsLiveMode:
    def test_live_mode_merges_ci_and_non_ci(self, client_unified: TestClient) -> None:
        ci_entry = AlertEntryDict(
            kind="alert",
            timestamp="2026-06-20T09:00:00Z",
            repo="unified-trading-pm",
            workflow_name="ci-status-update",
            severity="CRITICAL",
            conclusion="failure",
            message="CI FAIL",
            run_url=None,
        )
        non_ci_entry = AlertEntryDict(
            kind="git_health",
            timestamp="2026-06-20T09:05:00Z",
            repo="agent-orchestrator",
            workflow_name="ops-git_health",
            severity="WARNING",
            conclusion=None,
            message="Slot 3 git RED",
            run_url=None,
        )
        from deployment_api.routes._repo_ci_alerts import AlertsPayloadDict, derive_streams
        from deployment_api.routes._repo_ci_alerts import AlertEntryDict as _AE

        import datetime as dt

        ci_payload = AlertsPayloadDict(
            generated_at=dt.datetime.now(dt.UTC).isoformat(),
            source="live",
            alerts=[ci_entry],
            streams=derive_streams([ci_entry]),
        )

        with (
            patch(_PATCH_MOCK_MODE, return_value=False),
            patch(
                "deployment_api.routes.unified_alerts.load_alerts_payload",
                new=AsyncMock(return_value=ci_payload),
            ),
            patch(
                "deployment_api.routes.unified_alerts.load_non_ci_alerts",
                new=AsyncMock(return_value=[non_ci_entry]),
            ),
        ):
            resp = client_unified.get("/api/alerts")

        assert resp.status_code == 200
        body = resp.json()
        assert body["source"] == "live"
        kinds = {a["kind"] for a in body["alerts"]}
        assert "alert" in kinds
        assert "git_health" in kinds
        # Non-CI entry has a later timestamp so it appears first (newest-first).
        assert body["alerts"][0]["kind"] == "git_health"

    def test_live_mode_non_ci_failure_still_returns_ci_alerts(self, client_unified: TestClient) -> None:
        ci_entry = AlertEntryDict(
            kind="alert",
            timestamp="2026-06-20T09:00:00Z",
            repo="pm",
            workflow_name="ci-status-update",
            severity="INFO",
            conclusion="success",
            message="ok",
            run_url=None,
        )
        from deployment_api.routes._repo_ci_alerts import AlertsPayloadDict, derive_streams

        import datetime as dt

        ci_payload = AlertsPayloadDict(
            generated_at=dt.datetime.now(dt.UTC).isoformat(),
            source="live",
            alerts=[ci_entry],
            streams=derive_streams([ci_entry]),
        )

        with (
            patch(_PATCH_MOCK_MODE, return_value=False),
            patch(
                "deployment_api.routes.unified_alerts.load_alerts_payload",
                new=AsyncMock(return_value=ci_payload),
            ),
            patch(
                "deployment_api.routes.unified_alerts.load_non_ci_alerts",
                new=AsyncMock(return_value=[]),
            ),
        ):
            resp = client_unified.get("/api/alerts")

        assert resp.status_code == 200
        body = resp.json()
        assert len(body["alerts"]) == 1
        assert body["alerts"][0]["kind"] == "alert"

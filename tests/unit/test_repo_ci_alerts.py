"""Unit tests for the alert-ledger reader + lifecycle stream derivation."""

from __future__ import annotations

from unittest.mock import patch

import pytest
from fastapi import FastAPI
from fastapi.testclient import TestClient
from unified_trading_library import setup_events

from deployment_api.routes._repo_ci_alerts import AlertEntryDict, _parse_line, derive_streams

setup_events("deployment-api", "test")

_PATCH_MOCK_MODE = "deployment_api.routes.repo_ci.DeploymentApiConfig.is_mock_mode"


def _entry(ts: str, repo: str, wf: str, severity: str | None, conclusion: str | None) -> AlertEntryDict:
    return AlertEntryDict(
        kind="alert" if severity else "event",
        timestamp=ts,
        repo=repo,
        workflow_name=wf,
        severity=severity,
        conclusion=conclusion,
        message="m" if severity else None,
        run_url=None,
    )


class TestParseLine:
    def test_slack_alert_shape(self) -> None:
        line = (
            '{"event_type":"slack_alert","timestamp":"2026-06-10T12:00:00Z","repo":"unified-trading-pm",'
            '"workflow_name":"sit-unlock","severity":"INFO","conclusion":"success","run_url":"u","message":"ok"}'
        )
        parsed = _parse_line(line)
        assert parsed is not None
        assert parsed["kind"] == "alert"
        assert parsed["severity"] == "INFO"
        assert parsed["message"] == "ok"

    def test_workflow_event_shape(self) -> None:
        line = (
            '{"event_type":"github_workflow_event","timestamp":"2026-06-10T11:00:00Z",'
            '"repo_name":"greeks-service","workflow_name":"sit-gate","conclusion":"failure"}'
        )
        parsed = _parse_line(line)
        assert parsed is not None
        assert parsed["kind"] == "event"
        assert parsed["repo"] == "greeks-service"
        assert parsed["conclusion"] == "failure"

    def test_junk_lines_skipped(self) -> None:
        assert _parse_line("not json") is None
        assert _parse_line('{"event_type":"unknown"}') is None
        assert _parse_line("[1,2]") is None


class TestDeriveStreams:
    def test_current_and_previous_pairing(self) -> None:
        entries = [
            _entry("2026-06-10T12:00:00Z", "pm", "ci-status-update", "CRITICAL", "failure"),
            _entry("2026-06-10T12:50:00Z", "pm", "ci-status-update", "INFO", "success"),
        ]
        streams = derive_streams(entries)
        assert len(streams) == 1
        stream = streams[0]
        # Lifecycle traceability: current is the recovery, previous is the page.
        assert stream["current"]["severity"] == "INFO"
        assert stream["previous"] is not None
        assert stream["previous"]["severity"] == "CRITICAL"
        assert stream["count"] == 2

    def test_worst_current_sorts_first(self) -> None:
        entries = [
            _entry("2026-06-10T10:00:00Z", "a", "wf-green", "INFO", "success"),
            _entry("2026-06-10T09:00:00Z", "b", "wf-red", "CRITICAL", "failure"),
        ]
        streams = derive_streams(entries)
        assert streams[0]["workflow_name"] == "wf-red"  # CRITICAL current outranks newer INFO

    def test_single_entry_stream_has_no_previous(self) -> None:
        streams = derive_streams([_entry("2026-06-10T10:00:00Z", "a", "w", "WARNING", None)])
        assert streams[0]["previous"] is None


@pytest.fixture
def client_alerts() -> TestClient:
    from deployment_api.routes.repo_ci import router

    app = FastAPI()
    app.include_router(router)
    return TestClient(app, raise_server_exceptions=False)


class TestAlertsRouteMock:
    def test_mock_payload_lifecycle(self, client_alerts: TestClient) -> None:
        with patch(_PATCH_MOCK_MODE, return_value=True):
            resp = client_alerts.get("/api/repo-ci/alerts")
        assert resp.status_code == 200
        body = resp.json()
        assert body["source"] == "mock"
        assert body["alerts"][0]["timestamp"] >= body["alerts"][-1]["timestamp"]  # newest first
        # The FAILED -> RESOLVED pair surfaces as one stream with previous=CRITICAL.
        ci_stream = next(s for s in body["streams"] if s["workflow_name"] == "ci-status-update")
        assert ci_stream["current"]["severity"] == "INFO"
        assert ci_stream["previous"]["severity"] == "CRITICAL"
        # A live CRITICAL stream sorts first.
        assert body["streams"][0]["current"]["severity"] == "CRITICAL"

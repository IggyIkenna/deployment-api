"""Unit tests for the alert-ledger reader + lifecycle stream derivation."""

from __future__ import annotations

from unittest.mock import patch

import pytest
from fastapi import FastAPI
from fastapi.testclient import TestClient
from unified_trading_library import BucketNamingError, setup_events

from deployment_api.routes._repo_ci_alerts import (
    AlertEntryDict,
    _parse_alerting_service_line,
    _parse_line,
    derive_streams,
)

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
        # gha_ci_events plane: repo_name IS the subject (each repo reports on itself).
        assert parsed.get("subject_repo") == "greeks-service"

    def test_junk_lines_skipped(self) -> None:
        assert _parse_line("not json") is None
        assert _parse_line('{"event_type":"unknown"}') is None
        assert _parse_line("[1,2]") is None

    def test_slack_alert_subject_repo_distinct_from_emitter(self) -> None:
        """deployment_alerts_ingestion_completeness_2026_07_20.md todo 4: a cross-repo watcher
        (e.g. ci-status-update.yml, running in unified-trading-pm) reports on unified-trading-library
        — repo must stay the emitter while subject_repo carries the actual regression's repo."""
        line = (
            '{"event_type":"slack_alert","timestamp":"2026-07-21T12:00:00Z","repo":"unified-trading-pm",'
            '"subject_repo":"unified-trading-library","workflow_name":"ci-status-update",'
            '"severity":"CRITICAL","conclusion":"failure","run_url":"u","message":"CI regression"}'
        )
        parsed = _parse_line(line)
        assert parsed is not None
        assert parsed["repo"] == "unified-trading-pm"
        assert parsed.get("subject_repo") == "unified-trading-library"
        assert parsed["repo"] != parsed.get("subject_repo")

    def test_slack_alert_subject_repo_absent_on_old_rows(self) -> None:
        """A row written before todo 4 (no subject_repo key at all) degrades to None, not the
        emitter — never silently misreport the emitter as the subject."""
        line = (
            '{"event_type":"slack_alert","timestamp":"2026-06-10T12:00:00Z","repo":"unified-trading-pm",'
            '"workflow_name":"sit-unlock","severity":"INFO","conclusion":"success","run_url":"u","message":"ok"}'
        )
        parsed = _parse_line(line)
        assert parsed is not None
        assert parsed.get("subject_repo") is None


class TestParseAlertingServiceLine:
    def test_delivery_record_shape(self) -> None:
        line = (
            '{"alert_id":"a1","channel":"slack","status":"sent","response_detail":"accepted",'
            '"event_name":"DP_VM_STALL","alert_class":"DP_VM_STALL","severity":"warning",'
            '"message":"VM stalled","service":"alerting-service","deployment_target":"cefi-aster-1",'
            '"timestamp":"2026-07-21T12:00:00Z"}'
        )
        parsed = _parse_alerting_service_line(line)
        assert parsed is not None
        assert parsed["kind"] == "DP_VM_STALL"
        assert parsed["workflow_name"] == "DP_VM_STALL"
        assert parsed["repo"] == ""
        assert parsed["severity"] == "warning"
        assert parsed["message"] == "VM stalled"
        assert parsed["alert_class"] == "DP_VM_STALL"
        assert parsed["deployment_target"] == "cefi-aster-1"

    def test_decision_record_shape_uses_triggered_at(self) -> None:
        line = (
            '{"alert_id":"a2","rule_id":"margin_breach","triggered_at":"2026-07-21T11:00:00Z",'
            '"severity":"CRITICAL","alert_class":"MARGIN_BREACH","message":"margin low",'
            '"metric_value":0.1,"threshold":0.2}'
        )
        parsed = _parse_alerting_service_line(line)
        assert parsed is not None
        assert parsed["timestamp"] == "2026-07-21T11:00:00Z"
        assert parsed["alert_class"] == "MARGIN_BREACH"
        assert parsed.get("deployment_target") is None

    def test_missing_timestamp_skipped(self) -> None:
        assert _parse_alerting_service_line('{"alert_class":"X"}') is None

    def test_junk_lines_skipped(self) -> None:
        assert _parse_alerting_service_line("not json") is None
        assert _parse_alerting_service_line("[1,2]") is None


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


class TestReadAlertingServiceSync:
    def test_merges_history_blobs_into_entries(self) -> None:
        from unittest.mock import MagicMock

        from deployment_api.routes._repo_ci_alerts import _read_alerting_service_sync

        blob = MagicMock()
        blob.name = "alerting/history/date=2026-07-21/abc123.jsonl"
        mock_client = MagicMock()
        mock_client.list_blobs.return_value = [blob]
        line = (
            b'{"alert_id":"a1","event_name":"CONSOLIDATOR_DOWN","alert_class":"CONSOLIDATOR_DOWN",'
            b'"severity":"critical","message":"consolidator down","timestamp":"2026-07-21T09:00:00Z"}\n'
        )
        with (
            patch("deployment_api.routes._repo_ci_alerts.resolve_bucket_name", return_value="alerting-service-test"),
            patch("deployment_api.routes._repo_ci_alerts.get_storage_client", return_value=mock_client),
            patch("deployment_api.routes._repo_ci_alerts.download_from_storage", return_value=line),
        ):
            entries = _read_alerting_service_sync(days=1)
        assert len(entries) == 1
        assert entries[0]["alert_class"] == "CONSOLIDATOR_DOWN"
        mock_client.list_blobs.assert_called_once_with(
            "alerting-service-test", prefix="alerting/history/date=2026-07-21/"
        )

    def test_bucket_resolution_failure_returns_empty(self) -> None:
        from deployment_api.routes._repo_ci_alerts import _read_alerting_service_sync

        with patch(
            "deployment_api.routes._repo_ci_alerts.resolve_bucket_name",
            side_effect=RuntimeError("no yaml"),
        ):
            assert _read_alerting_service_sync(days=1) == []


class TestReadLedgersSync:
    """deployment_alerts_ingestion_completeness_2026_07_20.md todo 5 (QG 5.69): the CI-alerts
    bucket resolves via resolve_bucket_name(), never a hardcoded literal."""

    def test_resolves_bucket_and_reads_ledgers(self) -> None:
        from unittest.mock import MagicMock

        from deployment_api.routes._repo_ci_alerts import _read_ledgers_sync

        alert_blob = MagicMock()
        alert_blob.name = "cicd/alerts/2026-07-21/alerts.jsonl"
        mock_client = MagicMock()
        mock_client.list_blobs.return_value = [alert_blob]
        line = (
            b'{"event_type":"slack_alert","timestamp":"2026-07-21T09:00:00Z","repo":"deployment-api",'
            b'"workflow_name":"vm-health-x","severity":"CRITICAL","message":"m"}\n'
        )
        with (
            patch(
                "deployment_api.routes._repo_ci_alerts.resolve_bucket_name",
                return_value="unified-trading-cicd-events",
            ),
            patch("deployment_api.routes._repo_ci_alerts.get_storage_client", return_value=mock_client),
            patch("deployment_api.routes._repo_ci_alerts.download_from_storage", return_value=line),
            patch("deployment_api.routes._repo_ci_alerts._read_alerting_service_sync", return_value=[]),
        ):
            entries = _read_ledgers_sync(days=1)
        assert any(e["workflow_name"] == "vm-health-x" for e in entries)
        mock_client.list_blobs.assert_any_call("unified-trading-cicd-events", prefix="cicd/alerts/2026-07-21/")

    def test_bucket_resolution_failure_degrades_to_alerting_service_only(self) -> None:
        """A cicd-events resolution failure must not blank the alerting-service merge —
        best-effort degradation, matching the pattern _read_alerting_service_sync already uses."""
        from deployment_api.routes._repo_ci_alerts import AlertEntryDict, _read_ledgers_sync

        fallback_entry = AlertEntryDict(
            kind="alert",
            timestamp="2026-07-21T09:00:00Z",
            repo="",
            workflow_name="CONSOLIDATOR_DOWN",
            severity="CRITICAL",
            conclusion=None,
            message="m",
            run_url=None,
            alert_class="CONSOLIDATOR_DOWN",
        )
        with (
            patch(
                "deployment_api.routes._repo_ci_alerts.resolve_bucket_name",
                side_effect=BucketNamingError("no yaml"),
            ),
            patch(
                "deployment_api.routes._repo_ci_alerts._read_alerting_service_sync",
                return_value=[fallback_entry],
            ),
        ):
            entries = _read_ledgers_sync(days=1)
        assert entries == [fallback_entry]


class TestLoadAlertsPayloadPagination:
    """deployment_alerts_ingestion_completeness_2026_07_20.md todo 8: the old `_DEFAULT_DAYS = 2`
    / `_MAX_ITEMS = 400` couldn't support a real date-range filter, and a page past 400 rows was
    silently truncated. Now: a wider default window + explicit offset/limit pagination + a
    `capped` signal instead of a silent short list."""

    @pytest.fixture(autouse=True)
    def _clear_entries_cache(self) -> None:
        # _entries_cache is keyed by `days` and shared module state — every test here uses
        # days=1, so without a reset each test after the first would silently reuse the prior
        # test's patched entries instead of exercising its own patch.
        from deployment_api.routes import _repo_ci_alerts

        _repo_ci_alerts._entries_cache.clear()  # pyright: ignore[reportPrivateUsage]

    @staticmethod
    def _alert_entries(n: int) -> list[AlertEntryDict]:
        return [
            AlertEntryDict(
                kind="alert",
                timestamp=f"2026-07-{(i % 28) + 1:02d}T00:00:00Z",
                repo="r",
                workflow_name=f"wf-{i}",
                severity="INFO",
                conclusion=None,
                message="m",
                run_url=None,
            )
            for i in range(n)
        ]

    @pytest.mark.asyncio
    async def test_capped_true_when_more_rows_than_limit(self) -> None:
        from deployment_api.routes._repo_ci_alerts import load_alerts_payload

        entries = self._alert_entries(10)
        with patch(
            "deployment_api.routes._repo_ci_alerts._read_ledgers_sync",
            return_value=entries,
        ):
            payload = await load_alerts_payload(days=1, offset=0, limit=4)
        assert payload["total_count"] == 10
        assert payload["returned_count"] == 4
        assert payload["capped"] is True

    @pytest.mark.asyncio
    async def test_capped_false_when_all_rows_fit(self) -> None:
        from deployment_api.routes._repo_ci_alerts import load_alerts_payload

        entries = self._alert_entries(3)
        with patch(
            "deployment_api.routes._repo_ci_alerts._read_ledgers_sync",
            return_value=entries,
        ):
            payload = await load_alerts_payload(days=1, offset=0, limit=400)
        assert payload["total_count"] == 3
        assert payload["returned_count"] == 3
        assert payload["capped"] is False

    @pytest.mark.asyncio
    async def test_offset_advances_the_page(self) -> None:
        from deployment_api.routes._repo_ci_alerts import load_alerts_payload

        entries = self._alert_entries(10)
        with patch(
            "deployment_api.routes._repo_ci_alerts._read_ledgers_sync",
            return_value=entries,
        ):
            page1 = await load_alerts_payload(days=1, offset=0, limit=4)
            page2 = await load_alerts_payload(days=1, offset=4, limit=4)
        assert [a["workflow_name"] for a in page1["alerts"]] != [a["workflow_name"] for a in page2["alerts"]]
        assert page2["offset"] == 4
        assert page2["capped"] is True  # 10 total, offset 4 + 4 returned = 8 < 10

    @pytest.mark.asyncio
    async def test_days_clamped_to_max(self) -> None:
        from deployment_api.routes._repo_ci_alerts import _MAX_DAYS, load_alerts_payload

        with patch(
            "deployment_api.routes._repo_ci_alerts._read_ledgers_sync",
            return_value=[],
        ) as mock_read:
            payload = await load_alerts_payload(days=9999, offset=0, limit=10)
        assert payload["days"] == _MAX_DAYS
        mock_read.assert_called_once_with(_MAX_DAYS)

    @pytest.mark.asyncio
    async def test_streams_derive_from_full_window_not_just_the_page(self) -> None:
        """A stream whose only entry falls outside the requested page must still surface —
        streams are a bounded (repo, workflow) roll-up, not a row-count-bounded list."""
        from deployment_api.routes._repo_ci_alerts import load_alerts_payload

        entries = self._alert_entries(10)
        with patch(
            "deployment_api.routes._repo_ci_alerts._read_ledgers_sync",
            return_value=entries,
        ):
            payload = await load_alerts_payload(days=1, offset=0, limit=2)
        assert len(payload["alerts"]) == 2
        assert len(payload["streams"]) == 10  # every (repo, workflow) pair still represented


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

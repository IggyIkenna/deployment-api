"""Unit tests for Phase C monitor routes: backfill, experiments, live, scheduled.

S3 of deployment_ui_lifecycle_tabs_2026_05_08.md sustain queue.
"""

from __future__ import annotations

from unittest.mock import MagicMock, patch

import pytest
from fastapi import FastAPI
from fastapi.testclient import TestClient
from unified_trading_library import setup_events

from deployment_api.routes.monitor_backfill import router as backfill_router
from deployment_api.routes.monitor_experiments import router as experiments_router
from deployment_api.routes.monitor_live import router as live_router
from deployment_api.routes.monitor_scheduled import router as scheduled_router

setup_events("deployment-api", "test")

_HEADERS = {"X-API-Key": "test-key"}

_REG_BACKFILL = "deployment_api.routes.monitor_backfill.DeploymentsRegistry"
_REG_EXPERIMENTS = "deployment_api.routes.monitor_experiments.DeploymentsRegistry"
_REG_LIVE = "deployment_api.routes.monitor_live.DeploymentsRegistry"
_REG_SCHEDULED = "deployment_api.routes.monitor_scheduled.DeploymentsRegistry"


def _make_entry(**kwargs: object) -> MagicMock:
    """Build a minimal DeploymentRegistryEntry-like mock."""
    entry = MagicMock()
    entry.deployment_id = kwargs.get("deployment_id", "dep-001")
    entry.vm_name = kwargs.get("vm_name", "instruments-backfill-20260518")
    entry.asset_group = kwargs.get("asset_group", "cefi")
    entry.task = kwargs.get("task", "backfill")
    entry.mode = kwargs.get("mode", "batch")
    entry.start_date = kwargs.get("start_date", "2026-05-01")
    entry.end_date = kwargs.get("end_date", "2026-05-10")
    entry.status = kwargs.get("status", "running")
    entry.started_at = kwargs.get("started_at", "2026-05-18T10:00:00Z")
    entry.last_heartbeat_at = kwargs.get("last_heartbeat_at", "2026-05-18T10:01:00Z")
    entry.completed_at = kwargs.get("completed_at")
    entry.exit_code = kwargs.get("exit_code")
    entry.rows_in = kwargs.get("rows_in", 100)
    entry.rows_out = kwargs.get("rows_out", 99)
    entry.rows_error = kwargs.get("rows_error", 1)
    entry.events_emitted = kwargs.get("events_emitted", 5)
    entry.log_uri = kwargs.get("log_uri", "gs://logs/dep-001.log")
    return entry


@pytest.fixture(scope="module")
def backfill_client() -> TestClient:
    app = FastAPI()
    app.include_router(backfill_router)
    return TestClient(app)


@pytest.fixture(scope="module")
def experiments_client() -> TestClient:
    app = FastAPI()
    app.include_router(experiments_router)
    return TestClient(app)


@pytest.fixture(scope="module")
def live_client() -> TestClient:
    app = FastAPI()
    app.include_router(live_router)
    return TestClient(app)


@pytest.fixture(scope="module")
def scheduled_client() -> TestClient:
    app = FastAPI()
    app.include_router(scheduled_router)
    return TestClient(app)


class TestMonitorBackfill:
    def test_empty_registry_returns_200(self, backfill_client: TestClient) -> None:
        with patch(_REG_BACKFILL) as mock_cls:
            mock_reg = MagicMock()
            mock_reg.list_active.return_value = []
            mock_reg.list_recent_archive.return_value = []
            mock_cls.return_value = mock_reg
            r = backfill_client.get("/monitor/backfill")
        assert r.status_code == 200
        body = r.json()
        assert body["jobs"] == []
        assert body["total"] == 0
        assert "queried_at" in body
        assert "env" in body

    def test_backfill_vm_included(self, backfill_client: TestClient) -> None:
        entry = _make_entry(vm_name="instruments-backfill-cefi-20260518", status="running")
        with patch(_REG_BACKFILL) as mock_cls:
            mock_reg = MagicMock()
            mock_reg.list_active.return_value = [entry]
            mock_reg.list_recent_archive.return_value = []
            mock_cls.return_value = mock_reg
            r = backfill_client.get("/monitor/backfill")
        assert r.status_code == 200
        body = r.json()
        assert body["total"] == 1
        assert body["jobs"][0]["vm_name"] == "instruments-backfill-cefi-20260518"
        assert body["jobs"][0]["lifecycle_class"] == "EPHEMERAL_BATCH"

    def test_live_vm_excluded(self, backfill_client: TestClient) -> None:
        live = _make_entry(vm_name="strategy-live-defi-20260518")
        batch = _make_entry(vm_name="mtds-backfill-cefi-20260518", deployment_id="dep-002")
        with patch(_REG_BACKFILL) as mock_cls:
            mock_reg = MagicMock()
            mock_reg.list_active.return_value = [live, batch]
            mock_reg.list_recent_archive.return_value = []
            mock_cls.return_value = mock_reg
            r = backfill_client.get("/monitor/backfill")
        body = r.json()
        assert body["total"] == 1
        assert body["jobs"][0]["vm_name"] == "mtds-backfill-cefi-20260518"

    def test_status_filter(self, backfill_client: TestClient) -> None:
        running = _make_entry(vm_name="backfill-a-20260518", status="running")
        done = _make_entry(vm_name="backfill-b-20260518", status="completed", deployment_id="dep-b")
        with patch(_REG_BACKFILL) as mock_cls:
            mock_reg = MagicMock()
            mock_reg.list_active.return_value = [running, done]
            mock_reg.list_recent_archive.return_value = []
            mock_cls.return_value = mock_reg
            r = backfill_client.get("/monitor/backfill?status=running")
        body = r.json()
        assert body["total"] == 1
        assert body["jobs"][0]["status"] == "running"

    def test_cloud_param_reflected(self, backfill_client: TestClient) -> None:
        with patch(_REG_BACKFILL) as mock_cls:
            mock_reg = MagicMock()
            mock_reg.list_active.return_value = []
            mock_reg.list_recent_archive.return_value = []
            mock_cls.return_value = mock_reg
            r = backfill_client.get("/monitor/backfill?cloud=aws")
        assert r.json()["cloud"] == "aws"

    def test_registry_error_returns_empty(self, backfill_client: TestClient) -> None:
        with patch(_REG_BACKFILL, side_effect=RuntimeError("GCS unavailable")):
            r = backfill_client.get("/monitor/backfill")
        assert r.status_code == 200
        assert r.json()["total"] == 0

    def test_limit_param(self, backfill_client: TestClient) -> None:
        entries = [_make_entry(vm_name=f"backfill-{i}", deployment_id=f"dep-{i}") for i in range(10)]
        with patch(_REG_BACKFILL) as mock_cls:
            mock_reg = MagicMock()
            mock_reg.list_active.return_value = entries
            mock_reg.list_recent_archive.return_value = []
            mock_cls.return_value = mock_reg
            r = backfill_client.get("/monitor/backfill?limit=3")
        assert r.json()["total"] == 3


class TestMonitorExperiments:
    def test_empty_registry_returns_200(self, experiments_client: TestClient) -> None:
        with patch(_REG_EXPERIMENTS) as mock_cls:
            mock_reg = MagicMock()
            mock_reg.list_active.return_value = []
            mock_reg.list_recent_archive.return_value = []
            mock_cls.return_value = mock_reg
            r = experiments_client.get("/monitor/experiments")
        assert r.status_code == 200
        body = r.json()
        assert body["jobs"] == []
        assert "env" in body

    def test_strategy_backtest_included(self, experiments_client: TestClient) -> None:
        entry = _make_entry(vm_name="strategy-backtest-cefi-run42", status="running")
        with patch(_REG_EXPERIMENTS) as mock_cls:
            mock_reg = MagicMock()
            mock_reg.list_active.return_value = [entry]
            mock_reg.list_recent_archive.return_value = []
            mock_cls.return_value = mock_reg
            r = experiments_client.get("/monitor/experiments")
        body = r.json()
        assert body["total"] == 1
        j = body["jobs"][0]
        assert j["experiment_kind"] == "strategy_backtest"
        assert j["lifecycle_class"] == "EPHEMERAL_EXPERIMENT"

    def test_ml_train_included(self, experiments_client: TestClient) -> None:
        entry = _make_entry(vm_name="ml-train-defi-runXYZ", deployment_id="dep-ml")
        with patch(_REG_EXPERIMENTS) as mock_cls:
            mock_reg = MagicMock()
            mock_reg.list_active.return_value = [entry]
            mock_reg.list_recent_archive.return_value = []
            mock_cls.return_value = mock_reg
            r = experiments_client.get("/monitor/experiments")
        body = r.json()
        assert body["jobs"][0]["experiment_kind"] == "ml_training"

    def test_execution_backtest_included(self, experiments_client: TestClient) -> None:
        entry = _make_entry(vm_name="execution-backtest-cefi-run1", deployment_id="dep-eb")
        with patch(_REG_EXPERIMENTS) as mock_cls:
            mock_reg = MagicMock()
            mock_reg.list_active.return_value = [entry]
            mock_reg.list_recent_archive.return_value = []
            mock_cls.return_value = mock_reg
            r = experiments_client.get("/monitor/experiments")
        body = r.json()
        assert body["jobs"][0]["experiment_kind"] == "execution_backtest"

    def test_live_vm_excluded(self, experiments_client: TestClient) -> None:
        live = _make_entry(vm_name="strategy-live-defi-20260518")
        exp = _make_entry(vm_name="ml-train-defi-run1", deployment_id="dep-2")
        with patch(_REG_EXPERIMENTS) as mock_cls:
            mock_reg = MagicMock()
            mock_reg.list_active.return_value = [live, exp]
            mock_reg.list_recent_archive.return_value = []
            mock_cls.return_value = mock_reg
            r = experiments_client.get("/monitor/experiments")
        body = r.json()
        assert body["total"] == 1
        assert body["jobs"][0]["vm_name"] == "ml-train-defi-run1"

    def test_registry_error_returns_empty(self, experiments_client: TestClient) -> None:
        with patch(_REG_EXPERIMENTS, side_effect=RuntimeError("down")):
            r = experiments_client.get("/monitor/experiments")
        assert r.status_code == 200
        assert r.json()["total"] == 0


class TestMonitorLive:
    def test_empty_registry_returns_200(self, live_client: TestClient) -> None:
        with patch(_REG_LIVE) as mock_cls:
            mock_reg = MagicMock()
            mock_reg.list_active.return_value = []
            mock_reg.list_recent_archive.return_value = []
            mock_cls.return_value = mock_reg
            r = live_client.get("/monitor/live")
        assert r.status_code == 200
        body = r.json()
        assert body["clusters"] == []
        assert "env" in body

    def test_paper_trade_included(self, live_client: TestClient) -> None:
        entry = _make_entry(vm_name="strategy-paper-carry-20260518", status="running")
        with patch(_REG_LIVE) as mock_cls:
            mock_reg = MagicMock()
            mock_reg.list_active.return_value = [entry]
            mock_reg.list_recent_archive.return_value = []
            mock_cls.return_value = mock_reg
            r = live_client.get("/monitor/live")
        body = r.json()
        assert body["total"] == 1
        c = body["clusters"][0]
        assert c["deployment_kind"] == "paper_trade"
        assert c["lifecycle_class"] == "LONG_LIVED_LIVE"

    def test_live_trade_included(self, live_client: TestClient) -> None:
        entry = _make_entry(vm_name="strategy-live-defi-20260518", deployment_id="dep-live")
        with patch(_REG_LIVE) as mock_cls:
            mock_reg = MagicMock()
            mock_reg.list_active.return_value = [entry]
            mock_reg.list_recent_archive.return_value = []
            mock_cls.return_value = mock_reg
            r = live_client.get("/monitor/live")
        body = r.json()
        assert body["clusters"][0]["deployment_kind"] == "live_trade"

    def test_defi_recursive_included(self, live_client: TestClient) -> None:
        entry = _make_entry(vm_name="defi-recursive-aave-20260518", deployment_id="dep-defi")
        with patch(_REG_LIVE) as mock_cls:
            mock_reg = MagicMock()
            mock_reg.list_active.return_value = [entry]
            mock_reg.list_recent_archive.return_value = []
            mock_cls.return_value = mock_reg
            r = live_client.get("/monitor/live")
        body = r.json()
        assert body["clusters"][0]["deployment_kind"] == "defi_recursive_borrow"

    def test_batch_vm_excluded(self, live_client: TestClient) -> None:
        batch = _make_entry(vm_name="instruments-backfill-cefi")
        live = _make_entry(vm_name="strategy-paper-cefi-20260518", deployment_id="dep-l")
        with patch(_REG_LIVE) as mock_cls:
            mock_reg = MagicMock()
            mock_reg.list_active.return_value = [batch, live]
            mock_reg.list_recent_archive.return_value = []
            mock_cls.return_value = mock_reg
            r = live_client.get("/monitor/live")
        body = r.json()
        assert body["total"] == 1
        assert body["clusters"][0]["vm_name"] == "strategy-paper-cefi-20260518"

    def test_deduplication(self, live_client: TestClient) -> None:
        e1 = _make_entry(vm_name="strategy-live-defi-20260518", deployment_id="same-id")
        e2 = _make_entry(vm_name="strategy-live-defi-20260518", deployment_id="same-id")
        with patch(_REG_LIVE) as mock_cls:
            mock_reg = MagicMock()
            mock_reg.list_active.return_value = [e1]
            mock_reg.list_recent_archive.return_value = [e2]
            mock_cls.return_value = mock_reg
            r = live_client.get("/monitor/live")
        body = r.json()
        assert body["total"] == 1

    def test_registry_error_returns_empty(self, live_client: TestClient) -> None:
        with patch(_REG_LIVE, side_effect=RuntimeError("gcs down")):
            r = live_client.get("/monitor/live")
        assert r.status_code == 200
        assert r.json()["total"] == 0


class TestMonitorScheduled:
    def test_empty_registry_returns_200(self, scheduled_client: TestClient) -> None:
        with patch(_REG_SCHEDULED) as mock_cls:
            mock_reg = MagicMock()
            mock_reg.list_active.return_value = []
            mock_reg.list_recent_archive.return_value = []
            mock_cls.return_value = mock_reg
            r = scheduled_client.get("/monitor/scheduled")
        assert r.status_code == 200
        body = r.json()
        assert body["jobs"] == []
        assert body["phase_d_registry_available"] is True
        assert "env" in body

    def test_cron_vm_included(self, scheduled_client: TestClient) -> None:
        entry = _make_entry(vm_name="cron-qg-snapshot-20260518", status="running")
        with patch(_REG_SCHEDULED) as mock_cls:
            mock_reg = MagicMock()
            mock_reg.list_active.return_value = [entry]
            mock_reg.list_recent_archive.return_value = []
            mock_cls.return_value = mock_reg
            r = scheduled_client.get("/monitor/scheduled")
        body = r.json()
        assert body["total"] == 1
        j = body["jobs"][0]
        assert j["source"] == "registry_fallback"
        assert j["lifecycle_class"] == "SCHEDULED_RECURRING"

    def test_honest_coverage_included(self, scheduled_client: TestClient) -> None:
        entry = _make_entry(vm_name="honest-coverage-cefi-20260518", deployment_id="hc-1")
        with patch(_REG_SCHEDULED) as mock_cls:
            mock_reg = MagicMock()
            mock_reg.list_active.return_value = [entry]
            mock_reg.list_recent_archive.return_value = []
            mock_cls.return_value = mock_reg
            r = scheduled_client.get("/monitor/scheduled")
        body = r.json()
        assert body["total"] == 1

    def test_non_scheduled_vm_excluded(self, scheduled_client: TestClient) -> None:
        batch = _make_entry(vm_name="instruments-backfill-cefi")
        sched = _make_entry(vm_name="qg-snapshot-20260518", deployment_id="qs-1")
        with patch(_REG_SCHEDULED) as mock_cls:
            mock_reg = MagicMock()
            mock_reg.list_active.return_value = [batch, sched]
            mock_reg.list_recent_archive.return_value = []
            mock_cls.return_value = mock_reg
            r = scheduled_client.get("/monitor/scheduled")
        body = r.json()
        assert body["total"] == 1
        assert body["jobs"][0]["name"] == "qg-snapshot-20260518"

    def test_phase_d_registry_available_true(self, scheduled_client: TestClient) -> None:
        with patch(_REG_SCHEDULED) as mock_cls:
            mock_reg = MagicMock()
            mock_reg.list_active.return_value = []
            mock_reg.list_recent_archive.return_value = []
            mock_cls.return_value = mock_reg
            r = scheduled_client.get("/monitor/scheduled?cloud=aws")
        body = r.json()
        assert body["phase_d_registry_available"] is True
        assert body["cloud"] == "aws"

    def test_registry_error_returns_empty(self, scheduled_client: TestClient) -> None:
        with patch(_REG_SCHEDULED, side_effect=RuntimeError("scheduler api down")):
            r = scheduled_client.get("/monitor/scheduled")
        assert r.status_code == 200
        assert r.json()["total"] == 0

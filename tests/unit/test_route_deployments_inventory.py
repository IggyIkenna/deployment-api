"""Unit tests for routes/deployments_inventory.py — the unified deployment inventory.

Credential-free / --block-network safe: the deployment registry, the Cloud Run
executions client, and the GCE compute API are ALL mocked. Pins the API side of the
deployment-ui Deployments page contract (umbrella tabs + per-target rows + summary).

Verifies: VMs + Cloud Run jobs classified into one umbrella; umbrella/cloud filters;
an exit-137 VM surfaces status=failed + exit_code=137; the per-umbrella summary rolls
up counts/stale/last-failure correctly.
"""

from __future__ import annotations

import json
import os
import sys
from dataclasses import dataclass, field
from datetime import UTC, datetime
from types import ModuleType
from unittest.mock import patch

import pytest
from fastapi import FastAPI
from fastapi.testclient import TestClient

os.environ.setdefault("CLOUD_MOCK_MODE", "false")
os.environ.setdefault("CLOUD_PROVIDER", "local")
os.environ.setdefault("GCP_PROJECT_ID", "test-project")
os.environ.setdefault("DISABLE_AUTH", "true")
os.environ.setdefault("MOCK_STATE_MODE", "deterministic")

pytestmark = [pytest.mark.timeout(60)]

_FIXED_NOW = datetime(2026, 6, 22, 12, 0, 0, tzinfo=UTC)


@dataclass
class _FakeEntry:
    """Minimal stand-in for DeploymentRegistryEntry (the fields the inventory reads)."""

    vm_name: str
    asset_group: str = "cefi"
    status: str = "running"
    started_at: str = "2026-06-22T11:00:00Z"
    last_heartbeat_at: str = "2026-06-22T11:59:30Z"  # 30s ago vs _FIXED_NOW
    completed_at: str | None = None
    exit_code: int | None = None
    rows_out: int = 0
    extras: dict[str, str] = field(default_factory=dict)


@pytest.fixture
def client_inventory() -> TestClient:
    from deployment_api.routes.deployments_inventory import router

    app = FastAPI()
    app.include_router(router, prefix="/api")
    return TestClient(app, raise_server_exceptions=False)


# ---------------------------------------------------------------------------
# build_inventory — pure classification + status (no HTTP)
# ---------------------------------------------------------------------------


def _vm_entries() -> list[_FakeEntry]:
    return [
        # Running batch backfill (unknown prefix → EPHEMERAL_BATCH → BATCH umbrella).
        _FakeEntry(vm_name="cefi-binance-spot-20260622-014158", asset_group="cefi", rows_out=11_987),
        # Exit-137 OOM-killed backfill → failed + exit_code 137.
        _FakeEntry(
            vm_name="defi-backfill-20260622-014200",
            asset_group="defi",
            status="failed",
            completed_at="2026-06-22T03:00:00Z",
            exit_code=137,
            rows_out=0,
        ),
        # Long-lived live strategy VM (strategy-live- prefix → LONG_LIVED_LIVE → LIVE).
        _FakeEntry(vm_name="strategy-live-cefi-20260620", asset_group="cefi"),
        # Paper-trading VM (defi-paper- prefix → PAPER umbrella override).
        _FakeEntry(vm_name="defi-paper-trading-20260622", asset_group="defi"),
    ]


def test_build_inventory_classifies_vms_and_jobs() -> None:
    from deployment_api.routes._cloud_run_executions import CloudRunExecutionStatus
    from deployment_api.routes.deployments_inventory import build_inventory

    cr_status = {
        "prd-manifest-consolidator-cefi": CloudRunExecutionStatus(
            job_name="prd-manifest-consolidator-cefi",
            status="succeeded",
            last_run_at="2026-06-22T06:00:00Z",
            exit_code=0,
            log_uri="https://logs",
        )
    }
    items = build_inventory(_vm_entries(), cr_status, _FIXED_NOW)  # type: ignore[arg-type]
    by_name = {i.name: i for i in items}

    # All 4 VMs + exactly the LIVE Cloud Run jobs (dynamic census — one row per
    # live job in cr_status, NOT the full 61-entry static registry).
    vm_items = [i for i in items if i.kind == "VM"]
    job_items = [i for i in items if i.kind == "CLOUD_RUN_JOB"]
    assert len(vm_items) == 4
    assert len(job_items) == len(cr_status)

    # The live job binds to its registry stem's classification + status.
    consolidator = by_name["prd-manifest-consolidator-cefi"]
    assert consolidator.service == "manifest-consolidator"
    assert consolidator.asset_group == "cefi"
    assert consolidator.umbrella == "BATCH"
    assert consolidator.status == "succeeded"

    # Umbrella classification.
    assert by_name["cefi-binance-spot-20260622-014158"].umbrella == "BATCH"
    assert by_name["strategy-live-cefi-20260620"].umbrella == "LIVE"
    assert by_name["defi-paper-trading-20260622"].umbrella == "PAPER"

    # Exit-137 OOM VM → failed + exit_code 137.
    oom = by_name["defi-backfill-20260622-014200"]
    assert oom.status == "failed"
    assert oom.exit_code == 137
    assert oom.umbrella == "BATCH"

    # Running VM heartbeat 30s ago → running (not stale), age surfaced.
    running = by_name["cefi-binance-spot-20260622-014158"]
    assert running.status == "running"
    assert running.heartbeat_age_seconds == 30
    assert running.captured_progress == 11_987

    # Every item is GCP and carries exactly one umbrella.
    assert all(i.cloud == "GCP" for i in items)
    assert all(i.umbrella in {"LIVE", "BATCH", "PAPER", "EXPERIMENT"} for i in items)


def test_build_inventory_stale_running_vm() -> None:
    from deployment_api.routes.deployments_inventory import build_inventory

    # Heartbeat 20 min ago (> 15-min window) on a running VM → stale.
    stale = _FakeEntry(
        vm_name="cefi-okx-spot-20260622",
        status="running",
        last_heartbeat_at="2026-06-22T11:40:00Z",
    )
    items = build_inventory([stale], {}, _FIXED_NOW)  # type: ignore[arg-type]
    vm = next(i for i in items if i.name == "cefi-okx-spot-20260622")
    assert vm.status == "stale"
    assert vm.heartbeat_age_seconds == 1_200


def test_off_pattern_live_cloud_run_job_is_not_hidden() -> None:
    """The registry is a classification HINT, not an allow-list — a live job with
    no matching registry stem must still surface a row (the census bug this task
    fixes), classified via the honest BATCH default rather than dropped.
    """
    from deployment_api.routes._cloud_run_executions import CloudRunExecutionStatus
    from deployment_api.routes.deployments_inventory import build_inventory

    cr_status = {
        "prd-oddspapi-sports-scraper": CloudRunExecutionStatus(
            job_name="prd-oddspapi-sports-scraper",
            status="succeeded",
            last_run_at="2026-07-09T06:00:00Z",
            exit_code=0,
            log_uri="",
        )
    }
    items = build_inventory([], cr_status, _FIXED_NOW)
    assert len(items) == 1
    off_pattern = items[0]
    assert off_pattern.name == "prd-oddspapi-sports-scraper"
    assert off_pattern.kind == "CLOUD_RUN_JOB"
    assert off_pattern.umbrella == "BATCH"
    assert off_pattern.status == "succeeded"


def test_cloud_run_job_without_live_status_is_unknown() -> None:
    from deployment_api.routes.deployments_inventory import build_inventory

    # No Cloud Run execution status → jobs degrade to status="unknown", never crash.
    items = build_inventory([], {}, _FIXED_NOW)
    jobs = [i for i in items if i.kind == "CLOUD_RUN_JOB"]
    assert jobs
    assert all(j.status == "unknown" for j in jobs)


# ---------------------------------------------------------------------------
# build_umbrella_summary — rollup
# ---------------------------------------------------------------------------


def test_build_umbrella_summary_rolls_up() -> None:
    from deployment_api.routes._cloud_run_executions import CloudRunExecutionStatus
    from deployment_api.routes.deployments_inventory import build_inventory, build_umbrella_summary

    cr_status = {
        "prd-manifest-consolidator-cefi": CloudRunExecutionStatus(
            job_name="prd-manifest-consolidator-cefi",
            status="failed",
            last_run_at="2026-06-22T06:00:00Z",
            exit_code=1,
            log_uri="",
        )
    }
    items = build_inventory(_vm_entries(), cr_status, _FIXED_NOW)  # type: ignore[arg-type]
    summary = build_umbrella_summary("batch", items)

    assert summary.umbrella == "BATCH"
    # BATCH = the 2 batch VMs + every Cloud Run job (all BATCH/PAPER; PAPER excluded).
    assert summary.total >= 2
    # The OOM VM (failed) is counted.
    assert summary.counts_by_status.get("failed", 0) >= 1
    # last_failure surfaces a failed target with its exit code.
    assert summary.last_failure is not None
    assert summary.last_failure.exit_code in (1, 137)


def test_paper_summary_isolates_paper_targets() -> None:
    from deployment_api.routes.deployments_inventory import build_inventory, build_umbrella_summary

    items = build_inventory(_vm_entries(), {}, _FIXED_NOW)  # type: ignore[arg-type]
    summary = build_umbrella_summary("paper", items)
    assert summary.umbrella == "PAPER"
    # The defi-paper-trading VM + the PAPER Cloud Run jobs (paper-engine / determinism /
    # ledger-digest). The VM is present and every scoped item is PAPER.
    paper_items = [i for i in items if i.umbrella == "PAPER"]
    assert summary.total == len(paper_items)
    assert any(i.name == "defi-paper-trading-20260622" and i.kind == "VM" for i in paper_items)
    assert any(i.kind == "CLOUD_RUN_JOB" for i in paper_items)


# ---------------------------------------------------------------------------
# Routes — mock mode + filters + summary 404
# ---------------------------------------------------------------------------


def test_inventory_route_mock_shape(client_inventory: TestClient) -> None:
    with patch("deployment_api.routes.deployments_inventory._cfg") as mock_cfg:
        mock_cfg.is_mock_mode.return_value = True
        resp = client_inventory.get("/api/deployments/inventory")
    assert resp.status_code == 200
    body = resp.json()
    assert body["total"] == len(body["items"])
    umbrellas = {i["umbrella"] for i in body["items"]}
    assert {"LIVE", "BATCH", "PAPER"} <= umbrellas
    # The exit-137 mock OOM VM is present.
    oom = [i for i in body["items"] if i["exit_code"] == 137]
    assert oom and oom[0]["status"] == "failed"


def test_inventory_route_umbrella_filter(client_inventory: TestClient) -> None:
    with patch("deployment_api.routes.deployments_inventory._cfg") as mock_cfg:
        mock_cfg.is_mock_mode.return_value = True
        resp = client_inventory.get("/api/deployments/inventory", params={"umbrella": "live"})
    assert resp.status_code == 200
    body = resp.json()
    assert body["total"] >= 1
    assert all(i["umbrella"] == "LIVE" for i in body["items"])


def test_inventory_route_cloud_filter_aws(client_inventory: TestClient) -> None:
    """AWS parity (Phase 5) — cloud=aws returns the AWS items, all cloud=AWS."""
    with patch("deployment_api.routes.deployments_inventory._cfg") as mock_cfg:
        mock_cfg.is_mock_mode.return_value = True
        resp = client_inventory.get("/api/deployments/inventory", params={"cloud": "aws"})
    assert resp.status_code == 200
    body = resp.json()
    assert body["total"] >= 1
    assert all(i["cloud"] == "AWS" for i in body["items"])
    # An AWS EC2 backfill VM + a Batch job are present (the mock AWS estate).
    kinds = {i["kind"] for i in body["items"]}
    assert "VM" in kinds and "CLOUD_RUN_JOB" in kinds


def test_summary_route_mock(client_inventory: TestClient) -> None:
    with patch("deployment_api.routes.deployments_inventory._cfg") as mock_cfg:
        mock_cfg.is_mock_mode.return_value = True
        resp = client_inventory.get("/api/deployments/umbrella/batch/summary")
    assert resp.status_code == 200
    body = resp.json()
    assert body["umbrella"] == "BATCH"
    assert body["total"] >= 1
    assert "counts_by_status" in body
    # The mock OOM VM → a batch failure in the rollup.
    assert body["last_failure"] is not None
    assert body["last_failure"]["exit_code"] == 137


def test_summary_route_unknown_umbrella_404(client_inventory: TestClient) -> None:
    with patch("deployment_api.routes.deployments_inventory._cfg") as mock_cfg:
        mock_cfg.is_mock_mode.return_value = True
        resp = client_inventory.get("/api/deployments/umbrella/bogus/summary")
    assert resp.status_code == 404


def test_inventory_route_live_path_mocks_registry_and_cloud_run(client_inventory: TestClient) -> None:
    """Non-mock path reads the registry + Cloud Run executions (both mocked)."""
    from deployment_api.routes._cloud_run_executions import CloudRunExecutionStatus

    entries = _vm_entries()
    cr_status = {
        "prd-manifest-consolidator-cefi": CloudRunExecutionStatus(
            job_name="prd-manifest-consolidator-cefi",
            status="succeeded",
            last_run_at="2026-06-22T06:00:00Z",
            exit_code=0,
            log_uri="",
        )
    }

    # The live path now reads the registry via a parallel GCS loader (``_load_gcp_vm_entries``)
    # — patch that seam directly (the parallel reader itself is covered separately below).
    from deployment_api.routes import deployments_inventory as _inv_mod

    _inv_mod._inventory_cache.clear()  # pyright: ignore[reportPrivateUsage]  # isolate the short-TTL cache

    with (
        patch("deployment_api.routes.deployments_inventory._cfg") as mock_cfg,
        patch("deployment_api.routes.deployments_inventory._load_gcp_vm_entries", return_value=entries),
        patch(
            "deployment_api.routes.deployments_inventory.latest_execution_by_job",
            return_value=cr_status,
        ),
        patch(
            "deployment_api.routes.deployments_inventory.vm_run_log_rolling_uri",
            return_value="gs://deployment-scripts-test/log-archive/rolling/run.log",
        ),
    ):
        mock_cfg.is_mock_mode.return_value = False
        mock_cfg.require_gcp_project_id.return_value = "test-project"
        resp = client_inventory.get("/api/deployments/inventory")
    assert resp.status_code == 200
    body = resp.json()
    names = {i["name"] for i in body["items"]}
    # Every VM (active running + archived failed) + Cloud Run jobs present.
    assert "cefi-binance-spot-20260622-014158" in names
    assert "defi-backfill-20260622-014200" in names
    oom = next(i for i in body["items"] if i["name"] == "defi-backfill-20260622-014200")
    assert oom["status"] == "failed"
    assert oom["exit_code"] == 137
    # The manifest-consolidator Cloud Run job bound its live succeeded status.
    consolidator = [
        i for i in body["items"] if i["service"] == "manifest-consolidator" and i["kind"] == "CLOUD_RUN_JOB"
    ]
    assert consolidator
    assert any(c["status"] == "succeeded" for c in consolidator)


# ---------------------------------------------------------------------------
# parallel registry reader (the perf fix — concurrent GCS reads, per-key isolation)
# ---------------------------------------------------------------------------


@dataclass
class _ParsedEntry:
    """A stand-in registry entry parsed from JSON (the real type is conftest-stubbed)."""

    vm_name: str

    @classmethod
    def from_json(cls, raw: str) -> _ParsedEntry:
        return cls(vm_name=json.loads(raw)["vm_name"])  # raises JSONDecodeError on corrupt input


def test_download_entries_parallel_reads_concurrently_and_skips_unreadable() -> None:
    """The parallel reader lists keys, downloads+parses each concurrently, skips bad ones."""
    from deployment_api.routes.deployments_inventory import _download_entries_parallel, _list_json_keys

    store = {
        "deployments/active/a.json": '{"vm_name": "vm-a"}',
        "deployments/active/b.json": '{"vm_name": "vm-b"}',
        "deployments/active/bad.json": "{not-valid-json",  # corrupt → skipped, never crashes the batch
    }

    @dataclass
    class _Blob:
        name: str

    class _FakeStorage:
        def list_blobs(self, *, bucket: str, prefix: str) -> list[_Blob]:
            return [_Blob(name=k) for k in store if k.startswith(prefix)]

        def download_bytes(self, *, bucket: str, blob_path: str) -> bytes:
            return store[blob_path].encode("utf-8")

    fake = _FakeStorage()
    # The real DeploymentRegistryEntry is conftest-stubbed (a MagicMock), so patch the
    # parser the reader uses with a real one to exercise the parse + per-key skip logic.
    with patch("deployment_api.routes.deployments_inventory.DeploymentRegistryEntry", _ParsedEntry):
        keys = _list_json_keys(fake, "bkt", "deployments/active/")  # type: ignore[arg-type]
        assert set(keys) == set(store)
        entries = _download_entries_parallel(fake, "bkt", keys)  # type: ignore[arg-type]
    # The two valid entries parse; the corrupt one is silently skipped (per-key isolation).
    assert {e.vm_name for e in entries} == {"vm-a", "vm-b"}


# ---------------------------------------------------------------------------
# _cloud_run_executions — status mapping (pure, no GCP)
# ---------------------------------------------------------------------------


def test_status_for_execution_mapping() -> None:
    from deployment_api.routes._cloud_run_executions import _status_for_execution  # pyright: ignore[reportPrivateUsage]

    @dataclass
    class _Ex:
        completion_time: datetime | None = None
        running_count: int = 0
        failed_count: int = 0

    assert _status_for_execution(_Ex(completion_time=_FIXED_NOW)) == ("succeeded", 0)
    assert _status_for_execution(_Ex(completion_time=_FIXED_NOW, failed_count=1)) == ("failed", 1)
    assert _status_for_execution(_Ex(running_count=2)) == ("running", None)
    assert _status_for_execution(_Ex()) == ("pending", None)


def test_latest_execution_by_job_degrades_on_gcp_error() -> None:
    """A GCP import/list failure degrades to an empty map, never raises."""
    from deployment_api.routes import _cloud_run_executions

    # Inject a broken _gcp_sdk so the import inside the helper raises.
    broken = ModuleType("deployment_service.backends._gcp_sdk")

    def _boom(_name: str) -> object:
        raise RuntimeError("no creds")

    broken.__getattr__ = _boom  # type: ignore[attr-defined]
    with patch.dict(sys.modules, {"deployment_service.backends._gcp_sdk": broken}):
        result = _cloud_run_executions.latest_execution_by_job("test-project")
    assert result == {}

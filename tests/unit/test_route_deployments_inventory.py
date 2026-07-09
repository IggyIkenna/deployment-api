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
    rows_in: int = 0
    rows_out: int = 0
    rows_error: int = 0
    events_emitted: int = 0
    # D.1 host metric vector (0.0/True defaults mirror the real DeploymentRegistryEntry's
    # honestly-unknown legacy-row default).
    cpu_pct: float = 0.0
    mem_pct: float = 0.0
    mem_slope: float = 0.0
    disk_pct: float = 0.0
    io_write_rate_bytes_sec: float = 0.0
    net_recv_rate_bytes_sec: float = 0.0
    workload_alive: bool = True
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


def test_build_inventory_surfaces_tier0_free_wins() -> None:
    """The GCE aggregated-list join + registry counters are surfaced, not discarded.

    Pins deployment_obs_backend_kinds_health-007: machine_type/zone/labels/boot-disk
    (from the GCE aggregated-list) and rows_in/rows_error/events_emitted/uptime_hours
    (from the registry entry) all reach the wire item instead of being dropped down to
    just the running-VM-name set.
    """
    from deployment_api.routes.deployments_inventory import build_inventory

    entry = _FakeEntry(
        vm_name="cefi-binance-spot-20260622-014158",
        asset_group="cefi",
        rows_in=20_000,
        rows_out=11_987,
        rows_error=3,
        events_emitted=412,
    )
    vm_details_by_name = {
        "cefi-binance-spot-20260622-014158": {
            "machine_type": "e2-highmem-8",
            "zone": "asia-northeast1-c",
            "status": "RUNNING",
            "creation_timestamp": "2026-06-22T01:00:00Z",
            "last_stop_timestamp": "",
            "boot_disk_name": "cefi-binance-spot-20260622-014158-boot",
            "labels": {"team": "cefi", "env": "prod"},
        }
    }
    items = build_inventory([entry], {}, _FIXED_NOW, vm_details_by_name)  # type: ignore[arg-type]
    vm = next(i for i in items if i.name == "cefi-binance-spot-20260622-014158")

    assert vm.rows_in == 20_000
    assert vm.rows_error == 3
    assert vm.events_emitted == 412
    # started_at 11:00, _FIXED_NOW 12:00, still running (no completed_at) -> 1.0h.
    assert vm.uptime_hours == pytest.approx(1.0)
    assert vm.machine_type == "e2-highmem-8"
    assert vm.zone == "asia-northeast1-c"
    assert vm.health_status == "RUNNING"
    assert vm.boot_disk_name == "cefi-binance-spot-20260622-014158-boot"
    assert vm.labels == {"team": "cefi", "env": "prod"}

    # A VM absent from the join (e.g. archived / no-longer-running) degrades to honest
    # None, never a crash or a fabricated value.
    unjoined = _FakeEntry(vm_name="defi-paper-trading-20260622", asset_group="defi")
    items_unjoined = build_inventory([unjoined], {}, _FIXED_NOW, {})  # type: ignore[arg-type]
    vm_unjoined = next(i for i in items_unjoined if i.name == "defi-paper-trading-20260622")
    assert vm_unjoined.machine_type is None
    assert vm_unjoined.zone is None
    assert vm_unjoined.health_status is None
    assert vm_unjoined.boot_disk_name is None
    assert vm_unjoined.labels is None

    # Cloud Run jobs have no GCE instance / registry counters — the new fields stay None.
    job = next(i for i in items_unjoined if i.kind == "CLOUD_RUN_JOB")
    assert job.rows_in is None
    assert job.uptime_hours is None
    assert job.machine_type is None


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


def test_counts_by_kind_omits_absent_kinds() -> None:
    """deployment_obs_backend_kinds_health-006: per-kind rollup, no fabricated zero keys."""
    from deployment_api.routes.deployments_inventory import _counts_by_kind, build_inventory

    items = build_inventory(_vm_entries(), {}, _FIXED_NOW)  # type: ignore[arg-type]
    counts = _counts_by_kind(items)
    assert counts["VM"] == 4
    assert counts["CLOUD_RUN_JOB"] == sum(1 for i in items if i.kind == "CLOUD_RUN_JOB")
    assert sum(counts.values()) == len(items)
    # None of the not-yet-censused kinds appear — absence, not a fabricated 0.
    for absent_kind in ("CLOUD_RUN_SERVICE", "ECS_SERVICE", "LAMBDA", "CLOUD_FUNCTION"):
        assert absent_kind not in counts

    assert _counts_by_kind([]) == {}


# ---------------------------------------------------------------------------
# _composite_health_status — D.3 VM 7-state taxonomy (v1: 5 real states + honest unknown)
# ---------------------------------------------------------------------------


def test_composite_health_not_applicable_for_terminal_entries() -> None:
    from deployment_api.routes.deployments_inventory import _composite_health_status

    terminal = _FakeEntry(vm_name="x", status="failed", exit_code=137)
    assert _composite_health_status(terminal, hb_age_seconds=None) is None  # type: ignore[arg-type]


def test_composite_health_dead_when_control_plane_says_not_running() -> None:
    from deployment_api.routes.deployments_inventory import _composite_health_status

    zombie = _FakeEntry(vm_name="x", status="running")
    assert (
        _composite_health_status(zombie, hb_age_seconds=30, control_plane_running=False)  # type: ignore[arg-type]
        == "dead"
    )


def test_composite_health_hung_when_heartbeat_stale_and_control_plane_confirms_running() -> None:
    from deployment_api.routes.deployments_inventory import _composite_health_status

    frozen = _FakeEntry(vm_name="x", status="running", cpu_pct=10.0)
    assert (
        _composite_health_status(frozen, hb_age_seconds=1_200, control_plane_running=True)  # type: ignore[arg-type]
        == "hung"
    )


def test_composite_health_unknown_without_control_plane_confirmation_falls_back_to_heartbeat_only() -> None:
    """No running-set supplied (control_plane_running=None) → hung still fires from
    heartbeat staleness alone (no regression vs. the pre-D.3 `stale` status)."""
    from deployment_api.routes.deployments_inventory import _composite_health_status

    frozen = _FakeEntry(vm_name="x", status="running", cpu_pct=10.0)
    assert _composite_health_status(frozen, hb_age_seconds=1_200) == "hung"  # type: ignore[arg-type]


def test_composite_health_unknown_for_legacy_row_with_no_d1_sample() -> None:
    from deployment_api.routes.deployments_inventory import _composite_health_status

    legacy = _FakeEntry(vm_name="x", status="running")  # all D.1 fields default 0.0
    assert (
        _composite_health_status(legacy, hb_age_seconds=30, control_plane_running=True) == "unknown"  # type: ignore[arg-type]
    )


def test_composite_health_disk_full() -> None:
    from deployment_api.routes.deployments_inventory import _composite_health_status

    full = _FakeEntry(vm_name="x", status="running", disk_pct=95.0, cpu_pct=5.0)
    assert (
        _composite_health_status(full, hb_age_seconds=30, control_plane_running=True) == "disk-full"  # type: ignore[arg-type]
    )


def test_composite_health_oom_risk() -> None:
    from deployment_api.routes.deployments_inventory import _composite_health_status

    climbing = _FakeEntry(vm_name="x", status="running", mem_pct=85.0, mem_slope=2.5)
    assert (
        _composite_health_status(climbing, hb_age_seconds=30, control_plane_running=True) == "oom-risk"  # type: ignore[arg-type]
    )


def test_composite_health_working_from_io_write_rate() -> None:
    from deployment_api.routes.deployments_inventory import _composite_health_status

    writing = _FakeEntry(vm_name="x", status="running", cpu_pct=40.0, io_write_rate_bytes_sec=2_048.0)
    assert (
        _composite_health_status(writing, hb_age_seconds=30, control_plane_running=True) == "working"  # type: ignore[arg-type]
    )


def test_composite_health_unknown_when_idle_with_no_object_delta_signal() -> None:
    """Idle cpu/io/net with a real D.1 sample and no oom/disk flag, no umbrella/object_delta
    supplied by the caller — degrades to `unknown` rather than guessing (WS-D.0 principle 2)."""
    from deployment_api.routes.deployments_inventory import _composite_health_status

    idle = _FakeEntry(vm_name="x", status="running", cpu_pct=3.0, mem_pct=20.0)
    assert (
        _composite_health_status(idle, hb_age_seconds=30, control_plane_running=True) == "unknown"  # type: ignore[arg-type]
    )


def test_composite_health_workload_dead_when_daemon_alive_but_pid_gone() -> None:
    """Fresh heartbeat (daemon alive) + a resolved-dead CMD_PID reading → `workload-dead`,
    ahead of the D.1-metric-dependent states (the process is confirmed gone regardless of
    what disk/mem/cpu look like)."""
    from deployment_api.routes.deployments_inventory import _composite_health_status

    dead_pid = _FakeEntry(vm_name="x", status="running", cpu_pct=5.0, disk_pct=95.0, workload_alive=False)
    assert (
        _composite_health_status(dead_pid, hb_age_seconds=30, control_plane_running=True)  # type: ignore[arg-type]
        == "workload-dead"
    )


def test_composite_health_workload_alive_default_never_fires_workload_dead() -> None:
    """The honestly-unknown `True` default (unconfigured / legacy row) never claims dead."""
    from deployment_api.routes.deployments_inventory import _composite_health_status

    legacy = _FakeEntry(vm_name="x", status="running")  # workload_alive defaults True
    assert (
        _composite_health_status(legacy, hb_age_seconds=30, control_plane_running=True) == "unknown"  # type: ignore[arg-type]
    )


def test_composite_health_stalled_for_batch_umbrella_when_object_delta_zero_and_cpu_idle() -> None:
    """BATCH umbrella + object_delta==0 + cpu below the ceiling → `stalled` (the one row of the
    parent WS-D.3 threshold table whose prerequisite signal is wired)."""
    from unified_api_contracts import DeploymentUmbrella

    from deployment_api.routes.deployments_inventory import _composite_health_status

    flat = _FakeEntry(vm_name="x", status="running", cpu_pct=3.0, mem_pct=20.0)
    assert (
        _composite_health_status(
            flat,  # type: ignore[arg-type]
            hb_age_seconds=30,
            control_plane_running=True,
            umbrella=DeploymentUmbrella.BATCH,
            object_delta=0,
        )
        == "stalled"
    )


def test_composite_health_batch_working_when_object_delta_positive_despite_idle_proc_sample() -> None:
    """object_delta>0 counts toward `working` even with an idle-looking /proc sample (a batch VM
    writes in bursts) — never misreported as `stalled` while real progress landed."""
    from unified_api_contracts import DeploymentUmbrella

    from deployment_api.routes.deployments_inventory import _composite_health_status

    progressing = _FakeEntry(vm_name="x", status="running", cpu_pct=3.0, mem_pct=20.0)
    assert (
        _composite_health_status(
            progressing,  # type: ignore[arg-type]
            hb_age_seconds=30,
            control_plane_running=True,
            umbrella=DeploymentUmbrella.BATCH,
            object_delta=128,
        )
        == "working"
    )


def test_composite_health_batch_stalled_requires_cpu_below_ceiling() -> None:
    """object_delta==0 alone isn't enough — the threshold table is progress-metric primary, cpu
    SECONDARY (never a global cpu cut, but cpu still gates `stalled` per the v1 defaults)."""
    from unified_api_contracts import DeploymentUmbrella

    from deployment_api.routes.deployments_inventory import _composite_health_status

    busy_but_flat = _FakeEntry(vm_name="x", status="running", cpu_pct=45.0, mem_pct=20.0)
    assert (
        _composite_health_status(
            busy_but_flat,  # type: ignore[arg-type]
            hb_age_seconds=30,
            control_plane_running=True,
            umbrella=DeploymentUmbrella.BATCH,
            object_delta=0,
        )
        == "unknown"
    )


def test_composite_health_live_umbrella_stalled_stays_unknown() -> None:
    """LIVE umbrella has no wired stalled signal yet (needs an expected-active-window calendar
    this codebase doesn't have) — stays honest-`unknown` rather than guessing from idle io/net."""
    from unified_api_contracts import DeploymentUmbrella

    from deployment_api.routes.deployments_inventory import _composite_health_status

    idle_live = _FakeEntry(vm_name="x", status="running", cpu_pct=3.0, mem_pct=20.0)
    assert (
        _composite_health_status(
            idle_live,  # type: ignore[arg-type]
            hb_age_seconds=30,
            control_plane_running=True,
            umbrella=DeploymentUmbrella.LIVE,
            object_delta=0,
        )
        == "unknown"
    )


def test_build_inventory_threads_object_deltas_into_composite_health() -> None:
    """build_inventory's object_deltas map (batched by the caller) reaches the composite
    classifier — a BATCH VM in an asset_group with object_delta==0 surfaces `stalled`."""
    from deployment_api.routes.deployments_inventory import build_inventory

    entries = [
        # Unknown prefix -> EPHEMERAL_BATCH -> BATCH umbrella. A real (nonzero) D.1 sample so
        # `_has_d1_metrics` passes the honest-unknown legacy-row gate.
        _FakeEntry(vm_name="cefi-binance-spot-20260622-014158", asset_group="cefi", cpu_pct=3.0),
    ]
    vm_details_by_name = {e.vm_name: {"status": "RUNNING"} for e in entries}  # control-plane-confirmed
    object_deltas = {"cefi": 0}
    items = build_inventory(
        entries,  # type: ignore[arg-type]
        {},
        _FIXED_NOW,
        vm_details_by_name,
        object_deltas=object_deltas,
    )
    by_name = {i.name: i for i in items}
    assert by_name["cefi-binance-spot-20260622-014158"].composite_health_status == "stalled"


def test_batched_object_deltas_calls_once_per_distinct_asset_group() -> None:
    """ONE object_delta_for_asset_group call per DISTINCT BATCH asset_group, never per VM —
    WS-D.0 principle 5 (zero new bucket walks) at scale: many VMs sharing an asset_group must
    not multiply the manifest read."""
    from deployment_api.routes import deployments_inventory as mod

    entries = [
        _FakeEntry(vm_name="cefi-binance-spot-20260622-a", asset_group="cefi"),
        _FakeEntry(vm_name="cefi-binance-spot-20260622-b", asset_group="cefi"),  # same asset_group
        _FakeEntry(vm_name="defi-backfill-20260622-c", asset_group="defi"),
        _FakeEntry(
            vm_name="strategy-live-cefi-20260620", asset_group="cefi"
        ),  # LIVE umbrella — excluded from the batch
        _FakeEntry(vm_name="cefi-binance-spot-20260622-d", asset_group="cefi", status="failed"),  # not running
    ]
    with patch.object(mod, "object_delta_for_asset_group", return_value=(0, "ok")) as fake_lookup:
        deltas = mod._batched_object_deltas(entries, _FIXED_NOW)  # type: ignore[arg-type]
    assert fake_lookup.call_count == 2  # exactly {"cefi", "defi"}, not 5 (one per VM)
    assert set(deltas) == {"cefi", "defi"}


def test_build_inventory_threads_vm_details_control_plane_confirmation_into_composite_health() -> None:
    """build_inventory's vm_details_by_name key set reaches the composite classifier —
    a VM absent from the GCE aggregated-list join surfaces composite_health_status=dead."""
    from deployment_api.routes.deployments_inventory import build_inventory

    entries = _vm_entries()
    # NOT defi-paper-trading-20260622 — absent from the (real, non-None) GCE join below.
    vm_details_by_name = {
        "cefi-binance-spot-20260622-014158": {"status": "RUNNING"},
        "strategy-live-cefi-20260620": {"status": "RUNNING"},
    }
    items = build_inventory(entries, {}, _FIXED_NOW, vm_details_by_name)  # type: ignore[arg-type]
    by_name = {i.name: i for i in items}
    assert by_name["defi-paper-trading-20260622"].composite_health_status == "dead"
    # The failed/terminal OOM VM has no composite (not applicable).
    assert by_name["defi-backfill-20260622-014200"].composite_health_status is None


def test_build_inventory_present_but_not_running_join_entry_is_still_dead() -> None:
    """A VM PRESENT in the GCE join but with a non-RUNNING raw status (e.g. GCE keeps a
    STOPPING/TERMINATED instance visible in the aggregated-list for a while) must still
    resolve dead — mere key presence is not "running"; the raw status value is checked.
    """
    from deployment_api.routes.deployments_inventory import build_inventory

    entry = _FakeEntry(vm_name="cefi-stopping-vm", status="running")
    vm_details_by_name = {"cefi-stopping-vm": {"status": "TERMINATED"}}
    items = build_inventory([entry], {}, _FIXED_NOW, vm_details_by_name)  # type: ignore[arg-type]
    vm = next(i for i in items if i.name == "cefi-stopping-vm")
    assert vm.composite_health_status == "dead"


# ---------------------------------------------------------------------------
# _cloud_function_item — GCP Cloud Functions (gen2) census item builder
# ---------------------------------------------------------------------------


def test_cloud_function_item_builds_deployment_item() -> None:
    """A live Cloud Function classifies directly — no lifecycle_class needed,
    umbrella is always NONE (no live/batch/paper phase, mirrors ECS_SERVICE)."""
    from deployment_api.routes._gcp_cloud_functions import CloudFunctionStatus
    from deployment_api.routes.deployments_inventory import _cloud_function_item  # pyright: ignore[reportPrivateUsage]

    status = CloudFunctionStatus(
        name="trigger-ingest",
        status="running",
        runtime="python313",
        service_name="trigger-ingest",
        last_updated_at="2026-07-09T06:00:00+00:00",
    )
    item = _cloud_function_item(status)
    assert item.name == "trigger-ingest"
    assert item.kind == "CLOUD_FUNCTION"
    assert item.umbrella == "NONE"
    assert item.cloud == "GCP"
    assert item.status == "running"
    assert item.last_run_at == "2026-07-09T06:00:00+00:00"
    assert item.exit_code is None


# ---------------------------------------------------------------------------
# Alerts: fire on transition into oom-risk/stalled, never on a repeat-poll
# ---------------------------------------------------------------------------


def _health_item(name: str, composite_health_status: str | None) -> object:
    from deployment_api.routes.deployments_inventory import DeploymentItem

    return DeploymentItem(
        name=name,
        kind="VM",
        umbrella="BATCH",
        cloud="GCP",
        service="cefi-binance-spot",
        asset_group="cefi",
        status="running",
        composite_health_status=composite_health_status,
    )


def test_alert_on_health_transition_fires_once_per_transition() -> None:
    """deployment_obs_backend_kinds_health-018: alert only on a FRESH transition, never
    a repeat while the same alertable state persists across polls."""
    from deployment_api.routes import deployments_inventory as _inv_mod

    _inv_mod._last_alerted_health.clear()  # pyright: ignore[reportPrivateUsage]
    name = "cefi-binance-spot-20260622-014158"

    with patch.object(_inv_mod, "_persist_alert") as mock_persist:
        _inv_mod._alert_on_health_transition(_health_item(name, "oom-risk"))  # pyright: ignore[reportPrivateUsage]
        # Same state next poll -> no re-fire.
        _inv_mod._alert_on_health_transition(_health_item(name, "oom-risk"))  # pyright: ignore[reportPrivateUsage]
    assert mock_persist.call_count == 1
    call_kwargs = mock_persist.call_args.kwargs
    assert call_kwargs["alert_class"] == "oom-risk"
    assert call_kwargs["severity"] == "CRITICAL"
    assert name in call_kwargs["workflow_name"]

    with patch.object(_inv_mod, "_persist_alert") as mock_persist:
        # Recovers -> not alertable, no fire, but records the state.
        _inv_mod._alert_on_health_transition(_health_item(name, "working"))  # pyright: ignore[reportPrivateUsage]
        # Re-enters oom-risk -> a NEW transition -> fires again.
        _inv_mod._alert_on_health_transition(_health_item(name, "oom-risk"))  # pyright: ignore[reportPrivateUsage]
    assert mock_persist.call_count == 1


def test_alert_on_health_transition_ignores_non_alertable_states() -> None:
    from deployment_api.routes import deployments_inventory as _inv_mod

    _inv_mod._last_alerted_health.clear()  # pyright: ignore[reportPrivateUsage]
    with patch.object(_inv_mod, "_persist_alert") as mock_persist:
        for status in ("working", "hung", "dead", "disk-full", "unknown", None):
            _inv_mod._alert_on_health_transition(_health_item("vm-a", status))  # pyright: ignore[reportPrivateUsage]
    mock_persist.assert_not_called()


def test_persist_alert_writes_expected_row_shape() -> None:
    """The ledger row mirrors agent-orchestrator's _persist_to_gcs shape exactly, so
    GET /api/alerts (_repo_ci_alerts.py's _parse_line) picks it up with no reader change."""
    from deployment_api.routes import deployments_inventory as _inv_mod

    written_bucket = ""
    written_path = ""
    written_data = b""

    def _fake_download(bucket: str, path: str) -> bytes:
        raise FileNotFoundError("no existing blob")

    def _fake_upload(bucket: str, path: str, data: bytes, content_type: str | None = None) -> str:
        nonlocal written_bucket, written_path, written_data
        written_bucket, written_path, written_data = bucket, path, data
        return f"gs://{bucket}/{path}"

    with (
        patch.object(_inv_mod, "download_from_storage", side_effect=_fake_download),
        patch.object(_inv_mod, "upload_to_storage", side_effect=_fake_upload),
    ):
        _inv_mod._persist_alert(  # pyright: ignore[reportPrivateUsage]
            alert_class="oom-risk",
            workflow_name="vm-health-cefi-binance-spot",
            severity="CRITICAL",
            message="cefi-binance-spot is oom-risk",
            dedup_key="vm-health-cefi-binance-spot-oom-risk",
        )
    assert written_bucket == "unified-trading-cicd-events"
    assert written_path.startswith("cicd/alerts/")
    assert written_path.endswith("/alerts.jsonl")
    row = json.loads(written_data.decode("utf-8").strip())
    assert row["event_type"] == "slack_alert"
    assert row["alert_class"] == "oom-risk"
    assert row["repo"] == "deployment-api"
    assert row["workflow_name"] == "vm-health-cefi-binance-spot"
    assert row["severity"] == "CRITICAL"


def test_persist_alert_never_raises_on_storage_failure() -> None:
    """Shard-level isolation: a ledger-write failure never breaks the inventory computation."""
    from deployment_api.routes import deployments_inventory as _inv_mod

    with patch.object(_inv_mod, "download_from_storage", side_effect=RuntimeError("gcs down")):
        _inv_mod._persist_alert(  # pyright: ignore[reportPrivateUsage]
            alert_class="oom-risk",
            workflow_name="vm-health-x",
            severity="CRITICAL",
            message="x is oom-risk",
            dedup_key="vm-health-x-oom-risk",
        )  # must not raise


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
    # counts_by_kind rolls up per-kind, additive alongside the legacy vm_count/cloud_run_job_count.
    assert body["counts_by_kind"]["VM"] == body["vm_count"]
    assert body["counts_by_kind"]["CLOUD_RUN_JOB"] == body["cloud_run_job_count"]
    assert sum(body["counts_by_kind"].values()) == body["total"]
    # A kind absent from the (mock) estate is simply absent — never a fabricated 0 key.
    assert "ECS_SERVICE" not in body["counts_by_kind"]


def test_inventory_route_kind_filter(client_inventory: TestClient) -> None:
    """deployment_obs_backend_kinds_health-006: kind= isolates one DeploymentKind."""
    with patch("deployment_api.routes.deployments_inventory._cfg") as mock_cfg:
        mock_cfg.is_mock_mode.return_value = True
        resp = client_inventory.get("/api/deployments/inventory", params={"kind": "cloud_run_job"})
    assert resp.status_code == 200
    body = resp.json()
    assert body["total"] >= 1
    assert all(i["kind"] == "CLOUD_RUN_JOB" for i in body["items"])
    assert body["vm_count"] == 0
    assert body["counts_by_kind"] == {"CLOUD_RUN_JOB": body["total"]}

    # A kind with no rows in the mock estate (not yet censused) -> empty, not an error.
    with patch("deployment_api.routes.deployments_inventory._cfg") as mock_cfg:
        mock_cfg.is_mock_mode.return_value = True
        resp_empty = client_inventory.get("/api/deployments/inventory", params={"kind": "lambda"})
    assert resp_empty.status_code == 200
    body_empty = resp_empty.json()
    assert body_empty["total"] == 0
    assert body_empty["items"] == []
    assert body_empty["counts_by_kind"] == {}


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


def test_detail_route_mock_shape(client_inventory: TestClient) -> None:
    """Mock mode: item found, D.1 metrics honestly None (the mock never populates the entry cache)."""
    with patch("deployment_api.routes.deployments_inventory._cfg") as mock_cfg:
        mock_cfg.is_mock_mode.return_value = True
        resp = client_inventory.get("/api/deployments/cefi-binance-spot-20260622-014158/detail")
    assert resp.status_code == 200
    body = resp.json()
    assert body["item"]["name"] == "cefi-binance-spot-20260622-014158"
    assert body["cpu_pct"] is None
    assert body["workload_alive"] is None


def test_detail_route_unknown_name_404(client_inventory: TestClient) -> None:
    with patch("deployment_api.routes.deployments_inventory._cfg") as mock_cfg:
        mock_cfg.is_mock_mode.return_value = True
        resp = client_inventory.get("/api/deployments/does-not-exist/detail")
    assert resp.status_code == 404


def test_detail_route_live_path_includes_d1_metrics(client_inventory: TestClient) -> None:
    """deployment_obs_backend_kinds_health-017: the D.1 vector rides the SAME GCP census the
    list endpoint already runs (no new bucket walk) — hitting /inventory once, then /detail,
    surfaces the metrics stamped on the registry entry."""
    from deployment_api.routes import deployments_inventory as _inv_mod

    entry = _FakeEntry(
        vm_name="cefi-binance-spot-20260622-014158",
        cpu_pct=42.5,
        mem_pct=61.0,
        mem_slope=0.3,
        disk_pct=12.0,
        io_write_rate_bytes_sec=1_048_576.0,
        net_recv_rate_bytes_sec=2_048.0,
        workload_alive=True,
    )
    _inv_mod._inventory_cache.clear()  # pyright: ignore[reportPrivateUsage]
    _inv_mod._vm_entry_by_name_cache.clear()  # pyright: ignore[reportPrivateUsage]

    with (
        patch("deployment_api.routes.deployments_inventory._cfg") as mock_cfg,
        patch("deployment_api.routes.deployments_inventory._load_gcp_vm_entries", return_value=([entry], {})),
        patch("deployment_api.routes.deployments_inventory.latest_execution_by_job", return_value={}),
    ):
        mock_cfg.is_mock_mode.return_value = False
        mock_cfg.require_gcp_project_id.return_value = "test-project"
        # Populate the caches via the list endpoint first (the same cadence production hits).
        list_resp = client_inventory.get("/api/deployments/inventory")
        assert list_resp.status_code == 200
        resp = client_inventory.get("/api/deployments/cefi-binance-spot-20260622-014158/detail")
    assert resp.status_code == 200
    body = resp.json()
    assert body["item"]["name"] == "cefi-binance-spot-20260622-014158"
    assert body["cpu_pct"] == 42.5
    assert body["mem_pct"] == 61.0
    assert body["mem_slope"] == 0.3
    assert body["disk_pct"] == 12.0
    assert body["io_write_rate_bytes_sec"] == 1_048_576.0
    assert body["net_recv_rate_bytes_sec"] == 2_048.0
    assert body["workload_alive"] is True


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

    vm_details_by_name = {e.vm_name: {} for e in entries if e.status == "running"}
    with (
        patch("deployment_api.routes.deployments_inventory._cfg") as mock_cfg,
        patch(
            "deployment_api.routes.deployments_inventory._load_gcp_vm_entries",
            return_value=(entries, vm_details_by_name),
        ),
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


def test_load_gcp_vm_entries_does_not_filter_active_entries_by_control_plane_presence() -> None:
    """An ``active/`` registry entry whose VM the GCE aggregated-list no longer has
    (hard-killed/OOM/pre-empted before it could self-archive) must still be RETURNED —
    filtering it out here would silently vanish the exact case D.3 ``dead`` exists to
    catch, before ``build_inventory``/``_composite_health_status`` ever see it.
    """
    from datetime import UTC, datetime

    from deployment_api.routes.deployments_inventory import _load_gcp_vm_entries

    store = {
        "deployments/active/dead.json": '{"vm_name": "cefi-dead-vm"}',
        "deployments/active/alive.json": '{"vm_name": "cefi-alive-vm"}',
    }

    @dataclass
    class _Blob:
        name: str

    class _FakeStorage:
        def list_blobs(self, *, bucket: str, prefix: str) -> list[_Blob]:
            return [_Blob(name=k) for k in store if k.startswith(prefix)]

        def download_bytes(self, *, bucket: str, blob_path: str) -> bytes:
            return store[blob_path].encode("utf-8")

    # The GCE aggregated-list no longer has "cefi-dead-vm" at all (control plane says
    # gone) — only "cefi-alive-vm" is confirmed RUNNING.
    vm_details = {"cefi-alive-vm": {"status": "RUNNING"}}

    with (
        patch("deployment_api.routes.deployments_inventory.DeploymentRegistryEntry", _ParsedEntry),
        patch("deployment_api.routes.deployments_inventory.get_storage_client", return_value=_FakeStorage()),
        patch("deployment_api.routes.deployments_inventory.get_vm_instance_details", return_value=vm_details),
    ):
        entries, returned_vm_details = _load_gcp_vm_entries(datetime(2026, 6, 22, 12, tzinfo=UTC), "test-project")

    assert {e.vm_name for e in entries} == {"cefi-dead-vm", "cefi-alive-vm"}
    assert returned_vm_details == vm_details


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


# ---------------------------------------------------------------------------
# _cloud_run_services — the CLOUD_RUN_SERVICE census (pure mapping + degradation)
# ---------------------------------------------------------------------------


@dataclass
class _FakeConditionState:
    name: str


@dataclass
class _FakeTerminalCondition:
    state: _FakeConditionState


@dataclass
class _FakeRunV2Service:
    """Minimal stand-in for a run_v2 ``Service`` proto (the fields read defensively)."""

    name: str
    terminal_condition: _FakeTerminalCondition
    latest_ready_revision: str = ""
    latest_created_revision: str = ""
    uri: str = ""


def test_region_from_resource_name_parses_location_segment() -> None:
    from deployment_api.routes._cloud_run_services import (
        _region_from_resource_name,  # pyright: ignore[reportPrivateUsage]
    )

    full = "projects/test-project/locations/asia-northeast1/services/deployment-api"
    assert _region_from_resource_name(full, "fallback-region") == "asia-northeast1"
    assert _region_from_resource_name("garbage", "fallback-region") == "fallback-region"


def test_list_cloud_run_services_maps_ready_state_revision_and_region() -> None:
    from deployment_api.routes import _cloud_run_services

    fake_service = _FakeRunV2Service(
        name="projects/test-project/locations/asia-northeast1/services/deployment-api",
        terminal_condition=_FakeTerminalCondition(state=_FakeConditionState(name="CONDITION_SUCCEEDED")),
        latest_ready_revision=(
            "projects/test-project/locations/asia-northeast1/services/deployment-api/revisions/deployment-api-00007-abc"
        ),
        uri="https://deployment-api-xyz.a.run.app",
    )

    class _FakeServicesClient:
        def list_services(self, request: object) -> list[_FakeRunV2Service]:
            del request
            return [fake_service]

    class _FakeRunV2Namespace:
        ServicesClient = _FakeServicesClient

        class ListServicesRequest:
            def __init__(self, *, parent: str) -> None:
                self.parent = parent

    fake_module = ModuleType("deployment_service.backends._gcp_sdk")
    fake_module.run_v2 = _FakeRunV2Namespace  # type: ignore[attr-defined]
    fake_backends_pkg = ModuleType("deployment_service.backends")
    fake_backends_pkg._gcp_sdk = fake_module  # type: ignore[attr-defined]

    with patch.dict(
        sys.modules,
        {
            "deployment_service.backends": fake_backends_pkg,
            "deployment_service.backends._gcp_sdk": fake_module,
        },
    ):
        result = _cloud_run_services.list_cloud_run_services("test-project")

    assert len(result) == 1
    svc = result[0]
    assert svc.name == "deployment-api"
    assert svc.ready is True
    assert svc.state == "CONDITION_SUCCEEDED"
    assert svc.revision == "deployment-api-00007-abc"
    assert svc.region == "asia-northeast1"
    assert svc.uri == "https://deployment-api-xyz.a.run.app"


def test_list_cloud_run_services_not_ready_when_reconciling() -> None:
    from deployment_api.routes import _cloud_run_services

    fake_service = _FakeRunV2Service(
        name="projects/test-project/locations/asia-northeast1/services/still-deploying",
        terminal_condition=_FakeTerminalCondition(state=_FakeConditionState(name="CONDITION_RECONCILING")),
    )

    class _FakeServicesClient:
        def list_services(self, request: object) -> list[_FakeRunV2Service]:
            del request
            return [fake_service]

    class _FakeRunV2Namespace:
        ServicesClient = _FakeServicesClient

        class ListServicesRequest:
            def __init__(self, *, parent: str) -> None:
                self.parent = parent

    fake_module = ModuleType("deployment_service.backends._gcp_sdk")
    fake_module.run_v2 = _FakeRunV2Namespace  # type: ignore[attr-defined]
    fake_backends_pkg = ModuleType("deployment_service.backends")
    fake_backends_pkg._gcp_sdk = fake_module  # type: ignore[attr-defined]

    with patch.dict(
        sys.modules,
        {
            "deployment_service.backends": fake_backends_pkg,
            "deployment_service.backends._gcp_sdk": fake_module,
        },
    ):
        result = _cloud_run_services.list_cloud_run_services("test-project")

    assert result[0].ready is False
    assert result[0].state == "CONDITION_RECONCILING"


def test_list_cloud_run_services_degrades_on_gcp_error() -> None:
    """A GCP import/list failure degrades to an empty list, never raises."""
    from deployment_api.routes import _cloud_run_services

    broken = ModuleType("deployment_service.backends._gcp_sdk")

    def _boom(_name: str) -> object:
        raise RuntimeError("no creds")

    broken.__getattr__ = _boom  # type: ignore[attr-defined]
    with patch.dict(sys.modules, {"deployment_service.backends._gcp_sdk": broken}):
        result = _cloud_run_services.list_cloud_run_services("test-project")
    assert result == []


# ---------------------------------------------------------------------------
# build_inventory — CLOUD_RUN_SERVICE wiring (umbrella=NONE, Open-Q1)
# ---------------------------------------------------------------------------


def test_build_inventory_includes_cloud_run_service_with_umbrella_sentinel() -> None:
    from deployment_api.routes._cloud_run_services import CloudRunServiceStatus
    from deployment_api.routes.deployments_inventory import build_inventory

    services = [
        CloudRunServiceStatus(
            name="deployment-api",
            ready=True,
            state="CONDITION_SUCCEEDED",
            revision="deployment-api-00007-abc",
            region="asia-northeast1",
            uri="https://deployment-api-xyz.a.run.app",
        )
    ]
    items = build_inventory([], {}, _FIXED_NOW, cloud_run_services=services)
    service_items = [i for i in items if i.kind == "CLOUD_RUN_SERVICE"]
    assert len(service_items) == 1
    item = service_items[0]
    assert item.name == "deployment-api"
    # Services have no live/batch/paper phase — the wire umbrella is the
    # DeploymentUmbrella.NONE value, never one of the 4 phase umbrellas (Open-Q1).
    assert item.umbrella == "NONE"
    assert item.cloud == "GCP"
    assert item.status == "running"
    assert item.revision == "deployment-api-00007-abc"
    assert item.region == "asia-northeast1"


def test_build_inventory_defaults_to_no_cloud_run_services() -> None:
    """Omitting cloud_run_services (back-compat call shape) yields zero service rows."""
    from deployment_api.routes.deployments_inventory import build_inventory

    items = build_inventory([], {}, _FIXED_NOW)
    assert not [i for i in items if i.kind == "CLOUD_RUN_SERVICE"]


def test_build_inventory_skips_unclassifiable_cloud_run_service() -> None:
    """An empty service name can't classify — skipped, never crashes the census."""
    from deployment_api.routes._cloud_run_services import CloudRunServiceStatus
    from deployment_api.routes.deployments_inventory import build_inventory

    services = [
        CloudRunServiceStatus(
            name="",
            ready=True,
            state="CONDITION_SUCCEEDED",
            revision="",
            region="asia-northeast1",
            uri="",
        )
    ]
    items = build_inventory([], {}, _FIXED_NOW, cloud_run_services=services)
    assert not [i for i in items if i.kind == "CLOUD_RUN_SERVICE"]

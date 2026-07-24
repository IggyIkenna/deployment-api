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
import threading
import time
from dataclasses import dataclass, field
from datetime import UTC, datetime, timedelta
from types import ModuleType
from unittest.mock import patch

import pytest
from fastapi import FastAPI
from fastapi.testclient import TestClient

from tests.mocks import patch_inventory_secondary_census

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
    host_metrics_window: list[dict[str, float | str]] = field(default_factory=list)
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


def test_build_inventory_full_estate_surfaces_unmanaged_vms() -> None:
    """WS-D full-estate census: a live GCE instance with NO registry entry becomes an `unmanaged`
    row (never invisible), carrying its raw GCE state; provenance is adhoc (or control-plane for a
    managed out-of-band prefix); a registry-backed VM stays launched_by=deployment-api.
    """
    from deployment_api.routes.deployments_inventory import (
        LAUNCHED_BY_ADHOC,
        LAUNCHED_BY_CONTROL_PLANE,
        LAUNCHED_BY_DEPLOYMENT_API,
        build_inventory,
    )

    entries = _vm_entries()
    vm_details_by_name = {
        # A registered VM present in the join (stays a deployment-api row, NOT re-emitted as unmanaged).
        "cefi-binance-spot-20260622-014158": {"status": "RUNNING"},
        # UNMANAGED — an agent/operator ad-hoc launch (no registry entry) → adhoc.
        "onchain-perp-symbol-canon-20260709-123056": {
            "status": "RUNNING",
            "machine_type": "e2-standard-4",
            "zone": "asia-northeast1-c",
            "creation_timestamp": "2026-06-22T09:00:00Z",  # 3h before _FIXED_NOW
            "boot_disk_name": "onchain-perp-symbol-canon-20260709-123056-boot",
            "labels": {"purpose": "canon"},
        },
        # UNMANAGED — the 16-day zombie-watchdog (registry-less scheduled daemon) → adhoc.
        "vm-zombie-watchdog-20260623-171612": {"status": "RUNNING", "creation_timestamp": "2026-06-22T00:00:00Z"},
        # UNMANAGED but a control-plane prefix (managed out-of-band) → control-plane, never adhoc.
        "agent-orchestrator-central": {"status": "RUNNING"},
        # UNMANAGED and TERMINATED — GCE still lists it briefly; maps to status=stopped.
        "rogue-terminated-vm": {"status": "TERMINATED"},
    }
    items = build_inventory(entries, {}, _FIXED_NOW, vm_details_by_name)  # type: ignore[arg-type]
    by_name = {i.name: i for i in items}

    # Registry VMs (4) + the 4 unmanaged instances not already registered = 8 VM rows.
    vm_items = [i for i in items if i.kind == "VM"]
    assert len(vm_items) == len(entries) + 4

    # Registry-backed VM → deployment-api.
    assert by_name["cefi-binance-spot-20260622-014158"].launched_by == LAUNCHED_BY_DEPLOYMENT_API

    # Ad-hoc unmanaged VM → adhoc, carrying raw GCE state via health_status + the free-win fields;
    # no registry entry → no heartbeat; uptime derived from the creation timestamp.
    adhoc = by_name["onchain-perp-symbol-canon-20260709-123056"]
    assert adhoc.launched_by == LAUNCHED_BY_ADHOC
    assert adhoc.status == "running"
    assert adhoc.health_status == "RUNNING"
    assert adhoc.machine_type == "e2-standard-4"
    assert adhoc.zone == "asia-northeast1-c"
    assert adhoc.boot_disk_name == "onchain-perp-symbol-canon-20260709-123056-boot"
    assert adhoc.labels == {"purpose": "canon"}
    assert adhoc.heartbeat_age_seconds is None
    assert adhoc.composite_health_status is None
    assert adhoc.uptime_hours == pytest.approx(3.0)

    # The zombie-watchdog is registry-less → adhoc (NOT control-plane).
    assert by_name["vm-zombie-watchdog-20260623-171612"].launched_by == LAUNCHED_BY_ADHOC
    # A control-plane prefix is accounted-for → control-plane, never adhoc (keeps parity with
    # fleet_reconciliation's UNKNOWN set, which also excludes control-plane VMs).
    assert by_name["agent-orchestrator-central"].launched_by == LAUNCHED_BY_CONTROL_PLANE
    # A terminated unmanaged instance maps to stopped (still adhoc provenance).
    rogue = by_name["rogue-terminated-vm"]
    assert rogue.status == "stopped"
    assert rogue.health_status == "TERMINATED"
    assert rogue.launched_by == LAUNCHED_BY_ADHOC


def test_build_inventory_registry_degraded_still_renders_live_vms() -> None:
    """P2 census-decouple guarantee (the prod-blank-tab fix): when the registry read degrades to
    an EMPTY list (Firestore/GCS slow or failed), every LIVE GCE instance STILL renders as a VM
    row from the GCE aggregated-list join. A registry hiccup can never blank the fleet — the old
    bundled-future returned ([], {}) on timeout and dropped every live VM.
    """
    from deployment_api.routes.deployments_inventory import build_inventory

    # vm_entries=[] simulates a degraded registry read; the GCE join is populated (the live fleet).
    live_vms: dict[str, dict[str, object]] = {
        "cefi-binance-futures-2024-heavy-20260714-010101": {
            "status": "RUNNING",
            "machine_type": "n2-standard-8",
            "zone": "asia-northeast1-c",
        },
        "mtds-dex-swaps-backfill-20260714-020202": {"status": "RUNNING"},
        "strategy-live-cefi-20260714": {"status": "RUNNING"},
    }
    items = build_inventory([], {}, _FIXED_NOW, live_vms)  # type: ignore[arg-type]
    vm_items = [i for i in items if i.kind == "VM"]
    # Every live VM rendered despite the empty registry (enrichment degrades, the row never drops).
    assert {i.name for i in vm_items} == set(live_vms)
    assert len(vm_items) == len(live_vms)


def test_build_inventory_scale_many_registry_entries_render() -> None:
    """Scale sanity: the classifier+render path handles 1k+ registry entries without error or loss
    (the Firestore indexed query replaces the N-blob download upstream; build_inventory itself is a
    pure, I/O-free classifier, so it must not choke or silently drop at scale)."""
    from deployment_api.routes.deployments_inventory import build_inventory

    entries = [
        _FakeEntry(vm_name=f"cefi-binance-spot-2024-heavy-2026071{i % 10}-{i:06d}", asset_group="cefi")
        for i in range(1200)
    ]
    items = build_inventory(entries, {}, _FIXED_NOW, {})  # type: ignore[arg-type]
    vm_items = [i for i in items if i.kind == "VM"]
    assert len(vm_items) == 1200  # every entry classified + rendered, none silently dropped


def test_build_inventory_no_unmanaged_rows_without_a_real_join() -> None:
    """The full-estate union adds unmanaged rows ONLY for a real GCE join — None (no join offered)
    and {} (a real, empty census) add nothing, so the classification-only paths + existing callers
    stay byte-for-byte unchanged.
    """
    from deployment_api.routes.deployments_inventory import build_inventory

    entries = _vm_entries()
    assert len([i for i in build_inventory(entries, {}, _FIXED_NOW) if i.kind == "VM"]) == len(entries)  # type: ignore[arg-type]
    assert len([i for i in build_inventory(entries, {}, _FIXED_NOW, {}) if i.kind == "VM"]) == len(entries)  # type: ignore[arg-type]


def test_build_inventory_launched_by_provenance_for_cloud_run_jobs() -> None:
    """A live Cloud Run job that binds to a CLOUD_RUN_JOBS registry stem is deployment-api-launched;
    an off-pattern live job with no registry hint is adhoc (the job analogue of an unmanaged VM).
    """
    from deployment_api.routes._cloud_run_executions import CloudRunExecutionStatus
    from deployment_api.routes.deployments_inventory import (
        LAUNCHED_BY_ADHOC,
        LAUNCHED_BY_DEPLOYMENT_API,
        build_inventory,
    )

    cr_status = {
        "prd-manifest-consolidator-cefi": CloudRunExecutionStatus(
            job_name="prd-manifest-consolidator-cefi", status="succeeded", last_run_at=None, exit_code=0, log_uri=""
        ),
        "zzqx-000-unreg": CloudRunExecutionStatus(
            job_name="zzqx-000-unreg", status="running", last_run_at=None, exit_code=None, log_uri=""
        ),
    }
    by_name = {i.name: i for i in build_inventory([], cr_status, _FIXED_NOW)}
    assert by_name["prd-manifest-consolidator-cefi"].launched_by == LAUNCHED_BY_DEPLOYMENT_API
    assert by_name["zzqx-000-unreg"].launched_by == LAUNCHED_BY_ADHOC


def test_build_inventory_surfaces_leaked_resources_on_non_running_vms() -> None:
    """WS-D leaked-resource detection is wired into build_inventory: a non-running VM holding a DATA
    disk surfaces has_unreleased_resources + the itemised list; the boot disk is excluded (orphans'
    job); the cost reuses the orphans disk-rate SSOT + is labelled inferred (principle 8)."""
    from deployment_api.routes.deployments_inventory import build_inventory

    vm_details_by_name: dict[str, dict[str, object]] = {
        "adhoc-stopped-vm": {
            "status": "TERMINATED",
            "boot_disk_name": "adhoc-stopped-vm-boot",
            "attached_disk_names": ["adhoc-stopped-vm-boot", "adhoc-stopped-vm-data"],
        },
    }
    disk_details: dict[str, dict[str, object]] = {"adhoc-stopped-vm-data": {"size_gb": 200, "type": "pd-balanced"}}
    items = build_inventory([], {}, _FIXED_NOW, vm_details_by_name, disk_details=disk_details)
    row = next(i for i in items if i.name == "adhoc-stopped-vm")
    assert row.has_unreleased_resources is True
    assert row.unreleased_resources is not None
    leaked = row.unreleased_resources[0]
    assert leaked.type == "DISK"
    assert leaked.name == "adhoc-stopped-vm-data"
    assert leaked.est_monthly_usd == 26.0  # 200 GB * 0.130 (pd-balanced)
    assert leaked.cost_basis == "inferred"


def test_build_inventory_surfaces_orphan_reap_verdict_on_stopped_vm() -> None:
    """Fleet-tab consolidation: a STOPPED/TERMINATED VM's row carries reap_verdict/grace_hours/
    stopped_age_hours/monthly_disk_usd, joined from the SAME orphans SSOT
    (`_fleet_inventory.build_orphan_inventory`) the `/api/fleet/orphans` endpoint uses — the cost
    estimate must match exactly (no second estimator), and an ephemeral VM stopped well past the
    24h grace window verdicts `reap`."""
    from deployment_api.routes.deployments_inventory import build_inventory

    vm_details_by_name: dict[str, dict[str, object]] = {
        "adhoc-stopped-vm": {
            "status": "TERMINATED",
            "boot_disk_name": "adhoc-stopped-vm-boot",
            "last_stop_timestamp": "2026-06-20T12:00:00Z",  # 48h before _FIXED_NOW — past 24h grace
        },
    }
    disk_details: dict[str, dict[str, object]] = {"adhoc-stopped-vm-boot": {"size_gb": 100, "type": "pd-standard"}}
    items = build_inventory([], {}, _FIXED_NOW, vm_details_by_name, disk_details=disk_details)
    row = next(i for i in items if i.name == "adhoc-stopped-vm")
    assert row.reap_verdict == "reap"
    assert row.grace_hours == 24.0
    assert row.stopped_age_hours == pytest.approx(48.0)
    assert row.monthly_disk_usd == 5.2  # 100 GB * 0.052 (pd-standard) — same rate as /api/fleet/orphans


def test_build_inventory_running_vm_has_no_orphan_fields() -> None:
    """A RUNNING VM is not in the orphan candidate set — reap_verdict/grace_hours/stopped_age_hours/
    monthly_disk_usd all stay honestly None (never a fabricated non-orphan default)."""
    from deployment_api.routes.deployments_inventory import build_inventory

    entries = _vm_entries()
    vm_details_by_name: dict[str, dict[str, object]] = {
        "cefi-binance-spot-20260622-014158": {"status": "RUNNING"},
    }
    items = build_inventory(entries, {}, _FIXED_NOW, vm_details_by_name)  # type: ignore[arg-type]
    running = {i.name: i for i in items}["cefi-binance-spot-20260622-014158"]
    assert running.reap_verdict is None
    assert running.grace_hours is None
    assert running.stopped_age_hours is None
    assert running.monthly_disk_usd is None


def test_build_inventory_running_vm_has_no_leaked_resources() -> None:
    """A RUNNING VM with a data disk reports has_unreleased_resources=False (in-use, not leaked);
    a registry VM with no GCE join reports honest None (couldn't determine)."""
    from deployment_api.routes.deployments_inventory import build_inventory

    entries = _vm_entries()
    vm_details_by_name: dict[str, dict[str, object]] = {
        "cefi-binance-spot-20260622-014158": {
            "status": "RUNNING",
            "boot_disk_name": "cefi-binance-spot-20260622-014158-boot",
            "attached_disk_names": ["cefi-binance-spot-20260622-014158-boot", "cefi-data"],
        }
    }
    disk_details: dict[str, dict[str, object]] = {"cefi-data": {"size_gb": 100, "type": "pd-ssd"}}
    items = build_inventory(entries, {}, _FIXED_NOW, vm_details_by_name, disk_details=disk_details)  # type: ignore[arg-type]
    running = {i.name: i for i in items}["cefi-binance-spot-20260622-014158"]
    assert running.has_unreleased_resources is False
    assert running.unreleased_resources is None
    # No join at all → honest None (couldn't determine).
    no_join = build_inventory(entries, {}, _FIXED_NOW)  # type: ignore[arg-type]
    assert next(i for i in no_join if i.name == "defi-paper-trading-20260622").has_unreleased_resources is None


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

    def _fake_upload(bucket: str, path: str, data: bytes, content_type: str | None = None) -> str:
        nonlocal written_bucket, written_path, written_data
        written_bucket, written_path, written_data = bucket, path, data
        return f"gs://{bucket}/{path}"

    with (
        patch.object(_inv_mod, "resolve_bucket_name", return_value="unified-trading-cicd-events"),
        patch.object(_inv_mod, "upload_to_storage", side_effect=_fake_upload),
    ):
        _inv_mod._persist_alert(  # pyright: ignore[reportPrivateUsage]
            alert_class="oom-risk",
            workflow_name="vm-health-cefi-binance-spot",
            severity="CRITICAL",
            message="cefi-binance-spot is oom-risk",
            dedup_key="vm-health-cefi-binance-spot-oom-risk",
        )
    # QG 5.69: bucket comes from resolve_bucket_name(), never a hardcoded literal.
    assert written_bucket == "unified-trading-cicd-events"
    # One object per alert (race fix, todo 6) — no shared "alerts.jsonl" filename to clobber.
    assert written_path.startswith("cicd/alerts/")
    assert written_path.endswith(".jsonl")
    assert "alerts.jsonl" not in written_path
    row = json.loads(written_data.decode("utf-8").strip())
    assert row["event_type"] == "slack_alert"
    assert row["alert_class"] == "oom-risk"
    assert row["repo"] == "deployment-api"
    assert row["subject_repo"] is None  # no repo-scoped subject for a VM-health alert
    assert row["workflow_name"] == "vm-health-cefi-binance-spot"
    assert row["severity"] == "CRITICAL"


def test_persist_alert_writes_subject_repo_distinct_from_emitter() -> None:
    """deployment_alerts_ingestion_completeness_2026_07_20.md todo 4: subject_repo (the repo the
    alert is ABOUT) must be populated distinctly from repo (the emitter, always deployment-api)."""
    from deployment_api.routes import deployments_inventory as _inv_mod

    written_data = b""

    def _fake_upload(bucket: str, path: str, data: bytes, content_type: str | None = None) -> str:
        nonlocal written_data
        written_data = data
        return f"gs://{bucket}/{path}"

    with (
        patch.object(_inv_mod, "resolve_bucket_name", return_value="unified-trading-cicd-events"),
        patch.object(_inv_mod, "upload_to_storage", side_effect=_fake_upload),
    ):
        _inv_mod._persist_alert(  # pyright: ignore[reportPrivateUsage]
            alert_class="cross-repo-regression",
            workflow_name="ci-status-update",
            severity="CRITICAL",
            message="unified-trading-library CI regression",
            dedup_key="qg-fail:unified-trading-library:live-defi-rollout",
            subject_repo="unified-trading-library",
        )
    row = json.loads(written_data.decode("utf-8").strip())
    assert row["repo"] == "deployment-api"
    assert row["subject_repo"] == "unified-trading-library"
    assert row["repo"] != row["subject_repo"]


def test_persist_alert_writes_unique_object_per_call() -> None:
    """deployment_alerts_ingestion_completeness_2026_07_20.md todo 6: one object per alert, not a
    shared per-day filename — two concurrent alerts must land at two DISTINCT paths so neither
    overwrites the other (the read-modify-write race this replaces)."""
    from deployment_api.routes import deployments_inventory as _inv_mod

    written_paths: list[str] = []

    def _fake_upload(bucket: str, path: str, data: bytes, content_type: str | None = None) -> str:
        written_paths.append(path)
        return f"gs://{bucket}/{path}"

    with (
        patch.object(_inv_mod, "resolve_bucket_name", return_value="unified-trading-cicd-events"),
        patch.object(_inv_mod, "upload_to_storage", side_effect=_fake_upload),
    ):
        for _ in range(2):
            _inv_mod._persist_alert(  # pyright: ignore[reportPrivateUsage]
                alert_class="oom-risk",
                workflow_name="vm-health-x",
                severity="CRITICAL",
                message="x is oom-risk",
                dedup_key="vm-health-x-oom-risk",
            )
    assert len(written_paths) == 2
    assert written_paths[0] != written_paths[1]


def test_persist_alert_never_raises_on_storage_failure() -> None:
    """Shard-level isolation: a ledger-write failure never breaks the inventory computation."""
    from deployment_api.routes import deployments_inventory as _inv_mod

    with (
        patch.object(_inv_mod, "resolve_bucket_name", return_value="unified-trading-cicd-events"),
        patch.object(_inv_mod, "upload_to_storage", side_effect=RuntimeError("gcs down")),
    ):
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
        host_metrics_window=[
            {"cpu_pct": 40.0, "mem_pct": 60.0, "sampled_at": "2026-06-22T11:59:00Z"},
            {"cpu_pct": 42.5, "mem_pct": 61.0, "sampled_at": "2026-06-22T12:00:00Z"},
        ],
    )
    _inv_mod._inventory_cache.clear()  # pyright: ignore[reportPrivateUsage]
    _inv_mod._vm_entry_by_name_cache.clear()  # pyright: ignore[reportPrivateUsage]

    with (
        patch("deployment_api.routes.deployments_inventory._cfg") as mock_cfg,
        patch("deployment_api.routes.deployments_inventory._load_registry_entries", return_value=[entry]),
        patch("deployment_api.routes.deployments_inventory.get_vm_instance_details", return_value={}),
        patch("deployment_api.routes.deployments_inventory.latest_execution_by_job", return_value={}),
        # Secondary GCP censuses (services/functions/scheduler/disks/IPs/object-deltas/costs)
        # → honest-empty, credential-free: no real socket to GCP (offline / pytest-socket safe).
        patch_inventory_secondary_census(_inv_mod),
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
    # D.1 rolling window (D.2 STORE) — the last ~10 samples ride through to /detail for the
    # sparkline, not just the single most-recent point.
    assert len(body["host_metrics_window"]) == 2
    assert body["host_metrics_window"][0]["cpu_pct"] == 40.0
    assert body["host_metrics_window"][-1]["sampled_at"] == "2026-06-22T12:00:00Z"


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

    # The live path now reads the registry (``_load_registry_entries``, Firestore-first) and the
    # GCE aggregated-list (``get_vm_instance_details``) as TWO decoupled census futures — patch
    # both seams (the parallel reader itself is covered separately below).
    from deployment_api.routes import deployments_inventory as _inv_mod

    _inv_mod._inventory_cache.clear()  # pyright: ignore[reportPrivateUsage]  # isolate the short-TTL cache

    vm_details_by_name = {e.vm_name: {} for e in entries if e.status == "running"}
    with (
        patch("deployment_api.routes.deployments_inventory._cfg") as mock_cfg,
        patch(
            "deployment_api.routes.deployments_inventory._load_registry_entries",
            return_value=entries,
        ),
        patch(
            "deployment_api.routes.deployments_inventory.get_vm_instance_details",
            return_value=vm_details_by_name,
        ),
        patch(
            "deployment_api.routes.deployments_inventory.latest_execution_by_job",
            return_value=cr_status,
        ),
        # Secondary GCP censuses (services/functions/scheduler/disks/IPs/object-deltas/costs)
        # → honest-empty, credential-free: no real socket to GCP (offline / pytest-socket safe).
        patch_inventory_secondary_census(_inv_mod),
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
# WS-2 date-range overlap (deployment_ui_date_range_filter_and_search) — VM/registry rows
# ---------------------------------------------------------------------------


def test_vm_overlap_basis_no_range_always_overlaps() -> None:
    from deployment_api.routes.deployments_inventory import _vm_overlap_basis

    overlaps, basis = _vm_overlap_basis(
        started_at=datetime(2026, 6, 1, tzinfo=UTC),
        completed_at=None,
        last_heartbeat_at=datetime(2026, 6, 22, tzinfo=UTC),
        now=_FIXED_NOW,
        date_from=None,
        date_to=None,
    )
    assert overlaps is True
    assert basis is None


def test_vm_overlap_basis_no_interval_data_never_filtered_out() -> None:
    """Honest-absence: a row with no started_at at all is never dropped by the date filter."""
    from deployment_api.routes.deployments_inventory import _vm_overlap_basis

    overlaps, basis = _vm_overlap_basis(
        started_at=None,
        completed_at=None,
        last_heartbeat_at=None,
        now=_FIXED_NOW,
        date_from=datetime(2026, 6, 1, tzinfo=UTC),
        date_to=datetime(2026, 6, 10, tzinfo=UTC),
    )
    assert overlaps is True
    assert basis is None


def test_vm_overlap_basis_started_after_range_excluded() -> None:
    from deployment_api.routes.deployments_inventory import _vm_overlap_basis

    overlaps, basis = _vm_overlap_basis(
        started_at=datetime(2026, 6, 15, tzinfo=UTC),
        completed_at=None,
        last_heartbeat_at=_FIXED_NOW,
        now=_FIXED_NOW,
        date_from=datetime(2026, 6, 1, tzinfo=UTC),
        date_to=datetime(2026, 6, 10, tzinfo=UTC),
    )
    assert overlaps is False
    assert basis is None


def test_vm_overlap_basis_terminal_row_inside_range() -> None:
    from deployment_api.routes.deployments_inventory import _vm_overlap_basis

    overlaps, basis = _vm_overlap_basis(
        started_at=datetime(2026, 6, 5, tzinfo=UTC),
        completed_at=datetime(2026, 6, 6, tzinfo=UTC),
        last_heartbeat_at=datetime(2026, 6, 6, tzinfo=UTC),
        now=_FIXED_NOW,
        date_from=datetime(2026, 6, 1, tzinfo=UTC),
        date_to=datetime(2026, 6, 10, tzinfo=UTC),
    )
    assert overlaps is True
    assert basis is None


def test_vm_overlap_basis_terminal_row_completed_before_range() -> None:
    from deployment_api.routes.deployments_inventory import _vm_overlap_basis

    overlaps, basis = _vm_overlap_basis(
        started_at=datetime(2026, 5, 1, tzinfo=UTC),
        completed_at=datetime(2026, 5, 20, tzinfo=UTC),
        last_heartbeat_at=datetime(2026, 5, 20, tzinfo=UTC),
        now=_FIXED_NOW,
        date_from=datetime(2026, 6, 1, tzinfo=UTC),
        date_to=datetime(2026, 6, 10, tzinfo=UTC),
    )
    assert overlaps is False
    assert basis is None


def test_vm_overlap_basis_truly_live_row_always_overlaps_once_started() -> None:
    """No completed_at, fresh (<6h) heartbeat -> open-ended, overlaps any range starting after
    started_at (an unbounded live interval has no upper edge to fall short of)."""
    from deployment_api.routes.deployments_inventory import _vm_overlap_basis

    overlaps, basis = _vm_overlap_basis(
        started_at=datetime(2026, 6, 5, tzinfo=UTC),
        completed_at=None,
        last_heartbeat_at=_FIXED_NOW,  # fresh vs now
        now=_FIXED_NOW,
        date_from=datetime(2026, 6, 1, tzinfo=UTC),
        date_to=datetime(2026, 6, 10, tzinfo=UTC),
    )
    assert overlaps is True
    assert basis is None


def test_vm_overlap_basis_heartbeat_stale_row_uses_last_heartbeat_as_approx_end() -> None:
    """No completed_at, heartbeat >6h stale -> effective_end = last_heartbeat_at, basis=approx
    (the 219-rows-vs-12-actually-running gap the 2026-07-20 audit found)."""
    from deployment_api.routes.deployments_inventory import _vm_overlap_basis

    stale_heartbeat = datetime(2026, 6, 22, 5, 0, 0, tzinfo=UTC)  # 7h before _FIXED_NOW (12:00)
    overlaps, basis = _vm_overlap_basis(
        started_at=datetime(2026, 6, 20, tzinfo=UTC),
        completed_at=None,
        last_heartbeat_at=stale_heartbeat,
        now=_FIXED_NOW,
        date_from=datetime(2026, 6, 23, tzinfo=UTC),  # after the stale heartbeat -> excluded
        date_to=datetime(2026, 6, 25, tzinfo=UTC),
    )
    assert overlaps is False
    assert basis == "approx"

    overlaps2, basis2 = _vm_overlap_basis(
        started_at=datetime(2026, 6, 20, tzinfo=UTC),
        completed_at=None,
        last_heartbeat_at=stale_heartbeat,
        now=_FIXED_NOW,
        date_from=datetime(2026, 6, 21, tzinfo=UTC),  # before the stale heartbeat -> included
        date_to=datetime(2026, 6, 25, tzinfo=UTC),
    )
    assert overlaps2 is True
    assert basis2 == "approx"


def test_parse_date_query_bare_date_and_end_of_day() -> None:
    from deployment_api.routes.deployments_inventory import _parse_date_query

    start = _parse_date_query("2026-06-01", end_of_day=False)
    assert start == datetime(2026, 6, 1, 0, 0, 0, tzinfo=UTC)
    end = _parse_date_query("2026-06-01", end_of_day=True)
    assert end == datetime(2026, 6, 1, 23, 59, 59, 999999, tzinfo=UTC)
    assert _parse_date_query(None, end_of_day=True) is None


def test_parse_date_query_invalid_raises_400() -> None:
    from fastapi import HTTPException

    from deployment_api.routes.deployments_inventory import _parse_date_query

    with pytest.raises(HTTPException) as exc_info:
        _parse_date_query("not-a-date", end_of_day=False)
    assert exc_info.value.status_code == 400


def test_apply_date_range_no_op_without_params() -> None:
    from deployment_api.routes.deployments_inventory import DeploymentItem, _apply_date_range

    items = [
        DeploymentItem(
            name="x", kind="VM", umbrella="BATCH", cloud="GCP", service="x", asset_group="cefi", status="running"
        )
    ]
    assert _apply_date_range(items, _FIXED_NOW, None, None) is items


def test_apply_date_range_never_mutates_cached_item_and_skips_non_vm_kinds() -> None:
    """The inventory cache is shared across concurrent requests with different date ranges — a
    stamped basis must land on a copy, never the cached DeploymentItem itself."""
    from deployment_api.routes.deployments_inventory import DeploymentItem, _apply_date_range

    vm = DeploymentItem(
        name="stale-vm",
        kind="VM",
        umbrella="BATCH",
        cloud="GCP",
        service="stale-vm",
        asset_group="cefi",
        status="running",
        started_at="2026-06-20T00:00:00Z",
        completed_at=None,
        last_heartbeat_at="2026-06-22T05:00:00Z",  # 7h before _FIXED_NOW -> stale
    )
    job = DeploymentItem(
        name="a-job",
        kind="CLOUD_RUN_JOB",
        umbrella="BATCH",
        cloud="GCP",
        service="a-job",
        asset_group="cefi",
        status="succeeded",
    )
    out = _apply_date_range([vm, job], _FIXED_NOW, datetime(2026, 6, 19, tzinfo=UTC), datetime(2026, 6, 23, tzinfo=UTC))
    assert len(out) == 2
    out_vm = next(i for i in out if i.name == "stale-vm")
    assert out_vm.basis == "approx"
    assert out_vm is not vm  # a copy, never a mutation of the shared/cached instance
    assert vm.basis is None  # the original cached object stays untouched
    out_job = next(i for i in out if i.name == "a-job")
    assert out_job is job  # non-VM kinds pass through unchanged (no interval to scope on)


def test_inventory_route_date_range_filters_terminal_vm_rows(client_inventory: TestClient) -> None:
    """Route-level wiring: date_from/date_to excludes a VM whose interval doesn't overlap."""
    from deployment_api.routes import deployments_inventory as _inv_mod

    entries = [
        _FakeEntry(
            vm_name="cefi-in-range",
            started_at="2026-06-05T00:00:00Z",
            completed_at="2026-06-06T00:00:00Z",
            status="failed",
            exit_code=0,
        ),
        _FakeEntry(
            vm_name="defi-out-of-range",
            started_at="2026-05-01T00:00:00Z",
            completed_at="2026-05-02T00:00:00Z",
            status="failed",
            exit_code=0,
        ),
    ]
    _inv_mod._inventory_cache.clear()  # pyright: ignore[reportPrivateUsage]  # isolate the short-TTL cache

    with (
        patch("deployment_api.routes.deployments_inventory._cfg") as mock_cfg,
        patch("deployment_api.routes.deployments_inventory._load_registry_entries", return_value=entries),
        patch("deployment_api.routes.deployments_inventory.get_vm_instance_details", return_value={}),
        patch("deployment_api.routes.deployments_inventory.latest_execution_by_job", return_value={}),
        patch_inventory_secondary_census(_inv_mod),
    ):
        mock_cfg.is_mock_mode.return_value = False
        mock_cfg.require_gcp_project_id.return_value = "test-project"
        resp = client_inventory.get(
            "/api/deployments/inventory", params={"date_from": "2026-06-01", "date_to": "2026-06-10"}
        )
    assert resp.status_code == 200
    names = {i["name"] for i in resp.json()["items"]}
    assert "cefi-in-range" in names
    assert "defi-out-of-range" not in names


def test_inventory_route_invalid_date_returns_400(client_inventory: TestClient) -> None:
    with patch("deployment_api.routes.deployments_inventory._cfg") as mock_cfg:
        mock_cfg.is_mock_mode.return_value = True
        resp = client_inventory.get("/api/deployments/inventory", params={"date_from": "not-a-date"})
    assert resp.status_code == 400


# ---------------------------------------------------------------------------
# WS-2 archive range-read (deployment_ui_date_range_filter_and_search-002) — bypass the default
# 7-day _ARCHIVE_WINDOW_DAYS cap for date-range queries, up to the real 30-day GCS floor
# ---------------------------------------------------------------------------


def test_archive_floor_date_is_29_days_before_now() -> None:
    from deployment_api.routes.deployments_inventory import _archive_floor_date

    assert _archive_floor_date(_FIXED_NOW) == datetime(2026, 5, 24, tzinfo=UTC).date()


def test_load_registry_entries_for_date_range_reads_only_the_requested_bounded_window() -> None:
    """Bounded, day-partitioned read — only the requested days, never the whole corpus."""
    from deployment_api.routes.deployments_inventory import _load_registry_entries_for_date_range

    requested_prefixes: list[str] = []

    @dataclass
    class _Blob:
        name: str

    class _FakeStorage:
        def list_blobs(self, *, bucket: str, prefix: str) -> list[_Blob]:
            requested_prefixes.append(prefix)
            return []

        def download_bytes(self, *, bucket: str, blob_path: str) -> bytes:
            raise KeyError(blob_path)

    with patch("deployment_api.routes.deployments_inventory.get_storage_client", return_value=_FakeStorage()):
        entries, out_of_range = _load_registry_entries_for_date_range(
            _FIXED_NOW,
            datetime(2026, 6, 18, tzinfo=UTC),
            datetime(2026, 6, 20, tzinfo=UTC),
        )
    assert entries == []
    assert out_of_range is False
    # Exactly the 3 requested days — a bounded read, not the whole 30-day corpus.
    assert requested_prefixes == [
        "deployments/archive/2026-06-18/",
        "deployments/archive/2026-06-19/",
        "deployments/archive/2026-06-20/",
    ]


def test_load_registry_entries_for_date_range_out_of_range_clips_to_floor() -> None:
    """date_from predating the 30-day floor -> out_of_range=True, and the read CLIPS to the floor
    day forward rather than attempting the unavailable span."""
    from deployment_api.routes.deployments_inventory import _load_registry_entries_for_date_range

    requested_prefixes: list[str] = []

    @dataclass
    class _Blob:
        name: str

    class _FakeStorage:
        def list_blobs(self, *, bucket: str, prefix: str) -> list[_Blob]:
            requested_prefixes.append(prefix)
            return []

        def download_bytes(self, *, bucket: str, blob_path: str) -> bytes:
            raise KeyError(blob_path)

    with patch("deployment_api.routes.deployments_inventory.get_storage_client", return_value=_FakeStorage()):
        entries, out_of_range = _load_registry_entries_for_date_range(
            _FIXED_NOW,
            datetime(2026, 1, 1, tzinfo=UTC),  # long before the 30-day floor (2026-05-24)
            datetime(2026, 5, 25, tzinfo=UTC),
        )
    assert entries == []
    assert out_of_range is True
    assert requested_prefixes == ["deployments/archive/2026-05-24/", "deployments/archive/2026-05-25/"]


def test_load_registry_entries_for_date_range_beyond_today_clips_to_today() -> None:
    """A date_to beyond today clips to today — never requests a not-yet-written future prefix."""
    from deployment_api.routes.deployments_inventory import _load_registry_entries_for_date_range

    requested_prefixes: list[str] = []

    @dataclass
    class _Blob:
        name: str

    class _FakeStorage:
        def list_blobs(self, *, bucket: str, prefix: str) -> list[_Blob]:
            requested_prefixes.append(prefix)
            return []

        def download_bytes(self, *, bucket: str, blob_path: str) -> bytes:
            raise KeyError(blob_path)

    with patch("deployment_api.routes.deployments_inventory.get_storage_client", return_value=_FakeStorage()):
        _entries, out_of_range = _load_registry_entries_for_date_range(
            _FIXED_NOW,
            datetime(2026, 6, 21, tzinfo=UTC),
            datetime(2026, 7, 1, tzinfo=UTC),  # after _FIXED_NOW's date (2026-06-22)
        )
    assert out_of_range is False
    assert requested_prefixes == ["deployments/archive/2026-06-21/", "deployments/archive/2026-06-22/"]


def test_inventory_route_date_range_merges_archive_range_entries_and_reports_floor(
    client_inventory: TestClient,
) -> None:
    """date_from/date_to reaching beyond the default 7-day census pulls in the extra archived VM
    via the dedicated range read, dedupes against what's already present, and the response reports
    archive_floor / date_range_out_of_range for the UI banner (decision 5)."""
    from deployment_api.routes import deployments_inventory as _inv_mod
    from deployment_api.routes.deployments_inventory import _archive_floor_date

    now = datetime.now(UTC)
    recent_entry = _FakeEntry(
        vm_name="cefi-recent",
        started_at=now.isoformat(),
        completed_at=now.isoformat(),
        status="failed",
        exit_code=0,
    )
    old_entry = _FakeEntry(
        vm_name="cefi-old-in-archive-window",
        started_at=now.isoformat(),
        completed_at=now.isoformat(),
        status="failed",
        exit_code=0,
    )
    _inv_mod._inventory_cache.clear()  # pyright: ignore[reportPrivateUsage]

    with (
        patch("deployment_api.routes.deployments_inventory._cfg") as mock_cfg,
        patch("deployment_api.routes.deployments_inventory._load_registry_entries", return_value=[recent_entry]),
        patch(
            "deployment_api.routes.deployments_inventory._load_registry_entries_for_date_range",
            return_value=([recent_entry, old_entry], False),
        ),
        patch("deployment_api.routes.deployments_inventory.get_vm_instance_details", return_value={}),
        patch("deployment_api.routes.deployments_inventory.latest_execution_by_job", return_value={}),
        patch_inventory_secondary_census(_inv_mod),
    ):
        mock_cfg.is_mock_mode.return_value = False
        mock_cfg.require_gcp_project_id.return_value = "test-project"
        resp = client_inventory.get(
            "/api/deployments/inventory",
            params={"date_from": (now - timedelta(days=10)).date().isoformat(), "date_to": now.date().isoformat()},
        )
    assert resp.status_code == 200
    body = resp.json()
    names = [i["name"] for i in body["items"] if i["kind"] == "VM"]
    assert names.count("cefi-recent") == 1  # present in BOTH sources, never duplicated
    assert "cefi-old-in-archive-window" in names  # only reachable via the range-scoped read
    assert body["archive_floor"] == _archive_floor_date(now).isoformat()
    assert body["date_range_out_of_range"] is False


def test_inventory_route_date_range_out_of_range_flag(client_inventory: TestClient) -> None:
    with patch("deployment_api.routes.deployments_inventory._cfg") as mock_cfg:
        mock_cfg.is_mock_mode.return_value = True
        resp = client_inventory.get("/api/deployments/inventory", params={"date_from": "2000-01-01"})
    assert resp.status_code == 200
    assert resp.json()["date_range_out_of_range"] is True


def test_inventory_route_no_date_params_leaves_archive_floor_none(client_inventory: TestClient) -> None:
    with patch("deployment_api.routes.deployments_inventory._cfg") as mock_cfg:
        mock_cfg.is_mock_mode.return_value = True
        resp = client_inventory.get("/api/deployments/inventory")
    body = resp.json()
    assert body["archive_floor"] is None
    assert body["date_range_out_of_range"] is False


# ---------------------------------------------------------------------------
# WS-2 single-timestamp date-range match (deployment_ui_date_range_filter_and_search-003) —
# unmanaged VMs (no registry interval) + Cloud Run jobs / AWS Batch / Scheduler
# ---------------------------------------------------------------------------


def test_single_timestamp_overlaps_no_range_always_true() -> None:
    from deployment_api.routes.deployments_inventory import _single_timestamp_overlaps

    overlaps, basis = _single_timestamp_overlaps(datetime(2026, 6, 1, tzinfo=UTC), None, None)
    assert overlaps is True
    assert basis is None


def test_single_timestamp_overlaps_no_timestamp_never_filtered_out() -> None:
    from deployment_api.routes.deployments_inventory import _single_timestamp_overlaps

    overlaps, basis = _single_timestamp_overlaps(
        None, datetime(2026, 6, 1, tzinfo=UTC), datetime(2026, 6, 10, tzinfo=UTC)
    )
    assert overlaps is True
    assert basis is None


def test_single_timestamp_overlaps_within_range_is_approx() -> None:
    from deployment_api.routes.deployments_inventory import _single_timestamp_overlaps

    overlaps, basis = _single_timestamp_overlaps(
        datetime(2026, 6, 5, tzinfo=UTC), datetime(2026, 6, 1, tzinfo=UTC), datetime(2026, 6, 10, tzinfo=UTC)
    )
    assert overlaps is True
    assert basis == "approx"


def test_single_timestamp_overlaps_before_range_excluded() -> None:
    from deployment_api.routes.deployments_inventory import _single_timestamp_overlaps

    overlaps, basis = _single_timestamp_overlaps(
        datetime(2026, 5, 1, tzinfo=UTC), datetime(2026, 6, 1, tzinfo=UTC), datetime(2026, 6, 10, tzinfo=UTC)
    )
    assert overlaps is False
    assert basis == "approx"


def test_single_timestamp_overlaps_after_range_excluded() -> None:
    from deployment_api.routes.deployments_inventory import _single_timestamp_overlaps

    overlaps, basis = _single_timestamp_overlaps(
        datetime(2026, 7, 1, tzinfo=UTC), datetime(2026, 6, 1, tzinfo=UTC), datetime(2026, 6, 10, tzinfo=UTC)
    )
    assert overlaps is False
    assert basis == "approx"


def test_apply_date_range_unmanaged_vm_falls_back_to_single_timestamp() -> None:
    """A VM row with NO registry interval (started_at=None, e.g. an unmanaged/AWS-EC2 VM) still
    gets scoped, via last_run_at — never silently exempted from the filter just because it has no
    interval."""
    from deployment_api.routes.deployments_inventory import DeploymentItem, _apply_date_range

    unmanaged_in_range = DeploymentItem(
        name="adhoc-vm-in-range",
        kind="VM",
        umbrella="NONE",
        cloud="GCP",
        service="adhoc-vm-in-range",
        asset_group="",
        status="running",
        last_run_at="2026-06-05T00:00:00Z",
        started_at=None,
    )
    unmanaged_out_of_range = DeploymentItem(
        name="adhoc-vm-out-of-range",
        kind="VM",
        umbrella="NONE",
        cloud="GCP",
        service="adhoc-vm-out-of-range",
        asset_group="",
        status="running",
        last_run_at="2026-05-01T00:00:00Z",
        started_at=None,
    )
    out = _apply_date_range(
        [unmanaged_in_range, unmanaged_out_of_range],
        _FIXED_NOW,
        datetime(2026, 6, 1, tzinfo=UTC),
        datetime(2026, 6, 10, tzinfo=UTC),
    )
    names = {i.name for i in out}
    assert names == {"adhoc-vm-in-range"}
    assert next(i for i in out if i.name == "adhoc-vm-in-range").basis == "approx"


def test_apply_date_range_cloud_run_job_and_scheduler_use_single_timestamp() -> None:
    """CLOUD_RUN_JOB (covers GCP Cloud Run + AWS Batch, same wire kind) and SCHEDULER both scope
    on last_run_at, same as an unmanaged VM."""
    from deployment_api.routes.deployments_inventory import DeploymentItem, _apply_date_range

    job_in_range = DeploymentItem(
        name="manifest-consolidator-cefi",
        kind="CLOUD_RUN_JOB",
        umbrella="BATCH",
        cloud="GCP",
        service="manifest-consolidator",
        asset_group="cefi",
        status="succeeded",
        last_run_at="2026-06-05T00:00:00Z",
    )
    job_out_of_range = DeploymentItem(
        name="manifest-consolidator-defi",
        kind="CLOUD_RUN_JOB",
        umbrella="BATCH",
        cloud="AWS",  # AWS Batch job — same CLOUD_RUN_JOB wire kind
        service="manifest-consolidator",
        asset_group="defi",
        status="succeeded",
        last_run_at="2026-05-01T00:00:00Z",
    )
    scheduler_in_range = DeploymentItem(
        name="vm-zombie-watchdog-scheduler",
        kind="SCHEDULER",
        umbrella="NONE",
        cloud="GCP",
        service="vm-zombie-watchdog",
        asset_group="",
        status="running",
        last_run_at="2026-06-08T00:00:00Z",
    )
    out = _apply_date_range(
        [job_in_range, job_out_of_range, scheduler_in_range],
        _FIXED_NOW,
        datetime(2026, 6, 1, tzinfo=UTC),
        datetime(2026, 6, 10, tzinfo=UTC),
    )
    names = {i.name for i in out}
    assert names == {"manifest-consolidator-cefi", "vm-zombie-watchdog-scheduler"}
    assert all(i.basis == "approx" for i in out)


def test_apply_date_range_no_timestamp_kinds_pass_through_regardless_of_range() -> None:
    """A kind with NO timestamp signal at all (services/functions/...) is never scoped by the
    date filter, even if it were somehow stamped with a value outside the range."""
    from deployment_api.routes.deployments_inventory import DeploymentItem, _apply_date_range

    service = DeploymentItem(
        name="deployment-api",
        kind="CLOUD_RUN_SERVICE",
        umbrella="NONE",
        cloud="GCP",
        service="deployment-api",
        asset_group="",
        status="running",
        last_run_at="2020-01-01T00:00:00Z",  # would be WAY out of range if it were checked
    )
    out = _apply_date_range([service], _FIXED_NOW, datetime(2026, 6, 1, tzinfo=UTC), datetime(2026, 6, 10, tzinfo=UTC))
    assert out == [service]
    assert out[0].basis is None  # passthrough — never stamped, never mutated


# ---------------------------------------------------------------------------
# Census hang isolation (deployment_obs_backend_kinds_health P0) — a slow/hung provider
# degrades to an honest EMPTY census for its OWN kind and never blocks the whole inventory
# (the >240s / 0-byte cockpit hang). WS-B / shard-level failure isolation.
# ---------------------------------------------------------------------------


def test_census_or_degrade_returns_default_on_exception() -> None:
    """A provider census that RAISES degrades to the default — never propagates."""
    from deployment_api.routes import deployments_inventory as _inv_mod

    def _boom() -> list[int]:
        raise RuntimeError("provider exploded")

    fut = _inv_mod._census_pool.submit(_boom)  # pyright: ignore[reportPrivateUsage]
    assert _inv_mod._census_or_degrade("boom", fut, [7]) == [7]  # pyright: ignore[reportPrivateUsage]


def test_census_or_degrade_returns_default_on_timeout() -> None:
    """A census that HANGS past the wall-clock bound degrades to the default without blocking."""
    from deployment_api.routes import deployments_inventory as _inv_mod

    release = threading.Event()

    def _hang() -> list[str]:
        release.wait(timeout=30)  # self-releases as a safety net if the test forgets
        return ["ok"]

    fut = _inv_mod._census_pool.submit(_hang)  # pyright: ignore[reportPrivateUsage]
    try:
        with patch.object(_inv_mod, "_PROVIDER_CENSUS_TIMEOUT_SEC", 0.2):
            started = time.monotonic()
            result = _inv_mod._census_or_degrade("hang", fut, ["degraded"])  # pyright: ignore[reportPrivateUsage]
            elapsed = time.monotonic() - started
        assert result == ["degraded"]
        assert elapsed < 5.0  # returned on the bound, not after the 30s worker self-release
    finally:
        release.set()


def test_inventory_route_hung_provider_degrades_other_kinds_survive(client_inventory: TestClient) -> None:
    """A hung Cloud Run *services* census degrades to empty for that kind while the VM /
    jobs / functions / AWS censuses still return — the endpoint never blocks. P0."""
    from deployment_api.routes import deployments_inventory as _inv_mod

    _inv_mod._inventory_cache.clear()  # pyright: ignore[reportPrivateUsage]
    entry = _FakeEntry(vm_name="cefi-binance-spot-20260622-014158", rows_out=42)
    release = threading.Event()

    def _hanging_services(_project_id: str) -> list[object]:
        release.wait(timeout=30)  # blocks like a wedged control-plane RPC; safety self-release
        return []

    try:
        with (
            patch.object(_inv_mod, "_PROVIDER_CENSUS_TIMEOUT_SEC", 0.3),
            # Secondary GCP censuses (scheduler/disks/IPs/object-deltas/costs, + a benign default
            # for services/functions) → honest-empty, credential-free: no real socket to GCP. The
            # test's OWN list_cloud_run_services hang below is entered LATER, so it wins on that seam.
            patch_inventory_secondary_census(_inv_mod),
            patch("deployment_api.routes.deployments_inventory._cfg") as mock_cfg,
            patch("deployment_api.routes.deployments_inventory._load_registry_entries", return_value=[entry]),
            patch("deployment_api.routes.deployments_inventory.get_vm_instance_details", return_value={}),
            patch("deployment_api.routes.deployments_inventory.latest_execution_by_job", return_value={}),
            patch(
                "deployment_api.routes.deployments_inventory.list_cloud_run_services",
                side_effect=_hanging_services,
            ),
            patch("deployment_api.routes.deployments_inventory.list_cloud_functions", return_value={}),
            patch("deployment_api.routes.deployments_inventory._load_aws_items", return_value=([], {})),
        ):
            mock_cfg.is_mock_mode.return_value = False
            mock_cfg.require_gcp_project_id.return_value = "test-project"
            started = time.monotonic()
            resp = client_inventory.get("/api/deployments/inventory")
            elapsed = time.monotonic() - started
    finally:
        release.set()

    assert resp.status_code == 200
    body = resp.json()
    names = {i["name"] for i in body["items"]}
    assert "cefi-binance-spot-20260622-014158" in names  # VM census survived the services hang
    assert all(i["kind"] != "CLOUD_RUN_SERVICE" for i in body["items"])  # hung kind degraded to empty
    assert elapsed < 10.0  # bounded — not the >240s hang


# ---------------------------------------------------------------------------
# Background refresh pool concurrency bound (deployment_api_inventory_cold_path_concurrent_oom
# P0) — two DIFFERENT cache-key cold/stale refreshes must never run truly concurrently: each
# fans out through its own _census_pool (max_workers=10) plus several per-provider region pools,
# so 2 concurrent full computations OOM-killed the container in prod (17,002 MiB vs a 16,384 MiB
# limit). max_workers=1 on _inventory_refresh_pool restores full serialization across cache keys.
# ---------------------------------------------------------------------------


def test_inventory_refresh_pool_is_bounded_to_one_worker() -> None:
    """Regression guard: the background refresh pool must stay at max_workers=1 so two
    DIFFERENT cache-key refreshes can never fan out their full census concurrently."""
    from deployment_api.routes import deployments_inventory as _inv_mod

    assert _inv_mod._inventory_refresh_pool._max_workers == 1  # pyright: ignore[reportPrivateUsage]


def test_inventory_refresh_pool_serializes_different_cache_keys() -> None:
    """Two DIFFERENT cache-key refreshes submitted back-to-back run ONE AT A TIME, never
    overlapping — asserts the actual serialization behavior (not just the pool's nominal size),
    so a future accidental resize can't silently regress this."""
    from deployment_api.routes import deployments_inventory as _inv_mod

    first_running = threading.Event()
    release_first = threading.Event()
    second_started_while_first_ran = threading.Event()

    def _first() -> str:
        first_running.set()
        release_first.wait(timeout=10)  # self-releases as a safety net if the test forgets
        return "first"

    def _second() -> str:
        if first_running.is_set() and not release_first.is_set():
            second_started_while_first_ran.set()
        return "second"

    fut1 = _inv_mod._inventory_refresh_pool.submit(_first)  # pyright: ignore[reportPrivateUsage]
    try:
        assert first_running.wait(timeout=5)  # the first task has actually started
        fut2 = _inv_mod._inventory_refresh_pool.submit(_second)  # pyright: ignore[reportPrivateUsage]
        time.sleep(0.2)  # give a (buggy, >1-worker) pool a chance to start _second concurrently
        assert not second_started_while_first_ran.is_set()  # second must NOT have started yet
    finally:
        release_first.set()
    assert fut1.result(timeout=5) == "first"
    assert fut2.result(timeout=5) == "second"


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


def test_load_registry_entries_does_not_filter_by_control_plane_presence() -> None:
    """The registry read (RUNNING via ``resolve_active_registry`` + the 7-day archive window) must
    NOT filter by GCE control-plane presence — an active entry whose VM the GCE aggregated-list no
    longer has (hard-killed/OOM/pre-empted before it could self-archive) must still be RETURNED so
    ``build_inventory``/``_composite_health_status`` can classify it ``dead``. Post-decouple this is
    STRUCTURAL: ``_load_registry_entries`` never sees the GCE list (that is a separate census
    future), so control-plane presence cannot affect what the registry read returns.
    """
    from datetime import UTC, datetime

    from deployment_api.routes.deployments_inventory import _load_registry_entries

    # "cefi-dead-vm" is gone from the control plane; the registry read must still return it.
    active_entries = [_FakeEntry(vm_name="cefi-dead-vm"), _FakeEntry(vm_name="cefi-alive-vm")]

    @dataclass
    class _Blob:
        name: str

    class _FakeStorage:
        def list_blobs(self, *, bucket: str, prefix: str) -> list[_Blob]:
            return []  # no archive keys in this window

        def download_bytes(self, *, bucket: str, blob_path: str) -> bytes:
            raise KeyError(blob_path)

    with (
        patch(
            "deployment_api.routes.deployments_inventory.resolve_active_registry",
            return_value=active_entries,
        ),
        patch("deployment_api.routes.deployments_inventory.get_storage_client", return_value=_FakeStorage()),
    ):
        entries = _load_registry_entries(datetime(2026, 6, 22, 12, tzinfo=UTC))

    assert {e.vm_name for e in entries} == {"cefi-dead-vm", "cefi-alive-vm"}


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
    update_time: datetime | None = None
    create_time: datetime | None = None


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
        def list_services(self, request: object, timeout: float | None = None) -> list[_FakeRunV2Service]:
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
        def list_services(self, request: object, timeout: float | None = None) -> list[_FakeRunV2Service]:
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


def _wire_fake_services_client(fake_service: _FakeRunV2Service) -> dict[str, ModuleType]:
    """The sys.modules patch dict shared by every list_cloud_run_services fake-SDK test."""

    class _FakeServicesClient:
        def list_services(self, request: object, timeout: float | None = None) -> list[_FakeRunV2Service]:
            del request, timeout
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
    return {"deployment_service.backends": fake_backends_pkg, "deployment_service.backends._gcp_sdk": fake_module}


def test_list_cloud_run_services_maps_last_deployed_at_from_update_time() -> None:
    """last_deployed_at = update_time (WS-2/#4 — the Tier-0 free-win proxy for latest-revision
    create time, closing the audit-found asymmetry vs ECS_SERVICE)."""
    from deployment_api.routes import _cloud_run_services

    fake_service = _FakeRunV2Service(
        name="projects/test-project/locations/asia-northeast1/services/deployment-api",
        terminal_condition=_FakeTerminalCondition(state=_FakeConditionState(name="CONDITION_SUCCEEDED")),
        update_time=datetime(2026, 6, 20, 8, 0, 0, tzinfo=UTC),
        create_time=datetime(2026, 6, 1, 0, 0, 0, tzinfo=UTC),
    )
    with patch.dict(sys.modules, _wire_fake_services_client(fake_service)):
        result = _cloud_run_services.list_cloud_run_services("test-project")
    assert result[0].last_deployed_at == "2026-06-20T08:00:00+00:00"


def test_list_cloud_run_services_falls_back_to_create_time_when_never_updated() -> None:
    from deployment_api.routes import _cloud_run_services

    fake_service = _FakeRunV2Service(
        name="projects/test-project/locations/asia-northeast1/services/deployment-api",
        terminal_condition=_FakeTerminalCondition(state=_FakeConditionState(name="CONDITION_SUCCEEDED")),
        update_time=None,
        create_time=datetime(2026, 6, 1, 0, 0, 0, tzinfo=UTC),
    )
    with patch.dict(sys.modules, _wire_fake_services_client(fake_service)):
        result = _cloud_run_services.list_cloud_run_services("test-project")
    assert result[0].last_deployed_at == "2026-06-01T00:00:00+00:00"


def test_list_cloud_run_services_last_deployed_at_none_when_neither_timestamp_present() -> None:
    from deployment_api.routes import _cloud_run_services

    fake_service = _FakeRunV2Service(
        name="projects/test-project/locations/asia-northeast1/services/deployment-api",
        terminal_condition=_FakeTerminalCondition(state=_FakeConditionState(name="CONDITION_SUCCEEDED")),
    )
    with patch.dict(sys.modules, _wire_fake_services_client(fake_service)):
        result = _cloud_run_services.list_cloud_run_services("test-project")
    assert result[0].last_deployed_at is None


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
    # Ready + default min-instances 0 → an idle scale-to-zero service (the live row now
    # carries the D.3 sub-taxonomy on composite_health_status, not just a running/pending flag).
    assert item.composite_health_status == "scaled-to-zero"


def test_build_inventory_cloud_run_service_carries_last_deployed_at_as_last_modified_at() -> None:
    """The row builder maps status.last_deployed_at onto DeploymentItem.last_modified_at — the
    existing SSOT field for "deploy time, distinct from last-invoke" (shared with AWS Lambda),
    never a duplicate field. last_run_at stays honestly None (still no true invoke signal)."""
    from deployment_api.routes._cloud_run_services import CloudRunServiceStatus
    from deployment_api.routes.deployments_inventory import build_inventory

    services = [
        CloudRunServiceStatus(
            name="deployment-api",
            ready=True,
            state="CONDITION_SUCCEEDED",
            revision="deployment-api-00007-abc",
            region="asia-northeast1",
            uri="",
            last_deployed_at="2026-06-20T08:00:00+00:00",
        )
    ]
    item = next(
        i for i in build_inventory([], {}, _FIXED_NOW, cloud_run_services=services) if i.kind == "CLOUD_RUN_SERVICE"
    )
    assert item.last_modified_at == "2026-06-20T08:00:00+00:00"
    assert item.last_run_at is None


def test_build_inventory_wires_cloud_run_service_health_taxonomy() -> None:
    """A live Cloud Run service row carries the serving/scaled-to-zero/dead sub-taxonomy.

    Regression for the honesty gap where the classifiers existed but were never called by the
    row builder — service rows only ever showed a binary running/pending status.
    """
    from deployment_api.routes._cloud_run_services import CloudRunServiceStatus
    from deployment_api.routes.deployments_inventory import build_inventory

    def _svc(name: str, *, ready: bool, min_instances: int) -> CloudRunServiceStatus:
        return CloudRunServiceStatus(
            name=name,
            ready=ready,
            state="CONDITION_SUCCEEDED" if ready else "CONDITION_FAILED",
            revision=f"{name}-00001-abc",
            region="asia-northeast1",
            uri="",
            min_instance_count=min_instances,
        )

    services = [
        _svc("market-data-query", ready=True, min_instances=2),  # always-on → serving
        _svc("alerting", ready=True, min_instances=0),  # ready, idle → scaled-to-zero
        _svc("broken-service", ready=False, min_instances=1),  # revision failed → dead
    ]
    by_name = {i.name: i for i in build_inventory([], {}, _FIXED_NOW, cloud_run_services=services)}
    assert by_name["market-data-query"].composite_health_status == "serving"
    assert by_name["alerting"].composite_health_status == "scaled-to-zero"
    assert by_name["broken-service"].composite_health_status == "dead"


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


# ---------------------------------------------------------------------------
# GET /deployments/{name}/run-log/metadata — WS-4 decision 2 read-path resolution
# ---------------------------------------------------------------------------


def test_run_log_metadata_live_path_resolved(client_inventory: TestClient) -> None:
    """Live path hit: response carries its size/last-modified + location="live"."""
    from unified_trading_library import BlobMetadata

    from deployment_api.routes._run_log_resolution import RunLogLocation

    live_meta = BlobMetadata(
        name="vm-logs/cefi-binance-spot-20260622-014158/run.log",
        bucket="deployment-scripts-test",
        size=842_331,
        last_modified="2026-07-21T04:00:00Z",
    )
    with (
        patch("deployment_api.routes.deployments_inventory._cfg") as mock_cfg,
        patch(
            "deployment_api.routes.deployments_inventory.resolve_run_log_location",
            return_value=RunLogLocation(
                uri="gs://deployment-scripts-test/vm-logs/cefi-binance-spot-20260622-014158/run.log",
                location="live",
                metadata=live_meta,
            ),
        ) as mock_resolve,
    ):
        mock_cfg.is_mock_mode.return_value = False
        mock_cfg.require_gcp_project_id.return_value = "test-project"
        resp = client_inventory.get("/api/deployments/cefi-binance-spot-20260622-014158/run-log/metadata")
    assert resp.status_code == 200
    body = resp.json()
    assert body["exists"] is True
    assert body["location"] == "live"
    assert body["size_bytes"] == 842_331
    assert body["last_modified"] == "2026-07-21T04:00:00Z"
    mock_resolve.assert_called_once_with("cefi-binance-spot-20260622-014158", "test-project")


def test_run_log_metadata_honest_absence_when_neither_path_exists(client_inventory: TestClient) -> None:
    """Neither live nor archive object exists: honest exists=False, never a dead link."""
    from deployment_api.routes._run_log_resolution import RunLogLocation

    with (
        patch("deployment_api.routes.deployments_inventory._cfg") as mock_cfg,
        patch(
            "deployment_api.routes.deployments_inventory.resolve_run_log_location",
            return_value=RunLogLocation(
                uri="gs://deployment-scripts-test/log-archive/final/some-old-vm/run.log",
                location="archive",
                metadata=None,
            ),
        ),
    ):
        mock_cfg.is_mock_mode.return_value = False
        mock_cfg.require_gcp_project_id.return_value = "test-project"
        resp = client_inventory.get("/api/deployments/some-old-vm/run-log/metadata")
    assert resp.status_code == 200
    body = resp.json()
    assert body["exists"] is False
    assert body["size_bytes"] is None


# ---------------------------------------------------------------------------
# GET /deployments/{name}/run-log/tail — bounded byte-range tail
# ---------------------------------------------------------------------------


def test_run_log_tail_live_path_resolved(client_inventory: TestClient) -> None:
    """Live path hit: bounded tail read + line split, capped to the configured line count."""
    from unified_trading_library import BlobMetadata

    from deployment_api.routes._run_log_resolution import RunLogLocation

    live_meta = BlobMetadata(
        name="vm-logs/cefi-binance-spot-20260622-014158/run.log",
        bucket="deployment-scripts-test",
        size=10_000_000,
        last_modified="2026-07-21T04:00:00Z",
    )
    with (
        patch("deployment_api.routes.deployments_inventory._cfg") as mock_cfg,
        patch(
            "deployment_api.routes.deployments_inventory.resolve_run_log_location",
            return_value=RunLogLocation(
                uri="gs://deployment-scripts-test/vm-logs/cefi-binance-spot-20260622-014158/run.log",
                location="live",
                metadata=live_meta,
            ),
        ) as mock_resolve,
        patch(
            "deployment_api.routes.deployments_inventory.read_run_log_tail",
            return_value=(["line1", "line2", "line3"], 262_144),
        ) as mock_tail,
    ):
        mock_cfg.is_mock_mode.return_value = False
        mock_cfg.require_gcp_project_id.return_value = "test-project"
        mock_cfg.run_log_tail_max_lines = 300
        mock_cfg.run_log_tail_max_bytes = 262_144
        resp = client_inventory.get("/api/deployments/cefi-binance-spot-20260622-014158/run-log/tail")
    assert resp.status_code == 200
    body = resp.json()
    assert body["exists"] is True
    assert body["location"] == "live"
    assert body["size_bytes"] == 10_000_000
    assert body["lines"] == ["line1", "line2", "line3"]
    assert body["line_count"] == 3
    assert body["tail_bytes"] == 262_144
    mock_resolve.assert_called_once_with("cefi-binance-spot-20260622-014158", "test-project")
    mock_tail.assert_called_once_with(
        "gs://deployment-scripts-test/vm-logs/cefi-binance-spot-20260622-014158/run.log",
        10_000_000,
        max_bytes=262_144,
        max_lines=300,
    )


def test_run_log_tail_honest_absence_when_neither_path_exists(client_inventory: TestClient) -> None:
    """Neither live nor archive object exists: honest exists=False, no GCS read attempted."""
    from deployment_api.routes._run_log_resolution import RunLogLocation

    with (
        patch("deployment_api.routes.deployments_inventory._cfg") as mock_cfg,
        patch(
            "deployment_api.routes.deployments_inventory.resolve_run_log_location",
            return_value=RunLogLocation(
                uri="gs://deployment-scripts-test/log-archive/final/some-old-vm/run.log",
                location="archive",
                metadata=None,
            ),
        ),
        patch("deployment_api.routes.deployments_inventory.read_run_log_tail") as mock_tail,
    ):
        mock_cfg.is_mock_mode.return_value = False
        mock_cfg.require_gcp_project_id.return_value = "test-project"
        mock_cfg.run_log_tail_max_lines = 300
        resp = client_inventory.get("/api/deployments/some-old-vm/run-log/tail")
    assert resp.status_code == 200
    body = resp.json()
    assert body["exists"] is False
    assert body["lines"] == []
    assert body["line_count"] == 0
    mock_tail.assert_not_called()


def test_run_log_tail_lines_query_param_clamped_to_config_cap(client_inventory: TestClient) -> None:
    """A ``lines`` query above the configured cap is clamped, never passed through raw."""
    from unified_trading_library import BlobMetadata

    from deployment_api.routes._run_log_resolution import RunLogLocation

    live_meta = BlobMetadata(
        name="vm-logs/vm-x/run.log", bucket="deployment-scripts-test", size=1000, last_modified="2026-07-21T04:00:00Z"
    )
    with (
        patch("deployment_api.routes.deployments_inventory._cfg") as mock_cfg,
        patch(
            "deployment_api.routes.deployments_inventory.resolve_run_log_location",
            return_value=RunLogLocation(
                uri="gs://deployment-scripts-test/vm-logs/vm-x/run.log", location="live", metadata=live_meta
            ),
        ),
        patch(
            "deployment_api.routes.deployments_inventory.read_run_log_tail",
            return_value=([], 0),
        ) as mock_tail,
    ):
        mock_cfg.is_mock_mode.return_value = False
        mock_cfg.require_gcp_project_id.return_value = "test-project"
        mock_cfg.run_log_tail_max_lines = 300
        mock_cfg.run_log_tail_max_bytes = 262_144
        resp = client_inventory.get("/api/deployments/cefi-binance-spot-20260622-014158/run-log/tail?lines=5000")
    assert resp.status_code == 200
    mock_tail.assert_called_once_with(
        "gs://deployment-scripts-test/vm-logs/vm-x/run.log",
        1000,
        max_bytes=262_144,
        max_lines=300,
    )


# ---------------------------------------------------------------------------
# GET /deployments/{name}/run-log/download — short-lived signed URL (decision 4)
# ---------------------------------------------------------------------------


def test_run_log_download_live_path_resolved(client_inventory: TestClient) -> None:
    """Live path hit: a signed download URL is generated for the resolved bucket/object."""
    from unified_trading_library import BlobMetadata

    from deployment_api.routes._run_log_resolution import RunLogLocation

    live_meta = BlobMetadata(
        name="vm-logs/cefi-binance-spot-20260622-014158/run.log",
        bucket="deployment-scripts-test",
        size=842_331,
        last_modified="2026-07-21T04:00:00Z",
    )
    with (
        patch("deployment_api.routes.deployments_inventory._cfg") as mock_cfg,
        patch(
            "deployment_api.routes.deployments_inventory.resolve_run_log_location",
            return_value=RunLogLocation(
                uri="gs://deployment-scripts-test/vm-logs/cefi-binance-spot-20260622-014158/run.log",
                location="live",
                metadata=live_meta,
            ),
        ) as mock_resolve,
        patch(
            "deployment_api.routes.deployments_inventory.generate_download_url",
            return_value="https://storage.googleapis.com/deployment-scripts-test/vm-logs/cefi-binance-spot-20260622-014158/run.log?X-Goog-Signature=abc",
        ) as mock_signed_url,
    ):
        mock_cfg.is_mock_mode.return_value = False
        mock_cfg.require_gcp_project_id.return_value = "test-project"
        mock_cfg.run_log_download_url_expiry_minutes = 15
        resp = client_inventory.get("/api/deployments/cefi-binance-spot-20260622-014158/run-log/download")
    assert resp.status_code == 200
    body = resp.json()
    assert body["exists"] is True
    assert body["location"] == "live"
    assert body["download_url"].startswith("https://storage.googleapis.com/")
    assert body["expires_in_seconds"] == 900
    mock_resolve.assert_called_once_with("cefi-binance-spot-20260622-014158", "test-project")
    mock_signed_url.assert_called_once_with(
        "deployment-scripts-test",
        "vm-logs/cefi-binance-spot-20260622-014158/run.log",
        expiry_minutes=15,
    )


def test_run_log_download_honest_absence_when_neither_path_exists(client_inventory: TestClient) -> None:
    """Neither live nor archive object exists: honest exists=False, no signed URL generated."""
    from deployment_api.routes._run_log_resolution import RunLogLocation

    with (
        patch("deployment_api.routes.deployments_inventory._cfg") as mock_cfg,
        patch(
            "deployment_api.routes.deployments_inventory.resolve_run_log_location",
            return_value=RunLogLocation(
                uri="gs://deployment-scripts-test/log-archive/final/some-old-vm/run.log",
                location="archive",
                metadata=None,
            ),
        ),
        patch("deployment_api.routes.deployments_inventory.generate_download_url") as mock_signed_url,
    ):
        mock_cfg.is_mock_mode.return_value = False
        mock_cfg.require_gcp_project_id.return_value = "test-project"
        resp = client_inventory.get("/api/deployments/some-old-vm/run-log/download")
    assert resp.status_code == 200
    body = resp.json()
    assert body["exists"] is False
    assert body["download_url"] == ""
    mock_signed_url.assert_not_called()

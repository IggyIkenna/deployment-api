# Epic: observability_master
# Lifecycle: permanent
"""Unified deployment inventory — VMs + Cloud Run jobs at /repos grade.

The deployment-observability surface: every compute unit (a **VM** from the
deployment registry or a **Cloud Run job** from the classified ``CLOUD_RUN_JOBS``
registry) classified under exactly one live/batch/paper/experiment **umbrella** x
cloud x service x asset_group, with live status / last-run / exit_code / heartbeat.

The ``/repos`` page is the gold standard (overview + per-target detail); this is its
deployments-axis equivalent. GCP (VMs + Cloud Run jobs) and AWS (EC2 backfill VMs +
Batch Fargate jobs, ``cloud=AWS``) ride the same ``DeploymentItem`` contract so the UI
renders both clouds uniformly (Phase 5 parity).

Routes (collision-free with the existing ``routes/deployments/`` service-deploy CRUD
package that already owns ``GET /api/deployments`` + ``/api/deployments/{id}``):

* ``GET /api/deployments/inventory`` — the unified, filterable inventory.
* ``GET /api/deployments/umbrella/{umbrella}/summary`` — the per-umbrella rollup
  (the /repos-overview equivalent).
* ``GET /api/deployments/{name}/detail`` — per-target drill-down (D.1 metrics vector)
  for the popover; the thin list above stays composite + headline fields only.

Reuse: VM rows come from the SAME deployment registry ``/api/vm-deployments`` reads
(``DeploymentsRegistry``); Cloud Run rows census the LIVE job list from
``latest_execution_by_job`` (``run_v2.JobsClient.list_jobs``) — ``CLOUD_RUN_JOBS`` is a
classification HINT (stem match), not an allow-list, so an off-pattern live job still
gets a row (degrades to the static registry only when the live list itself fails).
Classification is the single deployment-service ``classify_deployment_target``
resolver — never re-derived here.

SSOT: ``plans/active/deployment_observability_parity_live_batch_paper_2026_06_22.md``
Phase 1.
"""

from __future__ import annotations

import json
import logging
import threading
import time
from concurrent.futures import ThreadPoolExecutor
from datetime import UTC, datetime, timedelta
from functools import partial
from typing import cast

from deployment_service.cloud_run_job_registry import CLOUD_RUN_JOBS
from deployment_service.deployment_classification import (
    UnclassifiedDeploymentError,
    classify_deployment_target,
)
from deployment_service.deployments_registry import (
    ACTIVE_PREFIX,
    ARCHIVE_PREFIX,
    DEFAULT_BUCKET,
    DeploymentRegistryEntry,
    vm_run_log_rolling_uri,
)
from fastapi import APIRouter, HTTPException, Query
from pydantic import BaseModel, Field
from unified_api_contracts import (
    DeploymentCloud,
    DeploymentKind,
    DeploymentTarget,
    DeploymentUmbrella,
    LifecycleClass,
    VmPrefixSpec,
    classify_vm_name,
)
from unified_trading_library import StorageClient, get_storage_client

from deployment_api.deployment_api_config import DeploymentApiConfig
from deployment_api.routes._aws_deployments import load_aws_inventory
from deployment_api.routes._cloud_run_executions import (
    CloudRunExecutionStatus,
    latest_execution_by_job,
)
from deployment_api.routes._gcp_cloud_functions import (
    CloudFunctionStatus,
    list_cloud_functions,
)
from deployment_api.vm_utils import get_vm_instance_details

router = APIRouter()
logger = logging.getLogger(__name__)

_cfg = DeploymentApiConfig()

# Heartbeat age beyond which a running VM is treated as stale (mirrors
# vm_deployments._calculate_health_status' 15-min staleness window).
_STALE_HEARTBEAT_MINUTES = 15

# Honest, minimal prefix→lifecycle registry — the SAME pattern as
# ``_fleet_census._CENSUS_PREFIX_REGISTRY`` (NOT a copy of deployment-service's
# ~50-entry ``VM_PREFIX_TO_BUCKET``, which would drift + couple us to a service
# repo). Only the live-today prefixes the inventory needs to umbrella correctly:
# the paper/live/recursive long-lived clusters + the obvious ephemeral/scheduled
# buckets. Longest-prefix-match wins (UAC ``classify_vm_name``). An unregistered
# prefix degrades to the EPHEMERAL_BATCH default below.
_VM_PREFIX_REGISTRY: dict[str, VmPrefixSpec] = {
    "planning": VmPrefixSpec(bucket=None, lifecycle_class=LifecycleClass.LONG_LIVED_LIVE),
    "human-planning": VmPrefixSpec(bucket=None, lifecycle_class=LifecycleClass.LONG_LIVED_LIVE),
    "agent-orchestrator": VmPrefixSpec(bucket=None, lifecycle_class=LifecycleClass.LONG_LIVED_LIVE),
    "strategy-live-": VmPrefixSpec(bucket=None, lifecycle_class=LifecycleClass.LONG_LIVED_LIVE),
    "defi-recursive-": VmPrefixSpec(bucket=None, lifecycle_class=LifecycleClass.LONG_LIVED_LIVE),
    # Paper launchers — the umbrella override is applied in classify_deployment_target
    # via the PAPER_PREFIXES match; the lifecycle here only seeds the resolver.
    "strategy-paper-": VmPrefixSpec(bucket=None, lifecycle_class=LifecycleClass.LONG_LIVED_LIVE),
    "defi-paper-": VmPrefixSpec(bucket=None, lifecycle_class=LifecycleClass.SCHEDULED_RECURRING),
    "funding-ensemble-paper-": VmPrefixSpec(bucket=None, lifecycle_class=LifecycleClass.SCHEDULED_RECURRING),
    # Experiments.
    "ml-train-": VmPrefixSpec(bucket=None, lifecycle_class=LifecycleClass.EPHEMERAL_EXPERIMENT),
    "strategy-backtest-": VmPrefixSpec(bucket=None, lifecycle_class=LifecycleClass.EPHEMERAL_EXPERIMENT),
    "execution-backtest-": VmPrefixSpec(bucket=None, lifecycle_class=LifecycleClass.EPHEMERAL_EXPERIMENT),
    "exp-": VmPrefixSpec(bucket=None, lifecycle_class=LifecycleClass.EPHEMERAL_EXPERIMENT),
    # Scheduled daemons.
    "vm-zombie-watchdog-": VmPrefixSpec(bucket=None, lifecycle_class=LifecycleClass.SCHEDULED_RECURRING),
    "manifest-consolidator-": VmPrefixSpec(bucket=None, lifecycle_class=LifecycleClass.SCHEDULED_RECURRING),
    "sports-scheduler-": VmPrefixSpec(bucket=None, lifecycle_class=LifecycleClass.SCHEDULED_RECURRING),
}

# Unknown prefix → batch (the honest default: an unregistered VM is almost always
# an ephemeral data-pipeline backfill, which IS EPHEMERAL_BATCH → BATCH umbrella).
_DEFAULT_LIFECYCLE = LifecycleClass.EPHEMERAL_BATCH

# Registry-read parallelism + a short-TTL inventory cache.
#
# The naive path read ~hundreds of per-VM registry JSONs SEQUENTIALLY over a
# transpacific GCS hop (291-VM census + 7-day archive) → >100s, so the cockpit
# Live/Batch/Paper tabs timed out. Two compounding fixes:
#   1. Parallel per-object reads (``_GCS_READ_WORKERS`` ThreadPool — the GCS-object-ops
#      pattern: GCS REST releases the GIL → true thread parallelism on I/O), and
#   2. a short-TTL in-process cache so the cockpit's repeated polls are instant and a
#      thundering herd of concurrent loads collapses to ONE cold read (lock-guarded).
_GCS_READ_WORKERS = 32
_INVENTORY_TTL_SEC = 45.0
_ARCHIVE_WINDOW_DAYS = 7

_inventory_cache: dict[str, tuple[float, list[DeploymentItem]]] = {}
_inventory_lock = threading.Lock()
# cache keys with an in-flight background refresh (so we kick off exactly one).
_inventory_refreshing: set[str] = set()
_inventory_refresh_pool = ThreadPoolExecutor(max_workers=2, thread_name_prefix="inv-refresh")

# VM registry entry by vm_name — piggybacks on the SAME GCP VM census _compute_inventory
# already runs every cache cycle (zero new bucket walks); lets the ``/{id}/detail`` drill-down
# serve the D.1 metrics vector (cpu/mem/disk/io/net + workload liveness) that live on
# ``DeploymentRegistryEntry`` but aren't carried onto the thin-list ``DeploymentItem``.
# Entries are indexed by ``vm_name`` (registry objects are keyed by ``deployment_id``, which
# a caller of ``/{id}/detail`` doesn't know), so this is a small derived side-cache, not a
# second source of truth.
_vm_entry_by_name_cache: dict[str, DeploymentRegistryEntry] = {}
_vm_entry_by_name_lock = threading.Lock()


class DeploymentItem(BaseModel):  # CORRECT-LOCAL: FastAPI API contract model
    """One classified compute unit in the unified inventory (VM or Cloud Run job).

    The wire shape the deployment-ui Deployments page consumes per row. Mirrors the
    classification fields of UAC ``DeploymentTarget`` + the live runtime fields.
    """

    name: str
    kind: str  # VM|CLOUD_RUN_JOB|CLOUD_RUN_SERVICE|ECS_SERVICE|LAMBDA|CLOUD_FUNCTION (UAC DeploymentKind)
    umbrella: str  # "LIVE" | "BATCH" | "PAPER" | "EXPERIMENT"
    cloud: str  # "GCP" | "AWS"
    service: str
    asset_group: str
    status: str  # running|succeeded|failed|stopped|stale|pending|unknown
    last_run_at: str | None = None
    exit_code: int | None = None
    heartbeat_age_seconds: int | None = None
    captured_progress: int | None = None  # rows_out for a VM backfill; None for jobs
    run_log_uri: str = ""
    # Tier-0 free wins — already fetched by the GCE aggregated-list / registry entry,
    # previously discarded. None for kinds without the underlying source (Cloud Run
    # jobs have no GCE instance / registry row-counters).
    rows_in: int | None = None
    rows_error: int | None = None
    events_emitted: int | None = None
    uptime_hours: float | None = None  # started_at -> completed_at (or now if still running)
    machine_type: str | None = None  # GCE aggregated-list, e.g. "e2-highmem-8"
    zone: str | None = None  # GCE aggregated-list zone name
    health_status: str | None = None  # raw GCE instance status (RUNNING/TERMINATED/...)
    boot_disk_name: str | None = None
    labels: dict[str, str] | None = None
    composite_health_status: str | None = None  # D.3 VM composite (hung|disk-full|oom-risk|working|unknown); n/a=None
    # AWS Tier-0 free wins — already fetched by the ECS/Lambda census, previously
    # discarded once collapsed into ``status``. None for kinds without the
    # underlying source (GCP kinds, EC2 VMs, Batch/Cloud-Run jobs).
    cluster: str | None = None  # ECS_SERVICE: owning cluster name
    desired_count: int | None = None  # ECS_SERVICE: desired task count (0 = scaled to zero)
    running_count: int | None = None  # ECS_SERVICE: currently running task count
    task_definition_revision: int | None = None  # ECS_SERVICE: active task-def revision
    runtime: str | None = None  # LAMBDA: declared runtime, "" for a container-image function
    memory_size_mb: int | None = None  # LAMBDA: configured memory
    package_type: str | None = None  # LAMBDA: "Zip" or "Image"


class DeploymentInventoryResponse(BaseModel):  # CORRECT-LOCAL: FastAPI API contract model
    """The unified deployment inventory (VMs + Cloud Run jobs), post-filter."""

    items: list[DeploymentItem] = Field(default_factory=list)  # type: ignore[reportUnknownVariableType]
    total: int
    vm_count: int
    cloud_run_job_count: int
    # Per-kind rollup across all 6 DeploymentKind values (VM/CLOUD_RUN_JOB/CLOUD_RUN_SERVICE/
    # ECS_SERVICE/LAMBDA/CLOUD_FUNCTION) — additive alongside vm_count/cloud_run_job_count
    # (kept for back-compat) so a new kind's census only needs to start emitting rows; a kind
    # with a failed/not-yet-shipped census is simply absent from the map (honest degradation,
    # never a KeyError or a fabricated zero for a kind the caller didn't ask about).
    counts_by_kind: dict[str, int] = Field(default_factory=dict)  # type: ignore[reportUnknownVariableType]


class DeploymentDetailResponse(BaseModel):  # CORRECT-LOCAL: FastAPI API contract model
    """Per-target drill-down (parent plan D.2 API layer) — the deep D.1 metrics vector

    alongside the thin-list item. ``/deployments/inventory`` intentionally keeps
    ``DeploymentItem`` to composite + headline fields so the list payload stays small at
    ~200-target scale; the metrics vector below lives here instead.

    HONEST SCOPE NOTE: these are the single most-recent sample stamped onto the registry
    entry each heartbeat tick (overwritten in place) — NOT yet a persisted rolling window
    of samples. The parent plan's D.2 STORE design calls for keeping the last ~10 samples
    on the registry entry so ``mem_slope`` / "sustained idle" have a trend to plot; that
    persistence hasn't shipped (see this plan's new rolling-window-persistence todo), so
    the popover gets a live point-in-time reading today, a sparkline once that lands.
    All metrics fields are ``None`` for a kind without D.1 capture (Cloud Run/ECS/Lambda/
    Cloud Function — VM-only for now, see parent D.2 CAPTURE) or a VM absent from this
    cycle's census (honest absence, never a fabricated 0.0).
    """

    item: DeploymentItem
    cpu_pct: float | None = None
    mem_pct: float | None = None
    mem_slope: float | None = None
    disk_pct: float | None = None
    io_write_rate_bytes_sec: float | None = None
    net_recv_rate_bytes_sec: float | None = None
    workload_alive: bool | None = None


class UmbrellaStatusFailure(BaseModel):  # CORRECT-LOCAL: FastAPI API contract model
    """The last/most-recent failing target in an umbrella summary."""

    name: str
    exit_code: int | None = None
    last_run_at: str | None = None


class UmbrellaSummaryResponse(BaseModel):  # CORRECT-LOCAL: FastAPI API contract model
    """Per-umbrella rollup — the /repos-overview equivalent."""

    umbrella: str
    total: int
    counts_by_status: dict[str, int] = Field(default_factory=dict)  # type: ignore[reportUnknownVariableType]
    stale_count: int
    last_failure: UmbrellaStatusFailure | None = None


# ---------------------------------------------------------------------------
# VM classification + runtime status
# ---------------------------------------------------------------------------


def _vm_lifecycle_class(vm_name: str) -> str:
    """Lifecycle-class string for a VM name (the honest local registry + default).

    Reuses the curated ``_fleet_census`` prefix registry + UAC longest-prefix
    matcher; an unregistered prefix degrades to EPHEMERAL_BATCH (the same honest
    default the fleet census uses — most unregistered VMs are batch backfills).
    """
    try:
        return classify_vm_name(vm_name, _VM_PREFIX_REGISTRY).value
    except ValueError:
        return _DEFAULT_LIFECYCLE.value


def _classify_vm(vm_name: str) -> DeploymentTarget:
    """Classify a VM into a DeploymentTarget via the single deployment-service resolver."""
    return classify_deployment_target(
        vm_name,
        lifecycle_class=_vm_lifecycle_class(vm_name),
        cloud=DeploymentCloud.GCP,
        kind=DeploymentKind.VM,
    )


def classify_vm_target(vm_name: str) -> DeploymentTarget:
    """Public: classify a VM/deployment name → its DeploymentTarget (umbrella/service/asset_group).

    Reuses the inventory's curated lifecycle resolver + the single
    ``classify_deployment_target`` resolver, so a consumer (the per-deployment
    freshness endpoint) never re-derives classification.
    """
    return _classify_vm(vm_name)


def active_registry_vm_names() -> set[str]:
    """The set of VM names with an ACTIVE deployment-registry entry (parallel-read).

    The "registered" set for cross-cloud reconciliation — a RUNNING GCE VM absent
    from this set is UNKNOWN (running but unregistered); an entry here with no running
    VM is EXPECTED-MISSING. Reuses the same parallel GCS reader as the inventory.
    """
    client = get_storage_client()
    keys = _list_json_keys(client, DEFAULT_BUCKET, ACTIVE_PREFIX)
    return {e.vm_name for e in _download_entries_parallel(client, DEFAULT_BUCKET, keys)}


def _parse_iso(raw: str | None) -> datetime | None:
    """Parse an ISO-8601 timestamp (Z-suffixed or offset-aware); None on any parse failure."""
    if not raw:
        return None
    try:
        parsed = datetime.fromisoformat(raw.replace("Z", "+00:00"))
    except ValueError:
        return None
    return parsed if parsed.tzinfo is not None else parsed.replace(tzinfo=UTC)


def _heartbeat_age_seconds(entry: DeploymentRegistryEntry, now: datetime) -> int | None:
    """Seconds since the registry entry's last heartbeat, or None if unparseable."""
    last_hb = _parse_iso(entry.last_heartbeat_at)
    if last_hb is None:
        return None
    return max(0, int((now - last_hb).total_seconds()))


def _vm_status(entry: DeploymentRegistryEntry, hb_age_seconds: int | None) -> str:
    """Wire status for a VM registry entry (exit-code-aware, stale-aware).

    A terminal entry (``completed``/``failed``) maps from its exit_code: 0 →
    ``succeeded``, non-zero (incl. 137 OOM) → ``failed``. A running entry whose
    heartbeat exceeds the staleness window is ``stale``; otherwise ``running``.
    """
    status = entry.status
    if status in ("completed", "failed"):
        if entry.exit_code is None:
            return "stopped"
        return "succeeded" if entry.exit_code == 0 else "failed"
    if status == "running":
        if hb_age_seconds is not None and hb_age_seconds > _STALE_HEARTBEAT_MINUTES * 60:
            return "stale"
        return "running"
    return status or "unknown"


def _uptime_hours(entry: DeploymentRegistryEntry, now: datetime) -> float | None:
    """Wall-clock hours run — ``started_at`` to ``completed_at`` (or ``now`` if still running)."""
    started = _parse_iso(entry.started_at)
    if started is None:
        return None
    ended = _parse_iso(entry.completed_at) or now
    return max(0.0, (ended - started).total_seconds() / 3600.0)


# D.3 v1 defaults (documented, not a global CPU cut — parent WS-D.0 principle 1).
_DISK_FULL_PCT_THRESHOLD = 90.0
_OOM_RISK_MEM_PCT_FLOOR = 80.0


def _has_d1_metrics(entry: DeploymentRegistryEntry) -> bool:
    """True iff the entry carries a real D.1 ``/proc`` sample.

    Pre-2026-07-09 registry rows default every D.1 field to 0.0 (honestly-unknown,
    per the dataclass's own docstring, never fabricated); six simultaneous real
    zeros — including ``disk_pct`` (an empty disk is not realistic) — is the
    legacy-row signature, not a genuine idle sample.
    """
    return any(
        (
            entry.cpu_pct,
            entry.mem_pct,
            entry.mem_slope,
            entry.disk_pct,
            entry.io_write_rate_bytes_sec,
            entry.net_recv_rate_bytes_sec,
        )
    )


def _composite_health_status(
    entry: DeploymentRegistryEntry,
    hb_age_seconds: int | None,
    *,
    control_plane_running: bool | None = None,
) -> str | None:
    """D.3 VM composite health — resource-in-band AND a work signal advancing
    (parent WS-D.0 principle 1), computed only for a currently ``running`` entry
    (a terminal entry's health is already the exit_code/status).

    v1 covers the 5 states whose signal exists today: ``dead`` / ``hung`` /
    ``disk-full`` / ``oom-risk`` / ``working``. ``stalled`` and ``workload-dead``
    need the manifest object-delta lookup and the CMD_PID liveness check
    respectively — separate, not-yet-shipped sibling todos in this plan — so they
    degrade to ``"unknown"`` rather than being guessed from a proxy signal
    (principle 2: a hint is not truth).

    ``control_plane_running`` is the GCE aggregated-list confirmation (the
    running-set ``_load_gcp_vm_entries`` already fetches) — ``None`` when the
    caller has no confirmation to offer, in which case ``dead`` never fires and
    ``hung`` falls back to heartbeat staleness alone (no regression vs. the
    pre-D.3 status, just less certain without the control-plane cross-check).
    """
    if entry.status != "running":
        return None
    if control_plane_running is False:
        return "dead"
    if hb_age_seconds is not None and hb_age_seconds > _STALE_HEARTBEAT_MINUTES * 60:
        return "hung"
    if not _has_d1_metrics(entry):
        return "unknown"
    if entry.disk_pct > _DISK_FULL_PCT_THRESHOLD:
        return "disk-full"
    if entry.mem_pct >= _OOM_RISK_MEM_PCT_FLOOR and entry.mem_slope > 0:
        return "oom-risk"
    if entry.io_write_rate_bytes_sec > 0 or entry.net_recv_rate_bytes_sec > 0:
        return "working"
    return "unknown"


def _vm_item(
    entry: DeploymentRegistryEntry,
    now: datetime,
    vm_details: dict[str, object] | None = None,
    *,
    control_plane_running: bool | None = None,
) -> DeploymentItem:
    """Build an inventory item from a VM deployment-registry entry.

    ``vm_details`` is this VM's entry from the GCE aggregated-list join
    (``get_vm_instance_details``), if any — surfaces the Tier-0 free wins
    (machine_type/zone/labels/boot-disk/raw status) already fetched there but
    previously discarded down to just the running-VM-name set.
    ``control_plane_running`` is the same GCE aggregated-list confirmation,
    reduced to a bool — feeds the D.3 composite classifier's ``dead``/``hung``.
    """
    target = _classify_vm(entry.vm_name)
    hb_age = _heartbeat_age_seconds(entry, now)
    status = _vm_status(entry, hb_age)
    run_log = ""
    completed_at = entry.completed_at
    if completed_at and len(completed_at) >= 10:
        date_stamp = completed_at[:10].replace("-", "")
        if date_stamp.isdigit():
            run_log = vm_run_log_rolling_uri(entry.vm_name, date_stamp)
    details = vm_details or {}
    labels = details.get("labels")
    return DeploymentItem(
        name=entry.vm_name,
        kind=target.kind.value,
        umbrella=target.umbrella.value,
        cloud=target.cloud.value,
        service=target.service,
        asset_group=target.asset_group,
        status=status,
        last_run_at=entry.completed_at or entry.started_at or None,
        exit_code=entry.exit_code,
        heartbeat_age_seconds=hb_age,
        captured_progress=entry.rows_out,
        run_log_uri=run_log,
        rows_in=entry.rows_in,
        rows_error=entry.rows_error,
        events_emitted=entry.events_emitted,
        uptime_hours=_uptime_hours(entry, now),
        machine_type=str(details.get("machine_type") or "") or None,
        zone=str(details.get("zone") or "") or None,
        health_status=str(details.get("status") or "") or None,
        boot_disk_name=str(details.get("boot_disk_name") or "") or None,
        labels=cast(dict[str, str], labels) if labels else None,
        composite_health_status=_composite_health_status(entry, hb_age, control_plane_running=control_plane_running),
    )


# ---------------------------------------------------------------------------
# Cloud Run job items (dynamic live census + registry classification hints)
# ---------------------------------------------------------------------------


def _cloud_run_status_for(
    target: DeploymentTarget, by_job: dict[str, CloudRunExecutionStatus]
) -> CloudRunExecutionStatus | None:
    """Find the live execution status for a registered job stem (suffix match).

    Live Cloud Run job names carry an env prefix (``prd-``) + asset_group suffix;
    the registry stores the stem. Match a live name whose tail equals the stem (or
    that contains the stem) so an env/ag-decorated live name binds to its target.
    """
    stem = target.name
    # Exact stem hit first (job named exactly the stem), then suffix/contains.
    if stem in by_job:
        return by_job[stem]
    for job_name, status in by_job.items():
        if job_name == stem or job_name.endswith(f"-{stem}") or stem in job_name:
            return status
    return None


def _match_registered_job(job_name: str) -> DeploymentTarget | None:
    """Bind a LIVE Cloud Run job name to its classified registry stem — a HINT,
    not an allow-list. Exact stem match first, then suffix/contains (the same
    rule ``_cloud_run_status_for`` applies in the other direction). ``None`` when
    no registry entry matches — the caller derives a classification instead of
    dropping the job from the census.
    """
    for target in CLOUD_RUN_JOBS:
        if target.name == job_name:
            return target
    for target in CLOUD_RUN_JOBS:
        stem = target.name
        if job_name.endswith(f"-{stem}") or stem in job_name:
            return target
    return None


def _classify_live_cloud_run_job(job_name: str) -> DeploymentTarget:
    """Classify a LIVE Cloud Run job — the registry stem as a HINT, else the
    honest BATCH default. Mirrors the VM inventory's unregistered-prefix degrade
    (``_DEFAULT_LIFECYCLE``): the registry's own docstring classification note is
    "audits / consolidator / catalogue / ... -> BATCH", so an off-pattern job is
    overwhelmingly infra/audit — classify it, never hide it.
    """
    registered = _match_registered_job(job_name)
    if registered is not None:
        return registered
    return classify_deployment_target(
        job_name,
        lifecycle_class=LifecycleClass.EPHEMERAL_BATCH.value,
        cloud=DeploymentCloud.GCP,
        kind=DeploymentKind.CLOUD_RUN_JOB,
    )


def _cloud_run_item_for_live_job(job_name: str, live: CloudRunExecutionStatus) -> DeploymentItem:
    """Build an inventory item for ONE live Cloud Run job (the dynamic census path).

    The wire ``name`` is the actual live job name, not the registry stem — an
    off-pattern job (no registry match) still gets its own row.
    """
    target = _classify_live_cloud_run_job(job_name)
    return DeploymentItem(
        name=job_name,
        kind=target.kind.value,
        umbrella=target.umbrella.value,
        cloud=target.cloud.value,
        service=target.service,
        asset_group=target.asset_group,
        status=live.status,
        last_run_at=live.last_run_at,
        exit_code=live.exit_code,
        heartbeat_age_seconds=None,
        captured_progress=None,
        run_log_uri=live.log_uri,
    )


def _cloud_run_item(target: DeploymentTarget, by_job: dict[str, CloudRunExecutionStatus]) -> DeploymentItem:
    """Build a registry-driven inventory item — the DEGRADED-path fallback only,
    used when the live Cloud Run job list itself failed (empty ``by_job``), so the
    census still shows the classified registry with status="unknown" rather than
    going empty.
    """
    live = _cloud_run_status_for(target, by_job)
    if live is None:
        return DeploymentItem(
            name=target.name,
            kind=target.kind.value,
            umbrella=target.umbrella.value,
            cloud=target.cloud.value,
            service=target.service,
            asset_group=target.asset_group,
            status="unknown",
            last_run_at=None,
            exit_code=None,
            heartbeat_age_seconds=None,
            captured_progress=None,
            run_log_uri="",
        )
    return DeploymentItem(
        name=target.name,
        kind=target.kind.value,
        umbrella=target.umbrella.value,
        cloud=target.cloud.value,
        service=target.service,
        asset_group=target.asset_group,
        status=live.status,
        last_run_at=live.last_run_at,
        exit_code=live.exit_code,
        heartbeat_age_seconds=None,
        captured_progress=None,
        run_log_uri=live.log_uri,
    )


# ---------------------------------------------------------------------------
# GCP Cloud Functions (gen2) census — existence + config only (WS-B). Gen2
# functions run on Cloud Run underneath, so umbrella is always NONE (same
# precedent as ECS_SERVICE / CLOUD_RUN_SERVICE — no live/batch/paper phase).
# ---------------------------------------------------------------------------


def _cloud_function_item(status: CloudFunctionStatus) -> DeploymentItem:
    """Build an inventory item for one live GCP Cloud Function (gen2).

    Existence + config only (per WS-B scope) — always classified directly (no
    ``classify_deployment_target`` lifecycle_class needed), mirroring the ECS
    service precedent for kinds with no live/batch/paper phase.
    """
    return DeploymentItem(
        name=status.name,
        kind=DeploymentKind.CLOUD_FUNCTION.value,
        umbrella=DeploymentUmbrella.NONE.value,
        cloud=DeploymentCloud.GCP.value,
        service=status.name,
        asset_group="",
        status=status.status,
        last_run_at=status.last_updated_at,
        exit_code=None,
        heartbeat_age_seconds=None,
        captured_progress=None,
        run_log_uri="",
    )


# ---------------------------------------------------------------------------
# Service-health sub-taxonomy (parent plan D.3) — ECS / Cloud Run service
# composite status. Services have no manifest/object-delta (they're not data
# producers), so they get a SEPARATE 4-state set from the VM 7-state taxonomy.
# Pure classifiers only: the ECS_SERVICE / CLOUD_RUN_SERVICE census that feeds
# these (desiredCount/runningCount, ready-state) is tracked separately in this
# plan's kinds-census todos — these functions are the reusable status-derivation
# half, ready for that census to call. Mirrors the ``_vm_status`` local pattern.
# ---------------------------------------------------------------------------

# error-rate threshold above which a service reads "degraded" even while fully
# scaled. v1 default (undocumented SLO in the plan) — revisit once the ECS/Cloud
# Run census is wired to a real error-rate signal (parent plan Open-Q7).
_SERVICE_ERROR_RATE_THRESHOLD = 0.05

SERVICE_STATUS_SERVING = "serving"
SERVICE_STATUS_SCALED_TO_ZERO = "scaled-to-zero"
SERVICE_STATUS_DEAD = "dead"
SERVICE_STATUS_DEGRADED = "degraded"


def ecs_service_health_status(
    desired_count: int,
    running_count: int,
    error_rate: float | None = None,
) -> str:
    """ECS service composite status from desired-vs-running (parent D.3).

    ``desired_count == 0`` is an intentional scale-to-zero (neutral, not an
    error) — never hidden, never flagged red. ``running_count == 0`` while
    something is desired is ``dead`` (should be up, isn't). Any capacity
    shortfall short of fully dead, or an error-rate over threshold, is
    ``degraded`` (amber) rather than a false ``serving`` green.
    """
    if desired_count <= 0:
        return SERVICE_STATUS_SCALED_TO_ZERO
    if running_count <= 0:
        return SERVICE_STATUS_DEAD
    if running_count < desired_count:
        return SERVICE_STATUS_DEGRADED
    if error_rate is not None and error_rate > _SERVICE_ERROR_RATE_THRESHOLD:
        return SERVICE_STATUS_DEGRADED
    return SERVICE_STATUS_SERVING


def cloud_run_service_health_status(
    ready: bool | None,
    min_instance_count: int = 0,
    active_instance_count: int | None = None,
    error_rate: float | None = None,
) -> str:
    """Cloud Run service composite status from ready-state + revision health
    (parent D.3) — the Cloud Run analog of ``ecs_service_health_status``, using
    the terminal-condition ready-state + traffic-serving revision in place of
    ECS's desired/running counts.

    ``ready is False`` means the latest revision failed to become ready — the
    service should be serving and isn't, so ``dead``. A service configured with
    ``min_instance_count == 0`` and observed with zero active instances is an
    intentional scale-to-zero. ``ready is None`` (state unknown / not yet
    resolved) degrades honest rather than claiming a green it can't back up.
    """
    if ready is False:
        return SERVICE_STATUS_DEAD
    if min_instance_count <= 0 and (active_instance_count is None or active_instance_count <= 0):
        return SERVICE_STATUS_SCALED_TO_ZERO
    if ready is None:
        return SERVICE_STATUS_DEGRADED
    if error_rate is not None and error_rate > _SERVICE_ERROR_RATE_THRESHOLD:
        return SERVICE_STATUS_DEGRADED
    return SERVICE_STATUS_SERVING


# ---------------------------------------------------------------------------
# Inventory assembly + filtering
# ---------------------------------------------------------------------------


def build_inventory(
    vm_entries: list[DeploymentRegistryEntry],
    cloud_run_status: dict[str, CloudRunExecutionStatus],
    now: datetime,
    vm_details_by_name: dict[str, dict[str, object]] | None = None,
) -> list[DeploymentItem]:
    """Assemble the full unified inventory (VMs + Cloud Run jobs).

    A VM whose name cannot be classified is logged + skipped (never crashes the
    inventory) — the no-silent-default classifier raises, we degrade per-row.

    ``vm_details_by_name`` is the GCE aggregated-list join (Tier-0 free wins —
    machine_type/zone/labels/boot-disk/raw status), reused for the control-plane
    confirmation feeding the D.3 composite ``dead``/``hung`` states: a VM is
    "running per GCP right now" only when its raw status in the join is exactly
    ``"RUNNING"`` — mere PRESENCE in the join is not enough, since GCE keeps a
    stopping/stopped/terminated instance visible in the aggregated-list for a
    while (present but definitely not running). ``None`` (a caller with no join
    to offer, e.g. pure-classification tests or a degraded path) degrades those
    states honestly (see ``_composite_health_status``) rather than guessing; an
    explicit ``{}`` (a real, empty GCE census) DOES confirm every entry as
    not-running.

    Cloud Run jobs census the LIVE job list (``cloud_run_status`` keys, already
    fetched by ``latest_execution_by_job``'s ``run_v2.JobsClient.list_jobs`` — no
    new API call) — ``CLOUD_RUN_JOBS`` is a classification HINT, not an allow-list,
    so an off-pattern job (no registry match) still gets a row instead of hiding.
    Only when the live list itself is empty (the GCP call failed) does the census
    degrade to the static registry with status="unknown" (never an empty census).
    """
    details_by_name = vm_details_by_name or {}
    items: list[DeploymentItem] = []
    for entry in vm_entries:
        try:
            control_plane_running = (
                str(details_by_name.get(entry.vm_name, {}).get("status", "")) == "RUNNING"
                if vm_details_by_name is not None
                else None
            )
            items.append(
                _vm_item(
                    entry,
                    now,
                    details_by_name.get(entry.vm_name),
                    control_plane_running=control_plane_running,
                )
            )
        except UnclassifiedDeploymentError as exc:
            logger.warning("inventory: skipping unclassifiable VM %r: %s", entry.vm_name, exc)
    if cloud_run_status:
        for job_name, status in cloud_run_status.items():
            items.append(_cloud_run_item_for_live_job(job_name, status))
    else:
        for target in CLOUD_RUN_JOBS:
            items.append(_cloud_run_item(target, cloud_run_status))
    return items


def _filter_items(
    items: list[DeploymentItem],
    *,
    umbrella: str | None,
    cloud: str | None,
    service: str | None,
    asset_group: str | None,
    status: str | None,
    kind: str | None = None,
) -> list[DeploymentItem]:
    """Apply the inventory query filters (case-insensitive on the enum axes)."""

    def _keep(item: DeploymentItem) -> bool:
        if umbrella and item.umbrella.upper() != umbrella.upper():
            return False
        if cloud and item.cloud.upper() != cloud.upper():
            return False
        if service and item.service != service:
            return False
        if asset_group and item.asset_group != asset_group:
            return False
        if status and item.status != status:
            return False
        return not (kind and item.kind.upper() != kind.upper())

    return [item for item in items if _keep(item)]


def _mock_inventory(now: datetime) -> list[DeploymentItem]:
    """A representative mock inventory (mock mode — no GCP / GCS access)."""
    return [
        DeploymentItem(
            name="cefi-binance-spot-20260622-014158",
            kind="VM",
            umbrella="BATCH",
            cloud="GCP",
            service="cefi-binance-spot-20260622-014158",
            asset_group="cefi",
            status="running",
            last_run_at="2026-06-22T01:41:58Z",
            exit_code=None,
            heartbeat_age_seconds=42,
            captured_progress=11_987,
            run_log_uri="",
        ),
        DeploymentItem(
            name="defi-backfill-20260622-OOM",
            kind="VM",
            umbrella="BATCH",
            cloud="GCP",
            service="defi-backfill",
            asset_group="defi",
            status="failed",
            last_run_at="2026-06-22T03:00:00Z",
            exit_code=137,
            heartbeat_age_seconds=3_600,
            captured_progress=0,
            run_log_uri="",
        ),
        DeploymentItem(
            name="strategy-live-cefi-20260620",
            kind="VM",
            umbrella="LIVE",
            cloud="GCP",
            service="strategy-live",
            asset_group="cefi",
            status="running",
            last_run_at="2026-06-20T00:00:00Z",
            exit_code=None,
            heartbeat_age_seconds=30,
            captured_progress=0,
            run_log_uri="",
        ),
        DeploymentItem(
            name="defi-paper-trading-20260622",
            kind="VM",
            umbrella="PAPER",
            cloud="GCP",
            service="defi-paper-trading",
            asset_group="defi",
            status="running",
            last_run_at="2026-06-22T00:00:00Z",
            exit_code=None,
            heartbeat_age_seconds=15,
            captured_progress=0,
            run_log_uri="",
        ),
        DeploymentItem(
            name="manifest-consolidator",
            kind="CLOUD_RUN_JOB",
            umbrella="BATCH",
            cloud="GCP",
            service="manifest-consolidator",
            asset_group="cefi",
            status="succeeded",
            last_run_at="2026-06-22T06:00:00Z",
            exit_code=0,
            heartbeat_age_seconds=None,
            captured_progress=None,
            run_log_uri="",
        ),
        # AWS estate (Phase 5 parity) — an EC2 backfill VM + a Batch Fargate job,
        # cloud=AWS, classified under the SAME umbrellas as the GCP items.
        DeploymentItem(
            name="mtds-backfill-cefi-20260622-aws",
            kind="VM",
            umbrella="BATCH",
            cloud="AWS",
            service="mtds-backfill",
            asset_group="cefi",
            status="running",
            last_run_at="2026-06-22T11:30:00Z",
            exit_code=None,
            heartbeat_age_seconds=None,
            captured_progress=None,
            run_log_uri="",
        ),
        DeploymentItem(
            name="manifest-consolidator-aws",
            kind="CLOUD_RUN_JOB",
            umbrella="BATCH",
            cloud="AWS",
            service="manifest-consolidator",
            asset_group="cefi",
            status="succeeded",
            last_run_at="2026-06-22T06:05:00Z",
            exit_code=0,
            heartbeat_age_seconds=None,
            captured_progress=None,
            run_log_uri="",
        ),
    ]


def _load_aws_items(now: datetime) -> list[DeploymentItem]:
    """Census + classify the live AWS estate into inventory items (Phase 5 parity).

    Reuses the curated ``_vm_lifecycle_class`` prefix resolver so AWS umbrella
    derivation matches GCP exactly. The census itself degrades to an empty list on any
    AWS error (no creds / boto3 absent / API down), so a missing AWS estate never
    blocks the GCP inventory — AWS rides the same ``DeploymentItem`` contract.
    """
    region = _cfg.aws_codebuild_region or "ap-northeast-1"
    item_dicts = load_aws_inventory(
        region=region,
        aws_account_id=_cfg.aws_account_id or "",
        lifecycle_for_name=_vm_lifecycle_class,
    )
    return [DeploymentItem(**d) for d in item_dicts]  # type: ignore[arg-type]


def _list_json_keys(client: StorageClient, bucket: str, prefix: str) -> list[str]:
    """List the ``.json`` object keys under one registry prefix (honest-empty on error)."""
    try:
        return [b.name for b in client.list_blobs(bucket=bucket, prefix=prefix) if b.name.endswith(".json")]
    except (OSError, ValueError, RuntimeError) as exc:
        logger.warning("inventory: list_blobs(%s) failed: %s", prefix, exc)
        return []


def _read_entry(client: StorageClient, bucket: str, key: str) -> DeploymentRegistryEntry | None:
    """Download + parse ONE registry entry; return None on any read/parse error (per-key isolation)."""
    try:
        raw = client.download_bytes(bucket=bucket, blob_path=key).decode("utf-8")
        return DeploymentRegistryEntry.from_json(raw)
    except (OSError, ValueError, KeyError, json.JSONDecodeError) as exc:
        logger.warning("inventory: skipping unreadable registry entry %s: %s", key, exc)
        return None


def _download_entries_parallel(client: StorageClient, bucket: str, keys: list[str]) -> list[DeploymentRegistryEntry]:
    """Download + parse many registry entries CONCURRENTLY (GCS-object-ops ThreadPool pattern).

    The dominant cost of the inventory is per-object GCS reads over a transpacific hop;
    reading them sequentially is the >100s the cockpit timed out on. GCS REST releases
    the GIL, so a ThreadPool gives true I/O parallelism. Per-key failures degrade to
    ``None`` (never crash the whole inventory).
    """
    if not keys:
        return []
    workers = min(_GCS_READ_WORKERS, len(keys))
    read_one = partial(_read_entry, client, bucket)
    with ThreadPoolExecutor(max_workers=workers) as pool:
        results = pool.map(read_one, keys)
    return [entry for entry in results if entry is not None]


def _load_gcp_vm_entries(
    now: datetime, project_id: str
) -> tuple[list[DeploymentRegistryEntry], dict[str, dict[str, object]]]:
    """Census the GCP VM registry (active + 7-day archive) with parallel reads.

    Runs the four coarse calls concurrently — the GCE aggregated-list, the active-key
    list, the archive-key list, then a single parallel download pool over every key —
    so the cold path is ~max(slowest single call) instead of their sum. Also returns the
    GCE aggregated-list join (name -> machine_type/zone/labels/boot-disk/raw status) so
    the caller can surface those Tier-0 free wins AND cross-check control-plane presence.

    Every ``active/`` entry is included — NOT pre-filtered to the GCE aggregated-list's
    key set. Filtering here would silently drop the exact "hard-killed VM" case the D.3
    ``dead`` composite state exists to catch: a registry entry whose VM the control plane
    no longer has must reach ``build_inventory``/``_composite_health_status`` to be
    classified ``dead``, not vanish from the census beforehand.
    """
    client = get_storage_client()
    bucket = DEFAULT_BUCKET
    today = now.date()
    archive_prefixes = [
        f"{ARCHIVE_PREFIX}{(today - timedelta(days=offset)).isoformat()}/" for offset in range(_ARCHIVE_WINDOW_DAYS)
    ]

    with ThreadPoolExecutor(max_workers=4) as pool:
        f_vm = pool.submit(get_vm_instance_details, project_id)
        f_active_keys = pool.submit(_list_json_keys, client, bucket, ACTIVE_PREFIX)
        f_archive_keys = pool.submit(
            lambda: [key for prefix in archive_prefixes for key in _list_json_keys(client, bucket, prefix)]
        )
        vm_details = f_vm.result()
        active_keys = f_active_keys.result()
        archive_keys = f_archive_keys.result()

    active = _download_entries_parallel(client, bucket, active_keys)
    recent = _download_entries_parallel(client, bucket, archive_keys)
    return active + recent, vm_details


def _compute_inventory(now: datetime, cloud: str | None) -> list[DeploymentItem]:
    """Build the live inventory (registry VMs + Cloud Run executions + AWS) — the cold path."""
    want_gcp = cloud is None or cloud.upper() == DeploymentCloud.GCP.value
    want_aws = cloud is None or cloud.upper() == DeploymentCloud.AWS.value

    items: list[DeploymentItem] = []
    if want_gcp:
        project_id = _cfg.require_gcp_project_id()
        try:
            vm_entries, vm_details_by_name = _load_gcp_vm_entries(now, project_id)
        except (OSError, ValueError, RuntimeError) as exc:
            logger.exception("inventory: VM registry read failed: %s", exc)
            raise HTTPException(status_code=502, detail="VM deployments registry unavailable") from exc

        cloud_run_status = latest_execution_by_job(project_id)
        items.extend(build_inventory(vm_entries, cloud_run_status, now, vm_details_by_name))

        with _vm_entry_by_name_lock:
            _vm_entry_by_name_cache.clear()
            _vm_entry_by_name_cache.update({e.vm_name: e for e in vm_entries})

        cloud_function_status = list_cloud_functions(project_id)
        items.extend(_cloud_function_item(status) for status in cloud_function_status.values())

    if want_aws:
        items.extend(_load_aws_items(now))

    return items


def _store_inventory(cache_key: str, items: list[DeploymentItem]) -> None:
    """Atomically record a fresh inventory snapshot for ``cache_key``."""
    with _inventory_lock:
        _inventory_cache[cache_key] = (time.monotonic(), items)


def _refresh_inventory(cache_key: str, cloud: str | None) -> None:
    """Background cache refresh — recompute + store, then clear the in-flight flag."""
    try:
        _store_inventory(cache_key, _compute_inventory(datetime.now(UTC), cloud))
    except (HTTPException, OSError, ValueError, RuntimeError) as exc:
        # Keep the stale snapshot on a failed refresh — never poison the cache.
        logger.warning("inventory: background refresh for %s failed: %s", cache_key, exc)
    finally:
        with _inventory_lock:
            _inventory_refreshing.discard(cache_key)


def _kick_background_refresh(cache_key: str, cloud: str | None) -> None:
    """Schedule exactly one background refresh per cache key (stale-while-revalidate)."""
    with _inventory_lock:
        if cache_key in _inventory_refreshing:
            return
        _inventory_refreshing.add(cache_key)
    _inventory_refresh_pool.submit(_refresh_inventory, cache_key, cloud)


def _load_inventory(now: datetime, cloud: str | None = None) -> list[DeploymentItem]:
    """Load the live inventory, stale-while-revalidate cached for a fast, smooth cockpit.

    GCP items load unless the caller filters ``cloud=aws``; AWS items load unless the
    caller filters ``cloud=gcp`` (so an unset / ``aws`` filter includes the AWS estate).
    Cache policy (mock mode bypasses it — already cheap + deterministic):

    * **Fresh** (< TTL) → served instantly.
    * **Stale** (> TTL) → the stale snapshot is served instantly AND a single background
      refresh is kicked off, so the operator never waits on the slow census after the
      first ever load (the cockpit polls repeatedly → always warm).
    * **Cold** (no snapshot) → computed synchronously, under a lock so a burst of polls
      collapses to ONE census, not N.
    """
    if _cfg.is_mock_mode():
        return _mock_inventory(now)

    cache_key = (cloud or "all").upper()
    cached = _inventory_cache.get(cache_key)
    if cached is not None:
        if (time.monotonic() - cached[0]) >= _INVENTORY_TTL_SEC:
            _kick_background_refresh(cache_key, cloud)
        return cached[1]

    # Cold path — lock so concurrent first-polls trigger exactly ONE census.
    with _inventory_lock:
        cached = _inventory_cache.get(cache_key)
        if cached is not None:
            return cached[1]
        items = _compute_inventory(now, cloud)
        _inventory_cache[cache_key] = (time.monotonic(), items)
        return items


# ---------------------------------------------------------------------------
# Routes
# ---------------------------------------------------------------------------

_VALID_UMBRELLAS = frozenset(u.value for u in DeploymentUmbrella)
_VALID_CLOUDS = frozenset(c.value for c in DeploymentCloud)


def _counts_by_kind(items: list[DeploymentItem]) -> dict[str, int]:
    """Per-kind row counts, one key per kind actually present — never a zero-filled 6-key map.

    A kind absent from ``items`` (its census hasn't shipped yet, or failed this cycle) is simply
    absent from the map — honest degradation, not a fabricated 0 masquerading as "censused, found
    none".
    """
    counts: dict[str, int] = {}
    for item in items:
        counts[item.kind] = counts.get(item.kind, 0) + 1
    return counts


@router.get("/deployments/inventory", response_model=DeploymentInventoryResponse)
def get_deployment_inventory(
    umbrella: str | None = Query(None, description="live|batch|paper|experiment (case-insensitive)"),
    cloud: str | None = Query(None, description="gcp|aws (case-insensitive)"),
    service: str | None = Query(None, description="Exact service stem filter"),
    asset_group: str | None = Query(None, description="cefi|defi|tradfi|sports|prediction"),
    status: str | None = Query(None, description="Exact status filter (running|succeeded|failed|stale|...)"),
    kind: str | None = Query(
        None,
        description="VM|CLOUD_RUN_JOB|CLOUD_RUN_SERVICE|ECS_SERVICE|LAMBDA|CLOUD_FUNCTION (case-insensitive)",
    ),
) -> DeploymentInventoryResponse:
    """Unified deployment inventory: every VM + Cloud Run job, classified by umbrella.

    GCP **and** AWS (Phase 5 parity) — AWS EC2 backfill VMs + Batch Fargate jobs ride
    the same ``DeploymentItem`` contract with ``cloud=AWS``. Each item carries its
    umbrella/cloud/service/asset_group classification + live status / last-run /
    exit_code / heartbeat / captured-progress.
    """
    now = datetime.now(UTC)
    items = _load_inventory(now, cloud=cloud)
    filtered = _filter_items(
        items,
        umbrella=umbrella,
        cloud=cloud,
        service=service,
        asset_group=asset_group,
        status=status,
        kind=kind,
    )
    vm_count = sum(1 for i in filtered if i.kind == DeploymentKind.VM.value)
    job_count = sum(1 for i in filtered if i.kind == DeploymentKind.CLOUD_RUN_JOB.value)
    return DeploymentInventoryResponse(
        items=filtered,
        total=len(filtered),
        vm_count=vm_count,
        cloud_run_job_count=job_count,
        counts_by_kind=_counts_by_kind(filtered),
    )


@router.get("/deployments/{name}/detail", response_model=DeploymentDetailResponse)
def get_deployment_detail(name: str) -> DeploymentDetailResponse:
    """Per-target drill-down: the thin-list item plus the D.1 metrics vector (popover).

    ``name`` is the ``DeploymentItem.name`` (VM name / Cloud Run job or service name), not
    an orchestration ``deployment_id`` — this endpoint reads the SAME cached census
    ``/deployments/inventory`` already computes (no new bucket walk / API call). 404 if the
    name isn't in the current (cached) inventory.
    """
    now = datetime.now(UTC)
    items = _load_inventory(now)
    item = next((i for i in items if i.name == name), None)
    if item is None:
        raise HTTPException(status_code=404, detail=f"Deployment {name!r} not found in the current inventory")
    with _vm_entry_by_name_lock:
        entry = _vm_entry_by_name_cache.get(name)
    if entry is None:
        return DeploymentDetailResponse(item=item)
    return DeploymentDetailResponse(
        item=item,
        cpu_pct=entry.cpu_pct,
        mem_pct=entry.mem_pct,
        mem_slope=entry.mem_slope,
        disk_pct=entry.disk_pct,
        io_write_rate_bytes_sec=entry.io_write_rate_bytes_sec,
        net_recv_rate_bytes_sec=entry.net_recv_rate_bytes_sec,
        workload_alive=entry.workload_alive,
    )


def build_umbrella_summary(umbrella: str, items: list[DeploymentItem]) -> UmbrellaSummaryResponse:
    """Roll the inventory items of one umbrella into the /repos-overview summary."""
    scoped = [i for i in items if i.umbrella.upper() == umbrella.upper()]
    counts: dict[str, int] = {}
    for item in scoped:
        counts[item.status] = counts.get(item.status, 0) + 1
    stale_count = sum(1 for i in scoped if i.status == "stale")
    failures = [i for i in scoped if i.status == "failed"]
    last_failure: UmbrellaStatusFailure | None = None
    if failures:
        # Most-recent failing target by last_run_at (lexicographic ISO sort; None last).
        worst = max(failures, key=lambda i: i.last_run_at or "")
        last_failure = UmbrellaStatusFailure(
            name=worst.name,
            exit_code=worst.exit_code,
            last_run_at=worst.last_run_at,
        )
    return UmbrellaSummaryResponse(
        umbrella=umbrella.upper(),
        total=len(scoped),
        counts_by_status=counts,
        stale_count=stale_count,
        last_failure=last_failure,
    )


@router.get("/deployments/umbrella/{umbrella}/summary", response_model=UmbrellaSummaryResponse)
def get_umbrella_summary(umbrella: str) -> UmbrellaSummaryResponse:
    """Per-umbrella rollup: counts by status, stale count, last failure.

    The /repos-overview equivalent for one umbrella (live|batch|paper|experiment).
    A 404 on an unknown umbrella (the closed set is the UAC ``DeploymentUmbrella``).
    """
    if umbrella.upper() not in _VALID_UMBRELLAS:
        raise HTTPException(
            status_code=404,
            detail=f"Unknown umbrella {umbrella!r}; expected one of {sorted(_VALID_UMBRELLAS)}",
        )
    now = datetime.now(UTC)
    items = _load_inventory(now)
    return build_umbrella_summary(umbrella, items)


__all__ = [
    "DeploymentDetailResponse",
    "DeploymentInventoryResponse",
    "DeploymentItem",
    "UmbrellaSummaryResponse",
    "active_registry_vm_names",
    "build_inventory",
    "build_umbrella_summary",
    "classify_vm_target",
    "router",
]

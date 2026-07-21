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
from concurrent.futures import Future, ThreadPoolExecutor
from concurrent.futures import TimeoutError as FutureTimeoutError
from datetime import UTC, date, datetime, timedelta
from functools import partial
from typing import cast

from deployment_service.cloud_run_job_registry import CLOUD_RUN_JOBS
from deployment_service.deployment_classification import (
    UnclassifiedDeploymentError,
    classify_deployment_target,
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
from unified_trading_library import (
    ACTIVE_PREFIX,
    ARCHIVE_PREFIX,
    DEFAULT_BUCKET,
    DeploymentRegistryEntry,
    StorageClient,
    download_from_storage,
    get_storage_client,
    upload_to_storage,
    vm_run_log_rolling_uri,
)

from deployment_api.deployment_api_config import DeploymentApiConfig
from deployment_api.registry_reader import resolve_active_registry
from deployment_api.routes._aws_deployments import load_aws_inventory
from deployment_api.routes._cloud_run_executions import (
    DEFAULT_CLOUD_RUN_REGION,
    CloudRunExecutionStatus,
    latest_execution_by_job,
    list_job_executions,
)
from deployment_api.routes._cloud_run_services import (
    CloudRunServiceStatus,
    list_cloud_run_services,
)
from deployment_api.routes._cloud_scheduler import (
    SchedulerJobStatus,
    list_scheduler_jobs,
)
from deployment_api.routes._gcp_cloud_functions import (
    CloudFunctionStatus,
    list_cloud_functions,
)
from deployment_api.routes._leaked_resources import (
    UnreleasedResource,
    detect_unreleased_resources,
    orphaned_disk,
    orphaned_static_ip,
)

# Service-health sub-taxonomy classifiers (parent D.3). Defined in their own leaf module
# and re-exported here (see __all__) because the AWS row builder in ``_aws_deployments`` —
# which ``deployments_inventory`` imports — needs them too, and a reverse import would cycle.
from deployment_api.routes._service_health import (
    SERVICE_STATUS_DEAD,
    SERVICE_STATUS_DEGRADED,
    SERVICE_STATUS_SCALED_TO_ZERO,
    SERVICE_STATUS_SERVING,
    cloud_run_service_health_status,
    ecs_service_health_status,
)

# VM wire-status + D.3 composite-health classifiers — extracted to their own leaf module to
# keep this hotspot file down; aliased back to the historical private names for existing call
# sites/tests (one-way import, _vm_health has no deployments_inventory dependency so no cycle).
from deployment_api.routes._vm_health import composite_health_status as _composite_health_status
from deployment_api.routes._vm_health import vm_status as _vm_status
from deployment_api.routes.health_consolidator import object_delta_for_asset_group
from deployment_api.services.cost_observability.service import CostObservabilityService
from deployment_api.vm_utils import (
    get_disk_details,
    get_vm_instance_details,
    list_gcp_region_names,
    list_reserved_addresses,
    list_unattached_disk_names,
)

router = APIRouter()
logger = logging.getLogger(__name__)

_cfg = DeploymentApiConfig()

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

# ``launched_by`` provenance values (WS-D full-estate plan). Provenance = registry-presence
# (principle 1): a resource with a deployment-registry entry / a registered Cloud Run job is
# ``deployment-api``-launched; a long-lived managed-infra resource with NO registry entry
# (control-plane VMs, Cloud Run services, Cloud Functions) is ``control-plane``; a live resource
# unaccounted by either — reconciliation's UNKNOWN set — is ``adhoc``; and a resource we cannot
# resolve provenance for yet (the AWS estate has no deployment registry / ``managed-by`` tag until
# the DEVOPS launcher-label todo lands) is ``unknown``. Kept faithful to fleet_reconciliation's
# union so ``launched_by=adhoc`` (running) matches its UNKNOWN count exactly.
LAUNCHED_BY_DEPLOYMENT_API = "deployment-api"
LAUNCHED_BY_CONTROL_PLANE = "control-plane"
LAUNCHED_BY_ADHOC = "adhoc"
LAUNCHED_BY_UNKNOWN = "unknown"

# Long-lived control-plane / live-infra VM prefixes managed OUT-OF-BAND — they write no
# deployment-registry entry, so a running VM matching one is accounted-for (not ``adhoc``) even
# without a registry blob. This is the SSOT for the set: ``fleet_reconciliation`` imports
# ``is_control_plane_vm`` FROM here (the reverse import would cycle — reconciliation already depends
# on this module), so the union is defined once. Mirrors the LONG_LIVED_LIVE prefixes in
# ``_VM_PREFIX_REGISTRY`` above.
_CONTROL_PLANE_PREFIXES = (
    "planning",
    "human-planning",
    "agent-orchestrator",
    "strategy-live-",
    "defi-recursive-",
)


def is_control_plane_vm(name: str) -> bool:
    """True if ``name`` is a long-lived control-plane VM managed out-of-band (writes no registry entry)."""
    return any(name.startswith(prefix) for prefix in _CONTROL_PLANE_PREFIXES)


# Multi-region census (WS-D) — the CONFIGURED region sets we actually deploy to (operator decision
# 2026-07-10: a small set for determinism, NOT a per-cycle fan-out to ~30 mostly-empty regions).
# asia-northeast1 is the GCP primary (all GCS data + the consolidators); ap-northeast-1 (Tokyo) the AWS
# primary — where the planning orchestrator VM (EIP 13.113.200.22) + the human-planning VM + the AWS
# EC2/ECS estate actually run (operator decision 2026-07-10, verified via ec2 describe-instances). The
# us-east-1 Lambda estate is reachable via a US-region selection or the all-regions sweep, NOT the
# Tokyo default. The ``?all_regions=true`` escape hatch sweeps every region for a periodic surprise-check
# (GCP live via ``list_gcp_region_names``; AWS via the curated ``_ALL_AWS_REGIONS`` — the census seam
# has no cheap describe-regions). Per-region honest degradation: one region's failure never blocks
# the others.
# asia-northeast1 is where the GCP Cloud Run jobs/services + Functions + Scheduler actually live
# (all GCS data is there); censusing empty regions every cycle only adds transpacific latency. The
# ``?all_regions=true`` sweep + a per-region degradation net catch anything elsewhere → add its
# region here if the sweep surfaces one. (GCE VMs / disks / IPs are already all-region aggregated.)
_CONFIGURED_GCP_REGIONS: tuple[str, ...] = ("asia-northeast1",)
_CONFIGURED_AWS_REGIONS: tuple[str, ...] = ("ap-northeast-1",)  # Tokyo — where the planning VM + AWS VMs run
_ALL_AWS_REGIONS: tuple[str, ...] = (
    "us-east-1",
    "us-east-2",
    "us-west-1",
    "us-west-2",
    "ca-central-1",
    "sa-east-1",
    "eu-west-1",
    "eu-west-2",
    "eu-west-3",
    "eu-central-1",
    "eu-north-1",
    "ap-northeast-1",
    "ap-northeast-2",
    "ap-southeast-1",
    "ap-southeast-2",
    "ap-south-1",
)

# The GCP region the cockpit's region selector opens on. Selecting it is identical to the configured
# default census (asia-northeast1 GCP + the primary AWS set), so the us-east-1 Lambda estate never
# drops off the default view.
_DEFAULT_GCP_REGION: str = _CONFIGURED_GCP_REGIONS[0]

# GCP region → the AWS region a caller most likely means by "its equivalent" when they pick a single
# GCP region in the selector. A specific-region census scopes AWS to this geographic match (falling
# back to the primary AWS set when a region is unpaired); the default and all-regions sweep are
# unaffected. Best-effort geography pairing, not a hard 1:1.
_GCP_TO_AWS_REGION: dict[str, str] = {
    "asia-northeast1": "ap-northeast-1",
    "asia-northeast2": "ap-northeast-3",
    "asia-northeast3": "ap-northeast-2",
    "asia-southeast1": "ap-southeast-1",
    "asia-south1": "ap-south-1",
    "australia-southeast1": "ap-southeast-2",
    "europe-west1": "eu-west-1",
    "europe-west2": "eu-west-2",
    "europe-west3": "eu-central-1",
    "europe-west4": "eu-west-1",
    "europe-north1": "eu-north-1",
    "us-central1": "us-east-1",
    "us-east1": "us-east-1",
    "us-east4": "us-east-2",
    "us-west1": "us-west-1",
    "us-west2": "us-west-2",
    "northamerica-northeast1": "ca-central-1",
    "southamerica-east1": "sa-east-1",
}


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
# Concurrency for the per-distinct-asset_group object-delta manifest reads (~a handful of
# asset_groups per census). Fanning them out keeps the cold census under the 45 s provider bound.
_OBJECT_DELTA_WORKERS = 8
_INVENTORY_TTL_SEC = 45.0
_ARCHIVE_WINDOW_DAYS = 7

_inventory_cache: dict[str, tuple[float, list[DeploymentItem]]] = {}
_inventory_lock = threading.Lock()
# cache keys with an in-flight background refresh (so we kick off exactly one).
_inventory_refreshing: set[str] = set()
_inventory_refresh_pool = ThreadPoolExecutor(max_workers=2, thread_name_prefix="inv-refresh")

# Per-provider census wall-clock bound. Each provider census (GCE VM registry / Cloud Run
# jobs / Cloud Run services / Cloud Functions / AWS) runs on its own worker; if one hangs
# (a GCP/AWS SDK call with no reachable deadline — a stuck transpacific socket, a wedged
# control-plane), the whole inventory used to block forever (the cockpit timed out at 240 s,
# 0 bytes). The wrapper below bounds each census independently and degrades a slow/hung
# provider to an honest EMPTY census for its own KIND (WS-B: one kind's failure never blocks
# the others / codex/04-architecture/shard-level-failure-isolation.md), so the operator still
# sees every other kind. Belt-and-suspenders with the client-level RPC timeouts on the GCP
# list calls (which stop the stuck worker thread from leaking and starving this pool).
_PROVIDER_CENSUS_TIMEOUT_SEC = 45.0
# One worker per top-level provider census so none queue behind another on the cold path: GCE VMs +
# Cloud Run jobs/services + Cloud Functions + Cloud Scheduler + disks + addresses + unattached-disks
# + object-delta + AWS (WS-D added scheduler / disks / addresses / unattached to the original set).
_census_pool = ThreadPoolExecutor(max_workers=10, thread_name_prefix="inv-census")


def _census_or_degrade[T](label: str, future: Future[T], default: T) -> T:
    """Resolve one provider census within the wall-clock bound; degrade to ``default`` on hang/error.

    A census that exceeds ``_PROVIDER_CENSUS_TIMEOUT_SEC`` or raises yields ``default`` (an
    empty census for that KIND) plus a loud log — it NEVER blocks or crashes the whole
    inventory. Never raises. (A timed-out worker is left to unwind on its own once the
    client-level RPC deadline fires; ``cancel()`` can't stop a thread already in a blocking
    SDK call, hence the paired RPC timeouts.)
    """
    try:
        return future.result(timeout=_PROVIDER_CENSUS_TIMEOUT_SEC)
    except FutureTimeoutError:
        logger.warning(
            "inventory: %s census exceeded %.0fs — degraded to empty for this cycle",
            label,
            _PROVIDER_CENSUS_TIMEOUT_SEC,
        )
        future.cancel()
        return default
    except Exception as exc:  # one provider's failure must never block the others (degradation net)
        logger.warning("inventory: %s census failed (%s: %s) — degraded to empty", label, type(exc).__name__, exc)
        return default


# VM registry entry by vm_name — piggybacks on the SAME GCP VM census _compute_inventory
# already runs every cache cycle (zero new bucket walks); lets the ``/{id}/detail`` drill-down
# serve the D.1 metrics vector (cpu/mem/disk/io/net + workload liveness) that live on
# ``DeploymentRegistryEntry`` but aren't carried onto the thin-list ``DeploymentItem``.
# Entries are indexed by ``vm_name`` (registry objects are keyed by ``deployment_id``, which
# a caller of ``/{id}/detail`` doesn't know), so this is a small derived side-cache, not a
# second source of truth.
_vm_entry_by_name_cache: dict[str, DeploymentRegistryEntry] = {}
_vm_entry_by_name_lock = threading.Lock()

# Shared GCS alert ledger (same store notify-slack.yml + agent-orchestrator's watchers
# write to) — GET /api/alerts reads it via _repo_ci_alerts.py, no reader-side change needed.
_ALERTS_BUCKET = "unified-trading-cicd-events"
# D.3 composite states this todo alerts on. "stalled" cannot occur yet (its manifest
# object-delta signal is a separate, not-yet-shipped sibling todo — see
# _composite_health_status) but is included so no code change is needed once it lands.
_ALERT_HEALTH_STATES = frozenset({"oom-risk", "stalled"})

# In-process last-alerted composite_health_status per VM name — fires an alert only on a
# fresh TRANSITION into an alertable state, never on every ~45s cache-refresh poll while the
# state persists. Resets on process restart (acceptable: at most one re-alert per VM already
# in an alerting state, self-corrects next cycle — no GCS read needed to bootstrap it, D.4's
# "no new central pull" cost budget).
_last_alerted_health: dict[str, str | None] = {}
_last_alerted_health_lock = threading.Lock()


class DeploymentItem(BaseModel):  # CORRECT-LOCAL: FastAPI API contract model
    """One classified compute unit in the unified inventory (VM, Cloud Run job, or service).

    The wire shape the deployment-ui Deployments page consumes per row. Mirrors the
    classification fields of UAC ``DeploymentTarget`` + the live runtime fields.
    """

    name: str
    kind: str  # VM|CLOUD_RUN_JOB|CLOUD_RUN_SERVICE|ECS_SERVICE|LAMBDA|CLOUD_FUNCTION (UAC DeploymentKind)
    umbrella: str  # "LIVE" | "BATCH" | "PAPER" | "EXPERIMENT" | "NONE" (services, Open-Q1)
    cloud: str  # "GCP" | "AWS"
    service: str
    asset_group: str
    status: str  # running|succeeded|failed|stopped|stale|pending|unknown
    # WS-2 per-kind date-range support matrix (what ``_apply_date_range`` actually has to match on):
    #   INTERVAL (started_at+completed_at/last_heartbeat_at, true start/end) — VM rows with a
    #     registry entry (``_vm_item``); see ``_vm_overlap_basis``.
    #   SINGLE-TIMESTAMP (this field only, no interval) — unmanaged/AWS-EC2 VMs (creation time),
    #     Cloud Run jobs + AWS Batch jobs (last run), Cloud Scheduler (last fire); see
    #     ``_SINGLE_TIMESTAMP_KINDS``/``_single_timestamp_overlaps``. Always basis="approx" on match.
    #   NONE (no timestamp signal at all) — Cloud Run/ECS services, Cloud Functions, disks, static
    #     IPs; ``_apply_date_range`` passes these through unfiltered regardless of the query range.
    last_run_at: str | None = None
    # Last DEPLOY/modify time — distinct from last_run_at (last INVOKE). Set for kinds whose last-run
    # is not honestly observable without a paid metric (AWS Lambda: last_run_at stays None; this
    # carries fn.last_modified so the UI can show last-*modified* with a tooltip, never a mislabelled
    # last-run). None for kinds that report a real last_run_at.
    last_modified_at: str | None = None
    exit_code: int | None = None
    heartbeat_age_seconds: int | None = None
    captured_progress: int | None = None  # rows_out for a VM backfill; None for jobs/services
    run_log_uri: str = ""
    # Provenance (WS-D full-estate) — who launched this compute unit: "deployment-api" (has a
    # registry entry / registered Cloud Run job), "control-plane" (long-lived managed infra with
    # no registry entry — control-plane VMs, Cloud Run services, Cloud Functions), "adhoc" (live
    # but unaccounted — an ad-hoc/stranded launch, reconciliation's UNKNOWN set), or "unknown" (no
    # provenance signal yet — the AWS estate until the managed-by tag lands). None on legacy rows.
    launched_by: str | None = None
    # Leaked/unreleased resources (WS-D) — a NON-running VM still holding billable resources (data
    # disks / static IPs; the boot disk is the orphans endpoint's job). has_unreleased_resources is
    # None when it can't be determined (no GCE join — honest absence, never a false "clean"); each
    # unreleased_resources item's est_monthly_usd is an inferred list-rate estimate (principle 8).
    has_unreleased_resources: bool | None = None
    unreleased_resources: list[UnreleasedResource] | None = None
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
    # D.3 composite health. VMs: dead|hung|disk-full|oom-risk|working|stalled|workload-dead|unknown.
    # Services (ECS/Cloud Run): serving|scaled-to-zero|dead|degraded. None = kind carries no composite.
    composite_health_status: str | None = None
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
    revision: str | None = None  # Cloud Run service's latest (ready|created) revision
    region: str | None = None  # Cloud Run service's serving region
    # Cost-per-target (WS-E) — three USD figures joined from the billing exports (BigQuery +
    # Athena, `cost` already GBP→USD-converted at query time; USD-only, no currency toggle here).
    # None when the resource has no billing row yet (export lag / no resource-granularity — honest
    # absence, never a fabricated 0). Matched to the target by name == billing resource_id.
    cost_actual_usd: float | None = None  # net cost on the most recent COMPLETE billing day
    cost_avg_7d_usd: float | None = None  # avg net cost over days actually billed (not ÷7 fixed window)
    cost_projected_24h_usd: float | None = None  # most recent COMPLETE day's net; partial-day-normalised fallback
    # WS-2 date-range overlap (raw registry interval, ISO) — None for kinds with no registry entry
    # (Cloud Run jobs/services, unmanaged VMs, ...); populated only in ``_vm_item``. Distinct from
    # ``last_run_at`` (which conflates started/completed for display) because the overlap formula
    # in ``_vm_overlap_basis`` needs the two bounds separately.
    started_at: str | None = None
    completed_at: str | None = None
    last_heartbeat_at: str | None = None
    # Honest-uncertainty marker (WS-2 decision 4) — "approx" when a field above is DERIVED rather
    # than authoritative (today: a heartbeat-stale VM's effective end for date-range overlap). None
    # = authoritative. Colour-only in the UI, no text label; reused by future approx sources
    # (single-timestamp kinds, unmanaged-VM fallback) per the plan's one-convention decision.
    basis: str | None = None


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
    # WS-2 date-range archive floor (decision 5) — set only when the request carried date_from/
    # date_to. ``archive_floor`` is the earliest day the archive actually retains (the real 30-day
    # GCS TTL); ``date_range_out_of_range`` is True when the requested date_from predates it, so the
    # UI can show an explicit "no data before <archive_floor>" banner instead of a silent partial
    # result. None/False for a request with no date filter.
    archive_floor: str | None = None
    date_range_out_of_range: bool = False


class DeploymentDetailResponse(BaseModel):  # CORRECT-LOCAL: FastAPI API contract model
    """Per-target drill-down (parent plan D.2 API layer) — the deep D.1 metrics vector

    alongside the thin-list item. ``/deployments/inventory`` intentionally keeps
    ``DeploymentItem`` to composite + headline fields so the list payload stays small at
    ~200-target scale; the metrics vector below lives here instead.

    The flat ``cpu_pct``/``mem_pct``/… fields are the single most-recent sample (the live
    point-in-time reading); ``host_metrics_window`` carries the last ~10 samples the heartbeat
    daemon persisted (parent plan D.2 STORE) so the popover can plot a sparkline / mem_slope
    trend instead of a single point. All flat metrics are ``None`` — and the window is ``[]`` —
    for a kind without D.1 capture (Cloud Run/ECS/Lambda/Cloud Function — VM-only, see parent
    D.2 CAPTURE) or a VM absent from this cycle's census (honest absence, never a fabricated 0.0
    or a fake flat line).
    """

    item: DeploymentItem
    cpu_pct: float | None = None
    mem_pct: float | None = None
    mem_slope: float | None = None
    disk_pct: float | None = None
    io_write_rate_bytes_sec: float | None = None
    net_recv_rate_bytes_sec: float | None = None
    workload_alive: bool | None = None
    # D.1 rolling window (oldest first, ~10 samples) — each a {cpu_pct/mem_pct/disk_pct/
    # mem_slope/io_write.../net_recv.../sampled_at} sample; [] when the kind has no D.1 capture.
    host_metrics_window: list[dict[str, float | str]] = Field(default_factory=list)
    # Cloud Run job run-history (WS-D #11) — the last ~10 executions (newest first), each a
    # {name/status/started_at/completed_at/duration_seconds}. [] for non-job kinds so "did it fire on
    # its cadence" is answerable by eye. Fetched on the detail path only (page_size=10); the thin-list
    # census stays page_size=1 (no new cost).
    run_history: list[dict[str, str | float | None]] = Field(default_factory=list)
    # Job → manifest bridge HINT (WS-D #12) — rows since the last manifest snapshot for a job's
    # asset_group ("rows since last run"), so a fired-but-produced-nothing job is spotted from here.
    # A LINK + hint only: the AUTHORITATIVE "did the run produce its data" verdict lives on the
    # consolidator page (consolidator_throughput_backlog_monitor plan). None for non-jobs / on error.
    object_delta: int | None = None


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


def _uptime_hours(entry: DeploymentRegistryEntry, now: datetime) -> float | None:
    """Wall-clock hours run — ``started_at`` to ``completed_at`` (or ``now`` if still running)."""
    started = _parse_iso(entry.started_at)
    if started is None:
        return None
    ended = _parse_iso(entry.completed_at) or now
    return max(0.0, (ended - started).total_seconds() / 3600.0)


def _persist_alert(*, alert_class: str, workflow_name: str, severity: str, message: str, dedup_key: str) -> None:
    """Append one JSONL row to the shared GCS alert ledger — best-effort, never raises.

    Mirrors agent-orchestrator's ``notifications.slack._persist_to_gcs`` write shape exactly
    (same bucket/path/row fields; ``repo`` names the writing SERVICE, not the alerting VM) so
    ``GET /api/alerts`` (``_repo_ci_alerts.py``'s ``_parse_line``) picks these up alongside
    CI/CD + other watcher alerts with zero reader-side changes. Shard-level isolation: a
    ledger-write failure logs a warning and never breaks the inventory computation it rides on.
    """
    try:
        date = datetime.now(UTC).strftime("%Y-%m-%d")
        blob_path = f"cicd/alerts/{date}/alerts.jsonl"
        row: dict[str, object] = {
            "event_type": "slack_alert",
            "timestamp": datetime.now(UTC).isoformat(),
            "repo": "deployment-api",
            "workflow_name": workflow_name,
            "severity": severity,
            "conclusion": None,
            "message": message,
            "run_url": "",
            "dedup_key": dedup_key,
            "alert_class": alert_class,
        }
        line = json.dumps(row)
        try:
            existing = download_from_storage(_ALERTS_BUCKET, blob_path).decode("utf-8", errors="replace")
        except (OSError, ValueError, RuntimeError):  # blob may not exist yet -> start fresh
            existing = ""
        new_content = (existing.rstrip("\n") + "\n" + line + "\n").lstrip("\n")
        upload_to_storage(_ALERTS_BUCKET, blob_path, new_content.encode("utf-8"), content_type="application/jsonl")
    except (OSError, ValueError, RuntimeError) as exc:
        logger.warning("inventory: alert-ledger persist failed (%s/%s): %s", alert_class, workflow_name, exc)


def _alert_on_health_transition(item: DeploymentItem) -> None:
    """Fire a ledger alert on a fresh transition INTO oom-risk/stalled (never a repeat-poll spam).

    Always records the item's current state (even a non-alertable one, e.g. recovery back to
    ``working``) so the NEXT transition into an alertable state is detected correctly.
    """
    status = item.composite_health_status
    with _last_alerted_health_lock:
        previous = _last_alerted_health.get(item.name)
        _last_alerted_health[item.name] = status
    if status is None or status not in _ALERT_HEALTH_STATES or status == previous:
        return
    _persist_alert(
        alert_class=status,  # "oom-risk" | "stalled"
        workflow_name=f"vm-health-{item.name}",
        severity="CRITICAL" if status == "oom-risk" else "WARNING",
        message=f"{item.name} ({item.service}/{item.asset_group}) is {status}",
        dedup_key=f"vm-health-{item.name}-{status}",
    )


def _prune_stale_alert_state(census_vm_names: set[str]) -> None:
    """Drop ``_last_alerted_health`` entries for VM names absent from the current census.

    Called once per full GCP census (GCE VMs are all-region aggregated regardless of
    ``region_scope`` — see ``_compute_inventory`` — so ``census_vm_names`` is always the
    complete live+registered VM name set, never a partial/region-scoped one). Without this
    the map grows forever with every VM ever seen across the fleet's churn (short-lived
    backfill VMs launched and torn down continuously).
    """
    with _last_alerted_health_lock:
        stale = [name for name in _last_alerted_health if name not in census_vm_names]
        for name in stale:
            del _last_alerted_health[name]


def _vm_item(
    entry: DeploymentRegistryEntry,
    now: datetime,
    vm_details: dict[str, object] | None = None,
    *,
    control_plane_running: bool | None = None,
    object_deltas: dict[str, int | None] | None = None,
    disk_details: dict[str, dict[str, object]] | None = None,
    addresses: dict[str, dict[str, object]] | None = None,
) -> DeploymentItem:
    """Build an inventory item from a VM deployment-registry entry.

    ``vm_details`` is this VM's entry from the GCE aggregated-list join
    (``get_vm_instance_details``), if any — surfaces the Tier-0 free wins
    (machine_type/zone/labels/boot-disk/raw status) already fetched there but
    previously discarded down to just the running-VM-name set.
    ``control_plane_running`` is the same GCE aggregated-list confirmation,
    reduced to a bool — feeds the D.3 composite classifier's ``dead``/``hung``.
    ``object_deltas`` is the BATCH-umbrella asset_group -> manifest object-delta
    map the caller batches once per census cycle (``_batched_object_deltas``) —
    feeds the composite classifier's ``stalled``.
    """
    target = _classify_vm(entry.vm_name)
    hb_age = _heartbeat_age_seconds(entry, now)
    status = _vm_status(entry, hb_age)
    object_delta = None
    if target.umbrella == DeploymentUmbrella.BATCH:
        object_delta = (object_deltas or {}).get(target.asset_group)
    run_log = ""
    completed_at = entry.completed_at
    if completed_at and len(completed_at) >= 10:
        date_stamp = completed_at[:10].replace("-", "")
        if date_stamp.isdigit():
            run_log = vm_run_log_rolling_uri(entry.vm_name, date_stamp)
    details = vm_details or {}
    labels = details.get("labels")
    has_unreleased, unreleased = detect_unreleased_resources(
        entry.vm_name, vm_details, disk_details or {}, addresses or {}, is_running=bool(control_plane_running)
    )
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
        launched_by=LAUNCHED_BY_DEPLOYMENT_API,  # a registry entry is the deployment-api-launched signal
        has_unreleased_resources=has_unreleased,
        unreleased_resources=unreleased or None,
        rows_in=entry.rows_in,
        rows_error=entry.rows_error,
        events_emitted=entry.events_emitted,
        uptime_hours=_uptime_hours(entry, now),
        machine_type=str(details.get("machine_type") or "") or None,
        zone=str(details.get("zone") or "") or None,
        health_status=str(details.get("status") or "") or None,
        boot_disk_name=str(details.get("boot_disk_name") or "") or None,
        labels=cast(dict[str, str], labels) if labels else None,
        composite_health_status=_composite_health_status(
            entry,
            hb_age,
            control_plane_running=control_plane_running,
            umbrella=target.umbrella,
            object_delta=object_delta,
        ),
        started_at=entry.started_at or None,
        completed_at=entry.completed_at,
        last_heartbeat_at=entry.last_heartbeat_at or None,
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
    # A live job that binds to a CLOUD_RUN_JOBS registry entry is deployment-api-launched; an
    # off-pattern live job with no registry hint is adhoc (the job-kind analogue of an unmanaged VM).
    launched_by = LAUNCHED_BY_DEPLOYMENT_API if _match_registered_job(job_name) is not None else LAUNCHED_BY_ADHOC
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
        launched_by=launched_by,
        region=live.region or None,  # multi-region census surfaces which region the job lives in
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
            launched_by=LAUNCHED_BY_DEPLOYMENT_API,  # driven from the CLOUD_RUN_JOBS registry
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
        launched_by=LAUNCHED_BY_DEPLOYMENT_API,  # driven from the CLOUD_RUN_JOBS registry
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
        launched_by=LAUNCHED_BY_CONTROL_PLANE,  # managed platform infra, no registry entry
    )


# ---------------------------------------------------------------------------
# Service-health sub-taxonomy (parent plan D.3) — ECS / Cloud Run service
# composite status. Services have no manifest/object-delta (they're not data
# producers), so they get a SEPARATE 4-state set from the VM 7-state taxonomy.
# The pure classifiers live in ``_service_health`` (imported + re-exported at the
# top of this module — shared with the AWS row builder in ``_aws_deployments``,
# where a reverse import would cycle) and are wired into the live service rows
# below (``_cloud_run_service_item`` + ``_ecs_service_item``).
# ---------------------------------------------------------------------------


# ---------------------------------------------------------------------------
# Cloud Run service items (live census — no live/batch/paper phase, Open-Q1)
# ---------------------------------------------------------------------------


def _cloud_run_service_item(status: CloudRunServiceStatus) -> DeploymentItem:
    """Build an inventory item for ONE live Cloud Run **service**.

    Services (``deployment-api``, ``market-data-query``, ``alerting``, ...) have no
    live/batch/paper phase — the wire ``umbrella`` is ``DeploymentUmbrella.NONE``
    (Open-Q1, 2026-07-09: no PLATFORM/INFRA umbrella was added; the Kind column
    alone tells the operator "this is a service"). ``service``/``asset_group``
    still derive via the single ``classify_deployment_target`` resolver — a
    nominal ``LONG_LIVED_LIVE`` lifecycle_class only satisfies the resolver's
    umbrella requirement; that resolved umbrella is discarded in favour of
    ``DeploymentUmbrella.NONE`` below (the resolver has no NONE lifecycle_class
    mapping to derive it from — see ``UMBRELLA_FOR_LIFECYCLE_CLASS``).
    """
    target = classify_deployment_target(
        status.name,
        lifecycle_class=LifecycleClass.LONG_LIVED_LIVE.value,
        cloud=DeploymentCloud.GCP,
        kind=DeploymentKind.CLOUD_RUN_SERVICE,
    )
    return DeploymentItem(
        name=status.name,
        kind=target.kind.value,
        umbrella=DeploymentUmbrella.NONE.value,
        cloud=target.cloud.value,
        service=target.service,
        asset_group=target.asset_group,
        status="running" if status.ready else "pending",
        # D.3 service sub-taxonomy (serving/scaled-to-zero/dead/degraded) — the composite
        # health chip. A ready service with min-instances > 0 is serving; ready + min 0 is an
        # idle scale-to-zero (neutral, not red); a revision that failed to go ready is dead.
        composite_health_status=cloud_run_service_health_status(
            ready=status.ready, min_instance_count=status.min_instance_count
        ),
        last_run_at=None,
        exit_code=None,
        heartbeat_age_seconds=None,
        captured_progress=None,
        run_log_uri="",
        launched_by=LAUNCHED_BY_CONTROL_PLANE,  # managed platform service, no registry entry
        revision=status.revision,
        region=status.region,
    )


# ---------------------------------------------------------------------------
# Inventory assembly + filtering
# ---------------------------------------------------------------------------


def _batched_object_deltas(vm_entries: list[DeploymentRegistryEntry], now: datetime) -> dict[str, int | None]:
    """ONE manifest object-delta lookup per DISTINCT BATCH-umbrella asset_group, never per VM.

    A VM census can carry hundreds of entries sharing a handful of asset_groups — re-deriving
    ``object_delta_for_asset_group`` (a GCS manifest-index read) once per VM would be an N+1
    against the manifest and would violate WS-D.0 principle 5 (zero new bucket walks). This
    collects the distinct (BATCH-umbrella, running, asset_group-carrying) set first, then reads
    each asset_group's delta exactly once; shard-level isolation — one asset_group's read failure
    (already caught inside ``object_delta_for_asset_group``) never drops another's entry.
    """
    asset_groups: set[str] = set()
    for entry in vm_entries:
        if entry.status != "running":
            continue
        try:
            target = _classify_vm(entry.vm_name)
        except UnclassifiedDeploymentError:
            continue
        if target.umbrella == DeploymentUmbrella.BATCH and target.asset_group:
            asset_groups.add(target.asset_group)
    if not asset_groups:
        return {}

    # Each asset_group's delta is an independent GCS manifest-index read; running them serially
    # (one per distinct asset_group) routinely blew past the 45 s per-provider census bound on a
    # cold cycle, degrading object-delta to empty and pushing the BATCH working/stalled composite
    # to "unknown". Fan them out — the read is I/O-bound (GIL released), so this is ~max(one read)
    # not their sum. object_delta_for_asset_group already catches its own errors (returns
    # (None, reason)), so no per-ag read can crash the map.
    def _delta(asset_group: str) -> tuple[str, int | None]:
        return asset_group, object_delta_for_asset_group(asset_group, now)[0]

    workers = min(_OBJECT_DELTA_WORKERS, len(asset_groups))
    with ThreadPoolExecutor(max_workers=workers, thread_name_prefix="obj-delta") as pool:
        return dict(pool.map(_delta, asset_groups))


# Raw GCE instance status -> inventory wire status for an UNMANAGED VM (no registry entry, so no
# heartbeat-derived _vm_status to lean on). RUNNING is live; the provisioning states are pending;
# every stop/terminate/suspend state collapses to "stopped" (gone); anything else is honest "unknown".
_GCE_STATUS_TO_WIRE: dict[str, str] = {
    "RUNNING": "running",
    "PROVISIONING": "pending",
    "STAGING": "pending",
    "STOPPING": "stopped",
    "STOPPED": "stopped",
    "SUSPENDING": "stopped",
    "SUSPENDED": "stopped",
    "REPAIRING": "unknown",
    "TERMINATED": "stopped",
}


def _unmanaged_vm_item(
    name: str,
    details: dict[str, object],
    now: datetime,
    *,
    disk_details: dict[str, dict[str, object]] | None = None,
    addresses: dict[str, dict[str, object]] | None = None,
) -> DeploymentItem:
    """Build an inventory row for a LIVE GCE instance that has NO deployment-registry entry.

    The full-estate census (WS-D) unions the registry with the live GCE aggregated-list so an
    unregistered instance — an agent/operator ad-hoc launch, or a long-lived control-plane VM — is
    never invisible in the cockpit (the exact "stranded VM chilling on our money" case). It carries
    ONLY the live GCE state the aggregated-list already fetched: the raw status (surfaced verbatim
    via ``health_status``), machine_type/zone/labels/boot-disk, and an ``uptime_hours`` derived from
    the instance ``creation_timestamp`` (so a 16-day zombie reads its true age). Registry-derived
    fields (heartbeat / rows / exit_code / composite health) honestly stay ``None`` — there is no
    registry entry to source them from. Classification degrades to a minimal NONE-umbrella row
    (never hidden) when the name can't be resolved. Provenance is ``control-plane`` for a managed
    out-of-band prefix, else ``adhoc`` (reconciliation's UNKNOWN set).
    """
    raw_status = str(details.get("status") or "")
    try:
        target = classify_vm_target(name)
        umbrella, service, asset_group = target.umbrella.value, target.service, target.asset_group
    except UnclassifiedDeploymentError:
        umbrella, service, asset_group = DeploymentUmbrella.NONE.value, name, ""
    created = _parse_iso(str(details.get("creation_timestamp") or "") or None)
    uptime = (now - created).total_seconds() / 3600.0 if created is not None and raw_status == "RUNNING" else None
    labels = details.get("labels")
    has_unreleased, unreleased = detect_unreleased_resources(
        name, details, disk_details or {}, addresses or {}, is_running=raw_status == "RUNNING"
    )
    return DeploymentItem(
        name=name,
        kind=DeploymentKind.VM.value,
        umbrella=umbrella,
        cloud=DeploymentCloud.GCP.value,
        service=service,
        asset_group=asset_group,
        status=_GCE_STATUS_TO_WIRE.get(raw_status, "unknown"),
        last_run_at=created.isoformat() if created is not None else None,
        run_log_uri="",
        launched_by=LAUNCHED_BY_CONTROL_PLANE if is_control_plane_vm(name) else LAUNCHED_BY_ADHOC,
        has_unreleased_resources=has_unreleased,
        unreleased_resources=unreleased or None,
        uptime_hours=max(0.0, uptime) if uptime is not None else None,
        machine_type=str(details.get("machine_type") or "") or None,
        zone=str(details.get("zone") or "") or None,
        health_status=raw_status or None,
        boot_disk_name=str(details.get("boot_disk_name") or "") or None,
        labels=cast(dict[str, str], labels) if labels else None,
    )


def build_inventory(
    vm_entries: list[DeploymentRegistryEntry],
    cloud_run_status: dict[str, CloudRunExecutionStatus],
    now: datetime,
    vm_details_by_name: dict[str, dict[str, object]] | None = None,
    cloud_run_services: list[CloudRunServiceStatus] | None = None,
    object_deltas: dict[str, int | None] | None = None,
    disk_details: dict[str, dict[str, object]] | None = None,
    addresses: dict[str, dict[str, object]] | None = None,
) -> list[DeploymentItem]:
    """Assemble the full unified inventory (VMs + Cloud Run jobs + Cloud Run services).

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

    A populated ``vm_details_by_name`` ALSO drives the full-estate census: every live GCE
    instance in the join with no matching ``vm_entries`` name is emitted as an ``unmanaged``
    row (``_unmanaged_vm_item``, ``launched_by=adhoc``/``control-plane``) so an unregistered VM
    is never invisible. This is the same (registry vs live-GCE) union ``fleet_reconciliation``
    computes — reused, not rebuilt. ``None``/``{}`` add no unmanaged rows.

    ``object_deltas`` is the BATCH-umbrella asset_group -> manifest object-delta
    map (``_batched_object_deltas``) — feeds the composite classifier's
    ``stalled``. ``None`` (the default, and every existing caller/test) degrades
    ``stalled`` to the honest ``"unknown"`` it already fell back to; this
    function stays a pure, I/O-free classifier either way — the manifest read
    happens in the caller (``_compute_inventory``), never inside this loop.

    Cloud Run jobs census the LIVE job list (``cloud_run_status`` keys, already
    fetched by ``latest_execution_by_job``'s ``run_v2.JobsClient.list_jobs`` — no
    new API call) — ``CLOUD_RUN_JOBS`` is a classification HINT, not an allow-list,
    so an off-pattern job (no registry match) still gets a row instead of hiding.
    Only when the live list itself is empty (the GCP call failed) does the census
    degrade to the static registry with status="unknown" (never an empty census).

    Cloud Run services (``cloud_run_services``, optional — omitted/``None`` yields
    zero service rows, never an error) census the LIVE service list
    (``list_cloud_run_services``'s ``run_v2.ServicesClient.list_services``); a
    per-service classification failure is logged + skipped, same shard-level
    isolation as the VM loop above — one bad service name never blocks the rest
    of the census.
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
                    object_deltas=object_deltas,
                    disk_details=disk_details,
                    addresses=addresses,
                )
            )
        except UnclassifiedDeploymentError as exc:
            logger.warning("inventory: skipping unclassifiable VM %r: %s", entry.vm_name, exc)
    # Full-estate census (WS-D): union the registry rows above with the live GCE aggregated-list so
    # EVERY live instance gets a row — an unregistered VM (an ad-hoc launch or an out-of-band
    # control-plane VM) becomes an `unmanaged` row instead of being invisible. This reuses the SAME
    # (registry vs live-GCE) union fleet_reconciliation computes — the registered set is exactly the
    # vm_entries names, the live set is the aggregated-list join already passed in — so there is no
    # second census. Only a REAL join adds rows: None (no join offered — pure-classification paths)
    # and {} (a real, empty GCE census) both add nothing.
    if vm_details_by_name:
        registered_names = {entry.vm_name for entry in vm_entries}
        for unmanaged_name in sorted(set(vm_details_by_name) - registered_names):
            items.append(
                _unmanaged_vm_item(
                    unmanaged_name,
                    vm_details_by_name[unmanaged_name],
                    now,
                    disk_details=disk_details,
                    addresses=addresses,
                )
            )
    if cloud_run_status:
        for job_name, status in cloud_run_status.items():
            items.append(_cloud_run_item_for_live_job(job_name, status))
    else:
        for target in CLOUD_RUN_JOBS:
            items.append(_cloud_run_item(target, cloud_run_status))
    for service_status in cloud_run_services or []:
        try:
            items.append(_cloud_run_service_item(service_status))
        except UnclassifiedDeploymentError as exc:
            logger.warning("inventory: skipping unclassifiable Cloud Run service %r: %s", service_status.name, exc)
    return items


# Heartbeat-staleness threshold for the date-range overlap formula — the SAME constant
# ``DeploymentsRegistry.reap_stale`` uses (``max_age_hours: int = 6``,
# unified_trading_library/deployment_registry.py). Reusing it keeps "still running" honest for
# overlap math: the 2026-07-20 audit found 219 registry rows reading ``status=running`` while only
# 12 GCE instances were actually RUNNING — a naive ``completed_at is None -> still open`` overlap
# test would badly overcount those heartbeat-stale zombies as live for every date range.
_REAP_STALE_HOURS = 6


def _parse_date_query(raw: str | None, *, end_of_day: bool) -> datetime | None:
    """Parse a ``date_from``/``date_to`` query value to a UTC datetime; 400s on a bad value.

    Accepts a bare ``YYYY-MM-DD`` (anchored to 00:00:00, or 23:59:59.999999 when ``end_of_day`` so a
    single-day range ``date_from == date_to`` is still inclusive) or a full ISO-8601 instant (used
    exactly as given).
    """
    if not raw:
        return None
    parsed = _parse_iso(raw)
    if parsed is None:
        raise HTTPException(status_code=400, detail=f"invalid date {raw!r} — expected YYYY-MM-DD or ISO-8601")
    if end_of_day and len(raw) == 10:  # bare date, no time component
        return parsed.replace(hour=23, minute=59, second=59, microsecond=999999)
    return parsed


def _vm_overlap_basis(
    *,
    started_at: datetime | None,
    completed_at: datetime | None,
    last_heartbeat_at: datetime | None,
    now: datetime,
    date_from: datetime | None,
    date_to: datetime | None,
) -> tuple[bool, str | None]:
    """VM/registry overlap test against ``[date_from, date_to]`` (WS-2 date-range filter).

    ``effective_end`` = ``completed_at`` when the row is terminal; else ``last_heartbeat_at`` once
    the heartbeat is stale (``>_REAP_STALE_HOURS``); else the row is truly live and open-ended
    (always overlaps). A heartbeat-derived ``effective_end`` is honestly approximate, so the caller
    gets ``basis="approx"`` back to stamp on the row (decision 4 — colour-only, no text label).
    ``started_at is None`` (no interval data at all) never filters the row OUT — honest-absence, it
    simply isn't date-range-scoped.
    """
    if date_from is None and date_to is None:
        return True, None
    if started_at is None:
        return True, None
    if date_to is not None and started_at > date_to:
        return False, None
    if completed_at is not None:
        return (date_from is None or completed_at >= date_from), None
    if last_heartbeat_at is not None and (now - last_heartbeat_at) > timedelta(hours=_REAP_STALE_HOURS):
        return (date_from is None or last_heartbeat_at >= date_from), "approx"
    return True, None  # open-ended — truly live, always overlaps


# Kinds with only ONE observable timestamp — no true start/end interval exists (WS-2 decision:
# match on it anyway, honestly marked approx). CLOUD_RUN_JOB covers BOTH the GCP Cloud Run job
# builder and the AWS Batch (Fargate) builder — they share this wire kind (see ``_aws_deployments``
# ``_batch_item``). SCHEDULER is Cloud Scheduler's single fire time (``last_attempt_at``).
_SINGLE_TIMESTAMP_KINDS = frozenset({DeploymentKind.CLOUD_RUN_JOB.value, "SCHEDULER"})


def _single_timestamp_overlaps(
    last_run_at: datetime | None, date_from: datetime | None, date_to: datetime | None
) -> tuple[bool, str | None]:
    """Point-in-range test for a kind with only ONE observable timestamp, not a true interval:
    an unmanaged VM's creation time, a Cloud Run/AWS Batch job's last run, or a Cloud Scheduler
    job's last fire (``item.last_run_at`` in every case — the field the respective row builders
    already populate it into). The single point stands in for the whole interval, so a MATCH is
    always ``basis="approx"`` — never claimed authoritative. ``last_run_at is None`` (no signal at
    all) is never filtered out — honest-absence, same convention as ``_vm_overlap_basis``.
    """
    if date_from is None and date_to is None:
        return True, None
    if last_run_at is None:
        return True, None
    if date_from is not None and last_run_at < date_from:
        return False, "approx"
    if date_to is not None and last_run_at > date_to:
        return False, "approx"
    return True, "approx"


def _apply_date_range(
    items: list[DeploymentItem],
    now: datetime,
    date_from: datetime | None,
    date_to: datetime | None,
) -> list[DeploymentItem]:
    """Scope registry-backed + single-timestamp rows to ``[date_from, date_to]`` (WS-2 overlap query).

    VM rows with a real registry interval (``started_at`` set) use ``_vm_overlap_basis``. VM rows
    with NO interval (unmanaged/AWS-EC2 — no registry entry to source one from) fall back to the
    single-timestamp match on ``last_run_at``, same as Cloud Run jobs/AWS Batch/Scheduler
    (``_SINGLE_TIMESTAMP_KINDS``). Every other kind (services, functions, disks, ...) passes through
    unfiltered — they carry no timestamp signal at all to scope on. Never mutates a cached
    ``DeploymentItem`` in place: ``_inventory_cache`` entries are shared across concurrent requests
    with different date ranges, so a stamped ``basis`` must land on a COPY, not the cached object.
    """
    if date_from is None and date_to is None:
        return items
    kept: list[DeploymentItem] = []
    for item in items:
        started_at = _parse_iso(item.started_at) if item.kind == DeploymentKind.VM.value else None
        if started_at is not None:
            overlaps, basis = _vm_overlap_basis(
                started_at=started_at,
                completed_at=_parse_iso(item.completed_at),
                last_heartbeat_at=_parse_iso(item.last_heartbeat_at),
                now=now,
                date_from=date_from,
                date_to=date_to,
            )
        elif item.kind == DeploymentKind.VM.value or item.kind in _SINGLE_TIMESTAMP_KINDS:
            overlaps, basis = _single_timestamp_overlaps(_parse_iso(item.last_run_at), date_from, date_to)
        else:
            kept.append(item)
            continue
        if not overlaps:
            continue
        kept.append(item.model_copy(update={"basis": basis}) if basis else item)
    return kept


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
        # Always-on Cloud Run service (WS-B kinds census) — no live/batch/paper
        # phase, so umbrella is DeploymentUmbrella.NONE (Open-Q1); Kind carries
        # the fact that this is a service.
        DeploymentItem(
            name="deployment-api",
            kind="CLOUD_RUN_SERVICE",
            umbrella=DeploymentUmbrella.NONE.value,
            cloud="GCP",
            service="deployment-api",
            asset_group="",
            status="running",
            last_run_at=None,
            exit_code=None,
            heartbeat_age_seconds=None,
            captured_progress=None,
            run_log_uri="",
            revision="deployment-api-00042-xyz",
            region="asia-northeast1",
        ),
    ]


def _load_aws_items(now: datetime, regions: tuple[str, ...] = _CONFIGURED_AWS_REGIONS) -> list[DeploymentItem]:
    """Census + classify the live AWS estate into inventory items (Phase 5 parity), multi-region.

    Reuses the curated ``_vm_lifecycle_class`` prefix resolver so AWS umbrella derivation matches
    GCP exactly. Fans out across ``regions`` (the configured set, or the curated all-regions set on
    the ``?all_regions`` sweep) with per-region isolation — a region's census failure (no creds /
    boto3 absent / API down / unsupported) degrades to empty for THAT region and never blocks the
    others or the GCP inventory. AWS rides the same ``DeploymentItem`` contract.
    """

    def _one(region: str) -> list[DeploymentItem]:
        try:
            item_dicts = load_aws_inventory(
                region=region,
                aws_account_id=_cfg.aws_account_id or "",
                lifecycle_for_name=_vm_lifecycle_class,
            )
            return [DeploymentItem(**d) for d in item_dicts]  # type: ignore[arg-type]
        except (OSError, ValueError, RuntimeError) as exc:
            logger.warning("inventory: AWS census for region %s degraded: %s", region, exc)
            return []

    if not regions:
        return []
    items: list[DeploymentItem] = []
    with ThreadPoolExecutor(max_workers=min(8, len(regions)), thread_name_prefix="aws-region") as pool:
        for region_items in pool.map(_one, regions):
            items.extend(region_items)
    return items


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


def _load_registry_entries(now: datetime) -> list[DeploymentRegistryEntry]:
    """Census the deployment registry: RUNNING deployments + the 7-day archive window.

    RUNNING entries come from :func:`resolve_active_registry` (Firestore-first indexed query
    when the migration flag is on, loud GCS-``active/`` fallback otherwise) — the scale path
    that replaces downloading ~3k ``active/`` blobs. The 7-day ARCHIVE window stays on the
    GCS parallel download (bounded, and the dead-VM/hard-killed detection the D.3 ``dead``
    composite state needs — a registry entry whose VM the control plane no longer has must
    reach ``build_inventory``/``_composite_health_status`` to be classified ``dead``).

    DECOUPLED from the GCE aggregated-list read (``get_vm_instance_details``) — the caller
    runs that as a SEPARATE census future. So a slow/failed registry read degrades this to a
    short/empty list WITHOUT dropping the live VMs: those render from the GCE join
    (``build_inventory``'s unmanaged-row union). This removes the old failure mode where one
    bundled future timed out and blanked BOTH the VM list and the registry (empty prod tab).
    """
    active = resolve_active_registry()
    client = get_storage_client()
    bucket = DEFAULT_BUCKET
    today = now.date()
    archive_prefixes = [
        f"{ARCHIVE_PREFIX}{(today - timedelta(days=offset)).isoformat()}/" for offset in range(_ARCHIVE_WINDOW_DAYS)
    ]
    archive_keys = [key for prefix in archive_prefixes for key in _list_json_keys(client, bucket, prefix)]
    recent = _download_entries_parallel(client, bucket, archive_keys)
    return active + recent


# GCS lifecycle TTL on ``deployments/archive/`` — live-confirmed 2026-07-20 (earliest day-prefix is
# exactly 30 days before the latest). The default cold-path census stays on the cheap
# ``_ARCHIVE_WINDOW_DAYS`` (7-day) window; a date-range query needs its OWN bounded read up to this
# real floor, never the whole 30-day corpus unless the requested range actually spans it.
_ARCHIVE_RETENTION_DAYS = 30


def _archive_floor_date(now: datetime) -> date:
    """The earliest day the archive actually retains data for (the real 30-day GCS floor)."""
    return (now - timedelta(days=_ARCHIVE_RETENTION_DAYS - 1)).date()


def _load_registry_entries_for_date_range(
    now: datetime, date_from: datetime | None, date_to: datetime | None
) -> tuple[list[DeploymentRegistryEntry], bool]:
    """Day-partitioned archive read scoped to ``[date_from, date_to]``, up to the 30-day GCS floor.

    Bypasses the default cold-path's 7-day ``_ARCHIVE_WINDOW_DAYS`` cap — a date-range query reads
    exactly the requested days directly (``deployments/archive/<day>/`` prefixes), still a BOUNDED
    listing, never a whole-corpus walk. Returns ``(entries, out_of_range)``: ``out_of_range`` is True
    when the requested ``date_from`` predates the real retention floor (decision 5 — the caller turns
    this into an explicit "no data before `<date>`" banner, never a silently clipped partial result).
    An empty/backwards clipped window (e.g. the whole request predates the floor) returns ``[]`` with
    ``out_of_range`` still correctly reported.
    """
    floor_date = _archive_floor_date(now)
    today = now.date()
    range_start = date_from.date() if date_from is not None else floor_date
    range_end = date_to.date() if date_to is not None else today
    out_of_range = range_start < floor_date
    clipped_start = max(range_start, floor_date)
    clipped_end = min(range_end, today)
    if clipped_start > clipped_end:
        return [], out_of_range

    client = get_storage_client()
    bucket = DEFAULT_BUCKET
    day_count = (clipped_end - clipped_start).days + 1
    prefixes = [
        f"{ARCHIVE_PREFIX}{(clipped_start + timedelta(days=offset)).isoformat()}/" for offset in range(day_count)
    ]
    keys = [key for prefix in prefixes for key in _list_json_keys(client, bucket, prefix)]
    entries = _download_entries_parallel(client, bucket, keys)
    return entries, out_of_range


def _scheduler_item(entry: SchedulerJobStatus) -> DeploymentItem:
    """Build an inventory row for one Cloud Scheduler job (WS-D #9) — the on-time / OVERDUE signal.

    Kind ``SCHEDULER``; ``composite_health_status`` carries the verdict (``overdue`` / ``on-time`` /
    ``paused``) so the UI slots it into the Health column (Cloud Run job rows carry none today).
    ``service`` is the target it triggers so the UI can cross-link to that job's row.
    """
    if entry.overdue:
        status, health = "failed", "overdue"
    elif entry.state == "ENABLED":
        status, health = "running", "on-time"
    else:
        status, health = "stopped", "paused"
    return DeploymentItem(
        name=entry.name,
        kind="SCHEDULER",
        umbrella=DeploymentUmbrella.NONE.value,
        cloud=DeploymentCloud.GCP.value,
        service=entry.target or entry.name,
        asset_group="",
        status=status,
        last_run_at=entry.last_attempt_at,
        run_log_uri="",
        launched_by=LAUNCHED_BY_CONTROL_PLANE,  # Cloud Scheduler is managed infra
        composite_health_status=health,
        region=entry.region or None,
    )


def _multi_region_scheduler(project_id: str, regions: tuple[str, ...], now: datetime) -> list[SchedulerJobStatus]:
    """Cloud Scheduler jobs across every region in ``regions``, concatenated (per-region isolation)."""
    entries: list[SchedulerJobStatus] = []
    if not regions:
        return entries
    with ThreadPoolExecutor(max_workers=min(8, len(regions)), thread_name_prefix="scheduler-region") as pool:
        for region_list in pool.map(lambda r: list_scheduler_jobs(project_id, r, now), regions):
            entries.extend(region_list)
    return entries


def _orphaned_resource_items(
    disk_details: dict[str, dict[str, object]],
    unattached_disk_names: set[str],
    addresses: dict[str, dict[str, object]],
) -> list[DeploymentItem]:
    """First-class rows for truly-orphaned resources with NO owning VM (WS-D #7): unattached
    persistent disks + reserved static IPs with no user.

    ``GET /api/fleet/orphans`` is VM-keyed and misses these entirely, yet each still bills. Emitted
    as ``kind=DISK`` / ``kind=STATIC_IP``, ``launched_by=unknown`` (no VM to attribute provenance),
    flagged ``has_unreleased_resources=True`` (the row IS the leak) with its inferred monthly cost,
    so the estate stranded-cost total picks it up. Disjoint from the #4 VM-attached leaks — a disk
    attached to a stopped VM has a ``users`` entry (not unattached) and an attached IP has a
    ``users`` self-link, so neither is double-counted here.
    """
    items: list[DeploymentItem] = []
    for disk_name in sorted(unattached_disk_names):
        disk = disk_details.get(disk_name, {})
        size_raw = disk.get("size_gb")
        size_gb = int(size_raw) if isinstance(size_raw, int) else None
        type_raw = disk.get("type")
        disk_type = str(type_raw) if isinstance(type_raw, str) and type_raw else None
        items.append(
            DeploymentItem(
                name=disk_name,
                kind="DISK",
                umbrella=DeploymentUmbrella.NONE.value,
                cloud=DeploymentCloud.GCP.value,
                service=disk_name,
                asset_group="",
                status="stopped",  # allocated + idle (no running workload) — the orphaned state
                run_log_uri="",
                launched_by=LAUNCHED_BY_UNKNOWN,
                has_unreleased_resources=True,
                unreleased_resources=[orphaned_disk(disk_name, size_gb, disk_type)],
            )
        )
    for addr_name, addr in sorted(addresses.items()):
        if addr.get("users"):  # attached to something → a VM's leaked resource (#4), not an orphan
            continue
        items.append(
            DeploymentItem(
                name=addr_name,
                kind="STATIC_IP",
                umbrella=DeploymentUmbrella.NONE.value,
                cloud=DeploymentCloud.GCP.value,
                service=addr_name,
                asset_group="",
                status="stopped",
                run_log_uri="",
                launched_by=LAUNCHED_BY_UNKNOWN,
                has_unreleased_resources=True,
                unreleased_resources=[orphaned_static_ip(addr_name)],
                region=str(addr.get("region") or "") or None,
            )
        )
    return items


def _gcp_regions_for_scope(region_scope: str, project_id: str) -> tuple[str, ...]:
    """The GCP regions to census for this scope: the configured default (``""``), every compute region
    (``"ALL"`` — falling back to the configured set if the region-list read fails), or a single
    caller-picked region."""
    if region_scope == "ALL":
        names = list_gcp_region_names(project_id)
        return tuple(names) if names else _CONFIGURED_GCP_REGIONS
    if not region_scope:
        return _CONFIGURED_GCP_REGIONS
    return (region_scope,)


def _aws_regions_for_scope(region_scope: str) -> tuple[str, ...]:
    """The AWS regions to census for this scope: the primary set (``""``), the curated full set
    (``"ALL"``), or the GCP-picked region's best-effort AWS equivalent (``_GCP_TO_AWS_REGION`` —
    primary set when unpaired)."""
    if region_scope == "ALL":
        return _ALL_AWS_REGIONS
    if not region_scope:
        return _CONFIGURED_AWS_REGIONS
    aws_equiv = _GCP_TO_AWS_REGION.get(region_scope)
    return (aws_equiv,) if aws_equiv else _CONFIGURED_AWS_REGIONS


def _multi_region_jobs(project_id: str, regions: tuple[str, ...]) -> dict[str, CloudRunExecutionStatus]:
    """Cloud Run jobs across every region in ``regions``, merged (region carried on each status).

    Each per-region ``latest_execution_by_job`` already degrades to ``{}`` on its own error, so a
    region that is down / unsupported never blocks the others (per-region honest isolation). A
    short-name collision across regions is vanishingly rare (all real jobs are single-region today);
    the later region wins, and each row carries its ``region`` so the collision is visible.
    """
    merged: dict[str, CloudRunExecutionStatus] = {}
    if not regions:
        return merged
    with ThreadPoolExecutor(max_workers=min(8, len(regions)), thread_name_prefix="cr-jobs-region") as pool:
        for region_map in pool.map(lambda r: latest_execution_by_job(project_id, region=r), regions):
            merged.update(region_map)
    return merged


def _multi_region_services(project_id: str, regions: tuple[str, ...]) -> list[CloudRunServiceStatus]:
    """Cloud Run services across every region in ``regions``, concatenated (per-region isolation)."""
    services: list[CloudRunServiceStatus] = []
    if not regions:
        return services
    with ThreadPoolExecutor(max_workers=min(8, len(regions)), thread_name_prefix="cr-svc-region") as pool:
        for region_list in pool.map(lambda r: list_cloud_run_services(project_id, region=r), regions):
            services.extend(region_list)
    return services


def _multi_region_functions(project_id: str, regions: tuple[str, ...]) -> dict[str, CloudFunctionStatus]:
    """Cloud Functions across every region in ``regions``, merged (per-region isolation)."""
    merged: dict[str, CloudFunctionStatus] = {}
    if not regions:
        return merged
    with ThreadPoolExecutor(max_workers=min(8, len(regions)), thread_name_prefix="cf-region") as pool:
        for region_map in pool.map(lambda r: list_cloud_functions(project_id, region=r), regions):
            merged.update(region_map)
    return merged


def _compute_inventory(now: datetime, cloud: str | None, region_scope: str = "") -> list[DeploymentItem]:
    """Build the live inventory (registry VMs + Cloud Run executions + AWS) — the cold path.

    ``region_scope`` selects the census breadth: ``""`` the configured default, ``"ALL"`` every region
    (the periodic surprise-check), or a single GCP region + its AWS equivalent. GCE VMs / disks / IPs
    are all-region aggregated regardless — only the regional Cloud Run / functions / scheduler APIs
    honour the scope, so a specific-region view still lists every VM.
    """
    want_gcp = cloud is None or cloud.upper() == DeploymentCloud.GCP.value
    want_aws = cloud is None or cloud.upper() == DeploymentCloud.AWS.value

    items: list[DeploymentItem] = []

    # Fan out every wanted provider census concurrently, each resolved through
    # _census_or_degrade (wall-clock bounded, honest per-kind empty on hang/error). A single
    # slow or hung provider degrades to an empty census for its OWN kind instead of blocking
    # the whole inventory — the cold path is ~max(slowest census) not their sum, and never
    # exceeds _PROVIDER_CENSUS_TIMEOUT_SEC per provider.
    aws_regions = _aws_regions_for_scope(region_scope)
    f_aws: Future[list[DeploymentItem]] | None = (
        _census_pool.submit(_load_aws_items, now, aws_regions) if want_aws else None
    )

    if want_gcp:
        project_id = _cfg.require_gcp_project_id()
        # GCE VMs / disks / addresses use all-region aggregated lists already; only the regional
        # Cloud Run jobs/services + Cloud Functions APIs need the multi-region fan-out.
        gcp_regions = _gcp_regions_for_scope(region_scope, project_id)
        # DECOUPLED (P2 migration): two independent futures instead of one bundled read.
        #   existence  = the GCE aggregated-list (fast) → drives which live VMs get a row
        #   enrichment = the registry (Firestore-first via resolve_active_registry) → metadata
        # Each degrades on its OWN via _census_or_degrade, so a slow/failed registry read yields
        # [] but the live VMs STILL render from the GCE join (build_inventory's unmanaged-row
        # union). The old bug: ONE future bundled both, so a registry timeout dropped every live
        # VM and blanked the prod tab.
        f_vm_details: Future[dict[str, dict[str, object]]] = _census_pool.submit(get_vm_instance_details, project_id)
        f_registry: Future[list[DeploymentRegistryEntry]] = _census_pool.submit(_load_registry_entries, now)
        f_jobs = _census_pool.submit(_multi_region_jobs, project_id, gcp_regions)
        f_services = _census_pool.submit(_multi_region_services, project_id, gcp_regions)
        f_functions = _census_pool.submit(_multi_region_functions, project_id, gcp_regions)
        f_scheduler = _census_pool.submit(_multi_region_scheduler, project_id, gcp_regions, now)
        # Disk + reserved-IP maps for leaked-resource detection on non-running VMs — two more
        # aggregated_list reads, each bounded + degrading to an empty map on failure (leak detection
        # then flags nothing rather than fabricating a false leak, and non-running rows report
        # has_unreleased_resources=None honestly).
        f_disks = _census_pool.submit(get_disk_details, project_id)
        f_addresses = _census_pool.submit(list_reserved_addresses, project_id)
        # Unattached-disk names → the truly-orphaned DISK rows (#7); same disks aggregated_list
        # class (the ``users`` field). Bounded + degrades to an empty set.
        f_unattached = _census_pool.submit(list_unattached_disk_names, project_id)

        vm_details_by_name = _census_or_degrade("gcp-vm-details", f_vm_details, {})
        vm_entries = _census_or_degrade("gcp-registry", f_registry, [])
        cloud_run_status = _census_or_degrade("cloud-run-jobs", f_jobs, {})
        cloud_run_services = _census_or_degrade("cloud-run-services", f_services, [])
        cloud_function_status = _census_or_degrade("cloud-functions", f_functions, {})
        scheduler_entries: list[SchedulerJobStatus] = _census_or_degrade("cloud-scheduler", f_scheduler, [])
        disk_details = _census_or_degrade("gcp-disks", f_disks, {})
        addresses = _census_or_degrade("gcp-addresses", f_addresses, {})
        unattached_disks: set[str] = _census_or_degrade("gcp-unattached-disks", f_unattached, set())
        # Object-delta is a manifest lookup keyed off the resolved VM entries — bound it too.
        object_deltas = _census_or_degrade(
            "object-delta", _census_pool.submit(_batched_object_deltas, vm_entries, now), {}
        )

        gcp_items = build_inventory(
            vm_entries,
            cloud_run_status,
            now,
            vm_details_by_name=vm_details_by_name,
            cloud_run_services=cloud_run_services,
            object_deltas=object_deltas,
            disk_details=disk_details,
            addresses=addresses,
        )
        items.extend(gcp_items)

        with _vm_entry_by_name_lock:
            _vm_entry_by_name_cache.clear()
            _vm_entry_by_name_cache.update({e.vm_name: e for e in vm_entries})

        items.extend(_cloud_function_item(status) for status in cloud_function_status.values())

        # Cloud Scheduler rows (#9): the on-time / OVERDUE signal (Health column) per scheduled job.
        items.extend(_scheduler_item(entry) for entry in scheduler_entries)

        # First-class orphaned-resource rows (#7): unattached disks + no-owner reserved static IPs.
        items.extend(_orphaned_resource_items(disk_details, unattached_disks, addresses))

        census_vm_names: set[str] = set()
        for vm_item in gcp_items:
            if vm_item.kind == DeploymentKind.VM.value:
                _alert_on_health_transition(vm_item)
                census_vm_names.add(vm_item.name)
        _prune_stale_alert_state(census_vm_names)

    if f_aws is not None:
        aws_items: list[DeploymentItem] = _census_or_degrade("aws", f_aws, [])
        items.extend(aws_items)

    _attach_costs(items)
    return items


def _attach_costs(items: list[DeploymentItem]) -> None:
    """Best-effort: attach the 3 USD cost figures per target by name == billing resource_id.

    Reuses the cost-observability service's CACHED billing window (WS-E) — one aggregation per
    inventory refresh (the inventory itself is cached), not per request. A GCP VM's billing
    ``resource.name`` is its instance name (== the deployment item name); Cloud Run job/service
    names match likewise. AWS ``resource_id`` is an ARN/instance-id that won't match the friendly
    name → those rows keep ``None`` (honest absence, never a fabricated 0). A billing-source
    failure or mock-without-facts leaves every cost field ``None`` — cost NEVER breaks the census.
    """
    try:
        cost_by_resource = CostObservabilityService().per_resource_daily(days=7)
    except Exception as exc:
        # best-effort enrichment — cost must never break the inventory census.
        logger.warning("[deployments-inventory] cost enrichment skipped (best-effort): %s", exc)
        return
    for item in items:
        rc = cost_by_resource.get(item.name)
        if rc is not None:
            item.cost_actual_usd = rc.actual_usd
            item.cost_avg_7d_usd = rc.avg_7d_usd
            item.cost_projected_24h_usd = rc.projected_24h_usd


def _store_inventory(cache_key: str, items: list[DeploymentItem]) -> None:
    """Atomically record a fresh inventory snapshot for ``cache_key``."""
    with _inventory_lock:
        _inventory_cache[cache_key] = (time.monotonic(), items)


def _refresh_inventory(cache_key: str, cloud: str | None, region_scope: str) -> None:
    """Background cache refresh — recompute + store, then clear the in-flight flag."""
    try:
        _store_inventory(cache_key, _compute_inventory(datetime.now(UTC), cloud, region_scope))
    except (HTTPException, OSError, ValueError, RuntimeError) as exc:
        # Keep the stale snapshot on a failed refresh — never poison the cache.
        logger.warning("inventory: background refresh for %s failed: %s", cache_key, exc)
    finally:
        with _inventory_lock:
            _inventory_refreshing.discard(cache_key)


def _kick_background_refresh(cache_key: str, cloud: str | None, region_scope: str) -> None:
    """Schedule exactly one background refresh per cache key (stale-while-revalidate)."""
    with _inventory_lock:
        if cache_key in _inventory_refreshing:
            return
        _inventory_refreshing.add(cache_key)
    _inventory_refresh_pool.submit(_refresh_inventory, cache_key, cloud, region_scope)


def _load_inventory(now: datetime, cloud: str | None = None, region_scope: str = "") -> list[DeploymentItem]:
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

    # The all-regions sweep gets its OWN cache slot so the one-off surprise-check never poisons (or
    # is served from) the default configured-region snapshot.
    cache_key = f"{(cloud or 'all').upper()}|{region_scope or 'CONFIGURED'}"
    cached = _inventory_cache.get(cache_key)
    if cached is not None:
        if (time.monotonic() - cached[0]) >= _INVENTORY_TTL_SEC:
            _kick_background_refresh(cache_key, cloud, region_scope)
        return cached[1]

    # Cold path — lock so concurrent first-polls trigger exactly ONE census.
    with _inventory_lock:
        cached = _inventory_cache.get(cache_key)
        if cached is not None:
            return cached[1]
        items = _compute_inventory(now, cloud, region_scope)
        _inventory_cache[cache_key] = (time.monotonic(), items)
        return items


# ---------------------------------------------------------------------------
# Routes
# ---------------------------------------------------------------------------

_VALID_UMBRELLAS = frozenset(u.value for u in DeploymentUmbrella)
_VALID_CLOUDS = frozenset(c.value for c in DeploymentCloud)


def _normalize_region_scope(region: str | None, all_regions: bool) -> str:
    """Map the ``region`` (and legacy ``all_regions``) query params to an internal census scope token:
    ``""`` the configured default, ``"ALL"`` the every-region sweep, or a specific GCP region name.

    The default region resolves to ``""`` so it stays identical to the configured default census —
    asia-northeast1 GCP + ap-northeast-1 AWS, both Tokyo, where the orchestrator + VMs actually run."""
    value = (region or "").strip().lower()
    if all_regions or value == "all":
        return "ALL"
    if not value or value == _DEFAULT_GCP_REGION:
        return ""
    return value


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
    all_regions: bool = Query(
        False,
        description="Sweep EVERY region (a one-off surprise-check) instead of the configured region set. "
        "Off by default (the census stays on the configured regions for determinism).",
    ),
    region: str | None = Query(
        None,
        description="GCP region to census (e.g. asia-northeast1, europe-west1). Empty or the default "
        "region = the configured default; 'all' sweeps every region. AWS is scoped to the region's "
        "geographic equivalent. Only regional resources (Cloud Run / functions / scheduler) honour "
        "this — VMs / disks / IPs are all-region aggregated regardless.",
    ),
    date_from: str | None = Query(
        None,
        description="Scope VM/registry rows to those overlapping [date_from, date_to] (YYYY-MM-DD or "
        "ISO-8601). VM overlap = started_at <= date_to AND effective_end >= date_from, where "
        "effective_end is completed_at, or last_heartbeat_at once heartbeat-stale (>6h), or "
        "open-ended while truly live. Other kinds (no registry interval) are unaffected.",
    ),
    date_to: str | None = Query(
        None,
        description="See date_from. A bare date is inclusive of its whole day.",
    ),
) -> DeploymentInventoryResponse:
    """Unified deployment inventory: every VM + Cloud Run job, classified by umbrella.

    GCP **and** AWS (Phase 5 parity) — AWS EC2 backfill VMs + Batch Fargate jobs ride
    the same ``DeploymentItem`` contract with ``cloud=AWS``. Each item carries its
    umbrella/cloud/service/asset_group classification + live status / last-run /
    exit_code / heartbeat / captured-progress. ``all_regions=true`` sweeps every region
    (the periodic surprise-check); the default censuses the configured region set.
    """
    now = datetime.now(UTC)
    region_scope = _normalize_region_scope(region, all_regions)
    parsed_date_from = _parse_date_query(date_from, end_of_day=False)
    parsed_date_to = _parse_date_query(date_to, end_of_day=True)
    items = _load_inventory(now, cloud=cloud, region_scope=region_scope)

    archive_floor_str: str | None = None
    out_of_range = False
    if parsed_date_from is not None or parsed_date_to is not None:
        floor_date = _archive_floor_date(now)
        archive_floor_str = floor_date.isoformat()
        out_of_range = parsed_date_from is not None and parsed_date_from.date() < floor_date
        # Bypass the default 7-day archive cap for THIS date-range request only — a bounded,
        # day-partitioned read of exactly the requested range (never the whole 30-day corpus
        # unless asked), merged in alongside any VM already present from the cached census
        # (never re-added, never double-counted). Never runs in mock mode / cloud=aws-only.
        if not _cfg.is_mock_mode() and (cloud is None or cloud.upper() == DeploymentCloud.GCP.value):
            range_entries, _ = _load_registry_entries_for_date_range(now, parsed_date_from, parsed_date_to)
            existing_vm_names = {item.name for item in items if item.kind == DeploymentKind.VM.value}
            items = items + [_vm_item(entry, now) for entry in range_entries if entry.vm_name not in existing_vm_names]

    items = _apply_date_range(items, now, parsed_date_from, parsed_date_to)
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
        archive_floor=archive_floor_str,
        date_range_out_of_range=out_of_range,
    )


class DeploymentRegionsResponse(BaseModel):  # CORRECT-LOCAL: FastAPI API contract model
    """Region options for the cockpit's region selector (WS-D region reconciliation).

    ``default`` is the region the selector opens on; ``regions`` is the selectable list (default first);
    ``all_value`` is the sentinel a caller passes as ``?region=`` to sweep every region.
    """

    default: str
    regions: list[str]
    all_value: str


@router.get("/deployments/regions", response_model=DeploymentRegionsResponse)
def get_deployment_regions() -> DeploymentRegionsResponse:
    """Dynamic region options for the region selector: the default region first, then every GCP compute
    region (so a region shows up the moment infra lands there), plus the ``all`` sweep sentinel.

    A cheap region-list metadata read; degrades to the configured default set if the read fails, so the
    selector always has at least its default entry.
    """
    if _cfg.is_mock_mode():
        names: list[str] = ["asia-northeast1", "europe-west1", "europe-west3", "us-central1", "us-east1"]
    else:
        listed = list_gcp_region_names(_cfg.require_gcp_project_id())
        names = list(listed) if listed else list(_CONFIGURED_GCP_REGIONS)
    # Default region pinned first, the remainder alphabetical + de-duplicated.
    ordered = [_DEFAULT_GCP_REGION, *sorted(n for n in dict.fromkeys(names) if n and n != _DEFAULT_GCP_REGION)]
    return DeploymentRegionsResponse(default=_DEFAULT_GCP_REGION, regions=ordered, all_value="all")


def _job_run_history(item: DeploymentItem) -> list[dict[str, str | float | None]]:
    """Last-N Cloud Run job executions for the detail popover (#11).

    Empty for any non-GCP-Cloud-Run-job kind, mock mode, or on any error (the popover simply shows
    no history). Fetched on the detail path only — ``list_job_executions`` uses ``page_size=10``; the
    thin-list census stays at 1, so this adds no cost to the list endpoint.
    """
    if item.kind != DeploymentKind.CLOUD_RUN_JOB.value or item.cloud != DeploymentCloud.GCP.value:
        return []
    if _cfg.is_mock_mode():
        return []
    try:
        project_id = _cfg.require_gcp_project_id()
    except (HTTPException, ValueError, RuntimeError):
        return []
    records = list_job_executions(project_id, item.name, region=item.region or DEFAULT_CLOUD_RUN_REGION)
    return [
        {
            "name": record.name,
            "status": record.status,
            "started_at": record.started_at,
            "completed_at": record.completed_at,
            "duration_seconds": record.duration_seconds,
        }
        for record in records
    ]


def _job_object_delta(item: DeploymentItem, now: datetime) -> int | None:
    """Rows since the last manifest snapshot for a Cloud Run job's asset_group — the #12 "rows since
    last run" HINT.

    A link + hint only: the AUTHORITATIVE "did the run produce its data" verdict lives on the
    consolidator page (``consolidator_throughput_backlog_monitor`` plan), keyed by the full job
    short-name; this deployments popover only helps spot a fired-but-produced-nothing job. Reuses the
    same ``object_delta_for_asset_group`` the census batches (which catches its own errors → None);
    None for non-jobs / no asset_group / mock.
    """
    if item.kind != DeploymentKind.CLOUD_RUN_JOB.value or not item.asset_group:
        return None
    if _cfg.is_mock_mode():
        return None
    return object_delta_for_asset_group(item.asset_group, now)[0]


@router.get("/deployments/{name}/detail", response_model=DeploymentDetailResponse)
def get_deployment_detail(name: str) -> DeploymentDetailResponse:
    """Per-target drill-down: the thin-list item plus the D.1 metrics vector (popover).

    ``name`` is the ``DeploymentItem.name`` (VM name / Cloud Run job or service name), not
    an orchestration ``deployment_id`` — this endpoint reads the SAME cached census
    ``/deployments/inventory`` already computes (no new bucket walk). A Cloud Run job additionally
    gets its last-N run-history (``page_size=10`` on the detail path only). 404 if the name isn't in
    the current (cached) inventory.
    """
    now = datetime.now(UTC)
    items = _load_inventory(now)
    item = next((i for i in items if i.name == name), None)
    if item is None:
        raise HTTPException(status_code=404, detail=f"Deployment {name!r} not found in the current inventory")
    run_history = _job_run_history(item)
    object_delta = _job_object_delta(item, now)
    with _vm_entry_by_name_lock:
        entry = _vm_entry_by_name_cache.get(name)
    if entry is None:
        return DeploymentDetailResponse(item=item, run_history=run_history, object_delta=object_delta)
    return DeploymentDetailResponse(
        item=item,
        cpu_pct=entry.cpu_pct,
        mem_pct=entry.mem_pct,
        mem_slope=entry.mem_slope,
        disk_pct=entry.disk_pct,
        io_write_rate_bytes_sec=entry.io_write_rate_bytes_sec,
        net_recv_rate_bytes_sec=entry.net_recv_rate_bytes_sec,
        workload_alive=entry.workload_alive,
        host_metrics_window=entry.host_metrics_window,
        run_history=run_history,
        object_delta=object_delta,
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
    "SERVICE_STATUS_DEAD",
    "SERVICE_STATUS_DEGRADED",
    "SERVICE_STATUS_SCALED_TO_ZERO",
    "SERVICE_STATUS_SERVING",
    "DeploymentDetailResponse",
    "DeploymentInventoryResponse",
    "DeploymentItem",
    "UmbrellaSummaryResponse",
    "active_registry_vm_names",
    "build_inventory",
    "build_umbrella_summary",
    "classify_vm_target",
    "cloud_run_service_health_status",
    "ecs_service_health_status",
    "is_control_plane_vm",
    "router",
]

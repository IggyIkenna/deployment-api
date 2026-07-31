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

Split 2026-07-31 into a facade package (mirrors the 2026-06-11 ``routes/deployments``
precedent — pure code motion; ``deployment_api_qg_size_gate_debt_2026_07_30.md``).
Submodules attach their endpoints to the shared ``router`` below at import time.
Patched module-level collaborators (``_cfg`` / ``get_storage_client`` / etc. — the
census "seams" the test suite mocks per ``tests/mocks.py``'s
``patch_inventory_secondary_census`` docstring: "the seams resolve as module globals,
so patching them on ``mod`` also covers the closures that call them") are resolved
through this facade module (``_inv``) AT CALL TIME by every submodule, so the existing
test patch surface ``deployment_api.routes.deployments_inventory.<name>`` keeps
intercepting regardless of which submodule now defines/calls the seam.
"""

from __future__ import annotations

import logging
import threading
from concurrent.futures import Future, ThreadPoolExecutor
from concurrent.futures import TimeoutError as FutureTimeoutError

from fastapi import APIRouter
from pydantic import BaseModel, Field
from unified_api_contracts import LifecycleClass, VmPrefixSpec
from unified_trading_library import (
    DeploymentRegistryEntry,
    generate_download_url,
    get_storage_client,
    resolve_bucket_name,
    upload_to_storage,
)

from deployment_api.deployment_api_config import DeploymentApiConfig
from deployment_api.registry_reader import resolve_active_registry
from deployment_api.routes._aws_deployments import load_aws_inventory
from deployment_api.routes._cloud_run_executions import (
    latest_execution_by_job,
    list_job_executions,
)
from deployment_api.routes._cloud_run_services import list_cloud_run_services
from deployment_api.routes._cloud_scheduler import list_scheduler_jobs
from deployment_api.routes._gcp_cloud_functions import list_cloud_functions
from deployment_api.routes._leaked_resources import UnreleasedResource
from deployment_api.routes._run_log_resolution import resolve_run_log_location
from deployment_api.routes._run_log_tail import read_run_log_tail
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


# Registry-read parallelism + a short-TTL inventory cache — see ``_registry_io.py``/
# ``_aggregation.py`` docstrings for the full rationale (kept there, next to their consumers).
_inventory_cache: dict[str, tuple[float, list[DeploymentItem]]] = {}
_inventory_lock = threading.Lock()
# cache keys with an in-flight background refresh (so we kick off exactly one).
_inventory_refreshing: set[str] = set()
# max_workers=1 (HARD RULE — see deployment_api_inventory_cold_path_concurrent_oom_2026_07_24.md):
# this pool is the ONLY thing standing between "one caller waits too long" (the bug the
# stale-while-revalidate design fixed) and "two DIFFERENT cache keys' full census fan-outs run
# truly concurrently and OOM the container" (the regression that design introduced — each
# _compute_inventory call internally fans out via _census_pool max_workers=10 plus several
# per-provider region pools up to max_workers=8 each; 2 concurrent full computations measured
# 17,002 MiB against a 16,384 MiB limit and got SIGKILL'd). A single worker restores the OLD
# code's process-wide serialization (only one cold census in flight at a time) while keeping the
# NEW per-caller 45s bound from _load_inventory/_kick_background_refresh — the two properties are
# orthogonal and compose. Do NOT raise this back to >1 without also bounding total concurrent
# fan-out some other way (see the issue doc's other candidate approaches).
_inventory_refresh_pool = ThreadPoolExecutor(max_workers=1, thread_name_prefix="inv-refresh")

# VM registry entry by vm_name — piggybacks on the SAME GCP VM census _compute_inventory
# already runs every cache cycle (zero new bucket walks); lets the ``/{id}/detail`` drill-down
# serve the D.1 metrics vector (cpu/mem/disk/io/net + workload liveness) that live on
# ``DeploymentRegistryEntry`` but aren't carried onto the thin-list ``DeploymentItem``.
# Entries are indexed by ``vm_name`` (registry objects are keyed by ``deployment_id``, which
# a caller of ``/{id}/detail`` doesn't know), so this is a small derived side-cache, not a
# second source of truth.
_vm_entry_by_name_cache: dict[str, DeploymentRegistryEntry] = {}
_vm_entry_by_name_lock = threading.Lock()

# D.3 composite states this todo alerts on. "stalled" fires only for the BATCH umbrella
# today (LIVE/PAPER degrade to "unknown" — see _composite_health_status's docstring).
# "hung" (a running VM whose GCE status is still RUNNING but its registry heartbeat has
# exceeded _STALE_HEARTBEAT_MINUTES=15, computed in _vm_health.py) was added here 2026-07-27
# per migration_vm_hung_detection_monitoring_gap_2026_07_27.md todo 1, closing Gap 1 (the
# state was already computed correctly but structurally excluded from paging). A same-session
# false-positive-risk investigation traced the heartbeat WRITE path (not the workload's data
# -progress cadence, a distinct and legitimately variable signal already handled per-VM-class
# by heartbeat_stall_watcher.py's PREFIX_IDLE_THRESHOLDS / _is_backfill_vm gate): every VM_TASK
# launched via setup-data-pipeline-vm.sh installs the same 60s-interval HeartbeatDaemon
# (deployment_service/vm/heartbeat_cli.py) unconditionally, live/backfill/canonical-migration
# alike — 15 minutes is a uniform ~15x margin over that fixed 60s tick for every VM class, so
# no per-VM-class override is needed for THIS signal (unlike the other two watchers' overrides,
# which cover different, workload-paced signals this one does not use).
_ALERT_HEALTH_STATES = frozenset({"oom-risk", "stalled", "hung"})

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
    # last-run). Also carries CLOUD_RUN_SERVICE's update_time/create_time (WS-2 decision 2 — the
    # always-on proxy timestamp; closes the audit-found asymmetry vs its ECS_SERVICE twin, which
    # instead reports this via last_run_at). None for kinds that report a real last_run_at.
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
    # "complete" | "partial" | None (no billing row yet). "partial" means `cost_actual_usd` /
    # `cost_projected_24h_usd` fall back to a still-accruing day (no complete billing day exists yet)
    # — the UI colour-codes off this, no text label (decision 4, 2026-07-20).
    cost_basis: str | None = None
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
    # Orphan/idle-spend join (Fleet-tab consolidation, 2026-07-21) — reused verbatim from the
    # `/api/fleet/orphans` SSOT (`_fleet_inventory.build_orphan_inventory`), never re-derived, so the
    # verdict/cost estimate never drifts between the two surfaces. Populated ONLY for VM rows
    # currently in a STOPPED/SUSPENDED/TERMINATED state (the orphan candidate set); None for a
    # running VM or any non-VM kind — honest absence, never a fabricated "not an orphan" default.
    # OrphanVerdict: reap|keep_within_grace|keep_not_ephemeral|keep_retained|keep_no_timestamp
    reap_verdict: str | None = None
    grace_hours: float | None = None  # stopped-age threshold (hours) the verdict above was computed against
    stopped_age_hours: float | None = None  # hours since last_stop_timestamp (falls back to creation)
    monthly_disk_usd: float | None = None  # ESTIMATE — boot-disk idle cost, same rate model as the orphans endpoint
    # Inline Resources column (deployment-ui Deployments.tsx's ResourceCell) — the single
    # most-recent D.1 host-metrics sample, same fields as DeploymentDetailResponse's flat
    # metrics (mirrored from `entry.cpu_pct`/etc. in `_vm_item()`, not re-derived). Deliberately
    # NOT `host_metrics_window` — that stays detail-only by design (see
    # DeploymentDetailResponse's docstring: keeps the ~200-target list payload small). None for a
    # kind without D.1 capture or a VM absent from this cycle's census — honest absence, never a
    # fabricated 0.0.
    cpu_pct: float | None = None
    mem_pct: float | None = None
    mem_slope: float | None = None
    disk_pct: float | None = None
    # Artifact-pipeline cross-link (Phase 3b) — the image/tarball this VM booted, straight off its
    # registry entry (`DeploymentRegistryEntry.image_digest`/`git_commit`), never re-derived. "" on
    # the entry becomes None here (honest absence, e.g. a pre-BoM row or a tarball VM launched
    # before the Phase 3c commit stamp) rather than a fabricated empty string. None for a kind with
    # no registry entry (Cloud Run jobs/services, unmanaged VMs).
    image_digest: str | None = None
    git_commit: str | None = None


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
# Route registration — package facade (split 2026-07-31 from the 2592-line
# ``routes/deployments_inventory.py`` per ``deployment_api_qg_size_gate_debt_2026_07_30.md``
# — pure code motion). Submodules attach their endpoints to the shared ``router`` above
# at import time; import order replays the original inline definition order.
# ---------------------------------------------------------------------------
# isort: off
from deployment_api.routes.deployments_inventory import _classification
from deployment_api.routes.deployments_inventory import _registry_io
from deployment_api.routes.deployments_inventory import _mock_data
from deployment_api.routes.deployments_inventory import _aggregation
from deployment_api.routes.deployments_inventory import _routes

# isort: on

# ---------------------------------------------------------------------------
# Public-surface re-exports — every symbol previously importable from
# ``deployment_api.routes.deployments_inventory`` resolves here unchanged
# (incl. the test-patch "seam" surface — see the module docstring above).
# ---------------------------------------------------------------------------
from deployment_api.routes.deployments_inventory._aggregation import (
    _attach_costs,  # pyright: ignore[reportPrivateUsage]
    _aws_instance_id_from_resource_id,  # pyright: ignore[reportPrivateUsage]
    _aws_regions_for_scope,  # pyright: ignore[reportPrivateUsage]
    _batched_object_deltas,  # pyright: ignore[reportPrivateUsage]
    _gcp_regions_for_scope,  # pyright: ignore[reportPrivateUsage]
    _kick_background_refresh,  # pyright: ignore[reportPrivateUsage]
    _load_inventory,  # pyright: ignore[reportPrivateUsage]
    _multi_region_functions,  # pyright: ignore[reportPrivateUsage]
    _multi_region_jobs,  # pyright: ignore[reportPrivateUsage]
    _multi_region_scheduler,  # pyright: ignore[reportPrivateUsage]
    _multi_region_services,  # pyright: ignore[reportPrivateUsage]
    _orphaned_resource_items,  # pyright: ignore[reportPrivateUsage]
    _refresh_inventory,  # pyright: ignore[reportPrivateUsage]
    _scheduler_item,  # pyright: ignore[reportPrivateUsage]
    _store_inventory,  # pyright: ignore[reportPrivateUsage]
    build_inventory,
)
from deployment_api.routes.deployments_inventory._classification import (
    _GCE_STATUS_TO_WIRE,  # pyright: ignore[reportPrivateUsage]
    SERVICE_STATUS_DEAD,
    SERVICE_STATUS_DEGRADED,
    SERVICE_STATUS_SCALED_TO_ZERO,
    SERVICE_STATUS_SERVING,
    _alert_on_health_transition,  # pyright: ignore[reportPrivateUsage]
    _classify_live_cloud_run_job,  # pyright: ignore[reportPrivateUsage]
    _classify_vm,  # pyright: ignore[reportPrivateUsage]
    _cloud_function_item,  # pyright: ignore[reportPrivateUsage]
    _cloud_run_item,  # pyright: ignore[reportPrivateUsage]
    _cloud_run_item_for_live_job,  # pyright: ignore[reportPrivateUsage]
    _cloud_run_service_item,  # pyright: ignore[reportPrivateUsage]
    _cloud_run_status_for,  # pyright: ignore[reportPrivateUsage]
    _composite_health_status,  # pyright: ignore[reportPrivateUsage]
    _heartbeat_age_seconds,  # pyright: ignore[reportPrivateUsage]
    _match_registered_job,  # pyright: ignore[reportPrivateUsage]
    _parse_iso,  # pyright: ignore[reportPrivateUsage]
    _persist_alert,  # pyright: ignore[reportPrivateUsage]
    _prune_stale_alert_state,  # pyright: ignore[reportPrivateUsage]
    _unmanaged_vm_item,  # pyright: ignore[reportPrivateUsage]
    _uptime_hours,  # pyright: ignore[reportPrivateUsage]
    _vm_item,  # pyright: ignore[reportPrivateUsage]
    _vm_lifecycle_class,  # pyright: ignore[reportPrivateUsage]
    active_registry_vm_names,
    classify_vm_target,
    cloud_run_service_health_status,
    ecs_service_health_status,
)
from deployment_api.routes.deployments_inventory._mock_data import (
    _mock_inventory,  # pyright: ignore[reportPrivateUsage]
)
from deployment_api.routes.deployments_inventory._registry_io import (
    _ARCHIVE_RETENTION_DAYS,  # pyright: ignore[reportPrivateUsage]
    _archive_floor_date,  # pyright: ignore[reportPrivateUsage]
    _download_entries_parallel,  # pyright: ignore[reportPrivateUsage]
    _list_json_keys,  # pyright: ignore[reportPrivateUsage]
    _load_aws_items,  # pyright: ignore[reportPrivateUsage]
    _load_registry_entries,  # pyright: ignore[reportPrivateUsage]
    _load_registry_entries_for_date_range,  # pyright: ignore[reportPrivateUsage]
    _read_entry,  # pyright: ignore[reportPrivateUsage]
)
from deployment_api.routes.deployments_inventory._routes import (
    DeploymentRegionsResponse,
    RunLogDownloadResponse,
    RunLogMetadataResponse,
    RunLogTailResponse,
    _apply_date_range,  # pyright: ignore[reportPrivateUsage]
    _counts_by_kind,  # pyright: ignore[reportPrivateUsage]
    _filter_items,  # pyright: ignore[reportPrivateUsage]
    _job_object_delta,  # pyright: ignore[reportPrivateUsage]
    _job_run_history,  # pyright: ignore[reportPrivateUsage]
    _normalize_region_scope,  # pyright: ignore[reportPrivateUsage]
    _parse_date_query,  # pyright: ignore[reportPrivateUsage]
    _single_timestamp_overlaps,  # pyright: ignore[reportPrivateUsage]
    _vm_overlap_basis,  # pyright: ignore[reportPrivateUsage]
    build_umbrella_summary,
    get_deployment_detail,
    get_deployment_inventory,
    get_deployment_regions,
    get_run_log_download,
    get_run_log_metadata,
    get_run_log_tail,
    get_umbrella_summary,
)

__all__ = [
    "SERVICE_STATUS_DEAD",
    "SERVICE_STATUS_DEGRADED",
    "SERVICE_STATUS_SCALED_TO_ZERO",
    "SERVICE_STATUS_SERVING",
    "CostObservabilityService",
    "DeploymentDetailResponse",
    "DeploymentInventoryResponse",
    "DeploymentItem",
    "DeploymentRegionsResponse",
    "DeploymentRegistryEntry",
    "RunLogDownloadResponse",
    "RunLogMetadataResponse",
    "RunLogTailResponse",
    "UmbrellaSummaryResponse",
    "active_registry_vm_names",
    "build_inventory",
    "build_umbrella_summary",
    "classify_vm_target",
    "cloud_run_service_health_status",
    "ecs_service_health_status",
    "generate_download_url",
    "get_disk_details",
    "get_storage_client",
    "get_vm_instance_details",
    "is_control_plane_vm",
    "latest_execution_by_job",
    "list_cloud_functions",
    "list_cloud_run_services",
    "list_gcp_region_names",
    "list_job_executions",
    "list_reserved_addresses",
    "list_scheduler_jobs",
    "list_unattached_disk_names",
    "load_aws_inventory",
    "object_delta_for_asset_group",
    "read_run_log_tail",
    "resolve_active_registry",
    "resolve_bucket_name",
    "resolve_run_log_location",
    "router",
    "upload_to_storage",
]

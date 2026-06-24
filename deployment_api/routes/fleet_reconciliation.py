# Epic: observability_master
# Lifecycle: permanent
"""GET /api/fleet/reconciliation — cross-cloud "every RUNNING instance is accounted for".

The per-cloud watchdogs run independently; nothing asserts that every RUNNING GCP+AWS
instance maps to a KNOWN/registered/expected deployment. This endpoint closes that gap:

* **UNKNOWN** — RUNNING but unregistered: a live GCE VM with no active deployment-registry
  entry, not a registered Cloud Run job, and not a recognised long-lived control-plane VM.
  An unknown instance is its own alert class (classify it or kill it).
* **EXPECTED-MISSING** — registered/active in the deployment registry but NOT currently
  running (the registry thinks it is up; the cloud says it is gone).

Reuse-first: the RUNNING set is the GCE aggregated-list (`get_vm_instance_details`); the
REGISTERED set is the parallel active-registry read (`active_registry_vm_names`) plus
`CLOUD_RUN_JOBS`; each row is classified via the single `classify_vm_target` resolver. AWS
rides the same `DeploymentItem` contract (`load_aws_inventory`) and degrades to empty
without creds — never blocks the GCP reconciliation.

SSOT: ``plans/active/unified_deployment_health_cockpit_2026_06_23.md`` Phase 4.
"""

from __future__ import annotations

import logging
import threading
import time
from concurrent.futures import ThreadPoolExecutor
from datetime import UTC, datetime

from deployment_service.cloud_run_job_registry import CLOUD_RUN_JOBS
from deployment_service.deployment_classification import UnclassifiedDeploymentError
from fastapi import APIRouter
from pydantic import BaseModel, Field

from deployment_api.deployment_api_config import DeploymentApiConfig
from deployment_api.routes._aws_deployments import load_aws_inventory
from deployment_api.routes.deployments_inventory import (
    active_registry_vm_names,
    classify_vm_target,
)
from deployment_api.vm_utils import get_vm_instance_details

router = APIRouter()
logger = logging.getLogger(__name__)

_cfg = DeploymentApiConfig()

# Long-lived control-plane / live-infra prefixes that do NOT write a deployment-registry
# entry (they are managed out-of-band) — a running VM matching one of these is accounted-for
# even without a registry blob. Mirrors the LONG_LIVED_LIVE prefixes the inventory curates.
_CONTROL_PLANE_PREFIXES = (
    "planning",
    "human-planning",
    "agent-orchestrator",
    "strategy-live-",
    "defi-recursive-",
)

# A noisy registry (un-reaped stale active entries) can yield thousands of EXPECTED-MISSING —
# cap the returned ROWS for a responsive payload while the COUNTS stay exact (the true totals).
_MAX_ROWS = 200

# Stale-while-revalidate short-TTL cache (mirrors routes/deployments_inventory.py). The
# reconciliation reads the full active registry (~2.4k entries) → ~13s cold; the cockpit's
# Fleet tab polls repeatedly, so cache the assembled response so repeat visits are <0.2s.
# Single cache slot — the endpoint takes no params (one cross-cloud reconciliation).
_RECON_TTL_SEC = 45.0
# (annotation is lazy under `from __future__ import annotations`, so the forward ref to the
# below-defined FleetReconciliationResponse needs no quotes.)
_recon_cache: tuple[float, FleetReconciliationResponse] | None = None
_recon_lock = threading.Lock()
_recon_refreshing = False
_recon_refresh_pool = ThreadPoolExecutor(max_workers=1, thread_name_prefix="recon-refresh")


class ReconciliationRow(BaseModel):  # CORRECT-LOCAL: FastAPI API contract model
    """One reconciled instance (UNKNOWN or EXPECTED-MISSING), classified for display."""

    name: str
    umbrella: str
    cloud: str
    service: str
    asset_group: str


class CloudReconciliation(BaseModel):  # CORRECT-LOCAL: FastAPI API contract model
    """Per-cloud reconciliation counts + the two alarm row-sets."""

    cloud: str
    running: int
    registered: int
    unknown_count: int = 0
    expected_missing_count: int = 0
    unknown: list[ReconciliationRow] = Field(default_factory=list)  # type: ignore[reportUnknownVariableType]  # capped to _MAX_ROWS
    expected_missing: list[ReconciliationRow] = Field(default_factory=list)  # type: ignore[reportUnknownVariableType]  # capped to _MAX_ROWS


class FleetReconciliationResponse(BaseModel):  # CORRECT-LOCAL: FastAPI API contract model
    """Cross-cloud reconciliation — every running instance accounted for, or flagged."""

    generated_at: str
    overall: str  # ok | degraded | critical
    clouds: list[CloudReconciliation] = Field(default_factory=list)  # type: ignore[reportUnknownVariableType]
    unknown_total: int
    expected_missing_total: int


def _row(name: str, cloud: str) -> ReconciliationRow:
    """Classify a name into a display row (degrades to UNKNOWN umbrella on a classify failure)."""
    try:
        target = classify_vm_target(name)
        return ReconciliationRow(
            name=name,
            umbrella=target.umbrella.value,
            cloud=cloud,
            service=target.service,
            asset_group=target.asset_group,
        )
    except UnclassifiedDeploymentError:
        return ReconciliationRow(name=name, umbrella="UNKNOWN", cloud=cloud, service="", asset_group="")


def _is_control_plane(name: str) -> bool:
    return any(name.startswith(p) for p in _CONTROL_PLANE_PREFIXES)


def _gcp_reconciliation() -> CloudReconciliation:
    """Reconcile the live GCP estate: running GCE VMs vs the registered + control-plane set."""
    project_id = _cfg.require_gcp_project_id()
    details = get_vm_instance_details(project_id)
    running = {name for name, d in details.items() if str(d.get("status")) == "RUNNING"}
    registered = active_registry_vm_names()
    cloud_run = {t.name for t in CLOUD_RUN_JOBS}
    accounted = registered | cloud_run | {n for n in running if _is_control_plane(n)}

    unknown = sorted(running - accounted)
    expected_missing = sorted(registered - running)
    return CloudReconciliation(
        cloud="GCP",
        running=len(running),
        registered=len(registered),
        unknown_count=len(unknown),
        expected_missing_count=len(expected_missing),
        unknown=[_row(n, "GCP") for n in unknown[:_MAX_ROWS]],
        expected_missing=[_row(n, "GCP") for n in expected_missing[:_MAX_ROWS]],
    )


def _aws_reconciliation() -> CloudReconciliation:
    """Reconcile the live AWS estate (degrades to empty without creds — never blocks GCP).

    The AWS census yields the RUNNING EC2/Batch set; without an AWS deployment registry the
    reconciliation reports running counts only (unknown/expected-missing ride the same
    contract once an AWS registry is wired). Honest-empty on any AWS error.
    """
    region = _cfg.aws_codebuild_region or "ap-northeast-1"
    try:
        items = load_aws_inventory(
            region=region,
            aws_account_id=_cfg.aws_account_id or "",
            lifecycle_for_name=lambda _name: "EPHEMERAL_BATCH",
        )
    except (OSError, ValueError, RuntimeError) as exc:
        logger.warning("reconciliation: AWS census degraded: %s", exc)
        items = []
    running = sum(1 for i in items if str(i.get("status")) == "running")
    return CloudReconciliation(cloud="AWS", running=running, registered=0, unknown_count=0, expected_missing_count=0)


def _mock_response(now: datetime) -> FleetReconciliationResponse:
    """Representative mock reconciliation (mock mode — no cloud access)."""
    unknown = [
        ReconciliationRow(
            name="rogue-vm-20260624", umbrella="BATCH", cloud="GCP", service="rogue-vm", asset_group="cefi"
        )
    ]
    missing = [
        ReconciliationRow(
            name="cefi-binance-spot-20260620", umbrella="BATCH", cloud="GCP", service="binance-spot", asset_group="cefi"
        )
    ]
    return FleetReconciliationResponse(
        generated_at=now.isoformat(),
        overall="degraded",
        clouds=[
            CloudReconciliation(
                cloud="GCP",
                running=42,
                registered=40,
                unknown_count=1,
                expected_missing_count=1,
                unknown=unknown,
                expected_missing=missing,
            ),
            CloudReconciliation(cloud="AWS", running=1, registered=0, unknown_count=0, expected_missing_count=0),
        ],
        unknown_total=1,
        expected_missing_total=1,
    )


def build_response(clouds: list[CloudReconciliation], now: datetime) -> FleetReconciliationResponse:
    """Assemble the cross-cloud response + the overall verdict (unknown is the worst signal)."""
    unknown_total = sum(c.unknown_count for c in clouds)
    missing_total = sum(c.expected_missing_count for c in clouds)
    overall = "critical" if unknown_total else "degraded" if missing_total else "ok"
    return FleetReconciliationResponse(
        generated_at=now.isoformat(),
        overall=overall,
        clouds=clouds,
        unknown_total=unknown_total,
        expected_missing_total=missing_total,
    )


def _compute_reconciliation() -> FleetReconciliationResponse:
    """Assemble the live cross-cloud reconciliation (the heavy ~13s registry+census read)."""
    return build_response([_gcp_reconciliation(), _aws_reconciliation()], datetime.now(UTC))


def _refresh_reconciliation() -> None:
    """Background cache refresh — recompute + store, then clear the in-flight flag."""
    global _recon_cache, _recon_refreshing
    try:
        result = _compute_reconciliation()
        with _recon_lock:
            _recon_cache = (time.monotonic(), result)
    except (OSError, ValueError, RuntimeError) as exc:
        # Keep the stale snapshot on a failed refresh — never poison the cache.
        logger.warning("reconciliation: background refresh failed: %s", exc)
    finally:
        with _recon_lock:
            _recon_refreshing = False


def _kick_background_refresh() -> None:
    """Schedule exactly one background reconciliation refresh (stale-while-revalidate)."""
    global _recon_refreshing
    with _recon_lock:
        if _recon_refreshing:
            return
        _recon_refreshing = True
    _recon_refresh_pool.submit(_refresh_reconciliation)


def _load_reconciliation() -> FleetReconciliationResponse:
    """Load the reconciliation, stale-while-revalidate cached (mirrors the inventory cache).

    * **Fresh** (< TTL) → served instantly.
    * **Stale** (> TTL) → the stale snapshot is served instantly AND one background refresh
      is kicked off, so the Fleet tab never waits on the slow registry read after the first
      load (the cockpit polls repeatedly → always warm).
    * **Cold** (no snapshot) → computed synchronously under the lock so a burst of first-polls
      collapses to ONE read, not N.
    """
    global _recon_cache
    cached = _recon_cache
    if cached is not None:
        if (time.monotonic() - cached[0]) >= _RECON_TTL_SEC:
            _kick_background_refresh()
        return cached[1]

    # Cold path — lock so concurrent first-polls trigger exactly ONE reconciliation.
    with _recon_lock:
        if _recon_cache is not None:
            return _recon_cache[1]
        result = _compute_reconciliation()
        _recon_cache = (time.monotonic(), result)
        return result


@router.get("/fleet/reconciliation", response_model=FleetReconciliationResponse)
def get_fleet_reconciliation() -> FleetReconciliationResponse:
    """Cross-cloud reconciliation: every RUNNING GCP+AWS instance accounted for, or flagged.

    UNKNOWN (running but unregistered) + EXPECTED-MISSING (registered but not running) per
    cloud. Read-only; AWS degrades to empty without creds — the GCP reconciliation always runs.
    Stale-while-revalidate cached (45s TTL) so the Fleet tab's repeat visits are <0.2s — the
    cold read is ~13s (full active registry), the same perf class the inventory endpoint fixed.
    """
    if _cfg.is_mock_mode():
        return _mock_response(datetime.now(UTC))
    return _load_reconciliation()


__all__ = [
    "CloudReconciliation",
    "FleetReconciliationResponse",
    "ReconciliationRow",
    "build_response",
    "get_fleet_reconciliation",
    "router",
]

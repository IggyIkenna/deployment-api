# Epic: monitoring_control_plane_master_2026_06_10
# Lifecycle: permanent
"""Pydantic response models for the deployment-ui FleetInfra tile.

Two read-only monitoring endpoints (plan: deployment_ui_monitoring_pane_2026_06_19.md):

* ``GET /api/fleet/vm-census``       → :class:`VmCensusResponse`
  The running / expected / zombie / OOM / stopped surface, derived from the live GCE
  instance inventory (``vm_utils.get_vm_instance_details``) + lifecycle classification.
* ``GET /api/fleet/infra-vm-health`` → :class:`InfraVmHealthResponse`
  A server-side proxy of the agent-orchestrator ``/api/fleet/summary`` so the
  deployment-ui single pane never makes a browser → orchestrator cross-origin hop.

These shapes match deployment-ui ``src/api/client.ts`` (VmCensusResponse / InfraVmHealthResponse
+ their nested types) field-for-field — the frontend + its playwright test are already shipped,
so the contract is fixed.
"""

from __future__ import annotations

from typing import Literal

from pydantic import BaseModel, Field

# Closed-set literals mirrored from the deployment-ui client.ts unions + the UAC
# LifecycleClass StrEnum. Kept as Literals (not the UAC enum) because these are the
# JSON wire contract the frontend asserts against, not a domain object.
VmLifecycleClass = Literal[
    "EPHEMERAL_BATCH",
    "EPHEMERAL_EXPERIMENT",
    "SCHEDULED_RECURRING",
    "LONG_LIVED_LIVE",
]
VmRunStatus = Literal["RUNNING", "STOPPING", "STOPPED", "TERMINATED"]
# Named aliases so the proxy can ``cast`` a runtime-constrained str to the wire literal
# (no ``# type: ignore`` — banned workspace-wide).
AlertSeverity = Literal["info", "warn", "crit"]
VmRole = Literal["epic", "planning", "unknown"]


class VmCensusEntry(BaseModel):  # CORRECT-LOCAL: deployment-ui response DTO, not a domain contract
    """One VM in the fleet census."""

    name: str
    prefix: str
    lifecycle_class: VmLifecycleClass
    status: VmRunStatus
    zombie: bool
    oom: bool
    age_min: int | None
    zone: str


class VmCensusResponse(BaseModel):  # CORRECT-LOCAL: deployment-ui response DTO
    """GET /api/fleet/vm-census response — the fleet roll-up + per-VM entries."""

    generated_at: str  # ISO-8601 UTC
    running: int
    expected: int
    zombie: int
    oom: int
    stopped: int
    vms: list[VmCensusEntry] = Field(default_factory=list)


# Reap-verdict wire literal — mirrors deployment-service
# vm_zombie_watchdog.ReapVerdict.verdict so the UI can render a reason per row.
OrphanVerdict = Literal[
    "reap",
    "keep_within_grace",
    "keep_not_ephemeral",
    "keep_retained",
    "keep_no_timestamp",
]


class OrphanEntry(BaseModel):  # CORRECT-LOCAL: deployment-ui response DTO
    """One STOPPED/TERMINATED VM with the data an operator needs to act on it."""

    name: str
    zone: str
    status: VmRunStatus
    lifecycle_class: VmLifecycleClass
    stopped_age_hours: float | None
    boot_disk_gb: int | None
    boot_disk_type: str | None
    monthly_disk_usd: float  # ESTIMATE — asia-northeast1 list rate x GB
    # ESTIMATE — monthly_disk_usd prorated by stopped_age_hours (list-rate x actual idle time so
    # far, not a real billing query). 0.0 when stopped_age_hours is unknown.
    cost_incurred_usd: float
    reapable: bool
    verdict: OrphanVerdict


class OrphanInventoryResponse(BaseModel):  # CORRECT-LOCAL: deployment-ui response DTO
    """GET /api/fleet/orphans response — stopped-VM inventory + idle-spend rollup."""

    generated_at: str  # ISO-8601 UTC
    grace_hours: float
    stopped_total: int
    reapable_total: int
    monthly_idle_usd: float  # all stopped boot disks, CURRENT RATE (ESTIMATE)
    monthly_reapable_usd: float  # only the reapable subset, CURRENT RATE (ESTIMATE)
    total_idle_cost_incurred_usd: float  # all stopped boot disks, ACCRUED SO FAR (ESTIMATE)
    total_reapable_cost_incurred_usd: float  # only the reapable subset, ACCRUED SO FAR (ESTIMATE)
    orphans: list[OrphanEntry] = Field(default_factory=list)


class ReapRequest(BaseModel):  # CORRECT-LOCAL: deployment-ui request DTO
    """POST /api/fleet/reap body — dry-run by default; mirrors the watchdog reaper gates."""

    dry_run: bool = True
    grace_hours: float = 24.0


class ReapResultEntry(BaseModel):  # CORRECT-LOCAL: deployment-ui response DTO
    """Per-VM outcome of a reap pass."""

    name: str
    zone: str
    monthly_disk_usd: float
    deleted: bool


class ReapResponse(BaseModel):  # CORRECT-LOCAL: deployment-ui response DTO
    """POST /api/fleet/reap response — candidates (dry-run) or deletion outcomes."""

    dry_run: bool
    grace_hours: float
    candidate_total: int
    reaped_total: int
    monthly_reclaimed_usd: float  # ESTIMATE — disk $/mo of actually-reaped VMs
    results: list[ReapResultEntry] = Field(default_factory=list)


class WatchdogKillEventRequest(BaseModel):  # CORRECT-LOCAL: deployment-ui request DTO
    """POST /api/fleet/watchdog/kill-events body — one resource-watchdog kill or violation event."""

    vm_name: str
    pid: int
    slot_id: str
    command: str
    reason: str
    rss_mb: int
    limit_mb: int
    pressure_level: str
    killed: bool


class WatchdogKillEventResponse(BaseModel):  # CORRECT-LOCAL: deployment-ui response DTO
    """POST /api/fleet/watchdog/kill-events response."""

    status: str
    rows_written: int


class DeleteInstanceResponse(BaseModel):  # CORRECT-LOCAL: deployment-ui response DTO
    """DELETE /api/fleet/instances/{name} response."""

    name: str
    zone: str
    deleted: bool


class InfraVmAlert(BaseModel):  # CORRECT-LOCAL: deployment-ui response DTO
    """A single notable condition surfaced from an orchestrator VM."""

    severity: AlertSeverity
    kind: str
    detail: str


class InfraVmWatchdog(BaseModel):  # CORRECT-LOCAL: deployment-ui response DTO
    """WorkerLivenessWatchdog standing posture for one orchestrator VM."""

    enabled: bool
    kills_today: int
    daily_cap: int
    dormant: bool
    flapping: bool


class InfraVmSummary(BaseModel):  # CORRECT-LOCAL: deployment-ui response DTO
    """Per-VM aggregation mirrored from the orchestrator's ``VmSummary`` payload."""

    vm_id: str
    role: VmRole
    label: str | None
    slots_total: int
    slots_working: int
    slots_idle: int
    slots_stale: int
    slots_paused: int
    slots_blocked: int
    backlog_total: int
    backlog_queued: int
    alerts: list[InfraVmAlert] = Field(default_factory=list)
    watchdog: InfraVmWatchdog | None


class InfraVmSlot(BaseModel):  # CORRECT-LOCAL: deployment-ui response DTO
    """One orchestrator VM in the fleet, with its reachability + summary."""

    id: str
    label: str
    url: str
    available: bool
    error: str | None
    stale: bool
    last_heartbeat_seconds_ago: int | None
    summary: InfraVmSummary | None


class InfraVmHealthResponse(BaseModel):  # CORRECT-LOCAL: deployment-ui response DTO
    """GET /api/fleet/infra-vm-health response — the proxied orchestrator fleet summary.

    Honest degradation: when the orchestrator is unreachable or no token is configured,
    ``available=False`` + an empty ``vms`` list (NEVER fabricated). ``orchestrator_url`` is
    always present so the UI can deep-link to the orchestrator's own fleet page.
    """

    available: bool
    orchestrator_url: str
    vms: list[InfraVmSlot] = Field(default_factory=list)


__all__ = [
    "AlertSeverity",
    "DeleteInstanceResponse",
    "InfraVmAlert",
    "InfraVmHealthResponse",
    "InfraVmSlot",
    "InfraVmSummary",
    "InfraVmWatchdog",
    "OrphanEntry",
    "OrphanInventoryResponse",
    "OrphanVerdict",
    "ReapRequest",
    "ReapResponse",
    "ReapResultEntry",
    "VmCensusEntry",
    "VmCensusResponse",
    "VmLifecycleClass",
    "VmRole",
    "VmRunStatus",
    "WatchdogKillEventRequest",
    "WatchdogKillEventResponse",
]

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
    "InfraVmAlert",
    "InfraVmHealthResponse",
    "InfraVmSlot",
    "InfraVmSummary",
    "InfraVmWatchdog",
    "VmCensusEntry",
    "VmCensusResponse",
    "VmLifecycleClass",
    "VmRole",
    "VmRunStatus",
]

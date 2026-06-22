# Epic: monitoring_control_plane_master_2026_06_10
# Lifecycle: permanent
"""Mock fixtures for the deployment-ui FleetInfra tile — drive UI dev + the playwright
mock-mode suite (plan: deployment_ui_monitoring_pane_2026_06_19.md).

A mock-mode deployment-api backend returns these so the frontend mock + its regression
spec have deterministic data covering: a LONG_LIVED_LIVE pair that is up, a TERMINATED
OOM-killed backfill VM (exercises the OOM chip), and a two-VM orchestrator fleet with a
working + a stale slot, alerts, and a watchdog posture.

NOTE: the OOM/zombie chips here are HAND-SEEDED fixture values to exercise the UI — the
LIVE census builder (``_fleet_census.build_vm_census``) honestly degrades OOM/zombie to
0/False because no readable watchdog verdict snapshot exists (see ``_fleet_census`` docstring).
"""

from __future__ import annotations

import datetime as dt

from deployment_api.routes._fleet_types import (
    InfraVmAlert,
    InfraVmHealthResponse,
    InfraVmSlot,
    InfraVmSummary,
    InfraVmWatchdog,
    VmCensusEntry,
    VmCensusResponse,
)

_ORCHESTRATOR_URL = "https://api.agent-orchestrator.odum-research.com"


def _now_iso() -> str:
    return dt.datetime.now(dt.UTC).isoformat()


def mock_vm_census() -> VmCensusResponse:
    """Mock census: 2 running (the LONG_LIVED_LIVE planning pair), 3 expected, 1 OOM
    (a TERMINATED defi backfill), 0 zombie, 1 stopped."""
    vms = [
        VmCensusEntry(
            name="planning-vm-1",
            prefix="planning",
            lifecycle_class="LONG_LIVED_LIVE",
            status="RUNNING",
            zombie=False,
            oom=False,
            age_min=8640,
            zone="asia-northeast1-c",
        ),
        VmCensusEntry(
            name="human-planning-vm-1",
            prefix="human-planning",
            lifecycle_class="LONG_LIVED_LIVE",
            status="RUNNING",
            zombie=False,
            oom=False,
            age_min=4320,
            zone="asia-northeast1-c",
        ),
        # OOM-killed defi backfill — TERMINATED, oom=true exercises the OOM chip.
        VmCensusEntry(
            name="vm-defi-backfill-20260622-014158",
            prefix="vm",
            lifecycle_class="EPHEMERAL_BATCH",
            status="TERMINATED",
            zombie=False,
            oom=True,
            age_min=95,
            zone="asia-northeast1-c",
        ),
    ]
    return VmCensusResponse(
        generated_at=_now_iso(),
        running=2,
        expected=3,
        zombie=0,
        oom=1,
        stopped=1,
        vms=vms,
    )


def mock_infra_vm_health() -> InfraVmHealthResponse:
    """Mock infra-vm-health: the central planning VM (working slots, healthy watchdog) +
    the human-planning VM (a stale slot + a stale-slot alert)."""
    planning = InfraVmSlot(
        id="planning",
        label="Central / Orchestrator",
        url="https://api.agent-orchestrator.odum-research.com",
        available=True,
        error=None,
        stale=False,
        last_heartbeat_seconds_ago=12,
        summary=InfraVmSummary(
            vm_id="planning",
            role="planning",
            label="Central / Orchestrator VM",
            slots_total=8,
            slots_working=3,
            slots_idle=5,
            slots_stale=0,
            slots_paused=0,
            slots_blocked=0,
            backlog_total=14,
            backlog_queued=6,
            alerts=[],
            watchdog=InfraVmWatchdog(
                enabled=True,
                kills_today=2,
                daily_cap=20,
                dormant=False,
                flapping=False,
            ),
        ),
    )
    human_planning = InfraVmSlot(
        id="human-planning",
        label="Human Planning",
        url="http://10.0.0.2:8765",
        available=True,
        error=None,
        stale=False,
        last_heartbeat_seconds_ago=45,
        summary=InfraVmSummary(
            vm_id="human-planning",
            role="planning",
            label="Human Planning VM",
            slots_total=4,
            slots_working=1,
            slots_idle=2,
            slots_stale=1,
            slots_paused=0,
            slots_blocked=0,
            backlog_total=3,
            backlog_queued=1,
            alerts=[
                InfraVmAlert(
                    severity="warn",
                    kind="slots_stale",
                    detail="1 slot(s) stale",
                )
            ],
            watchdog=InfraVmWatchdog(
                enabled=True,
                kills_today=0,
                daily_cap=20,
                dormant=False,
                flapping=False,
            ),
        ),
    )
    return InfraVmHealthResponse(
        available=True,
        orchestrator_url=_ORCHESTRATOR_URL,
        vms=[planning, human_planning],
    )


__all__ = ["mock_infra_vm_health", "mock_vm_census"]

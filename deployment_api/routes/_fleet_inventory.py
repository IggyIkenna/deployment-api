# Epic: monitoring_control_plane_master_2026_06_10
# Lifecycle: permanent
"""Stopped/orphaned-VM inventory + reap logic for GET /api/fleet/orphans + POST /api/fleet/reap.

The fleet census (``_fleet_census.py``) only counts RUNNING/STOPPED. This module enriches the
STOPPED/TERMINATED slice with what an operator needs to act: how long it has been stopped, its
lifecycle class, its boot-disk size + estimated idle $/month, and a reap-verdict that MIRRORS
deployment-service ``vm_zombie_watchdog._evaluate_terminated_vm`` (ephemeral lifecycle + stopped
past the grace window + not ``keep=true``-labelled / daemon → reapable). A stopped VM does zero
work, so reaping needs no stall detection — only proof it is an abandoned ephemeral job.

Disk-cost rates are asia-northeast1 (Tokyo) published list rates ($/GB/month) — an ESTIMATE
surfaced for relative cost-awareness, NOT a billing figure (honest-label contract).
"""

from __future__ import annotations

import logging
from datetime import UTC, datetime

from unified_api_contracts import LifecycleClass, classify_vm_name

from deployment_api.routes._fleet_census import _CENSUS_PREFIX_REGISTRY, _DEFAULT_LIFECYCLE
from deployment_api.routes._fleet_types import (
    OrphanEntry,
    OrphanInventoryResponse,
    OrphanVerdict,
    VmLifecycleClass,
)

logger = logging.getLogger(__name__)

# Mirror of deployment-service vm_zombie_watchdog.REAPABLE_LIFECYCLE_CLASSES — only ephemeral
# jobs are reapable; a paused LONG_LIVED_LIVE / SCHEDULED_RECURRING VM is never auto-deleted.
REAPABLE_LIFECYCLE_CLASSES: frozenset[LifecycleClass] = frozenset(
    {LifecycleClass.EPHEMERAL_BATCH, LifecycleClass.EPHEMERAL_EXPERIMENT}
)

# GCE states that mean "not running" — the orphan candidate set.
_STOPPED_STATES: frozenset[str] = frozenset({"STOPPED", "SUSPENDED", "TERMINATED"})

# asia-northeast1 (Tokyo) published persistent-disk list rates, $/GB/month. ESTIMATE only.
_DISK_MONTHLY_USD_PER_GB: dict[str, float] = {
    "pd-standard": 0.052,
    "pd-balanced": 0.130,
    "pd-ssd": 0.221,
    "pd-extreme": 0.221,
}
# Default rate when the disk type is unknown: pd-standard, the dominant (and cheapest) type, so
# an unknown-type estimate is conservative rather than inflated.
_DEFAULT_DISK_RATE: float = 0.052


def _classify(vm_name: str) -> LifecycleClass:
    """Longest-prefix lifecycle class for a VM name (EPHEMERAL_BATCH default on unknown)."""
    try:
        return classify_vm_name(vm_name, _CENSUS_PREFIX_REGISTRY)
    except ValueError:
        return _DEFAULT_LIFECYCLE


def _monthly_disk_usd(size_gb: int | None, disk_type: str | None) -> float:
    """Estimated $/month for a boot disk (0.0 when size is unknown)."""
    if not size_gb:
        return 0.0
    rate = _DISK_MONTHLY_USD_PER_GB.get(disk_type or "", _DEFAULT_DISK_RATE)
    return round(size_gb * rate, 2)


def _stopped_age_hours(stopped_ts: str, creation_ts: str, now: datetime) -> float | None:
    """Hours since the VM stopped (lastStopTimestamp; falls back to creation). None if neither."""
    raw = stopped_ts or creation_ts
    if not raw:
        return None
    try:
        ts = datetime.fromisoformat(raw)
    except ValueError:
        logger.warning("orphans: unparseable timestamp %r", raw)
        return None
    if ts.tzinfo is None:
        ts = ts.replace(tzinfo=UTC)
    return max(0.0, round((now - ts).total_seconds() / 3600.0, 2))


def _verdict(
    lifecycle: LifecycleClass,
    labels: dict[str, str],
    stopped_age_hours: float | None,
    grace_hours: float,
) -> OrphanVerdict:
    """Reap-verdict for a stopped VM — mirrors vm_zombie_watchdog._evaluate_terminated_vm."""
    if labels.get("keep") == "true" or labels.get("tier") in ("daemon", "scheduler"):
        return "keep_retained"
    if lifecycle not in REAPABLE_LIFECYCLE_CLASSES:
        return "keep_not_ephemeral"
    if stopped_age_hours is None:
        return "keep_no_timestamp"
    if stopped_age_hours < grace_hours:
        return "keep_within_grace"
    return "reap"


def _orphan_entry(
    name: str,
    details: dict[str, object],
    disk_details: dict[str, dict[str, object]],
    now: datetime,
    grace_hours: float,
) -> OrphanEntry:
    """Build one OrphanEntry from a stopped VM's GCE details + the disk map."""
    zone = str(details.get("zone", ""))  # noqa: qg-empty-fallback — GCE field absent → blank
    status_raw = str(details.get("status", "")).upper()  # noqa: qg-empty-fallback
    status = status_raw if status_raw in ("STOPPED", "TERMINATED") else "STOPPED"
    stopped_ts = str(details.get("last_stop_timestamp", ""))  # noqa: qg-empty-fallback
    creation_ts = str(details.get("creation_timestamp", ""))  # noqa: qg-empty-fallback
    boot_disk_name = str(details.get("boot_disk_name", ""))  # noqa: qg-empty-fallback
    labels_obj = details.get("labels")
    labels: dict[str, str] = labels_obj if isinstance(labels_obj, dict) else {}

    disk = disk_details.get(boot_disk_name, {})
    size_raw = disk.get("size_gb")
    boot_disk_gb = int(size_raw) if isinstance(size_raw, int) else None
    type_raw = disk.get("type")
    boot_disk_type = str(type_raw) if isinstance(type_raw, str) and type_raw else None

    lifecycle = _classify(name)
    age = _stopped_age_hours(stopped_ts, creation_ts, now)
    verdict = _verdict(lifecycle, labels, age, grace_hours)
    lifecycle_value: VmLifecycleClass = lifecycle.value

    return OrphanEntry(
        name=name,
        zone=zone,
        status=status,
        lifecycle_class=lifecycle_value,
        stopped_age_hours=age,
        boot_disk_gb=boot_disk_gb,
        boot_disk_type=boot_disk_type,
        monthly_disk_usd=_monthly_disk_usd(boot_disk_gb, boot_disk_type),
        reapable=verdict == "reap",
        verdict=verdict,
    )


def build_orphan_inventory(
    vm_details: dict[str, dict[str, object]],
    disk_details: dict[str, dict[str, object]],
    now: datetime,
    grace_hours: float,
) -> OrphanInventoryResponse:
    """Roll the STOPPED/TERMINATED slice of a GCE inventory into the orphan response.

    An empty ``vm_details`` (the honest result of a compute-API failure) yields a zero-count
    inventory — never a crash. Idle-spend rollups sum boot-disk $/month estimates.
    """
    orphans: list[OrphanEntry] = []
    for name in sorted(vm_details):
        details = vm_details[name]
        if str(details.get("status", "")).upper() not in _STOPPED_STATES:
            continue
        orphans.append(_orphan_entry(name, details, disk_details, now, grace_hours))

    reapable = [o for o in orphans if o.reapable]
    return OrphanInventoryResponse(
        generated_at=now.isoformat(),
        grace_hours=grace_hours,
        stopped_total=len(orphans),
        reapable_total=len(reapable),
        monthly_idle_usd=round(sum(o.monthly_disk_usd for o in orphans), 2),
        monthly_reapable_usd=round(sum(o.monthly_disk_usd for o in reapable), 2),
        orphans=orphans,
    )


__all__ = [
    "REAPABLE_LIFECYCLE_CLASSES",
    "build_orphan_inventory",
]

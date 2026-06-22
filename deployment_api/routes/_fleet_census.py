# Epic: monitoring_control_plane_master_2026_06_10
# Lifecycle: permanent
"""VM census builder for GET /api/fleet/vm-census.

Reads the live GCE instance inventory (``vm_utils.get_vm_instance_details``) and rolls it
into the running / expected / zombie / OOM / stopped surface the deployment-ui FleetInfra
tile renders.

Honest-degradation contract (this is a read-only monitoring endpoint):

* **Lifecycle classification** — the canonical prefix→lifecycle authority is
  deployment-service's ``VM_PREFIX_TO_BUCKET`` (~50 entries). Copying it here would couple
  deployment-api to a service repo + drift constantly, so we keep a small honest local
  registry of the live-today prefixes and route through the UAC ``classify_vm_name``
  longest-prefix matcher. An unknown prefix degrades to ``EPHEMERAL_BATCH`` (the safe
  default — the overwhelming majority of unregistered VM names are batch pipeline jobs).
* **zombie / OOM** — sourced from the GCS census snapshot written by deployment-service's
  ``vm_zombie_watchdog`` after each poll cycle (``vm-census/watchdog-census.json`` in the
  deployment-scripts bucket). ``load_watchdog_census`` reads the blob, checks staleness
  (>``_CENSUS_STALE_MINUTES``), and returns ``(zombie_names, oom_names)`` frozensets;
  absent/stale → ``(frozenset(), frozenset())`` so the census degrades honestly to 0 rather
  than fabricating data. OOM is always ``[]`` in the snapshot (no OOM detection exists today
  — honest absence).
"""

from __future__ import annotations

import json
import logging
from datetime import UTC, datetime, timedelta

from unified_api_contracts import LifecycleClass, VmPrefixSpec, classify_vm_name
from unified_trading_library import get_storage_client

from deployment_api.routes._fleet_types import (
    VmCensusEntry,
    VmCensusResponse,
    VmLifecycleClass,
    VmRunStatus,
)

logger = logging.getLogger(__name__)

# Honest, minimal prefix→lifecycle registry. NOT a copy of deployment-service's
# VM_PREFIX_TO_BUCKET (drift + service-coupling) — only the prefixes that actually run in
# the live fleet today plus the obvious ephemeral-vs-scheduled buckets. The bucket field is
# unused here (census never reads shard output); only ``lifecycle_class`` matters. Longest
# prefix wins (UAC ``classify_vm_name``), so a specific entry can refine a generic one.
_CENSUS_PREFIX_REGISTRY: dict[str, VmPrefixSpec] = {
    # LONG_LIVED_LIVE — the always-up fleet (CLAUDE.md § "TWO LIVE VMs": planning +
    # human-planning) + the live trading/orchestrator clusters.
    "planning": VmPrefixSpec(bucket=None, lifecycle_class=LifecycleClass.LONG_LIVED_LIVE),
    "human-planning": VmPrefixSpec(bucket=None, lifecycle_class=LifecycleClass.LONG_LIVED_LIVE),
    "agent-orchestrator": VmPrefixSpec(bucket=None, lifecycle_class=LifecycleClass.LONG_LIVED_LIVE),
    "agent-orch-vm-": VmPrefixSpec(bucket=None, lifecycle_class=LifecycleClass.LONG_LIVED_LIVE),
    "strategy-paper-": VmPrefixSpec(bucket=None, lifecycle_class=LifecycleClass.LONG_LIVED_LIVE),
    "strategy-live-": VmPrefixSpec(bucket=None, lifecycle_class=LifecycleClass.LONG_LIVED_LIVE),
    "defi-recursive-": VmPrefixSpec(bucket=None, lifecycle_class=LifecycleClass.LONG_LIVED_LIVE),
    # EPHEMERAL_EXPERIMENT — research / backtest jobs (mirror monitor_experiments prefixes).
    "ml-train-": VmPrefixSpec(bucket=None, lifecycle_class=LifecycleClass.EPHEMERAL_EXPERIMENT),
    "strategy-backtest-": VmPrefixSpec(bucket=None, lifecycle_class=LifecycleClass.EPHEMERAL_EXPERIMENT),
    "execution-backtest-": VmPrefixSpec(bucket=None, lifecycle_class=LifecycleClass.EPHEMERAL_EXPERIMENT),
    "exp-": VmPrefixSpec(bucket=None, lifecycle_class=LifecycleClass.EPHEMERAL_EXPERIMENT),
    # SCHEDULED_RECURRING — cron / scheduler / watchdog / consolidator daemons.
    "vm-zombie-watchdog-": VmPrefixSpec(bucket=None, lifecycle_class=LifecycleClass.SCHEDULED_RECURRING),
    "manifest-consolidator-": VmPrefixSpec(bucket=None, lifecycle_class=LifecycleClass.SCHEDULED_RECURRING),
    "sports-scheduler-": VmPrefixSpec(bucket=None, lifecycle_class=LifecycleClass.SCHEDULED_RECURRING),
    "qg-snapshot-": VmPrefixSpec(bucket=None, lifecycle_class=LifecycleClass.SCHEDULED_RECURRING),
}

# Unknown prefix → batch. The honest default: an unregistered VM is almost always an
# ephemeral data-pipeline backfill (cefi-*/defi-*/tradfi-*/etc.), which IS EPHEMERAL_BATCH.
_DEFAULT_LIFECYCLE = LifecycleClass.EPHEMERAL_BATCH

# GCS census snapshot written by vm_zombie_watchdog.py after each poll cycle.
_CENSUS_BLOB = "vm-census/watchdog-census.json"
_CENSUS_STALE_MINUTES = 30  # watchdog runs ~15 min; stale after 2 missed cycles → degrade

# GCE instance.status values → our 4-state wire literal. GCE has more states
# (PROVISIONING / STAGING / REPAIRING / SUSPENDED) — map them honestly to the nearest
# wire state rather than inventing values the frontend union doesn't allow.
_STATUS_MAP: dict[str, VmRunStatus] = {
    "RUNNING": "RUNNING",
    "PROVISIONING": "RUNNING",
    "STAGING": "RUNNING",
    "REPAIRING": "RUNNING",
    "STOPPING": "STOPPING",
    "SUSPENDING": "STOPPING",
    "STOPPED": "STOPPED",
    "SUSPENDED": "STOPPED",
    "TERMINATED": "TERMINATED",
}

# Lifecycle classes that are EXPECTED to always be up (the census "expected" count).
_EXPECTED_LIFECYCLES = frozenset({LifecycleClass.LONG_LIVED_LIVE, LifecycleClass.SCHEDULED_RECURRING})


def _vm_prefix(vm_name: str) -> str:
    """Best-effort human prefix for the UI ('cefi-binance-…' → 'cefi').

    The first dash-segment is the asset-group / job-family label the operator recognises.
    Purely cosmetic — the lifecycle classification uses the full name + longest-prefix match.
    """
    return vm_name.split("-", 1)[0] if "-" in vm_name else vm_name


def _classify_lifecycle(vm_name: str) -> VmLifecycleClass:
    """Classify a VM by name, degrading to EPHEMERAL_BATCH on an unknown prefix."""
    try:
        return classify_vm_name(vm_name, _CENSUS_PREFIX_REGISTRY).value
    except ValueError:
        return _DEFAULT_LIFECYCLE.value


def _map_status(raw_status: str) -> VmRunStatus:
    """Map a GCE instance.status to the 4-state wire literal (default STOPPED on unknown)."""
    return _STATUS_MAP.get(raw_status.upper(), "STOPPED")


def _age_min(creation_timestamp: str, now: datetime) -> int | None:
    """Minutes since ``creation_timestamp`` (ISO-8601 w/ tz). None if blank/unparseable."""
    if not creation_timestamp:
        return None
    try:
        created = datetime.fromisoformat(creation_timestamp)
    except ValueError:
        logger.warning("vm-census: unparseable creation_timestamp %r", creation_timestamp)
        return None
    if created.tzinfo is None:
        created = created.replace(tzinfo=UTC)
    delta_s = (now - created).total_seconds()
    return max(0, int(delta_s // 60))


def load_watchdog_census(
    project_id: str,
    now: datetime,
) -> tuple[frozenset[str], frozenset[str]]:
    """Read the watchdog census JSON written by vm_zombie_watchdog.py.

    Returns ``(zombie_names, oom_names)`` — both are frozensets of VM names.
    Degrades to ``(frozenset(), frozenset())`` when the blob is absent, unreadable,
    or stale (older than ``_CENSUS_STALE_MINUTES``) so the census never fabricates data.
    """
    bucket_name = f"deployment-scripts-{project_id}"
    try:
        storage = get_storage_client(project_id=project_id)
        bucket = storage.bucket(bucket_name)
        blob = bucket.blob(_CENSUS_BLOB)
        raw = blob.download_as_bytes()
        data = json.loads(raw)
        ts_str = str(data.get("ts", ""))  # noqa: qg-empty-fallback — absent ts → stale
        ts = datetime.fromisoformat(ts_str)
        if ts.tzinfo is None:
            ts = ts.replace(tzinfo=UTC)
        if (now - ts) > timedelta(minutes=_CENSUS_STALE_MINUTES):
            logger.warning(
                "vm-census: watchdog census stale (age=%.0f min > %d min threshold) — degrading zombie/OOM to 0",
                (now - ts).total_seconds() / 60,
                _CENSUS_STALE_MINUTES,
            )
            return frozenset(), frozenset()
        zombie_names = frozenset(str(n) for n in data.get("zombies", []))  # noqa: qg-empty-fallback — absent key → empty
        oom_names = frozenset(str(n) for n in data.get("oom", []))  # noqa: qg-empty-fallback — absent key → empty
        return zombie_names, oom_names
    except Exception as exc:
        logger.warning("vm-census: failed to load watchdog census (degrading to 0): %s", exc)
        return frozenset(), frozenset()


def build_vm_census(
    vm_details: dict[str, dict[str, object]],
    now: datetime,
    watchdog_census: tuple[frozenset[str], frozenset[str]] | None = None,
) -> VmCensusResponse:
    """Roll a GCE instance-details map into the VM-census response.

    Args:
        vm_details: ``{vm_name: {status, zone, creation_timestamp, machine_type}}`` as
            returned by ``vm_utils.get_vm_instance_details``. An EMPTY dict (the honest
            result of a compute-API failure) yields all-zero counts + an empty ``vms`` list
            — never a crash.
        now: UTC "now" for age computation (injected for deterministic tests).
        watchdog_census: ``(zombie_names, oom_names)`` frozensets from
            ``load_watchdog_census``. ``None`` or an absent/stale snapshot degrades
            honestly to ``zombie=0 / oom=False`` — never fabricated.

    Returns:
        A :class:`VmCensusResponse` with live zombie/OOM counts when the watchdog
        census snapshot is present and fresh; degraded all-zero counts otherwise.
    """
    zombie_names, oom_names = watchdog_census if watchdog_census is not None else (frozenset(), frozenset())

    entries: list[VmCensusEntry] = []
    running = expected = stopped = zombie_count = oom_count = 0

    for name in sorted(vm_details):
        details = vm_details[name]
        raw_status = str(details.get("status", ""))  # noqa: qg-empty-fallback — GCE field absent → census default
        zone = str(details.get("zone", ""))  # noqa: qg-empty-fallback — GCE field absent → census default
        creation_timestamp = str(details.get("creation_timestamp", ""))  # noqa: qg-empty-fallback — GCE field default

        lifecycle = _classify_lifecycle(name)
        status = _map_status(raw_status)
        is_zombie = name in zombie_names
        is_oom = name in oom_names

        if status == "RUNNING":
            running += 1
        elif status in ("STOPPED", "TERMINATED"):
            stopped += 1
        if lifecycle in _EXPECTED_LIFECYCLES:
            expected += 1
        if is_zombie:
            zombie_count += 1
        if is_oom:
            oom_count += 1

        entries.append(
            VmCensusEntry(
                name=name,
                prefix=_vm_prefix(name),
                lifecycle_class=lifecycle,
                status=status,
                zombie=is_zombie,
                oom=is_oom,
                age_min=_age_min(creation_timestamp, now),
                zone=zone,
            )
        )

    return VmCensusResponse(
        generated_at=now.isoformat(),
        running=running,
        expected=expected,
        zombie=zombie_count,
        oom=oom_count,
        stopped=stopped,
        vms=entries,
    )


__all__ = ["build_vm_census", "load_watchdog_census"]

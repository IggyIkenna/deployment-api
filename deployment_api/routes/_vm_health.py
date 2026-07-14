# Epic: observability_master
# Lifecycle: permanent
"""VM composite work-health classifiers (parent plan D.3).

Pure, I/O-free classifiers extracted from ``deployments_inventory`` to keep that hotspot
file under control: the wire status (``_vm_status``) and the D.3 composite health verdict
(``_composite_health_status``) for a VM deployment-registry entry. They take a
``DeploymentRegistryEntry`` plus caller-resolved signals and return a string — no
``DeploymentItem`` coupling, so ``deployments_inventory`` imports these one-way (and
re-exports them for existing callers/tests).
"""

from __future__ import annotations

from unified_api_contracts import DeploymentUmbrella
from unified_trading_library import DeploymentRegistryEntry

# A running VM whose heartbeat is older than this is stale/hung.
_STALE_HEARTBEAT_MINUTES = 15

# D.3 v1 defaults (documented, not a global CPU cut — parent WS-D.0 principle 1).
_DISK_FULL_PCT_THRESHOLD = 90.0
_OOM_RISK_MEM_PCT_FLOOR = 80.0
# `stalled` threshold table (parent WS-D.3, Open-Q3) — progress-metric primary, cpu secondary,
# NEVER a global CPU cut. Only the BATCH row is wired (object_delta is the only prerequisite
# signal that exists today); LIVE ("no events/heartbeat-progress >=5min during an expected-active
# window") and PAPER ("work_delta==0 >=15min") need signals — an active-window calendar and a
# rows_out-delta tracker respectively — that don't exist yet, so they stay honest-`"unknown"`.
_STALLED_BATCH_CPU_PCT_CEILING = 10.0


def vm_status(entry: DeploymentRegistryEntry, hb_age_seconds: int | None) -> str:
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


def composite_health_status(
    entry: DeploymentRegistryEntry,
    hb_age_seconds: int | None,
    *,
    control_plane_running: bool | None = None,
    umbrella: DeploymentUmbrella | None = None,
    object_delta: int | None = None,
) -> str | None:
    """D.3 VM composite health — resource-in-band AND a work signal advancing
    (parent WS-D.0 principle 1), computed only for a currently ``running`` entry
    (a terminal entry's health is already the exit_code/status).

    v1 covers 6 of the 7 states with a real signal today: ``dead`` / ``hung`` /
    ``workload-dead`` / ``disk-full`` / ``oom-risk`` / ``working``, plus
    ``stalled`` for the BATCH umbrella only — the one row of the parent WS-D.3
    threshold table whose prerequisite signal (``object_delta``, the manifest
    lookup) is wired. LIVE/PAPER ``stalled`` still degrade to ``"unknown"``:
    their threshold-table signals (an expected-active-window calendar for LIVE,
    a ``work_delta`` rows-out-delta tracker for PAPER) don't exist anywhere in
    the codebase yet, so guessing from a proxy would violate principle 2 (a
    hint is not truth).

    ``control_plane_running`` is the GCE aggregated-list confirmation (the
    ``get_vm_instance_details`` census future the inventory fetches) — ``None`` when the
    caller has no confirmation to offer, in which case ``dead`` never fires and
    ``hung`` falls back to heartbeat staleness alone (no regression vs. the
    pre-D.3 status, just less certain without the control-plane cross-check).

    ``umbrella`` / ``object_delta`` are this entry's classified umbrella + its
    asset_group's manifest object-delta, both resolved by the caller — the
    caller batches ``object_delta`` ONE lookup per DISTINCT asset_group across
    the whole census cycle (``_batched_object_deltas``), never per VM entry
    (WS-D.0 principle 5, zero new bucket walks). ``None`` for either (the
    default) means ``stalled`` never fires and ``working`` falls back to the
    pre-existing io/net-only check — no regression vs. the prior honest-unknown
    fallback. A positive ``object_delta`` also counts toward ``working`` (parent
    WS-D.3: "resource in-band AND (object_delta>0 OR io_write_rate>0)") since a
    batch VM can show real progress between bursty writes with an idle-looking
    ``/proc`` sample.
    """
    if entry.status != "running":
        return None
    if control_plane_running is False:
        return "dead"
    if hb_age_seconds is not None and hb_age_seconds > _STALE_HEARTBEAT_MINUTES * 60:
        return "hung"
    if entry.workload_alive is False:
        return "workload-dead"
    if not _has_d1_metrics(entry):
        return "unknown"
    if entry.disk_pct > _DISK_FULL_PCT_THRESHOLD:
        return "disk-full"
    if entry.mem_pct >= _OOM_RISK_MEM_PCT_FLOOR and entry.mem_slope > 0:
        return "oom-risk"
    # Parent WS-D.3: working = resource in-band AND (object_delta>0 OR io_write_rate>0) — the
    # manifest signal counts too, not just this tick's /proc rates (a batch VM can show a real
    # object_delta between bursty writes with an idle-looking sample).
    if entry.io_write_rate_bytes_sec > 0 or entry.net_recv_rate_bytes_sec > 0 or (object_delta or 0) > 0:
        return "working"
    if umbrella == DeploymentUmbrella.BATCH and object_delta == 0 and entry.cpu_pct < _STALLED_BATCH_CPU_PCT_CEILING:
        return "stalled"
    return "unknown"


__all__ = [
    "composite_health_status",
    "vm_status",
]

# Epic: observability_master
# Lifecycle: permanent
"""Run-log read-path resolution — live-first, archive-fallback (WS-4 decision 2).

SSOT for "which GCS object holds this VM's run.log content right now": try the
live streaming path (``vm-logs/{vm}/run.log``, 14-day TTL from last write) first,
for ANY vm regardless of ``completed_at``; fall back to the durable final-snapshot
archive path (``log-archive/final/{vm}/run.log``, no TTL, written once at actual VM
completion by ``HeartbeatDaemon._write_final_log_snapshot``) only when the live
object is confirmed absent.

Replaces the broken ``completed_at[:10]``-keyed rolling-date guess
(``vm_run_log_rolling_uri``), which assumed the archival cron wrote a copy dated by
completion day when it actually writes daily rolling copies keyed by cron-run date —
confirmed 404ing live for real VMs (``deployment_ui_vm_log_viewer_2026_07_20.md``).

This is a single-VM, request-time resolution (the metadata/tail/download endpoints
that call it each serve one VM per request) — deliberately NOT used by the bulk
inventory/vm-deployments census endpoints, which stay pure/I/O-free for their
45s-SWR-cached, whole-fleet walk (see ``deployments_inventory.py::_vm_item`` /
``vm_deployments.py::_to_model``).
"""

from __future__ import annotations

from dataclasses import dataclass

from unified_trading_library import BlobMetadata, gcs_describe_object, vm_log_stream_uri, vm_run_log_final_uri


@dataclass(frozen=True)
class RunLogLocation:
    """The resolved run.log object for one VM, with its metadata if it exists."""

    uri: str
    location: str  # "live" | "archive"
    metadata: BlobMetadata | None  # None => neither path has an object yet


def resolve_run_log_location(vm_name: str, project_id: str | None = None) -> RunLogLocation:
    """Resolve + describe the run.log object to read for ``vm_name`` — live-first, archive-fallback.

    Exactly two ``gcs_describe_object`` calls in the worst case (live miss + archive
    describe); one call when the live path hits. ``metadata is None`` means neither
    path has an object yet (e.g. a VM that completed before the final-snapshot writer
    shipped) — callers surface this as an honest "no log available" state, never a
    dead link.
    """
    live_uri = vm_log_stream_uri(vm_name, project_id)
    live_meta = gcs_describe_object(live_uri)
    if live_meta is not None:
        return RunLogLocation(uri=live_uri, location="live", metadata=live_meta)
    archive_uri = vm_run_log_final_uri(vm_name, project_id)
    archive_meta = gcs_describe_object(archive_uri)
    return RunLogLocation(uri=archive_uri, location="archive", metadata=archive_meta)

"""Offline data-status rollup worker — Cloud Run Job entrypoint.

Runs every 5 min via Cloud Scheduler cron (terraform module:
``deployment-service/terraform/gcp/data_status_rollup_scheduler.tf``).
For each tracked service, computes the FULL Jan 2018 → today data-status
response by calling :class:`DataStatusService.get_manifest_status`
synchronously and writes the gzipped JSON to::

    gs://{project_id}-data-status-rollups/{service}/full.json.gz

The deployment-api ``/api/data-status/manifest`` endpoint reads the
rollup, slices by the user's date range in-memory (microseconds), and
returns. **Latency drops from ~310-410s to <500ms** for the full range.

Plan:
    ``unified-trading-pm/plans/active/data_status_offline_rollup_2026_05_06.md``

Why offline rollup vs on-demand:
    The honest-coverage compute is GIL-bound Python loops over 5 x
    ~30 venues x ~8 data_types x ~3000 dates. Profiled in tier-3
    benchmark 2026-05-06: 411s serial, 327s with 5-process fork pool
    (DEFI alone is ~300s and caps any parallelism). A precomputed
    full-range rollup is a strict superset of every sub-range a user
    can pick — compute once globally, slice in the API.

Image:
    Reuses the deployment-api image (the same image powering the
    ``uts-shared-deployment-api`` Cloud Run service). Has all the
    deps + the DataStatusService code natively.

CLI:
    python -m deployment_api.scripts.data_status_rollup_worker \\
        --project=<gcp-project-id> \\
        --bucket=<gcp-project-id>-data-status-rollups
"""

from __future__ import annotations

import argparse
import contextlib
import datetime as _dt
import gzip
import io
import json
import logging
import multiprocessing
import resource
import sys
import time
from typing import Any, cast

from unified_trading_library import GcsEventSink, get_storage_client, log_event, run_lifecycle, setup_events

from deployment_api.services.data_status_service import DataStatusService

logger = logging.getLogger(__name__)


# Services to roll up — every service whose ``/api/data-status/manifest``
# response would otherwise compute on demand. Sourced from
# ``DataStatusService._BUCKET_TEMPLATES.keys()`` minus the ones that don't
# produce a coverage matrix (none currently — all entries qualify).
_DEFAULT_SERVICES: tuple[str, ...] = (
    "instruments-service",
    "market-tick-data-service",
    "market-data-processing-service",
    "features-delta-one-service",
    "features-volatility-service",
    "features-onchain-service",
    "features-sports-service",
    "features-calendar-service",
    "features-multi-timeframe-service",
    "features-cross-instrument-service",
    "features-commodity-service",
    # ml-training-service + ml-inference-service consolidated into ml-service (2026-05-21)
    "ml-service",
    "strategy-service",
    "execution-service",
)

# Public alias — imported by the in-service rollup endpoint (routes/data_status/_rollup.py),
# which runs this worker's compute in the gen1 service (the gen2 Job crashes natively).
DEFAULT_SERVICES: tuple[str, ...] = _DEFAULT_SERVICES

# Compute starts from this fixed date — covers every service's launch.
_ROLLUP_START_DATE = "2018-01-01"

# Per-service child-process memory ceiling — set comfortably below this
# service's Cloud Run container memory limit (see
# deployment-service/terraform/gcp/data_status_rollup_scheduler.tf), leaving
# headroom for the parent process + gen1 sidecar overhead. Each service's
# compute runs in its own throwaway spawned child capped at this limit, so a
# service whose full 2018-today range is simply too large (MTDS/MDPS — see
# plan deployment_api_cache_oom_and_ui_latency_remediation_2026_07_13.md §3,
# "no RAM tier through 64GB survives it") raises a catchable MemoryError
# inside ITS OWN child instead of Cloud Run OOM-killing the whole container.
# Before this isolation existed, a whole-container kill silently dropped
# every service queued after the offender — found 2026-07-13: only
# instruments-service (first in _DEFAULT_SERVICES) had ever produced a
# rollup blob; nothing past it (including small, cheap services) ever got a
# chance to run.
#
# STRUCTURAL, ACCEPTED per-service gaps as of 2026-08-02 (data volume growth
# outpacing these ceilings — see
# plans/active/issues/data_status_rollup_ml_service_full_blob_missing_2026_07_26.md
# todo 2 for the full evidence): in addition to the original MTDS gap above,
# instruments-service's manifest step now regularly raises MemoryError
# ("Unable to allocate 2.55 GiB for an array with shape (29, ~11.8M) and data
# type object") while its coverage step still succeeds, and
# market-data-processing-service now regularly hits _CHILD_JOIN_TIMEOUT_S on
# BOTH its manifest and coverage steps. Both are handled the SAME
# honest-failure way as MTDS — caught per-service, logged loudly via
# logger.error + a SERVICE_FAILED log_event, never a silent placeholder.
#
# SHARDED 2026-08-07 (infra_health_audit_findings_fix_2026_08_07.md P1, the
# uts-prod-data-status-rollup-svc container OOM — "Memory limit of 32768 MiB
# exceeded with 32860 MiB used" — causing the 420s per-service timeouts above for
# MTDS/instruments-service): the per-SERVICE isolation this ceiling protects was
# necessary but not sufficient — within one service's child, the manifest and
# coverage builds still looped over each MarketCategory asset-group SERIALLY
# in-process (data_status_service._THREAD_POOL_DISABLED forces "one AG at a time",
# but pandas/pyarrow/numpy's C allocators do not reliably return freed arena memory
# to the OS between loop iterations, so RSS ratchets up across the ~5 categories
# even though only one is logically alive at a time). _SERIAL_DISPATCH_ISOLATED
# (set True below) shards ONE LEVEL FURTHER: each category now runs in its OWN
# throwaway subprocess (manifest.py's _dispatch_serial_isolated,
# coverage.py's _build_coverage_for_cat_dispatch) so the OS reclaims 100% of a
# category's memory via full process exit before the next category starts, instead
# of relying on in-process GC. This is the genuine fix, not a ceiling bump — see
# data_status_service.py's _SERIAL_DISPATCH_ISOLATED docstring for the full
# rationale. The per-SERVICE ceilings below are kept unchanged as the outer
# defense-in-depth backstop.
_CHILD_RLIMIT_AS_BYTES = 24 * 1024**3  # 24Gi, for a 32Gi container ceiling
_CHILD_JOIN_TIMEOUT_S = 420  # wall-clock backstop; the rlimit above is the primary guard


def _today_iso() -> str:
    """Return today's UTC date as ``YYYY-MM-DD``."""
    return _dt.datetime.now(_dt.UTC).date().isoformat()


def _rollup_blob_path(service: str) -> str:
    """GCS object path for a service's manifest rollup blob.

    SSOT: ``rollup_cache.rollup_blob_path``.
    """
    from deployment_api.services.data_status.rollup_cache import rollup_blob_path

    return rollup_blob_path(service, "full")


def _coverage_blob_path(service: str) -> str:
    """GCS object path for a service's coverage-summary rollup blob."""
    from deployment_api.services.data_status.rollup_cache import rollup_blob_path

    return rollup_blob_path(service, "coverage")


def _build_one_service_rollup(dss: DataStatusService, service: str, end_date: str) -> dict[str, Any]:
    """Compute the full-range manifest rollup for one service.

    Bypasses the rollup-cache fast-path in
    :meth:`DataStatusService.get_manifest_status` — we are the writer of
    that cache, not its consumer; reading our own previous blob and re-
    writing it would freeze the rollup at the first written content
    forever. Always force-compute by calling the sync impl directly.

    Raises whatever the underlying call raises — caller decides whether
    to record_failed or record_captured (per CLAUDE.md "honest absence
    vs fake placeholders" — a partial / errored rollup is worse than no
    rollup, since the slicer would silently slice garbage).
    """
    return dss._get_manifest_status_sync(  # pyright: ignore[reportPrivateUsage]
        service=service,
        start_date=_ROLLUP_START_DATE,
        end_date=end_date,
        asset_groups=None,
    )


def _build_one_service_coverage(dss: DataStatusService, service: str) -> dict[str, Any]:
    """Compute the full coverage-summary for one service.

    Bypasses the rollup-cache fast-path for the same reason as
    :func:`_build_one_service_rollup` — the worker is the cache producer,
    not consumer. ``get_coverage_summary`` would otherwise read the
    existing blob (its own previous output, fresh by construction) and
    return it unchanged, freezing the rollup at the first written shape.
    """
    return dss._get_coverage_summary_sync(service=service, asset_groups=None)  # pyright: ignore[reportPrivateUsage]


def _gzip_payload(payload: dict[str, Any]) -> tuple[bytes, int]:
    """Gzip-compress a JSON-serialisable dict. Returns (compressed_bytes, raw_size)."""
    raw = json.dumps(payload, default=str).encode("utf-8")
    buf = io.BytesIO()
    with gzip.GzipFile(fileobj=buf, mode="wb", compresslevel=6) as gz:
        gz.write(raw)
    return buf.getvalue(), len(raw)


def _write_rollup_to_gcs(storage_client: object, bucket: str, service: str, payload: dict[str, Any]) -> dict[str, int]:
    """Gzip + upload the manifest rollup via the unified cloud-interface API."""
    compressed, raw_size = _gzip_payload(payload)
    storage_client.upload_bytes(  # pyright: ignore[reportAttributeAccessIssue,reportUnknownMemberType]
        bucket=bucket,
        blob_path=_rollup_blob_path(service),
        data=compressed,
        content_type="application/json",
        metadata={"content-encoding": "gzip"},
    )
    return {"size_compressed": len(compressed), "size_uncompressed": raw_size}


def _write_coverage_to_gcs(
    storage_client: object, bucket: str, service: str, payload: dict[str, Any]
) -> dict[str, int]:
    """Gzip + upload the coverage-summary blob (paired with manifest rollup)."""
    compressed, raw_size = _gzip_payload(payload)
    storage_client.upload_bytes(  # pyright: ignore[reportAttributeAccessIssue,reportUnknownMemberType]
        bucket=bucket,
        blob_path=_coverage_blob_path(service),
        data=compressed,
        content_type="application/json",
        metadata={"content-encoding": "gzip"},
    )
    return {"size_compressed": len(compressed), "size_uncompressed": raw_size}


def _set_child_memory_rlimit(limit_bytes: int) -> None:
    """Cap this process's virtual address space.

    Once exceeded, the allocator fails and Python raises a catchable
    ``MemoryError`` instead of the process silently growing until Cloud
    Run's platform-level OOM killer terminates the whole container.
    """
    resource.setrlimit(resource.RLIMIT_AS, (limit_bytes, limit_bytes))


def _child_build_and_write_service(
    project_id: str,
    bucket: str,
    service: str,
    end_date: str,
    rlimit_bytes: int,
    result_queue: multiprocessing.Queue[dict[str, Any]],
) -> None:
    """Child-process entrypoint: compute + write one service's rollup + coverage.

    Runs in its own spawned process (see :func:`_run_service_isolated`) so a
    memory blowup — a caught ``MemoryError`` from the rlimit above, or a
    harder native crash — only kills this throwaway child, never the parent
    request/container. Always puts exactly one result dict on the queue
    (even on failure) so the parent never blocks past its join timeout.
    """
    result: dict[str, Any] = {"service": service, "manifest_ok": False, "coverage_ok": False}
    try:
        _set_child_memory_rlimit(rlimit_bytes)
    except Exception as e:  # best-effort guard: proceed unprotected rather than abort the service
        logger.warning("could not set memory rlimit for service=%s: %s", service, e)

    try:
        # Force fully-serial per-asset-group compute in THIS fresh child
        # interpreter. Running asset groups in parallel threads/processes
        # holds every AG's cell-grid intermediate at once and OOMs well
        # before the rlimit above would catch it cleanly; serial holds one
        # AG at a time. This mirrors the two overrides the parent process
        # used to set on itself before this isolation existed — each child
        # is single-use and re-imports the module fresh, so there is no
        # "restore afterward" concern.
        import deployment_api.services.data_status_service as _dss_mod

        _dss_mod._PROCESS_POOL_DISABLED = True  # pyright: ignore[reportPrivateUsage]
        _dss_mod._THREAD_POOL_DISABLED = True  # pyright: ignore[reportPrivateUsage]
        # Per-category subprocess isolation — see _SERIAL_DISPATCH_ISOLATED's docstring
        # in data_status_service.py (root-cause fix for the 32Gi container OOM, 2026-08-07).
        _dss_mod._SERIAL_DISPATCH_ISOLATED = True  # pyright: ignore[reportPrivateUsage]

        storage_client = get_storage_client(project_id=project_id)
        dss = DataStatusService()
    except Exception as e:  # must never escape uncaught; the parent needs a result either way
        result["error"] = f"init: {e}"
        with contextlib.suppress(Exception):
            result_queue.put(result)
        return

    t0 = time.monotonic()
    try:
        payload = _build_one_service_rollup(dss, service, end_date)
        metrics = _write_rollup_to_gcs(storage_client, bucket, service, payload)
        result["manifest_ok"] = True
        result["manifest_elapsed_s"] = round(time.monotonic() - t0, 1)
        result["size_compressed"] = metrics["size_compressed"]
        result["size_uncompressed"] = metrics["size_uncompressed"]
        result["asset_groups_n"] = len(payload.get("asset_groups", {}))  # noqa: qg-empty-fallback — telemetry count only
        del payload, metrics
    except Exception as e:  # per-service isolation; caller logs + moves on
        result["manifest_elapsed_s"] = round(time.monotonic() - t0, 1)
        result["manifest_error"] = str(e)

    t1 = time.monotonic()
    try:
        cov_payload = _build_one_service_coverage(dss, service)
        cov_metrics = _write_coverage_to_gcs(storage_client, bucket, service, cov_payload)
        result["coverage_ok"] = True
        result["coverage_elapsed_s"] = round(time.monotonic() - t1, 1)
        result["coverage_size_compressed"] = cov_metrics["size_compressed"]
    except Exception as e:  # per-service isolation; caller logs + moves on
        result["coverage_elapsed_s"] = round(time.monotonic() - t1, 1)
        result["coverage_error"] = str(e)

    with contextlib.suppress(Exception):
        result_queue.put(result)


def _run_service_isolated(project_id: str, bucket: str, service: str, end_date: str) -> dict[str, Any]:
    """Run one service's rollup compute in an isolated spawned child process.

    Returns a result dict describing what happened, synthesising a failure
    result if the child was OOM-killed / crashed / timed out before it could
    report back — the caller (:func:`run_rollup`) treats this exactly like
    an in-process exception would have been treated before this isolation
    was added, so a doomed service (e.g. a full-history MTDS/MDPS build)
    never prevents any other service's rollup from being written.
    """
    ctx = multiprocessing.get_context("spawn")
    result_queue: multiprocessing.Queue[dict[str, Any]] = ctx.Queue()
    process = ctx.Process(
        target=_child_build_and_write_service,
        args=(project_id, bucket, service, end_date, _CHILD_RLIMIT_AS_BYTES, result_queue),
        daemon=True,
        name=f"rollup-{service[:24]}",
    )
    process.start()
    process.join(timeout=_CHILD_JOIN_TIMEOUT_S)

    if process.is_alive():
        process.terminate()
        process.join(timeout=10)
        if process.is_alive():
            process.kill()
        return {
            "service": service,
            "manifest_ok": False,
            "coverage_ok": False,
            "error": f"timed out after {_CHILD_JOIN_TIMEOUT_S}s",
        }

    if not result_queue.empty():
        return cast(dict[str, Any], result_queue.get())

    return {
        "service": service,
        "manifest_ok": False,
        "coverage_ok": False,
        "error": f"child exited with code {process.exitcode} before reporting a result (likely OOM-killed)",
    }


def run_rollup(project_id: str, bucket: str, services: list[str]) -> int:
    """Compute and upload one rollup per service. Returns process exit code.

    Each service's compute + write runs in its own isolated child process
    (see :func:`_run_service_isolated`) — a service whose full-history build
    is simply too large for this container's memory (MTDS/MDPS) fails on
    its own without preventing any other service's rollup from being
    written. Before this isolation existed, one such failure OOM-killed the
    whole container mid-sweep and silently dropped every service queued
    after the offender.
    """
    end_date = _today_iso()

    successes = 0
    failures: list[tuple[str, str]] = []
    for service in services:
        result = _run_service_isolated(project_id, bucket, service, end_date)

        if result.get("manifest_ok"):
            log_event(
                "SERVICE_PROCESSED",
                details={
                    "service": service,
                    "kind": "manifest",
                    "elapsed_s": result.get("manifest_elapsed_s"),
                    "size_compressed": result.get("size_compressed"),
                    "size_uncompressed": result.get("size_uncompressed"),
                    "asset_groups_n": result.get("asset_groups_n"),
                },
            )
            successes += 1
        else:
            error = result.get("manifest_error") or result.get("error") or "unknown"
            failures.append((service, f"manifest: {error}"))
            log_event(
                "SERVICE_FAILED",
                severity="ERROR",
                details={
                    "service": service,
                    "kind": "manifest",
                    "elapsed_s": result.get("manifest_elapsed_s"),
                    "error": str(error),
                },
            )
            logger.error("manifest rollup failed for service=%s: %s", service, error)

        # Coverage-summary rollup — paired with manifest. The deployment-ui
        # data-status panel fires both /manifest and /coverage-summary in
        # parallel; before this rollup landed coverage-summary was a 15s
        # on-demand GCS scan. Now it's a sub-second GCS-blob read.
        if result.get("coverage_ok"):
            log_event(
                "SERVICE_PROCESSED",
                details={
                    "service": service,
                    "kind": "coverage",
                    "elapsed_s": result.get("coverage_elapsed_s"),
                    "size_compressed": result.get("coverage_size_compressed"),
                },
            )
        else:
            error = result.get("coverage_error") or result.get("error") or "unknown"
            failures.append((service, f"coverage: {error}"))
            log_event(
                "SERVICE_FAILED",
                severity="ERROR",
                details={
                    "service": service,
                    "kind": "coverage",
                    "elapsed_s": result.get("coverage_elapsed_s"),
                    "error": str(error),
                },
            )
            logger.error("coverage rollup failed for service=%s: %s", service, error)

    # Non-zero exit if every service failed (cron fires next minute, idempotent
    # retry; no need to crash the job for partial successes).
    return 0 if successes > 0 else 1


def main() -> int:
    parser = argparse.ArgumentParser(
        description="Offline data-status rollup worker — see plan: data_status_offline_rollup_2026_05_06"
    )
    parser.add_argument("--project", required=True, help="GCP project ID")
    parser.add_argument(
        "--bucket",
        required=True,
        help="GCS bucket for rollup output (e.g. {project}-data-status-rollups)",
    )
    parser.add_argument(
        "--services",
        nargs="*",
        default=list(_DEFAULT_SERVICES),
        help="Services to roll up (default: all DataStatusService-tracked)",
    )
    args = parser.parse_args()

    # RuntimeError = already initialised by an outer bootstrap — acceptable.
    with contextlib.suppress(RuntimeError):
        _sink = GcsEventSink(
            project_id=args.project,
            bucket=f"{args.project}-events",
            service_name="data-status-rollup-worker",
        )
        setup_events(service_name="data-status-rollup-worker", mode="batch", sink=_sink)

    services: list[str] = list(args.services)  # pyright: ignore[reportAny]

    with run_lifecycle(
        service_name="data-status-rollup-worker",
        details={"project_id": args.project, "bucket": args.bucket},
    ):
        return run_rollup(args.project, args.bucket, services)


if __name__ == "__main__":
    sys.exit(main())

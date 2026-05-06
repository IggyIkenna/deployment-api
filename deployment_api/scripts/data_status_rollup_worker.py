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
    ``unified-trading-pm/plans/active/data_status_offline_rollup_2026_05_06.plan.md``

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
        --project=central-element-323112 \\
        --bucket=central-element-323112-data-status-rollups
"""

from __future__ import annotations

import argparse
import asyncio
import datetime as _dt
import gzip
import io
import json
import logging
import sys
import time
from typing import Any

from unified_trading_library.cloud_interface import get_storage_client
from unified_trading_library.event_sink import GcsEventSink
from unified_trading_library.events import log_event, setup_events

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
    "ml-training-service",
    "ml-inference-service",
    "strategy-service",
    "execution-service",
)

# Compute starts from this fixed date — covers every service's launch.
_ROLLUP_START_DATE = "2018-01-01"


def _today_iso() -> str:
    """Return today's UTC date as ``YYYY-MM-DD``."""
    return _dt.datetime.now(_dt.UTC).date().isoformat()


def _rollup_blob_path(service: str) -> str:
    """Canonical GCS object path for a service's rollup blob."""
    return f"{service}/full.json.gz"


def _build_one_service_rollup(
    dss: DataStatusService, service: str, end_date: str
) -> dict[str, Any]:
    """Synchronously call :meth:`DataStatusService.get_manifest_status`
    for the full range and return the dict.

    Raises whatever the underlying call raises — caller decides whether
    to record_failed or record_captured (per CLAUDE.md "honest absence
    vs fake placeholders" — a partial / errored rollup is worse than no
    rollup, since the slicer would silently slice garbage).
    """
    return asyncio.run(
        dss.get_manifest_status(
            service=service,
            start_date=_ROLLUP_START_DATE,
            end_date=end_date,
            asset_groups=None,  # all asset_groups — endpoint filters at slice time
        )
    )


def _write_rollup_to_gcs(
    storage_client: object, bucket: str, service: str, payload: dict[str, Any]
) -> dict[str, int]:
    """Gzip + upload the JSON payload via the unified cloud-interface API.

    Uses ``upload_bytes(bucket, blob_path, data, content_type, metadata)``
    on the storage client (UTL ``cloud_interface``). The wrapper hides the
    raw google-cloud-storage Blob.upload_from_string() — production code
    must go through this abstract layer per workspace SDK rules.
    """
    raw = json.dumps(payload, default=str).encode("utf-8")
    buf = io.BytesIO()
    with gzip.GzipFile(fileobj=buf, mode="wb", compresslevel=6) as gz:
        gz.write(raw)
    compressed = buf.getvalue()

    storage_client.upload_bytes(  # pyright: ignore[reportAttributeAccessIssue]
        bucket=bucket,
        blob_path=_rollup_blob_path(service),
        data=compressed,
        content_type="application/json",
        metadata={"content-encoding": "gzip"},
    )
    return {"size_compressed": len(compressed), "size_uncompressed": len(raw)}


def run_rollup(project_id: str, bucket: str, services: list[str]) -> int:
    """Compute and upload one rollup per service. Returns process exit code."""
    # Production observability per CLAUDE.md "no fire-and-forget" rule —
    # write structured lifecycle events to ``gs://{pid}-events/...`` where
    # ``unified-events-interface`` UI ingests them. Schema:
    # ``events/{service}/{YYYY-MM-DD}/{instance}/hour={H}/*.jsonl``.
    try:
        setup_events(
            service_name="data-status-rollup-worker",
            mode="batch",
            sink=GcsEventSink(
                project_id=project_id,
                bucket=f"{project_id}-events",
                service_name="data-status-rollup-worker",
            ),
        )
    except RuntimeError:
        # Already initialised by an outer bootstrap — acceptable.
        pass
    log_event("STARTED", details={"project_id": project_id, "bucket": bucket, "services": services})

    end_date = _today_iso()
    storage_client = get_storage_client(project_id=project_id)
    dss = DataStatusService()

    successes = 0
    failures: list[tuple[str, str]] = []
    for service in services:
        t0 = time.monotonic()
        try:
            payload = _build_one_service_rollup(dss, service, end_date)
            metrics = _write_rollup_to_gcs(storage_client, bucket, service, payload)
            elapsed = time.monotonic() - t0
            log_event(
                "SERVICE_PROCESSED",
                details={
                    "service": service,
                    "elapsed_s": round(elapsed, 1),
                    "size_compressed": metrics["size_compressed"],
                    "size_uncompressed": metrics["size_uncompressed"],
                    "asset_groups_n": len(payload.get("asset_groups", {})),
                },
            )
            successes += 1
        except (RuntimeError, ValueError, OSError) as e:
            elapsed = time.monotonic() - t0
            failures.append((service, str(e)))
            log_event(
                "SERVICE_FAILED",
                severity="ERROR",
                details={"service": service, "elapsed_s": round(elapsed, 1), "error": str(e)},
            )
            logger.exception("rollup failed for service=%s", service)

    log_event(
        "STOPPED" if not failures else "FAILED",
        details={
            "successes": successes,
            "failures": len(failures),
            "failed_services": [s for s, _ in failures],
        },
    )
    # Non-zero exit if every service failed (cron fires next minute, idempotent
    # retry; no need to crash the job for partial successes).
    return 0 if successes > 0 else 1


def main() -> int:
    parser = argparse.ArgumentParser(
        description="Offline data-status rollup worker — see plan: "
        "data_status_offline_rollup_2026_05_06"
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
    return run_rollup(args.project, args.bucket, list(args.services))


if __name__ == "__main__":
    sys.exit(main())

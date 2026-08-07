# Epic: monitoring_control_plane_master_2026_06_10
# Lifecycle: permanent
"""Offline cost-snapshot worker — Cloud Run Job entrypoint.

Runs every ~12 h via Cloud Scheduler. For each cloud (GCP / AWS / GitHub) it scans the
billing export ONCE over the full available window (~90 days), normalizes to ``CostRecord``
via the existing provider adapters, and writes the aggregated fact set as a per-cloud parquet
under a prefix in deployment-api's EXISTING state bucket (no dedicated cost bucket)::

    gs://unified-deployment-state-{project}/cost-snapshots/{cloud}.parquet

The deployment-api ``/api/costs/*`` endpoints download these small (~20-40 MB) parquets to
local ``/tmp`` and answer every view with a DuckDB SQL aggregation — no per-request BigQuery/
Athena scan (bounded to ~2 scans/day/cloud regardless of UI traffic) and no ~2 GB Python
materialization of the raw fact rows.

Mirrors the data-status rollup worker
(``deployment_api/scripts/data_status_rollup_worker.py``): same image, same
worker→GCS-snapshot shape, per-cloud isolation so one cloud's failure (e.g. AWS pending the
deployed-perms fix) never blocks another cloud's snapshot.

Plan:
    ``unified-trading-pm/plans/active/deployment_api_cache_oom_and_ui_latency_remediation_2026_07_13.md`` § 4b
SSOT:
    ``codex/05-infrastructure/billing-cost-observability.md``

CLI (``--bucket`` optional; defaults to the state bucket)::

    python -m deployment_api.scripts.cost_snapshot_worker --project=<gcp-project-id>
"""

from __future__ import annotations

import argparse
import contextlib
import datetime as _dt
import logging
import sys
import time
from typing import cast

from unified_trading_library import GcsEventSink, get_storage_client, log_event, run_lifecycle, setup_events

from deployment_api.deployment_api_config import DeploymentApiConfig
from deployment_api.services.cost_observability.models import CLOUD_AWS, CLOUD_GCP, CLOUD_GITHUB, CostRecord
from deployment_api.services.cost_observability.providers import aws_facts, gcp_facts, github_facts
from deployment_api.services.cost_observability.snapshot import (
    SNAPSHOT_CLOUDS,
    records_to_parquet_bytes,
    snapshot_blob_path,
)

logger = logging.getLogger(__name__)

# How far back to scan. The billing export effectively holds ~90 days (SSOT § 4b: 90-day and
# 180-day queries return the same ~168 K rows), so a comfortably wider window captures the whole
# available history plus any prior-period rows for the summary delta. Bounded → cheap BQ scan.
DEFAULT_LOOKBACK_DAYS = 400

# IAM/STS error indicators — strings whose presence in an exception message signals a
# cross-cloud authentication failure (AWS AssumeRole / GCP impersonation denied).
# When detected, the emitted event is CLOUD_AUTH_FAILED (registered AlertCode, HIGH severity,
# pages PagerDuty) instead of the generic SERVICE_ERROR. Per-cloud isolation keeps the Cloud
# Run Job exit code green when other clouds succeed, so these would otherwise go undetected.
_AUTH_ERROR_INDICATORS: tuple[str, ...] = (
    "AccessDenied",
    "UnauthorizedOperation",
    "AssumeRole",
    "PERMISSION_DENIED",
    "403 Forbidden",
    "not authorized to perform",
    "credentials could not",
    "insufficient authentication",
)


def _is_auth_error(error: str) -> bool:
    """Return True when ``error`` indicates an IAM/STS authentication failure."""
    err_lower = error.lower()
    return any(ind.lower() in err_lower for ind in _AUTH_ERROR_INDICATORS)


# GCP billing lags ~2 days; the exact cutoff only mattered for the is_provisional flag, which the
# snapshot does NOT store (recomputed at read time). Kept for the adapter signature.
_PROVISIONAL_TRAILING_DAYS = 2


def _window(days: int) -> tuple[_dt.date, _dt.date]:
    """Full snapshot window: ``[today-days, today+1)`` (end exclusive, includes today partial)."""
    end = _dt.datetime.now(_dt.UTC).date() + _dt.timedelta(days=1)
    return end - _dt.timedelta(days=days), end


def _load_cloud(cloud: str, cfg: DeploymentApiConfig, start: _dt.date, end: _dt.date) -> list[CostRecord]:
    """Run one cloud's provider adapter over the full window → normalized records."""
    cutoff = _dt.datetime.now(_dt.UTC).date() - _dt.timedelta(days=_PROVISIONAL_TRAILING_DAYS - 1)
    if cloud == CLOUD_GCP:
        project = cfg.require_gcp_project_id()
        table = f"{project}.{cfg.gcp_billing_dataset}.{cfg.gcp_billing_resource_table}"
        return gcp_facts(table, start, end, cutoff)
    if cloud == CLOUD_AWS:
        return aws_facts(
            cfg.aws_cur_database,
            cfg.aws_cur_table,
            cfg.aws_cur_region,
            cfg.aws_athena_output_bucket,
            start,
            end,
            cutoff,
            cfg.aws_athena_reader_role_arn,
        )
    if cloud == CLOUD_GITHUB:
        return github_facts(start, end)
    raise ValueError(f"unknown cloud {cloud!r}")


def _snapshot_one_cloud(
    cloud: str, cfg: DeploymentApiConfig, storage_client: object, bucket: str, start: _dt.date, end: _dt.date
) -> dict[str, object]:
    """Scan + normalize + upload one cloud's snapshot. Never raises — returns a result dict.

    Per-cloud isolation: a failure here (AWS perms, a BQ hiccup) is logged + reported and does
    NOT prevent the other clouds' snapshots from being written this cycle.
    """
    t0 = time.monotonic()
    result: dict[str, object] = {"cloud": cloud, "ok": False}
    try:
        records = _load_cloud(cloud, cfg, start, end)
        data = records_to_parquet_bytes(records)
        storage_client.upload_bytes(  # pyright: ignore[reportAttributeAccessIssue,reportUnknownMemberType]
            bucket=bucket,
            blob_path=snapshot_blob_path(cloud),
            data=data,
            content_type="application/octet-stream",
        )
        result.update(
            ok=True,
            rows=len(records),
            bytes=len(data),
            elapsed_s=round(time.monotonic() - t0, 1),
        )
    except Exception as exc:  # per-cloud isolation — one cloud's failure never blocks the others
        result.update(error=str(exc), elapsed_s=round(time.monotonic() - t0, 1))
    return result


def run_snapshot(project_id: str, bucket: str, clouds: list[str], days: int) -> int:
    """Compute + upload one parquet snapshot per cloud. Returns a process exit code.

    Non-zero only if EVERY cloud failed (the scheduler retries next cycle; a partial success is
    fine — the present clouds render, the absent one degrades honestly in the UI).
    """
    start, end = _window(days)
    cfg = DeploymentApiConfig()
    storage_client = get_storage_client(project_id=project_id)

    successes = 0
    for cloud in clouds:
        result = _snapshot_one_cloud(cloud, cfg, storage_client, bucket, start, end)
        if result.get("ok"):
            successes += 1
            log_event(
                "SERVICE_PROCESSED",
                details={
                    "cloud": cloud,
                    "rows": result.get("rows"),
                    "bytes": result.get("bytes"),
                    "elapsed_s": result.get("elapsed_s"),
                },
            )
            logger.info(
                "cost snapshot %s: %s rows, %s bytes in %ss",
                cloud,
                result.get("rows"),
                result.get("bytes"),
                result.get("elapsed_s"),
            )
        else:
            error_str = str(result.get("error", ""))  # noqa: qg-empty-fallback — _snapshot_one_cloud always sets error=str(exc); "" only if somehow missing
            event_code = "CLOUD_AUTH_FAILED" if _is_auth_error(error_str) else "SERVICE_ERROR"
            log_event(
                event_code,
                severity="ERROR",
                details={
                    "cloud": cloud,
                    "error": error_str,
                    "elapsed_s": result.get("elapsed_s"),
                    "operation": "cost_snapshot",
                },
            )
            logger.error("cost snapshot %s failed [%s]: %s", cloud, event_code, error_str)
    return 0 if successes > 0 else 1


def main() -> int:
    parser = argparse.ArgumentParser(
        description="Offline cost-snapshot worker — plan: deployment_api_cache_oom_and_ui_latency_remediation"
    )
    parser.add_argument("--project", required=True, help="GCP project ID")
    parser.add_argument(
        "--bucket",
        default=None,
        help="GCS bucket for snapshot output (default: deployment-api's state bucket, "
        "config.effective_state_bucket = unified-deployment-state-{project}); blobs land under cost-snapshots/",
    )
    parser.add_argument(
        "--clouds",
        nargs="*",
        default=list(SNAPSHOT_CLOUDS),
        help=f"Clouds to snapshot (default: {', '.join(SNAPSHOT_CLOUDS)})",
    )
    parser.add_argument(
        "--days",
        type=int,
        default=DEFAULT_LOOKBACK_DAYS,
        help=f"Lookback window in days (default: {DEFAULT_LOOKBACK_DAYS}; export holds ~90)",
    )
    args = parser.parse_args()
    clouds = cast("list[str]", args.clouds)
    project = cast("str", args.project)
    days = cast("int", args.days)
    # Default to the existing deployment-api state bucket (no dedicated cost bucket) — blobs land
    # under the cost-snapshots/ prefix (snapshot.snapshot_blob_path).
    bucket = cast("str | None", args.bucket) or DeploymentApiConfig().effective_state_bucket

    # RuntimeError = already initialised by an outer bootstrap — acceptable.
    with contextlib.suppress(RuntimeError):
        _sink = GcsEventSink(
            project_id=project,
            bucket=f"{project}-events",
            service_name="cost-snapshot-worker",
        )
        setup_events(service_name="cost-snapshot-worker", mode="batch", sink=_sink)

    with run_lifecycle(
        service_name="cost-snapshot-worker",
        details={"project_id": project, "bucket": bucket, "clouds": clouds},
    ):
        return run_snapshot(project, bucket, clouds, days)


if __name__ == "__main__":
    sys.exit(main())

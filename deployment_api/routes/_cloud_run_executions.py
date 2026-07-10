# Epic: observability_master
# Lifecycle: permanent
"""Latest-execution status for the classified GCP Cloud Run jobs.

The unified deployment inventory (``GET /api/deployments/inventory``) needs the
**live** status of every Cloud Run job, not just the static
``CLOUD_RUN_JOBS`` classification. This helper lists the latest execution of each
job through the GCP Cloud Run Admin API and maps it to the inventory wire shape.

Cloud-agnostic boundary: the GCP SDK is reached ONLY through deployment-service's
``backends._gcp_sdk`` lazy-import boundary (``run_v2.ExecutionsClient``), the same
client ``backends/gcp.py`` already uses — never an inline ``from google.cloud
import run_v2`` here (CLAUDE.md cloud-SDK-direct ban; ``_gcp_sdk`` is the one
sanctioned GCP-SDK seam). deployment-service is the sanctioned editable path dep.

Honest degradation: a Cloud Run list failure (creds / API down / region) is logged
and yields an empty status map so the inventory degrades to the static
classification with ``status="unknown"`` — never a crash, never a fabricated status.

SSOT: ``plans/active/deployment_observability_parity_live_batch_paper_2026_06_22.md``
Phase 1.
"""

from __future__ import annotations

import logging
from concurrent.futures import ThreadPoolExecutor
from dataclasses import dataclass
from datetime import UTC, datetime

logger = logging.getLogger(__name__)

# Cloud Run jobs live alongside the rest of the GCP estate in asia-northeast1
# (CLAUDE.md § VM launchers — all GCS data is in asia-northeast1).
DEFAULT_CLOUD_RUN_REGION = "asia-northeast1"

# Per-RPC deadline for the Cloud Run list calls. Kept below the inventory route's per-provider
# census wall-clock (_PROVIDER_CENSUS_TIMEOUT_SEC, 45 s) so a wedged control-plane RPC unwinds
# the census worker thread on its OWN instead of leaking it — DeadlineExceeded is caught below
# and degrades to the static classification. Prevents the inventory census pool from starving
# under a persistent hang.
_RPC_TIMEOUT_SEC = 30.0

# Per-job "latest execution" is an N+1 (one ListExecutions RPC per job). At ~70 jobs the serial
# loop routinely blew past the 45 s census wall-clock and degraded the whole Cloud Run jobs census
# to empty (jobs flickering out of the cockpit). Fan the per-job lookups out concurrently so the
# census is ~max(single RPC) instead of their sum. GCS/gRPC releases the GIL → true I/O parallelism.
_EXECUTION_LOOKUP_WORKERS = 16


@dataclass(frozen=True)
class CloudRunExecutionStatus:
    """Latest-execution status for one Cloud Run job (the inventory enrichment).

    Attributes:
        job_name: The short Cloud Run job name (last path segment), e.g.
            ``prd-manifest-consolidator-cefi``. Matched against a registry job's
            stem by suffix so an env-prefixed/asset-group-suffixed live name binds
            to its classified ``DeploymentTarget``.
        status: One of ``running`` / ``succeeded`` / ``failed`` / ``pending`` /
            ``unknown`` — the inventory wire status literal.
        last_run_at: ISO-8601 completion (or start) time of the latest execution,
            or ``None`` when never run.
        exit_code: ``0`` for a succeeded execution, ``1`` for a failed one, or
            ``None`` when running / pending / never run (Cloud Run executions carry
            counts, not a process rc — we synthesise 0/1 from succeeded/failed).
        log_uri: The execution's Cloud Logging URI, or ``""`` when absent.
        region: The Cloud Run region the job lives in (the multi-region census sets
            this so a job row shows its region + a resource outside the configured
            region set is flagged). ``""`` when unknown (a legacy single-region call).
    """

    job_name: str
    status: str
    last_run_at: str | None
    exit_code: int | None
    log_uri: str
    region: str = ""


def _iso(value: object) -> str | None:
    """Best-effort ISO-8601 from a Cloud Run timestamp (proto datetime), else None."""
    if value is None:
        return None
    try:
        # run_v2 timestamps are python datetimes (proto-plus). Normalise to UTC ISO.
        if isinstance(value, datetime):
            dt = value if value.tzinfo is not None else value.replace(tzinfo=UTC)
            return dt.isoformat()
    except (ValueError, TypeError):
        return None
    return None


def _status_for_execution(execution: object) -> tuple[str, int | None]:
    """Map a run_v2 Execution to ``(status, exit_code)``.

    A completed execution is ``succeeded`` (exit_code 0) unless ``failed_count > 0``
    → ``failed`` (exit_code 1). A running execution (``running_count > 0``) is
    ``running``; anything else with no completion is ``pending``. Cloud Run
    executions expose counts, not a process rc, so 0/1 is the honest synthesis.
    """
    completion = getattr(execution, "completion_time", None)
    failed_count = int(getattr(execution, "failed_count", 0) or 0)
    running_count = int(getattr(execution, "running_count", 0) or 0)
    if completion:
        if failed_count > 0:
            return "failed", 1
        return "succeeded", 0
    if running_count > 0:
        return "running", None
    return "pending", None


def latest_execution_by_job(
    project_id: str,
    region: str = DEFAULT_CLOUD_RUN_REGION,
) -> dict[str, CloudRunExecutionStatus]:
    """List every Cloud Run job's LATEST execution status, keyed by short job name.

    Lists jobs (``run_v2.JobsClient``) then, per job, the most-recent execution
    (``run_v2.ExecutionsClient`` — executions return newest-first). Returns a map
    ``{short_job_name: CloudRunExecutionStatus}``.

    Honest degradation: any GCP error is logged and yields ``{}`` so the inventory
    falls back to the static classification with ``status="unknown"``.
    """
    try:
        # GCP SDK reached ONLY via the deployment-service _gcp_sdk boundary.
        from deployment_service.backends import _gcp_sdk

        run_v2 = _gcp_sdk.run_v2
        jobs_client = run_v2.JobsClient()
        executions_client = run_v2.ExecutionsClient()
        parent = f"projects/{project_id}/locations/{region}"

        # run_v2 is the untyped GCP-SDK boundary (_gcp_sdk); its pager member types are
        # partially unknown — per-execution fields are read defensively via getattr() below,
        # so the unknown pager element type is safe here.
        def _resolve(job: object) -> tuple[str, CloudRunExecutionStatus]:
            """One job's latest-execution status — run CONCURRENTLY across jobs (the N+1 fix)."""
            full_name = str(job.name)  # pyright: ignore[reportAttributeAccessIssue]
            job_name = full_name.rsplit("/", 1)[-1]
            exec_request = run_v2.ListExecutionsRequest(parent=full_name, page_size=1)  # pyright: ignore[reportUnknownMemberType, reportUnknownArgumentType]
            latest = next(
                iter(executions_client.list_executions(request=exec_request, timeout=_RPC_TIMEOUT_SEC)),  # pyright: ignore[reportUnknownMemberType, reportUnknownArgumentType]
                None,
            )
            if latest is None:
                return job_name, CloudRunExecutionStatus(
                    job_name=job_name, status="pending", last_run_at=None, exit_code=None, log_uri="", region=region
                )
            status, exit_code = _status_for_execution(latest)
            last_run_at = _iso(getattr(latest, "completion_time", None)) or _iso(getattr(latest, "start_time", None))
            return job_name, CloudRunExecutionStatus(
                job_name=job_name,
                status=status,
                last_run_at=last_run_at,
                exit_code=exit_code,
                log_uri=str(getattr(latest, "log_uri", "") or ""),
                region=region,
            )

        jobs = list(
            jobs_client.list_jobs(  # pyright: ignore[reportUnknownMemberType, reportUnknownArgumentType]
                request=run_v2.ListJobsRequest(parent=parent), timeout=_RPC_TIMEOUT_SEC
            )
        )
        if not jobs:
            return {}
        workers = min(_EXECUTION_LOOKUP_WORKERS, len(jobs))
        with ThreadPoolExecutor(max_workers=workers, thread_name_prefix="cr-exec") as pool:
            return dict(pool.map(_resolve, jobs))
    except Exception as exc:
        logger.warning("Cloud Run executions list failed (degrading to static classification): %s", exc)
        return {}


@dataclass(frozen=True)
class ExecutionRecord:
    """One historical Cloud Run job execution (the detail-popover run-history vector, WS-D #11)."""

    name: str  # short execution name
    status: str  # running / succeeded / failed / pending
    started_at: str | None
    completed_at: str | None
    duration_seconds: float | None


def list_job_executions(
    project_id: str,
    job_short_name: str,
    region: str = DEFAULT_CLOUD_RUN_REGION,
    limit: int = 10,
) -> list[ExecutionRecord]:
    """The last ``limit`` executions of ONE Cloud Run job — the detail-popover run-history (#11).

    Only the ``/{name}/detail`` path calls this (``page_size=limit``); the thin-list census stays at
    ``page_size=1`` so its cost is unchanged. Executions return newest-first. Honest degradation: any
    GCP error yields an empty list (the popover simply shows no history), never a crash.
    """
    try:
        from deployment_service.backends import (
            _gcp_sdk,  # noqa: imports-inside-functions  # deferred SDK boundary (matches latest_execution_by_job)
        )

        run_v2 = _gcp_sdk.run_v2
        executions_client = run_v2.ExecutionsClient()
        parent = f"projects/{project_id}/locations/{region}/jobs/{job_short_name}"
        request = run_v2.ListExecutionsRequest(parent=parent, page_size=limit)  # pyright: ignore[reportUnknownMemberType, reportUnknownArgumentType]
        records: list[ExecutionRecord] = []
        for execution in executions_client.list_executions(request=request, timeout=_RPC_TIMEOUT_SEC):  # pyright: ignore[reportUnknownMemberType, reportUnknownArgumentType]
            if len(records) >= limit:
                break
            status, _ = _status_for_execution(execution)
            start_dt = getattr(execution, "start_time", None)
            completion_dt = getattr(execution, "completion_time", None)
            duration: float | None = None
            if isinstance(start_dt, datetime) and isinstance(completion_dt, datetime):
                duration = max(0.0, (completion_dt - start_dt).total_seconds())
            records.append(
                ExecutionRecord(
                    name=str(getattr(execution, "name", "") or "").rsplit("/", 1)[-1],
                    status=status,
                    started_at=_iso(start_dt),
                    completed_at=_iso(completion_dt),
                    duration_seconds=duration,
                )
            )
        return records
    except Exception as exc:
        logger.warning("Cloud Run job-executions list for %s failed (degrading to empty): %s", job_short_name, exc)
        return []


__all__ = [
    "DEFAULT_CLOUD_RUN_REGION",
    "CloudRunExecutionStatus",
    "ExecutionRecord",
    "latest_execution_by_job",
    "list_job_executions",
]

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
from dataclasses import dataclass
from datetime import UTC, datetime

logger = logging.getLogger(__name__)

# Cloud Run jobs live alongside the rest of the GCP estate in asia-northeast1
# (CLAUDE.md § VM launchers — all GCS data is in asia-northeast1).
DEFAULT_CLOUD_RUN_REGION = "asia-northeast1"


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
    """

    job_name: str
    status: str
    last_run_at: str | None
    exit_code: int | None
    log_uri: str


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
        result: dict[str, CloudRunExecutionStatus] = {}
        # run_v2 is the untyped GCP-SDK boundary (_gcp_sdk); its pager member types
        # are partially unknown — the per-execution fields are read defensively via
        # getattr() below, so the unknown pager element type is safe here.
        for job in jobs_client.list_jobs(request=run_v2.ListJobsRequest(parent=parent)):  # pyright: ignore[reportUnknownMemberType, reportUnknownVariableType]
            job_name = str(job.name).rsplit("/", 1)[-1]  # pyright: ignore[reportUnknownMemberType, reportUnknownArgumentType]
            exec_request = run_v2.ListExecutionsRequest(parent=str(job.name), page_size=1)  # pyright: ignore[reportUnknownMemberType, reportUnknownArgumentType]
            latest = next(iter(executions_client.list_executions(request=exec_request)), None)  # pyright: ignore[reportUnknownMemberType, reportUnknownArgumentType]
            if latest is None:
                result[job_name] = CloudRunExecutionStatus(
                    job_name=job_name,
                    status="pending",
                    last_run_at=None,
                    exit_code=None,
                    log_uri="",
                )
                continue
            status, exit_code = _status_for_execution(latest)
            last_run_at = _iso(getattr(latest, "completion_time", None)) or _iso(getattr(latest, "start_time", None))
            result[job_name] = CloudRunExecutionStatus(
                job_name=job_name,
                status=status,
                last_run_at=last_run_at,
                exit_code=exit_code,
                log_uri=str(getattr(latest, "log_uri", "") or ""),
            )
        return result
    except Exception as exc:
        logger.warning("Cloud Run executions list failed (degrading to static classification): %s", exc)
        return {}


__all__ = [
    "DEFAULT_CLOUD_RUN_REGION",
    "CloudRunExecutionStatus",
    "latest_execution_by_job",
]

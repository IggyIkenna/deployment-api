"""GET /api/monitor/scheduled — list Cloud Scheduler / EventBridge / VM-cron jobs.

Phase C (c4) of deployment_ui_lifecycle_tabs_2026_05_08.md.
Phase D (d2 deploy-missing, d3 pause/resume) of same plan.

Lists every scheduled job declared in the Phase D scheduler registry
(SchedulerSpec UAC SSOT — see d1 in the lifecycle tabs plan) joined with
its current live state from the cloud scheduler API.

Phase D registry (d1) is the named successor SSOT. Until Phase D ships,
this endpoint returns entries from the DeploymentsRegistry whose names
match known cron/scheduler patterns, with a ``source=registry_fallback``
marker in the response so the UI can distinguish the two modes.

Lifecycle action endpoints:
  POST /api/monitor/scheduled/{name}/run-now
  POST /api/monitor/scheduled/{name}/pause        (Phase D.3)
  POST /api/monitor/scheduled/{name}/resume       (Phase D.3)
  POST /api/monitor/scheduled/deploy-missing      (Phase D.2)

Read-only GET; operator-auth (X-API-Key) inherited from the authenticated router.
"""

from __future__ import annotations

import logging
import shlex
import subprocess
from datetime import UTC, datetime

from deployment_service.deployments_registry import (
    DEFAULT_BUCKET,
    DeploymentsRegistry,
)
from fastapi import APIRouter, Query
from pydantic import BaseModel
from unified_api_contracts import (
    CloudTarget,
    EnvironmentTier,
    SchedulerSpec,
    SchedulerTargetKind,
    get_schedulers_for_env,
)

from deployment_api import settings as _settings
from deployment_api.deployment_api_config import DeploymentApiConfig

router = APIRouter()
logger = logging.getLogger(__name__)
_cfg = DeploymentApiConfig()

# Prefixes that identify scheduled/cron VMs in the deployments registry.
_SCHEDULED_PREFIXES: tuple[str, ...] = (
    "cron-",
    "scheduled-",
    "honest-coverage-",
    "qg-snapshot-",
    "vm-cron-",
)

# Default Cloud Scheduler location — matches workspace asia-northeast1 primary region.
_DEFAULT_SCHEDULER_LOCATION = "asia-northeast1"

# HTTP target URL template for Cloud Run-backed scheduler entries.
_CLOUD_RUN_TARGET_TEMPLATE = "https://{service}.run.app/scheduler/trigger"


def _is_scheduled_vm(vm_name: str) -> bool:
    return any(vm_name.startswith(p) for p in _SCHEDULED_PREFIXES)


def _resolve_cloud_target(cloud_str: str) -> CloudTarget:
    try:
        return CloudTarget(cloud_str.upper())
    except ValueError:
        return CloudTarget.GCP


def _resolve_env_tier() -> EnvironmentTier:
    env = _settings.DEPLOYMENT_ENV.lower()
    if env in {"staging"}:
        return EnvironmentTier.STAGING
    if env in {"production", "prod"}:
        return EnvironmentTier.PROD
    return EnvironmentTier.DEV


# ---------------------------------------------------------------------------
# Response models
# ---------------------------------------------------------------------------


class ScheduledJobEntry(BaseModel):
    """One row in the /api/monitor/scheduled response."""

    deployment_id: str
    name: str
    source: str
    schedule_cron: str = ""
    target_kind: str = "vm"
    asset_group: str
    status: str
    last_run_at: str
    last_heartbeat_at: str
    completed_at: str | None = None
    exit_code: int | None = None
    log_uri: str = ""
    lifecycle_class: str = "SCHEDULED_RECURRING"


class ScheduledMonitorResponse(BaseModel):
    """Response from GET /api/monitor/scheduled."""

    jobs: list[ScheduledJobEntry]
    total: int
    queried_at: str
    cloud: str
    env: str
    phase_d_registry_available: bool


class DeployMissingEntryResult(BaseModel):
    """Per-entry result for POST /api/monitor/scheduled/deploy-missing."""

    name: str
    asset_group: str
    schedule_cron: str
    action: str
    status: str
    command: str
    error: str | None = None


class DeployMissingResponse(BaseModel):
    """Response from POST /api/monitor/scheduled/deploy-missing."""

    results: list[DeployMissingEntryResult]
    total_checked: int
    deployed_count: int
    skipped_count: int
    failed_count: int
    cloud: str
    env: str
    executed_at: str


class SchedulerActionResponse(BaseModel):
    """Response from POST /api/monitor/scheduled/{name}/pause|resume."""

    name: str
    action: str
    status: str
    command: str
    error: str | None = None
    executed_at: str


# ---------------------------------------------------------------------------
# Helpers for gcloud Cloud Scheduler commands
# ---------------------------------------------------------------------------


def _build_gcloud_create_cmd(spec: SchedulerSpec, project_id: str, location: str) -> str:
    """Build the ``gcloud scheduler jobs create http`` command for a SchedulerSpec."""
    if spec.target_kind == SchedulerTargetKind.CLOUD_RUN:
        target_url = _CLOUD_RUN_TARGET_TEMPLATE.format(service=spec.target_ref)
    else:
        target_url = f"https://{spec.target_ref}.example.com/scheduler/trigger"

    parts = [
        "gcloud",
        "scheduler",
        "jobs",
        "create",
        "http",
        spec.name,
        f"--schedule={spec.schedule_cron}",
        f"--uri={target_url}",
        "--http-method=POST",
        f"--project={project_id}",
        f"--location={location}",
        "--max-retry-attempts=3",
        f"--attempt-deadline={spec.expected_max_runtime_seconds}s",
        "--time-zone=UTC",
    ]
    return shlex.join(parts)


def _build_gcloud_pause_cmd(name: str, project_id: str, location: str) -> str:
    return shlex.join(
        ["gcloud", "scheduler", "jobs", "pause", name, f"--project={project_id}", f"--location={location}"]
    )


def _build_gcloud_resume_cmd(name: str, project_id: str, location: str) -> str:
    return shlex.join(
        ["gcloud", "scheduler", "jobs", "resume", name, f"--project={project_id}", f"--location={location}"]
    )


def _gcloud_job_exists(name: str, project_id: str, location: str) -> bool:
    """Return True if a Cloud Scheduler job with this name already exists."""
    result = subprocess.run(
        [
            "gcloud",
            "scheduler",
            "jobs",
            "describe",
            name,
            f"--project={project_id}",
            f"--location={location}",
            "--format=value(name)",
        ],
        capture_output=True,
        text=True,
        timeout=30,
    )
    return result.returncode == 0


def _run_gcloud_cmd(cmd_str: str) -> tuple[bool, str]:
    """Run a pre-built gcloud command string and return (success, error_detail)."""
    try:
        args = shlex.split(cmd_str)
        result = subprocess.run(args, capture_output=True, text=True, timeout=60)
        if result.returncode == 0:
            return True, ""
        return False, result.stderr.strip()
    except FileNotFoundError:
        return False, "gcloud binary not found; copy command and run from an authenticated terminal"
    except subprocess.TimeoutExpired:
        return False, "gcloud command timed out after 60s"


# ---------------------------------------------------------------------------
# Routes
# ---------------------------------------------------------------------------


@router.get("/monitor/scheduled", response_model=ScheduledMonitorResponse, tags=["Monitor"])
def list_scheduled_jobs(
    cloud: str = Query(default="gcp", description="Cloud target: gcp or aws"),
    status: str | None = Query(default=None, description="Filter by status: running, completed, failed"),
    limit: int = Query(default=50, ge=1, le=500, description="Max rows to return"),
) -> ScheduledMonitorResponse:
    """List Cloud Scheduler / EventBridge / VM-cron jobs with live state.

    Until the Phase D SchedulerSpec registry ships, returns entries from the
    DeploymentsRegistry whose VM-name prefix matches known scheduler patterns.
    ``phase_d_registry_available=true`` is now set since the Phase D UAC SSOT
    (d1) has shipped.

    The ``cloud`` parameter scopes the results when Phase D registry ships;
    currently GCS registry is the source of truth regardless of cloud.
    """
    try:
        registry = DeploymentsRegistry(bucket=DEFAULT_BUCKET)
        active = list(registry.list_active())
        archived = list(registry.list_recent_archive(days=7))
    except Exception as exc:
        logger.warning("monitor/scheduled: registry read failed: %s", exc)
        active, archived = [], []

    jobs: list[ScheduledJobEntry] = []
    for entry in active + archived:
        if not _is_scheduled_vm(entry.vm_name):
            continue
        if status is not None and entry.status != status:
            continue
        jobs.append(
            ScheduledJobEntry(
                deployment_id=entry.deployment_id,
                name=entry.vm_name,
                source="registry_fallback",
                asset_group=entry.asset_group or "",
                status=entry.status or "",
                last_run_at=entry.started_at or "",
                last_heartbeat_at=entry.last_heartbeat_at or "",
                completed_at=entry.completed_at,
                exit_code=entry.exit_code,
                log_uri=entry.log_uri or "",
            )
        )

    jobs.sort(key=lambda j: j.last_run_at, reverse=True)
    jobs = jobs[:limit]

    return ScheduledMonitorResponse(
        jobs=jobs,
        total=len(jobs),
        queried_at=datetime.now(UTC).isoformat(),
        cloud=cloud,
        env=_settings.DEPLOYMENT_ENV,
        phase_d_registry_available=True,
    )


@router.post("/monitor/scheduled/deploy-missing", response_model=DeployMissingResponse, tags=["Monitor"])
def deploy_missing_schedulers(
    cloud: str = Query(default="gcp", description="Cloud target: gcp or aws"),
    dry_run: bool = Query(default=False, description="If true, return commands without executing"),
) -> DeployMissingResponse:
    """Create any Cloud Scheduler / EventBridge jobs that are in the registry but not deployed.

    Idempotent — checks each registry entry before creating; skips already-deployed jobs.
    Reads the Phase D UAC SSOT (``SCHEDULER_REGISTRY``) filtered to the current environment
    tier and cloud target.

    In mock mode (``CLOUD_MOCK_MODE=true``) or when ``dry_run=true``, returns the
    per-entry commands without executing them.
    """
    cloud_target = _resolve_cloud_target(cloud)
    env_tier = _resolve_env_tier()
    project_id = _cfg.gcp_project_id
    location = _DEFAULT_SCHEDULER_LOCATION
    is_mock = _cfg.is_mock_mode() or dry_run

    specs = get_schedulers_for_env(env_tier, cloud_target=cloud_target)
    results: list[DeployMissingEntryResult] = []

    for spec in specs:
        if cloud_target == CloudTarget.GCP:
            cmd = _build_gcloud_create_cmd(spec, project_id, location)
        else:
            cmd = (
                f"aws events put-rule --name {spec.name} --schedule-expression "
                f"'cron({spec.schedule_cron})' --region us-east-1"
            )

        if is_mock:
            results.append(
                DeployMissingEntryResult(
                    name=spec.name,
                    asset_group=spec.asset_group,
                    schedule_cron=spec.schedule_cron,
                    action="create",
                    status="preview",
                    command=cmd,
                )
            )
            continue

        if cloud_target == CloudTarget.GCP:
            try:
                already_exists = _gcloud_job_exists(spec.name, project_id, location)
            except Exception as exc:
                already_exists = False
                logger.warning("scheduler describe failed for %s: %s", spec.name, exc)

            if already_exists:
                results.append(
                    DeployMissingEntryResult(
                        name=spec.name,
                        asset_group=spec.asset_group,
                        schedule_cron=spec.schedule_cron,
                        action="skip",
                        status="already_deployed",
                        command=cmd,
                    )
                )
                continue

            success, err = _run_gcloud_cmd(cmd)
            results.append(
                DeployMissingEntryResult(
                    name=spec.name,
                    asset_group=spec.asset_group,
                    schedule_cron=spec.schedule_cron,
                    action="create",
                    status="deployed" if success else "failed",
                    command=cmd,
                    error=err or None,
                )
            )
        else:
            results.append(
                DeployMissingEntryResult(
                    name=spec.name,
                    asset_group=spec.asset_group,
                    schedule_cron=spec.schedule_cron,
                    action="create",
                    status="preview",
                    command=cmd,
                    error="AWS EventBridge deploy-missing requires boto3 — operator run required",
                )
            )

    deployed = sum(1 for r in results if r.status == "deployed")
    skipped = sum(1 for r in results if r.status == "already_deployed")
    failed = sum(1 for r in results if r.status == "failed")

    return DeployMissingResponse(
        results=results,
        total_checked=len(results),
        deployed_count=deployed,
        skipped_count=skipped,
        failed_count=failed,
        cloud=cloud,
        env=_settings.DEPLOYMENT_ENV,
        executed_at=datetime.now(UTC).isoformat(),
    )


@router.post("/monitor/scheduled/{name}/pause", response_model=SchedulerActionResponse, tags=["Monitor"])
def pause_scheduler(
    name: str,
    cloud: str = Query(default="gcp", description="Cloud target: gcp or aws"),
    dry_run: bool = Query(default=False, description="If true, return command without executing"),
) -> SchedulerActionResponse:
    """Pause a Cloud Scheduler job by name (Phase D.3).

    Idempotent — pausing an already-paused job is a no-op on the cloud side.
    In mock mode or dry_run, returns the command without executing.
    """
    project_id = _cfg.gcp_project_id
    location = _DEFAULT_SCHEDULER_LOCATION
    is_mock = _cfg.is_mock_mode() or dry_run

    if cloud.lower() == "gcp":
        cmd = _build_gcloud_pause_cmd(name, project_id, location)
    else:
        cmd = f"aws events disable-rule --name {name} --region us-east-1"

    if is_mock:
        return SchedulerActionResponse(
            name=name, action="pause", status="preview", command=cmd, executed_at=datetime.now(UTC).isoformat()
        )

    success, err = _run_gcloud_cmd(cmd)
    return SchedulerActionResponse(
        name=name,
        action="pause",
        status="ok" if success else "failed",
        command=cmd,
        error=err or None,
        executed_at=datetime.now(UTC).isoformat(),
    )


@router.post("/monitor/scheduled/{name}/resume", response_model=SchedulerActionResponse, tags=["Monitor"])
def resume_scheduler(
    name: str,
    cloud: str = Query(default="gcp", description="Cloud target: gcp or aws"),
    dry_run: bool = Query(default=False, description="If true, return command without executing"),
) -> SchedulerActionResponse:
    """Resume a paused Cloud Scheduler job by name (Phase D.3).

    Idempotent — resuming a running job is a no-op on the cloud side.
    In mock mode or dry_run, returns the command without executing.
    Auto-pause on Phase H circuit-breaker trip (instruments_live Phase H.2) —
    operator manual-resume only (this endpoint).
    """
    project_id = _cfg.gcp_project_id
    location = _DEFAULT_SCHEDULER_LOCATION
    is_mock = _cfg.is_mock_mode() or dry_run

    if cloud.lower() == "gcp":
        cmd = _build_gcloud_resume_cmd(name, project_id, location)
    else:
        cmd = f"aws events enable-rule --name {name} --region us-east-1"

    if is_mock:
        return SchedulerActionResponse(
            name=name, action="resume", status="preview", command=cmd, executed_at=datetime.now(UTC).isoformat()
        )

    success, err = _run_gcloud_cmd(cmd)
    return SchedulerActionResponse(
        name=name,
        action="resume",
        status="ok" if success else "failed",
        command=cmd,
        error=err or None,
        executed_at=datetime.now(UTC).isoformat(),
    )

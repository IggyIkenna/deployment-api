"""GET /api/monitor/live — list running + recent LONG_LIVED_LIVE strategy clusters.

Phase C (c3) of deployment_ui_lifecycle_tabs_2026_05_08.md.
Phase E.2 — lifecycle actions (start/stop/pause/restart/drain).

Lists every paper-trade and live-trade strategy VM (strategy-paper-*, strategy-live-*,
defi-recursive-*) from the deployments registry. These are long-lived deployments that
persist across days, unlike EPHEMERAL_BATCH or EPHEMERAL_EXPERIMENT jobs.

Phase E.2 lifecycle action endpoints per cloud-target:
  POST /api/monitor/live/{name}/start
  POST /api/monitor/live/{name}/stop
  POST /api/monitor/live/{name}/pause
  POST /api/monitor/live/{name}/restart
  POST /api/monitor/live/{name}/drain

Cloud Run: scale min-instances for start/stop/pause; redeploy for restart;
traffic-split to 0 for drain. GKE/EKS: kubectl scale + rollout restart.
ECS: update-service desired-count. In mock/dry_run mode returns commands
without executing.

Read-only GET; operator-auth (X-API-Key) inherited from the authenticated router.
"""

from __future__ import annotations

import logging
import shlex
import subprocess
from datetime import UTC, datetime

from fastapi import APIRouter, HTTPException, Query
from pydantic import BaseModel
from unified_api_contracts import (
    LIVE_CLUSTER_REGISTRY,
    CloudTarget,
    EnvironmentTier,
    LiveClusterDeploymentKind,
    LiveClusterSpec,
)
from unified_trading_library import (
    DEFAULT_BUCKET,
    DeploymentsRegistry,
)

from deployment_api import settings as _settings
from deployment_api.deployment_api_config import DeploymentApiConfig
from deployment_api.registry_reader import resolve_active_registry

_cfg = DeploymentApiConfig()

_DEFAULT_CLOUD_RUN_REGION = "asia-northeast1"
_DEFAULT_GKE_NAMESPACE = "default"
_DEFAULT_ECS_CLUSTER = "live-cluster"
_DEFAULT_ECS_REGION = "us-east-1"

router = APIRouter()
logger = logging.getLogger(__name__)

# LONG_LIVED_LIVE VM-name prefixes.
# Kept in sync with VM_PREFIX_TO_BUCKET in vm_zombie_watchdog.py.
_LIVE_PREFIXES: tuple[str, ...] = (
    "strategy-paper-",
    "strategy-live-",
    "defi-recursive-",
)


def _is_live_vm(vm_name: str) -> bool:
    """Return True when vm_name is a LONG_LIVED_LIVE strategy deployment."""
    return any(vm_name.startswith(p) for p in _LIVE_PREFIXES)


def _infer_deployment_kind(vm_name: str) -> str:
    if vm_name.startswith("strategy-paper-"):
        return "paper_trade"
    if vm_name.startswith("strategy-live-"):
        return "live_trade"
    if vm_name.startswith("defi-recursive-"):
        return "defi_recursive_borrow"
    return "unknown"


class LiveClusterEntry(BaseModel):  # CORRECT-LOCAL: deployment-ui response DTO
    """One row in the /api/monitor/live response."""

    deployment_id: str
    vm_name: str
    deployment_kind: str
    asset_group: str
    task: str
    status: str
    started_at: str
    last_heartbeat_at: str
    completed_at: str | None = None
    exit_code: int | None = None
    log_uri: str = ""
    lifecycle_class: str = "LONG_LIVED_LIVE"


class LiveMonitorResponse(BaseModel):  # CORRECT-LOCAL: deployment-ui response DTO
    """Response from GET /api/monitor/live."""

    clusters: list[LiveClusterEntry]
    total: int
    queried_at: str
    cloud: str
    env: str


class LiveActionResponse(BaseModel):  # CORRECT-LOCAL: deployment-ui response DTO
    """Response from POST /api/monitor/live/{name}/{action} — Phase E.2."""

    name: str
    action: str
    status: str
    command: str
    error: str | None = None
    executed_at: str


# ---------------------------------------------------------------------------
# Phase E.2 helpers
# ---------------------------------------------------------------------------


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


def _lookup_cluster_spec(name: str, env_tier: EnvironmentTier, cloud_target: CloudTarget) -> LiveClusterSpec:
    for spec in LIVE_CLUSTER_REGISTRY:
        if spec.name == name and spec.environment_tier == env_tier and spec.cloud_target == cloud_target:
            return spec
    raise HTTPException(
        status_code=404,
        detail=f"Live cluster '{name}' not in registry for env={env_tier} cloud={cloud_target}",
    )


def _cloud_run_action_cmd(spec: LiveClusterSpec, action: str, project_id: str) -> str:
    region = _DEFAULT_CLOUD_RUN_REGION
    if action in {"stop", "pause"}:
        return shlex.join(
            [
                "gcloud",
                "run",
                "services",
                "update",
                spec.target_ref,
                "--min-instances=0",
                f"--project={project_id}",
                f"--region={region}",
            ]
        )
    if action == "start":
        return shlex.join(
            [
                "gcloud",
                "run",
                "services",
                "update",
                spec.target_ref,
                f"--min-instances={spec.expected_replicas}",
                f"--project={project_id}",
                f"--region={region}",
            ]
        )
    if action == "restart":
        return shlex.join(
            [
                "gcloud",
                "run",
                "services",
                "update",
                spec.target_ref,
                "--no-traffic",
                f"--project={project_id}",
                f"--region={region}",
            ]
        )
    return shlex.join(
        [
            "gcloud",
            "run",
            "services",
            "update-traffic",
            spec.target_ref,
            "--to-revisions=LATEST=0",
            f"--project={project_id}",
            f"--region={region}",
        ]
    )


def _kubectl_action_cmd(spec: LiveClusterSpec, action: str) -> str:
    ns = _DEFAULT_GKE_NAMESPACE
    if action in {"stop", "pause", "drain"}:
        return shlex.join(["kubectl", "scale", f"deployment/{spec.target_ref}", "--replicas=0", "-n", ns])
    if action == "start":
        return shlex.join(
            ["kubectl", "scale", f"deployment/{spec.target_ref}", f"--replicas={spec.expected_replicas}", "-n", ns]
        )
    return shlex.join(["kubectl", "rollout", "restart", f"deployment/{spec.target_ref}", "-n", ns])


def _ecs_action_cmd(spec: LiveClusterSpec, action: str) -> str:
    region = _DEFAULT_ECS_REGION
    if action == "restart":
        stop = shlex.join(
            [
                "aws",
                "ecs",
                "update-service",
                "--cluster",
                _DEFAULT_ECS_CLUSTER,
                "--service",
                spec.target_ref,
                "--desired-count=0",
                "--region",
                region,
            ]
        )
        start = shlex.join(
            [
                "aws",
                "ecs",
                "update-service",
                "--cluster",
                _DEFAULT_ECS_CLUSTER,
                "--service",
                spec.target_ref,
                f"--desired-count={spec.expected_replicas}",
                "--region",
                region,
            ]
        )
        return f"{stop} && {start}"
    desired = 0 if action in {"stop", "pause", "drain"} else spec.expected_replicas
    return shlex.join(
        [
            "aws",
            "ecs",
            "update-service",
            "--cluster",
            _DEFAULT_ECS_CLUSTER,
            "--service",
            spec.target_ref,
            f"--desired-count={desired}",
            "--region",
            region,
        ]
    )


def _build_cluster_action_cmd(spec: LiveClusterSpec, action: str, project_id: str) -> str:
    kind = spec.deployment_kind
    if kind == LiveClusterDeploymentKind.CLOUD_RUN:
        return _cloud_run_action_cmd(spec, action, project_id)
    if kind in {LiveClusterDeploymentKind.GKE, LiveClusterDeploymentKind.EKS}:
        return _kubectl_action_cmd(spec, action)
    if kind == LiveClusterDeploymentKind.ECS_SERVICE:
        return _ecs_action_cmd(spec, action)
    return f"echo 'unsupported deployment_kind={kind} for action={action}'"


def _run_cluster_cmd(cmd_str: str) -> tuple[bool, str]:
    try:
        args = shlex.split(cmd_str)
        result = subprocess.run(args, capture_output=True, text=True, timeout=60)
        if result.returncode == 0:
            return True, ""
        return False, result.stderr.strip()
    except FileNotFoundError:
        return False, "required CLI binary not found; run from an authenticated terminal"
    except subprocess.TimeoutExpired:
        return False, "command timed out after 60s"


def _do_cluster_action(name: str, action: str, cloud: str, dry_run: bool) -> LiveActionResponse:
    cloud_target = _resolve_cloud_target(cloud)
    env_tier = _resolve_env_tier()
    spec = _lookup_cluster_spec(name, env_tier, cloud_target)
    project_id = _cfg.gcp_project_id
    is_mock = _cfg.is_mock_mode() or dry_run

    cmd = _build_cluster_action_cmd(spec, action, project_id)

    if is_mock:
        return LiveActionResponse(
            name=name,
            action=action,
            status="preview",
            command=cmd,
            executed_at=datetime.now(UTC).isoformat(),
        )

    success, err = _run_cluster_cmd(cmd)
    return LiveActionResponse(
        name=name,
        action=action,
        status="ok" if success else "failed",
        command=cmd,
        error=err or None,
        executed_at=datetime.now(UTC).isoformat(),
    )


@router.get("/monitor/live", response_model=LiveMonitorResponse, tags=["Monitor"])
def list_live_clusters(
    cloud: str = Query(default="gcp", description="Cloud target: gcp or aws"),
    status: str | None = Query(default=None, description="Filter by status: running, completed, failed"),
    limit: int = Query(default=50, ge=1, le=500, description="Max rows to return"),
) -> LiveMonitorResponse:
    """List running and recent LONG_LIVED_LIVE strategy clusters (paper-trade, live-trade).

    Reads the deployments registry (active + 7-day archive — longer window than
    backfill/experiment because live clusters persist for days) and returns only
    rows whose VM-name prefix matches known long-lived strategy prefixes.
    The ``cloud`` parameter is accepted for interface parity; GCS registry is
    the source of truth regardless of cloud target.
    """
    try:
        registry = DeploymentsRegistry(bucket=DEFAULT_BUCKET)
        active = list(resolve_active_registry(gcs=registry))
        archived = list(registry.list_recent_archive(days=7))
    except Exception as exc:
        logger.warning("monitor/live: registry read failed: %s", exc)
        active, archived = [], []

    clusters: list[LiveClusterEntry] = []
    seen_ids: set[str] = set()
    for entry in active + archived:
        if not _is_live_vm(entry.vm_name):
            continue
        if status is not None and entry.status != status:
            continue
        if entry.deployment_id in seen_ids:
            continue
        seen_ids.add(entry.deployment_id)
        clusters.append(
            LiveClusterEntry(
                deployment_id=entry.deployment_id,
                vm_name=entry.vm_name,
                deployment_kind=_infer_deployment_kind(entry.vm_name),
                asset_group=entry.asset_group or "",
                task=entry.task or "",
                status=entry.status or "",
                started_at=entry.started_at or "",
                last_heartbeat_at=entry.last_heartbeat_at or "",
                completed_at=entry.completed_at,
                exit_code=entry.exit_code,
                log_uri=entry.log_uri or "",
            )
        )

    clusters.sort(key=lambda c: c.started_at, reverse=True)
    clusters = clusters[:limit]

    return LiveMonitorResponse(
        clusters=clusters,
        total=len(clusters),
        queried_at=datetime.now(UTC).isoformat(),
        cloud=cloud,
        env=_settings.DEPLOYMENT_ENV,
    )


# ---------------------------------------------------------------------------
# Phase E.2 lifecycle action routes
# ---------------------------------------------------------------------------


@router.post("/monitor/live/{name}/start", response_model=LiveActionResponse, tags=["Monitor"])
def start_live_cluster(
    name: str,
    cloud: str = Query(default="gcp", description="Cloud target: gcp or aws"),
    dry_run: bool = Query(default=False, description="If true, return command without executing"),
) -> LiveActionResponse:
    """Start a stopped live cluster (Cloud Run: set min-instances=N; GKE/EKS: scale up). Idempotent."""
    return _do_cluster_action(name, "start", cloud, dry_run)


@router.post("/monitor/live/{name}/stop", response_model=LiveActionResponse, tags=["Monitor"])
def stop_live_cluster(
    name: str,
    cloud: str = Query(default="gcp", description="Cloud target: gcp or aws"),
    dry_run: bool = Query(default=False, description="If true, return command without executing"),
) -> LiveActionResponse:
    """Stop a live cluster (Cloud Run: scale-to-0; GKE/EKS: replicas=0; ECS: desired-count=0). Idempotent."""
    return _do_cluster_action(name, "stop", cloud, dry_run)


@router.post("/monitor/live/{name}/pause", response_model=LiveActionResponse, tags=["Monitor"])
def pause_live_cluster(
    name: str,
    cloud: str = Query(default="gcp", description="Cloud target: gcp or aws"),
    dry_run: bool = Query(default=False, description="If true, return command without executing"),
) -> LiveActionResponse:
    """Pause a live cluster (same as stop; intent is temporary suspension). Idempotent."""
    return _do_cluster_action(name, "pause", cloud, dry_run)


@router.post("/monitor/live/{name}/restart", response_model=LiveActionResponse, tags=["Monitor"])
def restart_live_cluster(
    name: str,
    cloud: str = Query(default="gcp", description="Cloud target: gcp or aws"),
    dry_run: bool = Query(default=False, description="If true, return command without executing"),
) -> LiveActionResponse:
    """Rolling restart a live cluster (Cloud Run: redeploy; GKE/EKS: rollout restart)."""
    return _do_cluster_action(name, "restart", cloud, dry_run)


@router.post("/monitor/live/{name}/drain", response_model=LiveActionResponse, tags=["Monitor"])
def drain_live_cluster(
    name: str,
    cloud: str = Query(default="gcp", description="Cloud target: gcp or aws"),
    dry_run: bool = Query(default=False, description="If true, return command without executing"),
) -> LiveActionResponse:
    """Drain traffic from a live cluster before stopping (Cloud Run: traffic-split to 0)."""
    return _do_cluster_action(name, "drain", cloud, dry_run)

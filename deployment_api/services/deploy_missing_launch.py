"""Auto-launch service for deploy-missing endpoint (Phase 2).

Plan: deploy_missing_auto_launch_2026_05_07.md Phase 2.

Phase 2 items implemented here:
  1. launch_deploy_missing_vm() — subprocess launch of registered launcher script
  2. check_inflight_vm()        — per-shard idempotency via GCE VM name filter
  3. DEPLOY_MISSING_VM_LAUNCHED event + _poll_started_event() (90s poll)
  4. DeployMissingRateLimiter  — 30/op/hr · 200/op/day · 100/proj/hr (Phase 0 Decision 3)

Pre-launch gate: TarballStalenessChecker.ensure_fresh() wired as best-effort
(non-blocking if git timestamp is unavailable in the Cloud Run container).

VM naming: ``dm-{shard_key_hash}-{run_ts}`` where hash = SHA-256[:16] of shard_key.
Watchdog prefix: ``dm-`` registered in vm_zombie_watchdog.py + backfill_launch._REGISTERED_VM_PREFIXES.
Inflight check: ``gcloud compute instances list --filter=name~^dm-{hash}-.* AND status=RUNNING``.

Rate limit state is in-process; Firestore backing for cross-restart persistence is post-cutover.
"""

from __future__ import annotations

import hashlib
import logging
import subprocess
import time
import uuid
from collections import deque
from dataclasses import dataclass
from datetime import UTC, datetime
from os import environ as _process_env
from pathlib import Path

from unified_trading_library import log_event

from deployment_api.deployment_api_config import DeploymentApiConfig
from deployment_api.services.deploy_missing import (
    DeployMissingError,
    build_shard_key,
    get_launcher_script,
)
from deployment_api.services.tarball_staleness import TarballStalenessChecker

logger = logging.getLogger(__name__)

_cfg = DeploymentApiConfig()

_SUBPROCESS_TIMEOUT_SECONDS = 600
_STARTED_POLL_INTERVAL_SECONDS = 5
_STARTED_POLL_TIMEOUT_SECONDS = 90

# VM name first-segment — registered in vm_zombie_watchdog.py + backfill_launch._REGISTERED_VM_PREFIXES.
_DM_VM_PREFIX = "dm"


def _build_shard_key_hash(shard_key: str) -> str:
    """SHA-256 hex prefix (16 chars) — GCE-safe VM name segment."""
    return hashlib.sha256(shard_key.encode()).hexdigest()[:16]


def _build_run_ts() -> str:
    return datetime.now(UTC).strftime("%Y%m%d-%H%M%S")


def _launcher_dir() -> Path:
    root = Path(_cfg.workspace_root) if _cfg.workspace_root else Path(__file__).resolve().parents[3]
    return root / "deployment-service" / "scripts" / "vm"


def _build_events_uri(project_id: str, service: str, run_ts: str, vm_name: str) -> str:
    """Construct events GCS URI matching base_service.py SSOT."""
    date_part = run_ts[:8]
    iso_date = f"{date_part[:4]}-{date_part[4:6]}-{date_part[6:8]}"
    return f"gs://{project_id}-events/events/{service}/{iso_date}/{vm_name}/"  # noqa: gs-uri  — events bucket env-tier pending Phase 2.6 sub-step


def _get_latest_commit_timestamp(workspace_root: str) -> datetime | None:
    """Return HEAD commit timestamp from workspace root; None on failure."""
    try:
        result = subprocess.run(
            ["git", "log", "-1", "--format=%ct", "HEAD"],
            capture_output=True,
            text=True,
            timeout=10,
            check=False,
            cwd=workspace_root or None,
        )
        if result.returncode == 0 and result.stdout.strip():
            return datetime.fromtimestamp(int(result.stdout.strip()), tz=UTC)
    except (subprocess.TimeoutExpired, OSError, ValueError):
        pass
    return None


# ── Rate limiter (Phase 0 Decision 3) ─────────────────────────────────────────


class DeployMissingRateLimitError(ValueError):
    """Raised when a Phase 0 Decision 3 rate ceiling is exceeded."""


class DeployMissingRateLimiter:
    """In-process sliding-window rate limiter for deploy-missing launches.

    Phase 0 Decision 3 ceilings:
      - 30 launches / operator / hour
      - 200 launches / operator / day
      - 100 launches / project / hour
      - 1 active VM per shard_key (enforced by check_inflight_vm)

    In-process for Phase 2 MVP; Firestore-backed persistence is post-cutover.
    """

    _HR = 3600.0
    _DAY = 86400.0

    def __init__(
        self,
        per_op_hour: int = 30,
        per_op_day: int = 200,
        per_proj_hour: int = 100,
    ) -> None:
        self._per_op_hour = per_op_hour
        self._per_op_day = per_op_day
        self._per_proj_hour = per_proj_hour
        self._op_hour: dict[str, deque[float]] = {}
        self._op_day: dict[str, deque[float]] = {}
        self._proj_hour: deque[float] = deque()

    def _trim(self, bucket: deque[float], window: float) -> None:
        cutoff = time.monotonic() - window
        while bucket and bucket[0] < cutoff:
            bucket.popleft()

    def check_and_record(self, operator_id: str) -> None:
        """Check all ceilings and record launch. Raises DeployMissingRateLimitError on any exceed."""
        now = time.monotonic()

        oh = self._op_hour.setdefault(operator_id, deque())
        self._trim(oh, self._HR)
        if len(oh) >= self._per_op_hour:
            raise DeployMissingRateLimitError(
                f"Rate limit: {self._per_op_hour} deploys/hr per operator. Retry after 1 hour."
            )

        od = self._op_day.setdefault(operator_id, deque())
        self._trim(od, self._DAY)
        if len(od) >= self._per_op_day:
            raise DeployMissingRateLimitError(
                f"Rate limit: {self._per_op_day} deploys/day per operator. Retry after 24 hours."
            )

        self._trim(self._proj_hour, self._HR)
        if len(self._proj_hour) >= self._per_proj_hour:
            raise DeployMissingRateLimitError(
                f"Rate limit: {self._per_proj_hour} deploys/hr project-wide. Retry after 1 hour."
            )

        oh.append(now)
        od.append(now)
        self._proj_hour.append(now)


_rate_limiter = DeployMissingRateLimiter()


# ── In-flight VM check (per-shard idempotency) ────────────────────────────────


def check_inflight_vm(shard_key_hash: str, project_id: str) -> str | None:
    """Return the running VM name matching ``dm-{hash}-*``, or None.

    Filters GCE instances by name pattern and RUNNING status.
    In mock mode returns None immediately.
    """
    if _cfg.is_mock_mode():
        return None
    try:
        result = subprocess.run(
            [
                "gcloud",
                "compute",
                "instances",
                "list",
                f"--project={project_id}",
                "--filter",
                f"name~^dm-{shard_key_hash}- AND status=RUNNING",
                "--format=value(name)",
            ],
            capture_output=True,
            text=True,
            timeout=30,
            check=False,
        )
        if result.returncode != 0:
            logger.warning(
                "gcloud instances list exit %d: %s",
                result.returncode,
                result.stderr[:200],
            )
            return None
        names = result.stdout.strip().splitlines()
        return names[0] if names else None
    except (subprocess.TimeoutExpired, OSError) as exc:
        logger.warning("check_inflight_vm failed: %s", exc)
        return None


# ── STARTED event poller ───────────────────────────────────────────────────────


def _poll_started_event(
    events_uri: str,
    *,
    project_id: str,
    timeout: float = _STARTED_POLL_TIMEOUT_SECONDS,
) -> bool:
    """Poll GCS for STARTED lifecycle event. Returns True if observed, False on timeout.

    events_uri: ``gs://{bucket}/events/{service}/{date}/{vm_name}/``
    STARTED blob: events_uri + ``STARTED``
    In mock mode returns True immediately.
    """
    if _cfg.is_mock_mode():
        return True

    started_uri = f"{events_uri.rstrip('/')}STARTED"
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        try:
            result = subprocess.run(
                [
                    "gcloud",
                    "storage",
                    "objects",
                    "describe",
                    started_uri,
                    f"--project={project_id}",
                ],
                capture_output=True,
                text=True,
                timeout=10,
                check=False,
            )
            if result.returncode == 0:
                return True
        except (subprocess.TimeoutExpired, OSError) as exc:
            logger.debug("poll_started transient error: %s", exc)
        time.sleep(_STARTED_POLL_INTERVAL_SECONDS)
    return False


# ── Launch result ──────────────────────────────────────────────────────────────


@dataclass
class DeployMissingLaunchResult:
    service: str
    asset_group: str
    shard_key: str
    shard_key_hash: str
    vm_name: str
    correlation_id: str
    events_uri: str
    dry_run: bool
    started_confirmed: bool
    inflight_vm_name: str | None = None

    def to_dict(self) -> dict[str, object]:
        return {
            "service": self.service,
            "asset_group": self.asset_group,
            "shard_key": self.shard_key,
            "shard_key_hash": self.shard_key_hash,
            "vm_name": self.vm_name,
            "correlation_id": self.correlation_id,
            "events_uri": self.events_uri,
            "dry_run": self.dry_run,
            "started_confirmed": self.started_confirmed,
            "inflight_vm_name": self.inflight_vm_name,
        }


# ── Main launch function ───────────────────────────────────────────────────────


def launch_deploy_missing_vm(
    *,
    service: str,
    asset_group: str,
    row_key: dict[str, str],
    operator_id: str,
    dry_run: bool = False,
    skip_tarball_check: bool = False,
) -> DeployMissingLaunchResult:
    """Launch a deploy-missing backfill VM for ONE leaf shard.

    Phase 2 sequence:
      1. Input validation + launcher script lookup.
      2. Rate limit check (raises DeployMissingRateLimitError on ceiling exceeded).
      3. Build shard_key + shard_key_hash.
      4. In-flight VM check — return existing VM name if shard already running.
      5. Tarball freshness gate (best-effort; non-blocking if git unavailable).
      6. subprocess.run bash launcher with ``--shard-key=<key>`` arg.
      7. Emit DEPLOY_MISSING_VM_LAUNCHED event keyed on shard_key/correlation_id.
      8. Poll 90s for STARTED lifecycle event from the VM startup script.

    Args:
        service: Service slug from ``_SERVICE_LAUNCHER_SCRIPTS`` registry.
        asset_group: Lowercase asset_group (cefi/defi/tradfi/sports/prediction).
        row_key: Leaf shard row_key dict; must include 'venue', 'data_type', 'day'.
        operator_id: Caller identity for per-operator rate-limit buckets.
        dry_run: If True (or mock mode), returns stub result without subprocess.
        skip_tarball_check: If True, bypass Phase 1 tarball freshness gate.

    Raises:
        DeployMissingError: Invalid service/row_key or launcher not found.
        DeployMissingRateLimitError: Phase 0 Decision 3 ceiling exceeded.
    """
    if not row_key.get("venue") or not row_key.get("data_type"):
        raise DeployMissingError(f"row_key must include at least 'venue' and 'data_type'; got {row_key!r}")
    day = row_key.get("day") or row_key.get("date")
    if not day:
        raise DeployMissingError("row_key must include 'day' (or 'date') for surgical recovery")

    launcher_script = get_launcher_script(service)
    if launcher_script is None:
        raise DeployMissingError(
            f"No launcher script registered for service {service!r}. "
            "Add to _SERVICE_LAUNCHER_SCRIPTS in deployment_api/services/deploy_missing.py."
        )

    _rate_limiter.check_and_record(operator_id)

    shard_key = build_shard_key(asset_group, row_key)
    shard_key_hash = _build_shard_key_hash(shard_key)
    project_id = _cfg.gcp_project_id or "unknown"
    run_ts = _build_run_ts()
    correlation_id = str(uuid.uuid4())
    vm_name = f"{_DM_VM_PREFIX}-{shard_key_hash}-{run_ts}"

    inflight = check_inflight_vm(shard_key_hash, project_id)
    if inflight:
        logger.info(
            "deploy-missing: in-flight VM %s already running for shard_key_hash=%s",
            inflight,
            shard_key_hash,
        )
        events_uri = _build_events_uri(project_id, service, run_ts, inflight)
        log_event(
            "DEPLOY_MISSING_INFLIGHT",
            details={
                "vm_name": inflight,
                "shard_key_hash": shard_key_hash,
                "correlation_id": correlation_id,
            },
        )
        return DeployMissingLaunchResult(
            service=service,
            asset_group=asset_group,
            shard_key=shard_key,
            shard_key_hash=shard_key_hash,
            vm_name=inflight,
            correlation_id=correlation_id,
            events_uri=events_uri,
            dry_run=dry_run,
            started_confirmed=True,
            inflight_vm_name=inflight,
        )

    events_uri = _build_events_uri(project_id, service, run_ts, vm_name)

    # Tarball freshness gate (Phase 1 integration). Non-blocking on failure.
    if not skip_tarball_check and not (_cfg.is_mock_mode() or dry_run):
        workspace_root = _cfg.workspace_root or str(Path(__file__).resolve().parents[3])
        latest_ts = _get_latest_commit_timestamp(workspace_root)
        if latest_ts is not None:
            try:
                checker = TarballStalenessChecker(project_id=project_id)
                refresh = checker.ensure_fresh(asset_group, latest_ts)
                logger.info(
                    "deploy-missing tarball check: asset_group=%s status=%s triggered=%s",
                    asset_group,
                    refresh.status,
                    refresh.triggered,
                )
            except Exception as exc:
                logger.warning("Tarball freshness check failed (non-blocking): %s", exc)
        else:
            logger.warning(
                "deploy-missing: git timestamp unavailable at %s — skipping tarball check",
                workspace_root,
            )

    log_event(
        "DEPLOY_MISSING_VM_LAUNCH_REQUESTED",
        details={
            "vm_name": vm_name,
            "service": service,
            "asset_group": asset_group,
            "shard_key_hash": shard_key_hash,
            "correlation_id": correlation_id,
            "dry_run": str(dry_run),
        },
    )

    if _cfg.is_mock_mode() or dry_run:
        log_event(
            "DEPLOY_MISSING_VM_LAUNCHED",
            details={
                "vm_name": vm_name,
                "shard_key": shard_key,
                "correlation_id": correlation_id,
                "dry_run": "true",
                "events_uri": events_uri,
            },
        )
        return DeployMissingLaunchResult(
            service=service,
            asset_group=asset_group,
            shard_key=shard_key,
            shard_key_hash=shard_key_hash,
            vm_name=vm_name,
            correlation_id=correlation_id,
            events_uri=events_uri,
            dry_run=True,
            started_confirmed=True,
        )

    launcher_path = _launcher_dir() / Path(launcher_script).name
    if not launcher_path.is_file():
        raise DeployMissingError(
            f"Launcher '{launcher_path}' not found. Verify deployment-service is checked out at the workspace root."
        )

    env_diff: dict[str, str] = {
        "VM_NAME": vm_name,
        "RUN_TS": run_ts,
        "MANIFEST_PER_VM_SHARDS": "true",
        "VM_ASSET_GROUP": asset_group.upper(),
        "VM_DEPLOY_MISSING_SHARD_HASH": shard_key_hash,
        "VM_SHARD_KEY": shard_key,
        "VM_VENUE": row_key.get("venue", ""),  # noqa: qg-empty-fallback — optional row_key axis
        "VM_DATA_TYPE": row_key.get("data_type", ""),  # noqa: qg-empty-fallback — optional row_key axis
    }
    if row_key.get("instrument_type"):
        env_diff["VM_INSTRUMENT_TYPE"] = row_key["instrument_type"]
    inst_or_root = row_key.get("root") or row_key.get("instrument_id")
    if inst_or_root:
        env_diff["VM_ROOT_SYMBOL"] = inst_or_root
    if day:
        env_diff["VM_START_DATE"] = day
        env_diff["VM_END_DATE"] = day

    argv = ["bash", str(launcher_path), f"--shard-key={shard_key}"]
    full_env = {**_process_env, **env_diff}

    try:
        completed = subprocess.run(
            argv,
            env=full_env,
            capture_output=True,
            text=True,
            timeout=_SUBPROCESS_TIMEOUT_SECONDS,
            check=False,
            shell=False,
        )
    except subprocess.TimeoutExpired as exc:
        log_event(
            "DEPLOY_MISSING_LAUNCH_TIMEOUT",
            details={
                "vm_name": vm_name,
                "correlation_id": correlation_id,
                "timeout_seconds": str(_SUBPROCESS_TIMEOUT_SECONDS),
            },
        )
        raise DeployMissingError(f"Launcher timed out after {_SUBPROCESS_TIMEOUT_SECONDS}s for VM {vm_name}.") from exc

    if completed.returncode != 0:
        log_event(
            "DEPLOY_MISSING_LAUNCH_FAILED",
            details={
                "vm_name": vm_name,
                "correlation_id": correlation_id,
                "exit_code": str(completed.returncode),
                "stderr_tail": (completed.stderr or "")[-500:],
            },
        )
        raise DeployMissingError(
            f"Launcher exited {completed.returncode} for VM {vm_name}. stderr: {(completed.stderr or '')[-500:]}"
        )

    log_event(
        "DEPLOY_MISSING_VM_LAUNCHED",
        details={
            "vm_name": vm_name,
            "shard_key": shard_key,
            "correlation_id": correlation_id,
            "dry_run": "false",
            "events_uri": events_uri,
        },
    )

    started = _poll_started_event(events_uri, project_id=project_id)
    if not started:
        logger.warning(
            "deploy-missing: VM %s did not emit STARTED within %ds — proceeding",
            vm_name,
            _STARTED_POLL_TIMEOUT_SECONDS,
        )

    return DeployMissingLaunchResult(
        service=service,
        asset_group=asset_group,
        shard_key=shard_key,
        shard_key_hash=shard_key_hash,
        vm_name=vm_name,
        correlation_id=correlation_id,
        events_uri=events_uri,
        dry_run=False,
        started_confirmed=started,
    )


__all__ = [
    "DeployMissingLaunchResult",
    "DeployMissingRateLimitError",
    "DeployMissingRateLimiter",
    "check_inflight_vm",
    "launch_deploy_missing_vm",
]

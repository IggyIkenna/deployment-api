"""POST /api/backfill/launch — programmatic VM backfill launch.

Work-stream-A keystone for the 2026-05-23 live-DeFi deadline. See
`unified-trading-pm/plans/active/deployment_api_work_stream_a_2026_05_07.md`
for the full behavioural contract; SSOTs:

  * `codex/03-observability/lifecycle-events.md` — the launched VM emits
    STARTED / progress / STOPPED to `gs://{pid}-events/...` (events_uri
    returned in the response wires the operator straight to the live tail).
  * `codex/05-infrastructure/launcher-script-ssot.md` — every launcher lives
    under `deployment-service/scripts/vm/launch-*.sh`. This route shells out
    to that script (or echoes a dry-run stub).
  * CLAUDE.md "VM Naming Convention" — every VM name's first segment must
    appear in `VM_PREFIX_TO_BUCKET` in `vm_zombie_watchdog.py`. Unknown
    prefix → 400 with the registration instructions.

V1 inline mapping. Successor plan: `cloud_agnostic_launcher_registry_2026_05_XX`
(work-stream D) will lift `_TASK_TO_LAUNCHER` + `_REGISTERED_VM_PREFIXES` to a
UAC SSOT registry so AWS launchers slot in without touching this module.
"""

from __future__ import annotations

import logging
import subprocess
import uuid
from dataclasses import dataclass
from datetime import UTC, datetime
from os import environ as _process_env
from pathlib import Path

from fastapi import APIRouter, HTTPException
from unified_api_contracts.internal import (
    BackfillLaunchRequest,
    BackfillLaunchResult,
    BackfillLaunchTaskKind,
)
from unified_trading_library import log_event

from deployment_api.deployment_api_config import DeploymentApiConfig

router = APIRouter()
logger = logging.getLogger(__name__)

_cfg = DeploymentApiConfig()

# Default zone — matches `launch-cefi-sharded-backfill.sh:61`,
# `launch-mdps-backfill-vm.sh:41`, every other launcher under
# `deployment-service/scripts/vm/`.
_DEFAULT_ZONE = "asia-northeast1-c"

# Subprocess timeout. Bash launchers are typically <60s in steady state;
# `gcloud compute instances create` plus singleton-lock probing can take
# up to ~120s under contention. 600s is the same ceiling the plan
# specifies and matches operator-tolerance for a UI button click.
_SUBPROCESS_TIMEOUT_SECONDS = 600


@dataclass(frozen=True)
class _LauncherSpec:
    """Per-task launcher binding. v1 inline; lifted to UAC by successor plan."""

    launcher_filename: str  # under {workspace_root}/deployment-service/scripts/vm/
    vm_prefix_template: str  # e.g. "mdps-backfill-{asset_group}"; rendered with .format(...)
    service: str  # service that EMITS events from this VM (events_uri partition)


# Closed set: every supported task → launcher script + VM-name-prefix template
# + the events-emitting service. Adding a new task means: (a) extend
# `BackfillLaunchTaskKind` in UAC, (b) add the launcher under
# `deployment-service/scripts/vm/`, (c) register its prefix in
# `vm_zombie_watchdog.py:VM_PREFIX_TO_BUCKET`, (d) extend this dict + the
# `_REGISTERED_VM_PREFIXES` mirror.
_TASK_TO_LAUNCHER: dict[BackfillLaunchTaskKind, _LauncherSpec] = {
    # CeFi market-data
    BackfillLaunchTaskKind.CEFI_BACKFILL: _LauncherSpec(
        launcher_filename="launch-cefi-sharded-backfill.sh",
        vm_prefix_template="cefi-{venue}",
        service="market-tick-data-service",
    ),
    BackfillLaunchTaskKind.CEFI_FORWARD_POLL: _LauncherSpec(
        launcher_filename="launch-cefi-forward-poll.sh",
        vm_prefix_template="cefi-fwd",
        service="market-tick-data-service",
    ),
    # TradFi market-data
    BackfillLaunchTaskKind.TRADFI_BACKFILL: _LauncherSpec(
        launcher_filename="launch-tradfi-backfill-vm.sh",
        vm_prefix_template="tradfi-bf",
        service="market-tick-data-service",
    ),
    BackfillLaunchTaskKind.TRADFI_FORWARD_POLL: _LauncherSpec(
        launcher_filename="launch-tradfi-forward-poll.sh",
        vm_prefix_template="tradfi-fwd",
        service="market-tick-data-service",
    ),
    # DeFi / Prediction forward-poll (no batch backfill kind in v1; MTDS_*
    # tasks below cover the on-chain backfill axis).
    BackfillLaunchTaskKind.DEFI_FORWARD_POLL: _LauncherSpec(
        launcher_filename="launch-defi-forward-poll.sh",
        vm_prefix_template="defi-fwd",
        service="market-tick-data-service",
    ),
    BackfillLaunchTaskKind.PREDICTION_FORWARD_POLL: _LauncherSpec(
        launcher_filename="launch-prediction-forward-poll.sh",
        vm_prefix_template="prediction-fwd",
        service="market-tick-data-service",
    ),
    BackfillLaunchTaskKind.SPORTS_FORWARD_POLL: _LauncherSpec(
        launcher_filename="launch-sfi-forward-poll.sh",
        vm_prefix_template="sfi-fwd",
        service="instruments-service",
    ),
    # Sports per-source backfills
    BackfillLaunchTaskKind.AF_BACKFILL: _LauncherSpec(
        launcher_filename="launch-api-football-backfill-vm.sh",
        vm_prefix_template="af-backfill",
        service="instruments-service",
    ),
    BackfillLaunchTaskKind.FOOTYSTATS_BACKFILL: _LauncherSpec(
        launcher_filename="launch-footystats-backfill-vm.sh",
        vm_prefix_template="fs-backfill",
        service="instruments-service",
    ),
    BackfillLaunchTaskKind.SFI_BACKFILL: _LauncherSpec(
        launcher_filename="launch-sfi-backfill-vm.sh",
        vm_prefix_template="sfi-backfill",
        service="instruments-service",
    ),
    BackfillLaunchTaskKind.UNDERSTAT_BACKFILL: _LauncherSpec(
        launcher_filename="launch-understat-backfill-vm.sh",
        vm_prefix_template="us-backfill",
        service="instruments-service",
    ),
    BackfillLaunchTaskKind.TRANSFERMARKT_BACKFILL: _LauncherSpec(
        launcher_filename="launch-transfermarkt-backfill-vm.sh",
        vm_prefix_template="tm-backfill",
        service="instruments-service",
    ),
    BackfillLaunchTaskKind.OPENMETEO_BACKFILL: _LauncherSpec(
        launcher_filename="launch-openmeteo-backfill-vm.sh",
        vm_prefix_template="weather-backfill",
        service="instruments-service",
    ),
    # MTDS asset-group-scoped backfills
    BackfillLaunchTaskKind.MTDS_PREDICTION_BACKFILL: _LauncherSpec(
        launcher_filename="launch-mtds-prediction-backfill-vm.sh",
        vm_prefix_template="mtds-prediction",
        service="market-tick-data-service",
    ),
    BackfillLaunchTaskKind.MTDS_PERP_FUNDING_BACKFILL: _LauncherSpec(
        launcher_filename="launch-mtds-perp-funding-backfill-vm.sh",
        vm_prefix_template="mtds-perp-funding",
        service="market-tick-data-service",
    ),
    BackfillLaunchTaskKind.MTDS_GAS_FEES_BACKFILL: _LauncherSpec(
        launcher_filename="launch-mtds-gas-fees-backfill-vm.sh",
        vm_prefix_template="mtds-gas-fees",
        service="market-tick-data-service",
    ),
    BackfillLaunchTaskKind.MTDS_LST_RATES_BACKFILL: _LauncherSpec(
        launcher_filename="launch-mtds-lst-rates-backfill-vm.sh",
        vm_prefix_template="mtds-lst-rates",
        service="market-tick-data-service",
    ),
    BackfillLaunchTaskKind.MTDS_VAULT_SHARE_PRICE_BACKFILL: _LauncherSpec(
        launcher_filename="launch-mtds-vault-share-price-backfill-vm.sh",
        vm_prefix_template="mtds-vault",
        service="market-tick-data-service",
    ),
    BackfillLaunchTaskKind.MTDS_LENDING_INDICES_BACKFILL: _LauncherSpec(
        launcher_filename="launch-mtds-lending-indices-backfill-vm.sh",
        vm_prefix_template="mtds-lending-indices",
        service="market-tick-data-service",
    ),
    # MDPS / features / canonical migration
    BackfillLaunchTaskKind.MDPS_BACKFILL: _LauncherSpec(
        launcher_filename="launch-mdps-backfill-vm.sh",
        vm_prefix_template="mdps-backfill-{asset_group}",
        service="market-data-processing-service",
    ),
    BackfillLaunchTaskKind.FEATURES_BACKFILL: _LauncherSpec(
        launcher_filename="launch-features-backfill-vm.sh",
        vm_prefix_template="features",
        service="features-volatility-service",
    ),
    BackfillLaunchTaskKind.FEATURES_SPORTS_BACKFILL: _LauncherSpec(
        launcher_filename="launch-features-sports-backfill-vm.sh",
        vm_prefix_template="features",
        service="features-sports-service",
    ),
    BackfillLaunchTaskKind.CANONICAL_MIGRATION: _LauncherSpec(
        launcher_filename="launch-canonical-migration-vm.sh",
        vm_prefix_template="canonical-migration-{asset_group}",
        service="market-tick-data-service",
    ),
}


# Mirror of the `VM_PREFIX_TO_BUCKET` keyset in
# `deployment-service/scripts/vm/vm_zombie_watchdog.py:113`. Kept inline
# here so the route can validate registration without importing the
# watchdog module (which depends on `google.cloud.compute_v1` at import
# time and would burden the deployment-api startup).
#
# When you add a launcher prefix to the watchdog dict, mirror it here.
# QG static-walk in the successor plan will turn this into a single SSOT
# read.
_REGISTERED_VM_PREFIXES: frozenset[str] = frozenset(
    {
        # CeFi
        "cefi-mr-",
        "cefi-fwd-",
        "cefi-binance-",
        "cefi-bybit-",
        "cefi-deribit-",
        "cefi-coinbase-",
        "cefi-okx-",
        "cefi-upbit-",
        "cefi-hyperliquid-",
        "cefi-bitfinex-",
        "cefi-bitget-",
        "cefi-kraken-",
        "cefi-aster-",
        "cefi-cme-",
        "cefi-extended-",
        "cefi-lighter-",
        "cefi-pacifica-",
        "aster-fwd-",
        # TradFi
        "tradfi-bf-",
        "tradfi-fwd-",
        "tradfi-recent-",
        # MDPS sharded + per-AG
        "mdps-cefi-",
        "mdps-tradfi-",
        "mdps-defi-",
        "mdps-prediction-",
        "mdps-backfill-cefi-",
        "mdps-backfill-tradfi-",
        "mdps-backfill-defi-",
        "mdps-backfill-prediction-",
        "mdps-backfill-sports-",
        # MTDS asset-group-scoped
        "mtds-prediction-",
        "mtds-perp-funding-",
        "mtds-gas-fees-",
        "mtds-lst-rates-",
        "mtds-vault-",
        "mtds-lending-indices-",
        # Sports per-source backfill + audits
        "fs-backfill-",
        "af-backfill-",
        "af-audit-",
        "af-recover-",
        "tm-backfill-",
        "sfi-backfill-",
        "us-backfill-",
        "weather-backfill-",
        "fill-missing-player-stats-",
        # Features (heartbeat-only umbrella)
        "features-",
        # Canonical migration
        "canonical-migration-cefi-",
        "canonical-migration-tradfi-",
        "canonical-migration-defi-",
        "canonical-migration-prediction-",
        "canonical-migration-sports-",
        # Forward-poll prefixes that should exist in the watchdog dict —
        # tracked here so the launcher endpoint pre-flights them. Any
        # entry below that is NOT yet in the watchdog dict is a paired
        # registration the launcher-consolidation plan must close.
        "defi-fwd-",
        "prediction-fwd-",
        "sfi-fwd-",
        # MDPS sports bucket-pass
        "mdps-sports-bucket-",
        # Options chain backfills
        "opt-deribit-",
        "opt-cboe-",
        "opt-cme-",
    }
)


def _launcher_dir() -> Path:
    """Resolve the directory containing all launch-*.sh scripts.

    Default is `{workspace_root}/deployment-service/scripts/vm/`. The
    workspace root is preferentially read from `_cfg.workspace_root`
    (config-bound). If that's empty, derived from this module's location:
    `Path(__file__).resolve().parents[3]` walks
    `routes/ -> deployment_api/ -> deployment-api/ -> workspace_root`.
    """
    root = Path(_cfg.workspace_root) if _cfg.workspace_root else Path(__file__).resolve().parents[3]
    return root / "deployment-service" / "scripts" / "vm"


def _resolve_launcher(task: BackfillLaunchTaskKind) -> _LauncherSpec:
    """Resolve task → launcher spec. Closed set; unknown task → 400."""
    spec = _TASK_TO_LAUNCHER.get(task)
    if spec is None:
        raise HTTPException(
            status_code=400,
            detail={
                "code": "UNSUPPORTED_TASK",
                "message": f"Task '{task.value}' has no launcher mapping. "
                "Add an entry to _TASK_TO_LAUNCHER + register the VM prefix in "
                "deployment-service/scripts/vm/vm_zombie_watchdog.py:VM_PREFIX_TO_BUCKET.",
                "task": task.value,
            },
        )
    return spec


def _resolve_vm_name_prefix(spec: _LauncherSpec, request: BackfillLaunchRequest) -> str:
    """Render the VM-name prefix from the spec template + request fields.

    Lowercase asset_group + venue (matching the workspace `VM Naming Convention`
    rule that asset_group dict KEYS stay lowercase even though the symbol is
    `MarketAssetGroup`).
    """
    template_keys = {"asset_group": request.asset_group.lower()}
    if request.venue is not None:
        template_keys["venue"] = request.venue.lower()
    try:
        return spec.vm_prefix_template.format(**template_keys)
    except KeyError as exc:
        raise HTTPException(
            status_code=400,
            detail={
                "code": "MISSING_TEMPLATE_FIELD",
                "message": (
                    f"Task '{spec.launcher_filename}' prefix template "
                    f"'{spec.vm_prefix_template}' requires field {exc.args[0]!r}; "
                    "supply it on BackfillLaunchRequest."
                ),
            },
        ) from exc


def _validate_prefix_registered(prefix: str) -> None:
    """Raise 400 if `{prefix}-` is not in the watchdog registry."""
    lookup_key = f"{prefix}-"
    if lookup_key in _REGISTERED_VM_PREFIXES:
        return
    raise HTTPException(
        status_code=400,
        detail={
            "code": "UNREGISTERED_VM_PREFIX",
            "message": (
                f"VM-name prefix '{lookup_key}' is not registered in "
                "VM_PREFIX_TO_BUCKET. Register it in "
                "deployment-service/scripts/vm/vm_zombie_watchdog.py "
                "(per CLAUDE.md 'VM Naming Convention') so the zombie "
                "watchdog can supervise it. After editing the dict, "
                "relaunch the watchdog VM."
            ),
            "prefix": lookup_key,
        },
    )


def _build_run_ts() -> str:
    """RUN_TS in `YYYYMMDD-HHMMSS` UTC, matching every launcher's convention."""
    return datetime.now(UTC).strftime("%Y%m%d-%H%M%S")


def _apply_extra_metadata(env: dict[str, str], extra_metadata: dict[str, str]) -> None:
    """Forward-compat: VM_-prefix + uppercase every key not already so-shaped."""
    for raw_key, raw_value in extra_metadata.items():
        normalised = raw_key.upper()
        if not normalised.startswith("VM_"):
            normalised = f"VM_{normalised}"
        env.setdefault(normalised, raw_value)


def _build_env_diff(
    request: BackfillLaunchRequest,
    vm_name: str,
    run_ts: str,
) -> dict[str, str]:
    """Construct the env vars layered on top of the launcher's inherited env.

    Per CLAUDE.md "Per-VM shard isolation for concurrent backfills":
    `VM_NAME` and `MANIFEST_PER_VM_SHARDS=true` are mandatory on every
    multi-worker backfill — set unconditionally.
    """
    env: dict[str, str] = {
        "VM_NAME": vm_name,
        "RUN_TS": run_ts,
        "MANIFEST_PER_VM_SHARDS": "true",
        "VM_ASSET_GROUP": request.asset_group.upper(),
    }
    # gcloud metadata uses `,` as key-separator, so list-valued env vars
    # use `;` (per setup-data-pipeline-vm.sh:678).
    optional_str_pairs: list[tuple[str, str | None]] = [
        ("VM_VENUE", request.venue),
        ("VM_START_DATE", request.start_date),
        ("VM_END_DATE", request.end_date),
        ("VM_TIER_PLAN", request.tier_plan),
        ("VM_YEAR", request.year),
        ("VM_MONTH", request.month),
        ("VM_ROOT_SYMBOL", request.root_symbol),
    ]
    for key, value in optional_str_pairs:
        if value is not None:
            env[key] = value
    optional_list_pairs: list[tuple[str, list[str] | None]] = [
        ("VM_DATA_TYPES", request.data_types),
        ("VM_INSTRUMENT_IDS", request.instrument_ids),
    ]
    for key, value in optional_list_pairs:
        if value:
            env[key] = ";".join(value)
    if request.force:
        env["VM_FORCE"] = "true"
    if request.skip_dependency_check:
        env["SKIP_DEPENDENCY_CHECK"] = "true"
    _apply_extra_metadata(env, request.extra_metadata)
    return env


def _build_argv(launcher_path: Path, request: BackfillLaunchRequest) -> list[str]:
    """Build subprocess argv. Universal contract: launcher accepts --force / --dry-run.

    Per the v1 plan §2.A.6: `["bash", launcher_path, *flags]` where flags are
    request-derived `--force` / `--dry-run`. Per-task positional-args are
    pushed to the v1 inline successor plan; for now env vars carry the rest.
    """
    argv: list[str] = ["bash", str(launcher_path)]
    if request.force:
        argv.append("--force")
    if request.dry_run:
        argv.append("--dry-run")
    return argv


def _build_events_uri(project_id: str, service: str, run_ts: str, vm_name: str) -> str:
    """Construct events_uri matching base_service.py:159 SSOT.

    `gs://{pid}-events/events/{service}/{YYYY-MM-DD}/{vm_name}/`
    """
    date_partition = run_ts[:8]  # YYYYMMDD → YYYY-MM-DD
    iso_date = f"{date_partition[:4]}-{date_partition[4:6]}-{date_partition[6:8]}"
    return f"gs://{project_id}-events/events/{service}/{iso_date}/{vm_name}/"


def _stub_dry_run_result(
    spec: _LauncherSpec,
    vm_name: str,
    vm_name_prefix: str,
    run_ts: str,
    correlation_id: str,
    argv: list[str],
    env_diff: dict[str, str],
    project_id: str,
) -> BackfillLaunchResult:
    return BackfillLaunchResult(
        vm_name=vm_name,
        vm_name_prefix=vm_name_prefix,
        zone=_DEFAULT_ZONE,
        project_id=project_id,
        launched_at=datetime.now(UTC),
        correlation_id=correlation_id,
        launcher_script=spec.launcher_filename,
        dry_run=True,
        events_uri=_build_events_uri(project_id, spec.service, run_ts, vm_name),
        argv=argv,
        env_diff=env_diff,
    )


@router.post("/launch", response_model=BackfillLaunchResult)
def launch_backfill(request: BackfillLaunchRequest) -> BackfillLaunchResult:
    """Launch a backfill VM via the deployment-service launcher script.

    Auth is enforced upstream by `_authenticated_router` (verify_api_key).

    In `is_mock_mode()` OR `request.dry_run`, returns a stub `BackfillLaunchResult`
    WITHOUT calling subprocess — the resolved argv + env_diff are reflected back
    so the caller can verify the shape (CI / staging tests + UI dry-run preview).

    In real mode, `subprocess.run` shells the launcher with `shell=False`,
    `timeout=600`. Bash launchers handle the actual `gcloud compute instances
    create` invocation; this endpoint is a thin authenticated front door.
    """
    spec = _resolve_launcher(request.task)
    vm_name_prefix = _resolve_vm_name_prefix(spec, request)
    _validate_prefix_registered(vm_name_prefix)

    run_ts = _build_run_ts()
    vm_name = f"{vm_name_prefix}-{run_ts}"
    correlation_id = str(uuid.uuid4())
    project_id = _cfg.gcp_project_id or "unknown"
    launcher_path = _launcher_dir() / spec.launcher_filename
    argv = _build_argv(launcher_path, request)
    env_diff = _build_env_diff(request, vm_name, run_ts)

    log_event(
        "VM_LAUNCH_REQUESTED",
        severity="INFO",
        details={
            "vm_name": vm_name,
            "task": request.task.value,
            "asset_group": request.asset_group,
            "venue": request.venue or "",
            "correlation_id": correlation_id,
            "dry_run": str(request.dry_run),
            "launcher_script": spec.launcher_filename,
        },
    )

    if _cfg.is_mock_mode() or request.dry_run:
        result = _stub_dry_run_result(
            spec,
            vm_name,
            vm_name_prefix,
            run_ts,
            correlation_id,
            argv,
            env_diff,
            project_id,
        )
        log_event(
            "VM_LAUNCHED",
            severity="INFO",
            details={
                "vm_name": vm_name,
                "correlation_id": correlation_id,
                "dry_run": "true",
                "events_uri": result.events_uri,
            },
        )
        return result

    if not launcher_path.is_file():
        log_event(
            "VM_LAUNCH_FAILED",
            severity="ERROR",
            details={
                "vm_name": vm_name,
                "correlation_id": correlation_id,
                "reason": "launcher_script_missing",
                "launcher_path": str(launcher_path),
            },
        )
        raise HTTPException(
            status_code=500,
            detail={
                "code": "LAUNCHER_SCRIPT_MISSING",
                "message": (
                    f"Launcher script '{spec.launcher_filename}' not found at "
                    f"'{launcher_path}'. Verify deployment-service is checked "
                    "out at the workspace root + the launcher exists."
                ),
            },
        )

    # Layer env_diff onto the current process env so the launcher inherits
    # PATH / HOME / gcloud creds. `_process_env` is `os.environ` imported
    # under a different name — this is subprocess env forwarding, not
    # config-reading (which is the workspace rule against `os.environ` /
    # `os.getenv`; UnifiedCloudConfig is the canonical config surface).
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
            "LAUNCH_TIMEOUT",
            severity="ERROR",
            details={
                "vm_name": vm_name,
                "correlation_id": correlation_id,
                "timeout_seconds": str(_SUBPROCESS_TIMEOUT_SECONDS),
            },
        )
        raise HTTPException(
            status_code=504,
            detail={
                "code": "LAUNCH_TIMEOUT",
                "message": (
                    f"Launcher '{spec.launcher_filename}' did not return "
                    f"within {_SUBPROCESS_TIMEOUT_SECONDS}s. The VM may or may "
                    "not have been created — check gcloud compute instances list."
                ),
                "vm_name": vm_name,
                "correlation_id": correlation_id,
            },
        ) from exc

    if completed.returncode != 0:
        log_event(
            "VM_LAUNCH_FAILED",
            severity="ERROR",
            details={
                "vm_name": vm_name,
                "correlation_id": correlation_id,
                "exit_code": str(completed.returncode),
                "stderr_tail": completed.stderr[-500:] if completed.stderr else "",
            },
        )
        raise HTTPException(
            status_code=502,
            detail={
                "code": "VM_LAUNCH_FAILED",
                "message": (
                    f"Launcher '{spec.launcher_filename}' exited "
                    f"{completed.returncode}. stderr tail: "
                    f"{(completed.stderr or '')[-500:]}"
                ),
                "vm_name": vm_name,
                "correlation_id": correlation_id,
                "exit_code": completed.returncode,
            },
        )

    events_uri = _build_events_uri(project_id, spec.service, run_ts, vm_name)
    result = BackfillLaunchResult(
        vm_name=vm_name,
        vm_name_prefix=vm_name_prefix,
        zone=_DEFAULT_ZONE,
        project_id=project_id,
        launched_at=datetime.now(UTC),
        correlation_id=correlation_id,
        launcher_script=spec.launcher_filename,
        dry_run=False,
        events_uri=events_uri,
        argv=argv,
        env_diff=env_diff,
    )
    log_event(
        "VM_LAUNCHED",
        severity="INFO",
        details={
            "vm_name": vm_name,
            "correlation_id": correlation_id,
            "dry_run": "false",
            "events_uri": events_uri,
        },
    )
    return result

"""VM/Cloud-Run/Cloud-Function/Cloud-Run-service classification + row builders.

Split from ``routes/deployments_inventory.py`` (pure code motion; plan:
``deployment_api_qg_size_gate_debt_2026_07_30.md``). Patched module-level collaborators
(``get_storage_client`` / ``resolve_bucket_name`` / ``upload_to_storage`` / ``_persist_alert``
— the census "seams" ``tests/mocks.py``'s ``patch_inventory_secondary_census`` documents) are
resolved through the facade module (``_inv``) at call time so the existing test patch surface
``deployment_api.routes.deployments_inventory.<name>`` keeps intercepting.
"""

from __future__ import annotations

import json
import uuid
from datetime import UTC, datetime
from typing import cast

# Re-declared here so `deployment_service.cloud_run_job_registry` stays a single import site;
# these two are the only Cloud-Run-registry symbols this module needs.
from deployment_service.cloud_run_job_registry import CLOUD_RUN_JOBS
from deployment_service.deployment_classification import (
    UnclassifiedDeploymentError,
    classify_deployment_target,
)
from unified_api_contracts import (
    DeploymentCloud,
    DeploymentKind,
    DeploymentTarget,
    DeploymentUmbrella,
    LifecycleClass,
    classify_vm_name,
)
from unified_trading_library import (
    ACTIVE_PREFIX,
    DEFAULT_BUCKET,
    BucketNamingError,
    DeploymentRegistryEntry,
    vm_log_stream_uri,
)

import deployment_api.routes.deployments_inventory as _inv
from deployment_api.routes._cloud_run_executions import CloudRunExecutionStatus
from deployment_api.routes._cloud_run_services import CloudRunServiceStatus
from deployment_api.routes._fleet_inventory import DEFAULT_GRACE_HOURS
from deployment_api.routes._fleet_types import OrphanEntry
from deployment_api.routes._gcp_cloud_functions import CloudFunctionStatus
from deployment_api.routes._leaked_resources import detect_unreleased_resources
from deployment_api.routes._service_health import (
    SERVICE_STATUS_DEAD,
    SERVICE_STATUS_DEGRADED,
    SERVICE_STATUS_SCALED_TO_ZERO,
    SERVICE_STATUS_SERVING,
    cloud_run_service_health_status,
    ecs_service_health_status,
)
from deployment_api.routes._vm_health import composite_health_status as _composite_health_status
from deployment_api.routes._vm_health import vm_status as _vm_status
from deployment_api.routes.deployments_inventory import (
    _DEFAULT_LIFECYCLE,  # pyright: ignore[reportPrivateUsage]
    _VM_PREFIX_REGISTRY,  # pyright: ignore[reportPrivateUsage]
    LAUNCHED_BY_ADHOC,
    LAUNCHED_BY_CONTROL_PLANE,
    LAUNCHED_BY_DEPLOYMENT_API,
    DeploymentItem,
    is_control_plane_vm,
    logger,
)

__all__ = [
    "SERVICE_STATUS_DEAD",
    "SERVICE_STATUS_DEGRADED",
    "SERVICE_STATUS_SCALED_TO_ZERO",
    "SERVICE_STATUS_SERVING",
    "_GCE_STATUS_TO_WIRE",
    "_alert_on_health_transition",
    "_classify_live_cloud_run_job",
    "_classify_vm",
    "_cloud_function_item",
    "_cloud_run_item",
    "_cloud_run_item_for_live_job",
    "_cloud_run_service_item",
    "_cloud_run_status_for",
    "_composite_health_status",
    "_heartbeat_age_seconds",
    "_match_registered_job",
    "_parse_iso",
    "_persist_alert",
    "_prune_stale_alert_state",
    "_unmanaged_vm_item",
    "_uptime_hours",
    "_vm_item",
    "_vm_lifecycle_class",
    "active_registry_vm_names",
    "classify_vm_target",
    "cloud_run_service_health_status",
    "ecs_service_health_status",
]


# ---------------------------------------------------------------------------
# VM classification + runtime status
# ---------------------------------------------------------------------------


def _vm_lifecycle_class(vm_name: str) -> str:
    """Lifecycle-class string for a VM name (the honest local registry + default).

    Reuses the curated ``_fleet_census`` prefix registry + UAC longest-prefix
    matcher; an unregistered prefix degrades to EPHEMERAL_BATCH (the same honest
    default the fleet census uses — most unregistered VMs are batch backfills).
    """
    try:
        return classify_vm_name(vm_name, _VM_PREFIX_REGISTRY).value
    except ValueError:
        return _DEFAULT_LIFECYCLE.value


def _classify_vm(vm_name: str) -> DeploymentTarget:
    """Classify a VM into a DeploymentTarget via the single deployment-service resolver."""
    return classify_deployment_target(
        vm_name,
        lifecycle_class=_vm_lifecycle_class(vm_name),
        cloud=DeploymentCloud.GCP,
        kind=DeploymentKind.VM,
    )


def classify_vm_target(vm_name: str) -> DeploymentTarget:
    """Public: classify a VM/deployment name → its DeploymentTarget (umbrella/service/asset_group).

    Reuses the inventory's curated lifecycle resolver + the single
    ``classify_deployment_target`` resolver, so a consumer (the per-deployment
    freshness endpoint) never re-derives classification.
    """
    return _classify_vm(vm_name)


def active_registry_vm_names() -> set[str]:
    """The set of VM names with an ACTIVE deployment-registry entry (parallel-read).

    The "registered" set for cross-cloud reconciliation — a RUNNING GCE VM absent
    from this set is UNKNOWN (running but unregistered); an entry here with no running
    VM is EXPECTED-MISSING. Reuses the same parallel GCS reader as the inventory.
    """
    client = _inv.get_storage_client()
    keys = _inv._list_json_keys(client, DEFAULT_BUCKET, ACTIVE_PREFIX)  # pyright: ignore[reportPrivateUsage]
    return {e.vm_name for e in _inv._download_entries_parallel(client, DEFAULT_BUCKET, keys)}  # pyright: ignore[reportPrivateUsage]


def _parse_iso(raw: str | None) -> datetime | None:
    """Parse an ISO-8601 timestamp (Z-suffixed or offset-aware); None on any parse failure."""
    if not raw:
        return None
    try:
        parsed = datetime.fromisoformat(raw.replace("Z", "+00:00"))
    except ValueError:
        return None
    return parsed if parsed.tzinfo is not None else parsed.replace(tzinfo=UTC)


def _heartbeat_age_seconds(entry: DeploymentRegistryEntry, now: datetime) -> int | None:
    """Seconds since the registry entry's last heartbeat, or None if unparseable."""
    last_hb = _parse_iso(entry.last_heartbeat_at)
    if last_hb is None:
        return None
    return max(0, int((now - last_hb).total_seconds()))


def _uptime_hours(entry: DeploymentRegistryEntry, now: datetime) -> float | None:
    """Wall-clock hours run — ``started_at`` to ``completed_at`` (or ``now`` if still running)."""
    started = _parse_iso(entry.started_at)
    if started is None:
        return None
    ended = _parse_iso(entry.completed_at) or now
    return max(0.0, (ended - started).total_seconds() / 3600.0)


def _persist_alert(
    *,
    alert_class: str,
    workflow_name: str,
    severity: str,
    message: str,
    dedup_key: str,
    subject_repo: str | None = None,
) -> None:
    """Write one JSONL row to its own object in the shared GCS alert ledger — best-effort, never
    raises.

    Mirrors agent-orchestrator's ``notifications.slack._persist_to_gcs`` row shape exactly (same
    bucket/fields; ``repo`` names the writing SERVICE, not the alerting VM) so ``GET /api/alerts``
    (``_repo_ci_alerts.py``'s ``_parse_line`` via its ``cicd/alerts/{date}/`` prefix walk) picks
    these up alongside CI/CD + other watcher alerts with zero reader-side changes. Shard-level
    isolation: a ledger-write failure logs a warning and never breaks the inventory computation it
    rides on.

    ONE OBJECT PER ALERT (root-cause fix, not a mitigation — 2026-07-21): the prior implementation
    downloaded the whole day's shared ``alerts.jsonl``, appended one line, and re-uploaded it — an
    unlocked read-modify-write. Two overlapping calls silently clobbered each other (structurally
    identical to `persist_cicd_event_ledger_read_modify_write_race_2026_07_17.md`, which fixes the
    sibling events-ledger writer the same way). ``_read_ledgers_sync()`` already walks the whole
    ``cicd/alerts/{date}/`` prefix and parses every blob it finds — it never assumed a single
    filename — so a never-overwritten unique object per call eliminates the race with zero reader
    changes: no read, no merge, no lock.

    ``subject_repo`` is the repo the alert is ABOUT, distinct from ``repo`` (the emitter, always
    "deployment-api" here) — see the ``FIELD_COVERAGE`` contract in ``unified_api_contracts.alerting``
    and `deployment_alerts_ingestion_completeness_2026_07_20.md` todo 4. Callers with no repo-scoped
    subject (e.g. VM-health alerts) simply omit it, matching the ``zombie_watchdog`` plane's
    structural absence rather than fabricating a value.
    """
    try:
        bucket = _inv.resolve_bucket_name(cloud="gcp", kind="cicd-events")
        date = datetime.now(UTC).strftime("%Y-%m-%d")
        blob_path = f"cicd/alerts/{date}/{uuid.uuid4().hex}.jsonl"
        row: dict[str, object] = {
            "event_type": "slack_alert",
            "timestamp": datetime.now(UTC).isoformat(),
            "repo": "deployment-api",
            "subject_repo": subject_repo,
            "workflow_name": workflow_name,
            "severity": severity,
            "conclusion": None,
            "message": message,
            "run_url": "",
            "dedup_key": dedup_key,
            "alert_class": alert_class,
        }
        line = json.dumps(row)
        _inv.upload_to_storage(bucket, blob_path, (line + "\n").encode("utf-8"), content_type="application/jsonl")
    except (BucketNamingError, OSError, ValueError, RuntimeError) as exc:
        logger.warning("inventory: alert-ledger persist failed (%s/%s): %s", alert_class, workflow_name, exc)


def _alert_on_health_transition(item: DeploymentItem) -> None:
    """Fire a ledger alert on a fresh transition INTO oom-risk/stalled/hung (never a repeat-poll
    spam).

    Always records the item's current state (even a non-alertable one, e.g. recovery back to
    ``working``) so the NEXT transition into an alertable state is detected correctly.
    """
    status = item.composite_health_status
    with _inv._last_alerted_health_lock:  # pyright: ignore[reportPrivateUsage]
        previous = _inv._last_alerted_health.get(item.name)  # pyright: ignore[reportPrivateUsage]
        _inv._last_alerted_health[item.name] = status  # pyright: ignore[reportPrivateUsage]
    if status is None or status not in _inv._ALERT_HEALTH_STATES or status == previous:  # pyright: ignore[reportPrivateUsage]
        return
    _inv._persist_alert(  # pyright: ignore[reportPrivateUsage]
        alert_class=status,  # "oom-risk" | "stalled" | "hung"
        workflow_name=f"vm-health-{item.name}",
        severity="CRITICAL" if status == "oom-risk" else "WARNING",
        message=f"{item.name} ({item.service}/{item.asset_group}) is {status}",
        dedup_key=f"vm-health-{item.name}-{status}",
    )


def _prune_stale_alert_state(census_vm_names: set[str]) -> None:
    """Drop ``_last_alerted_health`` entries for VM names absent from the current census.

    Called once per full GCP census (GCE VMs are all-region aggregated regardless of
    ``region_scope`` — see ``_compute_inventory`` — so ``census_vm_names`` is always the
    complete live+registered VM name set, never a partial/region-scoped one). Without this
    the map grows forever with every VM ever seen across the fleet's churn (short-lived
    backfill VMs launched and torn down continuously).
    """
    with _inv._last_alerted_health_lock:  # pyright: ignore[reportPrivateUsage]
        stale = [name for name in _inv._last_alerted_health if name not in census_vm_names]  # pyright: ignore[reportPrivateUsage]
        for name in stale:
            del _inv._last_alerted_health[name]  # pyright: ignore[reportPrivateUsage]


def _vm_item(
    entry: DeploymentRegistryEntry,
    now: datetime,
    vm_details: dict[str, object] | None = None,
    *,
    control_plane_running: bool | None = None,
    object_deltas: dict[str, int | None] | None = None,
    disk_details: dict[str, dict[str, object]] | None = None,
    addresses: dict[str, dict[str, object]] | None = None,
    orphan: OrphanEntry | None = None,
) -> DeploymentItem:
    """Build an inventory item from a VM deployment-registry entry.

    ``vm_details`` is this VM's entry from the GCE aggregated-list join
    (``get_vm_instance_details``), if any — surfaces the Tier-0 free wins
    (machine_type/zone/labels/boot-disk/raw status) already fetched there but
    previously discarded down to just the running-VM-name set.
    ``control_plane_running`` is the same GCE aggregated-list confirmation,
    reduced to a bool — feeds the D.3 composite classifier's ``dead``/``hung``.
    ``object_deltas`` is the BATCH-umbrella asset_group -> manifest object-delta
    map the caller batches once per census cycle (``_batched_object_deltas``) —
    feeds the composite classifier's ``stalled``.
    ``orphan`` is this VM's entry (by name) from the orphans SSOT
    (``_fleet_inventory.build_orphan_inventory``), if it is currently stopped —
    ``None`` for a running VM (honest absence, never re-derived here).
    """
    target = _classify_vm(entry.vm_name)
    hb_age = _heartbeat_age_seconds(entry, now)
    status = _vm_status(entry, hb_age)
    object_delta = None
    if target.umbrella == DeploymentUmbrella.BATCH:
        object_delta = (object_deltas or {}).get(target.asset_group)
    # Live-first read path (WS-4 decision 2): the live streaming path is always the
    # primary candidate for ANY vm regardless of completed_at — replaces the broken
    # completed_at[:10]-keyed rolling-date guess, which 404s because the archiver
    # writes daily rolling copies keyed by cron-run date, not completion date.
    run_log = vm_log_stream_uri(entry.vm_name)
    details = vm_details or {}
    labels = details.get("labels")
    labels_dict = cast(dict[str, str], labels) if labels else None
    has_unreleased, unreleased = detect_unreleased_resources(
        entry.vm_name, vm_details, disk_details or {}, addresses or {}, is_running=bool(control_plane_running)
    )
    return DeploymentItem(
        name=entry.vm_name,
        kind=target.kind.value,
        umbrella=target.umbrella.value,
        cloud=target.cloud.value,
        service=target.service,
        asset_group=target.asset_group,
        status=status,
        last_run_at=entry.completed_at or entry.started_at or None,
        exit_code=entry.exit_code,
        heartbeat_age_seconds=hb_age,
        captured_progress=entry.rows_out,
        run_log_uri=run_log,
        launched_by=LAUNCHED_BY_DEPLOYMENT_API,  # a registry entry is the deployment-api-launched signal
        has_unreleased_resources=has_unreleased,
        unreleased_resources=unreleased or None,
        rows_in=entry.rows_in,
        rows_error=entry.rows_error,
        events_emitted=entry.events_emitted,
        uptime_hours=_uptime_hours(entry, now),
        machine_type=str(details.get("machine_type") or "") or None,
        zone=str(details.get("zone") or "") or None,
        health_status=str(details.get("status") or "") or None,
        boot_disk_name=str(details.get("boot_disk_name") or "") or None,
        labels=labels_dict,
        managed_by=labels_dict.get("managed-by") if labels_dict else None,
        composite_health_status=_composite_health_status(
            entry,
            hb_age,
            control_plane_running=control_plane_running,
            umbrella=target.umbrella,
            object_delta=object_delta,
        ),
        started_at=entry.started_at or None,
        completed_at=entry.completed_at,
        last_heartbeat_at=entry.last_heartbeat_at or None,
        reap_verdict=orphan.verdict if orphan is not None else None,
        grace_hours=DEFAULT_GRACE_HOURS if orphan is not None else None,
        stopped_age_hours=orphan.stopped_age_hours if orphan is not None else None,
        monthly_disk_usd=orphan.monthly_disk_usd if orphan is not None else None,
        cpu_pct=entry.cpu_pct,
        mem_pct=entry.mem_pct,
        mem_slope=entry.mem_slope,
        disk_pct=entry.disk_pct,
        image_digest=entry.image_digest or None,
        git_commit=entry.git_commit or None,
    )


# ---------------------------------------------------------------------------
# Cloud Run job items (dynamic live census + registry classification hints)
# ---------------------------------------------------------------------------


def _cloud_run_status_for(
    target: DeploymentTarget, by_job: dict[str, CloudRunExecutionStatus]
) -> CloudRunExecutionStatus | None:
    """Find the live execution status for a registered job stem (suffix match).

    Live Cloud Run job names carry an env prefix (``prd-``) + asset_group suffix;
    the registry stores the stem. Match a live name whose tail equals the stem (or
    that contains the stem) so an env/ag-decorated live name binds to its target.
    """
    stem = target.name
    # Exact stem hit first (job named exactly the stem), then suffix/contains.
    if stem in by_job:
        return by_job[stem]
    for job_name, status in by_job.items():
        if job_name == stem or job_name.endswith(f"-{stem}") or stem in job_name:
            return status
    return None


def _match_registered_job(job_name: str) -> DeploymentTarget | None:
    """Bind a LIVE Cloud Run job name to its classified registry stem — a HINT,
    not an allow-list. Exact stem match first, then suffix/contains (the same
    rule ``_cloud_run_status_for`` applies in the other direction). ``None`` when
    no registry entry matches — the caller derives a classification instead of
    dropping the job from the census.
    """
    for target in CLOUD_RUN_JOBS:
        if target.name == job_name:
            return target
    for target in CLOUD_RUN_JOBS:
        stem = target.name
        if job_name.endswith(f"-{stem}") or stem in job_name:
            return target
    return None


def _classify_live_cloud_run_job(job_name: str) -> DeploymentTarget:
    """Classify a LIVE Cloud Run job — the registry stem as a HINT, else the
    honest BATCH default. Mirrors the VM inventory's unregistered-prefix degrade
    (``_DEFAULT_LIFECYCLE``): the registry's own docstring classification note is
    "audits / consolidator / catalogue / ... -> BATCH", so an off-pattern job is
    overwhelmingly infra/audit — classify it, never hide it.
    """
    registered = _match_registered_job(job_name)
    if registered is not None:
        return registered
    return classify_deployment_target(
        job_name,
        lifecycle_class=LifecycleClass.EPHEMERAL_BATCH.value,
        cloud=DeploymentCloud.GCP,
        kind=DeploymentKind.CLOUD_RUN_JOB,
    )


def _cloud_run_item_for_live_job(job_name: str, live: CloudRunExecutionStatus) -> DeploymentItem:
    """Build an inventory item for ONE live Cloud Run job (the dynamic census path).

    The wire ``name`` is the actual live job name, not the registry stem — an
    off-pattern job (no registry match) still gets its own row.
    """
    target = _classify_live_cloud_run_job(job_name)
    # A live job that binds to a CLOUD_RUN_JOBS registry entry is deployment-api-launched; an
    # off-pattern live job with no registry hint is adhoc (the job-kind analogue of an unmanaged VM).
    launched_by = LAUNCHED_BY_DEPLOYMENT_API if _match_registered_job(job_name) is not None else LAUNCHED_BY_ADHOC
    return DeploymentItem(
        name=job_name,
        kind=target.kind.value,
        umbrella=target.umbrella.value,
        cloud=target.cloud.value,
        service=target.service,
        asset_group=target.asset_group,
        status=live.status,
        last_run_at=live.last_run_at,
        exit_code=live.exit_code,
        heartbeat_age_seconds=None,
        captured_progress=None,
        run_log_uri=live.log_uri,
        launched_by=launched_by,
        region=live.region or None,  # multi-region census surfaces which region the job lives in
    )


def _cloud_run_item(target: DeploymentTarget, by_job: dict[str, CloudRunExecutionStatus]) -> DeploymentItem:
    """Build a registry-driven inventory item — the DEGRADED-path fallback only,
    used when the live Cloud Run job list itself failed (empty ``by_job``), so the
    census still shows the classified registry with status="unknown" rather than
    going empty.
    """
    live = _cloud_run_status_for(target, by_job)
    if live is None:
        return DeploymentItem(
            name=target.name,
            kind=target.kind.value,
            umbrella=target.umbrella.value,
            cloud=target.cloud.value,
            service=target.service,
            asset_group=target.asset_group,
            status="unknown",
            last_run_at=None,
            exit_code=None,
            heartbeat_age_seconds=None,
            captured_progress=None,
            run_log_uri="",
            launched_by=LAUNCHED_BY_DEPLOYMENT_API,  # driven from the CLOUD_RUN_JOBS registry
        )
    return DeploymentItem(
        name=target.name,
        kind=target.kind.value,
        umbrella=target.umbrella.value,
        cloud=target.cloud.value,
        service=target.service,
        asset_group=target.asset_group,
        status=live.status,
        last_run_at=live.last_run_at,
        exit_code=live.exit_code,
        heartbeat_age_seconds=None,
        captured_progress=None,
        run_log_uri=live.log_uri,
        launched_by=LAUNCHED_BY_DEPLOYMENT_API,  # driven from the CLOUD_RUN_JOBS registry
    )


# ---------------------------------------------------------------------------
# GCP Cloud Functions (gen2) census — existence + config only (WS-B). Gen2
# functions run on Cloud Run underneath, so umbrella is always NONE (same
# precedent as ECS_SERVICE / CLOUD_RUN_SERVICE — no live/batch/paper phase).
# ---------------------------------------------------------------------------


def _cloud_function_item(status: CloudFunctionStatus) -> DeploymentItem:
    """Build an inventory item for one live GCP Cloud Function (gen2).

    Existence + config only (per WS-B scope) — always classified directly (no
    ``classify_deployment_target`` lifecycle_class needed), mirroring the ECS
    service precedent for kinds with no live/batch/paper phase.
    """
    return DeploymentItem(
        name=status.name,
        kind=DeploymentKind.CLOUD_FUNCTION.value,
        umbrella=DeploymentUmbrella.NONE.value,
        cloud=DeploymentCloud.GCP.value,
        service=status.name,
        asset_group="",
        status=status.status,
        last_run_at=status.last_updated_at,
        exit_code=None,
        heartbeat_age_seconds=None,
        captured_progress=None,
        run_log_uri="",
        launched_by=LAUNCHED_BY_CONTROL_PLANE,  # managed platform infra, no registry entry
    )


# ---------------------------------------------------------------------------
# Service-health sub-taxonomy (parent plan D.3) — ECS / Cloud Run service
# composite status. Services have no manifest/object-delta (they're not data
# producers), so they get a SEPARATE 4-state set from the VM 7-state taxonomy.
# The pure classifiers live in ``_service_health`` (imported + re-exported at the
# top of this module — shared with the AWS row builder in ``_aws_deployments``,
# where a reverse import would cycle) and are wired into the live service rows
# below (``_cloud_run_service_item`` + ``_ecs_service_item``).
# ---------------------------------------------------------------------------


# ---------------------------------------------------------------------------
# Cloud Run service items (live census — no live/batch/paper phase, Open-Q1)
# ---------------------------------------------------------------------------


def _cloud_run_service_item(status: CloudRunServiceStatus) -> DeploymentItem:
    """Build an inventory item for ONE live Cloud Run **service**.

    Services (``deployment-api``, ``market-data-query``, ``alerting``, ...) have no
    live/batch/paper phase — the wire ``umbrella`` is ``DeploymentUmbrella.NONE``
    (Open-Q1, 2026-07-09: no PLATFORM/INFRA umbrella was added; the Kind column
    alone tells the operator "this is a service"). ``service``/``asset_group``
    still derive via the single ``classify_deployment_target`` resolver — a
    nominal ``LONG_LIVED_LIVE`` lifecycle_class only satisfies the resolver's
    umbrella requirement; that resolved umbrella is discarded in favour of
    ``DeploymentUmbrella.NONE`` below (the resolver has no NONE lifecycle_class
    mapping to derive it from — see ``UMBRELLA_FOR_LIFECYCLE_CLASS``).

    ``last_modified_at`` carries ``status.last_deployed_at`` (the service's ``update_time``/
    ``create_time`` — a Tier-0 free win off the same list call, closing the audit-found asymmetry
    vs ``ECS_SERVICE``): ``DeploymentItem.last_modified_at`` is already the SSOT field for
    "deploy/modify time, distinct from last_run_at/last-invoke" (see its docstring — the same role
    it plays for AWS Lambda), so this reuses it rather than adding a parallel field with identical
    meaning. Feeds the always-on-kind sort-last + proxy-timestamp UI treatment (decision 2).
    """
    target = classify_deployment_target(
        status.name,
        lifecycle_class=LifecycleClass.LONG_LIVED_LIVE.value,
        cloud=DeploymentCloud.GCP,
        kind=DeploymentKind.CLOUD_RUN_SERVICE,
    )
    return DeploymentItem(
        name=status.name,
        kind=target.kind.value,
        umbrella=DeploymentUmbrella.NONE.value,
        cloud=target.cloud.value,
        service=target.service,
        asset_group=target.asset_group,
        status="running" if status.ready else "pending",
        # D.3 service sub-taxonomy (serving/scaled-to-zero/dead/degraded) — the composite
        # health chip. A ready service with min-instances > 0 is serving; ready + min 0 is an
        # idle scale-to-zero (neutral, not red); a revision that failed to go ready is dead.
        composite_health_status=cloud_run_service_health_status(
            ready=status.ready, min_instance_count=status.min_instance_count
        ),
        last_run_at=None,
        last_modified_at=status.last_deployed_at,
        exit_code=None,
        heartbeat_age_seconds=None,
        captured_progress=None,
        run_log_uri="",
        launched_by=LAUNCHED_BY_CONTROL_PLANE,  # managed platform service, no registry entry
        revision=status.revision,
        region=status.region,
    )


# Raw GCE instance status -> inventory wire status for an UNMANAGED VM (no registry entry, so no
# heartbeat-derived _vm_status to lean on). RUNNING is live; the provisioning states are pending;
# every stop/terminate/suspend state collapses to "stopped" (gone); anything else is honest "unknown".
_GCE_STATUS_TO_WIRE: dict[str, str] = {
    "RUNNING": "running",
    "PROVISIONING": "pending",
    "STAGING": "pending",
    "STOPPING": "stopped",
    "STOPPED": "stopped",
    "SUSPENDING": "stopped",
    "SUSPENDED": "stopped",
    "REPAIRING": "unknown",
    "TERMINATED": "stopped",
}


def _unmanaged_vm_item(
    name: str,
    details: dict[str, object],
    now: datetime,
    *,
    disk_details: dict[str, dict[str, object]] | None = None,
    addresses: dict[str, dict[str, object]] | None = None,
    orphan: OrphanEntry | None = None,
) -> DeploymentItem:
    """Build an inventory row for a LIVE GCE instance that has NO deployment-registry entry.

    The full-estate census (WS-D) unions the registry with the live GCE aggregated-list so an
    unregistered instance — an agent/operator ad-hoc launch, or a long-lived control-plane VM — is
    never invisible in the cockpit (the exact "stranded VM chilling on our money" case). It carries
    ONLY the live GCE state the aggregated-list already fetched: the raw status (surfaced verbatim
    via ``health_status``), machine_type/zone/labels/boot-disk, and an ``uptime_hours`` derived from
    the instance ``creation_timestamp`` (so a 16-day zombie reads its true age). Registry-derived
    fields (heartbeat / rows / exit_code / composite health) honestly stay ``None`` — there is no
    registry entry to source them from. Classification degrades to a minimal NONE-umbrella row
    (never hidden) when the name can't be resolved. Provenance is ``control-plane`` for a managed
    out-of-band prefix, else ``adhoc`` (reconciliation's UNKNOWN set).
    ``orphan`` — see ``_vm_item``'s docstring; same orphans-SSOT join, None when running.
    """
    raw_status = str(details.get("status") or "")
    try:
        target = classify_vm_target(name)
        umbrella, service, asset_group = target.umbrella.value, target.service, target.asset_group
    except UnclassifiedDeploymentError:
        umbrella, service, asset_group = DeploymentUmbrella.NONE.value, name, ""
    created = _parse_iso(str(details.get("creation_timestamp") or "") or None)
    uptime = (now - created).total_seconds() / 3600.0 if created is not None and raw_status == "RUNNING" else None
    labels = details.get("labels")
    labels_dict = cast(dict[str, str], labels) if labels else None
    has_unreleased, unreleased = detect_unreleased_resources(
        name, details, disk_details or {}, addresses or {}, is_running=raw_status == "RUNNING"
    )
    return DeploymentItem(
        name=name,
        kind=DeploymentKind.VM.value,
        umbrella=umbrella,
        cloud=DeploymentCloud.GCP.value,
        service=service,
        asset_group=asset_group,
        status=_GCE_STATUS_TO_WIRE.get(raw_status, "unknown"),
        last_run_at=created.isoformat() if created is not None else None,
        run_log_uri="",
        launched_by=LAUNCHED_BY_CONTROL_PLANE if is_control_plane_vm(name) else LAUNCHED_BY_ADHOC,
        has_unreleased_resources=has_unreleased,
        unreleased_resources=unreleased or None,
        uptime_hours=max(0.0, uptime) if uptime is not None else None,
        machine_type=str(details.get("machine_type") or "") or None,
        zone=str(details.get("zone") or "") or None,
        health_status=raw_status or None,
        boot_disk_name=str(details.get("boot_disk_name") or "") or None,
        labels=labels_dict,
        managed_by=labels_dict.get("managed-by") if labels_dict else None,
        reap_verdict=orphan.verdict if orphan is not None else None,
        grace_hours=DEFAULT_GRACE_HOURS if orphan is not None else None,
        stopped_age_hours=orphan.stopped_age_hours if orphan is not None else None,
        monthly_disk_usd=orphan.monthly_disk_usd if orphan is not None else None,
    )

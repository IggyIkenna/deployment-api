"""Full-census assembly: multi-provider fan-out, orphan/cost joins, stale-while-revalidate cache.

Split from ``routes/deployments_inventory.py`` (pure code motion; plan:
``deployment_api_qg_size_gate_debt_2026_07_30.md``). Patched module-level collaborators
(``_cfg`` / ``get_vm_instance_details`` / ``latest_execution_by_job`` / ``list_cloud_functions`` /
``list_cloud_run_services`` / ``list_scheduler_jobs`` / ``list_gcp_region_names`` /
``get_disk_details`` / ``list_reserved_addresses`` / ``list_unattached_disk_names`` /
``object_delta_for_asset_group`` / ``CostObservabilityService`` / ``_load_registry_entries`` /
``_load_aws_items`` / ``_PROVIDER_CENSUS_TIMEOUT_SEC`` — the census "seams"
``tests/mocks.py``'s ``patch_inventory_secondary_census`` documents) are resolved through the
facade module (``_inv``) at call time so the existing test patch surface
``deployment_api.routes.deployments_inventory.<name>`` keeps intercepting.
"""

from __future__ import annotations

import time
from concurrent.futures import Future, ThreadPoolExecutor
from concurrent.futures import TimeoutError as FutureTimeoutError
from datetime import UTC, datetime

from deployment_service.cloud_run_job_registry import CLOUD_RUN_JOBS
from deployment_service.deployment_classification import UnclassifiedDeploymentError
from fastapi import HTTPException
from unified_api_contracts import DeploymentCloud, DeploymentKind, DeploymentUmbrella
from unified_trading_library import DeploymentRegistryEntry

import deployment_api.routes.deployments_inventory as _inv
from deployment_api.routes._cloud_run_executions import CloudRunExecutionStatus
from deployment_api.routes._cloud_run_services import CloudRunServiceStatus
from deployment_api.routes._cloud_scheduler import SchedulerJobStatus
from deployment_api.routes._fleet_inventory import DEFAULT_GRACE_HOURS, build_orphan_inventory
from deployment_api.routes._fleet_types import OrphanEntry
from deployment_api.routes._gcp_cloud_functions import CloudFunctionStatus
from deployment_api.routes._leaked_resources import orphaned_disk, orphaned_static_ip
from deployment_api.routes.deployments_inventory import (
    _ALL_AWS_REGIONS,  # pyright: ignore[reportPrivateUsage]
    _CONFIGURED_AWS_REGIONS,  # pyright: ignore[reportPrivateUsage]
    _CONFIGURED_GCP_REGIONS,  # pyright: ignore[reportPrivateUsage]
    _GCP_TO_AWS_REGION,  # pyright: ignore[reportPrivateUsage]
    LAUNCHED_BY_CONTROL_PLANE,
    LAUNCHED_BY_UNKNOWN,
    DeploymentItem,
    _census_or_degrade,  # pyright: ignore[reportPrivateUsage]
    _census_pool,  # pyright: ignore[reportPrivateUsage]
    _inventory_cache,  # pyright: ignore[reportPrivateUsage]
    _inventory_lock,  # pyright: ignore[reportPrivateUsage]
    _inventory_refresh_pool,  # pyright: ignore[reportPrivateUsage]
    _inventory_refreshing,  # pyright: ignore[reportPrivateUsage]
    _vm_entry_by_name_cache,  # pyright: ignore[reportPrivateUsage]
    _vm_entry_by_name_lock,  # pyright: ignore[reportPrivateUsage]
    logger,
)
from deployment_api.routes.deployments_inventory._classification import (
    _alert_on_health_transition,  # pyright: ignore[reportPrivateUsage]
    _classify_vm,  # pyright: ignore[reportPrivateUsage]
    _cloud_function_item,  # pyright: ignore[reportPrivateUsage]
    _cloud_run_item,  # pyright: ignore[reportPrivateUsage]
    _cloud_run_item_for_live_job,  # pyright: ignore[reportPrivateUsage]
    _cloud_run_service_item,  # pyright: ignore[reportPrivateUsage]
    _prune_stale_alert_state,  # pyright: ignore[reportPrivateUsage]
    _unmanaged_vm_item,  # pyright: ignore[reportPrivateUsage]
    _vm_item,  # pyright: ignore[reportPrivateUsage]
)
from deployment_api.routes.deployments_inventory._mock_data import (
    _mock_inventory,  # pyright: ignore[reportPrivateUsage]
)

__all__ = [
    "_attach_costs",
    "_aws_instance_id_from_resource_id",
    "_aws_regions_for_scope",
    "_batched_object_deltas",
    "_compute_inventory",
    "_gcp_regions_for_scope",
    "_kick_background_refresh",
    "_load_inventory",
    "_multi_region_functions",
    "_multi_region_jobs",
    "_multi_region_scheduler",
    "_multi_region_services",
    "_orphaned_resource_items",
    "_refresh_inventory",
    "_scheduler_item",
    "_store_inventory",
    "build_inventory",
]

# Concurrency for the per-distinct-asset_group object-delta manifest reads (~a handful of
# asset_groups per census). Fanning them out keeps the cold census under the 45 s provider bound.
_OBJECT_DELTA_WORKERS = 8
_INVENTORY_TTL_SEC = 45.0


def _batched_object_deltas(vm_entries: list[DeploymentRegistryEntry], now: datetime) -> dict[str, int | None]:
    """ONE manifest object-delta lookup per DISTINCT BATCH-umbrella asset_group, never per VM.

    A VM census can carry hundreds of entries sharing a handful of asset_groups — re-deriving
    ``object_delta_for_asset_group`` (a GCS manifest-index read) once per VM would be an N+1
    against the manifest and would violate WS-D.0 principle 5 (zero new bucket walks). This
    collects the distinct (BATCH-umbrella, running, asset_group-carrying) set first, then reads
    each asset_group's delta exactly once; shard-level isolation — one asset_group's read failure
    (already caught inside ``object_delta_for_asset_group``) never drops another's entry.
    """
    asset_groups: set[str] = set()
    for entry in vm_entries:
        if entry.status != "running":
            continue
        try:
            target = _classify_vm(entry.vm_name)
        except UnclassifiedDeploymentError:
            continue
        if target.umbrella == DeploymentUmbrella.BATCH and target.asset_group:
            asset_groups.add(target.asset_group)
    if not asset_groups:
        return {}

    # Each asset_group's delta is an independent GCS manifest-index read; running them serially
    # (one per distinct asset_group) routinely blew past the 45 s per-provider census bound on a
    # cold cycle, degrading object-delta to empty and pushing the BATCH working/stalled composite
    # to "unknown". Fan them out — the read is I/O-bound (GIL released), so this is ~max(one read)
    # not their sum. object_delta_for_asset_group already catches its own errors (returns
    # (None, reason)), so no per-ag read can crash the map.
    def _delta(asset_group: str) -> tuple[str, int | None]:
        return asset_group, _inv.object_delta_for_asset_group(asset_group, now)[0]

    workers = min(_OBJECT_DELTA_WORKERS, len(asset_groups))
    with ThreadPoolExecutor(max_workers=workers, thread_name_prefix="obj-delta") as pool:
        return dict(pool.map(_delta, asset_groups))


def build_inventory(
    vm_entries: list[DeploymentRegistryEntry],
    cloud_run_status: dict[str, CloudRunExecutionStatus],
    now: datetime,
    vm_details_by_name: dict[str, dict[str, object]] | None = None,
    cloud_run_services: list[CloudRunServiceStatus] | None = None,
    object_deltas: dict[str, int | None] | None = None,
    disk_details: dict[str, dict[str, object]] | None = None,
    addresses: dict[str, dict[str, object]] | None = None,
) -> list[DeploymentItem]:
    """Assemble the full unified inventory (VMs + Cloud Run jobs + Cloud Run services).

    A VM whose name cannot be classified is logged + skipped (never crashes the
    inventory) — the no-silent-default classifier raises, we degrade per-row.

    ``vm_details_by_name`` is the GCE aggregated-list join (Tier-0 free wins —
    machine_type/zone/labels/boot-disk/raw status), reused for the control-plane
    confirmation feeding the D.3 composite ``dead``/``hung`` states: a VM is
    "running per GCP right now" only when its raw status in the join is exactly
    ``"RUNNING"`` — mere PRESENCE in the join is not enough, since GCE keeps a
    stopping/stopped/terminated instance visible in the aggregated-list for a
    while (present but definitely not running). ``None`` (a caller with no join
    to offer, e.g. pure-classification tests or a degraded path) degrades those
    states honestly (see ``_composite_health_status``) rather than guessing; an
    explicit ``{}`` (a real, empty GCE census) DOES confirm every entry as
    not-running.

    A populated ``vm_details_by_name`` ALSO drives the full-estate census: every live GCE
    instance in the join with no matching ``vm_entries`` name is emitted as an ``unmanaged``
    row (``_unmanaged_vm_item``, ``launched_by=adhoc``/``control-plane``) so an unregistered VM
    is never invisible. This is the same (registry vs live-GCE) union ``fleet_reconciliation``
    computes — reused, not rebuilt. ``None``/``{}`` add no unmanaged rows.

    ``object_deltas`` is the BATCH-umbrella asset_group -> manifest object-delta
    map (``_batched_object_deltas``) — feeds the composite classifier's
    ``stalled``. ``None`` (the default, and every existing caller/test) degrades
    ``stalled`` to the honest ``"unknown"`` it already fell back to; this
    function stays a pure, I/O-free classifier either way — the manifest read
    happens in the caller (``_compute_inventory``), never inside this loop.

    Cloud Run jobs census the LIVE job list (``cloud_run_status`` keys, already
    fetched by ``latest_execution_by_job``'s ``run_v2.JobsClient.list_jobs`` — no
    new API call) — ``CLOUD_RUN_JOBS`` is a classification HINT, not an allow-list,
    so an off-pattern job (no registry match) still gets a row instead of hiding.
    Only when the live list itself is empty (the GCP call failed) does the census
    degrade to the static registry with status="unknown" (never an empty census).

    Cloud Run services (``cloud_run_services``, optional — omitted/``None`` yields
    zero service rows, never an error) census the LIVE service list
    (``list_cloud_run_services``'s ``run_v2.ServicesClient.list_services``); a
    per-service classification failure is logged + skipped, same shard-level
    isolation as the VM loop above — one bad service name never blocks the rest
    of the census.

    Orphan/idle-spend join (Fleet-tab consolidation) — every STOPPED/SUSPENDED/TERMINATED VM in
    ``vm_details_by_name`` gets its ``reap_verdict``/``grace_hours``/``stopped_age_hours``/
    ``monthly_disk_usd`` populated from the SAME orphans SSOT (``_fleet_inventory.build_orphan_inventory``)
    the ``/api/fleet/orphans`` endpoint uses — computed ONCE here (pure, no new GCE call; reuses the
    already-fetched ``vm_details_by_name``/``disk_details``), never a second estimator. A running VM
    (or ``None``/``{}`` census) simply has no entry in the lookup, so those fields stay honestly ``None``.
    """
    details_by_name = vm_details_by_name or {}
    orphan_by_name: dict[str, OrphanEntry] = {}
    if vm_details_by_name:
        orphan_inventory = build_orphan_inventory(vm_details_by_name, disk_details or {}, now, DEFAULT_GRACE_HOURS)
        orphan_by_name = {o.name: o for o in orphan_inventory.orphans}
    items: list[DeploymentItem] = []
    for entry in vm_entries:
        try:
            control_plane_running = (
                str(details_by_name.get(entry.vm_name, {}).get("status", "")) == "RUNNING"
                if vm_details_by_name is not None
                else None
            )
            items.append(
                _vm_item(
                    entry,
                    now,
                    details_by_name.get(entry.vm_name),
                    control_plane_running=control_plane_running,
                    object_deltas=object_deltas,
                    disk_details=disk_details,
                    addresses=addresses,
                    orphan=orphan_by_name.get(entry.vm_name),
                )
            )
        except UnclassifiedDeploymentError as exc:
            logger.warning("inventory: skipping unclassifiable VM %r: %s", entry.vm_name, exc)
    # Full-estate census (WS-D): union the registry rows above with the live GCE aggregated-list so
    # EVERY live instance gets a row — an unregistered VM (an ad-hoc launch or an out-of-band
    # control-plane VM) becomes an `unmanaged` row instead of being invisible. This reuses the SAME
    # (registry vs live-GCE) union fleet_reconciliation computes — the registered set is exactly the
    # vm_entries names, the live set is the aggregated-list join already passed in — so there is no
    # second census. Only a REAL join adds rows: None (no join offered — pure-classification paths)
    # and {} (a real, empty GCE census) both add nothing.
    if vm_details_by_name:
        registered_names = {entry.vm_name for entry in vm_entries}
        for unmanaged_name in sorted(set(vm_details_by_name) - registered_names):
            items.append(
                _unmanaged_vm_item(
                    unmanaged_name,
                    vm_details_by_name[unmanaged_name],
                    now,
                    disk_details=disk_details,
                    addresses=addresses,
                    orphan=orphan_by_name.get(unmanaged_name),
                )
            )
    if cloud_run_status:
        for job_name, status in cloud_run_status.items():
            items.append(_cloud_run_item_for_live_job(job_name, status))
    else:
        for target in CLOUD_RUN_JOBS:
            items.append(_cloud_run_item(target, cloud_run_status))
    for service_status in cloud_run_services or []:
        try:
            items.append(_cloud_run_service_item(service_status))
        except UnclassifiedDeploymentError as exc:
            logger.warning("inventory: skipping unclassifiable Cloud Run service %r: %s", service_status.name, exc)
    return items


def _scheduler_item(entry: SchedulerJobStatus) -> DeploymentItem:
    """Build an inventory row for one Cloud Scheduler job (WS-D #9) — the on-time / OVERDUE signal.

    Kind ``SCHEDULER``; ``composite_health_status`` carries the verdict (``overdue`` / ``on-time`` /
    ``paused``) so the UI slots it into the Health column (Cloud Run job rows carry none today).
    ``service`` is the target it triggers so the UI can cross-link to that job's row.
    """
    if entry.overdue:
        status, health = "failed", "overdue"
    elif entry.state == "ENABLED":
        status, health = "running", "on-time"
    else:
        status, health = "stopped", "paused"
    return DeploymentItem(
        name=entry.name,
        kind="SCHEDULER",
        umbrella=DeploymentUmbrella.NONE.value,
        cloud=DeploymentCloud.GCP.value,
        service=entry.target or entry.name,
        asset_group="",
        status=status,
        last_run_at=entry.last_attempt_at,
        run_log_uri="",
        launched_by=LAUNCHED_BY_CONTROL_PLANE,  # Cloud Scheduler is managed infra
        composite_health_status=health,
        region=entry.region or None,
    )


def _multi_region_scheduler(project_id: str, regions: tuple[str, ...], now: datetime) -> list[SchedulerJobStatus]:
    """Cloud Scheduler jobs across every region in ``regions``, concatenated (per-region isolation)."""
    entries: list[SchedulerJobStatus] = []
    if not regions:
        return entries

    def _fetch(region: str) -> list[SchedulerJobStatus]:
        return _inv.list_scheduler_jobs(project_id, region, now)

    with ThreadPoolExecutor(max_workers=min(8, len(regions)), thread_name_prefix="scheduler-region") as pool:
        for region_list in pool.map(_fetch, regions):
            entries.extend(region_list)
    return entries


def _orphaned_resource_items(
    disk_details: dict[str, dict[str, object]],
    unattached_disk_names: set[str],
    addresses: dict[str, dict[str, object]],
) -> list[DeploymentItem]:
    """First-class rows for truly-orphaned resources with NO owning VM (WS-D #7): unattached
    persistent disks + reserved static IPs with no user.

    ``GET /api/fleet/orphans`` is VM-keyed and misses these entirely, yet each still bills. Emitted
    as ``kind=DISK`` / ``kind=STATIC_IP``, ``launched_by=unknown`` (no VM to attribute provenance),
    flagged ``has_unreleased_resources=True`` (the row IS the leak) with its inferred monthly cost,
    so the estate stranded-cost total picks it up. Disjoint from the #4 VM-attached leaks — a disk
    attached to a stopped VM has a ``users`` entry (not unattached) and an attached IP has a
    ``users`` self-link, so neither is double-counted here.
    """
    items: list[DeploymentItem] = []
    for disk_name in sorted(unattached_disk_names):
        disk = disk_details.get(disk_name, {})
        size_raw = disk.get("size_gb")
        size_gb = int(size_raw) if isinstance(size_raw, int) else None
        type_raw = disk.get("type")
        disk_type = str(type_raw) if isinstance(type_raw, str) and type_raw else None
        items.append(
            DeploymentItem(
                name=disk_name,
                kind="DISK",
                umbrella=DeploymentUmbrella.NONE.value,
                cloud=DeploymentCloud.GCP.value,
                service=disk_name,
                asset_group="",
                status="stopped",  # allocated + idle (no running workload) — the orphaned state
                run_log_uri="",
                launched_by=LAUNCHED_BY_UNKNOWN,
                has_unreleased_resources=True,
                unreleased_resources=[orphaned_disk(disk_name, size_gb, disk_type)],
            )
        )
    for addr_name, addr in sorted(addresses.items()):
        if addr.get("users"):  # attached to something → a VM's leaked resource (#4), not an orphan
            continue
        items.append(
            DeploymentItem(
                name=addr_name,
                kind="STATIC_IP",
                umbrella=DeploymentUmbrella.NONE.value,
                cloud=DeploymentCloud.GCP.value,
                service=addr_name,
                asset_group="",
                status="stopped",
                run_log_uri="",
                launched_by=LAUNCHED_BY_UNKNOWN,
                has_unreleased_resources=True,
                unreleased_resources=[orphaned_static_ip(addr_name)],
                region=str(addr.get("region") or "") or None,
            )
        )
    return items


def _gcp_regions_for_scope(region_scope: str, project_id: str) -> tuple[str, ...]:
    """The GCP regions to census for this scope: the configured default (``""``), every compute region
    (``"ALL"`` — falling back to the configured set if the region-list read fails), or a single
    caller-picked region."""
    if region_scope == "ALL":
        names = _inv.list_gcp_region_names(project_id)
        return tuple(names) if names else _CONFIGURED_GCP_REGIONS
    if not region_scope:
        return _CONFIGURED_GCP_REGIONS
    return (region_scope,)


def _aws_regions_for_scope(region_scope: str) -> tuple[str, ...]:
    """The AWS regions to census for this scope: the primary set (``""``), the curated full set
    (``"ALL"``), or the GCP-picked region's best-effort AWS equivalent (``_GCP_TO_AWS_REGION`` —
    primary set when unpaired)."""
    if region_scope == "ALL":
        return _ALL_AWS_REGIONS
    if not region_scope:
        return _CONFIGURED_AWS_REGIONS
    aws_equiv = _GCP_TO_AWS_REGION.get(region_scope)
    return (aws_equiv,) if aws_equiv else _CONFIGURED_AWS_REGIONS


def _multi_region_jobs(project_id: str, regions: tuple[str, ...]) -> dict[str, CloudRunExecutionStatus]:
    """Cloud Run jobs across every region in ``regions``, merged (region carried on each status).

    Each per-region ``latest_execution_by_job`` already degrades to ``{}`` on its own error, so a
    region that is down / unsupported never blocks the others (per-region honest isolation). A
    short-name collision across regions is vanishingly rare (all real jobs are single-region today);
    the later region wins, and each row carries its ``region`` so the collision is visible.
    """
    merged: dict[str, CloudRunExecutionStatus] = {}
    if not regions:
        return merged

    def _fetch(region: str) -> dict[str, CloudRunExecutionStatus]:
        return _inv.latest_execution_by_job(project_id, region=region)

    with ThreadPoolExecutor(max_workers=min(8, len(regions)), thread_name_prefix="cr-jobs-region") as pool:
        for region_map in pool.map(_fetch, regions):
            merged.update(region_map)
    return merged


def _multi_region_services(project_id: str, regions: tuple[str, ...]) -> list[CloudRunServiceStatus]:
    """Cloud Run services across every region in ``regions``, concatenated (per-region isolation)."""
    services: list[CloudRunServiceStatus] = []
    if not regions:
        return services

    def _fetch(region: str) -> list[CloudRunServiceStatus]:
        return _inv.list_cloud_run_services(project_id, region=region)

    with ThreadPoolExecutor(max_workers=min(8, len(regions)), thread_name_prefix="cr-svc-region") as pool:
        for region_list in pool.map(_fetch, regions):
            services.extend(region_list)
    return services


def _multi_region_functions(project_id: str, regions: tuple[str, ...]) -> dict[str, CloudFunctionStatus]:
    """Cloud Functions across every region in ``regions``, merged (per-region isolation)."""
    merged: dict[str, CloudFunctionStatus] = {}
    if not regions:
        return merged

    def _fetch(region: str) -> dict[str, CloudFunctionStatus]:
        return _inv.list_cloud_functions(project_id, region=region)

    with ThreadPoolExecutor(max_workers=min(8, len(regions)), thread_name_prefix="cf-region") as pool:
        for region_map in pool.map(_fetch, regions):
            merged.update(region_map)
    return merged


def _compute_inventory(now: datetime, cloud: str | None, region_scope: str = "") -> list[DeploymentItem]:
    """Build the live inventory (registry VMs + Cloud Run executions + AWS) — the cold path.

    ``region_scope`` selects the census breadth: ``""`` the configured default, ``"ALL"`` every region
    (the periodic surprise-check), or a single GCP region + its AWS equivalent. GCE VMs / disks / IPs
    are all-region aggregated regardless — only the regional Cloud Run / functions / scheduler APIs
    honour the scope, so a specific-region view still lists every VM.
    """
    want_gcp = cloud is None or cloud.upper() == DeploymentCloud.GCP.value
    want_aws = cloud is None or cloud.upper() == DeploymentCloud.AWS.value

    items: list[DeploymentItem] = []

    # Fan out every wanted provider census concurrently, each resolved through
    # _census_or_degrade (wall-clock bounded, honest per-kind empty on hang/error). A single
    # slow or hung provider degrades to an empty census for its OWN kind instead of blocking
    # the whole inventory — the cold path is ~max(slowest census) not their sum, and never
    # exceeds _PROVIDER_CENSUS_TIMEOUT_SEC per provider.
    aws_regions = _aws_regions_for_scope(region_scope)
    f_aws: Future[tuple[list[DeploymentItem], dict[str, str]]] | None = (
        _census_pool.submit(_inv._load_aws_items, now, aws_regions) if want_aws else None  # pyright: ignore[reportPrivateUsage]
    )

    if want_gcp:
        project_id = _inv._cfg.require_gcp_project_id()  # pyright: ignore[reportPrivateUsage]
        # GCE VMs / disks / addresses use all-region aggregated lists already; only the regional
        # Cloud Run jobs/services + Cloud Functions APIs need the multi-region fan-out.
        gcp_regions = _gcp_regions_for_scope(region_scope, project_id)
        # DECOUPLED (P2 migration): two independent futures instead of one bundled read.
        #   existence  = the GCE aggregated-list (fast) → drives which live VMs get a row
        #   enrichment = the registry (Firestore-first via resolve_active_registry) → metadata
        # Each degrades on its OWN via _census_or_degrade, so a slow/failed registry read yields
        # [] but the live VMs STILL render from the GCE join (build_inventory's unmanaged-row
        # union). The old bug: ONE future bundled both, so a registry timeout dropped every live
        # VM and blanked the prod tab.
        f_vm_details: Future[dict[str, dict[str, object]]] = _census_pool.submit(
            _inv.get_vm_instance_details, project_id
        )
        f_registry: Future[list[DeploymentRegistryEntry]] = _census_pool.submit(_inv._load_registry_entries, now)  # pyright: ignore[reportPrivateUsage]
        f_jobs = _census_pool.submit(_multi_region_jobs, project_id, gcp_regions)
        f_services = _census_pool.submit(_multi_region_services, project_id, gcp_regions)
        f_functions = _census_pool.submit(_multi_region_functions, project_id, gcp_regions)
        f_scheduler = _census_pool.submit(_multi_region_scheduler, project_id, gcp_regions, now)
        # Disk + reserved-IP maps for leaked-resource detection on non-running VMs — two more
        # aggregated_list reads, each bounded + degrading to an empty map on failure (leak detection
        # then flags nothing rather than fabricating a false leak, and non-running rows report
        # has_unreleased_resources=None honestly).
        f_disks = _census_pool.submit(_inv.get_disk_details, project_id)
        f_addresses = _census_pool.submit(_inv.list_reserved_addresses, project_id)
        # Unattached-disk names → the truly-orphaned DISK rows (#7); same disks aggregated_list
        # class (the ``users`` field). Bounded + degrades to an empty set.
        f_unattached = _census_pool.submit(_inv.list_unattached_disk_names, project_id)

        vm_details_by_name = _census_or_degrade("gcp-vm-details", f_vm_details, {})
        vm_entries = _census_or_degrade("gcp-registry", f_registry, [])
        cloud_run_status = _census_or_degrade("cloud-run-jobs", f_jobs, {})
        cloud_run_services = _census_or_degrade("cloud-run-services", f_services, [])
        cloud_function_status = _census_or_degrade("cloud-functions", f_functions, {})
        scheduler_entries: list[SchedulerJobStatus] = _census_or_degrade("cloud-scheduler", f_scheduler, [])
        disk_details = _census_or_degrade("gcp-disks", f_disks, {})
        addresses = _census_or_degrade("gcp-addresses", f_addresses, {})
        unattached_disks: set[str] = _census_or_degrade("gcp-unattached-disks", f_unattached, set())
        # Object-delta is a manifest lookup keyed off the resolved VM entries — bound it too.
        object_deltas = _census_or_degrade(
            "object-delta", _census_pool.submit(_batched_object_deltas, vm_entries, now), {}
        )

        gcp_items = build_inventory(
            vm_entries,
            cloud_run_status,
            now,
            vm_details_by_name=vm_details_by_name,
            cloud_run_services=cloud_run_services,
            object_deltas=object_deltas,
            disk_details=disk_details,
            addresses=addresses,
        )
        items.extend(gcp_items)

        with _vm_entry_by_name_lock:
            _vm_entry_by_name_cache.clear()
            _vm_entry_by_name_cache.update({e.vm_name: e for e in vm_entries})

        items.extend(_cloud_function_item(status) for status in cloud_function_status.values())

        # Cloud Scheduler rows (#9): the on-time / OVERDUE signal (Health column) per scheduled job.
        items.extend(_scheduler_item(entry) for entry in scheduler_entries)

        # First-class orphaned-resource rows (#7): unattached disks + no-owner reserved static IPs.
        items.extend(_orphaned_resource_items(disk_details, unattached_disks, addresses))

        census_vm_names: set[str] = set()
        for vm_item in gcp_items:
            if vm_item.kind == DeploymentKind.VM.value:
                _alert_on_health_transition(vm_item)
                census_vm_names.add(vm_item.name)
        _prune_stale_alert_state(census_vm_names)

    aws_instance_id_by_name: dict[str, str] = {}
    if f_aws is not None:
        aws_items, aws_instance_id_by_name = _census_or_degrade("aws", f_aws, ([], {}))
        items.extend(aws_items)

    _attach_costs(items, aws_instance_id_by_name)
    return items


def _aws_instance_id_from_resource_id(resource_id: str) -> str | None:
    """Parse the EC2 instance-id out of an AWS CUR ``line_item_resource_id``.

    That column is usually a full ARN (``arn:aws:ec2:<region>:<acct>:instance/i-...``) but some
    exports carry the bare instance-id — handle both. Returns ``None`` for any non-EC2-instance
    resource_id (buckets, other services, GCP's already-short names, ...).
    """
    tail = resource_id.rsplit("instance/", 1)[-1] if "instance/" in resource_id else resource_id
    return tail if tail.startswith("i-") else None


def _attach_costs(items: list[DeploymentItem], aws_instance_id_by_name: dict[str, str] | None = None) -> None:
    """Best-effort: attach the 3 USD cost figures per target by name == billing resource_id.

    Reuses the cost-observability service's CACHED billing window (WS-E) — one aggregation per
    inventory refresh (the inventory itself is cached), not per request. A GCP VM's billing
    ``resource.name`` is its instance name (== the deployment item name); Cloud Run job/service
    names match likewise. AWS ``resource_id`` is an ARN/instance-id that won't match the friendly
    name directly — ``aws_instance_id_by_name`` (the EC2 census's ``{instance_id: Name tag}``, built
    in ``_load_aws_items``) resolves it to the friendly name first so the AWS row joins the same
    way GCP does. No mapping found (unmapped instance, non-EC2 AWS resource, GCP row) → that row
    keeps ``None`` (honest absence, never a fabricated 0). A billing-source failure or
    mock-without-facts leaves every cost field ``None`` — cost NEVER breaks the census.
    """
    try:
        cost_by_resource = _inv.CostObservabilityService().per_resource_daily(days=7)
    except Exception as exc:
        # best-effort enrichment — cost must never break the inventory census.
        logger.warning("[deployments-inventory] cost enrichment skipped (best-effort): %s", exc)
        return
    if aws_instance_id_by_name:
        for resource_id, rc in list(cost_by_resource.items()):
            instance_id = _aws_instance_id_from_resource_id(resource_id)
            friendly_name = aws_instance_id_by_name.get(instance_id) if instance_id else None
            if friendly_name is not None and friendly_name not in cost_by_resource:
                cost_by_resource[friendly_name] = rc
    for item in items:
        rc = cost_by_resource.get(item.name)
        if rc is not None:
            item.cost_actual_usd = rc.actual_usd
            item.cost_avg_7d_usd = rc.avg_7d_usd
            item.cost_projected_24h_usd = rc.projected_24h_usd
            item.cost_basis = rc.cost_basis


def _store_inventory(cache_key: str, items: list[DeploymentItem]) -> None:
    """Atomically record a fresh inventory snapshot for ``cache_key``."""
    with _inventory_lock:
        _inventory_cache[cache_key] = (time.monotonic(), items)


def _refresh_inventory(cache_key: str, cloud: str | None, region_scope: str) -> list[DeploymentItem] | None:
    """Background cache refresh — recompute + store, then clear the in-flight flag.

    Returns the freshly computed items so a cold-path caller can bound-wait on this SAME
    submission (see ``_load_inventory``) instead of double-submitting; returns ``None`` on a
    failed compute (the stale snapshot, if any, is left untouched — never poisoned).
    """
    try:
        items = _compute_inventory(datetime.now(UTC), cloud, region_scope)
        _store_inventory(cache_key, items)
        return items
    except (HTTPException, OSError, ValueError, RuntimeError) as exc:
        # Keep the stale snapshot on a failed refresh — never poison the cache.
        logger.warning("inventory: background refresh for %s failed: %s", cache_key, exc)
        return None
    finally:
        with _inventory_lock:
            _inventory_refreshing.discard(cache_key)


def _kick_background_refresh(
    cache_key: str, cloud: str | None, region_scope: str
) -> Future[list[DeploymentItem] | None] | None:
    """Schedule exactly one background refresh per cache key (stale-while-revalidate).

    Returns the submitted ``Future`` so a cold-path caller can bound-wait on the SAME
    submission; returns ``None`` if a refresh for this key is already in flight (another poll
    already triggered it — never double-submit the same census).
    """
    with _inventory_lock:
        if cache_key in _inventory_refreshing:
            return None
        _inventory_refreshing.add(cache_key)
    return _inventory_refresh_pool.submit(_refresh_inventory, cache_key, cloud, region_scope)


def _load_inventory(now: datetime, cloud: str | None = None, region_scope: str = "") -> list[DeploymentItem]:
    """Load the live inventory, stale-while-revalidate cached for a fast, smooth cockpit.

    GCP items load unless the caller filters ``cloud=aws``; AWS items load unless the
    caller filters ``cloud=gcp`` (so an unset / ``aws`` filter includes the AWS estate).
    Cache policy (mock mode bypasses it — already cheap + deterministic):

    * **Fresh** (< TTL) → served instantly.
    * **Stale** (> TTL) → the stale snapshot is served instantly AND a single background
      refresh is kicked off, so the operator never waits on the slow census after the
      first ever load (the cockpit polls repeatedly → always warm).
    * **Cold** (no snapshot) → the SAME background refresh is kicked off (in-flight guard
      collapses a burst of first-polls to ONE census, not N) and this caller bound-waits on it
      for up to ``_PROVIDER_CENSUS_TIMEOUT_SEC``. A cold census that finishes within the bound
      returns the real items; one that doesn't degrades to an honest empty placeholder — the
      compute keeps running in the background regardless (a thread mid-census can't be
      cancelled) and warms the cache for the next poll, so a freshly-scaled Cloud Run instance
      (``minScale=1``/``maxScale=20``, in-process cache) never blocks a caller past the bound.
    """
    if _inv._cfg.is_mock_mode():  # pyright: ignore[reportPrivateUsage]
        return _mock_inventory(now)

    # The all-regions sweep gets its OWN cache slot so the one-off surprise-check never poisons (or
    # is served from) the default configured-region snapshot.
    cache_key = f"{(cloud or 'all').upper()}|{region_scope or 'CONFIGURED'}"
    cached = _inventory_cache.get(cache_key)
    if cached is not None:
        if (time.monotonic() - cached[0]) >= _INVENTORY_TTL_SEC:
            _kick_background_refresh(cache_key, cloud, region_scope)
        return cached[1]

    future = _kick_background_refresh(cache_key, cloud, region_scope)
    if future is None:
        # Another poll already triggered the cold compute concurrently — don't wait twice;
        # serve an honest empty placeholder, the next poll lands on the now-warm(er) cache.
        return []
    try:
        items = future.result(timeout=_inv._PROVIDER_CENSUS_TIMEOUT_SEC)  # pyright: ignore[reportPrivateUsage]
    except FutureTimeoutError:
        logger.warning(
            "inventory: cold census for %s exceeded %.0fs — degraded to empty placeholder; "
            "background compute continues and will warm the cache",
            cache_key,
            _inv._PROVIDER_CENSUS_TIMEOUT_SEC,  # pyright: ignore[reportPrivateUsage]
        )
        return []
    return items if items is not None else []

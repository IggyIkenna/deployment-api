"""GCE instance utilities for deployment-api.

This module provides utilities for fetching VM instance details from GCP.
It's a lightweight version of deployment-service's gcp_instance_lister.py
adapted for deployment-api's needs.
"""

from __future__ import annotations

import logging
from datetime import UTC, datetime
from typing import cast

from google.cloud import compute_v1

logger = logging.getLogger(__name__)

# Per-RPC deadline (< the inventory census wall-clock of 45 s) so a wedged GCE aggregated-list
# RPC unwinds the census worker on its own instead of leaking it — the except below degrades to
# an empty VM detail map. Keeps the inventory census pool from starving under a persistent hang.
_RPC_TIMEOUT_SEC = 30.0


def list_running_vm_names(project_id: str) -> set[str]:
    """Return the set of VM names currently in ``RUNNING`` state in ``project_id``.

    Uses ``aggregated_list`` so one API call covers every zone. On failure
    returns an empty set + logs a warning.
    """
    try:
        client = compute_v1.InstancesClient()
        request = compute_v1.AggregatedListInstancesRequest(project=project_id)
        running: set[str] = set()
        for _zone, scoped_list in client.aggregated_list(request=request, timeout=_RPC_TIMEOUT_SEC):
            instances = getattr(scoped_list, "instances", None)
            if not instances:
                continue
            for inst in instances:  # pyright: ignore[reportAny]
                inst_typed = cast(object, inst)
                status = str(getattr(inst_typed, "status", ""))
                name = str(getattr(inst_typed, "name", ""))
                if status == "RUNNING" and name:
                    running.add(name)
        logger.info("list_running_vm_names(%s): %d RUNNING VMs", project_id, len(running))
        return running
    except Exception as exc:
        logger.warning("list_running_vm_names(%s) failed: %s", project_id, exc)
        return set()


def get_vm_instance_details(project_id: str) -> dict[str, dict[str, object]]:
    """Fetch actual VM instance details from GCP.

    Returns a dict mapping vm_name -> {machine_type, zone, creation_timestamp, status,
    last_stop_timestamp, boot_disk_name, attached_disk_names, labels}. ``attached_disk_names``
    is every attached disk (tails of the disk self-links) — surfaces DATA disks for
    leaked-resource detection on non-running VMs, not just the boot disk.
    """
    try:
        client = compute_v1.InstancesClient()
        request = compute_v1.AggregatedListInstancesRequest(project=project_id)
        vm_details: dict[str, dict[str, object]] = {}

        for _zone_url, scoped_list in client.aggregated_list(request=request, timeout=_RPC_TIMEOUT_SEC):
            instances = getattr(scoped_list, "instances", None)
            if not instances:
                continue

            # Extract zone name from the zone URL
            zone_name = _zone_url.split("/")[-1] if "/" in _zone_url else _zone_url

            for inst in instances:
                inst_typed = cast(object, inst)
                name = str(getattr(inst_typed, "name", ""))
                status = str(getattr(inst_typed, "status", ""))
                machine_type_url = str(getattr(inst_typed, "machine_type", ""))
                machine_type = machine_type_url.split("/")[-1] if "/" in machine_type_url else machine_type_url
                creation_timestamp = str(getattr(inst_typed, "creation_timestamp", ""))
                last_stop_timestamp = str(getattr(inst_typed, "last_stop_timestamp", ""))
                # Boot-disk name + EVERY attached disk name (tails of the disk self-links) so the
                # caller can join against get_disk_details for size + pd-type → idle-disk cost. The
                # full attached list surfaces DATA disks for leaked-resource detection on a
                # non-running VM, not just the boot disk.
                boot_disk_name = ""
                attached_disk_names: list[str] = []
                for attached in getattr(inst_typed, "disks", None) or []:
                    disk_name = str(getattr(attached, "source", "")).split("/")[-1]
                    if disk_name:
                        attached_disk_names.append(disk_name)
                    if getattr(attached, "boot", False) and not boot_disk_name:
                        boot_disk_name = disk_name
                # labels is a proto map; cast to plain dict[str, str] for the caller.
                labels_raw = getattr(inst_typed, "labels", None) or {}
                labels: dict[str, str] = {str(k): str(v) for k, v in dict(labels_raw).items()}

                if name:
                    vm_details[name] = {
                        "machine_type": machine_type,
                        "zone": zone_name,
                        "status": status,
                        "creation_timestamp": creation_timestamp,
                        "last_stop_timestamp": last_stop_timestamp,
                        "boot_disk_name": boot_disk_name,
                        "attached_disk_names": attached_disk_names,
                        "labels": labels,
                    }

        logger.info("get_vm_instance_details(%s): found %d VMs", project_id, len(vm_details))
        return vm_details
    except Exception as exc:
        logger.warning("get_vm_instance_details(%s) failed: %s", project_id, exc)
        return {}


def get_disk_details(project_id: str) -> dict[str, dict[str, object]]:
    """Fetch persistent-disk size + pd-type for every disk in ``project_id``.

    Returns ``{disk_name: {"size_gb": int, "type": "pd-standard"|...}}``. One
    ``aggregated_list`` call covers all zones. On failure returns an empty map +
    logs a warning (callers degrade to a default rate / unknown size).
    """
    try:
        client = compute_v1.DisksClient()
        request = compute_v1.AggregatedListDisksRequest(project=project_id)
        disk_details: dict[str, dict[str, object]] = {}
        for _zone_url, scoped_list in client.aggregated_list(request=request, timeout=_RPC_TIMEOUT_SEC):
            disks = getattr(scoped_list, "disks", None)
            if not disks:
                continue
            for disk in disks:
                disk_typed = cast(object, disk)
                name = str(getattr(disk_typed, "name", ""))
                type_url = str(getattr(disk_typed, "type_", ""))
                disk_type = type_url.split("/")[-1] if "/" in type_url else type_url
                size_gb = int(getattr(disk_typed, "size_gb", 0) or 0)
                if name:
                    disk_details[name] = {"size_gb": size_gb, "type": disk_type}
        logger.info("get_disk_details(%s): found %d disks", project_id, len(disk_details))
        return disk_details
    except Exception as exc:
        logger.warning("get_disk_details(%s) failed: %s", project_id, exc)
        return {}


def list_unattached_disk_names(project_id: str) -> set[str]:
    """Return names of persistent disks NOT attached to any instance in ``project_id``.

    A disk's ``users`` field lists the instance self-links it is attached to; an empty
    ``users`` means the disk is unattached — still billing ``PD Capacity`` while doing
    nothing (orphaned). This is the DEFINITIVE attachment signal, vs a disk-name-vs-VM-name
    heuristic which false-positives on data disks that don't share their instance's name.
    One ``aggregated_list`` covers all zones. On failure returns an empty set, so orphaned-disk
    detection degrades to "flag nothing" — honest absence, never a false-positive orphan claim.
    """
    try:
        client = compute_v1.DisksClient()
        request = compute_v1.AggregatedListDisksRequest(project=project_id)
        unattached: set[str] = set()
        for _zone_url, scoped_list in client.aggregated_list(request=request, timeout=_RPC_TIMEOUT_SEC):
            disks = getattr(scoped_list, "disks", None)
            if not disks:
                continue
            for disk in disks:
                disk_typed = cast(object, disk)
                name = str(getattr(disk_typed, "name", ""))
                users = getattr(disk_typed, "users", None) or []
                if name and not users:
                    unattached.add(name)
        logger.info("list_unattached_disk_names(%s): %d unattached disks", project_id, len(unattached))
        return unattached
    except Exception as exc:
        logger.warning("list_unattached_disk_names(%s) failed: %s", project_id, exc)
        return set()


def _list_disk_source_image_names(project_id: str) -> set[str]:
    """Image names currently backing at least one live disk (internal — see `list_orphaned_image_names`).

    A ``Disk.source_image`` is the full image self-link (``.../global/images/<name>``); only the
    trailing ``<name>`` segment is kept so it matches ``Image.name`` directly.
    """
    client = compute_v1.DisksClient()
    request = compute_v1.AggregatedListDisksRequest(project=project_id)
    referenced: set[str] = set()
    for _zone_url, scoped_list in client.aggregated_list(request=request, timeout=_RPC_TIMEOUT_SEC):
        disks = getattr(scoped_list, "disks", None)
        if not disks:
            continue
        for disk in disks:
            source_image = str(getattr(cast(object, disk), "source_image", "") or "")
            if source_image:
                referenced.add(source_image.rsplit("/", 1)[-1])
    return referenced


def list_orphaned_image_names(project_id: str) -> set[str]:
    """Return custom-image names NOT currently backing any live disk.

    Every image in the project, minus every image referenced (live) as some disk's `source_image` —
    the remainder traces back to nothing currently provisioned from it, while still billing `Storage
    Image` every month. On failure returns an empty set — orphan detection degrades to "flag
    nothing", never a false-positive claim (mirrors ``list_unattached_disk_names``).
    """
    try:
        images_client = compute_v1.ImagesClient()
        request = compute_v1.ListImagesRequest(project=project_id)
        all_images = {
            str(getattr(cast(object, img), "name", ""))
            for img in images_client.list(request=request, timeout=_RPC_TIMEOUT_SEC)
        }
        orphaned = {name for name in all_images if name} - _list_disk_source_image_names(project_id)
        logger.info(
            "list_orphaned_image_names(%s): %d orphaned of %d images", project_id, len(orphaned), len(all_images)
        )
        return orphaned
    except Exception as exc:
        logger.warning("list_orphaned_image_names(%s) failed: %s", project_id, exc)
        return set()


def _age_days(creation_ts: str, now: datetime) -> float | None:
    """Days since ``creation_ts`` (a GCP RFC3339 ``creationTimestamp``), or None if unparseable."""
    if not creation_ts:
        return None
    try:
        ts = datetime.fromisoformat(creation_ts)
    except ValueError:
        return None
    if ts.tzinfo is None:
        ts = ts.replace(tzinfo=UTC)
    return max(0.0, (now - ts).total_seconds() / 86400.0)


def list_stale_machine_image_names(project_id: str, min_age_days: float, now: datetime) -> set[str]:
    """Return Machine Image names older than ``min_age_days``.

    Machine Images (full-VM backup artifacts) carry no "attached"/"in use" signal the way a disk or
    address does — restoring one doesn't leave a live back-reference. Age is the only available
    staleness proxy, so this is a heuristic, not a definitive orphan claim (unlike
    ``list_unattached_disk_names``'s live cross-ref). On failure returns an empty set.
    """
    try:
        client = compute_v1.MachineImagesClient()
        request = compute_v1.ListMachineImagesRequest(project=project_id)
        stale: set[str] = set()
        for mi in client.list(request=request, timeout=_RPC_TIMEOUT_SEC):
            mi_typed = cast(object, mi)
            name = str(getattr(mi_typed, "name", ""))
            age = _age_days(str(getattr(mi_typed, "creation_timestamp", "") or ""), now)
            if name and age is not None and age >= min_age_days:
                stale.add(name)
        logger.info("list_stale_machine_image_names(%s): %d stale (>= %s d)", project_id, len(stale), min_age_days)
        return stale
    except Exception as exc:
        logger.warning("list_stale_machine_image_names(%s) failed: %s", project_id, exc)
        return set()


def list_stale_snapshot_names(project_id: str, min_age_days: float, now: datetime) -> set[str]:
    """Return Disk Snapshot names older than ``min_age_days``.

    A snapshot is, by design, never "attached" to anything — GCP tracks no back-reference at all —
    so age is the only available staleness proxy (same heuristic caveat as
    ``list_stale_machine_image_names``). On failure returns an empty set.
    """
    try:
        client = compute_v1.SnapshotsClient()
        request = compute_v1.ListSnapshotsRequest(project=project_id)
        stale: set[str] = set()
        for snap in client.list(request=request, timeout=_RPC_TIMEOUT_SEC):
            snap_typed = cast(object, snap)
            name = str(getattr(snap_typed, "name", ""))
            age = _age_days(str(getattr(snap_typed, "creation_timestamp", "") or ""), now)
            if name and age is not None and age >= min_age_days:
                stale.add(name)
        logger.info("list_stale_snapshot_names(%s): %d stale (>= %s d)", project_id, len(stale), min_age_days)
        return stale
    except Exception as exc:
        logger.warning("list_stale_snapshot_names(%s) failed: %s", project_id, exc)
        return set()


def _record_address(addresses: dict[str, dict[str, object]], addr: object) -> None:
    """Extract one Address proto into the reserved-address map (best-effort per field)."""
    name = str(getattr(addr, "name", ""))
    if not name:
        return
    region_url = str(getattr(addr, "region", ""))
    region = region_url.split("/")[-1] if "/" in region_url else region_url
    users = [str(u) for u in (getattr(addr, "users", None) or [])]
    addresses[name] = {
        "address": str(getattr(addr, "address", "")),
        "status": str(getattr(addr, "status", "")),
        "address_type": str(getattr(addr, "address_type", "")),
        "region": region or "global",
        "users": users,
    }


def list_reserved_addresses(project_id: str) -> dict[str, dict[str, object]]:
    """Fetch every reserved static IP (regional + global) in ``project_id``.

    Returns ``{address_name: {"address", "status", "address_type", "region", "users"}}``.
    ``status`` is ``RESERVED`` (allocated + idle → billed) or ``IN_USE`` (attached); ``users``
    lists the resource self-links using it (empty = no owner). A reserved external IP that is idle,
    or attached to a non-running VM, is a leaked cost. One regional ``aggregated_list`` covers all
    regions; a separate global list covers global addresses. Each read degrades independently: on
    failure that half is skipped + a warning logged, so leak detection degrades to "flag nothing" —
    honest absence, never a false-positive leak claim.
    """
    addresses: dict[str, dict[str, object]] = {}
    try:
        client = compute_v1.AddressesClient()
        request = compute_v1.AggregatedListAddressesRequest(project=project_id)
        for _zone_url, scoped_list in client.aggregated_list(request=request, timeout=_RPC_TIMEOUT_SEC):
            region_addresses = getattr(scoped_list, "addresses", None)
            if not region_addresses:
                continue
            for addr in region_addresses:
                _record_address(addresses, cast(object, addr))
    except Exception as exc:
        logger.warning("list_reserved_addresses(%s) regional read failed: %s", project_id, exc)
    try:
        global_client = compute_v1.GlobalAddressesClient()
        global_request = compute_v1.ListGlobalAddressesRequest(project=project_id)
        for addr in global_client.list(request=global_request, timeout=_RPC_TIMEOUT_SEC):
            _record_address(addresses, cast(object, addr))
    except Exception as exc:
        logger.warning("list_reserved_addresses(%s) global read failed: %s", project_id, exc)
    logger.info("list_reserved_addresses(%s): found %d reserved addresses", project_id, len(addresses))
    return addresses


def list_gcp_region_names(project_id: str) -> list[str]:
    """List every Compute Engine region name in ``project_id`` (for the ?all_regions census sweep).

    Compute regions are a superset of the Cloud Run / Cloud Functions regions — a region that does
    not support those services simply yields an empty per-region census (honest degradation). On
    failure returns an empty list so the caller falls back to the configured region set.
    """
    try:
        client = compute_v1.RegionsClient()
        request = compute_v1.ListRegionsRequest(project=project_id)
        names = [str(getattr(r, "name", "")) for r in client.list(request=request, timeout=_RPC_TIMEOUT_SEC)]
        return sorted(name for name in names if name)
    except Exception as exc:
        logger.warning("list_gcp_region_names(%s) failed: %s", project_id, exc)
        return []


def delete_vm_instance(project_id: str, name: str, zone: str) -> bool:
    """Delete a GCE instance (and its auto-delete boot disk). Best-effort.

    Returns True iff the Compute API accepted the delete (the op is in-flight or
    done). Mirrors deployment-service ``vm_zombie_watchdog._kill_vm`` semantics:
    a confirm-poll failure still counts as a kill (delete already accepted).
    Returns False only if the initial delete call itself raised.
    """
    try:
        client = compute_v1.InstancesClient()
        op = client.delete(project=project_id, zone=zone, instance=name)
    except Exception as exc:
        logger.warning("delete_vm_instance(%s/%s in %s) API call failed: %s", project_id, name, zone, exc)
        return False
    try:
        op.result(timeout=120)
    except Exception as exc:
        logger.warning(
            "delete_vm_instance(%s/%s in %s) confirm-poll failed (delete in-flight, counting as deleted): %s",
            project_id,
            name,
            zone,
            exc,
        )
    return True


__all__ = [
    "delete_vm_instance",
    "get_disk_details",
    "get_vm_instance_details",
    "list_gcp_region_names",
    "list_reserved_addresses",
    "list_running_vm_names",
    "list_unattached_disk_names",
]

"""GCE instance utilities for deployment-api.

This module provides utilities for fetching VM instance details from GCP.
It's a lightweight version of deployment-service's gcp_instance_lister.py
adapted for deployment-api's needs.
"""

from __future__ import annotations

import logging
from typing import cast

from google.cloud import compute_v1

logger = logging.getLogger(__name__)


def list_running_vm_names(project_id: str) -> set[str]:
    """Return the set of VM names currently in ``RUNNING`` state in ``project_id``.

    Uses ``aggregated_list`` so one API call covers every zone. On failure
    returns an empty set + logs a warning.
    """
    try:
        client = compute_v1.InstancesClient()
        request = compute_v1.AggregatedListInstancesRequest(project=project_id)
        running: set[str] = set()
        for _zone, scoped_list in client.aggregated_list(request=request):
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

    Returns a dict mapping vm_name -> {machine_type, zone, creation_timestamp, status}.
    """
    try:
        client = compute_v1.InstancesClient()
        request = compute_v1.AggregatedListInstancesRequest(project=project_id)
        vm_details: dict[str, dict[str, object]] = {}

        for _zone_url, scoped_list in client.aggregated_list(request=request):
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
                # Boot-disk name (tail of the disk self-link) so the caller can join
                # against get_disk_details for size + pd-type → idle-disk cost.
                boot_disk_name = ""
                for attached in getattr(inst_typed, "disks", None) or []:
                    if getattr(attached, "boot", False):
                        boot_disk_name = str(getattr(attached, "source", "")).split("/")[-1]
                        break
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
        for _zone_url, scoped_list in client.aggregated_list(request=request):
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


__all__ = ["delete_vm_instance", "get_disk_details", "get_vm_instance_details", "list_running_vm_names"]

"""GCE instance utilities for deployment-api.

This module provides utilities for fetching VM instance details from GCP.
It's a lightweight version of deployment-service's gcp_instance_lister.py
adapted for deployment-api's needs.
"""

from __future__ import annotations

import logging
from typing import cast

from google.cloud import compute_v1  # noqa: TID251 — google.cloud.compute_v1 has no UTL cloud-interface abstraction

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

                if name:
                    vm_details[name] = {
                        "machine_type": machine_type,
                        "zone": zone_name,
                        "status": status,
                        "creation_timestamp": creation_timestamp,
                    }

        logger.info("get_vm_instance_details(%s): found %d VMs", project_id, len(vm_details))
        return vm_details
    except Exception as exc:
        logger.warning("get_vm_instance_details(%s) failed: %s", project_id, exc)
        return {}


__all__ = ["get_vm_instance_details", "list_running_vm_names"]

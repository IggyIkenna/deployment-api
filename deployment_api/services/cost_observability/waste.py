"""Cost-waste detection — pure SKU/usage-type classifiers for idle/orphaned resources.

Billing-native only, per the feature's core principle: nothing sourced outside the
GCP/AWS billing exports themselves. An idle static IP and an idle Elastic IP are each a
DISTINCT SKU/usage-type from their in-use counterpart, so those two are self-contained
matches:

* GCP bills `Static Ip Charge` only while a reserved external IP is NOT attached to a
  running instance — distinct from `External IP Charge on a Standard VM` (in-use). The
  SKU alone is definitive; no cross-reference needed.
* AWS bills a distinct `...ElasticIP:IdleAddress` usage-type for an unattached EIP,
  separate from the attached-EIP `BoxUsage`/`ElasticIP:...` lines.

A GCP persistent disk has no such distinct idle SKU — `... PD Capacity` bills the same
whether attached or not — so `WASTE_ORPHANED_DISK` additionally needs a live cross-ref
against the disk's actual attachment state (`vm_utils.list_unattached_disk_names`, which
reads each disk's Compute `users` field: empty == attached-to-nothing == orphaned), keyed
by `resource.name` (the disk's own resource name, verified live: `ikenna-windows-tokyo-
restored`). Using the disk's real `users` field — not a disk-name-vs-running-VM-name
heuristic — avoids false-positives on data disks whose name differs from their instance's.
AWS EBS has the same attached/idle SKU ambiguity and, unlike GCP, this codebase has no AWS
instance/volume-attachment API integration to cross-ref against — dropped (not fabricated)
until that integration exists.

GCP SKU descriptions carry a REGIONAL SUFFIX (e.g. `Static Ip Charge in Japan`,
`Storage PD Capacity in Japan`), so the matchers below test for the SKU STEM as a
substring, never an exact/`endswith` match — the latter silently misses every regional
variant (which is most of them).

Also covers three backup-artifact SKUs (custom images, machine images, disk snapshots) that carry
no in-use billing/idle SKU distinction at all — GCP bills `Storage Image` / `Storage Machine Image`
/ `Storage PD Snapshot` identically whether the artifact backs anything live or has been forgotten
since a one-off build two years ago, so these three ALWAYS need a live cross-reference (an image's
referencing disks; an age threshold for machine images/snapshots, which track no back-reference at
all) — never a SKU-only classification the way idle-static-IP is.

And one waste kind that isn't SKU-substring-classifiable at all: `WASTE_STOPPED_VM_DISK`, a VM's
disk that keeps billing `PD Capacity` after the VM's own compute usage (`Instance Core`/`Instance
Ram`) stopped appearing in the export — i.e. the job finished (or the VM was stopped) but the disk
wasn't reaped. That needs a per-resource TIMELINE (last compute-usage day vs last disk-usage day),
not a per-row classification, so it is computed separately in
``cost_observability.service._stopped_vm_disk_waste_rows`` — this module only defines the SKU
markers it keys off (see ``is_gcp_compute_usage_sku``).
"""

from __future__ import annotations

from deployment_api.services.cost_observability.models import CLOUD_AWS, CLOUD_GCP

WASTE_IDLE_STATIC_IP = "idle_static_ip"
WASTE_ORPHANED_DISK = "orphaned_disk"
WASTE_IDLE_ELASTIC_IP = "idle_elastic_ip"
WASTE_STOPPED_VM_DISK = "stopped_vm_disk"
WASTE_ORPHANED_IMAGE = "orphaned_image"
WASTE_ORPHANED_MACHINE_IMAGE = "orphaned_machine_image"
WASTE_ORPHANED_SNAPSHOT = "orphaned_snapshot"

# SKU stems — matched as substrings so the GCP regional suffix ("... in Japan") never hides them.
_GCP_IDLE_STATIC_IP_SKU_STEM = "Static Ip Charge"
_GCP_DISK_CAPACITY_SKU_STEM = "PD Capacity"
_GCP_IMAGE_STORAGE_SKU_STEM = "Storage Image"
_GCP_MACHINE_IMAGE_STORAGE_SKU_STEM = "Storage Machine Image"
_GCP_SNAPSHOT_STORAGE_SKU_STEM = "Storage PD Snapshot"
# Compute-usage markers — "Instance Core"/"Instance Ram" bill for every machine family (E2 Instance
# Core, N2 Instance Ram, …), so the family prefix is deliberately NOT part of the marker.
_GCP_COMPUTE_CORE_SKU_MARKER = "Instance Core"
_GCP_COMPUTE_RAM_SKU_MARKER = "Instance Ram"
_AWS_IDLE_ELASTIC_IP_USAGE_MARKER = "ElasticIP:IdleAddress"

# Default staleness bar for machine images / snapshots (no live back-reference exists for either —
# see the module docstring), mirroring the fleet orphans feature's DEFAULT_GRACE_HOURS module-
# constant pattern (routes._fleet_inventory). 30 days: long enough that an in-progress backup/DR
# rotation doesn't false-positive, short enough that a genuinely forgotten one surfaces promptly.
DEFAULT_STALE_BACKUP_DAYS: float = 30.0


def is_gcp_idle_static_ip_sku(sku: str) -> bool:
    # `Static Ip Charge` (idle, reserved-but-unattached) vs `External IP Charge on a Standard VM`
    # (in-use) — substring so `Static Ip Charge in Japan` and other regional variants still match.
    return _GCP_IDLE_STATIC_IP_SKU_STEM in sku


def is_gcp_disk_capacity_sku(sku: str) -> bool:
    # Matches `Balanced PD Capacity`, `SSD backed PD Capacity in Japan`, `Storage PD Capacity in
    # Japan`, … — substring, not endswith, since the region trails the stem.
    return _GCP_DISK_CAPACITY_SKU_STEM in sku


def is_gcp_image_storage_sku(sku: str) -> bool:
    # `Storage Image` — custom Compute Engine image storage (any region suffix).
    return _GCP_IMAGE_STORAGE_SKU_STEM in sku


def is_gcp_machine_image_storage_sku(sku: str) -> bool:
    # `Storage Machine Image` is its own SKU family, disjoint from plain `Storage Image` — checked
    # first by callers that branch on both, since "Storage Image" is not a substring of it either
    # way, but keeping the check explicit avoids relying on that.
    return _GCP_MACHINE_IMAGE_STORAGE_SKU_STEM in sku


def is_gcp_snapshot_storage_sku(sku: str) -> bool:
    return _GCP_SNAPSHOT_STORAGE_SKU_STEM in sku


def is_gcp_compute_usage_sku(sku: str) -> bool:
    """True for the direct vCPU/RAM usage charge of a running VM (any machine family/region)."""
    return _GCP_COMPUTE_CORE_SKU_MARKER in sku or _GCP_COMPUTE_RAM_SKU_MARKER in sku


def is_aws_idle_elastic_ip_usage_type(usage_type: str) -> bool:
    return _AWS_IDLE_ELASTIC_IP_USAGE_MARKER in usage_type


def classify_waste(
    *,
    cloud: str,
    sku: str,
    resource_id: str,
    unattached_disk_names: frozenset[str],
    orphaned_image_names: frozenset[str] = frozenset(),
    stale_machine_image_names: frozenset[str] = frozenset(),
    stale_snapshot_names: frozenset[str] = frozenset(),
) -> str:
    """One of the `WASTE_*` labels for a (cloud, sku, resource) triple, or `""` if not waste.

    Each `*_names` set is a live-GCP (or age-derived) cross-reference gating its own kind — pass an
    empty set when that particular live read is unavailable/unknown so the kind degrades to "not
    flagged" rather than a false-positive claim (honest-absence, not fabrication; mirrors the
    original `unattached_disk_names` contract). `WASTE_STOPPED_VM_DISK` is NOT produced here — see
    the module docstring.
    """
    if cloud == CLOUD_GCP:
        if is_gcp_idle_static_ip_sku(sku):
            return WASTE_IDLE_STATIC_IP
        if is_gcp_disk_capacity_sku(sku) and resource_id and resource_id in unattached_disk_names:
            return WASTE_ORPHANED_DISK
        if is_gcp_machine_image_storage_sku(sku) and resource_id and resource_id in stale_machine_image_names:
            return WASTE_ORPHANED_MACHINE_IMAGE
        if is_gcp_image_storage_sku(sku) and resource_id and resource_id in orphaned_image_names:
            return WASTE_ORPHANED_IMAGE
        if is_gcp_snapshot_storage_sku(sku) and resource_id and resource_id in stale_snapshot_names:
            return WASTE_ORPHANED_SNAPSHOT
    elif cloud == CLOUD_AWS and is_aws_idle_elastic_ip_usage_type(sku):
        return WASTE_IDLE_ELASTIC_IP
    return ""


__all__ = [
    "DEFAULT_STALE_BACKUP_DAYS",
    "WASTE_IDLE_ELASTIC_IP",
    "WASTE_IDLE_STATIC_IP",
    "WASTE_ORPHANED_DISK",
    "WASTE_ORPHANED_IMAGE",
    "WASTE_ORPHANED_MACHINE_IMAGE",
    "WASTE_ORPHANED_SNAPSHOT",
    "WASTE_STOPPED_VM_DISK",
    "classify_waste",
    "is_aws_idle_elastic_ip_usage_type",
    "is_gcp_compute_usage_sku",
    "is_gcp_disk_capacity_sku",
    "is_gcp_idle_static_ip_sku",
    "is_gcp_image_storage_sku",
    "is_gcp_machine_image_storage_sku",
    "is_gcp_snapshot_storage_sku",
]

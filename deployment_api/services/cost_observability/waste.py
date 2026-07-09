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
"""

from __future__ import annotations

from deployment_api.services.cost_observability.models import CLOUD_AWS, CLOUD_GCP

WASTE_IDLE_STATIC_IP = "idle_static_ip"
WASTE_ORPHANED_DISK = "orphaned_disk"
WASTE_IDLE_ELASTIC_IP = "idle_elastic_ip"

# SKU stems — matched as substrings so the GCP regional suffix ("... in Japan") never hides them.
_GCP_IDLE_STATIC_IP_SKU_STEM = "Static Ip Charge"
_GCP_DISK_CAPACITY_SKU_STEM = "PD Capacity"
_AWS_IDLE_ELASTIC_IP_USAGE_MARKER = "ElasticIP:IdleAddress"


def is_gcp_idle_static_ip_sku(sku: str) -> bool:
    # `Static Ip Charge` (idle, reserved-but-unattached) vs `External IP Charge on a Standard VM`
    # (in-use) — substring so `Static Ip Charge in Japan` and other regional variants still match.
    return _GCP_IDLE_STATIC_IP_SKU_STEM in sku


def is_gcp_disk_capacity_sku(sku: str) -> bool:
    # Matches `Balanced PD Capacity`, `SSD backed PD Capacity in Japan`, `Storage PD Capacity in
    # Japan`, … — substring, not endswith, since the region trails the stem.
    return _GCP_DISK_CAPACITY_SKU_STEM in sku


def is_aws_idle_elastic_ip_usage_type(usage_type: str) -> bool:
    return _AWS_IDLE_ELASTIC_IP_USAGE_MARKER in usage_type


def classify_waste(*, cloud: str, sku: str, resource_id: str, unattached_disk_names: frozenset[str]) -> str:
    """One of the `WASTE_*` labels for a (cloud, sku, resource) triple, or `""` if not waste.

    `unattached_disk_names` (disks whose Compute `users` field is empty) gates GCP disk-orphan
    detection only — pass an empty set when the live fleet is unknown so disks degrade to "not
    flagged" rather than a false-positive orphan claim (honest-absence, not fabrication).
    """
    if cloud == CLOUD_GCP:
        if is_gcp_idle_static_ip_sku(sku):
            return WASTE_IDLE_STATIC_IP
        if is_gcp_disk_capacity_sku(sku) and resource_id and resource_id in unattached_disk_names:
            return WASTE_ORPHANED_DISK
    elif cloud == CLOUD_AWS and is_aws_idle_elastic_ip_usage_type(sku):
        return WASTE_IDLE_ELASTIC_IP
    return ""


__all__ = [
    "WASTE_IDLE_ELASTIC_IP",
    "WASTE_IDLE_STATIC_IP",
    "WASTE_ORPHANED_DISK",
    "classify_waste",
    "is_aws_idle_elastic_ip_usage_type",
    "is_gcp_disk_capacity_sku",
    "is_gcp_idle_static_ip_sku",
]

# Epic: observability_master
# Lifecycle: permanent
"""Leaked / unreleased-resource detection for a NON-running VM (WS-D full-estate plan).

A stopped/terminated VM that still holds resources is a direct, silent cost: its DATA disks keep
billing, a reserved static IP keeps billing. The boot-disk-on-stopped-VM leak is ALREADY caught by
``GET /api/fleet/orphans`` (reused, not re-detected here); this module adds the gaps — non-boot DATA
disks still attached to a non-``RUNNING`` VM, and static IPs attached to it.

Cost figures are asia-northeast1 published list rates — an ESTIMATE ("inferred", refresh
periodically), NOT a billing figure (``deployment_full_estate_cost_provenance`` principle 8). The
disk rate is reused from the orphans SSOT (``_fleet_inventory.monthly_disk_usd``) so there is ONE
disk cost model — estimates never drift between the orphans endpoint and the deployments inventory.
"""

from __future__ import annotations

from pydantic import BaseModel

from deployment_api.routes._fleet_inventory import monthly_disk_usd

# asia-northeast1 reserved external static-IPv4 list rate, $/month. ESTIMATE only — an idle static
# IP is ~$0.010/hr ≈ $7.30/mo, and one attached to a STOPPED VM is billed the same. Inferred, never
# a billing figure (principle 8).
_STATIC_IP_MONTHLY_USD: float = 7.30


class UnreleasedResource(BaseModel):  # CORRECT-LOCAL: FastAPI API contract model
    """One billable resource a non-running VM still holds (a leaked cost).

    ``est_monthly_usd`` is an inferred list-rate estimate (principle 8), never a billing figure —
    ``cost_basis`` labels it as such for the UI so it can render the "est. / refresh periodically"
    marker rather than presenting an exact number.
    """

    type: str  # "DISK" | "STATIC_IP"
    name: str
    size_gb: int | None = None
    disk_type: str | None = None  # pd-standard / pd-ssd / ... (DISK only; None for STATIC_IP)
    est_monthly_usd: float
    cost_basis: str = "inferred"  # always inferred (list-rate estimate), never billing-derived


def orphaned_disk(name: str, size_gb: int | None, disk_type: str | None) -> UnreleasedResource:
    """An UnreleasedResource for a persistent disk with NO owning VM (a first-class orphaned row).

    Cost reuses the orphans disk-rate SSOT; ``est_monthly_usd`` is inferred (principle 8).
    """
    return UnreleasedResource(
        type="DISK",
        name=name,
        size_gb=size_gb,
        disk_type=disk_type,
        est_monthly_usd=monthly_disk_usd(size_gb, disk_type),
    )


def orphaned_static_ip(name: str) -> UnreleasedResource:
    """An UnreleasedResource for a reserved static IP with NO owner (idle-billed; inferred cost)."""
    return UnreleasedResource(type="STATIC_IP", name=name, est_monthly_usd=_STATIC_IP_MONTHLY_USD)


def _instance_uses_address(users: list[str], vm_name: str) -> bool:
    """True if any address ``users`` self-link points at this instance."""
    needle = f"/instances/{vm_name}"
    return any(needle in user for user in users)


def detect_unreleased_resources(
    vm_name: str,
    details: dict[str, object] | None,
    disk_details: dict[str, dict[str, object]],
    addresses: dict[str, dict[str, object]],
    *,
    is_running: bool,
) -> tuple[bool | None, list[UnreleasedResource]]:
    """Detect DATA disks + static IPs a NON-running VM still holds (a leaked cost).

    Returns ``(has_unreleased, resources)``:

    * ``has_unreleased is None`` — cannot determine (no GCE detail for this VM). Honest absence,
      never a false "clean" (principle: never a silent placeholder).
    * ``(False, [])`` — a ``RUNNING`` VM (a healthily-attached resource is in-use, not leaked —
      principle 6), or a non-running VM that cleanly released everything.
    * ``(True, [...])`` — a non-running VM still holding the itemised resources.

    The BOOT disk is intentionally excluded — ``GET /api/fleet/orphans`` already accounts for it
    (reuse, don't re-detect). Disk cost reuses the orphans ``monthly_disk_usd`` SSOT.
    """
    if not details:
        return None, []  # no GCE join for this VM → cannot assert clean OR leaked
    if is_running:
        return False, []

    resources: list[UnreleasedResource] = []

    boot_disk = str(details.get("boot_disk_name") or "")
    attached = details.get("attached_disk_names")
    attached_names = [str(d) for d in attached] if isinstance(attached, list) else []
    for disk_name in attached_names:
        if not disk_name or disk_name == boot_disk:
            continue  # boot disk is the orphans endpoint's job (reuse, don't re-detect)
        disk = disk_details.get(disk_name, {})
        size_raw = disk.get("size_gb")
        size_gb = int(size_raw) if isinstance(size_raw, int) else None
        type_raw = disk.get("type")
        disk_type = str(type_raw) if isinstance(type_raw, str) and type_raw else None
        resources.append(
            UnreleasedResource(
                type="DISK",
                name=disk_name,
                size_gb=size_gb,
                disk_type=disk_type,
                est_monthly_usd=monthly_disk_usd(size_gb, disk_type),
            )
        )

    for addr_name, addr in addresses.items():
        users_raw = addr.get("users")
        users = [str(u) for u in users_raw] if isinstance(users_raw, list) else []
        if _instance_uses_address(users, vm_name):
            resources.append(
                UnreleasedResource(type="STATIC_IP", name=addr_name, est_monthly_usd=_STATIC_IP_MONTHLY_USD)
            )

    return len(resources) > 0, resources


__all__ = ["UnreleasedResource", "detect_unreleased_resources"]

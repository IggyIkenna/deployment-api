"""Unit tests for routes/_leaked_resources.py — leaked/unreleased-resource detection.

Credential-free / pure over its inputs (no cloud access). Pins the WS-D contract: a NON-running
VM still holding DATA disks / static IPs is a leaked cost; the boot disk is excluded (the orphans
endpoint's job); a running VM never leaks; an absent GCE join is honest-None, never a false clean.
"""

from __future__ import annotations

from deployment_api.routes._leaked_resources import (
    _STATIC_IP_MONTHLY_USD,  # pyright: ignore[reportPrivateUsage]
    detect_unreleased_resources,
)


def _details(*, status: str, boot: str, attached: list[str]) -> dict[str, object]:
    return {"status": status, "boot_disk_name": boot, "attached_disk_names": attached}


def test_no_gce_detail_is_honest_none_never_false_clean() -> None:
    """No GCE join for this VM → (None, []) — honest absence, never a fabricated 'clean'."""
    assert detect_unreleased_resources("vm-x", None, {}, {}, is_running=False) == (None, [])
    assert detect_unreleased_resources("vm-x", {}, {}, {}, is_running=False) == (None, [])


def test_running_vm_never_leaks() -> None:
    """A RUNNING VM's attached resources are in-use, not leaked (principle 6) → (False, [])."""
    details = _details(status="RUNNING", boot="vm-x-boot", attached=["vm-x-boot", "vm-x-data"])
    has_unreleased, resources = detect_unreleased_resources("vm-x", details, {}, {}, is_running=True)
    assert has_unreleased is False
    assert resources == []


def test_stopped_vm_with_only_boot_disk_is_clean() -> None:
    """The boot disk is the orphans endpoint's job — a stopped VM holding ONLY its boot disk is
    clean here (no double-count)."""
    details = _details(status="TERMINATED", boot="vm-x-boot", attached=["vm-x-boot"])
    has_unreleased, resources = detect_unreleased_resources("vm-x", details, {}, {}, is_running=False)
    assert has_unreleased is False
    assert resources == []


def test_stopped_vm_with_data_disk_leaks_with_reused_cost() -> None:
    """A non-boot DATA disk on a stopped VM is a leak; cost reuses the orphans disk-rate SSOT."""
    details = _details(status="STOPPED", boot="vm-x-boot", attached=["vm-x-boot", "vm-x-data"])
    disk_details = {"vm-x-data": {"size_gb": 100, "type": "pd-ssd"}}
    has_unreleased, resources = detect_unreleased_resources("vm-x", details, disk_details, {}, is_running=False)
    assert has_unreleased is True
    assert len(resources) == 1
    disk = resources[0]
    assert disk.type == "DISK"
    assert disk.name == "vm-x-data"
    assert disk.size_gb == 100
    assert disk.disk_type == "pd-ssd"
    assert disk.est_monthly_usd == 22.1  # 100 GB * 0.221 (pd-ssd asia-northeast1 list rate)
    assert disk.cost_basis == "inferred"  # never a billing figure (principle 8)


def test_stopped_vm_with_attributable_static_ip_leaks() -> None:
    """A reserved static IP whose users self-link points at this stopped VM is a leak."""
    details = _details(status="STOPPED", boot="vm-x-boot", attached=["vm-x-boot"])
    addresses = {
        "vm-x-ip": {
            "status": "IN_USE",
            "users": ["https://.../zones/asia-northeast1-c/instances/vm-x"],
        },
        # A different VM's IP must NOT attach to vm-x.
        "vm-y-ip": {"status": "IN_USE", "users": ["https://.../instances/vm-y"]},
    }
    has_unreleased, resources = detect_unreleased_resources("vm-x", details, {}, addresses, is_running=False)
    assert has_unreleased is True
    ips = [r for r in resources if r.type == "STATIC_IP"]
    assert len(ips) == 1
    assert ips[0].name == "vm-x-ip"
    assert ips[0].est_monthly_usd == _STATIC_IP_MONTHLY_USD
    assert ips[0].size_gb is None


def test_data_disk_and_static_ip_both_surface() -> None:
    """A stopped VM holding both a data disk and a static IP surfaces both resources."""
    details = _details(status="TERMINATED", boot="vm-x-boot", attached=["vm-x-boot", "vm-x-data"])
    disk_details = {"vm-x-data": {"size_gb": 50, "type": "pd-standard"}}
    addresses = {"vm-x-ip": {"status": "IN_USE", "users": ["/instances/vm-x"]}}
    has_unreleased, resources = detect_unreleased_resources("vm-x", details, disk_details, addresses, is_running=False)
    assert has_unreleased is True
    assert {r.type for r in resources} == {"DISK", "STATIC_IP"}
    disk = next(r for r in resources if r.type == "DISK")
    assert disk.est_monthly_usd == 2.6  # 50 GB * 0.052 (pd-standard)


def test_unknown_disk_size_degrades_to_zero_cost_not_a_crash() -> None:
    """A data disk absent from the disk map (size unknown) still surfaces as a leak, at $0 estimate
    (honest — never a fabricated cost)."""
    details = _details(status="STOPPED", boot="b", attached=["b", "mystery-disk"])
    has_unreleased, resources = detect_unreleased_resources("vm-x", details, {}, {}, is_running=False)
    assert has_unreleased is True
    assert resources[0].name == "mystery-disk"
    assert resources[0].size_gb is None
    assert resources[0].est_monthly_usd == 0.0

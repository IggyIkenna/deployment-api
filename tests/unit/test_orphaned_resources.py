"""Unit tests for the WS-D orphaned-resource first-class rows (#7).

Credential-free / pure over inputs. Pins the contract: an unattached disk + a no-owner reserved
static IP each become a first-class row (kind=DISK / STATIC_IP, launched_by=unknown, flagged as its
own leak with an inferred cost); an ATTACHED disk/IP is NOT emitted here (it is a VM's leak, #4).
"""

from __future__ import annotations

import os

os.environ.setdefault("CLOUD_MOCK_MODE", "false")
os.environ.setdefault("CLOUD_PROVIDER", "local")
os.environ.setdefault("GCP_PROJECT_ID", "test-project")
os.environ.setdefault("DISABLE_AUTH", "true")

from deployment_api.routes import deployments_inventory as inv


def test_orphaned_disks_and_ips_become_first_class_rows() -> None:
    disk_details: dict[str, dict[str, object]] = {
        "orphan-disk": {"size_gb": 500, "type": "pd-ssd"},
        "attached-disk": {"size_gb": 100, "type": "pd-standard"},
    }
    unattached = {"orphan-disk"}  # attached-disk has a user → not orphaned
    addresses: dict[str, dict[str, object]] = {
        "orphan-ip": {"status": "RESERVED", "users": [], "region": "asia-northeast1"},
        "attached-ip": {"status": "IN_USE", "users": ["/instances/vm-x"], "region": "asia-northeast1"},
    }
    items = inv._orphaned_resource_items(disk_details, unattached, addresses)  # pyright: ignore[reportPrivateUsage]
    by_name = {i.name: i for i in items}

    # The unattached disk → a DISK row; unknown provenance (no owning VM); its own leak + inferred cost.
    disk = by_name["orphan-disk"]
    assert disk.kind == "DISK"
    assert disk.launched_by == "unknown"
    assert disk.has_unreleased_resources is True
    assert disk.unreleased_resources is not None
    assert disk.unreleased_resources[0].est_monthly_usd == 110.5  # 500 GB * 0.221 (pd-ssd)
    assert disk.unreleased_resources[0].cost_basis == "inferred"

    # The no-owner reserved IP → a STATIC_IP row.
    ip = by_name["orphan-ip"]
    assert ip.kind == "STATIC_IP"
    assert ip.launched_by == "unknown"
    assert ip.unreleased_resources is not None
    assert ip.unreleased_resources[0].type == "STATIC_IP"
    assert ip.region == "asia-northeast1"

    # Attached disk (not in the unattached set) + attached IP (has users) are NOT emitted here —
    # they belong to a VM's leaked-resource list (#4), never double-counted as orphans.
    assert "attached-disk" not in by_name
    assert "attached-ip" not in by_name


def test_no_orphans_is_empty() -> None:
    assert inv._orphaned_resource_items({}, set(), {}) == []  # pyright: ignore[reportPrivateUsage]

"""Deployment-registry GCS reads + the AWS estate census.

Split from ``routes/deployments_inventory.py`` (pure code motion; plan:
``deployment_api_qg_size_gate_debt_2026_07_30.md``). Patched module-level collaborators
(``_cfg`` / ``get_storage_client`` / ``resolve_active_registry`` / ``load_aws_inventory`` /
``DeploymentRegistryEntry`` — the census "seams" ``tests/mocks.py``'s
``patch_inventory_secondary_census`` documents) are resolved through the facade module
(``_inv``) at call time so the existing test patch surface
``deployment_api.routes.deployments_inventory.<name>`` keeps intercepting.
"""

from __future__ import annotations

import json
from concurrent.futures import ThreadPoolExecutor
from datetime import date, datetime, timedelta
from functools import partial

from unified_trading_library import ARCHIVE_PREFIX, DEFAULT_BUCKET, DeploymentRegistryEntry, StorageClient

import deployment_api.routes.deployments_inventory as _inv
from deployment_api.routes.deployments_inventory import DeploymentItem, logger
from deployment_api.routes.deployments_inventory._classification import (
    _vm_lifecycle_class,  # pyright: ignore[reportPrivateUsage]
)

__all__ = [
    "_ARCHIVE_RETENTION_DAYS",
    "_archive_floor_date",
    "_download_entries_parallel",
    "_list_json_keys",
    "_load_aws_items",
    "_load_registry_entries",
    "_load_registry_entries_for_date_range",
    "_read_entry",
]

# Registry-read parallelism — the dominant cost of the inventory is per-object GCS reads over
# a transpacific hop; reading them sequentially is the >100s the cockpit timed out on. GCS REST
# releases the GIL, so a ThreadPool gives true I/O parallelism.
_GCS_READ_WORKERS = 32
# Bounded window for the default cold-path registry census (RUNNING + this many trailing days of
# the ARCHIVE prefix) — a date-range query bypasses this via ``_load_registry_entries_for_date_range``.
_ARCHIVE_WINDOW_DAYS = 7


def _load_aws_items(
    now: datetime,
    regions: tuple[str, ...] = _inv._CONFIGURED_AWS_REGIONS,  # pyright: ignore[reportPrivateUsage]
) -> tuple[list[DeploymentItem], dict[str, str]]:
    """Census + classify the live AWS estate into inventory items (Phase 5 parity), multi-region.

    Reuses the curated ``_vm_lifecycle_class`` prefix resolver so AWS umbrella derivation matches
    GCP exactly. Fans out across ``regions`` (the configured set, or the curated all-regions set on
    the ``?all_regions`` sweep) with per-region isolation — a region's census failure (no creds /
    boto3 absent / API down / unsupported) degrades to empty for THAT region and never blocks the
    others or the GCP inventory. AWS rides the same ``DeploymentItem`` contract.

    Also returns the ``{instance_id: Name-tag}`` map collected across every region's EC2 census
    (decision 3, AWS cost attribution) — ``_attach_costs`` needs it to resolve an AWS CUR row's
    ARN/instance-id ``resource_id`` to the friendly name the items are keyed on.
    """

    def _one(region: str) -> tuple[list[DeploymentItem], dict[str, str]]:
        instance_id_by_name: dict[str, str] = {}
        try:
            item_dicts = _inv.load_aws_inventory(
                region=region,
                aws_account_id=_inv._cfg.aws_account_id or "",  # pyright: ignore[reportPrivateUsage]
                lifecycle_for_name=_vm_lifecycle_class,
                instance_id_by_name=instance_id_by_name,
            )
            return [DeploymentItem(**d) for d in item_dicts], instance_id_by_name  # type: ignore[arg-type]
        except (OSError, ValueError, RuntimeError) as exc:
            logger.warning("inventory: AWS census for region %s degraded: %s", region, exc)
            return [], {}

    if not regions:
        return [], {}
    items: list[DeploymentItem] = []
    instance_id_by_name: dict[str, str] = {}
    with ThreadPoolExecutor(max_workers=min(8, len(regions)), thread_name_prefix="aws-region") as pool:
        for region_items, region_instance_id_by_name in pool.map(_one, regions):
            items.extend(region_items)
            instance_id_by_name.update(region_instance_id_by_name)
    return items, instance_id_by_name


def _list_json_keys(client: StorageClient, bucket: str, prefix: str) -> list[str]:
    """List the ``.json`` object keys under one registry prefix (honest-empty on error)."""
    try:
        return [b.name for b in client.list_blobs(bucket=bucket, prefix=prefix) if b.name.endswith(".json")]
    except (OSError, ValueError, RuntimeError) as exc:
        logger.warning("inventory: list_blobs(%s) failed: %s", prefix, exc)
        return []


def _read_entry(client: StorageClient, bucket: str, key: str) -> DeploymentRegistryEntry | None:
    """Download + parse ONE registry entry; return None on any read/parse error (per-key isolation)."""
    try:
        raw = client.download_bytes(bucket=bucket, blob_path=key).decode("utf-8")
        return _inv.DeploymentRegistryEntry.from_json(raw)
    except (OSError, ValueError, KeyError, json.JSONDecodeError) as exc:
        logger.warning("inventory: skipping unreadable registry entry %s: %s", key, exc)
        return None


def _download_entries_parallel(client: StorageClient, bucket: str, keys: list[str]) -> list[DeploymentRegistryEntry]:
    """Download + parse many registry entries CONCURRENTLY (GCS-object-ops ThreadPool pattern).

    The dominant cost of the inventory is per-object GCS reads over a transpacific hop;
    reading them sequentially is the >100s the cockpit timed out on. GCS REST releases
    the GIL, so a ThreadPool gives true I/O parallelism. Per-key failures degrade to
    ``None`` (never crash the whole inventory).
    """
    if not keys:
        return []
    workers = min(_GCS_READ_WORKERS, len(keys))
    read_one = partial(_read_entry, client, bucket)
    with ThreadPoolExecutor(max_workers=workers) as pool:
        results = pool.map(read_one, keys)
    return [entry for entry in results if entry is not None]


def _load_registry_entries(now: datetime) -> list[DeploymentRegistryEntry]:
    """Census the deployment registry: RUNNING deployments + the 7-day archive window.

    RUNNING entries come from :func:`resolve_active_registry` (Firestore-first indexed query
    when the migration flag is on, loud GCS-``active/`` fallback otherwise) — the scale path
    that replaces downloading ~3k ``active/`` blobs. The 7-day ARCHIVE window stays on the
    GCS parallel download (bounded, and the dead-VM/hard-killed detection the D.3 ``dead``
    composite state needs — a registry entry whose VM the control plane no longer has must
    reach ``build_inventory``/``_composite_health_status`` to be classified ``dead``).

    DECOUPLED from the GCE aggregated-list read (``get_vm_instance_details``) — the caller
    runs that as a SEPARATE census future. So a slow/failed registry read degrades this to a
    short/empty list WITHOUT dropping the live VMs: those render from the GCE join
    (``build_inventory``'s unmanaged-row union). This removes the old failure mode where one
    bundled future timed out and blanked BOTH the VM list and the registry (empty prod tab).
    """
    active = _inv.resolve_active_registry()
    client = _inv.get_storage_client()
    bucket = DEFAULT_BUCKET
    today = now.date()
    archive_prefixes = [
        f"{ARCHIVE_PREFIX}{(today - timedelta(days=offset)).isoformat()}/" for offset in range(_ARCHIVE_WINDOW_DAYS)
    ]
    archive_keys = [key for prefix in archive_prefixes for key in _list_json_keys(client, bucket, prefix)]
    recent = _download_entries_parallel(client, bucket, archive_keys)
    return active + recent


# GCS lifecycle TTL on ``deployments/archive/`` — live-confirmed 2026-07-20 (earliest day-prefix is
# exactly 30 days before the latest). The default cold-path census stays on the cheap
# ``_ARCHIVE_WINDOW_DAYS`` (7-day) window; a date-range query needs its OWN bounded read up to this
# real floor, never the whole 30-day corpus unless the requested range actually spans it.
_ARCHIVE_RETENTION_DAYS = 30


def _archive_floor_date(now: datetime) -> date:
    """The earliest day the archive actually retains data for (the real 30-day GCS floor)."""
    return (now - timedelta(days=_ARCHIVE_RETENTION_DAYS - 1)).date()


def _load_registry_entries_for_date_range(
    now: datetime, date_from: datetime | None, date_to: datetime | None
) -> tuple[list[DeploymentRegistryEntry], bool]:
    """Day-partitioned archive read scoped to ``[date_from, date_to]``, up to the 30-day GCS floor.

    Bypasses the default cold-path's 7-day ``_ARCHIVE_WINDOW_DAYS`` cap — a date-range query reads
    exactly the requested days directly (``deployments/archive/<day>/`` prefixes), still a BOUNDED
    listing, never a whole-corpus walk. Returns ``(entries, out_of_range)``: ``out_of_range`` is True
    when the requested ``date_from`` predates the real retention floor (decision 5 — the caller turns
    this into an explicit "no data before `<date>`" banner, never a silently clipped partial result).
    An empty/backwards clipped window (e.g. the whole request predates the floor) returns ``[]`` with
    ``out_of_range`` still correctly reported.
    """
    floor_date = _archive_floor_date(now)
    today = now.date()
    range_start = date_from.date() if date_from is not None else floor_date
    range_end = date_to.date() if date_to is not None else today
    out_of_range = range_start < floor_date
    clipped_start = max(range_start, floor_date)
    clipped_end = min(range_end, today)
    if clipped_start > clipped_end:
        return [], out_of_range

    client = _inv.get_storage_client()
    bucket = DEFAULT_BUCKET
    day_count = (clipped_end - clipped_start).days + 1
    prefixes = [
        f"{ARCHIVE_PREFIX}{(clipped_start + timedelta(days=offset)).isoformat()}/" for offset in range(day_count)
    ]
    keys = [key for prefix in prefixes for key in _list_json_keys(client, bucket, prefix)]
    entries = _download_entries_parallel(client, bucket, keys)
    return entries, out_of_range

"""
Shared mocks for deployment-service tests.

Provides reusable mock builders for GCS, PathCombinatorics, etc.
"""

import contextlib
from collections.abc import Iterator
from types import ModuleType
from unittest.mock import MagicMock, patch


@contextlib.contextmanager
def patch_inventory_secondary_census(mod: ModuleType) -> Iterator[None]:
    """Patch the SECONDARY GCP census seams the deployment-inventory route fans out to.

    ``routes/deployments_inventory._compute_inventory`` fans out ~a dozen provider
    censuses concurrently. Route tests already mock the PRIMARY seams — the deployment
    registry (``_load_registry_entries``), the GCE aggregated-list
    (``get_vm_instance_details``), Cloud Run jobs (``latest_execution_by_job``) and the
    AWS census (``load_aws_inventory`` / ``_load_aws_items``). Every OTHER GCP list call
    stays LIVE otherwise: Cloud Run services / Cloud Functions / Cloud Scheduler / disks /
    reserved IPs / unattached disks / manifest object-deltas / cost observability.

    Each of those degrades to an empty census on failure (``_census_or_degrade``), so the
    test still PASSES — but an offline (``--block-network`` / pytest-socket ``--allow-hosts``)
    run first attempts a REAL transpacific socket to GCP for each. That is slow (gRPC
    retry/backoff), leaks background census threads past teardown (the ``I/O operation on
    closed file`` logging races), and is intermittently a hard failure (a partial
    token/response surfaces as ``JSONDecodeError``) — which is what flakes the local QG.

    Patching these seams to their honest EMPTY defaults keeps the route behaviour under
    test identical (they contribute zero rows either way in these fixtures) while making the
    test fully credential-free with no socket use. ``mod`` is the imported
    ``deployment_api.routes.deployments_inventory`` module — the seams resolve as module
    globals, so patching them on ``mod`` also covers the closures that call them (e.g.
    ``_multi_region_services`` / ``_batched_object_deltas``).
    """
    cost_service = MagicMock()
    cost_service.return_value.per_resource_daily.return_value = {}
    with (
        patch.object(mod, "list_cloud_run_services", return_value=[]),
        patch.object(mod, "list_cloud_functions", return_value={}),
        patch.object(mod, "list_scheduler_jobs", return_value=[]),
        patch.object(mod, "get_disk_details", return_value={}),
        patch.object(mod, "list_reserved_addresses", return_value={}),
        patch.object(mod, "list_unattached_disk_names", return_value=set()),
        patch.object(mod, "object_delta_for_asset_group", return_value=(None, "offline-test")),
        patch.object(mod, "CostObservabilityService", cost_service),
    ):
        yield


def make_mock_path_combinatorics():
    """Return a disabled PathCombinatorics mock (forces directory-listing path).

    Use in data_status tests to bypass combinatorics and exercise GCS listing.
    """
    pc = MagicMock()
    pc.get_combinatorics.return_value = []
    pc.has_service_combinatorics.return_value = False
    pc.get_service_prefixes_for_date.return_value = []
    pc.config_loader = MagicMock()
    pc.config_loader.load_service_config.side_effect = Exception("mock")
    return pc

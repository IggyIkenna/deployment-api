"""Unit tests for the WS-D multi-region census (deployments_inventory).

Credential-free: the per-region GCP census functions are mocked. Pins the contract — the default
censuses a configured region set; ``?all_regions`` sweeps every compute region (falling back to the
configured set if the region-list read fails); per-region results merge with the region carried.
"""

from __future__ import annotations

import os

os.environ.setdefault("CLOUD_MOCK_MODE", "false")
os.environ.setdefault("CLOUD_PROVIDER", "local")
os.environ.setdefault("GCP_PROJECT_ID", "test-project")
os.environ.setdefault("DISABLE_AUTH", "true")

from unittest.mock import patch

from deployment_api.routes import deployments_inventory as inv
from deployment_api.routes._cloud_run_executions import CloudRunExecutionStatus


def test_gcp_regions_for_scope_configured_by_default() -> None:
    """Default scope ("") → the configured region set, no region-list API call."""
    assert inv._gcp_regions_for_scope("", "p") == inv._CONFIGURED_GCP_REGIONS  # pyright: ignore[reportPrivateUsage]


def test_gcp_regions_for_scope_all_sweeps_every_region() -> None:
    """Scope "ALL" → every live compute region; falls back to the configured set on read failure."""
    with patch.object(inv, "list_gcp_region_names", return_value=["asia-northeast1", "us-west2", "europe-west4"]):
        assert inv._gcp_regions_for_scope("ALL", "p") == (  # pyright: ignore[reportPrivateUsage]
            "asia-northeast1",
            "us-west2",
            "europe-west4",
        )
    with patch.object(inv, "list_gcp_region_names", return_value=[]):
        assert inv._gcp_regions_for_scope("ALL", "p") == inv._CONFIGURED_GCP_REGIONS  # pyright: ignore[reportPrivateUsage]


def test_gcp_regions_for_scope_specific_region() -> None:
    """A specific region scope → just that region (no region-list call)."""
    assert inv._gcp_regions_for_scope("europe-west1", "p") == ("europe-west1",)  # pyright: ignore[reportPrivateUsage]


def test_aws_regions_for_scope() -> None:
    """AWS scope: configured default (""), curated full ("ALL"), or the GCP region's AWS equivalent."""
    assert inv._aws_regions_for_scope("") == inv._CONFIGURED_AWS_REGIONS  # pyright: ignore[reportPrivateUsage]
    assert inv._aws_regions_for_scope("ALL") == inv._ALL_AWS_REGIONS  # pyright: ignore[reportPrivateUsage]
    assert inv._aws_regions_for_scope("europe-west1") == ("eu-west-1",)  # pyright: ignore[reportPrivateUsage]
    # An unpaired GCP region falls back to the primary AWS set (never an empty AWS census).
    assert inv._aws_regions_for_scope("moon-base1") == inv._CONFIGURED_AWS_REGIONS  # pyright: ignore[reportPrivateUsage]


def test_normalize_region_scope() -> None:
    """The region / legacy all_regions params → the internal scope token; the default region → ""."""
    assert inv._normalize_region_scope(None, False) == ""  # pyright: ignore[reportPrivateUsage]
    assert inv._normalize_region_scope("", False) == ""  # pyright: ignore[reportPrivateUsage]
    # The default region is byte-identical to the configured census (no us-east-1 Lambda regression).
    assert inv._normalize_region_scope("asia-northeast1", False) == ""  # pyright: ignore[reportPrivateUsage]
    assert inv._normalize_region_scope("all", False) == "ALL"  # pyright: ignore[reportPrivateUsage]
    assert inv._normalize_region_scope(None, True) == "ALL"  # pyright: ignore[reportPrivateUsage]
    assert inv._normalize_region_scope("europe-west1", False) == "europe-west1"  # pyright: ignore[reportPrivateUsage]
    assert inv._normalize_region_scope("EUROPE-WEST1", False) == "europe-west1"  # pyright: ignore[reportPrivateUsage]


def test_multi_region_jobs_merges_per_region_with_region_carried() -> None:
    """Cloud Run jobs from each region merge into one map; each status carries its region."""

    def _fake(_project_id: str, region: str) -> dict[str, CloudRunExecutionStatus]:
        return {
            f"job-{region}": CloudRunExecutionStatus(
                job_name=f"job-{region}", status="succeeded", last_run_at=None, exit_code=0, log_uri="", region=region
            )
        }

    with patch.object(inv, "latest_execution_by_job", side_effect=_fake):
        merged = inv._multi_region_jobs("p", ("asia-northeast1", "us-central1"))  # pyright: ignore[reportPrivateUsage]
    assert set(merged) == {"job-asia-northeast1", "job-us-central1"}
    assert merged["job-us-central1"].region == "us-central1"


def test_multi_region_jobs_empty_regions_is_empty() -> None:
    """No regions → empty map (never an error)."""
    assert inv._multi_region_jobs("p", ()) == {}  # pyright: ignore[reportPrivateUsage]


def test_cloud_run_job_row_surfaces_region() -> None:
    """A live Cloud Run job row carries the region the multi-region census found it in."""
    live = CloudRunExecutionStatus(
        job_name="prd-x", status="succeeded", last_run_at=None, exit_code=0, log_uri="", region="us-central1"
    )
    item = inv._cloud_run_item_for_live_job("prd-x", live)  # pyright: ignore[reportPrivateUsage]
    assert item.region == "us-central1"

"""
AWS path pin for get_hierarchical_drilldown.

When CLOUD_PROVIDER=aws:
- get_hierarchical_drilldown MUST call read_availability_index with a bare
  bucket name (not a gs:// URI). The 2026-05-07 regression passed the gs://
  URI, which caused read_availability_index to silently return an empty
  DataFrame because UTL's reader expects a bare name, not a scheme-prefixed URI.
- The returned tree shape MUST be identical to the GCS path — same dict keys,
  same node structure, same totals. The cloud provider affects which storage
  backend reads the manifest (UCI routes S3 vs GCS); it MUST NOT change
  the response shape.
- build_bucket_name output must be passed directly to read_availability_index
  with no scheme prefix added by get_hierarchical_drilldown.

Plan: data_status_comprehensive_test_coverage_2026_05_07 § E.2.
"""

from __future__ import annotations

import os
from unittest.mock import MagicMock, patch

import pandas as pd

from deployment_api.services.data_status_hierarchical import get_hierarchical_drilldown


def _concrete_manifest() -> pd.DataFrame:
    """Minimal manifest with real rows for tree-shape verification.

    Covers instruments-service/cefi axes: venue → data_type → instrument_type
    → instrument_id → date.
    """
    return pd.DataFrame(
        {
            "date": ["2024-01-01", "2024-01-02"],
            "venue": ["BINANCE", "BINANCE"],
            "chain": ["", ""],
            "data_type": ["spot", "spot"],
            "instrument_type": ["SPOT", "SPOT"],
            "instrument_id": ["BTC-USD", "BTC-USD"],
            "league_id": ["", ""],
            "feature_group": ["", ""],
            "feature_family": ["", ""],
            "timeframe": ["", ""],
            "model_family": ["", ""],
            "training_period": ["", ""],
            "strategy_id": ["", ""],
            "client_id": ["", ""],
            "instruction_type": ["", ""],
            "canonical_question_group": ["", ""],
            "job_id": ["", ""],
            "archetype": ["", ""],
            "underlying": ["", ""],
            "capture_status": ["captured", "captured"],
            "error_reason": ["", ""],
        }
    )


_AWS_BUCKET = "instruments-store-cefi-427895769566"


class TestHierarchicalDrilldownAwsBucketNamePin:
    """read_availability_index must receive a bare bucket name, not a URI."""

    def test_read_availability_index_called_with_bare_bucket_name_not_gs_uri(self) -> None:
        """Catches the 2026-05-07 regression where gs://bucket/... was passed
        instead of the bare bucket name, causing a silent empty-DataFrame return."""
        mock_read = MagicMock(return_value=pd.DataFrame())

        with (
            patch.dict(os.environ, {"CLOUD_PROVIDER": "aws"}),
            patch(
                "deployment_api.services.data_status_hierarchical.read_availability_index",
                mock_read,
            ),
            patch(
                "deployment_api.services.data_status_hierarchical.build_bucket_name",
                return_value=_AWS_BUCKET,
            ),
        ):
            get_hierarchical_drilldown(
                service="instruments-service",
                asset_group="cefi",
                window_start="2024-01-01",
                window_end="2024-01-31",
                project_id="427895769566",
            )

        mock_read.assert_called_once()
        called_bucket = mock_read.call_args[0][0]
        assert not called_bucket.startswith("gs://"), (
            f"read_availability_index received a gs:// URI ({called_bucket!r}) — "
            "passing a URI silently produces an empty DataFrame. Must be the bare bucket name."
        )
        assert not called_bucket.startswith("s3://"), (
            f"read_availability_index received an s3:// URI ({called_bucket!r}) — "
            "must pass the bare bucket name; UCI handles the S3 dispatch internally."
        )

    def test_read_availability_index_receives_exact_bucket_from_build_bucket_name(self) -> None:
        """read_availability_index is called with exactly what build_bucket_name returns — no mutation."""
        mock_read = MagicMock(return_value=pd.DataFrame())

        with (
            patch.dict(os.environ, {"CLOUD_PROVIDER": "aws"}),
            patch(
                "deployment_api.services.data_status_hierarchical.read_availability_index",
                mock_read,
            ),
            patch(
                "deployment_api.services.data_status_hierarchical.build_bucket_name",
                return_value=_AWS_BUCKET,
            ),
        ):
            get_hierarchical_drilldown(
                service="instruments-service",
                asset_group="cefi",
                window_start="2024-01-01",
                window_end="2024-01-31",
                project_id="427895769566",
            )

        mock_read.assert_called_once_with(_AWS_BUCKET)


class TestHierarchicalDrilldownAwsShapeIdenticalToGcs:
    """Returned tree shape is identical regardless of CLOUD_PROVIDER."""

    def _call_drilldown(self, cloud_provider: str) -> dict[str, object]:
        bucket = f"instruments-store-cefi-fake-{cloud_provider}"
        with (
            patch.dict(os.environ, {"CLOUD_PROVIDER": cloud_provider}),
            patch(
                "deployment_api.services.data_status_hierarchical.read_availability_index",
                return_value=_concrete_manifest(),
            ),
            patch(
                "deployment_api.services.data_status_hierarchical.build_bucket_name",
                return_value=bucket,
            ),
        ):
            return get_hierarchical_drilldown(
                service="instruments-service",
                asset_group="cefi",
                window_start="2024-01-01",
                window_end="2024-01-31",
            )

    def test_aws_result_has_same_top_level_keys_as_gcs(self) -> None:
        """AWS path response has the same top-level keys as the GCS path."""
        gcs_result = self._call_drilldown("gcp")
        aws_result = self._call_drilldown("aws")

        assert set(aws_result.keys()) == set(gcs_result.keys()), (
            f"AWS/GCS key mismatch — AWS extra: {set(aws_result.keys()) - set(gcs_result.keys())}, "
            f"GCS extra: {set(gcs_result.keys()) - set(aws_result.keys())}"
        )

    def test_aws_result_axes_match_gcs(self) -> None:
        """The axes list is identical on AWS and GCS paths."""
        gcs_result = self._call_drilldown("gcp")
        aws_result = self._call_drilldown("aws")

        assert aws_result["axes"] == gcs_result["axes"], (
            f"AWS axes {aws_result['axes']!r} differ from GCS axes {gcs_result['axes']!r}."
        )

    def test_aws_result_totals_match_gcs(self) -> None:
        """Totals (captured / empty_confirmed / attempted_failed) are identical."""
        gcs_result = self._call_drilldown("gcp")
        aws_result = self._call_drilldown("aws")

        assert aws_result["totals"] == gcs_result["totals"], (
            f"AWS totals {aws_result['totals']!r} differ from GCS totals {gcs_result['totals']!r}."
        )

    def test_aws_result_tree_length_matches_gcs(self) -> None:
        """Tree top-level child count is identical on both paths."""
        gcs_result = self._call_drilldown("gcp")
        aws_result = self._call_drilldown("aws")

        assert len(aws_result.get("tree", [])) == len(gcs_result.get("tree", [])), (
            f"AWS tree has {len(aws_result.get('tree', []))} nodes; "
            f"GCS has {len(gcs_result.get('tree', []))} — shape drift."
        )


class TestHierarchicalDrilldownAwsRequiredKeys:
    """Response dict on the AWS path has all required top-level keys."""

    _REQUIRED_KEYS: frozenset[str] = frozenset(
        {
            "axes",
            "tree",
            "totals",
            "filtered_by",
            "service",
            "asset_group",
            "child_offset",
            "child_limit",
            "total_top_axis_children",
        }
    )

    def test_all_required_keys_present_when_aws_manifest_has_rows(self) -> None:
        """Response has all required keys when the manifest returns rows."""
        with (
            patch.dict(os.environ, {"CLOUD_PROVIDER": "aws"}),
            patch(
                "deployment_api.services.data_status_hierarchical.read_availability_index",
                return_value=_concrete_manifest(),
            ),
            patch(
                "deployment_api.services.data_status_hierarchical.build_bucket_name",
                return_value=_AWS_BUCKET,
            ),
        ):
            result = get_hierarchical_drilldown(
                service="instruments-service",
                asset_group="cefi",
                window_start="2024-01-01",
                window_end="2024-01-31",
            )

        missing = self._REQUIRED_KEYS - set(result.keys())
        assert not missing, f"AWS path response missing required keys: {missing}"

    def test_all_required_keys_present_when_aws_manifest_empty(self) -> None:
        """Response has all required keys even when the manifest is empty (empty-bucket path)."""
        with (
            patch.dict(os.environ, {"CLOUD_PROVIDER": "aws"}),
            patch(
                "deployment_api.services.data_status_hierarchical.read_availability_index",
                return_value=pd.DataFrame(),
            ),
            patch(
                "deployment_api.services.data_status_hierarchical.build_bucket_name",
                return_value=_AWS_BUCKET,
            ),
        ):
            result = get_hierarchical_drilldown(
                service="instruments-service",
                asset_group="cefi",
                window_start="2024-01-01",
                window_end="2024-01-31",
            )

        missing = self._REQUIRED_KEYS - set(result.keys())
        assert not missing, f"AWS path (empty manifest) response missing required keys: {missing}"

    def test_totals_dict_has_all_required_sub_keys_when_aws(self) -> None:
        """totals sub-dict has captured / empty_confirmed / attempted_failed / total / completion_pct."""
        with (
            patch.dict(os.environ, {"CLOUD_PROVIDER": "aws"}),
            patch(
                "deployment_api.services.data_status_hierarchical.read_availability_index",
                return_value=_concrete_manifest(),
            ),
            patch(
                "deployment_api.services.data_status_hierarchical.build_bucket_name",
                return_value=_AWS_BUCKET,
            ),
        ):
            result = get_hierarchical_drilldown(
                service="instruments-service",
                asset_group="cefi",
                window_start="2024-01-01",
                window_end="2024-01-31",
            )

        totals = result.get("totals", {})
        required_totals_keys = {
            "captured",
            "empty_confirmed",
            "attempted_failed",
            "total",
            "completion_pct",
        }
        missing = required_totals_keys - set(totals.keys())  # type: ignore[arg-type]
        assert not missing, f"totals dict missing keys: {missing}"
        assert totals["total"] == totals["captured"] + totals["empty_confirmed"] + totals["attempted_failed"]  # type: ignore[index]


class TestHierarchicalDrilldownAwsNoGcsDispatch:
    """No GCS-specific storage client is instantiated when CLOUD_PROVIDER=aws."""

    def test_gcs_storage_client_not_instantiated_by_drilldown_when_aws(self) -> None:
        """GCSStorageClient constructor must not be called during get_hierarchical_drilldown
        when CLOUD_PROVIDER=aws. Catches regressions where storage is hardcoded to GCS."""
        with (
            patch.dict(os.environ, {"CLOUD_PROVIDER": "aws"}),
            patch(
                "deployment_api.services.data_status_hierarchical.read_availability_index",
                return_value=pd.DataFrame(),
            ),
            patch(
                "deployment_api.services.data_status_hierarchical.build_bucket_name",
                return_value=_AWS_BUCKET,
            ),
            patch("unified_trading_library.cloud_interface.providers.gcp.GCSStorageClient") as mock_gcs,
        ):
            get_hierarchical_drilldown(
                service="instruments-service",
                asset_group="cefi",
                window_start="2024-01-01",
                window_end="2024-01-31",
            )

        mock_gcs.assert_not_called()

"""Unit tests for reader-side legacy_reason_classifier wiring (writegate Tier 3D.2).

Covers the two consumer-side wires in deployment-api:

* :func:`deployment_api.services.shard_detail._gcs_metadata` —
  classifies legacy ``empty_confirmed`` rows on the per-shard detail
  endpoint.
* :func:`deployment_api.services.data_status_drilldown.lookup_capture_status_for_shard` —
  classifies legacy ``empty_confirmed`` rows on the drill-down lookup.

Both call the same UTL ``classify_legacy_empty_row`` helper so behaviour
is identical to the Tier 3D.1 reconciler's batch back-fill.
"""

from __future__ import annotations

from unittest.mock import patch

import pandas as pd

from deployment_api.services.data_status_drilldown import lookup_capture_status_for_shard
from deployment_api.services.shard_detail import _gcs_metadata

# ---------------------------------------------------------------------------
# shard_detail._gcs_metadata
# ---------------------------------------------------------------------------


def test_gcs_metadata_classifies_legacy_empty_tradfi_weekend() -> None:
    # Legacy row: capture_status=empty_confirmed, error_reason="" (NULL).
    # 2024-01-06 is a Saturday — CME futures don't trade.
    manifest = {
        "capture_status": "empty_confirmed",
        "error_reason": "",
        "venue": "CME",
        "date": "2024-01-06",
        "data_type": "ohlcv_1m",
        "attempted_at": "2024-01-06T12:00:00Z",
    }
    out = _gcs_metadata(
        bucket=None,
        object_path=None,
        manifest=manifest,
        pq_row_count=None,
        asset_group="TRADFI",
    )
    assert out.capture_status == "empty_confirmed"
    assert out.error_reason == "EXPECTED_WEEKEND"


def test_gcs_metadata_classifies_legacy_empty_defi_pre_genesis() -> None:
    # Arbitrum genesis 2021-08-31 — anything pre-genesis classified as
    # EXPECTED_PRE_GENESIS_CHAIN.
    manifest = {
        "capture_status": "empty_confirmed",
        "error_reason": "",
        "chain": "ARBITRUM",
        "venue": "AAVE_V3",
        "date": "2021-01-15",
        "data_type": "lending_rates",
        "attempted_at": "2021-01-15T12:00:00Z",
    }
    out = _gcs_metadata(
        bucket=None,
        object_path=None,
        manifest=manifest,
        pq_row_count=None,
        asset_group="DEFI",
    )
    assert out.capture_status == "empty_confirmed"
    assert out.error_reason == "EXPECTED_PRE_GENESIS_CHAIN"


def test_gcs_metadata_preserves_existing_error_reason() -> None:
    # Modern post-Phase-2.E.2 row already carries a typed reason — the
    # reader-side fallback MUST NOT overwrite it.
    manifest = {
        "capture_status": "empty_confirmed",
        "error_reason": "EXPECTED_HOLIDAY",
        "venue": "NYSE",
        "date": "2024-12-25",
        "data_type": "ohlcv_1m",
        "attempted_at": "2024-12-25T12:00:00Z",
    }
    out = _gcs_metadata(
        bucket=None,
        object_path=None,
        manifest=manifest,
        pq_row_count=None,
        asset_group="TRADFI",
    )
    assert out.error_reason == "EXPECTED_HOLIDAY"


def test_gcs_metadata_skips_classifier_when_asset_group_is_none() -> None:
    # Backwards compat — callers that don't yet thread asset_group
    # should still work; error_reason stays None.
    manifest = {
        "capture_status": "empty_confirmed",
        "error_reason": "",
        "venue": "CME",
        "date": "2024-01-06",
    }
    out = _gcs_metadata(
        bucket=None,
        object_path=None,
        manifest=manifest,
        pq_row_count=None,
    )
    assert out.error_reason is None


def test_gcs_metadata_skips_classifier_for_unknown_asset_group() -> None:
    # Defensive — if asset_group is something the helper doesn't know,
    # we don't crash; we just leave error_reason as None.
    manifest = {
        "capture_status": "empty_confirmed",
        "error_reason": "",
        "venue": "CME",
        "date": "2024-01-06",
    }
    out = _gcs_metadata(
        bucket=None,
        object_path=None,
        manifest=manifest,
        pq_row_count=None,
        asset_group="EQUITIES",  # not in LEGACY_REASON_ASSET_GROUPS
    )
    assert out.error_reason is None


def test_gcs_metadata_does_not_classify_captured_rows() -> None:
    # A captured row (i.e. real data on disk) should never be
    # classified — the reader-side fallback only fires on
    # empty_confirmed.
    manifest = {
        "capture_status": "captured",
        "error_reason": "",
        "venue": "CME",
        "date": "2024-01-06",
        "data_type": "ohlcv_1m",
    }
    out = _gcs_metadata(
        bucket=None,
        object_path=None,
        manifest=manifest,
        pq_row_count=1440,
        asset_group="TRADFI",
    )
    assert out.capture_status == "captured"
    assert out.error_reason is None


# ---------------------------------------------------------------------------
# data_status_drilldown.lookup_capture_status_for_shard
# ---------------------------------------------------------------------------


def test_lookup_capture_status_classifies_legacy_empty_sports_pre_coverage() -> None:
    # api_football coverage starts 2018-01-01 per UAC SOURCE_COVERAGE_START.
    # ``lookup_capture_status_for_shard`` uppercases the venue filter, so
    # the manifest stub mirrors that — the classifier itself lowercases
    # before looking the source up.
    fake_manifest = pd.DataFrame(
        [
            {
                "date": "2017-06-15",
                "service_name": "instruments-service",
                "venue": "API_FOOTBALL",
                "data_type": "FIXTURES",
                "capture_status": "empty_confirmed",
                "error_reason": "",
                "attempted_at": "2017-06-15T12:00:00Z",
                "written_at": "2017-06-15T12:00:00Z",
            }
        ]
    )
    with (
        patch(
            "deployment_api.services.data_status_drilldown.build_bucket_name",
            return_value="instruments-store-sports-stub",
        ),
        patch(
            "deployment_api.services.data_status_drilldown.read_availability_index",
            return_value=fake_manifest,
        ),
    ):
        result = lookup_capture_status_for_shard(
            service="instruments-service",
            asset_group="sports",
            day="2017-06-15",
            venue="API_FOOTBALL",
            data_type="FIXTURES",
        )
    assert result["status"] == "empty_confirmed"
    assert result["error_reason"] == "EXPECTED_PRE_SOURCE_COVERAGE_START"


def test_lookup_capture_status_preserves_existing_error_reason() -> None:
    fake_manifest = pd.DataFrame(
        [
            {
                "date": "2024-01-06",
                "service_name": "market-tick-data-service",
                "venue": "CME",
                "data_type": "ohlcv_1m",
                "capture_status": "empty_confirmed",
                "error_reason": "EXPECTED_WEEKEND",
                "attempted_at": "2024-01-06T12:00:00Z",
                "written_at": "2024-01-06T12:00:00Z",
            }
        ]
    )
    with (
        patch(
            "deployment_api.services.data_status_drilldown.build_bucket_name",
            return_value="market-data-tick-tradfi-stub",
        ),
        patch(
            "deployment_api.services.data_status_drilldown.read_availability_index",
            return_value=fake_manifest,
        ),
    ):
        result = lookup_capture_status_for_shard(
            service="market-tick-data-service",
            asset_group="tradfi",
            day="2024-01-06",
            venue="CME",
            data_type="ohlcv_1m",
        )
    assert result["error_reason"] == "EXPECTED_WEEKEND"

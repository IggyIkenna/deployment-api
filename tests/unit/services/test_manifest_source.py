"""manifest_source — live-vs-beta index selection (CF-20 preview)."""

from __future__ import annotations

import io
from unittest.mock import MagicMock, patch

import pandas as pd

from deployment_api.services import manifest_source


def _parquet_bytes(df: pd.DataFrame) -> bytes:
    buf = io.BytesIO()
    df.to_parquet(buf, index=False)
    return buf.getvalue()


def test_asset_group_derivation() -> None:
    assert manifest_source._asset_group_from_bucket("market-data-tick-pred-prd-p") == "prediction"
    assert manifest_source._asset_group_from_bucket("market-data-tick-tradfi-prd-p") == "tradfi"
    assert manifest_source._asset_group_from_bucket("instruments-store-sports-prd-p") == "sports"
    assert manifest_source._asset_group_from_bucket("some-other-bucket") == ""


def test_live_mode_delegates_to_utl_reader() -> None:
    live_df = pd.DataFrame({"date": ["2026-01-01"]})
    with (
        patch.object(manifest_source, "DATA_STATUS_BETA_MANIFEST_BLOB", ""),
        patch.object(manifest_source, "read_availability_index", return_value=live_df) as utl_read,
    ):
        out = manifest_source.read_manifest_index("market-data-tick-tradfi-prd-p")
    utl_read.assert_called_once_with("market-data-tick-tradfi-prd-p")
    assert out is live_df


def test_beta_mode_reads_projected_blob() -> None:
    beta_df = pd.DataFrame({"date": ["2026-02-02"], "capture_status": ["captured"]})
    client = MagicMock()
    client.download_bytes.return_value = _parquet_bytes(beta_df)
    with (
        patch.object(
            manifest_source,
            "DATA_STATUS_BETA_MANIFEST_BLOB",
            "_index/audit/projected_index_{asset_group}.parquet",
        ),
        patch.object(manifest_source, "get_storage_client", return_value=client),
        patch.object(manifest_source, "read_availability_index") as utl_read,
    ):
        out = manifest_source.read_manifest_index("market-data-tick-pred-prd-p")
    client.download_bytes.assert_called_once_with(
        "market-data-tick-pred-prd-p", "_index/audit/projected_index_prediction.parquet"
    )
    utl_read.assert_not_called()
    assert out["date"].tolist() == ["2026-02-02"]


def test_beta_mode_fails_loud_on_missing_projection() -> None:
    client = MagicMock()
    client.download_bytes.side_effect = FileNotFoundError("no projection")
    with (
        patch.object(
            manifest_source,
            "DATA_STATUS_BETA_MANIFEST_BLOB",
            "_index/audit/projected_index_{asset_group}.parquet",
        ),
        patch.object(manifest_source, "get_storage_client", return_value=client),
    ):
        try:
            manifest_source.read_manifest_index("market-data-tick-defi-prd-p")
            raise AssertionError("expected loud failure")
        except FileNotFoundError:
            pass

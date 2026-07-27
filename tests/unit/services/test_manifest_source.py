"""manifest_source — live consolidated availability index read (monitoring view)."""

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
    with patch.object(manifest_source, "read_availability_index", return_value=live_df) as utl_read:
        out = manifest_source.read_manifest_index("market-data-tick-tradfi-prd-p")
    # read_availability_index_bare_defi_callers_2026_07_27.md: the bare fallback is now
    # column-projected to DRILLDOWN_COLUMNS (one cache-miss from an OOM on the 1.58 GB
    # defi-prd index otherwise).
    utl_read.assert_called_once_with("market-data-tick-tradfi-prd-p", columns=manifest_source.DRILLDOWN_COLUMNS)
    assert out is live_df


def test_live_empty_falls_back_to_consolidated_blob() -> None:
    """When the UTL live read returns EMPTY (stale gate dropped a valid index), the
    monitoring read must surface the consolidated blob directly — not 0 rows. (§R5)"""
    stale_df = pd.DataFrame({"date": ["2026-06-11"], "venue": ["DERIBIT"], "capture_status": ["captured"]})
    client = MagicMock()
    client.download_bytes.return_value = _parquet_bytes(stale_df)
    with (
        patch.object(manifest_source, "read_availability_index", return_value=pd.DataFrame()),
        patch.object(manifest_source, "get_storage_client", return_value=client),
    ):
        out = manifest_source.read_manifest_index("instruments-store-cefi-prd-p")
    client.download_bytes.assert_called_once_with(
        "instruments-store-cefi-prd-p", manifest_source._CONSOLIDATED_INDEX_BLOB
    )
    assert out["venue"].tolist() == ["DERIBIT"]


def test_live_stale_error_falls_back_to_consolidated_blob() -> None:
    """A raising UTL read (e.g. ManifestConsolidatorStaleError) also degrades to the
    consolidated blob for the monitoring view rather than 500-ing the UI."""
    stale_df = pd.DataFrame({"date": ["2026-06-11"], "instrument_count": [42]})
    client = MagicMock()
    client.download_bytes.return_value = _parquet_bytes(stale_df)
    with (
        patch.object(manifest_source, "read_availability_index", side_effect=RuntimeError("consolidator stale")),
        patch.object(manifest_source, "get_storage_client", return_value=client),
    ):
        out = manifest_source.read_manifest_index("instruments-store-defi-prd-p")
    assert out["instrument_count"].tolist() == [42]


def test_live_empty_and_no_consolidated_blob_returns_empty() -> None:
    """A genuinely-empty bucket (no consolidated blob either) → empty df, not an error."""
    client = MagicMock()
    client.download_bytes.side_effect = FileNotFoundError("no index")
    with (
        patch.object(manifest_source, "read_availability_index", return_value=pd.DataFrame()),
        patch.object(manifest_source, "get_storage_client", return_value=client),
    ):
        out = manifest_source.read_manifest_index("instruments-store-sports-prd-p")
    assert out.empty


def test_stale_fallback_handles_legacy_shaped_blob_without_crash() -> None:
    """The stale-tolerant fallback (~lines 76-93) does a RAW ``pd.read_parquet``
    that bypasses UTL ``read_availability_index``'s v1->v8 schema backfill —
    it never touches the raw bytes' column set. A legacy-shaped consolidated
    blob (pre-v9: no ``capture_status`` / ``source`` / ``pipeline_mode`` /
    ``data_type`` columns) must not crash the monitoring read, and — since
    there is no backfill on this path — the returned frame is the raw legacy
    shape VERBATIM (documented current behaviour; a future accidental
    "helpful" backfill on this path is a behaviour change, not a bugfix, and
    should show up here as a diff)."""
    legacy_df = pd.DataFrame({"date": ["2020-01-01", "2020-01-02"], "venue": ["BINANCE", "BINANCE"]})
    client = MagicMock()
    client.download_bytes.return_value = _parquet_bytes(legacy_df)
    with (
        patch.object(manifest_source, "read_availability_index", return_value=pd.DataFrame()),
        patch.object(manifest_source, "get_storage_client", return_value=client),
    ):
        out = manifest_source.read_manifest_index("market-data-tick-tradfi-prd-p")
    assert not out.empty
    assert list(out.columns) == ["date", "venue"]
    assert "capture_status" not in out.columns
    assert "source" not in out.columns
    assert "pipeline_mode" not in out.columns
    assert out["date"].tolist() == ["2020-01-01", "2020-01-02"]


def test_mode_uses_rollup_fast_path() -> None:
    """get_manifest_status keeps the rollup fast-path — regression guard for the bypass."""
    import asyncio

    import deployment_api.services.data_status_service as dss_mod
    from deployment_api.services.data_status import manifest as manifest_mod

    svc = dss_mod.DataStatusService()
    rollup_payload: dict[str, object] = {"asset_groups": {}, "rollup": True}
    sliced: dict[str, object] = {"asset_groups": {}, "sliced": True}
    with (
        patch.object(dss_mod, "_read_rollup_if_fresh", return_value=rollup_payload) as rollup_read,
        patch.object(manifest_mod, "slice_rollup_to_window", return_value=sliced) as slicer,
    ):
        out = asyncio.run(svc.get_manifest_status("market-tick-data-service", "2026-01-01", "2026-01-02"))
    rollup_read.assert_called_once_with("market-tick-data-service")
    slicer.assert_called_once()
    assert out is sliced


def test_unique_instrument_count_reads_catalogue_and_caches() -> None:
    df = pd.DataFrame({"instrument_id": ["A", "B", "A", "C"]})
    client = MagicMock()
    client.download_bytes.return_value = _parquet_bytes(df)
    manifest_source._UNIQUE_COUNT_CACHE.clear()
    with (
        patch.object(manifest_source, "get_storage_client", return_value=client),
        patch("unified_trading_library.resolve_bucket_name", return_value="instruments-store-cefi-prd-p"),
    ):
        assert manifest_source.read_unique_instrument_count("cefi") == 3
        assert manifest_source.read_unique_instrument_count("cefi") == 3  # cached
    client.download_bytes.assert_called_once_with("instruments-store-cefi-prd-p", "prod/catalog.parquet")


def test_unique_instrument_count_returns_none_on_missing_catalogue() -> None:
    client = MagicMock()
    client.download_bytes.side_effect = FileNotFoundError("no catalogue")
    manifest_source._UNIQUE_COUNT_CACHE.clear()
    with (
        patch.object(manifest_source, "get_storage_client", return_value=client),
        patch("unified_trading_library.resolve_bucket_name", return_value="b"),
    ):
        assert manifest_source.read_unique_instrument_count("defi") is None

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


def test_live_empty_falls_back_to_consolidated_blob() -> None:
    """When the UTL live read returns EMPTY (stale gate dropped a valid index), the
    monitoring read must surface the consolidated blob directly — not 0 rows. (§R5)"""
    stale_df = pd.DataFrame({"date": ["2026-06-11"], "venue": ["DERIBIT"], "capture_status": ["captured"]})
    client = MagicMock()
    client.download_bytes.return_value = _parquet_bytes(stale_df)
    with (
        patch.object(manifest_source, "DATA_STATUS_BETA_MANIFEST_BLOB", ""),
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
        patch.object(manifest_source, "DATA_STATUS_BETA_MANIFEST_BLOB", ""),
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
        patch.object(manifest_source, "DATA_STATUS_BETA_MANIFEST_BLOB", ""),
        patch.object(manifest_source, "read_availability_index", return_value=pd.DataFrame()),
        patch.object(manifest_source, "get_storage_client", return_value=client),
    ):
        out = manifest_source.read_manifest_index("instruments-store-sports-prd-p")
    assert out.empty


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


def test_beta_mode_uses_beta_namespaced_rollup() -> None:
    """In beta mode get_manifest_status reads the BETA-NAMESPACED rollup.

    Previously beta bypassed the rollup entirely because it was live-derived (serving
    it in beta would quietly render live data — CF-20/V5). Now the worker writes a
    beta-namespaced rollup (``{service}/full.beta.json.gz``) computed from the
    projected v9 index, and ``_read_rollup_if_fresh`` reads that same beta blob in
    beta mode — so the all-asset-group beta view is served from cache instead of
    live-computing every asset group per request (which exceeded the Cloud Run request
    timeout -> HTTP 503). The "never serve live-derived data in beta" invariant is now
    preserved by the blob NAMESPACING (see
    ``test_data_status_beta_rollup_and_cli_config.test_rollup_blob_path_live_vs_beta``),
    not by bypassing the fast-path.
    """
    import asyncio

    import deployment_api.services.data_status_service as dss_mod
    from deployment_api.services.data_status import manifest as manifest_mod

    svc = dss_mod.DataStatusService()
    rollup_payload: dict[str, object] = {"asset_groups": {}, "beta_rollup": True}
    sliced: dict[str, object] = {"asset_groups": {}, "sliced": True}
    with (
        patch.object(
            manifest_source,
            "DATA_STATUS_BETA_MANIFEST_BLOB",
            "_index/audit/projected_index_{asset_group}.parquet",
        ),
        patch.object(dss_mod, "_read_rollup_if_fresh", return_value=rollup_payload) as rollup_read,
        patch.object(manifest_mod, "slice_rollup_to_window", return_value=sliced) as slicer,
    ):
        out = asyncio.run(svc.get_manifest_status("market-tick-data-service", "2026-01-01", "2026-01-02"))
    rollup_read.assert_called_once_with("market-tick-data-service")
    slicer.assert_called_once()
    assert out is sliced


def test_live_mode_still_uses_rollup_fast_path() -> None:
    """Live mode (blob unset) keeps the rollup fast-path — regression guard for the bypass."""
    import asyncio

    import deployment_api.services.data_status_service as dss_mod
    from deployment_api.services.data_status import manifest as manifest_mod

    svc = dss_mod.DataStatusService()
    rollup_payload: dict[str, object] = {"asset_groups": {}, "rollup": True}
    sliced: dict[str, object] = {"asset_groups": {}, "sliced": True}
    with (
        patch.object(manifest_source, "DATA_STATUS_BETA_MANIFEST_BLOB", ""),
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


def test_is_beta_mode_tracks_blob_setting() -> None:
    with patch.object(manifest_source, "DATA_STATUS_BETA_MANIFEST_BLOB", ""):
        assert manifest_source.is_beta_mode() is False
    with patch.object(manifest_source, "DATA_STATUS_BETA_MANIFEST_BLOB", "x_{asset_group}.parquet"):
        assert manifest_source.is_beta_mode() is True

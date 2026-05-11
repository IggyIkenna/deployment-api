"""Unit tests for ``deployment_api.services.shard_detail``.

Covers the four shard_class branches (grouped / per_symbol / reference /
fixtures), the shard-not-found graceful-degradation path, and the
CeFi + DeFi branches of ``fetch_venue_detail``.
"""

from __future__ import annotations

from unittest.mock import patch

import pandas as pd
import pytest

import deployment_api.services.shard_detail as svc
from deployment_api.utils.storage_facade import ObjectInfo

# ---------------------------------------------------------------------------
# shard_class classification
# ---------------------------------------------------------------------------


class TestClassifyShard:
    @pytest.mark.parametrize(
        ("service", "category", "instrument_type", "data_type", "expected"),
        [
            (
                "market-tick-data-service",
                "CEFI",
                "OPTIONS_CHAIN",
                "options_chain",
                "grouped",
            ),
            (
                "market-tick-data-service",
                "CEFI",
                "PERPETUAL",
                "trades",
                "per_symbol",
            ),
            (
                "instruments-service",
                "CEFI",
                "PERPETUAL",
                "instruments",
                "reference",
            ),
            (
                "market-tick-data-service",
                "SPORTS",
                "FIXTURE",
                "fixtures",
                "fixtures",
            ),
            (
                "market-tick-data-service",
                "DEFI",
                "POOL",
                "dex_swaps",
                "grouped",
            ),
        ],
    )
    def test_classifies_correctly(
        self,
        service: str,
        category: str,
        instrument_type: str,
        data_type: str,
        expected: str,
    ) -> None:
        assert (
            svc._classify_shard(  # pyright: ignore[reportPrivateUsage]
                service=service,
                category=category,
                instrument_type=instrument_type,
                data_type=data_type,
            )
            == expected
        )


# ---------------------------------------------------------------------------
# DeFi composite venue parsing
# ---------------------------------------------------------------------------


class TestDefiComposite:
    def test_composite_protocol_chain(self) -> None:
        assert svc._defi_composite_parts("AAVE_V3-ETHEREUM") == ("AAVE_V3", "ETHEREUM")  # pyright: ignore[reportPrivateUsage]

    def test_chain_only(self) -> None:
        assert svc._defi_composite_parts("ETHEREUM") == (None, "ETHEREUM")  # pyright: ignore[reportPrivateUsage]

    def test_none(self) -> None:
        assert svc._defi_composite_parts(None) == (None, None)  # pyright: ignore[reportPrivateUsage]


class TestInstrumentTypeAuto:
    """``_resolve_instrument_type_auto`` + ``_resolve_schema`` AUTO branch.

    Covers the deployment-ui DeFi click flow: the click site only knows
    ``data_type`` (e.g. ``oracle_prices``) and the composite venue (e.g.
    ``CHAINLINK-ETHEREUM``); the backend resolves ``instrument_type`` by
    scanning ``CONTRACT_REGISTRY``.
    """

    def test_auto_resolves_known_data_type(self) -> None:
        result = svc._resolve_instrument_type_auto(  # pyright: ignore[reportPrivateUsage]
            category="DEFI", data_type="dex_pools", venue=None
        )
        assert result == "pool"

    def test_auto_resolves_lending_data_type(self) -> None:
        result = svc._resolve_instrument_type_auto(  # pyright: ignore[reportPrivateUsage]
            category="DEFI", data_type="liquidation_events", venue=None
        )
        assert result == "lending"

    def test_auto_returns_none_for_unknown_data_type(self) -> None:
        result = svc._resolve_instrument_type_auto(  # pyright: ignore[reportPrivateUsage]
            category="DEFI", data_type="not_a_real_data_type", venue=None
        )
        assert result is None

    def test_resolve_schema_auto_mode_marks_resolution(self) -> None:
        schema, resolved = svc._resolve_schema(  # pyright: ignore[reportPrivateUsage]
            category="DEFI", instrument_type="AUTO", data_type="dex_pools", venue=None
        )
        assert schema.registered is True
        assert schema.instrument_type_resolved_via == "auto"
        assert schema.instrument_type_resolved == "pool"
        assert resolved == "pool"
        # New ColumnSpec fields must be present in the response (set by UAC cf79d54).
        assert any(col.required is True for col in schema.columns)

    def test_resolve_schema_explicit_mode_unchanged(self) -> None:
        schema, resolved = svc._resolve_schema(  # pyright: ignore[reportPrivateUsage]
            category="DEFI", instrument_type="pool", data_type="dex_pools", venue=None
        )
        assert schema.registered is True
        assert schema.instrument_type_resolved_via == "explicit"
        assert resolved == "pool"

    def test_resolve_schema_auto_unresolved_returns_none_via(self) -> None:
        schema, resolved = svc._resolve_schema(  # pyright: ignore[reportPrivateUsage]
            category="DEFI",
            instrument_type="AUTO",
            data_type="not_a_real_data_type",
            venue=None,
        )
        assert schema.registered is False
        assert schema.instrument_type_resolved_via == "none"
        assert schema.instrument_type_resolved is None
        # The caller's literal value passes through when resolution fails so
        # the response coord is honest about what was requested.
        assert resolved == "AUTO"


# ---------------------------------------------------------------------------
# get_shard_detail — grouped branch
# ---------------------------------------------------------------------------


class TestGetShardDetailGrouped:
    def test_grouped_returns_distinct_symbols(self) -> None:
        # UAC's options_chain contract uses ``underlying`` as the symbol column —
        # the distinct-symbols pass projects that column out of the bundle parquet.
        fake_df = pd.DataFrame({"underlying": ["BTC-25MAR26-50000-C", "BTC-25MAR26-55000-C"]})
        objects = [
            ObjectInfo(
                name=(
                    "raw_tick_data/by_date/day=2026-04-18/category=cefi/venue=DERIBIT/"
                    "instrument_type=options_chain/data_type=options_chain/underlying=BTC/ticks.parquet"
                ),
                size=1000,
            )
        ]
        with (
            patch.object(svc, "list_objects", return_value=objects),
            patch.object(svc, "_read_parquet_columns", return_value=fake_df),
            patch.object(svc, "_read_parquet_footer_row_count", return_value=2),
            patch.object(svc, "get_object_metadata", return_value={"size": 1000, "updated": None}),
            patch.object(svc, "_manifest_row_for_coord", return_value=None),
            patch.object(svc, "_parquet_signed_url", return_value=None),
        ):
            resp = svc.get_shard_detail(
                service="market-tick-data-service",
                asset_group="CEFI",
                instrument_type="options_chain",
                data_type="options_chain",
                day="2026-04-18",
                venue="DERIBIT",
                underlying="BTC",
            )
        assert resp.shard_class == "grouped"
        assert resp.payload_grouped is not None
        keys = [entry["key"] for entry in resp.payload_grouped.instrument_list]
        assert "BTC-25MAR26-50000-C" in keys
        assert resp.gcs.row_count == 2
        assert resp.gcs.capture_status == "captured"

    def test_grouped_with_missing_parquet_returns_missing_status(self) -> None:
        # Without an underlying leaf, _gcs_path_for_shard falls back to
        # list_objects and returns ``None`` when the listing is empty.
        with (
            patch.object(svc, "list_objects", return_value=[]),
            patch.object(svc, "get_object_metadata", return_value=None),
            patch.object(svc, "_manifest_row_for_coord", return_value=None),
            patch.object(svc, "_parquet_signed_url", return_value=None),
        ):
            resp = svc.get_shard_detail(
                service="market-tick-data-service",
                asset_group="CEFI",
                instrument_type="options_chain",
                data_type="options_chain",
                day="2026-04-18",
                venue="DERIBIT",
            )
        assert resp.gcs.capture_status == "missing"
        assert resp.gcs.path is None
        assert resp.sample_rows == []
        assert resp.payload_grouped is not None
        assert resp.payload_grouped.instrument_list == []


# ---------------------------------------------------------------------------
# get_shard_detail — per_symbol branch
# ---------------------------------------------------------------------------


class TestGetShardDetailPerSymbol:
    def test_per_symbol_returns_time_series_head(self) -> None:
        fake_df = pd.DataFrame({"ts_event": ["2026-04-18T00:00:00Z"] * 3, "price": [1.0, 2.0, 3.0]})
        # _mtds_shard_path lists the venue+data_type prefix and picks the
        # parquet whose name ends with ``/{leaf}.parquet`` (dual-vocab fan-out
        # introduced 2026-05-01).  We supply an ObjectInfo matching the leaf
        # suffix so resolution returns a real path under either vocabulary.
        objects: list[ObjectInfo] = [
            ObjectInfo(
                name=(
                    "raw_tick_data/by_date/day=2026-04-18/asset_group=cefi/"
                    "venue=DERIBIT/instrument_type=perpetual/data_type=trades/"
                    "BTC-PERPETUAL.parquet"
                )
            )
        ]
        with (
            patch.object(svc, "list_objects", return_value=objects),
            patch.object(svc, "_read_parquet_columns", return_value=fake_df),
            patch.object(svc, "_read_parquet_footer_row_count", return_value=3),
            patch.object(svc, "get_object_metadata", return_value={"size": 500, "updated": None}),
            patch.object(svc, "_manifest_row_for_coord", return_value=None),
            patch.object(svc, "_parquet_signed_url", return_value=None),
        ):
            resp = svc.get_shard_detail(
                service="market-tick-data-service",
                asset_group="CEFI",
                instrument_type="PERPETUAL",
                data_type="trades",
                day="2026-04-18",
                venue="DERIBIT",
                instrument_id="BTC-PERPETUAL",
            )
        assert resp.shard_class == "per_symbol"
        assert len(resp.sample_rows) == 3
        assert resp.payload_per_symbol is not None
        assert resp.payload_per_symbol.instrument_list == [
            {"key": "BTC-PERPETUAL", "type": "symbol"}
        ]


# ---------------------------------------------------------------------------
# get_shard_detail — reference branch
# ---------------------------------------------------------------------------


class TestGetShardDetailReference:
    def test_reference_returns_instrument_definitions(self) -> None:
        fake_df = pd.DataFrame(
            {
                "instrument_key": ["DERIBIT:OPTION:BTC-50000-C", "DERIBIT:OPTION:BTC-55000-C"],
                "instrument_type": ["OPTION", "OPTION"],
            }
        )
        with (
            patch.object(svc, "list_objects", return_value=[]),
            patch.object(svc, "_read_parquet_columns", return_value=fake_df),
            patch.object(svc, "_read_parquet_footer_row_count", return_value=2),
            patch.object(svc, "get_object_metadata", return_value={"size": 200, "updated": None}),
            patch.object(svc, "_manifest_row_for_coord", return_value=None),
            patch.object(svc, "_parquet_signed_url", return_value=None),
        ):
            resp = svc.get_shard_detail(
                service="instruments-service",
                asset_group="CEFI",
                instrument_type="OPTION",
                data_type="instruments",
                day="2026-04-18",
                venue="DERIBIT",
            )
        assert resp.shard_class == "reference"
        assert resp.payload_reference is not None
        assert len(resp.payload_reference.instrument_definitions) == 2


# ---------------------------------------------------------------------------
# get_shard_detail — fixtures branch
# ---------------------------------------------------------------------------


class TestGetShardDetailFixtures:
    def test_fixtures_branch_shard_class(self) -> None:
        # Fixtures branch does not resolve a single parquet — the payload
        # is derived from the sports_reference pipeline in other code
        # paths.  Verify the endpoint still returns a typed response with
        # the right shard_class + empty fixtures list when no GCS path
        # resolves.
        with (
            patch.object(svc, "_manifest_row_for_coord", return_value=None),
            patch.object(svc, "_parquet_signed_url", return_value=None),
        ):
            resp = svc.get_shard_detail(
                service="instruments-service",
                asset_group="SPORTS",
                instrument_type="FIXTURE",
                data_type="fixtures",
                day="2026-04-12",
                venue="SFI",
            )
        assert resp.shard_class == "fixtures"
        assert resp.payload_fixtures is not None
        # No parquet resolved → empty list but no exception.
        assert resp.payload_fixtures.fixtures == []


# ---------------------------------------------------------------------------
# fetch_venue_detail — DeFi branches
# ---------------------------------------------------------------------------


class TestFetchVenueDetailDefi:
    def test_chain_only_returns_protocol_list(self) -> None:
        df = pd.DataFrame(
            {
                "protocol": ["AAVE_V3", "AAVE_V3", "UNISWAP_V3"],
                "pool_address": ["0xAAA", "0xBBB", "0xCCC"],
                "pool_id": [None, None, None],
            }
        )
        with (
            patch.object(svc, "_pick_latest_day", return_value="2026-04-18"),
            patch.object(svc, "_read_instruments_day_df", return_value=df),
        ):
            resp = svc.fetch_venue_detail(
                service="instruments-service",
                asset_group="DEFI",
                venue="ETHEREUM",
            )
        assert resp.chain == "ETHEREUM"
        assert resp.protocol is None
        protocols = {p["name"] for p in resp.protocols}
        assert protocols == {"AAVE_V3", "UNISWAP_V3"}
        assert resp.total_pools == 3

    def test_composite_returns_pool_listing(self) -> None:
        df = pd.DataFrame(
            {
                "protocol": ["AAVE_V3", "AAVE_V3", "UNISWAP_V3"],
                "pool_address": ["0xAAA", "0xBBB", "0xCCC"],
                "fee_tier": [30, 30, 5],
            }
        )
        with (
            patch.object(svc, "_pick_latest_day", return_value="2026-04-18"),
            patch.object(svc, "_read_instruments_day_df", return_value=df),
        ):
            resp = svc.fetch_venue_detail(
                service="instruments-service",
                asset_group="DEFI",
                venue="AAVE_V3-ETHEREUM",
            )
        assert resp.chain == "ETHEREUM"
        assert resp.protocol == "AAVE_V3"
        # DataFrame filtered to AAVE_V3 → 2 pools.
        assert len(resp.pools) == 2
        assert resp.total_pools == 2

    def test_defi_no_data_returns_empty_envelope(self) -> None:
        with (
            patch.object(svc, "_pick_latest_day", return_value=None),
            patch.object(svc, "_read_instruments_day_df", return_value=None),
        ):
            resp = svc.fetch_venue_detail(
                service="instruments-service",
                asset_group="DEFI",
                venue="ETHEREUM",
            )
        assert resp.chain == "ETHEREUM"
        assert resp.total_pools == 0
        assert resp.protocols == []


# ---------------------------------------------------------------------------
# fetch_venue_detail — CeFi branch
# ---------------------------------------------------------------------------


class TestFetchVenueDetailCefi:
    def test_cefi_returns_instrument_listing(self) -> None:
        df = pd.DataFrame(
            {
                "instrument_key": [
                    "BINANCE:PERPETUAL:BTC-USDT",
                    "BINANCE:PERPETUAL:ETH-USDT",
                ],
                "instrument_type": ["PERPETUAL", "PERPETUAL"],
            }
        )
        with (
            patch.object(svc, "_pick_latest_day", return_value="2026-04-18"),
            patch.object(svc, "_read_instruments_day_df", return_value=df),
        ):
            resp = svc.fetch_venue_detail(
                service="instruments-service",
                asset_group="CEFI",
                venue="BINANCE",
            )
        assert resp.asset_group == "CEFI"
        assert resp.venue == "BINANCE"
        assert resp.total_instruments == 2
        assert len(resp.instruments) == 2


# ---------------------------------------------------------------------------
# get_leaf_parquet_stats (writegate Phase 4.A.3)
# ---------------------------------------------------------------------------


class TestGetLeafParquetStats:
    def test_unresolved_path_returns_unavailable_with_error_reason(self) -> None:
        # Service whose name doesn't bind to any path resolver — the helper
        # returns ``available=False`` rather than raising so the UI can
        # render the diagnostic state.
        with patch.object(svc, "_gcs_path_for_shard", return_value=None):
            resp = svc.get_leaf_parquet_stats(
                service="unknown-service",
                asset_group="CEFI",
                instrument_type="PERPETUAL",
                data_type="trades",
                day="2026-04-18",
                venue="BINANCE",
            )
        assert resp.available is False
        assert resp.gs_uri is None
        assert resp.error_reason is not None
        assert "path_unresolved" in resp.error_reason
        assert resp.row_count == 0
        assert resp.columns == []

    def test_parquet_read_failure_returns_unavailable(self) -> None:
        # Path resolves but pyarrow read raises — helper still returns a
        # response with the error_reason populated, never re-raises.
        with (
            patch.object(svc, "_gcs_path_for_shard", return_value=("bucket-x", "path/parquet")),
            patch.object(svc, "_file_size_via_metadata", return_value=1234),
            patch.object(
                svc,
                "_read_parquet_columns",
                side_effect=RuntimeError("simulated parquet corruption"),
            ),
        ):
            resp = svc.get_leaf_parquet_stats(
                service="market-tick-data-service",
                asset_group="CEFI",
                instrument_type="PERPETUAL",
                data_type="trades",
                day="2026-04-18",
                venue="BINANCE",
            )
        assert resp.available is False
        assert resp.gs_uri == "gs://bucket-x/path/parquet"
        assert resp.error_reason is not None
        assert "RuntimeError" in resp.error_reason
        assert "simulated parquet corruption" in resp.error_reason
        assert resp.file_size_bytes == 1234

    def test_successful_read_computes_per_column_stats(self) -> None:
        df = pd.DataFrame(
            {
                "ts_event": pd.to_datetime(["2026-04-18T00:00:00Z"] * 4, utc=True),
                "price": [1.0, 2.0, None, 4.0],  # 1 null
                "size": [10, 20, 30, 40],  # 0 nulls
                "available_at": pd.to_datetime(
                    [
                        "2026-04-18T00:00:01Z",
                        "2026-04-18T00:00:02Z",
                        None,
                        "2026-04-18T00:00:04Z",
                    ],
                    utc=True,
                ),
            }
        )
        with (
            patch.object(svc, "_gcs_path_for_shard", return_value=("bucket-x", "path/parquet")),
            patch.object(svc, "_file_size_via_metadata", return_value=42),
            patch.object(svc, "_read_parquet_columns", return_value=df),
        ):
            resp = svc.get_leaf_parquet_stats(
                service="market-tick-data-service",
                asset_group="CEFI",
                instrument_type="PERPETUAL",
                data_type="trades",
                day="2026-04-18",
                venue="BINANCE",
            )
        assert resp.available is True
        assert resp.row_count == 4
        assert resp.column_count == 4
        assert resp.file_size_bytes == 42
        assert resp.truncated is False

        by_name = {c.name: c for c in resp.columns}
        assert by_name["price"].non_null_count == 3
        assert by_name["price"].null_count == 1
        assert by_name["price"].nan_ratio == 0.25
        assert by_name["size"].non_null_count == 4
        assert by_name["size"].null_count == 0
        assert by_name["size"].nan_ratio == 0.0
        # available_at envelope present + min/max + null_count derived
        assert resp.available_at.present is True
        assert resp.available_at.null_count == 1
        assert resp.available_at.min_iso is not None
        assert resp.available_at.max_iso is not None
        assert "2026-04-18" in resp.available_at.min_iso

    def test_missing_available_at_column_marked_present_false(self) -> None:
        # Writegate Phase 1A.future MissingAvailableAt failure mode.
        df = pd.DataFrame({"price": [1.0, 2.0]})
        with (
            patch.object(svc, "_gcs_path_for_shard", return_value=("bucket-x", "path/parquet")),
            patch.object(svc, "_file_size_via_metadata", return_value=10),
            patch.object(svc, "_read_parquet_columns", return_value=df),
        ):
            resp = svc.get_leaf_parquet_stats(
                service="market-tick-data-service",
                asset_group="CEFI",
                instrument_type="PERPETUAL",
                data_type="trades",
                day="2026-04-18",
                venue="BINANCE",
            )
        assert resp.available is True
        assert resp.available_at.present is False
        assert resp.available_at.min_iso is None
        assert resp.available_at.max_iso is None

    def test_truncates_oversize_parquets(self) -> None:
        # Spec the helper bounds compute time at _LEAF_STATS_ROW_LIMIT.
        # Patch the limit down to a tractable number for the unit test,
        # then verify the truncated flag + truncated_at_rows are set.
        n_rows = 12
        df = pd.DataFrame({"price": list(range(n_rows)), "available_at": [None] * n_rows})
        with (
            patch.object(svc, "_gcs_path_for_shard", return_value=("bucket-x", "path/parquet")),
            patch.object(svc, "_file_size_via_metadata", return_value=99),
            patch.object(svc, "_read_parquet_columns", return_value=df),
            patch.object(svc, "_LEAF_STATS_ROW_LIMIT", 5),
        ):
            resp = svc.get_leaf_parquet_stats(
                service="market-tick-data-service",
                asset_group="CEFI",
                instrument_type="PERPETUAL",
                data_type="trades",
                day="2026-04-18",
                venue="BINANCE",
            )
        assert resp.available is True
        assert resp.truncated is True
        assert resp.truncated_at_rows == 5
        assert resp.row_count == 5

    def test_zero_row_parquet_handled_cleanly(self) -> None:
        df = pd.DataFrame({"price": [], "available_at": []})
        with (
            patch.object(svc, "_gcs_path_for_shard", return_value=("bucket-x", "path/parquet")),
            patch.object(svc, "_file_size_via_metadata", return_value=0),
            patch.object(svc, "_read_parquet_columns", return_value=df),
        ):
            resp = svc.get_leaf_parquet_stats(
                service="market-tick-data-service",
                asset_group="CEFI",
                instrument_type="PERPETUAL",
                data_type="trades",
                day="2026-04-18",
                venue="BINANCE",
            )
        assert resp.available is True
        assert resp.row_count == 0
        assert resp.column_count == 2
        # Every column has nan_ratio == 0.0 when row_count is 0.
        for c in resp.columns:
            assert c.nan_ratio == 0.0
            assert c.non_null_count == 0
            assert c.null_count == 0

    def test_coord_echoed_in_response(self) -> None:
        with patch.object(svc, "_gcs_path_for_shard", return_value=None):
            resp = svc.get_leaf_parquet_stats(
                service="instruments-service",
                asset_group="SPORTS",
                instrument_type="FIXTURE",
                data_type="fixtures",
                day="2026-04-18",
                venue=None,
                instrument_id="abc-123",
            )
        assert resp.coord.service == "instruments-service"
        assert resp.coord.asset_group == "SPORTS"
        assert resp.coord.day == "2026-04-18"
        assert resp.coord.instrument_id == "abc-123"

    # -----------------------------------------------------------------
    # feature_family resolution (Phase 8B — features-repo consolidation)
    # -----------------------------------------------------------------
    def test_feature_family_resolved_from_uac_when_path_unresolved(self) -> None:
        # Path doesn't resolve, but feature_group → UAC mapping should
        # still populate feature_family on the unavailable response so
        # the UI can render the family badge even on missing parquets.
        with patch.object(svc, "_gcs_path_for_shard", return_value=None):
            resp = svc.get_leaf_parquet_stats(
                service="features-onchain-service",
                asset_group="DEFI",
                instrument_type="LST",
                data_type="lst_yields",
                day="2026-04-18",
                venue="LIDO",
                feature_group="lst_staking_yields",
            )
        assert resp.available is False
        assert resp.feature_family == "onchain"
        assert resp.coord.feature_family == "onchain"

    def test_feature_family_from_parquet_column_overrides_group_mapping(self) -> None:
        # Writer-stamped feature_family wins over the read-side UAC
        # mapping (writer is the SSOT per UTL MissingFeatureFamilyError).
        df = pd.DataFrame(
            {
                "ts_event": pd.to_datetime(["2026-04-18T00:00:00Z"] * 3, utc=True),
                "value": [1.0, 2.0, 3.0],
                "available_at": pd.to_datetime(["2026-04-18T00:00:01Z"] * 3, utc=True),
                "feature_family": ["onchain", "onchain", "onchain"],
            }
        )
        with (
            patch.object(svc, "_gcs_path_for_shard", return_value=("bucket-x", "path/parquet")),
            patch.object(svc, "_file_size_via_metadata", return_value=10),
            patch.object(svc, "_read_parquet_columns", return_value=df),
        ):
            resp = svc.get_leaf_parquet_stats(
                service="features-onchain-service",
                asset_group="DEFI",
                instrument_type="LST",
                data_type="lst_yields",
                day="2026-04-18",
                venue="LIDO",
                # Operator passes a wrong / mismatched feature_group;
                # the parquet column wins.
                feature_group="aave_lending_rates",
            )
        assert resp.available is True
        assert resp.feature_family == "onchain"
        assert resp.coord.feature_family == "onchain"

    def test_feature_family_none_for_non_features_service(self) -> None:
        df = pd.DataFrame(
            {
                "ts_event": pd.to_datetime(["2026-04-18T00:00:00Z"] * 2, utc=True),
                "price": [1.0, 2.0],
                "available_at": pd.to_datetime(["2026-04-18T00:00:01Z"] * 2, utc=True),
            }
        )
        with (
            patch.object(svc, "_gcs_path_for_shard", return_value=("bucket-x", "path/parquet")),
            patch.object(svc, "_file_size_via_metadata", return_value=10),
            patch.object(svc, "_read_parquet_columns", return_value=df),
        ):
            resp = svc.get_leaf_parquet_stats(
                service="market-tick-data-service",
                asset_group="CEFI",
                instrument_type="PERPETUAL",
                data_type="trades",
                day="2026-04-18",
                venue="BINANCE",
            )
        assert resp.available is True
        assert resp.feature_family is None
        assert resp.coord.feature_family is None

    def test_feature_family_unknown_group_returns_none(self) -> None:
        with patch.object(svc, "_gcs_path_for_shard", return_value=None):
            resp = svc.get_leaf_parquet_stats(
                service="features-onchain-service",
                asset_group="DEFI",
                instrument_type="LST",
                data_type="lst_yields",
                day="2026-04-18",
                feature_group="not_a_real_feature_group",
            )
        assert resp.feature_family is None
        assert resp.coord.feature_family is None

    def test_feature_family_drift_in_parquet_logged_returns_none(self) -> None:
        # Writer contract violation: multiple distinct feature_family
        # values in one parquet. Helper logs a warning + falls back to
        # the UAC group mapping rather than picking arbitrarily.
        df = pd.DataFrame(
            {
                "value": [1.0, 2.0, 3.0],
                "available_at": pd.to_datetime(["2026-04-18T00:00:01Z"] * 3, utc=True),
                "feature_family": ["onchain", "volatility", "onchain"],
            }
        )
        with (
            patch.object(svc, "_gcs_path_for_shard", return_value=("bucket-x", "path/parquet")),
            patch.object(svc, "_file_size_via_metadata", return_value=10),
            patch.object(svc, "_read_parquet_columns", return_value=df),
        ):
            resp = svc.get_leaf_parquet_stats(
                service="features-onchain-service",
                asset_group="DEFI",
                instrument_type="LST",
                data_type="lst_yields",
                day="2026-04-18",
                feature_group="lst_staking_yields",
            )
        # Multi-value detected → falls back to UAC mapping for
        # ``lst_staking_yields`` (onchain).
        assert resp.available is True
        assert resp.feature_family == "onchain"


# ---------------------------------------------------------------------------
# Completeness envelope (writegate slice (b) Phase 5.5; forward-compatible)
# ---------------------------------------------------------------------------


class TestCompletenessEnvelope:
    """``_compute_completeness_envelope`` derives min/max/mean + null + incomplete-window counts.

    Forward-compatible: when the parquet predates the writegate slice (c)
    per-service rollout, the columns are absent → ``present=False``. When
    present, the envelope computes the float stats + counts incomplete_window
    rows where the JSON list is non-empty.
    """

    def test_absent_column_returns_present_false(self) -> None:
        df = pd.DataFrame({"open": [100.0, 101.0], "close": [101.0, 102.0]})
        env = svc._compute_completeness_envelope(df)
        assert env.present is False
        assert env.min_fraction is None
        assert env.max_fraction is None
        assert env.mean_fraction is None
        assert env.null_count == 0
        assert env.incomplete_window_present_count == 0

    def test_all_present_and_full_completeness(self) -> None:
        df = pd.DataFrame(
            {
                "open": [100.0, 101.0, 102.0],
                "completeness_fraction": [1.0, 1.0, 1.0],
                "incomplete_window": ["[]", "[]", "[]"],
            }
        )
        env = svc._compute_completeness_envelope(df)
        assert env.present is True
        assert env.min_fraction == 1.0
        assert env.max_fraction == 1.0
        assert env.mean_fraction == 1.0
        assert env.null_count == 0
        # All incomplete_window values are "[]" → none counted as present.
        assert env.incomplete_window_present_count == 0

    def test_mixed_completeness_and_populated_incomplete_window(self) -> None:
        df = pd.DataFrame(
            {
                "open": [100.0, 101.0, 102.0, 103.0],
                "completeness_fraction": [1.0, 0.95, 0.5, 1.0],
                "incomplete_window": [
                    "[]",
                    '[{"venue": "BINANCE"}]',
                    '[{"venue": "BINANCE"}, {"venue": "OKX"}]',
                    "[]",
                ],
            }
        )
        env = svc._compute_completeness_envelope(df)
        assert env.present is True
        assert env.min_fraction == 0.5
        assert env.max_fraction == 1.0
        assert env.mean_fraction == round((1.0 + 0.95 + 0.5 + 1.0) / 4, 4)
        # Two rows have non-empty incomplete_window lists.
        assert env.incomplete_window_present_count == 2

    def test_null_completeness_rows_counted_separately(self) -> None:
        df = pd.DataFrame(
            {
                "open": [100.0, 101.0, 102.0],
                "completeness_fraction": [1.0, None, 0.97],
            }
        )
        env = svc._compute_completeness_envelope(df)
        assert env.present is True
        assert env.null_count == 1
        assert env.min_fraction == 0.97
        assert env.max_fraction == 1.0

    def test_all_null_completeness_returns_no_stats(self) -> None:
        df = pd.DataFrame(
            {
                "open": [100.0, 101.0],
                "completeness_fraction": [None, None],
            }
        )
        env = svc._compute_completeness_envelope(df)
        assert env.present is True
        assert env.null_count == 2
        assert env.min_fraction is None
        assert env.max_fraction is None
        assert env.mean_fraction is None

    def test_incomplete_window_absent_when_completeness_present(self) -> None:
        # Some emissions log incomplete rows via event payload only; the
        # row-level column may not exist.
        df = pd.DataFrame(
            {
                "open": [100.0],
                "completeness_fraction": [0.92],
            }
        )
        env = svc._compute_completeness_envelope(df)
        assert env.present is True
        assert env.incomplete_window_present_count == 0

    def test_get_leaf_parquet_stats_threads_completeness_envelope(self) -> None:
        # End-to-end: get_leaf_parquet_stats forwards the envelope on the
        # response.
        df = pd.DataFrame(
            {
                "ts_event": pd.to_datetime(["2026-05-08T00:00:00Z"] * 2, utc=True),
                "open": [100.0, 101.0],
                "available_at": pd.to_datetime(["2026-05-08T00:00:01Z"] * 2, utc=True),
                "completeness_fraction": [0.97, 1.0],
                "incomplete_window": ['[{"venue": "BINANCE"}]', "[]"],
            }
        )
        with (
            patch.object(svc, "_gcs_path_for_shard", return_value=("bucket-x", "path/parquet")),
            patch.object(svc, "_file_size_via_metadata", return_value=42),
            patch.object(svc, "_read_parquet_columns", return_value=df),
        ):
            resp = svc.get_leaf_parquet_stats(
                service="market-data-processing-service",
                asset_group="CEFI",
                instrument_type="PERPETUAL",
                data_type="ohlcv_1h",
                day="2026-05-08",
                venue="BINANCE",
            )
        assert resp.available is True
        assert resp.completeness.present is True
        assert resp.completeness.min_fraction == 0.97
        assert resp.completeness.max_fraction == 1.0
        assert resp.completeness.incomplete_window_present_count == 1

    def test_get_leaf_parquet_stats_absent_columns_returns_present_false(self) -> None:
        # Legacy parquet predates the emission-policy rollout → present=False;
        # endpoint stays backward-compatible.
        df = pd.DataFrame(
            {
                "ts_event": pd.to_datetime(["2026-05-08T00:00:00Z"], utc=True),
                "open": [100.0],
                "available_at": pd.to_datetime(["2026-05-08T00:00:01Z"], utc=True),
            }
        )
        with (
            patch.object(svc, "_gcs_path_for_shard", return_value=("bucket-x", "path/parquet")),
            patch.object(svc, "_file_size_via_metadata", return_value=10),
            patch.object(svc, "_read_parquet_columns", return_value=df),
        ):
            resp = svc.get_leaf_parquet_stats(
                service="market-data-processing-service",
                asset_group="CEFI",
                instrument_type="PERPETUAL",
                data_type="ohlcv_1h",
                day="2026-05-08",
                venue="BINANCE",
            )
        assert resp.available is True
        assert resp.completeness.present is False

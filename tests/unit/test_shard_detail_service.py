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
                category="CEFI",
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
                category="CEFI",
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
        objects: list[ObjectInfo] = []  # path resolved via direct leaf
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
                category="CEFI",
                instrument_type="PERPETUAL",
                data_type="trades",
                day="2026-04-18",
                venue="DERIBIT",
                instrument_id="BTC-PERPETUAL",
            )
        assert resp.shard_class == "per_symbol"
        assert len(resp.sample_rows) == 3
        assert resp.payload_per_symbol is not None
        assert resp.payload_per_symbol.instrument_list == [{"key": "BTC-PERPETUAL", "type": "symbol"}]


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
                category="CEFI",
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
                category="SPORTS",
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
        assert resp.category == "CEFI"
        assert resp.venue == "BINANCE"
        assert resp.total_instruments == 2
        assert len(resp.instruments) == 2


# ---------------------------------------------------------------------------
# Prediction venue detail
# ---------------------------------------------------------------------------


class TestFetchVenueDetailPrediction:
    def test_prediction_v9_instrument_id_returns_canonical_question_groups(self) -> None:
        # v9 canonical shape: cqg value lives in ``instrument_id`` (ManifestWriter
        # row_key field), NOT in ``underlying``.
        df = pd.DataFrame(
            {
                "venue": ["POLYMARKET", "POLYMARKET", "POLYMARKET"],
                "data_type": [
                    "prediction_canonical_question_group",
                    "prediction_canonical_question_group",
                    "prediction_canonical_question_group",
                ],
                "instrument_id": ["BTC_UP_DOWN_HOURLY", "ETH_UP_DOWN_HOURLY", "FOOTBALL"],
                "instrument_count": [100, 80, 60],
                "date": ["2026-05-22", "2026-05-22", "2026-05-22"],
                # v9 extra columns
                "pipeline_mode": ["batch_polymarket_clob"] * 3,
                "source": ["polymarket"] * 3,
            }
        )
        with patch.object(svc, "read_availability_index", return_value=df):
            resp = svc.fetch_venue_detail(
                service="instruments-service",
                asset_group="PREDICTION",
                venue="POLYMARKET",
            )
        assert resp.category == "PREDICTION"
        assert resp.venue == "POLYMARKET"
        assert resp.total_instruments == 3
        groups = [r["canonical_question_group"] for r in resp.instruments]
        assert "BTC_UP_DOWN_HOURLY" in groups
        assert "ETH_UP_DOWN_HOURLY" in groups
        assert "FOOTBALL" in groups
        # v9: pipeline_mode + source surfaced in each leaf
        btc_row = next(r for r in resp.instruments if r["canonical_question_group"] == "BTC_UP_DOWN_HOURLY")
        assert btc_row["pipeline_mode"] == "batch_polymarket_clob"
        assert btc_row["source"] == "polymarket"

    def test_prediction_legacy_underlying_returns_canonical_question_groups(self) -> None:
        # Legacy (pre-v9) shape: cqg value in ``underlying`` column.
        df = pd.DataFrame(
            {
                "venue": ["POLYMARKET", "POLYMARKET", "POLYMARKET"],
                "data_type": [
                    "prediction_canonical_question_group",
                    "prediction_canonical_question_group",
                    "prediction_canonical_question_group",
                ],
                "underlying": ["BTC_UP_DOWN_HOURLY", "ETH_UP_DOWN_HOURLY", "FOOTBALL"],
                "instrument_count": [100, 80, 60],
                "date": ["2026-05-22", "2026-05-22", "2026-05-22"],
            }
        )
        with patch.object(svc, "read_availability_index", return_value=df):
            resp = svc.fetch_venue_detail(
                service="instruments-service",
                asset_group="PREDICTION",
                venue="POLYMARKET",
            )
        assert resp.category == "PREDICTION"
        assert resp.venue == "POLYMARKET"
        assert resp.total_instruments == 3
        groups = [r["canonical_question_group"] for r in resp.instruments]
        assert "BTC_UP_DOWN_HOURLY" in groups
        assert "ETH_UP_DOWN_HOURLY" in groups
        assert "FOOTBALL" in groups

    def test_prediction_empty_manifest_returns_zero(self) -> None:
        with patch.object(svc, "read_availability_index", return_value=pd.DataFrame()):
            resp = svc.fetch_venue_detail(
                service="instruments-service",
                asset_group="PREDICTION",
                venue="POLYMARKET",
            )
        assert resp.total_instruments == 0
        assert resp.instruments == []

    def test_prediction_reads_mtds_bucket_not_instruments_store(self) -> None:
        # Regression: the prediction cqg-bundle manifest (`prediction_canonical_question_group`
        # with `observed_clusters`) is written by MTDS into `market-data-tick-pred-prd-*`,
        # NOT the instruments-store. Reading the instruments bucket returned an empty/wrong
        # v9 drilldown. The read must target the MTDS bucket regardless of the caller's
        # `service` arg (the data source is definitionally the MTDS manifest).
        captured: dict[str, str] = {}

        def _capture(bucket: str) -> pd.DataFrame:
            captured["bucket"] = bucket
            return pd.DataFrame()

        with patch.object(svc, "read_availability_index", side_effect=_capture):
            svc.fetch_venue_detail(
                service="instruments-service",  # UI passes this; prediction must still read MTDS
                asset_group="PREDICTION",
                venue="POLYMARKET",
            )
        assert "market-data-tick-pred-prd" in captured["bucket"], captured
        assert "instruments-store" not in captured["bucket"], captured


# ---------------------------------------------------------------------------
# instrument_type=AUTO resolution
# ---------------------------------------------------------------------------


class TestResolveInstrumentTypeAuto:
    """AUTO-mode resolution from CONTRACT_REGISTRY.

    The DataStatusTab DeFi click site does not have ``instrument_type`` in
    scope (only ``data_type``).  Passing ``"AUTO"`` opts the caller into
    backend resolution against the UAC registry.
    """

    def test_auto_resolves_defi_oracle_prices_to_first_match(self) -> None:
        # ``(defi, *, oracle_prices)`` is registered in CONTRACT_REGISTRY for
        # at least ``a_token`` and ``lst`` (legacy venue overrides).  The
        # alphabetically-first match wins for determinism.
        picked = svc._resolve_instrument_type_auto(  # pyright: ignore[reportPrivateUsage]
            category="DEFI",
            data_type="oracle_prices",
        )
        assert picked is not None, "expected a registered match for (defi, oracle_prices)"
        # ``a_token`` < ``lst`` alphabetically.
        assert picked == "a_token"

    def test_auto_returns_none_for_nonexistent_data_type(self) -> None:
        picked = svc._resolve_instrument_type_auto(  # pyright: ignore[reportPrivateUsage]
            category="DEFI",
            data_type="nonexistent_data_type_xyz",
        )
        assert picked is None

    def test_get_shard_detail_auto_resolves_and_echoes_in_coord(self) -> None:
        """Top-level ``get_shard_detail`` accepts ``"AUTO"`` and echoes the
        resolved instrument_type back in the response coord.
        """
        with (
            patch.object(svc, "list_objects", return_value=[]),
            patch.object(svc, "get_object_metadata", return_value=None),
            patch.object(svc, "_manifest_row_for_coord", return_value=None),
            patch.object(svc, "_parquet_signed_url", return_value=None),
        ):
            resp = svc.get_shard_detail(
                service="market-tick-data-service",
                category="DEFI",
                instrument_type="AUTO",
                data_type="oracle_prices",
                day="2026-03-26",
                venue="CHAINLINK",
            )
        # Resolved instrument_type should be echoed in the coord — never
        # the literal "AUTO".
        assert resp.coord.instrument_type != "AUTO"
        assert resp.coord.instrument_type != ""
        assert resp.schema_.registered is True
        assert resp.schema_.instrument_type_resolved_via == "auto"

    def test_get_shard_detail_auto_unresolved_returns_registered_false(self) -> None:
        """When AUTO can't resolve, ``registered`` is False with an honest
        message and ``instrument_type_resolved_via == "none"``.
        """
        with (
            patch.object(svc, "list_objects", return_value=[]),
            patch.object(svc, "get_object_metadata", return_value=None),
            patch.object(svc, "_manifest_row_for_coord", return_value=None),
            patch.object(svc, "_parquet_signed_url", return_value=None),
        ):
            resp = svc.get_shard_detail(
                service="market-tick-data-service",
                category="DEFI",
                instrument_type="AUTO",
                data_type="nonexistent_data_type_xyz",
                day="2026-03-26",
                venue="CHAINLINK",
            )
        assert resp.schema_.registered is False
        assert resp.schema_.instrument_type_resolved_via == "none"
        assert "No SchemaContract found" in resp.schema_.message

    def test_get_shard_detail_explicit_instrument_type_preserved(self) -> None:
        """Existing callers passing a concrete ``instrument_type`` are
        unchanged — ``instrument_type_resolved_via`` is ``"explicit"``.
        """
        with (
            patch.object(svc, "list_objects", return_value=[]),
            patch.object(svc, "get_object_metadata", return_value=None),
            patch.object(svc, "_manifest_row_for_coord", return_value=None),
            patch.object(svc, "_parquet_signed_url", return_value=None),
        ):
            resp = svc.get_shard_detail(
                service="market-tick-data-service",
                category="CEFI",
                instrument_type="options_chain",
                data_type="options_chain",
                day="2026-04-18",
                venue="DERIBIT",
                underlying="BTC",
            )
        assert resp.schema_.instrument_type_resolved_via == "explicit"
        assert resp.coord.instrument_type == "options_chain"

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


# ---------------------------------------------------------------------------
# v8 manifest columns surfaced on ShardGcsMetadata
# (writegate Phase 4 — pipeline_mode + service_emission_state +
# last_emission_decision_at + expected_window_completeness_fraction)
# ---------------------------------------------------------------------------


class TestShardGcsMetadataV8Columns:
    """``_gcs_metadata`` threads the 4 new v8 manifest columns when present.

    Forward-compatibility check: pre-v8 manifest rows (column keys absent)
    leave all four fields as ``None`` so the UI renders placeholder pills.
    """

    def test_v8_columns_threaded_from_manifest_row(self) -> None:
        manifest = {
            "capture_status": "captured",
            "error_reason": "",
            "attempted_at": "2026-05-11T12:00:00Z",
            "pipeline_mode": "batch_databento",
            "service_emission_state": "PUBLISHED_DEGRADED",
            "last_emission_decision_at": "2026-05-11T12:34:56Z",
            "expected_window_completeness_fraction": "0.97",
        }
        with patch.object(svc, "get_object_metadata", return_value=None):
            result = svc._gcs_metadata(
                bucket="bucket-x",
                object_path="some/path.parquet",
                manifest=manifest,
                pq_row_count=42,
                asset_group="cefi",
            )
        assert result.pipeline_mode == "batch_databento"
        assert result.service_emission_state == "PUBLISHED_DEGRADED"
        assert result.last_emission_decision_at == "2026-05-11T12:34:56Z"
        assert result.expected_window_completeness_fraction == pytest.approx(0.97)
        # Existing fields still populated correctly.
        assert result.capture_status == "captured"
        assert result.row_count == 42

    def test_v8_columns_absent_returns_none_for_forward_compat(self) -> None:
        # Pre-v8 manifest row — no v8 keys present at all.
        manifest = {
            "capture_status": "captured",
            "error_reason": "",
        }
        with patch.object(svc, "get_object_metadata", return_value=None):
            result = svc._gcs_metadata(
                bucket="bucket-x",
                object_path="some/path.parquet",
                manifest=manifest,
                pq_row_count=42,
                asset_group="cefi",
            )
        assert result.pipeline_mode is None
        assert result.service_emission_state is None
        assert result.last_emission_decision_at is None
        assert result.expected_window_completeness_fraction is None
        # Capture status still passes through normally.
        assert result.capture_status == "captured"

    def test_v8_columns_none_when_no_manifest_row(self) -> None:
        # No manifest row at all (e.g. shard not in manifest yet) — v8
        # fields default to None; capture_status derives from GCS size only.
        with patch.object(svc, "get_object_metadata", return_value={"size": 100, "updated": None}):
            result = svc._gcs_metadata(
                bucket="bucket-x",
                object_path="some/path.parquet",
                manifest=None,
                pq_row_count=10,
                asset_group="cefi",
            )
        assert result.pipeline_mode is None
        assert result.service_emission_state is None
        assert result.last_emission_decision_at is None
        assert result.expected_window_completeness_fraction is None
        assert result.capture_status == "captured"

    def test_off_closed_set_service_emission_state_dropped_silently(self) -> None:
        # A garbage value on disk MUST NOT leak into the API response —
        # the Literal type means we'd otherwise corrupt the OpenAPI schema.
        manifest = {
            "capture_status": "captured",
            "error_reason": "",
            "service_emission_state": "UNKNOWN_GARBAGE",
        }
        with patch.object(svc, "get_object_metadata", return_value=None):
            result = svc._gcs_metadata(
                bucket="bucket-x",
                object_path="some/path.parquet",
                manifest=manifest,
                pq_row_count=1,
                asset_group="cefi",
            )
        assert result.service_emission_state is None

    def test_unparseable_completeness_fraction_falls_back_to_none(self) -> None:
        # Float parse failure on the completeness column logs a warning
        # and falls back to None — never raises.
        manifest = {
            "capture_status": "captured",
            "error_reason": "",
            "expected_window_completeness_fraction": "not-a-float",
        }
        with patch.object(svc, "get_object_metadata", return_value=None):
            result = svc._gcs_metadata(
                bucket="bucket-x",
                object_path="some/path.parquet",
                manifest=manifest,
                pq_row_count=1,
                asset_group="cefi",
            )
        assert result.expected_window_completeness_fraction is None

    def test_all_four_service_emission_states_accepted(self) -> None:
        for state in (
            "PUBLISHED_OK",
            "PUBLISHED_DEGRADED",
            "STALE_DATA_HEARTBEAT_ONLY",
            "BLOCKED",
        ):
            manifest = {
                "capture_status": "captured",
                "error_reason": "",
                "service_emission_state": state,
            }
            with patch.object(svc, "get_object_metadata", return_value=None):
                result = svc._gcs_metadata(
                    bucket="bucket-x",
                    object_path="some/path.parquet",
                    manifest=manifest,
                    pq_row_count=1,
                    asset_group="cefi",
                )
            assert result.service_emission_state == state

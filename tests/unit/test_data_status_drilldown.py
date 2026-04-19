"""Unit tests for data_status_drilldown helpers.

Covers:
- Schema lookup for registered and unregistered shards.
- Per-shard instrument listing under all three bundling modes
  (per_symbol / per_underlying / per_condition_id).
- Bucket counts (named markets + conditionIds inside OTHER).
- CSV export for both per_symbol and per_condition_id shards.
"""

from __future__ import annotations

from unittest.mock import patch

import pandas as pd
import pytest

import deployment_api.services.data_status_drilldown as drilldown
from deployment_api.utils.storage_facade import ObjectInfo


@pytest.fixture(autouse=True)
def _clear_cache():
    drilldown.clear_drilldown_cache()
    yield
    drilldown.clear_drilldown_cache()


# ---------------------------------------------------------------------------
# Schema
# ---------------------------------------------------------------------------


class TestGetSchemaForShard:
    def test_registered_contract_returns_columns(self):
        result = drilldown.get_schema_for_shard(
            category="cefi",
            instrument_type="perpetual",
            data_type="trades",
        )
        assert result["registered"] is True
        assert result["symbol_column"] == "symbol"
        assert result["source"] == "CONTRACT_REGISTRY"
        cols = result["columns"]
        assert isinstance(cols, list)
        names = [c["name"] for c in cols]
        assert "instrument_id" in names
        assert "ts_event" in names
        assert "price" in names

    def test_unregistered_shard_falls_back_gracefully(self):
        result = drilldown.get_schema_for_shard(
            category="cefi",
            instrument_type="unknown_type",
            data_type="unknown_dt",
        )
        assert result["registered"] is False
        assert result["columns"] == []
        assert "message" in result

    def test_venue_override_source_flagged(self):
        # Aave V3 lending indices has a venue-specific contract override.
        result = drilldown.get_schema_for_shard(
            category="defi",
            instrument_type="lending",
            data_type="lending_indices",
            venue="AAVE_V3",
        )
        assert result["registered"] is True
        assert result["source"] == "VENUE_CONTRACT_OVERRIDES"
        assert result["venue"] == "AAVE_V3"


# ---------------------------------------------------------------------------
# Instruments for shard
# ---------------------------------------------------------------------------


def _obj(name: str, size: int = 1024) -> ObjectInfo:
    return ObjectInfo(name=name, size=size)


class TestListInstrumentsForShard:
    def test_per_symbol_bundling_returns_file_stems(self):
        # CeFi perpetuals: per_symbol bundling
        objects = [
            _obj(
                "raw_tick_data/by_date/day=2025-04-01/category=cefi/venue=BINANCE/"
                "instrument_type=perpetual/data_type=trades/BTC-USDT-PERP.parquet"
            ),
            _obj(
                "raw_tick_data/by_date/day=2025-04-01/category=cefi/venue=BINANCE/"
                "instrument_type=perpetual/data_type=trades/ETH-USDT-PERP.parquet"
            ),
        ]
        with patch.object(drilldown, "list_objects", return_value=objects):
            result = drilldown.list_instruments_for_shard(
                service="market-tick-data-service",
                category="cefi",
                venue="BINANCE",
                day="2025-04-01",
                instrument_type="perpetual",
                data_type="trades",
            )
        assert result["bundling"] == "per_symbol"
        iids = [i["instrument_id"] for i in result["instruments"]]
        assert "BTC-USDT-PERP" in iids
        assert "ETH-USDT-PERP" in iids

    def test_per_underlying_bundling_for_options_chain(self):
        objects = [
            _obj(
                "raw_tick_data/by_date/day=2025-04-01/category=cefi/venue=DERIBIT/"
                "instrument_type=options_chain/data_type=trades/BTC.parquet"
            ),
            _obj(
                "raw_tick_data/by_date/day=2025-04-01/category=cefi/venue=DERIBIT/"
                "instrument_type=options_chain/data_type=trades/ETH.parquet"
            ),
        ]
        with patch.object(drilldown, "list_objects", return_value=objects):
            result = drilldown.list_instruments_for_shard(
                service="market-tick-data-service",
                category="cefi",
                venue="DERIBIT",
                day="2025-04-01",
                instrument_type="options_chain",
                data_type="trades",
            )
        assert result["bundling"] == "per_underlying"
        iids = [i["instrument_id"] for i in result["instruments"]]
        assert iids == ["BTC", "ETH"]

    def test_per_condition_id_reads_bundle_parquet(self):
        objects = [
            _obj(
                "raw_tick_data/by_date/day=2025-04-01/category=prediction/"
                "venue=POLYMARKET/instrument_type=OTHER/"
                "data_type=prediction_trades/ticks.parquet",
                size=500,
            )
        ]
        fake_ids = ["0xabc", "0xdef", "0x123"]
        with (
            patch.object(drilldown, "list_objects", return_value=objects),
            patch.object(
                drilldown,
                "_distinct_values_in_parquet",
                return_value=fake_ids,
            ),
        ):
            result = drilldown.list_instruments_for_shard(
                service="market-tick-data-service",
                category="prediction",
                venue="POLYMARKET",
                day="2025-04-01",
                instrument_type="OTHER",
                data_type="prediction_trades",
            )
        assert result["bundling"] == "per_condition_id"
        iids = [i["instrument_id"] for i in result["instruments"]]
        assert iids == fake_ids
        # All should point to the same bundle file
        assert len({i["file_uri"] for i in result["instruments"]}) == 1


# ---------------------------------------------------------------------------
# Bucket counts
# ---------------------------------------------------------------------------


class TestComputeBucketCounts:
    def test_named_plus_other_bucket(self):
        # Venue listing returns BTC, ETH, OTHER instrument_types.
        venue_level = [
            _obj(
                "raw_tick_data/by_date/day=2025-04-01/category=prediction/"
                "venue=POLYMARKET/instrument_type=BTC/data_type=prediction_trades/ticks.parquet"
            ),
            _obj(
                "raw_tick_data/by_date/day=2025-04-01/category=prediction/"
                "venue=POLYMARKET/instrument_type=ETH/data_type=prediction_trades/ticks.parquet"
            ),
            _obj(
                "raw_tick_data/by_date/day=2025-04-01/category=prediction/"
                "venue=POLYMARKET/instrument_type=OTHER/data_type=prediction_trades/ticks.parquet"
            ),
        ]
        other_level = [
            _obj(
                "raw_tick_data/by_date/day=2025-04-01/category=prediction/"
                "venue=POLYMARKET/instrument_type=OTHER/data_type=prediction_trades/ticks.parquet"
            )
        ]

        def _list_objects_side_effect(bucket: str, prefix: str, max_results: int | None = None):
            if "instrument_type=OTHER" in prefix:
                return other_level
            return venue_level

        with (
            patch.object(drilldown, "list_objects", side_effect=_list_objects_side_effect),
            patch.object(
                drilldown,
                "_distinct_values_in_parquet",
                return_value=["c1", "c2", "c3", "c4", "c5"],
            ),
        ):
            result = drilldown.compute_bucket_counts(
                service="market-tick-data-service",
                category="prediction",
                venue="POLYMARKET",
                day="2025-04-01",
                data_type="prediction_trades",
            )
        assert result == {"named_market_count": 2, "other_market_count": 5}

    def test_no_other_bucket(self):
        objects = [
            _obj(
                "raw_tick_data/by_date/day=2025-04-01/category=cefi/venue=BINANCE/"
                "instrument_type=perpetual/data_type=trades/BTC-USDT.parquet"
            ),
            _obj(
                "raw_tick_data/by_date/day=2025-04-01/category=cefi/venue=BINANCE/"
                "instrument_type=perpetual/data_type=trades/ETH-USDT.parquet"
            ),
        ]
        with patch.object(drilldown, "list_objects", return_value=objects):
            result = drilldown.compute_bucket_counts(
                service="market-tick-data-service",
                category="cefi",
                venue="BINANCE",
                day="2025-04-01",
                data_type="trades",
            )
        assert result == {"named_market_count": 1, "other_market_count": 0}


# ---------------------------------------------------------------------------
# CSV export
# ---------------------------------------------------------------------------


class TestBuildCsvExport:
    def test_per_symbol_export_filters_by_instrument_ids(self):
        objects = [
            _obj(
                "raw_tick_data/by_date/day=2025-04-01/category=cefi/venue=BINANCE/"
                "instrument_type=perpetual/data_type=trades/BTC-USDT-PERP.parquet"
            ),
            _obj(
                "raw_tick_data/by_date/day=2025-04-01/category=cefi/venue=BINANCE/"
                "instrument_type=perpetual/data_type=trades/ETH-USDT-PERP.parquet"
            ),
        ]
        df_btc = pd.DataFrame({"price": [1.0, 2.0], "size": [10, 20]})
        df_eth = pd.DataFrame({"price": [3.0], "size": [30]})

        def _read_parquet(uri: str, columns=None):
            if "BTC" in uri:
                return df_btc
            return df_eth

        with (
            patch.object(drilldown, "list_objects", return_value=objects),
            patch.object(drilldown, "_read_parquet_columns", side_effect=_read_parquet),
        ):
            csv_text, rows, fname = drilldown.build_csv_export(
                service="market-tick-data-service",
                category="cefi",
                venue="BINANCE",
                day="2025-04-01",
                instrument_type="perpetual",
                data_type="trades",
                instrument_ids=["BTC-USDT-PERP"],
            )
        assert rows == 2  # only BTC selected
        assert "price,size" in csv_text.split("\n")[0]
        assert fname.endswith(".csv")

    def test_per_condition_id_export_filters_symbol_column(self):
        objects = [
            _obj(
                "raw_tick_data/by_date/day=2025-04-01/category=prediction/"
                "venue=POLYMARKET/instrument_type=OTHER/"
                "data_type=prediction_trades/ticks.parquet"
            )
        ]
        df = pd.DataFrame(
            {
                "conditionId": ["0xabc", "0xabc", "0xdef", "0x123"],
                "price": [0.5, 0.6, 0.7, 0.8],
            }
        )
        with (
            patch.object(drilldown, "list_objects", return_value=objects),
            patch.object(
                drilldown,
                "_distinct_values_in_parquet",
                return_value=["0xabc", "0xdef", "0x123"],
            ),
            patch.object(drilldown, "_read_parquet_columns", return_value=df),
        ):
            csv_text, rows, _fname = drilldown.build_csv_export(
                service="market-tick-data-service",
                category="prediction",
                venue="POLYMARKET",
                day="2025-04-01",
                instrument_type="OTHER",
                data_type="prediction_trades",
                instrument_ids=["0xabc"],
            )
        assert rows == 2
        assert csv_text.count("0xabc") == 2

    def test_row_cap_raises(self):
        objects = [
            _obj(
                "raw_tick_data/by_date/day=2025-04-01/category=cefi/venue=BINANCE/"
                "instrument_type=perpetual/data_type=trades/BTC-USDT.parquet"
            )
        ]
        big = pd.DataFrame({"x": list(range(10))})
        with (
            patch.object(drilldown, "list_objects", return_value=objects),
            patch.object(drilldown, "_read_parquet_columns", return_value=big),
        ):
            with pytest.raises(ValueError, match="BigQuery"):
                drilldown.build_csv_export(
                    service="market-tick-data-service",
                    category="cefi",
                    venue="BINANCE",
                    day="2025-04-01",
                    instrument_type="perpetual",
                    data_type="trades",
                    instrument_ids=[],
                    max_rows=5,
                )


# ---------------------------------------------------------------------------
# Bucket name resolution
# ---------------------------------------------------------------------------


class TestBuildBucketName:
    def test_mtds_cefi_bucket(self):
        assert (
            drilldown.build_bucket_name("market-tick-data-service", "CEFI", "proj")
            == "market-data-tick-cefi-proj"
        )

    def test_instruments_service_bucket(self):
        assert (
            drilldown.build_bucket_name("instruments-service", "PREDICTION", "proj")
            == "instruments-store-prediction-proj"
        )

    def test_unknown_service_raises(self):
        with pytest.raises(ValueError, match="Unknown service"):
            drilldown.build_bucket_name("foobar", "CEFI", "proj")

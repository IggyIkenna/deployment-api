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

    def test_uppercase_defi_pool_resolves_uniswap_v3_override(self):
        """Regression from audit 2026-04-19 §5.5 — UI sends uppercase
        ``POOL`` / ``POOL_DEFINITION`` from the availability manifest but
        UAC keys them as ``pool`` / ``dex_pool_state``. The endpoint now
        normalises both to the canonical form so the Uniswap V3
        ``pool_address`` override resolves."""
        result = drilldown.get_schema_for_shard(
            category="DEFI",
            instrument_type="POOL",
            data_type="POOL_DEFINITION",
            venue="UNISWAP_V3",
        )
        assert result["registered"] is True
        assert result["symbol_column"] == "pool_address"
        assert result["source"] == "VENUE_CONTRACT_OVERRIDES"
        assert result["venue"] == "UNISWAP_V3"

    def test_uppercase_uniswap_v4_resolves_pool_id(self):
        result = drilldown.get_schema_for_shard(
            category="DEFI",
            instrument_type="POOL",
            data_type="POOL_DEFINITION",
            venue="UNISWAP_V4",
        )
        assert result["registered"] is True
        assert result["symbol_column"] == "pool_id"
        assert result["source"] == "VENUE_CONTRACT_OVERRIDES"

    def test_uppercase_data_type_alias_resolves(self):
        # POOL_SNAPSHOT is a UI-level alias for dex_pool_state.
        result = drilldown.get_schema_for_shard(
            category="DEFI",
            instrument_type="POOL",
            data_type="POOL_SNAPSHOT",
            venue="CURVE",
        )
        assert result["registered"] is True
        assert result["symbol_column"] == "pool_address"


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
# Search + pagination (Polymarket 5k-condition shards need these)
# ---------------------------------------------------------------------------


def _polymarket_listing(num_ids: int):
    """Build a `list_instruments_for_shard` fixture that returns `num_ids` IDs."""
    objects = [
        _obj(
            "raw_tick_data/by_date/day=2025-04-01/category=prediction/"
            "venue=POLYMARKET/instrument_type=OTHER/"
            "data_type=prediction_trades/ticks.parquet",
            size=500,
        )
    ]
    ids = [f"0x{i:04x}" for i in range(num_ids)]
    return objects, ids


class TestSearchAndPagination:
    def test_default_limit_returns_page_and_total(self):
        objects, ids = _polymarket_listing(200)
        with (
            patch.object(drilldown, "list_objects", return_value=objects),
            patch.object(drilldown, "_distinct_values_in_parquet", return_value=ids),
        ):
            result = drilldown.list_instruments_for_shard(
                service="market-tick-data-service",
                category="prediction",
                venue="POLYMARKET",
                day="2025-04-01",
                instrument_type="OTHER",
                data_type="prediction_trades",
            )
        assert result["total_count"] == 200
        assert result["limit"] == drilldown.DEFAULT_INSTRUMENT_LIMIT
        assert result["offset"] == 0
        assert result["has_more"] is True
        assert len(result["instruments"]) == drilldown.DEFAULT_INSTRUMENT_LIMIT

    def test_limit_is_capped_at_max(self):
        objects, ids = _polymarket_listing(10)
        with (
            patch.object(drilldown, "list_objects", return_value=objects),
            patch.object(drilldown, "_distinct_values_in_parquet", return_value=ids),
        ):
            result = drilldown.list_instruments_for_shard(
                service="market-tick-data-service",
                category="prediction",
                venue="POLYMARKET",
                day="2025-04-01",
                instrument_type="OTHER",
                data_type="prediction_trades",
                limit=9999,
            )
        # 10 ids total, caller asked for 9999 → clamped to MAX but only 10 returned.
        assert result["limit"] == drilldown.MAX_INSTRUMENT_LIMIT
        assert result["total_count"] == 10
        assert result["has_more"] is False

    def test_offset_pages_through(self):
        objects, ids = _polymarket_listing(120)
        with (
            patch.object(drilldown, "list_objects", return_value=objects),
            patch.object(drilldown, "_distinct_values_in_parquet", return_value=ids),
        ):
            page1 = drilldown.list_instruments_for_shard(
                service="market-tick-data-service",
                category="prediction",
                venue="POLYMARKET",
                day="2025-04-01",
                instrument_type="OTHER",
                data_type="prediction_trades",
                limit=50,
                offset=0,
            )
            page2 = drilldown.list_instruments_for_shard(
                service="market-tick-data-service",
                category="prediction",
                venue="POLYMARKET",
                day="2025-04-01",
                instrument_type="OTHER",
                data_type="prediction_trades",
                limit=50,
                offset=50,
            )
            page3 = drilldown.list_instruments_for_shard(
                service="market-tick-data-service",
                category="prediction",
                venue="POLYMARKET",
                day="2025-04-01",
                instrument_type="OTHER",
                data_type="prediction_trades",
                limit=50,
                offset=100,
            )
        assert page1["total_count"] == 120
        assert len(page1["instruments"]) == 50
        assert page1["has_more"] is True
        assert len(page2["instruments"]) == 50
        assert page2["has_more"] is True
        assert len(page3["instruments"]) == 20
        assert page3["has_more"] is False
        # Pages should not overlap.
        seen = {i["instrument_id"] for i in page1["instruments"]}
        for p in page2["instruments"] + page3["instruments"]:
            assert p["instrument_id"] not in seen

    def test_search_case_insensitive_substring(self):
        objects, _ids = _polymarket_listing(0)
        custom_ids = ["TRUMP-2024", "BIDEN-2024", "trump-macro", "Other"]
        with (
            patch.object(drilldown, "list_objects", return_value=objects),
            patch.object(drilldown, "_distinct_values_in_parquet", return_value=custom_ids),
        ):
            result = drilldown.list_instruments_for_shard(
                service="market-tick-data-service",
                category="prediction",
                venue="POLYMARKET",
                day="2025-04-01",
                instrument_type="OTHER",
                data_type="prediction_trades",
                search="trump",
            )
        iids = [i["instrument_id"] for i in result["instruments"]]
        assert sorted(iids) == ["TRUMP-2024", "trump-macro"]
        assert result["total_count"] == 2
        assert result["search"] == "trump"

    def test_search_capped_at_max_results(self):
        # Build many matches so we verify the 100-cap.
        matches = [f"TRUMP-{i}" for i in range(drilldown.MAX_SEARCH_RESULTS + 50)]
        objects, _ = _polymarket_listing(0)
        with (
            patch.object(drilldown, "list_objects", return_value=objects),
            patch.object(drilldown, "_distinct_values_in_parquet", return_value=matches),
        ):
            result = drilldown.list_instruments_for_shard(
                service="market-tick-data-service",
                category="prediction",
                venue="POLYMARKET",
                day="2025-04-01",
                instrument_type="OTHER",
                data_type="prediction_trades",
                search="trump",
                limit=drilldown.MAX_INSTRUMENT_LIMIT,
            )
        assert result["total_count"] == drilldown.MAX_SEARCH_RESULTS

    def test_search_is_noop_for_per_underlying(self):
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
                search="BTC",  # intentionally would filter out ETH in per_symbol
            )
        iids = [i["instrument_id"] for i in result["instruments"]]
        assert "BTC" in iids and "ETH" in iids
        assert result["bundling"] == "per_underlying"
        assert result["total_count"] == 2

    def test_empty_search_returns_all(self):
        objects, ids = _polymarket_listing(10)
        with (
            patch.object(drilldown, "list_objects", return_value=objects),
            patch.object(drilldown, "_distinct_values_in_parquet", return_value=ids),
        ):
            result = drilldown.list_instruments_for_shard(
                service="market-tick-data-service",
                category="prediction",
                venue="POLYMARKET",
                day="2025-04-01",
                instrument_type="OTHER",
                data_type="prediction_trades",
                search="",
            )
        assert result["total_count"] == 10

    def test_negative_offset_clamped_to_zero(self):
        objects, ids = _polymarket_listing(5)
        with (
            patch.object(drilldown, "list_objects", return_value=objects),
            patch.object(drilldown, "_distinct_values_in_parquet", return_value=ids),
        ):
            result = drilldown.list_instruments_for_shard(
                service="market-tick-data-service",
                category="prediction",
                venue="POLYMARKET",
                day="2025-04-01",
                instrument_type="OTHER",
                data_type="prediction_trades",
                offset=-10,
            )
        assert result["offset"] == 0
        assert len(result["instruments"]) == 5

    def test_offset_past_end_returns_empty_page(self):
        objects, ids = _polymarket_listing(5)
        with (
            patch.object(drilldown, "list_objects", return_value=objects),
            patch.object(drilldown, "_distinct_values_in_parquet", return_value=ids),
        ):
            result = drilldown.list_instruments_for_shard(
                service="market-tick-data-service",
                category="prediction",
                venue="POLYMARKET",
                day="2025-04-01",
                instrument_type="OTHER",
                data_type="prediction_trades",
                offset=500,
            )
        assert result["instruments"] == []
        assert result["total_count"] == 5
        assert result["has_more"] is False

    def test_instruments_service_full_shard_pagination(self):
        """Low-cardinality shards (instruments-service) fit in a single page."""
        objects = [
            _obj(
                f"raw_tick_data/by_date/day=2025-04-01/category=cefi/venue=BINANCE/"
                f"instrument_type=perpetual/data_type=instruments/{sym}.parquet"
            )
            for sym in ["BTC-USDT", "ETH-USDT", "SOL-USDT"]
        ]
        with patch.object(drilldown, "list_objects", return_value=objects):
            result = drilldown.list_instruments_for_shard(
                service="instruments-service",
                category="cefi",
                venue="BINANCE",
                day="2025-04-01",
                instrument_type="perpetual",
                data_type="instruments",
            )
        assert result["total_count"] == 3
        assert result["has_more"] is False
        assert result["bundling"] == "per_symbol"


class TestIsValidInstrumentId:
    def test_accepts_normal_ids(self):
        assert drilldown._is_valid_instrument_id("BTC-USDT-PERP") is True
        assert drilldown._is_valid_instrument_id("0xdeadbeef") is True
        assert drilldown._is_valid_instrument_id("BTC/USD") is True

    def test_rejects_empty_and_whitespace(self):
        assert drilldown._is_valid_instrument_id("") is False
        assert drilldown._is_valid_instrument_id("   ") is False
        assert drilldown._is_valid_instrument_id("BTC USDT") is False
        assert drilldown._is_valid_instrument_id("A,B") is False
        assert drilldown._is_valid_instrument_id("A\nB") is False


class TestPreviewBundleSymbols:
    def test_returns_symbols_for_per_underlying(self):
        objects = [
            _obj(
                "raw_tick_data/by_date/day=2025-04-01/category=cefi/venue=DERIBIT/"
                "instrument_type=options_chain/data_type=trades/BTC.parquet"
            ),
        ]
        with (
            patch.object(drilldown, "list_objects", return_value=objects),
            patch.object(
                drilldown,
                "_distinct_values_in_parquet",
                return_value=[f"BTC-OPT-{i}" for i in range(50)],
            ),
        ):
            result = drilldown.preview_bundle_symbols(
                service="market-tick-data-service",
                category="cefi",
                venue="DERIBIT",
                day="2025-04-01",
                instrument_type="options_chain",
                data_type="trades",
                limit=5,
            )
        assert result["bundling"] == "per_underlying"
        symbols = result["symbols"]
        assert isinstance(symbols, list)
        assert len(symbols) == 5
        assert result["underlying"] == "BTC"

    def test_non_bundled_returns_explanatory_message(self):
        result = drilldown.preview_bundle_symbols(
            service="market-tick-data-service",
            category="cefi",
            venue="BINANCE",
            day="2025-04-01",
            instrument_type="perpetual",
            data_type="trades",
        )
        assert result["bundling"] == "per_symbol"
        assert result["symbols"] == []
        assert "Preview" in str(result["message"])


class TestGetShardInfo:
    def test_multi_type_recommends_first_named(self):
        objects = [
            _obj(
                "raw_tick_data/by_date/day=2025-04-01/category=cefi/venue=DERIBIT/"
                "instrument_type=options_chain/data_type=trades/BTC.parquet"
            ),
            _obj(
                "raw_tick_data/by_date/day=2025-04-01/category=cefi/venue=DERIBIT/"
                "instrument_type=perpetual/data_type=trades/BTC-PERP.parquet"
            ),
        ]
        with patch.object(drilldown, "list_objects", return_value=objects):
            result = drilldown.get_shard_info(
                service="market-tick-data-service",
                category="cefi",
                venue="DERIBIT",
                day="2025-04-01",
                data_type="trades",
            )
        names = [t["name"] for t in result["instrument_types"]]
        assert "options_chain" in names and "perpetual" in names
        # Bundling hints propagated per type.
        options_row = next(t for t in result["instrument_types"] if t["name"] == "options_chain")
        assert options_row["bundling"] == "per_underlying"
        assert result["recommended_instrument_type"] in names

    def test_polymarket_other_only_recommends_other(self):
        objects = [
            _obj(
                "raw_tick_data/by_date/day=2025-04-01/category=prediction/"
                "venue=POLYMARKET/instrument_type=OTHER/"
                "data_type=prediction_trades/ticks.parquet"
            )
        ]
        with patch.object(drilldown, "list_objects", return_value=objects):
            result = drilldown.get_shard_info(
                service="market-tick-data-service",
                category="prediction",
                venue="POLYMARKET",
                day="2025-04-01",
                data_type="prediction_trades",
            )
        assert result["recommended_instrument_type"] == "OTHER"
        assert result["instrument_types"][0]["bundling"] == "per_condition_id"

    def test_empty_returns_none_recommended(self):
        with patch.object(drilldown, "list_objects", return_value=[]):
            result = drilldown.get_shard_info(
                service="market-tick-data-service",
                category="cefi",
                venue="BINANCE",
                day="2025-04-01",
                data_type="trades",
            )
        assert result["instrument_types"] == []
        assert result["recommended_instrument_type"] is None


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

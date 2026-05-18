"""Unit tests for ``build_pool_breakdown`` — DeFi per-pool drilldown.

Mirrors the sports ``build_fixture_breakdown`` test suite. Mocks
``_parquet_schema_names`` + ``_read_parquet_columns`` + ``list_objects`` so no
real GCS calls are made.
"""

from __future__ import annotations

from collections.abc import Iterable
from types import SimpleNamespace
from typing import cast
from unittest.mock import patch

import pandas as pd
import pytest

import deployment_api.services.data_status_drilldown as drilldown


@pytest.fixture(autouse=True)
def _clear_cache():
    drilldown.clear_drilldown_cache()
    yield
    drilldown.clear_drilldown_cache()


def _objs(paths: Iterable[str]):
    return [SimpleNamespace(name=p, size=0) for p in paths]


def _df(col: str, ids: list[str]) -> pd.DataFrame:
    return pd.DataFrame({col: ids})


class TestBuildPoolBreakdown:
    """Per-pool coverage breakdown for one (day, venue, chain) DeFi shard."""

    _DAY = "2026-04-20"

    def _venue_objs(self):
        # Realistic DeFi GCS object listing under venue=UNISWAP_V3-ETHEREUM/.
        base = f"raw_tick_data/by_date/day={self._DAY}/category=defi/venue=UNISWAP_V3-ETHEREUM"
        return _objs(
            [
                f"{base}/instrument_type=pool/data_type=dex_pool_state/ticks.parquet",
                f"{base}/instrument_type=pool/data_type=dex_pool_swaps/ticks.parquet",
            ]
        )

    def test_no_data_for_venue_chain(self):
        # Listing returns empty -> status="no_data".
        with patch.object(drilldown, "_list_pool_entities_for_venue", return_value=[]):
            result = drilldown.build_pool_breakdown(day=self._DAY, venue="UNISWAP_V3", chain="ETHEREUM")
        assert result["status"] == "no_data"
        assert result["pools"] == []
        assert result["pools_expected"] == 0
        assert result["venue_chain"] == "UNISWAP_V3-ETHEREUM"

    def test_all_pools_captured_in_all_entities(self):
        entities = [
            ("pool", "dex_pool_state", "gs://x/state.parquet"),
            ("pool", "dex_pool_swaps", "gs://x/swaps.parquet"),
        ]
        # Both entities have the same 3 pool IDs.
        pools = {"USDC-WETH-500", "WBTC-WETH-500", "USDC-USDT-100"}

        def fake_read(uri):
            return ("captured", pools)

        with (
            patch.object(drilldown, "_list_pool_entities_for_venue", return_value=entities),
            patch.object(drilldown, "_read_pool_ids_from_parquet", side_effect=fake_read),
        ):
            result = drilldown.build_pool_breakdown(day=self._DAY, venue="UNISWAP_V3", chain="ETHEREUM")
        assert result["status"] == "resolved"
        assert result["pools_expected"] == 3
        assert set(result["data_types_expected"]) == {"dex_pool_state", "dex_pool_swaps"}
        for pool in cast(list[dict[str, object]], result["pools"]):
            cov = cast(dict[str, str], pool["coverage"])
            assert cov == {"dex_pool_state": "captured", "dex_pool_swaps": "captured"}

    def test_mixed_coverage_some_missing(self):
        # State has all 3, swaps only has 1 (the others are "missing").
        entities = [
            ("pool", "dex_pool_state", "gs://x/state.parquet"),
            ("pool", "dex_pool_swaps", "gs://x/swaps.parquet"),
        ]

        def fake_read(uri):
            if "state.parquet" in uri:
                return ("captured", {"USDC-WETH-500", "WBTC-WETH-500", "USDC-USDT-100"})
            if "swaps.parquet" in uri:
                return ("captured", {"USDC-WETH-500"})
            return ("attempted_failed", set())

        with (
            patch.object(drilldown, "_list_pool_entities_for_venue", return_value=entities),
            patch.object(drilldown, "_read_pool_ids_from_parquet", side_effect=fake_read),
        ):
            result = drilldown.build_pool_breakdown(day=self._DAY, venue="UNISWAP_V3", chain="ETHEREUM")
        pools_by_id = {p["pool_id"]: p for p in cast(list[dict[str, object]], result["pools"])}
        usdc_weth = pools_by_id["USDC-WETH-500"]
        wbtc_weth = pools_by_id["WBTC-WETH-500"]
        cov_usdc = cast(dict[str, str], usdc_weth["coverage"])
        cov_wbtc = cast(dict[str, str], wbtc_weth["coverage"])
        assert cov_usdc["dex_pool_swaps"] == "captured"
        # WBTC is in state but missing from swaps.
        assert cov_wbtc["dex_pool_state"] == "captured"
        assert cov_wbtc["dex_pool_swaps"] == "missing"

    def test_failed_entity_reflects_in_coverage(self):
        # State parquet read errored — every pool's state shows "failed".
        entities = [
            ("pool", "dex_pool_state", "gs://x/state.parquet"),
            ("pool", "dex_pool_swaps", "gs://x/swaps.parquet"),
        ]

        def fake_read(uri):
            if "state.parquet" in uri:
                return ("attempted_failed", set())
            return ("captured", {"USDC-WETH-500"})

        with (
            patch.object(drilldown, "_list_pool_entities_for_venue", return_value=entities),
            patch.object(drilldown, "_read_pool_ids_from_parquet", side_effect=fake_read),
        ):
            result = drilldown.build_pool_breakdown(day=self._DAY, venue="UNISWAP_V3", chain="ETHEREUM")
        assert result["status"] == "resolved"
        # USDC-WETH-500 is the only pool surfaced via swaps; state failed for everyone.
        pools = cast(list[dict[str, object]], result["pools"])
        assert len(pools) == 1
        cov = cast(dict[str, str], pools[0]["coverage"])
        assert cov["dex_pool_state"] == "failed"
        assert cov["dex_pool_swaps"] == "captured"

    def test_venue_chain_partition_uppercased(self):
        # Mixed-case venue/chain inputs always normalised to UPPER for partition.
        with patch.object(drilldown, "_list_pool_entities_for_venue", return_value=[]):
            result = drilldown.build_pool_breakdown(day=self._DAY, venue="uniswap_v3", chain="ethereum")
        assert result["venue_chain"] == "UNISWAP_V3-ETHEREUM"
        assert result["venue"] == "UNISWAP_V3"
        assert result["chain"] == "ETHEREUM"


class TestListPoolEntitiesForVenue:
    """Direct test for _list_pool_entities_for_venue (path-parsing logic)."""

    def test_extracts_instrument_type_and_data_type_from_partitions(self):
        objs = _objs(
            [
                "raw_tick_data/by_date/day=2026-04-20/category=defi/venue=UNISWAP_V3-ETHEREUM/instrument_type=pool/data_type=dex_pool_state/ticks.parquet",
                "raw_tick_data/by_date/day=2026-04-20/category=defi/venue=UNISWAP_V3-ETHEREUM/instrument_type=pool/data_type=dex_pool_swaps/ticks.parquet",
            ]
        )
        from deployment_api.services import data_status_drilldown as dd

        with patch.object(dd, "list_objects", return_value=objs, create=True):
            # list_objects is imported lazily inside _list_pool_entities_for_venue.
            # We patch sys.modules path with the storage_facade rather than dd.
            from deployment_api.utils import storage_facade

            with patch.object(storage_facade, "list_objects", return_value=objs):
                result = drilldown._list_pool_entities_for_venue(day="2026-04-20", venue_chain="UNISWAP_V3-ETHEREUM")
        assert len(result) == 2
        instr_types = {r[0] for r in result}
        data_types = {r[1] for r in result}
        assert instr_types == {"pool"}
        assert data_types == {"dex_pool_state", "dex_pool_swaps"}

    def test_skips_non_parquet_files(self):
        objs = _objs(
            [
                "raw_tick_data/by_date/day=2026-04-20/category=defi/venue=AAVE_V3-ETHEREUM/instrument_type=a_token/data_type=oracle_prices/ticks.parquet",
                "raw_tick_data/by_date/day=2026-04-20/category=defi/venue=AAVE_V3-ETHEREUM/instrument_type=a_token/data_type=oracle_prices/_SUCCESS",
            ]
        )
        from deployment_api.utils import storage_facade

        with patch.object(storage_facade, "list_objects", return_value=objs):
            result = drilldown._list_pool_entities_for_venue(day="2026-04-20", venue_chain="AAVE_V3-ETHEREUM")
        # Only the parquet should make it through.
        assert len(result) == 1


class TestReadPoolIdsFromParquet:
    """Direct test for _read_pool_ids_from_parquet column-resolution logic."""

    def test_canonical_id_picks_instrument_key(self):
        # instrument_key (canonical) is preferred when present.
        df = _df("instrument_key", ["A:B:C", "D:E:F", "A:B:C"])  # dup → set
        with (
            patch.object(drilldown, "_parquet_schema_names", return_value={"instrument_key", "venue"}),
            patch.object(drilldown, "_read_parquet_columns", return_value=df),
        ):
            status, ids = drilldown._read_pool_ids_from_parquet("gs://b/x.parquet")
        assert status == "captured"
        assert ids == {"A:B:C", "D:E:F"}

    def test_falls_back_to_pool_address(self):
        # No instrument_key → fall back to pool_address.
        df = _df("pool_address", ["0xabc", "0xdef"])
        with (
            patch.object(drilldown, "_parquet_schema_names", return_value={"pool_address"}),
            patch.object(drilldown, "_read_parquet_columns", return_value=df),
        ):
            status, ids = drilldown._read_pool_ids_from_parquet("gs://b/x.parquet")
        assert status == "captured"
        assert ids == {"0xabc", "0xdef"}

    def test_no_canonical_column_returns_empty_confirmed(self):
        with patch.object(drilldown, "_parquet_schema_names", return_value={"random_col"}):
            status, ids = drilldown._read_pool_ids_from_parquet("gs://b/x.parquet")
        assert status == "empty_confirmed"
        assert ids == set()

    def test_attempted_failed_on_read_error(self):
        with patch.object(drilldown, "_parquet_schema_names", side_effect=FileNotFoundError("missing")):
            status, ids = drilldown._read_pool_ids_from_parquet("gs://b/x.parquet")
        assert status == "attempted_failed"
        assert ids == set()

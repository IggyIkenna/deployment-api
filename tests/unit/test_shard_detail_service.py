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

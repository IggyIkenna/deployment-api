"""Regression: DeFi download/drilldown path must use the combined venue+chain token.

instruments-service writes DeFi parquets at
``instrument_availability/by_date/day={date}/venue={protocol}-{chain}/instruments.parquet``
(e.g. ``venue=AAVE_V3-ARBITRUM``).  Before this fix, both the CSV-export
function and the drilldown prefix builder used bare ``venue={venue}`` —
causing 0-row returns and 502 path-drift responses for all DeFi shards.

Plan: data_status_tab_and_downloads_remediation_2026_06_16.md
"""

from __future__ import annotations

from unittest.mock import patch

import pandas as pd
import pytest

from deployment_api.services.data_status_drilldown._csv_export import (
    build_instruments_shard_csv_export,
)
from deployment_api.services.data_status_drilldown._instruments import (
    _shard_prefix,  # pyright: ignore[reportPrivateUsage]
)

# ---------------------------------------------------------------------------
# _shard_prefix — drilldown reader path
# ---------------------------------------------------------------------------


class TestShardPrefixDefi:
    def test_defi_with_chain_uses_combined_token(self) -> None:
        prefix = _shard_prefix(
            "instruments-service",
            "defi",
            "AAVE_V3",
            "2026-01-15",
            "",
            "",
            chain="ARBITRUM",
        )
        assert "venue=AAVE_V3-ARBITRUM" in prefix

    def test_defi_without_chain_falls_back_to_bare_venue(self) -> None:
        prefix = _shard_prefix(
            "instruments-service",
            "defi",
            "UNISWAP_V3",
            "2026-01-15",
            "",
            "",
            chain=None,
        )
        assert "venue=UNISWAP_V3" in prefix
        assert "-" not in prefix.split("venue=")[1].split("/")[0]

    def test_non_defi_ignores_chain(self) -> None:
        prefix = _shard_prefix(
            "instruments-service",
            "cefi",
            "BINANCE",
            "2026-01-15",
            "",
            "",
            chain="ETHEREUM",
        )
        assert "venue=BINANCE" in prefix
        assert "ETHEREUM" not in prefix

    def test_sports_uses_league_axis_not_chain(self) -> None:
        prefix = _shard_prefix(
            "instruments-service",
            "sports",
            "OPTA",
            "2026-01-15",
            "EPL",
            "",
            chain="ETHEREUM",
        )
        assert "league=EPL" in prefix
        assert "ETHEREUM" not in prefix

    def test_mtds_path_unaffected_by_chain(self) -> None:
        prefix = _shard_prefix(
            "market-tick-data-service",
            "defi",
            "UNISWAP_V3",
            "2026-01-15",
            "spot",
            "ohlcv_1m",
            chain="ETHEREUM",
        )
        # MTDS uses raw_tick_data/ path, chain has no special meaning there
        assert "raw_tick_data" in prefix


# ---------------------------------------------------------------------------
# build_instruments_shard_csv_export — CSV export GCS URI
# ---------------------------------------------------------------------------


class TestBuildInstrumentsShardCsvExportDefi:
    def _make_mock_df(self) -> pd.DataFrame:
        return pd.DataFrame([{"instrument_key": "AAVE_V3-ARBITRUM:LENDING:USDC"}])

    def test_defi_with_chain_reads_combined_venue_path(self) -> None:
        captured_uri: list[str] = []

        def _fake_read_parquet(uri: str) -> pd.DataFrame:
            captured_uri.append(uri)
            return self._make_mock_df()

        with (
            patch(
                "deployment_api.services.data_status_drilldown._read_parquet_columns",
                side_effect=_fake_read_parquet,
            ),
            patch(
                "deployment_api.services.data_status_drilldown.build_bucket_name",
                return_value="instruments-store-defi-prd",
            ),
        ):
            csv_text, row_count, filename = build_instruments_shard_csv_export(
                service="instruments-service",
                asset_group="defi",
                venue="AAVE_V3",
                date="2026-01-15",
                chain="ARBITRUM",
            )

        assert len(captured_uri) == 1
        assert "venue=AAVE_V3-ARBITRUM" in captured_uri[0]
        assert row_count == 1

    def test_defi_without_chain_uses_bare_venue(self) -> None:
        captured_uri: list[str] = []

        def _fake_read_parquet(uri: str) -> pd.DataFrame:
            captured_uri.append(uri)
            return self._make_mock_df()

        with (
            patch(
                "deployment_api.services.data_status_drilldown._read_parquet_columns",
                side_effect=_fake_read_parquet,
            ),
            patch(
                "deployment_api.services.data_status_drilldown.build_bucket_name",
                return_value="instruments-store-defi-prd",
            ),
        ):
            build_instruments_shard_csv_export(
                service="instruments-service",
                asset_group="defi",
                venue="AAVE_V3",
                date="2026-01-15",
                chain=None,
            )

        assert len(captured_uri) == 1
        assert "venue=AAVE_V3/" in captured_uri[0]
        assert "-ARBITRUM" not in captured_uri[0]

    def test_non_defi_ignores_chain_parameter(self) -> None:
        captured_uri: list[str] = []

        def _fake_read_parquet(uri: str) -> pd.DataFrame:
            captured_uri.append(uri)
            return self._make_mock_df()

        with (
            patch(
                "deployment_api.services.data_status_drilldown._read_parquet_columns",
                side_effect=_fake_read_parquet,
            ),
            patch(
                "deployment_api.services.data_status_drilldown.build_bucket_name",
                return_value="instruments-store-cefi-prd",
            ),
        ):
            build_instruments_shard_csv_export(
                service="instruments-service",
                asset_group="cefi",
                venue="BINANCE",
                date="2026-01-15",
                chain="ETHEREUM",
            )

        assert len(captured_uri) == 1
        assert "venue=BINANCE/" in captured_uri[0]
        assert "ETHEREUM" not in captured_uri[0]

    def test_unsupported_service_raises_value_error(self) -> None:
        with pytest.raises(ValueError, match="does not use per-venue-day-bundle"):
            build_instruments_shard_csv_export(
                service="market-tick-data-service",
                asset_group="defi",
                venue="UNISWAP_V3",
                date="2026-01-15",
                chain="ETHEREUM",
            )

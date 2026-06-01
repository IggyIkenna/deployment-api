"""Cat A.4 — chain breakdown headline math: shards >> dates.

Plan: ``data_status_comprehensive_test_coverage_2026_05_07.plan.md`` Phase 1.

Reference incident 2026-05-07: ``_build_chain_breakdown`` reported
``ARBITRUM 32/54 shards`` (date-count math) for a 25k-true-shard universe.
The headline ratio shown in the UI must be the SHARD count
(``captured / expected_shards``), not the DATE count
(``dates_found / dates_expected``). The codex DeFi shard atom is
``(asset_group=defi, chain, venue/protocol, data_type, instrument_id, day)``
— per-day per-protocol per-instrument, NOT per-day collapsed.

This test synthesizes a small DeFi manifest with multiple data_types +
instruments per (chain, venue) and asserts the breakdown returns
``shards_expected >> dates_expected`` (because the within-day fan-out
multiplies the date count). If a future refactor collapses the headline
back to date-only math, ``shards_expected == dates_expected`` and the
test fails loud.
"""

from __future__ import annotations

from unittest.mock import MagicMock

import pandas as pd

from deployment_api.services.data_status_service import DataStatusService


def _stub_venue_mapping() -> MagicMock:
    """Stub VenueMapping that returns no per-venue start-date overrides;
    the chain-breakdown helper falls back to the calendar range."""
    mapping = MagicMock()
    mapping.get_venue_start_date = MagicMock(return_value=None)
    mapping.get_expected_trading_dates = MagicMock(return_value=[])
    return mapping


def _row(
    *,
    venue: str,
    chain: str,
    data_type: str,
    instrument_id: str,
    date: str,
    capture_status: str = "captured",
) -> dict[str, object]:
    return {
        "venue": venue,
        "chain": chain,
        "data_type": data_type,
        "instrument_id": instrument_id,
        "instrument_type": "",
        "date": date,
        "capture_status": capture_status,
        "asset_group": "defi",
    }


class TestChainBreakdownHeadlineIsShardsNotDates:
    """The ``ARBITRUM 32/54`` regression — headline shard count must
    fan out across (data_type, instrument) pairs per day, not collapse
    to a date count."""

    def test_per_chain_shards_exceed_dates_when_multiple_data_types_and_instruments(
        self,
    ) -> None:
        """For ARBITRUM with 2 venues x 3 data_types x 4 instruments x 5 days
        the shard universe is 2*3*4*5 = 120 cells, dates universe is just 5.
        Asserts the breakdown surfaces shards_found / shards_expected at the
        right magnitude.
        """
        rows: list[dict[str, object]] = []
        for venue in ("AAVE_V3-ARBITRUM", "BALANCER-ARBITRUM"):
            for data_type in ("lending_indices", "dex_swaps", "dex_pools"):
                for inst in ("USDC", "USDT", "WETH", "WBTC"):
                    for day in (
                        "2024-03-04",
                        "2024-03-05",
                        "2024-03-06",
                        "2024-03-07",
                        "2024-03-08",
                    ):
                        rows.append(
                            _row(
                                venue=venue,
                                chain="ARBITRUM",
                                data_type=data_type,
                                instrument_id=inst,
                                date=day,
                            )
                        )
        df = pd.DataFrame(rows)
        dss = DataStatusService()
        # _venue_expected_dates_for_chain reads venue_mapping for expected
        # date sets; with no override it falls back to the captured-date
        # set so dates_expected == 5 here. The shards math is independent.
        breakdown = dss._build_chain_breakdown(
            df,
            start_date="2024-03-04",
            end_date="2024-03-08",
            venue_mapping=_stub_venue_mapping(),
        )

        assert "ARBITRUM" in breakdown
        arb = breakdown["ARBITRUM"]
        assert isinstance(arb, dict)
        # Headline shard count is the captured-row total — 120 here.
        # Date count is at most 5.
        shards_found = arb.get("shards_found")
        dates_found = arb.get("dates_found")
        assert isinstance(shards_found, int)
        assert isinstance(dates_found, int)
        assert shards_found == 120, (
            f"shards_found should be 120 (2 venues x 3 data_types x 4 instruments x 5 days), got {shards_found}"
        )
        assert shards_found > dates_found * 10, (
            f"shard-vs-date fanout collapsed: shards_found={shards_found} "
            f"dates_found={dates_found} — headline math regressed to date-only"
        )

    def test_capture_status_filter_separates_captured_from_total(self) -> None:
        """When capture_status column is present, only ``captured`` rows
        count toward shards_found; empty_confirmed / attempted_failed
        rows do NOT contribute. Mirrors the v5+ honest-coverage rule."""
        rows: list[dict[str, object]] = []
        # 5 captured + 3 empty_confirmed + 2 attempted_failed for the same
        # (chain, venue, data_type, instrument, varying days).
        for i in range(5):
            rows.append(
                _row(
                    venue="AAVE_V3-BASE",
                    chain="BASE",
                    data_type="lending_indices",
                    instrument_id="USDC",
                    date=f"2024-03-{i + 1:02d}",
                    capture_status="captured",
                )
            )
        for i in range(3):
            rows.append(
                _row(
                    venue="AAVE_V3-BASE",
                    chain="BASE",
                    data_type="lending_indices",
                    instrument_id="USDC",
                    date=f"2024-03-{i + 6:02d}",
                    capture_status="empty_confirmed",
                )
            )
        for i in range(2):
            rows.append(
                _row(
                    venue="AAVE_V3-BASE",
                    chain="BASE",
                    data_type="lending_indices",
                    instrument_id="USDC",
                    date=f"2024-03-{i + 9:02d}",
                    capture_status="attempted_failed",
                )
            )
        df = pd.DataFrame(rows)
        dss = DataStatusService()
        breakdown = dss._build_chain_breakdown(
            df,
            start_date="2024-03-01",
            end_date="2024-03-10",
            venue_mapping=_stub_venue_mapping(),
        )
        assert "BASE" in breakdown
        base = breakdown["BASE"]
        assert isinstance(base, dict)
        # Only the 5 captured rows count toward shards_found.
        assert base.get("shards_found") == 5

    def test_empty_chain_column_returns_empty_breakdown(self) -> None:
        """When the manifest has no DeFi rows (no chain column), the
        breakdown is an empty dict — never a NaN / null / partial."""
        df = pd.DataFrame(
            [
                {
                    "venue": "BYBIT",
                    "data_type": "trades",
                    "date": "2024-03-04",
                    "capture_status": "captured",
                }
            ]
        )
        dss = DataStatusService()
        breakdown = dss._build_chain_breakdown(
            df,
            start_date="2024-03-04",
            end_date="2024-03-04",
            venue_mapping=_stub_venue_mapping(),
        )
        assert breakdown == {}

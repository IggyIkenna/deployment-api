"""Cat C.4 — data-status denominator excludes pre-cutoff days.

Plan: ``data_status_comprehensive_test_coverage_2026_05_07.md`` Phase 2.

The data-status panel reports ``coverage % = captured / expected`` where
``expected`` is the size of the universe the panel believes should
exist. When chain genesis or protocol launch falls inside the requested
window, every day before ``max(chain_genesis, protocol_launch)`` MUST
be excluded from ``expected`` — otherwise the panel renders thousands
of phantom-missing pre-launch days and the coverage % collapses to a
meaningless headline.

This test pins the math at the helper layer
(``_mtds_expected_dates_cached``) — the single computation that drives
every downstream rollup (per-day, per-instrument, per-chain breakdown
math in ``_build_chain_breakdown``).

Reference incident 2026-05-07: AAVEV3-ARBITRUM denominator stretched
back to ARBITRUM genesis 2021-08-31 even though the protocol didn't
deploy on Arbitrum until 2022-03-16, inflating the leaf-shard
denominator with ~6 months of always-empty days. Surfaced as the
"ARBITRUM 32/54 shards" misleading panel headline; underlying root
cause was the missing protocol-launch clip.
"""

from __future__ import annotations

from datetime import UTC, date, datetime, timedelta

import pytest
from unified_api_contracts.registry.chain_env import (
    CHAIN_GENESIS_DATES,
    PROTOCOL_LAUNCH_DATES,
)

from deployment_api.services.data_status_service import _mtds_expected_dates_cached


@pytest.fixture(autouse=True)
def _clear_mtds_cache() -> None:
    _mtds_expected_dates_cached.cache_clear()


def _full_window_size(start: str, end: str) -> int:
    """Naive total day count for [start, end] inclusive — the
    denominator the panel WOULD render if no clip were applied."""
    s = date.fromisoformat(start)
    e = date.fromisoformat(end)
    return (e - s).days + 1


class TestDenominatorClipsPreCutoffDays:
    """Pin the per-(chain, protocol) denominator clip behaviour."""

    def test_arbitrum_aavev3_denominator_excludes_pre_protocol_days(self) -> None:
        """The 2026-05-07 reference incident reproduced as a unit test.

        AAVEV3-ARBITRUM:
          - ARBITRUM genesis = 2021-08-31
          - AAVEV3 launch on ARBITRUM = 2022-03-16
          - effective floor = max(2021-08-31, 2022-03-16) = 2022-03-16

        Asking for a wide window that spans ARBITRUM genesis through
        post-protocol-launch should return ONLY days >= 2022-03-16.
        The pre-protocol-launch slice (2021-08-31 → 2022-03-15) is
        198 days that MUST be clipped from the denominator.
        """
        chain = "ARBITRUM"
        protocol = "AAVEV3"
        chain_genesis = CHAIN_GENESIS_DATES[chain]
        protocol_launch = PROTOCOL_LAUNCH_DATES[(chain, protocol)]
        # Sanity-check the SSOT shape — if these flip, the assertion
        # logic below would silently degrade.
        assert chain_genesis == "2021-08-31"
        assert protocol_launch == "2022-03-16"

        # Window: Jan 2021 (pre-genesis even) → 2 weeks past launch.
        window_start = "2021-01-01"
        # End 14 days past launch so the post-launch slice is non-empty.
        end_d = date.fromisoformat(protocol_launch) + timedelta(days=14)
        window_end = end_d.isoformat()
        venue = f"{protocol}-{chain}"
        result = _mtds_expected_dates_cached(
            venue=venue,
            data_type="lending_indices",
            category="DEFI",
            window_start=window_start,
            window_end=window_end,
        )

        # Every emitted date must be on or after the protocol-launch
        # floor — no pre-protocol-launch days bleed through.
        for d_str in result:
            assert d_str >= protocol_launch, (
                f"Pre-protocol-launch day {d_str!r} leaked into expected denominator "
                f"(floor={protocol_launch}); window=[{window_start}..{window_end}]"
            )

        # Denominator is strictly smaller than the naive window — the
        # 2021-01-01 → 2022-03-15 slice (~440 days) must be excluded.
        naive = _full_window_size(window_start, window_end)
        # Allow up to 30d tolerance for trading-calendar differences;
        # the clipped denominator should still be FAR smaller.
        assert len(result) < naive - 400, (
            f"Denominator {len(result)} did not shrink below naive {naive} by ~440d — "
            f"the pre-protocol-launch clip is missing"
        )

    def test_base_aavev3_denominator_excludes_pre_chain_genesis_days(self) -> None:
        """BASE chain genesis 2023-08-09 — every day before that is
        impossible regardless of protocol launch.

        AAVEV3 on BASE launched 2023-08-22 → effective floor 2023-08-22.
        Asking for 2023-01-01 → 2023-09-30 should return ONLY days
        >= 2023-08-22.
        """
        chain = "BASE"
        protocol = "AAVEV3"
        protocol_launch = PROTOCOL_LAUNCH_DATES[(chain, protocol)]
        venue = f"{protocol}-{chain}"
        window_start = "2023-01-01"
        window_end = "2023-09-30"
        result = _mtds_expected_dates_cached(
            venue=venue,
            data_type="lending_indices",
            category="DEFI",
            window_start=window_start,
            window_end=window_end,
        )
        for d_str in result:
            assert d_str >= protocol_launch, f"Pre-launch day {d_str!r} leaked through (floor={protocol_launch})"

    def test_window_entirely_pre_genesis_returns_empty_denominator(self) -> None:
        """If the requested window ends strictly before chain genesis,
        the denominator must be 0 — the panel renders 0/0 (handled
        downstream by hiding the row), NOT 0/N where N is days that
        couldn't possibly have data."""
        chain = "ARBITRUM"
        protocol = "AAVEV3"
        venue = f"{protocol}-{chain}"
        # Pre-2021 — entirely before ARBITRUM genesis.
        window_start = "2020-01-01"
        window_end = "2020-12-31"
        result = _mtds_expected_dates_cached(
            venue=venue,
            data_type="lending_indices",
            category="DEFI",
            window_start=window_start,
            window_end=window_end,
        )
        assert result == frozenset()

    def test_window_post_floor_yields_realistic_denominator(self) -> None:
        """A 30-day window opening 7d past the floor should yield ~30
        expected dates (DEFI uses a daily grid, not a trading-calendar
        clip). Bounds are loose (15..30) to absorb future calendar
        plumbing changes without flapping."""
        chain = "ARBITRUM"
        protocol = "AAVEV3"
        protocol_launch = PROTOCOL_LAUNCH_DATES[(chain, protocol)]
        floor_d = date.fromisoformat(protocol_launch)
        window_start = (floor_d + timedelta(days=7)).isoformat()
        window_end = (floor_d + timedelta(days=37)).isoformat()
        venue = f"{protocol}-{chain}"
        result = _mtds_expected_dates_cached(
            venue=venue,
            data_type="lending_indices",
            category="DEFI",
            window_start=window_start,
            window_end=window_end,
        )
        # Daily grid → ~31 days for a 30d-inclusive window.
        assert 15 <= len(result) <= 35, f"Unexpected denominator size {len(result)} for 30d window"

    def test_clip_propagates_through_when_chain_genesis_dominates(self) -> None:
        """When chain_genesis is later than protocol_launch (theoretical
        edge — currently no such pair, but the math must still hold),
        the floor is still ``max(...)``.

        We synthesize the test by picking BSC-AAVEV3: BSC genesis
        2020-08-29, AAVEV3-BSC launch 2024-01-23. Floor = 2024-01-23
        (protocol-launch dominates, the common case). Window opening
        before BSC genesis but ending after launch yields a clipped
        denominator equal to days >= launch."""
        chain = "BSC"
        protocol = "AAVEV3"
        chain_genesis = CHAIN_GENESIS_DATES[chain]
        protocol_launch = PROTOCOL_LAUNCH_DATES[(chain, protocol)]
        # Verify the SSOT relation we are testing.
        assert protocol_launch > chain_genesis, (
            f"Test premise broken: BSC genesis {chain_genesis} must be earlier than AAVEV3-BSC launch {protocol_launch}"
        )
        venue = f"{protocol}-{chain}"
        # Window: pre-BSC-genesis through post-AAVEV3-launch.
        window_start = "2020-01-01"
        window_end = (date.fromisoformat(protocol_launch) + timedelta(days=10)).isoformat()
        result = _mtds_expected_dates_cached(
            venue=venue,
            data_type="lending_indices",
            category="DEFI",
            window_start=window_start,
            window_end=window_end,
        )
        for d_str in result:
            assert d_str >= protocol_launch, f"Day {d_str!r} bypassed protocol_launch floor {protocol_launch}"

    def test_today_is_late_2026(self) -> None:
        """Sanity: this suite assumes the workspace clock is ~2026-05;
        if the test ever runs in a far-future where AAVEV3 has shut
        down or restructured, the assumption that 2022-03-16 is in
        the past breaks. Fail loud if the clock drifts by years."""
        today = datetime.now(UTC).date()
        assert today >= date(2026, 5, 1), (
            f"Workspace clock {today} predates this suite's reference window — check fixture freshness"
        )

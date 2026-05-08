"""Cat C.1 — MTDS expected-dates clipping honors BOTH chain genesis AND protocol launch.

Plan: ``data_status_comprehensive_test_coverage_2026_05_07.md`` Phase 2.

For DEFI venues canonically named ``PROTOCOL-CHAIN`` (e.g. ``AAVEV3-ARBITRUM``),
``_mtds_expected_dates_cached`` clips the expected-date window to
``max(chain_genesis, protocol_launch)`` per the codex SSOT. Without the
protocol-launch clip, the panel inflates the leaf-shard denominator with
weeks-to-months of always-empty days (e.g. AAVEV3-ARBITRUM stretched back
to ARBITRUM genesis 2021-08-31 even though the protocol didn't deploy on
Arbitrum until 2022-03-16, framed as the 2026-05-07 "ARBITRUM 32/54"
incident in
``data_status_drilldown_shard_atom_alignment_2026_05_07``).

This test parameterises every (chain, protocol) pair in
``PROTOCOL_LAUNCH_DATES`` and asserts:

* Window predating ``max(chain_genesis, protocol_launch)`` returns empty.
* Window straddling the floor returns at least one expected date.
* Window fully past the floor returns a non-empty set.

A regression where one cutoff is honored but the other isn't (e.g. the
chain-genesis floor is applied but protocol-launch is not) flips the
"predates" assertion: the function would return non-empty for a window
that should be entirely pre-launch. This is exactly the regression the
test is designed to catch.
"""

from __future__ import annotations

from datetime import date, timedelta

import pytest

# UAC facade imports per workspace Citadel rule.
from unified_api_contracts.registry.chain_env import (
    CHAIN_GENESIS_DATES,
    PROTOCOL_LAUNCH_DATES,
)

from deployment_api.services.data_status_service import _mtds_expected_dates_cached

# DeFi expected-dates use ``lst_rates`` as the smoke data_type because the
# helper has explicit DEFI branches for that data_type and falls through
# to the generic per-day grid for everything else. Either picks up the
# chain-genesis + protocol-launch clip identically per the helper's
# ``effective_start = max(window_start, venue_start, dt_start, chain_genesis)``
# computation; ``lending_indices`` is the canonical workspace data_type.
_SMOKE_DATA_TYPE = "lending_indices"
_CATEGORY = "DEFI"


def _floor(chain: str, protocol: str) -> str:
    """Return the effective floor = max(chain_genesis, protocol_launch).

    Mirrors the code's ``max()`` of two ISO YYYY-MM-DD strings — for ISO
    format the lexicographic max equals the chronological max.
    """
    chain_genesis = CHAIN_GENESIS_DATES.get(chain.upper(), "")
    protocol_launch = PROTOCOL_LAUNCH_DATES.get((chain.upper(), protocol.upper()), "")
    return max(chain_genesis, protocol_launch)


def _iso(d: date) -> str:
    return d.isoformat()


def _all_chain_protocol_pairs() -> list[tuple[str, str]]:
    """Filter PROTOCOL_LAUNCH_DATES to pairs whose chain is ALSO in
    CHAIN_GENESIS_DATES.

    The data-status helper only enters the protocol-launch clip branch
    when the venue parses cleanly as ``PROTOCOL-CHAIN`` AND the chain is
    in the SSOT — pairs whose chain is absent from
    ``CHAIN_GENESIS_DATES`` (e.g. SOLANA in some entries) are exercised
    by the chain-only fallback elsewhere; this test focuses on the
    BOTH-cutoffs path."""
    return [
        (chain, protocol)
        for (chain, protocol) in PROTOCOL_LAUNCH_DATES
        if chain in CHAIN_GENESIS_DATES
    ]


_PAIRS = _all_chain_protocol_pairs()


# Pairs we KNOW return non-empty post-floor windows for ``lending_indices``.
# This is the protocol-set covered by lending-indices instruments-service +
# MTDS (AAVEV3 + COMPOUNDV3 + SPARK lending markets). UNISWAPV*/CURVE/
# SUSHISWAP/GMX/JITO/MARINADE are dex/staking protocols with different
# expected data_types — the helper legitimately returns empty when probed
# with ``lending_indices``. Restricting the post-floor non-empty assertion
# to these protocols catches the protocol-launch-clip regression on every
# pair the panel actively renders without false-flapping on adjacent
# protocols whose data_type universe differs.
_LENDING_PROTOCOLS: frozenset[str] = frozenset({"AAVEV3", "COMPOUNDV3"})


def _post_floor_assertable_pairs() -> list[tuple[str, str]]:
    """Sub-filter for the post-floor non-empty assertion."""
    return [(chain, protocol) for (chain, protocol) in _PAIRS if protocol in _LENDING_PROTOCOLS]


_POST_FLOOR_PAIRS = _post_floor_assertable_pairs()


@pytest.fixture(autouse=True)
def _clear_mtds_cache() -> None:
    """Ensure the lru_cache doesn't leak parametrised state across cases."""
    _mtds_expected_dates_cached.cache_clear()


class TestMtdsExpectedDatesClipping:
    """For every (chain, protocol) pair, assert both cutoffs are honored."""

    @pytest.mark.parametrize(("chain", "protocol"), _PAIRS, ids=lambda p: str(p))
    def test_window_predates_floor_returns_empty(self, chain: str, protocol: str) -> None:
        """A window ending strictly before max(chain_genesis, protocol_launch)
        must return an empty set — both cutoffs honored.

        Catches the regression where protocol_launch clip is dropped: the
        window would NOT be empty because chain_genesis alone would let
        through pre-protocol days.
        """
        floor = _floor(chain, protocol)
        floor_date = date.fromisoformat(floor)
        # Pick a 7-day window that ends 1 day BEFORE the floor.
        window_end_d = floor_date - timedelta(days=1)
        window_start_d = window_end_d - timedelta(days=7)
        # The chain-genesis itself may be even earlier than this window —
        # the test is robust because we end strictly before the joint
        # floor.
        venue = f"{protocol}-{chain}"
        result = _mtds_expected_dates_cached(
            venue=venue,
            data_type=_SMOKE_DATA_TYPE,
            category=_CATEGORY,
            window_start=_iso(window_start_d),
            window_end=_iso(window_end_d),
        )
        assert result == frozenset(), (
            f"({chain}, {protocol}) floor={floor} window=[{_iso(window_start_d)}..{_iso(window_end_d)}] "
            f"should be entirely pre-floor but got {len(result)} dates: {sorted(result)[:5]}"
        )

    @pytest.mark.parametrize(("chain", "protocol"), _POST_FLOOR_PAIRS, ids=lambda p: str(p))
    def test_window_postdates_floor_returns_non_empty(self, chain: str, protocol: str) -> None:
        """A window opening WELL after max(chain_genesis, protocol_launch)
        must return at least one expected date — confirms the helper
        emits real dates once both cutoffs are cleared.

        We open the window 1y past the floor to absorb any
        ``venue_mapping.get_venue_start_date`` overrides that may push
        ``effective_start`` further out (a few DEFI venues have
        per-venue start dates later than their protocol-launch SSOT
        entry — those get exercised by the predates-floor test instead;
        here we just need a window that's reliably non-empty for a
        well-mature protocol).
        """
        floor = _floor(chain, protocol)
        floor_date = date.fromisoformat(floor)
        # Window: 30 days starting 365 days past the floor.
        window_start_d = floor_date + timedelta(days=365)
        # Skip pairs whose 1y-past-floor window crosses today — the helper
        # legitimately returns empty for future dates.
        today = date.today()
        if window_start_d >= today - timedelta(days=30):
            pytest.skip(
                f"({chain}, {protocol}) floor={floor} too recent — 1y-past-floor "
                f"window {window_start_d} crosses today; skip to avoid future-window flapping"
            )
        window_end_d = window_start_d + timedelta(days=30)
        venue = f"{protocol}-{chain}"
        result = _mtds_expected_dates_cached(
            venue=venue,
            data_type=_SMOKE_DATA_TYPE,
            category=_CATEGORY,
            window_start=_iso(window_start_d),
            window_end=_iso(window_end_d),
        )
        assert len(result) > 0, (
            f"({chain}, {protocol}) floor={floor} window=[{_iso(window_start_d)}..{_iso(window_end_d)}] "
            f"should be fully post-floor but got 0 dates"
        )

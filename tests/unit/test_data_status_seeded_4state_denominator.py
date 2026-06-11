"""Unit tests for the F4 seed-aware MTDS honest-coverage denominator.

``expected_unattempted`` is MATERIALISED by the writer (MTDS pre-flight /
IS ``enumerate_expected_universe`` v2 enumerator) and READ by consumers —
never re-derived per consumer (codex/02-data/
availability-manifest-and-data-status.md F4). These tests pin the
deployment-api consumer contract on ``_mtds_honest_coverage_for_venue``:

* (a) a (venue, data_type) WITH seeded ``expected_unattempted`` rows takes
  the 4-state READ denominator (captured + empty_confirmed +
  attempted_failed + expected_unattempted distinct manifest cells) and the
  genesis/launch re-derivation (``_mtds_expected_dates_for_venue_dt``) is
  NEVER called for it;
* (b) WITHOUT seeded rows the legacy re-derivation path is byte-identical
  (pre-seed reality — today's behaviour pinned);
* (c) mixed venues in one manifest frame: the seeded venue reads the
  4-state, the unseeded venue still re-derives.

Plan: cefi_manifest_canonicalisation_2026_06_01 §⑦(a).
"""

import importlib.util
import os
from unittest.mock import patch

import pandas as pd
import pytest

# Load directly to avoid circular import via services/__init__.py (same
# pattern as tests/unit/test_data_status_service.py).
_path = os.path.join(os.path.dirname(__file__), "../../deployment_api/services/data_status_service.py")
_spec = importlib.util.spec_from_file_location("_dss_seeded_standalone", os.path.abspath(_path))
assert _spec is not None and _spec.loader is not None
_dss_mod = importlib.util.module_from_spec(_spec)
_spec.loader.exec_module(_dss_mod)  # type: ignore[union-attr]

_COLS = ["date", "venue", "data_type", "capture_status", "error_reason", "instrument_id"]


def _manifest_df(rows: list[list[str]]) -> pd.DataFrame:
    """Minimal availability-index frame with the columns the honest-coverage
    rollup consumes (date / venue / data_type / capture_status /
    error_reason / instrument_id)."""
    return pd.DataFrame(rows, columns=_COLS)


def _venue_mapping():
    from unified_api_contracts import VenueMapping

    return VenueMapping()


@pytest.fixture(autouse=True)
def _clear_expected_dates_cache():
    """The derived-path lru_cache is process-shared — clear per test so the
    unseeded assertions observe real derivations, not cache hits."""
    _dss_mod._mtds_expected_dates_cached.cache_clear()
    yield
    _dss_mod._mtds_expected_dates_cached.cache_clear()


class TestSeededFourStateDenominator:
    """(a) Seeded (venue, dt) → 4-state READ, re-derivation never called."""

    def test_seeded_venue_reads_4state_and_skips_rederivation(self):
        """All BINANCE-SPOT declared dts (trades + book_snapshot_5) carry
        seeded rows → ``_mtds_expected_dates_for_venue_dt`` must NOT run.

        trades cells (instrument grain): (BTC-USDT, 04-17) captured,
        (BTC-USDT, 04-18) expected_unattempted pending, (ETH-USDT, 04-17)
        attempted_failed → denominator 3, numerator 1 (captured only;
        pending-fetch + failed are not skip-worthy).
        book_snapshot_5 cells: (BTC-USDT, 04-17) expected_unattempted
        pending → 1 expected / 0 found.
        """
        df = _manifest_df(
            [
                ["2026-04-17", "BINANCE-SPOT", "trades", "captured", "", "BTC-USDT"],
                ["2026-04-18", "BINANCE-SPOT", "trades", "expected_unattempted", "", "BTC-USDT"],
                ["2026-04-17", "BINANCE-SPOT", "trades", "attempted_failed", "HTTP_500", "ETH-USDT"],
                ["2026-04-17", "BINANCE-SPOT", "book_snapshot_5", "expected_unattempted", "", "BTC-USDT"],
            ]
        )
        with patch.object(
            _dss_mod,
            "_mtds_expected_dates_for_venue_dt",
            side_effect=AssertionError("F4 violation: re-derivation called for a seeded (venue, dt)"),
        ):
            honest = _dss_mod._mtds_honest_coverage_for_venue(
                df, "BINANCE-SPOT", "CEFI", "2026-04-17", "2026-04-18", _venue_mapping()
            )

        trades = honest["data_types"]["trades"]
        assert trades["expected_shards"] == 3
        assert trades["found_shards"] == 1
        assert trades["missing_shards"] == 2
        assert trades["completion_pct"] == 33.33
        assert trades["unit"] == "shard_instrument_days"
        assert trades["denominator_source"] == "materialised_expected_unattempted"
        assert sorted(trades["expected_instruments"]) == ["BTC-USDT", "ETH-USDT"]
        assert trades["missing_instruments"] == ["ETH-USDT"]

        snap = honest["data_types"]["book_snapshot_5"]
        assert snap["expected_shards"] == 1
        assert snap["found_shards"] == 0
        assert snap["denominator_source"] == "materialised_expected_unattempted"

        # Venue rollup = sum of the materialised per-dt denominators (never
        # the genesis-derived |instruments| x |dates| universe).
        assert honest["expected_shards"] == 4
        assert honest["found_shards"] == 1
        assert honest["missing_data_types"] == ["book_snapshot_5"]

    def test_seeded_known_empty_expected_rows_count_as_found(self):
        """EXPECTED_*-reasoned expected_unattempted is skip-worthy: it lands
        in BOTH the 4-state denominator and the numerator (existing module
        convention, unchanged by the seed-aware read)."""
        df = _manifest_df(
            [
                ["2026-04-17", "BINANCE-SPOT", "trades", "captured", "", "BTC-USDT"],
                ["2026-04-18", "BINANCE-SPOT", "trades", "expected_unattempted", "EXPECTED_VENUE_HOLIDAY", "BTC-USDT"],
                ["2026-04-17", "BINANCE-SPOT", "book_snapshot_5", "expected_unattempted", "", "BTC-USDT"],
            ]
        )
        honest = _dss_mod._mtds_honest_coverage_for_venue(
            df, "BINANCE-SPOT", "CEFI", "2026-04-17", "2026-04-18", _venue_mapping()
        )
        trades = honest["data_types"]["trades"]
        assert trades["expected_shards"] == 2
        assert trades["found_shards"] == 2
        assert trades["completion_pct"] == 100.0

    def test_seeded_rows_outside_window_are_clipped(self):
        """Seeded cells outside [window_start, window_end] don't inflate the
        denominator — parity with the derived path's window clipping."""
        df = _manifest_df(
            [
                ["2026-04-17", "BINANCE-SPOT", "trades", "expected_unattempted", "", "BTC-USDT"],
                ["2026-05-01", "BINANCE-SPOT", "trades", "expected_unattempted", "", "BTC-USDT"],
                ["2026-04-17", "BINANCE-SPOT", "book_snapshot_5", "expected_unattempted", "", "BTC-USDT"],
            ]
        )
        honest = _dss_mod._mtds_honest_coverage_for_venue(
            df, "BINANCE-SPOT", "CEFI", "2026-04-17", "2026-04-18", _venue_mapping()
        )
        assert honest["data_types"]["trades"]["expected_shards"] == 1

    def test_seeded_date_grain_when_no_instrument_ids(self):
        """Seeded rows with empty instrument_id fall to the date grain
        (``shard_days``) — distinct dates over the 4-state rows."""
        entry = _dss_mod._mtds_seeded_4state_dt_entry(
            _manifest_df(
                [
                    ["2026-04-17", "BINANCE-SPOT", "trades", "captured", "", ""],
                    ["2026-04-18", "BINANCE-SPOT", "trades", "expected_unattempted", "", ""],
                ]
            ),
            _manifest_df([["2026-04-17", "BINANCE-SPOT", "trades", "captured", "", ""]]),
            "trades",
            "2026-04-17",
            "2026-04-18",
        )
        assert entry["unit"] == "shard_days"
        assert entry["expected_shards"] == 2
        assert entry["found_shards"] == 1
        assert entry["missing_dates"] == ["2026-04-18"]
        assert "expected_instruments" not in entry


class TestUnseededLegacyRederivation:
    """(b) No seeded rows → pre-seed behaviour pinned (re-derivation runs)."""

    def test_unseeded_venue_keeps_derived_denominator(self):
        """BINANCE-SPOT with one legacy captured ``trades`` row and zero
        ``expected_unattempted`` rows pins today's derived outputs:
        trades legacy fallback 2 expected / 1 found; book_snapshot_5 MVP
        seed 21 instruments x 2 days = 42 expected / 0 found."""
        df = _manifest_df(
            [
                ["2026-04-17", "BINANCE-SPOT", "trades", "captured", "", ""],
            ]
        )
        with patch.object(
            _dss_mod,
            "_mtds_expected_dates_for_venue_dt",
            wraps=_dss_mod._mtds_expected_dates_for_venue_dt,
        ) as derive_spy:
            honest = _dss_mod._mtds_honest_coverage_for_venue(
                df, "BINANCE-SPOT", "CEFI", "2026-04-17", "2026-04-18", _venue_mapping()
            )

        # Re-derivation ran for every declared dt (the pre-seed path).
        derived_dts = {call.args[2] for call in derive_spy.call_args_list}
        assert derived_dts == {"trades", "book_snapshot_5"}

        trades = honest["data_types"]["trades"]
        assert trades["expected_shards"] == 2
        assert trades["found_shards"] == 1
        assert trades["unit"] == "shard_days_legacy"
        assert "denominator_source" not in trades

        snap = honest["data_types"]["book_snapshot_5"]
        assert snap["expected_shards"] == 42
        assert snap["found_shards"] == 0
        assert "denominator_source" not in snap

        assert honest["expected_shards"] == 44
        assert honest["found_shards"] == 1

    def test_attempted_failed_rows_alone_do_not_trigger_seeded_read(self):
        """The guard keys on ``expected_unattempted`` presence ONLY —
        attempted_failed rows (writer attempted, no seed) keep the derived
        denominator."""
        df = _manifest_df(
            [
                ["2026-04-17", "BINANCE-SPOT", "trades", "attempted_failed", "HTTP_500", ""],
            ]
        )
        honest = _dss_mod._mtds_honest_coverage_for_venue(
            df, "BINANCE-SPOT", "CEFI", "2026-04-17", "2026-04-18", _venue_mapping()
        )
        trades = honest["data_types"]["trades"]
        assert "denominator_source" not in trades
        # Derived Tier-3 denominator (21 MVP SPOT instruments x 2 window
        # days) — the failed row is neither skip-worthy nor a seed, so the
        # per-instrument MVP-seed universe stays authoritative.
        assert trades["expected_shards"] == 42
        assert trades["found_shards"] == 0


class TestMixedVenues:
    """(c) Seeded + unseeded venues in ONE manifest frame."""

    def test_seeded_and_unseeded_venues_dispatch_independently(self):
        df = _manifest_df(
            [
                # BINANCE-SPOT: fully seeded (both declared dts).
                ["2026-04-17", "BINANCE-SPOT", "trades", "captured", "", "BTC-USDT"],
                ["2026-04-18", "BINANCE-SPOT", "trades", "expected_unattempted", "", "BTC-USDT"],
                ["2026-04-17", "BINANCE-SPOT", "book_snapshot_5", "expected_unattempted", "", "BTC-USDT"],
                # OKX-SPOT: unseeded (captured row only).
                ["2026-04-17", "OKX-SPOT", "trades", "captured", "", ""],
            ]
        )
        vm = _venue_mapping()

        with patch.object(
            _dss_mod,
            "_mtds_expected_dates_for_venue_dt",
            wraps=_dss_mod._mtds_expected_dates_for_venue_dt,
        ) as derive_spy:
            seeded = _dss_mod._mtds_honest_coverage_for_venue(
                df, "BINANCE-SPOT", "CEFI", "2026-04-17", "2026-04-18", vm
            )
            unseeded = _dss_mod._mtds_honest_coverage_for_venue(df, "OKX-SPOT", "CEFI", "2026-04-17", "2026-04-18", vm)

        # The seeded venue never re-derived; the unseeded venue did.
        derived_venues = {call.args[1] for call in derive_spy.call_args_list}
        assert "BINANCE-SPOT" not in derived_venues
        assert "OKX-SPOT" in derived_venues

        # Seeded venue: 4-state denominators from the manifest cells.
        assert seeded["data_types"]["trades"]["denominator_source"] == "materialised_expected_unattempted"
        assert seeded["data_types"]["trades"]["expected_shards"] == 2
        assert seeded["data_types"]["book_snapshot_5"]["expected_shards"] == 1
        assert seeded["expected_shards"] == 3
        assert seeded["found_shards"] == 1

        # Unseeded venue: derived denominators, no provenance marker.
        for dt_entry in unseeded["data_types"].values():
            assert "denominator_source" not in dt_entry
        assert unseeded["data_types"]["trades"]["found_shards"] == 1

    def test_seeding_scoped_per_venue_not_per_frame(self):
        """A seeded dt on venue A must not flip venue B's same dt to the
        4-state read — the guard is per-(venue, dt), evaluated on the
        venue-sliced frame."""
        df = _manifest_df(
            [
                ["2026-04-17", "BINANCE-SPOT", "trades", "expected_unattempted", "", "BTC-USDT"],
                ["2026-04-17", "OKX-SPOT", "trades", "captured", "", ""],
            ]
        )
        unseeded = _dss_mod._mtds_honest_coverage_for_venue(
            df, "OKX-SPOT", "CEFI", "2026-04-17", "2026-04-18", _venue_mapping()
        )
        assert "denominator_source" not in unseeded["data_types"]["trades"]
        # Derived legacy denominator (2 window days), unchanged.
        assert unseeded["data_types"]["trades"]["expected_shards"] == 2

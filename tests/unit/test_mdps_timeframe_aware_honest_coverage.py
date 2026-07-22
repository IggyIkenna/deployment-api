"""MDPS timeframe-aware honest-coverage extension (mtds_data_status_page_parity_2026_07_21).

Covers the 3 converged/individual adversarial-review fixes on top of the
MTDS -> MDPS honest-coverage extension:

  1. CRITICAL (all 3 reviews): ``service`` threaded through
     ``_apply_mtds_honest_coverage`` -> ``mtds_honest_coverage_for_venue`` ->
     ``get_expected_data_types_for_venue(venue, service=service)`` so MDPS's
     expected-dt list narrows to ``MDPS_DERIVABLE_DATA_TYPES`` instead of the
     full MTDS raw vocabulary.
  2. Real bug (2/3 reviews): the Tier-3 timeframe mask ("tf_str") must be
     built from the SAME ``non_legacy_mask``-filtered slice as
     ``iid_str``/``rd_str`` -- not from unfiltered ``dt_rows`` -- to avoid a
     pandas index-misalignment when legacy + non-legacy rows coexist in the
     same (venue, dt) slice.
  3. Real gap (1/3 reviews + the design's own Open Question 1): the
     legacy-row-fallback branch's ``expected_shards`` must scale by
     ``len(timeframes)`` too, or it silently under-counts by that factor
     relative to the Tier-3 branch.

Also verifies every existing MTDS call path (``timeframes=None`` / no
``service=`` kwarg) is BYTE-FOR-BYTE unchanged.

``TestTier2VenueLevelTimeframeAwareness`` below covers the 2026-07-22
follow-up: the Tier-2 (venue-level, non-per-instrument) branch
(``_tier2_dt_entry`` in ``deployment_api/services/data_status/mtds.py``),
deliberately deferred from the original 2026-07-21 ship. MDPS's one current
venue-level derivable dt is ``liquidations`` (confirmed NOT in UAC's
``_PER_INSTRUMENT_SHARD_DATA_TYPES``, so it always dispatches to Tier-2,
never Tier-3).
"""

from __future__ import annotations

import pandas as pd

import deployment_api.services.data_status_service as _dss_mod
from deployment_api.services.data_status.instrument_coverage import per_instrument_coverage

_MTDS_COLS = [
    "date",
    "venue",
    "data_type",
    "service_name",
    "capture_status",
    "chain",
    "instrument_type",
    "league_id",
    "instrument_id",
    "row_count",
]

_MDPS_COLS = [*_MTDS_COLS, "timeframe"]


def _mtds_df(rows: list[list[object]]) -> pd.DataFrame:
    return pd.DataFrame(rows, columns=_MTDS_COLS)


def _mdps_df(rows: list[list[object]]) -> pd.DataFrame:
    return pd.DataFrame(rows, columns=_MDPS_COLS)


class TestServiceThreadingNarrowsExpectedDataTypes:
    """CRITICAL fix (all 3 reviews converged): service must reach
    get_expected_data_types_for_venue so MDPS narrows to MDPS_DERIVABLE_DATA_TYPES."""

    def test_mdps_narrows_deribit_expected_data_types(self) -> None:
        """DERIBIT declares 5 raw dts (incl. options_chain/futures_chain, which
        have no MDPS candle form). service="market-data-processing-service"
        must narrow ``expected_data_types`` to the 3 MDPS-derivable ones."""
        from unified_api_contracts import VenueMapping

        df = _mdps_df([])
        honest = _dss_mod._mtds_honest_coverage_for_venue(
            df,
            "DERIBIT",
            "CEFI",
            "2026-07-01",
            "2026-07-01",
            VenueMapping(),
            service="market-data-processing-service",
        )
        expected_dts = set(honest["expected_data_types"])
        assert expected_dts == {"trades", "book_snapshot_5", "derivative_ticker"}
        assert "options_chain" not in expected_dts
        assert "futures_chain" not in expected_dts
        # MDPS-only annotation must be present.
        assert honest["historical_coverage_gap"] is True

    def test_mtds_default_service_keeps_full_deribit_vocabulary(self) -> None:
        """Without service=, DERIBIT keeps the FULL 5-dt raw vocabulary --
        proves the narrowing is service-gated, not always-on."""
        from unified_api_contracts import VenueMapping

        df = _mdps_df([])
        honest = _dss_mod._mtds_honest_coverage_for_venue(
            df, "DERIBIT", "CEFI", "2026-07-01", "2026-07-01", VenueMapping()
        )
        expected_dts = set(honest["expected_data_types"])
        assert expected_dts == {
            "trades",
            "book_snapshot_5",
            "derivative_ticker",
            "options_chain",
            "futures_chain",
        }
        assert "historical_coverage_gap" not in honest

    def test_mtds_explicit_service_kwarg_also_unaffected(self) -> None:
        """service="market-tick-data-service" (the REAL production value MTDS
        calls now pass) is likewise unnarrowed and carries no MDPS annotation."""
        from unified_api_contracts import VenueMapping

        df = _mdps_df([])
        honest = _dss_mod._mtds_honest_coverage_for_venue(
            df,
            "DERIBIT",
            "CEFI",
            "2026-07-01",
            "2026-07-01",
            VenueMapping(),
            service="market-tick-data-service",
        )
        expected_dts = set(honest["expected_data_types"])
        assert "options_chain" in expected_dts
        assert "futures_chain" in expected_dts
        assert "historical_coverage_gap" not in honest


class TestMdpsTimeframeAwareExpectedAndFoundCounts:
    """MDPS with matching data -- expected/found counts correct at the
    (instrument, date, timeframe) grain."""

    def test_expected_shards_is_instruments_x_dates_x_timeframes(self) -> None:
        """2 instruments found on 1 date across 3 (of 7 canonical) timeframes.
        expected_shards must be |MVP perp seed instruments| x 1 date x 7
        canonical timeframes; found_shards = 2 instruments x 1 date x 3 tf."""
        from unified_api_contracts import VenueMapping

        rows = []
        for iid in ("BTC-PERP", "ETH-PERP"):
            for tf in ("1m", "5m", "1h"):
                rows.append(
                    [
                        "2026-07-01",
                        "BINANCE-FUTURES",
                        "derivative_ticker",
                        "market-data-processing-service",
                        "captured",
                        "",
                        "PERPETUAL",
                        "",
                        iid,
                        100,
                        tf,
                    ]
                )
        df = _mdps_df(rows)
        honest = _dss_mod._mtds_honest_coverage_for_venue(
            df,
            "BINANCE-FUTURES",
            "CEFI",
            "2026-07-01",
            "2026-07-01",
            VenueMapping(),
            service="market-data-processing-service",
        )
        data_types = honest["data_types"]
        assert isinstance(data_types, dict)
        dt_entry = data_types["derivative_ticker"]
        assert isinstance(dt_entry, dict)
        # MVP PERP seed = 10 instruments x 1 date x 7 canonical timeframes.
        assert dt_entry["expected_shards"] == 10 * 7
        # 2 instruments x 1 date x 3 timeframes actually captured.
        assert dt_entry["found_shards"] == 2 * 3
        assert dt_entry["unit"] == "shard_instrument_days"

    def test_timeframes_outside_canonical_set_are_not_counted(self) -> None:
        """A row carrying a non-canonical/garbage timeframe token must not
        inflate found_shards (mirrors the 'no KeyError, honestly 0%' contract
        for a manifest with an unexpected/blank timeframe value)."""
        from unified_api_contracts import VenueMapping

        df = _mdps_df(
            [
                [
                    "2026-07-01",
                    "BINANCE-FUTURES",
                    "derivative_ticker",
                    "market-data-processing-service",
                    "captured",
                    "",
                    "PERPETUAL",
                    "",
                    "BTC-PERP",
                    100,
                    "3w",  # not a canonical MDPS timeframe
                ],
            ]
        )
        honest = _dss_mod._mtds_honest_coverage_for_venue(
            df,
            "BINANCE-FUTURES",
            "CEFI",
            "2026-07-01",
            "2026-07-01",
            VenueMapping(),
            service="market-data-processing-service",
        )
        dt_entry = honest["data_types"]["derivative_ticker"]  # type: ignore[index]
        assert dt_entry["found_shards"] == 0


class TestTfStrMaskingIndexAlignment:
    """Review finding #2 (real bug, 2/3 reviews): tf_str must be built from
    the SAME non_legacy_mask-filtered slice as iid_str/rd_str, not from
    unfiltered dt_rows -- otherwise a legacy row coexisting with non-legacy
    rows in the same (venue, dt) slice misaligns timeframe values."""

    def test_mixed_legacy_and_nonlegacy_rows_attribute_timeframe_correctly(self) -> None:
        from unified_api_contracts import VenueMapping

        # Row 0: legacy (empty instrument_id) -- should NOT contaminate the
        # Tier-3 per-instrument/timeframe accounting below.
        # Row 1: BTC-PERP / 2026-07-01 / tf=1m -- SHOULD be counted.
        # Row 2: BTC-PERP / 2026-07-01 / tf=5m -- SHOULD be counted.
        rows = [
            [
                "2026-07-01",
                "BINANCE-FUTURES",
                "derivative_ticker",
                "market-data-processing-service",
                "captured",
                "",
                "PERPETUAL",
                "",
                "",  # legacy: empty instrument_id
                10,
                "1h",
            ],
            [
                "2026-07-01",
                "BINANCE-FUTURES",
                "derivative_ticker",
                "market-data-processing-service",
                "captured",
                "",
                "PERPETUAL",
                "",
                "BTC-PERP",
                100,
                "1m",
            ],
            [
                "2026-07-01",
                "BINANCE-FUTURES",
                "derivative_ticker",
                "market-data-processing-service",
                "captured",
                "",
                "PERPETUAL",
                "",
                "BTC-PERP",
                50,
                "5m",
            ],
        ]
        venue_df_ok = _mdps_df(rows)
        result = per_instrument_coverage(
            venue_df_ok=venue_df_ok,
            venue="BINANCE-FUTURES",
            dt="derivative_ticker",
            expected_dates={"2026-07-01"},
            cap=None,
            timeframes=["1m", "5m", "1h", "4h", "1d", "15m", "15s"],
        )
        # 1 legacy row coexists with 2 non-legacy rows -- Tier-3 stays
        # authoritative (per the existing "some Phase-8C + some legacy
        # coexist" convention) and annotates legacy_row_count, it does NOT
        # take the fully-legacy fallback branch.
        assert result["legacy_row_count"] == 1
        assert result["unit"] == "shard_instrument_days"
        per_instrument = result.get("per_instrument")
        assert isinstance(per_instrument, dict)
        btc_entry = per_instrument["BTC-PERP"]
        # Exactly 2 found shards for BTC-PERP (1m + 5m) -- NOT 3 (would
        # indicate the legacy row's timeframe leaked in) and NOT 0/1 (would
        # indicate a misaligned tf_str dropped a real match).
        assert btc_entry["found"] == 2


class TestLegacyRowFallbackTimeframeMultiplier:
    """Review finding #3 (real gap): the fully-legacy fallback branch must
    scale expected_shards by len(timeframes) and flag
    denominator_timeframe_aware=False (the chosen resolution: BOTH thread
    timeframes into the denominator AND mark provenance, per the review's
    'pick whichever is more correct' instruction)."""

    def test_legacy_fallback_multiplies_expected_by_timeframe_count(self) -> None:
        df = _mdps_df(
            [
                [
                    "2026-07-01",
                    "BINANCE-SPOT",
                    "trades",
                    "market-data-processing-service",
                    "captured",
                    "",
                    "SPOT_PAIR",
                    "",
                    "",  # empty instrument_id -> fully legacy slice
                    1000,
                    "1m",
                ],
            ]
        )
        timeframes = ["15s", "1m", "5m", "15m", "1h", "4h", "1d"]
        result = per_instrument_coverage(
            venue_df_ok=df,
            venue="BINANCE-SPOT",
            dt="trades",
            expected_dates={"2026-07-01"},
            cap=None,
            timeframes=timeframes,
        )
        assert result["unit"] == "shard_days_legacy"
        # 1 expected date x 7 timeframes = 7 (NOT 1, the un-fixed bug).
        assert result["expected_shards"] == 1 * len(timeframes)
        assert result["legacy_row_count"] == 1
        assert result["denominator_timeframe_aware"] is False

    def test_legacy_fallback_without_timeframes_is_unaffected(self) -> None:
        """timeframes=None (every existing MTDS caller) -- byte-for-byte the
        pre-existing behaviour, no new key."""
        df = _mtds_df(
            [
                [
                    "2026-07-01",
                    "BINANCE-SPOT",
                    "trades",
                    "market-tick-data-service",
                    "captured",
                    "",
                    "SPOT_PAIR",
                    "",
                    "",
                    1000,
                ],
            ]
        )
        result = per_instrument_coverage(
            venue_df_ok=df,
            venue="BINANCE-SPOT",
            dt="trades",
            expected_dates={"2026-07-01"},
            cap=None,
        )
        assert result["expected_shards"] == 1
        assert result["unit"] == "shard_days_legacy"
        assert "denominator_timeframe_aware" not in result


class TestTier2VenueLevelTimeframeAwareness:
    """2026-07-22 follow-up: the Tier-2 (venue-level, non-per-instrument)
    branch (``_tier2_dt_entry``) is now ALSO timeframe-aware for MDPS,
    mirroring the Tier-3 ``per_instrument_coverage`` pattern. MDPS's one
    current venue-level derivable dt is ``liquidations`` -- confirmed NOT in
    UAC's ``_PER_INSTRUMENT_SHARD_DATA_TYPES``, so ``BINANCE-FUTURES``
    ``liquidations`` always dispatches here, never to the Tier-3 branch.
    """

    def test_expected_shards_is_dates_x_canonical_timeframes(self) -> None:
        """1 date x 7 canonical MDPS timeframes; 3 of the 7 actually
        captured -- found_shards must equal 3, not 1 (the pre-fix bug's
        under-multiplied denominator would also silently mismatch a
        found_shards computed from raw dates)."""
        from unified_api_contracts import VenueMapping

        rows = []
        for tf in ("1m", "5m", "1h"):
            rows.append(
                [
                    "2026-07-01",
                    "BINANCE-FUTURES",
                    "liquidations",
                    "market-data-processing-service",
                    "captured",
                    "",
                    "PERPETUAL",
                    "",
                    "",
                    100,
                    tf,
                ]
            )
        df = _mdps_df(rows)
        honest = _dss_mod._mtds_honest_coverage_for_venue(
            df,
            "BINANCE-FUTURES",
            "CEFI",
            "2026-07-01",
            "2026-07-01",
            VenueMapping(),
            service="market-data-processing-service",
        )
        data_types = honest["data_types"]
        assert isinstance(data_types, dict)
        dt_entry = data_types["liquidations"]
        assert isinstance(dt_entry, dict)
        # 1 expected date x 7 canonical timeframes.
        assert dt_entry["expected_shards"] == 1 * 7
        # 3 (date, timeframe) pairs actually captured.
        assert dt_entry["found_shards"] == 3
        assert dt_entry["missing_shards"] == 7 - 3
        assert dt_entry["unit"] == "shard_days"

    def test_timeframes_outside_canonical_set_are_not_counted(self) -> None:
        """A row carrying a non-canonical/garbage timeframe token must not
        inflate found_shards -- mirrors the Tier-3 'no KeyError, honestly 0%'
        contract."""
        from unified_api_contracts import VenueMapping

        df = _mdps_df(
            [
                [
                    "2026-07-01",
                    "BINANCE-FUTURES",
                    "liquidations",
                    "market-data-processing-service",
                    "captured",
                    "",
                    "PERPETUAL",
                    "",
                    "",
                    100,
                    "3w",  # not a canonical MDPS timeframe
                ],
            ]
        )
        honest = _dss_mod._mtds_honest_coverage_for_venue(
            df,
            "BINANCE-FUTURES",
            "CEFI",
            "2026-07-01",
            "2026-07-01",
            VenueMapping(),
            service="market-data-processing-service",
        )
        dt_entry = honest["data_types"]["liquidations"]  # type: ignore[index]
        assert dt_entry["found_shards"] == 0
        assert dt_entry["expected_shards"] == 7

    def test_no_timeframe_column_degrades_to_zero_found_not_keyerror(self) -> None:
        """A manifest slice with no ``timeframe`` column at all (an MTDS-shape
        row that somehow lands under an MDPS-scoped query) degrades to zero
        found pairs -- honestly 0%, never a KeyError -- matching the Tier-3
        contract exactly. expected_shards still multiplies by the canonical
        timeframe count (the denominator reflects what SHOULD exist,
        independent of what the manifest slice happens to carry)."""
        from unified_api_contracts import VenueMapping

        df = _mtds_df(
            [
                [
                    "2026-07-01",
                    "BINANCE-FUTURES",
                    "liquidations",
                    "market-data-processing-service",
                    "captured",
                    "",
                    "PERPETUAL",
                    "",
                    "",
                    100,
                ],
            ]
        )
        honest = _dss_mod._mtds_honest_coverage_for_venue(
            df,
            "BINANCE-FUTURES",
            "CEFI",
            "2026-07-01",
            "2026-07-01",
            VenueMapping(),
            service="market-data-processing-service",
        )
        dt_entry = honest["data_types"]["liquidations"]  # type: ignore[index]
        assert dt_entry["expected_shards"] == 7
        assert dt_entry["found_shards"] == 0

    def test_mtds_default_service_byte_for_byte_unchanged(self) -> None:
        """Without service= (the real MTDS production call path), the Tier-2
        denominator/numerator stay per-(venue, dt, date) exactly as before
        this feature existed -- no timeframe multiplier, dates counted
        directly regardless of any ``timeframe`` column."""
        from unified_api_contracts import VenueMapping

        df = _mtds_df(
            [
                [
                    "2026-07-01",
                    "BINANCE-FUTURES",
                    "liquidations",
                    "market-tick-data-service",
                    "captured",
                    "",
                    "PERPETUAL",
                    "",
                    "",
                    100,
                ],
            ]
        )
        honest = _dss_mod._mtds_honest_coverage_for_venue(
            df,
            "BINANCE-FUTURES",
            "CEFI",
            "2026-07-01",
            "2026-07-01",
            VenueMapping(),
        )
        dt_entry = honest["data_types"]["liquidations"]  # type: ignore[index]
        # 1 expected date, NOT multiplied by any timeframe count.
        assert dt_entry["expected_shards"] == 1
        assert dt_entry["found_shards"] == 1
        assert dt_entry["completion_pct"] == 100.0
        assert "historical_coverage_gap" not in honest

    def test_mtds_explicit_service_kwarg_also_byte_for_byte_unchanged(self) -> None:
        """service="market-tick-data-service" (the real production value MTDS
        calls now pass) is likewise unmultiplied."""
        from unified_api_contracts import VenueMapping

        df = _mdps_df(
            [
                [
                    "2026-07-01",
                    "BINANCE-FUTURES",
                    "liquidations",
                    "market-tick-data-service",
                    "captured",
                    "",
                    "PERPETUAL",
                    "",
                    "",
                    100,
                    "1m",
                ],
            ]
        )
        honest = _dss_mod._mtds_honest_coverage_for_venue(
            df,
            "BINANCE-FUTURES",
            "CEFI",
            "2026-07-01",
            "2026-07-01",
            VenueMapping(),
            service="market-tick-data-service",
        )
        dt_entry = honest["data_types"]["liquidations"]  # type: ignore[index]
        # 1 expected date, NOT x7 -- and found is the raw per-date union
        # (the row's timeframe value is irrelevant on this path).
        assert dt_entry["expected_shards"] == 1
        assert dt_entry["found_shards"] == 1

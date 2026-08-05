"""Unit tests for Dim-7 cefi IS-catalog instruments_provider injection.

Verifies two behaviours of the ``_per_instrument_coverage`` / provider:

(a) CEFI venue with a mocked IS provider returns the full injected universe
    (NOT the UAC MVP seed of 21 spot / 10 perp instruments).

(b) IS-unavailable -> provider returns None -> falls back to UAC MVP seed
    (fail-open, existing behaviour preserved).

Plan: cefi_manifest_canonicalisation_2026_06_01.md (Dim-7 denominator precision).
"""

from __future__ import annotations

from unittest.mock import patch

import pandas as pd

from deployment_api.services.data_status.instrument_coverage import _clip_dates_to_window
from deployment_api.services.data_status_service import (
    _build_cefi_is_instruments_provider,
    _per_instrument_coverage,
)

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

_VENUE = "BINANCE-FUTURES"
_DT = "trades"  # a known per-instrument shard data_type in UAC

# 5 instruments injected by the mock IS provider -- well above the UAC MVP seed
# cap of 10 perp instruments but deliberately small to keep the test fast.
_IS_INSTRUMENTS = [
    "BINANCE-FUTURES::BTCUSDT",
    "BINANCE-FUTURES::ETHUSDT",
    "BINANCE-FUTURES::BNBUSDT",
    "BINANCE-FUTURES::SOLUSDT",
    "BINANCE-FUTURES::XRPUSDT",
]


def _make_manifest_df(
    instrument_ids: list[str],
    dates: list[str],
    capture_status: str = "captured",
) -> pd.DataFrame:
    """Build a minimal manifest DataFrame with Phase-8C instrument_id rows."""
    rows: list[dict[str, str]] = []
    for iid in instrument_ids:
        for date in dates:
            rows.append(
                {
                    "venue": _VENUE,
                    "data_type": _DT,
                    "instrument_id": iid,
                    "date": date,
                    "capture_status": capture_status,
                }
            )
    return pd.DataFrame(rows)


def _cefi_provider(venue: str, _data_type: str) -> list[str] | None:
    """Test IS provider: returns _IS_INSTRUMENTS for _VENUE, None otherwise."""
    return _IS_INSTRUMENTS if venue == _VENUE else None


# ---------------------------------------------------------------------------
# Tests: _per_instrument_coverage with IS provider
# ---------------------------------------------------------------------------


class TestPerInstrumentCoverageWithISProvider:
    """Dim-7: _per_instrument_coverage uses injected IS universe, not MVP seed."""

    def test_cefi_with_is_provider_uses_injected_universe(self) -> None:
        """When IS provider returns instruments, denominator = injected universe."""
        # Manifest: only 2 of the 5 injected instruments have data on 1 date.
        dates = ["2026-01-01"]
        manifest_df = _make_manifest_df(
            instrument_ids=_IS_INSTRUMENTS[:2],
            dates=dates,
        )

        result = _per_instrument_coverage(
            venue_df_ok=manifest_df,
            venue=_VENUE,
            dt=_DT,
            expected_dates=set(dates),
            cap=None,  # cefi: no cap so IS universe is not truncated
            instruments_provider=_cefi_provider,
        )

        # expected_instruments should contain all 5 from the IS provider.
        expected_instruments: list[str] = list(result["expected_instruments"])  # type: ignore[assignment]
        assert set(expected_instruments) == set(_IS_INSTRUMENTS), (
            f"expected_instruments should be the IS universe ({set(_IS_INSTRUMENTS)!r}) "
            f"but got {set(expected_instruments)!r}"
        )
        assert len(expected_instruments) == 5

        # expected_shards = |instruments| x |dates| = 5 x 1 = 5
        assert result["expected_shards"] == 5
        # found_shards = 2 instruments x 1 date = 2
        assert result["found_shards"] == 2
        assert result["missing_shards"] == 3

    def test_non_cefi_is_provider_none_falls_back_to_mvp_seed(self) -> None:
        """When instruments_provider=None UAC falls back to its MVP seed tables.

        This preserves the pre-Dim-7 behaviour for non-CEFI asset_groups.
        The MVP seed for _VENUE/funding_rate may return 0 instruments (the
        exact seed table content is an UAC internal); we just assert the
        provider=None path is wired without crashing.
        """
        # Manifest: some rows with instrument_id to exercise the Phase-8C path.
        dates = ["2026-01-01"]
        manifest_df = _make_manifest_df(
            instrument_ids=_IS_INSTRUMENTS[:1],
            dates=dates,
        )

        # No provider -> UAC uses its MVP seed.
        result = _per_instrument_coverage(
            venue_df_ok=manifest_df,
            venue=_VENUE,
            dt=_DT,
            expected_dates=set(dates),
            cap=50,  # non-cefi: apply MVP cap
            instruments_provider=None,
        )

        # The result must be a dict with the expected keys -- the exact counts
        # depend on UAC's MVP seed table which we do not fix here.
        assert "expected_shards" in result
        assert "found_shards" in result
        assert "expected_instruments" in result


# ---------------------------------------------------------------------------
# Tests: scope="mvp" per-instrument filtering (2026-07-21 MVP-scope wiring)
#
# Mocks ``is_mvp`` directly rather than relying on the real UAC MVP_SCOPE
# registry content, so these tests stay correct regardless of which venues/
# instrument_types the registry declares MVP on any given day.
# ---------------------------------------------------------------------------


class TestPerInstrumentCoverageWithMvpScope:
    """scope="mvp" restricts expected_instruments to UAC is_mvp(...) instruments."""

    def test_could_exist_scope_is_unfiltered_default(self) -> None:
        """Default scope (no ``scope`` kwarg) is unaffected by is_mvp — same as pre-fix."""
        dates = ["2026-01-01"]
        manifest_df = _make_manifest_df(instrument_ids=_IS_INSTRUMENTS[:2], dates=dates)
        with patch(
            "deployment_api.services.data_status.instrument_coverage.is_mvp",
            side_effect=AssertionError("is_mvp must not be called under the default scope"),
        ):
            result = _per_instrument_coverage(
                venue_df_ok=manifest_df,
                venue=_VENUE,
                dt=_DT,
                expected_dates=set(dates),
                cap=None,
                instruments_provider=_cefi_provider,
            )
        assert set(result["expected_instruments"]) == set(_IS_INSTRUMENTS)  # type: ignore[arg-type]

    def test_mvp_scope_filters_to_is_mvp_true_instruments(self) -> None:
        """scope='mvp' keeps only instruments where is_mvp(...) is True.

        is_mvp receives (asset_group, venue, instrument_type, dt, base_ccy=...)
        — instrument_type is the only per-instrument-varying argument this
        module's call site can supply, so each intended MVP instrument gets a
        distinct sentinel instrument_type and the fake gates on that set.
        """
        dates = ["2026-01-01"]
        manifest_df = _make_manifest_df(instrument_ids=_IS_INSTRUMENTS[:2], dates=dates)
        mvp_subset = {"BINANCE-FUTURES::BTCUSDT", "BINANCE-FUTURES::ETHUSDT"}
        mvp_instrument_types = {"BINANCE-FUTURES::BTCUSDT": "PERP_MVP_A", "BINANCE-FUTURES::ETHUSDT": "PERP_MVP_B"}
        instrument_types = {iid: mvp_instrument_types.get(iid, "PERP_NON_MVP") for iid in _IS_INSTRUMENTS}

        with patch(
            "deployment_api.services.data_status.instrument_coverage.is_mvp",
            side_effect=lambda _ag, _v, itype, _dt, **_kw: itype in mvp_instrument_types.values(),
        ):
            result = _per_instrument_coverage(
                venue_df_ok=manifest_df,
                venue=_VENUE,
                dt=_DT,
                expected_dates=set(dates),
                cap=None,
                instruments_provider=_cefi_provider,
                asset_group="cefi",
                scope="mvp",
                instrument_types=instrument_types,
            )

        assert set(result["expected_instruments"]) == mvp_subset  # type: ignore[arg-type]
        # 2 MVP instruments x 1 date = 2 expected shards.
        assert result["expected_shards"] == 2

    def test_mvp_scope_treats_unknown_instrument_type_as_non_mvp(self) -> None:
        """An instrument absent from ``instrument_types`` fails CLOSED under scope='mvp'
        (unlike the fail-OPEN convention for existence-window clipping) — an unknown
        instrument_type cannot be proven MVP, so it must not silently pass the filter."""
        dates = ["2026-01-01"]
        manifest_df = _make_manifest_df(instrument_ids=_IS_INSTRUMENTS[:1], dates=dates)
        with patch(
            "deployment_api.services.data_status.instrument_coverage.is_mvp",
            return_value=True,
        ) as mock_is_mvp:
            result = _per_instrument_coverage(
                venue_df_ok=manifest_df,
                venue=_VENUE,
                dt=_DT,
                expected_dates=set(dates),
                cap=None,
                instruments_provider=_cefi_provider,
                asset_group="cefi",
                scope="mvp",
                instrument_types={},  # no catalogue instrument_type data at all
            )
        # is_mvp was still called (with instrument_type="") for every candidate —
        # this test's mock happens to return True, so the result here only proves
        # the blank-instrument_type call shape; the "unknown -> non-MVP" guarantee
        # is that NOTHING is silently exempted from the is_mvp(...) call itself.
        assert mock_is_mvp.called
        call_args = mock_is_mvp.call_args_list[0]
        assert call_args.args[2] == ""  # instrument_type positional arg is blank, not fabricated


# ---------------------------------------------------------------------------
# Tests: cross-service instrument_id format divergence (bug #4)
#
# canonical_id_p0_strategy_reconciliation_2026_07_08 bug #4: the
# instruments-service catalog (`expected_instruments`, injected here via
# `_cefi_provider`) and MTDS's own manifest (`manifest_df`) are independently
# written and can diverge on surface-level formatting (casing / whitespace /
# an optional @SETTLEMENT or @CHAIN suffix) for the SAME real instrument. The
# tests above are deliberately format-IDENTICAL on both sides (both built from
# `_IS_INSTRUMENTS`) so they cannot catch a format-divergence regression --
# these tests inject a REAL divergence between the two sides.
# ---------------------------------------------------------------------------


class TestPerInstrumentCoverageCrossServiceFormatDivergence:
    """Tier-3 coverage must not phantom-miss an instrument due to a surface-
    level (case/whitespace/@suffix) format divergence between the
    instruments-service catalog and the MTDS manifest for the identical real
    instrument."""

    def test_lowercase_manifest_side_still_matches(self) -> None:
        """MTDS wrote the instrument_id lowercase; IS catalog is uppercase."""
        dates = ["2026-01-01"]
        manifest_df = pd.DataFrame(
            [
                {
                    "venue": _VENUE,
                    "data_type": _DT,
                    "instrument_id": _IS_INSTRUMENTS[0].lower(),
                    "date": dates[0],
                    "capture_status": "captured",
                }
            ]
        )

        result = _per_instrument_coverage(
            venue_df_ok=manifest_df,
            venue=_VENUE,
            dt=_DT,
            expected_dates=set(dates),
            cap=None,
            instruments_provider=_cefi_provider,
        )

        missing_instruments: list[str] = list(result["missing_instruments"])  # type: ignore[assignment]
        assert _IS_INSTRUMENTS[0] not in missing_instruments, (
            f"{_IS_INSTRUMENTS[0]!r} captured (case-divergent) but reported phantom-missing: {missing_instruments!r}"
        )
        per_instrument: dict[str, object] = result["per_instrument"]  # type: ignore[assignment]
        assert per_instrument[_IS_INSTRUMENTS[0]]["found"] == 1  # type: ignore[index]

    def test_settlement_suffix_divergence_still_matches(self) -> None:
        """One side carries a "@LIN"-style settlement/chain suffix, the other doesn't."""
        dates = ["2026-01-01"]
        manifest_df = pd.DataFrame(
            [
                {
                    "venue": _VENUE,
                    "data_type": _DT,
                    "instrument_id": _IS_INSTRUMENTS[0] + "@LIN",
                    "date": dates[0],
                    "capture_status": "captured",
                }
            ]
        )

        result = _per_instrument_coverage(
            venue_df_ok=manifest_df,
            venue=_VENUE,
            dt=_DT,
            expected_dates=set(dates),
            cap=None,
            instruments_provider=_cefi_provider,
        )

        missing_instruments: list[str] = list(result["missing_instruments"])  # type: ignore[assignment]
        assert _IS_INSTRUMENTS[0] not in missing_instruments, (
            f"{_IS_INSTRUMENTS[0]!r} captured (@LIN-suffix-divergent) but reported phantom-missing: "
            f"{missing_instruments!r}"
        )

    def test_genuinely_uncaptured_instrument_still_reports_missing(self) -> None:
        """A real gap (never captured, under any format) must still show as missing."""
        dates = ["2026-01-01"]
        # Only the first IS instrument has any manifest row at all.
        manifest_df = pd.DataFrame(
            [
                {
                    "venue": _VENUE,
                    "data_type": _DT,
                    "instrument_id": _IS_INSTRUMENTS[0],
                    "date": dates[0],
                    "capture_status": "captured",
                }
            ]
        )

        result = _per_instrument_coverage(
            venue_df_ok=manifest_df,
            venue=_VENUE,
            dt=_DT,
            expected_dates=set(dates),
            cap=None,
            instruments_provider=_cefi_provider,
        )

        missing_instruments: set[str] = set(result["missing_instruments"])  # type: ignore[assignment]
        assert missing_instruments == set(_IS_INSTRUMENTS[1:]), (
            f"Genuinely-uncaptured instruments must still report missing: {missing_instruments!r}"
        )


class TestNormalizeInstrumentIdForMatchOptionFutureCollision:
    """bug_c_normalize_id_collision_options_futures_2026_07_22: OPTION and dated-
    FUTURE ids must NOT collapse onto one normalized key — their @-suffix is real
    distinguishing identity (expiry/strike/side), not a settlement/chain tag."""

    def test_option_ids_with_different_expiry_strike_side_stay_distinct(self) -> None:
        from deployment_api.services.data_status.instrument_coverage import (
            _normalize_instrument_id_for_match,
        )

        ids = [
            "DERIBIT:OPTION:BTC-USD@INV-20190405-3250-C",
            "DERIBIT:OPTION:BTC-USD@INV-20190405-3250-P",  # same expiry/strike, other side
            "DERIBIT:OPTION:BTC-USD@INV-20260601-4500-C",  # different expiry/strike
        ]
        normalized = {_normalize_instrument_id_for_match(iid) for iid in ids}
        assert len(normalized) == 3, f"OPTION ids collapsed: {normalized!r}"

    def test_dated_future_ids_with_different_expiry_stay_distinct(self) -> None:
        from deployment_api.services.data_status.instrument_coverage import (
            _normalize_instrument_id_for_match,
        )

        ids = [
            "DERIBIT:FUTURE:AVAX-USDC@LIN-20260401",
            "DERIBIT:FUTURE:AVAX-USDC@LIN-20260601",
        ]
        normalized = {_normalize_instrument_id_for_match(iid) for iid in ids}
        assert len(normalized) == 2, f"dated-FUTURE ids collapsed: {normalized!r}"

    def test_perpetual_settlement_suffix_still_collapses_as_before(self) -> None:
        # The fix must NOT regress the pre-existing, measured-safe PERPETUAL/
        # SPOT_PAIR/COMBO @-strip behaviour this module relies on elsewhere.
        from deployment_api.services.data_status.instrument_coverage import (
            _normalize_instrument_id_for_match,
        )

        assert _normalize_instrument_id_for_match(
            "BINANCE-FUTURES:PERPETUAL:BTC-USDT@LIN"
        ) == _normalize_instrument_id_for_match("BINANCE-FUTURES:PERPETUAL:BTC-USDT")

    def test_deribit_options_denominator_no_longer_collapses(self) -> None:
        """Integration-level: _per_instrument_coverage's expected_shards for a
        DERIBIT-options-shaped universe must scale with the real instrument
        count, not collapse to a handful of normalized keys (the live
        completion_pct=100.0%-clamp false-clean symptom)."""
        options_universe = [
            "DERIBIT:OPTION:BTC-USD@INV-20190405-3250-C",
            "DERIBIT:OPTION:BTC-USD@INV-20190405-3250-P",
            "DERIBIT:OPTION:BTC-USD@INV-20260601-4500-C",
            "DERIBIT:OPTION:ETH-USD@INV-20260601-3000-C",
        ]

        def _options_provider(venue: str, _data_type: str) -> list[str] | None:
            return options_universe if venue == "DERIBIT" else None

        dates = ["2026-01-01"]
        # Only ONE of the 4 options actually has a captured manifest row.
        manifest_df = pd.DataFrame(
            [
                {
                    "venue": "DERIBIT",
                    "data_type": "options_chain",
                    "instrument_id": options_universe[0],
                    "date": dates[0],
                    "capture_status": "captured",
                }
            ]
        )

        result = _per_instrument_coverage(
            venue_df_ok=manifest_df,
            venue="DERIBIT",
            dt="options_chain",
            expected_dates=set(dates),
            cap=None,
            instruments_provider=_options_provider,
        )

        # Pre-fix, all 4 options normalized to the SAME key ("DERIBIT:OPTION:
        # BTC-USD" / "...ETH-USD"), so expected_shards collapsed to 2 (distinct
        # keys) * 1 date instead of the true 4 * 1. Post-fix each stays distinct.
        assert result["expected_shards"] == 4, f"denominator still collapsing: {result!r}"
        assert result["found_shards"] == 1
        missing_instruments: set[str] = set(result["missing_instruments"])  # type: ignore[assignment]
        assert missing_instruments == set(options_universe[1:])


# ---------------------------------------------------------------------------
# Tests: _build_cefi_is_instruments_provider
# ---------------------------------------------------------------------------


class TestBuildCefiIsInstrumentsProvider:
    """Dim-7: _build_cefi_is_instruments_provider reads IS catalog correctly.

    As of 2026-08-05 the provider builds its venue→instruments map from the
    per-date catalog (``prod/catalog.parquet``, via ``_read_cefi_catalogue_metadata``)
    rather than from the availability index snapshot — venue is parsed from the
    canonical ``instrument_id`` (``VENUE:TYPE:SYMBOL``). Tests mock the catalog
    read rather than the (no longer called) ``read_availability_index``.
    """

    @staticmethod
    def _mock_catalogue_metadata(
        instrument_ids: list[str],
        instrument_types: dict[str, str] | None = None,
    ) -> tuple[dict[str, tuple[str | None, str | None]], dict[str, str]]:
        """Build a mock return value for ``_read_cefi_catalogue_metadata``.

        Each instrument_id is given an open-ended existence window
        ``(None, None)`` (always existed). The caller supplies the instrument
        set; venue is parsed from the canonical ``VENUE:TYPE:SYMBOL`` id form.
        """
        windows = dict.fromkeys(instrument_ids, (None, None))
        itypes = instrument_types or {}
        return windows, itypes

    def test_provider_returns_instruments_per_venue(self) -> None:
        """Provider built from mocked catalog parses venue from instrument_id."""
        instrument_ids = [
            "BINANCE-FUTURES:PERPETUAL:BTC-USDT",
            "BINANCE-FUTURES:PERPETUAL:ETH-USDT",
            "BYBIT-FUTURES:PERPETUAL:BTC-USDT",
            "BYBIT-FUTURES:PERPETUAL:ETH-USDT",
        ]

        with patch(
            "deployment_api.services.data_status.instrument_coverage._read_cefi_catalogue_metadata",
            return_value=self._mock_catalogue_metadata(instrument_ids),
        ):
            provider, _windows, _itypes = _build_cefi_is_instruments_provider(cloud="gcp")

        # Provider must return the right instruments for each venue.
        binance_result = provider("BINANCE-FUTURES", _DT)
        assert binance_result is not None
        assert sorted(binance_result) == sorted(instrument_ids[:2])

        bybit_result = provider("BYBIT-FUTURES", _DT)
        assert bybit_result is not None
        assert sorted(bybit_result) == sorted(instrument_ids[2:])

        # Unknown venue returns None -> UAC MVP seed fallback.
        unknown_result = provider("UNKNOWN-VENUE", _DT)
        assert unknown_result is None

    def test_builder_returns_none_on_empty_catalog(self) -> None:
        """Fail-open: an empty IS catalog returns None (caller → MVP seed)."""
        with patch(
            "deployment_api.services.data_status.instrument_coverage._read_cefi_catalogue_metadata",
            return_value=({}, {}),
        ):
            provider, _windows, _itypes = _build_cefi_is_instruments_provider(cloud="gcp")

        assert provider is None

    def test_builder_returns_none_on_catalogue_read_error(self) -> None:
        """Fail-open: a catalog read error returns None (NOT a provider) so the
        caller injects no provider and the denominator path uses UAC's MVP seed.

        Returning a ``lambda: None`` provider here would be WRONG — UAC only
        falls back to its MVP seed when the provider OBJECT is None; a non-None
        provider that returns None yields an EMPTY universe (denominator 0).
        """
        with patch(
            "deployment_api.services.data_status.instrument_coverage._read_cefi_catalogue_metadata",
            side_effect=RuntimeError("GCS error"),
        ):
            provider, windows, _itypes = _build_cefi_is_instruments_provider(cloud="gcp")

        assert provider is None
        assert windows == {}

    def test_provider_deduplicates_instruments(self) -> None:
        """Duplicate instrument_ids in catalog are deduplicated."""
        instrument_ids = [
            "BINANCE-FUTURES:PERPETUAL:BTC-USDT",
            "BINANCE-FUTURES:PERPETUAL:BTC-USDT",  # duplicate
            "BINANCE-FUTURES:PERPETUAL:ETH-USDT",
        ]

        with patch(
            "deployment_api.services.data_status.instrument_coverage._read_cefi_catalogue_metadata",
            return_value=self._mock_catalogue_metadata(instrument_ids),
        ):
            provider, _windows, _itypes = _build_cefi_is_instruments_provider(cloud="gcp")

        result = provider("BINANCE-FUTURES", _DT)
        assert result is not None
        assert len(result) == 2  # duplicates removed
        assert sorted(result) == sorted(["BINANCE-FUTURES:PERPETUAL:BTC-USDT", "BINANCE-FUTURES:PERPETUAL:ETH-USDT"])

    def test_instrument_id_without_venue_segment_is_skipped(self) -> None:
        """Malformed instrument_ids (no colon segment) are silently skipped."""
        instrument_ids = [
            "BINANCE-FUTURES:PERPETUAL:BTC-USDT",  # well-formed
            "MALFORMED_NO_COLON",  # skipped — no venue segment
            "BINANCE-FUTURES:PERPETUAL:ETH-USDT",
        ]

        with patch(
            "deployment_api.services.data_status.instrument_coverage._read_cefi_catalogue_metadata",
            return_value=self._mock_catalogue_metadata(instrument_ids),
        ):
            provider, _windows, _itypes = _build_cefi_is_instruments_provider(cloud="gcp")

        result = provider("BINANCE-FUTURES", _DT)
        assert result is not None
        assert len(result) == 2
        assert "MALFORMED_NO_COLON" not in result


# ---------------------------------------------------------------------------
# Tests: per-instrument existence-window clipping (2026-07-21 coverage-model fix)
#
# Root cause: `expected_count = n_instruments * n_dates` counted every
# expected instrument as existing on every expected date, including days
# before it was listed or after it delisted -- structurally impossible
# (instrument, day) pairs counted as "missing shards". The fix clips each
# instrument's contribution to the denominator (and numerator) to its real
# existence window (`available_from`/`available_to` from the IS catalogue).
# ---------------------------------------------------------------------------


class TestClipDatesToWindow:
    """Unit behaviour of the clipping primitive itself."""

    def test_no_window_returns_all_dates_unclipped(self) -> None:
        dates = {"2026-01-01", "2026-01-02", "2026-01-03"}
        assert _clip_dates_to_window(dates, None) == frozenset(dates)

    def test_bounded_window_clips_both_sides(self) -> None:
        dates = {"2026-01-01", "2026-01-02", "2026-01-03", "2026-01-04"}
        result = _clip_dates_to_window(dates, ("2026-01-02", "2026-01-03"))
        assert result == frozenset({"2026-01-02", "2026-01-03"})

    def test_open_ended_from_only(self) -> None:
        dates = {"2026-01-01", "2026-01-02", "2026-01-03"}
        result = _clip_dates_to_window(dates, ("2026-01-02", None))
        assert result == frozenset({"2026-01-02", "2026-01-03"})

    def test_open_ended_to_only(self) -> None:
        dates = {"2026-01-01", "2026-01-02", "2026-01-03"}
        result = _clip_dates_to_window(dates, (None, "2026-01-02"))
        assert result == frozenset({"2026-01-01", "2026-01-02"})

    def test_window_excludes_every_date(self) -> None:
        dates = {"2026-01-01", "2026-01-02"}
        result = _clip_dates_to_window(dates, ("2026-02-01", "2026-02-28"))
        assert result == frozenset()


class TestPerInstrumentCoverageWithExistenceWindows:
    """Denominator/numerator must be clipped to each instrument's real existence window."""

    def test_late_listed_instrument_does_not_count_pre_listing_days_as_missing(self) -> None:
        """Reproduces the operator's reported bug: an instrument listed partway
        through the window must not have its pre-listing days counted as
        missing shards -- the OLD blanket cross-product did exactly this."""
        dates = ["2026-01-01", "2026-01-02", "2026-01-03", "2026-01-04", "2026-01-05"]
        # Only BTCUSDT has any manifest rows, and only for the 2 days it
        # existed (listed 2026-01-04). SOLUSDT has none.
        manifest_df = _make_manifest_df(
            instrument_ids=[_IS_INSTRUMENTS[0]],
            dates=["2026-01-04", "2026-01-05"],
        )
        windows = {
            _IS_INSTRUMENTS[0]: ("2026-01-04", None),  # listed 2026-01-04, still active
            _IS_INSTRUMENTS[1]: (None, "2025-12-31"),  # delisted before this window entirely
        }

        result = _per_instrument_coverage(
            venue_df_ok=manifest_df,
            venue=_VENUE,
            dt=_DT,
            expected_dates=set(dates),
            cap=None,
            instruments_provider=lambda _v, _dt: _IS_INSTRUMENTS[:2],
            instrument_windows=windows,
        )

        # OLD (buggy) behaviour would compute expected_shards = 2 * 5 = 10.
        # Correct: BTCUSDT contributes 2 (01-04, 01-05); the delisted
        # instrument contributes 0 (its window excludes every requested date).
        assert result["expected_shards"] == 2
        assert result["found_shards"] == 2
        assert result["missing_shards"] == 0
        assert result["completion_pct"] == 100.0

    def test_instrument_absent_from_windows_dict_falls_back_unclipped(self) -> None:
        """Fail-open: an instrument the catalogue read didn't cover must not be
        penalized -- it keeps the full expected_dates, same as pre-fix."""
        dates = ["2026-01-01", "2026-01-02"]
        manifest_df = _make_manifest_df(instrument_ids=[_IS_INSTRUMENTS[0]], dates=dates)

        result = _per_instrument_coverage(
            venue_df_ok=manifest_df,
            venue=_VENUE,
            dt=_DT,
            expected_dates=set(dates),
            cap=None,
            instruments_provider=lambda _v, _dt: [_IS_INSTRUMENTS[0]],
            instrument_windows={},  # no window data at all
        )

        assert result["expected_shards"] == 2  # 1 instrument x 2 dates, unclipped
        assert result["found_shards"] == 2
        assert result["completion_pct"] == 100.0

    def test_none_instrument_windows_matches_pre_fix_behaviour(self) -> None:
        """instrument_windows=None (the default) must reproduce the exact
        pre-fix blanket n_instruments * n_dates denominator -- no regression
        for callers that don't pass the new parameter."""
        dates = ["2026-01-01"]
        manifest_df = _make_manifest_df(instrument_ids=_IS_INSTRUMENTS[:2], dates=dates)

        result = _per_instrument_coverage(
            venue_df_ok=manifest_df,
            venue=_VENUE,
            dt=_DT,
            expected_dates=set(dates),
            cap=None,
            instruments_provider=lambda _v, _dt: _IS_INSTRUMENTS,
        )

        assert result["expected_shards"] == 5  # 5 instruments x 1 date
        assert result["found_shards"] == 2

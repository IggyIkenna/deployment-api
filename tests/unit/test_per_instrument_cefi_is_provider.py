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


# ---------------------------------------------------------------------------
# Tests: _build_cefi_is_instruments_provider
# ---------------------------------------------------------------------------


class TestBuildCefiIsInstrumentsProvider:
    """Dim-7: _build_cefi_is_instruments_provider reads IS catalog correctly."""

    def test_provider_returns_instruments_per_venue(self) -> None:
        """Provider built from a mocked IS catalog returns the right instruments."""
        # Mock the IS catalog DataFrame returned by read_availability_index.
        mock_catalog_df = pd.DataFrame(
            {
                "venue": [_VENUE, _VENUE, "BYBIT-FUTURES", "BYBIT-FUTURES"],
                "instrument_id": [
                    "BINANCE-FUTURES::BTCUSDT",
                    "BINANCE-FUTURES::ETHUSDT",
                    "BYBIT-FUTURES::BTCUSDT",
                    "BYBIT-FUTURES::ETHUSDT",
                ],
            }
        )

        with (
            patch(
                "deployment_api.services.data_status_service.resolve_bucket_name",
                return_value="instruments-store-cefi-test",
            ),
            patch(
                "deployment_api.services.data_status_service.read_availability_index",
                return_value=mock_catalog_df,
            ),
            patch(
                "deployment_api.services.data_status.instrument_coverage.get_storage_client",
                side_effect=RuntimeError("no live catalogue in this test"),
            ),
        ):
            provider, _windows = _build_cefi_is_instruments_provider(cloud="gcp")

        # Provider must return the right instruments for each venue.
        binance_result = provider(_VENUE, _DT)
        assert binance_result is not None
        assert sorted(binance_result) == sorted(["BINANCE-FUTURES::BTCUSDT", "BINANCE-FUTURES::ETHUSDT"])

        bybit_result = provider("BYBIT-FUTURES", _DT)
        assert bybit_result is not None
        assert sorted(bybit_result) == sorted(["BYBIT-FUTURES::BTCUSDT", "BYBIT-FUTURES::ETHUSDT"])

        # Unknown venue returns None -> UAC MVP seed fallback.
        unknown_result = provider("UNKNOWN-VENUE", _DT)
        assert unknown_result is None

    def test_builder_returns_none_on_gcs_error(self) -> None:
        """Fail-open: a GCS error returns None (NOT a provider) so the caller
        injects no provider and the denominator path uses UAC's MVP seed.

        Returning a ``lambda: None`` provider here would be WRONG — UAC only
        falls back to its MVP seed when the provider OBJECT is None; a non-None
        provider that returns None yields an EMPTY universe (denominator 0).
        """
        with (
            patch(
                "deployment_api.services.data_status_service.resolve_bucket_name",
                side_effect=RuntimeError("bucket not found"),
            ),
        ):
            provider, windows = _build_cefi_is_instruments_provider(cloud="gcp")

        assert provider is None
        assert windows == {}

    def test_builder_returns_none_on_empty_catalog(self) -> None:
        """Fail-open: an empty IS catalog returns None (caller → MVP seed)."""
        with (
            patch(
                "deployment_api.services.data_status_service.resolve_bucket_name",
                return_value="instruments-store-cefi-test",
            ),
            patch(
                "deployment_api.services.data_status_service.read_availability_index",
                return_value=pd.DataFrame(),
            ),
            patch(
                "deployment_api.services.data_status.instrument_coverage.get_storage_client",
                side_effect=RuntimeError("no live catalogue in this test"),
            ),
        ):
            provider, _windows = _build_cefi_is_instruments_provider(cloud="gcp")

        assert provider is None

    def test_provider_deduplicates_instruments(self) -> None:
        """Duplicate instrument_ids in IS catalog are deduplicated."""
        mock_catalog_df = pd.DataFrame(
            {
                "venue": [_VENUE, _VENUE, _VENUE],
                "instrument_id": [
                    "BINANCE-FUTURES::BTCUSDT",
                    "BINANCE-FUTURES::BTCUSDT",  # duplicate
                    "BINANCE-FUTURES::ETHUSDT",
                ],
            }
        )

        with (
            patch(
                "deployment_api.services.data_status_service.resolve_bucket_name",
                return_value="instruments-store-cefi-test",
            ),
            patch(
                "deployment_api.services.data_status_service.read_availability_index",
                return_value=mock_catalog_df,
            ),
            patch(
                "deployment_api.services.data_status.instrument_coverage.get_storage_client",
                side_effect=RuntimeError("no live catalogue in this test"),
            ),
        ):
            provider, _windows = _build_cefi_is_instruments_provider(cloud="gcp")

        result = provider(_VENUE, _DT)
        assert result is not None
        assert len(result) == 2  # duplicates removed
        assert sorted(result) == sorted(["BINANCE-FUTURES::BTCUSDT", "BINANCE-FUTURES::ETHUSDT"])


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

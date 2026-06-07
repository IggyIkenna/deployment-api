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
        ):
            provider = _build_cefi_is_instruments_provider(cloud="gcp")

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
            provider = _build_cefi_is_instruments_provider(cloud="gcp")

        assert provider is None

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
        ):
            provider = _build_cefi_is_instruments_provider(cloud="gcp")

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
        ):
            provider = _build_cefi_is_instruments_provider(cloud="gcp")

        result = provider(_VENUE, _DT)
        assert result is not None
        assert len(result) == 2  # duplicates removed
        assert sorted(result) == sorted(["BINANCE-FUTURES::BTCUSDT", "BINANCE-FUTURES::ETHUSDT"])

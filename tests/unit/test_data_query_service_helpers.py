"""Unit tests for the pure-helper methods on ``DataQueryService``.

These cover the date-parse / venue-categorisation / default-data-types
helpers that feed the service's higher-level GCS-touching methods. Pure
methods (no GCS / no IO) — the service ctor only stores ``project_id``.
"""

from __future__ import annotations

from datetime import UTC, datetime

from deployment_api.services.data_query_service import DataQueryService


def _svc() -> DataQueryService:
    return DataQueryService(project_id="test-project")


# ---------------------------------------------------------------------------
# build_bucket_name
# ---------------------------------------------------------------------------


def test_build_bucket_name_lowercases_asset_group() -> None:
    svc = _svc()
    assert svc.build_bucket_name("market-data-tick", "CEFI") == "market-data-tick-cefi-test-project"
    assert (
        svc.build_bucket_name("market-data-tick", "tradfi")
        == "market-data-tick-tradfi-test-project"
    )


# ---------------------------------------------------------------------------
# _venue_to_category
# ---------------------------------------------------------------------------


def test_venue_to_category_cefi_match() -> None:
    svc = _svc()
    assert svc._venue_to_category("BYBIT") == "CEFI"
    assert svc._venue_to_category("binance-spot") == "CEFI"  # lowercase match
    assert svc._venue_to_category("OKX") == "CEFI"
    assert svc._venue_to_category("DERIBIT") == "CEFI"


def test_venue_to_category_tradfi_match() -> None:
    svc = _svc()
    assert svc._venue_to_category("CME") == "TRADFI"
    assert svc._venue_to_category("CBOE") == "TRADFI"
    assert svc._venue_to_category("nasdaq") == "TRADFI"


def test_venue_to_category_defi_match() -> None:
    svc = _svc()
    assert svc._venue_to_category("UNISWAPV3-ARBITRUM") == "DEFI"
    assert svc._venue_to_category("aavev3-base") == "DEFI"
    assert svc._venue_to_category("CURVE-MAINNET") == "DEFI"


def test_venue_to_category_unknown_returns_none() -> None:
    svc = _svc()
    assert svc._venue_to_category("UNKNOWN_VENUE") is None
    assert svc._venue_to_category("") is None


# ---------------------------------------------------------------------------
# _parse_avail_date
# ---------------------------------------------------------------------------


def test_parse_avail_date_iso_with_t_separator() -> None:
    svc = _svc()
    out = svc._parse_avail_date("2026-05-07T12:34:56Z", "test-label")
    assert out == datetime(2026, 5, 7, 12, 34, 56, tzinfo=UTC)


def test_parse_avail_date_iso_with_offset() -> None:
    svc = _svc()
    out = svc._parse_avail_date("2026-05-07T12:34:56+00:00", "test-label")
    assert out == datetime(2026, 5, 7, 12, 34, 56, tzinfo=UTC)


def test_parse_avail_date_yyyy_mm_dd() -> None:
    """Plain YYYY-MM-DD → midnight UTC."""
    svc = _svc()
    out = svc._parse_avail_date("2026-05-07", "test-label")
    assert out == datetime(2026, 5, 7, 0, 0, 0, tzinfo=UTC)


def test_parse_avail_date_invalid_returns_none() -> None:
    """Unparseable input → None (logs warning)."""
    svc = _svc()
    assert svc._parse_avail_date("not-a-date", "test-label") is None
    assert svc._parse_avail_date("", "test-label") is None
    assert svc._parse_avail_date("2026/05/07", "test-label") is None  # wrong separator


# ---------------------------------------------------------------------------
# _default_data_types
# ---------------------------------------------------------------------------


def test_default_data_types_cefi() -> None:
    svc = _svc()
    assert svc._default_data_types("CEFI") == ["trades", "book_snapshot_5"]


def test_default_data_types_tradfi() -> None:
    svc = _svc()
    assert svc._default_data_types("TRADFI") == ["trades", "ohlcv_1m", "tbbo"]


def test_default_data_types_defi() -> None:
    svc = _svc()
    assert svc._default_data_types("DEFI") == ["dex_swaps", "lending_indices"]


def test_default_data_types_unknown_falls_back_to_trades() -> None:
    svc = _svc()
    assert svc._default_data_types("UNKNOWN") == ["trades"]

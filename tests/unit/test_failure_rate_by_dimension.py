"""Unit tests for ``_build_failure_rate_by_dimension`` (data-status panel
"show only failures" filter binding).

The helper projects the per-venue breakdown into a compact
``{venue: {failure_rate, attempted_failed_count}}`` map, scoped to venues
whose ``capture_status_counts.attempted_failed`` is strictly positive.
This pairs with the data-status UI's failure-only filter without making the
client walk the entire per-venue tree.
"""

from __future__ import annotations

from deployment_api.services.data_status_service import _build_failure_rate_by_dimension


def test_empty_venues_dict_returns_empty_map() -> None:
    """No venues → empty projection."""
    assert _build_failure_rate_by_dimension({}) == {}


def test_non_dict_venue_entry_skipped() -> None:
    """Defensive: a non-dict venue entry (corrupted upstream payload) is
    silently skipped rather than raising. Exercises the ``isinstance`` guard.
    """
    venues_dict: dict[str, object] = {
        "BYBIT": "not-a-dict",  # corrupted
        "BINANCE": {
            "failure_rate": 0.25,
            "capture_status_counts": {"attempted_failed": 5},
        },
    }
    out = _build_failure_rate_by_dimension(venues_dict)
    assert "BYBIT" not in out
    assert out["BINANCE"]["failure_rate"] == 0.25
    assert out["BINANCE"]["attempted_failed_count"] == 5


def test_zero_failed_count_excluded() -> None:
    """Venues with zero attempted_failed are excluded — UI's failure filter
    only needs venues that actually failed."""
    venues_dict: dict[str, object] = {
        "OKX": {
            "failure_rate": 0.0,
            "capture_status_counts": {"attempted_failed": 0},
        },
        "DERIBIT": {
            "failure_rate": 0.5,
            "capture_status_counts": {"attempted_failed": 10},
        },
    }
    out = _build_failure_rate_by_dimension(venues_dict)
    assert "OKX" not in out
    assert out["DERIBIT"]["failure_rate"] == 0.5
    assert out["DERIBIT"]["attempted_failed_count"] == 10


def test_missing_capture_status_counts_treated_as_zero() -> None:
    """Venue without capture_status_counts dict → attempted_failed defaults
    to 0 → excluded from output."""
    venues_dict: dict[str, object] = {
        "HYPERLIQUID": {"failure_rate": 0.0},  # missing capture_status_counts
    }
    out = _build_failure_rate_by_dimension(venues_dict)
    assert out == {}


def test_non_numeric_failure_rate_falls_back_to_zero() -> None:
    """Defensive: failure_rate of an unexpected type → 0.0 fallback. Hits
    the ``isinstance(..., (int, float))`` guard."""
    venues_dict: dict[str, object] = {
        "ASTER": {
            "failure_rate": "not-a-number",
            "capture_status_counts": {"attempted_failed": 3},
        },
    }
    out = _build_failure_rate_by_dimension(venues_dict)
    assert out["ASTER"]["failure_rate"] == 0.0
    assert out["ASTER"]["attempted_failed_count"] == 3

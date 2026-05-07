"""Unit tests for the distinct-axis helpers used by event-driven coverage
(SPORTS league/fixture, PREDICTION underlying).

These pure-pandas helpers feed ``_compute_attempt_coverage`` and
``_build_coverage_metrics`` — they're the foundation of the data-status
panel's event-driven coverage column.
"""

from __future__ import annotations

import pandas as pd

from deployment_api.services.data_status_service import (
    _compute_attempt_coverage,
    _distinct_pairs,
    _distinct_values,
    _sports_attempt_count,
)


def _df(rows: list[dict[str, object]]) -> pd.DataFrame:
    return pd.DataFrame(rows)


# ---------------------------------------------------------------------------
# _distinct_values
# ---------------------------------------------------------------------------


def test_distinct_values_missing_column_returns_zero() -> None:
    out = _distinct_values(_df([{"venue": "BYBIT"}]), "league_id")
    assert out == 0


def test_distinct_values_counts_unique_non_blank() -> None:
    df = _df(
        [
            {"league_id": "EPL"},
            {"league_id": "EPL"},  # dup
            {"league_id": "BUNDESLIGA"},
            {"league_id": ""},  # blank dropped
            {"league_id": "   "},  # whitespace dropped
        ]
    )
    assert _distinct_values(df, "league_id") == 2


# ---------------------------------------------------------------------------
# _distinct_pairs
# ---------------------------------------------------------------------------


def test_distinct_pairs_missing_either_column_returns_zero() -> None:
    df = _df([{"league_id": "EPL"}])
    assert _distinct_pairs(df, "league_id", "fixture_type") == 0
    assert _distinct_pairs(df, "fixture_type", "league_id") == 0


def test_distinct_pairs_counts_unique_pairs() -> None:
    df = _df(
        [
            {"league_id": "EPL", "fixture_type": "REGULAR_SEASON"},
            {"league_id": "EPL", "fixture_type": "REGULAR_SEASON"},  # dup
            {"league_id": "EPL", "fixture_type": "PLAYOFF"},
            {"league_id": "BUNDESLIGA", "fixture_type": "REGULAR_SEASON"},
            {"league_id": "", "fixture_type": "REGULAR_SEASON"},  # blank dropped
        ]
    )
    assert _distinct_pairs(df, "league_id", "fixture_type") == 3


# ---------------------------------------------------------------------------
# _sports_attempt_count — cascading axis selection
# ---------------------------------------------------------------------------


def test_sports_attempt_count_prefers_league_fixture_pair() -> None:
    df = _df(
        [
            {
                "league_id": "EPL",
                "fixture_type": "REGULAR",
                "venue": "API_FOOTBALL",
                "instrument_type": "fixture",
            },
            {
                "league_id": "EPL",
                "fixture_type": "PLAYOFF",
                "venue": "API_FOOTBALL",
                "instrument_type": "fixture",
            },
        ]
    )
    assert _sports_attempt_count(df) == 2  # 2 distinct (league, fixture_type) pairs


def test_sports_attempt_count_falls_back_to_league_only() -> None:
    """No fixture_type column → falls back to league count."""
    df = _df(
        [
            {"league_id": "EPL", "venue": "API_FOOTBALL"},
            {"league_id": "BUNDESLIGA", "venue": "API_FOOTBALL"},
        ]
    )
    assert _sports_attempt_count(df) == 2


def test_sports_attempt_count_falls_back_to_venue_instrument_type_pair() -> None:
    """No league columns → falls back to (venue, instrument_type) pair count."""
    df = _df(
        [
            {"venue": "API_FOOTBALL", "instrument_type": "fixture"},
            {"venue": "API_FOOTBALL", "instrument_type": "lineup"},
            {"venue": "FOOTYSTATS", "instrument_type": "fixture"},
        ]
    )
    assert _sports_attempt_count(df) == 3


def test_sports_attempt_count_falls_back_to_venue_only() -> None:
    df = _df(
        [
            {"venue": "API_FOOTBALL"},
            {"venue": "FOOTYSTATS"},
        ]
    )
    assert _sports_attempt_count(df) == 2


# ---------------------------------------------------------------------------
# _compute_attempt_coverage — category dispatch
# ---------------------------------------------------------------------------


def test_compute_attempt_coverage_empty_returns_zeros() -> None:
    assert _compute_attempt_coverage(_df([]), "PREDICTION") == (0, 0)


def test_compute_attempt_coverage_prediction_uses_underlying() -> None:
    df = _df(
        [
            {"underlying": "BTC_UP_DOWN_HOURLY"},
            {"underlying": "BTC_UP_DOWN_HOURLY"},  # dup
            {"underlying": "SPX_UP_DOWN_DAILY"},
        ]
    )
    assert _compute_attempt_coverage(df, "PREDICTION") == (2, 2)


def test_compute_attempt_coverage_sports_uses_sports_count() -> None:
    df = _df(
        [
            {"league_id": "EPL", "fixture_type": "REGULAR"},
            {"league_id": "BUNDESLIGA", "fixture_type": "REGULAR"},
        ]
    )
    assert _compute_attempt_coverage(df, "SPORTS") == (2, 2)


def test_compute_attempt_coverage_dense_category_returns_zero() -> None:
    """CeFi / TradFi / DeFi are dense — return (0, 0) so caller falls back to
    capture coverage."""
    df = _df([{"venue": "BYBIT", "instrument_id": "BTC-USDT"}])
    assert _compute_attempt_coverage(df, "CEFI") == (0, 0)
    assert _compute_attempt_coverage(df, "DEFI") == (0, 0)

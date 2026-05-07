"""Unit tests for ``_compute_empty_reason_counts`` (writegate Phase 4.A
empty_confirmed taxonomy rollup — companion to the failure-pillar work).

Plan: ``writegate_honest_coverage_endtoend_2026_05_06.plan.md`` Phase 4.A.

The helper rolls up ``capture_status=empty_confirmed`` rows by their
``error_reason`` per the closed taxonomy in
``unified_api_contracts.canonical.crosscutting.honest_coverage.EMPTY_CONFIRMED_REASONS``.
This is the operator-visible payoff of the Tier 3D.1 reconciler back-fill,
Tier 3D.2 reader-side fallback, Tier 2.E.2 writer-side
``record_expected_empty(reason=...)``, and Tier 3B sports
``record_empty(reason=SOURCE_RETURNED_ZERO)`` work.

Coverage:

* Empty DataFrame → all-zero reason dict.
* No ``capture_status`` column (legacy v3 manifest) → all zeros.
* No ``error_reason`` column but empty rows present → all in
  ``empty_unclassified`` (legacy null-reason rows).
* Each registered EMPTY_CONFIRMED_REASONS member routes to the
  correct bucket.
* Unrecognised reason string → ``empty_unclassified`` (defensive — also
  catches legacy rows where the Tier 3D.1 back-fill hasn't reached yet).
* NULL / NaN / empty-string ``error_reason`` on an empty row →
  ``empty_unclassified``.
* Captured / attempted_failed rows do NOT contribute to any empty bucket.
"""

from __future__ import annotations

import numpy as np
import pandas as pd

from deployment_api.services.data_status_service import (
    _EMPTY_REASON_KEYS,
    _compute_empty_reason_counts,
)


def _df(rows: list[dict[str, object]]) -> pd.DataFrame:
    return pd.DataFrame(rows)


def test_empty_dataframe_returns_zero_reasons() -> None:
    out = _compute_empty_reason_counts(_df([]))
    assert set(out.keys()) == set(_EMPTY_REASON_KEYS)
    assert all(v == 0 for v in out.values())


def test_no_capture_status_column_returns_zero_reasons() -> None:
    out = _compute_empty_reason_counts(_df([{"venue": "BYBIT", "data_type": "trades"}]))
    assert all(v == 0 for v in out.values())


def test_empty_rows_no_error_reason_column_all_unclassified() -> None:
    """Whole slice is legacy null-reason rows."""
    out = _compute_empty_reason_counts(
        _df(
            [
                {"capture_status": "empty_confirmed", "venue": "CME"},
                {"capture_status": "empty_confirmed", "venue": "NYSE"},
            ]
        )
    )
    assert out["empty_unclassified"] == 2
    assert sum(out.values()) == 2


def test_each_registered_reason_routes_to_correct_bucket() -> None:
    """Every closed-set EMPTY_CONFIRMED_REASONS member maps to its own key."""
    rows = [
        {"capture_status": "empty_confirmed", "error_reason": "EXPECTED_HOLIDAY"},
        {"capture_status": "empty_confirmed", "error_reason": "EXPECTED_WEEKEND"},
        {"capture_status": "empty_confirmed", "error_reason": "EXPECTED_PAUSED_LEAGUE"},
        {
            "capture_status": "empty_confirmed",
            "error_reason": "EXPECTED_PRE_SOURCE_COVERAGE_START",
        },
        {"capture_status": "empty_confirmed", "error_reason": "EXPECTED_PRE_GENESIS_CHAIN"},
        {"capture_status": "empty_confirmed", "error_reason": "EXPECTED_INSTRUMENT_NOT_LISTED"},
        {"capture_status": "empty_confirmed", "error_reason": "EXPECTED_INSTRUMENT_DELISTED"},
        {"capture_status": "empty_confirmed", "error_reason": "EXPECTED_PARTIAL_HALF_DAY"},
        {"capture_status": "empty_confirmed", "error_reason": "SOURCE_RETURNED_ZERO"},
    ]
    out = _compute_empty_reason_counts(_df(rows))
    assert out["EXPECTED_HOLIDAY"] == 1
    assert out["EXPECTED_WEEKEND"] == 1
    assert out["EXPECTED_PAUSED_LEAGUE"] == 1
    assert out["EXPECTED_PRE_SOURCE_COVERAGE_START"] == 1
    assert out["EXPECTED_PRE_GENESIS_CHAIN"] == 1
    assert out["EXPECTED_INSTRUMENT_NOT_LISTED"] == 1
    assert out["EXPECTED_INSTRUMENT_DELISTED"] == 1
    assert out["EXPECTED_PARTIAL_HALF_DAY"] == 1
    assert out["SOURCE_RETURNED_ZERO"] == 1
    assert out["empty_unclassified"] == 0


def test_unrecognised_reason_routes_to_unclassified() -> None:
    """Defensive: a reason string outside the closed set lands in unclassified.

    Catches the case where a future writer emits a reason before this
    taxonomy is extended — surface as unclassified, don't silently drop.
    """
    out = _compute_empty_reason_counts(
        _df(
            [
                {"capture_status": "empty_confirmed", "error_reason": "TOTALLY_NEW_REASON"},
                {"capture_status": "empty_confirmed", "error_reason": "another_unknown"},
            ]
        )
    )
    assert out["empty_unclassified"] == 2
    assert sum(out.values()) == 2


def test_null_blank_reason_routes_to_unclassified() -> None:
    """NULL / NaN / blank error_reason on an empty row → unclassified.

    These are legacy pre-Tier-3D.1 rows the back-fill reconciler hasn't
    reached yet. Counting them under unclassified gives the operator a
    cheap progress indicator.
    """
    out = _compute_empty_reason_counts(
        _df(
            [
                {"capture_status": "empty_confirmed", "error_reason": ""},
                {"capture_status": "empty_confirmed", "error_reason": None},
                {"capture_status": "empty_confirmed", "error_reason": np.nan},
                {"capture_status": "empty_confirmed", "error_reason": "   "},
            ]
        )
    )
    assert out["empty_unclassified"] == 4
    assert sum(out.values()) == 4


def test_captured_and_failed_rows_excluded_from_buckets() -> None:
    """Only ``empty_confirmed`` rows count — captured / attempted_failed
    have their own breakdowns (capture_status_counts + failure_pillars).
    """
    out = _compute_empty_reason_counts(
        _df(
            [
                {"capture_status": "captured", "error_reason": "EXPECTED_HOLIDAY"},
                {"capture_status": "attempted_failed", "error_reason": "EXPECTED_HOLIDAY"},
                {"capture_status": "empty_confirmed", "error_reason": "EXPECTED_HOLIDAY"},
            ]
        )
    )
    assert out["EXPECTED_HOLIDAY"] == 1  # only the empty_confirmed row counts


def test_mixed_reasons_aggregate_correctly() -> None:
    """A realistic venue slice — some rows with each reason."""
    rows = [
        {"capture_status": "empty_confirmed", "error_reason": "EXPECTED_HOLIDAY"},
        {"capture_status": "empty_confirmed", "error_reason": "EXPECTED_HOLIDAY"},
        {"capture_status": "empty_confirmed", "error_reason": "EXPECTED_WEEKEND"},
        {"capture_status": "empty_confirmed", "error_reason": "EXPECTED_WEEKEND"},
        {"capture_status": "empty_confirmed", "error_reason": "EXPECTED_WEEKEND"},
        {"capture_status": "empty_confirmed", "error_reason": "SOURCE_RETURNED_ZERO"},
        {"capture_status": "empty_confirmed", "error_reason": ""},  # legacy
        {"capture_status": "captured", "error_reason": "EXPECTED_HOLIDAY"},  # excluded
    ]
    out = _compute_empty_reason_counts(_df(rows))
    assert out["EXPECTED_HOLIDAY"] == 2
    assert out["EXPECTED_WEEKEND"] == 3
    assert out["SOURCE_RETURNED_ZERO"] == 1
    assert out["empty_unclassified"] == 1
    # All other keys remain zero.
    assert out["EXPECTED_PAUSED_LEAGUE"] == 0
    assert out["EXPECTED_PRE_GENESIS_CHAIN"] == 0


def test_empty_reason_keys_match_closed_set_taxonomy() -> None:
    """Closed-set guarantee: keys mirror UAC EMPTY_CONFIRMED_REASONS + an
    ``empty_unclassified`` catch-all."""
    from unified_api_contracts.canonical.crosscutting.honest_coverage import (
        EMPTY_CONFIRMED_REASONS,
    )

    expected_known = set(EMPTY_CONFIRMED_REASONS)
    actual_known = set(_EMPTY_REASON_KEYS) - {"empty_unclassified"}
    assert actual_known == expected_known, (
        "deployment-api _EMPTY_REASON_KEYS drifted from UAC EMPTY_CONFIRMED_REASONS. "
        f"In UAC but not deployment-api: {expected_known - actual_known}. "
        f"In deployment-api but not UAC: {actual_known - expected_known}."
    )
    assert "empty_unclassified" in _EMPTY_REASON_KEYS

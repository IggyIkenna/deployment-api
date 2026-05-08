"""Unit tests for ``_compute_failure_pillar_counts`` (writegate Phase 4.A item 1).

Plan: ``writegate_honest_coverage_endtoend_2026_05_06.md`` Phase 4.A.

The helper bundles ``capture_status=attempted_failed`` rows by typed-error
class prefix into a fixed taxonomy so the deployment-ui DataStatusTab can
render per-pillar failure breakdowns instead of one opaque "failure_rate".

Coverage:

* Empty DataFrame → all-zero pillar dict.
* No ``capture_status`` column → all zeros (legacy v3 manifest).
* No ``error_reason`` column but failures present → all in ``failed_other``.
* Each registered prefix routes to the correct pillar.
* Unrecognised prefix → ``failed_other`` (regression guard for future error
  classes that ship before the taxonomy is updated).
* Captured / empty_confirmed rows do NOT contribute to any pillar.
* NaN ``error_reason`` on a failed row → ``failed_other`` (defensive).
"""

from __future__ import annotations

import pandas as pd

from deployment_api.services.data_status_service import (
    _FAILURE_PILLAR_KEYS,
    _compute_failure_pillar_counts,
)


def _df(rows: list[dict[str, object]]) -> pd.DataFrame:
    return pd.DataFrame(rows)


def test_empty_dataframe_returns_zero_pillars() -> None:
    out = _compute_failure_pillar_counts(_df([]))
    assert set(out.keys()) == set(_FAILURE_PILLAR_KEYS)
    assert all(v == 0 for v in out.values())


def test_no_capture_status_column_returns_zero_pillars() -> None:
    """Legacy v3 manifest rows without capture_status default to captured —
    no failures means no pillar entries."""
    out = _compute_failure_pillar_counts(_df([{"venue": "BYBIT", "data_type": "trades"}]))
    assert all(v == 0 for v in out.values())


def test_failed_rows_no_error_reason_column_all_other() -> None:
    """When error_reason column is missing entirely, failures land in failed_other."""
    out = _compute_failure_pillar_counts(
        _df(
            [
                {"capture_status": "attempted_failed"},
                {"capture_status": "attempted_failed"},
                {"capture_status": "captured"},
            ]
        )
    )
    assert out["failed_other"] == 2
    assert sum(out.values()) == 2


def test_each_prefix_routes_to_expected_pillar() -> None:
    out = _compute_failure_pillar_counts(
        _df(
            [
                {
                    "capture_status": "attempted_failed",
                    "error_reason": "UpstreamTimestampBiasError(observed_dates=(2026-05-01, 2026-05-01))",
                },
                {
                    "capture_status": "attempted_failed",
                    "error_reason": "MalformedTickFieldError(field='close')",
                },
                {
                    "capture_status": "attempted_failed",
                    "error_reason": "ClusterCoverageError(missing=2, observed=9)",
                },
                {
                    "capture_status": "attempted_failed",
                    "error_reason": "MissingClusterValidationError(data_type='options_chain')",
                },
                {
                    "capture_status": "attempted_failed",
                    "error_reason": "LookaheadBiasError(input_available_at='2026-05-01T12:00')",
                },
            ]
        )
    )
    assert out["failed_timestamp_bias"] == 1
    assert out["failed_malformed"] == 1
    assert out["failed_cluster"] == 2  # ClusterCoverageError + MissingClusterValidationError
    assert out["failed_lookahead_bias"] == 1
    assert out["failed_other"] == 0


def test_unrecognised_error_routes_to_other() -> None:
    """Future typed-error classes that ship before the taxonomy is updated
    must surface as ``failed_other``, NOT silently disappear."""
    out = _compute_failure_pillar_counts(
        _df(
            [
                {
                    "capture_status": "attempted_failed",
                    "error_reason": "FutureNanRatioExceededError(col='close', ratio=0.95)",
                },
                {
                    "capture_status": "attempted_failed",
                    "error_reason": "SomeRandomError(message='oops')",
                },
            ]
        )
    )
    assert out["failed_other"] == 2
    # No false matches in registered pillars.
    assert out["failed_timestamp_bias"] == 0
    assert out["failed_malformed"] == 0
    assert out["failed_cluster"] == 0


def test_captured_and_empty_confirmed_excluded() -> None:
    """Only attempted_failed rows contribute; captured/empty_confirmed ignored."""
    out = _compute_failure_pillar_counts(
        _df(
            [
                {
                    "capture_status": "captured",
                    "error_reason": "UpstreamTimestampBiasError(...)",
                },
                {
                    "capture_status": "empty_confirmed",
                    "error_reason": "MalformedTickFieldError(...)",
                },
                {
                    "capture_status": "attempted_failed",
                    "error_reason": "UpstreamTimestampBiasError(observed=2026-05-01)",
                },
            ]
        )
    )
    assert out["failed_timestamp_bias"] == 1
    assert sum(out.values()) == 1


def test_nan_error_reason_routes_to_other() -> None:
    """NaN/None error_reason on a failed row falls into failed_other."""
    out = _compute_failure_pillar_counts(
        _df(
            [
                {"capture_status": "attempted_failed", "error_reason": None},
                {"capture_status": "attempted_failed", "error_reason": float("nan")},
            ]
        )
    )
    assert out["failed_other"] == 2


def test_pillar_keys_are_stable() -> None:
    """The pillar key set is part of the deployment-ui contract — adding new
    pillars is fine; renaming or removing breaks the UI binding."""
    expected_keys = {
        "failed_timestamp_bias",
        "failed_malformed",
        "failed_cluster",
        "failed_lookahead_bias",
        "failed_nan_ratio",
        "failed_schema",
        "failed_empty_placeholder_backfill",
        "failed_missing_available_at",
        "failed_other",
    }
    assert set(_FAILURE_PILLAR_KEYS) == expected_keys

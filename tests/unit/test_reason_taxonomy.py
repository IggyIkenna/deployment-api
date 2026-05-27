"""Unit tests for the Data Status reason taxonomy classifier.

Anchored on the REAL ``error_reason`` strings observed in the v8 production
manifest (see design/data-status-redesign/COVERAGE_FIDELITY.md §1) so the
classifier stays faithful to what the writer actually stamps.
"""

from __future__ import annotations

import pytest

from deployment_api.services.reason_taxonomy import (
    REASON_CATEGORY_LABELS,
    ReasonCategory,
    classify_reason,
    is_phantom_reason,
    rollup_reasons,
)


@pytest.mark.parametrize(
    ("status", "reason", "expected"),
    [
        # captured
        ("captured", "", ReasonCategory.CAPTURED),
        ("captured", None, ReasonCategory.CAPTURED),
        (None, "", ReasonCategory.CAPTURED),  # legacy null status → captured
        # honest-empty — coverage window (the DeFi bulk)
        ("empty_confirmed", "EXPECTED_PRE_GENESIS_CHAIN", ReasonCategory.EMPTY_COVERAGE),
        ("empty_confirmed", "EXPECTED_INSTRUMENT_NOT_LISTED", ReasonCategory.EMPTY_COVERAGE),
        ("empty_confirmed", "EXPECTED_PAST_SOURCE_COVERAGE_END", ReasonCategory.EMPTY_COVERAGE),
        ("empty_confirmed", "EXPECTED_PRE_VENUE_LAUNCH", ReasonCategory.EMPTY_COVERAGE),
        # honest-empty — calendar/season
        ("empty_confirmed", "EXPECTED_HOLIDAY", ReasonCategory.EMPTY_CALENDAR),
        ("empty_confirmed", "EXPECTED_PAUSED_LEAGUE", ReasonCategory.EMPTY_CALENDAR),
        # honest-empty — source returned zero
        ("empty_confirmed", "SOURCE_RETURNED_ZERO", ReasonCategory.EMPTY_SOURCE_ZERO),
        ("empty_confirmed", "EXPECTED_UPSTREAM_EMPTY", ReasonCategory.EMPTY_SOURCE_ZERO),
        # honest-empty — blank / unknown reason
        ("empty_confirmed", "", ReasonCategory.EMPTY_UNCLASSIFIED),
        ("empty_confirmed", "weird_unmapped_reason", ReasonCategory.EMPTY_UNCLASSIFIED),
        # failures — real observed strings
        ("attempted_failed", "HTTP_429", ReasonCategory.FAIL_RATE_LIMITED),
        ("attempted_failed", "VENUE_FETCH_FAILED", ReasonCategory.FAIL_AUTH),
        ("attempted_failed", "UNCLASSIFIED_VENUE_ERROR", ReasonCategory.FAIL_AUTH),
        ("attempted_failed", "HTTP_401 Unauthorized", ReasonCategory.FAIL_AUTH),
        ("attempted_failed", "Connection timeout to host https", ReasonCategory.FAIL_NETWORK),
        ("attempted_failed", "Server disconnected", ReasonCategory.FAIL_NETWORK),
        (
            "attempted_failed",
            "HTTPSConnectionPool(host='storage.googleapis.com', port=443)",
            ReasonCategory.FAIL_NETWORK,
        ),
        ("attempted_failed", "Response payload is not completed", ReasonCategory.FAIL_NETWORK),
        ("attempted_failed", "404 GET https", ReasonCategory.FAIL_NOT_FOUND),
        ("attempted_failed", "HTTP_405", ReasonCategory.FAIL_NOT_FOUND),
        ("attempted_failed", "SCHEMA_VALIDATION_FAILED", ReasonCategory.FAIL_SCHEMA),
        ("attempted_failed", "In CSV column #4", ReasonCategory.FAIL_SCHEMA),
        ("attempted_failed", "[Errno 28] Error writing bytes to file. Detail", ReasonCategory.FAIL_IO),
        ("attempted_failed", "LegacyBlankErrorReasonError", ReasonCategory.FAIL_LEGACY_MIGRATION),
        ("attempted_failed", "LEGACY_THIRDKEY_DRIFT_RECON_2026_05_07", ReasonCategory.FAIL_LEGACY_MIGRATION),
        (
            "attempted_failed",
            "legacy_bare_name_migrated_to_protocol_solana_2026_05_14",
            ReasonCategory.FAIL_LEGACY_MIGRATION,
        ),
        ("attempted_failed", "something nobody mapped yet", ReasonCategory.FAIL_OTHER),
        ("attempted_failed", "", ReasonCategory.FAIL_OTHER),
        # expected_unattempted split
        ("expected_unattempted", "EXPECTED_PRE_GENESIS_CHAIN", ReasonCategory.EMPTY_COVERAGE),
        ("expected_unattempted", "", ReasonCategory.PENDING),
        # phantom wins over status (captured row with no parquet)
        ("captured", "phantom_captured_no_parquet_at_canonical_path", ReasonCategory.FAIL_PHANTOM),
        ("attempted_failed", "phantom_captured_no_parquet_at_canonical_path", ReasonCategory.FAIL_PHANTOM),
    ],
)
def test_classify_reason(status: str | None, reason: str | None, expected: ReasonCategory) -> None:
    assert classify_reason(status, reason) == expected


def test_classify_reason_case_insensitive() -> None:
    assert classify_reason("ATTEMPTED_FAILED", "http_429") == ReasonCategory.FAIL_RATE_LIMITED
    assert classify_reason("Empty_Confirmed", "SOURCE_RETURNED_ZERO") == ReasonCategory.EMPTY_SOURCE_ZERO


def test_is_phantom_reason() -> None:
    assert is_phantom_reason("phantom_captured_no_parquet_at_canonical_path")
    assert not is_phantom_reason("HTTP_429")
    assert not is_phantom_reason("")


def test_rollup_reasons_full_grid_and_counts() -> None:
    pairs: list[tuple[str | None, str | None]] = [
        ("captured", ""),
        ("captured", ""),
        ("empty_confirmed", "EXPECTED_PRE_GENESIS_CHAIN"),
        ("attempted_failed", "HTTP_429"),
        ("attempted_failed", "HTTP_429"),
        ("attempted_failed", "VENUE_FETCH_FAILED"),
        ("captured", "phantom_captured_no_parquet_at_canonical_path"),
    ]
    out = rollup_reasons(pairs)
    # every category key present (fixed grid)
    assert set(out) == {c.value for c in ReasonCategory}
    assert out[ReasonCategory.CAPTURED.value] == 2
    assert out[ReasonCategory.EMPTY_COVERAGE.value] == 1
    assert out[ReasonCategory.FAIL_RATE_LIMITED.value] == 2
    assert out[ReasonCategory.FAIL_AUTH.value] == 1
    assert out[ReasonCategory.FAIL_PHANTOM.value] == 1
    assert sum(out.values()) == len(pairs)


def test_every_category_has_a_label() -> None:
    for cat in ReasonCategory:
        assert cat in REASON_CATEGORY_LABELS
        assert REASON_CATEGORY_LABELS[cat]

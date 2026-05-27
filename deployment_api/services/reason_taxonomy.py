"""Reason taxonomy — map raw manifest ``capture_status`` + ``error_reason`` to a
closed set of operator-facing categories for the Data Status UI.

The availability manifest (v8) stamps ``error_reason`` as a free-form string —
either a typed-empty reason from the UAC ``EMPTY_CONFIRMED_REASONS`` closed set
(``EXPECTED_*`` / ``SOURCE_RETURNED_ZERO`` / …) or, for ``attempted_failed``
rows, ``repr(typed_error)`` / a raw HTTP-ish string (``HTTP_429``,
``VENUE_FETCH_FAILED``, ``Connection timeout to host https``, …).

The redesigned Data Status tab needs to answer "WHY is this empty / failed?"
with a handful of stable buckets (rate-limit, auth, network, schema, phantom,
honest-empty, legacy-migration-noise). This module is that mapping. It is a
UI-presentation concern (an HTTP DTO axis), so it lives service-local in
deployment-api rather than in UAC — but the ``EXPECTED_*`` membership it keys
on is sourced from the UAC closed set so the two never drift.

Closed set of categories — see ``ReasonCategory``. Anything unrecognised lands
in ``fail_other`` / ``empty_unclassified`` so a new failure mode is never
silently dropped from operator visibility.
"""

from __future__ import annotations

from collections import Counter
from collections.abc import Iterable
from enum import StrEnum

from unified_api_contracts import EMPTY_CONFIRMED_REASONS as _UAC_EMPTY_CONFIRMED_REASONS

# Materialise as a plainly-typed set so downstream set algebra is fully typed
# under basedpyright strict mode (the UAC export is an untyped frozenset).
EMPTY_CONFIRMED_REASONS: frozenset[str] = frozenset(str(r) for r in _UAC_EMPTY_CONFIRMED_REASONS)

__all__ = [
    "REASON_CATEGORY_LABELS",
    "ReasonCategory",
    "classify_reason",
    "is_phantom_reason",
    "rollup_reasons",
]


class ReasonCategory(StrEnum):
    """Operator-facing reason buckets bound by the Data Status UI."""

    CAPTURED = "captured"
    # honest-empty (empty_confirmed)
    EMPTY_CALENDAR = "empty_calendar"
    EMPTY_COVERAGE = "empty_coverage"
    EMPTY_SOURCE_ZERO = "empty_source_zero"
    EMPTY_UNCLASSIFIED = "empty_unclassified"
    # attempted_failed
    FAIL_RATE_LIMITED = "fail_rate_limited"
    FAIL_AUTH = "fail_auth"
    FAIL_NETWORK = "fail_network"
    FAIL_NOT_FOUND = "fail_not_found"
    FAIL_SCHEMA = "fail_schema"
    FAIL_IO = "fail_io"
    FAIL_PHANTOM = "fail_phantom"
    FAIL_LEGACY_MIGRATION = "fail_legacy_migration"
    FAIL_OTHER = "fail_other"
    # expected_unattempted (pending fetch — not yet attempted, not a failure)
    PENDING = "pending"


REASON_CATEGORY_LABELS: dict[str, str] = {
    ReasonCategory.CAPTURED: "Captured",
    ReasonCategory.EMPTY_CALENDAR: "Empty — calendar/season",
    ReasonCategory.EMPTY_COVERAGE: "Empty — outside source coverage",
    ReasonCategory.EMPTY_SOURCE_ZERO: "Empty — source returned zero",
    ReasonCategory.EMPTY_UNCLASSIFIED: "Empty — unclassified",
    ReasonCategory.FAIL_RATE_LIMITED: "Failed — rate limited (HTTP 429)",
    ReasonCategory.FAIL_AUTH: "Failed — auth / API key / OAuth",
    ReasonCategory.FAIL_NETWORK: "Failed — network / timeout",
    ReasonCategory.FAIL_NOT_FOUND: "Failed — not found (HTTP 404/405)",
    ReasonCategory.FAIL_SCHEMA: "Failed — schema / parse",
    ReasonCategory.FAIL_IO: "Failed — disk / I/O",
    ReasonCategory.FAIL_PHANTOM: "Phantom — manifest says captured, no parquet",
    ReasonCategory.FAIL_LEGACY_MIGRATION: "Legacy migration marker (not a live failure)",
    ReasonCategory.FAIL_OTHER: "Failed — other / unclassified",
    ReasonCategory.PENDING: "Pending fetch (not yet attempted)",
}

# ``empty_confirmed`` reason sub-grouping. Calendar/season vs out-of-coverage
# vs source-zero — keyed on the UAC closed set so this never drifts from the
# writer-stamped vocabulary.
_EMPTY_CALENDAR_REASONS: frozenset[str] = frozenset(
    {
        "EXPECTED_HOLIDAY",
        "EXPECTED_WEEKEND",
        "EXPECTED_PARTIAL_HALF_DAY",
        "EXPECTED_OUTSIDE_TRADING_HOURS",
        "EXPECTED_OUTSIDE_TRANSFER_WINDOW",
        "EXPECTED_PAUSED_LEAGUE",
        "EXPECTED_PRE_SEASON",
        "EXPECTED_POST_SEASON",
        "EXPECTED_NO_FIXTURE",
        "EXPECTED_FIXTURE_CANCELLED",
        "EXPECTED_FIXTURE_POSTPONED",
        "EXPECTED_PROTOCOL_PAUSED",
    }
)
_EMPTY_SOURCE_ZERO_REASONS: frozenset[str] = frozenset(
    {
        "SOURCE_RETURNED_ZERO",
        "EXPECTED_UPSTREAM_EMPTY",
        "EXPECTED_NO_FUNDING_RATE_TICKS",
        "EXPECTED_NO_PNL_STREAM",
        "NO_INPUT_AVAILABLE",
        "LEG_ABSENT_LEFT",
        "LEG_ABSENT_RIGHT",
    }
)
# Everything else in EMPTY_CONFIRMED_REASONS is a coverage-window reason
# (PRE_GENESIS_CHAIN, PRE_VENUE_LAUNCH, INSTRUMENT_NOT_LISTED, DELISTED,
# PAST_SOURCE_COVERAGE_END, OUT_OF_COVERAGE_WINDOW, KNOWN_SOURCE_GAP, …).
_EMPTY_COVERAGE_REASONS: frozenset[str] = frozenset(EMPTY_CONFIRMED_REASONS) - (
    _EMPTY_CALENDAR_REASONS | _EMPTY_SOURCE_ZERO_REASONS
)

# Phantom marker stamped by the phantom-reconciler audit when the manifest row
# says ``captured`` but no parquet exists at the canonical path.
_PHANTOM_MARKER = "phantom_captured_no_parquet"

# Substring fragments (lower-cased) → failure category. Ordered by specificity:
# the FIRST match wins, so put narrow markers (429, 404) before broad ones.
_FAIL_SUBSTRINGS: tuple[tuple[str, ReasonCategory], ...] = (
    (_PHANTOM_MARKER, ReasonCategory.FAIL_PHANTOM),
    ("http_429", ReasonCategory.FAIL_RATE_LIMITED),
    ("429", ReasonCategory.FAIL_RATE_LIMITED),
    ("rate limit", ReasonCategory.FAIL_RATE_LIMITED),
    ("too many requests", ReasonCategory.FAIL_RATE_LIMITED),
    ("http_401", ReasonCategory.FAIL_AUTH),
    ("http_403", ReasonCategory.FAIL_AUTH),
    ("401", ReasonCategory.FAIL_AUTH),
    ("403", ReasonCategory.FAIL_AUTH),
    ("unauthorized", ReasonCategory.FAIL_AUTH),
    ("forbidden", ReasonCategory.FAIL_AUTH),
    ("api key", ReasonCategory.FAIL_AUTH),
    ("api_key", ReasonCategory.FAIL_AUTH),
    ("apikey", ReasonCategory.FAIL_AUTH),
    ("oauth", ReasonCategory.FAIL_AUTH),
    ("credential", ReasonCategory.FAIL_AUTH),
    ("authentication", ReasonCategory.FAIL_AUTH),
    ("invalid signature", ReasonCategory.FAIL_AUTH),
    ("venue_fetch_failed", ReasonCategory.FAIL_AUTH),
    ("unclassified_venue_error", ReasonCategory.FAIL_AUTH),
    ("http_404", ReasonCategory.FAIL_NOT_FOUND),
    ("http_405", ReasonCategory.FAIL_NOT_FOUND),
    ("404", ReasonCategory.FAIL_NOT_FOUND),
    ("405", ReasonCategory.FAIL_NOT_FOUND),
    ("not found", ReasonCategory.FAIL_NOT_FOUND),
    ("timeout", ReasonCategory.FAIL_NETWORK),
    ("timed out", ReasonCategory.FAIL_NETWORK),
    ("connection", ReasonCategory.FAIL_NETWORK),
    ("connectionpool", ReasonCategory.FAIL_NETWORK),
    ("disconnect", ReasonCategory.FAIL_NETWORK),
    ("payload is not completed", ReasonCategory.FAIL_NETWORK),
    ("ssl", ReasonCategory.FAIL_NETWORK),
    ("read timed", ReasonCategory.FAIL_NETWORK),
    ("schema_validation_failed", ReasonCategory.FAIL_SCHEMA),
    ("schema", ReasonCategory.FAIL_SCHEMA),
    ("malformed", ReasonCategory.FAIL_SCHEMA),
    ("in csv column", ReasonCategory.FAIL_SCHEMA),
    ("parse", ReasonCategory.FAIL_SCHEMA),
    ("validationerror", ReasonCategory.FAIL_SCHEMA),
    ("errno 28", ReasonCategory.FAIL_IO),
    ("no space left", ReasonCategory.FAIL_IO),
    ("error writing bytes", ReasonCategory.FAIL_IO),
    ("disk", ReasonCategory.FAIL_IO),
    ("legacyblankerrorreason", ReasonCategory.FAIL_LEGACY_MIGRATION),
    ("legacy_thirdkey_drift", ReasonCategory.FAIL_LEGACY_MIGRATION),
    ("legacy_bare_name_migrated", ReasonCategory.FAIL_LEGACY_MIGRATION),
    ("legacy", ReasonCategory.FAIL_LEGACY_MIGRATION),
)

_STATUS_CAPTURED = "captured"
_STATUS_EMPTY = "empty_confirmed"
_STATUS_FAILED = "attempted_failed"
_STATUS_UNATTEMPTED = "expected_unattempted"


def is_phantom_reason(error_reason: str) -> bool:
    """True if the reason is the phantom-manifest marker (captured, no parquet)."""
    return _PHANTOM_MARKER in (error_reason or "").lower()


def _classify_empty_reason(reason: str) -> ReasonCategory:
    """Sub-classify an ``empty_confirmed`` row's ``error_reason``."""
    if reason in _EMPTY_CALENDAR_REASONS:
        return ReasonCategory.EMPTY_CALENDAR
    if reason in _EMPTY_SOURCE_ZERO_REASONS:
        return ReasonCategory.EMPTY_SOURCE_ZERO
    if reason in _EMPTY_COVERAGE_REASONS:
        return ReasonCategory.EMPTY_COVERAGE
    # Blank or non-closed-set reason on an empty row → unclassified.
    return ReasonCategory.EMPTY_UNCLASSIFIED


def _classify_failure_reason(reason_lc: str) -> ReasonCategory:
    """Sub-classify an ``attempted_failed`` row's lower-cased ``error_reason``."""
    for fragment, category in _FAIL_SUBSTRINGS:
        if fragment in reason_lc:
            return category
    return ReasonCategory.FAIL_OTHER


# Status → handler. ``expected_unattempted`` and the failure path are special-
# cased in ``classify_reason`` because they branch on the reason string.
_SIMPLE_STATUS_CATEGORY: dict[str, ReasonCategory] = {
    _STATUS_CAPTURED: ReasonCategory.CAPTURED,
}


def classify_reason(capture_status: str | None, error_reason: str | None) -> ReasonCategory:
    """Map a manifest ``(capture_status, error_reason)`` pair to a UI category.

    Args:
        capture_status: ``captured`` / ``empty_confirmed`` / ``attempted_failed`` /
            ``expected_unattempted`` (case-insensitive; blank/None → captured,
            matching UTL legacy-read semantics).
        error_reason: raw manifest reason string (may be blank).

    Returns:
        A :class:`ReasonCategory`. ``captured`` rows whose reason is the phantom
        marker classify as :attr:`ReasonCategory.FAIL_PHANTOM` regardless of
        status, since a phantom ``captured`` row is functionally a missing
        parquet.
    """
    status = (capture_status or _STATUS_CAPTURED).strip().lower()
    reason = (error_reason or "").strip()

    # Phantom wins over status — a "captured" row with no parquet is not captured.
    if is_phantom_reason(reason):
        return ReasonCategory.FAIL_PHANTOM

    simple = _SIMPLE_STATUS_CATEGORY.get(status)
    if simple is not None:
        return simple
    if status == _STATUS_EMPTY:
        return _classify_empty_reason(reason)
    if status == _STATUS_UNATTEMPTED:
        # EXPECTED_* reason → known-empty (treat like coverage); else pending fetch.
        return ReasonCategory.EMPTY_COVERAGE if reason.startswith("EXPECTED_") else ReasonCategory.PENDING
    if status == _STATUS_FAILED:
        return _classify_failure_reason(reason.lower())

    # Unknown status string — be honest, don't pretend it's captured.
    return ReasonCategory.FAIL_OTHER


def rollup_reasons(
    pairs: Iterable[tuple[str | None, str | None]],
) -> dict[str, int]:
    """Roll up ``(capture_status, error_reason)`` pairs into a category-count map.

    Every :class:`ReasonCategory` key is present (count 0 when unmatched) so the
    UI can render a fixed grid without conditional checks.
    """
    counts: Counter[str] = Counter()
    for status, reason in pairs:
        counts[classify_reason(status, reason).value] += 1
    return {cat.value: int(counts.get(cat.value, 0)) for cat in ReasonCategory}

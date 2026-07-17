"""4-state capture-status counters, failure pillars + coverage metrics.

Split out of the 6,663-line ``data_status_service.py`` god-module
(codex ratchet plan 2026-06-10). The facade module re-exports every
public + legacy-underscore name, so callers keep importing from
``deployment_api.services.data_status_service``.
"""

import logging
from typing import Literal, cast

import pandas as pd
from unified_api_contracts import (
    CaptureStatusCounts,
    compute_honest_coverage,
    is_out_of_coverage_window,
)

from deployment_api.services.data_status_union import (
    has_provenance_columns as _has_provenance_columns,
)
from deployment_api.services.data_status_union import (
    union_reduce_to_cells as _union_reduce_to_cells,
)

logger = logging.getLogger(__name__)

# Per-category coverage semantics — distinguishes "dense" categories where every
# underlying is expected to produce data every day (CeFi, TradFi, DeFi) from
# "event-driven" categories where underlyings only trade on a fraction of days
# (sports fixtures, Polymarket conditionIds). For event-driven categories the
# shards-weighted ``capture_coverage_pct`` vastly understates real coverage
# because the denominator assumes every (underlying x day) combo should have
# trades. The displayed ``completion_pct`` is therefore the ``attempt_coverage_pct``
# (did we observe this underlying at all), with ``capture_coverage_pct`` kept
# for the detail drill-down and ``empty_rate_estimate`` showing the fraction
# of underlying-days that had no trades.
COVERAGE_SEMANTICS: dict[str, Literal["dense", "event_driven"]] = {
    "CEFI": "dense",
    "TRADFI": "dense",
    "DEFI": "dense",
    "SPORTS": "event_driven",
    "PREDICTION": "event_driven",
}


def distinct_pairs(df: pd.DataFrame, col_a: str, col_b: str) -> int:
    """Return count of distinct non-empty (col_a, col_b) pairs. 0 if cols missing."""
    if col_a not in df.columns or col_b not in df.columns:
        return 0
    pairs = {
        (str(a), str(b))  # pyright: ignore[reportAny]
        for a, b in zip(df[col_a].tolist(), df[col_b].tolist(), strict=True)  # pyright: ignore[reportAny]
        if a and b and str(a).strip() and str(b).strip()  # pyright: ignore[reportAny]
    }
    return len(pairs)


def distinct_values(df: pd.DataFrame, col: str) -> int:
    """Return count of distinct non-empty values in col. 0 if col missing."""
    if col not in df.columns:
        return 0
    return len({str(v) for v in df[col].tolist() if v and str(v).strip()})  # pyright: ignore[reportAny]


def sports_attempt_count(filtered: pd.DataFrame) -> int:
    """Pick the most specific sports attempt axis available in the manifest."""
    n = distinct_pairs(filtered, "league_id", "fixture_type")
    if n:
        return n
    n = distinct_values(filtered, "league_id")
    if n:
        return n
    n = distinct_pairs(filtered, "venue", "instrument_type")
    if n:
        return n
    return distinct_values(filtered, "venue")


CAPTURE_STATUS_COL = "capture_status"
CAPTURE_STATUS_CAPTURED = "captured"
CAPTURE_STATUS_EMPTY = "empty_confirmed"
CAPTURE_STATUS_FAILED = "attempted_failed"
EXPECTED_REASON_PREFIX = "EXPECTED_"


def compute_capture_status_counts(df: pd.DataFrame) -> CaptureStatusCounts:
    """Bucket manifest rows by ``capture_status`` (UTL v5 column) into a 5-field CaptureStatusCounts.

    Legacy rows (pre-Phase-A parquet, no ``capture_status`` column, or NaN
    values inside a mixed DataFrame) coerce to ``"captured"`` — matches the
    legacy-read semantics of ``ManifestWriter.lookup`` in UTL.
    ``expected_unattempted`` rows are split by ``error_reason``:
    - EXPECTED_* prefix → ``expected_unattempted_known_empty`` (skip-worthy)
    - other → ``expected_unattempted_pending_fetch`` (retry)
    """
    if df.empty:
        return CaptureStatusCounts()
    if CAPTURE_STATUS_COL not in df.columns:
        return CaptureStatusCounts(captured=len(df))
    # G3/M5 UNION: post-migration a cell carries one row per (source,
    # pipeline_mode). Collapse to one honest row per cell BEFORE bucketing so a
    # cell captured by >1 source/mode counts once (≥1 captured ⇒ captured) and
    # never inflates ``captured`` past the de-dup'd denominator. v8 manifests
    # (no provenance columns) already have one row per cell → skipped (no-op).
    if _has_provenance_columns(df):
        df = _union_reduce_to_cells(df)
    series = df[CAPTURE_STATUS_COL].fillna(CAPTURE_STATUS_CAPTURED).astype(str).str.lower()
    reason_col = df["error_reason"].astype(str) if "error_reason" in df.columns else pd.Series("", index=df.index)
    eu_mask = series == "expected_unattempted"
    known_empty = 0
    pending_fetch = 0
    if eu_mask.any():
        eu_reasons = reason_col[eu_mask]
        known_empty = int(eu_reasons.str.startswith(EXPECTED_REASON_PREFIX).sum())  # pyright: ignore[reportUnknownMemberType]
        pending_fetch = int((~eu_reasons.str.startswith(EXPECTED_REASON_PREFIX)).sum())  # pyright: ignore[reportUnknownMemberType]
    return CaptureStatusCounts(
        captured=int(
            (
                (series == CAPTURE_STATUS_CAPTURED)
                | ~series.isin(
                    [CAPTURE_STATUS_CAPTURED, CAPTURE_STATUS_EMPTY, CAPTURE_STATUS_FAILED, "expected_unattempted"]
                )
            ).sum()
        ),
        empty_confirmed=int((series == CAPTURE_STATUS_EMPTY).sum()),
        attempted_failed=int((series == CAPTURE_STATUS_FAILED).sum()),
        expected_unattempted_known_empty=known_empty,
        expected_unattempted_pending_fetch=pending_fetch,
        # OOW clip (operator direction 2026-06-23): the subset of
        # ``empty_confirmed`` cells that are never-collectable (out-of-coverage
        # lifecycle reasons + schedule-defining FIXTURES no-match-day empties).
        # Populating it here makes ``compute_honest_coverage(counts)`` exclude
        # those cells from BOTH numerator and denominator, so an out-of-life
        # empty reads as a BLANK not a coverage success — matching the
        # denominator math ``coverage.py`` already does. Every consumer that
        # bases honest_coverage on ``compute_capture_status_counts``
        # (``derive_capture_status_rates`` → panel rollup + per-venue breakdown)
        # therefore returns the in-window-clipped %, consistent with coverage.py.
        out_of_window=compute_out_of_window_count(df),
    )


# ---------------------------------------------------------------------------
# Per-pillar failure breakdown (writegate Phase 4.A item 1)
# ---------------------------------------------------------------------------
#
# The ``capture_status=attempted_failed`` rows carry a free-form
# ``error_reason`` string set by the writer to ``repr(typed_error)``. To give
# operators (deployment-ui DataStatusTab) per-pillar visibility instead of one
# opaque "failure_rate" gauge, we bucket these strings by typed-error class
# name into a fixed taxonomy.
#
# Each entry maps a UTL/MTDS typed-error class name to the manifest-row
# breakdown column the UI binds. New typed-error classes ship over time
# (``NanRatioExceededError``, ``SchemaMismatchError``, etc. per the writegate
# plan); add them here in the same change as the typed-error class lands.
# Anything unrecognised falls into ``failed_other`` so we don't silently drop
# a new failure mode from operator visibility.

FAILURE_PILLAR_BY_ERROR_PREFIX: dict[str, str] = {
    "UpstreamTimestampBiasError": "failed_timestamp_bias",
    "MalformedTickFieldError": "failed_malformed",
    "ClusterCoverageError": "failed_cluster",
    "MissingClusterValidationError": "failed_cluster",
    "LookaheadBiasError": "failed_lookahead_bias",
}

FAILURE_PILLAR_KEYS: tuple[str, ...] = (
    "failed_timestamp_bias",
    "failed_malformed",
    "failed_cluster",
    "failed_lookahead_bias",
    "failed_nan_ratio",  # placeholder — class lands in writegate Phase 1A.future
    "failed_schema",  # placeholder — class lands in writegate Phase 1A.future
    "failed_empty_placeholder_backfill",  # placeholder — reconciler error
    "failed_missing_available_at",  # placeholder — write-time guard
    "failed_other",  # catch-all for unrecognised reprs
)


# ---------------------------------------------------------------------------
# Per-empty-reason breakdown (writegate Phase 4.A — empty_confirmed taxonomy).
# ---------------------------------------------------------------------------
#
# Companion to ``compute_failure_pillar_counts``. Where pillars bucket
# ``attempted_failed`` rows by typed-error class, this rolls up
# ``empty_confirmed`` rows by their ``error_reason`` — the closed taxonomy
# from ``unified_api_contracts.canonical.crosscutting.honest_coverage.EMPTY_CONFIRMED_REASONS``
# stamped by Tier 3D.1 reconciler (existing rows) + Tier 3D.2 reader-side
# fallback + Tier 2.E.2 writer-side ``record_expected_empty(reason=...)``
# + Tier 3B sports ``record_empty(reason=SOURCE_RETURNED_ZERO)``.
#
# Without this rollup, the Phase 2.E + Phase 3.D + Phase 3.B work that
# stamps typed reasons on every empty_confirmed row stays invisible to the
# operator — the UI would see "X empty_confirmed shards" with no breakdown
# of WHY (calendar holiday vs paused league vs source returned zero vs
# pre-genesis chain). This rollup lets the data-status panel render a
# stacked-bar of empty reasons next to the failure-pillars stack.

# Closed-set keys exact-match the EMPTY_CONFIRMED_REASONS taxonomy plus a
# ``empty_unclassified`` catch-all for legacy null-reason rows that haven't
# been re-stamped yet by the Tier 3D.1 reconciler. Once the back-fill
# completes for an asset_group, the catch-all should drop to zero — its
# count is a cheap progress indicator for the back-fill rollout.
EMPTY_REASON_KEYS: tuple[str, ...] = (
    "EXPECTED_HOLIDAY",
    "EXPECTED_WEEKEND",
    "EXPECTED_PAUSED_LEAGUE",
    "EXPECTED_PRE_SOURCE_COVERAGE_START",
    "EXPECTED_PRE_GENESIS_CHAIN",
    "EXPECTED_CHAIN_AGGREGATE",
    "EXPECTED_PRE_VENUE_LAUNCH",
    "EXPECTED_INSTRUMENT_NOT_LISTED",
    "EXPECTED_INSTRUMENT_DELISTED",
    "EXPECTED_PARTIAL_HALF_DAY",
    "EXPECTED_OUTSIDE_TRADING_HOURS",
    "EXPECTED_OUTSIDE_TRANSFER_WINDOW",
    "EXPECTED_PRE_SEASON",
    "EXPECTED_POST_SEASON",
    "EXPECTED_SOURCE_DOES_NOT_COVER_LEAGUE",
    "EXPECTED_SOURCE_DOES_NOT_OFFER_DATA_TYPE",
    "EXPECTED_REFDATA_CADENCE_CHANGE",
    "EXPECTED_DEPRECATED_DATA_TYPE",
    "EXPECTED_KNOWN_SOURCE_GAP",
    # Bounded evidenced out-of-bounds range (UAC canonical.coverage_exclusions). OUT OF MODEL
    # (clipped from numerator + denominator) — so it MUST keep its own visible bucket here:
    # an out-of-model range that is invisible is indistinguishable from data we lost.
    "EXPECTED_UPSTREAM_OUT_OF_BOUNDS",
    "EXPECTED_UPSTREAM_EMPTY",
    "EXPECTED_OUT_OF_COVERAGE_WINDOW",
    "EXPECTED_FIXTURE_CANCELLED",
    "EXPECTED_FIXTURE_POSTPONED",
    "EXPECTED_NO_FIXTURE",
    "EXPECTED_NO_MAPPING",
    "EXPECTED_OUTSIDE_PROCESSING_SCOPE",
    "EXPECTED_LEGACY_MIGRATION_MISSING_EXPIRY",
    "EXPECTED_NO_FUNDING_RATE_TICKS",
    "EXPECTED_NO_PNL_STREAM",
    "EXPECTED_PROTOCOL_PAUSED",
    "EXPECTED_PAST_SOURCE_COVERAGE_END",
    "EXPECTED_SOURCE_DELIVERY_LAG",
    "EXPECTED_BOOKMAKER_NO_LEAGUE_COVERAGE",
    "EXPECTED_NO_PROVIDER_COVERAGE",
    "EXPECTED_NOT_ENOUGH_TVL",  # DeFi sub-TVL pool — outside MVP capture universe (UAC parity)
    "EXPECTED_WRITE_GATE_NAN_THRESHOLD_EXCEEDED",  # write-gate NaN cap — feature-service parity (UAC)
    "SOURCE_RETURNED_ZERO",
    "NO_INPUT_AVAILABLE",
    "LEG_ABSENT_LEFT",
    "LEG_ABSENT_RIGHT",
    "empty_unclassified",  # legacy rows pre-Tier-3D.1 back-fill
)


def compute_empty_reason_counts(df: pd.DataFrame) -> dict[str, int]:
    """Bucket ``empty_confirmed`` rows by ``error_reason`` per the closed taxonomy.

    Args:
        df: Manifest slice — typically a venue or category sub-frame.

    Returns:
        ``{empty_reason: count}`` for every key in ``EMPTY_REASON_KEYS``.
        Reasons with zero matches are included with count 0 so the UI can
        render the full grid without conditional checks.

    Empty rows whose ``error_reason`` doesn't match any registered closed-set
    member fall into ``empty_unclassified`` rather than being silently
    dropped — this counts the legacy rows that pre-date the Tier 3D.1
    reconciler back-fill and surfaces back-fill progress to the operator.
    Empty rows with NULL/blank ``error_reason`` (Tier 3D.1 hasn't reached
    them yet) also land in ``empty_unclassified``.
    """
    out: dict[str, int] = dict.fromkeys(EMPTY_REASON_KEYS, 0)
    if df.empty or CAPTURE_STATUS_COL not in df.columns:
        return out
    empty_mask = df[CAPTURE_STATUS_COL].fillna(CAPTURE_STATUS_CAPTURED).astype(str).str.lower() == CAPTURE_STATUS_EMPTY
    if not bool(empty_mask.any()):
        return out
    if "error_reason" not in df.columns:
        # Whole slice is legacy null-reason rows.
        out["empty_unclassified"] = int(empty_mask.sum())
        return out
    reasons = df.loc[empty_mask, "error_reason"].fillna("").astype(str).str.strip()
    known = set(EMPTY_REASON_KEYS) - {"empty_unclassified"}
    for reason in reasons:
        if reason in known:
            out[reason] += 1
        else:
            # Empty string, NaN-coerced "", or unrecognised value → unclassified.
            out["empty_unclassified"] += 1
    return out


def compute_out_of_window_count(df: pd.DataFrame) -> int:
    """Count ``empty_confirmed`` rows whose cell is out-of-coverage-window.

    Out-of-window cells (pre-genesis chains, pre-launch venues, delisted
    instruments, post/pre-season, deprecated data_types, etc.) carry one of the
    lifecycle reasons in ``OUT_OF_COVERAGE_WINDOW_REASONS``.  They are
    **never-collectable** — not gaps — and must be excluded from the
    completion-% denominator.

    DATA-TYPE-AWARE (operator direction 2026-06-23): a schedule-DEFINING
    data_type (sports ``FIXTURES`` — the API-Football schedule) that is
    ``empty_confirmed`` with ``SOURCE_RETURNED_ZERO`` means "no matches that day
    = complete" → RESOLVED, out-of-window. So when a ``data_type`` column is
    present we pass it through ``is_out_of_coverage_window`` so FIXTURES
    no-match-day empties stop counting as gaps. An ENRICHMENT data_type's
    ``SOURCE_RETURNED_ZERO`` is NOT excluded (its zero may be a real miss).

    Within-window absences (weekends, holidays, paused leagues) and blank/None
    reasons still count in the denominator via ``is_out_of_coverage_window``
    returning False for those.

    Args:
        df: Manifest slice (any scope — category, venue, chain, …).

    Returns:
        Number of ``empty_confirmed`` rows flagged as out-of-window.
    """
    if df.empty or CAPTURE_STATUS_COL not in df.columns:
        return 0
    empty_mask = df[CAPTURE_STATUS_COL].fillna(CAPTURE_STATUS_CAPTURED).astype(str).str.lower() == CAPTURE_STATUS_EMPTY
    if not bool(empty_mask.any()):
        return 0
    if "error_reason" not in df.columns:
        return 0  # no reason column → assume within-window (conservative)
    reasons = df.loc[empty_mask, "error_reason"].fillna("").astype(str).str.strip()
    if "data_type" in df.columns:
        data_types = df.loc[empty_mask, "data_type"].fillna("").astype(str).str.strip()
        reason_list: list[str] = [str(r) for r in reasons.tolist()]  # pyright: ignore[reportAny]
        dtype_list: list[str] = [str(d) for d in data_types.tolist()]  # pyright: ignore[reportAny]
        return sum(
            1
            for reason, data_type in zip(reason_list, dtype_list, strict=True)
            if is_out_of_coverage_window(reason, data_type)
        )
    return int(reasons.apply(is_out_of_coverage_window).sum())  # pyright: ignore[reportUnknownMemberType]


def compute_failure_pillar_counts(df: pd.DataFrame) -> dict[str, int]:
    """Bucket ``attempted_failed`` rows by typed-error class prefix.

    Args:
        df: Manifest slice — typically a venue or category sub-frame.

    Returns:
        ``{pillar_key: count}`` for every key in ``FAILURE_PILLAR_KEYS``.
        Pillars with zero matches are included with count 0 so the UI can
        render the full grid without conditional checks.

    Failed rows whose ``error_reason`` doesn't match any registered prefix
    fall into ``failed_other`` rather than being silently dropped — this
    catches future typed-error classes that ship before this taxonomy is
    extended, surfacing them as "unclassified failures" in the UI.
    """
    out: dict[str, int] = dict.fromkeys(FAILURE_PILLAR_KEYS, 0)
    if df.empty or CAPTURE_STATUS_COL not in df.columns:
        return out
    failed_mask = (
        df[CAPTURE_STATUS_COL].fillna(CAPTURE_STATUS_CAPTURED).astype(str).str.lower() == CAPTURE_STATUS_FAILED
    )
    if not bool(failed_mask.any()):
        return out
    if "error_reason" not in df.columns:
        # All failures are unclassified.
        out["failed_other"] = int(failed_mask.sum())
        return out
    failed_reasons = df.loc[failed_mask, "error_reason"].fillna("").astype(str)
    for reason in failed_reasons:
        matched = False
        for prefix, pillar in FAILURE_PILLAR_BY_ERROR_PREFIX.items():
            if reason.startswith(prefix):
                out[pillar] += 1
                matched = True
                break
        if not matched:
            out["failed_other"] += 1
    return out


def derive_capture_status_rates(
    counts: CaptureStatusCounts,
    total_expected_cells: int,
) -> dict[str, float | int]:
    """Turn capture_status counts + expected-cells denominator into rates.

    ``attempt_coverage_pct`` / ``capture_coverage_pct`` are rounded to 2 dp
    and clamped to 100 so malformed denominators don't produce >100% figures.
    ``empty_rate`` / ``failure_rate`` are rounded to 4 dp and clamped to
    ``[0, 1]``.  Returns 0.0 for all rates when ``total_expected_cells`` is
    0 so callers always get a well-formed dict.
    ``honest_coverage`` uses the canonical UAC formula
    (``compute_honest_coverage``): numerator = captured +
    (empty_confirmed - out_of_window) + expected_unattempted_known_empty. The
    ``out_of_window`` subset is populated upstream by
    ``compute_capture_status_counts`` so out-of-life empties are CLIPPED from
    both numerator and denominator here — consistent with coverage.py.
    """
    captured = counts.captured
    empty = counts.empty_confirmed
    failed = counts.attempted_failed
    attempted = captured + empty + failed
    denom = max(1, int(total_expected_cells))
    attempted_denom = max(1, attempted)
    return {
        "captured_count": captured,
        "empty_confirmed_count": empty,
        "attempted_failed_count": failed,
        "attempted_total": attempted,
        "honest_coverage": round(compute_honest_coverage(counts), 6),
        "attempt_coverage_pct": min(round(attempted / denom * 100, 2), 100.0) if total_expected_cells > 0 else 0.0,
        "capture_coverage_pct": min(round(captured / denom * 100, 2), 100.0) if total_expected_cells > 0 else 0.0,
        "empty_rate": max(0.0, min(1.0, round(empty / attempted_denom, 4))),
        "failure_rate": max(0.0, min(1.0, round(failed / attempted_denom, 4))),
    }


def build_failure_rate_by_dimension(
    venues_dict: dict[str, object],
) -> dict[str, dict[str, float | int]]:
    """Project the ``venues_dict`` into a {venue: {failure_rate, attempted_failed_count}} map.

    Only includes venues whose ``capture_status_counts.attempted_failed`` is
    strictly positive, so the UI can bind the "show only failures" filter
    without walking the full per-venue tree client-side.
    """
    out: dict[str, dict[str, float | int]] = {}
    for venue_name, venue_entry_raw in venues_dict.items():
        if not isinstance(venue_entry_raw, dict):
            continue
        venue_entry = cast(dict[str, object], venue_entry_raw)
        venue_failure_rate_raw = venue_entry.get("failure_rate", 0.0)
        venue_failure_rate = float(venue_failure_rate_raw) if isinstance(venue_failure_rate_raw, (int, float)) else 0.0
        v_counts_raw = venue_entry.get("capture_status_counts", {})  # noqa: qg-empty-fallback — pre-v9 rollup rows lack counts
        v_counts = cast(dict[str, int], v_counts_raw) if isinstance(v_counts_raw, dict) else {}
        failed_count = int(v_counts.get("attempted_failed", 0) or 0)
        if failed_count > 0:
            out[str(venue_name)] = {
                "failure_rate": venue_failure_rate,
                "attempted_failed_count": failed_count,
            }
    return out


def compute_attempt_coverage(
    filtered: pd.DataFrame,
    category: str,
) -> tuple[int, int]:
    """Return (attempt_found, attempt_expected) for an event-driven category.

    For PREDICTION (Polymarket), the attempt unit is a distinct ``underlying``
    in the filtered manifest — an underlying is "observed" if at least one
    shard exists for it in the date range. Since the availability manifest
    only contains rows that were actually captured, expected == found by
    definition: if we attempted a conditionId and it had zero trades, it
    never reaches the manifest.

    For SPORTS the attempt unit is ``(league_id, fixture_type)`` — fixtures
    only occur on match days, so (league, fixture_type) observation is the right
    attempt axis. When either column is missing we fall back to ``league_id``
    alone, then to ``(venue, instrument_type)``, then to ``venue`` so this
    is robust to manifest schema variation.

    Returns ``(0, 0)`` when no suitable attempt axis is found — callers must
    fall back to capture coverage in that case.
    """
    if filtered.empty:
        return 0, 0
    cat_upper = category.upper()
    if cat_upper == "PREDICTION":
        n = distinct_values(filtered, "underlying")
        return n, n
    if cat_upper == "SPORTS":
        n = sports_attempt_count(filtered)
        return n, n
    return 0, 0


def build_coverage_metrics(
    filtered: pd.DataFrame,
    category: str,
    capture_coverage_pct: float,
    total_expected_cells: int = 0,
) -> dict[str, object]:
    """Resolve the event-driven vs dense coverage metrics for one category.

    See ``COVERAGE_SEMANTICS`` for the per-category classification.

    Dense categories (CeFi / TradFi / DeFi): every underlying is expected to
    produce data every day, so ``attempt == capture`` and the existing
    shards-weighted ``capture_coverage_pct`` is the right displayed number.
    Event-driven categories (sports fixtures, Polymarket conditionIds): the
    shards-weighted ratio understates real coverage because the denominator
    assumes every (underlying x day) combo should have trades. We display
    attempt coverage and expose capture + empty-rate for the drill-down.

    Phase-C honest-coverage upgrade: when the manifest exposes a
    ``capture_status`` column (UTL v5) with any non-``captured`` rows, we
    also derive ``failure_rate`` + structured ``capture_status_counts`` from
    the column directly. The proxy path (distinct-underlying count) remains
    the default for event-driven categories whose adapters haven't been
    re-run post-Phase-B — that keeps the PREDICTION attempt number honest
    even before the Phase-B sentinel rows land.
    """
    coverage_semantics = COVERAGE_SEMANTICS.get(category.upper(), "dense")
    attempt_found, attempt_expected = compute_attempt_coverage(filtered, category)
    capture_counts = compute_capture_status_counts(filtered)
    has_phase_b_rows = capture_counts.empty_confirmed + capture_counts.attempted_failed > 0
    capture_rates = derive_capture_status_rates(capture_counts, total_expected_cells)

    if coverage_semantics == "event_driven" and attempt_expected > 0 and not has_phase_b_rows:
        attempt_coverage_pct = min(round(attempt_found / attempt_expected * 100, 2), 100.0)
        empty_rate_estimate: float | None = None
        if attempt_coverage_pct > 0:
            # empty_rate_estimate: fraction of attempted underlying-days that
            # had no trades. Clamped to [0, 1].
            empty_rate_estimate = max(
                0.0,
                min(1.0, round(1.0 - (capture_coverage_pct / attempt_coverage_pct), 4)),
            )
        completion_pct = attempt_coverage_pct
        failure_rate = float(capture_rates["failure_rate"])
    elif has_phase_b_rows and total_expected_cells > 0:
        # Phase B sentinel rows are present — prefer capture_status-derived
        # metrics over the distinct-underlying proxy. ``empty_rate_estimate``
        # becomes the concrete ``empty_rate`` (fraction of attempts returning
        # zero rows).
        attempt_coverage_pct = float(capture_rates["attempt_coverage_pct"])
        empty_rate_estimate = float(capture_rates["empty_rate"])
        completion_pct = attempt_coverage_pct
        failure_rate = float(capture_rates["failure_rate"])
    else:
        attempt_coverage_pct = capture_coverage_pct
        empty_rate_estimate = None
        completion_pct = capture_coverage_pct
        failure_rate = float(capture_rates["failure_rate"])
    # OOW split: count empty_confirmed rows that are never-collectable lifecycle
    # cells. These are excluded from the completion-% denominator by the
    # coverage.py layer (which calls compute_out_of_window_count directly).
    # Surfaced here so the manifest.py path also exposes out_of_window in its
    # capture_status_counts output. Read it off ``capture_counts`` (already
    # populated by compute_capture_status_counts) so the value the honest-
    # coverage clip used == the value displayed, with no second df walk.
    oow_count = capture_counts.out_of_window
    counts_dict = {
        "captured": capture_counts.captured,
        "empty_confirmed": capture_counts.empty_confirmed,
        "attempted_failed": capture_counts.attempted_failed,
        "expected_unattempted_known_empty": capture_counts.expected_unattempted_known_empty,
        "expected_unattempted_pending_fetch": capture_counts.expected_unattempted_pending_fetch,
        "out_of_window": oow_count,
    }
    return {
        "coverage_semantics": coverage_semantics,
        "capture_coverage_pct": capture_coverage_pct,
        "attempt_coverage_pct": attempt_coverage_pct,
        "empty_rate_estimate": empty_rate_estimate,
        "failure_rate": failure_rate,
        "completion_pct": completion_pct,
        "capture_status_counts": counts_dict,
        "counts": counts_dict,
        "coverage": float(capture_rates["honest_coverage"]),
    }


# Countries tracked for transfer window calendar (denominator for Transfermarkt data)

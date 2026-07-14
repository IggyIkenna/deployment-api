"""Pre-flight memory-cost guard for the manifest-status LIVE cell-grid build.

**Incident context** (2026-07-13 / 2026-07-14): ``uts-shared-deployment-api``
(a 4 GiB Cloud Run service) OOM-crash-looped repeatedly because
``ManifestStatusMixin._get_manifest_status_sync`` (``manifest.py``) falls
through to an on-demand, full in-memory cell-grid build whenever the
precomputed rollup blob is stale/missing. That live build was MEASURED to
use 18 GB (instruments-service, full history), 81 GB (market-tick-data-
service, full history), and 56 GB (market-data-processing-service, just 3
months) of RAM for a **single request** — instantly OOM-killing the whole
container (every OTHER open tab/endpoint sharing that worker pool, not just
the offending request). The default UI date range is full-history
(2018-01-01 to today), so one click is enough to trigger this.

This module estimates — CHEAPLY, from the request shape alone, no GCS reads
— what a live build would cost, so ``manifest.py`` can refuse it BEFORE
attempting it. Scope boundary: this does NOT make the live build itself
fast or memory-efficient (that re-architecture is deferred/scheduled
separately) — it only prevents a single request from taking down the whole
container. See :mod:`deployment_api.utils.bounded_subprocess` for the
second (defense-in-depth) layer that bounds whatever DOES get attempted.
"""

from __future__ import annotations

import datetime as dt
import logging
from dataclasses import dataclass

logger = logging.getLogger(__name__)

_MiB = 1024**2
_GiB = 1024**3


@dataclass(frozen=True)
class _CalibrationAnchor:
    """One measured ``(service, date-range days, category count) -> peak RSS`` point.

    Every anchor below built the service's FULL default category set (5 —
    CEFI/TRADFI/DEFI/SPORTS/PREDICTION) with no venue/row filter — the
    worst-case shape for that (service, days) pair. Measured 2026-07-13/14
    investigating the OOM-crash-loop (see module docstring).
    """

    service: str
    days: int
    categories: int
    measured_bytes: int

    @property
    def bytes_per_day_per_category(self) -> float:
        return self.measured_bytes / (self.days * self.categories)


# 2018-01-01 -> 2026-07-13 (the incident date) inclusive ~= 3120 days. Both
# full-history anchors below were measured over that span.
_FULL_HISTORY_DAYS = 3120

_CALIBRATION_ANCHORS: tuple[_CalibrationAnchor, ...] = (
    _CalibrationAnchor(service="instruments-service", days=_FULL_HISTORY_DAYS, categories=5, measured_bytes=18 * _GiB),
    _CalibrationAnchor(
        service="market-tick-data-service", days=_FULL_HISTORY_DAYS, categories=5, measured_bytes=81 * _GiB
    ),
    # Only a 3-month window, not full-history — MDPS's per-day-per-category
    # rate is ~25x market-tick-data-service's (larger derived cell grids per
    # day). This is the anchor that drives the conservative default rate
    # below for any service without its own measurement.
    _CalibrationAnchor(service="market-data-processing-service", days=90, categories=5, measured_bytes=56 * _GiB),
)

_RATE_BY_SERVICE: dict[str, float] = {a.service: a.bytes_per_day_per_category for a in _CALIBRATION_ANCHORS}

# Conservative fallback for any service NOT in the calibration table (incl.
# every features-*/ml-service/strategy-service/execution-service consumer of
# this same manifest code path) — use the WORST observed per-day-per-category
# rate rather than an average or the cheapest anchor. The whole point of this
# guard is to refuse too early rather than too late.
_DEFAULT_RATE_BYTES_PER_DAY_PER_CATEGORY: float = max(_RATE_BY_SERVICE.values())

# ── Narrowing discounts ──────────────────────────────────────────────────
#
# A venue filter (or a secondary-axis row filter — league_id / fixture_id /
# canonical_question_group / job_id / chain / pipeline_modes) narrows the
# CELL-GRID build cost, but manifest.py's ``_build_manifest_category`` loads
# the ENTIRE per-category availability index from GCS unconditionally
# (``self._read_defi_merged_index(...)``) and only applies the filter mask
# AFTER that load — so a narrowed request is cheaper, but not proportionally
# free. Both discounts are floored so a heavily-filtered request never scores
# as "free" purely on the strength of a filter, keeping the guard conservative.
_VENUE_REFERENCE_COUNT = 8  # documented conservative "typical many-venues" count
_VENUE_DISCOUNT_FLOOR = 0.4
_ROW_FILTER_DISCOUNT = 0.7  # any single secondary-axis filter present
_MIN_COMBINED_NARROWING_FACTOR = 0.35


def _rate_for_service(service: str) -> float:
    return _RATE_BY_SERVICE.get(service, _DEFAULT_RATE_BYTES_PER_DAY_PER_CATEGORY)


def _date_span_days(start_date: str, end_date: str) -> int:
    """Inclusive day count between two ISO date strings. Malformed input errs wide (full-history)."""
    try:
        start = dt.date.fromisoformat(start_date)
        end = dt.date.fromisoformat(end_date)
    except ValueError:
        logger.warning(
            "live_build_guard: could not parse date range %r..%r — treating as full-history for safety",
            start_date,
            end_date,
        )
        return _FULL_HISTORY_DAYS
    days = (end - start).days + 1
    return max(days, 1)


def _narrowing_factor(*, venue_count: int | None, has_row_filter: bool) -> float:
    """Bounded discount for a venue and/or secondary-axis row filter. See module docstring for rationale."""
    factor = 1.0
    if venue_count is not None and venue_count > 0:
        venue_factor = min(1.0, venue_count / _VENUE_REFERENCE_COUNT)
        factor *= max(venue_factor, _VENUE_DISCOUNT_FLOOR)
    if has_row_filter:
        factor *= _ROW_FILTER_DISCOUNT
    return max(factor, _MIN_COMBINED_NARROWING_FACTOR)


def estimate_live_build_bytes(
    *,
    service: str,
    start_date: str,
    end_date: str,
    category_count: int,
    venue_count: int | None = None,
    has_row_filter: bool = False,
) -> int:
    """Cheaply estimate the peak-RSS cost (bytes) of an on-demand live build.

    Pure function — no GCS/network calls — so it's safe to call on every
    request before deciding whether to attempt the live build at all.
    ``category_count`` is the number of asset-group categories the request
    would actually build (after service-restriction filtering — see
    ``ManifestStatusMixin._get_manifest_status_sync``'s ``cat_list``).
    Deliberately conservative: unknown services, unparsable date ranges, and
    heavily-narrowed requests all resolve toward a HIGHER estimate, not a
    lower one — erring toward refusing too early rather than too late.
    """
    days = _date_span_days(start_date, end_date)
    rate = _rate_for_service(service)
    narrowing = _narrowing_factor(venue_count=venue_count, has_row_filter=has_row_filter)
    estimate = rate * days * max(category_count, 1) * narrowing
    return int(estimate)


def would_exceed_budget(estimate_bytes: int, budget_bytes: int) -> bool:
    """True if a live build estimate exceeds the configured safe memory budget."""
    return estimate_bytes > budget_bytes

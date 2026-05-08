"""Honest-coverage Phase-C mock payload helpers.

Mock mode (``CLOUD_MOCK_MODE=true``) bypasses GCS reads. Prior to this
module the mock responses returned empty dicts — which meant the UI's
Category Breakdown card, heatmap, "Show only failures" toggle, and retry
button never rendered in local dev. That made the Phase-C UI flows
un-reviewable via Playwright without a live cloud backend.

These helpers synthesise realistic v5-shaped payloads (capture_status
counts, failure_rate, attempt vs capture metrics) so local dev + Playwright
audits can exercise the full Phase-C UI surface. All numbers are
deterministic per (service, start_date, end_date) so repeated runs render
identically.

See: unified-trading-pm/plans/active/honest_coverage_metrics_2026_04_19.md
"""

from __future__ import annotations

from typing import cast

from unified_api_contracts.internal import MarketCategory

# Representative venue lists per category — same shape as real manifest
# without pulling in the full venue registry. Order matters: the first
# venue in each list gets a non-zero failure_rate in the seed so the
# "Show only failures" filter and retry button have something to bite on.
_MOCK_VENUES_BY_ASSET_GROUP: dict[str, list[str]] = {
    "CEFI": ["BINANCE-SPOT", "OKX-SPOT", "COINBASE-SPOT"],
    "TRADFI": ["DATABENTO-DBEQ", "DATABENTO-GLBX"],
    "DEFI": ["UNISWAP_V3", "AAVE_V3"],
    "SPORTS": ["FOOTYSTATS", "SFI", "UNDERSTAT_XG"],
    "PREDICTION": ["POLYMARKET"],
}

# Per-category "coverage_semantics" — dense categories treat every (venue,
# date) pair as expected; event-driven ones only count observed pairs so
# the shards denominator shrinks dramatically.
_COVERAGE_SEMANTICS: dict[str, str] = {
    "CEFI": "dense",
    "TRADFI": "dense",
    "DEFI": "dense",
    "SPORTS": "event_driven",
    "PREDICTION": "event_driven",
}


def _mock_venue_entry(
    *,
    venue: str,
    dates_expected: int,
    captured: int,
    empty_confirmed: int,
    attempted_failed: int,
) -> dict[str, object]:
    """Build a single per-venue entry matching the real shape in
    ``DataStatusService._build_single_venue_entry``."""
    attempted = captured + empty_confirmed + attempted_failed
    denom = max(1, dates_expected)
    attempted_denom = max(1, attempted)
    return {
        "dates_found": captured,
        "dates_expected": dates_expected,
        "dates_expected_venue": dates_expected,
        "dates_missing": max(0, dates_expected - captured),
        "missing_dates": [],
        "dates_found_list": [],
        "dates_missing_list": [],
        "completion_pct": min(round(captured / denom * 100, 2), 100.0),
        "venue_start_date": "2024-01-01",
        "capture_status_counts": {
            "captured": captured,
            "empty_confirmed": empty_confirmed,
            "attempted_failed": attempted_failed,
        },
        "attempt_coverage_pct": min(round(attempted / denom * 100, 2), 100.0),
        "capture_coverage_pct": min(round(captured / denom * 100, 2), 100.0),
        "empty_rate": round(empty_confirmed / attempted_denom, 4),
        "failure_rate": round(attempted_failed / attempted_denom, 4),
    }


def _mock_category_entry(
    *,
    category: str,
    dates_expected: int,
    captured_total: int,
    empty_total: int,
    failed_total: int,
) -> dict[str, object]:
    """Build a category entry carrying every field the UI expects."""
    semantics = _COVERAGE_SEMANTICS.get(category, "dense")
    attempted_total = captured_total + empty_total + failed_total
    denom = max(1, dates_expected)
    attempted_denom = max(1, attempted_total)
    capture_pct = min(round(captured_total / denom * 100, 2), 100.0)
    attempt_pct = min(round(attempted_total / denom * 100, 2), 100.0)
    empty_rate = round(empty_total / attempted_denom, 4) if attempted_total else None
    failure_rate = round(failed_total / attempted_denom, 4)

    # Spread the counts across the category's venues. The first venue
    # soaks the failures so the "Show only failures" toggle has a clear
    # hit.
    venues = _MOCK_VENUES_BY_ASSET_GROUP.get(category, [])
    per_venue_expected = max(1, dates_expected // max(1, len(venues)))
    venues_dict: dict[str, object] = {}
    remaining_failed = failed_total
    remaining_empty = empty_total
    remaining_captured = captured_total
    failure_rate_by_dimension: dict[str, dict[str, float | int]] = {}
    for idx, v in enumerate(venues):
        # Give the first venue a failure share; evenly split the rest.
        v_failed = remaining_failed if idx == 0 else 0
        v_empty = remaining_empty // max(1, len(venues) - idx) if remaining_empty else 0
        v_captured = remaining_captured // max(1, len(venues) - idx) if remaining_captured else 0
        remaining_failed -= v_failed
        remaining_empty -= v_empty
        remaining_captured -= v_captured
        venues_dict[v] = _mock_venue_entry(
            venue=v,
            dates_expected=per_venue_expected,
            captured=v_captured,
            empty_confirmed=v_empty,
            attempted_failed=v_failed,
        )
        if v_failed > 0:
            failure_rate_by_dimension[v] = {
                "failure_rate": (round(v_failed / max(1, v_captured + v_empty + v_failed), 4)),
                "attempted_failed_count": int(v_failed),
            }

    completion_pct = attempt_pct if semantics == "event_driven" else capture_pct

    return {
        "asset_group": category,
        "bucket": f"mock-bucket-{category.lower()}",
        "prefixes_queried": 0,
        "dates_found": captured_total,
        "dates_expected": dates_expected,
        "dates_missing": max(0, dates_expected - captured_total),
        "shards_found": captured_total,
        "shards_expected": dates_expected,
        "completion_pct": completion_pct,
        "completion_pct_dates": completion_pct,
        "completion_pct_shards_weighted": capture_pct,
        "attempt_coverage_pct": attempt_pct,
        "capture_coverage_pct": capture_pct,
        "coverage_semantics": semantics,
        "empty_rate_estimate": empty_rate,
        "failure_rate": failure_rate,
        "capture_status_counts": {
            "captured": captured_total,
            "empty_confirmed": empty_total,
            "attempted_failed": failed_total,
        },
        "venue_weighted": True,
        "venue_dates_found": captured_total,
        "venue_dates_expected": dates_expected,
        "unit": "fixtures" if category == "SPORTS" else "dates",
        "effective_start_date": "2024-01-01",
        "missing_dates": [],
        "dates_found_list": [],
        "dates_missing_list": [],
        "venues": venues_dict,
        "failure_rate_by_dimension": failure_rate_by_dimension,
    }


def build_mock_turbo_response(
    service: str,
    start_date: str,
    end_date: str,
    asset_groups: list[str] | None = None,
) -> dict[str, object]:
    """Return a turbo-shaped response with realistic v5 capture_status
    data across every relevant category.

    Category selection matches the ``_SERVICE_CATEGORY_RESTRICTIONS`` logic
    — instruments-service / MTDS / features-calendar get all 5 categories;
    MDPS + features-* narrow to their supported subsets.

    Seeding is deterministic: PREDICTION has high attempt but low capture
    (event-driven), CEFI/TRADFI/DEFI are dense (~99% capture), SPORTS shows
    the hybrid pattern. Every category gets at least one attempted_failed
    row in its first venue so the "Show only failures" toggle + retry
    button are exercised.
    """
    # Per-service category restrictions mirror the real service
    service_restrictions: dict[str, frozenset[str]] = {
        "market-data-processing-service": frozenset({"CEFI", "TRADFI", "DEFI"}),
        "features-delta-one-service": frozenset({"CEFI", "TRADFI", "DEFI"}),
        "features-volatility-service": frozenset({"CEFI", "TRADFI", "DEFI"}),
        "features-multi-timeframe-service": frozenset({"CEFI", "TRADFI", "DEFI"}),
        "features-cross-instrument-service": frozenset({"CEFI", "TRADFI", "DEFI"}),
        "features-onchain-service": frozenset({"DEFI"}),
        "features-sports-service": frozenset({"SPORTS"}),
        "features-commodity-service": frozenset({"TRADFI"}),
        "strategy-service": frozenset({"CEFI", "TRADFI", "DEFI"}),
        "execution-service": frozenset({"CEFI", "TRADFI", "DEFI"}),
    }
    allowed = service_restrictions.get(service)
    all_cats = [str(c) for c in MarketCategory]
    cat_list = [c for c in all_cats if (not allowed or c in allowed)]
    if asset_groups:
        wanted = {c.upper() for c in asset_groups}
        cat_list = [c for c in cat_list if c in wanted]

    # Deterministic per-category seed
    seeds: dict[str, dict[str, int]] = {
        "CEFI": {
            "dates_expected": 120,
            "captured": 118,
            "empty": 1,
            "failed": 1,
        },
        "TRADFI": {
            "dates_expected": 100,
            "captured": 97,
            "empty": 2,
            "failed": 1,
        },
        "DEFI": {
            "dates_expected": 110,
            "captured": 102,
            "empty": 5,
            "failed": 3,
        },
        "SPORTS": {
            "dates_expected": 300,
            "captured": 260,
            "empty": 30,
            "failed": 10,
        },
        "PREDICTION": {
            "dates_expected": 800,
            "captured": 185,
            "empty": 550,
            "failed": 65,
        },
    }

    result_categories: dict[str, object] = {}
    total_captured = 0
    total_expected = 0
    for cat in cat_list:
        seed = seeds.get(cat)
        if not seed:
            continue
        entry = _mock_category_entry(
            category=cat,
            dates_expected=int(seed["dates_expected"]),
            captured_total=int(seed["captured"]),
            empty_total=int(seed["empty"]),
            failed_total=int(seed["failed"]),
        )
        result_categories[cat] = entry
        total_captured += int(seed["captured"])
        total_expected += int(seed["dates_expected"])

    overall_pct = (
        min(round(total_captured / max(1, total_expected) * 100, 2), 100.0)
        if total_expected > 0
        else 0.0
    )
    return {
        "service": service,
        "date_range": {"start": start_date, "end": end_date, "days": 30},
        "mode": "turbo",
        "sub_dimension": "venue",
        "overall_completion_pct": overall_pct,
        "overall_completion_pct_dates": overall_pct,
        "overall_completion_pct_shards_weighted": overall_pct,
        "overall_dates_found": total_captured,
        "overall_dates_expected": total_expected,
        "overall_shards_found": total_captured,
        "overall_shards_expected": total_expected,
        "total_missing": max(0, total_expected - total_captured),
        "migration_in_progress": False,
        "asset_groups": result_categories,
        "mock": True,
    }


def build_mock_shard_instruments(
    service: str,
    category: str,
    venue: str,
    day: str,
    instrument_type: str,
    data_type: str,
) -> dict[str, object]:
    """Return a realistic instruments-for-shard response with a mix of
    capture_status values so the UI drill-down can render badges + retry
    buttons."""
    # One captured, one empty_confirmed, one attempted_failed — so the UI
    # has every badge colour on screen.
    instruments: list[dict[str, object]] = [
        {
            "instrument_id": f"{venue}-CAPTURED-1",
            "file_uri": f"gs://mock/{venue}/{day}/CAPTURED-1.parquet",  # noqa: gs-uri — mock fixture URI
            "size_bytes": 1024,
            "capture_status": "captured",
            "error_reason": "",
            "attempted_at": f"{day}T00:10:00+00:00",
        },
        {
            "instrument_id": f"{venue}-EMPTY-1",
            "file_uri": f"gs://mock/{venue}/{day}/EMPTY-1.parquet",  # noqa: gs-uri — mock fixture URI
            "size_bytes": 0,
            "capture_status": "empty_confirmed",
            "error_reason": "",
            "attempted_at": f"{day}T00:12:00+00:00",
        },
        {
            "instrument_id": f"{venue}-FAILED-1",
            "file_uri": f"gs://mock/{venue}/{day}/FAILED-1.parquet",  # noqa: gs-uri — mock fixture URI
            "size_bytes": 0,
            "capture_status": "attempted_failed",
            "error_reason": "RATE_LIMIT_HIT",
            "attempted_at": f"{day}T00:14:00+00:00",
        },
    ]
    return cast(
        dict[str, object],
        {
            "service": service,
            "category": category.lower(),
            "venue": venue,
            "day": day,
            "instrument_type": instrument_type,
            "data_type": data_type,
            "bundling": "per_symbol",
            "instruments": instruments,
            "bucket": f"mock-bucket-{category.lower()}",
            "prefix": (
                f"raw_tick_data/by_date/day={day}/category={category.lower()}/"
                f"venue={venue}/instrument_type={instrument_type}/"
                f"data_type={data_type}/"
            ),
            "total_count": len(instruments),
            "limit": 50,
            "offset": 0,
            "has_more": False,
            "search": "",
            "mock": True,
        },
    )

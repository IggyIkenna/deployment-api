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

from deployment_api.services.data_status_hierarchical import DrilldownNode, list_supported_pairs

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
    asset_group: str,
    dates_expected: int,
    captured_total: int,
    empty_total: int,
    failed_total: int,
) -> dict[str, object]:
    """Build a category entry carrying every field the UI expects."""
    semantics = _COVERAGE_SEMANTICS.get(asset_group, "dense")
    attempted_total = captured_total + empty_total + failed_total
    denom = max(1, dates_expected)
    attempted_denom = max(1, attempted_total)
    capture_pct = min(round(captured_total / denom * 100, 2), 100.0)
    attempt_pct = min(round(attempted_total / denom * 100, 2), 100.0)
    empty_rate = round(empty_total / attempted_denom, 4) if attempted_total else None
    failure_rate = round(failed_total / attempted_denom, 4)

    # Spread the counts across the asset_group's venues. The first venue
    # soaks the failures so the "Show only failures" toggle has a clear
    # hit.
    venues = _MOCK_VENUES_BY_ASSET_GROUP.get(asset_group, [])
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
        "asset_group": asset_group,
        "bucket": f"mock-bucket-{asset_group.lower()}",
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
        "unit": "fixtures" if asset_group == "SPORTS" else "dates",
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
        # DEFI removed (UAC cloud-providers.yaml DEFI key removed 2026-07-17,
        # asset-group parity sweep — mirrors data_status/defi.py).
        "features-volatility-service": frozenset({"CEFI", "TRADFI"}),
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
            asset_group=cat,
            dates_expected=int(seed["dates_expected"]),
            captured_total=int(seed["captured"]),
            empty_total=int(seed["empty"]),
            failed_total=int(seed["failed"]),
        )
        result_categories[cat] = entry
        total_captured += int(seed["captured"])
        total_expected += int(seed["dates_expected"])

    overall_pct = min(round(total_captured / max(1, total_expected) * 100, 2), 100.0) if total_expected > 0 else 0.0
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
    asset_group: str,
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
            "asset_group": asset_group.lower(),
            "venue": venue,
            "day": day,
            "instrument_type": instrument_type,
            "data_type": data_type,
            "bundling": "per_symbol",
            "instruments": instruments,
            "bucket": f"mock-bucket-{asset_group.lower()}",
            "prefix": (
                f"raw_tick_data/by_date/day={day}/asset_group={asset_group.lower()}/"
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


# ---------------------------------------------------------------------------
# Cherry-pick D (2026-07-20): rich /coverage-summary mock inventory + a
# non-empty /drilldown mock tree.
#
# Pre-2026-07-20, GET /coverage-summary in mock mode returned an all-zero
# inventory because ``routes/data_status/_status_core.get_coverage_summary``
# is registered FIRST on the shared router (FastAPI first-registration-wins
# on the duplicate ``/coverage-summary`` path — see the import-order comment
# in ``routes/data_status/__init__.py``) with a zero mock, while a RICH
# per-asset_group mock already existed in the unreachable duplicate
# ``routes/data_status/_deploy_turbo.get_data_coverage_summary``. Rather than
# reorder routes (which the ``__init__.py`` comment explicitly forbids), the
# rich numbers are ported here and wired into the WINNING handler's mock
# branch. Same story for the drilldown mock: it returned a hardcoded empty
# ``tree: []`` — replaced with a small tree synthesised from the SSOT
# ``SHARD_AXIS_MATRIX`` via the real ``DrilldownNode`` dataclass, so the mock
# payload always matches the production node schema.
# ---------------------------------------------------------------------------

# Per-category seed, ported byte-for-byte (shard counts / unique dates /
# venues / latest-day breakdowns) from the pre-2026-07-20 unreachable
# ``_deploy_turbo.get_data_coverage_summary`` mock branch — only the
# per-category *shape* below changed, to match the REAL response schema
# (``CoverageStatusMixin._build_coverage_for_cat`` / ``_get_coverage_summary_sync``)
# instead of the sibling duplicate route's now-dead schema.
_MOCK_COVERAGE_SEED: dict[str, dict[str, object]] = {
    "CEFI": {
        "total_shards": 25000,
        "unique_dates": 2200,
        "unique_venues": 18,
        "date_range": {"start": "2019-01-01", "end": "2026-04-03"},
        "latest_day": "2026-04-03",
        "latest_day_instruments": {"SPOT": 1200, "PERPETUAL": 800, "FUTURE": 600, "OPTION": 1271},
        "latest_day_total": 3871,
        "group_axis": "instrument_type",
        "empty": 12,
        "failed": 5,
    },
    "TRADFI": {
        "total_shards": 40000,
        "unique_dates": 1800,
        "unique_venues": 6,
        "date_range": {"start": "2019-01-01", "end": "2026-04-03"},
        "latest_day": "2026-04-03",
        "latest_day_instruments": {"EQUITY": 8000, "ETF": 3500, "INDEX": 200, "OPTION": 2500, "FUTURE": 514},
        "latest_day_total": 14714,
        "group_axis": "instrument_type",
        "empty": 20,
        "failed": 8,
    },
    "DEFI": {
        "total_shards": 10000,
        "unique_dates": 250,
        "unique_venues": 35,
        "date_range": {"start": "2020-01-20", "end": "2026-04-03"},
        "latest_day": "2026-04-03",
        "latest_day_instruments": {"LP_POOL": 1200, "LENDING_POOL": 622},
        "latest_day_total": 1822,
        "group_axis": "venue",
        "empty": 40,
        "failed": 3,
    },
    "SPORTS": {
        "total_shards": 3800,
        "unique_dates": 36,
        "unique_venues": 3,
        "date_range": {"start": "2026-03-01", "end": "2026-04-03"},
        "latest_day": "2026-04-03",
        "latest_day_instruments": {"FIXTURE": 450},
        "latest_day_total": 450,
        "group_axis": "data_type",
        "empty": 15,
        "failed": 2,
    },
}


def build_mock_coverage_summary(
    service: str,
    asset_groups: list[str] | None = None,
) -> dict[str, object]:
    """Realistic per-asset_group ``/coverage-summary`` mock.

    Matches the REAL response schema (``CoverageStatusMixin.
    _get_coverage_summary_sync``): ``asset_groups`` keyed by category, each
    entry carrying ``total_shards`` / ``capture_status_counts`` (5-state) /
    ``completion_pct`` / ... — NOT the sibling ``_deploy_turbo`` duplicate's
    dead ``categories`` schema. ``asset_groups`` param (comma-split by the
    caller) narrows the categories exactly like the real path's ``ag_list``.
    """
    wanted = {c.upper() for c in asset_groups} if asset_groups else None
    result_categories: dict[str, object] = {}
    total_shards = 0
    total_instrument_rows = 0
    all_dates: set[str] = set()
    total_latest_day_instruments = 0
    total_capture_status: dict[str, int] = {
        "captured": 0,
        "empty_confirmed": 0,
        "attempted_failed": 0,
        "expected_unattempted": 0,
        "out_of_window": 0,
    }
    for cat, seed in _MOCK_COVERAGE_SEED.items():
        if wanted is not None and cat not in wanted:
            continue
        shards = int(cast(int, seed["total_shards"]))
        failed = int(cast(int, seed["failed"]))
        empty = int(cast(int, seed["empty"]))
        captured = max(0, shards - failed - empty)
        capture_status_counts: dict[str, int] = {
            "captured": captured,
            "empty_confirmed": empty,
            "attempted_failed": failed,
            "expected_unattempted": 0,
            "out_of_window": 0,
        }
        completion_pct = round(captured / max(1, shards) * 100, 2)
        date_range = cast(dict[str, str], seed["date_range"])
        entry: dict[str, object] = {
            "total_shards": shards,
            "total_instrument_rows": shards,
            "total_instruments": shards,
            "unique_instruments": None,
            "unique_dates": seed["unique_dates"],
            "unique_venues": seed["unique_venues"],
            "group_axis": seed["group_axis"],
            "date_range": date_range,
            "latest_day": seed["latest_day"],
            "latest_day_instruments": seed["latest_day_instruments"],
            "latest_day_total": seed["latest_day_total"],
            "breakdowns": {},
            "capture_status_counts": capture_status_counts,
            "completion_pct": completion_pct,
        }
        result_categories[cat] = entry
        total_shards += shards
        total_instrument_rows += shards
        total_latest_day_instruments += int(cast(int, seed["latest_day_total"]))
        all_dates.add(str(date_range["start"]))
        all_dates.add(str(date_range["end"]))
        for key in total_capture_status:
            total_capture_status[key] += capture_status_counts[key]

    cov_total = (
        total_capture_status["captured"]
        + total_capture_status["empty_confirmed"]
        + total_capture_status["attempted_failed"]
        + total_capture_status["expected_unattempted"]
    )
    overall_pct = round(total_capture_status["captured"] / cov_total * 100, 2) if cov_total > 0 else 0.0
    return {
        "service": service,
        "asset_groups": result_categories,
        "totals": {
            "shards": total_shards,
            "instrument_rows": total_instrument_rows,
            "dates_across_categories": len(all_dates),
            "latest_day_instruments": total_latest_day_instruments,
            "unique_instruments": 0,
            "capture_status_counts": total_capture_status,
            "completion_pct": overall_pct,
        },
        "totals_source": "mock",
        "mock": True,
    }


def build_mock_drilldown_tree(service: str, asset_group: str) -> dict[str, object]:
    """Synthesize a small, realistic 2-level hierarchical drilldown tree.

    Pre-2026-07-20 the mock branch of ``GET /drilldown/{service}/{asset_group}``
    returned a hardcoded empty ``tree: []`` + all-zero totals, leaving the
    deployment-ui ``HierarchicalShardDrilldown`` component unreviewable via
    Playwright without a live cloud backend. Built from the SAME
    ``DrilldownNode`` dataclass + ``to_dict()`` the real
    ``get_hierarchical_drilldown`` builder uses (via the public
    ``list_supported_pairs`` SSOT lookup) so the mock payload's node shape
    never drifts from the production schema.

    ``head_axis`` is the SSOT-declared top axis for ``(service, asset_group)``
    (``SHARD_AXIS_MATRIX`` via ``list_supported_pairs``); falls back to
    ``venue`` for an undeclared pair — mirrors
    ``data_status_hierarchical._resolve_axis_order``'s fallback. The 2x2 seed
    (2 top-axis values x 2 dates) puts one ``attempted_failed`` leaf and one
    ``empty_confirmed`` leaf under the second top value — everything else
    ``captured`` — so the 4-state heatmap + "show only failures" toggle have
    real, non-zero seed data to render.
    """
    match = next(
        (p for p in list_supported_pairs() if p["service"] == service and p["asset_group"] == asset_group),
        None,
    )
    axes_list: list[str] = cast(list[str], match["axes"]) if match else ["venue", "date"]
    head_axis = str(axes_list[0]) if axes_list else "venue"

    top_values = [f"MOCK_{head_axis.upper()}_A", f"MOCK_{head_axis.upper()}_B"]
    dates = ["2026-01-01", "2026-01-02"]

    top_nodes: list[DrilldownNode] = []
    for top_idx, top_val in enumerate(top_values):
        leaves: list[DrilldownNode] = []
        for day_idx, day in enumerate(dates):
            row_key = {head_axis: top_val, "date": day}
            if top_idx == 1 and day_idx == 0:
                leaves.append(DrilldownNode(axis="date", value=day, attempted_failed=1, row_key=row_key))
            elif top_idx == 1 and day_idx == 1:
                leaves.append(DrilldownNode(axis="date", value=day, empty_confirmed=1, row_key=row_key))
            else:
                leaves.append(DrilldownNode(axis="date", value=day, captured=1, row_key=row_key))
        top_nodes.append(
            DrilldownNode(
                axis=head_axis,
                value=top_val,
                captured=sum(leaf.captured for leaf in leaves),
                empty_confirmed=sum(leaf.empty_confirmed for leaf in leaves),
                attempted_failed=sum(leaf.attempted_failed for leaf in leaves),
                children=leaves,
                row_key={head_axis: top_val},
            )
        )

    total_captured = sum(n.captured for n in top_nodes)
    total_empty = sum(n.empty_confirmed for n in top_nodes)
    total_failed = sum(n.attempted_failed for n in top_nodes)
    total_all = total_captured + total_empty + total_failed
    completion_pct = round((total_captured + total_empty) / total_all * 100, 2) if total_all > 0 else 0.0

    return {
        "service": service,
        "asset_group": asset_group,
        "axes": [head_axis, "date"],
        "tree": [n.to_dict() for n in top_nodes],
        "totals": {
            "captured": total_captured,
            "empty_confirmed": total_empty,
            "attempted_failed": total_failed,
            "expected_unattempted": 0,
            "total": total_all,
            "completion_pct": completion_pct,
        },
        "filtered_by": {},
        "mock": True,
    }

"""Pre-computed data-status rollup readers + window slicing helpers.

Split out of the 6,663-line ``data_status_service.py`` god-module
(codex ratchet plan 2026-06-10). The facade module re-exports every
public + legacy-underscore name, so callers keep importing from
``deployment_api.services.data_status_service``.
"""

import json
import logging
import time
from typing import cast

import pandas as pd
from unified_api_contracts.registry import (
    DEPRECATED_DEFI_GHOST_VENUE_NAMES,
)

from deployment_api.settings import gcp_project_id as _pid

logger = logging.getLogger(__name__)

ALL_DEFI_GHOST_VENUES: frozenset[str] = DEPRECATED_DEFI_GHOST_VENUE_NAMES

# Infrastructure/oracle entries that appear in DeFi sub-buckets but are NOT
# DeFi protocols — they pollute the chain venue breakdown. Checked on the
# prefix (before the first "-") so both "ALCHEMY" and "ALCHEMY-ETHEREUM" match.
DEFI_NON_PROTOCOL_VENUE_PREFIXES: frozenset[str] = frozenset(
    {
        "COINBASE",  # COINBASE-SPOT — CeFi oracle source leaking from oracle-prices bucket
        "ALCHEMY",  # gas-fee RPC provider — data tracked in gas-fees sub-bucket
        "ANKR",  # LST staking RPC provider — data tracked in lst-rates sub-bucket
        "GAS_FEES",  # data_type string appearing as venue (defensive)
    }
)

ROLLUP_BUCKET_TEMPLATE: str = "{pid}-data-status-rollups"
ROLLUP_STALENESS_SEC: int = 1800  # 30 min — cron fires every 5; 30 covers 6 missed cycles
ROLLUP_CACHE: dict[str, tuple[float, dict[str, object]]] = {}
ROLLUP_CACHE_TTL_SEC: int = 1800  # in-process re-read TTL — match the GCS-staleness window above.
# Rationale: the rollup worker (Cloud Run Job) fires every 5 min, so any rollup
# we hold in-process is at most 5 min behind canonical. A 60s TTL was the
# initial conservative pick, but it forces a fresh transpacific 9-19 MB GCS
# round-trip every time the UI is idle for >60s — which is the common case
# (the user clicks Data Status, reads, comes back ~minutes later). Bumping to
# 1800s means a warm UI session never re-downloads the rollup; correctness is
# unchanged because data_status_service still falls through to on-demand if the
# blob mtime is older than ROLLUP_STALENESS_SEC.


def rollup_bucket() -> str:
    return ROLLUP_BUCKET_TEMPLATE.format(pid=_pid)


def filter_dates_in_window(dates: list[str] | None, start_date: str, end_date: str) -> list[str]:
    if not dates:
        return []
    return [d for d in dates if start_date <= d <= end_date]


def strip_defi_ghost_venues(cat_payload: dict[str, object]) -> dict[str, object]:
    """Remove era-2 no-underscore venue names from a DEFI asset-group payload."""
    if not ALL_DEFI_GHOST_VENUES:
        return cat_payload

    def _excluded(v: str) -> bool:
        prefix = v.split("-", 1)[0]
        return (
            v in ALL_DEFI_GHOST_VENUES or prefix in ALL_DEFI_GHOST_VENUES or prefix in DEFI_NON_PROTOCOL_VENUE_PREFIXES
        )

    venues = cat_payload.get("venues")
    if isinstance(venues, dict):
        clean = {v: p for v, p in venues.items() if not _excluded(v)}  # pyright: ignore[reportUnknownVariableType,reportUnknownArgumentType]
        if len(clean) < len(venues):  # pyright: ignore[reportUnknownArgumentType]
            cat_payload = {**cat_payload, "venues": clean}
    chains_data = cat_payload.get("chains")
    if not isinstance(chains_data, dict):
        return cat_payload
    cleaned: dict[str, object] = {}
    for chain_name, chain_data in chains_data.items():  # pyright: ignore[reportUnknownVariableType]
        if isinstance(chain_data, dict):
            chain_venues = chain_data.get("venues")  # pyright: ignore[reportUnknownMemberType,reportUnknownVariableType]
            if isinstance(chain_venues, list):
                cv = [v for v in chain_venues if not _excluded(v)]  # pyright: ignore[reportUnknownVariableType,reportUnknownArgumentType]
                chain_data = {**chain_data, "venues": cv, "venue_count": len(cv)}  # pyright: ignore[reportUnknownVariableType,reportUnknownArgumentType]
            elif isinstance(chain_venues, dict):
                cv2 = {v: p for v, p in chain_venues.items() if not _excluded(v)}  # pyright: ignore[reportUnknownVariableType,reportUnknownArgumentType]
                chain_data = {**chain_data, "venues": cv2, "venue_count": len(cv2)}  # pyright: ignore[reportUnknownVariableType,reportUnknownArgumentType]
        cleaned[chain_name] = chain_data
    return {**cat_payload, "chains": cleaned}


def slice_rollup_to_window(
    rollup: dict[str, object],
    start_date: str,
    end_date: str,
    asset_groups_filter: list[str] | None,
) -> dict[str, object]:
    """Return a windowed subset of a full-range rollup.

    The rollup payload was computed with ``start_date="2018-01-01",
    end_date=today``. The slicer:
      1. Drops asset_groups not in the user's filter (if any).
      2. For each asset_group → venue → data_type, filters
         ``missing_dates``, ``dates_found_list``, and per-instrument date
         lists to the requested window.
      3. Recomputes per-(asset_group, venue, data_type) counts.
      4. Recomputes the overall totals.

    Behaviour parity with the on-demand path: same response shape, same
    ``mode="turbo"``, same percentages — only the date arrays + counts shrink
    to the requested window.
    """
    overall_found = 0
    overall_expected = 0
    overall_shards_found = 0
    overall_shards_expected = 0

    asset_groups = rollup.get("asset_groups")
    if not isinstance(asset_groups, dict):
        # Malformed rollup — surface loud rather than slice garbage.
        raise RuntimeError(f"rollup payload missing 'asset_groups' dict (got {type(asset_groups).__name__})")

    sliced_asset_groups: dict[str, object] = {}
    filter_set = {ag.upper() for ag in asset_groups_filter} if asset_groups_filter else None
    for cat, cat_payload in asset_groups.items():  # pyright: ignore[reportUnknownVariableType]
        if filter_set is not None and cat.upper() not in filter_set:  # pyright: ignore[reportUnknownMemberType]
            continue
        if not isinstance(cat_payload, dict):
            sliced_asset_groups[cat] = cat_payload  # pass-through unknown shapes
            continue
        if cat.lower() == "defi":  # pyright: ignore[reportUnknownMemberType]
            cat_payload = strip_defi_ghost_venues(cat_payload)  # pyright: ignore[reportUnknownArgumentType]
        sliced_cat = slice_asset_group(cat_payload, start_date, end_date)  # pyright: ignore[reportUnknownArgumentType]
        sliced_asset_groups[cat] = sliced_cat
        overall_found += int(cast(int, sliced_cat.get("dates_found", 0)))
        overall_expected += int(cast(int, sliced_cat.get("dates_expected", 0)))
        overall_shards_found += int(cast(int, sliced_cat.get("_venue_found_sliced", 0)))
        overall_shards_expected += int(cast(int, sliced_cat.get("_venue_expected_sliced", 0)))
        sliced_cat.pop("_venue_found_sliced", None)
        sliced_cat.pop("_venue_expected_sliced", None)

    overall_pct_dates = (
        min(round(overall_found / max(1, overall_expected) * 100, 2), 100.0) if overall_expected > 0 else 0.0
    )
    overall_pct_shards = (
        min(round(overall_shards_found / overall_shards_expected * 100, 2), 100.0)
        if overall_shards_expected > 0
        else overall_pct_dates
    )

    total_days = (pd.Timestamp(end_date) - pd.Timestamp(start_date)).days + 1

    return {
        "service": rollup.get("service"),
        "date_range": {"start": start_date, "end": end_date, "days": total_days},
        "mode": "turbo",
        "sub_dimension": rollup.get("sub_dimension", "venue"),
        "overall_completion_pct": overall_pct_shards,
        "overall_completion_pct_dates": overall_pct_dates,
        "overall_completion_pct_shards_weighted": overall_pct_shards,
        "overall_dates_found": overall_found,
        "overall_dates_expected": overall_expected,
        "overall_shards_found": overall_shards_found,
        "overall_shards_expected": overall_shards_expected,
        "migration_in_progress": rollup.get("migration_in_progress", False),
        "asset_groups": sliced_asset_groups,
        "served_from": "rollup",
    }


def slice_asset_group(cat_payload: dict[str, object], start_date: str, end_date: str) -> dict[str, object]:
    """Slice one asset_group's payload to the date window. See slice_rollup_to_window."""
    sliced: dict[str, object] = dict(cat_payload)
    venue_found_total = 0
    venue_expected_total = 0
    cat_found_dates: set[str] = set()

    venues_in = cat_payload.get("venues")
    if isinstance(venues_in, dict):
        sliced_venues: dict[str, object] = {}
        for venue, venue_payload in venues_in.items():  # pyright: ignore[reportUnknownVariableType]
            if not isinstance(venue_payload, dict):
                sliced_venues[venue] = venue_payload
                continue
            sv = slice_venue(venue_payload, start_date, end_date)  # pyright: ignore[reportUnknownArgumentType]
            sliced_venues[venue] = sv
            venue_found_total += int(cast(int, sv.get("dates_found", 0)))
            venue_expected_total += int(cast(int, sv.get("dates_expected", 0)))
            for d in sv.get("dates_found_list", []) or []:  # pyright: ignore[reportGeneralTypeIssues,reportUnknownVariableType]
                cat_found_dates.add(str(d))  # pyright: ignore[reportUnknownArgumentType]
        sliced["venues"] = sliced_venues

    cat_total_days = (pd.Timestamp(end_date) - pd.Timestamp(start_date)).days + 1
    cat_found = len(cat_found_dates)
    cat_pct_dates = min(round(cat_found / max(1, cat_total_days) * 100, 2), 100.0)
    cat_pct_shards = (
        min(round(venue_found_total / venue_expected_total * 100, 2), 100.0)
        if venue_expected_total > 0
        else cat_pct_dates
    )

    sliced["dates_found"] = cat_found
    sliced["dates_expected"] = cat_total_days
    sliced["dates_missing"] = max(0, cat_total_days - cat_found)
    sliced["completion_pct"] = cat_pct_shards
    sliced["completion_pct_dates"] = cat_pct_dates
    # Internal counters drained by the parent — popped before returning to the user.
    sliced["_venue_found_sliced"] = venue_found_total
    sliced["_venue_expected_sliced"] = venue_expected_total
    return sliced


def slice_venue(venue_payload: dict[str, object], start_date: str, end_date: str) -> dict[str, object]:
    """Slice one venue's payload to the date window. Recursive into per-data_type if present."""
    sliced: dict[str, object] = dict(venue_payload)

    found = filter_dates_in_window(cast(list[str] | None, venue_payload.get("dates_found_list")), start_date, end_date)
    missing = filter_dates_in_window(cast(list[str] | None, venue_payload.get("missing_dates")), start_date, end_date)
    expected_dates_full = cast(list[str] | None, venue_payload.get("dates_expected_list")) or (found + missing)
    expected = filter_dates_in_window(expected_dates_full, start_date, end_date)

    sliced["dates_found_list"] = found
    sliced["missing_dates"] = missing
    sliced["dates_found"] = len(found)
    sliced["dates_expected"] = len(expected) if expected else (len(found) + len(missing))
    sliced["dates_missing"] = sliced["dates_expected"] - sliced["dates_found"]
    if sliced["dates_expected"] > 0:
        sliced["completion_pct"] = min(round(sliced["dates_found"] / sliced["dates_expected"] * 100, 2), 100.0)
    else:
        sliced["completion_pct"] = 0.0

    # Per-data_type breakdown (MTDS honest-coverage shape).
    honest_dts = venue_payload.get("honest_data_types")
    if isinstance(honest_dts, dict):
        sliced["honest_data_types"] = {
            dt: slice_venue(dt_payload, start_date, end_date) if isinstance(dt_payload, dict) else dt_payload  # pyright: ignore[reportUnknownArgumentType]
            for dt, dt_payload in honest_dts.items()  # pyright: ignore[reportUnknownVariableType]
        }

    return sliced


def read_coverage_rollup_if_fresh(service: str) -> dict[str, object] | None:
    """Read ``gs://{pid}-data-status-rollups/{service}/coverage.json.gz``.

    Companion to :func:`_read_rollup_if_fresh` for the
    ``/api/data-status/coverage-summary`` endpoint. Same staleness threshold,
    same in-process cache, just a different blob path.
    """
    cache_key = f"coverage:{service}"
    cached = ROLLUP_CACHE.get(cache_key)
    now = time.monotonic()
    if cached is not None and (now - cached[0]) < ROLLUP_CACHE_TTL_SEC:
        return cached[1]

    try:
        from unified_trading_library import get_storage_client

        client = get_storage_client(project_id=_pid)
        bucket_name = rollup_bucket()
        blob_path = f"{service}/coverage.json.gz"
        if not client.blob_exists(bucket_name, blob_path):  # pyright: ignore[reportAttributeAccessIssue]
            return None
        meta = client.get_blob_metadata(bucket_name, blob_path)  # pyright: ignore[reportAttributeAccessIssue]
        if meta is not None and getattr(meta, "updated", None) is not None:
            age_sec = (pd.Timestamp.now(tz="UTC") - pd.Timestamp(meta.updated)).total_seconds()  # type: ignore[reportAttributeAccessIssue, reportUnknownArgumentType]
            if age_sec > ROLLUP_STALENESS_SEC:
                logger.info(
                    "coverage rollup for %s is stale (%.0fs > %ds threshold) — falling through",
                    service,
                    age_sec,
                    ROLLUP_STALENESS_SEC,
                )
                return None
        raw = client.download_bytes(bucket_name, blob_path)  # pyright: ignore[reportAttributeAccessIssue]
        import gzip

        payload_bytes = gzip.decompress(raw) if raw[:2] == b"\x1f\x8b" else raw
        payload = json.loads(payload_bytes.decode("utf-8"))  # pyright: ignore[reportAny]
        if not isinstance(payload, dict):
            logger.warning("coverage rollup for %s is not a dict — ignoring", service)
            return None
        ROLLUP_CACHE[cache_key] = (now, payload)
        return payload  # pyright: ignore[reportUnknownVariableType]
    except (OSError, ValueError, RuntimeError, json.JSONDecodeError) as exc:
        logger.info(
            "coverage rollup read failed for %s (%s) — falling through to on-demand",
            service,
            exc,
        )
        return None


def filter_coverage_to_asset_groups(
    rollup: dict[str, object], asset_groups_filter: list[str] | None
) -> dict[str, object]:
    """Filter a coverage-summary rollup to the requested asset_groups.

    Coverage-summary has no date axis — the rollup IS the answer for any
    request. We just trim ``asset_groups`` to the user's filter and
    recompute ``totals`` from the survivors.
    """
    if not asset_groups_filter:
        return {**rollup, "served_from": "rollup", "totals_source": "rollup"}

    filter_set = {ag.upper() for ag in asset_groups_filter}
    asset_groups = rollup.get("asset_groups", {})
    if not isinstance(asset_groups, dict):
        return {**rollup, "served_from": "rollup", "totals_source": "rollup"}

    filtered: dict[str, object] = {cat: payload for cat, payload in asset_groups.items() if cat.upper() in filter_set}  # pyright: ignore[reportUnknownVariableType,reportUnknownMemberType]
    totals_keys = (
        "shards",
        "instrument_rows",
        "dates_across_asset_groups",
        "latest_day_instruments",
    )
    totals: dict[str, int] = dict.fromkeys(totals_keys, 0)
    for cat_payload in filtered.values():
        if isinstance(cat_payload, dict):
            for k in totals_keys:
                v = cat_payload.get(k, 0)  # pyright: ignore[reportUnknownMemberType,reportUnknownVariableType]
                if isinstance(v, (int, float)):
                    totals[k] += int(v)

    return {
        **rollup,
        "asset_groups": filtered,
        "totals": totals,
        "served_from": "rollup",
        "totals_source": "rollup",
    }

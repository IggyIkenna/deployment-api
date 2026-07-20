"""Flat (primary-axis x date) capture_status matrix — a heatmap-shaped grid.

``GET /coverage-grid`` — cherry-pick E (2026-07-20, one of three self-contained
picks from the analysis of Harsh's superseded ``feat/data-status-redesign``
branch). Distinct from the hierarchical drilldown (``_deploy_turbo.py``'s
``/drilldown/{service}/{asset_group}`` — a nested tree, lazy-loaded per
branch) and from ``/turbo``/``/manifest`` (per-venue nested breakdowns): this
returns a SINGLE flat 2D matrix, one row per primary-axis value and one cell
per date, shaped for a heatmap widget that wants the whole (primary x date)
surface for a window in one call — no tree-walk, no lazy-load.

Reuses prod's OWN reader — the shared package facade's
``_read_manifest_index`` (``deployment_api.services.manifest_source
.read_manifest_index``), the SAME reader ``data_status_hierarchical
.get_hierarchical_drilldown`` / ``_live_coverage.py`` call, with its existing
``DRILLDOWN_COLUMNS`` column-pushdown + date-window row-group pushdown — plus
an in-process ``BoundedCache`` TTL cache mirroring the
``data_status_drilldown`` package's ``_core.py`` pattern (thread-safe,
bounded-entries — deliberately NOT the older hand-rolled unbounded
``_INDEX_CACHE`` dict in ``data_status_service.py``, whose "day-keyed, grew
forever" bug ``_core.py``'s own comment documents; and NOT Harsh's own
``index_cache.py`` / mock modules from the superseded branch).

Mirrors ``_axis_census.py`` / ``_distinct_values.py``'s submodule shape:
attaches to the shared package ``router`` — registered in
``routes/data_status/__init__.py``'s import block.
"""

from __future__ import annotations

import logging
from typing import cast

import pandas as pd
from fastapi import HTTPException, Query
from unified_api_contracts.registry import get_shard_axes
from unified_trading_library import BucketNamingError

import deployment_api.routes.data_status as _ds
from deployment_api.routes.data_status import router
from deployment_api.utils.bounded_cache import BoundedCache

logger = logging.getLogger(__name__)

# 4-state capture_status vocabulary — every cell always carries all 4 keys
# (zero-filled when absent) so the UI never needs conditional checks. Mirrors
# ``data_status_hierarchical._aggregate_counts`` / ``coverage_metrics
# .compute_capture_status_counts``'s 4-state bucket set.
_CAPTURE_STATUS_KEYS: tuple[str, ...] = (
    "captured",
    "empty_confirmed",
    "attempted_failed",
    "expected_unattempted",
)

# In-process TTL cache — mirrors the ``data_status_drilldown`` package's
# ``BoundedCache`` pattern (thread-safe, bounded 500 entries, 5-min TTL; same
# constants ``_core.py`` uses). Keyed by the full (bucket, window) request
# shape since ``read_manifest_index``'s pushdown path is window-scoped — a
# different window reads a different parquet row-group slice, not a
# superset/subset of a cached one.
_GRID_CACHE_MAX_ENTRIES = 500
_GRID_CACHE_TTL_SECONDS = 300.0
_grid_cache = BoundedCache(
    name="data_status_coverage_grid",
    maxsize=_GRID_CACHE_MAX_ENTRIES,
    ttl_seconds=_GRID_CACHE_TTL_SECONDS,
)


def _read_grid_index_cached(bucket: str, start_date: str, end_date: str) -> pd.DataFrame:
    """5-minute TTL-cached read of the manifest window this grid needs.

    Delegates to the shared facade's ``_read_manifest_index`` (resolved
    through ``deployment_api.routes.data_status`` at call time, matching
    ``_live_coverage.py``'s ``_ds._read_manifest_index(bucket)`` convention)
    so the SAME test-patch surface every sibling data-status endpoint uses
    keeps intercepting.
    """
    cache_key = f"{bucket}:{start_date}:{end_date}"
    cached = _grid_cache.get(cache_key)
    if cached is not None:
        return cast(pd.DataFrame, cached)
    df = _ds._read_manifest_index(bucket, date_window=(start_date, end_date))  # pyright: ignore[reportPrivateUsage]
    _grid_cache[cache_key] = df
    return df


def clear_coverage_grid_cache() -> None:
    """Reset the TTL cache (used by tests and ``/turbo/clear``)."""
    _grid_cache.clear()


def _resolve_primary_axis(service: str, asset_group: str) -> str:
    """First axis of the service's SHARD_AXIS_MATRIX (usually ``venue``).

    Falls back to ``venue`` for an undeclared (service, asset_group) pair —
    mirrors ``data_status_hierarchical._resolve_axis_order``'s fallback.
    """
    axes = get_shard_axes(service, asset_group)
    return axes[0] if axes else "venue"


def build_coverage_grid(df: pd.DataFrame, primary_axis: str) -> dict[str, object]:
    """Pure (primary_axis x date) capture_status matrix builder.

    Returns ``{"dates": [...], "rows": [{"primary": ..., "cells": [...]}]}``
    — heatmap-shaped: EVERY row's ``cells`` list is DENSE over the SAME
    ``dates`` list (one cell per date, zero-filled 4-state counts for a
    (primary, date) pair absent from the manifest) so a UI heatmap can render
    columns aligned without per-row date bookkeeping.

    Honest-absence: an empty df, or one missing ``primary_axis``/``date``,
    returns ``{"dates": [], "rows": []}`` — never a fabricated row. Legacy
    rows without a ``capture_status`` column (or an unrecognised value)
    coerce to ``captured`` (SSOT parity with ``coverage_metrics
    .compute_capture_status_counts`` / the drilldown's ``_aggregate_counts``
    — a bare status-string match would silently drop them, under-counting
    the grid the same way the pre-2026-06-18 drilldown bug did).
    """
    if df.empty or primary_axis not in df.columns or "date" not in df.columns:
        return {"dates": [], "rows": []}

    working = pd.DataFrame(
        {
            "primary": df[primary_axis].astype(str),
            "date": df["date"].astype(str),
        }
    )
    if "capture_status" in df.columns:
        cs = df["capture_status"].astype(str).str.lower()
        working["capture_status"] = cs.where(cs.isin(_CAPTURE_STATUS_KEYS), "captured").to_numpy()
    else:
        working["capture_status"] = "captured"
    working = working[(working["primary"] != "") & (working["primary"].str.lower() != "nan")]
    if working.empty:
        return {"dates": [], "rows": []}

    dates: list[str] = sorted(working["date"].unique())  # pyright: ignore[reportAny]
    counts = pd.crosstab([working["primary"], working["date"]], working["capture_status"])
    counts = counts.reindex(columns=list(_CAPTURE_STATUS_KEYS), fill_value=0)
    lookup: dict[tuple[str, str], dict[str, int]] = {
        cast(tuple[str, str], idx): {str(k): int(v) for k, v in row.items()}  # pyright: ignore[reportUnknownMemberType,reportUnknownVariableType,reportAny]
        for idx, row in counts.iterrows()  # pyright: ignore[reportUnknownMemberType,reportUnknownVariableType]
    }
    zero_counts: dict[str, int] = dict.fromkeys(_CAPTURE_STATUS_KEYS, 0)

    primary_values: list[str] = sorted(working["primary"].unique())  # pyright: ignore[reportAny]
    rows: list[dict[str, object]] = []
    for primary_val in primary_values:
        cells: list[dict[str, object]] = []
        for date_val in dates:
            counts_for_cell = lookup.get((primary_val, date_val), zero_counts)
            cells.append({"date": date_val, **counts_for_cell})
        rows.append({"primary": primary_val, "cells": cells})
    return {"dates": dates, "rows": rows}


@router.get("/coverage-grid")
async def get_coverage_grid(
    service: str = Query(..., description="Service name"),
    asset_group: str = Query(..., description="Asset group (e.g. 'cefi', 'defi')"),
    start_date: str = Query(..., description="ISO YYYY-MM-DD window lower bound"),
    end_date: str = Query(..., description="ISO YYYY-MM-DD window upper bound"),
) -> dict[str, object]:
    """Flat (primary-axis x date) capture_status matrix for a heatmap widget.

    ``primary_axis`` is the first axis of ``(service, asset_group)``'s
    ``SHARD_AXIS_MATRIX`` entry (usually ``venue``). Returns
    ``{service, asset_group, primary_axis, dates: [...],
    rows: [{primary, cells: [{date, captured, empty_confirmed,
    attempted_failed, expected_unattempted}, ...]}, ...]}``.

    Honest-absence: a producer-less (service, asset_group) pair (no bucket
    registered) or a genuinely empty window both render an empty grid
    (``dates: [], rows: []``), never a 500 or a fabricated row.
    """
    primary_axis = _resolve_primary_axis(service, asset_group)
    try:
        bucket = _ds.build_bucket_name(service, asset_group)
    except BucketNamingError:
        return {
            "service": service,
            "asset_group": asset_group.lower(),
            "primary_axis": primary_axis,
            "dates": [],
            "rows": [],
        }
    except (OSError, RuntimeError, ValueError) as exc:
        logger.exception("Error in get_coverage_grid")
        raise HTTPException(status_code=500, detail="Internal server error. Check server logs.") from exc
    try:
        df = _read_grid_index_cached(bucket, start_date, end_date)
        grid = build_coverage_grid(df, primary_axis)
    except (OSError, RuntimeError, ValueError) as exc:
        logger.exception("Error in get_coverage_grid")
        raise HTTPException(status_code=500, detail="Internal server error. Check server logs.") from exc

    return {
        "service": service,
        "asset_group": asset_group.lower(),
        "primary_axis": primary_axis,
        "dates": grid["dates"],
        "rows": grid["rows"],
    }

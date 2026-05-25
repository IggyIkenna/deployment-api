"""Per-(primary-axis x date) coverage grid for the redesigned Data Status UI.

The redesign's heatmap / matrix / stacked visuals need a dense
``(asset_group, primary_value, date) -> capture_status counts`` rollup that the
existing ``/manifest`` + ``/turbo`` endpoints collapse away (they keep the day
axis only as found/expected counts). This builds that grid from the SAME
availability frame the drilldown reads — synthetic in ``CLOUD_MOCK_MODE``,
the real GCS manifest otherwise — so heatmap/matrix get real per-day cells.

Response per asset_group::

    {
      "primary": "venue",
      "primary_values": ["BINANCE-FUTURES", ...],
      "sub_axes": ["data_type", "instrument_type", "instrument_id"],
      "grid": {"<primary_value>": {"<YYYY-MM-DD>": {captured, empty_confirmed,
                                                    attempted_failed}}},
      "by_primary": {"<primary_value>": {captured, empty_confirmed,
                                         attempted_failed}},
      "total": {captured, empty_confirmed, attempted_failed},
      "meta": {"primary_count": int, "days": int, "shards_per_day": int},
    }
"""

from __future__ import annotations

import pandas as pd
from unified_api_contracts.registry.data_status_axis_matrix import get_primary_axis, get_shard_axes
from unified_trading_library import UnifiedCloudConfig, read_availability_index

from deployment_api.services.data_status_drilldown import build_bucket_name
from deployment_api.services.data_status_mock_drilldown import build_mock_availability_index

_CLOUD_CFG = UnifiedCloudConfig()

_STATUSES = ("captured", "empty_confirmed", "attempted_failed")


def _counts(frame: pd.DataFrame) -> dict[str, int]:
    if "capture_status" not in frame.columns:
        return {"captured": int(len(frame)), "empty_confirmed": 0, "attempted_failed": 0}
    cs = frame["capture_status"].astype(str)
    return {s: int((cs == s).sum()) for s in _STATUSES}


def build_coverage_grid(
    service: str,
    asset_group: str,
    window_start: str,
    window_end: str,
    *,
    project_id: str | None = None,
) -> dict[str, object]:
    """Build the per-(primary, date) coverage grid for one (service, asset_group)."""
    axes = get_shard_axes(service, asset_group)
    primary = get_primary_axis(service, asset_group) or (axes[0] if axes else "venue")
    full_axes = axes if "date" in axes else (*axes, "date")

    if _CLOUD_CFG.is_mock_mode():
        df = build_mock_availability_index(service, asset_group, full_axes, window_start, window_end)
    else:
        bucket = build_bucket_name(service, asset_group, project_id=project_id)
        df = read_availability_index(bucket)

    empty_result: dict[str, object] = {
        "primary": primary,
        "primary_values": [],
        "sub_axes": [a for a in axes if a != primary and a != "date"],
        "grid": {},
        "by_primary": {},
        "total": {s: 0 for s in _STATUSES},
        "meta": {"primary_count": 0, "days": 0, "shards_per_day": 0},
    }
    if df is None or len(df) == 0 or primary not in df.columns:  # pyright: ignore[reportUnnecessaryComparison]
        return empty_result
    if "date" in df.columns:
        df = df[(df["date"] >= window_start) & (df["date"] <= window_end)]

    primary_values = sorted(v for v in df[primary].astype(str).unique() if v not in ("", "nan"))  # pyright: ignore[reportAny]
    dates = sorted(v for v in df["date"].astype(str).unique() if v not in ("", "nan"))  # pyright: ignore[reportAny]

    grid: dict[str, dict[str, dict[str, int]]] = {}
    by_primary: dict[str, dict[str, int]] = {}
    for pv in primary_values:  # pyright: ignore[reportAny]
        sub = df[df[primary].astype(str) == pv]  # pyright: ignore[reportUnknownVariableType]
        by_primary[pv] = _counts(sub)  # pyright: ignore[reportUnknownArgumentType]
        per_date: dict[str, dict[str, int]] = {}
        for dt, day_frame in sub.groupby(sub["date"].astype(str)):  # pyright: ignore[reportUnknownVariableType, reportUnknownMemberType, reportAny]
            per_date[str(dt)] = _counts(day_frame)  # pyright: ignore[reportUnknownArgumentType]
        grid[pv] = per_date

    total = _counts(df)  # pyright: ignore[reportUnknownArgumentType]
    total_shards = sum(total.values())
    n_days = len(dates)
    return {
        "primary": primary,
        "primary_values": primary_values,
        "sub_axes": [a for a in axes if a != primary and a != "date"],
        "grid": grid,
        "by_primary": by_primary,
        "total": total,
        "meta": {
            "primary_count": len(primary_values),
            "days": n_days,
            "shards_per_day": round(total_shards / n_days) if n_days else 0,
        },
    }

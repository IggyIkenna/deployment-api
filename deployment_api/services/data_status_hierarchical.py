"""Hierarchical shard-atom drill-down for the Data Status page.

Plan: ``data_status_drilldown_shard_atom_alignment_2026_05_07.md`` Phase 1.

The pre-2026-05-07 data-status panel collapsed the per-asset_group drill-down
to ``venue -> instrument_type -> day``, which is the right depth for
``instruments-service`` (its writer atomicity boundary IS roughly venue-level)
but discards the within-day fan-out for every other service. Per the codex
"Per-asset-group shard-key matrix" the right depth varies:

* MTDS CeFi:       ``venue -> data_type -> instrument_type -> instrument_id -> day``
* MTDS DeFi:       ``chain -> venue -> data_type -> instrument_id -> day``
* MTDS TradFi:     ``venue -> data_type -> instrument_type -> root_or_inst -> day``
* features-*:      ``venue -> feature_group -> timeframe -> instrument_id -> day``
* sports services: ``data_type -> league_id -> day``
* prediction:      ``venue -> canonical_question_group -> day``

This module is the SSOT-driven generic builder. The axis order per
``(service, asset_group)`` comes from
``unified_api_contracts.registry.data_status_axis_matrix.SHARD_AXIS_MATRIX``;
the leaf row count comes from the v5+ availability manifest read by
``unified_trading_library.read_availability_index``. Each leaf carries
``capture_status`` counts (captured / empty_confirmed / attempted_failed)
and the structured ``row_key`` consumed by the per-leaf SmartDownloadButton
+ the surgical Deploy-Missing CLI invocation
(``--shard-key=<asset_group>|<venue>|<data_type>|...``).

The generic builder returns a TREE (each node has ``children`` list +
aggregate counts). Lazy-load via ``filters``: callers pass progressively
deeper filter dicts and the builder returns only the branch that matches.
"""

from __future__ import annotations

import logging
from dataclasses import dataclass, field
from typing import cast

import pandas as pd
from unified_api_contracts.features import FEATURE_GROUP_TO_FAMILY
from unified_api_contracts.registry import EMPTY_OR_DEPRECATED_DEFI_VENUES
from unified_api_contracts.registry.data_status_axis_matrix import (
    SHARD_AXIS_MATRIX,
    get_shard_axes,
)
from unified_trading_library import UnifiedCloudConfig

from deployment_api.services.data_status_drilldown import build_bucket_name
from deployment_api.services.data_status_mock_drilldown import build_mock_availability_index
from deployment_api.services.index_cache import read_index_cached
from deployment_api.services.reason_taxonomy import (
    ReasonCategory,
    classify_reason,
    rollup_reasons_frame,
)

_CLOUD_CFG = UnifiedCloudConfig()

logger = logging.getLogger(__name__)

_ALL_DEFI_GHOST_VENUES: frozenset[str] = EMPTY_OR_DEPRECATED_DEFI_VENUES

# ---------------------------------------------------------------------------
# Defaults
# ---------------------------------------------------------------------------

# Defensive cap on any one node's children list. Pagination via the
# ``child_offset`` / ``child_limit`` params is the primary mechanism for
# venues with thousands of instruments (BINANCE-FUTURES PERPETUAL,
# DERIBIT options chains); this cap is an emergency bound.
# Plan: data_status_drilldown_shard_atom_alignment_2026_05_07.md
# § Phase 6 (operator finding 2026-05-07: per-instrument drilldown was
# silently truncated at the prior 500 default).
_MAX_CHILDREN_PER_NODE: int = 10_000

# Default per-page child count when the caller passes a non-null
# ``child_limit``. Operators can override via query params.
_DEFAULT_CHILD_PAGE_SIZE: int = 200

# Manifest columns we always project into the tree, even when not part of
# the SHARD_AXIS_MATRIX axes for the requested service. ``date`` is implicit
# on every row; ``capture_status`` / ``error_reason`` drive the leaf badges.
# ``underlying`` is required for the bundled-data_type root virtualisation
# (options_chain / futures_chain populate ``underlying`` and leave
# ``instrument_id`` empty per the manifest writer).
_REQUIRED_COLUMNS: tuple[str, ...] = (
    "date",
    "capture_status",
    "error_reason",
    "underlying",
)


@dataclass
class DrilldownNode:
    """One node in the hierarchical drill-down tree.

    ``axis`` is the manifest column this node groups by (e.g. ``venue``,
    ``chain``); ``value`` is the specific value of that column (e.g.
    ``ARBITRUM``). ``axis="day"`` marks the leaf level — at that point
    ``children`` is empty and the per-status counts represent a single
    ``(shard_atom, day)`` row in the manifest.
    """

    axis: str
    value: str
    captured: int = 0
    empty_confirmed: int = 0
    attempted_failed: int = 0
    children: list[DrilldownNode] = field(default_factory=list)
    row_key: dict[str, str] = field(default_factory=dict)
    # Closed-set reason category (reason_taxonomy.ReasonCategory) for the
    # WHOLE subtree below this node — answers "why isn't this captured?".
    # ``""`` until populated (only the deepest/leaf axis sets it, to keep
    # tree-build cheap; intermediate nodes rely on the response-level
    # ``reason_summary``). Phantom > failed > empty > captured precedence.
    reason_category: str = ""

    @property
    def total(self) -> int:
        return self.captured + self.empty_confirmed + self.attempted_failed

    @property
    def completion_pct(self) -> float:
        denom = self.total
        if denom <= 0:
            return 0.0
        return round(self.captured / denom * 100, 2)

    def to_dict(self) -> dict[str, object]:
        return {
            "axis": self.axis,
            "value": self.value,
            "captured": self.captured,
            "empty_confirmed": self.empty_confirmed,
            "attempted_failed": self.attempted_failed,
            "total": self.total,
            "completion_pct": self.completion_pct,
            "row_key": self.row_key,
            "reason_category": self.reason_category,
            "children": [c.to_dict() for c in self.children],
            "is_leaf": not self.children,
        }


def _resolve_axis_order(service: str, asset_group: str) -> tuple[str, ...]:
    """Return the codex shard-axis order with ``date`` appended at the leaf.

    For ``(service, asset_group)`` pairs not declared in the SSOT the
    fallback is ``(venue, date)`` — the universal lowest-common-denominator
    that matches the pre-2026-05-07 flat panel. Logs a warning so the
    operator can declare the missing pair in UAC.
    """
    axes = get_shard_axes(service, asset_group)
    if not axes:
        logger.warning(
            "drilldown: no SHARD_AXIS_MATRIX entry for (%s, %s); falling back to (venue, date)",
            service,
            asset_group,
        )
        axes = ("venue",)
    if "date" in axes:
        return axes
    return (*axes, "date")


_NON_AXIS_FILTERS: frozenset[str] = frozenset({"feature_family"})
"""Filter keys that are NOT in any service's SHARD_AXIS_MATRIX axes but
are valid manifest columns (post-:func:`_stamp_feature_family`) and so
get applied unconditionally if the caller supplies them. Plan:
features-repo consolidation Phase 8B (deployment-api side)."""


def _filter_manifest(df: pd.DataFrame, axes: tuple[str, ...], filters: dict[str, str]) -> pd.DataFrame:
    """Apply level-by-level filters, narrowing the manifest to the
    requested branch.

    Filters in ``axes`` AND filters in :data:`_NON_AXIS_FILTERS` (e.g.
    ``feature_family``, the parent classification of ``feature_group``)
    both apply. Other filter keys are ignored (defensive).
    """
    out = df
    for axis in axes:
        val = filters.get(axis)
        if val is None or axis not in out.columns:
            continue
        out = out[out[axis].astype(str) == str(val)]
    for non_axis_key in _NON_AXIS_FILTERS:
        val = filters.get(non_axis_key)
        if val is None or non_axis_key not in out.columns:
            continue
        out = out[out[non_axis_key].astype(str) == str(val)]
    return out


def _aggregate_counts(rows: pd.DataFrame) -> tuple[int, int, int]:
    """Per-status counts for a slice of manifest rows.

    Pre-v5 manifests omit ``capture_status``; in that case every row counts
    as captured (the legacy assumption — degenerate compatibility path).
    """
    if "capture_status" not in rows.columns:
        return len(rows), 0, 0
    cs = rows["capture_status"].astype(str)
    captured = int((cs == "captured").sum())
    empty_confirmed = int((cs == "empty_confirmed").sum())
    attempted_failed = int((cs == "attempted_failed").sum())
    return captured, empty_confirmed, attempted_failed


# Above this row count we skip the (vectorised but still O(rows)) reason
# rollup at the response root — only hit on an UNFILTERED whole-asset-group
# drilldown, which is not the operator's normal path (they filter to a venue /
# chain first). Filtered slices are far below this and always summarised.
_REASON_SUMMARY_ROW_CAP: int = 600_000


def _reason_summary_for_slice(rows: pd.DataFrame) -> dict[str, int]:
    """Closed-set reason-category counts for a manifest slice (or ``{}`` when too big).

    Returns the full category grid (every key present) so the UI renders a fixed
    breakdown. Skipped (``{}``) above :data:`_REASON_SUMMARY_ROW_CAP` to bound the
    cost of an unfiltered whole-AG drilldown; the UI shows "narrow the filter to
    see the reason breakdown" in that case.
    """
    if rows.empty or "capture_status" not in rows.columns:
        return {}
    if len(rows) > _REASON_SUMMARY_ROW_CAP:
        return {}
    reason_col = rows["error_reason"] if "error_reason" in rows.columns else rows["capture_status"].map(lambda _v: "")  # pyright: ignore[reportUnknownMemberType,reportUnknownLambdaType,reportUnknownArgumentType]
    return rollup_reasons_frame(rows["capture_status"], reason_col)


def _dominant_reason_category(rows: pd.DataFrame) -> str:
    """The single reason category that best describes a (small) leaf slice.

    A leaf is one ``(shard_atom, day)`` — usually a single manifest row, but a
    bundled / multi-row leaf is summarised by its dominant NON-captured category
    (so a leaf that's mostly captured but has one failure still surfaces the
    failure), falling back to ``captured`` when everything is captured.
    """
    if rows.empty:
        return ""
    if "capture_status" not in rows.columns:
        return ReasonCategory.CAPTURED.value
    if len(rows) == 1:
        row = rows.iloc[0]
        status = row.get("capture_status")  # pyright: ignore[reportUnknownMemberType,reportUnknownVariableType]
        reason = row.get("error_reason") if "error_reason" in rows.columns else ""  # pyright: ignore[reportUnknownMemberType,reportUnknownVariableType]
        return classify_reason(
            None if status is None else str(status),  # pyright: ignore[reportUnknownArgumentType]
            None if reason is None else str(reason),  # pyright: ignore[reportUnknownArgumentType]
        ).value
    summary = rollup_reasons_frame(
        rows["capture_status"],
        rows["error_reason"] if "error_reason" in rows.columns else rows["capture_status"].map(lambda _v: ""),  # pyright: ignore[reportUnknownArgumentType,reportUnknownMemberType,reportUnknownLambdaType]
    )
    non_captured = {k: v for k, v in summary.items() if k != ReasonCategory.CAPTURED.value and v > 0}
    if non_captured:
        return max(non_captured, key=lambda k: non_captured[k])
    return ReasonCategory.CAPTURED.value


def _children_for_axis(
    rows: pd.DataFrame,
    axis: str,
    deeper_axes: tuple[str, ...],
    parent_row_key: dict[str, str],
    expand_to_depth: int,
    current_depth: int,
) -> list[DrilldownNode]:
    """Build the ordered child list for a given axis level.

    When ``expand_to_depth <= current_depth`` we stop recursing and return
    leaf-shaped nodes (no children), letting the UI lazy-load deeper on
    expand. The aggregate counts at each non-leaf node still reflect the
    full subtree below it — we just don't materialise the subtree until
    asked.
    """
    if axis not in rows.columns:
        return []
    if rows.empty:
        return []
    values = sorted(v for v in rows[axis].astype(str).unique() if v != "" and v != "nan")  # pyright: ignore[reportAny]
    if len(values) > _MAX_CHILDREN_PER_NODE:
        logger.warning(
            "drilldown: axis %s has %d values, truncating to %d (caller should paginate)",
            axis,
            len(values),
            _MAX_CHILDREN_PER_NODE,
        )
        values = values[:_MAX_CHILDREN_PER_NODE]
    children: list[DrilldownNode] = []
    for val in values:  # pyright: ignore[reportAny]
        sub = rows[rows[axis].astype(str) == val]  # pyright: ignore[reportUnknownVariableType]
        captured, empty_confirmed, attempted_failed = _aggregate_counts(sub)  # pyright: ignore[reportUnknownArgumentType]
        node_row_key = {**parent_row_key, axis: val}
        node = DrilldownNode(
            axis=axis,
            value=val,  # pyright: ignore[reportAny]
            captured=captured,
            empty_confirmed=empty_confirmed,
            attempted_failed=attempted_failed,
            row_key=node_row_key,
        )
        if deeper_axes and current_depth < expand_to_depth:
            next_axis, *rest = deeper_axes
            node.children = _children_for_axis(
                sub,  # pyright: ignore[reportUnknownArgumentType]
                next_axis,
                tuple(rest),
                node_row_key,
                expand_to_depth,
                current_depth + 1,
            )
        if not deeper_axes:
            # True leaf (deepest axis) — stamp the WHY badge from this slice.
            node.reason_category = _dominant_reason_category(sub)  # pyright: ignore[reportUnknownArgumentType]
        children.append(node)
    return children


def _stamp_feature_family(df: pd.DataFrame) -> pd.DataFrame:
    """Add (or fill in) a ``feature_family`` column on a manifest df.

    Read-side derivation of the parent classification of ``feature_group``
    using the UAC ``FEATURE_GROUP_TO_FAMILY`` registry. Write-time stamps
    win when present (writer-stamped per UTL :class:`MissingFeatureFamilyError`
    enforcement); empty / missing rows are filled by the registry lookup,
    so consumers can group / filter by feature_family even when the
    manifest predates the column. Rows without a ``feature_group`` (e.g.
    market-tick-data-service rows) keep ``feature_family`` empty.

    Plan: features-repo consolidation Phase 8B (deployment-api side).
    """
    if "feature_group" not in df.columns:
        return df
    out = df.copy()
    if "feature_family" not in out.columns:
        out["feature_family"] = ""
    fg = out["feature_group"].astype(str)
    ff = out["feature_family"].astype(str)
    needs_stamp = (ff == "") | (ff == "nan")
    if needs_stamp.any():
        derived = fg.map(lambda g: FEATURE_GROUP_TO_FAMILY[g].value if g in FEATURE_GROUP_TO_FAMILY else "")
        out.loc[needs_stamp, "feature_family"] = derived[needs_stamp]
    return out


def _coalesce_instrument_id_from_underlying(df: pd.DataFrame) -> pd.DataFrame:
    """Promote ``underlying`` into ``instrument_id`` for bundled-data_type rows.

    Per ``availability-manifest-and-data-status.md`` § "underlying vs
    instrument_id", bundled chain shards (``data_type ∈ {options_chain,
    futures_chain}``) populate ``underlying`` with the base asset (BTC,
    ETH, ESH4, NQH4) and leave ``instrument_id`` empty. The
    SHARD_AXIS_MATRIX axis is ``instrument_id`` for MTDS / MDPS CeFi +
    TradFi, so without virtualisation the drilldown's empty-string
    filter (``v != ""`` in ``_children_for_axis``) excludes every
    bundled row from the per-instrument level.

    This helper is read-side only — the manifest writer is unchanged.
    Per-instrument rows keep their ``instrument_id``; bundled rows get
    ``instrument_id = underlying`` so the drilldown surfaces them at
    the per-root level (BTC, ETH for Deribit options; ESH4, NQH4 for
    CME futures).

    Plan: data_status_drilldown_shard_atom_alignment_2026_05_07.md
    § Phase 6 (operator finding 2026-05-07: bundled options/futures
    drilldown collapsed at empty ``instrument_id``).
    """
    if "instrument_id" not in df.columns or "underlying" not in df.columns:
        return df
    out = df.copy()
    inst = out["instrument_id"].astype(str)
    under = out["underlying"].astype(str)
    needs_root = (inst == "") | (inst == "nan")
    out.loc[needs_root, "instrument_id"] = under[needs_root]
    return out


def get_hierarchical_drilldown(
    service: str,
    asset_group: str,
    window_start: str,
    window_end: str,
    *,
    filters: dict[str, str] | None = None,
    expand_to_depth: int = 2,
    project_id: str | None = None,
    child_offset: int = 0,
    child_limit: int | None = None,
) -> dict[str, object]:
    """Build the hierarchical drill-down tree for ``(service, asset_group)``.

    Args:
        service: Service slug (matches ``data_status_axis_matrix`` keys).
        asset_group: Lowercase asset_group (``cefi`` / ``defi`` / ...).
        window_start: ISO YYYY-MM-DD window lower bound.
        window_end: ISO YYYY-MM-DD window upper bound.
        filters: Per-axis filter dict for lazy-load (e.g.
            ``{"chain": "ARBITRUM", "venue": "AAVE_V3-ARBITRUM"}``); the
            tree returned is the subtree rooted at the deepest matched
            level. ``None`` returns the whole tree from the top axis.
        expand_to_depth: How many levels below the matched root to
            materialise children for. Default 2 keeps responses small;
            UI re-fetches with deeper filters on expand.
        project_id: GCP project for bucket name resolution; defaults to
            the deployment-api configured project.
        child_offset: Pagination offset into the TOP-level children list
            (the head axis below the deepest matched filter). Default 0.
        child_limit: Max top-level children returned. ``None`` = no
            slice (return all up to ``_MAX_CHILDREN_PER_NODE``); positive
            int slices ``[child_offset : child_offset + child_limit]``.
            Used by the UI to lazy-load big per-instrument lists
            (BINANCE-FUTURES PERPETUAL, DERIBIT options chains, …).

    Returns:
        ``{"axes": (tuple), "tree": [DrilldownNode.to_dict(), ...],
            "totals": {captured, empty_confirmed, attempted_failed,
                       total, completion_pct}, "filtered_by": filters,
            "service": service, "asset_group": asset_group,
            "child_offset": int, "child_limit": int | None,
            "total_top_axis_children": int}``.

        ``total_top_axis_children`` is the unfiltered child count at the
        head axis - UI uses it to render "showing N-M of T" + load-more.

    Raises:
        ValueError: If the bucket cannot be resolved or the manifest read
            fails. Callers translate to HTTP 4xx/5xx.
    """
    filters = filters or {}
    axes = _resolve_axis_order(service, asset_group)

    bucket = build_bucket_name(service, asset_group, project_id=project_id)
    # ``read_availability_index`` takes the BUCKET NAME (no scheme), NOT
    # a ``gs://...`` URI — passing the URI silently returns an empty df.
    # Same call shape as ``data_status_service.py``'s preflight skip path.
    manifest_uri = f"gs://{bucket}/_index/availability_index.parquet"  # noqa: gs-uri  — URI composer, bucket already resolved
    # Local mock mode: no GCS manifest to read, so synthesise a per-shard
    # availability frame with the same columns the real index returns. Lets
    # the redesigned Data Status UI render against the local stack.
    if _CLOUD_CFG.is_mock_mode():
        df = build_mock_availability_index(service, asset_group, axes, window_start, window_end)
    else:
        df = read_index_cached(bucket)
    if df is not None and len(df) > 0 and asset_group.lower() == "defi" and "venue" in df.columns:  # pyright: ignore[reportUnnecessaryComparison]
        df = df[~df["venue"].isin(_ALL_DEFI_GHOST_VENUES)].reset_index(drop=True)
    if df is None or len(df) == 0:  # pyright: ignore[reportUnnecessaryComparison]
        return {
            "axes": list(axes),
            "tree": [],
            "totals": {
                "captured": 0,
                "empty_confirmed": 0,
                "attempted_failed": 0,
                "total": 0,
                "completion_pct": 0.0,
            },
            "filtered_by": filters,
            "service": service,
            "asset_group": asset_group,
            "manifest_uri": manifest_uri,
            "child_offset": child_offset,
            "child_limit": child_limit,
            "total_top_axis_children": 0,
        }

    if "date" in df.columns:
        df = df[(df["date"] >= window_start) & (df["date"] <= window_end)]

    # Promote ``underlying`` into ``instrument_id`` BEFORE filtering so
    # bundled-data_type rows respond to operator-supplied
    # ``instrument_id`` filters keyed by root (BTC, ESH4, …).
    df = _coalesce_instrument_id_from_underlying(df)
    # Stamp ``feature_family`` (read-side) so callers can filter / group
    # features-* manifests by the parent classification axis even when
    # the manifest predates the column. Plan: features-repo consolidation
    # Phase 8B (deployment-api side).
    df = _stamp_feature_family(df)
    df = _filter_manifest(df, axes, filters)

    reason_summary = _reason_summary_for_slice(df)

    matched_depth = sum(1 for axis in axes if filters.get(axis) is not None)
    remaining_axes = axes[matched_depth:]
    if not remaining_axes:
        captured, empty_confirmed, attempted_failed = _aggregate_counts(df)
        return {
            "axes": list(axes),
            "tree": [],
            "totals": _totals_dict(captured, empty_confirmed, attempted_failed),
            "reason_summary": reason_summary,
            "filtered_by": filters,
            "service": service,
            "asset_group": asset_group,
            "manifest_uri": manifest_uri,
            "child_offset": child_offset,
            "child_limit": child_limit,
            "total_top_axis_children": 0,
        }

    head_axis, *rest = remaining_axes
    tree = _children_for_axis(
        df,
        head_axis,
        tuple(rest),
        dict(filters),
        expand_to_depth=expand_to_depth,
        current_depth=0,
    )

    total_top_axis_children = len(tree)
    if child_limit is not None and child_limit >= 0:
        tree = tree[child_offset : child_offset + child_limit]
    elif child_offset > 0:
        tree = tree[child_offset:]

    captured, empty_confirmed, attempted_failed = _aggregate_counts(df)
    return {
        "axes": list(axes),
        "tree": [n.to_dict() for n in tree],
        "totals": _totals_dict(captured, empty_confirmed, attempted_failed),
        "reason_summary": reason_summary,
        "filtered_by": filters,
        "service": service,
        "asset_group": asset_group,
        "manifest_uri": manifest_uri,
        "child_offset": child_offset,
        "child_limit": child_limit,
        "total_top_axis_children": total_top_axis_children,
    }


def _totals_dict(captured: int, empty_confirmed: int, attempted_failed: int) -> dict[str, object]:
    total = captured + empty_confirmed + attempted_failed
    completion_pct = round(captured / total * 100, 2) if total > 0 else 0.0
    return {
        "captured": captured,
        "empty_confirmed": empty_confirmed,
        "attempted_failed": attempted_failed,
        "total": total,
        "completion_pct": completion_pct,
    }


def list_supported_pairs() -> list[dict[str, object]]:
    """Return all ``(service, asset_group)`` pairs this endpoint supports.

    UI consumers introspect this to decide which services render the
    hierarchical drill-down vs the legacy flat panel during the Phase 2
    migration. Returns ordered list keyed by service then asset_group.
    """
    out: list[dict[str, object]] = []
    for (service, asset_group), axes in sorted(SHARD_AXIS_MATRIX.items()):
        out.append(
            {
                "service": service,
                "asset_group": asset_group,
                "axes": list(axes) + (["date"] if "date" not in axes else []),
            }
        )
    return out


__all__ = [
    "DrilldownNode",
    "get_hierarchical_drilldown",
    "list_supported_pairs",
]


# Silence unused-import: ``cast`` is reserved for future leaf row_key
# coercion when the manifest schema gains nullable typed columns.
_ = cast

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
``capture_status`` counts (captured / empty_confirmed / attempted_failed /
expected_unattempted) and the structured ``row_key`` consumed by the per-leaf SmartDownloadButton
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
from unified_api_contracts.registry import (
    EMPTY_OR_DEPRECATED_DEFI_VENUES,
    SHARD_AXIS_MATRIX,
    get_shard_axes,
)

from deployment_api.services.data_status_drilldown import build_bucket_name
from deployment_api.services.data_status_union import (
    has_provenance_columns,
    provenance_breakdown,
    union_reduce_to_cells,
)
from deployment_api.services.manifest_source import read_manifest_index as read_availability_index

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
class DrilldownNode:  # CORRECT-LOCAL: in-process drilldown tree node
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
    expected_unattempted: int = 0
    children: list[DrilldownNode] = field(default_factory=list)
    row_key: dict[str, str] = field(default_factory=dict)
    # G3/M5 per-cell provenance: per-(pipeline_mode, source) capture_status
    # CELL counts. Populated only at the shard-atom leaf (the ``date`` level)
    # so the UI can render "captured via batch_databento + replay_databento,
    # missing in live_databento". Empty on v8 manifests (no provenance cols).
    provenance: list[dict[str, object]] = field(default_factory=list)

    @property
    def total(self) -> int:
        # B3: denominator includes the 4th bin (expected_unattempted) so the
        # completion_pct is captured / (captured+empty+failed+expected).
        return self.captured + self.empty_confirmed + self.attempted_failed + self.expected_unattempted

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
            "expected_unattempted": self.expected_unattempted,
            "total": self.total,
            "completion_pct": self.completion_pct,
            "row_key": self.row_key,
            "provenance": self.provenance,
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


_NON_AXIS_FILTERS: frozenset[str] = frozenset({"feature_family", "pipeline_mode", "source"})
"""Filter keys that are NOT in any service's SHARD_AXIS_MATRIX axes but
are valid manifest columns and so get applied unconditionally if the caller
supplies them. ``feature_family``: features-repo consolidation Phase 8B.
``pipeline_mode`` / ``source`` (G3/M5): the v9 provenance axes — let an
operator narrow the tree to a single mode/source (e.g. "show only the
live_databento view")."""

# G3/M5: provenance axes the caller may inject as GROUP-BY levels at the top of
# the tree (e.g. group the whole tree by ``pipeline_mode`` to compare batch vs
# live coverage). Restricted to the provenance columns — every other axis comes
# from the SHARD_AXIS_MATRIX SSOT.
_GROUP_BY_AXES: frozenset[str] = frozenset({"pipeline_mode", "source"})


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


def _aggregate_counts(rows: pd.DataFrame) -> tuple[int, int, int, int]:
    """Per-status counts for a slice of manifest rows.

    Returns the 4-state tuple ``(captured, empty_confirmed, attempted_failed,
    expected_unattempted)``. B2: the 4th bin (``expected_unattempted``) makes
    genuinely-missing cells visible in the drilldown tree — the most useful
    "where's the missing data" view.

    Pre-v5 manifests omit ``capture_status``; in that case every row counts
    as captured (the legacy assumption — degenerate compatibility path).

    G3/M5 UNION: post-migration a cell carries one row per (source,
    pipeline_mode). Collapse to one honest row per cell first so a leaf cell
    captured by >1 source/mode counts once (≥1 captured ⇒ captured) instead of
    inflating the tree counts past the de-dup'd denominator. v8 manifests (no
    provenance columns) already have one row per cell → no-op.
    """
    if "capture_status" not in rows.columns:
        return len(rows), 0, 0, 0
    if has_provenance_columns(rows):
        rows = union_reduce_to_cells(rows)
    cs = rows["capture_status"].astype(str)
    captured = int((cs == "captured").sum())
    empty_confirmed = int((cs == "empty_confirmed").sum())
    attempted_failed = int((cs == "attempted_failed").sum())
    expected_unattempted = int((cs == "expected_unattempted").sum())
    return captured, empty_confirmed, attempted_failed, expected_unattempted


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
        captured, empty_confirmed, attempted_failed, expected_unattempted = _aggregate_counts(sub)  # pyright: ignore[reportUnknownArgumentType]
        node_row_key = {**parent_row_key, axis: val}
        node = DrilldownNode(
            axis=axis,
            value=val,  # pyright: ignore[reportAny]
            captured=captured,
            empty_confirmed=empty_confirmed,
            attempted_failed=attempted_failed,
            expected_unattempted=expected_unattempted,
            row_key=node_row_key,
        )
        # Shard-atom leaf (no deeper axis) → attach the per-(pipeline_mode,
        # source) breakdown so the UI can show which mode/source answered the
        # cell. Cell-grain + honest (v8 manifests yield []).
        if not deeper_axes:
            node.provenance = provenance_breakdown(sub)  # pyright: ignore[reportUnknownArgumentType]
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


# Bundled prediction data_type constant (matches the ManifestWriter row_key).
_PREDICTION_BUNDLED_DT: str = "prediction_canonical_question_group"


def _coalesce_cqg_from_instrument_id(df: pd.DataFrame) -> pd.DataFrame:
    """Promote ``instrument_id`` → ``canonical_question_group`` for v9 prediction rows.

    v9 canonical prediction shape (``prediction_manifest_canonicalisation_2026_06_01.md``):
    the bundled manifest row has ``data_type="prediction_canonical_question_group"`` and
    stores the canonical_question_group value in ``instrument_id`` (the ManifestWriter
    row_key field).  The SHARD_AXIS_MATRIX for prediction uses ``canonical_question_group``
    as the second axis, so without this promotion the drilldown filter + ``_children_for_axis``
    find no ``canonical_question_group`` column and return empty.

    Read-side only — the manifest writer is unchanged.  Rows that already have a
    non-empty ``canonical_question_group`` column (pre-v9 legacy writes) are left
    as-is; only rows with ``data_type == "prediction_canonical_question_group"`` and
    an empty / absent ``canonical_question_group`` are filled from ``instrument_id``.

    Plan: ``prediction_manifest_canonicalisation_2026_06_01.md``
    § "Deployment-API/UI prediction v9 data-status alignment".
    """
    if "data_type" not in df.columns or "instrument_id" not in df.columns:
        return df
    pred_mask = df["data_type"].astype(str) == _PREDICTION_BUNDLED_DT
    if not pred_mask.any():
        return df
    out = df.copy()
    if "canonical_question_group" not in out.columns:
        out["canonical_question_group"] = ""
    cqg = out["canonical_question_group"].astype(str)
    inst = out["instrument_id"].astype(str)
    # Fill empty cqg slots on prediction bundled rows from instrument_id.
    needs_fill = pred_mask & ((cqg == "") | (cqg == "nan"))
    out.loc[needs_fill, "canonical_question_group"] = inst[needs_fill]
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
    group_by: list[str] | None = None,
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
                       expected_unattempted, total, completion_pct}, "filtered_by": filters,
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
    df = read_availability_index(bucket)
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
                "expected_unattempted": 0,
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
            "provenance": [],
        }

    if "date" in df.columns:
        df = df[(df["date"] >= window_start) & (df["date"] <= window_end)]

    # Promote ``underlying`` into ``instrument_id`` BEFORE filtering so
    # bundled-data_type rows respond to operator-supplied
    # ``instrument_id`` filters keyed by root (BTC, ESH4, …).
    df = _coalesce_instrument_id_from_underlying(df)
    # v9 prediction: promote ``instrument_id`` → ``canonical_question_group`` so
    # the SHARD_AXIS_MATRIX axis filter + drilldown children work correctly.
    # Read-side only; rows with a pre-existing non-empty cqg column are untouched.
    df = _coalesce_cqg_from_instrument_id(df)
    # Stamp ``feature_family`` (read-side) so callers can filter / group
    # features-* manifests by the parent classification axis even when
    # the manifest predates the column. Plan: features-repo consolidation
    # Phase 8B (deployment-api side).
    df = _stamp_feature_family(df)
    # G3/M5: inject requested provenance group-by axes (pipeline_mode / source)
    # at the TOP of the tree so an operator can compare batch-vs-live coverage.
    axes = _apply_group_by_axes(axes, group_by, df)
    df = _filter_manifest(df, axes, filters)

    matched_depth = sum(1 for axis in axes if filters.get(axis) is not None)
    remaining_axes = axes[matched_depth:]
    if not remaining_axes:
        captured, empty_confirmed, attempted_failed, expected_unattempted = _aggregate_counts(df)
        return {
            "axes": list(axes),
            "tree": [],
            "totals": _totals_dict(captured, empty_confirmed, attempted_failed, expected_unattempted),
            "filtered_by": filters,
            "service": service,
            "asset_group": asset_group,
            "manifest_uri": manifest_uri,
            "child_offset": child_offset,
            "child_limit": child_limit,
            "total_top_axis_children": 0,
            "provenance": provenance_breakdown(df),
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

    captured, empty_confirmed, attempted_failed, expected_unattempted = _aggregate_counts(df)
    return {
        "axes": list(axes),
        "tree": [n.to_dict() for n in tree],
        "totals": _totals_dict(captured, empty_confirmed, attempted_failed, expected_unattempted),
        "filtered_by": filters,
        "service": service,
        "asset_group": asset_group,
        "manifest_uri": manifest_uri,
        "child_offset": child_offset,
        "child_limit": child_limit,
        "total_top_axis_children": total_top_axis_children,
        "provenance": provenance_breakdown(df),
    }


def _apply_group_by_axes(axes: tuple[str, ...], group_by: list[str] | None, df: pd.DataFrame) -> tuple[str, ...]:
    """Prepend valid provenance group-by axes (pipeline_mode / source) to the
    tree axis order.

    Only ``_GROUP_BY_AXES`` columns that actually exist in the manifest and are
    not already an axis are added, preserving caller order. v8 manifests (no
    provenance columns) get the base axes unchanged.
    """
    if not group_by:
        return axes
    extra = [g for g in group_by if g in _GROUP_BY_AXES and g in df.columns and g not in axes]
    if not extra:
        return axes
    return (*extra, *axes)


def _totals_dict(
    captured: int, empty_confirmed: int, attempted_failed: int, expected_unattempted: int
) -> dict[str, object]:
    # B3: denominator includes expected_unattempted (the 4th bin) so the
    # completion_pct is captured / (captured+empty+failed+expected).
    total = captured + empty_confirmed + attempted_failed + expected_unattempted
    completion_pct = round(captured / total * 100, 2) if total > 0 else 0.0
    return {
        "captured": captured,
        "empty_confirmed": empty_confirmed,
        "attempted_failed": attempted_failed,
        "expected_unattempted": expected_unattempted,
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

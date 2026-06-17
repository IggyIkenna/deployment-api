"""Post-migration v9 manifest UNION read path (G3 / M5 / M4).

Post the ``pipeline_mode``-source-aware migration
(``master_data_canonicalisation_migration_catalogue_2026_06_07.md`` G3, the
``pipeline_mode_source_batch_live_replay_standardisation_2026_06_05.md`` M5)
a single logical **cell** — ``(asset_group, venue, instrument_type, data_type,
day, [instrument grain])`` — can carry MULTIPLE availability-manifest rows:
one per ``source`` (multi-source provenance) and one per ``pipeline_mode``
(``{batch,live,replay}_<source>``). The data-status CONSUMER must UNION those
rows into ONE honest ``capture_status`` per cell — never double-count raw rows
(which inflates ``captured`` past the de-dup'd denominator), never pick a
single mode/source and hide the rest.

UNION rule (M5): a cell is ``captured`` iff **≥1** source/mode captured it.
The honest per-cell status is the BEST answer any source/mode provides, by the
status precedence below — ``captured`` (data landed) beats ``empty_confirmed``
(a source confirmed legitimately-empty) beats ``attempted_failed`` (tried,
failed) beats ``expected_unattempted`` (never tried). Within
``expected_unattempted`` a known-empty (``EXPECTED_*`` reason) answer beats a
pending-fetch one — the cell is honestly answered.

The mode-CONTEXTUAL precedence (M4: ``live > replay > batch`` for a live
consumer; ``batch > replay > live`` for a batch consumer) decides WHICH mode's
*value* a live/batch reader consumes when several modes captured the same
cell. Data-status is mode-agnostic — it answers "is this cell available from
ANY mode" — so it unions over modes rather than picking one. The live read-path
M4 precedence service lives in ``batch-live-reconciliation-service`` (live-side
track, out of scope here).

This module is the single home for that reduction so the panel rollup
(``data_status_service``) and the hierarchical drilldown
(``data_status_hierarchical``) stay aligned (shard-granularity SSOT). It is a
pure read-side transform — the manifest writer is unchanged.
"""

from __future__ import annotations

import pandas as pd

# Provenance axes we UNION OVER (collapse). A single logical cell carries one
# manifest row per (source, pipeline_mode[, transport]); these columns are the
# difference between rows of the SAME cell, never part of the cell identity.
# ``cadence`` (M8) + ``transport`` (M5b) are observability columns, also unioned.
PROVENANCE_COLS: tuple[str, ...] = ("source", "pipeline_mode", "transport", "cadence")

# The shard-atom (cell) dimension columns. A "cell" is the logical shard the
# system can answer via ANY source/mode. MUST stay aligned with the manifest
# writer row_key (shard-granularity SSOT) — drift is a silent correctness bug.
# Only the subset present in a given manifest is used as the de-dup key.
CELL_DIMENSION_COLS: tuple[str, ...] = (
    "date",
    "asset_group",
    "venue",
    "chain",
    "instrument_type",
    "data_type",
    "instrument_id",
    "underlying",
    "league_id",
    "fixture_id",
    "fixture_type",
    "timeframe",
    "feature_group",
    "feature_group_version",
    "canonical_question_group",
)

_CAPTURE_STATUS_COL = "capture_status"
_ERROR_REASON_COL = "error_reason"
_PIPELINE_MODE_COL = "pipeline_mode"
_SOURCE_COL = "source"
_TRANSPORT_COL = "transport"
_CADENCE_COL = "cadence"
_EXPECTED_PREFIX = "EXPECTED_"
# Legacy / NaN capture_status coerces to ``captured`` — matches UTL
# ``ManifestWriter.lookup`` legacy-read semantics + ``_aggregate_counts``.
_LEGACY_DEFAULT = "captured"

# Union precedence (lower wins). The cell's honest status is the best answer any
# source/mode provides. ``captured`` ⇒ data landed ; ``empty_confirmed`` ⇒ a
# source confirmed empty ; ``attempted_failed`` ⇒ tried + failed ;
# ``expected_unattempted`` ⇒ never tried. Unknown/legacy values rank as
# ``captured`` (the legacy-read default).
_STATUS_RANK: dict[str, int] = {
    "captured": 0,
    "empty_confirmed": 1,
    "attempted_failed": 2,
    "expected_unattempted": 3,
}

# M4 mode-contextual precedence — the TIEBREAK among rows of the SAME status.
# The honest capture_status is ALWAYS the M5 status-union (captured wins
# regardless of mode); M4 only decides WHICH mode's row REPRESENTS the cell
# (its source / error_reason / transport in the reduced row) when several modes
# share the winning status. Data-status uses the live-consumer order
# (live > replay > batch — live is real-time truth, replay fills gaps, batch
# backs history); the symmetric batch-consumer order lives in the live read-path
# resolver (batch-live-reconciliation-service), out of scope here. The
# ``live`` prefix (``_mode_of``) reduces every ``live_<source>`` value (and
# the legacy transitional alias) to the ``live`` mode.
_MODE_RANK: dict[str, int] = {"live": 0, "replay": 1, "batch": 2}
_MODE_RANK_UNKNOWN = 3


def _mode_of(pipeline_mode: str) -> str:
    """Extract the reconciliation mode ({live,replay,batch}) from a
    ``{mode}_{source}`` pipeline_mode (``live_binance`` → ``live``). The
    ``live`` prefix match also reduces the legacy transitional alias to
    ``live``."""
    pm = pipeline_mode.strip().lower()
    if pm.startswith("live"):
        return "live"
    if pm.startswith("replay"):
        return "replay"
    if pm.startswith("batch"):
        return "batch"
    return ""


def cell_key_columns(df: pd.DataFrame) -> list[str]:
    """Return the cell-identity columns present in ``df`` (de-dup key)."""
    return [c for c in CELL_DIMENSION_COLS if c in df.columns]


def has_provenance_columns(df: pd.DataFrame) -> bool:
    """True when ``df`` carries any provenance axis (a v9 multi-row manifest).

    v8 manifests omit ``source`` / ``pipeline_mode`` entirely and already have
    one row per cell — for them the union reduction is a guaranteed no-op, so
    callers skip it to keep v8 behaviour byte-identical.
    """
    return any(c in df.columns for c in PROVENANCE_COLS)


def union_reduce_to_cells(df: pd.DataFrame) -> pd.DataFrame:
    """Collapse multi-(source, pipeline_mode) rows to ONE honest row per cell.

    Each surviving row carries the UNION-winning ``capture_status`` (and the
    matching ``error_reason``) for its cell, per the M5 rule. Idempotent: a
    manifest that already has one row per cell is returned unchanged in shape.
    Returns ``df`` untouched when there is no ``capture_status`` column or no
    cell-key columns to de-dup on.
    """
    if df.empty or _CAPTURE_STATUS_COL not in df.columns:
        return df
    keys = cell_key_columns(df)
    if not keys:
        return df

    status = df[_CAPTURE_STATUS_COL].fillna(_LEGACY_DEFAULT).astype(str).str.lower()  # pyright: ignore[reportUnknownMemberType]
    base_rank = status.map(_STATUS_RANK).fillna(_STATUS_RANK[_LEGACY_DEFAULT]).astype(int)  # pyright: ignore[reportUnknownMemberType,reportUnknownArgumentType]
    reason = (
        df[_ERROR_REASON_COL].fillna("").astype(str)  # pyright: ignore[reportUnknownMemberType]
        if _ERROR_REASON_COL in df.columns
        else pd.Series("", index=df.index)
    )
    # Within ``expected_unattempted``, a known-empty (EXPECTED_*) answer beats a
    # pending-fetch one (the cell IS honestly answered). +1 demotes pending.
    eu_mask = status == "expected_unattempted"
    eu_refine = (eu_mask & ~reason.str.startswith(_EXPECTED_PREFIX)).astype(int)  # pyright: ignore[reportUnknownMemberType]
    # M4 mode-precedence TIEBREAK (live > replay > batch) — applied AFTER status
    # + eu_refine, so it only chooses the REPRESENTATIVE row among rows that
    # share the winning status (never changes the M5 capture_status outcome).
    if _PIPELINE_MODE_COL in df.columns:
        mode_rank = (
            df[_PIPELINE_MODE_COL]  # pyright: ignore[reportUnknownMemberType]
            .fillna("")
            .astype(str)
            .map(lambda pm: _MODE_RANK.get(_mode_of(pm), _MODE_RANK_UNKNOWN))  # pyright: ignore[reportUnknownArgumentType,reportUnknownLambdaType]
            .astype(int)
        )
    else:
        mode_rank = pd.Series(_MODE_RANK_UNKNOWN, index=df.index)
    combined = base_rank * 100 + eu_refine * 10 + mode_rank

    ranked = df.assign(_union_rank=combined)
    reduced = (
        ranked.sort_values("_union_rank", kind="stable")  # pyright: ignore[reportUnknownMemberType]
        .drop_duplicates(subset=keys, keep="first")  # pyright: ignore[reportUnknownMemberType]
        .drop(columns="_union_rank")
        .reset_index(drop=True)
    )
    return reduced


def _provenance_value(series_present: bool, sub: pd.DataFrame, col: str) -> str:
    if not series_present:
        return ""
    val = sub[col].iloc[0]  # pyright: ignore[reportUnknownMemberType,reportUnknownVariableType,reportAny]
    return "" if val is None else str(val)  # pyright: ignore[reportAny]


def provenance_breakdown(df: pd.DataFrame) -> list[dict[str, object]]:
    """Per-(pipeline_mode, source) capture_status CELL counts for a slice.

    The drilldown surface for M5 / M4: shows, for a node/cell, how each
    ``pipeline_mode`` x ``source`` answered it - e.g. ``captured`` via
    ``batch_databento`` + ``replay_databento`` but missing in ``live_databento``.
    Counts are CELL-grain within each (pipeline_mode, source) group (so a source
    that captured a cell across >1 transport counts the cell once). Returns
    ``[]`` when the manifest carries neither provenance column (v8). Sorted
    deterministically by (pipeline_mode, source).
    """
    if df.empty:
        return []
    has_pm = _PIPELINE_MODE_COL in df.columns
    has_src = _SOURCE_COL in df.columns
    if not has_pm and not has_src:
        return []
    has_transport = _TRANSPORT_COL in df.columns
    has_cadence = _CADENCE_COL in df.columns
    group_cols = [c for c in (_PIPELINE_MODE_COL, _SOURCE_COL) if c in df.columns]

    work = df.copy()
    for col in group_cols:
        work[col] = work[col].fillna("").astype(str)  # pyright: ignore[reportUnknownMemberType]

    rows: list[dict[str, object]] = []
    for group_vals, sub in work.groupby(group_cols, dropna=False):  # pyright: ignore[reportUnknownVariableType,reportUnknownMemberType]
        # Reduce within this (pipeline_mode, source) group to cells so a cell
        # spanning >1 transport is not double-counted.
        cells = union_reduce_to_cells(sub)  # pyright: ignore[reportUnknownArgumentType]
        cs = (
            cells[_CAPTURE_STATUS_COL].fillna(_LEGACY_DEFAULT).astype(str).str.lower()  # pyright: ignore[reportUnknownMemberType]
            if _CAPTURE_STATUS_COL in cells.columns
            else pd.Series(_LEGACY_DEFAULT, index=cells.index)
        )
        # groupby on a list of columns yields tuple keys (one element per
        # group column), zipped back to their column names.
        value_by_col = dict(zip(group_cols, (str(v) for v in group_vals), strict=True))  # pyright: ignore[reportAny]
        rows.append(
            {
                "pipeline_mode": value_by_col.get(_PIPELINE_MODE_COL, ""),
                "source": value_by_col.get(_SOURCE_COL, ""),
                "transport": _provenance_value(has_transport, cells, _TRANSPORT_COL),
                "cadence": _provenance_value(has_cadence, cells, _CADENCE_COL),
                "captured": int((cs == "captured").sum()),
                "empty_confirmed": int((cs == "empty_confirmed").sum()),
                "attempted_failed": int((cs == "attempted_failed").sum()),
                "expected_unattempted": int((cs == "expected_unattempted").sum()),
            }
        )
    rows.sort(key=lambda r: (str(r["pipeline_mode"]), str(r["source"])))
    return rows


__all__ = [
    "CELL_DIMENSION_COLS",
    "PROVENANCE_COLS",
    "cell_key_columns",
    "has_provenance_columns",
    "provenance_breakdown",
    "union_reduce_to_cells",
]

"""Axis Value Census — the non-canonical-naming / duplication detector.

``GET /axis-value-census`` — per ``(service, asset_group)``, the RAW
(uncanonicalised) distinct values + row counts of every enumerable manifest
axis (``venue`` / ``chain`` / ``instrument_type`` / ``data_type`` / ``source``
/ ``pipeline_mode``) present in the consolidated availability index.

**Why this exists (operator, 2026-07-18):** "used to list all the instrument
types and data types and chains etc where relevant for this AG that existed
in the gcs data/manifest — this was a good way to check non canonical naming
and duplications but was removed from the ui/api. I really need to add it
back." A 2026-07-18 audit
(``tradfi_consolidated_closeout_2026_07_18.md`` Phase B "Enumeration-driven
migration" todo) re-confirmed the value: the tradfi manifest alone carries 18
distinct ``instrument_type`` spellings with case/plural dupes
(``FUTURE``/``future``/``FUTURES``, ``EQUITY``/``equity``, ...).

**Where it was removed:** ``deployment-api@512180be`` ("canonicalise
instrument_type + collapse/exclude venue duplicates in the data-status
hierarchical drilldown display") taught the hierarchical drilldown
(``services/data_status_hierarchical.py``'s "PIECE A") to COLLAPSE raw
spelling variants (``FUTURE``/``future``/``FUTURES`` -> one node,
bare-vs-``-SOLANA`` venue duplicates merged) at display time — the right call
for that drilldown's UX, but it also means the drilldown can no longer be
used as a drift DETECTOR. This endpoint restores that signal as a standalone
diagnostic read of the same raw manifest, deliberately bypassing PIECE A's
canonicalisation so every raw spelling survives with its own count.

**Backend for an already-shipped UI (Track-6, cross-cutting restoration):**
the frontend half of this exact restoration shipped first, from the parallel
``cefi_consolidated_closeout_2026_07_18.md`` Track-6 —
``deployment-ui@3fb6779`` ("restore Axis Value Census panel") — as a full
``AxisValueCensus`` React component + Vitest specs + a Playwright L2
regression spec + ``fetchAxisValueCensus`` client call + mock-api fixtures,
all built against the contract documented on ``AxisValueCensus`` /
``fetchAxisValueCensus`` in ``deployment-ui/src/api/client.ts`` (path
``GET /data-status/axis-value-census``; response
``{service, asset_group, row_count, axes: Record<str, {value,count}[]>,
truncated_axes: str[]}``) — but that endpoint did not exist anywhere in
deployment-api yet. This module is that missing backend half, matching the
UI's contract byte-for-byte (function/route name, response shape, and the
"top 200 values, else flag in truncated_axes" cap the UI's tooltip already
describes) so the already-shipped panel + its e2e spec light up unmodified.

Single-walk discipline: ONE bounded, column-pruned, in-process-cached read of
the consolidated ``_index/availability_index.parquet`` via
``read_availability_index(bucket, columns=[...])`` — the SAME reader +
in-process TTL cache every other data-status endpoint uses (``_ds
._read_availability_index``), never a fresh whole-corpus GCS walk.
Honest-absence: mirrors ``_catalogue.py``'s ``_distinct_values`` convention
(the reader backfills every requested column to ``""`` for legacy rows that
predate it, so "column missing from this bucket's schema" and "column present
but 100% blank" are indistinguishable through this reader) — every requested
axis is always present in the response, resolving to ``[]`` when blank.

Split as a new submodule (mirrors ``_catalogue.py``'s P6 precedent — attaches
to the shared package ``router``, see ``routes/data_status/__init__.py``'s
import block for the registration convention).

Plan: ``tradfi_consolidated_closeout_2026_07_18.md`` Phase C "RE-ADD the
data-status dimensions enumeration view" /
``cefi_consolidated_closeout_2026_07_18.md`` Track-6.
"""

from __future__ import annotations

import logging

import pandas as pd
from fastapi import HTTPException, Query

import deployment_api.routes.data_status as _ds
from deployment_api.routes.data_status import router

logger = logging.getLogger(__name__)

# The manifest axes surfaced by this audit panel — every one is a genuine
# ``availability_index`` column per the v8/v9 schema (SSOT:
# ``unified_trading_library/manifest_writer/_read_index.py::_V8_COLUMNS``).
# The deployment-ui ``AxisValueCensus`` panel (``AXIS_ORDER``) currently
# renders only ``venue`` / ``chain`` / ``instrument_type`` / ``data_type``;
# ``source`` / ``pipeline_mode`` are included here too (the operator's
# original ask, and the exact axis a 2026-07-18 audit caught drifting —
# stale ``source=barchart`` despite Barchart being retired) so the API stays
# ahead of the UI and future axis additions there need no backend change.
AXIS_CENSUS_COLUMNS: tuple[str, ...] = (
    "venue",
    "chain",
    "instrument_type",
    "data_type",
    "source",
    "pipeline_mode",
)

# Sentinel spellings that mean "no value was recorded" — never counted as a
# real distinct value (honest-absence; matches ``_catalogue.py::_distinct_
# values``'s blank-drop convention, extended with the "None"/"nan" literals
# the reader's own column-backfill can introduce).
_BLANK_SENTINELS: frozenset[str] = frozenset({"", "none", "nan", "<na>"})

# Per-axis cap on returned distinct values — matches the deployment-ui
# ``AxisValueCensus`` panel's already-shipped tooltip text ("Only the top 200
# values are shown"). A capped axis's name is added to ``truncated_axes`` so
# the UI can render its "truncated" badge; the kept values are always the
# highest-count ones (the biggest duplication clusters), never an arbitrary
# slice.
_MAX_VALUES_PER_AXIS = 200


def _axis_value_counts(df: pd.DataFrame, column: str) -> list[dict[str, object]]:
    """Raw distinct values + row counts of one manifest column, DESCENDING by
    count (the biggest duplication clusters surface first, and are what
    survives the ``_MAX_VALUES_PER_AXIS`` cap).

    Deliberately NOT display-canonicalised — every raw spelling variant
    (``FUTURE``/``future``/``FUTURES``) survives as its own entry; that is the
    entire point of an audit panel meant to catch non-canonical naming +
    duplication, as opposed to ``data_status_hierarchical.py``'s PIECE A
    (which merges those same variants for END-USER display) or
    ``_catalogue.py``'s ``_distinct_values`` (a sorted name list for a filter
    dropdown, no counts). Honest-absence: an absent or entirely-blank column
    returns ``[]``.
    """
    if df.empty or column not in df.columns:
        return []
    series = df[column].astype(str).str.strip()
    series = series[~series.str.lower().isin(_BLANK_SENTINELS)]
    if series.empty:
        return []
    counts = series.value_counts()
    return [{"value": str(value), "count": int(count)} for value, count in counts.items()]  # pyright: ignore[reportUnknownArgumentType,reportUnknownVariableType]


@router.get("/axis-value-census")
async def get_axis_value_census(
    service: str = Query(..., description="Service name"),
    asset_group: str = Query(..., description="Asset group"),
) -> dict[str, object]:
    """Raw distinct values + counts of every enumerable manifest axis for
    ``(service, asset_group)`` — the canonical-naming-drift audit panel
    backing the deployment-ui ``AxisValueCensus`` component.

    Returns, per axis in :data:`AXIS_CENSUS_COLUMNS`, a list of
    ``{"value": <raw string>, "count": <row count>}`` sorted by count
    descending, capped at :data:`_MAX_VALUES_PER_AXIS` (the capped axis name
    is then also listed in ``truncated_axes``). The UI/consumer is
    responsible for flagging likely case/plural collisions (e.g.
    ``FUTURE``/``future``/``FUTURES`` all present for the same asset_group) —
    this endpoint intentionally returns the raw values undecided, not a
    pre-collapsed verdict.
    """
    try:
        bucket = _ds.build_bucket_name(service, asset_group)
        df = _ds._read_availability_index(bucket, columns=list(AXIS_CENSUS_COLUMNS))  # pyright: ignore[reportPrivateUsage]
    except (OSError, RuntimeError, ValueError) as exc:
        logger.exception("Error in get_axis_value_census")
        raise HTTPException(status_code=500, detail="Internal server error. Check server logs.") from exc

    axes: dict[str, list[dict[str, object]]] = {}
    truncated_axes: list[str] = []
    for axis in AXIS_CENSUS_COLUMNS:
        values = _axis_value_counts(df, axis)
        if len(values) > _MAX_VALUES_PER_AXIS:
            values = values[:_MAX_VALUES_PER_AXIS]
            truncated_axes.append(axis)
        axes[axis] = values

    return {
        "service": service,
        "asset_group": asset_group.lower(),
        "row_count": len(df),
        "axes": axes,
        "truncated_axes": truncated_axes,
    }

"""Axis Value Census — the non-canonical-naming / duplication detector.

``GET /axis-value-census`` — per ``(service, asset_group)``, the RAW
(uncanonicalised) distinct values + row counts of every enumerable manifest
axis (``venue`` / ``chain`` / ``instrument_type`` / ``data_type`` / ``source``
/ ``pipeline_mode`` / ``timeframe``) present in the consolidated availability
index.

**MDPS candle-layer extension (candle_feature issue todo 42, 2026-07-21):**
MTDS (raw ticks) and MDPS (derived candles) share the same per-asset_group
bucket, so a plain unfiltered read of ``service=="market-data-processing-
service"`` silently returned the MTDS raw-tick census instead. Requesting
this service now filters the read to MDPS's own ``service_name`` rows (see
:func:`_filter_to_candle_rows`) and badges the ``data_type`` axis
``is_canonical`` against the SOURCE ``DATA_TYPES_BY_ASSET_GROUP`` vocabulary
(see :func:`_badge_data_type_against_source`) — NOT the aggregated
``mdps_data_type_key`` vocabulary, per the corrected ruling in
``candle_feature_canonical_path_divergence_2026_07_20.md`` that keeps the
candle object path's ``data_type`` on SOURCE.

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
from unified_api_contracts.registry import DATA_TYPES_BY_ASSET_GROUP

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
# ``timeframe`` (candle_feature issue todo 42, 2026-07-21) makes the census
# useful on the MDPS candle layer, whose shard atom includes a timeframe the
# raw-tick axes above never carried.
AXIS_CENSUS_COLUMNS: tuple[str, ...] = (
    "venue",
    "chain",
    "instrument_type",
    "data_type",
    "source",
    "pipeline_mode",
    "timeframe",
)

# MTDS (raw ticks) and MDPS (derived candles) share the SAME per-asset_group
# "market-data" bucket (``SERVICE_TO_KIND`` in
# ``data_status_drilldown/_core.py``) — the availability_index rows for both
# live side by side, disambiguated only by the ``service_name`` column. A
# census requested for MTDS therefore already reads ~all rows in the bucket
# (MDPS's handful of candle rows are noise), but a census requested for MDPS
# must filter to ``service_name`` == this value or it silently returns the
# MTDS raw-tick census instead of the candle one (candle_feature issue todo
# 42, 2026-07-21).
_CANDLE_SERVICE_NAME = "market-data-processing-service"

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


def _filter_to_candle_rows(df: pd.DataFrame) -> pd.DataFrame:
    """Restrict ``df`` to the MDPS candle rows in a shared MTDS/MDPS bucket.

    ``service_name`` is always present in ``df`` here (the caller requests it
    as an extra read column specifically for this filter — never rendered as
    an axis itself), backfilled to ``""`` for legacy rows that predate the
    column; those legacy rows correctly fail this exact-match and are
    excluded, since they cannot be attributed to MDPS.
    """
    if df.empty or "service_name" not in df.columns:
        return df.iloc[0:0]
    return df[df["service_name"] == _CANDLE_SERVICE_NAME]


def _badge_data_type_against_source(entries: list[dict[str, object]], asset_group: str) -> list[dict[str, object]]:
    """Badge each ``data_type`` census entry ``is_canonical`` against the
    SOURCE ``DATA_TYPES_BY_ASSET_GROUP`` vocabulary.

    MDPS candle object paths carry the SOURCE ``data_type``
    (``derivative_ticker``/``trades``/...), never the aggregated
    ``mdps_data_type_key`` (``deriv_ohlcv_15m``) — the corrected ruling in
    ``candle_feature_canonical_path_divergence_2026_07_20.md`` ("CORRECTED
    RULING 2026-07-21") keeps the path on SOURCE and re-points the MANIFEST to
    match it instead. Badging against the aggregated vocabulary here would
    reintroduce the superseded framing and flag every genuinely-canonical
    candle row as drift.
    """
    canonical = frozenset(DATA_TYPES_BY_ASSET_GROUP.get(asset_group.lower(), []))
    return [{**entry, "is_canonical": entry["value"] in canonical} for entry in entries]


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

    When ``service == "market-data-processing-service"`` (the MDPS candle
    layer, which shares its bucket with MTDS raw-tick rows) the manifest read
    is additionally filtered to ``service_name`` rows attributable to MDPS
    (see :func:`_filter_to_candle_rows`), and the ``data_type`` axis entries
    each carry an extra ``is_canonical`` flag badged against the SOURCE
    ``DATA_TYPES_BY_ASSET_GROUP`` vocabulary (see
    :func:`_badge_data_type_against_source`) — every other ``(service,
    asset_group)`` keeps the plain, unbadged ``{value, count}`` shape.
    """
    is_candle_census = service == _CANDLE_SERVICE_NAME
    read_columns = [*AXIS_CENSUS_COLUMNS, "service_name"] if is_candle_census else list(AXIS_CENSUS_COLUMNS)
    try:
        bucket = _ds.build_bucket_name(service, asset_group)
        df = _ds._read_availability_index(bucket, columns=read_columns)  # pyright: ignore[reportPrivateUsage]
    except (OSError, RuntimeError, ValueError) as exc:
        logger.exception("Error in get_axis_value_census")
        raise HTTPException(status_code=500, detail="Internal server error. Check server logs.") from exc

    if is_candle_census:
        df = _filter_to_candle_rows(df)

    axes: dict[str, list[dict[str, object]]] = {}
    truncated_axes: list[str] = []
    for axis in AXIS_CENSUS_COLUMNS:
        values = _axis_value_counts(df, axis)
        if axis == "data_type" and is_candle_census:
            values = _badge_data_type_against_source(values, asset_group)
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

"""Availability-derived instrument catalogue explorer (P6 phase-1).

``GET /catalogue`` (+ ``/download-catalogue-csv`` twin) — a de-duped,
cross-day instrument list, pinned by ``(service, asset_group)`` and narrowed
by optional ``venue`` / ``instrument_type`` / ``data_type``. This is
DELIBERATELY NOT "the catalogue" — deployment-api cannot reach the
instruments-service ``InstrumentCatalogReader`` SSOT (T4 — no service→service
imports) — so every response is labeled "captured instruments
(availability-derived)": what we've actually captured, not what the venue
could theoretically list. A true-catalogue projection is P6 phase-2.

**Two-source split (fixed 2026-07-17 — real-data bug: cefi/defi/tradfi
returned ``total_count=0``).** The per-row source differs by asset_group,
because the availability manifest's shard atom differs:

* **prediction / sports** — ``_index/availability_index.parquet`` IS
  per-entity (a genuine ``instrument_id``/``league_id`` column per row), so
  these two read it directly via ``_read_availability_index`` (unchanged).
* **cefi / defi / tradfi** — the ``_index`` shard atom is the VENUE (no
  per-instrument rows there at all), so there is no ``instrument_id`` to key
  on. These three instead read the per-asset-group identity catalogue
  ``prod/catalog.parquet`` (the SAME file ``catalogue_lifecycle.py`` /
  ``manifest_source.read_unique_instrument_count`` already read for the
  new-listings/expiries panels + the unique-instrument count) — see
  ``_read_identity_catalogue``. It carries no live per-day ``capture_status``/
  ``error_reason``/``attempted_at`` (those are manifest-only, per-shard
  fields), so those three fields honestly default to
  ``captured``/``""``/``""`` for identity-catalogue rows (never fabricated —
  ``prod/catalog.parquet`` only ever contains instruments captured at least
  once). Its own precomputed ``mvp`` column is used directly for ``is_mvp``
  (rather than re-derived via ``is_mvp_for_manifest_row``, which needs
  manifest-only axis columns this file doesn't carry) — mirrors
  ``catalogue_lifecycle.py``'s ``_row()`` reading ``rec.get("mvp")`` as-is.

Single-walk discipline: both paths are ONE bounded GCS read per request
(``_read_availability_index`` or one ``download_bytes`` of
``prod/catalog.parquet``) — never a whole-corpus GCS parquet walk.

Split as a new submodule (not folded into ``_query_meta.py``) attaching to the
shared package ``router`` — see ``routes/data_status/__init__.py`` import
block for the registration convention.

Plan: ``data_status_page_ux_and_canonicalisation_2026_07_16.md`` P6.
"""

from __future__ import annotations

import io
import logging

import pandas as pd
from fastapi import HTTPException, Query
from fastapi.responses import Response

import deployment_api.routes.data_status as _ds
from deployment_api.routes.data_status import router
from deployment_api.routes.data_status._coverage_scope import is_mvp_for_manifest_row
from deployment_api.utils.storage_client import get_storage_client

logger = logging.getLogger(__name__)

CATALOGUE_LABEL = "captured instruments (availability-derived)"

DEFAULT_CATALOGUE_LIMIT = 50
MAX_CATALOGUE_LIMIT = 500
MAX_CATALOGUE_SEARCH_RESULTS = 500

# cefi/defi/tradfi: the availability ``_index`` is VENUE-level (no per-instrument
# rows) — read the per-instrument identity catalogue instead. prediction/sports
# are NOT in this set (their ``_index`` already carries a genuine per-row
# instrument_id/league_id and keeps working unchanged).
#
# DO NOT ADD "sports" (or "prediction") HERE. This has been prototyped and reverted
# more than once — it LOOKS like a consistency win and is a real regression. Measured
# against the live sports ``prod/catalog.parquet`` (2026-07-17, 27,250 rows):
#   1. ``venue`` is BLANK on 27,250/27,250 rows = 100.0% — the explorer's venue narrow
#      would silently return nothing for every sports venue filter.
#   2. There is NO ``capture_status`` column — the identity path defaults that field to
#      ``captured``, so all ~27k rows would be stamped "captured" from a file that has
#      no capture evidence at all. That FABRICATES a status (honest-absence violation,
#      ``codex/02-data/honest-absence-downstream-handling.md``). The default is only
#      honest for cefi/defi/tradfi, whose catalog rows are captured-at-least-once.
#   3. ``instrument_type`` is lowercase legacy (fixture/player/team/league), not the
#      UPPERCASE canonical vocabulary this endpoint's type narrow matches on.
# The regression test ``test_sports_not_in_identity_catalogue_asset_groups`` guards this.
_IDENTITY_CATALOGUE_ASSET_GROUPS: frozenset[str] = frozenset({"cefi", "defi", "tradfi"})

# Columns projected from ``prod/catalog.parquet`` for this endpoint (schema-aware
# — only present columns are read, same graceful-degrade pattern as
# ``catalogue_lifecycle.py``/``prediction_catalogue.py``'s own per-consumer
# column subsets over the SAME file).
_IDENTITY_CATALOGUE_READ_COLUMNS: list[str] = [
    "instrument_id",
    "venue",
    "instrument_type",
    "data_type",
    "base_asset",
    "mvp",
]

# Columns surfaced per catalogue row (in order) on the CSV twin.
_CATALOGUE_CSV_COLUMNS: list[str] = [
    "instrument_id",
    "venue",
    "instrument_type",
    "data_type",
    "capture_status",
    "error_reason",
    "attempted_at",
    "is_mvp",
]


def _identity_catalogue_bucket(asset_group: str, *, cloud: str = "gcp") -> str:
    """Resolve the identity-catalogue bucket for ``asset_group``.

    Deliberately asset_group-scoped, NOT ``(service, asset_group)`` like
    ``_ds.build_bucket_name`` — ``prod/catalog.parquet`` lives in the
    instruments-service bucket regardless of which service tab the caller is
    viewing (mirrors ``catalogue_lifecycle.py::_catalogue_bucket``)."""
    from unified_trading_library import resolve_bucket_name  # noqa: qg-inside-import

    return resolve_bucket_name(cloud=cloud, kind="instruments-store", asset_group=asset_group)


def _read_identity_catalogue(asset_group: str) -> pd.DataFrame:
    """Read ``prod/catalog.parquet`` (ONE bounded GCS GET) for cefi/defi/tradfi.

    This is the per-instrument IDENTITY-level source — the fix for the
    real-data bug where these three asset groups' venue-level ``_index`` has
    no ``instrument_id`` column at all. Empty DataFrame on any failure
    (missing bucket/object, unreadable parquet, no ``instrument_id`` column)
    — shard-isolated: never raises, the caller degrades to an honest empty
    catalogue for this asset_group (same "never raise" contract as
    ``catalogue_lifecycle.py::_read_catalogue`` /
    ``prediction_catalogue.py::_read_catalogue`` reading the SAME file)."""
    import pyarrow.parquet as pq  # noqa: imports-inside-functions — lazy heavy SDK

    try:
        bucket = _identity_catalogue_bucket(asset_group)
        raw = get_storage_client().download_bytes(bucket, "prod/catalog.parquet")
        schema_names = set(pq.read_schema(io.BytesIO(raw)).names)
        present = [c for c in _IDENTITY_CATALOGUE_READ_COLUMNS if c in schema_names]
        if "instrument_id" not in present:
            return pd.DataFrame()
        df = pd.read_parquet(io.BytesIO(raw), columns=present)
    except Exception as exc:
        # Shard-isolated: a bad asset_group must not sink this request.
        logger.warning(
            "catalogue: prod/catalog.parquet read failed for asset_group=%s (%s) — returning empty",
            asset_group,
            exc,
        )
        return pd.DataFrame()
    return df


def _load_catalogue_frame(
    *,
    service: str,
    asset_group: str,
    venue: str | None,
    instrument_type: str | None,
    data_type: str | None,
) -> pd.DataFrame:
    """Load the per-instrument source for ``(service, asset_group)`` and
    narrow to the requested axes. ONE bounded read either way — no GCS
    parquet walk (single-walk discipline). See module docstring for the
    two-source split rationale."""
    ag = asset_group.lower()
    if ag in _IDENTITY_CATALOGUE_ASSET_GROUPS:
        df = _read_identity_catalogue(ag)
    else:
        bucket = _ds.build_bucket_name(service, asset_group)
        df = _ds._read_availability_index(bucket)  # pyright: ignore[reportPrivateUsage]
    if df.empty or "instrument_id" not in df.columns:
        return df

    mask = pd.Series(data=True, index=df.index)
    if venue and "venue" in df.columns:
        mask &= df["venue"] == venue
    if instrument_type and "instrument_type" in df.columns:
        requested_it = instrument_type.upper()
        mask &= df["instrument_type"].astype(str).str.upper() == requested_it
    if data_type and "data_type" in df.columns:
        mask &= df["data_type"].astype(str).str.upper() == data_type.upper()
    return df[mask]


def _dedupe_latest(df: pd.DataFrame) -> pd.DataFrame:
    """One row per ``instrument_id`` — the most-recently-written manifest row
    wins (same dedup semantics as ``_core.py::_scoped_manifest_rows``)."""
    if df.empty or "instrument_id" not in df.columns:
        return df
    ordered = df.sort_values("written_at") if "written_at" in df.columns else df
    return ordered.drop_duplicates(subset=["instrument_id"], keep="last")


def _distinct_values(df: pd.DataFrame, column: str) -> list[str]:
    """Sorted distinct NON-BLANK string values of ``column`` for the filter
    dropdowns (F3, live UI review round 3 2026-07-17).

    Returns ``[]`` when the column is absent or entirely blank — honest-absence:
    e.g. the sports catalogue's ``venue`` is 100% blank (see the module-level
    ``_IDENTITY_CATALOGUE_ASSET_GROUPS`` note), so its venue dropdown offers only
    the "any" default rather than a fabricated option. Values are returned exactly
    as stored so a selected ``venue`` matches the catalogue narrow's case-sensitive
    ``==`` (``instrument_type``/``data_type`` narrow upper-cases both sides, so
    their casing is immaterial)."""
    if df.empty or column not in df.columns:
        return []
    series = df[column].dropna().astype(str).str.strip()
    return sorted({value for value in series if value})


def _row_is_mvp(row: pd.Series, asset_group: str, *, identity_catalogue: bool) -> bool:  # type: ignore[type-arg]
    """Per-row ``is_mvp`` — sourced differently depending on which frame the
    row came from (see module docstring's two-source split).

    Identity-catalogue rows (cefi/defi/tradfi) carry their OWN precomputed
    ``mvp`` boolean (IS stamps it at roll-up time via the same UAC ``is_mvp``
    predicate, over the FULL instrument context) — read it directly, mirroring
    ``catalogue_lifecycle.py``'s ``_row()`` doing the same
    (``bool(mvp_raw) if mvp_raw is not None and not _pandas_missing(mvp_raw)
    else False``). Manifest rows (prediction/sports) have no such column, so
    they keep recomputing via ``is_mvp_for_manifest_row`` (unchanged
    behaviour)."""
    if identity_catalogue:
        val = row.get("mvp")
        if val is None:
            return False
        try:
            if pd.isna(val):  # type: ignore[reportArgumentType]
                return False
        except (TypeError, ValueError):
            pass
        return bool(val)
    return is_mvp_for_manifest_row(row, asset_group)  # pyright: ignore[reportUnknownArgumentType]


_IS_MVP_COL: str = "_is_mvp"


def _is_mvp_series(df: pd.DataFrame, asset_group: str, *, identity_catalogue: bool) -> pd.Series:  # type: ignore[type-arg]
    """``is_mvp`` for EVERY row of ``df``, as a column — the vectorised twin of
    ``_row_is_mvp`` (which stays the per-row SSOT for the semantics).

    Identity-catalogue rows carry a precomputed ``mvp`` boolean, so this is a
    cheap column read (the whole point — it takes tradfi's 1.17M rows off the
    per-row path). Manifest-backed rows (prediction/sports) have no such column
    and must still recompute per row via ``is_mvp_for_manifest_row``; that is
    the same cost those asset groups already paid, so no regression."""
    if df.empty:
        return pd.Series(data=False, index=df.index, dtype=bool)
    if identity_catalogue:
        if "mvp" not in df.columns:
            return pd.Series(data=False, index=df.index, dtype=bool)
        return df["mvp"].map(_truthy_mvp).astype(bool)

    def _manifest_row_is_mvp(row: pd.Series) -> bool:  # type: ignore[type-arg]
        return is_mvp_for_manifest_row(row, asset_group)  # pyright: ignore[reportUnknownArgumentType]

    return df.apply(_manifest_row_is_mvp, axis=1).astype(bool)  # pyright: ignore[reportUnknownMemberType,reportUnknownVariableType]


def _truthy_mvp(val: object) -> bool:
    """Same coercion as ``_row_is_mvp``'s identity branch (NA/None -> False)."""
    if val is None:
        return False
    try:
        if pd.isna(val):  # type: ignore[reportArgumentType]
            return False
    except (TypeError, ValueError):
        pass
    return bool(val)


def _prepare_catalogue_frame(
    df: pd.DataFrame,
    *,
    asset_group: str,
    search: str | None,
    mvp_only: bool,
    identity_catalogue: bool,
) -> pd.DataFrame:
    """Narrow + tag + ORDER the frame so that every remaining row is a result
    row, in final response order — WITHOUT building any row dicts.

    This is the pagination enabler: ``len()`` of the result is the total_count
    and ``.iloc[offset:offset+limit]`` is the page, so only the page ever gets
    materialised into dicts (previously all 1.17M tradfi rows were built to
    show 50).

    **Operation order is load-bearing and mirrors the pre-pagination code
    exactly** (mvp_only filter -> search filter in frame order -> cap at
    ``MAX_CATALOGUE_SEARCH_RESULTS`` -> sort by instrument_id). The cap lands
    BEFORE the sort, so a capped search returns the first N matches in frame
    order and then sorts those — changing this order would silently change which
    rows a capped search returns."""
    if df.empty or "instrument_id" not in df.columns:
        return df

    out = df.copy()
    iid = out["instrument_id"].astype(str).str.strip()
    out = out[iid != ""]
    if out.empty:
        return out

    out[_IS_MVP_COL] = _is_mvp_series(out, asset_group, identity_catalogue=identity_catalogue)
    if mvp_only:
        out = out[out[_IS_MVP_COL]]

    if search:
        needle = search.strip().lower()
        if needle:
            hit = out["instrument_id"].astype(str).str.lower().str.contains(needle, regex=False, na=False)
            out = out[hit].head(MAX_CATALOGUE_SEARCH_RESULTS)

    return out.sort_values("instrument_id", kind="stable")


def _rows_from_frame(df: pd.DataFrame, *, asset_group: str, identity_catalogue: bool) -> list[dict[str, object]]:
    """Materialise catalogue row dicts from an ALREADY-prepared frame slice.

    Callers pass either the whole prepared frame (CSV export — must carry every
    filtered row) or just the requested page (JSON route). Filtering/ordering is
    NOT redone here — ``_prepare_catalogue_frame`` owns it, so both callers are
    structurally guaranteed to agree on rows + order."""
    if df.empty or "instrument_id" not in df.columns:
        return []

    out: list[dict[str, object]] = []
    for _, row in df.iterrows():  # pyright: ignore[reportUnknownVariableType,reportUnknownMemberType]
        iid = str(row.get("instrument_id") or "").strip()  # pyright: ignore[reportUnknownMemberType,reportUnknownArgumentType]
        if not iid:
            continue
        row_is_mvp = (
            _truthy_mvp(row.get(_IS_MVP_COL))  # pyright: ignore[reportUnknownMemberType,reportUnknownArgumentType]
            if _IS_MVP_COL in df.columns
            else _row_is_mvp(row, asset_group, identity_catalogue=identity_catalogue)  # pyright: ignore[reportUnknownArgumentType]
        )
        out.append(
            {
                "instrument_id": iid,
                "venue": str(row.get("venue") or ""),  # pyright: ignore[reportUnknownMemberType,reportUnknownArgumentType]
                "instrument_type": str(row.get("instrument_type") or ""),  # pyright: ignore[reportUnknownMemberType,reportUnknownArgumentType]
                "data_type": str(row.get("data_type") or ""),  # pyright: ignore[reportUnknownMemberType,reportUnknownArgumentType]
                "capture_status": str(row.get("capture_status") or "captured").lower(),  # pyright: ignore[reportUnknownMemberType,reportUnknownArgumentType]
                "error_reason": str(row.get("error_reason") or ""),  # pyright: ignore[reportUnknownMemberType,reportUnknownArgumentType]
                "attempted_at": str(row.get("attempted_at") or ""),  # pyright: ignore[reportUnknownMemberType,reportUnknownArgumentType]
                "is_mvp": row_is_mvp,
            }
        )
    return out


def _catalogue_rows(
    df: pd.DataFrame,
    *,
    asset_group: str,
    search: str | None,
    mvp_only: bool,
    identity_catalogue: bool,
) -> list[dict[str, object]]:
    """Project a de-duped source slice into catalogue row dicts (ALL rows).

    Retained as the full-list path (CSV export); the JSON route uses the
    paginated ``_build_catalogue_page`` instead."""
    prepared = _prepare_catalogue_frame(
        df,
        asset_group=asset_group,
        search=search,
        mvp_only=mvp_only,
        identity_catalogue=identity_catalogue,
    )
    return _rows_from_frame(prepared, asset_group=asset_group, identity_catalogue=identity_catalogue)


def _build_catalogue_rows(
    *,
    service: str,
    asset_group: str,
    venue: str | None,
    instrument_type: str | None,
    data_type: str | None,
    search: str | None,
    mvp_only: bool,
) -> list[dict[str, object]]:
    """Shared build path for both the JSON route + the CSV twin — guarantees
    the CSV export matches the on-screen list exactly (same rows, same order,
    same is_mvp/search/mvp_only semantics)."""
    df = _load_catalogue_frame(
        service=service,
        asset_group=asset_group,
        venue=venue,
        instrument_type=instrument_type,
        data_type=data_type,
    )
    deduped = _dedupe_latest(df)
    identity_catalogue = asset_group.lower() in _IDENTITY_CATALOGUE_ASSET_GROUPS
    return _catalogue_rows(
        deduped,
        asset_group=asset_group,
        search=search,
        mvp_only=mvp_only,
        identity_catalogue=identity_catalogue,
    )


def _build_catalogue_page(
    *,
    service: str,
    asset_group: str,
    venue: str | None,
    instrument_type: str | None,
    data_type: str | None,
    search: str | None,
    mvp_only: bool,
    limit: int,
    offset: int,
) -> tuple[list[dict[str, object]], int]:
    """``(page_rows, total_count)`` — the paginated twin of
    ``_build_catalogue_rows``, for the JSON route.

    Identical source/narrow/dedupe/order pipeline; the ONLY difference is that
    dict-building happens after the page slice, so a 50-row page costs 50 row
    builds instead of the whole asset group. ``total_count`` is still the full
    filtered count (a frame ``len()``, not a build), so pagination metadata is
    unchanged. Single-walk discipline unaffected — still ONE bounded GCS read."""
    df = _load_catalogue_frame(
        service=service,
        asset_group=asset_group,
        venue=venue,
        instrument_type=instrument_type,
        data_type=data_type,
    )
    deduped = _dedupe_latest(df)
    identity_catalogue = asset_group.lower() in _IDENTITY_CATALOGUE_ASSET_GROUPS
    prepared = _prepare_catalogue_frame(
        deduped,
        asset_group=asset_group,
        search=search,
        mvp_only=mvp_only,
        identity_catalogue=identity_catalogue,
    )
    total_count = len(prepared)
    page_frame = prepared.iloc[offset : offset + limit]
    page = _rows_from_frame(page_frame, asset_group=asset_group, identity_catalogue=identity_catalogue)
    return page, total_count


@router.get("/catalogue")
async def get_instrument_catalogue(
    service: str = Query(..., description="Service name"),
    asset_group: str = Query(..., description="Asset group"),
    venue: str | None = Query(None, description="Optional venue narrow"),
    instrument_type: str | None = Query(None, description="Optional instrument_type narrow"),
    data_type: str | None = Query(None, description="Optional data_type narrow"),
    search: str | None = Query(None, description="Case-insensitive substring match on instrument_id"),
    mvp_only: bool = Query(False, description="Restrict to is_mvp-true instruments"),
    limit: int = Query(DEFAULT_CATALOGUE_LIMIT, ge=1, le=MAX_CATALOGUE_LIMIT, description="Page size"),
    offset: int = Query(0, ge=0, description="Page offset"),
) -> dict[str, object]:
    """De-duped, availability-derived instrument list across every captured day.

    Pinned by ``(service, asset_group)`` (resolves the manifest bucket);
    ``venue`` / ``instrument_type`` / ``data_type`` optionally narrow further.
    Every response is explicitly labeled ``"captured instruments
    (availability-derived)"`` (the ``label`` field) — NOT "the catalogue":
    deployment-api has no access to the instruments-service catalogue SSOT
    (T4), so this only reflects instruments the availability manifest has
    ever recorded a row for, de-duped to their most-recently-written state.

    Response shape mirrors ``/instruments-for-shard``:
    ``{instruments, total_count, limit, offset, has_more, search, mvp_only,
    label, ...}``. Every instrument dict carries ``is_mvp`` + ``capture_status``
    regardless of the ``mvp_only`` toggle.
    """
    safe_limit = max(1, min(int(limit or DEFAULT_CATALOGUE_LIMIT), MAX_CATALOGUE_LIMIT))
    safe_offset = max(0, int(offset or 0))
    try:
        page, total_count = _build_catalogue_page(
            service=service,
            asset_group=asset_group,
            venue=venue,
            instrument_type=instrument_type,
            data_type=data_type,
            search=search,
            mvp_only=mvp_only,
            limit=safe_limit,
            offset=safe_offset,
        )
    except (OSError, RuntimeError, ValueError) as exc:
        logger.exception("Error in get_instrument_catalogue")
        raise HTTPException(status_code=500, detail="Internal server error. Check server logs.") from exc

    return {
        "service": service,
        "asset_group": asset_group.lower(),
        "venue": venue,
        "instrument_type": instrument_type,
        "data_type": data_type,
        "label": CATALOGUE_LABEL,
        "instruments": page,
        "total_count": total_count,
        "limit": safe_limit,
        "offset": safe_offset,
        "has_more": (safe_offset + len(page)) < total_count,
        "search": (search or "").strip(),
        "mvp_only": mvp_only,
    }


@router.get("/catalogue-filter-options")
async def get_catalogue_filter_options(
    service: str = Query(..., description="Service name"),
    asset_group: str = Query(..., description="Asset group"),
) -> dict[str, object]:
    """Distinct venue / instrument_type / data_type values present in the
    ``(service, asset_group)`` catalogue — populates the Catalogue Explorer's
    filter dropdowns so the operator selects a real value instead of typing one
    (F3, live UI review round 3 2026-07-17).

    Single-walk: reads the SAME source ``/catalogue`` reads (unnarrowed,
    column-pruned, ONE bounded GCS GET), de-dupes to latest-per-instrument (so a
    value only ever appears if selecting it would return rows), and returns the
    sorted distinct non-blank values per axis. An axis whose column is absent or
    all-blank returns ``[]`` (honest-absence) — the UI then offers only the "any"
    default for that axis.
    """
    try:
        df = _load_catalogue_frame(
            service=service,
            asset_group=asset_group,
            venue=None,
            instrument_type=None,
            data_type=None,
        )
        deduped = _dedupe_latest(df)
    except (OSError, RuntimeError, ValueError) as exc:
        logger.exception("Error in get_catalogue_filter_options")
        raise HTTPException(status_code=500, detail="Internal server error. Check server logs.") from exc

    return {
        "service": service,
        "asset_group": asset_group.lower(),
        "venues": _distinct_values(deduped, "venue"),
        "instrument_types": _distinct_values(deduped, "instrument_type"),
        "data_types": _distinct_values(deduped, "data_type"),
    }


@router.get("/download-catalogue-csv")
async def download_catalogue_csv(
    service: str = Query(..., description="Service name"),
    asset_group: str = Query(..., description="Asset group"),
    venue: str | None = Query(None, description="Optional venue narrow"),
    instrument_type: str | None = Query(None, description="Optional instrument_type narrow"),
    data_type: str | None = Query(None, description="Optional data_type narrow"),
    search: str | None = Query(None, description="Case-insensitive substring match on instrument_id"),
    mvp_only: bool = Query(False, description="Restrict the export to is_mvp-true instruments"),
) -> Response:
    """Stream the availability-derived catalogue as CSV — matches ``/catalogue``
    (same search + mvp_only filter, same shared row-builder) exactly."""
    try:
        rows = _build_catalogue_rows(
            service=service,
            asset_group=asset_group,
            venue=venue,
            instrument_type=instrument_type,
            data_type=data_type,
            search=search,
            mvp_only=mvp_only,
        )
    except (OSError, RuntimeError, ValueError) as exc:
        logger.exception("Error in download_catalogue_csv")
        raise HTTPException(status_code=500, detail="Internal server error. Check server logs.") from exc

    csv_df = pd.DataFrame(data=rows or None, columns=_CATALOGUE_CSV_COLUMNS)
    csv_text = csv_df.to_csv(index=False)
    filename = f"{service}_{asset_group}_catalogue.csv"
    headers = {
        "Content-Disposition": f'attachment; filename="{filename}"',
        "X-Row-Count": str(len(rows)),
    }
    return Response(content=csv_text, media_type="text/csv; charset=utf-8", headers=headers)

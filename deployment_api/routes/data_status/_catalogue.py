"""Availability-derived instrument catalogue explorer (P6 phase-1).

``GET /catalogue`` (+ ``/download-catalogue-csv`` twin) — a de-duped,
cross-day instrument list built ONLY from the availability manifest
(``read_availability_index``), pinned by ``(service, asset_group)`` and
narrowed by optional ``venue`` / ``instrument_type`` / ``data_type``. This is
DELIBERATELY NOT "the catalogue" — deployment-api cannot reach the
instruments-service ``InstrumentCatalogReader`` SSOT (T4 — no service→service
imports) — so every response is labeled "captured instruments
(availability-derived)": what we've actually captured, not what the venue
could theoretically list. A true-catalogue projection is P6 phase-2.

Single-walk discipline: this reads the availability manifest ONCE per request
(``_read_availability_index`` — the same bounded, already-imported collaborator
``routes/data_status/_live_coverage.py``/``_coverage_scope.py`` use for the
coverage grid) — never a whole-corpus GCS parquet walk.

Split as a new submodule (not folded into ``_query_meta.py``) attaching to the
shared package ``router`` — see ``routes/data_status/__init__.py`` import
block for the registration convention.

Plan: ``data_status_page_ux_and_canonicalisation_2026_07_16.md`` P6.
"""

from __future__ import annotations

import logging

import pandas as pd
from fastapi import HTTPException, Query
from fastapi.responses import Response

import deployment_api.routes.data_status as _ds
from deployment_api.routes.data_status import router
from deployment_api.routes.data_status._coverage_scope import is_mvp_for_manifest_row

logger = logging.getLogger(__name__)

CATALOGUE_LABEL = "captured instruments (availability-derived)"

DEFAULT_CATALOGUE_LIMIT = 50
MAX_CATALOGUE_LIMIT = 500
MAX_CATALOGUE_SEARCH_RESULTS = 500

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


def _load_catalogue_frame(
    *,
    service: str,
    asset_group: str,
    venue: str | None,
    instrument_type: str | None,
    data_type: str | None,
) -> pd.DataFrame:
    """Read the availability manifest for ``(service, asset_group)`` and
    narrow to the requested axes. ONE bounded manifest read — no GCS parquet
    walk (single-walk discipline)."""
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


def _catalogue_rows(
    df: pd.DataFrame,
    *,
    asset_group: str,
    search: str | None,
    mvp_only: bool,
) -> list[dict[str, object]]:
    """Project a de-duped manifest slice into catalogue row dicts, applying
    ``mvp_only`` + ``search`` (mirrors ``_instruments.py::_tag_is_mvp`` +
    ``_apply_search_and_pagination`` — same is_mvp axis-plumbing rationale,
    read straight from the manifest row via ``is_mvp_for_manifest_row``)."""
    if df.empty or "instrument_id" not in df.columns:
        return []

    out: list[dict[str, object]] = []
    for _, row in df.iterrows():  # pyright: ignore[reportUnknownVariableType,reportUnknownMemberType]
        iid = str(row.get("instrument_id") or "").strip()  # pyright: ignore[reportUnknownMemberType,reportUnknownArgumentType]
        if not iid:
            continue
        row_is_mvp = is_mvp_for_manifest_row(row, asset_group)  # pyright: ignore[reportUnknownArgumentType]
        if mvp_only and not row_is_mvp:
            continue
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

    if search:
        needle = search.strip().lower()
        if needle:
            out = [r for r in out if needle in str(r["instrument_id"]).lower()][:MAX_CATALOGUE_SEARCH_RESULTS]

    out.sort(key=lambda r: str(r["instrument_id"]))
    return out


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
    return _catalogue_rows(deduped, asset_group=asset_group, search=search, mvp_only=mvp_only)


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
        logger.exception("Error in get_instrument_catalogue")
        raise HTTPException(status_code=500, detail="Internal server error. Check server logs.") from exc

    total_count = len(rows)
    safe_limit = max(1, min(int(limit or DEFAULT_CATALOGUE_LIMIT), MAX_CATALOGUE_LIMIT))
    safe_offset = max(0, int(offset or 0))
    page = rows[safe_offset : safe_offset + safe_limit]

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

"""Prediction-market catalogue browser — human-readable browsing by category /
canonical_question_group (plan ``data_status_page_ux_and_canonicalisation_2026_07_16``
P3).

Reads the SAME per-instrument lifecycle catalogue as ``catalogue_lifecycle.py``
(``prod/catalog.parquet`` in ``instruments-store-prediction``) and
``manifest_source.read_unique_instrument_count`` — deployment-api cannot reach
the IS ``InstrumentCatalogReader`` (T4), so this is a read-only projection of
the same identity-level parquet.

**Schema reality (verified against
``instruments-service/scripts/build_instrument_catalogue.py``'s ``CATALOG_COLUMNS``
+ ``docs/PREDICTION_INSTRUMENTS.md``, 2026-07-16):** ``catalog.parquet`` does
NOT carry a per-market ``canonical_question_group`` column — that value only
exists as a SEPARATE bundle-grain row (``data_type=
"prediction_canonical_question_group"``, ``instrument_id=cqg``), which is a
shard-tracking summary row (the prediction manifest/shard axis), not a
browsable market. ``canonical_question_group`` as a genuine per-ROW column only
lives in the availability MANIFEST (``manifest_source.DRILLDOWN_COLUMNS``),
a different parquet entirely. Genuine per-market catalogue rows DO carry
``raw_symbol`` / ``base_asset`` / ``underlying`` (the 2026-07-09 identity fix).
So this module DERIVES the cqg per market row via the same deterministic
classifiers the IS adapters call at capture time
(:func:`~unified_api_contracts.predictions.classify_kalshi_to_canonical_group` /
:func:`~unified_api_contracts.predictions.classify_polymarket_to_canonical_group`)
— a pure, side-effect-free re-derivation of the same classification the
adapters already compute, not a stored value — then composes
:func:`~unified_api_contracts.predictions.category_for_group` for the coarse
category. Bundle rows are excluded (never surfaced as a browsable "market").

Honest-absence label fallback (never fabricate a title): ``raw_symbol`` ->
``base_asset`` (first 50 chars) -> ``event_title`` (only if the schema carries
it — NaN until a regen per ``PREDICTION_INSTRUMENTS.md`` lines 324-326;
absent from ``CATALOG_COLUMNS`` today, but schema-checked defensively) ->
``instrument_id``.
"""

from __future__ import annotations

import io
import logging
import time
from typing import TypedDict

import pandas as pd
from unified_api_contracts.predictions import (
    CanonicalQuestionGroup,
    PredictionMarketCategory,
    category_for_group,
    classify_kalshi_to_canonical_group,
    classify_polymarket_to_canonical_group,
)

from deployment_api.utils.storage_client import get_storage_client

logger = logging.getLogger(__name__)

# Bundle-grain rows (shard-tracking summaries, not browsable markets) — see
# module docstring. Matches the ManifestWriter / build_instrument_catalogue.py
# constant (``_PREDICTION_CQG_DATA_TYPE`` there).
_CQG_BUNDLE_DATA_TYPE: str = "prediction_canonical_question_group"

_BASE_ASSET_LABEL_MAX_LEN: int = 50

# Columns projected from catalog.parquet (schema-aware — only present columns
# are read, so an older/newer catalogue degrades gracefully). ``event_title``
# is NOT in today's ``CATALOG_COLUMNS`` (dropped before roll-up per
# PREDICTION_INSTRUMENTS.md's P3 follow-up) but is listed as a candidate for a
# future regen — the label fallback chain below is honest either way.
_READ_COLUMNS: list[str] = [
    "instrument_id",
    "instrument_type",
    "venue",
    "raw_symbol",
    "base_asset",
    "underlying",
    "available_from",
    "available_to",
    "data_type",
    "mvp",
    "event_title",
]

_DEFAULT_LIMIT: int = 50
_MAX_LIMIT: int = 500

# 5-min TTL, keyed on the full filter+pagination tuple. Same rationale as
# catalogue_lifecycle: the catalogue refreshes at most daily, the browser
# re-renders on every filter change, and each miss is a transpacific GCS GET.
_CACHE_TTL_SEC: int = 300


class PredictionCatalogueRow(TypedDict):  # CORRECT-LOCAL — API response shape, no cross-service consumer
    """One browsable prediction-market row for the catalogue browser."""

    instrument_id: str
    instrument_type: str
    venue: str
    label: str
    canonical_question_group: str
    category: str
    underlying: str
    available_from: str
    available_to: str
    mvp: bool


class PredictionCatalogueResult(TypedDict):  # CORRECT-LOCAL — API response shape, no cross-service consumer
    """Paginated + faceted prediction-catalogue response."""

    rows: list[PredictionCatalogueRow]
    total: int
    category_counts: dict[str, int]
    cqg_counts: dict[str, int]


#: Cache key = the FILTER set only. ``limit``/``offset`` are deliberately NOT in
#: it: the whole cost of this endpoint (a ~184 MB transpacific GCS GET + a
#: ~2.7M-row classification pass, measured 2026-07-17 at 84.5s + 86.0s = ~173s
#: cold) is identical for every page of the same filter set, so keying on the
#: page made "Next page" re-pay the entire ~173s. See ``_PAGE_WINDOW_ROWS``.
_CatalogueCacheKey = tuple[str | None, str | None, str | None, str | None]

#: How many leading rows of a filter set's result are retained for cheap paging.
#: Deliberately BOUNDED rather than caching the whole result: an unfiltered
#: result is ~2.7M rows, and holding that (or the source frame — the identity
#: catalogues measure 122-312 MB deep) risks exactly the OOM class this plan's
#: P1 already root-caused on the honest-coverage writer. 5,000 rows = 100 pages
#: at the default 50/page, which covers real browsing; a request reaching past
#: the window falls back to a full rebuild (correct, just slow — logged).
_PAGE_WINDOW_ROWS: int = 5_000


class _CachedFilterSet(TypedDict):
    """Everything a filter set needs to answer any page inside the window."""

    window: list[PredictionCatalogueRow]  # leading _PAGE_WINDOW_ROWS of the sorted result
    total: int  # full count (NOT len(window)) — pagination metadata stays exact
    category_counts: dict[str, int]
    cqg_counts: dict[str, int]


_CATALOGUE_CACHE: dict[_CatalogueCacheKey, tuple[float, _CachedFilterSet]] = {}


def _bucket(*, cloud: str = "gcp") -> str:
    from unified_trading_library import resolve_bucket_name  # noqa: qg-inside-import

    return resolve_bucket_name(cloud=cloud, kind="instruments-store-prediction")


def _read_catalogue() -> pd.DataFrame | None:
    """Read prediction's ``prod/catalog.parquet`` (ONE GCS GET). ``None`` on any
    failure (missing bucket/object, unreadable parquet) — shard-isolated, the
    caller returns an honest empty result, never raises."""
    import pyarrow.parquet as pq  # noqa: imports-inside-functions — lazy heavy SDK

    try:
        bucket = _bucket()
        raw = get_storage_client().download_bytes(bucket, "prod/catalog.parquet")
        schema_names = set(pq.read_schema(io.BytesIO(raw)).names)
        present = [c for c in _READ_COLUMNS if c in schema_names]
        df = pd.read_parquet(io.BytesIO(raw), columns=present or None)
    except Exception as exc:
        # Shard-isolated: a catalogue read failure must not sink the caller.
        logger.warning("prediction-catalogue read failed (%s) — returning empty result", exc)
        return None
    if df.empty:
        return None
    return df


def _norm(val: object) -> str:
    if val is None:
        return ""
    try:
        if pd.isna(val):  # type: ignore[reportArgumentType]
            return ""
    except (TypeError, ValueError):
        pass
    return str(val).strip()


def _pandas_missing(val: object) -> bool:
    try:
        return bool(pd.isna(val))  # type: ignore[reportArgumentType]
    except (TypeError, ValueError):
        return False


def _iso_date(val: object) -> str:
    """Normalise a catalogue date value to a ``YYYY-MM-DD`` prefix (or "")."""
    s = _norm(val)
    return s[:10] if s else ""


def _classify_row(venue: str, raw_symbol: str, instrument_id: str) -> CanonicalQuestionGroup:
    """Derive the cqg for one market row via the SAME deterministic classifiers
    the IS adapters call at capture time (pure function, no I/O) — see module
    docstring for why this is re-derived rather than read from a stored column.
    ``raw_symbol`` doubles as the Kalshi ticker / Polymarket slug; no title is
    available at this grain (mirrors ``cross_venue_mapping._classify_polymarket``'s
    ``title = titles.get(...) or slug`` fallback), so title/slug/event_slug all
    take ``raw_symbol``. An unrecognised/future venue or a market with neither
    ``raw_symbol`` nor ``instrument_id`` honestly routes to ``OTHER`` (never a
    guessed classification)."""
    v = venue.strip().upper()
    ticker_or_slug = raw_symbol or instrument_id
    if not ticker_or_slug:
        return CanonicalQuestionGroup.OTHER
    if v == "KALSHI":
        return classify_kalshi_to_canonical_group(ticker=ticker_or_slug)
    if v == "POLYMARKET":
        return classify_polymarket_to_canonical_group(
            title=ticker_or_slug,
            slug=ticker_or_slug,
            event_slug=ticker_or_slug,
            outcome="",
            condition_id=instrument_id,
        )
    return CanonicalQuestionGroup.OTHER


def _label(rec: dict[str, object]) -> str:
    """Honest fallback chain — never fabricate: ``raw_symbol`` -> ``base_asset``
    (first 50 chars) -> ``event_title`` (if the schema carries it) ->
    ``instrument_id``."""
    raw_symbol = _norm(rec.get("raw_symbol"))
    if raw_symbol:
        return raw_symbol
    base_asset = _norm(rec.get("base_asset"))
    if base_asset:
        return base_asset[:_BASE_ASSET_LABEL_MAX_LEN]
    event_title = _norm(rec.get("event_title"))
    if event_title:
        return event_title
    return _norm(rec.get("instrument_id"))


def _build_rows(df: pd.DataFrame, *, venue: str | None = None) -> list[PredictionCatalogueRow]:
    """Project catalogue records into browsable rows, excluding cqg-bundle
    summary rows. Classification is cached per unique (venue, raw_symbol,
    instrument_id) and per unique cqg so a large catalogue classifies each
    distinct market/underlying-question exactly once, not per row.

    ``venue`` narrows the FRAME before classification. This is a pure
    optimisation, not a semantic change: ``venue`` is a raw catalogue column
    (no classification needed to evaluate it) and the caller applied the exact
    same narrow to the built rows immediately afterwards — doing it here just
    stops us classifying ~2.7M rows to then discard most of them. Facet counts
    are unaffected because they are computed over the venue-filtered set anyway.
    """
    if "data_type" in df.columns:
        df = df[df["data_type"].astype(str) != _CQG_BUNDLE_DATA_TYPE]
    if venue and "venue" in df.columns:
        df = df[df["venue"].astype(str).str.upper() == venue]
    rows: list[PredictionCatalogueRow] = []
    cqg_cache: dict[tuple[str, str, str], CanonicalQuestionGroup] = {}
    category_cache: dict[CanonicalQuestionGroup, PredictionMarketCategory] = {}
    for rec in df.to_dict(orient="records"):
        venue = _norm(rec.get("venue"))
        raw_symbol = _norm(rec.get("raw_symbol"))
        instrument_id = _norm(rec.get("instrument_id"))
        cache_key = (venue, raw_symbol, instrument_id)
        cqg = cqg_cache.get(cache_key)
        if cqg is None:
            cqg = _classify_row(venue, raw_symbol, instrument_id)
            cqg_cache[cache_key] = cqg
        category = category_cache.get(cqg)
        if category is None:
            category = category_for_group(cqg)
            category_cache[cqg] = category
        mvp_raw = rec.get("mvp")
        rows.append(
            PredictionCatalogueRow(
                instrument_id=instrument_id,
                instrument_type=_norm(rec.get("instrument_type")),
                venue=venue,
                label=_label(rec),
                canonical_question_group=cqg.value,
                category=category.value,
                underlying=_norm(rec.get("underlying")),
                available_from=_iso_date(rec.get("available_from")),
                available_to=_iso_date(rec.get("available_to")),
                mvp=bool(mvp_raw) if mvp_raw is not None and not _pandas_missing(mvp_raw) else False,
            )
        )
    return rows


def _facet_counts(rows: list[PredictionCatalogueRow]) -> tuple[dict[str, int], dict[str, int]]:
    """Facet counts over the given rows (category -> count, cqg -> count)."""
    category_counts: dict[str, int] = {}
    cqg_counts: dict[str, int] = {}
    for row in rows:
        category_counts[row["category"]] = category_counts.get(row["category"], 0) + 1
        cqg_counts[row["canonical_question_group"]] = cqg_counts.get(row["canonical_question_group"], 0) + 1
    return category_counts, cqg_counts


def read_prediction_catalogue(
    *,
    category: str | None = None,
    canonical_question_group: str | None = None,
    venue: str | None = None,
    search: str | None = None,
    limit: int = _DEFAULT_LIMIT,
    offset: int = 0,
) -> PredictionCatalogueResult:
    """Browse the live prediction-market catalogue by category / cqg.

    Facet counts (``category_counts`` / ``cqg_counts``) are computed over the
    venue+search-filtered set BEFORE the category/cqg narrow, so the UI's
    category ``<select>`` can render every bucket's count regardless of which
    one is currently selected (standard faceted-search shape). Read-only,
    5-min TTL, shard-isolated (a catalogue read failure returns an honest empty
    result, never raises)."""
    limit = max(1, min(limit, _MAX_LIMIT))
    offset = max(0, offset)
    venue_f = venue.strip().upper() if venue and venue.strip() else None
    category_f = category.strip().lower() if category and category.strip() else None
    cqg_f = (
        canonical_question_group.strip().upper()
        if canonical_question_group and canonical_question_group.strip()
        else None
    )
    search_f = search.strip().lower() if search and search.strip() else None

    key: _CatalogueCacheKey = (venue_f, category_f, cqg_f, search_f)
    now = time.monotonic()
    cached = _CATALOGUE_CACHE.get(key)
    if cached is not None and (now - cached[0]) < _CACHE_TTL_SEC:
        entry = cached[1]
        # Serve from the retained window when the requested page lies inside it.
        # A page beyond the window is rare (>100 pages deep) and falls through to
        # a full rebuild rather than being answered from a truncated list.
        if offset + limit <= len(entry["window"]) or offset + limit <= entry["total"] <= _PAGE_WINDOW_ROWS:
            return PredictionCatalogueResult(
                rows=entry["window"][offset : offset + limit],
                total=entry["total"],
                category_counts=entry["category_counts"],
                cqg_counts=entry["cqg_counts"],
            )
        logger.info(
            "prediction-catalogue: page offset=%d limit=%d is past the cached %d-row window (total=%d) — rebuilding",
            offset,
            limit,
            len(entry["window"]),
            entry["total"],
        )

    df = _read_catalogue()
    if df is None:
        empty_entry = _CachedFilterSet(window=[], total=0, category_counts={}, cqg_counts={})
        _CATALOGUE_CACHE[key] = (now, empty_entry)
        return PredictionCatalogueResult(rows=[], total=0, category_counts={}, cqg_counts={})

    # venue is a raw column -> narrow before classifying (see _build_rows).
    all_rows = _build_rows(df, venue=venue_f)

    if search_f:
        all_rows = [
            r
            for r in all_rows
            if search_f in r["label"].lower()
            or search_f in r["instrument_id"].lower()
            or search_f in r["canonical_question_group"].lower()
        ]

    category_counts, cqg_counts = _facet_counts(all_rows)

    filtered = all_rows
    if category_f:
        filtered = [r for r in filtered if r["category"] == category_f]
    if cqg_f:
        filtered = [r for r in filtered if r["canonical_question_group"] == cqg_f]

    filtered.sort(key=lambda r: (r["venue"], r["label"]))
    total = len(filtered)

    _CATALOGUE_CACHE[key] = (
        now,
        _CachedFilterSet(
            window=filtered[:_PAGE_WINDOW_ROWS],
            total=total,
            category_counts=category_counts,
            cqg_counts=cqg_counts,
        ),
    )
    return PredictionCatalogueResult(
        rows=filtered[offset : offset + limit],
        total=total,
        category_counts=category_counts,
        cqg_counts=cqg_counts,
    )


def _clear_cache() -> None:  # test hook
    _CATALOGUE_CACHE.clear()


__all__ = [
    "PredictionCatalogueResult",
    "PredictionCatalogueRow",
    "read_prediction_catalogue",
]

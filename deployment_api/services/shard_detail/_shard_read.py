"""Parquet footer / sample-row reads, signed URLs + the shard-detail aggregator.

Split from ``services/shard_detail.py`` (pure code motion; plan:
``codex_violations_ratchet_to_five_2026_06_10.md`` Phase-1 P2). Patched
module-level collaborators are resolved through the package facade
(``_sd``) at call time so the existing test patch surface
``deployment_api.services.shard_detail.<name>`` keeps intercepting.
"""

from __future__ import annotations

import logging
from typing import cast
from urllib.parse import urlencode

import pandas as pd

import deployment_api.services.shard_detail as _sd
from deployment_api.services.shard_detail._shard_core import (
    _SAMPLE_ROW_LIMIT,  # pyright: ignore[reportPrivateUsage]
    _SIGNED_URL_TTL_SECONDS,  # pyright: ignore[reportPrivateUsage]
    _classify_shard,  # pyright: ignore[reportPrivateUsage]
    _gcs_metadata,  # pyright: ignore[reportPrivateUsage]
    _resolve_schema,  # pyright: ignore[reportPrivateUsage]
)
from deployment_api.settings import gcp_project_id as _pid
from deployment_api.types.shard_detail import (
    ShardCoord,
    ShardDetailResponse,
    ShardDownloadUrls,
    ShardPayloadFixtures,
    ShardPayloadGrouped,
    ShardPayloadPerSymbol,
    ShardPayloadReference,
    ShardSchemaColumn,
)

logger = logging.getLogger(__name__)


def _read_parquet_footer_row_count(gs_uri: str) -> int | None:
    """Return ``num_rows`` from the parquet footer, or ``None`` on any error.

    Does not read any row groups — pyarrow's ``ParquetFile.metadata`` walk
    only fetches the footer (one GCS range request) and is cheap.
    """
    import gcsfs
    import pyarrow.parquet as pq

    if not gs_uri.startswith("gs://"):
        return None
    bucket_key = gs_uri[len("gs://") :]
    try:
        fs_any: object = gcsfs.GCSFileSystem(project=_pid)  # pyright: ignore[reportUnknownMemberType]
        open_fn: object = getattr(fs_any, "open", None)
        if not callable(open_fn):
            return None
        fh_obj: object = open_fn(bucket_key, "rb")
        try:
            pf_ctor: object = pq.ParquetFile  # pyright: ignore[reportUnknownMemberType, reportUnknownVariableType]
            if not callable(pf_ctor):  # pyright: ignore[reportUnknownArgumentType]
                return None
            pf: object = pf_ctor(fh_obj)  # pyright: ignore[reportUnknownVariableType]
            meta: object = getattr(pf, "metadata", None)
            if meta is None:
                return None
            num_rows: object = getattr(meta, "num_rows", None)
            if isinstance(num_rows, int):
                return num_rows
            return None
        finally:
            close: object = getattr(fh_obj, "close", None)
            if callable(close):
                close()
    except (OSError, RuntimeError, ValueError) as exc:
        logger.warning("parquet footer read failed for %s: %s", gs_uri, exc)
        return None


def _sample_rows(*, gs_uri: str, limit: int, schema_columns: list[ShardSchemaColumn]) -> list[dict[str, object]]:
    """Return the first ``limit`` rows of the parquet as a list of dicts.

    Only projects the first 20 declared columns (or all columns when the
    schema is unknown) to keep the JSON payload small.
    """
    project_cols: list[str] | None = [c.name for c in schema_columns[:20]] if schema_columns else None
    try:
        df: pd.DataFrame = _sd._read_parquet_columns(gs_uri, project_cols)  # pyright: ignore[reportPrivateUsage]
    except (OSError, ValueError, RuntimeError) as exc:
        logger.warning("sample-rows read failed for %s: %s", gs_uri, exc)
        return []
    if df.empty:
        return []
    head = df.head(limit)
    records_raw = head.to_dict(orient="records")  # pyright: ignore[reportUnknownMemberType,reportUnknownVariableType]
    records = cast(list[object], records_raw)
    out: list[dict[str, object]] = []
    for r in records:
        if isinstance(r, dict):
            typed_r: dict[str, object] = {}
            for k, v in cast(dict[object, object], r).items():
                typed_r[str(k)] = v
            out.append(typed_r)
    return out


def _distinct_symbols(*, gs_uri: str, symbol_column: str | None) -> list[dict[str, str]]:
    """Return the list of distinct symbol-column values in a bundle parquet."""
    if not symbol_column:
        return []
    try:
        df: pd.DataFrame = _sd._read_parquet_columns(gs_uri, [symbol_column])  # pyright: ignore[reportPrivateUsage]
    except (OSError, ValueError, RuntimeError) as exc:
        logger.warning("distinct-symbols read failed for %s: %s", gs_uri, exc)
        return []
    if symbol_column not in df.columns or df.empty:
        return []
    seen: set[str] = set()
    out: list[dict[str, str]] = []
    raw_values: object = df[symbol_column].dropna().unique().tolist()  # pyright: ignore[reportUnknownMemberType]
    values_list: list[object] = cast(list[object], raw_values)
    for v in values_list:
        key = str(v).strip()
        if key and key not in seen:
            seen.add(key)
            out.append({"key": key, "type": symbol_column})
    out.sort(key=lambda d: d["key"])
    return out


# ---------------------------------------------------------------------------
# Signed URL + CSV-projection link helpers
# ---------------------------------------------------------------------------


def _parquet_signed_url(bucket: str | None, object_path: str | None) -> str | None:
    """Generate a 1-hour signed download URL for a GCS parquet.

    Returns ``None`` when any step fails (missing creds, mock mode,
    non-GCS storage).  The UI treats a ``None`` link as "signed URL not
    available" and falls back to the CSV projection URL.
    """
    if not bucket or not object_path:
        return None
    from google.cloud import storage  # noqa: cloud-sdk-direct

    try:
        client: object = storage.Client(project=_pid)  # pyright: ignore[reportUnknownMemberType, reportUnknownVariableType]
        bucket_obj_fn: object = getattr(client, "bucket", None)
        if not callable(bucket_obj_fn):
            return None
        bucket_obj: object = bucket_obj_fn(bucket)
        blob_fn: object = getattr(bucket_obj, "blob", None)
        if not callable(blob_fn):
            return None
        blob: object = blob_fn(object_path)
        gen_fn: object = getattr(blob, "generate_signed_url", None)
        if not callable(gen_fn):
            return None
        url_obj: object = gen_fn(expiration=_SIGNED_URL_TTL_SECONDS, method="GET")
        return str(url_obj) if url_obj else None
    except (OSError, RuntimeError, ValueError) as exc:
        logger.warning("signed URL generation failed for gs://%s/%s: %s", bucket, object_path, exc)
        return None


def _csv_projection_url(
    *,
    service: str,
    category: str,
    venue: str | None,
    day: str,
    instrument_type: str,
    data_type: str,
    instrument_id: str | None,
) -> str | None:
    """Return the relative UI-facing URL for the existing CSV download."""
    if not venue:
        return None
    params: dict[str, str] = {
        "service": service,
        "category": category,
        "venue": venue,
        "day": day,
        "instrument_type": instrument_type,
        "data_type": data_type,
    }
    if instrument_id:
        params["instrument_ids"] = instrument_id
    return f"/api/data-status/download-csv?{urlencode(params)}"


# ---------------------------------------------------------------------------
# Public API: get_shard_detail
# ---------------------------------------------------------------------------


def get_shard_detail(
    *,
    service: str,
    category: str,
    instrument_type: str,
    data_type: str,
    day: str,
    venue: str | None = None,
    underlying: str | None = None,
    instrument_id: str | None = None,
) -> ShardDetailResponse:
    """Build the unified shard-detail response for one coordinate.

    The function never raises for data-level failures — missing parquet,
    unreadable manifest, or unclassified shards all resolve to a
    ``missing`` capture status with empty payloads so the UI can render
    the error state.  Programmer errors (unknown service bucket) do
    raise ``ValueError`` at the boundary.
    """
    schema, resolved_it = _resolve_schema(
        category=category,
        instrument_type=instrument_type,
        data_type=data_type,
        venue=venue,
    )

    # Use the resolved instrument_type for shard classification + path
    # resolution downstream so AUTO callers land on the correct GCS layout.
    # When auto resolution failed (resolved_it == ""), fall back to the
    # caller's literal so classification still produces a deterministic
    # shard_class and we surface the registered=False schema honestly.
    effective_instrument_type = resolved_it or instrument_type

    shard_class = _classify_shard(
        service=service,
        category=category,
        instrument_type=effective_instrument_type,
        data_type=data_type,
    )

    # Fixtures branch has its own parquet layout (sports_reference) — we
    # do not hit _gcs_path_for_shard for sports.
    bucket: str | None = None
    object_path: str | None = None
    if shard_class != "fixtures":
        resolved = _sd._gcs_path_for_shard(  # pyright: ignore[reportPrivateUsage]
            service=service,
            category=category,
            instrument_type=effective_instrument_type,
            data_type=data_type,
            venue=venue,
            day=day,
            underlying=underlying,
            instrument_id=instrument_id,
        )
        if resolved is not None:
            bucket, object_path = resolved

    gs_uri: str | None = f"gs://{bucket}/{object_path}" if bucket and object_path else None  # noqa: gs-uri  — URI composer, bucket already resolved

    pq_row_count: int | None = None
    sample_rows: list[dict[str, object]] = []
    if gs_uri is not None:
        pq_row_count = _sd._read_parquet_footer_row_count(gs_uri)  # pyright: ignore[reportPrivateUsage]
        sample_rows = _sample_rows(gs_uri=gs_uri, limit=_SAMPLE_ROW_LIMIT, schema_columns=schema.columns)

    # Manifest capture_status lookup — shard-scoped, never fatal.
    manifest_row: dict[str, str] | None = None
    if bucket is not None:
        manifest_row = _sd._manifest_row_for_coord(  # pyright: ignore[reportPrivateUsage]
            bucket=bucket,
            venue=venue,
            day=day,
            data_type=data_type,
            instrument_id=instrument_id,
        )

    gcs_block = _gcs_metadata(
        bucket=bucket,
        object_path=object_path,
        manifest=manifest_row,
        pq_row_count=pq_row_count,
    )

    download_urls = ShardDownloadUrls(
        parquet_signed_url=_sd._parquet_signed_url(bucket, object_path),  # pyright: ignore[reportPrivateUsage]
        csv_projected=_csv_projection_url(
            service=service,
            category=category,
            venue=venue,
            day=day,
            instrument_type=effective_instrument_type,
            data_type=data_type,
            instrument_id=instrument_id,
        ),
    )

    # Branch by shard_class for payload
    payload_grouped: ShardPayloadGrouped | None = None
    payload_per_symbol: ShardPayloadPerSymbol | None = None
    payload_reference: ShardPayloadReference | None = None
    payload_fixtures: ShardPayloadFixtures | None = None

    if shard_class == "grouped":
        distinct = _distinct_symbols(gs_uri=gs_uri, symbol_column=schema.symbol_column) if gs_uri is not None else []
        payload_grouped = ShardPayloadGrouped(instrument_list=distinct)
    elif shard_class == "per_symbol":
        leaf = instrument_id or underlying or ""
        instrument_list: list[dict[str, str]] = (
            [{"key": leaf, "type": schema.symbol_column or "instrument_id"}] if leaf else []
        )
        payload_per_symbol = ShardPayloadPerSymbol(instrument_list=instrument_list)
    elif shard_class == "reference":
        payload_reference = ShardPayloadReference(instrument_definitions=list(sample_rows) if sample_rows else [])
    elif shard_class == "fixtures":
        payload_fixtures = ShardPayloadFixtures(fixtures=list(sample_rows) if sample_rows else [])

    coord = ShardCoord(
        service=service,
        asset_group=category,
        # Echo the resolved instrument_type (not the literal "AUTO" /
        # "UNKNOWN") so the UI displays the actual axis name.  When the
        # backend could not resolve, we keep the caller's value so the
        # response is honest about what was requested.
        instrument_type=effective_instrument_type,
        data_type=data_type,
        day=day,
        venue=venue,
        underlying=underlying,
        instrument_id=instrument_id,
    )

    return ShardDetailResponse(
        coord=coord,
        shard_class=shard_class,
        schema=schema,  # pyright: ignore[reportCallIssue]
        gcs=gcs_block,
        download_urls=download_urls,
        sample_rows=sample_rows,
        payload_grouped=payload_grouped,
        payload_per_symbol=payload_per_symbol,
        payload_reference=payload_reference,
        payload_fixtures=payload_fixtures,
    )


# ---------------------------------------------------------------------------
# Public API: fetch_venue_detail  (CeFi + DeFi)
# ---------------------------------------------------------------------------

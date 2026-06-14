"""Leaf parquet stats (available_at envelope, completeness, column stats).

Split from ``services/shard_detail.py`` (pure code motion; plan:
``codex_violations_ratchet_to_five_2026_06_10.md`` Phase-1 P2). Patched
module-level collaborators are resolved through the package facade
(``_sd``) at call time so the existing test patch surface
``deployment_api.services.shard_detail.<name>`` keeps intercepting.
"""

from __future__ import annotations

import logging

import pandas as pd
from unified_api_contracts.features import get_feature_family

import deployment_api.services.shard_detail as _sd
from deployment_api.types.shard_detail import (
    LeafAvailableAtEnvelope,
    LeafCompletenessEnvelope,
    LeafParquetColumnStat,
    LeafParquetStats,
    ShardCoord,
)

logger = logging.getLogger(__name__)

_LEAF_STATS_ROW_LIMIT = 500_000


def _safe_iso_or_none(ts: object) -> str | None:
    """Coerce a pandas Timestamp / datetime / NaT to ISO8601 or None."""
    if ts is None:
        return None
    try:
        if pd.isna(ts):  # pyright: ignore[reportCallIssue,reportArgumentType]
            return None
    except (TypeError, ValueError):
        pass
    try:
        # pandas Timestamps + python datetimes both expose isoformat().
        iso = getattr(ts, "isoformat", None)
        if callable(iso):
            return str(iso())
        return str(ts)
    except (TypeError, ValueError):
        return None


def _compute_available_at_envelope(df: pd.DataFrame) -> LeafAvailableAtEnvelope:
    """Derive the ``available_at`` envelope from a leaf parquet DataFrame.

    Returns ``present=False`` when the column is absent (writegate
    Phase 1A.future ``MissingAvailableAt`` failure mode). When present,
    populates min / max ISO8601 timestamps + null count.
    """
    if "available_at" not in df.columns:
        return LeafAvailableAtEnvelope(present=False)
    col = df["available_at"]
    null_count = int(col.isna().sum())
    non_null = col.dropna()
    if non_null.empty:
        return LeafAvailableAtEnvelope(present=True, null_count=null_count)
    try:
        # Coerce to pandas Timestamp where possible — silent passthrough
        # for already-datetime columns, NaN-fill for unparsable scalars.
        coerced = pd.to_datetime(non_null, errors="coerce", utc=True)
        if coerced.notna().any():
            return LeafAvailableAtEnvelope(
                present=True,
                min_iso=_safe_iso_or_none(coerced.min()),
                max_iso=_safe_iso_or_none(coerced.max()),
                null_count=null_count + int(coerced.isna().sum()),
            )
    except (ValueError, TypeError):
        pass
    # Fallback: lexicographic min/max on the string repr (already an ISO
    # string in our pipeline, so this is a safety net only).
    try:
        as_str = non_null.astype(str)
        return LeafAvailableAtEnvelope(
            present=True,
            min_iso=str(as_str.min()),
            max_iso=str(as_str.max()),
            null_count=null_count,
        )
    except (ValueError, TypeError):
        return LeafAvailableAtEnvelope(present=True, null_count=null_count)


def _compute_completeness_envelope(df: pd.DataFrame) -> LeafCompletenessEnvelope:
    """Derive the ``completeness_fraction`` + ``incomplete_window`` envelope.

    Populated when the parquet has a ``completeness_fraction`` column written
    via the writegate slice (b) emission-policy hooks (`publish_with_policy()`
    / `publish_with_manifest_lookup()`). Absent column → ``present=False`` —
    the parquet predates the slice (c) per-service rollout (Phase 6.1-6.9).

    Min / max / mean computed over non-null float values; null_count is the
    count of rows where ``completeness_fraction`` is NaN. The
    ``incomplete_window_present_count`` is the count of rows where the
    ``incomplete_window`` column (string JSON) is non-null AND non-empty
    (e.g. ``"[]"`` counts as empty; ``"[{...}]"`` counts as present). Zero
    when ``incomplete_window`` is absent.

    Forward-compatible: when the slice (c) rollout adds the columns, this
    helper auto-surfaces the envelope without any further deployment-api
    changes. Per the workspace "Live = batch — same data, same fields" rule
    the columns ship in the same parquet schema regardless of emission mode.
    """
    if "completeness_fraction" not in df.columns:
        return LeafCompletenessEnvelope(present=False)
    col = df["completeness_fraction"]
    null_count = int(col.isna().sum())
    non_null = col.dropna()
    if non_null.empty:
        return LeafCompletenessEnvelope(present=True, null_count=null_count)
    try:
        coerced = pd.to_numeric(non_null, errors="coerce")
        coerced_list: list[object] = coerced.dropna().tolist()  # pyright: ignore[reportUnknownVariableType,reportUnknownMemberType]
        valid_values: list[float] = [float(v) for v in coerced_list]  # pyright: ignore[reportAny,reportArgumentType]
    except (TypeError, ValueError):
        valid_values = []
    if not valid_values:
        return LeafCompletenessEnvelope(present=True, null_count=null_count)
    # incomplete_window column is optional even when completeness_fraction is
    # present (some emissions log incomplete rows via event payload only,
    # leaving the per-row column null).
    incomplete_present = 0
    if "incomplete_window" in df.columns:
        iw_col = df["incomplete_window"]
        # Non-null + non-empty: empty list serialises as "[]" (length 2);
        # null values + scalar empty strings count as "no incomplete window".
        iw_list: list[object] = iw_col.dropna().tolist()  # pyright: ignore[reportUnknownVariableType,reportUnknownMemberType]
        for raw in iw_list:
            value = str(raw).strip()
            if value and value not in ("[]", "{}", "null"):
                incomplete_present += 1
    return LeafCompletenessEnvelope(
        present=True,
        min_fraction=round(min(valid_values), 4),
        max_fraction=round(max(valid_values), 4),
        mean_fraction=round(sum(valid_values) / len(valid_values), 4),
        null_count=null_count,
        incomplete_window_present_count=incomplete_present,
    )


def _compute_column_stats(df: pd.DataFrame) -> list[LeafParquetColumnStat]:
    """Compute per-column non-null / null / NaN-ratio for a leaf parquet."""
    out: list[LeafParquetColumnStat] = []
    row_count = len(df)
    for name in df.columns:
        col = df[name]
        non_null = int(col.notna().sum())
        null = max(0, row_count - non_null)
        ratio = (null / row_count) if row_count > 0 else 0.0
        if ratio < 0.0:
            ratio = 0.0
        elif ratio > 1.0:
            ratio = 1.0
        out.append(
            LeafParquetColumnStat(
                name=str(name),
                dtype=str(col.dtype),
                non_null_count=non_null,
                null_count=null,
                nan_ratio=round(float(ratio), 4),
            )
        )
    return out


def _file_size_via_metadata(bucket: str | None, object_path: str | None) -> int | None:
    if not bucket or not object_path:
        return None
    try:
        meta = _sd.get_object_metadata(bucket, object_path)
    except (OSError, RuntimeError):
        return None
    if meta is None:
        return None
    size = meta.get("size")
    return size if isinstance(size, int) else None


def _resolve_feature_family(
    df: pd.DataFrame | None,
    feature_group: str | None,
) -> str | None:
    """Resolve ``feature_family`` for a features-* shard.

    Resolution order (write-time wins, fallback to declarative mapping):
    1. Parquet column ``feature_family`` (writer-stamped per UTL
       :class:`MissingFeatureFamilyError` enforcement). Picks the first
       non-null distinct value; logs + returns ``None`` if multiple
       distinct values appear (writer contract violation).
    2. UAC :func:`get_feature_family(feature_group)` lookup against the
       ``FEATURE_GROUP_TO_FAMILY`` registry. Returns ``None`` if
       ``feature_group`` is None / empty / unknown.

    Returns the lowercase StrEnum ``value`` (e.g. ``"volatility"``,
    ``"onchain"``) or ``None`` for non-features shards / unmapped groups.
    """
    if df is not None and "feature_family" in df.columns:
        # ``.unique()`` returns an ``np.ndarray`` (typed Any in pyright);
        # round-trip through ``set`` of explicit ``str`` to land at a
        # ``set[str]`` we can reason about.
        non_null = df["feature_family"].dropna().astype(str)
        unique_set: set[str] = {str(v) for v in non_null.tolist()}
        unique_set.discard("")
        if len(unique_set) == 1:
            return next(iter(unique_set))
        if len(unique_set) > 1:
            logger.warning(
                "leaf-stats: parquet has %d distinct feature_family values: %s",
                len(unique_set),
                sorted(unique_set)[:5],
            )
    if feature_group:
        family = get_feature_family(feature_group)
        if family is not None:
            return str(family.value)
    return None


def get_leaf_parquet_stats(
    *,
    service: str,
    asset_group: str,
    instrument_type: str,
    data_type: str,
    day: str,
    venue: str | None = None,
    underlying: str | None = None,
    instrument_id: str | None = None,
    feature_group: str | None = None,
) -> LeafParquetStats:
    """Compute live per-leaf-parquet stats for one shard coordinate.

    Mirrors the resolution shape of :func:`get_shard_detail` /
    :func:`get_schema_for_shard` so a single coordinate tuple lights all
    three views off the same parquet. Never raises for data-level
    failures — missing path / parquet read error / corrupt file all
    resolve to ``available=False`` with an ``error_reason`` so the UI can
    render the error state without a 500.

    ``feature_group`` is an optional kwarg used to resolve the
    ``feature_family`` axis for features-* shards. When provided AND the
    parquet either lacks the write-time ``feature_family`` column OR
    cannot be read, the helper falls back to UAC
    :func:`get_feature_family(feature_group)`. Plan: features-repo
    consolidation Phase 8B (deployment-api side).
    """
    family_from_group: str | None = _resolve_feature_family(None, feature_group)
    coord = ShardCoord(
        service=service,
        asset_group=asset_group,
        instrument_type=instrument_type,
        data_type=data_type,
        day=day,
        venue=venue,
        underlying=underlying,
        instrument_id=instrument_id,
        feature_family=family_from_group,
    )
    resolved = _sd._gcs_path_for_shard(  # pyright: ignore[reportPrivateUsage]
        service=service,
        category=asset_group,
        instrument_type=instrument_type,
        data_type=data_type,
        venue=venue,
        day=day,
        underlying=underlying,
        instrument_id=instrument_id,
    )
    if resolved is None:
        return LeafParquetStats(
            coord=coord,
            gs_uri=None,
            available=False,
            error_reason="path_unresolved: no parquet matches this coordinate",
            feature_family=family_from_group,
        )
    bucket, object_path = resolved
    # gs-uri-rationale: function GCS-locked (resolved tuple from upstream `build_bucket()` GCS-only helper).
    # Cloud-agnostic migration tracked in deployment_api_shard_detail_gcs_locked_2026_05_17.md.
    gs_uri = f"gs://{bucket}/{object_path}"  # noqa: gs-uri
    file_size = _sd._file_size_via_metadata(bucket, object_path)  # pyright: ignore[reportPrivateUsage]

    try:
        df = _sd._read_parquet_columns(gs_uri)  # pyright: ignore[reportPrivateUsage]
    except (OSError, RuntimeError, ValueError) as exc:
        return LeafParquetStats(
            coord=coord,
            gs_uri=gs_uri,
            available=False,
            error_reason=f"{type(exc).__name__}: {exc}"[:500],
            file_size_bytes=file_size,
            feature_family=family_from_group,
        )

    truncated = False
    truncated_at: int | None = None
    if len(df) > _sd._LEAF_STATS_ROW_LIMIT:  # pyright: ignore[reportPrivateUsage]
        truncated = True
        truncated_at = _sd._LEAF_STATS_ROW_LIMIT  # pyright: ignore[reportPrivateUsage]
        df = df.head(_sd._LEAF_STATS_ROW_LIMIT)  # pyright: ignore[reportPrivateUsage]

    columns = _compute_column_stats(df)
    available_at_envelope = _compute_available_at_envelope(df)
    completeness_envelope = _compute_completeness_envelope(df)
    feature_family_resolved: str | None = _resolve_feature_family(df, feature_group)

    final_coord = coord.model_copy(update={"feature_family": feature_family_resolved})
    return LeafParquetStats(
        coord=final_coord,
        gs_uri=gs_uri,
        available=True,
        row_count=len(df),
        column_count=len(columns),
        columns=columns,
        available_at=available_at_envelope,
        completeness=completeness_envelope,
        file_size_bytes=file_size,
        truncated=truncated,
        truncated_at_rows=truncated_at,
        feature_family=feature_family_resolved,
    )

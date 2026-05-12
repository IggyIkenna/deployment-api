"""Unified shard-detail service for ``GET /api/data-status/shard-detail``.

Derives every piece of a Data-Status shard-detail response (schema,
GCS metadata, sample rows, branch-specific payload, download URLs) from a
single ``(service, asset_group, instrument_type, data_type, venue, day, …)``
coordinate.  The four ``shard_class`` branches (``grouped`` / ``per_symbol``
/ ``reference`` / ``fixtures``) are classified by
:func:`_classify_shard` so the UI does not need to encode this routing
logic itself.

Kept separate from :mod:`data_status_drilldown` to sidestep the dense
per-fixture / bundle-preview helpers already living there — shard-detail
only needs footer reads + manifest lookup + first-N-rows + a distinct
pass for grouped shards.

Shard-level failure isolation applies throughout: every GCS / parquet /
manifest call catches its own exceptions and returns a ``missing`` /
``attempted_failed`` capture status rather than raising.  The endpoint
is a read surface for operators; a blown-up branch should never prevent
the other branches from rendering.
"""

from __future__ import annotations

import datetime as _dt
import logging
import math as _math
import re as _re
from typing import Literal, cast
from urllib.parse import urlencode

import numpy as _np
import pandas as pd
from unified_api_contracts import (
    VENUE_CONTRACT_OVERRIDES,
    SchemaContract,
    SchemaContractNotFoundError,
    lookup_contract,
)
from unified_api_contracts.features import get_feature_family
from unified_api_contracts.internal.schemas.contracts import CONTRACT_REGISTRY
from unified_trading_library import (
    LEGACY_REASON_ASSET_GROUPS,
    build_bucket,
    classify_legacy_empty_row,
    read_availability_index,
)

from deployment_api.services.data_status_drilldown import (
    _read_parquet_columns,  # pyright: ignore[reportPrivateUsage]
    build_bucket_name,
)
from deployment_api.settings import gcp_project_id as _pid
from deployment_api.types.shard_detail import (
    CaptureStatusLiteral,
    LeafAvailableAtEnvelope,
    LeafCompletenessEnvelope,
    LeafParquetColumnStat,
    LeafParquetStats,
    ServiceEmissionStateLiteral,
    ShardClassLiteral,
    ShardCoord,
    ShardDetailResponse,
    ShardDownloadUrls,
    ShardGcsMetadata,
    ShardPayloadFixtures,
    ShardPayloadGrouped,
    ShardPayloadPerSymbol,
    ShardPayloadReference,
    ShardSchema,
    ShardSchemaColumn,
    VenueDetailResponse,
)
from deployment_api.utils.storage_facade import get_object_metadata, list_objects


class _Sentinel:
    """Marker class: unique singleton for "this branch did not match"."""


_SENTINEL = _Sentinel()

# UI sentinels passed when the click site doesn't have an instrument_type
# axis in scope (DeFi protocol drilldown — only data_type and composite
# venue are known). The resolver scans CONTRACT_REGISTRY for any
# (asset_group, *, data_type) tuple and returns the first deterministic match.
_AUTO_SENTINELS: frozenset[str] = frozenset({"AUTO", "UNKNOWN", "AUTO_DETECT_FAIL", ""})

logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------

# Sample-rows cap per response.  Kept low because the response is JSON —
# larger dumps belong in the CSV / signed-URL download path.
_SAMPLE_ROW_LIMIT: int = 100

# Signed URL TTL for the parquet download link.
_SIGNED_URL_TTL_SECONDS: int = 3600

# Closed-set membership check for the v8 ``service_emission_state`` manifest
# column. Mirrors UAC ``ServiceEmissionStateEnum`` (see
# ``unified_api_contracts.canonical.crosscutting.service_emission_state``) —
# kept as a frozenset of literal strings so the hot path avoids the enum-
# constructor cost on every manifest row read. Drift between this set and the
# UAC enum is review-blocking; the Literal in
# ``deployment_api/types/shard_detail.py`` owns the type-level surface.
_VALID_SERVICE_EMISSION_STATES: frozenset[str] = frozenset(
    {"PUBLISHED_OK", "PUBLISHED_DEGRADED", "STALE_DATA_HEARTBEAT_ONLY", "BLOCKED"}
)

# Data types that are always grouped / bundle-style — one parquet per
# ``(venue, day)`` holds many symbols (strikes, expiries, pool addresses, …).
# Classification of these short-circuits to ``grouped`` regardless of
# instrument_type.
_GROUPED_DATA_TYPES: frozenset[str] = frozenset(
    {
        "options_chain",
        "futures_chain",
        "combo_chain",
        "dex_pools",
        "dex_swaps",
        "liquidation_events",
        "flash_loan_events",
        "token_transfers",
        "bridge_events",
        "mev_events",
        "governance_events",
        "position_data",
        "staking_yields",
    }
)

# Per-symbol instrument types — one parquet per instrument per day.
_PER_SYMBOL_INSTRUMENT_TYPES: frozenset[str] = frozenset(
    {"PERPETUAL", "SPOT_PAIR", "SPOT", "EQUITY", "FUTURE"}
)

# Shard-class defaults per service when the (category, instrument_type,
# data_type) tuple is otherwise ambiguous.  instruments-service always
# publishes reference data; sports publishes fixtures under instruments
# bucket but is handled as its own branch.
_SERVICE_DEFAULT_SHARD_CLASS: dict[str, ShardClassLiteral] = {
    "instruments-service": "reference",
    "corporate-actions": "reference",
}


def _classify_shard(
    *,
    service: str,
    category: str,
    instrument_type: str,
    data_type: str,
) -> ShardClassLiteral:
    """Resolve ``shard_class`` from the shard coordinate.

    Precedence:

    1. ``SPORTS`` category → ``fixtures`` (the sports data pipeline keys
       everything off the league x day fixtures parquet).
    2. Service default — instruments-service / corporate-actions publish
       reference data regardless of instrument_type.
    3. ``data_type`` in the grouped bundle set (``options_chain``,
       ``dex_swaps``, …) → ``grouped``.
    4. ``instrument_type`` uppercase is in the per-symbol set → ``per_symbol``.
    5. Fallback → ``grouped`` (safer default for unknown shards — the
       ``instrument_list`` branch can surface an empty list without
       misrepresenting the data as time-series).
    """
    _ = service  # referenced below via _SERVICE_DEFAULT_SHARD_CLASS
    cat_upper = (category or "").upper()
    if cat_upper == "SPORTS":
        return "fixtures"

    svc_default = _SERVICE_DEFAULT_SHARD_CLASS.get((service or "").lower())
    if svc_default is not None:
        return svc_default

    dt_lower = (data_type or "").lower()
    if dt_lower in _GROUPED_DATA_TYPES:
        return "grouped"

    it_upper = (instrument_type or "").upper()
    if it_upper in _PER_SYMBOL_INSTRUMENT_TYPES:
        return "per_symbol"

    return "grouped"


# ---------------------------------------------------------------------------
# Schema lookup (mirrors data_status_drilldown.get_schema_for_shard but
# surfaces the new ColumnSpec fields — ``required`` and
# ``provided_by_venues`` — into the response so the UI can split Core vs
# Venue-specific columns).
# ---------------------------------------------------------------------------


def _column_dict(col: object) -> ShardSchemaColumn:
    """Build a ShardSchemaColumn from a UAC ColumnSpec instance.

    Uses ``getattr`` against the pydantic model so this compiles even when
    UAC's ColumnSpec gains additional fields in later versions.
    """
    name = str(getattr(col, "name", ""))
    dtype = str(getattr(col, "dtype", ""))
    nullable_raw = getattr(col, "nullable", False)
    nullable = bool(nullable_raw) if isinstance(nullable_raw, bool) else False
    required_raw = getattr(col, "required", True)
    required = bool(required_raw) if isinstance(required_raw, bool) else True
    provided_by_venues_raw: object = getattr(col, "provided_by_venues", None)
    provided_by_venues: list[str] | None = None
    if isinstance(provided_by_venues_raw, (frozenset, set, list, tuple)):
        collected: list[str] = []
        # Iterate via an explicit object cast — provided_by_venues_raw
        # carries Unknown element types from UAC's pydantic model.
        for v in list(cast("list[object]", provided_by_venues_raw)):  # pyright: ignore[reportUnknownArgumentType]
            collected.append(str(v))
        provided_by_venues = sorted(collected)
    description_raw: object = getattr(col, "description", None)
    description = str(description_raw) if description_raw else ""
    return ShardSchemaColumn(
        name=name,
        dtype=dtype,
        nullable=nullable,
        required=required,
        provided_by_venues=provided_by_venues,
        description=description,
    )


def _resolve_instrument_type_auto(
    *, category: str, data_type: str, venue: str | None
) -> str | None:
    """Resolve an ``instrument_type`` for a (category, data_type) pair.

    Used when the UI passes ``instrument_type=AUTO`` (or one of the other
    sentinels in :data:`_AUTO_SENTINELS`) because the click site only
    knows the ``data_type`` axis — DeFi protocol drilldowns are the
    canonical case.

    Resolution order:

    1. **Venue override**: any ``(category, venue.upper(), instrument_type,
       data_type)`` tuple in ``VENUE_CONTRACT_OVERRIDES`` is consulted
       first when ``venue`` is supplied — the per-venue schema wins.
    2. **Base registry**: any ``(category.lower(), instrument_type,
       data_type.lower())`` tuple in ``CONTRACT_REGISTRY``. Multiple
       matches are sorted alphabetically and the first one is returned
       so the resolution is deterministic across processes.

    Returns ``None`` if no contract matches.
    """
    cat_norm = (category or "").lower()
    dt_norm = (data_type or "").lower()

    if venue:
        venue_norm = venue.upper()
        venue_matches: list[str] = sorted(
            it
            for (c, v, it, dt) in VENUE_CONTRACT_OVERRIDES
            if c == cat_norm and v == venue_norm and dt == dt_norm
        )
        if venue_matches:
            return venue_matches[0]

    base_matches: list[str] = sorted(
        it for (c, it, dt) in CONTRACT_REGISTRY if c == cat_norm and dt == dt_norm
    )
    if base_matches:
        return base_matches[0]
    return None


def _resolve_schema(
    *, category: str, instrument_type: str, data_type: str, venue: str | None
) -> tuple[ShardSchema, str]:
    """Resolve a ShardSchema from the UAC contract registry.

    Returns ``(schema, resolved_instrument_type)``. The resolved
    instrument_type echoes the caller value when ``instrument_type`` is
    explicit; when the caller passes one of :data:`_AUTO_SENTINELS` the
    registry-derived pick is returned so downstream branches (path
    resolution, shard classification) operate on the concrete axis.
    """
    cat_norm = (category or "").lower()
    is_auto = (instrument_type or "").upper() in _AUTO_SENTINELS

    resolved_via: Literal["explicit", "auto", "none"]
    it_norm: str
    if is_auto:
        auto_pick = _resolve_instrument_type_auto(
            category=category, data_type=data_type, venue=venue
        )
        if auto_pick is None:
            return (
                ShardSchema(
                    registered=False,
                    source="none",
                    symbol_column=None,
                    columns=[],
                    message=(
                        f"No SchemaContract found in UAC registry for "
                        f"category={cat_norm!r} data_type={data_type!r}. "
                        "Caller passed instrument_type=AUTO and the registry "
                        "scan returned no matches."
                    ),
                    instrument_type_resolved_via="none",
                    instrument_type_resolved=None,
                ),
                instrument_type,
            )
        it_norm = auto_pick
        resolved_via = "auto"
    else:
        it_norm = (
            (instrument_type or "").lower()
            if (instrument_type or "").isupper()
            else instrument_type
        )
        resolved_via = "explicit"

    dt_norm = (data_type or "").lower() if (data_type or "").isupper() else data_type
    try:
        contract: SchemaContract = lookup_contract(
            asset_group=cat_norm,
            instrument_type=it_norm,
            data_type=dt_norm,
            venue=venue,
        )
    except SchemaContractNotFoundError:
        return (
            ShardSchema(
                registered=False,
                source="none",
                symbol_column=None,
                columns=[],
                message=(
                    "No contract registered for this shard. "
                    "The UI should fall back to projecting actual parquet columns."
                ),
                instrument_type_resolved_via=resolved_via,
                instrument_type_resolved=it_norm,
            ),
            it_norm,
        )

    override_key = (cat_norm, (venue or "").upper(), it_norm, dt_norm)
    is_override = venue is not None and override_key in VENUE_CONTRACT_OVERRIDES

    columns = [_column_dict(c) for c in contract.columns]
    source: Literal["CONTRACT_REGISTRY", "VENUE_CONTRACT_OVERRIDES", "none"] = (
        "VENUE_CONTRACT_OVERRIDES" if is_override else "CONTRACT_REGISTRY"
    )
    return (
        ShardSchema(
            registered=True,
            source=source,
            symbol_column=contract.symbol_column,
            columns=columns,
            message="",
            instrument_type_resolved_via=resolved_via,
            instrument_type_resolved=it_norm,
        ),
        it_norm,
    )


# ---------------------------------------------------------------------------
# GCS path resolution + manifest lookup
# ---------------------------------------------------------------------------


def _defi_composite_parts(venue: str | None) -> tuple[str | None, str | None]:
    """Return ``(protocol, chain)`` for a DeFi composite venue string.

    ``AAVE_V3-ETHEREUM`` → ``("AAVE_V3", "ETHEREUM")``.  ``ETHEREUM`` → ``(None, "ETHEREUM")``.
    ``None`` → ``(None, None)``.
    """
    if not venue:
        return (None, None)
    if "-" in venue:
        head, _, tail = venue.partition("-")
        return (head or None, tail or None)
    return (None, venue)


def _list_first_parquet(bucket: str, prefix: str) -> str | None:
    """Return the first ``.parquet`` object name under ``prefix`` or ``None``."""
    try:
        objects = list_objects(bucket, prefix, max_results=10)
    except (OSError, RuntimeError) as exc:
        logger.warning("list_objects failed for %s/%s: %s", bucket, prefix, exc)
        return None
    for o in objects:
        name = getattr(o, "name", None)
        if isinstance(name, str) and name.endswith(".parquet"):
            return name
    return None


def _mtds_shard_path(
    *,
    bucket: str,
    cat_lower: str,
    instrument_type: str,
    data_type: str,
    venue: str | None,
    day: str,
    underlying: str | None,
    instrument_id: str | None,
) -> tuple[str, str] | None:
    """MTDS-family parquet path resolution — isolated for complexity budget.

    Builds a ``category={ag}/`` prefix; ``list_objects`` (via the dual-vocab
    helper in ``storage_facade.list_objects``) transparently fans out to
    both ``category=`` (legacy on-disk) and ``asset_group=`` (canonical
    new writes).  Per CLAUDE.md SSOT the two coexist on disk without a
    re-keying migration.  When ``leaf`` is supplied (full path known) we
    list the venue+data_type level prefix and pick the parquet matching
    the leaf — this makes the hot path correct under either vocabulary
    rather than blindly returning a path that may not exist on disk.
    """
    it_disk = (instrument_type or "").lower()
    dt_disk = (data_type or "").lower()
    venue_disk = (venue or "").upper()
    prefix = (
        f"raw_tick_data/by_date/day={day}/category={cat_lower}/"
        f"venue={venue_disk}/instrument_type={it_disk}/data_type={dt_disk}/"
    )
    leaf = instrument_id or underlying
    is_derivative_bundle = dt_disk in _GROUPED_DATA_TYPES and it_disk in {
        "options_chain",
        "futures_chain",
    }
    if leaf and is_derivative_bundle:
        # underlying= bundle: leaf is the underlying; pick the existing
        # path under either hive vocabulary.
        target_suffix = f"underlying={leaf}/ticks.parquet"
        for o in list_objects(bucket, prefix, max_results=200):
            n = getattr(o, "name", "")
            if isinstance(n, str) and n.endswith(target_suffix):
                return (bucket, n)
        return None
    if leaf:
        target_suffix = f"/{leaf}.parquet"
        for o in list_objects(bucket, prefix, max_results=200):
            n = getattr(o, "name", "")
            if isinstance(n, str) and n.endswith(target_suffix):
                return (bucket, n)
        return None
    # No leaf: list the prefix and return the first parquet.
    name = _list_first_parquet(bucket, prefix)
    if name is None:
        return None
    return (bucket, name)


def _gcs_path_for_shard(
    *,
    service: str,
    category: str,
    instrument_type: str,
    data_type: str,
    venue: str | None,
    day: str,
    underlying: str | None,
    instrument_id: str | None,
) -> tuple[str, str] | None:
    """Best-effort resolution of the primary parquet for a shard.

    Returns ``(bucket, object_path)`` for the file shard-detail should
    read, or ``None`` when no matching file exists.  The file is a
    *representative* member of the shard — for grouped data_types it is
    the one bundle parquet per ``(venue, day)``; for per_symbol shards it
    is the single symbol parquet.
    """
    svc = (service or "").lower()
    cat_lower = (category or "").lower()
    day_clean = str(day)

    try:
        bucket = build_bucket_name(service, category)
    except ValueError:
        return None

    # instruments-service / corporate-actions: one parquet per (venue, day)
    if svc in {"instruments-service", "corporate-actions"}:
        if cat_lower == "sports":
            # Sports fixtures live under sports_reference (see
            # data_status_drilldown._entity_gs_uri).  The shard-detail
            # endpoint delegates to build_fixture_breakdown for the
            # fixtures branch so we do not resolve a single parquet here.
            return None
        path = f"instrument_availability/by_date/day={day_clean}/venue={venue}/instruments.parquet"
        return (bucket, path)

    # MTDS family — hive-partitioned by (day, category, venue, instrument_type, data_type).
    if svc in {"market-tick-data-service", "market-data-processing-service"}:
        return _mtds_shard_path(
            bucket=bucket,
            cat_lower=cat_lower,
            instrument_type=instrument_type,
            data_type=data_type,
            venue=venue,
            day=day_clean,
            underlying=underlying,
            instrument_id=instrument_id,
        )

    # Default: feature / strategy pipelines not surfaced via this endpoint yet.
    return None


def _manifest_coord_mask(
    df: pd.DataFrame,
    *,
    day: str,
    venue: str | None,
    data_type: str,
    instrument_id: str | None,
) -> pd.Series:  # pyright: ignore[reportMissingTypeArgument]
    """Build the boolean pandas mask matching one shard coordinate."""
    mask = df["date"] == day
    if venue and "venue" in df.columns:
        mask = mask & (df["venue"] == venue)
    if data_type and "data_type" in df.columns:
        mask = mask & (df["data_type"] == data_type)
    if instrument_id and "instrument_id" in df.columns:
        mask = mask & (df["instrument_id"] == instrument_id)
    return mask


def _manifest_row_for_coord(
    *, bucket: str, venue: str | None, day: str, data_type: str, instrument_id: str | None
) -> dict[str, str] | None:
    """Return the manifest row matching the shard coordinate or ``None``.

    The availability manifest (`read_availability_index`) is the
    authoritative source for ``capture_status`` + ``error_reason`` +
    ``attempted_at``.  Missing manifest → returns ``None`` so the caller
    can fall back to a ``missing`` capture status derived from the GCS
    stat call.
    """
    try:
        df = read_availability_index(bucket)
    except (OSError, RuntimeError, ValueError) as exc:
        logger.warning("manifest read failed for %s: %s", bucket, exc)
        return None

    if df.empty or "date" not in df.columns:
        return None

    mask = _manifest_coord_mask(
        df, day=day, venue=venue, data_type=data_type, instrument_id=instrument_id
    )
    scoped = df.loc[mask]
    if scoped.empty:
        return None

    if "written_at" in scoped.columns:
        scoped = scoped.sort_values("written_at").tail(1)
    row_records = scoped.to_dict(orient="records")  # pyright: ignore[reportUnknownMemberType,reportUnknownVariableType]
    row_list = cast(list[object], row_records)
    if not row_list:
        return None
    raw = row_list[0]
    if not isinstance(raw, dict):
        return None
    out: dict[str, str] = {}
    for k, v in cast(dict[object, object], raw).items():
        out[str(k)] = "" if v is None else str(v)
    return out


def _classify_legacy_empty_reason(
    *,
    status: str,
    error_reason: str | None,
    manifest: dict[str, str],
    asset_group: str | None,
) -> str | None:
    """Reader-side fallback for legacy ``empty_confirmed`` manifest rows.

    Returns ``error_reason`` unchanged unless the row is a pre-Phase-2.E.2
    legacy row (status=empty_confirmed AND error_reason empty). In that
    case classify on the fly via the UTL helper — same SSOT the Tier 3D.1
    reconciler uses for batch back-fill so the UI sees a typed reason
    immediately, without waiting for the reconciler to land on this row.
    """
    if error_reason is not None or status != "empty_confirmed" or not asset_group:
        return error_reason
    ag_lower = asset_group.lower()
    if ag_lower not in LEGACY_REASON_ASSET_GROUPS:
        return error_reason
    try:
        return classify_legacy_empty_row(ag_lower, manifest)
    except (ValueError, TypeError, KeyError, AttributeError) as exc:
        logger.warning(
            "classify_legacy_empty_row failed for %s row %s: %s",
            ag_lower,
            manifest,
            exc,
        )
        return error_reason


def _gcs_metadata(
    *,
    bucket: str | None,
    object_path: str | None,
    manifest: dict[str, str] | None,
    pq_row_count: int | None,
    asset_group: str | None = None,
) -> ShardGcsMetadata:
    """Build the ``gcs`` block of the shard-detail response.

    When ``asset_group`` is provided and the manifest row has
    ``capture_status=empty_confirmed`` AND a missing ``error_reason``,
    :func:`_classify_legacy_empty_reason` classifies at read-time
    (writegate Tier 3D.2 reader-side fallback) so the UI sees a typed
    reason for legacy rows immediately, without waiting for the
    Tier 3D.1 batch back-fill to land on this row.
    """
    full_path = f"gs://{bucket}/{object_path}" if bucket and object_path else None
    size_bytes: int | None = None
    captured_at: str | None = None
    if bucket and object_path:
        try:
            meta = get_object_metadata(bucket, object_path)
        except (OSError, RuntimeError) as exc:
            logger.warning("get_object_metadata failed for %s/%s: %s", bucket, object_path, exc)
            meta = None
        if meta:
            size_raw = meta.get("size")
            if isinstance(size_raw, int):
                size_bytes = size_raw
            updated_raw = meta.get("updated")
            if updated_raw is not None:
                captured_at = str(updated_raw)

    status: CaptureStatusLiteral
    error_reason: str | None = None
    pipeline_mode: str | None = None
    service_emission_state: ServiceEmissionStateLiteral | None = None
    last_emission_decision_at: str | None = None
    expected_window_completeness_fraction: float | None = None
    if manifest is not None:
        manifest_status = (manifest.get("capture_status") or "").lower()
        if manifest_status in {"captured", "empty_confirmed", "attempted_failed"}:
            status = cast(CaptureStatusLiteral, manifest_status)
        elif size_bytes is not None:
            status = "captured"
        else:
            status = "missing"
        err = manifest.get("error_reason") or ""
        error_reason = err or None
        error_reason = _classify_legacy_empty_reason(
            status=status,
            error_reason=error_reason,
            manifest=manifest,
            asset_group=asset_group,
        )
        attempted_at_raw = manifest.get("attempted_at")
        if not captured_at and attempted_at_raw:
            captured_at = attempted_at_raw

        # v8 manifest columns (writegate Phase 4). Forward-compatible:
        # pre-v8 manifest rows lack these keys, so ``manifest.get(...)``
        # returns ``None`` / empty string and the field stays ``None``.
        pipeline_mode_raw = manifest.get("pipeline_mode") or ""
        pipeline_mode = pipeline_mode_raw or None

        emission_state_raw = manifest.get("service_emission_state") or ""
        if emission_state_raw in _VALID_SERVICE_EMISSION_STATES:
            service_emission_state = cast(ServiceEmissionStateLiteral, emission_state_raw)
        # else: drop silently — invalid value on disk would otherwise leak
        # an off-closed-set string into the API response. ``None`` matches
        # the pre-v8 pre-existence shape so the UI renders the same
        # placeholder pill.

        last_emission_decision_raw = manifest.get("last_emission_decision_at") or ""
        last_emission_decision_at = last_emission_decision_raw or None

        completeness_raw = manifest.get("expected_window_completeness_fraction") or ""
        if completeness_raw:
            try:
                expected_window_completeness_fraction = float(completeness_raw)
            except ValueError as exc:
                logger.warning(
                    "expected_window_completeness_fraction parse failed for row %s: %s",
                    manifest,
                    exc,
                )
    else:
        status = "captured" if size_bytes is not None else "missing"

    return ShardGcsMetadata(
        path=full_path,
        file_size_bytes=size_bytes,
        row_count=pq_row_count,
        captured_at=captured_at,
        capture_status=status,
        error_reason=error_reason,
        pipeline_mode=pipeline_mode,
        service_emission_state=service_emission_state,
        last_emission_decision_at=last_emission_decision_at,
        expected_window_completeness_fraction=expected_window_completeness_fraction,
    )


# ---------------------------------------------------------------------------
# Parquet sample + distinct-symbol extraction (read-only, shard-isolated)
# ---------------------------------------------------------------------------


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


def _sample_rows(
    *, gs_uri: str, limit: int, schema_columns: list[ShardSchemaColumn]
) -> list[dict[str, object]]:
    """Return the first ``limit`` rows of the parquet as a list of dicts.

    Only projects the first 20 declared columns (or all columns when the
    schema is unknown) to keep the JSON payload small.
    """
    project_cols: list[str] | None = (
        [c.name for c in schema_columns[:20]] if schema_columns else None
    )
    try:
        df: pd.DataFrame = _read_parquet_columns(gs_uri, project_cols)
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
        df: pd.DataFrame = _read_parquet_columns(gs_uri, [symbol_column])
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
    try:
        from google.cloud import storage  # pyright: ignore[reportMissingImports]
    except ImportError:
        logger.debug("google-cloud-storage not importable; no signed URL available")
        return None
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
        "asset_group": category,
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
    asset_group: str,
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
    # Resolve schema first — when the UI passes instrument_type=AUTO the
    # resolver scans the registry and returns the concrete instrument_type.
    # Use the resolved value for downstream classification + path lookup
    # so a single coordinate is consistent across all branches of the
    # response.
    schema, resolved_instrument_type = _resolve_schema(
        category=asset_group,
        instrument_type=instrument_type,
        data_type=data_type,
        venue=venue,
    )

    shard_class = _classify_shard(
        service=service,
        category=asset_group,
        instrument_type=resolved_instrument_type,
        data_type=data_type,
    )

    # Fixtures branch has its own parquet layout (sports_reference) — we
    # do not hit _gcs_path_for_shard for sports.
    bucket: str | None = None
    object_path: str | None = None
    if shard_class != "fixtures":
        resolved = _gcs_path_for_shard(
            service=service,
            category=asset_group,
            instrument_type=resolved_instrument_type,
            data_type=data_type,
            venue=venue,
            day=day,
            underlying=underlying,
            instrument_id=instrument_id,
        )
        if resolved is not None:
            bucket, object_path = resolved

    gs_uri: str | None = f"gs://{bucket}/{object_path}" if bucket and object_path else None

    pq_row_count: int | None = None
    sample_rows: list[dict[str, object]] = []
    if gs_uri is not None:
        pq_row_count = _read_parquet_footer_row_count(gs_uri)
        sample_rows = _sample_rows(
            gs_uri=gs_uri, limit=_SAMPLE_ROW_LIMIT, schema_columns=schema.columns
        )

    # Manifest capture_status lookup — shard-scoped, never fatal.
    manifest_row: dict[str, str] | None = None
    if bucket is not None:
        manifest_row = _manifest_row_for_coord(
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
        asset_group=asset_group,
    )

    download_urls = ShardDownloadUrls(
        parquet_signed_url=_parquet_signed_url(bucket, object_path),
        csv_projected=_csv_projection_url(
            service=service,
            category=asset_group,
            venue=venue,
            day=day,
            instrument_type=instrument_type,
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
        distinct = (
            _distinct_symbols(gs_uri=gs_uri, symbol_column=schema.symbol_column)
            if gs_uri is not None
            else []
        )
        payload_grouped = ShardPayloadGrouped(instrument_list=distinct)
    elif shard_class == "per_symbol":
        leaf = instrument_id or underlying or ""
        instrument_list: list[dict[str, str]] = (
            [{"key": leaf, "type": schema.symbol_column or "instrument_id"}] if leaf else []
        )
        payload_per_symbol = ShardPayloadPerSymbol(instrument_list=instrument_list)
    elif shard_class == "reference":
        payload_reference = ShardPayloadReference(
            instrument_definitions=list(sample_rows) if sample_rows else []
        )
    elif shard_class == "fixtures":
        payload_fixtures = ShardPayloadFixtures(fixtures=list(sample_rows) if sample_rows else [])

    coord = ShardCoord(
        service=service,
        asset_group=asset_group,
        instrument_type=instrument_type,
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


def _instruments_bucket_for_category(category: str) -> str:
    return build_bucket("instruments", project_id=_pid, asset_group=category.lower())


# Compiled regex for stripping the underscore in DeFi protocol-version
# tokens, e.g. ``AAVE_V3`` → ``AAVEV3``, ``UNISWAP_V3`` → ``UNISWAPV3``.
# This handles the convention mismatch between UAC composite venues
# (``<PROTOCOL>_V<N>-<CHAIN>``) and the actual GCS partition layout
# (``<PROTOCOL>V<N>-<CHAIN>``) that the instruments-service writers use.
_DEFI_VERSION_UNDERSCORE_RE = _re.compile(r"_V(\d+)")


def _venue_aliases_for_bucket(category: str, venue: str) -> list[str]:
    """Return the list of venue strings to try when resolving a GCS path.

    The UI / UAC and the instruments-service GCS writer disagree on a few
    naming conventions. Rather than picking one canonical form, we try the
    plausible aliases in order and use whichever the bucket actually has.

    Conventions per category:

    * **DEFI**: try the literal venue first, then strip the underscore
      from version tokens (``AAVE_V3-ETHEREUM`` → ``AAVEV3-ETHEREUM``),
      and the reverse (``AAVEV3-ETHEREUM`` → ``AAVE_V3-ETHEREUM``).
    * **SPORTS**: the partition key is ``league=<NAME>`` not ``venue=<NAME>``
      — handled by ``_partition_key_for_category`` rather than aliasing.
    * **CEFI / TRADFI / PREDICTION**: venue is canonical — single alias.
    """
    aliases: list[str] = [venue]
    cat_upper = (category or "").upper()
    if cat_upper == "DEFI":
        # Strip the underscore in _V<N>: "AAVE_V3-ETHEREUM" → "AAVEV3-ETHEREUM"
        no_underscore = _DEFI_VERSION_UNDERSCORE_RE.sub(r"V\1", venue)
        if no_underscore != venue:
            aliases.append(no_underscore)
        # And the reverse — if caller passed "AAVEV3-ETHEREUM", try "AAVE_V3-ETHEREUM".
        # Match an upper-case-letter prefix immediately followed by V<digits>.
        with_underscore = _re.sub(r"([A-Z])(V\d+)", r"\1_\2", venue)
        if with_underscore != venue and with_underscore not in aliases:
            aliases.append(with_underscore)
    return aliases


def _partition_key_for_category(category: str) -> str:
    """Return the GCS partition key name used by the instruments-service writer.

    SPORTS partitions by ``league=<NAME>``; every other category partitions
    by ``venue=<NAME>``. The instrument-axis difference reflects that sports
    fixture metadata is keyed off the league, not off any single data
    provider.
    """
    if (category or "").upper() == "SPORTS":
        return "league"
    return "venue"


def _read_instruments_day_df(
    *, bucket: str, venue: str, day: str, category: str = ""
) -> pd.DataFrame | None:
    """Read the instruments-service per-(venue, day) parquet, or ``None``.

    Tries every venue alias from :func:`_venue_aliases_for_bucket`. Falls
    back to a nested-partition listing when the leaf doesn't exist —
    SPORTS uses
    ``day={day}/league={league}/venue={data_provider}/instruments.parquet``
    where multiple data providers can publish the same league. We
    enumerate every ``instruments.parquet`` under the league directory
    and concat them so the caller sees the union.

    Shard-isolated: swallows every IO error and returns ``None``.
    """
    aliases = _venue_aliases_for_bucket(category, venue)
    partition_key = _partition_key_for_category(category)
    last_err: Exception | None = None
    for alias in aliases:
        leaf_uri = (
            f"gs://{bucket}/instrument_availability/by_date/day={day}/"
            f"{partition_key}={alias}/instruments.parquet"
        )
        try:
            df = _read_parquet_columns(leaf_uri, None)
            if df is not None and not df.empty:
                return df
        except (OSError, ValueError, RuntimeError) as exc:
            last_err = exc

        # Nested-partition fallback (SPORTS sub-provider layout).
        nested_prefix = f"instrument_availability/by_date/day={day}/{partition_key}={alias}/"
        try:
            nested_objs = list_objects(bucket, nested_prefix, max_results=200)
            parquet_paths = [
                getattr(o, "name", "")
                for o in nested_objs
                if isinstance(getattr(o, "name", None), str)
                and getattr(o, "name", "").endswith("instruments.parquet")
            ]
            frames: list[pd.DataFrame] = []
            for path in parquet_paths:
                gs_uri = f"gs://{bucket}/{path}"
                try:
                    sub_df = _read_parquet_columns(gs_uri, None)
                    if sub_df is not None and not sub_df.empty:
                        frames.append(sub_df)
                except (OSError, ValueError, RuntimeError) as exc:
                    last_err = exc
                    continue
            if frames:
                return pd.concat(frames, ignore_index=True)
        except (OSError, RuntimeError) as exc:
            last_err = exc
            continue
    if last_err is not None:
        logger.warning(
            "instruments day read failed for bucket=%s aliases=%s day=%s: %s",
            bucket,
            aliases,
            day,
            last_err,
        )
    return None


def _list_day_prefixes(bucket: str) -> list[str]:
    """List the ``day=YYYY-MM-DD`` sub-directories under ``instrument_availability/by_date/``.

    Uses ``google.cloud.storage`` directly with ``delimiter='/'`` so we
    get the day-level directories without paging through every leaf file.
    The UCI ``StorageClient`` wrapper strips the ``.prefixes`` attribute
    that this approach requires, so we go to the raw SDK for this one
    listing — the bucket name and project are still derived from
    ``UnifiedCloudConfig`` via ``_pid``.
    """
    from google.cloud import storage as _gcs

    try:
        gcs_client = _gcs.Client(project=_pid)
        prefix = "instrument_availability/by_date/"
        it = gcs_client.list_blobs(bucket, prefix=prefix, delimiter="/")
        list(it)  # consume to populate ``.prefixes``
        raw_prefixes: object = getattr(it, "prefixes", set())
        if not isinstance(raw_prefixes, (set, frozenset, list, tuple)):
            return []
        days: list[str] = []
        for p in cast(list[object], list(raw_prefixes)):
            if not isinstance(p, str):
                continue
            sub = p.removeprefix(prefix).rstrip("/")
            if sub.startswith("day="):
                days.append(sub.removeprefix("day="))
        days.sort()
        return days
    except (OSError, RuntimeError) as exc:
        logger.warning("list day prefixes failed for %s: %s", bucket, exc)
        return []


_RECENT_DAYS_PROBE_WINDOW: int = 120
"""How many days backwards from today to probe directly before falling back
to the full ``day=*`` prefix listing. 120 days covers the common case
(latest data is within the last few months) without paying the cost of
listing every day prefix in buckets that span years x dozens of leagues
x multiple data providers.
"""


def _pick_latest_day(bucket: str, venue: str, category: str = "") -> str | None:
    """Return the latest ``day=YYYY-MM-DD`` for which the venue partition exists.

    Strategy:

    1. **Recent-day probe (fast path)**: walk backwards from today over
       the last :data:`_RECENT_DAYS_PROBE_WINDOW` days. For each candidate
       day, probe ``day={D}/{partition_key}={alias}/`` directly via
       ``list_objects(max_results=1)``. Return on first hit. This is the
       common case (latest data is days-to-weeks old) and avoids the
       cost of fully enumerating the bucket's day-prefix tree.

    2. **Full-prefix fallback (slow path)**: only when the recent-day
       probe finds nothing, enumerate every ``day=*`` sub-directory and
       walk in reverse-chronological order. Useful for rarely-updated
       venues whose latest day is older than the probe window.

    Handles the DEFI ``AAVE_V3`` vs ``AAVEV3`` alias mismatch and the
    SPORTS ``league=<NAME>`` partition key via
    :func:`_venue_aliases_for_bucket` and
    :func:`_partition_key_for_category`.
    """
    import datetime as _dt

    aliases = _venue_aliases_for_bucket(category, venue)
    partition_key = _partition_key_for_category(category)

    def _probe(day: str) -> bool:
        for alias in aliases:
            test_prefix = f"instrument_availability/by_date/day={day}/{partition_key}={alias}/"
            try:
                if list_objects(bucket, test_prefix, max_results=1):
                    return True
            except (OSError, RuntimeError) as exc:
                logger.debug("list_objects probe failed for %s/%s: %s", bucket, test_prefix, exc)
                continue
        return False

    today = _dt.date.today()
    # Sports fixture catalogs publish forward-dated entries (scheduled
    # fixtures up to a month out), so walk forward 30 days first, then
    # backwards through the probe window.
    for delta in range(-30, _RECENT_DAYS_PROBE_WINDOW):
        candidate = (today - _dt.timedelta(days=delta)).isoformat()
        if _probe(candidate):
            return candidate

    # Fallback: scan the full prefix tree (may be slow for huge buckets).
    days = _list_day_prefixes(bucket)
    if not days:
        return None
    for day in reversed(days):
        if _probe(day):
            return day
    return None


def _cefi_venue_detail(category: str, venue: str) -> VenueDetailResponse:
    """CeFi / TradFi / Sports / Prediction branch — generic venue-detail.

    Despite the name this handles every non-DEFI category. The ``category``
    is passed through to the bucket-aware helpers so they can apply the
    DEFI underscore aliasing or the SPORTS ``league=`` partition key when
    needed.
    """
    bucket = _instruments_bucket_for_category(category)
    day = _pick_latest_day(bucket, venue, category=category)
    instruments: list[dict[str, object]] = []
    if day is not None:
        df = _read_instruments_day_df(bucket=bucket, venue=venue, day=day, category=category)
        if df is not None and not df.empty:
            records_raw = df.to_dict(orient="records")  # pyright: ignore[reportUnknownMemberType,reportUnknownVariableType]
            records = cast(list[object], records_raw)
            for r in records[:500]:
                typed = _typed_row(r)
                if typed is not None:
                    instruments.append(typed)
    return VenueDetailResponse(
        asset_group=category.upper(),
        venue=venue,
        day=day,
        total_instruments=len(instruments),
        instruments=instruments,
    )


def _is_pool_row(row: dict[str, object]) -> bool:
    """Row represents a DeFi pool when it carries pool_address or pool_id."""
    return bool(row.get("pool_address") or row.get("pool_id"))


def _json_safe_numeric(v: object) -> object | _Sentinel:
    """Coerce numpy/python numerics. Returns _SENTINEL when v is not numeric-shaped."""
    if isinstance(v, float) and _math.isnan(v):
        return None
    if isinstance(v, _np.bool_):
        return bool(v)
    if isinstance(v, _np.integer):
        return int(v)
    if isinstance(v, _np.floating):
        f = float(v)
        return None if _math.isnan(f) else f
    return _SENTINEL


def _json_safe_temporal(v: object) -> object | _Sentinel:
    """Coerce pandas/datetime values to ISO-8601. Returns _SENTINEL when not temporal."""
    if isinstance(v, pd.Timestamp):
        return None if pd.isna(v) else v.isoformat()
    if isinstance(v, (_dt.datetime, _dt.date)):
        return v.isoformat()
    return _SENTINEL


def _json_safe_container(v: object) -> object | _Sentinel:
    """Recursively convert containers. Returns _SENTINEL when v is not a container."""
    if isinstance(v, _np.ndarray):
        return [_json_safe_value(x) for x in v.tolist()]
    if isinstance(v, (list, tuple)):
        return [_json_safe_value(x) for x in v]
    if isinstance(v, dict):
        return {str(k): _json_safe_value(val) for k, val in cast(dict[object, object], v).items()}
    if isinstance(v, (set, frozenset)):
        return [_json_safe_value(x) for x in v]
    return _SENTINEL


def _json_safe_value(v: object) -> object:
    """Convert pandas / numpy / Timestamp scalars into JSON-serialisable Python primitives.

    Pydantic-core's JSON serialiser refuses to encode ``numpy.int64``,
    ``numpy.float64``, ``pandas.Timestamp``, ``pandas.NaT``, and bare ``float('nan')``
    on ``int``-typed fields (raises ``TypeError: 'float' object cannot be interpreted
    as an integer``). This helper normalises every value into a primitive that JSON
    can carry losslessly.
    """
    if v is None:
        return None
    numeric = _json_safe_numeric(v)
    if numeric is not _SENTINEL:
        return numeric
    temporal = _json_safe_temporal(v)
    if temporal is not _SENTINEL:
        return temporal
    container = _json_safe_container(v)
    if container is not _SENTINEL:
        return container
    if isinstance(v, bytes):
        return v.decode("utf-8", errors="replace")
    # Generic pandas NA sentinel — covers Int64 / Float64 / boolean dtypes.
    try:
        if pd.isna(v):  # pyright: ignore[reportArgumentType]
            return None
    except (TypeError, ValueError):
        pass
    # Anything still exotic (custom numpy scalar subclass, decimal.Decimal, etc.)
    # gets stringified as a last-resort fallback so pydantic-core never sees a
    # type its JSON serialiser refuses.
    if isinstance(v, (str, int, float, bool)):
        return v
    return str(v)


def _typed_row(raw: object) -> dict[str, object] | None:
    """Coerce a pandas-records raw row into a strictly typed JSON-safe ``dict[str, object]``."""
    if not isinstance(raw, dict):
        return None
    typed: dict[str, object] = {}
    for k, v in cast(dict[object, object], raw).items():
        typed[str(k)] = _json_safe_value(v)
    return typed


def _defi_composite_detail(
    df: pd.DataFrame, *, venue: str, chain: str, protocol: str, day: str
) -> VenueDetailResponse:
    """Build a composite (protocol-chain) DeFi venue-detail response."""
    if "protocol" in df.columns:
        df = df[df["protocol"].astype(str).str.upper() == protocol.upper()]
    pools: list[dict[str, object]] = []
    tokens: list[dict[str, object]] = []
    records_raw = df.to_dict(orient="records")  # pyright: ignore[reportUnknownMemberType,reportUnknownVariableType]
    records = cast(list[object], records_raw)
    for r in records[:1_000]:
        typed = _typed_row(r)
        if typed is None:
            continue
        (pools if _is_pool_row(typed) else tokens).append(typed)
    return VenueDetailResponse(
        asset_group="DEFI",
        venue=venue,
        chain=chain,
        protocol=protocol,
        total_pools=len(pools),
        total_tokens=len(tokens),
        pools=pools,
        tokens=tokens,
        day=day,
    )


def _defi_chain_only_detail(
    df: pd.DataFrame, *, venue: str, chain: str, day: str
) -> VenueDetailResponse:
    """Build a chain-only DeFi venue-detail response with per-protocol aggregates."""
    protocols_agg: dict[str, dict[str, int]] = {}
    total_pools = 0
    total_tokens = 0
    records_raw = df.to_dict(orient="records")  # pyright: ignore[reportUnknownMemberType,reportUnknownVariableType]
    records = cast(list[object], records_raw)
    for r in records:
        typed = _typed_row(r)
        if typed is None:
            continue
        proto = str(typed.get("protocol") or "UNKNOWN")
        stats = protocols_agg.setdefault(proto, {"pool_count": 0, "token_count": 0})
        if _is_pool_row(typed):
            stats["pool_count"] += 1
            total_pools += 1
        else:
            stats["token_count"] += 1
            total_tokens += 1
    protocols_list: list[dict[str, object]] = [
        {"name": name_, "pool_count": stats["pool_count"], "token_count": stats["token_count"]}
        for name_, stats in sorted(protocols_agg.items())
    ]
    return VenueDetailResponse(
        asset_group="DEFI",
        venue=venue,
        chain=chain,
        protocol=None,
        total_pools=total_pools,
        total_tokens=total_tokens,
        protocols=protocols_list,
        day=day,
    )


def _load_defi_df(
    bucket: str, *, venue: str, chain: str, protocol: str | None, day: str
) -> pd.DataFrame | None:
    """Try the composite file first; fall back to the chain file for composite venues."""
    df = _read_instruments_day_df(bucket=bucket, venue=venue, day=day, category="DEFI")
    if (df is None or df.empty) and protocol is not None:
        df = _read_instruments_day_df(bucket=bucket, venue=chain, day=day, category="DEFI")
    return df


def _defi_venue_detail(venue: str) -> VenueDetailResponse:
    """DeFi branch — understands chain-only vs composite protocol-chain.

    Chain only (``ETHEREUM``) → returns the list of protocols observed on
    that chain plus aggregate pool / token counts.  Composite
    (``AAVE_V3-ETHEREUM``) → returns the pool + token listing scoped to
    that protocol on that chain.
    """
    protocol, chain = _defi_composite_parts(venue)
    if chain is None:
        return VenueDetailResponse(asset_group="DEFI", venue=venue)

    bucket = _instruments_bucket_for_category("defi")
    day = _pick_latest_day(bucket, venue, category="DEFI")
    if day is None:
        return VenueDetailResponse(
            asset_group="DEFI",
            venue=venue,
            chain=chain,
            protocol=protocol,
            day=None,
        )

    df = _load_defi_df(bucket, venue=venue, chain=chain, protocol=protocol, day=day)
    if df is None or df.empty:
        return VenueDetailResponse(
            asset_group="DEFI",
            venue=venue,
            chain=chain,
            protocol=protocol,
            day=day,
        )

    if protocol is not None:
        return _defi_composite_detail(df, venue=venue, chain=chain, protocol=protocol, day=day)
    return _defi_chain_only_detail(df, venue=venue, chain=chain, day=day)


def fetch_venue_detail(*, service: str, asset_group: str, venue: str) -> VenueDetailResponse:
    """Return venue-scoped detail for the Data Status drilldown.

    ``asset_group == "DEFI"`` branches on whether ``venue`` is a bare chain
    (``ETHEREUM``) or a composite protocol-chain (``AAVE_V3-ETHEREUM``);
    all other asset groups use the CeFi branch (latest-day instruments
    listing for the venue).
    """
    _ = service  # Reserved for future per-service routing; keeps the signature
    # stable for the UI caller today.
    cat_upper = (asset_group or "").upper()
    if cat_upper == "DEFI":
        return _defi_venue_detail(venue)
    return _cefi_venue_detail(cat_upper.lower() if cat_upper else "cefi", venue)


# ---------------------------------------------------------------------------
# Public API: get_leaf_parquet_stats (writegate Phase 4.A.3)
# ---------------------------------------------------------------------------
#
# Distinct from `get_schema_for_shard` (declared SchemaContract) and
# `get_shard_detail` (full unified drilldown). This helper computes LIVE
# parquet stats — row count, per-column non-null + NaN ratio, and the
# `available_at` envelope — for the deployment-ui schema-view modal
# (Phase 4.B.3). Path resolution mirrors `get_shard_detail`'s
# `_gcs_path_for_shard` so the same coordinate tuple lights both views
# off the same parquet.
#
# Safety bound: parquet sizes at the per-day per-shard layer are well
# below `_LEAF_STATS_ROW_LIMIT` (500k) for every (asset_group, data_type)
# pair we currently support; the limit exists to bound worst-case
# read time for a corrupt-large parquet rather than as a normal-traffic
# truncation. When the limit fires the response sets `truncated=True` so
# the UI can render a "stats from first N rows" hint.

_LEAF_STATS_ROW_LIMIT = 500_000


def _safe_iso_or_none(ts: object) -> str | None:
    """Coerce a pandas Timestamp / datetime / NaT to ISO8601 or None."""
    if ts is None:
        return None
    try:
        if pd.isna(ts):  # pyright: ignore[reportUnknownArgumentType]
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
        meta = get_object_metadata(bucket, object_path)
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
    resolved = _gcs_path_for_shard(
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
    gs_uri = f"gs://{bucket}/{object_path}"
    file_size = _file_size_via_metadata(bucket, object_path)

    try:
        df = _read_parquet_columns(gs_uri)
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
    if len(df) > _LEAF_STATS_ROW_LIMIT:
        truncated = True
        truncated_at = _LEAF_STATS_ROW_LIMIT
        df = df.head(_LEAF_STATS_ROW_LIMIT)

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

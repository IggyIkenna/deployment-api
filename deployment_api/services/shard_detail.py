"""Unified shard-detail service for ``GET /api/data-status/shard-detail``.

Derives every piece of a Data-Status shard-detail response (schema,
GCS metadata, sample rows, branch-specific payload, download URLs) from a
single ``(service, category, instrument_type, data_type, venue, day, …)``
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

import logging
import re as _re
from typing import Literal, cast
from urllib.parse import urlencode

import pandas as pd
from unified_api_contracts import (
    CONTRACT_REGISTRY,
    VENUE_CONTRACT_OVERRIDES,
    SchemaContract,
    SchemaContractNotFoundError,
    lookup_contract,
)
from unified_api_contracts.features import get_feature_family
from unified_trading_library import (
    LEGACY_REASON_ASSET_GROUPS,
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
from deployment_api.utils.storage_facade import get_object_metadata, list_objects, list_prefixes

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
        "dex_pool_state",
        "dex_pool_swaps",
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
_PER_SYMBOL_INSTRUMENT_TYPES: frozenset[str] = frozenset({"PERPETUAL", "SPOT_PAIR", "SPOT", "EQUITY", "FUTURE"})

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
       ``dex_pool_swaps``, …) → ``grouped``.
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
# instrument_type=AUTO resolution
# ---------------------------------------------------------------------------

# Sentinel values that opt the caller into automatic instrument_type
# resolution from the UAC contract registry.  Matched case-insensitively.
_AUTO_INSTRUMENT_TYPE_TOKENS: frozenset[str] = frozenset({"auto", "unknown", ""})


def _is_auto_instrument_type(instrument_type: str | None) -> bool:
    """Return True when ``instrument_type`` opts in to AUTO resolution."""
    if instrument_type is None:
        return True
    return instrument_type.strip().lower() in _AUTO_INSTRUMENT_TYPE_TOKENS


def _resolve_instrument_type_auto(*, category: str, data_type: str, venue: str | None = None) -> str | None:
    """Pick an instrument_type for ``(category, data_type)`` when caller passes AUTO.

    Search order:

    1. Venue override — any
       ``(category.lower(), venue.upper(), instrument_type, data_type.lower())``
       tuple in :data:`VENUE_CONTRACT_OVERRIDES` wins so DeFi protocol-chain
       composites land on the legacy override row.
    2. Base registry — any ``(category.lower(), instrument_type, data_type.lower())``
       tuple in :data:`CONTRACT_REGISTRY`.  When several rows match, the
       alphabetically-first ``instrument_type`` is returned for determinism.

    Returns ``None`` when no contract is registered for the
    ``(category, data_type)`` combination — callers should treat that as a
    schema-not-registered response and let the UI render the honest
    "no contract" message.
    """
    cat_norm = (category or "").lower()
    dt_norm = (data_type or "").lower()

    # Venue override first — DeFi composite venues (PROTOCOL-CHAIN) carry
    # legacy schema overrides keyed by the protocol head.
    if venue:
        venue_norm = (venue.split("-", 1)[0] if "-" in venue else venue).upper()
        venue_matches: list[str] = sorted(
            {it for (c, v, it, dt) in VENUE_CONTRACT_OVERRIDES if c == cat_norm and v == venue_norm and dt == dt_norm}
        )
        if venue_matches:
            picked_v = venue_matches[0]
            logger.info(
                "AUTO instrument_type resolved via venue override: "
                "category=%s venue=%s data_type=%s -> %s (n_matches=%d)",
                cat_norm,
                venue_norm,
                dt_norm,
                picked_v,
                len(venue_matches),
            )
            return picked_v

    matches: list[str] = sorted({it for (c, it, dt) in CONTRACT_REGISTRY if c == cat_norm and dt == dt_norm})
    if matches:
        picked = matches[0]
        if len(matches) > 1:
            logger.info(
                "AUTO instrument_type resolved via base registry (multi-match): "
                "category=%s data_type=%s -> %s (n_matches=%d, all=%s)",
                cat_norm,
                dt_norm,
                picked,
                len(matches),
                matches,
            )
        return picked
    return None


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


def _resolve_schema(
    *, category: str, instrument_type: str, data_type: str, venue: str | None
) -> tuple[ShardSchema, str]:
    """Resolve a ShardSchema from the UAC contract registry.

    Returns ``(schema, resolved_instrument_type)``.  When the caller passes
    ``"AUTO"`` / ``"UNKNOWN"`` / empty for ``instrument_type``, the registry
    is searched for any matching ``(category, data_type)`` tuple and the
    resolved instrument_type is returned alongside the schema (so the
    response coord can echo the actual axis name rather than the literal
    sentinel).  ``schema.instrument_type_resolved_via`` documents the path
    taken: ``explicit`` / ``auto`` / ``none``.
    """
    cat_norm = (category or "").lower()
    dt_norm = (data_type or "").lower() if (data_type or "").isupper() else data_type
    dt_norm_lookup = (data_type or "").lower()

    auto_requested = _is_auto_instrument_type(instrument_type)
    resolved_via: str = "explicit"
    effective_it: str = instrument_type or ""

    if auto_requested:
        picked = _resolve_instrument_type_auto(category=cat_norm, data_type=dt_norm_lookup, venue=venue)
        if picked is None:
            return (
                ShardSchema(
                    registered=False,
                    source="none",
                    symbol_column=None,
                    columns=[],
                    message=(f"No SchemaContract found for category={cat_norm} data_type={dt_norm_lookup}"),
                    instrument_type_resolved_via="none",
                ),
                "",
            )
        effective_it = picked
        resolved_via = "auto"

    it_norm = (effective_it or "").lower() if (effective_it or "").isupper() else effective_it
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
                instrument_type_resolved_via=cast("Literal['explicit', 'auto', 'none']", resolved_via),
            ),
            effective_it,
        )

    override_key = (cat_norm, (venue or "").upper(), it_norm, dt_norm)
    is_override = venue is not None and override_key in VENUE_CONTRACT_OVERRIDES

    columns = [_column_dict(c) for c in contract.columns]
    resolved_via_literal = cast("Literal['explicit', 'auto', 'none']", resolved_via)
    if is_override:
        return (
            ShardSchema(
                registered=True,
                source="VENUE_CONTRACT_OVERRIDES",
                symbol_column=contract.symbol_column,
                columns=columns,
                message="",
                instrument_type_resolved_via=resolved_via_literal,
            ),
            effective_it,
        )
    return (
        ShardSchema(
            registered=True,
            source="CONTRACT_REGISTRY",
            symbol_column=contract.symbol_column,
            columns=columns,
            message="",
            instrument_type_resolved_via=resolved_via_literal,
        ),
        effective_it,
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

    Tries canonical ``asset_group=`` hive key first; falls back to legacy
    ``category=`` for parquets written before the asset_group vocabulary
    migration (UAC partition_paths.py: "Legacy on-disk objects use
    ``category=`` — readers that need both should try canonical first then
    fall back").
    """
    it_disk = (instrument_type or "").lower()
    dt_disk = (data_type or "").lower()
    venue_disk = (venue or "").upper()
    leaf = instrument_id or underlying
    is_derivative_bundle = dt_disk in _GROUPED_DATA_TYPES and it_disk in {
        "options_chain",
        "futures_chain",
    }

    for hive_key in (f"asset_group={cat_lower}", f"category={cat_lower}"):
        prefix = (
            f"raw_tick_data/by_date/day={day}/{hive_key}/"
            f"venue={venue_disk}/instrument_type={it_disk}/data_type={dt_disk}/"
        )
        if leaf and is_derivative_bundle:
            obj = f"{prefix}underlying={leaf}/ticks.parquet"
            try:
                if get_object_metadata(bucket, obj) is not None:
                    return (bucket, obj)
            except (OSError, RuntimeError):
                pass
        elif leaf:
            obj = f"{prefix}{leaf}.parquet"
            try:
                if get_object_metadata(bucket, obj) is not None:
                    return (bucket, obj)
            except (OSError, RuntimeError):
                pass
        else:
            name = _list_first_parquet(bucket, prefix)
            if name is not None:
                return (bucket, name)
    return None


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

    mask = _manifest_coord_mask(df, day=day, venue=venue, data_type=data_type, instrument_id=instrument_id)
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
    full_path = f"gs://{bucket}/{object_path}" if bucket and object_path else None  # noqa: gs-uri  — URI composer, bucket already resolved
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


def _sample_rows(*, gs_uri: str, limit: int, schema_columns: list[ShardSchemaColumn]) -> list[dict[str, object]]:
    """Return the first ``limit`` rows of the parquet as a list of dicts.

    Only projects the first 20 declared columns (or all columns when the
    schema is unknown) to keep the JSON payload small.
    """
    project_cols: list[str] | None = [c.name for c in schema_columns[:20]] if schema_columns else None
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
        resolved = _gcs_path_for_shard(
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
        pq_row_count = _read_parquet_footer_row_count(gs_uri)
        sample_rows = _sample_rows(gs_uri=gs_uri, limit=_SAMPLE_ROW_LIMIT, schema_columns=schema.columns)

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
    )

    download_urls = ShardDownloadUrls(
        parquet_signed_url=_parquet_signed_url(bucket, object_path),
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


def _instruments_bucket_for_category(category: str) -> str:
    return build_bucket_name("instruments-service", category)


# Compiled regex for stripping the underscore in DeFi protocol-version
# tokens, e.g. ``AAVE_V3-ETHEREUM`` → ``AAVEV3-ETHEREUM``.
# Instruments-service GCS partition keys historically used the no-underscore
# ghost form (``AAVEV3-ETHEREUM``). Writers were updated to canonical
# ``AAVE_V3-ETHEREUM`` in 2026-05-23, but old parquets on GCS may still live
# under ghost-name paths until a full migration completes. The alias list
# tries canonical first; ghost is a backward-compat fallback.
_DEFI_VERSION_UNDERSCORE_RE = _re.compile(r"_V(\d+)")


def _venue_aliases_for_bucket(category: str, venue: str) -> list[str]:
    """Return the list of venue strings to try when resolving a GCS path.

    The UI / UAC and the instruments-service GCS writer disagree on a few
    naming conventions. Rather than picking one canonical form, we try the
    plausible aliases in order and use whichever the bucket actually has.

    Conventions per category:

    * **DEFI**: try the literal venue first (canonical ``AAVE_V3-ETHEREUM``),
      then the ghost-name alias without underscore (``AAVEV3-ETHEREUM``) as a
      backward-compat fallback for old GCS parquets written before 2026-05-23.
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
        # And the reverse — if caller passed "AAVEV3-ETHEREUM" (ghost name),
        # try "AAVE_V3-ETHEREUM" (canonical). Match uppercase letter immediately
        # followed by V<digits> (no preceding underscore).
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


def _read_instruments_day_df(*, bucket: str, venue: str, day: str, category: str = "") -> pd.DataFrame | None:
    """Read the instruments-service per-(venue, day) parquet, or ``None``.

    Shard-isolated: swallows every IO error and returns ``None``.
    """
    aliases = _venue_aliases_for_bucket(category, venue)
    partition_key = _partition_key_for_category(category)
    last_err: Exception | None = None
    for alias in aliases:
        leaf_uri = (
            f"gs://{bucket}/instrument_availability/by_date/day={day}/{partition_key}={alias}/instruments.parquet"  # noqa: gs-uri  — URI composer, bucket already resolved
        )
        try:
            df = _read_parquet_columns(leaf_uri, None)
            if df is not None and not df.empty:  # pyright: ignore[reportUnnecessaryComparison]
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
                if isinstance(getattr(o, "name", None), str) and getattr(o, "name", "").endswith("instruments.parquet")
            ]
            frames: list[pd.DataFrame] = []
            for path in parquet_paths:
                # gs-uri-rationale: GCS-locked (legacy UTL `build_bucket()` GCS-only).
                # Cloud-agnostic migration: deployment_api_shard_detail_gcs_locked_2026_05_17.md.
                gs_uri = f"gs://{bucket}/{path}"  # noqa: gs-uri
                try:
                    sub_df = _read_parquet_columns(gs_uri, None)
                    if sub_df is not None and not sub_df.empty:  # pyright: ignore[reportUnnecessaryComparison]
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
    """List the ``day=YYYY-MM-DD`` sub-directories under ``instrument_availability/by_date/``."""
    prefix = "instrument_availability/by_date/"
    try:
        return list_prefixes(bucket, prefix)
    except (OSError, RuntimeError) as exc:
        logger.warning("list_day_prefixes failed for %s: %s", bucket, exc)
        return []


def _pick_latest_day(bucket: str, venue: str) -> str | None:
    """Return the latest ``day=YYYY-MM-DD`` directory seen for a venue."""
    prefix = "instrument_availability/by_date/"
    try:
        objects = list_objects(bucket, prefix, max_results=5_000)
    except (OSError, RuntimeError) as exc:
        logger.warning("list_objects latest-day failed for %s/%s: %s", bucket, prefix, exc)
        return None
    days: set[str] = set()
    marker = "day="
    venue_marker = f"venue={venue}"
    for o in objects:
        name = getattr(o, "name", None)
        if not isinstance(name, str) or venue_marker not in name:
            continue
        idx = name.find(marker)
        if idx < 0:
            continue
        tail = name[idx + len(marker) :]
        days.add(tail.split("/", 1)[0])
    return sorted(days)[-1] if days else None


def _cefi_venue_detail(category: str, venue: str) -> VenueDetailResponse:
    """CeFi branch — mirrors the pre-existing venue-detail shape."""
    bucket = _instruments_bucket_for_category(category)
    day = _pick_latest_day(bucket, venue)
    instruments: list[dict[str, object]] = []
    total_before_cap = 0
    if day is not None:
        df = _read_instruments_day_df(bucket=bucket, venue=venue, day=day)
        if df is not None and not df.empty:
            records_raw = df.to_dict(orient="records")  # pyright: ignore[reportUnknownMemberType,reportUnknownVariableType]
            records = cast(list[object], records_raw)
            total_before_cap = len(records)
            for r in records[:500]:
                if isinstance(r, dict):
                    typed_r: dict[str, object] = {}
                    for k, v in cast(dict[object, object], r).items():
                        typed_r[str(k)] = v
                    instruments.append(typed_r)
    return VenueDetailResponse(
        category=category.upper(),
        venue=venue,
        day=day,
        total_instruments=len(instruments),
        total_instruments_unfiltered=total_before_cap,
        instruments=instruments,
    )


def _is_pool_row(row: dict[str, object]) -> bool:
    """Row represents a DeFi pool when it carries pool_address or pool_id."""
    return bool(row.get("pool_address") or row.get("pool_id"))


def _typed_row(raw: object) -> dict[str, object] | None:
    """Coerce a pandas-records raw row into a strictly typed ``dict[str, object]``."""
    if not isinstance(raw, dict):
        return None
    typed: dict[str, object] = {}
    for k, v in cast(dict[object, object], raw).items():
        typed[str(k)] = v
    return typed


def _defi_composite_detail(df: pd.DataFrame, *, venue: str, chain: str, protocol: str, day: str) -> VenueDetailResponse:
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
        category="DEFI",
        venue=venue,
        chain=chain,
        protocol=protocol,
        total_pools=len(pools),
        total_tokens=len(tokens),
        pools=pools,
        tokens=tokens,
        day=day,
    )


def _defi_chain_only_detail(df: pd.DataFrame, *, venue: str, chain: str, day: str) -> VenueDetailResponse:
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
        category="DEFI",
        venue=venue,
        chain=chain,
        protocol=None,
        total_pools=total_pools,
        total_tokens=total_tokens,
        protocols=protocols_list,
        day=day,
    )


def _load_defi_df(bucket: str, *, venue: str, chain: str, protocol: str | None, day: str) -> pd.DataFrame | None:
    """Try the composite file first; fall back to the chain file for composite venues."""
    df = _read_instruments_day_df(bucket=bucket, venue=venue, day=day)
    if (df is None or df.empty) and protocol is not None:
        df = _read_instruments_day_df(bucket=bucket, venue=chain, day=day)
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
        return VenueDetailResponse(category="DEFI", venue=venue)

    bucket = _instruments_bucket_for_category("defi")
    day = _pick_latest_day(bucket, venue)
    if day is None:
        return VenueDetailResponse(
            category="DEFI",
            venue=venue,
            chain=chain,
            protocol=protocol,
            day=None,
        )

    df = _load_defi_df(bucket, venue=venue, chain=chain, protocol=protocol, day=day)
    if df is None or df.empty:
        return VenueDetailResponse(
            category="DEFI",
            venue=venue,
            chain=chain,
            protocol=protocol,
            day=day,
        )

    if protocol is not None:
        return _defi_composite_detail(df, venue=venue, chain=chain, protocol=protocol, day=day)
    return _defi_chain_only_detail(df, venue=venue, chain=chain, day=day)


def _prediction_venue_detail(venue: str) -> VenueDetailResponse:
    """Prediction branch — returns canonical_question_group breakdown for the venue.

    Reads the instruments-service prediction manifest index and aggregates by
    canonical_question_group on the latest available day for the given venue.
    Shard axis per UAC SHARD_AXIS_MATRIX:
    ("instruments-service", "prediction"): ("venue", "canonical_question_group").

    The prediction manifest stores question group names in the ``underlying``
    column (not a ``canonical_question_group`` column); ``data_type`` is always
    ``prediction_canonical_question_group``.
    """
    bucket = _instruments_bucket_for_category("prediction")
    try:
        df = read_availability_index(bucket)
    except (OSError, RuntimeError, ValueError) as exc:
        logger.warning("prediction venue-detail: manifest read failed for %s: %s", bucket, exc)
        return VenueDetailResponse(category="PREDICTION", venue=venue)

    if df.empty or "venue" not in df.columns:
        return VenueDetailResponse(category="PREDICTION", venue=venue)

    venue_df = df[df["venue"].astype(str).str.upper() == venue.upper()].copy()
    if venue_df.empty:
        return VenueDetailResponse(category="PREDICTION", venue=venue)

    day: str | None = None
    if "date" in venue_df.columns:
        dates = sorted(str(d) for d in venue_df["date"].dropna().unique() if str(d).strip())  # pyright: ignore[reportAny]
        if dates:
            day = dates[-1]
            venue_df = venue_df[venue_df["date"].astype(str) == day].copy()

    instruments: list[dict[str, object]] = []
    if "underlying" in venue_df.columns and "instrument_count" in venue_df.columns:
        agg = venue_df.groupby("underlying")["instrument_count"].sum().sort_values(ascending=False)
        for grp, count in agg.items():  # pyright: ignore[reportAny]
            if str(grp).strip():
                instruments.append(
                    {
                        "key": str(grp),
                        "type": f"{int(count):,} instruments",  # pyright: ignore[reportAny]
                        "canonical_question_group": str(grp),
                        "instrument_count": int(count),  # pyright: ignore[reportAny]
                    }
                )

    return VenueDetailResponse(
        category="PREDICTION",
        venue=venue,
        day=day,
        total_instruments=len(instruments),
        total_instruments_unfiltered=len(instruments),
        instruments=instruments[:500],
    )


def fetch_venue_detail(*, service: str, asset_group: str, venue: str) -> VenueDetailResponse:
    """Return venue-scoped detail for the Data Status drilldown.

    ``asset_group == "DEFI"`` branches on whether ``venue`` is a bare chain
    (``ETHEREUM``) or a composite protocol-chain (``AAVE_V3-ETHEREUM``);
    ``asset_group == "PREDICTION"`` returns canonical_question_group breakdown;
    all other asset groups use the CeFi branch (latest-day instruments
    listing for the venue).
    """
    _ = service  # Reserved for future per-service routing; keeps the signature
    # stable for the UI caller today.
    cat_upper = (asset_group or "").upper()
    if cat_upper == "DEFI":
        return _defi_venue_detail(venue)
    if cat_upper == "PREDICTION":
        return _prediction_venue_detail(venue)
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
    # gs-uri-rationale: function GCS-locked (resolved tuple from upstream `build_bucket()` GCS-only helper).
    # Cloud-agnostic migration tracked in deployment_api_shard_detail_gcs_locked_2026_05_17.md.
    gs_uri = f"gs://{bucket}/{object_path}"  # noqa: gs-uri
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

"""Shard classification, schema + path resolution, manifest lookup, GCS metadata.

Split from ``services/shard_detail.py`` (pure code motion; plan:
``codex_violations_ratchet_to_five_2026_06_10.md`` Phase-1 P2). Patched
module-level collaborators are resolved through the package facade
(``_sd``) at call time so the existing test patch surface
``deployment_api.services.shard_detail.<name>`` keeps intercepting.
"""

from __future__ import annotations

import logging
from typing import Literal, cast

import pandas as pd
from unified_api_contracts import (
    CONTRACT_REGISTRY,
    VENUE_CONTRACT_OVERRIDES,
    SchemaContract,
    SchemaContractNotFoundError,
    lookup_contract,
)
from unified_trading_library import (
    LEGACY_REASON_ASSET_GROUPS,
    classify_legacy_empty_row,
)

import deployment_api.services.shard_detail as _sd
from deployment_api.services.data_status_drilldown import build_bucket_name
from deployment_api.types.shard_detail import (
    CaptureStatusLiteral,
    ServiceEmissionStateLiteral,
    ShardClassLiteral,
    ShardGcsMetadata,
    ShardSchema,
    ShardSchemaColumn,
)

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
        objects = _sd.list_objects(bucket, prefix, max_results=10)
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
                if _sd.get_object_metadata(bucket, obj) is not None:
                    return (bucket, obj)
            except (OSError, RuntimeError):
                pass
        elif leaf:
            obj = f"{prefix}{leaf}.parquet"
            try:
                if _sd.get_object_metadata(bucket, obj) is not None:
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
        df = _sd.read_availability_index(bucket)
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
            meta = _sd.get_object_metadata(bucket, object_path)
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

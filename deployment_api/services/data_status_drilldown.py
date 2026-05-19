"""Drill-down helpers for the Data Status page.

Adds schema introspection, per-day instrument listing, per-venue bucket
counts (named markets + conditionIds-inside-OTHER), and CSV downloads for
selected instruments.

Deliberately lives outside ``data_status_service.py`` to keep that module
under the 900-line codex-compliance budget and because these concerns are
self-contained — they don't mutate the manifest-based status pipeline, they
just read additional artifacts on demand.

All GCS reads are TTL-cached (5 min) to avoid hammering the bucket when the
UI re-expands a venue or day.
"""

from __future__ import annotations

import logging
import re as _re
import time
from typing import cast

import pandas as pd
from unified_api_contracts import SchemaContract, SchemaContractNotFoundError, lookup_contract
from unified_api_contracts.internal.schemas.contracts import (
    VENUE_CONTRACT_OVERRIDES,
)
from unified_trading_library import (
    LEGACY_REASON_ASSET_GROUPS,
    classify_legacy_empty_row,
    read_availability_index,
    resolve_bucket_name,
)

from deployment_api.settings import gcp_project_id as _pid
from deployment_api.utils.storage_facade import list_objects

logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Bucket naming — delegates to cloud-providers.yaml via resolve_bucket_name().
# Phase 2.6.4 delegate flip: ml-models-store / ml-predictions-store per yaml SSOT.
# ---------------------------------------------------------------------------

# Service → yaml kind mapping.
_SERVICE_TO_KIND: dict[str, str] = {
    "instruments-service": "instruments-store",
    "corporate-actions": "instruments-store",
    "market-tick-data-service": "market-data",
    "market-data-processing-service": "market-data",
    "features-delta-one-service": "features-delta-one",
    "features-volatility-service": "features-volatility",
    "features-onchain-service": "features-onchain",
    "features-sports-service": "features-sports",
    "features-calendar-service": "features-calendar",
    "features-multi-timeframe-service": "features-multi-timeframe",
    "features-cross-instrument-service": "features-cross-instrument",
    "ml-training-service": "ml-models-store",
    "ml-inference-service": "ml-predictions-store",
    "strategy-service": "strategy-store",
    "execution-service": "execution-store",
}

# CORRECT-LOCAL: features-commodity has no yaml kind; flat template retained until
# cloud-providers.yaml gains the entry (bucket_name_ssot plan pending item).
_COMMODITY_BUCKET_TEMPLATE = "features-commodity-{pid}"

# Kinds whose per-AG yaml dict omits PREDICTION — route to a flat prediction kind instead.
_PREDICTION_KIND_MAP: dict[str, str] = {
    "instruments-store": "instruments-store-prediction",
    "market-data": "market-data-tick-prediction",
    "strategy-store": "strategy-store-prediction",
    "execution-store": "execution-store-prediction",
}


# Types that bundle many underlyings / strikes into one parquet per day.
# For these the shard unit is the underlying root (ES, NQ, BTC), not the
# individual contract — a single "instrument_id" selection downloads the
# bundle parquet for that root.
_BUNDLED_INSTRUMENT_TYPES: frozenset[str] = frozenset(
    {
        "options_chain",
        "futures_chain",
        "combo",
        # Polymarket "OTHER" bucket also bundles many conditionIds into one file
        # per day, but the UI pretends those are individual conditionIds because
        # the user selects them by their in-file instrument_id.
    }
)


# Venues/shards that bundle many symbols into one parquet (Polymarket OTHER,
# options chains, etc.) — we tag these as "per_underlying" so the UI knows
# one selection = whole bundle.
def _is_bundled(instrument_type: str) -> bool:
    return (instrument_type or "").lower() in _BUNDLED_INSTRUMENT_TYPES


def build_bucket_name(service: str, asset_group: str, project_id: str | None = None) -> str:
    """Resolve the GCS bucket for a (service, asset group) pair."""
    if service == "features-commodity-service":
        return _COMMODITY_BUCKET_TEMPLATE.format(pid=project_id or _pid)
    kind = _SERVICE_TO_KIND.get(service)
    if kind is None:
        raise ValueError(f"Unknown service: {service}")
    ag: str | None = asset_group.lower() if asset_group else None
    if ag == "prediction":
        pred_kind = _PREDICTION_KIND_MAP.get(kind)
        if pred_kind:
            return resolve_bucket_name(cloud="gcp", kind=pred_kind)
    return resolve_bucket_name(cloud="gcp", kind=kind, asset_group=ag)


# ---------------------------------------------------------------------------
# TTL cache for read-heavy drill-down calls (5-min).
# ---------------------------------------------------------------------------

_CACHE_TTL_SECONDS = 300.0
_cache: dict[str, tuple[float, object]] = {}


def _cache_get(key: str) -> object | None:
    entry = _cache.get(key)
    if entry is None:
        return None
    ts, value = entry
    if (time.monotonic() - ts) > _CACHE_TTL_SECONDS:
        _cache.pop(key, None)
        return None
    return value


def _cache_put(key: str, value: object) -> None:
    _cache[key] = (time.monotonic(), value)


def clear_drilldown_cache() -> None:
    """Reset the TTL cache (used by tests and /turbo/clear)."""
    _cache.clear()


# ---------------------------------------------------------------------------
# Manifest capture_status lookup (Phase-C honest-coverage)
# ---------------------------------------------------------------------------

# Default capture_status for rows absent from the manifest OR legacy pre-v5
# rows that carry no capture_status column. Matches the UTL legacy-read
# coercion in ``ManifestWriter.lookup``.
_DEFAULT_CAPTURE_STATUS = "captured"


def _scoped_manifest_rows(bucket: str, venue: str, day: str) -> pd.DataFrame | None:
    """Return the (date, venue) slice of the manifest or None on any miss.

    Returns ``None`` when the manifest is unreachable, empty, missing the
    ``date`` filter column, or contains no matching rows. The calling code
    then falls back to the safe ``captured`` default.
    """
    try:
        df = read_availability_index(bucket)
    except (OSError, RuntimeError, ValueError) as exc:
        logger.warning("capture-status manifest read failed for %s: %s", bucket, exc)
        return None

    if df.empty or "date" not in df.columns:
        return None

    mask = df["date"] == day
    if "venue" in df.columns:
        mask = mask & (df["venue"] == venue)
    scoped = df.loc[mask]
    if scoped.empty or "instrument_id" not in scoped.columns:
        return None

    # Dedup by instrument_id — keep the latest written_at per key. When the
    # column is missing (very old parquet) we fall back to last-seen order.
    if "written_at" in scoped.columns:
        scoped = scoped.sort_values("written_at").drop_duplicates(subset=["instrument_id"], keep="last")
    else:
        scoped = scoped.drop_duplicates(subset=["instrument_id"], keep="last")
    return scoped


def _build_capture_metadata_lookup(scoped: pd.DataFrame) -> dict[str, dict[str, str]]:
    """Build an instrument_id -> {capture_status, error_reason, attempted_at}
    lookup from a deduped manifest slice."""
    by_iid: dict[str, dict[str, str]] = {}
    for row in scoped.to_dict(orient="records"):  # pyright: ignore[reportUnknownMemberType,reportUnknownVariableType]
        iid = str(row.get("instrument_id") or "").strip()  # pyright: ignore[reportUnknownMemberType,reportUnknownArgumentType]
        if not iid:
            continue
        by_iid[iid] = {
            "capture_status": str(
                row.get("capture_status") or _DEFAULT_CAPTURE_STATUS  # pyright: ignore[reportUnknownMemberType,reportUnknownArgumentType]
            ).lower(),
            "error_reason": str(row.get("error_reason") or ""),  # pyright: ignore[reportUnknownMemberType,reportUnknownArgumentType]
            "attempted_at": str(row.get("attempted_at") or ""),  # pyright: ignore[reportUnknownMemberType,reportUnknownArgumentType]
        }
    return by_iid


def _attach_capture_status_to_instruments(
    instruments: list[dict[str, object]],
    *,
    bucket: str,
    venue: str,
    day: str,
) -> None:
    """Mutate each instrument dict in-place to add manifest capture metadata.

    Reads ``gs://<bucket>/_index/availability_index.parquet``, filters to
    ``(date == day, venue == venue)``, then joins each instrument to its
    manifest row by ``instrument_id``. Adds three keys to every instrument:
    - ``capture_status``:  "captured" | "empty_confirmed" | "attempted_failed"
    - ``error_reason``:    classified error string (empty for captured/empty)
    - ``attempted_at``:    ISO-8601 UTC timestamp (empty for legacy rows)

    When multiple shards match a tuple (re-runs, re-tries), the row with the
    latest ``written_at`` wins — same dedup semantics as UTL
    ``_merge_dataframes``.

    Failure to read the manifest is non-fatal: every instrument defaults to
    ``capture_status="captured"`` + empty error/attempted_at so the drill-down
    stays usable even when the manifest is unreachable.
    """
    if not instruments:
        return
    scoped = _scoped_manifest_rows(bucket, venue, day)
    if scoped is None:
        _apply_default_capture_status(instruments)
        return
    by_iid = _build_capture_metadata_lookup(scoped)
    for inst in instruments:
        iid = str(inst.get("instrument_id") or "").strip()
        meta = by_iid.get(iid)
        if meta is None:
            inst["capture_status"] = _DEFAULT_CAPTURE_STATUS
            inst["error_reason"] = ""
            inst["attempted_at"] = ""
        else:
            inst["capture_status"] = meta["capture_status"]
            inst["error_reason"] = meta["error_reason"]
            inst["attempted_at"] = meta["attempted_at"]


def _apply_default_capture_status(instruments: list[dict[str, object]]) -> None:
    """Stamp the safe default (captured / no error / no attempted_at) on every row."""
    for inst in instruments:
        inst.setdefault("capture_status", _DEFAULT_CAPTURE_STATUS)
        inst.setdefault("error_reason", "")
        inst.setdefault("attempted_at", "")


# Capture-status branches surfaced by the multi-axis drilldown / download
# endpoints. The download UX in deployment-ui DataStatusDrilldown branches
# on the ``X-Capture-Status`` response header into the matching empty-state
# panel:
#
# - ``captured`` — manifest claims rows; parquet read returned >0 rows;
#   today's happy path (HTTP 200 + CSV body).
# - ``empty_confirmed`` — adapter ran and the source legitimately returned
#   zero rows (paused league day, pre-source-launch, dormant prediction
#   market). HTTP 200 + header-only body explaining the honest empty.
# - ``attempted_failed`` — adapter ran and raised; ``error_reason``
#   carries the classified failure. HTTP 200 + header-only body.
# - ``never_attempted`` — manifest has no row at all for the requested
#   axis-tuple; the adapter never ran. HTTP 404.
# - ``path_drift`` — manifest says ``captured`` but the downloader probed
#   the parquet path and got 0 rows. Real bug class: writer/downloader
#   path-template drift, chain-bundle / instrument_type / hive-vocab
#   axis drift, or a per-league/per-job_id sub-partition the downloader
#   doesn't walk into. HTTP 502 — the operator needs to know the data
#   the manifest claims is in fact unreachable.
_CAPTURE_STATUS_HEADER = "X-Capture-Status"

_AXIS_KWARGS: tuple[str, ...] = (
    "venue",
    "instrument_type",
    "data_type",
    "instrument_id",
    "chain",
    "league_id",
    "fixture_id",
    "canonical_question_group",
    "job_id",
    "model_family",
    "training_period",
    "strategy_id",
    "instruction_type",
    "feature_group",
    "timeframe",
)


def lookup_capture_status_for_shard(
    *,
    service: str,
    asset_group: str,
    day: str,
    **axes: str | None,
) -> dict[str, str]:
    """Look up the manifest ``capture_status`` for an axis-tuple shard.

    Reads the (service, asset_group) availability index, filters to the
    requested ``day`` AND every populated axis kwarg, and returns the
    most recently written row's capture metadata. When no row matches,
    returns ``{"status": "never_attempted", ...}`` so the caller can
    branch — the adapter never ran for this shard, distinct from
    "ran and got nothing".

    Args:
        service: Service name (e.g. ``instruments-service``,
            ``market-tick-data-service``, ``ml-training-service``).
        asset_group: Asset group bucket key (cefi/tradfi/defi/sports/
            prediction/shared).
        day: ISO date (``YYYY-MM-DD``) to filter on.
        **axes: The v7 shard axes (venue, chain, league_id,
            fixture_id, canonical_question_group, job_id,
            model_family, training_period, strategy_id,
            instruction_type, feature_group, timeframe, instrument_id,
            instrument_type, data_type). Each axis kwarg is applied as
            an equality filter when the manifest carries that column;
            missing columns are silently skipped (no narrowing) so the
            lookup degrades gracefully against partially-migrated
            v4/v5/v6 manifests.

    Returns:
        Dict with keys ``status`` (one of ``captured``,
        ``empty_confirmed``, ``attempted_failed``, ``never_attempted``),
        ``error_reason`` (classified error string or ``""``),
        ``attempted_at`` (ISO-8601 UTC timestamp or ``""``),
        ``written_at`` (manifest-write time or ``""``).

    Plan: data_status_multi_axis_shard_propagation_2026_05_06.md
    Phase 3 (drilldown polish — distinguish empty_confirmed from
    path-drift zero bytes in the download UX).
    """
    try:
        bucket = build_bucket_name(service, asset_group)
    except ValueError:
        return {
            "status": "never_attempted",
            "error_reason": "",
            "attempted_at": "",
            "written_at": "",
        }
    try:
        index = read_availability_index(bucket)
    except (OSError, RuntimeError, ValueError) as exc:
        logger.warning("lookup_capture_status_for_shard manifest read failed for %s: %s", bucket, exc)
        return {
            "status": "never_attempted",
            "error_reason": "",
            "attempted_at": "",
            "written_at": "",
        }
    if index.empty or "date" not in index.columns:
        return {
            "status": "never_attempted",
            "error_reason": "",
            "attempted_at": "",
            "written_at": "",
        }
    mask = index["date"].astype(str) == day
    if "service_name" in index.columns:
        mask = mask & (index["service_name"].astype(str) == service)
    for col in _AXIS_KWARGS:
        val = axes.get(col)
        if not val or col not in index.columns:
            continue
        normalised = val.upper() if col in ("venue", "chain", "instruction_type") else val
        mask = mask & (index[col].fillna("").astype(str) == normalised)
    matched = index.loc[mask]
    if matched.empty:
        return {
            "status": "never_attempted",
            "error_reason": "",
            "attempted_at": "",
            "written_at": "",
        }
    if "written_at" in matched.columns:
        matched = matched.sort_values("written_at", ascending=False)
    row = matched.iloc[0]
    raw_status = row.get("capture_status") if "capture_status" in matched.columns else None
    status = (
        str(raw_status).lower()
        if raw_status is not None and not pd.isna(raw_status) and str(raw_status)
        else _DEFAULT_CAPTURE_STATUS
    )
    if status not in ("captured", "empty_confirmed", "attempted_failed"):
        status = _DEFAULT_CAPTURE_STATUS
    error_reason = _classify_lookup_legacy_empty_reason(
        status=status,
        error_reason=str(row.get("error_reason") or ""),
        row=row,
        asset_group=asset_group,
        bucket=bucket,
    )
    return {
        "status": status,
        "error_reason": error_reason,
        "attempted_at": str(row.get("attempted_at") or ""),
        "written_at": str(row.get("written_at") or ""),
    }


def _classify_lookup_legacy_empty_reason(
    *,
    status: str,
    error_reason: str,
    row: pd.Series[object],
    asset_group: str,
    bucket: str,
) -> str:
    """Reader-side fallback for legacy ``empty_confirmed`` lookup rows.

    Returns ``error_reason`` unchanged unless the row is a pre-Phase-2.E.2
    legacy row (status=empty_confirmed AND error_reason empty). In that
    case, classify on the fly via the UTL helper — same SSOT the
    Tier 3D.1 reconciler uses for batch back-fill (writegate Tier 3D.2).
    """
    if error_reason or status != "empty_confirmed":
        return error_reason
    ag_lower = asset_group.lower()
    if ag_lower not in LEGACY_REASON_ASSET_GROUPS:
        return error_reason
    # Convert the pandas Series to a plain dict — UTL's classifier
    # accepts ``Mapping[str, object]`` and pandas Series fails the
    # nominal type check even though it implements ``.get()``.
    row_dict: dict[str, object] = {str(k): v for k, v in row.items()}
    try:
        return classify_legacy_empty_row(ag_lower, row_dict)
    except (ValueError, TypeError, KeyError, AttributeError) as exc:
        logger.warning(
            "classify_legacy_empty_row failed for %s in %s: %s",
            ag_lower,
            bucket,
            exc,
        )
        return error_reason


# ---------------------------------------------------------------------------
# Schema lookup
# ---------------------------------------------------------------------------


def _column_dicts(contract: SchemaContract) -> list[dict[str, object]]:
    return [
        {
            "name": col.name,
            "dtype": col.dtype,
            "nullable": col.nullable,
            "description": col.description or "",
        }
        for col in contract.columns
    ]


# Known data_type / instrument_type aliases emitted by UI / manifest that
# don't match the canonical UAC contract keys 1:1. Source:
# deployment-ui-playwright-audit 2026-04-19 §5.5 — the UI can send
# ``POOL_DEFINITION`` / ``POOL_SNAPSHOT`` / ``LIQUIDITY_POOL`` / ``POOL``
# (uppercase) while UAC keys them as ``pool`` / ``dex_pool_state``.
_INSTRUMENT_TYPE_ALIASES: dict[str, str] = {
    "POOL": "pool",
    "POOL_SNAPSHOT": "pool",
    "LIQUIDITY_POOL": "pool",
    "DEX_POOL": "dex_pool",
}

_DATA_TYPE_ALIASES: dict[str, str] = {
    "POOL_DEFINITION": "dex_pool_state",
    "INSTRUMENT_DEFINITION": "dex_pool_state",
    "POOL_SNAPSHOT": "dex_pool_state",
    "POOL_STATE": "dex_pool_state",
    "POOL_SWAPS": "dex_pool_swaps",
}


def _normalise_instrument_type(raw: str) -> str:
    """Return the UAC-canonical instrument_type for a UI-supplied value."""
    if raw in _INSTRUMENT_TYPE_ALIASES:
        return _INSTRUMENT_TYPE_ALIASES[raw]
    # Registry keys are lowercase snake_case — lowercase all-caps inputs
    # so ``POOL`` resolves even without an explicit alias.
    return raw.lower() if raw.isupper() else raw


def _normalise_data_type(raw: str) -> str:
    """Return the UAC-canonical data_type for a UI-supplied value."""
    if raw in _DATA_TYPE_ALIASES:
        return _DATA_TYPE_ALIASES[raw]
    return raw.lower() if raw.isupper() else raw


# SPORTS data_type → instrument_type resolver. The UI passes
# ``instrument_type=""`` for sports (the manifest doesn't carry one), but UAC
# contract keys require an instrument_type (``("sports", "match", "fixtures")``,
# ``("sports", "reference", "leagues")``, …). Resolve at the boundary so the
# UI doesn't need to know UAC's internal partitioning. SSOT plan:
# ``sports_uac_schema_contracts_registration_2026_04_24``.
_SPORTS_DATA_TYPE_TO_INSTRUMENT_TYPE: dict[str, str] = {
    # Family A — API-Football reference
    "leagues": "reference",
    "teams": "reference",
    "venues": "reference",
    "transfermarkt_leagues": "reference",
    "sfi_leagues": "reference",
    # league-level snapshot (slightly different shape from match facts)
    "standings": "league",
    # Family B — API-Football match fact + Family D progressive + Family E
    "fixtures": "match",
    "fixture_events": "match",
    "fixture_stats": "match",
    "fixture_lineups": "match",
    "player_stats": "match",
    "injuries": "match",
    "sfi_progressive_stats": "match",
    "matches": "match",
    "predictions": "match",
    "xg": "match",
    "weather": "match",
    # Family C — Transfermarkt player values
    "player_values": "player",
    # Family E — FSS derivative
    "fixture_features": "feature",
    # Existing pre-plan registration: sports/odds/trades
    "trades": "odds",
}


_AUTO_SENTINELS: frozenset[str] = frozenset({"auto", "unknown", "auto_detect_fail", ""})


def _resolve_sports_instrument_type(asset_group: str, instrument_type: str, data_type: str) -> str:
    """If category=sports and instrument_type is empty/AUTO, infer from data_type.

    The UI passes ``"AUTO"`` (uppercase sentinel) at SPORTS click sites — the
    SPORTS manifest has no instrument_type axis, so we resolve it from
    ``data_type`` via :data:`_SPORTS_DATA_TYPE_TO_INSTRUMENT_TYPE`.
    """
    if asset_group != "sports":
        return instrument_type
    if instrument_type and instrument_type.lower() not in _AUTO_SENTINELS:
        return instrument_type
    return _SPORTS_DATA_TYPE_TO_INSTRUMENT_TYPE.get(data_type, instrument_type)


def get_schema_for_shard(  # noqa: C901 — 4-step schema resolution pipeline with fallback chain; branching is inherent
    *,
    asset_group: str,
    instrument_type: str,
    data_type: str,
    venue: str | None = None,
    service: str | None = None,
    bucket: str | None = None,
    day: str | None = None,
    chain: str | None = None,
    instrument_id: str | None = None,
    league_id: str | None = None,
    fixture_id: str | None = None,
    canonical_question_group: str | None = None,
    job_id: str | None = None,
    model_family: str | None = None,
    training_period: str | None = None,
    strategy_id: str | None = None,
    instruction_type: str | None = None,
    feature_group: str | None = None,
    timeframe: str | None = None,
) -> dict[str, object]:
    """Return the SchemaContract columns for a shard tuple — leaf-shard granularity.

    Phase 3 of data_status_multi_axis_shard_propagation_2026_05_06.md
    — schema view ALWAYS at the deepest shard level. Each leaf parquet has
    its own column shape (CeFi spot per-instrument vs DeFi options-chain
    bundle vs sports per-league fixture parquet differ in column shape),
    so the v7 multi-axis kwargs are threaded through to the parquet
    projection fallback.

    Resolution order:

    1. UAC contract registry — ``lookup_contract(asset_group,
       instrument_type, data_type, venue)``. Honours
       ``VENUE_CONTRACT_OVERRIDES``. Cleanest path.
    2. Legacy v4 instruments-service catalogue synthesis: empty
       instrument_type+data_type collapses to the
       ``("instrument_catalogue", "instrument_catalogue")`` contract.
    3. Parquet-projection fallback: when the registry has no entry
       AND we have ``service`` + ``bucket`` + ``day`` + enough axis
       kwargs to identify a leaf parquet, probe the actual parquet
       on GCS via :func:`_parquet_schema_names` and return the real
       column names. Distinguishes from #1 via ``source =
       "PARQUET_PROJECTION"`` so the UI can flag it as inferred,
       not contract-backed.
    4. Honest absence — return ``registered: False`` with an empty
       ``columns`` list and a non-confusing message. UI should
       render "no schema yet" rather than blank or fake columns.

    The honest-absence branch (#4) is correct for shards where the
    writer hasn't shipped yet — we explicitly DO NOT make up columns
    that aren't on disk and aren't in the registry.
    """
    # Normalise UI inputs so the lookup hits the UAC registry keys
    # (lowercase snake_case). The UI passes ``POOL`` / ``POOL_DEFINITION``
    # from the manifest; UAC keys those as ``pool`` / ``dex_pool_state``.
    cat_norm = asset_group.lower()
    it_norm = _normalise_instrument_type(instrument_type)
    dt_norm = _normalise_data_type(data_type)
    # Sports manifest carries no instrument_type axis — resolve from data_type.
    it_norm = _resolve_sports_instrument_type(cat_norm, it_norm, dt_norm)

    # instruments-service legacy v4 rows for cefi/tradfi/defi carry empty
    # instrument_type and empty data_type. Synthesise the catalogue axes so
    # the lookup resolves the registered INSTRUMENT_CATALOGUE contract
    # instead of failing.
    if (service or "").lower() == "instruments-service" and not it_norm and not dt_norm:
        it_norm = "instrument_catalogue"
        dt_norm = "instrument_catalogue"

    # Step 1+2: Venue override takes priority, else base registry.
    contract: SchemaContract | None
    try:
        contract = lookup_contract(
            asset_group=cat_norm,
            instrument_type=it_norm,
            data_type=dt_norm,
            venue=venue,
        )
    except SchemaContractNotFoundError:
        contract = None

    if contract is not None:
        source = "CONTRACT_REGISTRY"
        if venue is not None:
            override_key = (cat_norm, (venue or "").upper(), it_norm, dt_norm)
            if override_key in VENUE_CONTRACT_OVERRIDES:
                source = "VENUE_CONTRACT_OVERRIDES"
        return {
            "registered": True,
            "asset_group": contract.asset_group,
            "instrument_type": contract.instrument_type,
            "data_type": contract.data_type,
            "venue": (venue or "").upper() if venue else None,
            "symbol_column": contract.symbol_column,
            "source": source,
            "columns": _column_dicts(contract),
            "required_row_count_min": contract.required_row_count_min,
        }

    # Step 3: Parquet-projection fallback. Build the leaf-shard kwargs
    # dict from the multi-axis params + try every plausible parquet path
    # for this (service, asset_group). When one resolves, project its
    # column names from the parquet schema and return them as the
    # leaf-shard schema. SHARD_AXIS_MATRIX-driven so per-asset-group
    # path layouts are honoured (DEFI chain= partition, sports
    # league= partition, ML model_family= / training_period= /
    # job_id=, etc.).
    axes: dict[str, str | None] = {
        "venue": venue,
        "instrument_type": instrument_type,
        "data_type": data_type,
        "instrument_id": instrument_id,
        "chain": chain,
        "league_id": league_id,
        "fixture_id": fixture_id,
        "canonical_question_group": canonical_question_group,
        "job_id": job_id,
        "model_family": model_family,
        "training_period": training_period,
        "strategy_id": strategy_id,
        "instruction_type": instruction_type,
        "feature_group": feature_group,
        "timeframe": timeframe,
    }
    projected = _project_leaf_parquet_columns(
        service=service,
        asset_group=cat_norm,
        bucket=bucket,
        day=day,
        axes=axes,
    )
    if projected is not None:
        cols, gs_uri = projected
        return {
            "registered": False,
            "asset_group": cat_norm,
            "instrument_type": it_norm,
            "data_type": dt_norm,
            "venue": (venue or "").upper() if venue else None,
            "symbol_column": None,
            "source": "PARQUET_PROJECTION",
            "columns": [{"name": c, "dtype": "", "nullable": True, "description": ""} for c in cols],
            "projected_from": gs_uri,
        }

    # Step 4: Honest absence. Surface the probed paths so a path-drift
    # bug is actionable from the modal — operator can paste the URI into
    # ``gcloud storage ls`` and see whether the parquet really doesn't
    # exist or the projection template just doesn't match the on-disk
    # layout.
    probed_paths: list[str] = []
    if service and bucket and day:
        try:
            probed_paths = _build_leaf_parquet_candidates(
                service=service, asset_group=cat_norm, day=day, axes=axes, bucket=bucket
            )
        except (ValueError, RuntimeError):
            probed_paths = []
    contract_key = f"({cat_norm}, {it_norm or '∅'}, {dt_norm or '∅'})"
    if venue:
        contract_key += f" venue={venue.upper()}"
    parts = [
        f"No UAC schema contract registered for {contract_key}",
        f"and no parquet found on disk for service '{service}' on day '{day}'.",
    ]
    if probed_paths:
        parts.append("Paths probed (first-hit-wins):")
        for p in probed_paths:
            parts.append(f"  • {p}")
    parts.append(
        "Either the writer hasn't shipped this axis yet, or the "
        "projection path-template doesn't match the on-disk layout — "
        "file the latter as a path-drift bug."
    )
    return {
        "registered": False,
        "asset_group": cat_norm,
        "instrument_type": it_norm,
        "data_type": dt_norm,
        "venue": venue,
        "symbol_column": None,
        "source": "none",
        "columns": [],
        "message": "\n".join(parts),
        "probed_paths": probed_paths,
    }


def _project_leaf_parquet_columns(
    *,
    service: str | None,
    asset_group: str,
    bucket: str | None,
    day: str | None,
    axes: dict[str, str | None],
) -> tuple[list[str], str] | None:
    """Probe the actual parquet for a leaf shard and return its column names.

    Returns ``(columns, gs_uri)`` on success, ``None`` when no candidate
    parquet exists. The candidate paths depend on (service, asset_group);
    sports uses UAC ``candidate_parquet_paths`` (per-league subpartition
    first, then bare path), instruments-service uses the per-(venue, day)
    bundle, MTDS / MDPS / features-* probe the per-instrument or
    per-feature_group leaf, and ML / strategy / execution probe the
    per-job_id partition.

    Failure to resolve is non-fatal — caller falls through to the honest
    ``registered: False`` response.
    """
    if not (service and bucket and day):
        return None
    try:
        candidates = _build_leaf_parquet_candidates(
            service=service, asset_group=asset_group, day=day, axes=axes, bucket=bucket
        )
    except (ValueError, RuntimeError) as exc:
        logger.debug("leaf parquet candidate build failed: %s", exc)
        return None
    for gs_uri in candidates:
        # Directory prefix candidates need a list-first probe to find the
        # actual parquet file under the prefix.
        if gs_uri.endswith("/"):
            resolved = _first_parquet_under_prefix(gs_uri)
            if resolved is None:
                continue
            gs_uri = resolved
        try:
            names = _parquet_schema_names(gs_uri)
        except (OSError, RuntimeError, ValueError):
            continue
        if names:
            # Stable order: alphabetic with the obvious anchors first.
            anchors = [c for c in ("date", "timestamp", "available_at", "instrument_id") if c in names]
            rest = sorted(n for n in names if n not in anchors)
            return ([*anchors, *rest], gs_uri)
    return None


def _first_parquet_under_prefix(gs_prefix: str) -> str | None:
    """List ``gs_prefix`` and return the first .parquet URI, or None.

    Used by the schema projection fallback when the candidate path is a
    hive-partition directory rather than a fully-qualified parquet file.
    Limits the listing to 10 results — we only need one parquet to
    project the schema.
    """
    if not gs_prefix.startswith("gs://"):
        return None
    rest = gs_prefix[len("gs://") :]
    bucket, _, prefix = rest.partition("/")
    if not bucket:
        return None
    try:
        blobs = list_objects(bucket_name=bucket, prefix=prefix, max_results=10)
    except (OSError, RuntimeError, ValueError):
        return None
    for blob in blobs:
        name = getattr(blob, "name", None)
        if isinstance(name, str) and name.endswith(".parquet"):
            return f"gs://{bucket}/{name}"  # noqa: gs-uri  — URI composer, bucket already resolved
    return None


def _build_leaf_parquet_candidates(  # noqa: C901 — per-service GCS path routing dispatch; branching inherent to multi-service layout
    *,
    service: str,
    asset_group: str,
    day: str,
    axes: dict[str, str | None],
    bucket: str,
) -> list[str]:
    """Return the ordered list of GCS URIs to probe for the leaf parquet.

    Per-service routing matches the writers' actual on-disk layouts.
    First-hit-wins on the projection — caller iterates the list.
    """
    svc = service.lower()
    candidates: list[str] = []

    # Sports uses the UAC SSOT.
    if asset_group == "sports":
        from unified_api_contracts.sports import (
            candidate_parquet_uris,
        )

        league = axes.get("league_id") or ""
        data_type_value = (axes.get("data_type") or "").lower()
        if data_type_value:
            try:
                uri_list = candidate_parquet_uris(data_type=data_type_value, day=day, league_id=league or None)
                for uri in uri_list:  # pyright: ignore[reportUnknownVariableType]
                    if isinstance(uri, str):
                        candidates.append(uri)
            except (KeyError, ValueError) as exc:
                logger.debug("sports candidate_parquet_uris miss: %s", exc)
        return candidates

    # instruments-service / corporate-actions: per-(venue, day) bundle.
    venue = axes.get("venue") or ""
    if svc in ("instruments-service", "corporate-actions") and venue:
        candidates.append(f"gs://{bucket}/by_date/day={day}/venue={venue.upper()}/instruments.parquet")  # noqa: gs-uri  — URI composer, bucket already resolved
        return candidates

    # MTDS / MDPS / features-*: per-instrument (or bundle root for
    # options_chain / futures_chain) leaf parquet. We don't enumerate
    # every (chain, instrument_type, data_type) hive partition because
    # the right path depends on the writer's version; instead we
    # construct the most common variants and let the projection
    # short-circuit on first hit.
    instrument_type = (axes.get("instrument_type") or "").lower()
    data_type_value = (axes.get("data_type") or "").lower()
    instrument_id = axes.get("instrument_id") or ""
    chain = (axes.get("chain") or "").upper()
    feature_group = (axes.get("feature_group") or "").lower()
    timeframe = (axes.get("timeframe") or "").lower()

    if data_type_value and venue:
        prefix_root = f"gs://{bucket}/raw_tick_data/by_date/day={day}"  # noqa: gs-uri  — URI composer, bucket already resolved
        venue_partition = f"venue={venue.upper()}"
        if chain:
            venue_partition = f"venue={venue.upper()}-{chain}"
        if instrument_type:
            partitions = (
                f"asset_group={asset_group}/{venue_partition}/instrument_type={instrument_type}"
                f"/data_type={data_type_value}"
            )
            if instrument_id:
                candidates.append(f"{prefix_root}/{partitions}/instrument_id={instrument_id}/{instrument_id}.parquet")
                candidates.append(f"{prefix_root}/{partitions}/{instrument_id}.parquet")
            candidates.append(f"{prefix_root}/{partitions}/")

    # features-* layout: feature_group + timeframe partitions.
    if feature_group:
        partitions = f"by_date/day={day}/feature_group={feature_group}"
        if timeframe:
            partitions += f"/timeframe={timeframe}"
        if venue:
            partitions += f"/venue={venue.upper()}"
        if instrument_id:
            candidates.append(f"gs://{bucket}/{partitions}/{instrument_id}.parquet")  # noqa: gs-uri  — URI composer, bucket already resolved
        candidates.append(f"gs://{bucket}/{partitions}/")  # noqa: gs-uri  — URI composer, bucket already resolved

    # ML / strategy / execution: experiment-keyed partitions. The
    # writers may not yet emit job_id= (Phase 1B work) so we probe
    # both with and without.
    model_family = axes.get("model_family") or ""
    training_period = axes.get("training_period") or ""
    job_id = axes.get("job_id") or ""
    strategy_id = axes.get("strategy_id") or ""
    instruction_type = (axes.get("instruction_type") or "").upper()
    if svc in ("ml-training-service", "ml-inference-service") and model_family:
        base = f"gs://{bucket}/by_date/day={day}/model_family={model_family}"  # noqa: gs-uri  — URI composer, bucket already resolved
        if training_period:
            base += f"/training_period={training_period}"
        if job_id:
            candidates.append(f"{base}/job_id={job_id}/")
        candidates.append(f"{base}/")
    if svc in ("strategy-service", "execution-service") and strategy_id:
        base = f"gs://{bucket}/by_date/day={day}/strategy_id={strategy_id}"  # noqa: gs-uri  — URI composer, bucket already resolved
        if instruction_type:
            base += f"/instruction_type={instruction_type}"
        if job_id:
            candidates.append(f"{base}/job_id={job_id}/")
        candidates.append(f"{base}/")

    return candidates


# ---------------------------------------------------------------------------
# Instruments for a given (day, venue, instrument_type, data_type) shard
# ---------------------------------------------------------------------------


# Services that bundle every instrument for a (venue, day) pair into one
# parquet. The drill-down reads the parquet itself to surface the
# instrument_ids because the file layout carries no per-symbol partitioning.
_PER_VENUE_DAY_BUNDLE_SERVICES: frozenset[str] = frozenset({"instruments-service", "corporate-actions"})


# Symbol column per service for the bundled-parquet case. instruments-service
# stores the canonical identifier as ``instrument_key``.
_SERVICE_BUNDLE_SYMBOL_COLUMN: dict[str, str] = {
    "instruments-service": "instrument_key",
    "corporate-actions": "instrument_key",
}


def _shard_prefix(service: str, asset_group: str, venue: str, day: str, instrument_type: str, data_type: str) -> str:
    """Build the GCS prefix for a shard, routed by service.

    Different services use different bucket layouts — the drill-down must
    route per service, otherwise clicks produce empty modals because the
    prefix points at the wrong partition scheme.

    * ``instruments-service`` / ``corporate-actions`` — one parquet per
      ``(venue, day)`` under ``instrument_availability/by_date/``. No
      ``instrument_type`` / ``data_type`` partitioning — those live as
      columns inside the parquet. Sports additionally groups by league.
    * ``market-tick-data-service`` / ``market-data-processing-service`` —
      ``raw_tick_data/by_date/day=.../category=.../venue=.../instrument_type=<lower>/data_type=<lower>/``.
      MTDS writes the ``instrument_type`` / ``data_type`` axis values in
      lower case on disk (e.g. ``instrument_type=spot``), so we normalise
      the UI's upper-case inputs before building the prefix.
    * Other services fall back to the MTDS-shaped prefix. Features / ML /
      strategy do not surface drill-down today so this is effectively the
      previous behaviour.
    """
    svc = service.lower()
    if svc in _PER_VENUE_DAY_BUNDLE_SERVICES:
        if asset_group.lower() == "sports":
            # Sports groups by league inside the per-day listing; the UI
            # passes the league label through ``instrument_type`` because
            # that is the axis the manifest uses upstream. A dedicated
            # league kwarg can be added when the UI learns to send one.
            league = instrument_type or ""
            return f"instrument_availability/by_date/day={day}/league={league}/venue={venue}/"
        return f"instrument_availability/by_date/day={day}/venue={venue}/"

    if svc in {"market-tick-data-service", "market-data-processing-service"}:
        return (
            f"raw_tick_data/by_date/day={day}/category={asset_group.lower()}/"
            f"venue={venue}/instrument_type={instrument_type.lower()}/"
            f"data_type={data_type.lower()}/"
        )

    return (
        f"raw_tick_data/by_date/day={day}/category={asset_group.lower()}/"
        f"venue={venue}/instrument_type={instrument_type}/data_type={data_type}/"
    )


def _infer_symbol_column_for_shard(asset_group: str, instrument_type: str, data_type: str, venue: str) -> str:
    """Best-effort symbol column when no contract is registered.

    Falls back to ``instrument_id`` (the canonical column since Phase 1.2);
    per-venue conventions (pool_address for UNISWAP_V3 etc.) are picked up
    automatically via ``lookup_contract`` when the contract exists.
    """
    try:
        contract = lookup_contract(
            asset_group=asset_group.lower(),
            instrument_type=instrument_type,
            data_type=data_type,
            venue=venue,
        )
        return contract.symbol_column
    except SchemaContractNotFoundError:
        # Polymarket OTHER bucket stores conditionId as the market identifier.
        if venue.upper() == "POLYMARKET" and instrument_type.upper() == "OTHER":
            return "conditionId"
        return "instrument_id"


def _collect_parquet_files(bucket: str, prefix: str) -> list[dict[str, object]]:
    """Return ``[{file_uri, size_bytes, _name}, ...]`` for all parquets under prefix."""
    try:
        objects = list_objects(bucket, prefix, max_results=10_000)
    except (OSError, RuntimeError) as exc:
        logger.warning("list_objects failed for %s/%s: %s", bucket, prefix, exc)
        return []

    files: list[dict[str, object]] = []
    for o in objects:
        name = getattr(o, "name", None)
        if not isinstance(name, str) or not name.endswith(".parquet"):
            continue
        size = getattr(o, "size", None)
        files.append(
            {
                "file_uri": f"gs://{bucket}/{name}",  # noqa: gs-uri  — URI composer, bucket already resolved
                "size_bytes": int(size) if isinstance(size, int) else 0,
                "_name": name,
            }
        )
    return files


def _bundling_mode(venue: str, instrument_type: str, service: str = "") -> str:
    # instruments-service writes one bundled parquet per (venue, day) with
    # every instrument as a row — independent of venue / instrument_type —
    # so the whole service is a ``per_venue_day_bundle`` mode.
    if service and service.lower() in _PER_VENUE_DAY_BUNDLE_SERVICES:
        return "per_venue_day_bundle"
    if venue.upper() == "POLYMARKET" and instrument_type.upper() == "OTHER":
        return "per_condition_id"
    if _is_bundled(instrument_type):
        return "per_underlying"
    return "per_symbol"


def _expand_per_condition_id(
    parquet_files: list[dict[str, object]],
    asset_group: str,
    instrument_type: str,
    data_type: str,
    venue: str,
) -> list[dict[str, object]]:
    if not parquet_files:
        return []
    pf = parquet_files[0]
    symbol_col = _infer_symbol_column_for_shard(asset_group, instrument_type, data_type, venue)
    try:
        distinct_ids = _distinct_values_in_parquet(str(pf["file_uri"]), symbol_col)
    except (OSError, ValueError, RuntimeError) as exc:
        logger.warning("Failed to read bundle parquet %s: %s", pf["file_uri"], exc)
        return []
    return [
        {
            "instrument_id": sid,
            "file_uri": pf["file_uri"],
            "size_bytes": pf["size_bytes"],
            "bundled_under": str(pf["_name"]).split("/")[-1],
        }
        for sid in distinct_ids
    ]


def _expand_per_file(parquet_files: list[dict[str, object]]) -> list[dict[str, object]]:
    """Use the file stem as instrument_id (per_symbol / per_underlying modes)."""
    return [
        {
            "instrument_id": str(pf["_name"]).split("/")[-1].replace(".parquet", ""),
            "file_uri": pf["file_uri"],
            "size_bytes": pf["size_bytes"],
        }
        for pf in parquet_files
    ]


def _expand_per_venue_day_bundle(
    parquet_files: list[dict[str, object]],
    service: str,
    instrument_type: str,
) -> list[dict[str, object]]:
    """Expand a ``(venue, day)`` bundle (instruments-service style).

    The parquet holds every instrument for the (venue, day) pair. Each row
    becomes one drill-down entry; ``instrument_type`` (if provided by the
    UI) filters the rows so the modal only shows e.g. ``SPOT_PAIR`` when
    the user asked for spot instruments.
    """
    if not parquet_files:
        return []
    pf = parquet_files[0]
    symbol_col = _SERVICE_BUNDLE_SYMBOL_COLUMN.get(service.lower(), "instrument_key")
    uri = str(pf["file_uri"])
    try:
        df = _read_parquet_columns(uri, None)
    except (OSError, ValueError, RuntimeError) as exc:
        logger.warning("Failed to read bundle parquet %s: %s", uri, exc)
        return []

    # Filter by instrument_type when the UI provided one. instruments-service
    # stores it as a column in the same parquet. The UI often sends the
    # coarse family name (``SPOT``) while the parquet carries the specific
    # sub-type (``SPOT_PAIR``), so the match is case-insensitive and allows
    # prefix equality (``SPOT`` -> matches ``SPOT_PAIR`` / ``SPOT``).
    if instrument_type and "instrument_type" in df.columns:
        requested_it = instrument_type.upper()
        col = df["instrument_type"].astype(str).str.upper()
        df = df[(col == requested_it) | col.str.startswith(f"{requested_it}_")]

    if symbol_col not in df.columns:
        return []
    seen: set[str] = set()
    out: list[dict[str, object]] = []
    bundled_under = str(pf["_name"]).split("/")[-1]
    for v in df[symbol_col].dropna().tolist():  # pyright: ignore[reportUnknownVariableType,reportUnknownMemberType]
        sid = str(v).strip()
        if not sid or sid in seen:
            continue
        seen.add(sid)
        out.append(
            {
                "instrument_id": sid,
                "file_uri": uri,
                "size_bytes": pf["size_bytes"],
                "bundled_under": bundled_under,
            }
        )
    out.sort(key=lambda d: str(d["instrument_id"]))
    return out


# Backend default + cap for /instruments-for-shard pagination.
DEFAULT_INSTRUMENT_LIMIT: int = 50
MAX_INSTRUMENT_LIMIT: int = 500
# Search matches are also capped (keeps the payload small even if the
# search string is very generic).
MAX_SEARCH_RESULTS: int = 100


def _is_valid_instrument_id(candidate: str) -> bool:
    """Lightweight sanity check for a direct-paste instrument_id.

    Rejects obviously malformed blobs (newlines, spaces, empty) but does not
    try to enforce any specific venue format — instrument IDs are opaque
    tokens from the UI's perspective, and the actual existence check happens
    inside ``build_csv_export``.
    """
    if not candidate:
        return False
    stripped = candidate.strip()
    if not stripped:
        return False
    # Disallow whitespace / commas / shell-metas so callers can't sneak
    # multi-ID lookups through the paste field.
    if any(ch.isspace() for ch in stripped):
        return False
    # Everything else (dashes, slashes, 0x prefixes, conditionIds) passes —
    # except commas, which would sneak multi-ID lookups through the paste field.
    return "," not in stripped


def _apply_search_and_pagination(
    instruments: list[dict[str, object]],
    *,
    search: str | None,
    limit: int,
    offset: int,
    bundling: str,
) -> tuple[list[dict[str, object]], int]:
    """Return ``(page, total_count)`` for the selected slice.

    Search is a case-insensitive substring match on ``instrument_id`` and
    only applies to ``per_symbol`` / ``per_condition_id`` shards where the
    leaf IDs are individually meaningful. Bundled shards
    (``per_underlying``) intentionally skip search — the user is choosing a
    whole bundle by its root, not searching among bundles.
    """
    filtered = instruments
    if search and bundling != "per_underlying":
        needle = search.strip().lower()
        filtered = [inst for inst in instruments if needle in str(inst.get("instrument_id", "")).lower()][
            :MAX_SEARCH_RESULTS
        ]

    total = len(filtered)
    start = max(0, offset)
    end = start + max(1, limit)
    page = filtered[start:end]
    return page, total


def _list_instruments_full(
    *,
    service: str,
    asset_group: str,
    venue: str,
    day: str,
    instrument_type: str,
    data_type: str,
    project_id: str | None = None,
) -> dict[str, object]:
    """Return the full (un-paginated, un-searched) listing for a shard.

    Used internally by ``build_csv_export`` (needs to see every instrument
    in the shard) and by ``list_instruments_for_shard`` which then applies
    search + pagination on top.
    """
    cache_key = f"instruments:{service}:{asset_group}:{venue}:{day}:{instrument_type}:{data_type}"
    cached = _cache_get(cache_key)
    if cached is not None:
        return cast_dict(cached)

    bucket = build_bucket_name(service, asset_group, project_id)
    prefix = _shard_prefix(service, asset_group, venue, day, instrument_type, data_type)
    parquet_files = _collect_parquet_files(bucket, prefix)
    bundling = _bundling_mode(venue, instrument_type, service)

    if bundling == "per_condition_id":
        instruments = _expand_per_condition_id(parquet_files, asset_group, instrument_type, data_type, venue)
    elif bundling == "per_venue_day_bundle":
        instruments = _expand_per_venue_day_bundle(parquet_files, service, instrument_type)
    else:
        instruments = _expand_per_file(parquet_files)

    # Phase-C honest-coverage: attach capture_status / error_reason /
    # attempted_at from the manifest availability_index so the drill-down
    # modal can render status badges + retry tooltips per instrument.
    _attach_capture_status_to_instruments(instruments, bucket=bucket, venue=venue, day=day)

    full: dict[str, object] = {
        "service": service,
        "asset_group": asset_group.lower(),
        "venue": venue,
        "day": day,
        "instrument_type": instrument_type,
        "data_type": data_type,
        "bundling": bundling,
        "instruments": instruments,
        "bucket": bucket,
        "prefix": prefix,
    }
    _cache_put(cache_key, full)
    return full


def list_instruments_for_shard(
    *,
    service: str,
    asset_group: str,
    venue: str,
    day: str,
    instrument_type: str,
    data_type: str,
    limit: int = DEFAULT_INSTRUMENT_LIMIT,
    offset: int = 0,
    search: str | None = None,
    project_id: str | None = None,
) -> dict[str, object]:
    """List the instrument_ids present in a specific shard.

    Three bundling modes:
    - ``per_symbol``: one parquet per symbol (perpetuals, spot, equities,
      individual futures). Each file is a single instrument.
    - ``per_underlying``: one parquet per underlying root that carries every
      strike/expiry (options_chain, futures_chain, combo). The root IS the
      instrument_id from the UI's perspective.
    - ``per_condition_id``: Polymarket OTHER bucket — one parquet bundles
      many conditionIds. The parquet is read to surface distinct market IDs.

    The ``limit`` / ``offset`` / ``search`` parameters gate the response
    payload for large shards (e.g. Polymarket OTHER with ~5k conditionIds).
    The underlying listing is still cached in full so repeated pagination
    hits are cheap.
    """
    # Clamp the page window defensively before doing any work.
    safe_limit = max(1, min(int(limit or DEFAULT_INSTRUMENT_LIMIT), MAX_INSTRUMENT_LIMIT))
    safe_offset = max(0, int(offset or 0))

    full = _list_instruments_full(
        service=service,
        asset_group=asset_group,
        venue=venue,
        day=day,
        instrument_type=instrument_type,
        data_type=data_type,
        project_id=project_id,
    )

    raw_list_obj: object = full.get("instruments", [])
    raw_list = cast(list[object], raw_list_obj if isinstance(raw_list_obj, list) else [])
    full_instruments: list[dict[str, object]] = [
        cast_dict(cast(dict[str, object], i)) for i in raw_list if isinstance(i, dict)
    ]
    bundling_mode = str(full.get("bundling", "per_symbol"))

    page, total_count = _apply_search_and_pagination(
        full_instruments,
        search=search,
        limit=safe_limit,
        offset=safe_offset,
        bundling=bundling_mode,
    )

    return {
        "service": full.get("service", service),
        "asset_group": cast(str, full.get("asset_group", asset_group.lower())),
        "venue": full.get("venue", venue),
        "day": full.get("day", day),
        "instrument_type": full.get("instrument_type", instrument_type),
        "data_type": full.get("data_type", data_type),
        "bundling": bundling_mode,
        "instruments": page,
        "bucket": full.get("bucket", ""),
        "prefix": full.get("prefix", ""),
        "total_count": total_count,
        "limit": safe_limit,
        "offset": safe_offset,
        "has_more": (safe_offset + len(page)) < total_count,
        "search": (search or "").strip(),
    }


def preview_bundle_symbols(
    *,
    service: str,
    asset_group: str,
    venue: str,
    day: str,
    instrument_type: str,
    data_type: str,
    limit: int = 20,
    project_id: str | None = None,
) -> dict[str, object]:
    """Return the first ``limit`` symbol-column values inside a bundled shard.

    Meant for the "preview symbols inside" expander on the Instruments modal
    for ``per_underlying`` shards (options_chain / futures_chain / combo) —
    lets the user eyeball the contents of the bundle parquet before
    downloading it.
    """
    bundling = _bundling_mode(venue, instrument_type)
    if bundling != "per_underlying":
        return {
            "bundling": bundling,
            "symbols": [],
            "message": "Preview only applies to per_underlying bundles.",
        }

    listing = _list_instruments_full(
        service=service,
        asset_group=asset_group,
        venue=venue,
        day=day,
        instrument_type=instrument_type,
        data_type=data_type,
        project_id=project_id,
    )
    raw_list_obj: object = listing.get("instruments", [])
    raw_list = cast(list[object], raw_list_obj if isinstance(raw_list_obj, list) else [])
    if not raw_list:
        return {"bundling": bundling, "symbols": [], "message": "Bundle not found."}

    # Preview the first underlying's bundle parquet — the call-site passes
    # the specific underlying by using a single-underlying offset window if
    # it wants to preview a different root.
    first_obj = raw_list[0]
    if not isinstance(first_obj, dict):
        return {"bundling": bundling, "symbols": [], "message": "Bundle not found."}
    first = cast_dict(cast(dict[str, object], first_obj))
    uri = str(first.get("file_uri", ""))
    symbol_col = _infer_symbol_column_for_shard(asset_group, instrument_type, data_type, venue)
    try:
        symbols = _distinct_values_in_parquet(uri, symbol_col)[:limit]
    except (OSError, ValueError, RuntimeError) as exc:
        logger.warning("Failed to preview bundle parquet %s: %s", uri, exc)
        return {"bundling": bundling, "symbols": [], "message": str(exc)}

    return {
        "bundling": bundling,
        "underlying": str(first.get("instrument_id", "")),
        "file_uri": uri,
        "symbol_column": symbol_col,
        "symbols": symbols,
    }


def get_shard_info(
    *,
    service: str,
    asset_group: str,
    venue: str,
    day: str,
    data_type: str,
    project_id: str | None = None,
) -> dict[str, object]:
    """Return the instrument_types (and their bundling modes) present on a
    venue+day+data_type shard — lets the UI pick the right instrument_type
    when opening the drill-down instead of guessing from ``data_type``.

    Response shape:
    ``{instrument_types: [{name, bundling}, ...], recommended_instrument_type: str | None}``.

    ``recommended_instrument_type`` is the first non-``OTHER`` type if any
    (venues with a named + OTHER split usually want named as the default)
    or ``OTHER`` / the single type otherwise.
    """
    cache_key = f"shard_info:{service}:{asset_group}:{venue}:{day}:{data_type}"
    cached = _cache_get(cache_key)
    if isinstance(cached, dict):
        return cast_dict(cast(dict[str, object], cached))

    bucket = build_bucket_name(service, asset_group, project_id)
    venue_prefix = f"raw_tick_data/by_date/day={day}/category={asset_group.lower()}/venue={venue}/"
    instrument_types = sorted(_collect_instrument_types(bucket, venue_prefix))

    types_out: list[dict[str, str]] = []
    for it in instrument_types:
        types_out.append(
            {
                "name": it,
                "bundling": _bundling_mode(venue, it),
            }
        )

    named = [t["name"] for t in types_out if t["name"].upper() != "OTHER"]
    recommended: str | None
    if named:
        recommended = named[0]
    elif types_out:
        recommended = types_out[0]["name"]
    else:
        recommended = None

    result: dict[str, object] = {
        "service": service,
        "asset_group": asset_group.lower(),
        "venue": venue,
        "day": day,
        "data_type": data_type,
        "instrument_types": types_out,
        "recommended_instrument_type": recommended,
    }
    _cache_put(cache_key, result)
    return result


def cast_dict(obj: object) -> dict[str, object]:
    """Narrow ``object`` → ``dict[str, object]`` without runtime cost."""
    if isinstance(obj, dict):
        return obj  # pyright: ignore[reportReturnType]
    raise TypeError(f"Expected dict, got {type(obj).__name__}")


# ---------------------------------------------------------------------------
# Bucket counts (named markets vs conditionIds inside OTHER)
# ---------------------------------------------------------------------------


def compute_bucket_counts(
    *,
    service: str,
    asset_group: str,
    venue: str,
    day: str,
    data_type: str,
    project_id: str | None = None,
) -> dict[str, int]:
    """Return ``{"named_market_count": N, "other_market_count": M}``.

    - ``named_market_count`` is the number of distinct instrument_types
      under the venue for the given day (excluding ``OTHER``).
    - ``other_market_count`` is the number of distinct symbol-column values
      inside the OTHER-bucket parquet (if present). Zero when there is no
      OTHER bucket.

    This performs one GCS list per venue/day and, if OTHER exists, one
    parquet read. Results cache 5 min.
    """
    cache_key = f"bucket_counts:{service}:{asset_group}:{venue}:{day}:{data_type}"
    cached = _cache_get(cache_key)
    if isinstance(cached, dict):
        cached_typed: dict[str, int] = {}
        for raw_k, raw_v in cached.items():  # pyright: ignore[reportUnknownVariableType]
            key_str = str(raw_k)  # pyright: ignore[reportUnknownArgumentType]
            val_int = int(raw_v) if isinstance(raw_v, (int, float, str)) else 0
            cached_typed[key_str] = val_int
        return cached_typed

    bucket = build_bucket_name(service, asset_group, project_id)
    venue_prefix = f"raw_tick_data/by_date/day={day}/category={asset_group.lower()}/venue={venue}/"
    instrument_types = _collect_instrument_types(bucket, venue_prefix)
    named = sum(1 for it in instrument_types if it.upper() != "OTHER")

    other_count = 0
    if any(it.upper() == "OTHER" for it in instrument_types):
        other_count = _count_distinct_in_other_bucket(bucket, venue_prefix, asset_group, venue, data_type)

    result = {"named_market_count": named, "other_market_count": other_count}
    _cache_put(cache_key, result)
    return result


def _collect_instrument_types(bucket: str, venue_prefix: str) -> set[str]:
    """Parse distinct instrument_type values from all object paths under the venue."""
    try:
        objects = list_objects(bucket, venue_prefix, max_results=10_000)
    except (OSError, RuntimeError) as exc:
        logger.warning("list_objects failed for %s/%s: %s", bucket, venue_prefix, exc)
        return set()

    marker = "instrument_type="
    found: set[str] = set()
    for o in objects:
        name = getattr(o, "name", None)
        if not isinstance(name, str):
            continue
        idx = name.find(marker)
        if idx == -1:
            continue
        it = name[idx + len(marker) :].split("/", 1)[0]
        if it:
            found.add(it)
    return found


def _count_distinct_in_other_bucket(
    bucket: str, venue_prefix: str, asset_group: str, venue: str, data_type: str
) -> int:
    """Read the first OTHER-bucket parquet and return the distinct-symbol count."""
    symbol_col = _infer_symbol_column_for_shard(asset_group, "OTHER", data_type, venue)
    other_prefix = f"{venue_prefix}instrument_type=OTHER/data_type={data_type}/"
    try:
        other_objects = list_objects(bucket, other_prefix, max_results=10)
    except (OSError, RuntimeError) as exc:
        logger.warning("list_objects OTHER failed: %s", exc)
        return 0
    for o in other_objects:
        name = getattr(o, "name", None)
        if not isinstance(name, str) or not name.endswith(".parquet"):
            continue
        try:
            return len(_distinct_values_in_parquet(f"gs://{bucket}/{name}", symbol_col))  # noqa: gs-uri  — URI composer, bucket already resolved
        except (OSError, ValueError, RuntimeError) as exc:
            logger.warning("Failed to read OTHER parquet %s: %s", name, exc)
    return 0


# ---------------------------------------------------------------------------
# Parquet helpers (distinct symbol values, CSV export)
# ---------------------------------------------------------------------------

# Max rows we will export as CSV in a single response. Larger requests get
# rejected with a 413-equivalent error advising BigQuery external tables.
MAX_CSV_ROWS: int = 500_000


def _read_parquet_columns(gs_uri: str, columns: list[str] | None = None) -> pd.DataFrame:
    """Read a parquet from gs:// with gcsfs + pyarrow.

    We read the full row group in one shot because the files at this layer
    are already per-day per-shard (rarely >500k rows). The return is coerced
    to a typed ``pd.DataFrame`` at the boundary since neither ``gcsfs`` nor
    ``pyarrow`` ship basedpyright-friendly stubs.
    """

    # Local imports to keep module import-time cheap (gcsfs pulls aiohttp).
    import gcsfs
    import pyarrow.parquet as pq

    if not gs_uri.startswith("gs://"):
        raise ValueError(f"Not a gs:// URI: {gs_uri}")  # noqa: gs-uri  — error message string, not a URI constructor
    bucket_key = gs_uri[len("gs://") :]
    # gcsfs + pyarrow lack usable type stubs; we keep every cross-boundary
    # value narrowed to ``object`` and re-check at the DataFrame boundary.
    fs_any: object = gcsfs.GCSFileSystem(project=_pid)  # pyright: ignore[reportUnknownMemberType]
    open_fn: object = getattr(fs_any, "open", None)
    if not callable(open_fn):
        raise RuntimeError("gcsfs.GCSFileSystem missing open()")
    fh_obj: object = open_fn(bucket_key, "rb")
    try:
        read_table: object = pq.read_table  # pyright: ignore[reportUnknownMemberType, reportUnknownVariableType]
        if not callable(read_table):  # pyright: ignore[reportUnknownArgumentType]
            raise RuntimeError("pyarrow.parquet.read_table is not callable")
        table: object = read_table(fh_obj, columns=columns)  # pyright: ignore[reportUnknownVariableType]
        to_pandas: object = getattr(table, "to_pandas", None)  # pyright: ignore[reportUnknownArgumentType]
        if not callable(to_pandas):
            raise RuntimeError("pyarrow table missing to_pandas()")
        df_obj: object = to_pandas()
    finally:
        close: object = getattr(fh_obj, "close", None)
        if callable(close):
            close()
    if not isinstance(df_obj, pd.DataFrame):
        raise RuntimeError("pyarrow returned non-DataFrame payload")
    return df_obj


def _parquet_schema_names(gs_uri: str) -> set[str]:
    """Return the set of top-level column names present in the parquet at ``gs_uri``.

    Used to pick the current column variant when the parquet schema has drifted
    (e.g. FIXTURES moved from ``fixture_id`` → ``af_fixture_id``; see
    ``instruments-service`` orchestrator ~L3220 for the prefer-af pattern).
    """
    import gcsfs
    import pyarrow.parquet as pq

    if not gs_uri.startswith("gs://"):
        raise ValueError(f"Not a gs:// URI: {gs_uri}")  # noqa: gs-uri  — error message string, not a URI constructor
    bucket_key = gs_uri[len("gs://") :]
    fs_any: object = gcsfs.GCSFileSystem(project=_pid)  # pyright: ignore[reportUnknownMemberType]
    open_fn: object = getattr(fs_any, "open", None)
    if not callable(open_fn):
        raise RuntimeError("gcsfs.GCSFileSystem missing open()")
    fh_obj: object = open_fn(bucket_key, "rb")
    try:
        pf_ctor: object = pq.ParquetFile  # pyright: ignore[reportUnknownMemberType, reportUnknownVariableType]
        if not callable(pf_ctor):  # pyright: ignore[reportUnknownArgumentType]
            raise RuntimeError("pyarrow.parquet.ParquetFile is not callable")
        pf: object = pf_ctor(fh_obj)  # pyright: ignore[reportUnknownVariableType]
        schema_obj: object = getattr(pf, "schema_arrow", None)
        names_obj: object = getattr(schema_obj, "names", None) if schema_obj is not None else None
    finally:
        close: object = getattr(fh_obj, "close", None)
        if callable(close):
            close()
    if not isinstance(names_obj, list):
        return set()
    return {str(n) for n in cast(list[object], names_obj)}


def _pick_column(candidates: list[str], available: set[str]) -> str | None:
    """Return the first ``candidates`` entry that appears in ``available``."""
    for c in candidates:
        if c in available:
            return c
    return None


def _distinct_values_in_parquet(gs_uri: str, column: str) -> list[str]:
    df = _read_parquet_columns(gs_uri, [column])
    if column not in df.columns:
        return []
    # Coerce each cell through str() at the boundary — pandas dtype may be Any.
    out: set[str] = set()
    col_series = df[column].dropna()
    raw_values: object = col_series.unique().tolist()  # pyright: ignore[reportUnknownMemberType]
    # pandas .tolist() is guaranteed to return a plain list at runtime.
    values_list: list[object] = cast(list[object], raw_values)
    for v in values_list:
        s = str(v).strip()
        if s:
            out.add(s)
    return sorted(out)


def build_csv_export(
    *,
    service: str,
    asset_group: str,
    venue: str,
    day: str,
    instrument_type: str,
    data_type: str,
    instrument_ids: list[str],
    project_id: str | None = None,
    max_rows: int = MAX_CSV_ROWS,
) -> tuple[str, int, str]:
    """Return ``(csv_text, row_count, filename)`` for the selected instruments.

    - For per_symbol shards: reads the parquet for each selected
      ``instrument_id`` (file stem) and concatenates.
    - For per_underlying (options_chain / futures_chain / combo): each
      selection is the root (ES, NQ, BTC), reads the corresponding single
      parquet.
    - For per_condition_id (Polymarket OTHER): reads the one bundle
      parquet and filters to the selected conditionIds.

    Raises ``ValueError`` if row count would exceed ``max_rows``.
    """
    # CSV export must see every instrument in the shard, not just the first
    # page — users select IDs that may live on any page.
    listing = _list_instruments_full(
        service=service,
        asset_group=asset_group,
        venue=venue,
        day=day,
        instrument_type=instrument_type,
        data_type=data_type,
        project_id=project_id,
    )
    bundling = str(listing["bundling"])
    raw_instruments_obj: object = listing["instruments"]
    raw_list = cast(list[object], raw_instruments_obj if isinstance(raw_instruments_obj, list) else [])
    all_instruments: list[dict[str, object]] = []
    for i in raw_list:
        if isinstance(i, dict):
            all_instruments.append(cast_dict(cast(dict[str, object], i)))

    selected = set(instrument_ids) if instrument_ids else None

    frames: list[pd.DataFrame] = []

    if bundling == "per_condition_id" and all_instruments:
        # Single bundle parquet, filter by symbol column.
        pf_uri = str(all_instruments[0]["file_uri"])
        symbol_col = _infer_symbol_column_for_shard(asset_group, instrument_type, data_type, venue)
        df = _read_parquet_columns(pf_uri)  # full parquet
        if selected and symbol_col in df.columns:
            df = df[df[symbol_col].astype(str).isin(selected)]
        frames.append(df)
    else:
        # per_symbol or per_underlying: one parquet per instrument.
        for inst in all_instruments:
            iid = str(inst["instrument_id"])
            if selected is not None and iid not in selected:
                continue
            uri = str(inst["file_uri"])
            try:
                df = _read_parquet_columns(uri)
            except (OSError, ValueError, RuntimeError) as exc:
                logger.warning("Failed to read %s: %s", uri, exc)
                continue
            frames.append(df)

    if not frames:
        return "", 0, _csv_filename(service, venue, day, instrument_type, data_type)

    combined = pd.concat(frames, ignore_index=True, sort=False)
    if len(combined) > max_rows:
        raise ValueError(
            f"CSV export would include {len(combined):,} rows (> {max_rows:,}). "
            "Use a BigQuery external table over the parquet files instead."
        )

    csv_text = combined.to_csv(index=False)
    return csv_text, len(combined), _csv_filename(service, venue, day, instrument_type, data_type)


def _csv_filename(service: str, venue: str, day: str, instrument_type: str, data_type: str) -> str:
    return f"{service}_{venue}_{day}_{instrument_type}_{data_type}.csv"


def build_fixtures_csv_export(
    *,
    day: str,
    league_id: str,
    project_id: str | None = None,
    max_rows: int = MAX_CSV_ROWS,
) -> tuple[str, int, str]:
    """Return ``(csv_text, row_count, filename)`` for one (day, league) fixtures slice.

    Sports FIXTURES don't fit the per-instrument ``build_csv_export`` contract
    — the source parquet is a single daily file at
    ``gs://instruments-store-sports-{pid}/sports_reference/by_date/day={day}/entity=fixtures/fixtures.parquet``
    with all leagues in one file, keyed by ``af_league_id`` (API-Football
    numeric). This helper reads that parquet, maps canonical ``league_id`` →
    API-Football numeric via UAC, filters, and returns CSV.

    Args:
        day: ``YYYY-MM-DD``.
        league_id: Canonical league identifier (e.g. ``EPL``).
        project_id: GCP project (defaults to deployment-api settings).
        max_rows: hard cap, raises ``ValueError`` if exceeded (matches
            ``build_csv_export`` semantics).

    Raises:
        ValueError: unknown league_id, league has no api_football_id mapping,
            or row count would exceed ``max_rows``.
        FileNotFoundError: the day's fixtures parquet doesn't exist on GCS
            (adapter didn't run that day).
    """
    from unified_api_contracts.sports import get_league

    league = get_league(league_id)
    if league is None:
        raise ValueError(f"Unknown league_id: {league_id}")
    if league.api_football_id is None:
        raise ValueError(f"League {league_id} has no api_football_id — not sourced from API-Football")
    af_id = int(league.api_football_id)

    _sports_bucket = resolve_bucket_name(cloud="gcp", kind="instruments-store", asset_group="sports")
    gs_uri = f"gs://{_sports_bucket}/sports_reference/by_date/day={day}/entity=fixtures/fixtures.parquet"  # noqa: gs-uri  — URI composer, bucket resolved via resolve_bucket_name

    try:
        df = _read_parquet_columns(gs_uri)
    except (OSError, FileNotFoundError):
        # Adapter never ran for this (day, league) — return empty CSV with a
        # 0 row count so the caller can surface a "no data" hint to the user
        # instead of a download failure. The HTTP route sets X-Data-Status
        # so the UI can still distinguish "empty confirmed" from "never ran".
        return "", 0, _fixtures_csv_filename(day, league_id)

    if "af_league_id" not in df.columns:
        # Empty or malformed day file — return empty CSV rather than erroring.
        return "", 0, _fixtures_csv_filename(day, league_id)
    af_series = pd.to_numeric(df["af_league_id"], errors="coerce")
    filtered = df[af_series == af_id]
    if len(filtered) > max_rows:
        raise ValueError(
            f"Fixtures CSV export would include {len(filtered):,} rows (> {max_rows:,}). "
            "Narrow by date or use a BigQuery external table."
        )
    csv_text = filtered.to_csv(index=False)
    return csv_text, len(filtered), _fixtures_csv_filename(day, league_id)


def _fixtures_csv_filename(day: str, league_id: str) -> str:
    return f"instruments-service_FIXTURES_{league_id}_{day}.csv"


# ---------------------------------------------------------------------------
# Per-fixture drilldown: entity map, breakdown, download
# ---------------------------------------------------------------------------

# Fixture-scoped entities (STANDINGS is league-level — deliberately excluded).
# Order determines render sequence in CSV + JSON downloads.
_FIXTURE_ENTITIES: tuple[tuple[str, str, str], ...] = (
    # (data_type label, entity path suffix, parquet filename)
    ("FIXTURES", "entity=fixtures", "fixtures.parquet"),
    ("FIXTURE_STATS", "entity=fixture_stats", "fixture_stats.parquet"),
    ("FIXTURE_LINEUPS", "entity=fixture_lineups", "fixture_lineups.parquet"),
    ("FIXTURE_EVENTS", "entity=fixture_events", "fixture_events.parquet"),
    ("PLAYER_STATS", "entity=player_stats", "player_stats.parquet"),
    ("INJURIES", "entity=injuries", "injuries.parquet"),
    ("XG", "entity=understat_xg", "understat_xg.parquet"),
    ("WEATHER", "entity=weather", "weather.parquet"),
)

# Logical → candidate column names for the master fixtures parquet.
# The canonical API-Football schema uses `af_*` prefixes and `timestamp` /
# `status_short`; older writes may carry the legacy names. We probe the schema
# and pick whichever variant exists. Output keys in the API response stay
# canonical (left column) regardless of which variant was on disk.
_FIXTURE_META_ALIASES: dict[str, list[str]] = {
    "fixture_id": ["af_fixture_id", "fixture_id"],
    "af_league_id": ["af_league_id"],
    "kickoff_utc": ["timestamp", "kickoff_utc"],
    "home_team_name": ["af_home_name", "home_team_name"],
    "away_team_name": ["af_away_name", "away_team_name"],
    "status": ["status_short", "status"],
    "venue_id": ["venue_id"],
}


def _entity_gs_uri(*, day: str, path_suffix: str, filename: str, project_id: str | None = None) -> str:
    _sports_bucket = resolve_bucket_name(cloud="gcp", kind="instruments-store", asset_group="sports")
    return f"gs://{_sports_bucket}/sports_reference/by_date/day={day}/{path_suffix}/{filename}"  # noqa: gs-uri  — URI composer, bucket resolved via resolve_bucket_name


def _probe_fid_column(gs_uri: str) -> tuple[str, str | None]:
    """Probe the parquet schema and pick the canonical fixture-id column.

    Returns ``(status, fid_col)`` where ``status`` is one of ``"ok"`` /
    ``"empty_confirmed"`` / ``"attempted_failed"``. ``fid_col`` is ``None``
    on any non-``"ok"`` status.
    """
    try:
        schema_names = _parquet_schema_names(gs_uri)
    except (OSError, FileNotFoundError):
        return ("attempted_failed", None)
    except (ValueError, RuntimeError) as exc:
        logger.warning("per-fixture entity schema probe failed for %s: %s", gs_uri, exc)
        return ("attempted_failed", None)
    fid_col = _pick_column(["af_fixture_id", "fixture_id"], schema_names)
    if fid_col is None:
        return ("empty_confirmed", None)
    return ("ok", fid_col)


def _extract_fixture_ids_from_series(series: pd.Series) -> set[str]:  # pyright: ignore[reportMissingTypeArgument,reportUnknownParameterType]
    """Extract string fixture IDs from a pandas Series.

    ``af_fixture_id`` is ``int64`` on disk; pandas may render it with a
    trailing ``".0"`` when coerced through ``str()`` after dtype promotion.
    Strip that artifact so IDs round-trip cleanly to GCS paths and CSV.
    """
    raw_values: object = series.dropna().unique().tolist()  # pyright: ignore[reportUnknownMemberType]
    values_list: list[object] = cast(list[object], raw_values)
    out: set[str] = set()
    for v in values_list:
        s = str(v).strip()
        if s.endswith(".0"):
            s = s[:-2]
        if s:
            out.add(s)
    return out


def _read_entity_fixture_ids(gs_uri: str) -> tuple[str, set[str]]:
    """Return ``(capture_status, fixture_ids)`` for one per-day entity parquet.

    ``capture_status`` is one of ``"captured"`` / ``"empty_confirmed"`` /
    ``"attempted_failed"``. When ``"attempted_failed"`` the returned set is
    empty; callers should fall through to per-fixture ``"missing"`` /
    ``"attempted_failed"`` classification.

    Per-entity parquets key rows on ``af_fixture_id`` (canonical, per
    instruments-service orchestrator); older writes used ``fixture_id``.
    """
    status, fid_col = _probe_fid_column(gs_uri)
    if fid_col is None:
        return (status, set())
    try:
        df = _read_parquet_columns(gs_uri, [fid_col])
    except (OSError, FileNotFoundError):
        return ("attempted_failed", set())
    except (ValueError, RuntimeError) as exc:
        logger.warning("per-fixture entity read failed for %s: %s", gs_uri, exc)
        return ("attempted_failed", set())
    if fid_col not in df.columns or df.empty:
        return ("empty_confirmed", set())
    fixture_ids = _extract_fixture_ids_from_series(df[fid_col])
    if not fixture_ids:
        return ("empty_confirmed", set())
    return ("captured", fixture_ids)


def _load_fixture_meta(
    *,
    day: str,
    league_id: str,
    project_id: str | None = None,
) -> tuple[list[dict[str, object]], int | None]:
    """Return ``(fixtures_for_league, af_league_id_or_None)``.

    Each fixture dict has: fixture_id, kickoff_utc, home_team_name,
    away_team_name, status, venue_id.

    Raises ``FileNotFoundError`` when the day's fixtures parquet is absent;
    ``ValueError`` when the canonical league_id can't be mapped to an
    API-Football numeric id.
    """
    from unified_api_contracts.sports import get_league

    league = get_league(league_id)
    if league is None:
        raise ValueError(f"Unknown league_id: {league_id}")
    if league.api_football_id is None:
        raise ValueError(f"League {league_id} has no api_football_id — not sourced from API-Football")
    af_id = int(league.api_football_id)

    gs_uri = _entity_gs_uri(
        day=day,
        path_suffix="entity=fixtures",
        filename="fixtures.parquet",
        project_id=project_id,
    )

    schema_names = _parquet_schema_names(gs_uri)
    # Resolve each logical field to whichever schema variant is present.
    resolved: dict[str, str] = {}
    for canonical, variants in _FIXTURE_META_ALIASES.items():
        picked = _pick_column(variants, schema_names)
        if picked is not None:
            resolved[canonical] = picked
    proj_cols = list(resolved.values())
    if not proj_cols:
        return ([], af_id)
    df = _read_parquet_columns(gs_uri, proj_cols)
    # Rename variant → canonical so downstream row access is schema-agnostic.
    df = df.rename(columns={variant: canonical for canonical, variant in resolved.items()})

    if "af_league_id" not in df.columns or df.empty:
        return ([], af_id)

    af_series = pd.to_numeric(df["af_league_id"], errors="coerce")
    filtered = df[af_series == af_id]
    fixtures: list[dict[str, object]] = []
    for row in filtered.to_dict(orient="records"):  # pyright: ignore[reportUnknownMemberType,reportUnknownVariableType]
        # af_fixture_id is int64; trim pandas ".0" decimal artifact on str().
        raw_fid = str(row.get("fixture_id") or "").strip()  # pyright: ignore[reportUnknownMemberType,reportUnknownArgumentType]
        if raw_fid.endswith(".0"):
            raw_fid = raw_fid[:-2]
        if not raw_fid:
            continue
        fixtures.append(
            {
                "fixture_id": raw_fid,
                "kickoff_utc": str(row.get("kickoff_utc") or ""),  # pyright: ignore[reportUnknownMemberType,reportUnknownArgumentType]
                "home_team_name": str(row.get("home_team_name") or ""),  # pyright: ignore[reportUnknownMemberType,reportUnknownArgumentType]
                "away_team_name": str(row.get("away_team_name") or ""),  # pyright: ignore[reportUnknownMemberType,reportUnknownArgumentType]
                "status": str(row.get("status") or ""),  # pyright: ignore[reportUnknownMemberType,reportUnknownArgumentType]
                "venue_id": str(row.get("venue_id") or ""),  # pyright: ignore[reportUnknownMemberType,reportUnknownArgumentType]
            }
        )
    return (fixtures, af_id)


def build_fixture_breakdown(
    *,
    day: str,
    league_id: str,
    project_id: str | None = None,
) -> dict[str, object]:
    """Return per-fixture coverage for one (day, league_id).

    Response shape:

    ``{
        day, league_id, af_league_id,
        fixtures_expected: int,
        fixtures: [
          {fixture_id, kickoff_utc, home_team_name, away_team_name, status,
           coverage: {FIXTURES: "captured", FIXTURE_STATS: "missing", ...},
           coverage_summary: {captured, empty_confirmed, missing, failed}}
        ]
    }``

    When the day's master fixtures parquet is absent (adapter never ran for
    this day), returns ``{"fixtures_expected": 0, "fixtures": [],
    "status": "no_schedule"}``. Phase-3 semantics: the UI surfaces this as
    "no schedule recorded" rather than red-missing fixtures.
    """
    try:
        fixtures_meta, af_id = _load_fixture_meta(day=day, league_id=league_id, project_id=project_id)
    except (OSError, FileNotFoundError):
        return {
            "day": day,
            "league_id": league_id,
            "af_league_id": None,
            "fixtures_expected": 0,
            "fixtures": [],
            "status": "no_schedule",
        }

    # Resolve per-entity fixture_id sets ONCE per entity (bounded by 8 reads).
    entity_status: dict[str, tuple[str, set[str]]] = {}
    for data_type, path_suffix, filename in _FIXTURE_ENTITIES:
        gs_uri = _entity_gs_uri(day=day, path_suffix=path_suffix, filename=filename, project_id=project_id)
        entity_status[data_type] = _read_entity_fixture_ids(gs_uri)

    fixture_rows: list[dict[str, object]] = []
    for meta in fixtures_meta:
        fid = str(meta["fixture_id"])
        coverage: dict[str, str] = {}
        summary = {"captured": 0, "empty_confirmed": 0, "missing": 0, "failed": 0}
        for data_type, _suffix, _filename in _FIXTURE_ENTITIES:
            status, ids = entity_status[data_type]
            if status == "attempted_failed":
                coverage[data_type] = "attempted_failed"
                summary["failed"] += 1
                continue
            if status == "empty_confirmed":
                coverage[data_type] = "empty_confirmed"
                summary["empty_confirmed"] += 1
                continue
            # status == "captured" — per-fixture presence check
            if fid in ids:
                coverage[data_type] = "captured"
                summary["captured"] += 1
            else:
                coverage[data_type] = "missing"
                summary["missing"] += 1
        row: dict[str, object] = dict(meta)
        row["coverage"] = coverage
        row["coverage_summary"] = summary
        fixture_rows.append(row)

    return {
        "day": day,
        "league_id": league_id,
        "af_league_id": af_id,
        "fixtures_expected": len(fixture_rows),
        "fixtures": fixture_rows,
        "status": "resolved",
    }


def _fixture_download_filename(fixture_id: str, fmt: str) -> str:
    safe_fmt = "csv" if fmt.lower() == "csv" else "json"
    return f"instruments-service_FIXTURE_{fixture_id}.{safe_fmt}"


# ---------------------------------------------------------------------------
# DeFi per-pool drilldown — institutional equivalent of the sports
# build_fixture_breakdown for chain-native data.
# ---------------------------------------------------------------------------


# Candidate columns that may carry the canonical pool/asset identifier in DeFi
# tick parquets. Order = preference: ``instrument_key`` is the canonical UAC
# format (``UNISWAP_V3-ETHEREUM:POOL:USDC-WETH-500``); ``pool_address`` /
# ``pool_id`` / ``token`` are venue-specific fallbacks (Uniswap V2/V3/V4 +
# AAVE / Lido / etc. legacy override columns).
_DEFI_POOL_ID_CANDIDATE_COLS: tuple[str, ...] = (
    "instrument_key",
    "instrument_id",
    "pool_address",
    "pool_id",
    "pair_address",
    "token",
    "token_symbol",
    "symbol",
)


def _defi_tick_bucket(project_id: str | None = None) -> str:
    # project_id ignored — resolve_bucket_name resolves from SSOT (cloud-providers.yaml)
    return resolve_bucket_name(cloud="gcp", kind="market-data", asset_group="defi")


def _venue_chain_partition(venue: str, chain: str) -> str:
    """Combine venue + chain into the single GCS partition key DeFi uses.

    DeFi parquets are written under ``venue={VENUE}-{CHAIN}/...`` (combined
    partition); the UI-facing API exposes them as separate ``venue`` + ``chain``
    parameters so the rest of the data-status surface stays category-uniform.
    """
    return f"{venue.upper()}-{chain.upper()}"


def _defi_venue_chain_aliases(venue_chain: str) -> list[str]:
    """Aliases to try for a DeFi pool-breakdown ``venue={...}/`` GCS prefix.

    GCS layout has two conventions per protocol:
      1. Combined ``venue={PROTOCOL}-{CHAIN}/`` for single-chain protocols
         (e.g. EIGENLAYER-ETHEREUM).
      2. Protocol-only ``venue={PROTOCOL}/`` for multi-chain protocols
         (e.g. AAVE_V3, UNISWAP_V3 — chain encoded in pool_id row).
    Also tries underscore variants per UAC↔GCS naming drift (AAVE_V3 vs AAVEV3).
    """

    protocol_only = venue_chain.split("-")[0]
    out: list[str] = [venue_chain, protocol_only]
    for candidate in (
        _re.sub(r"_(V\d+)", r"\1", venue_chain),
        _re.sub(r"_(V\d+)", r"\1", protocol_only),
        _re.sub(r"([A-Z])(V\d+)", r"\1_\2", venue_chain),
    ):
        if candidate not in out:
            out.append(candidate)
    return out


def _list_defi_objects_with_aliases(*, bucket: str, day: str, venue_chain: str) -> list[object]:
    """List objects under the first DeFi venue alias that has any parquets.

    Tries each alias from :func:`_defi_venue_chain_aliases` x 2 hive-key
    candidates x 2 venue-partition shapes:

      - Hive key: ``asset_group=defi/`` (canonical, post-2026-04 writes)
        first; ``category=defi/`` (legacy) as fallback per workspace rule
        "asset_group= is canonical for new writes; readers must try
        canonical first then fall back to legacy".
      - Venue partition: ``venue={protocol}-{chain}/`` (combined, used by
        single-chain protocols like EIGENLAYER-ETHEREUM) AND
        ``venue={protocol}/chain={CHAIN}/`` (separated, the actual on-disk
        layout for multi-chain protocols like AAVE_V3 / UNISWAP_V3).

    First non-empty match wins.
    """
    from deployment_api.utils.storage_facade import list_objects as _list_objects

    # Pull the chain part once; multi-chain protocols partition it
    # separately on disk while single-chain protocols leave it combined.
    chain_part = ""
    if "-" in venue_chain:
        _, _, chain_part = venue_chain.rpartition("-")
    candidates: list[str] = []
    for alias in _defi_venue_chain_aliases(venue_chain):
        protocol_only = alias.split("-")[0]
        for hive_key in ("asset_group", "category"):
            # Combined-partition layout (single-chain protocols)
            candidates.append(f"raw_tick_data/by_date/day={day}/{hive_key}=defi/venue={alias}/")
            # Separated-partition layout (multi-chain protocols) — only
            # makes sense when we have a chain part to put under chain=.
            if chain_part:
                candidates.append(
                    f"raw_tick_data/by_date/day={day}/{hive_key}=defi/venue={protocol_only}/chain={chain_part}/"
                )
    for prefix in candidates:
        try:
            found = _list_objects(bucket, prefix, max_results=DEFAULT_INSTRUMENT_LIMIT)
        except (OSError, ValueError, RuntimeError) as exc:
            logger.warning("build_pool_breakdown: list failed for %s/%s: %s", bucket, prefix, exc)
            continue
        if found:
            return list(found)
    return []


def _list_pool_entities_for_venue(
    *, day: str, venue_chain: str, project_id: str | None = None
) -> list[tuple[str, str, str]]:
    """Return ``[(instrument_type, data_type, gs_uri), ...]`` for one (day, venue+chain).

    Walks the GCS prefix
    ``market-data-tick-defi-{pid}/raw_tick_data/by_date/day={day}/category=defi/
    venue={venue_chain}/`` and surfaces every ``instrument_type=`` /
    ``data_type=`` partition that has a written parquet. Capped by
    ``DEFAULT_INSTRUMENT_LIMIT`` to keep the listing bounded.

    DeFi venue+chain aliasing: UI/UAC use ``AAVE_V3-ETHEREUM`` but the GCS
    writer drops the underscore (``AAVEV3-ETHEREUM``). Tries the literal
    form first, then the underscore-stripped form (and reverse) — uses
    whichever the bucket actually has.
    """
    bucket = _defi_tick_bucket(project_id)
    objects = _list_defi_objects_with_aliases(bucket=bucket, day=day, venue_chain=venue_chain)
    if not objects:
        return []

    seen: set[tuple[str, str]] = set()
    out: list[tuple[str, str, str]] = []
    for obj in objects:
        if not obj.name.endswith(".parquet"):
            continue
        instrument_type = ""
        data_type = ""
        for token in obj.name.split("/"):
            if token.startswith("instrument_type="):
                instrument_type = token[len("instrument_type=") :]
            elif token.startswith("data_type="):
                data_type = token[len("data_type=") :]
        if not instrument_type or not data_type:
            continue
        key = (instrument_type, data_type)
        if key in seen:
            continue
        seen.add(key)
        gs_uri = f"gs://{bucket}/{obj.name}"  # noqa: gs-uri  — URI composer, bucket already resolved
        out.append((instrument_type, data_type, gs_uri))
    return out


def _read_pool_ids_from_parquet(gs_uri: str) -> tuple[str, set[str]]:
    """Return ``(capture_status, pool_ids)`` for one DeFi entity parquet.

    Tries each column in :data:`_DEFI_POOL_ID_CANDIDATE_COLS` in order until it
    finds one present in the parquet schema. Empty set + ``"empty_confirmed"``
    when the parquet exists but has no canonical-ID column or no rows;
    ``"attempted_failed"`` on any read error.
    """
    try:
        schema_names = _parquet_schema_names(gs_uri)
    except (OSError, FileNotFoundError):
        return ("attempted_failed", set())
    except (ValueError, RuntimeError) as exc:
        logger.warning("pool breakdown schema probe failed for %s: %s", gs_uri, exc)
        return ("attempted_failed", set())

    pool_id_col = _pick_column(list(_DEFI_POOL_ID_CANDIDATE_COLS), schema_names)
    if pool_id_col is None:
        return ("empty_confirmed", set())

    try:
        df = _read_parquet_columns(gs_uri, [pool_id_col])
    except (OSError, FileNotFoundError):
        return ("attempted_failed", set())
    except (ValueError, RuntimeError) as exc:
        logger.warning("pool breakdown read failed for %s: %s", gs_uri, exc)
        return ("attempted_failed", set())

    if pool_id_col not in df.columns or df.empty:
        return ("empty_confirmed", set())

    raw_values: object = df[pool_id_col].dropna().unique().tolist()  # pyright: ignore[reportUnknownMemberType]
    values_list: list[object] = cast(list[object], raw_values)
    pool_ids: set[str] = set()
    for v in values_list:
        s = str(v).strip()
        if s:
            pool_ids.add(s)
    if not pool_ids:
        return ("empty_confirmed", set())
    return ("captured", pool_ids)


def build_pool_breakdown(
    *,
    day: str,
    venue: str,
    chain: str,
    project_id: str | None = None,
) -> dict[str, object]:
    """Per-pool coverage for one (day, venue, chain) DeFi shard.

    Mirrors :func:`build_fixture_breakdown` but for chain-native protocols.
    Walks every ``(instrument_type, data_type)`` partition under the
    ``venue=<VENUE>-<CHAIN>/`` prefix, extracts the canonical pool / asset
    identifier from each parquet, and builds a per-pool coverage map showing
    which data_types captured each pool.

    Response shape::

        {
          "day": str, "venue": str, "chain": str,
          "venue_chain": "UNISWAP_V3-ETHEREUM",
          "data_types_expected": ["dex_pool_state", "dex_pool_swaps"],
          "pools_expected": int,
          "pools": [
            {"pool_id": "USDC-WETH-500",
             "coverage": {"dex_pool_state": "captured", "dex_pool_swaps": "missing"},
             "coverage_summary": {"captured": 1, "empty_confirmed": 0,
                                   "missing": 1, "failed": 0}}
          ],
          "status": "resolved" | "no_data"
        }

    ``status="no_data"`` means no parquets exist for the (day, venue, chain) —
    the UI surfaces this as a "venue not yet captured for this day" state
    (same semantics as sports' ``"no_schedule"``).
    """
    venue_chain = _venue_chain_partition(venue, chain)
    entities = _list_pool_entities_for_venue(day=day, venue_chain=venue_chain, project_id=project_id)
    if not entities:
        return {
            "day": day,
            "venue": venue.upper(),
            "chain": chain.upper(),
            "venue_chain": venue_chain,
            "data_types_expected": [],
            "pools_expected": 0,
            "pools": [],
            "status": "no_data",
        }

    # Read pool_id sets per (instrument_type, data_type); union them to get
    # the universe of pools captured anywhere on this day.
    per_entity_status: dict[tuple[str, str], tuple[str, set[str]]] = {}
    universe: set[str] = set()
    for instrument_type, data_type, gs_uri in entities:
        status, pool_ids = _read_pool_ids_from_parquet(gs_uri)
        per_entity_status[(instrument_type, data_type)] = (status, pool_ids)
        universe |= pool_ids

    data_types_expected = sorted({dt for _it, dt, _u in entities})
    pools_rows: list[dict[str, object]] = []
    for pool_id in sorted(universe):
        coverage: dict[str, str] = {}
        summary = {"captured": 0, "empty_confirmed": 0, "missing": 0, "failed": 0}
        for instrument_type, data_type, _gs_uri in entities:
            status, pool_ids = per_entity_status[(instrument_type, data_type)]
            if status == "attempted_failed":
                state = "failed"
            elif status == "empty_confirmed":
                state = "empty_confirmed"
            elif pool_id in pool_ids:
                state = "captured"
            else:
                state = "missing"
            coverage[data_type] = state
            summary[state] += 1
        pools_rows.append(
            {
                "pool_id": pool_id,
                "coverage": coverage,
                "coverage_summary": summary,
            }
        )

    return {
        "day": day,
        "venue": venue.upper(),
        "chain": chain.upper(),
        "venue_chain": venue_chain,
        "data_types_expected": data_types_expected,
        "pools_expected": len(universe),
        "pools": pools_rows,
        "status": "resolved",
    }


def _filter_entity_rows_for_fixture(gs_uri: str, fixture_id: str) -> tuple[str, pd.DataFrame | None]:
    """Read a per-day entity parquet and return ``(capture_status, rows_for_fixture_or_none)``.

    ``rows_for_fixture_or_none`` is ``None`` for ``attempted_failed`` /
    ``empty_confirmed``, an empty DataFrame for ``missing`` (parquet has rows
    but none for this fixture), or a populated slice for ``captured``.
    """
    try:
        df = _read_parquet_columns(gs_uri, None)
    except (OSError, FileNotFoundError):
        return ("attempted_failed", None)
    except (ValueError, RuntimeError) as exc:
        logger.warning("per-fixture download read failed for %s: %s", gs_uri, exc)
        return ("attempted_failed", None)

    # Support both new flat schema (``af_fixture_id``) and legacy
    # (``fixture_id``); migrated parquets use ``af_fixture_id`` as the
    # canonical join key.
    fid_col = (
        "af_fixture_id" if "af_fixture_id" in df.columns else ("fixture_id" if "fixture_id" in df.columns else None)
    )
    if fid_col is None or df.empty:
        return ("empty_confirmed", None)

    fid_series = df[fid_col].astype(str).str.split(".").str[0]  # strip float ".0" suffix
    filtered = df[fid_series == fixture_id]
    if filtered.empty:
        return ("missing", filtered)
    return ("captured", filtered)


def _resolve_fixture_day(*, fixture_id: str, day: str | None, project_id: str | None = None) -> str:
    """Return the canonical kickoff day for a fixture_id.

    When ``day`` is supplied it is used as-is (fast path). When omitted the
    caller has no anchor — we do not scan the entire reference store; we
    raise ``ValueError`` so the HTTP layer returns 400. This keeps the
    endpoint cheap and predictable.
    """
    if day:
        return day
    raise ValueError(
        "fixture_id alone cannot resolve a day — pass ?day=YYYY-MM-DD "
        "together with ?fixture_id=... (the UI already knows the day from "
        "the breakdown response)."
    )


def build_fixture_download(
    *,
    fixture_id: str,
    day: str,
    fmt: str,
    project_id: str | None = None,
) -> tuple[str, int, str, str]:
    """Return ``(body_text, row_count, filename, media_type)`` for one fixture.

    ``fmt`` is ``"csv"`` or ``"json"``. CSV produces a denormalised shape:
    one leading column ``entity`` + the entity's own columns; rows from
    different entities are concatenated with union-of-columns (missing
    columns = blank cell). JSON produces ``{fixture_id, day, entities: {...},
    coverage: {...}}`` where each entity value is either a list of records
    (captured) or a sentinel ``{"capture_status": "..."}`` dict.
    """
    fmt_lower = fmt.lower()
    if fmt_lower not in ("csv", "json"):
        raise ValueError(f"Unsupported format: {fmt!r} (expected 'csv' or 'json')")

    # Resolve the day (must be supplied today — see _resolve_fixture_day docstring).
    day_resolved = _resolve_fixture_day(fixture_id=fixture_id, day=day, project_id=project_id)

    per_entity: dict[str, tuple[str, pd.DataFrame | None]] = {}
    for data_type, path_suffix, filename in _FIXTURE_ENTITIES:
        gs_uri = _entity_gs_uri(
            day=day_resolved,
            path_suffix=path_suffix,
            filename=filename,
            project_id=project_id,
        )
        per_entity[data_type] = _filter_entity_rows_for_fixture(gs_uri, fixture_id)

    coverage: dict[str, str] = {dt: status for dt, (status, _rows) in per_entity.items()}
    total_captured_rows = sum((0 if rows is None else len(rows)) for _status, rows in per_entity.values())

    if total_captured_rows == 0:
        # Nothing found for this fixture across any entity — surface a 404
        # at the HTTP layer.
        raise FileNotFoundError(f"fixture_id {fixture_id!r} not found in any entity for day {day_resolved}")

    filename_out = _fixture_download_filename(fixture_id, fmt_lower)

    if fmt_lower == "csv":
        frames: list[pd.DataFrame] = []
        for data_type, (status, rows) in per_entity.items():
            if status != "captured" or rows is None or rows.empty:
                continue
            tagged = rows.copy()
            tagged.insert(0, "entity", data_type)
            frames.append(tagged)
        if not frames:
            # defensive — total_captured_rows above should have caught this
            raise FileNotFoundError(f"fixture_id {fixture_id!r} resolved no CSV-writable rows")
        merged = pd.concat(frames, ignore_index=True, sort=False)
        csv_text = merged.to_csv(index=False)
        return (csv_text, len(merged), filename_out, "text/csv; charset=utf-8")

    # JSON
    import json

    entities_payload: dict[str, object] = {}
    for data_type, (status, rows) in per_entity.items():
        if status == "captured" and rows is not None and not rows.empty:
            entities_payload[data_type] = {
                "capture_status": "captured",
                "rows": rows.to_dict(orient="records"),  # pyright: ignore[reportUnknownMemberType]
            }
        else:
            entities_payload[data_type] = {"capture_status": status}

    body = {
        "fixture_id": fixture_id,
        "day": day_resolved,
        "coverage": coverage,
        "entities": entities_payload,
    }
    json_text = json.dumps(body, default=str)
    return (json_text, total_captured_rows, filename_out, "application/json")


# ---------------------------------------------------------------------------
# Instruments shard CSV export (instruments-service / corporate-actions)
# ---------------------------------------------------------------------------

MAX_SHARD_CSV_ROWS = 50_000


def build_instruments_shard_csv_export(
    *,
    service: str,
    asset_group: str,
    venue: str,
    date: str,
    project_id: str | None = None,
    max_rows: int = MAX_SHARD_CSV_ROWS,
) -> tuple[str, int, str]:
    """Return ``(csv_text, row_count, filename)`` for one (venue, date) instruments shard.

    instruments-service writes one parquet per ``(venue, day)`` at
    ``instrument_availability/by_date/day={date}/venue={venue}/instruments.parquet``.
    This endpoint reads that file and streams the entire shard as CSV so the
    operator can inspect what instruments were captured on a given day.

    Only valid for services in ``_PER_VENUE_DAY_BUNDLE_SERVICES``
    (instruments-service, corporate-actions).

    Raises:
        ValueError: unsupported service, or row count exceeds ``max_rows``.
        FileNotFoundError: parquet does not exist (adapter never ran that day).
    """
    svc = service.lower()
    if svc not in _PER_VENUE_DAY_BUNDLE_SERVICES:
        raise ValueError(
            f"Service {service!r} does not use per-venue-day-bundle sharding. "
            f"Supported: {sorted(_PER_VENUE_DAY_BUNDLE_SERVICES)}"
        )

    bucket = build_bucket_name(service, asset_group, project_id)
    gs_uri = f"gs://{bucket}/instrument_availability/by_date/day={date}/venue={venue}/instruments.parquet"  # noqa: gs-uri  — URI composer, bucket already resolved

    try:
        df = _read_parquet_columns(gs_uri)
    except (OSError, FileNotFoundError):
        return "", 0, _shard_csv_filename(service, asset_group, venue, date)

    if len(df) > max_rows:
        raise ValueError(
            f"Shard CSV export would include {len(df):,} rows (> {max_rows:,}). "
            "Use a BigQuery external table over the parquet files instead."
        )

    csv_text = df.to_csv(index=False)
    return csv_text, len(df), _shard_csv_filename(service, asset_group, venue, date)


def _shard_csv_filename(service: str, asset_group: str, venue: str, date: str) -> str:
    return f"{service}_{asset_group}_{venue}_{date}.csv"


# ---------------------------------------------------------------------------
# MTDS availability-catalog CSV export (market-tick-data-service)
# ---------------------------------------------------------------------------

MTDS_SHARD_SERVICES: frozenset[str] = frozenset({"market-tick-data-service", "market-data-processing-service"})

# Columns to include in the availability catalog CSV (in order).
# These are the v5 manifest columns that operators care about.
_MTDS_CATALOG_COLUMNS: list[str] = [
    "date",
    "venue",
    "instrument_type",
    "data_type",
    "instrument_id",
    "capture_status",
    "error_reason",
    "attempted_at",
    "written_at",
]


def build_mtds_shard_csv_export(
    *,
    service: str,
    asset_group: str,
    venue: str,
    date: str,
    project_id: str | None = None,
) -> tuple[str, int, str]:
    """Return ``(csv_text, row_count, filename)`` for one (venue, date) MTDS catalog.

    MTDS writes one parquet per symbol under
    ``raw_tick_data/by_date/day={date}/…/{symbol}.parquet`` — exporting the
    raw tick data is impractical (millions of rows). Instead this function
    exports the **availability manifest** slice for ``(venue, date)`` so the
    operator can see exactly which instruments were captured, their
    ``capture_status``, and any ``error_reason`` in a single CSV.

    Returns an empty CSV string (row_count=0) when the manifest is unreachable
    or contains no rows for the requested (venue, date) — callers can inspect
    ``X-Data-Status: empty_or_missing`` to distinguish.

    Raises:
        ValueError: unsupported service.
    """
    svc = service.lower()
    if svc not in MTDS_SHARD_SERVICES:
        raise ValueError(f"Service {service!r} is not an MTDS-family service. Supported: {sorted(MTDS_SHARD_SERVICES)}")

    bucket = build_bucket_name(service, asset_group, project_id)
    try:
        df = read_availability_index(bucket)
    except (OSError, RuntimeError, ValueError) as exc:
        logger.warning("MTDS catalog export: manifest read failed for %s: %s", bucket, exc)
        return "", 0, _shard_csv_filename(service, asset_group, venue, date)

    if df.empty or "date" not in df.columns:
        return "", 0, _shard_csv_filename(service, asset_group, venue, date)

    mask = df["date"] == date
    if "venue" in df.columns:
        mask = mask & (df["venue"] == venue)
    scoped = df.loc[mask].copy()

    if scoped.empty:
        return "", 0, _shard_csv_filename(service, asset_group, venue, date)

    # Keep only the catalog columns that exist; fill missing ones with "".
    out_cols = [c for c in _MTDS_CATALOG_COLUMNS if c in scoped.columns]
    scoped = scoped[out_cols]

    sort_cols = [c for c in ("instrument_type", "data_type", "instrument_id") if c in scoped.columns]
    if sort_cols:
        scoped = scoped.sort_values(sort_cols)

    csv_text: str = scoped.to_csv(index=False)  # pyright: ignore[reportUnknownMemberType]
    return csv_text, len(scoped), _shard_csv_filename(service, asset_group, venue, date)

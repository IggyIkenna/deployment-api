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
import time
from typing import cast

import pandas as pd
from unified_api_contracts import SchemaContract, SchemaContractNotFoundError, lookup_contract
from unified_api_contracts.internal.schemas.contracts import (
    VENUE_CONTRACT_OVERRIDES,
)
from unified_trading_library import read_availability_index

from deployment_api.settings import gcp_project_id as _pid
from deployment_api.utils.storage_facade import list_objects

logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Bucket naming (mirrors DataStatusService._BUCKET_TEMPLATES without the
# circular dependency of importing the big service).
# ---------------------------------------------------------------------------

_BUCKET_TEMPLATES: dict[str, str] = {
    "instruments-service": "instruments-store-{cat}-{pid}",
    "corporate-actions": "instruments-store-{cat}-{pid}",
    "market-tick-data-service": "market-data-tick-{cat}-{pid}",
    "market-data-processing-service": "market-data-tick-{cat}-{pid}",
    "features-delta-one-service": "features-delta-one-{cat}-{pid}",
    "features-volatility-service": "features-volatility-{cat}-{pid}",
    "features-onchain-service": "features-onchain-{pid}",
    "features-sports-service": "features-sports-{pid}",
    "features-calendar-service": "features-calendar-{pid}",
    "features-multi-timeframe-service": "features-multi-timeframe-{cat}-{pid}",
    "features-cross-instrument-service": "features-cross-instrument-{cat}-{pid}",
    "features-commodity-service": "features-commodity-{pid}",
    "ml-training-service": "ml-models-store-{pid}",
    "ml-inference-service": "ml-predictions-{pid}",
    "strategy-service": "strategy-store-{pid}",
    "execution-service": "execution-store-{pid}",
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


def build_bucket_name(service: str, category: str, project_id: str | None = None) -> str:
    """Resolve the GCS bucket for a (service, category) pair."""
    pid = project_id or _pid
    template = _BUCKET_TEMPLATES.get(service)
    if template is None:
        raise ValueError(f"Unknown service: {service}")
    return template.format(cat=category.lower(), pid=pid)


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
        scoped = scoped.sort_values("written_at").drop_duplicates(
            subset=["instrument_id"], keep="last"
        )
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


def get_schema_for_shard(
    *,
    category: str,
    instrument_type: str,
    data_type: str,
    venue: str | None = None,
) -> dict[str, object]:
    """Return the SchemaContract columns for a shard tuple.

    Falls back gracefully when no contract is registered — returns an empty
    column list with ``registered: False`` so the UI can render a
    "no schema registered — running raw projection" affordance instead of
    raising.
    """
    # Normalise UI inputs so the lookup hits the UAC registry keys
    # (lowercase snake_case). The UI passes ``POOL`` / ``POOL_DEFINITION``
    # from the manifest; UAC keys those as ``pool`` / ``dex_pool_state``.
    cat_norm = category.lower()
    it_norm = _normalise_instrument_type(instrument_type)
    dt_norm = _normalise_data_type(data_type)
    # Venue override takes priority, else base registry, else fallback.
    try:
        contract = lookup_contract(
            category=cat_norm,
            instrument_type=it_norm,
            data_type=dt_norm,
            venue=venue,
        )
    except SchemaContractNotFoundError:
        return {
            "registered": False,
            "category": cat_norm,
            "instrument_type": it_norm,
            "data_type": dt_norm,
            "venue": venue,
            "symbol_column": None,
            "source": "none",
            "columns": [],
            "message": (
                "No contract registered for this shard. "
                "The UI should fall back to projecting actual parquet columns."
            ),
        }

    # Figure out whether we resolved via override or base registry.
    source = "CONTRACT_REGISTRY"
    if venue is not None:
        override_key = (cat_norm, (venue or "").upper(), it_norm, dt_norm)
        if override_key in VENUE_CONTRACT_OVERRIDES:
            source = "VENUE_CONTRACT_OVERRIDES"

    return {
        "registered": True,
        "category": contract.category,
        "instrument_type": contract.instrument_type,
        "data_type": contract.data_type,
        "venue": (venue or "").upper() if venue else None,
        "symbol_column": contract.symbol_column,
        "source": source,
        "columns": _column_dicts(contract),
        "required_row_count_min": contract.required_row_count_min,
    }


# ---------------------------------------------------------------------------
# Instruments for a given (day, venue, instrument_type, data_type) shard
# ---------------------------------------------------------------------------


# Services that bundle every instrument for a (venue, day) pair into one
# parquet. The drill-down reads the parquet itself to surface the
# instrument_ids because the file layout carries no per-symbol partitioning.
_PER_VENUE_DAY_BUNDLE_SERVICES: frozenset[str] = frozenset(
    {"instruments-service", "corporate-actions"}
)


# Symbol column per service for the bundled-parquet case. instruments-service
# stores the canonical identifier as ``instrument_key``.
_SERVICE_BUNDLE_SYMBOL_COLUMN: dict[str, str] = {
    "instruments-service": "instrument_key",
    "corporate-actions": "instrument_key",
}


def _shard_prefix(
    service: str, category: str, venue: str, day: str, instrument_type: str, data_type: str
) -> str:
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
        if category.lower() == "sports":
            # Sports groups by league inside the per-day listing; the UI
            # passes the league label through ``instrument_type`` because
            # that is the axis the manifest uses upstream. A dedicated
            # league kwarg can be added when the UI learns to send one.
            league = instrument_type or ""
            return f"instrument_availability/by_date/day={day}/league={league}/venue={venue}/"
        return f"instrument_availability/by_date/day={day}/venue={venue}/"

    if svc in {"market-tick-data-service", "market-data-processing-service"}:
        return (
            f"raw_tick_data/by_date/day={day}/category={category.lower()}/"
            f"venue={venue}/instrument_type={instrument_type.lower()}/"
            f"data_type={data_type.lower()}/"
        )

    return (
        f"raw_tick_data/by_date/day={day}/category={category.lower()}/"
        f"venue={venue}/instrument_type={instrument_type}/data_type={data_type}/"
    )


def _infer_symbol_column_for_shard(
    category: str, instrument_type: str, data_type: str, venue: str
) -> str:
    """Best-effort symbol column when no contract is registered.

    Falls back to ``instrument_id`` (the canonical column since Phase 1.2);
    per-venue conventions (pool_address for UNISWAP_V3 etc.) are picked up
    automatically via ``lookup_contract`` when the contract exists.
    """
    try:
        contract = lookup_contract(
            category=category.lower(),
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
                "file_uri": f"gs://{bucket}/{name}",
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
    category: str,
    instrument_type: str,
    data_type: str,
    venue: str,
) -> list[dict[str, object]]:
    if not parquet_files:
        return []
    pf = parquet_files[0]
    symbol_col = _infer_symbol_column_for_shard(category, instrument_type, data_type, venue)
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
        filtered = [
            inst for inst in instruments if needle in str(inst.get("instrument_id", "")).lower()
        ][:MAX_SEARCH_RESULTS]

    total = len(filtered)
    start = max(0, offset)
    end = start + max(1, limit)
    page = filtered[start:end]
    return page, total


def _list_instruments_full(
    *,
    service: str,
    category: str,
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
    cache_key = f"instruments:{service}:{category}:{venue}:{day}:{instrument_type}:{data_type}"
    cached = _cache_get(cache_key)
    if cached is not None:
        return cast_dict(cached)

    bucket = build_bucket_name(service, category, project_id)
    prefix = _shard_prefix(service, category, venue, day, instrument_type, data_type)
    parquet_files = _collect_parquet_files(bucket, prefix)
    bundling = _bundling_mode(venue, instrument_type, service)

    if bundling == "per_condition_id":
        instruments = _expand_per_condition_id(
            parquet_files, category, instrument_type, data_type, venue
        )
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
        "category": category.lower(),
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
    category: str,
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
        category=category,
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
        "category": full.get("category", category.lower()),
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
    category: str,
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
        category=category,
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
    symbol_col = _infer_symbol_column_for_shard(category, instrument_type, data_type, venue)
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
    category: str,
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
    cache_key = f"shard_info:{service}:{category}:{venue}:{day}:{data_type}"
    cached = _cache_get(cache_key)
    if isinstance(cached, dict):
        return cast_dict(cast(dict[str, object], cached))

    bucket = build_bucket_name(service, category, project_id)
    venue_prefix = f"raw_tick_data/by_date/day={day}/category={category.lower()}/venue={venue}/"
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
        "category": category.lower(),
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
        return obj  # type: ignore[return-value]
    raise TypeError(f"Expected dict, got {type(obj).__name__}")


# ---------------------------------------------------------------------------
# Bucket counts (named markets vs conditionIds inside OTHER)
# ---------------------------------------------------------------------------


def compute_bucket_counts(
    *,
    service: str,
    category: str,
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
    cache_key = f"bucket_counts:{service}:{category}:{venue}:{day}:{data_type}"
    cached = _cache_get(cache_key)
    if isinstance(cached, dict):
        cached_typed: dict[str, int] = {}
        for raw_k, raw_v in cached.items():  # pyright: ignore[reportUnknownVariableType]
            key_str = str(raw_k)  # pyright: ignore[reportUnknownArgumentType]
            val_int = int(raw_v) if isinstance(raw_v, (int, float, str)) else 0
            cached_typed[key_str] = val_int
        return cached_typed

    bucket = build_bucket_name(service, category, project_id)
    venue_prefix = f"raw_tick_data/by_date/day={day}/category={category.lower()}/venue={venue}/"
    instrument_types = _collect_instrument_types(bucket, venue_prefix)
    named = sum(1 for it in instrument_types if it.upper() != "OTHER")

    other_count = 0
    if any(it.upper() == "OTHER" for it in instrument_types):
        other_count = _count_distinct_in_other_bucket(
            bucket, venue_prefix, category, venue, data_type
        )

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
    bucket: str, venue_prefix: str, category: str, venue: str, data_type: str
) -> int:
    """Read the first OTHER-bucket parquet and return the distinct-symbol count."""
    symbol_col = _infer_symbol_column_for_shard(category, "OTHER", data_type, venue)
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
            return len(_distinct_values_in_parquet(f"gs://{bucket}/{name}", symbol_col))
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
        raise ValueError(f"Not a gs:// URI: {gs_uri}")
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
    category: str,
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
        category=category,
        venue=venue,
        day=day,
        instrument_type=instrument_type,
        data_type=data_type,
        project_id=project_id,
    )
    bundling = str(listing["bundling"])
    raw_instruments_obj: object = listing["instruments"]
    raw_list = cast(
        list[object], raw_instruments_obj if isinstance(raw_instruments_obj, list) else []
    )
    all_instruments: list[dict[str, object]] = []
    for i in raw_list:
        if isinstance(i, dict):
            all_instruments.append(cast_dict(cast(dict[str, object], i)))

    selected = set(instrument_ids) if instrument_ids else None

    frames: list[pd.DataFrame] = []

    if bundling == "per_condition_id" and all_instruments:
        # Single bundle parquet, filter by symbol column.
        pf_uri = str(all_instruments[0]["file_uri"])
        symbol_col = _infer_symbol_column_for_shard(category, instrument_type, data_type, venue)
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
        raise ValueError(
            f"League {league_id} has no api_football_id — not sourced from API-Football"
        )
    af_id = int(league.api_football_id)

    pid = project_id or _pid
    gs_uri = f"gs://instruments-store-sports-{pid}/sports_reference/by_date/day={day}/entity=fixtures/fixtures.parquet"

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

    # af_league_id column may be object/str/int — coerce at the boundary
    # via pandas.to_numeric (keeps basedpyright strict-mode happy — the
    # lambda+apply path surfaces reportUnknownLambdaType for the cell type).
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

# Columns we strictly need from the master fixtures parquet for breakdown.
# Keep the projection narrow so gcsfs read cost stays bounded.
_FIXTURE_META_COLUMNS: list[str] = [
    "fixture_id",
    "af_league_id",
    "kickoff_utc",
    "home_team_name",
    "away_team_name",
    "status",
    "venue_id",
]


def _entity_gs_uri(
    *, day: str, path_suffix: str, filename: str, project_id: str | None = None
) -> str:
    pid = project_id or _pid
    return (
        f"gs://instruments-store-sports-{pid}/sports_reference/"
        f"by_date/day={day}/{path_suffix}/{filename}"
    )


def _read_entity_fixture_ids(gs_uri: str) -> tuple[str, set[str]]:
    """Return ``(capture_status, fixture_ids)`` for one per-day entity parquet.

    ``capture_status`` is one of ``"captured"`` / ``"empty_confirmed"`` /
    ``"attempted_failed"``. When ``"attempted_failed"`` the returned set is
    empty; callers should fall through to per-fixture ``"missing"`` /
    ``"attempted_failed"`` classification.
    """
    try:
        df = _read_parquet_columns(gs_uri, ["fixture_id"])
    except (OSError, FileNotFoundError):
        return ("attempted_failed", set())
    except (ValueError, RuntimeError) as exc:
        logger.warning("per-fixture entity read failed for %s: %s", gs_uri, exc)
        return ("attempted_failed", set())

    if "fixture_id" not in df.columns or df.empty:
        return ("empty_confirmed", set())

    raw_series = df["fixture_id"].dropna()
    raw_values: object = raw_series.unique().tolist()  # pyright: ignore[reportUnknownMemberType]
    values_list: list[object] = cast(list[object], raw_values)
    fixture_ids: set[str] = set()
    for v in values_list:
        s = str(v).strip()
        if s:
            fixture_ids.add(s)
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
        raise ValueError(
            f"League {league_id} has no api_football_id — not sourced from API-Football"
        )
    af_id = int(league.api_football_id)

    gs_uri = _entity_gs_uri(
        day=day,
        path_suffix="entity=fixtures",
        filename="fixtures.parquet",
        project_id=project_id,
    )
    df = _read_parquet_columns(gs_uri, _FIXTURE_META_COLUMNS)

    if "af_league_id" not in df.columns or df.empty:
        return ([], af_id)

    af_series = pd.to_numeric(df["af_league_id"], errors="coerce")
    filtered = df[af_series == af_id]
    fixtures: list[dict[str, object]] = []
    for row in filtered.to_dict(orient="records"):  # pyright: ignore[reportUnknownMemberType,reportUnknownVariableType]
        fixture_id = str(row.get("fixture_id") or "").strip()  # pyright: ignore[reportUnknownMemberType,reportUnknownArgumentType]
        if not fixture_id:
            continue
        fixtures.append(
            {
                "fixture_id": fixture_id,
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
        fixtures_meta, af_id = _load_fixture_meta(
            day=day, league_id=league_id, project_id=project_id
        )
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
        gs_uri = _entity_gs_uri(
            day=day, path_suffix=path_suffix, filename=filename, project_id=project_id
        )
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


def _filter_entity_rows_for_fixture(
    gs_uri: str, fixture_id: str
) -> tuple[str, pd.DataFrame | None]:
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

    if "fixture_id" not in df.columns or df.empty:
        return ("empty_confirmed", None)

    fid_series = df["fixture_id"].astype(str)
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
    total_captured_rows = sum(
        (0 if rows is None else len(rows)) for _status, rows in per_entity.values()
    )

    if total_captured_rows == 0:
        # Nothing found for this fixture across any entity — surface a 404
        # at the HTTP layer.
        raise FileNotFoundError(
            f"fixture_id {fixture_id!r} not found in any entity for day {day_resolved}"
        )

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

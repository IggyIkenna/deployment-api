"""Per-day instrument listing, bundle preview, shard info + bucket counts.

Split from ``services/data_status_drilldown.py`` (pure code motion; plan:
``codex_violations_ratchet_to_five_2026_06_10.md`` Phase-1 P2). Patched
module-level collaborators (``list_objects`` / ``read_availability_index`` /
``resolve_bucket_name`` / ``build_bucket_name`` / the parquet readers) are
resolved through the package facade (``_dd``) at call time so the existing
test patch surface ``deployment_api.services.data_status_drilldown.<name>``
keeps intercepting.
"""

from __future__ import annotations

import logging
from typing import cast

from unified_api_contracts import SchemaContractNotFoundError, lookup_contract

import deployment_api.services.data_status_drilldown as _dd
from deployment_api.services.data_status_drilldown._core import (
    _attach_capture_status_to_instruments,  # pyright: ignore[reportPrivateUsage]
    _cache_get,  # pyright: ignore[reportPrivateUsage]
    _cache_put,  # pyright: ignore[reportPrivateUsage]
    _is_bundled,  # pyright: ignore[reportPrivateUsage]
)

logger = logging.getLogger(__name__)

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
      ``raw_tick_data/by_date/day=.../asset_group=.../venue=.../instrument_type=<lower>/data_type=<lower>/``
      (v9 canonical; ``storage_facade.list_objects`` transparently also probes the legacy ``category=`` prefix).
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
            f"raw_tick_data/by_date/day={day}/asset_group={asset_group.lower()}/"
            f"venue={venue}/instrument_type={instrument_type.lower()}/"
            f"data_type={data_type.lower()}/"
        )

    return (
        f"raw_tick_data/by_date/day={day}/asset_group={asset_group.lower()}/"
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
        objects = _dd.list_objects(bucket, prefix, max_results=10_000)
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
        distinct_ids = _dd._distinct_values_in_parquet(str(pf["file_uri"]), symbol_col)  # pyright: ignore[reportPrivateUsage]
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
        df = _dd._read_parquet_columns(uri, None)  # pyright: ignore[reportPrivateUsage]
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
    for v in df[symbol_col].dropna().tolist():  # pyright: ignore[reportUnknownVariableType,reportUnknownMemberType,reportAny]
        sid = str(v).strip()  # pyright: ignore[reportAny]
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

    bucket = _dd.build_bucket_name(service, category, project_id)
    prefix = _shard_prefix(service, category, venue, day, instrument_type, data_type)
    parquet_files = _collect_parquet_files(bucket, prefix)
    bundling = _bundling_mode(venue, instrument_type, service)

    if bundling == "per_condition_id":
        instruments = _expand_per_condition_id(parquet_files, category, instrument_type, data_type, venue)
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
    category = asset_group
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
    category = asset_group
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
        symbols = _dd._distinct_values_in_parquet(uri, symbol_col)[:limit]  # pyright: ignore[reportPrivateUsage]
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
    category = asset_group
    cache_key = f"shard_info:{service}:{category}:{venue}:{day}:{data_type}"
    cached = _cache_get(cache_key)
    if isinstance(cached, dict):
        return cast_dict(cast(dict[str, object], cached))

    bucket = _dd.build_bucket_name(service, category, project_id)
    venue_prefix = f"raw_tick_data/by_date/day={day}/asset_group={category.lower()}/venue={venue}/"
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
        return obj  # pyright: ignore[reportReturnType,reportUnknownVariableType]
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
    category = asset_group
    cache_key = f"bucket_counts:{service}:{category}:{venue}:{day}:{data_type}"
    cached = _cache_get(cache_key)
    if isinstance(cached, dict):
        cached_typed: dict[str, int] = {}
        for raw_k, raw_v in cached.items():  # pyright: ignore[reportUnknownVariableType]
            key_str = str(raw_k)  # pyright: ignore[reportUnknownArgumentType]
            val_int = int(raw_v) if isinstance(raw_v, (int, float, str)) else 0
            cached_typed[key_str] = val_int
        return cached_typed

    bucket = _dd.build_bucket_name(service, category, project_id)
    venue_prefix = f"raw_tick_data/by_date/day={day}/asset_group={category.lower()}/venue={venue}/"
    instrument_types = _collect_instrument_types(bucket, venue_prefix)
    named = sum(1 for it in instrument_types if it.upper() != "OTHER")

    other_count = 0
    if any(it.upper() == "OTHER" for it in instrument_types):
        other_count = _count_distinct_in_other_bucket(bucket, venue_prefix, category, venue, data_type)

    result = {"named_market_count": named, "other_market_count": other_count}
    _cache_put(cache_key, result)
    return result


def _collect_instrument_types(bucket: str, venue_prefix: str) -> set[str]:
    """Parse distinct instrument_type values from all object paths under the venue."""
    try:
        objects = _dd.list_objects(bucket, venue_prefix, max_results=10_000)
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


def _count_distinct_in_other_bucket(bucket: str, venue_prefix: str, category: str, venue: str, data_type: str) -> int:
    """Read the first OTHER-bucket parquet and return the distinct-symbol count."""
    symbol_col = _infer_symbol_column_for_shard(category, "OTHER", data_type, venue)
    other_prefix = f"{venue_prefix}instrument_type=OTHER/data_type={data_type}/"
    try:
        other_objects = _dd.list_objects(bucket, other_prefix, max_results=10)
    except (OSError, RuntimeError) as exc:
        logger.warning("list_objects OTHER failed: %s", exc)
        return 0
    for o in other_objects:
        name = getattr(o, "name", None)
        if not isinstance(name, str) or not name.endswith(".parquet"):
            continue
        try:
            return len(_dd._distinct_values_in_parquet(f"gs://{bucket}/{name}", symbol_col))  # noqa: gs-uri  — URI composer, bucket already resolved  # pyright: ignore[reportPrivateUsage]
        except (OSError, ValueError, RuntimeError) as exc:
            logger.warning("Failed to read OTHER parquet %s: %s", name, exc)
    return 0


# ---------------------------------------------------------------------------
# Parquet helpers (distinct symbol values, CSV export)
# ---------------------------------------------------------------------------

# Max rows we will export as CSV in a single response. Larger requests get
# rejected with a 413-equivalent error advising BigQuery external tables.

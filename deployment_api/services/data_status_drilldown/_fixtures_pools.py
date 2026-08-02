"""Sports fixture breakdown/download + DeFi pool breakdown drill-downs.

Split from ``services/data_status_drilldown.py`` (pure code motion; plan:
``codex_violations_ratchet_to_five_2026_06_10.md`` Phase-1 P2). Patched
module-level collaborators (``list_objects`` / ``read_availability_index`` /
``resolve_bucket_name`` / ``build_bucket_name`` / the parquet readers) are
resolved through the package facade (``_dd``) at call time so the existing
test patch surface ``deployment_api.services.data_status_drilldown.<name>``
keeps intercepting.
"""

from __future__ import annotations

import json
import logging
import re as _re
from typing import cast

import pandas as pd

import deployment_api.services.data_status_drilldown as _dd
from deployment_api.services.data_status_drilldown._csv_export import (
    _pick_column,  # pyright: ignore[reportPrivateUsage]
)
from deployment_api.services.data_status_drilldown._instruments import DEFAULT_INSTRUMENT_LIMIT

logger = logging.getLogger(__name__)

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
    _sports_bucket = _dd.resolve_bucket_name(cloud="gcp", kind="instruments-store", asset_group="sports")
    return f"gs://{_sports_bucket}/sports_reference/by_date/day={day}/{path_suffix}/{filename}"  # noqa: gs-uri  — URI composer, bucket resolved via resolve_bucket_name


def _probe_fid_column(gs_uri: str) -> tuple[str, str | None]:
    """Probe the parquet schema and pick the canonical fixture-id column.

    Returns ``(status, fid_col)`` where ``status`` is one of ``"ok"`` /
    ``"empty_confirmed"`` / ``"attempted_failed"``. ``fid_col`` is ``None``
    on any non-``"ok"`` status.
    """
    try:
        schema_names = _dd._parquet_schema_names(gs_uri)  # pyright: ignore[reportPrivateUsage]
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
        df = _dd._read_parquet_columns(gs_uri, [fid_col])  # pyright: ignore[reportPrivateUsage]
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


def _read_split_fixture_meta_frame(*, day: str, project_id: str | None = None) -> pd.DataFrame | None:
    """Fall back to the split ``fixtures_schedule`` per-league shards for one day.

    instruments-service cut the FIXTURES writer over to the schedule/outcomes
    entity-folder split with no legacy dual-write (first observed
    2026-07-14) — see
    ``plans/active/issues/features_sports_fixtures_split_reader_gap_2026_07_15.md``.
    The fixture-breakdown meta (kickoff/teams/status/venue) only needs
    schedule columns, all present on ``fixtures_schedule``. Returns ``None``
    when no split shard exists for the day either (a genuine gap).
    """
    _sports_bucket = _dd.resolve_bucket_name(cloud="gcp", kind="instruments-store", asset_group="sports")
    blob_paths = _dd.split_entity_league_blob_paths(_sports_bucket, day, "fixtures_schedule")
    if not blob_paths:
        return None
    frames = [_dd._read_parquet_columns(f"gs://{_sports_bucket}/{p}") for p in blob_paths]  # noqa: gs-uri  — URI composer, bucket already resolved  # pyright: ignore[reportPrivateUsage]
    df = pd.concat(frames, ignore_index=True, sort=False)
    return df if not df.empty else None


def _load_fixture_meta(
    *,
    day: str,
    league_id: str,
    project_id: str | None = None,
) -> tuple[list[dict[str, object]], int | None]:
    """Return ``(fixtures_for_league, af_league_id_or_None)``.

    Each fixture dict has: fixture_id, kickoff_utc, home_team_name,
    away_team_name, status, venue_id.

    Raises ``FileNotFoundError`` when neither the legacy singleton nor the
    split ``fixtures_schedule`` shards exist for the day; ``ValueError`` when
    the canonical league_id can't be mapped to an API-Football numeric id.
    """
    from unified_api_contracts.sports import get_league  # noqa: imports-inside-functions

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

    # instruments-service cut the FIXTURES writer over to the fixtures_schedule/
    # fixtures_outcomes split with no legacy dual-write (2026-07-14+) — fall back
    # to the split per-league shards when the legacy singleton is absent.
    split_frame: pd.DataFrame | None = None
    try:
        schema_names = _dd._parquet_schema_names(gs_uri)  # pyright: ignore[reportPrivateUsage]
    except (OSError, FileNotFoundError):
        split_frame = _read_split_fixture_meta_frame(day=day, project_id=project_id)
        if split_frame is None:
            raise
        schema_names = set(split_frame.columns)

    # Resolve each logical field to whichever schema variant is present.
    resolved: dict[str, str] = {}
    for canonical, variants in _FIXTURE_META_ALIASES.items():
        picked = _pick_column(variants, schema_names)
        if picked is not None:
            resolved[canonical] = picked
    proj_cols = list(resolved.values())
    if not proj_cols:
        return ([], af_id)
    df = split_frame[proj_cols] if split_frame is not None else _dd._read_parquet_columns(gs_uri, proj_cols)  # pyright: ignore[reportPrivateUsage]
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
    return _dd.resolve_bucket_name(cloud="gcp", kind="market-data", asset_group="defi")


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
    Also tries underscore variants per UAC↔GCS naming drift (AAVE_V3 vs AAVE_V3).
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
    # Canonical-only: emit the ``asset_group=defi/`` shape. ``storage_facade.list_objects``
    # fans this out across the canonical ``pipeline_mode={mode}_{source}/`` layers and
    # never reads the legacy bare / ``category=`` twins — so no double-count. (The
    # explicit ``category=`` candidate loop was removed: the migration copied every
    # legacy twin to the canonical shape, so canonical-only never misses a cell.)
    candidates: list[str] = []
    for alias in _defi_venue_chain_aliases(venue_chain):
        protocol_only = alias.split("-")[0]
        # Combined-partition layout (single-chain protocols)
        candidates.append(f"raw_tick_data/by_date/day={day}/asset_group=defi/venue={alias}/")
        # Separated-partition layout (multi-chain protocols) — only makes sense
        # when we have a chain part to put under chain=.
        if chain_part:
            candidates.append(
                f"raw_tick_data/by_date/day={day}/asset_group=defi/venue={protocol_only}/chain={chain_part}/"
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
    writer drops the underscore (``AAVE_V3-ETHEREUM``). Tries the literal
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
        if not obj.name.endswith(".parquet"):  # pyright: ignore[reportUnknownMemberType,reportAttributeAccessIssue]
            continue
        instrument_type = ""
        data_type = ""
        for token in obj.name.split("/"):  # pyright: ignore[reportUnknownMemberType,reportAttributeAccessIssue,reportUnknownVariableType]
            if token.startswith("instrument_type="):  # pyright: ignore[reportUnknownMemberType]
                instrument_type = token[len("instrument_type=") :]  # pyright: ignore[reportUnknownVariableType]
            elif token.startswith("data_type="):  # pyright: ignore[reportUnknownMemberType]
                data_type = token[len("data_type=") :]  # pyright: ignore[reportUnknownVariableType]
        if not instrument_type or not data_type:
            continue
        key = (instrument_type, data_type)  # pyright: ignore[reportUnknownVariableType]
        if key in seen:
            continue
        seen.add(key)  # pyright: ignore[reportUnknownArgumentType]
        gs_uri = f"gs://{bucket}/{obj.name}"  # noqa: gs-uri  — URI composer, bucket already resolved  # pyright: ignore[reportUnknownMemberType,reportAttributeAccessIssue]
        out.append((instrument_type, data_type, gs_uri))  # pyright: ignore[reportUnknownArgumentType]
    return out


def _read_pool_ids_from_parquet(gs_uri: str) -> tuple[str, set[str]]:
    """Return ``(capture_status, pool_ids)`` for one DeFi entity parquet.

    Tries each column in :data:`_DEFI_POOL_ID_CANDIDATE_COLS` in order until it
    finds one present in the parquet schema. Empty set + ``"empty_confirmed"``
    when the parquet exists but has no canonical-ID column or no rows;
    ``"attempted_failed"`` on any read error.
    """
    try:
        schema_names = _dd._parquet_schema_names(gs_uri)  # pyright: ignore[reportPrivateUsage]
    except (OSError, FileNotFoundError):
        return ("attempted_failed", set())
    except (ValueError, RuntimeError) as exc:
        logger.warning("pool breakdown schema probe failed for %s: %s", gs_uri, exc)
        return ("attempted_failed", set())

    pool_id_col = _pick_column(list(_DEFI_POOL_ID_CANDIDATE_COLS), schema_names)
    if pool_id_col is None:
        return ("empty_confirmed", set())

    try:
        df = _dd._read_parquet_columns(gs_uri, [pool_id_col])  # pyright: ignore[reportPrivateUsage]
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
    entities = _dd._list_pool_entities_for_venue(day=day, venue_chain=venue_chain, project_id=project_id)  # pyright: ignore[reportPrivateUsage]
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
        status, pool_ids = _dd._read_pool_ids_from_parquet(gs_uri)  # pyright: ignore[reportPrivateUsage]
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
        df = _dd._read_parquet_columns(gs_uri, None)  # pyright: ignore[reportPrivateUsage]
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

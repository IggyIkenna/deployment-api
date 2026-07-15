"""Parquet read/select utilities + instrument / fixture / shard CSV exports.

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

import pandas as pd

import deployment_api.services.data_status_drilldown as _dd
from deployment_api.services.data_status_drilldown._instruments import (
    _PER_VENUE_DAY_BUNDLE_SERVICES,  # pyright: ignore[reportPrivateUsage]
    _infer_symbol_column_for_shard,  # pyright: ignore[reportPrivateUsage]
    _list_instruments_full,  # pyright: ignore[reportPrivateUsage]
    cast_dict,
)
from deployment_api.settings import gcp_project_id as _pid

logger = logging.getLogger(__name__)

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

    if not gs_uri.startswith("gs://"):  # noqa: gs-uri (parsing a caller-supplied URI, not constructing one)
        raise ValueError(f"Not a gs:// URI: {gs_uri}")  # noqa: gs-uri  — error message string, not a URI constructor
    bucket_key = gs_uri[len("gs://") :]  # noqa: gs-uri (parsing a caller-supplied URI, not constructing one)
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

    if not gs_uri.startswith("gs://"):  # noqa: gs-uri (parsing a caller-supplied URI, not constructing one)
        raise ValueError(f"Not a gs:// URI: {gs_uri}")  # noqa: gs-uri  — error message string, not a URI constructor
    bucket_key = gs_uri[len("gs://") :]  # noqa: gs-uri (parsing a caller-supplied URI, not constructing one)
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
    df = _dd._read_parquet_columns(gs_uri, [column])  # pyright: ignore[reportPrivateUsage]
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
    category = asset_group
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
        symbol_col = _infer_symbol_column_for_shard(category, instrument_type, data_type, venue)
        df = _dd._read_parquet_columns(pf_uri)  # full parquet  # pyright: ignore[reportPrivateUsage]
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
                df = _dd._read_parquet_columns(uri)  # pyright: ignore[reportPrivateUsage]
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


def _read_split_fixtures_frame(bucket: str, day: str) -> pd.DataFrame | None:
    """Read + left-join the split ``fixtures_schedule``/``fixtures_outcomes`` shards for one day.

    instruments-service cut the FIXTURES writer over to the schedule/outcomes
    entity-folder split with no legacy dual-write (first observed 2026-07-14)
    — see ``plans/active/issues/features_sports_fixtures_split_reader_gap_2026_07_15.md``.
    Both split entities keep the writer's original raw column names unchanged
    (the split only partitions columns across two files, it does not rename
    them), so a left-join of outcomes onto schedule on ``af_fixture_id``
    reproduces the same shape the legacy singleton read returned. Returns
    ``None`` when the schedule shard itself is absent (a genuine gap).
    """
    schedule_paths = _dd.split_entity_league_blob_paths(bucket, day, "fixtures_schedule")
    if not schedule_paths:
        return None
    schedule_frames = [_dd._read_parquet_columns(f"gs://{bucket}/{p}") for p in schedule_paths]  # noqa: gs-uri  — URI composer, bucket already resolved  # pyright: ignore[reportPrivateUsage]
    schedule_df = pd.concat(schedule_frames, ignore_index=True, sort=False)
    if schedule_df.empty or "af_fixture_id" not in schedule_df.columns:
        return schedule_df

    outcomes_paths = _dd.split_entity_league_blob_paths(bucket, day, "fixtures_outcomes")
    if not outcomes_paths:
        return schedule_df
    outcomes_frames = [_dd._read_parquet_columns(f"gs://{bucket}/{p}") for p in outcomes_paths]  # noqa: gs-uri  — URI composer, bucket already resolved  # pyright: ignore[reportPrivateUsage]
    outcomes_df = pd.concat(outcomes_frames, ignore_index=True, sort=False)
    if outcomes_df.empty or "af_fixture_id" not in outcomes_df.columns:
        return schedule_df
    outcome_only_cols = [c for c in outcomes_df.columns if c == "af_fixture_id" or c not in schedule_df.columns]
    return schedule_df.merge(outcomes_df[outcome_only_cols], on="af_fixture_id", how="left")


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
    API-Football numeric via UAC, filters, and returns CSV. For dates
    on/after the fixtures-schedule/outcomes entity-folder split cutover
    (first observed 2026-07-14, no legacy dual-write), the singleton is
    absent — :func:`_read_split_fixtures_frame` reads the split per-league
    shards instead.

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

    _sports_bucket = _dd.resolve_bucket_name(cloud="gcp", kind="instruments-store", asset_group="sports")
    gs_uri = f"gs://{_sports_bucket}/sports_reference/by_date/day={day}/entity=fixtures/fixtures.parquet"  # noqa: gs-uri  — URI composer, bucket resolved via resolve_bucket_name

    try:
        df = _dd._read_parquet_columns(gs_uri)  # pyright: ignore[reportPrivateUsage]
    except (OSError, FileNotFoundError):
        split_df = _read_split_fixtures_frame(_sports_bucket, day)
        if split_df is None:
            # Adapter never ran for this (day, league) — return empty CSV with a
            # 0 row count so the caller can surface a "no data" hint to the user
            # instead of a download failure. The HTTP route sets X-Data-Status
            # so the UI can still distinguish "empty confirmed" from "never ran".
            return "", 0, _fixtures_csv_filename(day, league_id)
        df = split_df

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
    chain: str | None = None,
    project_id: str | None = None,
    max_rows: int = MAX_SHARD_CSV_ROWS,
) -> tuple[str, int, str]:
    """Return ``(csv_text, row_count, filename)`` for one (venue, date) instruments shard.

    instruments-service writes one parquet per ``(venue, day)`` at
    ``instrument_availability/by_date/day={date}/venue={venue}/instruments.parquet``.
    For DeFi, ``venue`` is the combined ``{protocol}-{chain}`` token (e.g.
    ``AAVE_V3-ARBITRUM``); pass the raw protocol as ``venue`` and the chain as
    ``chain`` and this function assembles the combined key automatically.

    Only valid for services in ``_PER_VENUE_DAY_BUNDLE_SERVICES``
    (instruments-service, corporate-actions).

    Raises:
        ValueError: unsupported service, or row count exceeds ``max_rows``.
        FileNotFoundError: parquet does not exist (adapter never ran that day).
    """
    category = asset_group
    svc = service.lower()
    if svc not in _PER_VENUE_DAY_BUNDLE_SERVICES:
        raise ValueError(
            f"Service {service!r} does not use per-venue-day-bundle sharding. "
            f"Supported: {sorted(_PER_VENUE_DAY_BUNDLE_SERVICES)}"
        )

    venue_key = f"{venue}-{chain}" if category.lower() == "defi" and chain else venue
    bucket = _dd.build_bucket_name(service, category, project_id)
    gs_uri = f"gs://{bucket}/instrument_availability/by_date/day={date}/venue={venue_key}/instruments.parquet"  # noqa: gs-uri  — URI composer, bucket already resolved

    try:
        df = _dd._read_parquet_columns(gs_uri)  # pyright: ignore[reportPrivateUsage]
    except (OSError, FileNotFoundError):
        return "", 0, _shard_csv_filename(service, category, venue, date)

    if len(df) > max_rows:
        raise ValueError(
            f"Shard CSV export would include {len(df):,} rows (> {max_rows:,}). "
            "Use a BigQuery external table over the parquet files instead."
        )

    csv_text = df.to_csv(index=False)
    return csv_text, len(df), _shard_csv_filename(service, category, venue, date)


def _shard_csv_filename(service: str, category: str, venue: str, date: str) -> str:
    return f"{service}_{category}_{venue}_{date}.csv"


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
    category: str,
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

    bucket = _dd.build_bucket_name(service, category, project_id)
    try:
        df = _dd.read_availability_index(bucket)
    except (OSError, RuntimeError, ValueError) as exc:
        logger.warning("MTDS catalog export: manifest read failed for %s: %s", bucket, exc)
        return "", 0, _shard_csv_filename(service, category, venue, date)

    if df.empty or "date" not in df.columns:
        return "", 0, _shard_csv_filename(service, category, venue, date)

    mask = df["date"] == date
    if "venue" in df.columns:
        mask = mask & (df["venue"] == venue)
    scoped = df.loc[mask].copy()

    if scoped.empty:
        return "", 0, _shard_csv_filename(service, category, venue, date)

    # Keep only the catalog columns that exist; fill missing ones with "".
    out_cols = [c for c in _MTDS_CATALOG_COLUMNS if c in scoped.columns]
    scoped = scoped[out_cols]

    sort_cols = [c for c in ("instrument_type", "data_type", "instrument_id") if c in scoped.columns]
    if sort_cols:
        scoped = scoped.sort_values(sort_cols)

    csv_text: str = scoped.to_csv(index=False)  # pyright: ignore[reportUnknownMemberType]
    return csv_text, len(scoped), _shard_csv_filename(service, category, venue, date)

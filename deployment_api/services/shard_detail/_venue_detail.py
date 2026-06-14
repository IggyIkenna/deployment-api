"""Per-venue instrument-detail branch (CeFi / DeFi / prediction venue drill-down).

Split from ``services/shard_detail.py`` (pure code motion; plan:
``codex_violations_ratchet_to_five_2026_06_10.md`` Phase-1 P2). Patched
module-level collaborators are resolved through the package facade
(``_sd``) at call time so the existing test patch surface
``deployment_api.services.shard_detail.<name>`` keeps intercepting.
"""

from __future__ import annotations

import logging
import re as _re
from typing import cast

import pandas as pd

import deployment_api.services.shard_detail as _sd
from deployment_api.services.data_status_drilldown import build_bucket_name
from deployment_api.services.shard_detail._shard_core import (
    _defi_composite_parts,  # pyright: ignore[reportPrivateUsage]
)
from deployment_api.types.shard_detail import VenueDetailResponse
from deployment_api.utils.storage_facade import list_prefixes

logger = logging.getLogger(__name__)


def _instruments_bucket_for_category(category: str) -> str:
    return build_bucket_name("instruments-service", category)


# Compiled regex for stripping the underscore in DeFi protocol-version
# tokens, e.g. ``AAVE_V3-ETHEREUM`` → ``AAVEV3-ETHEREUM``.
# Instruments-service GCS partition keys historically used the no-underscore
# ghost form (``AAVEV3-ETHEREUM``). Writers were updated to canonical
# ``AAVE_V3-ETHEREUM`` in 2026-05-23, but old parquets on GCS may still live
# under ghost-name paths until a full migration completes. The alias list
# tries canonical first, then the ghost form for legacy-named GCS parquets.
_DEFI_VERSION_UNDERSCORE_RE = _re.compile(r"_V(\d+)")


def _venue_aliases_for_bucket(category: str, venue: str) -> list[str]:
    """Return the list of venue strings to try when resolving a GCS path.

    The UI / UAC and the instruments-service GCS writer disagree on a few
    naming conventions. Rather than picking one canonical form, we try the
    plausible aliases in order and use whichever the bucket actually has.

    Conventions per category:

    * **DEFI**: try the literal venue first (canonical ``AAVE_V3-ETHEREUM``),
      then the ghost-name alias without underscore (``AAVEV3-ETHEREUM``) as a
      legacy-path fallback for old GCS parquets written before 2026-05-23.
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
            df = _sd._read_parquet_columns(leaf_uri, None)  # pyright: ignore[reportPrivateUsage]
            if df is not None and not df.empty:  # pyright: ignore[reportUnnecessaryComparison]
                return df
        except (OSError, ValueError, RuntimeError) as exc:
            last_err = exc

        # Nested-partition fallback (SPORTS sub-provider layout).
        nested_prefix = f"instrument_availability/by_date/day={day}/{partition_key}={alias}/"
        try:
            nested_objs = _sd.list_objects(bucket, nested_prefix, max_results=200)
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
                    sub_df = _sd._read_parquet_columns(gs_uri, None)  # pyright: ignore[reportPrivateUsage]
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
        objects = _sd.list_objects(bucket, prefix, max_results=5_000)
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
    day = _sd._pick_latest_day(bucket, venue)  # pyright: ignore[reportPrivateUsage]
    instruments: list[dict[str, object]] = []
    total_before_cap = 0
    if day is not None:
        df = _sd._read_instruments_day_df(bucket=bucket, venue=venue, day=day)  # pyright: ignore[reportPrivateUsage]
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
    df = _sd._read_instruments_day_df(bucket=bucket, venue=venue, day=day)  # pyright: ignore[reportPrivateUsage]
    if (df is None or df.empty) and protocol is not None:
        df = _sd._read_instruments_day_df(bucket=bucket, venue=chain, day=day)  # pyright: ignore[reportPrivateUsage]
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
    day = _sd._pick_latest_day(bucket, venue)  # pyright: ignore[reportPrivateUsage]
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

    Reads the MTDS prediction manifest index and aggregates by
    canonical_question_group on the latest available day for the given venue.
    Shard axis per UAC SHARD_AXIS_MATRIX:
    ("market-tick-data-service", "prediction"): ("venue", "canonical_question_group", "data_type").

    v9 canonical shape (``prediction_manifest_canonicalisation_2026_06_01.md``):
    - ``data_type = "prediction_canonical_question_group"`` (bundled row)
    - ``instrument_id`` = canonical_question_group value (the cqg lives here in v9,
      NOT in ``underlying`` — the ManifestWriter row_key uses ``instrument_id``).
    - ``observed_clusters`` = ``{conditionId: row_count}`` (per-market drilldown).
    - v9 columns: ``schema_version``, ``source``, ``asset_group``, ``pipeline_mode``,
      ``available_at``.

    Legacy shape (pre-v9): cqg value was stored in ``underlying``.  We prefer
    ``instrument_id`` (v9) and fall back to ``underlying`` gracefully so reads
    remain correct across the migration window.
    """
    # The prediction cqg-bundle manifest (`data_type=prediction_canonical_question_group`
    # with `observed_clusters`) is written by MTDS into `market-data-tick-pred-prd-{pid}`,
    # NOT the instruments-store — the shard axis is ("market-tick-data-service",
    # "prediction"). Reading the instruments-store bucket here returned an empty/wrong
    # drilldown for v9 rows (prediction_manifest_canonicalisation_2026_06_01.md).
    bucket = build_bucket_name("market-tick-data-service", "prediction")
    try:
        df = _sd.read_availability_index(bucket)
    except (OSError, RuntimeError, ValueError) as exc:
        logger.warning("prediction venue-detail: manifest read failed for %s: %s", bucket, exc)
        return VenueDetailResponse(category="PREDICTION", venue=venue)

    if df.empty or "venue" not in df.columns:
        return VenueDetailResponse(category="PREDICTION", venue=venue)

    # Restrict to the bundled data_type so raw per-cid rows are excluded.
    bundled_dt = "prediction_canonical_question_group"
    if "data_type" in df.columns:
        bundled_mask = df["data_type"].astype(str) == bundled_dt
        if bundled_mask.any():
            df = df[bundled_mask].copy()

    venue_df = df[df["venue"].astype(str).str.upper() == venue.upper()].copy()
    if venue_df.empty:
        return VenueDetailResponse(category="PREDICTION", venue=venue)

    day: str | None = None
    if "date" in venue_df.columns:
        dates = sorted(str(d) for d in venue_df["date"].dropna().unique() if str(d).strip())  # pyright: ignore[reportAny]
        if dates:
            day = dates[-1]
            venue_df = venue_df[venue_df["date"].astype(str) == day].copy()

    # Resolve the column that holds the canonical_question_group value.
    # v9: cqg lives in ``instrument_id`` (ManifestWriter row_key).
    # Legacy: cqg lives in ``underlying``.
    cqg_col: str | None = None
    if "instrument_id" in venue_df.columns and venue_df["instrument_id"].astype(str).str.strip().ne("").any():
        cqg_col = "instrument_id"
    elif "underlying" in venue_df.columns:
        cqg_col = "underlying"

    instruments: list[dict[str, object]] = []
    if cqg_col is not None:
        # Count per canonical_question_group: prefer ``observed_clusters`` total
        # (v9 bundled row carries the per-market count dict); fall back to
        # ``instrument_count`` (legacy column) or row-count.
        cqg_series = venue_df[cqg_col].astype(str)
        has_instrument_count = "instrument_count" in venue_df.columns
        for grp in sorted(cqg_series.unique()):  # pyright: ignore[reportAny]
            grp_str = str(grp)  # pyright: ignore[reportAny]
            if not grp_str.strip() or grp_str == "nan":
                continue
            grp_rows = venue_df[cqg_series == grp_str]
            count = int(grp_rows["instrument_count"].fillna(0).sum()) if has_instrument_count else len(grp_rows)  # pyright: ignore[reportAny]
            instruments.append(
                {
                    "key": grp_str,
                    "type": f"{count:,} markets",
                    "canonical_question_group": grp_str,
                    "instrument_count": count,
                    # v9: expose pipeline_mode + source from the first bundled row
                    # so the UI filter chips can show live vs batch correctly.
                    "pipeline_mode": str(
                        grp_rows["pipeline_mode"].iloc[0]  # pyright: ignore[reportAny]
                        if "pipeline_mode" in grp_rows.columns and not grp_rows.empty
                        else ""
                    ),
                    "source": str(
                        grp_rows["source"].iloc[0]  # pyright: ignore[reportAny]
                        if "source" in grp_rows.columns and not grp_rows.empty
                        else ""
                    ),
                }
            )
        # Sort descending by count to match the previous groupby behaviour.
        instruments.sort(key=lambda r: -int(r["instrument_count"]))  # pyright: ignore[reportArgumentType]

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

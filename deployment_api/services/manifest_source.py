"""Manifest-index source — the live consolidated availability index.

The data-status surfaces (status tab / hierarchical / drilldowns / shard detail / CSV
export) all read the availability ``_index`` of a market-data bucket via the LIVE
consolidated index (UTL ``read_availability_index``), with a stale-tolerant fallback to
the consolidated blob directly when the live freshness gate drops a stale-but-valid index
(the data-status is a MONITORING view, not a live-trading read).
"""

from __future__ import annotations

import io
import logging

import pandas as pd
from unified_trading_library import (
    get_storage_client,
    read_availability_index,
)

logger = logging.getLogger(__name__)

# The canonical consolidated availability index blob — read directly (no live freshness
# gate) as the stale-tolerant fallback for the data-status MONITORING view (see
# read_manifest_index docstring + plan proper_instrument_catalogue_lifecycle_rollup §R5).
_CONSOLIDATED_INDEX_BLOB = "_index/availability_index.parquet"

# Bucket tag → canonical asset_group (the env-tiered bucket names abbreviate prediction).
_TAG_TO_ASSET_GROUP: dict[str, str] = {
    "pred": "prediction",
    "prediction": "prediction",
    "cefi": "cefi",
    "defi": "defi",
    "tradfi": "tradfi",
    "sports": "sports",
}


def _asset_group_from_bucket(bucket: str) -> str:
    """Derive the asset_group token from an env-tiered bucket name
    (``market-data-tick-<tag>-<env>-<project>`` / ``instruments-store-<tag>-…``)."""
    for prefix in ("market-data-tick-", "instruments-store-"):
        if bucket.startswith(prefix):
            tag = bucket.removeprefix(prefix).split("-", 1)[0]
            return _TAG_TO_ASSET_GROUP.get(tag, tag)
    return ""


# Columns the hierarchical drill-down (deployment_api.services.data_status_hierarchical
# .get_hierarchical_drilldown) reads, filters on, groups by, or otherwise depends on for a
# read-side transform — enumerated by auditing that module PLUS its transitive column
# dependencies, not just its own ``df[...]`` accesses:
#   * every unified_api_contracts.registry.data_status_axis_matrix.SHARD_AXIS_MATRIX axis,
#     across EVERY (service, asset_group) pair (the caller's pair isn't known until request
#     time, so this must be the union, not just one call's axes) — venue/chain/data_type/
#     instrument_type/instrument_id/league_id/canonical_question_group/feature_group/
#     timeframe/model_family/training_period/strategy_id/instruction_type/job_id;
#   * date/capture_status/error_reason (leaf + aggregate counts) and underlying
#     (data_status_hierarchical._coalesce_instrument_id_from_underlying);
#   * feature_family (_stamp_feature_family + a _NON_AXIS_FILTERS entry) and
#     pipeline_mode/source (_NON_AXIS_FILTERS + _GROUP_BY_AXES);
#   * transport/cadence/asset_group/fixture_id — deployment_api.services.data_status_union
#     PROVENANCE_COLS / CELL_DIMENSION_COLS (union_reduce_to_cells / provenance_breakdown);
#   * client_id/attempted_at/written_at — NOT read by the drilldown tree itself, but
#     required so unified_trading_library.manifest_writer._read_index._merge_shard_frames()
#     (self-shard vs consolidated-blob reconciliation, invoked inside the pushdown read
#     below) dedups + recency-tie-breaks IDENTICALLY to the full-schema read — omitting them
#     would silently narrow the dedup key / degrade the tie-break to insertion order for
#     services whose manifest rows are client_id-differentiated (strategy-service,
#     execution-service) or that race a same-VM write against the consolidator.
# A column missing from this list silently drops out of the pushdown-read tree (the slim
# read path projects ONLY these columns) — extend it, never narrow it, when the drilldown
# or its axis matrix grows a new column.
DRILLDOWN_COLUMNS: list[str] = [
    "date",
    "capture_status",
    "error_reason",
    "venue",
    "chain",
    "data_type",
    "instrument_type",
    "instrument_id",
    "league_id",
    "canonical_question_group",
    "feature_group",
    "timeframe",
    "model_family",
    "training_period",
    "strategy_id",
    "instruction_type",
    "job_id",
    "underlying",
    "feature_family",
    "pipeline_mode",
    "source",
    "transport",
    "cadence",
    "asset_group",
    "fixture_id",
    "client_id",
    "attempted_at",
    "written_at",
]


def read_manifest_index(bucket: str, *, date_window: tuple[str, str] | None = None) -> pd.DataFrame:
    """The live consolidated availability ``_index`` for ``bucket``.

    STALE-TOLERANT MONITORING READ (2026-06-15): the data-status is a MONITORING view, not
    a live-trading read. UTL ``read_availability_index`` applies a ~120 s live staleness
    gate — when the consolidated ``_index/availability_index.parquet`` is older than that
    AND the per-VM shard dir holds only seed shards, it drops the (valid) consolidated index
    and returns ~empty. Correct for trading; but for data-status it shows 0 instruments when
    a stale-but-valid catalogue exists — the failure the operator saw while the manifest
    consolidators are paused by the held pre-migration drain (plan
    ``proper_instrument_catalogue_lifecycle_rollup_2026_06_04`` §R5). So when the live read
    yields empty (or raises) we read the consolidated blob DIRECTLY (no freshness gate) and
    surface the stale rows + their ``written_at`` age rather than 0. Live trading readers do
    not call this path — their 120 s gate is unchanged.

    Args:
        bucket: the manifest bucket to read.
        date_window: optional ``(window_start, window_end)`` ISO-date pair (inclusive). When
            set, takes the pyarrow row-group predicate-pushdown path (UTL
            ``read_availability_index``'s ``columns=``/``filters=`` slim path,
            :data:`DRILLDOWN_COLUMNS` projected + a ``date`` range filter) so only the
            requested window's row groups are decoded, bounding peak memory to roughly the
            window subset instead of the whole multi-year index (see UTL
            ``read_availability_index`` docstring: ~14.86 GiB -> ~5 MB for a single-day
            filter on the real 27.4M-row DeFi index —
            ``mtds_backfill_vm_startup_oom_rc137_2026_07_14`` / the drilldown P2 proper-root-
            fix follow-up). A pushdown read that raises OR comes back genuinely empty (either
            a truly-empty window, or a stale/missing consolidated blob) falls through to the
            unfiltered full read below — the SAME stale-tolerant path this function already
            runs for ``date_window=None`` — so a stale-but-valid catalogue still renders
            instead of silently showing 0 rows. ``None`` (the default) is byte-identical to
            the pre-pushdown behaviour: this function never even calls the slim path.
    """
    if date_window is not None:
        window_start, window_end = date_window
        try:
            pushed = read_availability_index(
                bucket,
                columns=DRILLDOWN_COLUMNS,
                filters=[("date", ">=", window_start), ("date", "<=", window_end)],
            )
        except Exception as _pushdown_err:  # pushdown read degrades to the full read below
            logger.warning(
                "data-status: pushdown read_availability_index(%s, window=%s..%s) raised "
                "%s — falling back to the full unfiltered read",
                bucket,
                window_start,
                window_end,
                type(_pushdown_err).__name__,
            )
            pushed = None
        if pushed is not None and not pushed.empty:
            return pushed
        # Pushdown raised, or came back genuinely empty (a real empty window, or a
        # stale/missing consolidated blob) — fall through to the unfiltered,
        # stale-tolerant full read below (identical to date_window=None).

    try:
        live = read_availability_index(bucket)
    except Exception as _live_err:  # monitoring read degrades to stale, never hard-fails the UI
        logger.warning(
            "data-status: live read_availability_index(%s) raised %s — falling back to the consolidated blob",
            bucket,
            type(_live_err).__name__,
        )
        live = None
    if live is not None and not live.empty:
        return live
    # Stale-tolerant fallback: the live gate dropped a stale-but-valid consolidated
    # index (consolidator paused). Read it directly for the monitoring view.
    try:
        raw = get_storage_client().download_bytes(bucket, _CONSOLIDATED_INDEX_BLOB)
        stale = pd.read_parquet(io.BytesIO(raw))
        if not stale.empty:
            logger.warning(
                "data-status STALE-TOLERANT read: %s live index empty; using %s directly "
                "(%d rows — consolidator paused/stale, monitoring view)",
                bucket,
                _CONSOLIDATED_INDEX_BLOB,
                len(stale),
            )
            return stale
    except Exception as _stale_err:  # genuinely-empty bucket -> empty df, not an error
        logger.debug(
            "data-status stale-tolerant read: no consolidated blob for %s (%s)",
            bucket,
            type(_stale_err).__name__,
        )
    return live if live is not None else pd.DataFrame()


_UNIQUE_COUNT_CACHE: dict[str, int] = {}


def read_unique_instrument_count(asset_group: str, *, cloud: str = "gcp") -> int | None:
    """Deduplicated instrument-identity count for an asset_group, from the lifecycle
    catalogue (``prod/catalog.parquet`` — one row per instrument with
    available_from/to; the ONLY identity-level source: the availability ``_index``
    carries per-shard COUNTS, so summing it over days multi-counts).

    Cached per-process (the catalogue changes daily at most). ``None`` when the
    catalogue is missing/unreadable — callers surface ``null``, never a fake 0."""
    from unified_trading_library import resolve_bucket_name  # noqa: qg-inside-import

    ag = asset_group.lower()
    if ag in _UNIQUE_COUNT_CACHE:
        return _UNIQUE_COUNT_CACHE[ag]
    try:
        # Prediction's bucket is its own KIND in cloud-providers.yaml (no asset_group
        # entry under instruments-store) — same mapping as build_instrument_catalogue.
        if ag == "prediction":
            bucket = resolve_bucket_name(cloud=cloud, kind="instruments-store-prediction")
        else:
            bucket = resolve_bucket_name(cloud=cloud, kind="instruments-store", asset_group=ag)
        raw = get_storage_client().download_bytes(bucket, "prod/catalog.parquet")
        df = pd.read_parquet(io.BytesIO(raw), columns=["instrument_id"])
        count = int(df["instrument_id"].nunique())
    except Exception as exc:
        logger.warning("unique-instrument catalogue read failed for %s (%s) — returning None", ag, exc)
        return None
    _UNIQUE_COUNT_CACHE[ag] = count
    return count

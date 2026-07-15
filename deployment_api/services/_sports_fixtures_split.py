"""Shared GCS-listing helper for the sports FIXTURES entity-folder split.

instruments-service cut the FIXTURES writer over to a two-entity
``fixtures_schedule``/``fixtures_outcomes`` split with NO legacy dual-write
(first observed 2026-07-14, no feature-flag gate) — every direct
``entity=fixtures`` reader in this repo silently degrades (empty/None) for
any date on/after the cutover. See
``plans/active/issues/features_sports_fixtures_split_reader_gap_2026_07_15.md``.

This helper is the shared "list the per-league split-entity shards" probe,
mirroring the pattern already shipped in
``instruments-service/instruments_service/reference_data/sports_dependency.py``
and ``unified-trading-library/unified_trading_library/fixtures/joined_reader.py``.
Each caller in this repo still owns its own parquet byte-read (they use
different readers — pyarrow-direct vs gcsfs-backed), so only the prefix-probe
logic is centralized here.
"""

from __future__ import annotations

from deployment_api.utils.storage_facade import list_objects

# Matches instruments-service's `_orch._sports_ref_pm("fixtures_schedule" / "fixtures_outcomes")`
# — api-football is currently the only source for the split FIXTURES entities.
_SPLIT_ENTITY_PIPELINE_MODE = "batch_api_football"


def split_entity_league_blob_paths(bucket: str, day: str, entity_name: str) -> list[str]:
    """Return per-league parquet blob paths for a split FIXTURES entity on ``day``.

    Split entities (``fixtures_schedule`` / ``fixtures_outcomes``) are written
    per-league under a ``pipeline_mode=`` hive segment
    (``entity={entity_name}/league={L}/{entity_name}.parquet``). Probes the
    canonical ``pipeline_mode=batch_api_football/`` prefix first (the current
    writer shape), falls back to the (unexpected but defensive) bare-prefix
    shape. Returns ``[]`` when neither prefix has any shard for the day.
    """
    canonical_prefix = (
        f"sports_reference/by_date/day={day}/pipeline_mode={_SPLIT_ENTITY_PIPELINE_MODE}/entity={entity_name}/league="
    )
    legacy_prefix = f"sports_reference/by_date/day={day}/entity={entity_name}/league="
    for prefix in (canonical_prefix, legacy_prefix):
        blob_paths = [o.name for o in list_objects(bucket, prefix) if o.name.endswith(f"/{entity_name}.parquet")]
        if blob_paths:
            return blob_paths
    return []

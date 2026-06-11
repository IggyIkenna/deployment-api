"""Data-status drill-down deep-dive helpers (deployment-api service layer).

Adds schema introspection, per-day instrument listing, per-venue bucket
counts (named markets + conditionIds-inside-OTHER), and CSV downloads for
selected instruments.

Deliberately lives outside ``data_status_service.py`` to keep that module
under the 900-line codex-compliance budget and because these concerns are
read-on-demand drill-downs, not status aggregation — they don't cache state,
just read additional artifacts on demand.

All GCS reads are TTL-cached (5 min) to avoid hammering the bucket when the
UI re-expands a venue or day.

Package facade (split 2026-06-11 from the 2,586-line
``services/data_status_drilldown.py`` per
``codex_violations_ratchet_to_five_2026_06_10.md`` Phase-1 P2 — pure code
motion). Module-level collaborators imported here (``list_objects`` /
``read_availability_index`` / ``resolve_bucket_name``) plus the re-exported
helpers below form the test patch surface — submodules resolve them through
this module at call time, so
``patch("deployment_api.services.data_status_drilldown.<name>", ...)``
keeps working.
"""

from __future__ import annotations

import logging

from unified_api_contracts import SchemaContract, SchemaContractNotFoundError, lookup_contract
from unified_api_contracts.internal.schemas.contracts import (
    VENUE_CONTRACT_OVERRIDES,
)
from unified_trading_library import (
    LEGACY_REASON_ASSET_GROUPS,
    AssetGroup,
    classify_legacy_empty_row,
    read_availability_index,
    resolve_bucket_name,
)

from deployment_api.settings import gcp_project_id as _pid
from deployment_api.utils.storage_facade import list_objects

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Re-exports — every symbol previously importable from
# ``deployment_api.services.data_status_drilldown`` resolves here unchanged
# (Citadel safety contract: preserve the surface; tests patch + import these).
# _core defines the shared cache/bucket primitives the other
# submodules resolve via this facade at call time.
# ---------------------------------------------------------------------------
from deployment_api.services.data_status_drilldown._core import (
    _AXIS_KWARGS,  # pyright: ignore[reportPrivateUsage]
    _BUNDLED_INSTRUMENT_TYPES,  # pyright: ignore[reportPrivateUsage]
    _CACHE_TTL_SECONDS,  # pyright: ignore[reportPrivateUsage]
    _CAPTURE_STATUS_HEADER,  # pyright: ignore[reportPrivateUsage]
    _DEFAULT_CAPTURE_STATUS,  # pyright: ignore[reportPrivateUsage]
    COMMODITY_BUCKET_TEMPLATE,
    PREDICTION_KIND_MAP,
    SERVICE_TO_KIND,
    _apply_default_capture_status,  # pyright: ignore[reportPrivateUsage]
    _attach_capture_status_to_instruments,  # pyright: ignore[reportPrivateUsage]
    _build_capture_metadata_lookup,  # pyright: ignore[reportPrivateUsage]
    _cache,  # pyright: ignore[reportPrivateUsage]
    _cache_get,  # pyright: ignore[reportPrivateUsage]
    _cache_put,  # pyright: ignore[reportPrivateUsage]
    _classify_lookup_legacy_empty_reason,  # pyright: ignore[reportPrivateUsage]
    _is_bundled,  # pyright: ignore[reportPrivateUsage]
    _scoped_manifest_rows,  # pyright: ignore[reportPrivateUsage]
    build_bucket_name,
    clear_drilldown_cache,
    lookup_capture_status_for_shard,
)
from deployment_api.services.data_status_drilldown._csv_export import (
    MAX_CSV_ROWS,
    MAX_SHARD_CSV_ROWS,
    MTDS_SHARD_SERVICES,
    _csv_filename,  # pyright: ignore[reportPrivateUsage]
    _distinct_values_in_parquet,  # pyright: ignore[reportPrivateUsage]
    _fixtures_csv_filename,  # pyright: ignore[reportPrivateUsage]
    _parquet_schema_names,  # pyright: ignore[reportPrivateUsage]
    _pick_column,  # pyright: ignore[reportPrivateUsage]
    _read_parquet_columns,  # pyright: ignore[reportPrivateUsage]
    _shard_csv_filename,  # pyright: ignore[reportPrivateUsage]
    build_csv_export,
    build_fixtures_csv_export,
    build_instruments_shard_csv_export,
    build_mtds_shard_csv_export,
)
from deployment_api.services.data_status_drilldown._fixtures_pools import (
    _DEFI_POOL_ID_CANDIDATE_COLS,  # pyright: ignore[reportPrivateUsage]
    _FIXTURE_ENTITIES,  # pyright: ignore[reportPrivateUsage]
    _FIXTURE_META_ALIASES,  # pyright: ignore[reportPrivateUsage]
    _defi_tick_bucket,  # pyright: ignore[reportPrivateUsage]
    _defi_venue_chain_aliases,  # pyright: ignore[reportPrivateUsage]
    _entity_gs_uri,  # pyright: ignore[reportPrivateUsage]
    _extract_fixture_ids_from_series,  # pyright: ignore[reportPrivateUsage]
    _filter_entity_rows_for_fixture,  # pyright: ignore[reportPrivateUsage]
    _fixture_download_filename,  # pyright: ignore[reportPrivateUsage]
    _list_defi_objects_with_aliases,  # pyright: ignore[reportPrivateUsage]
    _list_pool_entities_for_venue,  # pyright: ignore[reportPrivateUsage]
    _load_fixture_meta,  # pyright: ignore[reportPrivateUsage]
    _probe_fid_column,  # pyright: ignore[reportPrivateUsage]
    _read_entity_fixture_ids,  # pyright: ignore[reportPrivateUsage]
    _read_pool_ids_from_parquet,  # pyright: ignore[reportPrivateUsage]
    _resolve_fixture_day,  # pyright: ignore[reportPrivateUsage]
    _venue_chain_partition,  # pyright: ignore[reportPrivateUsage]
    build_fixture_breakdown,
    build_fixture_download,
    build_pool_breakdown,
)
from deployment_api.services.data_status_drilldown._instruments import (
    _PER_VENUE_DAY_BUNDLE_SERVICES,  # pyright: ignore[reportPrivateUsage]
    _SERVICE_BUNDLE_SYMBOL_COLUMN,  # pyright: ignore[reportPrivateUsage]
    DEFAULT_INSTRUMENT_LIMIT,
    MAX_INSTRUMENT_LIMIT,
    MAX_SEARCH_RESULTS,
    _apply_search_and_pagination,  # pyright: ignore[reportPrivateUsage]
    _bundling_mode,  # pyright: ignore[reportPrivateUsage]
    _collect_instrument_types,  # pyright: ignore[reportPrivateUsage]
    _collect_parquet_files,  # pyright: ignore[reportPrivateUsage]
    _count_distinct_in_other_bucket,  # pyright: ignore[reportPrivateUsage]
    _expand_per_condition_id,  # pyright: ignore[reportPrivateUsage]
    _expand_per_file,  # pyright: ignore[reportPrivateUsage]
    _expand_per_venue_day_bundle,  # pyright: ignore[reportPrivateUsage]
    _infer_symbol_column_for_shard,  # pyright: ignore[reportPrivateUsage]
    _is_valid_instrument_id,  # pyright: ignore[reportPrivateUsage]
    _list_instruments_full,  # pyright: ignore[reportPrivateUsage]
    _shard_prefix,  # pyright: ignore[reportPrivateUsage]
    cast_dict,
    compute_bucket_counts,
    get_shard_info,
    list_instruments_for_shard,
    preview_bundle_symbols,
)
from deployment_api.services.data_status_drilldown._schema import (
    _AUTO_SENTINELS,  # pyright: ignore[reportPrivateUsage]
    _DATA_TYPE_ALIASES,  # pyright: ignore[reportPrivateUsage]
    _INSTRUMENT_TYPE_ALIASES,  # pyright: ignore[reportPrivateUsage]
    _SPORTS_DATA_TYPE_TO_INSTRUMENT_TYPE,  # pyright: ignore[reportPrivateUsage]
    _build_leaf_parquet_candidates,  # pyright: ignore[reportPrivateUsage]
    _column_dicts,  # pyright: ignore[reportPrivateUsage]
    _first_parquet_under_prefix,  # pyright: ignore[reportPrivateUsage]
    _normalise_data_type,  # pyright: ignore[reportPrivateUsage]
    _normalise_instrument_type,  # pyright: ignore[reportPrivateUsage]
    _project_leaf_parquet_columns,  # pyright: ignore[reportPrivateUsage]
    _resolve_sports_instrument_type,  # pyright: ignore[reportPrivateUsage]
    get_schema_for_shard,
)

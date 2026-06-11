"""Unified shard-detail service for ``GET /api/data-status/shard-detail``.

Derives every piece of a Data-Status shard-detail response (schema,
GCS metadata, sample rows, branch-specific payload, download URLs) from a
single ``(service, category, instrument_type, data_type, venue, day, …)``
coordinate.  The four ``shard_class`` branches (``grouped`` / ``per_symbol``
/ ``reference`` / ``fixtures``) are classified by
:func:`_classify_shard` so the UI does not need to encode this routing
logic itself.

Kept separate from :mod:`data_status_drilldown` to sidestep the dense
per-fixture / bundle-preview helpers already living there — shard-detail
only needs footer reads + manifest lookup + first-N-rows + a distinct
pass for grouped shards.

Shard-level failure isolation applies throughout: every GCS / parquet /
manifest call catches its own exceptions and returns a ``missing`` /
``attempted_failed`` capture status rather than raising.  The endpoint
is a read surface for operators; a blown-up branch should never prevent
the other branches from rendering.
"""

from __future__ import annotations

import logging
import re as _re
from typing import Literal, cast
from urllib.parse import urlencode

import pandas as pd
from unified_api_contracts import (
    CONTRACT_REGISTRY,
    VENUE_CONTRACT_OVERRIDES,
    SchemaContract,
    SchemaContractNotFoundError,
    lookup_contract,
)
from unified_api_contracts.features import get_feature_family
from unified_trading_library import (
    LEGACY_REASON_ASSET_GROUPS,
    classify_legacy_empty_row,
    read_availability_index,
)

from deployment_api.services.data_status_drilldown import (
    _read_parquet_columns,  # pyright: ignore[reportPrivateUsage]
    build_bucket_name,
)
from deployment_api.settings import gcp_project_id as _pid
from deployment_api.types.shard_detail import (
    CaptureStatusLiteral,
    LeafAvailableAtEnvelope,
    LeafCompletenessEnvelope,
    LeafParquetColumnStat,
    LeafParquetStats,
    ServiceEmissionStateLiteral,
    ShardClassLiteral,
    ShardCoord,
    ShardDetailResponse,
    ShardDownloadUrls,
    ShardGcsMetadata,
    ShardPayloadFixtures,
    ShardPayloadGrouped,
    ShardPayloadPerSymbol,
    ShardPayloadReference,
    ShardSchema,
    ShardSchemaColumn,
    VenueDetailResponse,
)
from deployment_api.utils.storage_facade import get_object_metadata, list_objects, list_prefixes

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Package facade (split 2026-06-11 from the 1,777-line ``services/shard_detail.py``
# per ``codex_violations_ratchet_to_five_2026_06_10.md`` Phase-1 P2 — pure code
# motion). The imported collaborators above (``get_object_metadata`` /
# ``list_objects`` / ``read_availability_index`` / ``_read_parquet_columns``)
# plus the re-exported helpers below are the test patch surface — submodules
# resolve them through this module at call time, so
# ``patch.object(shard_detail, "<name>", ...)`` keeps intercepting.
# ---------------------------------------------------------------------------
from deployment_api.services.shard_detail._leaf_stats import (
    _LEAF_STATS_ROW_LIMIT,  # pyright: ignore[reportPrivateUsage]
    _compute_available_at_envelope,  # pyright: ignore[reportPrivateUsage]
    _compute_column_stats,  # pyright: ignore[reportPrivateUsage]
    _compute_completeness_envelope,  # pyright: ignore[reportPrivateUsage]
    _file_size_via_metadata,  # pyright: ignore[reportPrivateUsage]
    _resolve_feature_family,  # pyright: ignore[reportPrivateUsage]
    _safe_iso_or_none,  # pyright: ignore[reportPrivateUsage]
    get_leaf_parquet_stats,
)
from deployment_api.services.shard_detail._shard_core import (
    _AUTO_INSTRUMENT_TYPE_TOKENS,  # pyright: ignore[reportPrivateUsage]
    _GROUPED_DATA_TYPES,  # pyright: ignore[reportPrivateUsage]
    _PER_SYMBOL_INSTRUMENT_TYPES,  # pyright: ignore[reportPrivateUsage]
    _SERVICE_DEFAULT_SHARD_CLASS,  # pyright: ignore[reportPrivateUsage]
    _VALID_SERVICE_EMISSION_STATES,  # pyright: ignore[reportPrivateUsage]
    _classify_legacy_empty_reason,  # pyright: ignore[reportPrivateUsage]
    _classify_shard,  # pyright: ignore[reportPrivateUsage]
    _column_dict,  # pyright: ignore[reportPrivateUsage]
    _defi_composite_parts,  # pyright: ignore[reportPrivateUsage]
    _gcs_metadata,  # pyright: ignore[reportPrivateUsage]
    _gcs_path_for_shard,  # pyright: ignore[reportPrivateUsage]
    _is_auto_instrument_type,  # pyright: ignore[reportPrivateUsage]
    _list_first_parquet,  # pyright: ignore[reportPrivateUsage]
    _manifest_coord_mask,  # pyright: ignore[reportPrivateUsage]
    _manifest_row_for_coord,  # pyright: ignore[reportPrivateUsage]
    _mtds_shard_path,  # pyright: ignore[reportPrivateUsage]
    _resolve_instrument_type_auto,  # pyright: ignore[reportPrivateUsage]
    _resolve_schema,  # pyright: ignore[reportPrivateUsage]
)
from deployment_api.services.shard_detail._shard_read import (
    _csv_projection_url,  # pyright: ignore[reportPrivateUsage]
    _distinct_symbols,  # pyright: ignore[reportPrivateUsage]
    _parquet_signed_url,  # pyright: ignore[reportPrivateUsage]
    _read_parquet_footer_row_count,  # pyright: ignore[reportPrivateUsage]
    _sample_rows,  # pyright: ignore[reportPrivateUsage]
    get_shard_detail,
)
from deployment_api.services.shard_detail._venue_detail import (
    _DEFI_VERSION_UNDERSCORE_RE,  # pyright: ignore[reportPrivateUsage]
    _cefi_venue_detail,  # pyright: ignore[reportPrivateUsage]
    _defi_chain_only_detail,  # pyright: ignore[reportPrivateUsage]
    _defi_composite_detail,  # pyright: ignore[reportPrivateUsage]
    _defi_venue_detail,  # pyright: ignore[reportPrivateUsage]
    _instruments_bucket_for_category,  # pyright: ignore[reportPrivateUsage]
    _is_pool_row,  # pyright: ignore[reportPrivateUsage]
    _list_day_prefixes,  # pyright: ignore[reportPrivateUsage]
    _load_defi_df,  # pyright: ignore[reportPrivateUsage]
    _partition_key_for_category,  # pyright: ignore[reportPrivateUsage]
    _pick_latest_day,  # pyright: ignore[reportPrivateUsage]
    _prediction_venue_detail,  # pyright: ignore[reportPrivateUsage]
    _read_instruments_day_df,  # pyright: ignore[reportPrivateUsage]
    _typed_row,  # pyright: ignore[reportPrivateUsage]
    _venue_aliases_for_bucket,  # pyright: ignore[reportPrivateUsage]
    fetch_venue_detail,
)

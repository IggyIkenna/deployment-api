"""
Data Status API Routes - Thin handlers only.

Endpoint for checking data completion status across services.
Business logic delegated to service layer modules.

Package facade (split 2026-06-11 from the 2,550-line ``routes/data_status.py``
per ``codex_violations_ratchet_to_five_2026_06_10.md`` Phase-1 P2 — pure code
motion). The submodules register their routes on the single shared ``router``
defined here, so the mounted route table is byte-identical to the pre-split
module. Module-level collaborators below (``_cfg``, the service singletons,
and the imported service-layer functions) are the test patch surface —
submodules resolve them through this module at call time, so
``patch("deployment_api.routes.data_status.<name>", ...)`` keeps working.
"""

import logging

from fastapi import APIRouter

# deploy_missing_launch is imported lazily inside post_deploy_missing_launch
# to break the services/__init__ → deployment_manager → routes → services circular chain.
# Module-level reference so tests can patch it without reaching into function locals.
from unified_trading_library import read_availability_index as _read_availability_index

from deployment_api.deployment_api_config import DeploymentApiConfig
from deployment_api.services import DataAnalyticsService, DataQueryService, DataStatusService
from deployment_api.services.data_status_drilldown import (
    DEFAULT_INSTRUMENT_LIMIT,
    MAX_INSTRUMENT_LIMIT,
    MTDS_SHARD_SERVICES,
    build_bucket_name,
    build_csv_export,
    build_fixture_breakdown,
    build_fixture_download,
    build_fixtures_csv_export,
    build_instruments_shard_csv_export,
    build_mtds_shard_csv_export,
    build_pool_breakdown,
    clear_drilldown_cache,
    compute_bucket_counts,
    get_schema_for_shard,
    get_shard_info,
    list_instruments_for_shard,
    lookup_capture_status_for_shard,
    preview_bundle_symbols,
)
from deployment_api.services.data_status_hierarchical import (
    get_hierarchical_drilldown,
    list_supported_pairs,
)
from deployment_api.services.data_status_mock import (
    build_mock_shard_instruments,
    build_mock_turbo_response,
)
from deployment_api.services.deploy_missing import (
    DeployMissingError,
    assert_tarball_not_blocked,
    build_deploy_missing_preview,
    build_live_cluster_launch_preview,
)
from deployment_api.services.deploy_missing import (
    list_supported_live_cluster_roles as deploy_missing_supported_live_cluster_roles,
)
from deployment_api.services.deploy_missing import (
    list_supported_services as deploy_missing_supported_services,
)

# Stale-tolerant MONITORING read: on an EMPTY live result the data-status surfaces fall
# back to the consolidated ``_index`` blob directly (no live freshness gate) so a
# paused/stale consolidator does not blank the board. Module-level alias so the coverage
# routes resolve it through the facade (``_ds._read_manifest_index``) and the test patch
# surface keeps intercepting. SSOT: ``services/manifest_source.py``.
from deployment_api.services.manifest_source import read_manifest_index as _read_manifest_index

_cfg = DeploymentApiConfig()

logger = logging.getLogger(__name__)

router = APIRouter()

# Initialize service instances
data_status_service = DataStatusService()
data_query_service = DataQueryService()
data_analytics_service = DataAnalyticsService()

# ---------------------------------------------------------------------------
# Route registration — each submodule attaches its endpoints to the shared
# ``router`` above at import time. Import order REPLAYS the original inline
# definition order (FastAPI matches routes in registration order; the
# duplicate ``/coverage-summary`` path relies on it), so keep this sequence.
# ---------------------------------------------------------------------------
# isort: off
from deployment_api.routes.data_status import _status_core
from deployment_api.routes.data_status import _deploy_turbo
from deployment_api.routes.data_status import _query_meta
from deployment_api.routes.data_status import _downloads
from deployment_api.routes.data_status import _live_coverage
from deployment_api.routes.data_status import _rollup
from deployment_api.routes.data_status import _catalogue

# isort: on

# ---------------------------------------------------------------------------
# Public-surface re-exports — every symbol previously importable from
# ``deployment_api.routes.data_status`` resolves here unchanged (Citadel
# safety contract: preserve the surface; tests import these directly).
# ---------------------------------------------------------------------------
from deployment_api.routes.data_status._catalogue import (
    download_catalogue_csv,
    get_instrument_catalogue,
)
from deployment_api.routes.data_status._deploy_turbo import (
    clear_turbo_cache,
    get_data_coverage_summary,
    get_data_status_drilldown,
    get_data_status_turbo,
    get_deploy_live_cluster_roles,
    get_deploy_missing_services,
    get_drilldown_supported_pairs,
    get_turbo_cache_stats,
    post_deploy_live_cluster_preview,
    post_deploy_missing_launch,
    post_deploy_missing_preview,
)
from deployment_api.routes.data_status._downloads import (
    _attempted_failed_csv_body,  # pyright: ignore[reportPrivateUsage]
    _capture_status_response,  # pyright: ignore[reportPrivateUsage]
    _empty_confirmed_csv_body,  # pyright: ignore[reportPrivateUsage]
    _path_drift_response,  # pyright: ignore[reportPrivateUsage]
    clear_drilldown_cache_endpoint,
    download_csv,
    download_fixture_payload,
    download_fixtures_csv,
    download_shard_csv,
    get_fixture_breakdown,
    get_pool_breakdown,
    get_shard_info_endpoint,
)
from deployment_api.routes.data_status._live_coverage import (
    ConfigVersionTriple,
    CoverageScope,
    LiveCaptureStatus,
    LiveStatusResponse,
    LiveStatusRow,
    VenueYearCoverageResponse,
    VenueYearRow,
    get_honest_coverage,
    get_live_data_status,
    get_venue_year_coverage,
)
from deployment_api.routes.data_status._query_meta import (
    analyze_data_patterns,
    get_bucket_counts,
    get_bundle_preview,
    get_instrument_availability,
    get_instruments_for_shard,
    get_instruments_list,
    get_multi_service_status,
    get_schema,
    get_venue_filters,
    list_files_in_path,
    search_instruments,
)
from deployment_api.routes.data_status._status_core import (
    calculate_missing_shards,
    get_coverage_summary,
    get_data_status,
    get_data_status_manifest,
    get_last_updated,
)

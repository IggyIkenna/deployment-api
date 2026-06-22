"""
Data status business logic service — thin FACADE.

``DataStatusService`` is composed from the linear mixin chain in
``deployment_api/services/data_status/`` (cli -> defi -> sports ->
breakdowns_domain -> breakdowns_core -> venue_resolution -> coverage ->
missing_shards -> manifest). The 6,663-line god-module was decomposed
2026-06-11 (codex ratchet plan ``codex_violations_ratchet_to_five_2026_06_10``).

This module remains the ONLY public import surface:

* ``DataStatusService`` keeps its full public method set (inherited).
* Every legacy module-level helper/constant is re-exported below under
  its original name, so existing imports and test patch targets
  (``deployment_api.services.data_status_service.<name>``) keep working.
* The names listed in ``__all__`` under "patch points" are CANONICAL
  here — package modules resolve them late-bound through this module
  (``import deployment_api.services.data_status_service as _dss``), so
  ``unittest.mock.patch`` on this module is visible to the moved code.
"""

import gzip
import json
import logging
import time
from pathlib import Path
from typing import cast

import pandas as pd
import yaml
from unified_api_contracts import (
    VenueMapping,
    get_expected_data_types_for_venue,
    get_venue_data_type_start_date,
)
from unified_api_contracts.sports import (
    FEATURE_UPSTREAM_REQUIREMENTS,
    get_expected_leagues_for_source,
    get_league_fixture_calendar,
)
from unified_trading_library import get_storage_client, resolve_bucket_name

from deployment_api.services.data_status.breakdowns_core import CoreBreakdownsMixin
from deployment_api.services.data_status.breakdowns_domain import DomainBreakdownsMixin
from deployment_api.services.data_status.cli import DataStatusCliMixin
from deployment_api.services.data_status.coverage import CoverageStatusMixin
from deployment_api.services.data_status.coverage_metrics import (
    CAPTURE_STATUS_CAPTURED as _CAPTURE_STATUS_CAPTURED,
)
from deployment_api.services.data_status.coverage_metrics import (
    CAPTURE_STATUS_COL as _CAPTURE_STATUS_COL,
)
from deployment_api.services.data_status.coverage_metrics import (
    CAPTURE_STATUS_EMPTY as _CAPTURE_STATUS_EMPTY,
)
from deployment_api.services.data_status.coverage_metrics import (
    CAPTURE_STATUS_FAILED as _CAPTURE_STATUS_FAILED,
)
from deployment_api.services.data_status.coverage_metrics import (
    COVERAGE_SEMANTICS,
)
from deployment_api.services.data_status.coverage_metrics import (
    EMPTY_REASON_KEYS as _EMPTY_REASON_KEYS,
)
from deployment_api.services.data_status.coverage_metrics import (
    EXPECTED_REASON_PREFIX as _EXPECTED_REASON_PREFIX,
)
from deployment_api.services.data_status.coverage_metrics import (
    FAILURE_PILLAR_BY_ERROR_PREFIX as _FAILURE_PILLAR_BY_ERROR_PREFIX,
)
from deployment_api.services.data_status.coverage_metrics import (
    FAILURE_PILLAR_KEYS as _FAILURE_PILLAR_KEYS,
)
from deployment_api.services.data_status.coverage_metrics import (
    build_coverage_metrics as _build_coverage_metrics,
)
from deployment_api.services.data_status.coverage_metrics import (
    build_failure_rate_by_dimension as _build_failure_rate_by_dimension,
)
from deployment_api.services.data_status.coverage_metrics import (
    compute_attempt_coverage as _compute_attempt_coverage,
)
from deployment_api.services.data_status.coverage_metrics import (
    compute_capture_status_counts as _compute_capture_status_counts,
)
from deployment_api.services.data_status.coverage_metrics import (
    compute_empty_reason_counts as _compute_empty_reason_counts,
)
from deployment_api.services.data_status.coverage_metrics import (
    compute_failure_pillar_counts as _compute_failure_pillar_counts,
)
from deployment_api.services.data_status.coverage_metrics import (
    compute_out_of_window_count as _compute_out_of_window_count,
)
from deployment_api.services.data_status.coverage_metrics import (
    derive_capture_status_rates as _derive_capture_status_rates,
)
from deployment_api.services.data_status.coverage_metrics import (
    distinct_pairs as _distinct_pairs,
)
from deployment_api.services.data_status.coverage_metrics import (
    distinct_values as _distinct_values,
)
from deployment_api.services.data_status.coverage_metrics import (
    sports_attempt_count as _sports_attempt_count,
)
from deployment_api.services.data_status.defi import DefiStatusMixin
from deployment_api.services.data_status.frame_utils import (
    PREDICTION_BUNDLED_DT as _PREDICTION_BUNDLED_DT,
)
from deployment_api.services.data_status.frame_utils import (
    TRANSFER_COUNTRIES as _TRANSFER_COUNTRIES,
)
from deployment_api.services.data_status.frame_utils import (
    clamp_to_venue_starts as _clamp_to_venue_starts,
)
from deployment_api.services.data_status.frame_utils import (
    derive_underlying_from_instrument_id as _derive_underlying_from_instrument_id,
)
from deployment_api.services.data_status.frame_utils import (
    ensure_underlying_column as _ensure_underlying_column,
)
from deployment_api.services.data_status.frame_utils import (
    promote_prediction_cqg_from_instrument_id as _promote_prediction_cqg_from_instrument_id,
)
from deployment_api.services.data_status.instrument_coverage import (
    DEFAULT_PER_INSTRUMENT_SENTINEL_CAP as _DEFAULT_PER_INSTRUMENT_SENTINEL_CAP,
)
from deployment_api.services.data_status.instrument_coverage import (
    PER_INSTRUMENT_BREAKDOWN_MAX_SIZE as _PER_INSTRUMENT_BREAKDOWN_MAX_SIZE,
)
from deployment_api.services.data_status.instrument_coverage import (
    build_cefi_is_instruments_provider as _build_cefi_is_instruments_provider,
)
from deployment_api.services.data_status.instrument_coverage import (
    per_instrument_coverage as _per_instrument_coverage,
)
from deployment_api.services.data_status.manifest import ManifestStatusMixin
from deployment_api.services.data_status.manifest import (
    build_category_in_subprocess as _build_category_in_subprocess,
)
from deployment_api.services.data_status.missing_shards import MissingShardsMixin
from deployment_api.services.data_status.mtds import (
    DEFI_DATA_TYPE_ALIASES as _DEFI_DATA_TYPE_ALIASES,
)
from deployment_api.services.data_status.mtds import (
    DEFI_SOURCE_TO_DATA_TYPE as _DEFI_SOURCE_TO_DATA_TYPE,
)
from deployment_api.services.data_status.mtds import (
    MTDS_CATEGORY_META,
    PREDICTION_DATA_TYPE_META,
)
from deployment_api.services.data_status.mtds import (
    TRADFI_TICK_ONLY_DATA_TYPES as _TRADFI_TICK_ONLY_DATA_TYPES,
)
from deployment_api.services.data_status.mtds import (
    canonicalise_defi_data_types as _canonicalise_defi_data_types,
)
from deployment_api.services.data_status.mtds import (
    is_mtds_honest_coverage_target as _is_mtds_honest_coverage_target,
)
from deployment_api.services.data_status.mtds import (
    mtds_expected_dates_cached as _mtds_expected_dates_cached,
)
from deployment_api.services.data_status.mtds import (
    mtds_expected_dates_for_venue_dt as _mtds_expected_dates_for_venue_dt,
)
from deployment_api.services.data_status.mtds import (
    mtds_expected_venues as _mtds_expected_venues,
)
from deployment_api.services.data_status.mtds import (
    mtds_honest_coverage_for_venue as _mtds_honest_coverage_for_venue,
)
from deployment_api.services.data_status.mtds import (
    shared_venue_mapping as _shared_venue_mapping,
)
from deployment_api.services.data_status.rollup_cache import (
    ALL_DEFI_GHOST_VENUES as _ALL_DEFI_GHOST_VENUES,
)
from deployment_api.services.data_status.rollup_cache import (
    DEFI_NON_PROTOCOL_VENUE_PREFIXES as _DEFI_NON_PROTOCOL_VENUE_PREFIXES,
)
from deployment_api.services.data_status.rollup_cache import (
    ROLLUP_BUCKET_TEMPLATE as _ROLLUP_BUCKET_TEMPLATE,
)
from deployment_api.services.data_status.rollup_cache import (
    ROLLUP_CACHE as _ROLLUP_CACHE,
)
from deployment_api.services.data_status.rollup_cache import (
    ROLLUP_CACHE_TTL_SEC as _ROLLUP_CACHE_TTL_SEC,
)
from deployment_api.services.data_status.rollup_cache import (
    ROLLUP_STALENESS_SEC as _ROLLUP_STALENESS_SEC,
)
from deployment_api.services.data_status.rollup_cache import (
    filter_coverage_to_asset_groups as _filter_coverage_to_asset_groups,
)
from deployment_api.services.data_status.rollup_cache import (
    filter_dates_in_window as _filter_dates_in_window,
)
from deployment_api.services.data_status.rollup_cache import (
    read_coverage_rollup_if_fresh as _read_coverage_rollup_if_fresh,
)
from deployment_api.services.data_status.rollup_cache import (
    rollup_blob_path as _rollup_blob_path,
)
from deployment_api.services.data_status.rollup_cache import (
    rollup_bucket as _rollup_bucket,
)
from deployment_api.services.data_status.rollup_cache import (
    slice_asset_group as _slice_asset_group,
)
from deployment_api.services.data_status.rollup_cache import (
    slice_rollup_to_window as _slice_rollup_to_window,
)
from deployment_api.services.data_status.rollup_cache import (
    slice_venue as _slice_venue,
)
from deployment_api.services.data_status.rollup_cache import (
    strip_defi_ghost_venues as _strip_defi_ghost_venues,
)
from deployment_api.services.data_status.sports import SportsStatusMixin
from deployment_api.services.data_status.sports_helpers import (
    FEATURES_SPORTS_DATA_TYPE_META,
    FEATURES_SPORTS_PER_CALC_META,
    SPORTS_DATA_TYPE_META,
)
from deployment_api.services.data_status.sports_helpers import (
    expected_dates_for_upstream as _expected_dates_for_upstream,
)
from deployment_api.services.data_status.sports_helpers import (
    sports_expected_dates_for_league as _sports_expected_dates_for_league,
)
from deployment_api.services.data_status.sports_helpers import (
    sports_honest_coverage as _sports_honest_coverage,
)
from deployment_api.services.data_status.sports_helpers import (
    sports_trigger_dates_for_league as _sports_trigger_dates_for_league,
)
from deployment_api.services.data_status.sports_helpers import (
    sports_trigger_dates_for_window as _sports_trigger_dates_for_window,
)
from deployment_api.services.data_status.venue_resolution import VenueResolutionMixin
from deployment_api.services.manifest_source import read_manifest_index as read_availability_index
from deployment_api.settings import DATA_STATUS_DISABLE_PROCESS_POOL as _DISABLE_PROCESS_POOL
from deployment_api.settings import deployment_env_short as _env_short
from deployment_api.settings import gcp_project_id as _pid
from deployment_api.utils.storage_facade import list_objects

__all__ = [  # noqa: RUF022
    "COVERAGE_SEMANTICS",
    "FEATURES_SPORTS_DATA_TYPE_META",
    "FEATURES_SPORTS_PER_CALC_META",
    "MTDS_CATEGORY_META",
    "PREDICTION_DATA_TYPE_META",
    "SPORTS_DATA_TYPE_META",
    "CoreBreakdownsMixin",
    "CoverageStatusMixin",
    "DataStatusCliMixin",
    "DataStatusService",
    "DefiStatusMixin",
    "DomainBreakdownsMixin",
    "ManifestStatusMixin",
    "MissingShardsMixin",
    "SportsStatusMixin",
    "VenueResolutionMixin",
    # ── patch points (canonical here; package modules resolve late-bound) ──
    "VenueMapping",
    "get_expected_data_types_for_venue",
    "get_expected_leagues_for_source",
    "get_league_fixture_calendar",
    "get_venue_data_type_start_date",
    "list_objects",
    "read_availability_index",
    "resolve_bucket_name",
    "clear_index_cache",
    "clear_rollup_cache",
    "get_effective_start_date",
    # ── facade-canonical underscore constants (package modules + tests resolve these late-bound here) ──
    "_ALL_DEFI_GHOST_VENUES",
    "_CAPTURE_STATUS_CAPTURED",
    "_CAPTURE_STATUS_COL",
    "_CAPTURE_STATUS_EMPTY",
    "_CAPTURE_STATUS_FAILED",
    "_DEFAULT_PER_INSTRUMENT_SENTINEL_CAP",
    "_DEFI_DATA_TYPE_ALIASES",
    "_DEFI_NON_PROTOCOL_VENUE_PREFIXES",
    "_DEFI_SOURCE_TO_DATA_TYPE",
    "_EMPTY_REASON_KEYS",
    "_EXPECTED_REASON_PREFIX",
    "_FAILURE_PILLAR_BY_ERROR_PREFIX",
    "_FAILURE_PILLAR_KEYS",
    "_PER_INSTRUMENT_BREAKDOWN_MAX_SIZE",
    "_PREDICTION_BUNDLED_DT",
    "_ROLLUP_BUCKET_TEMPLATE",
    "_ROLLUP_CACHE",
    "_ROLLUP_CACHE_TTL_SEC",
    "_ROLLUP_STALENESS_SEC",
    "_TRADFI_TICK_ONLY_DATA_TYPES",
    "_TRANSFER_COUNTRIES",
    "_build_category_in_subprocess",
    "_build_cefi_is_instruments_provider",
    "_build_coverage_metrics",
    "_build_failure_rate_by_dimension",
    "_canonicalise_defi_data_types",
    "_clamp_to_venue_starts",
    "_compute_attempt_coverage",
    "_compute_capture_status_counts",
    "_compute_empty_reason_counts",
    "_compute_failure_pillar_counts",
    "_compute_out_of_window_count",
    "_derive_capture_status_rates",
    "_derive_underlying_from_instrument_id",
    "_distinct_pairs",
    "_distinct_values",
    "_ensure_underlying_column",
    "_expected_dates_for_upstream",
    "_features_sports_expected_dates_for_calculator",
    "_filter_coverage_to_asset_groups",
    "_filter_dates_in_window",
    "_is_mtds_honest_coverage_target",
    "_mtds_expected_dates_cached",
    "_mtds_expected_dates_for_venue_dt",
    "_mtds_expected_venues",
    "_mtds_honest_coverage_for_venue",
    "_per_instrument_coverage",
    "_promote_prediction_cqg_from_instrument_id",
    "_read_coverage_rollup_if_fresh",
    "_read_index_cached",
    "_read_rollup_if_fresh",
    "_rollup_bucket",
    "_shared_venue_mapping",
    "_slice_asset_group",
    "_slice_rollup_to_window",
    "_slice_venue",
    "_sports_attempt_count",
    "_sports_expected_dates_for_league",
    "_sports_honest_coverage",
    "_sports_trigger_dates_for_league",
    "_sports_trigger_dates_for_window",
    "_strip_defi_ghost_venues",
]

logger = logging.getLogger(__name__)


def _features_sports_expected_dates_for_calculator(
    calc_name: str,
    league_id: str,
    start_date: str,
    end_date: str,
) -> list[str]:
    """Per-feature-group expected dates for one (calculator, league).

    Phase 3 honest-coverage: a calculator runs for date D x league L iff every
    *required* upstream is in-coverage on (D, L). The expected denominator
    for the calculator is the intersection of expected-date sets across its
    required upstreams (UAC ``FEATURE_UPSTREAM_REQUIREMENTS``).

    For ``derived``-source upstreams (Stage D dependencies on other
    calculators' outputs) we recurse — the dependent calculator's coverage
    propagates. To prevent runaway recursion on a malformed catalogue the
    walker carries a visited set; a cycle aborts to a "no expected dates"
    answer (caller treats as zero, surfaces in the data-status as a config
    bug).

    Returns the sorted list of dates in [start_date, end_date] where this
    calculator is expected to have a manifest row for ``league_id``.
    Empty if the calculator is unknown OR every date is out of every
    required upstream's coverage (NaN-by-design throughout).
    """

    def _walk(
        name: str,
        visited: frozenset[str],
    ) -> list[str] | None:
        if name in visited:
            # Cycle in the derived-DAG; bail to "unknown" rather than infinite-loop.
            return None
        next_visited = visited | {name}
        reqs = FEATURE_UPSTREAM_REQUIREMENTS.get(name) or FEATURE_UPSTREAM_REQUIREMENTS.get(f"{name}_calculator", [])
        if not reqs:
            return None
        required_reqs = [r for r in reqs if r.required]
        if not required_reqs:
            # Calculator with only optional upstreams — fall back to the
            # league's full fixture calendar.
            return get_league_fixture_calendar(league_id, start_date, end_date)
        intersection: set[str] | None = None
        for req in required_reqs:
            dates = _expected_dates_for_upstream(req, league_id, start_date, end_date, _walk, next_visited)
            if dates is None:
                # Required upstream has no dates we can model; skip silently —
                # the data-status reads from the manifest anyway, and over-
                # counting expected here is worse than under-counting.
                continue
            date_set = set(dates)
            intersection = date_set if intersection is None else intersection & date_set
        if intersection is None:
            return []
        return sorted(intersection)

    out = _walk(calc_name, frozenset())
    return out or []


# ProcessPool toggle — config-driven (DATA_STATUS_DISABLE_PROCESS_POOL via pydantic
# settings, not a raw process-env read). Linux deploys keep the fork pool; macOS dev hosts
# MUST set it: the pool forks after grpc/GCS is initialised, which dies on macOS
# (BrokenProcessPool; diagnosed 2026-06-12 under the CF-20 beta projected indexes). With it
# set, the build falls back to a thread pool (overlaps the I/O-bound index loads). Module-level
# so tests/scripts can still patch it.
_PROCESS_POOL_DISABLED = _DISABLE_PROCESS_POOL

# ThreadPool toggle — when True the per-asset-group build runs FULLY SERIAL (no
# ProcessPool AND no ThreadPool). Default False: API requests keep the thread pool
# (overlaps the I/O-bound index loads → fast responses). The in-service rollup
# endpoint (routes/data_status/_rollup.py) sets it True: computing all asset groups
# in PARALLEL threads holds every AG's cell-grid intermediate at once and blew the
# 8 GiB service memory limit ("Memory limit of 8192 MiB exceeded with 8435 MiB used");
# serial holds one AG at a time (~1.7 GiB peak) so it fits. (The standalone Cloud Run
# Job is unusable regardless — gen2-forced native crash; R7 follow-up #4.) Module-level
# so tests/scripts can patch it.
_THREAD_POOL_DISABLED = False

# Build fan-out cap — the MAX concurrent per-asset-group cell-grid builds in the
# Process/Thread pool of ``build_all_categories``. Each parallel worker holds a full
# AG cell-grid intermediate (~1.4 GiB peak for the big market-tick-data-service index),
# so fanning out all 5 asset groups at once peaked at 8604 MiB and OOM-killed the 8 GiB
# Cloud Run instance (the per-request ``/data-status/turbo`` "Unknown error" — the OOM
# returns no JSON body; the UI's catch-block on the async parse call falls back to that string).
# Capping at 3 bounds the peak to ~base + 3x grid (~5.7 GiB) so a per-request fresh compute
# fits even the old 8 GiB box. The rollup endpoint runs FULLY serial via the two toggles
# above; this caps the on-demand API path race-free (no per-request global mutation, which
# would race under concurrency=80). Module-level so tests/scripts can patch it.
_MAX_BUILD_WORKERS = 3

_INDEX_CACHE: dict[str, tuple[float, pd.DataFrame]] = {}
_INDEX_CACHE_TTL = 300  # 5 minutes


def _read_index_cached(bucket: str) -> pd.DataFrame:
    """Read availability index with 5-minute TTL cache."""
    now = time.monotonic()
    cached = _INDEX_CACHE.get(bucket)
    if cached and (now - cached[0]) < _INDEX_CACHE_TTL:
        return cached[1]
    idx = read_availability_index(bucket)
    if _ALL_DEFI_GHOST_VENUES and "venue" in idx.columns:
        # Match both bare form (AAVE_V3) and hyphenated-chain form (AAVE_V3-ETHEREUM)
        venue_prefix = idx["venue"].str.split("-", n=1).str[0]
        ghost_mask = venue_prefix.isin(_ALL_DEFI_GHOST_VENUES)
        if ghost_mask.any():
            logger.debug("_read_index_cached: dropping %d ghost venue rows from %s", int(ghost_mask.sum()), bucket)
            idx = idx[~ghost_mask].reset_index(drop=True)
    _INDEX_CACHE[bucket] = (now, idx)
    return idx


# ── Offline rollup fast-path (plan: data_status_offline_rollup_2026_05_06) ──
#
# Cloud Run Job ``uts-prod-data-status-rollup`` writes
# ``gs://{pid}-data-status-rollups/{service}/full.json.gz`` every 5 min via
# ``*/5 * * * *`` Cloud Scheduler. We read it here, in-process-cache for
# 60s (refresh window inside one warm Cloud Run instance), and slice to the
# user's date window. Falls through to the on-demand compute when the rollup
# is missing or older than ``_ROLLUP_STALENESS_SEC``.

# Full set of deprecated ghost DeFi venue names — now canonical in UAC.


def _read_rollup_if_fresh(service: str) -> dict[str, object] | None:
    """Read ``gs://{pid}-data-status-rollups/{service}/full.json.gz``.

    Returns the deserialised payload if the blob exists AND is younger than
    ``_ROLLUP_STALENESS_SEC``. Returns ``None`` otherwise — caller then falls
    through to the on-demand compute path.

    Two cache layers:
      * **In-process** (60s TTL): avoids re-fetching the same rollup every
        request from the same Cloud Run instance.
      * **GCS staleness check** (30 min TTL): protects against serving a
        rollup older than 6 cron cycles when the worker is broken.
    """
    cached = _ROLLUP_CACHE.get(service)
    now = time.monotonic()
    if cached is not None and (now - cached[0]) < _ROLLUP_CACHE_TTL_SEC:
        return cached[1]

    try:
        client = get_storage_client(project_id=_pid)
        bucket_name = _rollup_bucket()
        blob_path = _rollup_blob_path(service, "full")
        # Unified cloud_interface flat API (NOT raw google-cloud-storage Blob):
        #   blob_exists(bucket, blob_path) -> bool
        #   get_blob_metadata(bucket, blob_path) -> BlobMetadata | None
        #   download_bytes(bucket, blob_path) -> bytes
        if not client.blob_exists(bucket_name, blob_path):  # pyright: ignore[reportAttributeAccessIssue]
            return None
        meta = client.get_blob_metadata(bucket_name, blob_path)  # pyright: ignore[reportAttributeAccessIssue]
        # BlobMetadata exposes ``updated`` as a datetime (or string ISO depending
        # on backend). Treat missing/parse-errors as "fresh enough" to read. Respect
        # staleness — otherwise a frozen live blob (e.g. the worker stalled) is served
        # indefinitely.
        if meta is not None and getattr(meta, "updated", None) is not None:
            age_sec = (pd.Timestamp.now(tz="UTC") - pd.Timestamp(meta.updated)).total_seconds()  # type: ignore[reportAttributeAccessIssue, reportUnknownArgumentType]
            if age_sec > _ROLLUP_STALENESS_SEC:
                logger.info(
                    "rollup for %s is stale (%.0fs > %ds threshold) — falling through to on-demand",
                    service,
                    age_sec,
                    _ROLLUP_STALENESS_SEC,
                )
                return None
        raw = client.download_bytes(bucket_name, blob_path)  # pyright: ignore[reportAttributeAccessIssue]
        # Worker uploaded with content_encoding=gzip. The unified
        # download_bytes returns raw bytes (no auto-decompress); we
        # decompress explicitly. The first two bytes (0x1f 0x8b) are the
        # gzip magic — defensive sniff lets us handle a future change to
        # auto-decompressing transports without churning this code.
        payload_bytes = gzip.decompress(raw) if raw[:2] == b"\x1f\x8b" else raw
        payload = json.loads(payload_bytes.decode("utf-8"))  # pyright: ignore[reportAny]
        if not isinstance(payload, dict):
            logger.warning("rollup for %s is not a dict — ignoring", service)
            return None
        _ROLLUP_CACHE[service] = (now, payload)
        return payload  # pyright: ignore[reportUnknownVariableType]
    except (OSError, ValueError, RuntimeError, json.JSONDecodeError) as exc:
        logger.info("rollup read failed for %s (%s) — falling through to on-demand", service, exc)
        return None


def clear_rollup_cache() -> None:
    """Flush the in-process rollup cache. Wired into ``/turbo/clear``."""
    _ROLLUP_CACHE.clear()


def clear_index_cache() -> None:
    """Clear the availability index cache.

    Also clears the process-level :func:`_mtds_expected_dates_cached`
    LRU. That cache is keyed only on UAC config (process-immutable in
    steady state) and so cannot serve stale manifest data, but
    ``/turbo/clear`` is a "make sure nothing is cached" knob — so we
    flush it too to keep the endpoint's contract honest.
    """
    _INDEX_CACHE.clear()
    _EXPECTED_START_DATES_CACHE["value"] = None
    _mtds_expected_dates_cached.cache_clear()


# ── expected_start_dates.yaml cached loader ────────────────────────────────
# Loaded once at first use; `clear_index_cache()` resets it (also called by
# /api/data-status/turbo/clear). The config is the SSOT for per-category
# launch dates — used to clamp category-level aggregations to
# max(user_start_date, category_start) so the Data Status page does not
# score pre-launch phantom dates as "missing".
_EXPECTED_START_DATES_CACHE: dict[str, object] = {"value": None}


def _load_expected_start_dates_cached() -> dict[str, object]:
    """Load expected_start_dates.yaml from the PM configs directory.

    Cached for process lifetime. Returns an empty dict if the file is
    missing (callers then fall back to the user-supplied start_date).
    """
    cached = _EXPECTED_START_DATES_CACHE.get("value")
    if isinstance(cached, dict):
        return cast(dict[str, object], cached)

    # Prefer app_config.get_config_dir() (respects bundled pm-configs/ + sibling
    # workspace lookup). Fall back to repo-relative path for test contexts where
    # the FastAPI app hasn't been initialised.
    from deployment_api.app_config import get_config_dir

    config_dir: object
    try:
        config_dir = get_config_dir()
    except RuntimeError:
        # Test / standalone contexts: fall back to workspace sibling
        here = Path(__file__).resolve()
        # deployment-api/deployment_api/services/data_status_service.py
        # → workspace root = parents[4]
        workspace_root = here.parents[4]
        config_dir = workspace_root / "unified-trading-pm" / "configs"

    yaml_path = config_dir / "expected_start_dates.yaml"  # pyright: ignore[reportOperatorIssue]

    if not yaml_path.exists():
        logger.warning(
            "expected_start_dates.yaml not found at %s — category aggregates will NOT clamp to launch dates",
            yaml_path,
        )
        _EXPECTED_START_DATES_CACHE["value"] = {}
        return {}

    with open(yaml_path) as f:
        raw = yaml.safe_load(f) or {}  # pyright: ignore[reportUnknownVariableType]
    if not isinstance(raw, dict):
        _EXPECTED_START_DATES_CACHE["value"] = {}
        return {}

    _EXPECTED_START_DATES_CACHE["value"] = raw
    return cast(dict[str, object], raw)


def get_effective_start_date(
    user_start_date: str,
    service: str,
    category: str,
    venue: str | None = None,
) -> str:
    """Return ``max(user_start_date, configured_launch_date)``.

    Uses `expected_start_dates.yaml` as the SSOT:
    - If ``venue`` is provided and has a venue-specific date, that wins.
    - Otherwise falls back to the service/category ``category_start``.
    - If neither is configured, returns ``user_start_date`` unchanged.

    This is used to clamp category-level aggregations so pre-launch dates
    are not counted as "missing" in the Data Status page.
    """
    cfg = _load_expected_start_dates_cached()
    svc_cfg_val = cfg.get(service)
    if not isinstance(svc_cfg_val, dict):
        return user_start_date
    svc_cfg = cast(dict[str, object], svc_cfg_val)
    cat_cfg_val = svc_cfg.get(category) or svc_cfg.get(category.upper())
    if not isinstance(cat_cfg_val, dict):
        return user_start_date
    cat_cfg = cast(dict[str, object], cat_cfg_val)

    candidate: str | None = None
    if venue is not None:
        venues_val = cat_cfg.get("venues")
        if isinstance(venues_val, dict):
            v_val = cast(dict[str, object], venues_val).get(venue)
            if isinstance(v_val, str):
                candidate = v_val

    if candidate is None:
        cat_start_val = cat_cfg.get("category_start")
        if isinstance(cat_start_val, str):
            candidate = cat_start_val

    if candidate is None:
        return user_start_date
    return max(user_start_date, candidate)


class DataStatusService(ManifestStatusMixin):
    """
    Business logic service for data status operations.

    This service handles:
    - CLI wrapper for data status commands
    - Missing shards calculation
    - Data completeness validation
    - Cross-service status aggregation

    Implementation lives in the ``deployment_api/services/data_status/``
    mixin chain; this facade composes it and owns the patchable
    module-level state above.
    """

    def __init__(self, project_id: str | None = None, deployment_env_short: str | None = None):
        """Initialize data status service."""
        self.project_id = project_id or _pid
        self.deployment_env_short = deployment_env_short or _env_short

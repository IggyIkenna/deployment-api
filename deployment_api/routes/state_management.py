# --- Completed breakdown helpers (verification/warnings/errors) ---
import asyncio
import json
import logging
import re
from datetime import UTC, date, datetime, timedelta
from pathlib import Path
from typing import cast

import yaml
from fastapi import HTTPException
from pydantic import BaseModel, Field

from deployment_api.routes.deployment_caching import (
    _verification_pending,
    get_cached_deployment_state,
)
from deployment_api.routes.deployments_helpers import (
    _deployment_config,
    _set_verification_cache,
)
from deployment_api.routes.log_analysis import analyze_deployment_logs

logger = logging.getLogger(__name__)


def _status_str(val: object) -> str:
    return val.value if hasattr(val, "value") else str(val)


def _extract_severity_and_logger(line: str) -> tuple[str, str | None]:  # noqa: C901
    """Extract the real severity and logger name from a log line.

    On VMs, ALL Python logs go to stderr, so the serial console / Ops Agent
    tags every line as ERROR regardless of the actual log level.  The raw
    line looks like:
        ERROR{"severity": "INFO", "message": "..."}
    We parse the JSON ``severity`` and ``logger`` fields when present.
    For non-JSON lines we fall back to keyword matching, using word-boundary
    checks to avoid false positives from class names like
    ``GenericErrorHandlingService`` or module paths like ``error_handling``.

    Returns:
        Tuple of (severity, logger_name_or_None)
    """
    logger_name: str | None = None

    # Try to find a JSON object with a severity field
    json_match = re.search(r'\{.*"severity"\s*:', line)
    if json_match:
        try:
            payload = cast(dict[str, object], json.loads(line[json_match.start() :]))
            sev_raw: object = payload.get("severity") or ""
            sev = (cast(str, sev_raw) if isinstance(sev_raw, str) else "").upper()
            logger_raw: object = payload.get("logger")
            logger_name = cast(str, logger_raw) if isinstance(logger_raw, str) else None
            if sev in ("ERROR", "CRITICAL", "FATAL", "ALERT", "EMERGENCY"):
                return "ERROR", logger_name
            elif sev == "WARNING":
                return "WARNING", logger_name
            elif sev in ("INFO", "DEBUG", "NOTICE", "DEFAULT"):
                return sev, logger_name
            # Unknown severity in JSON — treat as INFO
            return "INFO", logger_name
        except (json.JSONDecodeError, ValueError) as e:
            logger.debug("Suppressed %s during operation: %s", type(e).__name__, e)
            pass

    # Non-JSON line: fall back to keyword matching with word-boundary checks
    # to avoid false positives from class/module names like
    # "GenericErrorHandlingService", "error_handling.py", etc.
    if re.search(r"\bERROR\b", line) or re.search(r"\bFAILED\b", line, re.IGNORECASE):
        return "ERROR", logger_name
    elif re.search(r"\bWARNING\b", line) or re.search(r"\bWARN\b", line):
        return "WARNING", logger_name
    return "INFO", logger_name


def _extract_date_range(date_val: object) -> tuple[str | None, str | None]:
    if isinstance(date_val, dict):
        start = date_val.get("start") or None
        end = date_val.get("end") or start
        return start, end

    if not date_val:
        return None, None

    s = str(date_val)
    return s, s


def _get_state_date_range(state) -> tuple[str | None, str | None]:
    start_date = state.config.get("start_date") if hasattr(state, "config") else None
    end_date = state.config.get("end_date") if hasattr(state, "config") else None
    if start_date and end_date:
        return start_date, end_date

    # Fallback: derive from shard dimensions
    starts: list[str] = []
    ends: list[str] = []
    for shard in cast(list[object], getattr(state, "shards", []) or []):
        dims_raw: object = getattr(shard, "dimensions", None) or {}
        dims = cast(dict[str, object], dims_raw) if isinstance(dims_raw, dict) else {}
        s, e = _extract_date_range(dims.get("date"))
        if s:
            starts.append(s)
        if e:
            ends.append(e)

    if not starts or not ends:
        return None, None

    return min(starts), max(ends)


def _extract_error_warning_shard_ids(
    log_analysis: dict[str, object] | None,
) -> tuple[set[str], set[str]]:
    if not log_analysis:
        return set(), set()

    errors = log_analysis.get("errors") or []
    warnings = log_analysis.get("warnings") or []

    shard_ids_with_errors = {e.get("shard_id") for e in errors if e.get("shard_id")}
    shard_ids_with_warnings = {w.get("shard_id") for w in warnings if w.get("shard_id")}

    # Errors take precedence over warnings
    shard_ids_with_warnings -= shard_ids_with_errors

    return shard_ids_with_errors, shard_ids_with_warnings


# ---------------------------------------------------------------------------
# Shard outcome classification
# ---------------------------------------------------------------------------

# Tier 1: Job lifecycle (from shard.status + failure_category)
_INFRA_FAILURE_CATEGORIES = frozenset(
    {
        "zone_exhaustion",
        "ip_quota",
        "cpu_quota",
        "ssd_quota",
        "preemption",
    }
)
_CODE_FAILURE_CATEGORIES = frozenset(
    {
        "application_error",
        "network_error",
        "auth_error",
    }
)


def _shard_has_force(shard) -> bool:
    """Return True if --force was passed to this shard's CLI args."""
    args = getattr(shard, "args", None) or []
    return "--force" in args


def _classify_shard(  # noqa: C901
    shard: object,
    blob_exists: bool | None = None,
    blob_updated: datetime | None = None,
    has_log_errors: bool = False,
    has_log_warnings: bool = False,
) -> str:
    """Classify a single shard into a human-readable outcome category.

    Decision tree (evaluated top-to-bottom):
      1. Job lifecycle failures (PENDING/CANCELLED/FAILED subtypes)
      2. RUNNING → still in progress
      3. SUCCEEDED path:
         a. Log errors → COMPLETED_WITH_ERRORS
         b. Log warnings → COMPLETED_WITH_WARNINGS
         c. Data verification (uses blob timestamp + --force flag)
    """
    status = _status_str(getattr(shard, "status", ""))

    # --- Tier 0: non-succeeded statuses ---
    if status == "pending":
        return "NEVER_RAN"
    if status == "cancelled":
        return "CANCELLED"
    if status == "running":
        return "STILL_RUNNING"

    if status == "failed":
        fc_raw: object = getattr(shard, "failure_category", None)
        fc: str = cast(str, fc_raw) if isinstance(fc_raw, str) else ""
        fc_lower = fc.lower() if fc else ""
        if fc_lower in _INFRA_FAILURE_CATEGORIES:
            return "INFRA_FAILURE"
        if fc_lower == "timeout":
            return "TIMEOUT_FAILURE"
        if fc_lower in _CODE_FAILURE_CATEGORIES:
            return "CODE_FAILURE"
        # Unknown failure or VM died without writing status
        return "VM_DIED"

    # --- Tier 1: succeeded but check logs ---
    if has_log_errors:
        return "COMPLETED_WITH_ERRORS"
    if has_log_warnings:
        return "COMPLETED_WITH_WARNINGS"

    # --- Tier 2: data verification (blob timestamp check) ---
    if blob_exists is None:
        # Verification hasn't run / no blob data available
        return "UNVERIFIED"

    is_force = _shard_has_force(shard)
    shard_start = _parse_iso_dt(getattr(shard, "start_time", None))
    shard_end = _parse_iso_dt(getattr(shard, "end_time", None))

    if blob_exists:
        if blob_updated and shard_start and shard_end:
            # Add 60-second tolerance on both ends for clock skew / upload lag
            tolerance = 60  # seconds
            lower = shard_start - timedelta(seconds=tolerance)
            upper = shard_end + timedelta(seconds=tolerance)

            if lower <= blob_updated <= upper:
                return "VERIFIED"
            else:
                # Data exists but timestamp doesn't match the job interval
                if is_force:
                    return "DATA_STALE"
                else:
                    return "EXPECTED_SKIP"
        else:
            # Blob exists but we can't compare timestamps (missing data)
            if is_force:
                # With force, data exists → tentatively verified
                return "VERIFIED"
            else:
                return "EXPECTED_SKIP"
    else:
        # No blob at all
        return "DATA_MISSING"


def _parse_iso_dt(val: str | None) -> datetime | None:
    """Parse ISO datetime string to timezone-aware datetime, or None."""
    if not val:
        return None
    try:
        dt = datetime.fromisoformat(val.replace("Z", "+00:00"))
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=UTC)
        return dt
    except (ValueError, AttributeError):
        return None


def _build_blob_timestamp_map(
    turbo_result: dict[str, object],
) -> dict[str, dict[str, dict[str, object]]]:
    """Extract per-(category, venue, date) blob timestamps from turbo result.

    Returns: {category: {venue: {date_str: blob_updated_datetime}}}

    These timestamps are captured during the turbo GCS queries with zero
    extra API calls (blob.updated is already in the list_blobs response).
    """
    result: dict[str, dict[str, dict[str, object]]] = {}

    for cat_name, cat_data in (turbo_result.get("categories") or {} or {}).items():
        if not isinstance(cat_data, dict):
            continue
        ts_map = cat_data.get("_venue_date_blob_timestamps")
        if ts_map and isinstance(ts_map, dict):
            result[cat_name] = ts_map

    return result


def _resolve_shard_blob_data(  # noqa: C901
    state,
    existing_cat_dates: dict[str, set],
    existing_venue_dates: dict[str, dict[str, set]],
    blob_timestamps: dict[str, dict[str, dict[str, object]]],
) -> dict[str, tuple[bool, datetime | None]]:
    """Map each succeeded shard to (blob_exists, blob_updated) using turbo data.

    Uses the pre-computed existence sets (from turbo) and blob timestamps
    (from the same turbo queries). No additional GCS calls needed.

    Blob timestamps are keyed by the service's sub-dimension value:
      - market-tick-data-handler: keyed by venue (e.g. "BINANCE-SPOT")
      - instruments-service:     keyed by venue (e.g. "NYSE")
      - features-*-service:      keyed by feature_group (e.g. "technical_indicators")
      - corporate-actions:       keyed by "_all" (no sub-dimension)

    Returns: {shard_id: (blob_exists, blob_updated_or_None)}
    """
    result: dict[str, tuple[bool, datetime | None]] = {}

    for shard in cast(list[object], getattr(state, "shards", []) or []):
        if _status_str(getattr(shard, "status", "")) != "succeeded":
            continue
        sid_raw: object = getattr(shard, "shard_id", "")
        if not sid_raw:
            continue
        sid = cast(str, sid_raw)

        dims_raw: object = getattr(shard, "dimensions", None) or {}
        dims = cast(dict[str, object], dims_raw) if isinstance(dims_raw, dict) else {}
        cat = cast(str, dims.get("category") or "")
        venue_val = cast(str, dims.get("venue") or "")
        start_date, _ = _extract_date_range(dims.get("date"))

        if not cat or not start_date:
            result[sid] = (False, None)
            continue

        # Check existence using the turbo sets
        data_exists = False
        if cat in existing_cat_dates:
            if venue_val and cat in existing_venue_dates and venue_val in existing_venue_dates[cat]:
                if start_date in existing_venue_dates[cat][venue_val]:
                    data_exists = True
            else:
                if start_date in existing_cat_dates[cat]:
                    data_exists = True

        # Get blob timestamp from turbo data.
        # Blob timestamps are keyed by the sub-dimension used in the turbo query.
        # Try matching keys in priority order:
        #   1. venue (market-tick, instruments)
        #   2. feature_group / feature_type (features services)
        #   3. _all (services with no sub-dimension, e.g. corporate-actions)
        #   4. Scan all keys as last resort
        blob_updated_raw: object = None
        if data_exists and cat in blob_timestamps:
            cat_ts = blob_timestamps[cat]

            # Build list of candidate keys from shard dimensions
            # (venue, feature_group, feature_type -- whatever sub-dimension the service uses)
            candidate_keys: list[str] = []
            if venue_val:
                candidate_keys.append(venue_val)
            for dim_name in ("feature_group", "feature_type", "sub_dimension"):
                dv_raw: object = dims.get(dim_name, "")
                dv = cast(str, dv_raw) if isinstance(dv_raw, str) else ""
                if dv and dv not in candidate_keys:
                    candidate_keys.append(dv)
            candidate_keys.append("_all")

            for key in candidate_keys:
                if key in cat_ts:
                    ts: object = cat_ts[key].get(start_date)
                    if ts is not None:
                        blob_updated_raw = ts
                        break

            # Fallback: scan all keys if no candidate matched
            if blob_updated_raw is None:
                for _v_key, v_dates in cat_ts.items():
                    ts = v_dates.get(start_date)
                    if ts is not None and (
                        blob_updated_raw is None
                        or (
                            isinstance(ts, datetime)
                            and isinstance(blob_updated_raw, datetime)
                            and ts > blob_updated_raw
                        )
                    ):
                        blob_updated_raw = ts

        blob_updated: datetime | None = (
            cast(datetime, blob_updated_raw) if isinstance(blob_updated_raw, datetime) else None
        )
        result[sid] = (data_exists, blob_updated)

    return result


def _classify_all_shards(
    state,
    log_analysis: dict[str, object] | None,
    blob_data: dict[str, tuple[bool, datetime | None]] | None = None,
) -> dict[str, str]:
    """Classify every shard in a deployment into outcome categories.

    Returns dict of shard_id → classification string.
    """
    shard_ids_with_errors, shard_ids_with_warnings = _extract_error_warning_shard_ids(log_analysis)

    classifications: dict[str, str] = {}
    for shard in cast(list[object], getattr(state, "shards", []) or []):
        sid_raw: object = getattr(shard, "shard_id", "")
        if not sid_raw:
            continue
        sid = cast(str, sid_raw)

        has_errors = sid in shard_ids_with_errors
        has_warnings = sid in shard_ids_with_warnings

        blob_exists = None
        blob_updated = None
        if blob_data and sid in blob_data:
            blob_exists, blob_updated = blob_data[sid]

        classifications[sid] = _classify_shard(
            shard,
            blob_exists=blob_exists,
            blob_updated=blob_updated,
            has_log_errors=has_errors,
            has_log_warnings=has_warnings,
        )

    return classifications


def _compute_classification_counts(
    classifications: dict[str, str],
) -> dict[str, int]:
    """Aggregate per-shard classifications into counts."""
    counts: dict[str, int] = {}
    for cls in classifications.values():
        counts[cls] = counts.get(cls, 0) + 1
    return counts


def _build_existing_dates_sets(  # noqa: C901
    turbo_result: dict[str, object],
) -> tuple[dict[str, set[str]], dict[str, dict[str, set[str]]]]:
    """Build category+date and venue+date sets from turbo data status result.

    Mirrors logic used by /api/data-status/missing-shards.
    """

    existing_cat_dates: dict[str, set[str]] = {}
    existing_venue_dates: dict[str, dict[str, set[str]]] = {}

    for cat_name, cat_data in (turbo_result.get("categories") or {} or {}).items():
        if not isinstance(cat_data, dict):
            continue
        if "error" in cat_data:
            continue

        existing_cat_dates[cat_name] = set()
        existing_venue_dates[cat_name] = {}

        # Internal fast path: precomputed set
        if "_dates_set" in cat_data and isinstance(cat_data.get("_dates_set"), set):
            existing_cat_dates[cat_name] = cat_data["_dates_set"]
            continue

        # Common: explicit list of dates
        if "dates_found_list" in cat_data:
            existing_cat_dates[cat_name] = set(cat_data.get("dates_found_list") or [])
            continue

        # Venue map
        venues_data = cat_data.get("venues") or {}
        if isinstance(venues_data, dict):
            for venue_name, venue_data in venues_data.items():
                if not isinstance(venue_data, dict):
                    continue
                dates_found = venue_data.get("dates_found_list") or []
                venue_dates = set(dates_found)
                existing_venue_dates[cat_name][venue_name] = venue_dates
                existing_cat_dates[cat_name].update(venue_dates)

    return existing_cat_dates, existing_venue_dates


def _compute_verified_succeeded_shard_ids(  # noqa: C901
    state,
    existing_cat_dates: dict[str, set],
    existing_venue_dates: dict[str, dict[str, set]],
) -> set:
    verified: set[str] = set()

    for shard in cast(list[object], getattr(state, "shards", []) or []):
        if _status_str(getattr(shard, "status", "")) != "succeeded":
            continue

        dims_raw: object = getattr(shard, "dimensions", None) or {}
        dims = cast(dict[str, object], dims_raw) if isinstance(dims_raw, dict) else {}
        cat = cast(str, dims.get("category") or "")
        venue_val = cast(str, dims.get("venue") or "")
        start_date, _ = _extract_date_range(dims.get("date"))
        date_str = start_date or ""

        if not cat or not date_str:
            continue

        data_exists = False
        if cat in existing_cat_dates:
            if venue_val and cat in existing_venue_dates and venue_val in existing_venue_dates[cat]:
                if date_str in existing_venue_dates[cat][venue_val]:
                    data_exists = True
            else:
                if date_str in existing_cat_dates[cat]:
                    data_exists = True

        if data_exists:
            sid_raw: object = getattr(shard, "shard_id", "")
            if isinstance(sid_raw, str):
                verified.add(sid_raw)

    verified.discard("")
    return verified


def _compute_completed_breakdown(
    state,
    log_analysis: dict[str, object] | None,
    existing_cat_dates: dict[str, set[str]] | None = None,
    existing_venue_dates: dict[str, dict[str, set[str]]] | None = None,
) -> dict[str, object]:
    succeeded_ids: set[str] = {
        cast(str, getattr(s, "shard_id", ""))
        for s in cast(list[object], getattr(state, "shards", []) or [])
        if _status_str(getattr(s, "status", "")) == "succeeded" and getattr(s, "shard_id", None)
    }

    shard_ids_with_errors, shard_ids_with_warnings = _extract_error_warning_shard_ids(log_analysis)

    completed_with_errors = len(succeeded_ids & shard_ids_with_errors)
    completed_with_warnings = len(succeeded_ids & shard_ids_with_warnings)

    verified_clean_ids: set = set()
    if existing_cat_dates is not None and existing_venue_dates is not None:
        verified_ids = _compute_verified_succeeded_shard_ids(
            state, existing_cat_dates, existing_venue_dates
        )
        verified_clean_ids = verified_ids - shard_ids_with_errors - shard_ids_with_warnings

    completed_with_verification = len(succeeded_ids & verified_clean_ids)

    classified = (
        (succeeded_ids & shard_ids_with_errors)
        | (succeeded_ids & shard_ids_with_warnings)
        | (succeeded_ids & verified_clean_ids)
    )
    completed_other = len(succeeded_ids - classified)

    return {
        "completed_with_errors": completed_with_errors,
        "completed_with_warnings": completed_with_warnings,
        "completed_with_verification": completed_with_verification,
        "completed": completed_other,
    }


def _categories_from_state(state) -> list[str] | None:
    cats: set[str] = set()
    for s in cast(list[object], getattr(state, "shards", []) or []):
        if _status_str(getattr(s, "status", "")) != "succeeded":
            continue
        dims_raw: object = getattr(s, "dimensions", None) or {}
        dims = cast(dict[str, object], dims_raw) if isinstance(dims_raw, dict) else {}
        cat_raw: object = dims.get("category")
        if cat_raw and isinstance(cat_raw, str):
            cats.add(cat_raw)
    return sorted(cats) or None


async def _compute_and_cache_verification(
    state_manager,
    deployment_id: str,
    state,
) -> dict[str, object]:
    # Turbo data status (file existence verification) — start preparing args
    from .data_status import get_data_status_turbo_impl

    start_date, end_date = _get_state_date_range(state)
    if not start_date or not end_date:
        raise RuntimeError("Missing start_date/end_date; cannot verify output files")

    # Run log analysis and TURBO data status CONCURRENTLY.
    # Previously these ran sequentially, adding ~10s of log analysis latency
    # before the fast (~3s) TURBO queries even started.
    async def _run_log_analysis() -> dict[str, object] | None:
        try:
            result = await analyze_deployment_logs(state_manager, deployment_id, state)
            return result.get("log_analysis")
        except (OSError, ValueError, RuntimeError) as e:
            logger.warning("[VERIFY] Log analysis failed for %s: %s", deployment_id, e)
            return None

    async def _run_turbo() -> dict[str, object]:
        return await get_data_status_turbo_impl(
            service=getattr(state, "service", ""),
            start_date=start_date,
            end_date=end_date,
            category=_categories_from_state(state),
            venue=None,
            folder=None,
            data_type=None,
            include_sub_dimensions=True,
            include_dates_list=True,
            full_dates_list=True,
            first_day_of_month_only=False,
        )

    log_analysis, turbo_result = await asyncio.gather(_run_log_analysis(), _run_turbo())

    if isinstance(turbo_result, dict) and turbo_result.get("error"):
        raise RuntimeError(str(turbo_result.get("error")))

    existing_cat_dates, existing_venue_dates = _build_existing_dates_sets(turbo_result)

    # Build blob timestamp map from turbo results (zero extra API calls)
    blob_timestamps = _build_blob_timestamp_map(turbo_result)

    # Resolve per-shard blob data (existence + timestamp) from turbo data
    blob_data = _resolve_shard_blob_data(
        state, existing_cat_dates, existing_venue_dates, blob_timestamps
    )

    # Classify every shard using the full decision tree
    shard_classifications = _classify_all_shards(state, log_analysis, blob_data)
    classification_counts = _compute_classification_counts(shard_classifications)

    # Keep backward-compatible breakdown fields
    breakdown = _compute_completed_breakdown(
        state,
        log_analysis,
        existing_cat_dates=existing_cat_dates,
        existing_venue_dates=existing_venue_dates,
    )

    # Merge new classification data into the breakdown
    breakdown["shard_classifications"] = shard_classifications
    breakdown["classification_counts"] = classification_counts

    _set_verification_cache(deployment_id, breakdown)
    return breakdown


async def _run_verification_and_cache_background(deployment_id: str) -> None:
    try:
        from deployment import StateManager

        state_manager = StateManager(
            bucket_name=DEFAULT_STATE_BUCKET,
            project_id=DEFAULT_PROJECT_ID,
        )

        state = await get_cached_deployment_state(state_manager, deployment_id, force_refresh=True)
        if not state:
            return

        await _compute_and_cache_verification(state_manager, deployment_id, state)
    except (OSError, ValueError, RuntimeError) as e:
        logger.warning("[VERIFY] Background verification failed for %s: %s", deployment_id, e)
    finally:
        _verification_pending.discard(deployment_id)


# Default cloud settings from deployment config
DEFAULT_PROJECT_ID = _deployment_config.effective_project_id
DEFAULT_REGION = _deployment_config.effective_region
DEFAULT_SERVICE_ACCOUNT = (
    _deployment_config.service_account_email
    or f"instruments-service-cloud-run@{DEFAULT_PROJECT_ID}.iam.gserviceaccount.com"
)
DEFAULT_STATE_BUCKET = _deployment_config.effective_state_bucket

# Deployment concurrency defaults
DEFAULT_MAX_CONCURRENT = _deployment_config.default_max_concurrent
MAX_CONCURRENT_HARD_LIMIT = _deployment_config.max_concurrent_hard_limit


def get_all_zones_for_vm_lookup(primary_region: str | None = None) -> list[str]:
    """Get all zones in the configured region for VM lookup.

    Single-region mode: only GCS_REGION zones. VMs are created within one region
    with zone failover (1a -> 1b -> 1c).

    Args:
        primary_region: Optional; defaults to GCS_REGION from settings

    Returns:
        List of zone names (e.g., ["asia-northeast1-a", "asia-northeast1-b", "asia-northeast1-c"])
    """
    region = primary_region or DEFAULT_REGION
    return [f"{region}-a", f"{region}-b", f"{region}-c"]


# Pydantic models for request/response
class DeployRequest(BaseModel):
    """Request body for creating a deployment."""

    service: str = Field(..., description="Service name to deploy")
    compute: str = Field("cloud_run", description="Compute mode: cloud_run or vm")
    mode: str = Field(
        "batch",
        description="Deployment mode: 'batch' (one-off jobs) or 'live' (long-running services). "
        "Affects how status refresh checks completion (Jobs API vs Cloud Run Services/revisions).",
    )
    start_date: str | None = Field(
        None,
        description="Start date (YYYY-MM-DD). Optional - defaults to earliest category_start "
        "from expected_start_dates.yaml for the service.",
    )
    end_date: str | None = Field(
        None,
        description="End date (YYYY-MM-DD). Optional for 'none' date granularity services - "
        "defaults to yesterday if not provided.",
    )
    category: list[str] | None = Field(None, description="Categories to deploy")
    venue: list[str] | None = Field(None, description="Venues to deploy")
    folder: list[str] | None = Field(
        None,
        description="Folders/instrument types to deploy (e.g., 'spot', 'perpetuals'). "
        "Passed as --instrument-types CLI arg to filter within each shard.",
    )
    data_type: list[str] | None = Field(
        None,
        description="Data types to deploy (e.g., 'trades', 'book_snapshot_5'). "
        "Passed as --data-types CLI arg to filter within each shard.",
    )
    feature_group: list[str] | None = Field(None, description="Feature groups to deploy")
    timeframe: list[str] | None = Field(None, description="Timeframes to deploy")
    instrument: list[str] | None = Field(None, description="Instruments to deploy")
    target_type: list[str] | None = Field(None, description="Target types to deploy")
    domain: str | None = Field(None, description="Domain for execution services")
    force: bool = Field(False, description="Force overwrite existing data")
    dry_run: bool = Field(True, description="Preview without deploying")
    log_level: str = Field("INFO", description="Log level")
    max_workers: int | None = Field(None, description="Max parallel workers")
    max_threads: int = Field(100, description="Max concurrent threads for launching")
    respect_start_dates: bool = Field(True, description="Filter shards by venue start dates")
    region: str | None = Field(None, description="GCP region (e.g., asia-northeast1)")
    vm_zone: str | None = Field(
        None, description="GCP zone for VM deployments (e.g., asia-northeast1-a)"
    )
    extra_args: str | None = Field(
        None,
        description="Additional CLI args to pass to service (e.g., '--data-types trades')",
    )
    tag: str | None = Field(
        None,
        description="Human-readable description/annotation for this deployment (e.g., 'Fixed Curve adapter')",  # noqa: E501
    )
    cloud_config_path: str | None = Field(
        None,
        description="Cloud storage path to config directory (gs://... or s3://...) for dynamic config discovery",  # noqa: E501
    )
    skip_venue_sharding: bool = Field(
        False,
        description="Skip venue as a sharding dimension. All venues in selected categories "
        "will be processed in a single shard per date. Reduces job count significantly "
        "but requires larger machines (auto-scaled based on max_workers).",
    )
    skip_feature_group_sharding: bool = Field(
        False,
        description="Skip feature_group as a sharding dimension. All feature groups "
        "will be processed in a single shard per date. Reduces job count for feature services.",
    )
    date_granularity: str | None = Field(
        None,
        description="Override date granularity (daily, weekly, monthly, none). "
        "This is a runtime override that does not modify the service config. "
        "weekly = 7-day chunks, monthly = 30-day chunks, none = no date sharding "
        "(single shard, no start/end date passed to service). Reduces job count.",
    )
    max_concurrent: int | None = Field(
        None,
        description="Max simultaneously running jobs/VMs. If total shards exceeds this, rolling launch is used. "  # noqa: E501
        "Default: 2000. Hard limit: 2500.",
    )
    include_all_shards: bool = Field(
        False,
        description="If true, dry run response will include all shards (not just first 50). Use with caution for large deployments.",  # noqa: E501
    )
    deploy_missing_only: bool = Field(
        False,
        description="If true, use backend to calculate missing shards (more accurate than exclude_dates). "  # noqa: E501
        "This fetches full date lists from GCS to determine what data exists, avoiding the "
        "truncation issue with exclude_dates passed from frontend.",
    )
    first_day_of_month_only: bool = Field(
        False,
        description="If true, only generate shards for the first day of each month. "
        "Useful for TARDIS free tier (no API key required for first day of month).",
    )
    exclude_dates: dict[str, object] | None = Field(
        None,
        description="Dates to exclude. Supports two formats: "
        "(1) Category-level: {'CEFI': ['2024-01-01', ...]} - excludes all shards for those category+date combos. "  # noqa: E501
        "(2) Venue-level: {'CEFI': {'BINANCE-SPOT': ['2024-01-01', ...], 'UPBIT': [...]}} - "
        "excludes only specific category+venue+date combos. "
        "Venue-level format enables precise 'deploy missing' for services with venue sharding.",
    )


_FALLBACK_START_DATE = "2020-01-01"


def _get_service_earliest_start(service: str, config_dir: str) -> str:
    """Look up the earliest category_start for a service from expected_start_dates.yaml.

    Returns the earliest category_start date string, or _FALLBACK_START_DATE if not found.
    """
    try:
        start_dates_path = Path(config_dir) / "expected_start_dates.yaml"
        if not start_dates_path.exists():
            return _FALLBACK_START_DATE

        with open(start_dates_path) as f:
            data = yaml.safe_load(f) or {}

        service_config = data.get(service)
        if not service_config or not isinstance(service_config, dict):
            return _FALLBACK_START_DATE

        # Find earliest category_start across all categories for this service
        earliest = None
        for _cat_name, cat_data in service_config.items():
            if not isinstance(cat_data, dict):
                continue
            cat_start = cat_data.get("category_start")
            if cat_start and (earliest is None or str(cat_start) < earliest):
                earliest = str(cat_start)

        return earliest or _FALLBACK_START_DATE
    except (OSError, ValueError, RuntimeError):
        return _FALLBACK_START_DATE


def _resolve_deploy_dates(
    deploy_request: "DeployRequest", config_dir: str = "configs"
) -> tuple[str, str]:
    """Resolve effective start_date and end_date for a deployment request.

    When start_date/end_date are omitted, defaults are:
      - start_date: earliest category_start from expected_start_dates.yaml for the service
      - end_date: yesterday

    Returns (start_date_str, end_date_str) guaranteed to be valid YYYY-MM-DD.
    """
    yesterday = (date.today() - timedelta(days=1)).isoformat()

    start = deploy_request.start_date or _get_service_earliest_start(
        deploy_request.service, config_dir
    )
    end = deploy_request.end_date or yesterday

    # Validate format
    try:
        datetime.strptime(start, "%Y-%m-%d").replace(tzinfo=UTC)
        datetime.strptime(end, "%Y-%m-%d").replace(tzinfo=UTC)
    except ValueError as e:
        raise HTTPException(status_code=400, detail=f"Invalid date format: {e}") from e

    return start, end


class ShardInfo(BaseModel):
    """Information about a single shard."""

    shard_id: str
    dimensions: dict[str, object]
    cli_args: list[str]


class ShardPreview(BaseModel):
    """Preview of shards that would be created."""

    service: str
    total_shards: int
    shards: list[ShardInfo]
    summary: dict[str, object]
    cli_command: str
    dry_run: bool = True


class DeploymentResult(BaseModel):
    """Result of a deployment."""

    deployment_id: str
    service: str
    status: str
    total_shards: int
    compute_mode: str
    started_at: str
    dry_run: bool


class DeploymentSummary(BaseModel):
    """Summary of a deployment."""

    deployment_id: str
    service: str
    status: str
    total_shards: int
    completed_shards: int
    failed_shards: int
    progress_percentage: float
    started_at: str | None
    compute_mode: str

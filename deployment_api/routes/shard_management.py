"""
Shard management utilities for deployments.

Contains functions for classifying, verifying, and managing deployment shards,
including data verification and status classification logic.
"""

import logging
import re
from datetime import UTC, datetime, timedelta
from typing import cast

logger = logging.getLogger(__name__)

# Infrastructure failure categories
_INFRA_FAILURE_CATEGORIES = {
    "infra_failure",
    "resource_unavailable",
    "quota_exceeded",
    "network_error",
    "preempted",
    "spot_terminated",
}

# Code/application failure categories
_CODE_FAILURE_CATEGORIES = {
    "code_failure",
    "application_error",
    "invalid_input",
    "data_error",
    "config_error",
}


def _status_str(val: object) -> str:
    """Convert various status representations to string."""
    if isinstance(val, str):
        return val
    elif isinstance(val, dict):
        return val.get("status", "unknown")
    elif hasattr(val, "status"):
        return str(val.status)
    else:
        return str(val)


def _shard_has_force(shard: object) -> bool:
    """Return True if --force was passed to this shard's CLI args."""
    args = getattr(shard, "args", None) or []
    return "--force" in args


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


def _extract_date_range(date_val: object) -> tuple[str | None, str | None]:
    """
    Extract start and end date from various date specifications.

    Handles:
    - Single date: "2024-01-01" -> ("2024-01-01", "2024-01-01")
    - Date range: "2024-01-01,2024-01-31" -> ("2024-01-01", "2024-01-31")
    - Date range: "2024-01-01 to 2024-01-31" -> ("2024-01-01", "2024-01-31")
    - Relative: "last-7-days" -> computed range
    """
    if not date_val:
        return None, None

    date_str = str(date_val).strip()

    # Handle comma-separated range
    if "," in date_str:
        parts = [p.strip() for p in date_str.split(",", 1)]
        return parts[0] if parts[0] else None, parts[1] if len(parts) > 1 and parts[1] else None

    # Handle "to" separated range
    if " to " in date_str:
        parts = [p.strip() for p in date_str.split(" to ", 1)]
        return parts[0] if parts[0] else None, parts[1] if len(parts) > 1 and parts[1] else None

    # Handle relative dates
    if date_str.startswith("last-") and date_str.endswith("-days"):
        try:
            days = int(date_str.replace("last-", "").replace("-days", ""))
            end_date = datetime.now(UTC).strftime("%Y-%m-%d")
            start_date = (datetime.now(UTC) - timedelta(days=days - 1)).strftime("%Y-%m-%d")
            return start_date, end_date
        except ValueError as e:
            logger.debug("Suppressed %s during operation: %s", type(e).__name__, e)
            pass

    # Single date - treat as single day range
    if re.match(r"\d{4}-\d{2}-\d{2}", date_str):
        return date_str, date_str

    return None, None


def _extract_error_warning_shard_ids(log_analysis: dict | None) -> tuple[set, set]:
    """Extract shard IDs that have log errors or warnings."""
    if not log_analysis or not isinstance(log_analysis, dict):
        return set(), set()

    errors = log_analysis.get("errors") or []
    warnings = log_analysis.get("warnings") or []

    error_shard_ids = {e.get("shard_id") for e in errors if e.get("shard_id")}
    warning_shard_ids = {w.get("shard_id") for w in warnings if w.get("shard_id")}

    error_shard_ids.discard(None)
    warning_shard_ids.discard(None)

    return error_shard_ids, warning_shard_ids


def _classify_shard(
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


def _build_blob_timestamp_map(
    turbo_result: dict,
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


def _resolve_shard_blob_data(
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
        sid: object = getattr(shard, "shard_id", "")
        if not sid:
            continue

        dims: object = getattr(shard, "dimensions", None) or {}
        dims_dict = cast(dict[str, object], dims) if isinstance(dims, dict) else {}
        cat = cast(str, dims_dict.get("category") or "")
        venue_val = cast(str, dims_dict.get("venue") or "")
        start_date, _ = _extract_date_range(dims_dict.get("date"))

        sid_str = cast(str, sid)
        if not cat or not start_date:
            result[sid_str] = (False, None)
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
                dv_raw: object = dims_dict.get(dim_name, "")
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

            # Fallback: scan all sub-dimension keys if the above didn't work
            if blob_updated_raw is None:
                for subdim_key, subdim_data in cat_ts.items():
                    if subdim_key not in candidate_keys and isinstance(subdim_data, dict):
                        ts = subdim_data.get(start_date)
                        if ts is not None:
                            blob_updated_raw = ts
                            break

        blob_updated: datetime | None = cast(datetime, blob_updated_raw) if isinstance(blob_updated_raw, datetime) else None
        result[sid_str] = (data_exists, blob_updated)

    return result


def _classify_all_shards(
    state,
    log_analysis: dict | None,
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


def _build_existing_dates_sets(
    turbo_result: dict,
) -> tuple[dict[str, set], dict[str, dict[str, set]]]:
    """Build category+date and venue+date sets from turbo data status result.

    Mirrors logic used by /api/data-status/missing-shards.
    """

    existing_cat_dates: dict[str, set] = {}
    existing_venue_dates: dict[str, dict[str, set]] = {}

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


def _compute_verified_succeeded_shard_ids(
    state,
    existing_cat_dates: dict[str, set],
    existing_venue_dates: dict[str, dict[str, set]],
) -> set:
    """Compute set of shard IDs that succeeded and have verified data."""
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
    log_analysis: dict | None,
    existing_cat_dates: dict[str, set] | None = None,
    existing_venue_dates: dict[str, dict[str, set]] | None = None,
) -> dict:
    """Compute detailed breakdown of completed shards by status and verification."""
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
        verified_ids = _compute_verified_succeeded_shard_ids(state, existing_cat_dates, existing_venue_dates)
        verified_clean_ids = verified_ids - shard_ids_with_errors - shard_ids_with_warnings

    verified_clean = len(verified_clean_ids)

    return {
        "completed_with_errors": completed_with_errors,
        "completed_with_warnings": completed_with_warnings,
        "verified_clean": verified_clean,
        "verified_clean_ids": list(verified_clean_ids),
        "succeeded_ids": list(succeeded_ids),
        "error_shard_ids": list(shard_ids_with_errors),
        "warning_shard_ids": list(shard_ids_with_warnings),
    }


def get_all_zones_for_vm_lookup(primary_region: str | None = None) -> list[str]:
    """
    Get all zones to search for VMs during lookup operations.

    Returns zones in priority order: primary region first, then failover regions.
    This ensures we find VMs efficiently while supporting cross-region deployments.
    """
    from deployment_api import settings as _settings

    zones = []

    # Add primary region zones first
    region = primary_region or _settings.GCS_REGION or "us-central1"
    if region == "us-central1":
        zones.extend(["us-central1-a", "us-central1-b", "us-central1-c"])
    elif region == "us-east1":
        zones.extend(["us-east1-b", "us-east1-c", "us-east1-d"])
    elif region == "europe-west1":
        zones.extend(["europe-west1-b", "europe-west1-c", "europe-west1-d"])

    # Add all failover regions
    for failover_region in _settings.ALL_FAILOVER_REGIONS:
        if failover_region != region:  # Don't duplicate primary
            if failover_region == "us-central1":
                zones.extend(["us-central1-a", "us-central1-b", "us-central1-c"])
            elif failover_region == "us-east1":
                zones.extend(["us-east1-b", "us-east1-c", "us-east1-d"])
            elif failover_region == "europe-west1":
                zones.extend(["europe-west1-b", "europe-west1-c", "europe-west1-d"])

    # Remove duplicates while preserving order
    seen = set()
    unique_zones = []
    for zone in zones:
        if zone not in seen:
            seen.add(zone)
            unique_zones.append(zone)

    return unique_zones


def _categories_from_state(state) -> list[str] | None:
    """Extract unique categories from deployment state shards."""
    if not state or not hasattr(state, "shards"):
        return None

    categories: set[str] = set()
    for shard in cast(list[object], getattr(state, "shards", []) or []):
        dims_raw: object = getattr(shard, "dimensions", None) or {}
        dims = cast(dict[str, object], dims_raw) if isinstance(dims_raw, dict) else {}
        cat_raw: object = dims.get("category")
        if cat_raw and isinstance(cat_raw, str):
            categories.add(cat_raw)

    return sorted(categories) if categories else None


def _get_state_date_range(state) -> tuple[str | None, str | None]:
    """Extract the date range from deployment state by examining all shards."""
    if not state or not hasattr(state, "shards"):
        return None, None

    all_dates: list[str] = []
    for shard in cast(list[object], getattr(state, "shards", []) or []):
        dims_raw: object = getattr(shard, "dimensions", None) or {}
        dims = cast(dict[str, object], dims_raw) if isinstance(dims_raw, dict) else {}
        start_date, end_date = _extract_date_range(dims.get("date"))
        if start_date:
            all_dates.append(start_date)
        if end_date and end_date != start_date:
            all_dates.append(end_date)

    if not all_dates:
        return None, None

    all_dates.sort()
    return all_dates[0], all_dates[-1]

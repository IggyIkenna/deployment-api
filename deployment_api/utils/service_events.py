"""
Service event parsing and shard state management utilities.

Handles parsing of SERVICE_EVENT messages from logs and updating shard states accordingly.
"""

import logging
import re
from datetime import UTC, datetime
from typing import cast

logger = logging.getLogger(__name__)


def parse_service_event(log_line: str) -> dict[str, object] | None:
    """Parse standardized service event from log line.

    Serial console lines often have a prefix (e.g. "[ 34.18] docker[1207]: ")
    before SERVICE_EVENT, so we search for the pattern anywhere in the line.

    Args:
        log_line: Single line from serial console logs

    Returns:
        dict with event_name, details, timestamp (if SERVICE_EVENT found)
        None otherwise
    """
    match = re.search(
        r"SERVICE_EVENT:\s+([A-Za-z0-9_]+)(?:\s+\(([^)]*)\))?",
        log_line.strip(),
    )
    if not match:
        return None
    event_name, details = match.groups()
    return {
        "event_name": event_name,
        "details": details or "",
        "timestamp": datetime.now(UTC),
    }


def update_shard_state_from_event(  # noqa: C901
    shard_state: dict[str, object],
    event: dict[str, object],
) -> dict[str, object]:
    """Update shard state based on parsed event.

    Args:
        shard_state: Current shard state (one shard from shards array)
        event: Parsed event dict from parse_service_event()

    Returns:
        Updated shard state
    """
    event_name = cast(str, event["event_name"])
    details: object = event["details"]
    timestamp = cast(datetime, event["timestamp"])

    if "stage_timings" not in shard_state or shard_state["stage_timings"] is None:
        shard_state["stage_timings"] = {}

    stage_timings = cast(dict[str, object], shard_state["stage_timings"])
    _details_dict: dict[str, object] = (
        cast(dict[str, object], details) if isinstance(details, dict) else {}
    )
    _is_validation_failed: bool = (
        event_name == "FAILED" and _details_dict.get("error_category") == "validation"
    )
    if event_name in [
        "VALIDATION_STARTED",
        "VALIDATION_COMPLETED",
        "VALIDATION_FAILED",
        "FAILED",
    ] and (event_name not in ("FAILED",) or _is_validation_failed):
        if event_name == "VALIDATION_STARTED":
            shard_state["current_stage"] = "validation"
            shard_state["stage_started_at"] = timestamp.isoformat()
            shard_state["status"] = "initializing"
        elif event_name == "VALIDATION_COMPLETED":
            started = cast(str, shard_state.get("stage_started_at"))
            if started:
                try:
                    start_dt = datetime.fromisoformat(started.replace("Z", "+00:00"))
                    if start_dt.tzinfo is None:
                        start_dt = start_dt.replace(tzinfo=UTC)
                    stage_timings["validation"] = (timestamp - start_dt).total_seconds()
                except (ValueError, TypeError) as e:
                    logger.debug("Suppressed %s during operation: %s", type(e).__name__, e)
        elif event_name == "VALIDATION_FAILED" or _is_validation_failed:
            shard_state["status"] = "failed"
            shard_state["failure_category"] = "validation_failed"

    elif event_name in ["DATA_INGESTION_STARTED", "DATA_INGESTION_COMPLETED"]:
        if event_name == "DATA_INGESTION_STARTED":
            shard_state["current_stage"] = "ingestion"
            shard_state["stage_started_at"] = timestamp.isoformat()
            shard_state["status"] = "running"
        elif event_name == "DATA_INGESTION_COMPLETED":
            started = cast(str, shard_state.get("stage_started_at"))
            if started:
                try:
                    start_dt = datetime.fromisoformat(started.replace("Z", "+00:00"))
                    if start_dt.tzinfo is None:
                        start_dt = start_dt.replace(tzinfo=UTC)
                    stage_timings["ingestion"] = (timestamp - start_dt).total_seconds()
                except (ValueError, TypeError) as e:
                    logger.debug("Suppressed %s during operation: %s", type(e).__name__, e)
            shard_state["stage_details"] = details

    elif event_name in ["PROCESSING_STARTED", "PROCESSING_COMPLETED"]:
        if event_name == "PROCESSING_STARTED":
            shard_state["current_stage"] = "processing"
            shard_state["stage_started_at"] = timestamp.isoformat()
        elif event_name == "PROCESSING_COMPLETED":
            started = cast(str, shard_state.get("stage_started_at"))
            if started:
                try:
                    start_dt = datetime.fromisoformat(started.replace("Z", "+00:00"))
                    if start_dt.tzinfo is None:
                        start_dt = start_dt.replace(tzinfo=UTC)
                    stage_timings["processing"] = (timestamp - start_dt).total_seconds()
                except (ValueError, TypeError) as e:
                    logger.debug("Suppressed %s during operation: %s", type(e).__name__, e)

    elif event_name == "DATA_BROADCAST":
        shard_state["current_stage"] = "broadcasting"

    elif event_name in ["PERSISTENCE_STARTED", "PERSISTENCE_COMPLETED"]:
        if event_name == "PERSISTENCE_STARTED":
            shard_state["current_stage"] = "persistence"
            shard_state["stage_started_at"] = timestamp.isoformat()
        elif event_name == "PERSISTENCE_COMPLETED":
            started = cast(str, shard_state.get("stage_started_at"))
            if started:
                try:
                    start_dt = datetime.fromisoformat(started.replace("Z", "+00:00"))
                    if start_dt.tzinfo is None:
                        start_dt = start_dt.replace(tzinfo=UTC)
                    stage_timings["persistence"] = (timestamp - start_dt).total_seconds()
                except (ValueError, TypeError) as e:
                    logger.debug("Suppressed %s during operation: %s", type(e).__name__, e)

    elif event_name == "STOPPED":
        shard_state["status"] = "completed"
        shard_state["current_stage"] = "completed"
        shard_state["progress"] = 100

    elif event_name == "FAILED":
        shard_state["status"] = "failed"
        shard_state["failure_category"] = "service_failed"
        shard_state["stage_details"] = details

    # Parse progress counters from details (e.g., "BTC-USDT-SWAP (5/325)" or "2025-01-01 (1/30)")
    details_str: str = str(details) if not isinstance(details, str) else details  # type: ignore[arg-type]  # details is object
    progress_match = re.search(r"\((\d+)/(\d+)\)", details_str)
    if progress_match:
        _grps = progress_match.groups()
        current_s: str = str(_grps[0]) if _grps else ""
        total_s: str = str(_grps[1]) if len(_grps) > 1 else ""
        try:
            current_val = int(current_s)
            total_val = int(total_s)
            shard_state["progress_current"] = current_val
            shard_state["progress_total"] = total_val
            shard_state["progress_message"] = f"{current_val}/{total_val}"
            if total_val > 0:
                shard_state["progress"] = int((current_val / total_val) * 100)
        except ValueError as e:
            logger.debug("Suppressed %s during operation: %s", type(e).__name__, e)

    shard_state["stage_details"] = details
    return shard_state

"""
Service utility functions for event parsing and state management.

Contains functions for parsing service events from logs and updating shard states.
"""

import logging
import re
from datetime import UTC, datetime
from pathlib import Path
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


def _get_started_str(shard_state: dict[str, object]) -> str | None:
    """Extract stage_started_at as str from shard_state, or None."""
    raw: object = shard_state.get("stage_started_at")
    if isinstance(raw, str):
        return raw
    return None


def _get_stage_timings(shard_state: dict[str, object]) -> dict[str, object]:
    """Return the stage_timings dict from shard_state, creating it if absent."""
    raw: object = shard_state.get("stage_timings")
    if isinstance(raw, dict):
        return cast(dict[str, object], raw)
    result: dict[str, object] = {}
    shard_state["stage_timings"] = result
    return result


def update_shard_state_from_event(  # noqa: C901
    shard_state: dict[str, object], event: dict[str, object]
) -> dict[str, object]:
    """Update shard state based on parsed event.

    Args:
        shard_state: Current shard state (one shard from shards array)
        event: Parsed event dict from parse_service_event()

    Returns:
        Updated shard state
    """
    event_name_raw: object = event["event_name"]
    event_name: str = event_name_raw if isinstance(event_name_raw, str) else ""
    details_raw: object = event.get("details")
    details: str = details_raw if isinstance(details_raw, str) else ""
    timestamp_raw: object = event.get("timestamp")
    timestamp = timestamp_raw if isinstance(timestamp_raw, datetime) else datetime.now(UTC)

    # Ensure stage_timings exists
    stage_timings = _get_stage_timings(shard_state)

    # FAILED alone → service_failed; only VALIDATION_FAILED → validation_failed
    _is_validation_failed = False
    if event_name in [
        "VALIDATION_STARTED",
        "VALIDATION_COMPLETED",
        "VALIDATION_FAILED",
    ] and (event_name not in ("FAILED",) or _is_validation_failed):
        if event_name == "VALIDATION_STARTED":
            shard_state["current_stage"] = "validation"
            shard_state["stage_started_at"] = timestamp.isoformat()
            shard_state["status"] = "initializing"
        elif event_name == "VALIDATION_COMPLETED":
            started = _get_started_str(shard_state)
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
            started = _get_started_str(shard_state)
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
            started = _get_started_str(shard_state)
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
            started = _get_started_str(shard_state)
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
    progress_match = re.search(r"\((\d+)/(\d+)\)", details)
    if progress_match:
        current_s, total_s = progress_match.groups()
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


def get_config_dir() -> Path:
    """Get the configs directory path."""
    # Try relative to this file
    api_dir = Path(__file__).parent.parent  # Go up from utils to api
    repo_root = api_dir.parent
    configs_dir = repo_root / "configs"

    if configs_dir.exists():
        return configs_dir

    raise RuntimeError(f"Could not find configs directory at {configs_dir}")


def get_ui_dist_dir() -> Path | None:
    """Get the UI dist directory if it exists (for production serving)."""
    api_dir = Path(__file__).parent.parent  # Go up from utils to api
    repo_root = api_dir.parent
    ui_dist = repo_root / "ui" / "dist"

    if ui_dist.exists() and (ui_dist / "index.html").exists():
        return ui_dist
    return None

"""GET /api/vm/events — live VM lifecycle event tail.

Reads structured events from
`gs://{gcp_project_id}-events/events/{service}/{YYYY-MM-DD}/{vm_name}/hour={H}/*.jsonl`
per the SSOT in `unified-trading-library/feature_service_base/base_service.py:159`
and `event_sink.py` GcsEventSink. Each blob holds ONE JSON record:

  {
    "event": "STARTED",
    "service": "instruments-service",
    "timestamp": "2026-05-07T00:29:14Z",
    "metadata": {
      "service_name": "instruments-service",
      "severity": "INFO",
      "details": {"correlation_id": "abc-123", ...}
    }
  }

Plan: `deployment_api_work_stream_a_2026_05_07.plan.md` Phase 2.B.

V1 inline `_PREFIX_TO_SERVICE` mapping; lifted to UAC by the same successor
plan as `backfill_launch.py`'s `_TASK_TO_LAUNCHER`.
"""

from __future__ import annotations

import base64
import json
import logging
from datetime import UTC, datetime
from typing import cast

from fastapi import APIRouter, HTTPException, Query
from unified_api_contracts.internal import (
    VMEventListResult,
    VMLifecycleEvent,
)
from unified_trading_library.cloud_interface import get_storage_client
from unified_trading_library.events import log_event

from deployment_api.deployment_api_config import DeploymentApiConfig

router = APIRouter()
logger = logging.getLogger(__name__)

_cfg = DeploymentApiConfig()

_DEFAULT_PAGE_SIZE = 1000
_MAX_PAGE_SIZE = 5000

# Severity ordering so a `severity_floor` query param can filter by ≥.
# Matches `unified_trading_library.events` log_event() severity vocabulary.
_SEVERITY_ORDER: dict[str, int] = {
    "DEBUG": 0,
    "INFO": 1,
    "WARNING": 2,
    "ERROR": 3,
    "CRITICAL": 4,
}


# v1 inline prefix → service mapping. The events-bucket layout partitions by
# service-name, but operators address VMs by vm_name. Most callers know the
# service explicitly + pass `service=` directly; this map provides a default
# inference for the common case where they don't.
_PREFIX_TO_SERVICE: dict[str, str] = {
    "cefi-": "market-tick-data-service",
    "tradfi-": "market-tick-data-service",
    "defi-": "market-tick-data-service",
    "prediction-": "market-tick-data-service",
    "mtds-": "market-tick-data-service",
    "mdps-": "market-data-processing-service",
    "mdps-backfill-": "market-data-processing-service",
    "canonical-migration-": "market-tick-data-service",
    "fs-backfill-": "instruments-service",
    "af-backfill-": "instruments-service",
    "af-audit-": "instruments-service",
    "af-recover-": "instruments-service",
    "tm-backfill-": "instruments-service",
    "sfi-backfill-": "instruments-service",
    "us-backfill-": "instruments-service",
    "weather-backfill-": "instruments-service",
    "fill-missing-player-stats-": "instruments-service",
    "features-": "features-volatility-service",
}


def _infer_service_from_vm_name(vm_name: str) -> str:
    """Best-effort service inference. Longest-prefix match wins."""
    matches = [(p, s) for p, s in _PREFIX_TO_SERVICE.items() if vm_name.startswith(p)]
    if not matches:
        raise HTTPException(
            status_code=400,
            detail={
                "code": "SERVICE_INFERENCE_FAILED",
                "message": (
                    f"Could not infer service from vm_name '{vm_name}'. "
                    "Pass the `service=` query param explicitly, or register "
                    "the prefix in routes/vm_events.py:_PREFIX_TO_SERVICE."
                ),
                "vm_name": vm_name,
            },
        )
    matches.sort(key=lambda pair: len(pair[0]), reverse=True)
    return matches[0][1]


def _normalise_date(date: str | None) -> str:
    """Default to today UTC; validate YYYY-MM-DD shape."""
    if date is None:
        return datetime.now(UTC).strftime("%Y-%m-%d")
    try:
        datetime.strptime(date, "%Y-%m-%d").replace(tzinfo=UTC)
    except ValueError as exc:
        raise HTTPException(
            status_code=400,
            detail={
                "code": "BAD_DATE",
                "message": f"date '{date}' is not in YYYY-MM-DD shape",
            },
        ) from exc
    return date


def _normalise_hour_range(from_hour: int | None, to_hour: int | None, date: str) -> tuple[int, int]:
    """Default hour range. For today: 0..current_hour. For past dates: 0..23."""
    today = datetime.now(UTC).strftime("%Y-%m-%d")
    is_today = date == today
    default_to = datetime.now(UTC).hour if is_today else 23
    start = 0 if from_hour is None else from_hour
    end = default_to if to_hour is None else to_hour
    if not (0 <= start <= 23) or not (0 <= end <= 23) or start > end:
        raise HTTPException(
            status_code=400,
            detail={
                "code": "BAD_HOUR_RANGE",
                "message": (
                    f"hour range invalid: from_hour={from_hour}, to_hour={to_hour}. "
                    "Both must be in [0, 23] inclusive and from_hour <= to_hour."
                ),
            },
        )
    return start, end


def _normalise_severity(severity_floor: str | None) -> int:
    """Translate severity floor to numeric threshold. None → INFO."""
    floor = (severity_floor or "INFO").upper()
    if floor not in _SEVERITY_ORDER:
        raise HTTPException(
            status_code=400,
            detail={
                "code": "BAD_SEVERITY",
                "message": (
                    f"severity_floor '{severity_floor}' must be one of "
                    f"{sorted(_SEVERITY_ORDER.keys())}"
                ),
            },
        )
    return _SEVERITY_ORDER[floor]


def _parse_event_jsonl(blob_bytes: bytes, blob_name: str) -> VMLifecycleEvent | None:
    """Parse one blob's JSONL content into a VMLifecycleEvent.

    Per CLAUDE.md "Shard-level failure isolation", parse failures emit an
    EVENT_PARSE_FAILED log event and return None — the request continues
    with the remaining blobs.
    """
    try:
        text = blob_bytes.decode().strip()
        if not text:
            return None
        # Each blob holds ONE record (per GcsEventSink.write_event); but be
        # defensive — handle multi-line JSONL too. Take the first line.
        first_line = text.split("\n", 1)[0]
        row: object = json.loads(first_line)
    except (json.JSONDecodeError, UnicodeDecodeError) as exc:
        log_event(
            "EVENT_PARSE_FAILED",
            severity="WARNING",
            details={"blob_name": blob_name, "error": str(exc)},
        )
        return None
    if not isinstance(row, dict):
        log_event(
            "EVENT_PARSE_FAILED",
            severity="WARNING",
            details={"blob_name": blob_name, "error": "row is not a dict"},
        )
        return None

    row_dict = cast(dict[str, object], row)
    event = str(row_dict.get("event", ""))
    service = str(row_dict.get("service", ""))
    timestamp_raw = row_dict.get("timestamp")
    if not isinstance(timestamp_raw, str):
        log_event(
            "EVENT_PARSE_FAILED",
            severity="WARNING",
            details={"blob_name": blob_name, "error": "missing or non-str timestamp"},
        )
        return None
    try:
        timestamp = datetime.fromisoformat(timestamp_raw.replace("Z", "+00:00"))
    except ValueError as exc:
        log_event(
            "EVENT_PARSE_FAILED",
            severity="WARNING",
            details={"blob_name": blob_name, "error": f"bad timestamp: {exc}"},
        )
        return None

    metadata_raw = row_dict.get("metadata", {})
    metadata = cast(dict[str, object], metadata_raw) if isinstance(metadata_raw, dict) else {}
    severity = str(metadata.get("severity", "INFO")).upper()
    if severity not in _SEVERITY_ORDER:
        severity = "INFO"

    details_raw = metadata.get("details", {})
    details_dict: dict[str, str] = {}
    if isinstance(details_raw, dict):
        for key, value in cast(dict[str, object], details_raw).items():
            details_dict[str(key)] = "" if value is None else str(value)

    correlation_id = details_dict.get("correlation_id") or None

    return VMLifecycleEvent(
        event=event,
        service=service,
        timestamp=timestamp,
        severity=severity,
        correlation_id=correlation_id,
        details=details_dict,
        raw_metadata=metadata,
    )


def _encode_page_token(blob_name: str) -> str:
    return base64.urlsafe_b64encode(blob_name.encode()).decode()


def _decode_page_token(token: str) -> str:
    try:
        return base64.urlsafe_b64decode(token.encode()).decode()
    except (ValueError, UnicodeDecodeError) as exc:
        raise HTTPException(
            status_code=400,
            detail={
                "code": "BAD_PAGE_TOKEN",
                "message": f"page_token is malformed: {exc}",
            },
        ) from exc


def _mock_events(vm_name: str, service: str, date: str) -> VMEventListResult:
    """Synthesize a 3-event preview so the UI smoke render works without GCS."""
    base_ts = datetime.fromisoformat(f"{date}T00:00:00+00:00")
    events = [
        VMLifecycleEvent(
            event="STARTED",
            service=service,
            timestamp=base_ts,
            severity="INFO",
            correlation_id="mock-correlation-id",
            details={"correlation_id": "mock-correlation-id", "vm_name": vm_name},
            raw_metadata={"severity": "INFO"},
        ),
        VMLifecycleEvent(
            event="INSTRUMENT_PROCESSED",
            service=service,
            timestamp=base_ts,
            severity="INFO",
            correlation_id="mock-correlation-id",
            details={"correlation_id": "mock-correlation-id", "rows": "1440"},
            raw_metadata={"severity": "INFO"},
        ),
        VMLifecycleEvent(
            event="STOPPED",
            service=service,
            timestamp=base_ts,
            severity="INFO",
            correlation_id="mock-correlation-id",
            details={"correlation_id": "mock-correlation-id", "exit_code": "0"},
            raw_metadata={"severity": "INFO"},
        ),
    ]
    return VMEventListResult(
        vm_name=vm_name,
        service=service,
        date=date,
        hours_scanned=[0],
        total_events=len(events),
        events=events,
        truncated=False,
        next_page_token=None,
    )


def _resolve_events_bucket() -> str:
    """`gs://{pid}-events` per UTL feature_service_base/base_service.py:159."""
    project_id = _cfg.gcp_project_id or "unknown"
    return f"{project_id}-events"


def _list_event_blobs_in_hour(
    storage,  # StorageClient — typed as object to avoid imports leaking
    bucket: str,
    service: str,
    date: str,
    vm_name: str,
    hour: int,
) -> list[str]:
    """List blob names under the (vm, hour) partition. Sorted asc by name
    (= chronological since `{ts_us}_{seq}` is lexicographically sortable)."""
    prefix = f"events/{service}/{date}/{vm_name}/hour={hour:02d}/"
    blobs = list(storage.list_blobs(bucket=bucket, prefix=prefix))
    names = [blob.name for blob in blobs if blob.name.endswith(".jsonl")]
    names.sort()
    return names


@router.get("/events", response_model=VMEventListResult)
def list_vm_events(
    vm_name: str = Query(..., description="VM name (required)"),
    service: str | None = Query(
        None,
        description="Service name partition. Inferred from vm_name prefix if omitted.",
    ),
    date: str | None = Query(None, description="YYYY-MM-DD (default: today UTC)"),
    from_hour: int | None = Query(None, ge=0, le=23, description="Inclusive (default: 0)"),
    to_hour: int | None = Query(
        None, ge=0, le=23, description="Inclusive (default: today=current hour, else 23)"
    ),
    severity_floor: str | None = Query(
        None,
        description="INFO|WARNING|ERROR|CRITICAL — drop events below floor (default: INFO)",
    ),
    page_size: int = Query(
        _DEFAULT_PAGE_SIZE, ge=1, le=_MAX_PAGE_SIZE, description="Max events per page"
    ),
    next_page_token: str | None = Query(
        None,
        description="Opaque cursor from a prior truncated response.",
    ),
) -> VMEventListResult:
    """Return VM lifecycle events parsed from the events bucket.

    Auth is enforced upstream by `_authenticated_router` (verify_api_key).
    """
    resolved_service = service if service is not None else _infer_service_from_vm_name(vm_name)
    resolved_date = _normalise_date(date)
    start_hour, end_hour = _normalise_hour_range(from_hour, to_hour, resolved_date)
    severity_threshold = _normalise_severity(severity_floor)

    if _cfg.is_mock_mode():
        return _mock_events(vm_name, resolved_service, resolved_date)

    return _list_real_events(
        vm_name=vm_name,
        service=resolved_service,
        date=resolved_date,
        start_hour=start_hour,
        end_hour=end_hour,
        severity_threshold=severity_threshold,
        page_size=page_size,
        next_page_token=next_page_token,
    )


def _collect_blob_names(
    storage: object,
    bucket: str,
    service: str,
    date: str,
    vm_name: str,
    start_hour: int,
    end_hour: int,
) -> tuple[list[tuple[int, str]], list[int]]:
    """List blob names across the requested hour partitions.

    Returns (sorted [(hour, name)], hours_with_data).
    """
    all_blob_names: list[tuple[int, str]] = []
    hours_scanned: list[int] = []
    for hour in range(start_hour, end_hour + 1):
        names = _list_event_blobs_in_hour(storage, bucket, service, date, vm_name, hour)
        if names:
            hours_scanned.append(hour)
        for name in names:
            all_blob_names.append((hour, name))
    return all_blob_names, hours_scanned


def _fetch_and_parse_event(
    storage: object,
    bucket: str,
    blob_name: str,
) -> VMLifecycleEvent | None:
    """Download + parse one blob. Per shard-level failure isolation, log +
    return None on fetch / parse failure — never raise.
    """
    try:
        blob_bytes = cast(
            "bytes",
            storage.download_bytes(bucket=bucket, blob_path=blob_name),  # pyright: ignore[reportAttributeAccessIssue]
        )
    except (OSError, ValueError) as exc:
        log_event(
            "EVENT_FETCH_FAILED",
            severity="WARNING",
            details={"blob_name": blob_name, "error": str(exc)},
        )
        return None
    return _parse_event_jsonl(blob_bytes, blob_name)


def _list_real_events(
    *,
    vm_name: str,
    service: str,
    date: str,
    start_hour: int,
    end_hour: int,
    severity_threshold: int,
    page_size: int,
    next_page_token: str | None,
) -> VMEventListResult:
    bucket = _resolve_events_bucket()
    storage = get_storage_client(project_id=_cfg.gcp_project_id)
    cursor_blob = _decode_page_token(next_page_token) if next_page_token else None

    all_blob_names, hours_scanned = _collect_blob_names(
        storage, bucket, service, date, vm_name, start_hour, end_hour
    )
    if cursor_blob is not None:
        all_blob_names = [(h, n) for h, n in all_blob_names if n > cursor_blob]

    truncated = len(all_blob_names) > page_size
    page_blobs = all_blob_names[:page_size]

    events: list[VMLifecycleEvent] = []
    for _, blob_name in page_blobs:
        parsed = _fetch_and_parse_event(storage, bucket, blob_name)
        if parsed is None:
            continue
        if _SEVERITY_ORDER[parsed.severity] < severity_threshold:
            continue
        events.append(parsed)
    events.sort(key=lambda evt: evt.timestamp)

    next_token = _encode_page_token(page_blobs[-1][1]) if (truncated and page_blobs) else None
    return VMEventListResult(
        vm_name=vm_name,
        service=service,
        date=date,
        hours_scanned=hours_scanned,
        total_events=len(events),
        events=events,
        truncated=truncated,
        next_page_token=next_token,
    )


# Re-exported for tests + helpers that want the inferred-service helper without
# round-tripping through HTTP. Kept at module level rather than inline so test
# fixtures can import it directly (e.g. when constructing fake bucket layouts).
__all__ = ["_infer_service_from_vm_name", "_resolve_events_bucket", "router"]

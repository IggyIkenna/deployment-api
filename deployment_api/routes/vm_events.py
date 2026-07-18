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

Plan: `deployment_api_work_stream_a_2026_05_07.md` Phase 2.B.

V1 inline `_PREFIX_TO_SERVICE` mapping; lifted to UAC by the same successor
plan as `backfill_launch.py`'s `_TASK_TO_LAUNCHER`.
"""

from __future__ import annotations

import base64
import json
import logging
from datetime import UTC, datetime
from typing import cast

from fastapi import APIRouter, Depends, HTTPException, Query
from pydantic import BaseModel
from unified_api_contracts.internal import (
    VMEventListResult,
    VMLifecycleEvent,
)
from unified_trading_library import get_storage_client, log_event

from deployment_api.deployment_api_config import DeploymentApiConfig
from deployment_api.rate_limiting import endpoint_rate_limit

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
    "fts-backfill-": "features-sports-service",
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


def _parse_since(since: str) -> tuple[str, int]:
    """Parse ISO 8601 `since` timestamp into (YYYY-MM-DD, hour_int).

    Returns the date string and the UTC hour so the caller can set
    date + from_hour for the blob scan.
    """
    try:
        ts = datetime.fromisoformat(since.replace("Z", "+00:00"))
    except ValueError as exc:
        raise ValueError(f"Cannot parse `since` as ISO 8601 timestamp: {since!r}") from exc
    ts_utc = ts.astimezone(UTC)
    return ts_utc.strftime("%Y-%m-%d"), ts_utc.hour


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
                "message": (f"severity_floor '{severity_floor}' must be one of {sorted(_SEVERITY_ORDER.keys())}"),
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
        row: object = json.loads(first_line)  # pyright: ignore[reportAny]
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
    event = str(row_dict.get("event", ""))  # noqa: qg-empty-fallback — event row display default
    service = str(row_dict.get("service", ""))  # noqa: qg-empty-fallback — event row display default
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

    metadata_raw = row_dict.get("metadata", {})  # noqa: qg-empty-fallback — optional event metadata
    metadata = cast(dict[str, object], metadata_raw) if isinstance(metadata_raw, dict) else {}
    severity = str(metadata.get("severity", "INFO")).upper()
    if severity not in _SEVERITY_ORDER:
        severity = "INFO"

    details_raw = metadata.get("details", {})  # noqa: qg-empty-fallback — optional event metadata
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
    storage: object,  # StorageClient — typed as object to avoid imports leaking
    # pyright: ignore[reportUnknownParameterType,reportMissingParameterType]
    bucket: str,
    service: str,
    date: str,
    vm_name: str,
    hour: int,
) -> list[str]:
    """List blob names under the (vm, hour) partition. Sorted asc by name
    (= chronological since `{ts_us}_{seq}` is lexicographically sortable)."""
    prefix = f"events/{service}/{date}/{vm_name}/hour={hour:02d}/"
    blobs = list(storage.list_blobs(bucket=bucket, prefix=prefix))  # pyright: ignore[reportUnknownVariableType,reportUnknownMemberType,reportUnknownArgumentType,reportAttributeAccessIssue]
    names = [blob.name for blob in blobs if blob.name.endswith(".jsonl")]  # pyright: ignore[reportUnknownVariableType,reportUnknownMemberType]
    names.sort()  # pyright: ignore[reportUnknownMemberType]
    return names  # pyright: ignore[reportUnknownVariableType]


@router.get("/events", response_model=VMEventListResult)
def list_vm_events(
    _rl: None = Depends(endpoint_rate_limit(30)),
    vm_name: str = Query(..., description="VM name (required)"),
    service: str | None = Query(
        None,
        description="Service name partition. Inferred from vm_name prefix if omitted.",
    ),
    since: str | None = Query(
        None,
        description=(
            "ISO 8601 timestamp — return events at or after this time. "
            "When set, overrides `date` and `from_hour` (derived from the timestamp). "
            "Example: 2026-05-15T10:00:00Z"
        ),
    ),
    date: str | None = Query(None, description="YYYY-MM-DD (default: today UTC). Ignored when `since` is set."),
    from_hour: int | None = Query(
        None, ge=0, le=23, description="Inclusive (default: 0). Ignored when `since` is set."
    ),
    to_hour: int | None = Query(None, ge=0, le=23, description="Inclusive (default: today=current hour, else 23)"),
    severity_floor: str | None = Query(
        None,
        description="INFO|WARNING|ERROR|CRITICAL — drop events below floor (default: INFO)",
    ),
    page_size: int = Query(_DEFAULT_PAGE_SIZE, ge=1, le=_MAX_PAGE_SIZE, description="Max events per page"),
    next_page_token: str | None = Query(
        None,
        description="Opaque cursor from a prior truncated response.",
    ),
) -> VMEventListResult:
    """Return VM lifecycle events parsed from the events bucket.

    Auth is enforced upstream by `_authenticated_router` (verify_api_key).
    When `since` is provided it takes precedence over `date` + `from_hour`.
    """
    if since is not None:
        try:
            date, from_hour = _parse_since(since)
        except ValueError as exc:
            raise HTTPException(status_code=400, detail=f"Invalid input: {exc}") from exc
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


@router.get("/{vm_name}/events", response_model=VMEventListResult)
def list_filtered_vm_events(
    vm_name: str,
    _rl: None = Depends(endpoint_rate_limit(30)),
    service: str | None = Query(
        None,
        description="Service name partition. Inferred from vm_name prefix if omitted.",
    ),
    since: str | None = Query(
        None,
        description="ISO 8601 timestamp — return events at or after this time.",
    ),
    type: str | None = Query(
        None,
        description="Filter by event type, e.g. STARTED, STOPPED, DATA_BROADCAST.",
    ),
    limit: int = Query(
        100,
        ge=1,
        le=5000,
        description="Max number of events to return (default 100).",
    ),
) -> VMEventListResult:
    """Return VM lifecycle events filtered by type/time, path-param based.

    Extends the query-param GET /events with path-based {vm_name}, event-type
    filter, and a limit cap (simpler than full pagination for log-viewer use).
    """
    resolved_service = service if service is not None else _infer_service_from_vm_name(vm_name)

    if _cfg.is_mock_mode():
        resolved_date = datetime.now(UTC).strftime("%Y-%m-%d")
        result = _mock_events(vm_name, resolved_service, resolved_date)
        filtered = [e for e in result.events if type is None or e.event == type]
        limited = filtered[:limit]
        return VMEventListResult(
            vm_name=vm_name,
            service=resolved_service,
            date=resolved_date,
            events=limited,
            total_events=len(filtered),
            hours_scanned=result.hours_scanned,
            truncated=len(filtered) > limit,
            next_page_token=None,
        )

    try:
        resolved_date, start_hour = (
            _parse_since(since) if since is not None else (datetime.now(UTC).strftime("%Y-%m-%d"), 0)
        )
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=f"Invalid input: {exc}") from exc

    try:
        real_result = _list_real_events(
            vm_name=vm_name,
            service=resolved_service,
            date=resolved_date,
            start_hour=start_hour,
            end_hour=23,
            severity_threshold=0,
            page_size=limit,
            next_page_token=None,
        )
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=f"Invalid input: {exc}") from exc

    filtered = [e for e in real_result.events if type is None or e.event == type]
    limited = filtered[:limit]
    return VMEventListResult(
        vm_name=vm_name,
        service=resolved_service,
        date=resolved_date,
        events=limited,
        total_events=len(filtered),
        hours_scanned=real_result.hours_scanned,
        truncated=len(filtered) > limit,
        next_page_token=None,
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
            storage.download_bytes(bucket=bucket, blob_path=blob_name),  # pyright: ignore[reportAttributeAccessIssue,reportUnknownMemberType]
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

    all_blob_names, hours_scanned = _collect_blob_names(storage, bucket, service, date, vm_name, start_hour, end_hour)
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


class VmLogLine(BaseModel):  # CORRECT-LOCAL: FastAPI API contract model
    """One structured log entry returned by the log-tail endpoint."""

    timestamp: str
    event: str
    severity: str
    message: str


class VmLogTailResult(BaseModel):  # CORRECT-LOCAL: FastAPI API contract model
    vm_name: str
    service: str
    lines: list[VmLogLine]
    total_lines: int


def _event_to_log_line(evt: VMLifecycleEvent) -> VmLogLine:
    ts = evt.timestamp.isoformat(timespec="seconds").replace("+00:00", "Z")
    msg = evt.details.get("message", "")  # noqa: qg-empty-fallback — fallback assembled below
    if not msg:
        msg = ", ".join(f"{k}={v}" for k, v in evt.details.items())
    return VmLogLine(timestamp=ts, event=evt.event, severity=evt.severity, message=msg)


@router.get("/logs/{vm_name}", response_model=VmLogTailResult)
def tail_vm_logs(
    vm_name: str,
    _rl: None = Depends(endpoint_rate_limit(30)),
    service: str | None = Query(None, description="Service name. Inferred from vm_name prefix if omitted."),
    tail: int = Query(100, ge=1, le=1000, description="Number of most-recent log lines to return"),
    since: str | None = Query(None, description="ISO 8601 timestamp — return lines at or after this time"),
) -> VmLogTailResult:
    """Return the last N log lines for a VM.

    Reads from the same GCS events bucket as GET /api/vm/events, but returns a
    simplified text-friendly representation (timestamp, event, severity, message)
    suitable for inline log viewers.
    """
    resolved_service = service if service is not None else _infer_service_from_vm_name(vm_name)

    if _cfg.is_mock_mode():
        mock_date = datetime.now(UTC).strftime("%Y-%m-%d")
        mock_result = _mock_events(vm_name, resolved_service, mock_date)
        lines = [_event_to_log_line(e) for e in mock_result.events[:tail]]
        return VmLogTailResult(vm_name=vm_name, service=resolved_service, lines=lines, total_lines=len(lines))

    if since is not None:
        try:
            date, from_hour = _parse_since(since)
        except ValueError as exc:
            raise HTTPException(status_code=400, detail=f"Invalid input: {exc}") from exc
    else:
        date = datetime.now(UTC).strftime("%Y-%m-%d")
        from_hour = 0

    real_result = _list_real_events(
        vm_name=vm_name,
        service=resolved_service,
        date=date,
        start_hour=from_hour,
        end_hour=23,
        severity_threshold=0,
        page_size=tail,
        next_page_token=None,
    )
    all_lines = [_event_to_log_line(e) for e in real_result.events]
    tail_lines = all_lines[-tail:]
    return VmLogTailResult(vm_name=vm_name, service=resolved_service, lines=tail_lines, total_lines=len(tail_lines))


def infer_service_from_vm_name(vm_name: str) -> str:
    """Public wrapper — see `_infer_service_from_vm_name` for details."""
    return _infer_service_from_vm_name(vm_name)


def fetch_vm_events_window(
    vm_name: str,
    service: str,
    date: str,
    start_hour: int,
    end_hour: int,
) -> VMEventListResult:
    """Public helper for callers that need raw event lists (e.g. vm_health)."""
    return _list_real_events(
        vm_name=vm_name,
        service=service,
        date=date,
        start_hour=start_hour,
        end_hour=end_hour,
        severity_threshold=0,
        page_size=5000,
        next_page_token=None,
    )


def mock_vm_events_for_date(vm_name: str, service: str, date: str) -> VMEventListResult:
    """Public wrapper around _mock_events for external callers."""
    return _mock_events(vm_name, service, date)


__all__ = [
    "fetch_vm_events_window",
    "infer_service_from_vm_name",
    "mock_vm_events_for_date",
    "router",
]

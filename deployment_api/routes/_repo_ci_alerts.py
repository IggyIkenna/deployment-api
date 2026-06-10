"""
Alert-ledger reader for the repo-CI dashboard (operator traceability, 2026-06-10).

Every Slack alert is persisted by notify-slack.yml to
gs://{CICD_EVENTS_BUCKET}/cicd/alerts/{date}/alerts.jsonl, and every promotion workflow
persists state events to cicd/events/{repo}/{date}/events.jsonl (persist-cicd-event.yml).
This module reads BOTH and derives per-(repo, workflow) lifecycle STREAMS — the current
state and the previous state — so any Slack page can be traced to its history on the
dashboard instead of Slack scrollback.

Plan: ci_dashboard_deployment_ui_2026_06_10.md (alert-history mirror, elevated to v1).
"""

from __future__ import annotations

import asyncio
import datetime as dt
import json
import logging
import time
from typing import TypedDict, cast

logger = logging.getLogger(__name__)

_BUCKET = "unified-trading-cicd-events"
_CACHE_TTL_SECONDS = 60.0
_DEFAULT_DAYS = 2
_MAX_ITEMS = 400

_cache: tuple[float, AlertsPayloadDict] | None = None


class AlertEntryDict(TypedDict):
    """One ledger entry (a Slack alert or a persisted workflow state event)."""

    kind: str  # "alert" | "event"
    timestamp: str
    repo: str
    workflow_name: str
    severity: str | None  # alerts only
    conclusion: str | None
    message: str | None  # alerts only
    run_url: str | None


class AlertStreamDict(TypedDict):
    """Lifecycle of one (repo, workflow) alert stream: current vs previous state."""

    repo: str
    workflow_name: str
    current: AlertEntryDict
    previous: AlertEntryDict | None
    count: int


class AlertsPayloadDict(TypedDict):
    """GET /api/repo-ci/alerts response."""

    generated_at: str
    source: str
    alerts: list[AlertEntryDict]  # newest first (capped)
    streams: list[AlertStreamDict]  # one per (repo, workflow), worst-current first


def _parse_line(line: str) -> AlertEntryDict | None:
    """Parse one JSONL line from either ledger shape (lenient — skip junk lines)."""
    try:
        raw = cast(object, json.loads(line))
    except json.JSONDecodeError:
        return None
    if not isinstance(raw, dict):
        return None
    obj = cast(dict[str, object], raw)
    event_type = str(obj.get("event_type") or "")
    if event_type == "slack_alert":
        return AlertEntryDict(
            kind="alert",
            timestamp=str(obj.get("timestamp") or ""),
            repo=str(obj.get("repo") or ""),
            workflow_name=str(obj.get("workflow_name") or ""),
            severity=str(obj.get("severity") or "INFO"),
            conclusion=str(obj.get("conclusion")) if obj.get("conclusion") else None,
            message=str(obj.get("message") or ""),
            run_url=str(obj.get("run_url")) if obj.get("run_url") else None,
        )
    if event_type == "github_workflow_event":
        return AlertEntryDict(
            kind="event",
            timestamp=str(obj.get("timestamp") or ""),
            repo=str(obj.get("repo_name") or ""),
            workflow_name=str(obj.get("workflow_name") or ""),
            severity=None,
            conclusion=str(obj.get("conclusion")) if obj.get("conclusion") else None,
            message=None,
            run_url=None,
        )
    return None


def derive_streams(entries: list[AlertEntryDict]) -> list[AlertStreamDict]:
    """Group chronologically-sorted entries into (repo, workflow) lifecycle streams.

    `current` = newest entry; `previous` = the one before it — the operator's "what is
    the state now and what was it last" traceability pair. Streams sort worst-first
    (CRITICAL current > WARNING > failure-conclusion > rest), then newest-first.
    """
    by_stream: dict[tuple[str, str], list[AlertEntryDict]] = {}
    for entry in entries:
        by_stream.setdefault((entry["repo"], entry["workflow_name"]), []).append(entry)

    def severity_rank(entry: AlertEntryDict) -> int:
        if entry["severity"] == "CRITICAL" or entry["conclusion"] == "failure":
            return 0
        if entry["severity"] == "WARNING":
            return 1
        return 2

    streams: list[AlertStreamDict] = []
    for (repo, workflow_name), stream_entries in by_stream.items():
        ordered = sorted(stream_entries, key=lambda e: e["timestamp"])
        streams.append(
            AlertStreamDict(
                repo=repo,
                workflow_name=workflow_name,
                current=ordered[-1],
                previous=ordered[-2] if len(ordered) > 1 else None,
                count=len(ordered),
            )
        )
    streams.sort(key=lambda s: (severity_rank(s["current"]), s["current"]["timestamp"]))
    # worst first, and within a rank newest first:
    streams.sort(key=lambda s: s["current"]["timestamp"], reverse=True)
    streams.sort(key=lambda s: severity_rank(s["current"]))
    return streams


def _read_ledgers_sync(days: int) -> list[AlertEntryDict]:
    """List + read the last N days of alert/event JSONL blobs from GCS."""
    from unified_trading_library import download_from_storage, get_storage_client

    client = get_storage_client()
    dates = [(dt.datetime.now(dt.UTC) - dt.timedelta(days=offset)).strftime("%Y-%m-%d") for offset in range(days)]
    entries: list[AlertEntryDict] = []
    # Alerts ledger: one stream per date.
    for date in dates:
        for prefix in (f"cicd/alerts/{date}/",):
            try:
                for blob in client.list_blobs(_BUCKET, prefix=prefix):
                    raw = download_from_storage(_BUCKET, blob.name)
                    for line in raw.decode("utf-8", errors="replace").splitlines():
                        parsed = _parse_line(line)
                        if parsed:
                            entries.append(parsed)
            except Exception as exc:
                logger.warning("[REPO-CI] alert ledger read failed for %s: %s", prefix, exc)
    # Events ledger: per-repo per-date — list once per date across repos via delimiter walk.
    try:
        for blob in client.list_blobs(_BUCKET, prefix="cicd/events/"):
            if not any(f"/{date}/" in blob.name for date in dates):
                continue
            raw = download_from_storage(_BUCKET, blob.name)
            for line in raw.decode("utf-8", errors="replace").splitlines():
                parsed = _parse_line(line)
                if parsed:
                    entries.append(parsed)
    except Exception as exc:
        logger.warning("[REPO-CI] events ledger read failed: %s", exc)
    return entries


async def load_alerts_payload(source: str = "live", days: int = _DEFAULT_DAYS) -> AlertsPayloadDict:
    """Read the ledgers (cached 60 s) and shape the alerts + lifecycle streams payload."""
    global _cache
    now = time.monotonic()
    if _cache is not None and now - _cache[0] < _CACHE_TTL_SECONDS:
        return _cache[1]
    entries = await asyncio.to_thread(_read_ledgers_sync, days)
    entries.sort(key=lambda e: e["timestamp"], reverse=True)
    payload = AlertsPayloadDict(
        generated_at=dt.datetime.now(dt.UTC).isoformat(),
        source=source,
        alerts=[e for e in entries if e["kind"] == "alert"][:_MAX_ITEMS],
        streams=derive_streams(entries[:_MAX_ITEMS]),
    )
    _cache = (now, payload)
    return payload

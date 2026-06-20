"""
Unified alert-ledger reader — superset of /api/repo-ci/alerts.

Reads two GCS ledgers from bucket unified-trading-cicd-events:
  cicd/alerts/{date}/alerts.jsonl    — CI Slack alerts + workflow events
  infra/alerts/{date}/alerts.jsonl   — infra alerts written by agent-orchestrator
                                       health.py / watchdog / GCS alert writer

Returns a single payload so the deployment-ui Monitoring Pane shows every alert
class (CI + VM-down + consolidator-down + worker-liveness + git-health) in one view.

Plan: deployment_ui_monitoring_pane_2026_06_19.md (task 004).
"""

from __future__ import annotations

import asyncio
import datetime as dt
import json
import logging
import time
from typing import TypedDict, cast

from unified_trading_library import download_from_storage, get_storage_client

logger = logging.getLogger(__name__)

_BUCKET = "unified-trading-cicd-events"
_CACHE_TTL_SECONDS = 60.0
_DEFAULT_DAYS = 2
_MAX_ITEMS = 400

_cache: tuple[float, UnifiedAlertsPayloadDict] | None = None


class UnifiedAlertEntryDict(TypedDict):  # CORRECT-LOCAL: unified alerts GCS payload shape
    """One ledger entry from either the CI or the infra alert ledger."""

    kind: str  # "alert" | "event" | "infra_alert"
    timestamp: str
    domain: str  # "cicd" for CI, "infra/worker"|"infra/vm"|"infra/git"|… for infra
    severity: str | None
    message: str | None
    component: str | None  # CI: repo name; infra: slot_N / component field
    # CI-specific (None for infra alerts)
    repo: str | None
    workflow_name: str | None
    conclusion: str | None
    run_url: str | None
    # infra-specific (None for CI alerts)
    dedup_key: str | None


class UnifiedAlertStreamDict(TypedDict):  # CORRECT-LOCAL: unified alerts GCS payload shape
    """Lifecycle of one (repo, workflow) stream derived from CI alerts."""

    repo: str
    workflow_name: str
    current: UnifiedAlertEntryDict
    previous: UnifiedAlertEntryDict | None
    count: int


class UnifiedAlertsPayloadDict(TypedDict):  # CORRECT-LOCAL: unified alerts GCS payload shape
    """GET /api/alerts response — superset of GET /api/repo-ci/alerts."""

    generated_at: str
    source: str
    alerts: list[UnifiedAlertEntryDict]  # newest first (capped), all domains
    streams: list[UnifiedAlertStreamDict]  # CI (repo, workflow) lifecycle streams
    infra_alert_count: int  # count of infra_alert entries in alerts[]


def _parse_ci_line(line: str) -> UnifiedAlertEntryDict | None:
    """Parse one JSONL line from cicd/alerts/ or cicd/events/."""
    try:
        raw = cast(object, json.loads(line))
    except json.JSONDecodeError:
        return None
    if not isinstance(raw, dict):
        return None
    obj = cast(dict[str, object], raw)
    event_type = str(obj.get("event_type") or "")
    if event_type == "slack_alert":
        repo = str(obj.get("repo") or "")
        return UnifiedAlertEntryDict(
            kind="alert",
            timestamp=str(obj.get("timestamp") or ""),
            domain="cicd",
            severity=str(obj.get("severity") or "INFO"),
            message=str(obj.get("message") or ""),
            component=repo or None,
            repo=repo,
            workflow_name=str(obj.get("workflow_name") or ""),
            conclusion=str(obj.get("conclusion")) if obj.get("conclusion") else None,
            run_url=str(obj.get("run_url")) if obj.get("run_url") else None,
            dedup_key=None,
        )
    if event_type == "github_workflow_event":
        repo = str(obj.get("repo_name") or "")
        return UnifiedAlertEntryDict(
            kind="event",
            timestamp=str(obj.get("timestamp") or ""),
            domain="cicd",
            severity=None,
            message=None,
            component=repo or None,
            repo=repo,
            workflow_name=str(obj.get("workflow_name") or ""),
            conclusion=str(obj.get("conclusion")) if obj.get("conclusion") else None,
            run_url=None,
            dedup_key=None,
        )
    return None


def _parse_infra_line(line: str) -> UnifiedAlertEntryDict | None:
    """Parse one JSONL line from infra/alerts/ written by gcs_alert_writer.py."""
    try:
        raw = cast(object, json.loads(line))
    except json.JSONDecodeError:
        return None
    if not isinstance(raw, dict):
        return None
    obj = cast(dict[str, object], raw)
    if str(obj.get("event_type") or "") != "infra_alert":
        return None
    return UnifiedAlertEntryDict(
        kind="infra_alert",
        timestamp=str(obj.get("timestamp") or ""),
        domain=str(obj.get("domain") or "infra"),
        severity=str(obj.get("severity") or "INFO"),
        message=str(obj.get("message") or ""),
        component=str(obj.get("component")) if obj.get("component") else None,
        repo=None,
        workflow_name=None,
        conclusion=None,
        run_url=None,
        dedup_key=str(obj.get("dedup_key")) if obj.get("dedup_key") else None,
    )


def _derive_ci_streams(
    entries: list[UnifiedAlertEntryDict],
) -> list[UnifiedAlertStreamDict]:
    """Group CI entries into (repo, workflow) lifecycle streams, worst-current first."""
    by_stream: dict[tuple[str, str], list[UnifiedAlertEntryDict]] = {}
    for entry in entries:
        if entry["repo"] is None or entry["workflow_name"] is None:
            continue
        key = (entry["repo"], entry["workflow_name"])
        by_stream.setdefault(key, []).append(entry)

    def _severity_rank(entry: UnifiedAlertEntryDict) -> int:
        if entry["severity"] == "CRITICAL" or entry["conclusion"] == "failure":
            return 0
        if entry["severity"] == "WARNING":
            return 1
        return 2

    streams: list[UnifiedAlertStreamDict] = []
    for (repo, workflow_name), stream_entries in by_stream.items():
        ordered = sorted(stream_entries, key=lambda e: e["timestamp"])
        streams.append(
            UnifiedAlertStreamDict(
                repo=repo,
                workflow_name=workflow_name,
                current=ordered[-1],
                previous=ordered[-2] if len(ordered) > 1 else None,
                count=len(ordered),
            )
        )
    streams.sort(key=lambda s: s["current"]["timestamp"], reverse=True)
    streams.sort(key=lambda s: _severity_rank(s["current"]))
    return streams


def _read_ledgers_sync(days: int) -> list[UnifiedAlertEntryDict]:
    """Read both CI and infra alert JSONL ledgers from GCS for the last N days."""
    client = get_storage_client()
    dates = [(dt.datetime.now(dt.UTC) - dt.timedelta(days=offset)).strftime("%Y-%m-%d") for offset in range(days)]
    entries: list[UnifiedAlertEntryDict] = []

    # CI alerts ledger
    for date in dates:
        try:
            for blob in client.list_blobs(_BUCKET, prefix=f"cicd/alerts/{date}/"):
                raw = download_from_storage(_BUCKET, blob.name)
                for line in raw.decode("utf-8", errors="replace").splitlines():
                    parsed = _parse_ci_line(line)
                    if parsed:
                        entries.append(parsed)
        except Exception as exc:
            logger.warning("[ALERTS] CI alert ledger read failed for %s: %s", date, exc)

    # CI events ledger
    try:
        for blob in client.list_blobs(_BUCKET, prefix="cicd/events/"):
            if not any(f"/{date}/" in blob.name for date in dates):
                continue
            raw = download_from_storage(_BUCKET, blob.name)
            for line in raw.decode("utf-8", errors="replace").splitlines():
                parsed = _parse_ci_line(line)
                if parsed:
                    entries.append(parsed)
    except Exception as exc:
        logger.warning("[ALERTS] CI events ledger read failed: %s", exc)

    # Infra alerts ledger (written by agent-orchestrator gcs_alert_writer.py)
    for date in dates:
        try:
            for blob in client.list_blobs(_BUCKET, prefix=f"infra/alerts/{date}/"):
                raw = download_from_storage(_BUCKET, blob.name)
                for line in raw.decode("utf-8", errors="replace").splitlines():
                    parsed = _parse_infra_line(line)
                    if parsed:
                        entries.append(parsed)
        except Exception as exc:
            logger.warning("[ALERTS] infra alert ledger read failed for %s: %s", date, exc)

    return entries


async def load_unified_alerts_payload(source: str = "live", days: int = _DEFAULT_DAYS) -> UnifiedAlertsPayloadDict:
    """Read all alert ledgers (cached 60 s) and return the unified payload."""
    global _cache
    now = time.monotonic()
    if _cache is not None and now - _cache[0] < _CACHE_TTL_SECONDS:
        return _cache[1]
    entries = await asyncio.to_thread(_read_ledgers_sync, days)
    entries.sort(key=lambda e: e["timestamp"], reverse=True)
    capped = entries[:_MAX_ITEMS]
    infra_count = sum(1 for e in capped if e["kind"] == "infra_alert")
    payload = UnifiedAlertsPayloadDict(
        generated_at=dt.datetime.now(dt.UTC).isoformat(),
        source=source,
        alerts=capped,
        streams=_derive_ci_streams(capped),
        infra_alert_count=infra_count,
    )
    _cache = (now, payload)
    return payload

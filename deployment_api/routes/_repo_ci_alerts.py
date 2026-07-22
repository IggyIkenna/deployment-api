"""
Alert-ledger reader for the repo-CI dashboard (operator traceability, 2026-06-10).

Every Slack alert is persisted by notify-slack.yml (or deployment-api's own `_persist_alert`) to
gs://{CICD_EVENTS_BUCKET}/cicd/alerts/{date}/<unique>.jsonl, and every promotion workflow persists
state events to cicd/events/{repo}/{date}/<unique>.jsonl (the `persist-event` composite action).
One object per event/alert (not a shared per-day filename) since
`deployment_alerts_ingestion_completeness_2026_07_20.md` todo 6 — this module has always read both
ledgers as a PREFIX WALK (`list_blobs(prefix=...)`), so it required no reader change. This module
reads BOTH and derives per-(repo, workflow) lifecycle STREAMS — the current state and the previous
state — so any Slack page can be traced to its history on the dashboard instead of Slack scrollback.

Plan: ci_dashboard_deployment_ui_2026_06_10.md (alert-history mirror, elevated to v1).
"""

from __future__ import annotations

import asyncio
import datetime as dt
import json
import logging
import time
from typing import NotRequired, TypedDict, cast

import pyarrow as pa
import pyarrow.parquet as pq
from unified_trading_library import (
    BucketNamingError,
    download_from_storage,
    get_storage_client,
    resolve_bucket_name,
)

logger = logging.getLogger(__name__)

_CACHE_TTL_SECONDS = 60.0
# deployment_alerts_ingestion_completeness_2026_07_20.md todo 8: 2 days / a silent 400-item cap
# couldn't support Plan B's date-range filter. Widened retention to 30 days (bounded
# day-partitioned reads, unchanged single-walk discipline) with real offset/limit pagination
# instead of a truncation — a caller past the last page gets an explicit `capped` signal, never a
# silently short list.
#
# _DEFAULT_DAYS deliberately stays narrower than _MAX_DAYS: the `alerting_service` plane writes one
# object per alert event (~20-24k objects/day), and `_read_alerting_service_sync()` lists+downloads
# each one individually and synchronously — a 30-day unqualified request walks 277k+ objects in one
# request, which OOM-kills or 504s the Cloud Run container (measured 2026-07-22, see
# deployment_alerts_ingestion_completeness_2026_07_20.md). Defaulting to 1 day keeps an unqualified
# `/api/alerts` call inside what the reader can actually survive; the date-range picker can still
# widen up to `_MAX_DAYS` explicitly. This is a stopgap, not a fix for the underlying per-object
# read pattern.
_DEFAULT_DAYS = 1
_MAX_DAYS = 30
_DEFAULT_PAGE_SIZE = 400
_MAX_PAGE_SIZE = 2000

# Keyed by the (clamped) `days` window — each window's raw entries are cached independently so
# pagination (offset/limit) never triggers a re-fetch, only a re-slice of the cached entries.
_entries_cache: dict[int, tuple[float, list[AlertEntryDict]]] = {}


class AlertEntryDict(TypedDict):  # CORRECT-LOCAL: CI-alerts GCS payload shape (internal)
    """One ledger entry (a Slack alert or a persisted workflow state event)."""

    kind: str  # "alert" | "event" | "vm_down" | "worker_liveness" | "git_health" | "consolidator_down"
    timestamp: str
    repo: str  # the EMITTING repo/service (see subject_repo for the repo the alert is ABOUT)
    workflow_name: str
    severity: str | None  # alerts only
    conclusion: str | None
    message: str | None  # alerts only
    run_url: str | None
    alert_class: str | None  # non-CI watcher class ("worker_liveness", "git_health", etc.)
    # The deployment/VM target (``vm_name``) an infra alert names, so the UI can deep-link the alert to
    # its ``/deployments/{name}`` detail (parity #4). Absent for CI alerts (no target). NotRequired so
    # only the alert path that carries a target sets it.
    deployment_target: NotRequired[str | None]
    # The repo the alert is ABOUT (distinct from ``repo``, the emitter) — populated for slack_alert
    # rows written after deployment_alerts_ingestion_completeness_2026_07_20.md todo 4 landed;
    # structurally absent (None) for older rows and for planes with no repo-scoped subject.
    # NotRequired so alerting_service/zombie_watchdog rows (no subject concept) simply omit it.
    subject_repo: NotRequired[str | None]


class AlertStreamDict(TypedDict):  # CORRECT-LOCAL: CI-alerts GCS payload shape (internal)
    """Lifecycle of one (repo, workflow) alert stream: current vs previous state."""

    repo: str
    workflow_name: str
    current: AlertEntryDict
    previous: AlertEntryDict | None
    count: int


class AlertsPayloadDict(TypedDict):  # CORRECT-LOCAL: CI-alerts GCS payload shape (internal)
    """GET /api/repo-ci/alerts response."""

    generated_at: str
    source: str
    alerts: list[AlertEntryDict]  # newest first, this page only (see offset/limit/capped)
    streams: list[AlertStreamDict]  # one per (repo, workflow), worst-current first, over the FULL window
    days: int  # the day window actually served (clamped to _MAX_DAYS)
    total_count: int  # total alert-class entries in the requested window, pre-pagination
    returned_count: int  # len(alerts) — how many landed on this page
    offset: int
    limit: int
    capped: bool  # True when more entries exist beyond this page — never a silent short list


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
        # Non-CI watcher entries carry an `alert_class` field (e.g. "worker_liveness",
        # "git_health", "vm_down", "consolidator_down"). Use it as the kind directly so
        # the unified ledger surfaces all alert domains, not just CI/CD.
        alert_class = str(obj.get("alert_class") or "")
        kind = alert_class if alert_class else "alert"
        # subject_repo: the repo the alert is ABOUT (todo 4). Writers that emit it (notify-slack.yml
        # since deployment_alerts_ingestion_completeness_2026_07_20.md, deployment-api's own
        # _persist_alert) set it explicitly; older rows / writers that haven't been updated yet carry
        # no such key, so this degrades to None (structurally absent) rather than misreporting the
        # emitter as the subject.
        subject_repo = str(obj.get("subject_repo")) if obj.get("subject_repo") else None
        return AlertEntryDict(
            kind=kind,
            timestamp=str(obj.get("timestamp") or ""),
            repo=str(obj.get("repo") or ""),
            workflow_name=str(obj.get("workflow_name") or ""),
            severity=str(obj.get("severity") or "INFO"),
            conclusion=str(obj.get("conclusion")) if obj.get("conclusion") else None,
            message=str(obj.get("message") or ""),
            run_url=str(obj.get("run_url")) if obj.get("run_url") else None,
            alert_class=alert_class if alert_class else None,
            # The infra/deployment watchers (vm_down / worker_liveness / deployment lifecycle) flatten
            # ``details.vm_name`` to the top level → the /deployments/{name} deep-link target (#4).
            deployment_target=str(obj.get("vm_name") or obj.get("deployment_id") or "") or None,
            subject_repo=subject_repo,
        )
    if event_type == "github_workflow_event":
        # gha_ci_events plane: `repo_name` IS the subject — each repo's own workflow reports on
        # itself, so subject_repo == repo here (no separate emitter concept; FIELD_COVERAGE table
        # in unified_api_contracts.alerting.ledger).
        repo_name = str(obj.get("repo_name") or "")
        return AlertEntryDict(
            kind="event",
            timestamp=str(obj.get("timestamp") or ""),
            repo=repo_name,
            workflow_name=str(obj.get("workflow_name") or ""),
            severity=None,
            conclusion=str(obj.get("conclusion")) if obj.get("conclusion") else None,
            message=None,
            run_url=None,
            alert_class=None,
            subject_repo=repo_name or None,
        )
    return None


def _parse_alerting_service_line(line: str) -> AlertEntryDict | None:
    """Parse one JSONL line from alerting-service's own ``alerting/history/`` store.

    Shape differs from the CI ledgers this module otherwise reads: no ``event_type``
    discriminator, and two row shapes coexist there — delivery records
    (``channel``/``status``/``response_detail``/``event_name``/``timestamp``) and decision
    records (``rule_id``/``triggered_at``/``metric_value``/``threshold``) — both now
    carrying the normalised ``alert_class``/``severity``/``message``/``service``/
    ``deployment_target`` fields (``deployment_alerts_ingestion_completeness_2026_07_20.md``
    todo 2). ``subject_repo``/``workflow`` have no concept here (infra/venue-scoped, not
    repo-scoped — a structural absence, not a bug); ``alert_class`` stands in for both
    ``kind`` and ``workflow_name`` so alerting-service classes group into their own streams.
    """
    try:
        raw = cast(object, json.loads(line))
    except json.JSONDecodeError:
        return None
    if not isinstance(raw, dict):
        return None
    obj = cast(dict[str, object], raw)
    timestamp = str(obj.get("timestamp") or obj.get("triggered_at") or "")
    if not timestamp:
        return None
    alert_class = str(obj.get("alert_class") or obj.get("event_name") or "") or None
    deployment_target = str(obj.get("deployment_target")) if obj.get("deployment_target") else None
    return AlertEntryDict(
        kind=alert_class if alert_class else "alert",
        timestamp=timestamp,
        repo="",
        workflow_name=alert_class or "",
        severity=str(obj.get("severity") or "INFO"),
        conclusion=None,
        message=str(obj.get("message") or ""),
        run_url=None,
        alert_class=alert_class,
        deployment_target=deployment_target,
    )


def _read_alerting_service_sync(days: int) -> list[AlertEntryDict]:
    """List + read the last N days of alerting-service's own alert-history JSONL blobs.

    Bounded day-partitioned reads only (single-walk discipline) — mirrors
    ``_read_ledgers_sync``'s per-date prefix walk, against alerting-service's own
    dedicated bucket (resolved via ``resolve_bucket_name()``, never hardcoded) instead
    of the CI-alerts bucket.
    """
    dates = [(dt.datetime.now(dt.UTC) - dt.timedelta(days=offset)).strftime("%Y-%m-%d") for offset in range(days)]
    entries: list[AlertEntryDict] = []
    try:
        bucket = resolve_bucket_name(cloud="gcp", kind="alerting-service")
        client = get_storage_client()
    except Exception as exc:
        logger.warning("[REPO-CI] alerting-service bucket resolution failed: %s", exc)
        return entries
    for date in dates:
        prefix = f"alerting/history/date={date}/"
        try:
            for blob in client.list_blobs(bucket, prefix=prefix):
                raw = download_from_storage(bucket, blob.name)
                for line in raw.decode("utf-8", errors="replace").splitlines():
                    parsed = _parse_alerting_service_line(line)
                    if parsed:
                        entries.append(parsed)
        except Exception as exc:
            logger.warning("[REPO-CI] alerting-service ledger read failed for %s: %s", prefix, exc)
    return entries


def _parse_kill_switch_audit_row(row: dict[str, object]) -> AlertEntryDict | None:
    """Parse one row from a ``KillSwitchBus`` arm/disarm parquet audit file.

    Row shape matches ``unified_trading_library.kill_switch.audit_log._event_row()`` exactly
    (``event_type``/``switch_id``/``provenance``/``event_timestamp``/``actor``/``recovery_mode``/
    ``cooldown_seconds_elapsed``/``metadata_json``) — a read-only PROJECTION of that writer's schema,
    never re-derived (``deployment_alerts_ingestion_completeness_2026_07_20.md`` todo 9). ``severity``
    is intentionally omitted (structurally ABSENT for this plane — armed/disarmed is not a
    paging-tier axis — per ``FIELD_COVERAGE`` in ``unified_api_contracts.alerting.ledger``).
    """
    event_type = row.get("event_type")
    switch_id = row.get("switch_id")
    ts = row.get("event_timestamp")
    if not event_type or not switch_id or ts is None:
        return None
    timestamp = ts.isoformat() if isinstance(ts, dt.datetime) else str(ts)
    alert_class = f"kill_switch_{event_type}"
    actor = str(row.get("actor") or "")
    if event_type == "armed":
        provenance = row.get("provenance")
        message = f"Kill switch {switch_id} armed by {actor}"
        if provenance:
            message += f" (provenance={provenance})"
    else:
        recovery_mode = row.get("recovery_mode")
        message = f"Kill switch {switch_id} disarmed by {actor}"
        if recovery_mode:
            message += f" (recovery_mode={recovery_mode})"
    return AlertEntryDict(
        kind=alert_class,
        timestamp=timestamp,
        repo="unified-trading-library",
        workflow_name=f"kill-switch-{switch_id}",
        severity=None,
        conclusion=None,
        message=message,
        run_url=None,
        alert_class=alert_class,
    )


def _read_kill_switch_audit_log_sync(days: int) -> list[AlertEntryDict]:
    """List + read the last N days of ``KillSwitchBus`` arm/disarm parquet audit files.

    Read-only projection of ``unified_trading_library.kill_switch.audit_log``'s
    ``ParquetAuditLogWriter`` store — bounded day-partitioned reads only (single-walk discipline),
    mirrors ``_read_alerting_service_sync``'s per-date prefix walk against the dedicated
    kill-switch-audit-log bucket (resolved via ``resolve_bucket_name()``, never hardcoded). Never
    touches the write path (``deployment_alerts_ingestion_completeness_2026_07_20.md`` todo 9).
    """
    dates = [(dt.datetime.now(dt.UTC) - dt.timedelta(days=offset)).strftime("%Y-%m-%d") for offset in range(days)]
    entries: list[AlertEntryDict] = []
    try:
        bucket = resolve_bucket_name(cloud="gcp", kind="kill-switch-audit-log")
        client = get_storage_client()
    except Exception as exc:
        logger.warning("[REPO-CI] kill-switch audit-log bucket resolution failed: %s", exc)
        return entries
    for date in dates:
        prefix = f"{date}/"
        try:
            for blob in client.list_blobs(bucket, prefix=prefix):
                if not blob.name.endswith(".parquet"):
                    continue
                raw = download_from_storage(bucket, blob.name)
                try:
                    table = pq.read_table(pa.BufferReader(raw))
                except Exception as exc:
                    logger.warning("[REPO-CI] kill-switch audit-log parquet parse failed for %s: %s", blob.name, exc)
                    continue
                for row in table.to_pylist():
                    parsed = _parse_kill_switch_audit_row(row)
                    if parsed:
                        entries.append(parsed)
        except Exception as exc:
            logger.warning("[REPO-CI] kill-switch audit-log read failed for %s: %s", prefix, exc)
    return entries


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
    entries: list[AlertEntryDict] = []
    try:
        bucket = resolve_bucket_name(cloud="gcp", kind="cicd-events")
    except BucketNamingError as exc:
        logger.warning("[REPO-CI] cicd-events bucket resolution failed: %s", exc)
        bucket = None
    if bucket is not None:
        client = get_storage_client()
        dates = [(dt.datetime.now(dt.UTC) - dt.timedelta(days=offset)).strftime("%Y-%m-%d") for offset in range(days)]
        # Alerts ledger: one stream per date.
        for date in dates:
            for prefix in (f"cicd/alerts/{date}/",):
                try:
                    for blob in client.list_blobs(bucket, prefix=prefix):
                        raw = download_from_storage(bucket, blob.name)
                        for line in raw.decode("utf-8", errors="replace").splitlines():
                            parsed = _parse_line(line)
                            if parsed:
                                entries.append(parsed)
                except Exception as exc:
                    logger.warning("[REPO-CI] alert ledger read failed for %s: %s", prefix, exc)
        # Events ledger: per-repo per-date — list once per date across repos via delimiter walk.
        try:
            for blob in client.list_blobs(bucket, prefix="cicd/events/"):
                if not any(f"/{date}/" in blob.name for date in dates):
                    continue
                raw = download_from_storage(bucket, blob.name)
                for line in raw.decode("utf-8", errors="replace").splitlines():
                    parsed = _parse_line(line)
                    if parsed:
                        entries.append(parsed)
        except Exception as exc:
            logger.warning("[REPO-CI] events ledger read failed: %s", exc)
    # alerting-service's own store — its own bucket, ~20 alert classes feeding
    # #uts-live-alerts/#data-pipeline-alerts (deployment_alerts_ingestion_completeness_2026_07_20.md
    # todo 3). A best-effort merge: a read failure here must not blank out the CI ledgers above.
    entries.extend(_read_alerting_service_sync(days))
    # KillSwitchBus arm/disarm parquet audit log — a cheap read-only projection of an existing
    # store (deployment_alerts_ingestion_completeness_2026_07_20.md todo 9). Best-effort merge, same
    # pattern as the alerting-service store above.
    entries.extend(_read_kill_switch_audit_log_sync(days))
    return entries


async def _get_cached_entries(days: int) -> list[AlertEntryDict]:
    """Read (or reuse the cached) sorted ledger entries for a given day-window.

    Cached per `days` value so re-paginating (offset/limit) the SAME window never re-triggers a
    GCS read — only re-reading a wider/narrower window does.
    """
    now = time.monotonic()
    cached = _entries_cache.get(days)
    if cached is not None and now - cached[0] < _CACHE_TTL_SECONDS:
        return cached[1]
    entries = await asyncio.to_thread(_read_ledgers_sync, days)
    entries.sort(key=lambda e: e["timestamp"], reverse=True)
    _entries_cache[days] = (now, entries)
    return entries


async def load_alerts_payload(
    source: str = "live",
    days: int = _DEFAULT_DAYS,
    offset: int = 0,
    limit: int = _DEFAULT_PAGE_SIZE,
) -> AlertsPayloadDict:
    """Read the ledgers (cached 60 s per window) and shape the paginated alerts +
    lifecycle-streams payload.

    Bounded day-partitioned reads only (single-walk discipline unchanged) — `days` is clamped to
    `_MAX_DAYS` so a caller can't force an unbounded walk. `offset`/`limit` paginate the alerts
    list explicitly; `capped` tells the caller whether more rows exist beyond this page, so a
    truncation is never silent.
    """
    days = max(1, min(days, _MAX_DAYS))
    offset = max(0, offset)
    limit = max(1, min(limit, _MAX_PAGE_SIZE))
    entries = await _get_cached_entries(days)
    # Include all alert-class entries (CI "alert" + non-CI kinds) in the alerts list.
    # "event" (promotion workflow state) entries are streams-only, not alerts.
    alert_entries = [e for e in entries if e["kind"] != "event"]
    total_count = len(alert_entries)
    page = alert_entries[offset : offset + limit]
    payload = AlertsPayloadDict(
        generated_at=dt.datetime.now(dt.UTC).isoformat(),
        source=source,
        alerts=page,
        # Streams derive from the FULL window (not just this page) — the (repo, workflow)
        # lifecycle roll-up is already bounded by cardinality, not row count, so slicing it by
        # page would silently drop streams whose entries all fall outside the current page.
        streams=derive_streams(entries),
        days=days,
        total_count=total_count,
        returned_count=len(page),
        offset=offset,
        limit=limit,
        capped=offset + len(page) < total_count,
    )
    return payload

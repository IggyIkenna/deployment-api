"""
Non-CI ops alert ledger reader for the unified /api/alerts endpoint.

Agent-orchestrator (and other ops watchers) write structured alerts to
gs://{CICD_EVENTS_BUCKET}/ops/alerts/{date}/alert_{ts}.json
when conditions like git-health RED, worker-liveness watchdog-kill, VM-down,
consolidator-down, or data-pipeline failures are detected.

This module reads those blobs and maps them to AlertEntryDict entries
using the same kind-extensible shape as the CI alert ledger
(_repo_ci_alerts.py). New kinds added here:
  "git_health"        — slot/worktree git staleness
  "worker_liveness"   — watchdog kill / frozen worker
  "vm_down"           — VM unreachable / health-check failed
  "consolidator_down" — manifest consolidator stale/missing
  "data_pipeline"     — data capture failures / pipeline stall

Plan: deployment_ui_monitoring_pane_2026_06_19.md (unified ledger INFRA P1).
"""

from __future__ import annotations

import asyncio
import datetime as dt
import json
import logging
import time
from typing import TypedDict, cast

from unified_trading_library import download_from_storage, get_storage_client

from ._repo_ci_alerts import AlertEntryDict

logger = logging.getLogger(__name__)

_BUCKET = "unified-trading-cicd-events"
_OPS_PREFIX = "ops/alerts/"
_CACHE_TTL_SECONDS = 60.0
_DEFAULT_DAYS = 2
_MAX_ITEMS = 200

_cache: tuple[float, list[AlertEntryDict]] | None = None


class NonCiAlertsPayloadDict(TypedDict):  # CORRECT-LOCAL: non-CI ops alert GCS payload shape (internal)
    """Typed shape for the non-CI ops alert read result."""

    generated_at: str
    entries: list[AlertEntryDict]


def _parse_ops_line(line: str) -> AlertEntryDict | None:
    """Parse one ops_alert JSON blob into an AlertEntryDict (lenient — skip junk)."""
    line = line.strip()
    if not line:
        return None
    try:
        raw = cast(object, json.loads(line))
    except json.JSONDecodeError:
        return None
    if not isinstance(raw, dict):
        return None
    obj = cast(dict[str, object], raw)
    if str(obj.get("event_type") or "") != "ops_alert":
        return None
    kind = str(obj.get("kind") or "")
    if not kind:
        return None
    return AlertEntryDict(
        kind=kind,
        timestamp=str(obj.get("timestamp") or ""),
        repo=str(obj.get("source") or "agent-orchestrator"),
        workflow_name=f"ops-{kind}",
        severity=str(obj.get("severity") or "INFO"),
        conclusion=None,
        message=str(obj.get("message") or ""),
        run_url=str(obj.get("run_url")) if obj.get("run_url") else None,
    )


def _read_non_ci_alerts_sync(days: int) -> list[AlertEntryDict]:
    """List + read the last N days of ops alert blobs from GCS."""
    client = get_storage_client()
    dates = [(dt.datetime.now(dt.UTC) - dt.timedelta(days=offset)).strftime("%Y-%m-%d") for offset in range(days)]
    entries: list[AlertEntryDict] = []
    for date in dates:
        prefix = f"{_OPS_PREFIX}{date}/"
        try:
            for blob in client.list_blobs(_BUCKET, prefix=prefix):
                try:
                    raw = download_from_storage(_BUCKET, blob.name)
                    for line in raw.decode("utf-8", errors="replace").splitlines():
                        parsed = _parse_ops_line(line)
                        if parsed:
                            entries.append(parsed)
                except Exception as exc:
                    logger.warning("[NON-CI] blob read failed %s: %s", blob.name, exc)
        except Exception as exc:
            logger.warning("[NON-CI] alert ledger list failed for %s: %s", prefix, exc)
    return entries


async def load_non_ci_alerts(days: int = _DEFAULT_DAYS) -> list[AlertEntryDict]:
    """Read non-CI ops alerts (cached 60 s)."""
    global _cache
    now = time.monotonic()
    if _cache is not None and now - _cache[0] < _CACHE_TTL_SECONDS:
        return _cache[1]
    entries = await asyncio.to_thread(_read_non_ci_alerts_sync, days)
    _cache = (now, entries)
    return entries

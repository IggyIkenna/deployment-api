# Epic: features_sports_honest_coverage_2026_05_05
# Lifecycle: permanent
"""Sports coverage-drift snapshot + comparator worker (Phase 8.B cron entrypoint).

Wires the drift comparator shipped in ``deployment_api/services/coverage_drift.py``
(``detect_drift``/``DriftEvent``, `acd2d25`) into an actual daily job: this module snapshots
the features-sports-service manifest's per-(calc, league) coverage to a parquet blob, then
compares today's snapshot against one taken ``lookback_days`` back and persists an alert for
every drop the comparator reports.

Runs IN the deployment-api gen1 service (mirrors ``deployment_api/scripts/data_status_rollup_
worker.py`` and ``cost_snapshot_worker.py`` — a standalone Cloud Run Job crashes on this
repo's native pandas/pyarrow compute; the gen1 Cloud Scheduler->service POST route
(``/api/data-status/sports/coverage-drift-run``) is the working pattern here).

Snapshot storage mirrors ``cost_observability/snapshot.py``: a prefix in deployment-api's
EXISTING state bucket (``DeploymentApiConfig().effective_state_bucket``), not a dedicated
bucket, since the artifact is a handful of small per-day parquets.

Window choice: the comparator's own docstring says "caller can pre-filter to a date window
before passing in" — comparing coverage over the FULL manifest history would make a recent
regression invisible (a few missing days barely moves a whole-history percentage). This
worker reads a ``SNAPSHOT_WINDOW_DAYS``-day trailing window (predicate-pushdown via
``read_manifest_index``'s ``date_window``, bounding the read — no whole-corpus walk) so the
coverage % is sensitive to recent, not lifetime, capture behavior.

Plan: features_sports_honest_coverage_2026_05_05.md, Phase 8.B.
Issue: features_sports_deployment_ui_coverage_tab_and_registry_playbook_2026_07_21.md, todo 3.

CLI (manual/dev invocation; production trigger is the POST route above)::

    python -m deployment_api.scripts.coverage_drift_worker --lookback-days=7
"""

from __future__ import annotations

import argparse
import datetime as _dt
import io
import logging
import sys
from typing import cast

import pandas as pd
from unified_trading_library import download_from_storage, resolve_bucket_name, storage_exists, upload_to_storage

from deployment_api.deployment_api_config import DeploymentApiConfig
from deployment_api.routes.deployments_inventory import _persist_alert  # pyright: ignore[reportPrivateUsage]
from deployment_api.services.coverage_drift import DriftEvent, detect_drift, known_calculators
from deployment_api.services.manifest_source import read_manifest_index

logger = logging.getLogger(__name__)

# Snapshots live under a prefix in deployment-api's existing state bucket — no dedicated
# bucket for a handful of small per-day parquets (mirrors cost_observability/snapshot.py).
SNAPSHOT_BLOB_PREFIX = "coverage-drift-snapshots/sports"

# Trailing window read from the manifest for each snapshot. Wide enough that a real
# calculator's coverage % is stable day-to-day (not dominated by in-progress today's rows),
# narrow enough that a genuine regression this week moves the percentage meaningfully.
SNAPSHOT_WINDOW_DAYS = 30

# Compare today's snapshot against one taken this many days back. Matches the module's own
# docstring ("today's snapshot against N days back") and the shipped HANDOFF spec's default.
DEFAULT_LOOKBACK_DAYS = 7

SNAPSHOT_COLUMNS: tuple[str, ...] = ("feature_group", "league_id", "capture_status")


def snapshot_blob_path(date: str) -> str:
    """GCS object path for one day's sports coverage-drift snapshot parquet."""
    return f"{SNAPSHOT_BLOB_PREFIX}/{date}.parquet"


def _sports_manifest_snapshot(*, window_days: int = SNAPSHOT_WINDOW_DAYS, cloud: str = "gcp") -> pd.DataFrame:
    """Current features-sports-service manifest rows, projected to the comparator's schema.

    Reads a bounded trailing window via ``read_manifest_index``'s predicate-pushdown
    ``date_window`` (row-group filtered, not a whole-corpus decode), then keeps only rows for
    a KNOWN calculator (``coverage_drift.known_calculators()``) — residual/removed-entity rows
    would otherwise dilute the coverage percentage with noise the comparator was never
    designed to see.
    """
    bucket = resolve_bucket_name(cloud=cloud, kind="features-sports")
    end = _dt.datetime.now(_dt.UTC).date()
    start = end - _dt.timedelta(days=window_days)
    idx = read_manifest_index(bucket, date_window=(start.isoformat(), end.isoformat()))
    if idx.empty or "feature_group" not in idx.columns:
        return pd.DataFrame(columns=list(SNAPSHOT_COLUMNS))
    calcs = known_calculators()
    filtered = idx[idx["feature_group"].isin(calcs)]
    cols = [c for c in SNAPSHOT_COLUMNS if c in filtered.columns]
    return filtered[cols].reset_index(drop=True)


def write_snapshot(state_bucket: str, date: str) -> dict[str, object]:
    """Snapshot today's sports manifest coverage rows to GCS. Never raises — degrades to a
    result dict the caller logs (a missed snapshot just means tomorrow has no baseline)."""
    try:
        df = _sports_manifest_snapshot()
        buf = io.BytesIO()
        df.to_parquet(buf, index=False)
        data = buf.getvalue()
        upload_to_storage(state_bucket, snapshot_blob_path(date), data, content_type="application/octet-stream")
        return {"ok": True, "rows": len(df), "bytes": len(data)}
    except Exception as exc:  # worker boundary — must report, not crash the scheduler tick
        logger.error("coverage-drift snapshot write failed for %s: %s", date, exc)
        return {"ok": False, "error": str(exc)}


def _read_snapshot(state_bucket: str, date: str) -> pd.DataFrame | None:
    """Load a prior day's snapshot. Returns None if absent — the first ``lookback_days`` of
    this worker's life have no baseline yet; that is an honest gap, not an error."""
    try:
        path = snapshot_blob_path(date)
        if not storage_exists(state_bucket, path):
            return None
        raw = download_from_storage(state_bucket, path)
        return pd.read_parquet(io.BytesIO(raw))
    except Exception as exc:  # same worker-boundary contract as write_snapshot
        logger.warning("coverage-drift snapshot read failed for %s: %s", date, exc)
        return None


def run_drift_check(state_bucket: str, today: str, lookback_days: int = DEFAULT_LOOKBACK_DAYS) -> list[DriftEvent]:
    """Compare today's snapshot against ``lookback_days`` back; persist one alert per event.

    Requires ``write_snapshot(state_bucket, today)`` to have already run this cycle — this
    function only reads. Returns an empty list (no alerts) when either snapshot is missing,
    so the first week of this worker's life is silent rather than erroring.
    """
    today_df = _read_snapshot(state_bucket, today)
    if today_df is None:
        logger.warning("coverage-drift: today's snapshot (%s) missing — write_snapshot must run first", today)
        return []
    previous_date = (_dt.date.fromisoformat(today) - _dt.timedelta(days=lookback_days)).isoformat()
    previous_df = _read_snapshot(state_bucket, previous_date)
    if previous_df is None:
        logger.info(
            "coverage-drift: no snapshot %d day(s) back (%s) yet — skipping comparison", lookback_days, previous_date
        )
        return []

    events = detect_drift(previous_df, today_df)
    for event in events:
        league = event.league_id or "global"
        _persist_alert(
            alert_class="coverage_drift",
            workflow_name=f"sports-coverage-drift-{event.calc}-{league}",
            severity="WARNING",
            message=str(event),
            dedup_key=f"coverage-drift-{event.calc}-{league}-{today}",
        )
    return events


def run(lookback_days: int = DEFAULT_LOOKBACK_DAYS) -> dict[str, object]:
    """Write today's snapshot, then run + alert on the drift comparison. Scheduler entrypoint.

    Never raises (both steps degrade to a result dict) so a Cloud Scheduler POST always gets a
    clean 200 — a transient GCS hiccup on one cycle just means no snapshot/alert this run, and
    the next day's tick tries again.
    """
    today = _dt.datetime.now(_dt.UTC).date().isoformat()
    state_bucket = DeploymentApiConfig().effective_state_bucket
    write_result = write_snapshot(state_bucket, today)
    events = run_drift_check(state_bucket, today, lookback_days) if write_result.get("ok") else []
    return {
        "date": today,
        "write": write_result,
        "drift_events": [str(e) for e in events],
        "drift_event_count": len(events),
    }


def main() -> int:
    parser = argparse.ArgumentParser(description="Sports coverage-drift snapshot + comparator worker")
    parser.add_argument(
        "--lookback-days",
        type=int,
        default=DEFAULT_LOOKBACK_DAYS,
        help=f"Days back to compare against (default: {DEFAULT_LOOKBACK_DAYS})",
    )
    args = parser.parse_args()
    lookback_days = cast("int", args.lookback_days)

    result = run(lookback_days)
    logger.info("coverage-drift run: %s", result)
    write_ok = cast("dict[str, object]", result["write"]).get("ok")
    return 0 if write_ok else 1


if __name__ == "__main__":
    sys.exit(main())

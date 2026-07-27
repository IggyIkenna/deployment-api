"""Writers for the durable operational-data BigQuery tables (idle_spend, reap_events).

deployment_durable_operational_data_bigquery_2026_07_21.md — the central-side counterpart
to the VM-side flat-event publishers in deployment-service. Uses the UTL
`insert_rows` streaming-insert wrapper — never a raw `INSERT` SQL string or
`google.cloud.bigquery` — so no query-building/escaping concerns apply here at all.

Best-effort — a write failure here must never break the reap/delete/snapshot request
that triggered it (same shard-level-failure-isolation contract the VM-side publishers
already follow).
"""

from __future__ import annotations

import logging
from datetime import UTC, datetime

from unified_trading_library import get_analytics_client

logger = logging.getLogger(__name__)

DATASET = "deployment_operational_data"


def _utcnow_iso() -> str:
    return datetime.now(UTC).strftime("%Y-%m-%dT%H:%M:%SZ")


def write_reap_event(
    project_id: str,
    *,
    vm_name: str,
    age_hours: float | None,
    reclaimed_usd_per_month: float,
    actor: str,
    dry_run: bool,
) -> None:
    """One row per successfully-deleted VM. Skipped entirely on dry_run (nothing happened)."""
    if dry_run:
        return
    try:
        client = get_analytics_client(provider="gcp", project_id=project_id)
        client.insert_rows(
            "reap_events",
            [
                {
                    "ts": _utcnow_iso(),
                    "vm_name": vm_name,
                    "age_hours": age_hours,
                    "reclaimed_usd_per_month": reclaimed_usd_per_month,
                    "actor": actor,
                    "dry_run": dry_run,
                }
            ],
            dataset=DATASET,
        )
    except Exception as exc:
        logger.warning("reap_events insert failed for %s: %s", vm_name, exc)


def write_idle_spend_snapshot(
    project_id: str,
    *,
    stopped_total: int,
    reapable_total: int,
    monthly_idle_usd: float,
    monthly_reapable_usd: float,
    per_resource: list[dict[str, object]],
) -> int:
    """One ROLLUP row (resource_name=NULL, the 4 totals) + one row per idle resource.

    `per_resource` entries: {"resource_name", "lifecycle_class", "age_hours",
    "monthly_idle_usd", "monthly_reapable_usd"} — the latter None when not reapable.
    Returns the number of rows written (0 on failure, logged — never raises).
    """
    ts = _utcnow_iso()
    rows: list[dict[str, object]] = [
        {
            "ts": ts,
            "resource_name": None,
            "lifecycle_class": None,
            "age_hours": None,
            "stopped_total": stopped_total,
            "reapable_total": reapable_total,
            "monthly_idle_usd": monthly_idle_usd,
            "monthly_reapable_usd": monthly_reapable_usd,
        }
    ]
    for r in per_resource:
        rows.append(
            {
                "ts": ts,
                "resource_name": r.get("resource_name"),
                "lifecycle_class": r.get("lifecycle_class"),
                "age_hours": r.get("age_hours"),
                "stopped_total": None,
                "reapable_total": None,
                "monthly_idle_usd": r.get("monthly_idle_usd"),
                "monthly_reapable_usd": r.get("monthly_reapable_usd"),
            }
        )
    try:
        client = get_analytics_client(provider="gcp", project_id=project_id)
        return client.insert_rows("idle_spend", rows, dataset=DATASET)
    except Exception as exc:
        logger.warning("idle_spend snapshot insert failed: %s", exc)
        return 0

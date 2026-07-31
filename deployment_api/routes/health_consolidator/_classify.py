# Epic: observability_master
# Lifecycle: permanent
"""``health_consolidator`` freshness/verdict classification — split out of the facade (2026-07-31).

Pure functions (status/verdict derivation, worst-first rollup, the AG-projection) with no
GCS I/O and no module-level collaborator the test suite patches by name — safe to live in
their own module regardless of which submodule calls them.
"""

from __future__ import annotations

from datetime import datetime

from deployment_api.routes._cloud_run_executions import CloudRunExecutionStatus
from deployment_api.routes.health_consolidator._models import (
    ConsolidatorAgHealth,
    ConsolidatorHealth,
    ConsolidatorHealthResponse,
)


def _status_rank(status: str) -> int:
    """Order statuses worst-first for the overall rollup."""
    return {"critical": 0, "degraded": 1, "unknown": 2, "ok": 3}.get(status, 2)


def _classify_ag(age: float | None, budget: int, shards_exist: bool) -> tuple[str, bool, str]:
    """Derive (status, fallback_active, detail) for one asset_group.

    * Index FRESH (age <= budget) → ok.
    * Index STALE/MISSING **and** per-VM shards exist → the consolidator is behind/down
      and the read path would fall back to the OOM-prone recovery merge → critical.
    * Index STALE/MISSING but **no** shards → a genuinely empty / never-written bucket,
      not an outage → degraded (nothing to consolidate yet).
    """
    if age is not None and age <= budget:
        return "ok", False, f"index heartbeat {age:.0f}s old (<= {budget}s budget)"
    if shards_exist:
        age_str = f"{age:.0f}s" if age is not None else "missing"
        return (
            "critical",
            True,
            f"index {age_str} (> {budget}s budget) while per-VM shards exist — consolidator behind/DOWN",
        )
    age_str = f"{age:.0f}s old" if age is not None else "missing"
    return "degraded", False, f"index {age_str}; no per-VM shards — genuinely empty bucket, not an outage"


def _is_fired_but_empty(
    exec_status: CloudRunExecutionStatus | None, index_age: float | None, budget: int, now: datetime
) -> bool:
    """Did the job fire successfully-and-recently yet leave a STALE index → wrote nothing?

    The consolidator touches the index mtime EVERY cycle (incl. no-op cycles), so a recent
    SUCCEEDED execution should have advanced the index. If the latest execution succeeded within
    the budget window but the index is nonetheless older than the budget, the run exited 0 and
    produced nothing — the silent failure a liveness-only view shows as "succeeded". If the last
    success is ALSO old (> budget), that's just down/behind (``stale_output``), not fired-but-empty.
    """
    if exec_status is None or exec_status.exit_code != 0 or exec_status.last_run_at is None:
        return False
    if index_age is None or index_age <= budget:
        return False  # index fresh → the run DID write; not empty
    try:
        exec_dt = datetime.fromisoformat(exec_status.last_run_at)
    except ValueError:
        return False
    if exec_dt.tzinfo is None:  # can't safely diff a naive stamp against tz-aware ``now``
        return False
    exec_age = (now - exec_dt).total_seconds()
    return 0 <= exec_age <= budget  # a RECENT green run against a STALE index


def _verdict(status: str, pending: int | None, *, fired_but_empty: bool = False) -> str:
    """Data-correctness lens: the execution-join ``fired_but_empty`` first, else freshness+backlog.

    ``fired_but_empty`` (a recent SUCCEEDED execution against a stale index — see
    ``_is_fired_but_empty``) is the precise silent-failure signal and takes precedence over the
    freshness-derived ``stale_output``; the rest is derived from what one cheap index stat +
    shard-list tell us.
    """
    if fired_but_empty:
        return "fired_but_empty"  # execution succeeded recently yet the index is stale → wrote nothing
    if status == "critical":
        return "stale_output"  # index stale while per-VM shards wait → output is behind
    if status == "degraded":
        return "empty"  # stale/missing index but nothing to consolidate
    if status == "ok":
        return "producing" if (pending or 0) > 0 else "produced"
    return "unknown"


def _authoritative_verdict(
    run_verdict: str | None, freshness_verdict: str, pending: int | None, index_row_count: int | None
) -> str:
    """Map the consolidator's SELF-REPORTED run verdict onto the endpoint vocabulary.

    ``latest.json`` is authoritative for what the run KNOWS (it failed; it's live), but its per-run
    ``empty`` means "this CYCLE wrote 0 rows" — a NO-OP cycle on a fully-populated index reports
    ``empty`` too — so it is NOT a reliable "the index is empty" signal. We reconcile against the
    real ``index_row_count`` (cheap parquet footer):

    * ``failed`` → ``stale_output`` (the freshness view can't see a failed run).
    * ``produced`` → produced/producing (per backlog).
    * ``empty`` **and the index actually holds rows** → a no-op cycle on real data → defer to the
      freshness-derived verdict (produced / producing / stale_output per status + backlog).
    * ``empty`` **and the index is genuinely empty** → ``fired_but_empty`` ONLY if shards were waiting
      to be absorbed (``pending > 0``), else a genuinely idle bucket (``empty``).
    * anything else / absent → freshness-derived.
    """
    if run_verdict == "failed":
        return "stale_output"
    if run_verdict == "produced":
        return "producing" if (pending or 0) > 0 else "produced"
    if run_verdict == "empty":
        if (index_row_count or 0) > 0:
            return freshness_verdict  # no-op cycle on a populated index → not "empty"
        return "fired_but_empty" if (pending or 0) > 0 else "empty"
    return freshness_verdict


def _ag_from_consolidator(c: ConsolidatorHealth) -> ConsolidatorAgHealth:
    """Project a market-data ConsolidatorHealth back onto the legacy per-AG shape (no extra GCS read)."""
    return ConsolidatorAgHealth(
        asset_group=c.asset_group or "",
        bucket=c.bucket,
        status=c.status,
        index_age_seconds=c.index_age_seconds,
        staleness_budget_seconds=c.staleness_budget_seconds,
        per_vm_shard_fallback_active=c.status == "critical",
        last_successful_run_at=c.last_successful_run_at,
        pending_shard_count=c.pending_shard_count,
        total_shard_count=c.total_shard_count,
        detail=c.detail,
    )


def build_consolidator_health(
    ag_entries: list[ConsolidatorAgHealth],
    now: datetime,
    consolidators: list[ConsolidatorHealth] | None = None,
) -> ConsolidatorHealthResponse:
    """Roll postures into the response with a worst-first overall across the whole estate."""
    consolidators = consolidators or []
    statuses = [c.status for c in consolidators] or [e.status for e in ag_entries]
    overall = min(statuses, key=_status_rank) if statuses else "ok"
    return ConsolidatorHealthResponse(
        generated_at=now.isoformat(),
        overall=overall,
        asset_groups=ag_entries,
        consolidators=consolidators,
    )

# Epic: observability_master
# Lifecycle: permanent
"""``health_consolidator`` mock-mode estate — split out of the facade for size (2026-07-31).

Pure sample-data construction (no I/O, no test-patched module-level seam) — safe to live
in its own module regardless of which submodule builds the mock response.
"""

from __future__ import annotations

from datetime import datetime

from deployment_api.routes.health_consolidator._classify import (
    _ag_from_consolidator,
    build_consolidator_health,
)
from deployment_api.routes.health_consolidator._models import ConsolidatorHealth, ConsolidatorHealthResponse
from deployment_api.routes.health_consolidator._reads import _AUDIT_BEARING_KINDS


def _mock_consolidator(
    category: str,
    kind: str,
    asset_group: str | None,
    status: str,
    verdict: str,
    age: float | None,
    pending: int,
    total: int,
    detail: str,
    ts: str,
    *,
    exec_status: str = "succeeded",
    exec_exit: int | None = 0,
    reporting: bool = True,
    trigger_cron: str = "*/1 * * * *",  # matches the live estate — every consolidator shares this cron
) -> ConsolidatorHealth:
    # Map the endpoint verdict back to the consolidator's self-reported run verdict for the mock.
    run_verdict = {"fired_but_empty": "empty", "stale_output": "failed", "empty": "empty"}.get(verdict, "produced")
    _audits = kind in _AUDIT_BEARING_KINDS  # phantom/reprobe audits only touch market-data / instruments
    return ConsolidatorHealth(
        category=category,
        kind=kind,
        asset_group=asset_group,
        job_name=f"uts-prod-manifest-consolidator-{category}",
        bucket=f"{category}-mock",
        status=status,
        verdict=verdict,
        index_age_seconds=age,
        staleness_budget_seconds=86400,
        trigger_cron=trigger_cron,
        last_successful_run_at=ts if age is not None else None,
        pending_shard_count=pending,
        total_shard_count=total,
        # Oldest un-absorbed shard ≈ the index age when a backlog is waiting (merge-stuck-for).
        oldest_pending_shard_age_seconds=age if (pending > 0 and age is not None) else None,
        index_row_count=(1_000_000 + total * 50_000) if age is not None else None,
        index_size_bytes=(20_000_000 + total * 4_000_000) if age is not None else None,
        execution_status=exec_status,
        execution_last_run_at=ts,
        execution_exit_code=exec_exit,
        # A reporting consolidator publishes latest.json; a dead one (reporting=False) has none.
        run_reporting=reporting,
        run_verdict=run_verdict if reporting else None,
        run_last_run_at=ts if reporting else None,
        run_shards_changed=(pending if reporting else None),
        run_rows_added=(pending * 1000 if reporting else None),
        run_duration_ms=(8400.0 if reporting else None),
        # Dark data-correctness actors run only on market-data / instruments buckets (mock sample).
        phantom_audit_at=ts if _audits else None,
        phantom_count=(total % 4) if _audits else None,
        # Placeholder path (no real gs:// URI / project id — the live endpoint carries the
        # reconcile-published gs:// link; the UI treats it opaquely).
        phantom_triage_link=("mock://phantom-triage/triage_mock.jsonl" if (_audits and (total % 4) > 0) else None),
        reprobe_audit_at=ts if _audits else None,
        reprobe_new_empties=pending if _audits else None,
        reprobe_disagreements=(1 if pending > 5 else 0) if _audits else None,
        reprobe_reclassified=0 if _audits else None,
        detail=detail,
    )


def _mock_response(now: datetime) -> ConsolidatorHealthResponse:
    """Representative mock consolidator estate (mock mode — no GCS access) spanning kinds + statuses."""
    ts = now.isoformat()
    consolidators = [
        _mock_consolidator(
            "market-data-cefi",
            "market-data",
            "cefi",
            "ok",
            "producing",
            42.0,
            2,
            6,
            "index heartbeat 42s old (<= 86400s budget)",
            ts,
        ),
        _mock_consolidator(
            "market-data-defi",
            "market-data",
            "defi",
            "critical",
            "stale_output",
            90000.0,
            47,
            48,
            "index 90000s (> 86400s budget) while per-VM shards exist — consolidator behind/DOWN",
            ts,
        ),
        _mock_consolidator(
            "instruments-cefi",
            "instruments",
            "cefi",
            "ok",
            "produced",
            310.0,
            0,
            12,
            "index heartbeat 310s old (<= 86400s budget)",
            ts,
        ),
        _mock_consolidator(
            "features-delta-one-cefi",
            "features-delta-one",
            "cefi",
            "ok",
            "produced",
            120.0,
            0,
            4,
            "index heartbeat 120s old (<= 86400s budget)",
            ts,
        ),
        _mock_consolidator(
            "execution-cefi",
            "execution",
            "cefi",
            "degraded",
            "empty",
            None,
            0,
            0,
            "index missing; no per-VM shards — genuinely empty bucket, not an outage",
            ts,
        ),
        _mock_consolidator(
            "gas-fees", "gas-fees", None, "ok", "produced", 88.0, 0, 3, "index heartbeat 88s old (<= 86400s budget)", ts
        ),
        _mock_consolidator(
            "features-onchain-defi",
            "features-onchain",
            "defi",
            "critical",
            "fired_but_empty",
            95000.0,
            5,
            9,
            "execution SUCCEEDED recently yet index 95000s (> 86400s budget) — job ran green but wrote nothing",
            ts,
            exec_status="succeeded",
            exec_exit=0,
        ),
        # A DEAD consolidator — declared in the catalog but never fired up, so it publishes no
        # latest.json. The tab must show it honestly as "not yet reporting", never a fake all-clear.
        _mock_consolidator(
            "strategy",
            "strategy",
            None,
            "degraded",
            "empty",
            None,
            0,
            0,
            "no index and no shards — consolidator not yet fired up (not reporting)",
            ts,
            exec_status="pending",
            exec_exit=None,
            reporting=False,
        ),
    ]
    ag_entries = [_ag_from_consolidator(c) for c in consolidators if c.kind == "market-data" and c.asset_group]
    return build_consolidator_health(ag_entries, now, consolidators=consolidators)

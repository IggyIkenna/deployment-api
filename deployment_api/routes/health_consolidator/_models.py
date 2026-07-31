# Epic: observability_master
# Lifecycle: permanent
"""``health_consolidator`` API contract models — split out of the facade for size (2026-07-31).

Pure pydantic response shapes, no I/O and no module-level collaborators the test suite
patches — safe to live in their own module regardless of which submodule builds them.
"""

from __future__ import annotations

from pydantic import BaseModel, Field


class ConsolidatorAgHealth(BaseModel):  # CORRECT-LOCAL: FastAPI API contract model
    """Per-asset_group manifest-consolidator posture."""

    asset_group: str
    bucket: str
    status: str  # "ok" | "degraded" | "critical" | "unknown"
    index_age_seconds: float | None = None  # heartbeat age of the consolidated index
    staleness_budget_seconds: int
    per_vm_shard_fallback_active: bool  # stale/missing index WHILE shards exist → recovery merge
    last_successful_run_at: str | None = None  # ISO-8601, derived from index mtime
    pending_shard_count: int | None = None  # per-VM shards written since the last merge (backlog)
    total_shard_count: int | None = None  # per-VM shards present (fan-in width)
    detail: str


class ConsolidatorHealth(BaseModel):  # CORRECT-LOCAL: FastAPI API contract model
    """Per-CONSOLIDATOR posture — one entry per (kind, asset_group) in the terraform estate.

    The full estate (~25 Cloud Run consolidator jobs) vs the 5 market-data ``asset_groups``
    the legacy view carried. Sourced from the generated ``consolidator_catalog`` (a projection
    of the deployment-service terraform), so a newly-declared consolidator surfaces here with
    no code change once the catalog is regenerated.
    """

    category: str  # terraform key + the Cloud Run job suffix (e.g. "market-data-cefi")
    kind: str  # grouping axis (market-data / instruments / features-* / execution / …)
    asset_group: str | None  # None for flat consolidators (strategy / gas-fees / ml-…)
    job_name: str  # Cloud Run job short-name — the join key the deployments popover links on
    bucket: str
    status: str  # ok | degraded | critical | unknown  (freshness posture)
    verdict: str  # produced | producing | stale_output | fired_but_empty | empty | unknown  (data-correctness)
    index_age_seconds: float | None = None
    staleness_budget_seconds: int
    trigger_cron: str | None = None  # Cloud Scheduler cron that fires this consolidator (e.g. "*/1 * * * *")
    last_successful_run_at: str | None = None
    pending_shard_count: int | None = None  # backlog: shards written since the last merge
    total_shard_count: int | None = None  # fan-in width: how many per-VM shards feed this index
    oldest_pending_shard_age_seconds: float | None = None  # age of the OLDEST un-absorbed shard (merge-stuck-for)
    index_row_count: int | None = None  # absolute rows in the consolidated index (parquet num_rows)
    index_size_bytes: int | None = None  # size of the consolidated index file on disk
    # Cloud Run execution truth for THIS consolidator job (the fired-but-empty discriminator):
    # a recent SUCCEEDED execution whose index is nonetheless stale = the job ran green but wrote
    # nothing (verdict ``fired_but_empty``), vs a genuinely down/behind job (no recent success).
    execution_status: str | None = None  # running | succeeded | failed | pending  (latest execution)
    execution_last_run_at: str | None = None  # ISO-8601 completion time of the latest execution
    execution_exit_code: int | None = None  # 0 succeeded / 1 failed (synthesised from Cloud Run counts)
    # AUTHORITATIVE per-run summary the consolidator job self-publishes to _index/latest.json each
    # cycle (UTL manifest_consolidator). ``run_reporting`` = a latest.json exists = the consolidator
    # is live and reporting; False = dead / never-run (no latest.json). When present, its verdict is
    # authoritative for produced/empty/failed (supersedes the Cloud-Run-execution inference).
    run_reporting: bool = False  # does this consolidator publish a latest.json (is it live)?
    run_verdict: str | None = None  # produced | empty | failed  (self-reported)
    run_last_run_at: str | None = None  # ISO-8601 of the last self-reported run
    run_shards_changed: int | None = None  # shards merged in the last run
    run_rows_added: int | None = None  # rows added to the index in the last run
    run_duration_ms: float | None = None  # last run's duration
    # Dark data-correctness actors — the last phantom audit + last empty re-probe for THIS AG, self-
    # published by the instruments-service reconcile + e2e-testing re-probe scripts to
    # _index/{phantom,reprobe}_audit_latest.json in this bucket. Absent = "no audit yet" (the UI shows
    # the staleness loudly; phantom is ~weekly). None for buckets no audit targets (features/execution/flat).
    phantom_audit_at: str | None = None  # ISO-8601 of the last phantom audit
    phantom_count: int | None = None  # phantoms found in the last audit (0 = clean, honest)
    phantom_triage_link: str | None = None  # gs:// drill-down JSONL, when phantoms > 0
    reprobe_audit_at: str | None = None  # ISO-8601 of the last empty re-probe
    reprobe_new_empties: int | None = None  # new empty_confirmed cells re-probed
    reprobe_disagreements: int | None = None  # oracle/re-fetch says data SHOULD exist (candidate C1 bugs)
    reprobe_reclassified: int | None = None  # proven-misclassified empties auto-flipped to attempted_failed
    detail: str


class ConsolidatorHealthResponse(BaseModel):  # CORRECT-LOCAL: FastAPI API contract model
    """GET /api/health/consolidator response — per-AG drill-down + the full per-consolidator estate."""

    generated_at: str  # ISO-8601 UTC
    overall: str  # worst status across the estate (ok|degraded|critical|unknown)
    asset_groups: list[ConsolidatorAgHealth] = Field(default_factory=list)  # legacy: the 5 market-data AGs
    consolidators: list[ConsolidatorHealth] = Field(default_factory=list)  # the full ~25-job estate

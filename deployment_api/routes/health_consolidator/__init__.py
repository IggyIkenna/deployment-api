# Epic: observability_master
# Lifecycle: permanent
"""GET /api/health/consolidator — manifest-consolidator health drill-down per asset_group.

Replaces today's binary up/down (``CONSOLIDATOR_DOWN`` alert / ``assert_consolidator_healthy``
raise) with a per-asset_group posture the cockpit Health pane can render: for each
asset_group's ``market-data`` bucket we report the consolidated ``_index/
availability_index.parquet`` heartbeat age (the consolidator touches its mtime every
cycle, incl. no-op cycles), whether per-VM shards exist behind a stale/missing index
(= the consolidator is BEHIND or DOWN, the recovery-merge fallback would activate), and
the derived health status.

Reuse: the SAME ``unified_trading_library.manifest_writer`` internals the consolidator
liveness contract uses — ``consolidated_blob_age_sec`` (the heartbeat), ``per_vm_shards_exist``
(the stale-vs-empty discriminator), ``resolve_consolidated_staleness_sec`` (the budget).
NO new GCS-walk; one metadata read (``blob.reload()``) + one cheap shard-list per AG.

Honest degradation: a per-AG read failure yields ``status="unknown"`` for that AG (logged),
never a 5xx — this is a read-only monitoring endpoint.

Plan: unified-trading-pm/plans/active/unified_deployment_health_cockpit_2026_06_23.md Phase 1.
SSOT: codex/05-infrastructure/manifest-consolidator-ssot.md +
codex/05-infrastructure/deployment-observability.md.

Split 2026-07-31 into a facade package (mirrors the 2026-06-11 ``routes/deployments``
precedent — pure code motion; ``deployment_api_qg_size_gate_debt_2026_07_30.md``). The
API contract models (``_models.py``), the freshness/verdict classification helpers
(``_classify.py``), the consolidator catalog loader (``_catalog.py``), the per-bucket
cheap-read helpers (``_reads.py``), and the mock-mode estate (``_mock.py``) moved out —
none of them is a module-qualified seam the test suite patches by name (every GCS
collaborator they touch is passed in as an already-resolved ``client``/``entry`` argument,
never read off this module's own globals). The functions that ARE patched by name
(``deployment_api.routes.health_consolidator.<name>`` — ``resolve_bucket_name`` /
``get_storage_client`` / ``consolidated_blob_age_sec`` / ``per_vm_shard_backlog`` /
``per_vm_shards_exist`` / ``read_availability_index`` / ``_compute_consolidator_health``)
stay physically in THIS module, so the existing patch surface keeps intercepting.
"""

from __future__ import annotations

import logging
import threading
import time
from concurrent.futures import ThreadPoolExecutor
from datetime import UTC, datetime, timedelta

from fastapi import APIRouter
from unified_trading_library import (
    AssetGroup,
    consolidated_blob_age_sec,
    get_storage_client,
    per_vm_shard_backlog,
    per_vm_shards_exist,
    read_availability_index,
    resolve_bucket_name,
    resolve_consolidated_staleness_sec,
)

from deployment_api.deployment_api_config import DeploymentApiConfig
from deployment_api.routes._cloud_run_executions import (
    CloudRunExecutionStatus,
    latest_execution_by_job,
)
from deployment_api.routes.health_consolidator._catalog import _CATALOG, _catalog_bucket, _env_project
from deployment_api.routes.health_consolidator._classify import (
    _ag_from_consolidator,
    _authoritative_verdict,
    _classify_ag,
    _is_fired_but_empty,
    _status_rank,
    _verdict,
    build_consolidator_health,
)
from deployment_api.routes.health_consolidator._mock import _mock_response
from deployment_api.routes.health_consolidator._models import (
    ConsolidatorAgHealth,
    ConsolidatorHealth,
    ConsolidatorHealthResponse,
)
from deployment_api.routes.health_consolidator._reads import (
    _as_float,
    _as_int,
    _as_str,
    _audit_fields,
    _index_absolutes,
    _read_latest_run,
)

router = APIRouter()
logger = logging.getLogger(__name__)

_cfg = DeploymentApiConfig()

# The asset_groups whose market-data buckets carry an availability_index the
# consolidator maintains (the canonical lowercase set, UAC AssetGroup literals).
_ASSET_GROUPS: tuple[AssetGroup, ...] = ("cefi", "defi", "tradfi", "sports", "prediction")

# Per-asset_group market-data bucket KIND. cefi/defi/tradfi/sports live under the shared
# ``market-data`` kind (the ``market-data-tick-<ag>-...`` buckets); prediction has its own
# dedicated flat key ``market-data-tick-prediction`` (``market-data-tick-pred-...``), so the
# shared ``market-data`` kind has no ``prediction`` entry. Resolve the right kind per AG.
_MARKET_DATA_KIND: dict[str, str] = {"prediction": "market-data-tick-prediction"}


def _market_data_kind(asset_group: str) -> str:
    """Bucket kind for an asset_group's market-data store (prediction has a dedicated key)."""
    return _MARKET_DATA_KIND.get(asset_group, "market-data")


# Per-asset_group consolidated-staleness budget. Most AGs' market-data consolidator runs
# ~every minute, so the global default (``resolve_consolidated_staleness_sec()`` = 120s) is
# right. cefi market-tick is a DAILY batch (capture cron ``0 6 * * *``) and its consolidator
# effectively runs only ~every 5 min, so a 120s budget false-flags it ``degraded`` ~60% of
# every cycle even though nothing is wrong; cefi's own launchers set the intended tolerance to
# 86400s (``MANIFEST_CONSOLIDATED_STALENESS_SEC``) — mirror that so the health check matches the
# AG's real cadence and only fires on a genuine >24h stall. Verified 2026-07-09 (Cloud Run
# executions 5 min apart, index age climbing 174→228s under the 120s budget).
# sports' consolidated blob refreshes on a ~11-min cadence (observed 17:00:41 -> 17:11:42 UTC),
# so it routinely aged past the generic 120s default and false-flagged a healthy consolidator as
# DOWN in this cockpit view — the identical class the cefi override fixed. 1800s (30min)
# comfortably covers the observed cadence with margin while staying well under a horizon that
# would mask a genuine multi-hour outage. defi had the same missing-override gap (its own real
# merge cadence is ~31-32min — see
# ``AG_CONSOLIDATOR_INFLIGHT_HORIZON_SEC["defi"]`` in the UTL module below), just undiscovered
# longer because the long cadence made every read fall into the expensive per-VM-shard-merge
# fallback almost every time rather than only occasionally
# (defi_manifest_consolidator_staleness_budget_missing_2026_07_29.md). 3600s (1h) mirrors the same
# margin philosophy as the sports fix. Mirrors
# ``unified-trading-library/unified_trading_library/manifest_writer/_staleness_budget.py``'s
# ``AG_STALENESS_BUDGET_SEC`` (duplicated, not imported — deployment-api depends on UTL, not vice
# versa; keep the two dicts in sync).
_AG_STALENESS_BUDGET_SEC: dict[str, int] = {"cefi": 86400, "sports": 1800, "defi": 3600}


def _budget_for(asset_group: str, default: int) -> int:
    """Staleness budget for an asset_group — its cadence-matched override, else the global default."""
    return _AG_STALENESS_BUDGET_SEC.get(asset_group, default)


def _consolidator_health(
    entry: dict[str, str | None],
    budget: int,
    now: datetime,
    exec_status: CloudRunExecutionStatus | None = None,
) -> ConsolidatorHealth:
    """One consolidator's posture: freshness + backlog + fan-in + verdict (single index stat + shard-list).

    ``exec_status`` (the job's latest Cloud Run execution, looked up once per request) enables the
    ``fired_but_empty`` verdict — a recent green run against a stale index. Absent it, the verdict
    degrades to the freshness-derived signal (``stale_output`` / ``producing`` / …).
    """
    base = {
        "category": entry["category"] or "",
        "kind": entry["kind"] or "",
        "asset_group": entry["asset_group"],
        "job_name": entry["job_name"] or "",
        "staleness_budget_seconds": budget,
        "trigger_cron": entry.get("trigger_cron"),
    }
    exec_kind = exec_status.status if exec_status is not None else None
    exec_run_at = exec_status.last_run_at if exec_status is not None else None
    exec_exit = exec_status.exit_code if exec_status is not None else None
    try:
        bucket = _catalog_bucket(entry)
    except (KeyError, ValueError) as exc:
        return ConsolidatorHealth(
            **base,
            execution_status=exec_kind,
            execution_last_run_at=exec_run_at,
            execution_exit_code=exec_exit,
            bucket="",
            status="unknown",
            verdict="unknown",
            detail=f"bucket resolve failed: {exc}",
        )
    try:
        client = get_storage_client()
        age = consolidated_blob_age_sec(client, bucket)
        index_mtime = (now - timedelta(seconds=age)) if age is not None else None
        # ONE prefix list gives the backlog counts, the fan-in width, AND the oldest pending shard.
        backlog = per_vm_shard_backlog(client, bucket, index_mtime)
        pending_count, total_count = backlog.pending, backlog.total
        oldest_pending_age = (
            round((now - backlog.oldest_pending_at).total_seconds(), 1)
            if backlog.oldest_pending_at is not None
            else None
        )
        status, _, detail = _classify_ag(age, budget, total_count > 0)
        # Absolute snapshot of the consolidated index (rows via a cheap footer read, size via metadata).
        row_count, size_bytes = _index_absolutes(client, bucket)
        # The consolidator's self-published run summary (authoritative when present; absent = not live).
        run = _read_latest_run(client, bucket)
        # The dark data-correctness actors' last audit for this bucket (phantom + empty re-probe).
        audit = _audit_fields(client, bucket, entry["kind"] or "")
        run_verdict = _as_str(run.get("verdict")) if run is not None else None
        fired_empty = _is_fired_but_empty(exec_status, age, budget, now)
        freshness_verdict = _verdict(status, pending_count, fired_but_empty=fired_empty)
        verdict = (
            _authoritative_verdict(run_verdict, freshness_verdict, pending_count, row_count)
            if run is not None
            else freshness_verdict
        )
        if verdict == "fired_but_empty" and run is not None:
            detail = f"{detail} — consolidator self-reports it ran but produced no rows (fired-but-empty)"
        elif fired_empty:
            detail = f"execution SUCCEEDED recently yet {detail} — job ran green but wrote nothing (fired-but-empty)"
        return ConsolidatorHealth(
            **base,
            execution_status=exec_kind,
            execution_last_run_at=exec_run_at,
            execution_exit_code=exec_exit,
            run_reporting=run is not None,
            run_verdict=run_verdict,
            run_last_run_at=_as_str(run.get("last_run_at")) if run is not None else None,
            run_shards_changed=_as_int(run.get("shards_changed")) if run is not None else None,
            run_rows_added=_as_int(run.get("rows_added")) if run is not None else None,
            run_duration_ms=_as_float(run.get("duration_ms")) if run is not None else None,
            bucket=bucket,
            status=status,
            verdict=verdict,
            index_age_seconds=round(age, 1) if age is not None else None,
            last_successful_run_at=index_mtime.isoformat() if index_mtime is not None else None,
            pending_shard_count=pending_count,
            total_shard_count=total_count,
            oldest_pending_shard_age_seconds=oldest_pending_age,
            index_row_count=row_count,
            index_size_bytes=size_bytes,
            **audit,
            detail=detail,
        )
    except (OSError, ValueError, RuntimeError) as exc:
        logger.warning("consolidator-health: read failed for %s (%s): %s", base["category"], bucket, exc)
        return ConsolidatorHealth(
            **base,
            execution_status=exec_kind,
            execution_last_run_at=exec_run_at,
            execution_exit_code=exec_exit,
            bucket=bucket,
            status="unknown",
            verdict="unknown",
            detail=f"read failed: {exc}",
        )


def _fetch_executions() -> dict[str, CloudRunExecutionStatus]:
    """Latest Cloud Run execution per job (ONE batched list), keyed by short job name.

    Honest degradation: any resolution/GCP failure yields ``{}`` so the estate view still renders
    with the freshness-derived verdict (no ``fired_but_empty`` refinement), never a 5xx.
    """
    try:
        _, project = _env_project()
    except (OSError, ValueError, RuntimeError) as exc:
        logger.warning("consolidator-health: project resolve failed, skipping execution join (%s)", exc)
        return {}
    return latest_execution_by_job(project)


def _entry_budget(entry: dict[str, str | None], default_budget: int) -> int:
    """Per-consolidator staleness budget from the catalog (cadence-matched), else the AG/global default.

    The catalog carries a per-(kind,AG) ``staleness_budget_seconds`` (live market-data ticks = 120s,
    every other consolidator = its producers' 86400s — see ``gen_consolidator_catalog.py``), so each
    job is judged against its OWN cadence rather than a uniform 120s. Falls back to the legacy per-AG
    override then the global default when a catalog is old/absent.
    """
    raw = entry.get("staleness_budget_seconds")
    if raw:
        try:
            return int(raw)
        except ValueError:
            logger.warning("consolidator-health: bad catalog budget %r for %s", raw, entry.get("category"))
    return _budget_for(entry["asset_group"] or "", default_budget)


def _build_consolidators(now: datetime, default_budget: int) -> list[ConsolidatorHealth]:
    """Fan out the per-consolidator reads across the estate (GCS I/O-bound → a small thread pool)."""
    if not _CATALOG:
        return []

    executions = _fetch_executions()  # one batched Cloud Run list, joined per job below

    def one(entry: dict[str, str | None]) -> ConsolidatorHealth:
        return _consolidator_health(
            entry,
            _entry_budget(entry, default_budget),
            now,
            executions.get(entry["job_name"] or ""),
        )

    with ThreadPoolExecutor(max_workers=min(12, len(_CATALOG))) as pool:
        results = list(pool.map(one, _CATALOG))
    results.sort(key=lambda c: (_status_rank(c.status), c.category))  # worst-first, then stable by category
    return results


def _ag_health(
    asset_group: AssetGroup, budget: int, now: datetime, *, include_backlog: bool = False
) -> ConsolidatorAgHealth:
    """Build the consolidator posture for one asset_group (honest per-AG degradation).

    ``include_backlog=True`` also counts the per-VM shard backlog (shards written since
    the last merge → not yet absorbed) via ONE extra prefix list. It is opt-in: the
    Consolidators-tab endpoint sets it; the per-deployment ``/freshness`` reuse (via
    ``consolidator_posture``) leaves it off so that hotter path pays no extra list.
    """
    try:
        bucket = resolve_bucket_name(cloud="gcp", kind=_market_data_kind(asset_group), asset_group=asset_group)
    except (OSError, ValueError) as exc:
        logger.warning("consolidator-health: bucket resolution failed for %s: %s", asset_group, exc)
        return ConsolidatorAgHealth(
            asset_group=asset_group,
            bucket="",
            status="unknown",
            staleness_budget_seconds=budget,
            per_vm_shard_fallback_active=False,
            detail=f"bucket resolution failed: {exc}",
        )
    try:
        client = get_storage_client()
        age = consolidated_blob_age_sec(client, bucket)
        shards_exist = age is None or age > budget
        index_mtime = (now - timedelta(seconds=age)) if age is not None else None
        pending_count: int | None = None
        total_count: int | None = None
        if include_backlog:
            # ONE prefix list gives BOTH the backlog counts AND shard existence.
            backlog = per_vm_shard_backlog(client, bucket, index_mtime)
            pending_count, total_count = backlog.pending, backlog.total
            shards_present = total_count > 0
        else:
            # Only pay for the shard-list when the index looks stale/missing (the discriminator).
            shards_present = per_vm_shards_exist(client, bucket, exclude_self=True) if shards_exist else False
        status, fallback, detail = _classify_ag(age, budget, shards_present)
        last_run = index_mtime.isoformat() if index_mtime is not None else None
        return ConsolidatorAgHealth(
            asset_group=asset_group,
            bucket=bucket,
            status=status,
            index_age_seconds=round(age, 1) if age is not None else None,
            staleness_budget_seconds=budget,
            per_vm_shard_fallback_active=fallback,
            last_successful_run_at=last_run,
            pending_shard_count=pending_count,
            total_shard_count=total_count,
            detail=detail,
        )
    except (OSError, ValueError, RuntimeError) as exc:
        logger.warning("consolidator-health: read failed for %s (%s): %s", asset_group, bucket, exc)
        return ConsolidatorAgHealth(
            asset_group=asset_group,
            bucket=bucket,
            status="unknown",
            staleness_budget_seconds=budget,
            per_vm_shard_fallback_active=False,
            detail=f"consolidator read failed: {exc}",
        )


def consolidator_posture(asset_group: AssetGroup, now: datetime) -> ConsolidatorAgHealth:
    """Public per-asset_group manifest-index posture (index age / fallback / last run).

    The availability-index heartbeat IS the manifest-derived freshness for an
    asset_group's owned shards, so the per-deployment freshness endpoint
    (``/api/deployments/{id}/freshness``) reuses this rather than re-walking the
    manifest. Uses the canonical consolidated-staleness budget.
    """
    return _ag_health(asset_group, _budget_for(asset_group, resolve_consolidated_staleness_sec()), now)


def object_delta_for_bucket(bucket: str) -> tuple[int | None, str]:
    """Object-count delta = a manifest LOOKUP off the consolidated index (no new bucket walk).

    Reads the SAME consolidated ``availability_index`` blob ``consolidator_posture`` already
    resolved (``read_availability_index`` hits the process-level index cache health_consolidator
    just warmed), sums ``row_count``-else-``instrument_count`` for ``capture_status="captured"``
    rows per written date, and diffs the two most recent written dates. This is the authoritative
    write-truth signal for WS-D's composite health (D.1) — objects that actually landed, not the
    log-scraped ``rows_out`` hint. Honest degradation: any read failure or <2 distinct written
    dates yields ``(None, <reason>)``, never a false zero.

    Lives here (bucket-only, not deployment-id-scoped) rather than in the per-deployment
    ``/freshness`` route so both that route AND the composite-health `stalled` classifier
    (``object_delta_for_asset_group`` below — batched ONE call per distinct asset_group per
    census cycle, not once per VM entry) can share the same manifest read without a circular
    import between ``deployment_freshness`` and ``deployments_inventory``.
    """
    try:
        index = read_availability_index(bucket, columns=["date", "row_count", "instrument_count", "capture_status"])
    except (OSError, ValueError, RuntimeError) as exc:
        return None, f"manifest read failed: {exc}"
    if index.empty:
        return None, "manifest index is empty"
    captured = index[index["capture_status"] == "captured"]
    if captured.empty:
        return None, "no captured rows in manifest index"
    # Coerce to numeric FIRST — the availability index can store row_count / instrument_count
    # as an object/string dtype (nullable or mixed), which made `row_count > 0` raise
    # TypeError("'>' not supported between instances of 'str' and 'int'") and silently degrade
    # EVERY object-delta to None, breaking the composite-health working/stalled signal that reads
    # it. to_numeric(errors="coerce") turns unparseable cells into NaN → 0 (honest absence).
    import pandas as pd  # lazy: pandas is only needed on this manifest-read path

    row_count = pd.to_numeric(captured["row_count"], errors="coerce").fillna(0)
    instrument_count = pd.to_numeric(captured["instrument_count"], errors="coerce").fillna(0)
    counts = row_count.where(row_count > 0, instrument_count)
    by_date = counts.groupby(captured["date"]).sum().sort_index()
    if len(by_date) < 2:
        return None, f"only {len(by_date)} distinct written date(s) in manifest — nothing to diff yet"
    latest_date, prior_date = by_date.index[-1], by_date.index[-2]
    delta = int(by_date.iloc[-1] - by_date.iloc[-2])
    return delta, f"{latest_date} object count {by_date.iloc[-1]:.0f} vs {prior_date} {by_date.iloc[-2]:.0f}"


def object_delta_for_asset_group(asset_group: str, now: datetime) -> tuple[int | None, str]:
    """Object-count delta for an asset_group's market-data bucket — keyed by asset_group ALONE.

    A thin combinator over ``consolidator_posture`` (bucket resolution) + ``object_delta_for_bucket``,
    so a caller that needs this per DISTINCT asset_group (not per specific deployment_id) — e.g. the
    composite-health `stalled` classifier looping many VM entries that share an asset_group — can
    batch it exactly once per asset_group per cycle instead of re-deriving it per VM.
    """
    if asset_group not in _ASSET_GROUPS:
        return None, f"asset_group {asset_group!r} has no availability-index to read freshness from"
    posture = consolidator_posture(asset_group, now)  # type: ignore[arg-type]  # validated against _ASSET_GROUPS above
    if not posture.bucket:
        return None, posture.detail
    return object_delta_for_bucket(posture.bucket)


def _compute_consolidator_health() -> ConsolidatorHealthResponse:
    """The real (slow) estate walk — GCS index reads + Cloud Run execution lookups per
    consolidator (measured ~15s on a dev box). Never call directly from a route — go
    through ``get_consolidator_health`` so callers get the stale-while-revalidate cache.
    """
    now = datetime.now(UTC)
    default_budget = resolve_consolidated_staleness_sec()
    consolidators = _build_consolidators(now, default_budget)
    if consolidators:
        # Derive the legacy per-AG view (the 5 market-data ones) from the estate — no extra reads.
        ag_entries = [_ag_from_consolidator(c) for c in consolidators if c.kind == "market-data" and c.asset_group]
        ag_entries.sort(key=lambda e: _status_rank(e.status))
    else:
        # Catalog missing → fall back to direct 5-AG reads so the tab still works.
        ag_entries = [
            _ag_health(ag, _budget_for(ag, default_budget), now, include_backlog=True) for ag in _ASSET_GROUPS
        ]
    return build_consolidator_health(ag_entries, now, consolidators=consolidators)


# Stale-while-revalidate snapshot cache (same pattern as vm_deployments.py): the cockpit
# polls this route every 30s and the health-overview tile reuses it — without a cache
# every poll redid the full ~15s walk back-to-back. Staleness budgets are minutes-scale,
# so a snapshot a TTL old is as honest as a live read. ``generated_at`` inside the
# payload keeps the snapshot's true compute time visible.
_CONSOLIDATOR_HEALTH_TTL_SEC = 25.0
_consolidator_health_cache: tuple[float, ConsolidatorHealthResponse] | None = None
_consolidator_health_lock = threading.Lock()
_consolidator_health_refreshing = False
_consolidator_health_refresh_pool = ThreadPoolExecutor(max_workers=1, thread_name_prefix="consolidator-health-refresh")


def _refresh_consolidator_health() -> None:
    """Background cache refresh — recompute + store, then clear the in-flight flag."""
    global _consolidator_health_cache, _consolidator_health_refreshing
    try:
        result = _compute_consolidator_health()
        with _consolidator_health_lock:
            _consolidator_health_cache = (time.monotonic(), result)
    except (OSError, ValueError, RuntimeError) as exc:
        # Keep the stale snapshot on a failed refresh — never poison the cache.
        logger.warning("consolidator-health: background refresh failed: %s", exc)
    finally:
        with _consolidator_health_lock:
            _consolidator_health_refreshing = False


@router.get("/health/consolidator", response_model=ConsolidatorHealthResponse)
def get_consolidator_health() -> ConsolidatorHealthResponse:
    """Per-asset_group manifest-consolidator health drill-down.

    For each asset_group's ``market-data`` bucket: the consolidated availability-index
    heartbeat age, whether the per-VM shard recovery-merge fallback is active (stale index
    + shards present = consolidator behind/down), the derived health status, and the last
    successful run timestamp. Read-only; degrades to ``status="unknown"`` per-AG on a read
    failure, never a 5xx.

    Served from a stale-while-revalidate snapshot (see ``_compute_consolidator_health``):
    fresh (< TTL) → instant; stale → the snapshot is served instantly and ONE background
    refresh is kicked off; cold (first call) → computed synchronously under a lock so a
    burst of polls collapses to one walk.
    """
    if _cfg.is_mock_mode():
        return _mock_response(datetime.now(UTC))

    global _consolidator_health_cache, _consolidator_health_refreshing
    with _consolidator_health_lock:
        cached = _consolidator_health_cache
        stale = cached is not None and (time.monotonic() - cached[0]) >= _CONSOLIDATOR_HEALTH_TTL_SEC
        if cached is not None and stale and not _consolidator_health_refreshing:
            _consolidator_health_refreshing = True
            _consolidator_health_refresh_pool.submit(_refresh_consolidator_health)
    if cached is not None:
        return cached[1]

    # Cold path — lock so concurrent first-polls trigger exactly ONE walk.
    with _consolidator_health_lock:
        cached = _consolidator_health_cache
        if cached is not None:
            return cached[1]
        result = _compute_consolidator_health()
        _consolidator_health_cache = (time.monotonic(), result)
        return result


__all__ = [
    "ConsolidatorAgHealth",
    "ConsolidatorHealth",
    "ConsolidatorHealthResponse",
    "build_consolidator_health",
    "consolidator_posture",
    "router",
]

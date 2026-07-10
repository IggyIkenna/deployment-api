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
"""

from __future__ import annotations

import io
import json
import logging
from concurrent.futures import ThreadPoolExecutor
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import TYPE_CHECKING

from fastapi import APIRouter
from pydantic import BaseModel, Field
from unified_trading_library import (
    AssetGroup,
    StorageClient,
    UnifiedCloudConfig,
    consolidated_blob_age_sec,
    get_storage_client,
    per_vm_shard_backlog,
    per_vm_shards_exist,
    read_availability_index,
    resolve_bucket_name,
    resolve_consolidated_staleness_sec,
)
from unified_trading_library.cloud_interface.bucket_naming import (  # noqa: qg-deep-import
    _resolve_deployment_env_short,  # not re-exported from the UTL top-level package
)

from deployment_api.deployment_api_config import DeploymentApiConfig

if TYPE_CHECKING:
    from _typeshed import WriteableBuffer

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
_AG_STALENESS_BUDGET_SEC: dict[str, int] = {"cefi": 86400}


def _budget_for(asset_group: str, default: int) -> int:
    """Staleness budget for an asset_group — its cadence-matched override, else the global default."""
    return _AG_STALENESS_BUDGET_SEC.get(asset_group, default)


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
    verdict: str  # produced | producing | stale_output | empty | unknown  (data-correctness lens)
    index_age_seconds: float | None = None
    staleness_budget_seconds: int
    last_successful_run_at: str | None = None
    pending_shard_count: int | None = None  # backlog: shards written since the last merge
    total_shard_count: int | None = None  # fan-in width: how many per-VM shards feed this index
    index_row_count: int | None = None  # absolute rows in the consolidated index (parquet num_rows)
    index_size_bytes: int | None = None  # size of the consolidated index file on disk
    detail: str


class ConsolidatorHealthResponse(BaseModel):  # CORRECT-LOCAL: FastAPI API contract model
    """GET /api/health/consolidator response — per-AG drill-down + the full per-consolidator estate."""

    generated_at: str  # ISO-8601 UTC
    overall: str  # worst status across the estate (ok|degraded|critical|unknown)
    asset_groups: list[ConsolidatorAgHealth] = Field(default_factory=list)  # legacy: the 5 market-data AGs
    consolidators: list[ConsolidatorHealth] = Field(default_factory=list)  # the full ~25-job estate


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


# ── Per-consolidator catalog (the full estate, not just the 5 market-data AGs) ─────────
# The catalog is a PROJECTION of the deployment-service terraform consolidator locals,
# generated by ``scripts/gen_consolidator_catalog.py`` and committed as a JSON artifact so it
# travels with the deployed image (deployment-service is NOT on the Cloud Run image). Adding a
# consolidator = a terraform edit + a generator re-run; this endpoint then shows it automatically.
_CATALOG_PATH = Path(__file__).resolve().parent.parent / "consolidator_catalog.generated.json"


def _load_catalog() -> list[dict[str, str | None]]:
    try:
        doc = json.loads(_CATALOG_PATH.read_text(encoding="utf-8"))
    except (OSError, ValueError) as exc:
        logger.warning("consolidator-health: catalog load failed (%s) — estate view empty", exc)
        return []
    entries = doc.get("consolidators")
    return list(entries) if isinstance(entries, list) else []


_CATALOG: list[dict[str, str | None]] = _load_catalog()

_env_project_cache: tuple[str, str] | None = None


def _env_project() -> tuple[str, str]:
    """(deployment_env_short, gcp_project_id) — the two slots a catalog ``bucket_template`` fills. Cached."""
    global _env_project_cache
    if _env_project_cache is None:
        _env_project_cache = (_resolve_deployment_env_short(), UnifiedCloudConfig().gcp_project_id)
    return _env_project_cache


def _catalog_bucket(entry: dict[str, str | None]) -> str:
    """Fill a catalog ``bucket_template`` (``…-{env}-{project}``) with the live env/project."""
    env, project = _env_project()
    return (entry["bucket_template"] or "").format(env=env, project=project)


def _verdict(status: str, pending: int | None) -> str:
    """Data-correctness lens derived cheaply from freshness + backlog.

    A precise ``fired_but_empty`` (job exited 0 but wrote nothing) needs per-run execution
    history — a later refinement; the list view derives the signal from what one cheap index
    stat + shard-list already tell us.
    """
    if status == "critical":
        return "stale_output"  # index stale while per-VM shards wait → output is behind
    if status == "degraded":
        return "empty"  # stale/missing index but nothing to consolidate
    if status == "ok":
        return "producing" if (pending or 0) > 0 else "produced"
    return "unknown"


_INDEX_BLOB = "_index/availability_index.parquet"


class _RangedIndexReader(io.RawIOBase):
    """Seekable read-only view over a GCS blob via ranged reads, so pyarrow can read just the
    parquet footer (``num_rows``) instead of downloading a multi-hundred-MB index — a 219 MB index
    yields its 14 M row count from ~490 KB of footer (verified 2026-07-10)."""

    def __init__(self, client: StorageClient, bucket: str, path: str, size: int) -> None:
        super().__init__()
        self._client = client
        self._bucket = bucket
        self._path = path
        self._size = size
        self._pos = 0

    def seek(self, offset: int, whence: int = io.SEEK_SET) -> int:
        if whence == io.SEEK_SET:
            self._pos = offset
        elif whence == io.SEEK_CUR:
            self._pos += offset
        elif whence == io.SEEK_END:
            self._pos = self._size + offset
        return self._pos

    def tell(self) -> int:
        return self._pos

    def readable(self) -> bool:
        return True

    def seekable(self) -> bool:
        return True

    def readinto(self, buffer: WriteableBuffer) -> int:
        view = memoryview(buffer).cast("B")
        end = min(self._size, self._pos + len(view))
        if end <= self._pos:
            return 0
        data = self._client.download_bytes_range(self._bucket, self._path, self._pos, end)
        n = len(data)
        view[:n] = data
        self._pos += n
        return n


def _index_absolutes(client: StorageClient, bucket: str) -> tuple[int | None, int | None]:
    """``(row_count, size_bytes)`` for a bucket's consolidated index. Size is one metadata call;
    rows come from a cheap ranged parquet-footer read (never downloads the whole index). Best-effort:
    a missing index or a transient read hiccup returns ``None`` for the affected field — an
    observability nicety layered on the freshness posture, not a correctness gate."""
    try:
        meta = client.get_blob_metadata(bucket, _INDEX_BLOB)
    except (OSError, ValueError, RuntimeError):
        return None, None
    if meta is None:
        return None, None
    size = int(meta.size or 0)
    if size <= 0:
        return None, None
    try:
        import pyarrow as pa  # noqa: imports-inside-functions — lazy heavy dep, only for the footer read
        import pyarrow.parquet as pq  # noqa: imports-inside-functions

        native = pa.PythonFile(_RangedIndexReader(client, bucket, _INDEX_BLOB, size), mode="r")
        rows = pq.ParquetFile(native).metadata.num_rows
        return int(rows), size
    except (OSError, ValueError, RuntimeError):
        return None, size


def _consolidator_health(entry: dict[str, str | None], budget: int, now: datetime) -> ConsolidatorHealth:
    """One consolidator's posture: freshness + backlog + fan-in + verdict (single index stat + shard-list)."""
    base = {
        "category": entry["category"] or "",
        "kind": entry["kind"] or "",
        "asset_group": entry["asset_group"],
        "job_name": entry["job_name"] or "",
        "staleness_budget_seconds": budget,
    }
    try:
        bucket = _catalog_bucket(entry)
    except (KeyError, ValueError) as exc:
        return ConsolidatorHealth(
            **base, bucket="", status="unknown", verdict="unknown", detail=f"bucket resolve failed: {exc}"
        )
    try:
        client = get_storage_client()
        age = consolidated_blob_age_sec(client, bucket)
        index_mtime = (now - timedelta(seconds=age)) if age is not None else None
        # ONE prefix list gives BOTH the backlog counts AND the fan-in width.
        pending_count, total_count = per_vm_shard_backlog(client, bucket, index_mtime)
        status, _, detail = _classify_ag(age, budget, total_count > 0)
        # Absolute snapshot of the consolidated index (rows via a cheap footer read, size via metadata).
        row_count, size_bytes = _index_absolutes(client, bucket)
        return ConsolidatorHealth(
            **base,
            bucket=bucket,
            status=status,
            verdict=_verdict(status, pending_count),
            index_age_seconds=round(age, 1) if age is not None else None,
            last_successful_run_at=index_mtime.isoformat() if index_mtime is not None else None,
            pending_shard_count=pending_count,
            total_shard_count=total_count,
            index_row_count=row_count,
            index_size_bytes=size_bytes,
            detail=detail,
        )
    except (OSError, ValueError, RuntimeError) as exc:
        logger.warning("consolidator-health: read failed for %s (%s): %s", base["category"], bucket, exc)
        return ConsolidatorHealth(
            **base, bucket=bucket, status="unknown", verdict="unknown", detail=f"read failed: {exc}"
        )


def _build_consolidators(now: datetime, default_budget: int) -> list[ConsolidatorHealth]:
    """Fan out the per-consolidator reads across the estate (GCS I/O-bound → a small thread pool)."""
    if not _CATALOG:
        return []

    def one(entry: dict[str, str | None]) -> ConsolidatorHealth:
        return _consolidator_health(entry, _budget_for(entry["asset_group"] or "", default_budget), now)

    with ThreadPoolExecutor(max_workers=min(12, len(_CATALOG))) as pool:
        results = list(pool.map(one, _CATALOG))
    results.sort(key=lambda c: (_status_rank(c.status), c.category))  # worst-first, then stable by category
    return results


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
            pending_count, total_count = per_vm_shard_backlog(client, bucket, index_mtime)
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
) -> ConsolidatorHealth:
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
        last_successful_run_at=ts if age is not None else None,
        pending_shard_count=pending,
        total_shard_count=total,
        index_row_count=(1_000_000 + total * 50_000) if age is not None else None,
        index_size_bytes=(20_000_000 + total * 4_000_000) if age is not None else None,
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
    ]
    ag_entries = [_ag_from_consolidator(c) for c in consolidators if c.kind == "market-data" and c.asset_group]
    return build_consolidator_health(ag_entries, now, consolidators=consolidators)


@router.get("/health/consolidator", response_model=ConsolidatorHealthResponse)
def get_consolidator_health() -> ConsolidatorHealthResponse:
    """Per-asset_group manifest-consolidator health drill-down.

    For each asset_group's ``market-data`` bucket: the consolidated availability-index
    heartbeat age, whether the per-VM shard recovery-merge fallback is active (stale index
    + shards present = consolidator behind/down), the derived health status, and the last
    successful run timestamp. Read-only; degrades to ``status="unknown"`` per-AG on a read
    failure, never a 5xx.
    """
    now = datetime.now(UTC)
    if _cfg.is_mock_mode():
        return _mock_response(now)
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


__all__ = [
    "ConsolidatorAgHealth",
    "ConsolidatorHealth",
    "ConsolidatorHealthResponse",
    "build_consolidator_health",
    "consolidator_posture",
    "router",
]

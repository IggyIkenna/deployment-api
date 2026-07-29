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
import threading
import time
from concurrent.futures import ThreadPoolExecutor
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import TYPE_CHECKING, TypedDict

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
from deployment_api.routes._cloud_run_executions import (
    CloudRunExecutionStatus,
    latest_execution_by_job,
)

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


_INDEX_BLOB = "_index/availability_index.parquet"
_LATEST_RUN_BLOB = "_index/latest.json"  # the consolidator's self-published run summary (UTL manifest_consolidator)
# Dark data-correctness actors' self-published per-AG summaries (instruments-service phantom reconcile +
# e2e-testing empty re-probe), written to the SAME bucket the consolidator card reads. Absent = "no audit
# yet" (honest). Only market-data / instruments buckets carry these; other kinds read None (harmless).
_PHANTOM_AUDIT_BLOB = "_index/phantom_audit_latest.json"
_REPROBE_AUDIT_BLOB = "_index/reprobe_audit_latest.json"
# The kinds an audit targets — gate the two extra reads to these so features/execution/flat entries
# don't pay for a metadata HEAD that will always miss.
_AUDIT_BEARING_KINDS = frozenset({"market-data", "instruments"})


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


def _read_index_json(client: StorageClient, bucket: str, blob: str) -> dict[str, object] | None:
    """Read a small self-published ``_index/*.json`` summary object, or ``None`` if absent/malformed.

    Absent = the producer has never run under the summary-emitting code → the cockpit shows the
    honest "not reporting / no audit yet" state, NEVER a fabricated all-clear. Best-effort: any
    read/parse hiccup returns ``None``.

    Existence is checked via ``get_blob_metadata`` FIRST (returns ``None`` for a missing object,
    like ``_index_absolutes``) so a missing object never surfaces the provider's raw ``NotFound``
    (404) — which is NOT an ``OSError`` — as a 5xx on this read-only endpoint.
    """
    try:
        if client.get_blob_metadata(bucket, blob) is None:
            return None  # object absent → not reporting
        raw = client.download_bytes(bucket, blob)
    except (OSError, ValueError, RuntimeError):
        return None
    try:
        parsed = json.loads(raw.decode("utf-8"))
    except (ValueError, UnicodeDecodeError):
        return None
    return parsed if isinstance(parsed, dict) else None


def _read_latest_run(client: StorageClient, bucket: str) -> dict[str, object] | None:
    """The consolidator's self-published ``_index/latest.json`` run summary, or ``None`` if absent.

    Absent = this consolidator has never run under the summary-emitting code (dead / not-yet-fired /
    not-yet-redeployed) → the cockpit shows it as "not yet reporting", NEVER a fabricated all-clear.
    """
    return _read_index_json(client, bucket, _LATEST_RUN_BLOB)


class _AuditFields(TypedDict):
    """The audit-summary kwargs spread into ``ConsolidatorHealth`` — one typed slot per model field."""

    phantom_audit_at: str | None
    phantom_count: int | None
    phantom_triage_link: str | None
    reprobe_audit_at: str | None
    reprobe_new_empties: int | None
    reprobe_disagreements: int | None
    reprobe_reclassified: int | None


def _audit_fields(client: StorageClient, bucket: str, kind: str) -> _AuditFields:
    """The last phantom-audit + empty-re-probe summaries for this consolidator's bucket.

    Two cheap ``_index/*.json`` reads, gated to the kinds an audit actually targets so
    features/execution/flat consolidators pay nothing. Every field is ``None`` when the summary is
    absent (honest "no audit yet") — never a fabricated clean verdict. Best-effort throughout.
    """
    fields = _AuditFields(
        phantom_audit_at=None,
        phantom_count=None,
        phantom_triage_link=None,
        reprobe_audit_at=None,
        reprobe_new_empties=None,
        reprobe_disagreements=None,
        reprobe_reclassified=None,
    )
    if kind not in _AUDIT_BEARING_KINDS:
        return fields
    phantom = _read_index_json(client, bucket, _PHANTOM_AUDIT_BLOB)
    if phantom is not None:
        fields["phantom_audit_at"] = _as_str(phantom.get("generated_at"))
        fields["phantom_count"] = _as_int(phantom.get("phantom_count"))
        fields["phantom_triage_link"] = _as_str(phantom.get("triage_jsonl"))
    reprobe = _read_index_json(client, bucket, _REPROBE_AUDIT_BLOB)
    if reprobe is not None:
        fields["reprobe_audit_at"] = _as_str(reprobe.get("generated_at"))
        fields["reprobe_new_empties"] = _as_int(reprobe.get("new_empties"))
        fields["reprobe_disagreements"] = _as_int(reprobe.get("disagreements"))
        fields["reprobe_reclassified"] = _as_int(reprobe.get("reclassified"))
    return fields


def _as_str(v: object) -> str | None:
    return v if isinstance(v, str) else None


def _as_int(v: object) -> int | None:
    return v if isinstance(v, int) and not isinstance(v, bool) else None


def _as_float(v: object) -> float | None:
    return float(v) if isinstance(v, (int, float)) and not isinstance(v, bool) else None


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

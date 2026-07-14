# Epic: monitoring_control_plane_master_2026_06_10
# Lifecycle: permanent
"""Periodic GCS parquet snapshot of normalized cost facts + a DuckDB reader.

The billing page used to re-query BigQuery/Athena on every cache-miss, materializing the
whole per-resource fact set (~168 K ``CostRecord`` objects) for a window and holding up to
~6 overlapping windows at once (~2 GB), and it paid a BQ/Athena scan per miss. This module
replaces that path:

* A background worker (``scripts/cost_snapshot_worker.py``) scans each cloud ONCE every
  ~12 h and writes a per-cloud parquet to
  ``gs://{pid}-cost-snapshots/{cloud}.parquet`` (the aggregated fact set, ~168 K rows /
  ~20-40 MB — NOT the ~2 GB raw billing export).
* Each API instance downloads those small parquets to local ``/tmp`` on demand and refreshes
  them at most every ~12 h, then answers every view with a **DuckDB SQL aggregation** over
  the local files — no per-request cloud scan, no full-table Python materialization.

Per-cloud layout (``gcp.parquet`` / ``aws.parquet`` / ``github.parquet``) mirrors the
existing per-cloud isolation in ``service._load_window`` — a cloud whose snapshot is missing
(e.g. AWS pending the deployed-perms fix) simply drops out of the union; the others render.

Plan:
    ``unified-trading-pm/plans/active/deployment_api_cache_oom_and_ui_latency_remediation_2026_07_13.md`` § 4b
SSOT:
    ``codex/05-infrastructure/billing-cost-observability.md``
"""

from __future__ import annotations

import io
import json
import logging
import os
import tempfile
import threading
import time
from collections.abc import Sequence
from datetime import UTC, datetime
from pathlib import Path
from typing import cast

import duckdb
import pyarrow as pa
import pyarrow.parquet as pq
from unified_trading_library import get_storage_client

from deployment_api.services.cost_observability.models import (
    CLOUD_AWS,
    CLOUD_GCP,
    CLOUD_GITHUB,
    CostRecord,
)

logger = logging.getLogger(__name__)

# Clouds that get their own parquet slice, in render order (matches service.CLOUD_ORDER).
SNAPSHOT_CLOUDS: tuple[str, ...] = (CLOUD_GCP, CLOUD_AWS, CLOUD_GITHUB)

# GCS bucket + object layout. Mirrors data-status' ``{pid}-data-status-rollups`` template
# (rollup_cache.ROLLUP_BUCKET_TEMPLATE) — one flat per-cloud object, no date partitioning
# (the snapshot always holds the full available history, ~90 days, in one file).
SNAPSHOT_BUCKET_TEMPLATE: str = "{pid}-cost-snapshots"

# Re-download a locally-cached parquet at most this often. The worker refreshes the GCS
# object every ~12 h; matching that here means a warm instance never re-downloads within a
# refresh cycle. Billing lags ~2 days, so ≤12 h staleness on the "today so far" figure is
# immaterial (SSOT § 4b decision 2). Module constant (monkeypatchable in tests).
SNAPSHOT_REFRESH_SEC: float = 12 * 3600

# One shared parquet schema so worker (writer) and store (reader) never drift. ``day`` stays a
# string (YYYY-MM-DD) to match ``CostRecord.day`` and the existing endpoints; DuckDB compares
# it lexically (ISO dates sort correctly) and casts to DATE where needed. ``labels`` is a JSON
# string (DuckDB ``json_extract_string`` reads it) so the schema is flat + portable. The
# provisional flag is deliberately NOT stored — it depends on "today" and is recomputed at read
# time (a stored flag would freeze at snapshot-build time and mis-mark a day that has since
# finalized).
SNAPSHOT_SCHEMA: pa.Schema = pa.schema(
    [
        ("cloud", pa.string()),
        ("day", pa.string()),
        ("service", pa.string()),
        ("resource_id", pa.string()),
        ("resource_kind", pa.string()),
        ("region", pa.string()),
        ("cost", pa.float64()),
        ("credit", pa.float64()),
        ("currency", pa.string()),
        ("cost_native", pa.float64()),
        ("credit_native", pa.float64()),
        ("sku", pa.string()),
        ("usage_amount", pa.float64()),
        ("usage_unit", pa.string()),
        ("zone", pa.string()),
        ("purchase_option", pa.string()),
        ("machine_type", pa.string()),
        ("vcpu", pa.int64()),
        ("memory_gb", pa.float64()),
        ("is_placeholder", pa.bool_()),
        ("labels", pa.string()),
    ]
)


def snapshot_blob_path(cloud: str) -> str:
    """GCS object path for one cloud's cost snapshot parquet."""
    return f"{cloud}.parquet"


# --- typed coercion of DuckDB row cells (fetchall returns untyped tuples) -----
def _s(v: object) -> str:
    return v if isinstance(v, str) else ("" if v is None else str(v))


def _f(v: object) -> float:
    return float(v) if isinstance(v, (int, float)) else 0.0


def _fn(v: object) -> float | None:
    return float(v) if isinstance(v, (int, float)) else None


def _i(v: object) -> int | None:
    return int(v) if isinstance(v, int) else None


# --- record <-> parquet ------------------------------------------------------
def records_to_table(records: Sequence[CostRecord]) -> pa.Table:
    """Columnarize a list of ``CostRecord`` into an Arrow table on ``SNAPSHOT_SCHEMA``.

    ``labels`` is serialized to a compact JSON string; every other field maps 1:1. Empty
    input yields an empty table with the right schema (so the worker can still write a valid,
    queryable parquet for a cloud that returned nothing this cycle).
    """
    cols: dict[str, list[object]] = {name: [] for name in SNAPSHOT_SCHEMA.names}
    for r in records:
        cols["cloud"].append(r.cloud)
        cols["day"].append(r.day)
        cols["service"].append(r.service)
        cols["resource_id"].append(r.resource_id)
        cols["resource_kind"].append(r.resource_kind)
        cols["region"].append(r.region)
        cols["cost"].append(r.cost)
        cols["credit"].append(r.credit)
        cols["currency"].append(r.currency)
        cols["cost_native"].append(r.cost_native)
        cols["credit_native"].append(r.credit_native)
        cols["sku"].append(r.sku)
        cols["usage_amount"].append(r.usage_amount)
        cols["usage_unit"].append(r.usage_unit)
        cols["zone"].append(r.zone)
        cols["purchase_option"].append(r.purchase_option)
        cols["machine_type"].append(r.machine_type)
        cols["vcpu"].append(r.vcpu)
        cols["memory_gb"].append(r.memory_gb)
        cols["is_placeholder"].append(r.is_placeholder)
        cols["labels"].append(json.dumps(r.labels, separators=(",", ":")) if r.labels else "")
    # pyarrow ships only partial type stubs → the array/schema calls read as Unknown under strict
    # pyright; the shapes are pinned by SNAPSHOT_SCHEMA, so silence the library-level unknowns.
    arrays = [  # pyright: ignore[reportUnknownVariableType]
        pa.array(cols[name], type=SNAPSHOT_SCHEMA.field(name).type)  # pyright: ignore[reportUnknownMemberType, reportUnknownArgumentType]
        for name in SNAPSHOT_SCHEMA.names
    ]
    return pa.Table.from_arrays(arrays, schema=SNAPSHOT_SCHEMA)  # pyright: ignore[reportUnknownArgumentType]


def table_to_parquet_bytes(table: pa.Table) -> bytes:
    """Serialize an Arrow table to parquet bytes (snappy) for GCS upload."""
    buf = io.BytesIO()
    pq.write_table(table, buf, compression="snappy")  # pyright: ignore[reportUnknownMemberType]
    return buf.getvalue()


def records_to_parquet_bytes(records: Sequence[CostRecord]) -> bytes:
    """Convenience: ``CostRecord`` list → parquet bytes on ``SNAPSHOT_SCHEMA``."""
    return table_to_parquet_bytes(records_to_table(records))


# --- read-side store ---------------------------------------------------------
class CostSnapshotStore:
    """Per-cloud parquet cache on local disk + a DuckDB reader over the union.

    Thread-safe. ``ensure_fresh()`` downloads each cloud's parquet on first use and re-downloads
    it once older than ``SNAPSHOT_REFRESH_SEC`` (single-flight per cloud under a lock). ``query``
    runs SQL against a ``cost_records`` view that is the UNION of whichever per-cloud files are
    present locally — a cloud with no snapshot is simply absent (honest degradation), never an
    error. All money math happens in DuckDB, so Python only ever receives the small grouped
    result, never the raw fact rows.
    """

    def __init__(self, project_id: str, *, local_dir: str | None = None) -> None:
        self._project_id = project_id
        self._bucket = SNAPSHOT_BUCKET_TEMPLATE.format(pid=project_id)
        base = local_dir or os.path.join(tempfile.gettempdir(), "cost-snapshots")
        self._dir = Path(base)
        self._dir.mkdir(parents=True, exist_ok=True)
        self._lock = threading.Lock()
        # Last successful local-write monotonic time per cloud; drives the refresh cadence.
        self._loaded_at: dict[str, float] = {}
        # Clouds whose most recent download attempt found no blob — cached so a missing cloud
        # (e.g. AWS pre-perms-fix) does not trigger a GCS round-trip on every request.
        self._absent: dict[str, float] = {}

    def _local_path(self, cloud: str) -> Path:
        return self._dir / f"{cloud}.parquet"

    def _download_one(self, cloud: str) -> None:
        """Download one cloud's parquet to local disk if the GCS blob exists. Best-effort.

        A missing blob / transient GCS failure leaves any existing local copy in place (serve
        the last snapshot) and records the miss so the caller degrades to "cloud absent" rather
        than surfacing an error.
        """
        blob_path = snapshot_blob_path(cloud)
        try:
            client = get_storage_client(project_id=self._project_id)
            if not client.blob_exists(self._bucket, blob_path):  # pyright: ignore[reportAttributeAccessIssue]
                self._absent[cloud] = time.monotonic()
                return
            raw = client.download_bytes(self._bucket, blob_path)  # pyright: ignore[reportAttributeAccessIssue]
            tmp = self._local_path(cloud).with_suffix(".parquet.tmp")
            tmp.write_bytes(raw)
            tmp.replace(self._local_path(cloud))  # atomic swap — never a half-written read
            self._loaded_at[cloud] = time.monotonic()
            self._absent.pop(cloud, None)
        except (OSError, ValueError, RuntimeError) as exc:
            logger.warning("cost snapshot download for %s failed: %s", cloud, exc)

    def ensure_fresh(self, *, force: bool = False) -> None:
        """Ensure each cloud's local parquet is present + younger than the refresh window."""
        now = time.monotonic()
        with self._lock:
            for cloud in SNAPSHOT_CLOUDS:
                have_local = self._local_path(cloud).exists()
                loaded_at = self._loaded_at.get(cloud, 0.0)
                fresh = have_local and (now - loaded_at) < SNAPSHOT_REFRESH_SEC
                # Re-probe a known-absent cloud on the same cadence (so AWS starts rendering the
                # cycle after its blob first appears), not on every request.
                absent_at = self._absent.get(cloud)
                absent_recent = absent_at is not None and (now - absent_at) < SNAPSHOT_REFRESH_SEC
                if force or (not fresh and not absent_recent):
                    self._download_one(cloud)

    def present_clouds(self) -> list[str]:
        """Clouds with a local parquet available right now (order = SNAPSHOT_CLOUDS)."""
        return [c for c in SNAPSHOT_CLOUDS if self._local_path(c).exists()]

    def _read_parquet_union_sql(self) -> str | None:
        """A ``read_parquet([...])`` expression over the present per-cloud files, or None."""
        paths = [str(self._local_path(c)) for c in SNAPSHOT_CLOUDS if self._local_path(c).exists()]
        if not paths:
            return None
        quoted = ", ".join(f"'{p}'" for p in paths)
        return f"read_parquet([{quoted}], union_by_name=true)"

    def query(
        self, sql: str, params: Sequence[object] | None = None, *, force_refresh: bool = False
    ) -> list[tuple[object, ...]]:
        """Run ``sql`` against the ``cost_records`` view (union of present per-cloud parquets).

        ``sql`` references the table name ``cost_records``. Returns raw tuples (fetchall). A
        fresh DuckDB in-memory connection per call keeps this thread-safe under gunicorn's sync
        worker threadpool; the parquet files are memory-mapped so this is cheap. Returns an empty
        list when no snapshot is present (every cloud absent) — callers render an empty page.
        """
        self.ensure_fresh(force=force_refresh)
        union = self._read_parquet_union_sql()
        if union is None:
            return []
        con = duckdb.connect(database=":memory:")
        try:
            # nosec B608 — `union` is a read_parquet([...]) over hardcoded per-cloud filenames under
            # a config-derived local dir (no user input); the caller's `sql` is a module-internal
            # constant. Same false-positive class as queries.py's builders.
            con.execute(f"CREATE VIEW cost_records AS SELECT * FROM {union}")  # nosec B608
            cur = con.execute(sql, list(params) if params else None)
            return cast("list[tuple[object, ...]]", cur.fetchall())
        finally:
            con.close()

    def records_in_window(self, start_iso: str, end_iso: str, provisional_cutoff_iso: str) -> list[CostRecord]:
        """Rebuild ``CostRecord``s for ``[start_iso, end_iso)`` from the snapshot (day exclusive end).

        The transitional read path used by ``service._load_window`` — hands the existing Python
        aggregation the exact same record shape it got from the live providers, only sourced from
        the local parquet instead of a BigQuery/Athena scan. ``is_provisional`` is recomputed here
        against ``provisional_cutoff_iso`` (it is deliberately not stored in the snapshot).
        Returns ``[]`` when no snapshot is present. (Increment 2 replaces this with in-DuckDB
        aggregation so the raw rows never materialize in Python.)
        """
        rows = self.query(
            "SELECT cloud, day, service, resource_id, resource_kind, region, cost, credit, currency, "
            "cost_native, credit_native, sku, usage_amount, usage_unit, zone, purchase_option, "
            "machine_type, vcpu, memory_gb, is_placeholder, labels "
            "FROM cost_records WHERE day >= ? AND day < ?",
            [start_iso, end_iso],
        )
        out: list[CostRecord] = []
        for r in rows:
            day = _s(r[1])
            labels_json = _s(r[20])
            labels = cast("dict[str, str]", json.loads(labels_json)) if labels_json else {}
            out.append(
                CostRecord(
                    cloud=_s(r[0]),
                    day=day,
                    service=_s(r[2]),
                    resource_id=_s(r[3]),
                    resource_kind=_s(r[4]),
                    region=_s(r[5]),
                    cost=_f(r[6]),
                    credit=_f(r[7]),
                    currency=_s(r[8]),
                    cost_native=_f(r[9]),
                    credit_native=_f(r[10]),
                    sku=_s(r[11]),
                    usage_amount=_f(r[12]),
                    usage_unit=_s(r[13]),
                    zone=_s(r[14]),
                    purchase_option=_s(r[15]),
                    machine_type=_s(r[16]),
                    vcpu=_i(r[17]),
                    memory_gb=_fn(r[18]),
                    is_placeholder=bool(r[19]),
                    is_provisional=day >= provisional_cutoff_iso,
                    labels=labels,
                )
            )
        return out

    def snapshot_age_sec(self) -> dict[str, float | None]:
        """Local age (seconds) of each present cloud parquet, for an observability endpoint."""
        now = time.time()
        out: dict[str, float | None] = {}
        for cloud in SNAPSHOT_CLOUDS:
            p = self._local_path(cloud)
            out[cloud] = round(now - p.stat().st_mtime, 1) if p.exists() else None
        return out


def utc_now_iso() -> str:
    return datetime.now(UTC).isoformat()


# Module singleton — one store per process (shared across workers-of-a-process is not a thing;
# each gunicorn worker is its own process → its own /tmp cache + DuckDB, which is fine: the files
# are tiny). Keyed defensively by project in case config ever changes at runtime.
_STORE: dict[str, CostSnapshotStore] = {}
_STORE_LOCK = threading.Lock()


def get_cost_snapshot_store(project_id: str) -> CostSnapshotStore:
    """Return the process-wide snapshot store for ``project_id`` (created on first use)."""
    with _STORE_LOCK:
        store = _STORE.get(project_id)
        if store is None:
            store = CostSnapshotStore(project_id)
            _STORE[project_id] = store
        return store

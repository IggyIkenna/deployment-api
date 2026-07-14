"""Unit tests for the cost-observability GCS parquet snapshot + DuckDB read path.

Covers the parquet round-trip, the ``CostSnapshotStore`` reader (present-cloud detection,
window slicing, DuckDB aggregation), the bounded ``CostWindowCache``, and ``_load_window``'s
snapshot-preferred-with-live-fallback wiring. No GCS access — ``_download_one`` is stubbed and
parquet files are written straight into a tmp dir.
"""

from __future__ import annotations

import json
import os

import pyarrow as pa
import pytest

os.environ.setdefault("GCP_PROJECT_ID", "test-project")

from deployment_api.services.cost_observability import service as svc
from deployment_api.services.cost_observability import snapshot as snap
from deployment_api.services.cost_observability.cache import CostWindowCache
from deployment_api.services.cost_observability.models import (
    CLOUD_AWS,
    CLOUD_GCP,
    KIND_BUCKET,
    KIND_VM,
    CostRecord,
)
from deployment_api.services.cost_observability.service import CostObservabilityService


def _rec(cloud: str, day: str, **kw: object) -> CostRecord:
    base: dict[str, object] = {
        "cloud": cloud,
        "day": day,
        "service": "Compute Engine",
        "resource_id": "r1",
        "resource_kind": KIND_VM,
        "region": "r",
        "cost": 1.0,
    }
    base.update(kw)
    return CostRecord(**base)  # pyright: ignore[reportArgumentType]


def _store_with(
    tmp_path: object, monkeypatch: pytest.MonkeyPatch, files: dict[str, list[CostRecord]]
) -> snap.CostSnapshotStore:
    """A store whose local dir already holds the given per-cloud parquet files; GCS disabled."""
    monkeypatch.setattr(snap.CostSnapshotStore, "_download_one", lambda _self, _cloud: None)
    store = snap.CostSnapshotStore("test-project", "test-state-bucket", local_dir=str(tmp_path))
    for cloud, recs in files.items():
        store._local_path(cloud).write_bytes(snap.records_to_parquet_bytes(recs))  # pyright: ignore[reportPrivateUsage]
    return store


# --- blob layout -------------------------------------------------------------
def test_snapshot_blob_path_uses_state_bucket_prefix() -> None:
    # Cost blobs live under a prefix in the shared state bucket, not a dedicated bucket.
    assert snap.snapshot_blob_path("gcp") == "cost-snapshots/gcp.parquet"
    assert snap.snapshot_blob_path("aws") == "cost-snapshots/aws.parquet"


# --- parquet round-trip ------------------------------------------------------
def test_records_roundtrip_preserves_values_and_labels(tmp_path: object, monkeypatch: pytest.MonkeyPatch) -> None:
    recs = [
        _rec(
            CLOUD_GCP,
            "2026-07-10",
            resource_id="mtds-vm",
            region="asia-northeast1",
            cost=12.5,
            credit=-1.5,
            currency="GBP",
            cost_native=10.0,
            credit_native=-1.2,
            sku="N2 Instance Core running",
            usage_amount=24.0,
            usage_unit="hour",
            zone="asia-northeast1-c",
            purchase_option="on-demand",
            machine_type="n2-highmem-16",
            vcpu=16,
            memory_gb=128.0,
            labels={"purpose": "backfill", "venue": "BINANCE"},
        ),
        _rec(CLOUD_GCP, "2026-07-11", service="Cloud Storage", resource_id="b", resource_kind=KIND_BUCKET, cost=9.5),
    ]
    store = _store_with(tmp_path, monkeypatch, {CLOUD_GCP: recs})
    tbl = store.window_table("2026-07-01", "2026-08-01")
    assert tbl.num_rows == 2
    by_id = {r["resource_id"]: r for r in tbl.to_pylist()}
    vm = by_id["mtds-vm"]
    assert (vm["cost"], vm["credit"], vm["currency"], vm["cost_native"]) == (12.5, -1.5, "GBP", 10.0)
    assert (vm["machine_type"], vm["vcpu"], vm["memory_gb"]) == ("n2-highmem-16", 16, 128.0)
    # labels round-trip through the JSON string column
    assert json.loads(vm["labels"]) == {"purpose": "backfill", "venue": "BINANCE"}
    assert by_id["b"]["labels"] == ""  # empty-labels row stores "" (aggregate reads it as absent)


def test_window_table_slices_by_day_exclusive_end(tmp_path: object, monkeypatch: pytest.MonkeyPatch) -> None:
    recs = [_rec(CLOUD_GCP, d) for d in ("2026-07-08", "2026-07-10", "2026-07-12", "2026-07-14")]
    store = _store_with(tmp_path, monkeypatch, {CLOUD_GCP: recs})
    # end is EXCLUSIVE → 07-12 included, 07-14 excluded; 07-08 below start excluded.
    tbl = store.window_table("2026-07-09", "2026-07-14")
    assert sorted(r["day"] for r in tbl.to_pylist()) == ["2026-07-10", "2026-07-12"]


def test_present_clouds_and_union_aggregation(tmp_path: object, monkeypatch: pytest.MonkeyPatch) -> None:
    store = _store_with(
        tmp_path,
        monkeypatch,
        {
            CLOUD_GCP: [_rec(CLOUD_GCP, "2026-07-10", cost=3.0), _rec(CLOUD_GCP, "2026-07-11", cost=4.0)],
            CLOUD_AWS: [_rec(CLOUD_AWS, "2026-07-10", cost=5.0)],
        },
    )
    assert store.present_clouds() == [CLOUD_GCP, CLOUD_AWS]
    rows = store.query("SELECT cloud, ROUND(SUM(cost),2) FROM cost_records GROUP BY cloud ORDER BY cloud")
    assert rows == [(CLOUD_AWS, 5.0), (CLOUD_GCP, 7.0)]


def test_query_empty_when_no_snapshot(tmp_path: object, monkeypatch: pytest.MonkeyPatch) -> None:
    store = _store_with(tmp_path, monkeypatch, {})
    assert store.present_clouds() == []
    assert store.query("SELECT * FROM cost_records") == []
    assert store.window_table("2026-07-01", "2026-08-01").num_rows == 0


# --- bounded window cache ----------------------------------------------------
def test_cost_window_cache_evicts_oldest_over_cap() -> None:
    cache = CostWindowCache(max_entries=2)
    for key in ("a", "b", "c"):
        cache.get_or_load(key, lambda: [_rec(CLOUD_GCP, "2026-07-10")])
    keys = set(cache._store)  # pyright: ignore[reportPrivateUsage]
    assert keys == {"b", "c"}  # 'a' evicted (FIFO)


def test_cost_window_cache_restore_refreshes_recency() -> None:
    cache = CostWindowCache(max_entries=2)
    cache.get_or_load("a", lambda: [_rec(CLOUD_GCP, "2026-07-10")])
    cache.get_or_load("b", lambda: [_rec(CLOUD_GCP, "2026-07-10")])
    # Re-touch 'a' (force reload) so it is no longer the oldest, then add 'c' → 'b' evicts.
    cache.get_or_load("a", lambda: [_rec(CLOUD_GCP, "2026-07-10")], force=True)
    cache.get_or_load("c", lambda: [_rec(CLOUD_GCP, "2026-07-10")])
    assert set(cache._store) == {"a", "c"}  # pyright: ignore[reportPrivateUsage]


# --- _load_window_table: snapshot preferred, live fallback -------------------
class _FakeStore:
    def __init__(self, clouds: list[str], recs: list[CostRecord]) -> None:
        self._clouds = clouds
        self._recs = recs

    def ensure_fresh(self, *, force: bool = False) -> None:
        return None

    def present_clouds(self) -> list[str]:
        return self._clouds

    def window_table(self, _s: str, _e: str) -> pa.Table:
        return snap.records_to_table(self._recs)


def _live_service(monkeypatch: pytest.MonkeyPatch) -> CostObservabilityService:
    sentinel = [_rec(CLOUD_GCP, "2026-07-10", resource_id="LIVE")]
    monkeypatch.setattr(svc, "gcp_facts", lambda *_a, **_k: sentinel)
    monkeypatch.setattr(svc, "aws_facts", lambda *_a, **_k: [])
    monkeypatch.setattr(svc, "github_facts", lambda *_a, **_k: [])
    s = CostObservabilityService()
    monkeypatch.setattr(type(s._cfg), "is_mock_mode", lambda _self: False)  # pyright: ignore[reportPrivateUsage]
    return s


def _resource_ids(table: pa.Table) -> list[str]:
    return [r["resource_id"] for r in table.to_pylist()]


def test_load_window_prefers_snapshot_when_present(monkeypatch: pytest.MonkeyPatch) -> None:
    from datetime import date

    s = _live_service(monkeypatch)
    snap_recs = [_rec(CLOUD_GCP, "2026-07-10", resource_id="SNAPSHOT")]
    monkeypatch.setattr(svc, "get_cost_snapshot_store", lambda _pid, _bucket: _FakeStore([CLOUD_GCP], snap_recs))
    out = s._load_window_table(date(2026, 7, 1), date(2026, 8, 1))  # pyright: ignore[reportPrivateUsage]
    assert _resource_ids(out) == ["SNAPSHOT"]  # snapshot won, providers not consulted


def test_load_window_falls_back_to_live_when_no_snapshot(monkeypatch: pytest.MonkeyPatch) -> None:
    from datetime import date

    s = _live_service(monkeypatch)
    monkeypatch.setattr(svc, "get_cost_snapshot_store", lambda _pid, _bucket: _FakeStore([], []))
    out = s._load_window_table(date(2026, 7, 1), date(2026, 8, 1))  # pyright: ignore[reportPrivateUsage]
    assert _resource_ids(out) == ["LIVE"]  # no snapshot → live providers


def test_load_window_falls_back_when_snapshot_raises(monkeypatch: pytest.MonkeyPatch) -> None:
    from datetime import date

    s = _live_service(monkeypatch)

    def _boom(_pid: str, _bucket: str) -> object:
        raise RuntimeError("gcs down")

    monkeypatch.setattr(svc, "get_cost_snapshot_store", _boom)
    out = s._load_window_table(date(2026, 7, 1), date(2026, 8, 1))  # pyright: ignore[reportPrivateUsage]
    assert _resource_ids(out) == ["LIVE"]  # snapshot error degrades to live, never 5xx

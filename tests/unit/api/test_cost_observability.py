"""Unit tests for the cost-observability service, providers, and aggregation.

The cloud providers are monkeypatched to return fixed CostRecords, so these run with no
BigQuery/Athena access. Route-level behaviour is exercised live (verified against the real
exports) and by the deployment-ui Playwright spec via mock-api.
"""

from __future__ import annotations

import os
from datetime import date

import pytest

os.environ.setdefault("GCP_PROJECT_ID", "test-project")

from deployment_api.services.cost_observability import CostObservabilityService, CostRecord
from deployment_api.services.cost_observability import providers as prov
from deployment_api.services.cost_observability import service as svc


# --- provider pure helpers ---------------------------------------------------
def test_as_float_coerces_str_int_none() -> None:
    assert prov._as_float("12.5") == 12.5
    assert prov._as_float(3) == 3.0
    assert prov._as_float(None) == 0.0
    assert prov._as_float("") == 0.0
    assert prov._as_float("not-a-number") == 0.0


def test_short_name_strips_gce_path() -> None:
    assert prov._short_name("projects/106/instances/mtds-perp-funding-backfill") == "mtds-perp-funding-backfill"
    assert prov._short_name("mock-events-bucket") == "mock-events-bucket"


def test_kind_classification() -> None:
    assert prov._gcp_kind("Compute Engine", "vm-1") == "vm"
    assert prov._gcp_kind("Cloud Storage", "bkt") == "bucket"
    assert prov._gcp_kind("Cloud Run", "svc") == "other"
    assert prov._gcp_kind("Compute Engine", "") == "other"  # no resource id
    assert prov._aws_kind("AmazonEC2", "i-0abc") == "vm"
    assert prov._aws_kind("AmazonS3", "my-bucket") == "bucket"
    assert prov._aws_kind("AmazonEC2", "not-an-instance") == "other"


def test_github_facts_deterministic_and_flagged() -> None:
    start, end = date(2026, 7, 1), date(2026, 7, 4)
    a = prov.github_facts(start, end)
    b = prov.github_facts(start, end)
    assert [r.cost for r in a] == [r.cost for r in b]  # deterministic
    assert a, "expected github dummy records"
    assert all(r.is_placeholder for r in a)
    assert {r.day for r in a} == {"2026-07-01", "2026-07-02", "2026-07-03"}


# --- service aggregation (monkeypatched providers) ---------------------------
def _fake_gcp(_table: str, start: date, end: date, _cutoff: date) -> list[CostRecord]:
    recs: list[CostRecord] = []
    d = start
    while d < end:
        iso = d.isoformat()
        recs.append(
            CostRecord(
                cloud="gcp",
                day=iso,
                service="Compute Engine",
                resource_id="vm-1",
                resource_kind="vm",
                region="asia-northeast1",
                cost=10.0,
            )
        )
        recs.append(
            CostRecord(
                cloud="gcp",
                day=iso,
                service="Cloud Storage",
                resource_id="bkt-1",
                resource_kind="bucket",
                region="asia-northeast1",
                cost=5.0,
            )
        )
        d = date.fromordinal(d.toordinal() + 1)
    return recs


def _fake_aws(_db: str, _t: str, _r: str, _b: str, start: date, end: date, _c: date) -> list[CostRecord]:
    recs: list[CostRecord] = []
    d = start
    while d < end:
        recs.append(
            CostRecord(
                cloud="aws",
                day=d.isoformat(),
                service="Amazon EC2",
                resource_id="i-0a",
                resource_kind="vm",
                region="ap-northeast-1",
                cost=2.0,
            )
        )
        d = date.fromordinal(d.toordinal() + 1)
    return recs


@pytest.fixture
def service(monkeypatch: pytest.MonkeyPatch) -> CostObservabilityService:
    monkeypatch.setattr(svc, "gcp_facts", _fake_gcp)
    monkeypatch.setattr(svc, "aws_facts", _fake_aws)
    monkeypatch.setattr(svc, "github_facts", lambda start, end: [])
    s = CostObservabilityService()
    # Force the real-provider path (conftest sets CLOUD_MOCK_MODE=true globally); patch the
    # method on the class since the config is a pydantic model with validate_assignment.
    monkeypatch.setattr(type(s._cfg), "is_mock_mode", lambda _self: False)
    return s


def test_summarize_totals_and_deltas(service: CostObservabilityService) -> None:
    r = service.summarize(days=3)
    assert r.days == 3
    # gcp 15/day * 3 = 45, aws 2/day * 3 = 6, github 0 -> total 51
    gcp = next(c for c in r.clouds if c.cloud == "gcp")
    aws = next(c for c in r.clouds if c.cloud == "aws")
    assert gcp.total == 45.0
    assert aws.total == 6.0
    assert r.total == 51.0
    assert len(gcp.daily) == 3
    # equal prior window → 0% delta
    assert gcp.delta_pct == 0.0


def test_breakdown_by_service_groups_and_sorts(service: CostObservabilityService) -> None:
    r = service.breakdown("service", "all", days=3)
    labels = [(row.cloud, row.label, row.cost) for row in r.rows]
    assert ("gcp", "Compute Engine", 30.0) in labels  # 10*3
    assert ("gcp", "Cloud Storage", 15.0) in labels  # 5*3
    assert ("aws", "Amazon EC2", 6.0) in labels
    # sorted descending
    assert [row.cost for row in r.rows] == sorted((row.cost for row in r.rows), reverse=True)
    assert r.rows[0].share_pct > 0


def test_breakdown_by_bucket_filters_kind(service: CostObservabilityService) -> None:
    r = service.breakdown("bucket", "all", days=2)
    assert all(row.resource_kind == "bucket" for row in r.rows)
    assert {row.label for row in r.rows} == {"bkt-1"}


def test_breakdown_resource_carries_kind_for_leaf_tables(service: CostObservabilityService) -> None:
    r = service.breakdown("resource", "all", days=2)
    kinds = {row.label: row.resource_kind for row in r.rows}
    assert kinds["vm-1"] == "vm"
    assert kinds["bkt-1"] == "bucket"


def test_breakdown_cloud_filter(service: CostObservabilityService) -> None:
    r = service.breakdown("service", "aws", days=2)
    assert {row.cloud for row in r.rows} == {"aws"}


def test_timeseries_per_day_per_cloud(service: CostObservabilityService) -> None:
    r = service.timeseries(days=3, cloud="all")
    assert len(r.points) == 3
    for p in r.points:
        assert p.values["gcp"] == 15.0
        assert p.values["aws"] == 2.0


def test_cache_avoids_requery_until_forced(service: CostObservabilityService, monkeypatch: pytest.MonkeyPatch) -> None:
    calls = {"n": 0}

    def counting_gcp(_t: str, s: date, e: date, c: date) -> list[CostRecord]:
        calls["n"] += 1
        return _fake_gcp(_t, s, e, c)

    monkeypatch.setattr(svc, "gcp_facts", counting_gcp)
    service._cache.clear()
    service.breakdown("service", "gcp", days=3)
    first = calls["n"]
    service.breakdown("region", "gcp", days=3)  # same window → cache hit, no new query
    assert calls["n"] == first
    service.breakdown("service", "gcp", days=3, force=True)  # force → re-query
    assert calls["n"] > first

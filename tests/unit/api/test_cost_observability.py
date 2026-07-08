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
from deployment_api.services.cost_observability.queries import aws_facts_sql, gcp_facts_sql


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


def test_gcp_facts_sql_selects_sku_and_usage_columns() -> None:
    sql = gcp_facts_sql("proj.billing_export.resource_v1", date(2026, 7, 1), date(2026, 7, 4))
    assert "sku.description" in sql
    assert "usage.pricing_unit" in sql
    assert "usage.amount_in_pricing_units" in sql
    assert "location.zone" in sql
    assert "GROUP BY day, service, resource_id, region, sku, usage_unit, zone" in sql


def test_aws_facts_sql_selects_usage_type_and_amount() -> None:
    sql = aws_facts_sql("aws_billing", "cur_uts_cost_usage", date(2026, 7, 1), date(2026, 7, 4))
    assert "line_item_usage_type" in sql
    assert "line_item_usage_amount" in sql
    assert "line_item_availability_zone" in sql
    assert "GROUP BY 1, 2, 3, 4, 5, 6, 7" in sql


class _FakeAnalyticsClient:
    def __init__(self, rows: list[dict[str, object]]) -> None:
        self._rows = rows

    def execute_query(self, _sql: str) -> list[dict[str, object]]:
        return self._rows


def test_gcp_facts_maps_sku_and_usage_onto_cost_record(monkeypatch: pytest.MonkeyPatch) -> None:
    rows = [
        {
            "day": "2026-07-01",
            "service": "Cloud Storage",
            "resource_id": "my-bucket",
            "region": "us",
            "sku": "Coldline Storage US Regional",
            "usage_unit": "gibibyte month",
            "cost": 1.5,
            "credit": 0.0,
            "usage_amount": 12.5,
            "zone": "us-central1-a",
        }
    ]
    monkeypatch.setattr(prov, "get_analytics_client", lambda provider: _FakeAnalyticsClient(rows))
    out = prov.gcp_facts("proj.dataset.table", date(2026, 7, 1), date(2026, 7, 4), date(2026, 7, 3))
    assert len(out) == 1
    rec = out[0]
    assert rec.sku == "Coldline Storage US Regional"
    assert rec.usage_unit == "gibibyte month"
    assert rec.usage_amount == 12.5
    assert rec.zone == "us-central1-a"


def test_aws_facts_maps_usage_type_into_sku_field(monkeypatch: pytest.MonkeyPatch) -> None:
    rows = [
        {
            "day": "2026-07-01",
            "service": "Amazon EC2",
            "service_code": "AmazonEC2",
            "resource_id": "i-0abc",
            "region": "us-east-1",
            "usage_type": "BoxUsage:t3.micro",
            "cost": 3.2,
            "usage_amount": 24.0,
            "zone": "us-east-1a",
        }
    ]
    monkeypatch.setattr(prov, "AWSAnalyticsClient", lambda region, output_bucket: _FakeAnalyticsClient(rows))
    out = prov.aws_facts(
        "aws_billing",
        "cur_uts_cost_usage",
        "us-east-1",
        "uts-billing-cur",
        date(2026, 7, 1),
        date(2026, 7, 4),
        date(2026, 7, 3),
    )
    assert len(out) == 1
    rec = out[0]
    assert rec.sku == "BoxUsage:t3.micro"  # AWS usage_type is the SKU analog
    assert rec.usage_amount == 24.0
    assert rec.usage_unit == ""  # not sourced from the AWS export
    assert rec.zone == "us-east-1a"


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


def _fake_gcp_credited(_table: str, start: date, end: date, _cutoff: date) -> list[CostRecord]:
    """GCP rows carrying a promotional credit — net = cost + credit (credit ≤ 0)."""
    recs: list[CostRecord] = []
    d = start
    while d < end:
        recs.append(
            CostRecord(
                cloud="gcp",
                day=d.isoformat(),
                service="Compute Engine",
                resource_id="vm-1",
                resource_kind="vm",
                region="asia-northeast1",
                cost=10.0,
                credit=-2.0,  # promo credit applied that day
            )
        )
        d = date.fromordinal(d.toordinal() + 1)
    return recs


def test_summary_and_breakdown_are_net_of_credits(monkeypatch: pytest.MonkeyPatch) -> None:
    """The page must report what's actually invoiced (net), not the pre-credit gross, while still
    exposing gross + credit for the 'you pay = gross - credits' headline."""
    monkeypatch.setattr(svc, "gcp_facts", _fake_gcp_credited)
    monkeypatch.setattr(svc, "aws_facts", lambda *a, **k: [])
    monkeypatch.setattr(svc, "github_facts", lambda start, end: [])
    s = CostObservabilityService()
    monkeypatch.setattr(type(s._cfg), "is_mock_mode", lambda _self: False)

    r = s.summarize(days=3)
    gcp = next(c for c in r.clouds if c.cloud == "gcp")
    # gross 10*3 = 30, credit -2*3 = -6, net = 24 (what actually gets invoiced)
    assert gcp.gross == 30.0
    assert gcp.credit == -6.0
    assert gcp.total == 24.0
    assert r.gross == 30.0 and r.credit == -6.0 and r.total == 24.0  # grand rolls up net
    assert gcp.daily == [8.0, 8.0, 8.0]  # sparkline is net per day

    # breakdown + timeseries also net, so the whole page reconciles to the headline
    bd = s.breakdown("service", "gcp", days=3)
    assert next(row.cost for row in bd.rows if row.label == "Compute Engine") == 24.0  # net, not 30
    ts = s.timeseries(days=3, cloud="gcp")
    assert all(p.values["gcp"] == 8.0 for p in ts.points)


def test_breakdown_by_service_groups_and_sorts(service: CostObservabilityService) -> None:
    r = service.breakdown("service", "all", days=3)
    labels = [(row.cloud, row.label, row.cost) for row in r.rows]
    assert ("gcp", "Compute Engine", 30.0) in labels  # 10*3
    assert ("gcp", "Cloud Storage", 15.0) in labels  # 5*3
    assert ("aws", "Amazon EC2", 6.0) in labels
    # sorted descending
    assert [row.cost for row in r.rows] == sorted((row.cost for row in r.rows), reverse=True)
    assert r.rows[0].share_pct > 0


def _fake_gcp_with_sku(_table: str, start: date, end: date, _cutoff: date) -> list[CostRecord]:
    """Two SKUs under one service — the hidden-cost-driver case (Coldline Class A Operations)."""
    recs: list[CostRecord] = []
    d = start
    while d < end:
        recs.append(
            CostRecord(
                cloud="gcp",
                day=d.isoformat(),
                service="Cloud Storage",
                resource_id="bkt-1",
                resource_kind="bucket",
                region="us",
                cost=3.0,
                sku="Regional Coldline Class A Operations",
            )
        )
        recs.append(
            CostRecord(
                cloud="gcp",
                day=d.isoformat(),
                service="Cloud Storage",
                resource_id="bkt-1",
                resource_kind="bucket",
                region="us",
                cost=1.0,
                sku="Standard Storage US",
            )
        )
        d = date.fromordinal(d.toordinal() + 1)
    return recs


def test_breakdown_by_sku_groups_by_cloud_service_sku(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(svc, "gcp_facts", _fake_gcp_with_sku)
    monkeypatch.setattr(svc, "aws_facts", lambda *a, **k: [])
    monkeypatch.setattr(svc, "github_facts", lambda start, end: [])
    s = CostObservabilityService()
    monkeypatch.setattr(type(s._cfg), "is_mock_mode", lambda _self: False)

    r = s.breakdown("sku", "gcp", days=2)
    rows = {row.label: (row.cost, row.detail, row.cloud) for row in r.rows}
    assert rows["Regional Coldline Class A Operations"] == (6.0, "Cloud Storage", "gcp")  # 3.0 * 2 days
    assert rows["Standard Storage US"] == (2.0, "Cloud Storage", "gcp")  # 1.0 * 2 days
    # sorted descending, same as every other dimension
    assert [row.cost for row in r.rows] == sorted((row.cost for row in r.rows), reverse=True)


def test_breakdown_by_sku_defaults_missing_sku_to_unknown(service: CostObservabilityService) -> None:
    # the shared `service` fixture's fake facts don't set `sku` (default "")
    r = service.breakdown("sku", "gcp", days=1)
    assert {row.label for row in r.rows} == {"Unknown"}


def _fake_gcp_with_zone(_table: str, start: date, end: date, _cutoff: date) -> list[CostRecord]:
    """Two zones under one region — the "finer zone cut" case."""
    recs: list[CostRecord] = []
    d = start
    while d < end:
        recs.append(
            CostRecord(
                cloud="gcp",
                day=d.isoformat(),
                service="Compute Engine",
                resource_id="vm-1",
                resource_kind="vm",
                region="asia-northeast1",
                cost=4.0,
                zone="asia-northeast1-a",
            )
        )
        recs.append(
            CostRecord(
                cloud="gcp",
                day=d.isoformat(),
                service="Compute Engine",
                resource_id="vm-2",
                resource_kind="vm",
                region="asia-northeast1",
                cost=2.0,
                zone="asia-northeast1-b",
            )
        )
        d = date.fromordinal(d.toordinal() + 1)
    return recs


def test_breakdown_by_zone_is_finer_than_region(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(svc, "gcp_facts", _fake_gcp_with_zone)
    monkeypatch.setattr(svc, "aws_facts", lambda *a, **k: [])
    monkeypatch.setattr(svc, "github_facts", lambda start, end: [])
    s = CostObservabilityService()
    monkeypatch.setattr(type(s._cfg), "is_mock_mode", lambda _self: False)

    by_region = s.breakdown("region", "gcp", days=2)
    assert {row.label for row in by_region.rows} == {"asia-northeast1"}  # collapsed — same region

    by_zone = s.breakdown("zone", "gcp", days=2)
    costs = {row.label: row.cost for row in by_zone.rows}
    assert costs == {"asia-northeast1-a": 8.0, "asia-northeast1-b": 4.0}  # 4*2, 2*2 — a finer cut


def test_breakdown_by_zone_defaults_missing_zone_to_unknown(service: CostObservabilityService) -> None:
    # the shared `service` fixture's fake facts don't set `zone` (default "")
    r = service.breakdown("zone", "gcp", days=1)
    assert {row.label for row in r.rows} == {"unknown"}


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

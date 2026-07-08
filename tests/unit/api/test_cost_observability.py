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

from deployment_api.services.cost_observability import CostObservabilityService, CostRecord, waste
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


def test_as_int_or_none_coerces_and_rejects_blank() -> None:
    assert prov._as_int_or_none("16") == 16
    assert prov._as_int_or_none(16) == 16
    assert prov._as_int_or_none(None) is None
    assert prov._as_int_or_none("") is None
    assert prov._as_int_or_none("not-a-number") is None
    assert prov._as_int_or_none(True) is None  # bool is not a valid core count


def test_mib_to_gb_converts_and_handles_missing() -> None:
    assert prov._mib_to_gb("2048") == 2.0
    assert prov._mib_to_gb("131072") == 128.0  # n2-highmem-16, verified live
    assert prov._mib_to_gb(None) is None
    assert prov._mib_to_gb("") is None


def test_kind_classification() -> None:
    assert prov._gcp_kind("Compute Engine", "vm-1") == "vm"
    assert prov._gcp_kind("Cloud Storage", "bkt") == "bucket"
    assert prov._gcp_kind("Cloud Run", "svc") == "other"
    assert prov._gcp_kind("Compute Engine", "") == "other"  # no resource id
    assert prov._aws_kind("AmazonEC2", "i-0abc") == "vm"
    assert prov._aws_kind("AmazonS3", "my-bucket") == "bucket"
    assert prov._aws_kind("AmazonEC2", "not-an-instance") == "other"


def test_purchase_option_classification() -> None:
    # GCP: spot/preemptible compute SKUs -> spot; other compute-core/ram SKUs -> on-demand;
    # non-compute SKUs (storage, network, …) -> other, regardless of cloud.
    assert prov._purchase_option("gcp", "Spot Preemptible N2 Instance Core running in Tokyo") == "spot"
    assert prov._purchase_option("gcp", "N2 Instance Core running in Tokyo") == "on-demand"
    assert prov._purchase_option("gcp", "N2 Instance Ram running in Tokyo") == "on-demand"
    assert prov._purchase_option("gcp", "Coldline Storage US Regional") == "other"
    # AWS: usage_type embeds the purchase mode as a text prefix.
    assert prov._purchase_option("aws", "APN1-SpotUsage:c5.xlarge") == "spot"
    assert prov._purchase_option("aws", "APN1-BoxUsage:c5.xlarge") == "on-demand"
    assert prov._purchase_option("aws", "APN1-HeavyUsage:db.r5.large") == "on-demand"
    assert prov._purchase_option("aws", "DataTransfer-Out-Bytes") == "other"


def test_gcp_facts_and_aws_facts_populate_purchase_option(monkeypatch: pytest.MonkeyPatch) -> None:
    gcp_rows = [
        {
            "day": "2026-07-01",
            "service": "Compute Engine",
            "resource_id": "vm-1",
            "region": "asia-northeast1",
            "sku": "Spot Preemptible N2 Instance Core running in Tokyo",
            "usage_unit": "hour",
            "cost": 1.0,
            "credit": 0.0,
            "usage_amount": 1.0,
        }
    ]
    monkeypatch.setattr(prov, "get_analytics_client", lambda provider: _FakeAnalyticsClient(gcp_rows))
    gcp_out = prov.gcp_facts("proj.dataset.table", date(2026, 7, 1), date(2026, 7, 4), date(2026, 7, 3))
    assert gcp_out[0].purchase_option == "spot"

    aws_rows = [
        {
            "day": "2026-07-01",
            "service": "Amazon EC2",
            "service_code": "AmazonEC2",
            "resource_id": "i-0abc",
            "region": "us-east-1",
            "usage_type": "BoxUsage:t3.micro",
            "cost": 3.2,
            "usage_amount": 24.0,
        }
    ]
    monkeypatch.setattr(prov, "AWSAnalyticsClient", lambda region, output_bucket: _FakeAnalyticsClient(aws_rows))
    aws_out = prov.aws_facts(
        "aws_billing",
        "cur_uts_cost_usage",
        "us-east-1",
        "uts-billing-cur",
        date(2026, 7, 1),
        date(2026, 7, 4),
        date(2026, 7, 3),
    )
    assert aws_out[0].purchase_option == "on-demand"


def test_gcp_facts_sql_selects_sku_and_usage_columns() -> None:
    sql = gcp_facts_sql("proj.billing_export.resource_v1", date(2026, 7, 1), date(2026, 7, 4))
    assert "sku.description" in sql
    assert "usage.pricing_unit" in sql
    assert "usage.amount_in_pricing_units" in sql
    assert "location.zone" in sql
    assert "GROUP BY day, service, resource_id, region, sku, usage_unit, zone" in sql


def test_gcp_facts_sql_selects_machine_spec_system_labels() -> None:
    sql = gcp_facts_sql("proj.billing_export.resource_v1", date(2026, 7, 1), date(2026, 7, 4))
    assert "compute.googleapis.com/machine_spec" in sql
    assert "compute.googleapis.com/cores" in sql
    assert "compute.googleapis.com/memory" in sql
    assert "UNNEST(system_labels)" in sql


def test_aws_facts_sql_selects_usage_type_and_amount() -> None:
    sql = aws_facts_sql("aws_billing", "cur_uts_cost_usage", date(2026, 7, 1), date(2026, 7, 4))
    assert "line_item_usage_type" in sql
    assert "line_item_usage_amount" in sql
    assert "line_item_availability_zone" in sql
    assert "GROUP BY 1, 2, 3, 4, 5, 6, 7" in sql


def test_aws_facts_sql_uses_net_cost_and_includes_tax_and_fee() -> None:
    """Invoice reconciliation: net-of-discounts cost + Tax/Fee lines, not usage-only gross."""
    sql = aws_facts_sql("aws_billing", "cur_uts_cost_usage", date(2026, 7, 1), date(2026, 7, 4))
    assert "line_item_net_unblended_cost" in sql
    assert "line_item_unblended_cost" not in sql  # plain (non-net) column must not appear
    assert "'Usage', 'DiscountedUsage', 'Tax', 'Fee'" in sql


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


def test_gcp_facts_maps_machine_spec_onto_cost_record(monkeypatch: pytest.MonkeyPatch) -> None:
    rows = [
        {
            "day": "2026-07-01",
            "service": "Compute Engine",
            "resource_id": "projects/106/instances/n2-highmem-16-vm",
            "region": "asia-northeast1",
            "sku": "N2 Instance Core running in Tokyo",
            "usage_unit": "",
            "cost": 4.0,
            "credit": 0.0,
            "usage_amount": 16.0,
            "machine_spec": "n2-highmem-16",
            "machine_cores": "16",
            "machine_memory_mib": "131072",
        },
        {
            # Same VM's disk SKU row — no machine-spec system_labels (verified live).
            "day": "2026-07-01",
            "service": "Compute Engine",
            "resource_id": "extraspace-disk",
            "region": "asia-northeast1",
            "sku": "Balanced PD Capacity",
            "usage_unit": "gibibyte month",
            "cost": 0.18,
            "credit": 0.0,
            "usage_amount": 10.0,
            "machine_spec": None,
            "machine_cores": None,
            "machine_memory_mib": None,
        },
    ]
    monkeypatch.setattr(prov, "get_analytics_client", lambda provider: _FakeAnalyticsClient(rows))
    out = prov.gcp_facts("proj.dataset.table", date(2026, 7, 1), date(2026, 7, 4), date(2026, 7, 3))
    assert len(out) == 2
    vm_rec, disk_rec = out
    assert vm_rec.machine_type == "n2-highmem-16"
    assert vm_rec.vcpu == 16
    assert vm_rec.memory_gb == 128.0
    assert disk_rec.machine_type == ""
    assert disk_rec.vcpu is None
    assert disk_rec.memory_gb is None


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
    # No real GCE calls from unit tests — the orphaned-disk cross-ref (empty fleet == "no
    # running VM matches", the honest default) is exercised explicitly in its own tests below.
    monkeypatch.setattr(svc, "list_running_vm_names", lambda _project_id: set())
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


def test_breakdown_rows_bifurcate_gross_credit_net(monkeypatch: pytest.MonkeyPatch) -> None:
    """Each breakdown row must carry gross/credit alongside net, and roll up to the summary total."""
    monkeypatch.setattr(svc, "gcp_facts", _fake_gcp_credited)
    monkeypatch.setattr(svc, "aws_facts", lambda *a, **k: [])
    monkeypatch.setattr(svc, "github_facts", lambda start, end: [])
    s = CostObservabilityService()
    monkeypatch.setattr(type(s._cfg), "is_mock_mode", lambda _self: False)

    summary = s.summarize(days=3)
    gcp_summary = next(c for c in summary.clouds if c.cloud == "gcp")

    for dimension in ("service", "resource", "region", "day", "sku"):
        bd = s.breakdown(dimension, "gcp", days=3)
        row_gross = round(sum(row.gross for row in bd.rows), 2)
        row_credit = round(sum(row.credit for row in bd.rows), 2)
        row_net = round(sum(row.cost for row in bd.rows), 2)
        for row in bd.rows:
            assert round(row.gross + row.credit, 2) == row.cost, f"{dimension} row {row.label} doesn't reconcile"
        assert row_gross == gcp_summary.gross, dimension
        assert row_credit == gcp_summary.credit, dimension
        assert row_net == gcp_summary.total, dimension


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


def test_breakdown_resource_and_service_expose_purchase_option(monkeypatch: pytest.MonkeyPatch) -> None:
    """A resource/service row folds to 'spot' if ANY of its underlying SKU lines is spot-priced —
    the SPOT-VMs HARD RULE question is "did any spot cost show up here", not an arbitrary pick."""
    recs = [
        # vm-spot: entirely spot compute.
        CostRecord(
            cloud="gcp",
            day="2026-07-01",
            service="Compute Engine",
            resource_id="vm-spot",
            resource_kind="vm",
            region="asia-northeast1",
            cost=1.0,
            sku="Spot Preemptible N2 Instance Core running in Tokyo",
            purchase_option="spot",
        ),
        # vm-mixed: one on-demand line + one spot line -> resource folds to spot.
        CostRecord(
            cloud="gcp",
            day="2026-07-01",
            service="Compute Engine",
            resource_id="vm-mixed",
            resource_kind="vm",
            region="asia-northeast1",
            cost=2.0,
            sku="N2 Instance Core running in Tokyo",
            purchase_option="on-demand",
        ),
        CostRecord(
            cloud="gcp",
            day="2026-07-02",
            service="Compute Engine",
            resource_id="vm-mixed",
            resource_kind="vm",
            region="asia-northeast1",
            cost=1.0,
            sku="Spot Preemptible N2 Instance Core running in Tokyo",
            purchase_option="spot",
        ),
        # bkt-1: storage SKU, purchase-option axis doesn't apply -> other.
        CostRecord(
            cloud="gcp",
            day="2026-07-01",
            service="Cloud Storage",
            resource_id="bkt-1",
            resource_kind="bucket",
            region="asia-northeast1",
            cost=0.5,
            sku="Coldline Storage US Regional",
            purchase_option="other",
        ),
    ]
    monkeypatch.setattr(svc, "gcp_facts", lambda *a, **k: recs)
    monkeypatch.setattr(svc, "aws_facts", lambda *a, **k: [])
    monkeypatch.setattr(svc, "github_facts", lambda start, end: [])
    s = CostObservabilityService()
    monkeypatch.setattr(type(s._cfg), "is_mock_mode", lambda _self: False)

    by_resource = {row.label: row.purchase_option for row in s.breakdown("resource", "gcp", days=2).rows}
    assert by_resource["vm-spot"] == "spot"
    assert by_resource["vm-mixed"] == "spot"  # any spot line present -> spot
    assert by_resource["bkt-1"] == "other"

    by_service = {row.label: row.purchase_option for row in s.breakdown("service", "gcp", days=2).rows}
    assert by_service["Compute Engine"] == "spot"  # rolls up from vm-spot + vm-mixed
    assert by_service["Cloud Storage"] == "other"


def test_breakdown_resource_carries_machine_spec_from_sibling_sku_rows(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A VM's disk-SKU row has no machine_type; the resource row must still surface the spec
    carried on its Core/Ram-SKU row (verified live: only the instance SKU rows have it)."""

    def fake_gcp(_table: str, start: date, end: date, _cutoff: date) -> list[CostRecord]:
        return [
            CostRecord(
                cloud="gcp",
                day=start.isoformat(),
                service="Compute Engine",
                resource_id="vm-1",
                resource_kind="vm",
                region="asia-northeast1",
                cost=4.0,
                machine_type="e2-highmem-16",
                vcpu=16,
                memory_gb=128.0,
            ),
            CostRecord(
                cloud="gcp",
                day=start.isoformat(),
                service="Compute Engine",
                resource_id="vm-1",
                resource_kind="vm",
                region="asia-northeast1",
                cost=0.5,  # e.g. the attached-disk SKU row for the same VM
            ),
            CostRecord(
                cloud="gcp",
                day=start.isoformat(),
                service="Cloud Storage",
                resource_id="bkt-1",
                resource_kind="bucket",
                region="asia-northeast1",
                cost=1.0,
            ),
        ]

    monkeypatch.setattr(svc, "gcp_facts", fake_gcp)
    monkeypatch.setattr(svc, "aws_facts", lambda *a, **k: [])
    monkeypatch.setattr(svc, "github_facts", lambda start, end: [])
    s = CostObservabilityService()
    monkeypatch.setattr(type(s._cfg), "is_mock_mode", lambda _self: False)

    r = s.breakdown("resource", "all", days=1)
    vm_row = next(row for row in r.rows if row.label == "vm-1")
    assert vm_row.machine_type == "e2-highmem-16"
    assert vm_row.vcpu == 16
    assert vm_row.memory_gb == 128.0
    assert vm_row.cost == 4.5  # both SKU rows for the resource rolled up

    bkt_row = next(row for row in r.rows if row.label == "bkt-1")
    assert bkt_row.machine_type == ""
    assert bkt_row.vcpu is None
    assert bkt_row.memory_gb is None


def test_breakdown_cloud_filter(service: CostObservabilityService) -> None:
    r = service.breakdown("service", "aws", days=2)
    assert {row.cloud for row in r.rows} == {"aws"}


def test_timeseries_per_day_per_cloud(service: CostObservabilityService) -> None:
    r = service.timeseries(days=3, cloud="all")
    assert len(r.points) == 3
    for p in r.points:
        assert p.values["gcp"] == 15.0
        assert p.values["aws"] == 2.0


# --- waste classifiers --------------------------------------------------------
def test_is_gcp_idle_static_ip_sku_matches_exact_idle_sku_only() -> None:
    assert waste.is_gcp_idle_static_ip_sku("Static Ip Charge") is True
    assert waste.is_gcp_idle_static_ip_sku("External IP Charge on a Standard VM") is False


def test_is_gcp_disk_capacity_sku_matches_pd_capacity_suffix() -> None:
    assert waste.is_gcp_disk_capacity_sku("SSD backed PD Capacity") is True
    assert waste.is_gcp_disk_capacity_sku("Storage PD Capacity") is True
    assert waste.is_gcp_disk_capacity_sku("N2 Instance Core running in Tokyo") is False


def test_is_aws_idle_elastic_ip_usage_type_matches_idle_marker() -> None:
    assert waste.is_aws_idle_elastic_ip_usage_type("APN1-ElasticIP:IdleAddress") is True
    assert waste.is_aws_idle_elastic_ip_usage_type("BoxUsage:t3.micro") is False


def test_classify_waste_gcp_idle_static_ip_needs_no_fleet_cross_ref() -> None:
    label = waste.classify_waste(
        cloud="gcp", sku="Static Ip Charge", resource_id="harsh-static-ip", running_vm_names=frozenset()
    )
    assert label == waste.WASTE_IDLE_STATIC_IP


def test_classify_waste_gcp_disk_orphaned_when_no_matching_running_vm() -> None:
    label = waste.classify_waste(
        cloud="gcp",
        sku="SSD backed PD Capacity",
        resource_id="ikenna-windows-tokyo-restored",
        running_vm_names=frozenset(),
    )
    assert label == waste.WASTE_ORPHANED_DISK


def test_classify_waste_gcp_disk_not_flagged_when_vm_is_running() -> None:
    label = waste.classify_waste(
        cloud="gcp",
        sku="SSD backed PD Capacity",
        resource_id="vm-1",
        running_vm_names=frozenset({"vm-1"}),
    )
    assert label == ""


def test_classify_waste_aws_idle_elastic_ip() -> None:
    label = waste.classify_waste(
        cloud="aws", sku="APN1-ElasticIP:IdleAddress", resource_id="eipalloc-0abc", running_vm_names=frozenset()
    )
    assert label == waste.WASTE_IDLE_ELASTIC_IP


def test_classify_waste_no_match_returns_empty() -> None:
    label = waste.classify_waste(
        cloud="gcp", sku="N1 Predefined Instance Core", resource_id="vm-1", running_vm_names=frozenset()
    )
    assert label == ""


# --- service-level waste flagging (dimension=resource) ------------------------
def _fake_gcp_with_waste(_table: str, start: date, end: date, _cutoff: date) -> list[CostRecord]:
    return [
        CostRecord(
            cloud="gcp",
            day=start.isoformat(),
            service="Compute Engine",
            resource_id="harsh-static-ip",
            resource_kind="other",
            region="asia-northeast1",
            cost=5.95,
            sku="Static Ip Charge",
        ),
        CostRecord(
            cloud="gcp",
            day=start.isoformat(),
            service="Compute Engine",
            resource_id="ikenna-windows-tokyo-restored",
            resource_kind="other",
            region="asia-northeast1",
            cost=68.62,
            sku="SSD backed PD Capacity",
        ),
        CostRecord(
            cloud="gcp",
            day=start.isoformat(),
            service="Compute Engine",
            resource_id="vm-1",
            resource_kind="vm",
            region="asia-northeast1",
            cost=10.0,
            sku="N1 Predefined Instance Core running in Tokyo",
        ),
    ]


def test_breakdown_resource_flags_idle_static_ip_and_orphaned_disk(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(svc, "gcp_facts", _fake_gcp_with_waste)
    monkeypatch.setattr(svc, "aws_facts", lambda *a, **k: [])
    monkeypatch.setattr(svc, "github_facts", lambda start, end: [])
    monkeypatch.setattr(svc, "list_running_vm_names", lambda _project_id: {"vm-1"})  # ikenna disk has no match
    s = CostObservabilityService()
    monkeypatch.setattr(type(s._cfg), "is_mock_mode", lambda _self: False)

    rows = {row.label: row for row in s.breakdown("resource", "gcp", days=1).rows}
    assert rows["harsh-static-ip"].is_idle is True
    assert rows["harsh-static-ip"].waste_kind == waste.WASTE_IDLE_STATIC_IP
    assert rows["ikenna-windows-tokyo-restored"].is_idle is True
    assert rows["ikenna-windows-tokyo-restored"].waste_kind == waste.WASTE_ORPHANED_DISK
    assert rows["vm-1"].is_idle is False
    assert rows["vm-1"].waste_kind == ""


def test_breakdown_resource_disk_not_flagged_when_matching_vm_runs(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(svc, "gcp_facts", _fake_gcp_with_waste)
    monkeypatch.setattr(svc, "aws_facts", lambda *a, **k: [])
    monkeypatch.setattr(svc, "github_facts", lambda start, end: [])
    monkeypatch.setattr(svc, "list_running_vm_names", lambda _project_id: {"ikenna-windows-tokyo-restored", "vm-1"})
    s = CostObservabilityService()
    monkeypatch.setattr(type(s._cfg), "is_mock_mode", lambda _self: False)

    rows = {row.label: row for row in s.breakdown("resource", "gcp", days=1).rows}
    assert rows["ikenna-windows-tokyo-restored"].is_idle is False


def test_breakdown_bucket_dimension_never_flags_waste(service: CostObservabilityService) -> None:
    r = service.breakdown("bucket", "all", days=2)
    assert all(row.is_idle is False and row.waste_kind == "" for row in r.rows)


def test_breakdown_bucket_dimension_skips_fleet_lookup(monkeypatch: pytest.MonkeyPatch) -> None:
    calls = {"n": 0}

    def counting_running(_project_id: str) -> set[str]:
        calls["n"] += 1
        return set()

    monkeypatch.setattr(svc, "gcp_facts", _fake_gcp)
    monkeypatch.setattr(svc, "aws_facts", _fake_aws)
    monkeypatch.setattr(svc, "github_facts", lambda start, end: [])
    monkeypatch.setattr(svc, "list_running_vm_names", counting_running)
    s = CostObservabilityService()
    monkeypatch.setattr(type(s._cfg), "is_mock_mode", lambda _self: False)

    s.breakdown("bucket", "all", days=2)
    assert calls["n"] == 0  # bucket dimension never needs the running-VM cross-ref


def _fake_gcp_storage(_table: str, start: date, end: date, _cutoff: date) -> list[CostRecord]:
    """One bucket with a storage-volume SKU (Coldline) + an operations SKU (excluded — count unit)."""
    recs: list[CostRecord] = []
    d = start
    while d < end:
        iso = d.isoformat()
        recs.append(
            CostRecord(
                cloud="gcp",
                day=iso,
                service="Cloud Storage",
                resource_id="my-bucket",
                resource_kind="bucket",
                region="us",
                cost=0.05,
                sku="Coldline Storage US Regional",
                usage_unit="gibibyte month",
                usage_amount=1.0,
            )
        )
        recs.append(
            CostRecord(
                cloud="gcp",
                day=iso,
                service="Cloud Storage",
                resource_id="my-bucket",
                resource_kind="bucket",
                region="us",
                cost=0.01,
                sku="Regional Standard Class A Operations",
                usage_unit="count",
                usage_amount=500.0,
            )
        )
        d = date.fromordinal(d.toordinal() + 1)
    return recs


def test_bucket_breakdown_adds_storage_gb_and_class_split(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(svc, "gcp_facts", _fake_gcp_storage)
    monkeypatch.setattr(svc, "aws_facts", lambda *a, **k: [])
    monkeypatch.setattr(svc, "github_facts", lambda start, end: [])
    s = CostObservabilityService()
    monkeypatch.setattr(type(s._cfg), "is_mock_mode", lambda _self: False)

    r = s.breakdown("bucket", "gcp", days=3)
    row = next(row for row in r.rows if row.label == "my-bucket")

    expected_gb = 3.0 * svc._AVG_DAYS_PER_MONTH / 3  # 3 days * 1.0 GiB-month / 3-day window
    assert row.storage_gb == pytest.approx(expected_gb, abs=0.01)
    assert row.storage_class_gb is not None
    assert set(row.storage_class_gb) == {"Coldline"}  # operations SKU (count unit) excluded
    assert row.storage_class_gb["Coldline"] == pytest.approx(expected_gb, abs=0.01)
    assert row.cost == pytest.approx(0.18, abs=0.001)  # (0.05 + 0.01) * 3
    assert row.cost_per_gb == pytest.approx(row.cost / row.storage_gb, abs=0.0005)


def _fake_aws_storage(_db: str, _t: str, _r: str, _b: str, start: date, end: date, _c: date) -> list[CostRecord]:
    recs: list[CostRecord] = []
    d = start
    while d < end:
        recs.append(
            CostRecord(
                cloud="aws",
                day=d.isoformat(),
                service="Amazon Simple Storage Service",
                resource_id="my-s3-bucket",
                resource_kind="bucket",
                region="us-east-1",
                cost=0.02,
                sku="TimedStorage-GlacierByteHrs",
                usage_amount=2.0,
            )
        )
        d = date.fromordinal(d.toordinal() + 1)
    return recs


def test_bucket_breakdown_classifies_aws_storage_types(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(svc, "gcp_facts", lambda *a, **k: [])
    monkeypatch.setattr(svc, "aws_facts", _fake_aws_storage)
    monkeypatch.setattr(svc, "github_facts", lambda start, end: [])
    s = CostObservabilityService()
    monkeypatch.setattr(type(s._cfg), "is_mock_mode", lambda _self: False)

    r = s.breakdown("bucket", "aws", days=3)
    row = next(row for row in r.rows if row.label == "my-s3-bucket")

    expected_gb = 3.0 * 2.0 * svc._AVG_DAYS_PER_MONTH / 3  # 3 days * 2.0 usage_amount / 3-day window
    assert row.storage_class_gb == {"Coldline": pytest.approx(expected_gb, abs=0.01)}


def test_non_bucket_breakdown_rows_carry_no_storage_fields(service: CostObservabilityService) -> None:
    r = service.breakdown("resource", "all", days=2)
    assert all(row.storage_gb is None and row.storage_class_gb is None for row in r.rows)


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

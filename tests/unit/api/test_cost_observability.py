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


def test_aws_facts_sql_splits_gross_cost_and_credit() -> None:
    """AWS mirrors GCP's cost/credit split so the tab reports net-of-credits: cost = unblended over
    Usage/DiscountedUsage/Tax/Fee, credit = unblended over Credit line-items. This CUR's crawler
    schema has no `line_item_net_unblended_cost` column (an earlier switch to it silently zeroed the
    whole AWS tab, since a failed per-cloud Athena query is isolated)."""
    sql = aws_facts_sql("aws_billing", "cur_uts_cost_usage", date(2026, 7, 1), date(2026, 7, 4))
    assert "line_item_unblended_cost" in sql
    assert "line_item_net_unblended_cost" not in sql  # absent from this CUR's schema — would error → 0 rows
    assert "AS cost" in sql and "AS credit" in sql  # the gross/credit split
    assert "line_item_line_item_type = 'Credit'" in sql  # credit CASE branch
    assert "'Usage', 'DiscountedUsage', 'Tax', 'Fee', 'Credit'" in sql


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
    # No real GCE calls from unit tests — the orphaned-disk cross-ref (empty unattached-disk
    # set == "flag nothing", the honest default) is exercised explicitly in its own tests below.
    monkeypatch.setattr(svc, "list_unattached_disk_names", lambda _project_id: set())
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


def test_cost_component_classification() -> None:
    """Text-pattern maps a GCP SKU / AWS usage_type into the 4 cost buckets (verified live 2026-07-09)."""
    assert svc._cost_component("gcp", "Regional Coldline Class A Operations") == "operations"
    assert svc._cost_component("gcp", "Standard Storage Tokyo") == "storage"
    assert svc._cost_component("gcp", "Download APAC") == "egress"
    assert svc._cost_component("aws", "APN1-Requests-Tier1") == "operations"
    assert svc._cost_component("aws", "APN1-TimedStorage-ByteHrs") == "storage"
    assert svc._cost_component("aws", "APN1-DataTransfer-Out-Bytes") == "egress"


def test_breakdown_bucket_splits_net_cost_into_components(monkeypatch: pytest.MonkeyPatch) -> None:
    """A bucket's net cost is split into storage / operations / egress by SKU and the parts sum to the
    row's cost. An event-log bucket is operations-dominated (millions of Class-A writes on little
    stored data) — the split must surface that, not read as a bare storage total."""
    recs = [
        # events bucket: tiny storage, huge Class-A operations, a little egress.
        CostRecord(
            cloud="gcp",
            day="2026-07-01",
            service="Cloud Storage",
            resource_id="events",
            resource_kind="bucket",
            region="asia-northeast1",
            cost=2400.0,
            sku="Regional Coldline Class A Operations",
            usage_unit="count",
        ),
        CostRecord(
            cloud="gcp",
            day="2026-07-01",
            service="Cloud Storage",
            resource_id="events",
            resource_kind="bucket",
            region="asia-northeast1",
            cost=0.5,
            sku="Standard Storage Tokyo",
            usage_unit="gibibyte month",
            usage_amount=60.0,
        ),
        CostRecord(
            cloud="gcp",
            day="2026-07-01",
            service="Cloud Storage",
            resource_id="events",
            resource_kind="bucket",
            region="asia-northeast1",
            cost=5.0,
            sku="Download APAC",
            usage_unit="gibibyte",
        ),
        # cefi bucket: storage-dominated.
        CostRecord(
            cloud="gcp",
            day="2026-07-01",
            service="Cloud Storage",
            resource_id="cefi",
            resource_kind="bucket",
            region="asia-northeast1",
            cost=300.0,
            sku="Standard Storage Tokyo",
            usage_unit="gibibyte month",
            usage_amount=50000.0,
        ),
        CostRecord(
            cloud="gcp",
            day="2026-07-01",
            service="Cloud Storage",
            resource_id="cefi",
            resource_kind="bucket",
            region="asia-northeast1",
            cost=90.0,
            sku="Regional Standard Class A Operations",
            usage_unit="count",
        ),
    ]
    monkeypatch.setattr(svc, "gcp_facts", lambda *a, **k: recs)
    monkeypatch.setattr(svc, "aws_facts", lambda *a, **k: [])
    monkeypatch.setattr(svc, "github_facts", lambda start, end: [])
    s = CostObservabilityService()
    monkeypatch.setattr(type(s._cfg), "is_mock_mode", lambda _self: False)

    rows = {row.label: row for row in s.breakdown("bucket", "gcp", days=2).rows}

    events = rows["events"].cost_by_component
    assert events is not None
    # parts sum (net) to the row's cost within rounding
    assert round(sum(events.values()), 2) == pytest.approx(rows["events"].cost, abs=0.02)
    # operations dominate storage — the whole point of the split
    assert events["operations"] > events["storage"]
    assert events["storage"] == pytest.approx(0.5, abs=0.01)
    assert events["egress"] == pytest.approx(5.0, abs=0.01)

    cefi = rows["cefi"].cost_by_component
    assert cefi is not None
    assert cefi["storage"] > cefi["operations"]


def test_breakdown_caps_to_top_n_with_reconciling_other_and_unattributed(monkeypatch: pytest.MonkeyPatch) -> None:
    """A high-cardinality dimension shows the top _BREAKDOWN_LIMIT groups + an 'Other (N more)' roll-up
    (+ an 'Unattributed' row for resource-less cost); the total equals the TRUE window total (not the
    shrunk top-N sum) and the shown rows sum to it EXACTLY — the "Other" row absorbs the residual."""
    recs = [
        CostRecord(
            cloud="gcp",
            day="2026-07-01",
            service="Compute Engine",
            resource_id=f"vm-{i:03d}",
            resource_kind="vm",
            region="asia-northeast1",
            cost=float(150 - i),  # 150 distinct resources, descending cost -> forces the top-100 cap
        )
        for i in range(150)
    ]
    # resource-less spend (no resource_id) -> surfaced as the "Unattributed" row, not dropped
    recs.append(
        CostRecord(
            cloud="gcp",
            day="2026-07-01",
            service="Cloud Run",
            resource_id="",
            resource_kind="other",
            region="asia-northeast1",
            cost=42.0,
        )
    )
    monkeypatch.setattr(svc, "gcp_facts", lambda *a, **k: recs)
    monkeypatch.setattr(svc, "aws_facts", lambda *a, **k: [])
    monkeypatch.setattr(svc, "github_facts", lambda start, end: [])
    s = CostObservabilityService()
    monkeypatch.setattr(type(s._cfg), "is_mock_mode", lambda _self: False)

    r = s.breakdown("resource", "gcp", days=2)
    real = [row for row in r.rows if not row.is_aggregate]
    agg = [row for row in r.rows if row.is_aggregate]

    expected_total = round(sum(x.cost for x in recs), 2)  # 11325 (vms) + 42 (unattributed)
    assert r.total == pytest.approx(expected_total, abs=0.01)
    assert r.total_groups == 150  # distinct resources; the unattributed cost is NOT a group
    assert len(real) == svc._BREAKDOWN_LIMIT  # capped to the top 100
    assert any(row.label.startswith("Other (") for row in agg)
    assert any(row.label.startswith("Unattributed") and row.cost == pytest.approx(42.0, abs=0.01) for row in agg)
    # shown rows (top 100 + Other + Unattributed) sum to the header total EXACTLY
    assert round(sum(row.cost for row in r.rows), 2) == pytest.approx(r.total, abs=0.01)


def test_breakdown_no_cap_below_limit_has_no_aggregate_rows(monkeypatch: pytest.MonkeyPatch) -> None:
    """A dimension with fewer than _BREAKDOWN_LIMIT groups shows every group and NO roll-up row."""
    recs = [
        CostRecord(
            cloud="gcp",
            day="2026-07-01",
            service=f"Service {i}",
            resource_id=f"r-{i}",
            resource_kind="other",
            region="asia-northeast1",
            cost=10.0,
        )
        for i in range(5)
    ]
    monkeypatch.setattr(svc, "gcp_facts", lambda *a, **k: recs)
    monkeypatch.setattr(svc, "aws_facts", lambda *a, **k: [])
    monkeypatch.setattr(svc, "github_facts", lambda start, end: [])
    s = CostObservabilityService()
    monkeypatch.setattr(type(s._cfg), "is_mock_mode", lambda _self: False)

    r = s.breakdown("service", "gcp", days=2)
    assert r.total_groups == 5
    assert all(not row.is_aggregate for row in r.rows)
    assert r.total == pytest.approx(50.0, abs=0.01)


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
def test_is_gcp_idle_static_ip_sku_matches_stem_including_regional_suffix() -> None:
    assert waste.is_gcp_idle_static_ip_sku("Static Ip Charge") is True
    assert waste.is_gcp_idle_static_ip_sku("Static Ip Charge in Japan") is True  # regional variant
    assert waste.is_gcp_idle_static_ip_sku("External IP Charge on a Standard VM") is False


def test_is_gcp_disk_capacity_sku_matches_stem_including_regional_suffix() -> None:
    assert waste.is_gcp_disk_capacity_sku("Balanced PD Capacity") is True
    assert waste.is_gcp_disk_capacity_sku("SSD backed PD Capacity in Japan") is True  # regional
    assert waste.is_gcp_disk_capacity_sku("Storage PD Capacity in Japan") is True  # regional
    assert waste.is_gcp_disk_capacity_sku("N2 Instance Core running in Tokyo") is False


def test_is_aws_idle_elastic_ip_usage_type_matches_idle_marker() -> None:
    assert waste.is_aws_idle_elastic_ip_usage_type("APN1-ElasticIP:IdleAddress") is True
    assert waste.is_aws_idle_elastic_ip_usage_type("BoxUsage:t3.micro") is False


def test_classify_waste_gcp_idle_static_ip_needs_no_disk_cross_ref() -> None:
    label = waste.classify_waste(
        cloud="gcp",
        sku="Static Ip Charge in Japan",
        resource_id="harsh-static-ip",
        unattached_disk_names=frozenset(),
    )
    assert label == waste.WASTE_IDLE_STATIC_IP


def test_classify_waste_gcp_disk_orphaned_when_unattached() -> None:
    label = waste.classify_waste(
        cloud="gcp",
        sku="SSD backed PD Capacity in Japan",
        resource_id="ikenna-windows-tokyo-restored",
        unattached_disk_names=frozenset({"ikenna-windows-tokyo-restored"}),
    )
    assert label == waste.WASTE_ORPHANED_DISK


def test_classify_waste_gcp_disk_not_flagged_when_attached() -> None:
    # Disk SKU matches, but the disk is attached (absent from the unattached set) → not orphaned.
    label = waste.classify_waste(
        cloud="gcp",
        sku="SSD backed PD Capacity in Japan",
        resource_id="attached-disk",
        unattached_disk_names=frozenset(),
    )
    assert label == ""


def test_classify_waste_aws_idle_elastic_ip() -> None:
    label = waste.classify_waste(
        cloud="aws", sku="APN1-ElasticIP:IdleAddress", resource_id="eipalloc-0abc", unattached_disk_names=frozenset()
    )
    assert label == waste.WASTE_IDLE_ELASTIC_IP


def test_classify_waste_no_match_returns_empty() -> None:
    label = waste.classify_waste(
        cloud="gcp", sku="N1 Predefined Instance Core", resource_id="vm-1", unattached_disk_names=frozenset()
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
            # REGIONAL SKU strings (as the real billing export emits them) — proves the
            # substring matchers survive the "... in Japan" suffix that exact/endswith missed.
            cost=5.95,
            sku="Static Ip Charge in Japan",
        ),
        CostRecord(
            cloud="gcp",
            day=start.isoformat(),
            service="Compute Engine",
            resource_id="ikenna-windows-tokyo-restored",
            resource_kind="other",
            region="asia-northeast1",
            cost=68.62,
            sku="SSD backed PD Capacity in Japan",
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
    # ikenna's disk is unattached (orphaned); the regional SKU strings must still classify.
    monkeypatch.setattr(svc, "list_unattached_disk_names", lambda _project_id: {"ikenna-windows-tokyo-restored"})
    s = CostObservabilityService()
    monkeypatch.setattr(type(s._cfg), "is_mock_mode", lambda _self: False)

    rows = {row.label: row for row in s.breakdown("resource", "gcp", days=1).rows}
    assert rows["harsh-static-ip"].is_idle is True
    assert rows["harsh-static-ip"].waste_kind == waste.WASTE_IDLE_STATIC_IP
    assert rows["ikenna-windows-tokyo-restored"].is_idle is True
    assert rows["ikenna-windows-tokyo-restored"].waste_kind == waste.WASTE_ORPHANED_DISK
    assert rows["vm-1"].is_idle is False
    assert rows["vm-1"].waste_kind == ""


def test_breakdown_resource_disk_not_flagged_when_attached(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(svc, "gcp_facts", _fake_gcp_with_waste)
    monkeypatch.setattr(svc, "aws_facts", lambda *a, **k: [])
    monkeypatch.setattr(svc, "github_facts", lambda start, end: [])
    # ikenna's disk is attached (absent from the unattached set) → never flagged orphaned.
    monkeypatch.setattr(svc, "list_unattached_disk_names", lambda _project_id: set())
    s = CostObservabilityService()
    monkeypatch.setattr(type(s._cfg), "is_mock_mode", lambda _self: False)

    rows = {row.label: row for row in s.breakdown("resource", "gcp", days=1).rows}
    assert rows["ikenna-windows-tokyo-restored"].is_idle is False


def test_breakdown_resource_surfaces_cheap_waste_below_the_top_n_cap(monkeypatch: pytest.MonkeyPatch) -> None:
    """Cost-waste is cheap by nature, so a plain top-N-by-cost cap would hide it. The idle IP
    must still surface even when far more than _BREAKDOWN_LIMIT pricier resources outrank it."""

    def _many_plus_cheap_waste(_table: str, start: date, end: date, _cutoff: date) -> list[CostRecord]:
        recs: list[CostRecord] = [
            CostRecord(
                cloud="gcp",
                day=start.isoformat(),
                service="Compute Engine",
                resource_id=f"vm-{i}",
                resource_kind="vm",
                region="asia-northeast1",
                cost=100.0 + i,  # every VM is pricier than the idle IP below
                sku="N2 Instance Core running in Japan",
            )
            for i in range(svc._BREAKDOWN_LIMIT + 5)
        ]
        recs.append(
            CostRecord(
                cloud="gcp",
                day=start.isoformat(),
                service="Compute Engine",
                resource_id="harsh-static-ip",
                resource_kind="other",
                region="asia-northeast1",
                cost=2.58,  # cheap — ranks below every VM, so a naive top-N cap would drop it
                sku="Static Ip Charge in Japan",
            )
        )
        return recs

    monkeypatch.setattr(svc, "gcp_facts", _many_plus_cheap_waste)
    monkeypatch.setattr(svc, "aws_facts", lambda *a, **k: [])
    monkeypatch.setattr(svc, "github_facts", lambda start, end: [])
    monkeypatch.setattr(svc, "list_unattached_disk_names", lambda _project_id: set())
    s = CostObservabilityService()
    monkeypatch.setattr(type(s._cfg), "is_mock_mode", lambda _self: False)

    rows = {row.label: row for row in s.breakdown("resource", "gcp", days=1).rows}
    assert "harsh-static-ip" in rows  # surfaced despite ranking below the top-N cost cut
    assert rows["harsh-static-ip"].waste_kind == waste.WASTE_IDLE_STATIC_IP


def test_breakdown_bucket_dimension_never_flags_waste(service: CostObservabilityService) -> None:
    r = service.breakdown("bucket", "all", days=2)
    assert all(row.is_idle is False and row.waste_kind == "" for row in r.rows)


def test_breakdown_bucket_dimension_skips_fleet_lookup(monkeypatch: pytest.MonkeyPatch) -> None:
    calls = {"n": 0}

    def counting_lookup(_project_id: str) -> set[str]:
        calls["n"] += 1
        return set()

    monkeypatch.setattr(svc, "gcp_facts", _fake_gcp)
    monkeypatch.setattr(svc, "aws_facts", _fake_aws)
    monkeypatch.setattr(svc, "github_facts", lambda start, end: [])
    monkeypatch.setattr(svc, "list_unattached_disk_names", counting_lookup)
    s = CostObservabilityService()
    monkeypatch.setattr(type(s._cfg), "is_mock_mode", lambda _self: False)

    s.breakdown("bucket", "all", days=2)
    assert calls["n"] == 0  # bucket dimension never needs the unattached-disk cross-ref


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
    assert row.cost == pytest.approx(0.18, abs=0.001)  # (0.05 storage + 0.01 ops) * 3
    # $/GB is the effective STORAGE rate — storage-SKU cost (0.05 * 3 = 0.15) over stored GB,
    # NOT the row's total cost (0.18, which includes the operations SKU). The operations-inclusive
    # formula would over-read the rate by (0.18/0.15) = 1.2x here, and far worse on a real
    # write-heavy events bucket.
    expected_storage_cost = 0.05 * 3
    assert row.cost_per_gb == pytest.approx(expected_storage_cost / expected_gb, abs=0.0005)


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


def test_rows_without_storage_skus_carry_no_storage_fields(service: CostObservabilityService) -> None:
    # The service fixture's rows (a VM + a bucket with NO storage-volume SKU) carry no gibibyte-month
    # usage, so storage detail stays absent — storage attaches only to bucket rows that actually
    # billed storage volume, never fabricated.
    r = service.breakdown("resource", "all", days=2)
    assert all(row.storage_gb is None and row.storage_class_gb is None for row in r.rows)


def test_resource_dimension_bucket_rows_carry_storage_for_the_leaf_table(monkeypatch: pytest.MonkeyPatch) -> None:
    """The 'Top storage buckets' leaf table is fed by the RESOURCE dimension, so bucket-kind rows
    must carry storage detail there too — not only under the dedicated By-bucket dimension."""
    monkeypatch.setattr(svc, "gcp_facts", _fake_gcp_storage)
    monkeypatch.setattr(svc, "aws_facts", lambda *a, **k: [])
    monkeypatch.setattr(svc, "github_facts", lambda start, end: [])
    monkeypatch.setattr(svc, "list_unattached_disk_names", lambda _project_id: set())
    s = CostObservabilityService()
    monkeypatch.setattr(type(s._cfg), "is_mock_mode", lambda _self: False)

    r = s.breakdown("resource", "gcp", days=3)
    row = next(row for row in r.rows if row.label == "my-bucket")
    assert row.storage_gb is not None and row.storage_gb > 0  # populated in resource dim, not just By-bucket
    assert row.storage_class_gb == {"Coldline": pytest.approx(row.storage_gb, abs=0.01)}


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

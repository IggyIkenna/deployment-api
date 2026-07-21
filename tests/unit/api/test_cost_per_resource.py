"""Unit tests — per-resource daily cost (the deployment inventory cost column, WS-E).

Covers `CostObservabilityService.per_resource_daily` (the 3 USD figures: actual last-complete-day
/ trailing-7d average / projected-24h) and `_attach_costs` (the name == resource_id join onto the
inventory items). The billing window is patched — no cloud calls.
"""

from __future__ import annotations

from datetime import UTC, datetime, timedelta
from unittest.mock import patch

from deployment_api.routes import deployments_inventory as inv
from deployment_api.routes.deployments_inventory import DeploymentItem
from deployment_api.services.cost_observability import CostObservabilityService, CostRecord
from deployment_api.services.cost_observability.models import ResourceDailyCost
from deployment_api.services.cost_observability.snapshot import records_to_table


def _rec(resource_id: str, day: str, cost: float, credit: float = 0.0) -> CostRecord:
    return CostRecord(
        cloud="gcp",
        day=day,
        service="Compute Engine",
        resource_id=resource_id,
        resource_kind="vm",
        region="us-central1",
        cost=cost,
        credit=credit,
    )


def _day(n: int) -> str:
    return (datetime.now(UTC).date() - timedelta(days=n)).isoformat()


def test_per_resource_daily_three_values() -> None:
    # vm-a ran 3 days: $10 two days ago, $20 yesterday, $5 partial today.
    recs = [
        _rec("vm-a", _day(2), 10.0),
        _rec("vm-a", _day(1), 20.0),
        _rec("vm-a", _day(0), 5.0),  # today (partial) — excluded from "actual"
        _rec("vm-b", _day(1), 7.0),
    ]
    svc = CostObservabilityService()
    with patch.object(svc, "_window_table", return_value=records_to_table(recs)):
        out = svc.per_resource_daily(days=7)

    a = out["vm-a"]
    assert a.actual_usd == 20.0  # most recent COMPLETE day (yesterday), not today's partial $5
    assert a.avg_7d_usd == round(
        (10.0 + 20.0 + 5.0) / 3, 2
    )  # total net ÷ days actually billed (3), not the 7-day window
    assert a.projected_24h_usd == 20.0  # peak observed daily (≈ a full 24h day)
    assert out["vm-b"].actual_usd == 7.0


def test_per_resource_daily_net_of_credits() -> None:
    recs = [_rec("vm-c", _day(1), 30.0, credit=-12.0)]  # net = 18
    svc = CostObservabilityService()
    with patch.object(svc, "_window_table", return_value=records_to_table(recs)):
        out = svc.per_resource_daily(days=7)
    assert out["vm-c"].actual_usd == 18.0


def test_per_resource_daily_skips_rows_without_resource_id() -> None:
    recs = [_rec("", _day(1), 99.0)]  # no resource granularity → not attributable
    svc = CostObservabilityService()
    with patch.object(svc, "_window_table", return_value=records_to_table(recs)):
        out = svc.per_resource_daily(days=7)
    assert out == {}


def test_attach_costs_joins_by_name_and_degrades_honestly() -> None:
    items = [
        DeploymentItem(
            name="vm-a", kind="VM", umbrella="LIVE", cloud="GCP", service="s", asset_group="", status="running"
        ),
        DeploymentItem(
            name="vm-unbilled",
            kind="VM",
            umbrella="BATCH",
            cloud="AWS",
            service="s",
            asset_group="",
            status="succeeded",
        ),
    ]
    fake = {"vm-a": ResourceDailyCost(actual_usd=20.0, avg_7d_usd=5.0, projected_24h_usd=22.0)}
    with patch.object(inv.CostObservabilityService, "per_resource_daily", return_value=fake):
        inv._attach_costs(items)  # pyright: ignore[reportPrivateUsage]

    assert items[0].cost_actual_usd == 20.0
    assert items[0].cost_avg_7d_usd == 5.0
    assert items[0].cost_projected_24h_usd == 22.0
    # No billing row → honest None, never a fabricated 0.
    assert items[1].cost_actual_usd is None


def test_attach_costs_never_raises_on_billing_failure() -> None:
    items = [
        DeploymentItem(
            name="vm-a", kind="VM", umbrella="LIVE", cloud="GCP", service="s", asset_group="", status="running"
        ),
    ]
    with patch.object(inv.CostObservabilityService, "per_resource_daily", side_effect=RuntimeError("bq down")):
        inv._attach_costs(items)  # must not raise — cost is best-effort
    assert items[0].cost_actual_usd is None

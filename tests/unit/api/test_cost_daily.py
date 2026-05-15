"""Unit tests for cost_daily.py — GET /api/costs/daily."""

from __future__ import annotations

import json
from collections.abc import Generator
from unittest.mock import MagicMock, patch

import pytest
from fastapi import FastAPI
from fastapi.testclient import TestClient
from unified_trading_library import setup_events

from deployment_api.routes.cost_daily import VmCostRow, _aggregate, _parse_blob, router

setup_events("deployment-api", "test")

_PATCH_MOCK = "deployment_api.deployment_api_config.DeploymentApiConfig.is_mock_mode"


@pytest.fixture
def mock_client() -> Generator[TestClient]:
    with patch(_PATCH_MOCK, return_value=True):
        app = FastAPI()
        app.include_router(router)
        yield TestClient(app)


@pytest.fixture
def prod_client() -> Generator[TestClient]:
    with patch(_PATCH_MOCK, return_value=False):
        app = FastAPI()
        app.include_router(router)
        yield TestClient(app)


class TestMockMode:
    def test_returns_200(self, mock_client: TestClient) -> None:
        r = mock_client.get("/costs/daily?date=2026-05-15")
        assert r.status_code == 200

    def test_has_required_fields(self, mock_client: TestClient) -> None:
        r = mock_client.get("/costs/daily?date=2026-05-15")
        body = r.json()
        assert "date" in body
        assert "total_usd" in body
        assert "by_asset_group" in body
        assert "by_archetype" in body
        assert "by_vm" in body

    def test_date_is_echoed(self, mock_client: TestClient) -> None:
        r = mock_client.get("/costs/daily?date=2026-05-15")
        assert r.json()["date"] == "2026-05-15"

    def test_total_equals_sum_of_vms(self, mock_client: TestClient) -> None:
        r = mock_client.get("/costs/daily?date=2026-05-15")
        body = r.json()
        vm_sum = sum(v["total_usd"] for v in body["by_vm"])
        assert abs(body["total_usd"] - vm_sum) < 0.001

    def test_by_asset_group_sums_correctly(self, mock_client: TestClient) -> None:
        r = mock_client.get("/costs/daily?date=2026-05-15")
        body = r.json()
        ag_sum = sum(ag["total_usd"] for ag in body["by_asset_group"])
        assert abs(body["total_usd"] - ag_sum) < 0.001

    def test_bad_date_returns_400(self, mock_client: TestClient) -> None:
        r = mock_client.get("/costs/daily?date=not-a-date")
        assert r.status_code == 400


class TestParseBlobUnit:
    def test_valid_blob_parses(self) -> None:
        row = {
            "vm_name": "cefi-backfill-20260515",
            "asset_group": "cefi",
            "archetype": "carry_staked_basis",
            "machine_type": "n1-standard-4",
            "runtime_hours": 2.0,
            "compute_usd": 0.38,
            "disk_usd": 0.01,
            "total_usd": 0.39,
        }
        result = _parse_blob(json.dumps(row).encode(), "test.jsonl")
        assert result is not None
        assert result.vm_name == "cefi-backfill-20260515"
        assert result.asset_group == "cefi"

    def test_empty_blob_returns_none(self) -> None:
        result = _parse_blob(b"", "empty.jsonl")
        assert result is None

    def test_invalid_json_returns_none(self) -> None:
        result = _parse_blob(b"not json", "bad.jsonl")
        assert result is None


class TestAggregateUnit:
    def _row(self, ag: str, arch: str, total: float) -> VmCostRow:
        return VmCostRow(
            vm_name=f"{ag}-vm",
            asset_group=ag,
            archetype=arch,
            machine_type="n1-standard-4",
            runtime_hours=1.0,
            compute_usd=total * 0.9,
            disk_usd=total * 0.1,
            total_usd=total,
        )

    def test_empty_rows_returns_zero(self) -> None:
        result = _aggregate([], "2026-05-15")
        assert result.total_usd == 0.0
        assert result.by_asset_group == []
        assert result.by_vm == []

    def test_single_row_aggregates_correctly(self) -> None:
        rows = [self._row("cefi", "carry_staked_basis", 1.5)]
        result = _aggregate(rows, "2026-05-15")
        assert result.total_usd == 1.5
        assert len(result.by_asset_group) == 1
        assert result.by_asset_group[0].asset_group == "cefi"
        assert result.by_asset_group[0].vm_count == 1

    def test_two_groups_aggregated_separately(self) -> None:
        rows = [
            self._row("cefi", "carry_staked_basis", 1.0),
            self._row("defi", "arbitrage_price_dispersion", 2.0),
        ]
        result = _aggregate(rows, "2026-05-15")
        assert result.total_usd == 3.0
        assert len(result.by_asset_group) == 2
        groups = {ag.asset_group: ag for ag in result.by_asset_group}
        assert groups["cefi"].total_usd == 1.0
        assert groups["defi"].total_usd == 2.0

    def test_by_vm_sorted_descending(self) -> None:
        rows = [
            self._row("cefi", "carry_staked_basis", 1.0),
            self._row("defi", "arbitrage_price_dispersion", 5.0),
            self._row("tradfi", "carry_staked_basis", 3.0),
        ]
        result = _aggregate(rows, "2026-05-15")
        totals = [r.total_usd for r in result.by_vm]
        assert totals == sorted(totals, reverse=True)


class TestProdModeNoBlobs:
    def test_returns_zero_when_no_blobs(self, prod_client: TestClient) -> None:
        mock_storage = MagicMock()
        mock_storage.list_blobs.return_value = []
        with patch(
            "deployment_api.routes.cost_daily.get_storage_client", return_value=mock_storage
        ):
            r = prod_client.get("/costs/daily?date=2026-05-15")
        assert r.status_code == 200
        assert r.json()["total_usd"] == 0.0

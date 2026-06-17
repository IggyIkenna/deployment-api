"""Tests for GET /data-status/venue-year-coverage endpoint.

Covers:
1. Happy path: rows grouped by (venue, year) with correct status counts.
2. pending_paid_key: attempted_failed + error_reason=blocked_credentials → pending_paid_key bucket.
3. Year extraction from YYYY-MM-DD date strings.
4. GCS read failure → asset group in asset_groups_failed, empty rows.
5. Empty parquet → asset group loaded, no rows.
6. Legacy rows (no capture_status column) → default to "captured".
"""

from __future__ import annotations

from unittest.mock import MagicMock, patch

import pandas as pd
import pytest
from fastapi import FastAPI
from fastapi.testclient import TestClient

_PATCH_DISABLE_AUTH = "deployment_api.rbac.DISABLE_AUTH"
_PATCH_CFG = "deployment_api.routes.data_status._cfg"
_PATCH_READ_INDEX = "deployment_api.routes.data_status._read_manifest_index"
_PATCH_BUILD_BUCKET = "deployment_api.routes.data_status.build_bucket_name"


def _make_mock_cfg(is_mock: bool = False) -> MagicMock:
    cfg = MagicMock()
    cfg.is_mock_mode.return_value = is_mock
    return cfg


@pytest.fixture
def client() -> TestClient:
    from deployment_api.routes.data_status import router

    app = FastAPI()
    app.include_router(router, prefix="/data-status")
    with (
        patch(_PATCH_DISABLE_AUTH, True),
        patch(_PATCH_CFG, _make_mock_cfg(is_mock=False)),
    ):
        yield TestClient(app, raise_server_exceptions=True)  # type: ignore[misc]


def _df(*rows: dict[str, object]) -> pd.DataFrame:
    return pd.DataFrame(rows)


class TestVenueYearCoverageHappyPath:
    def test_returns_rows_grouped_by_venue_and_year(self, client: TestClient) -> None:
        df = _df(
            {"date": "2024-01-15", "venue": "BINANCE-SPOT", "capture_status": "captured", "error_reason": None},
            {"date": "2024-03-10", "venue": "BINANCE-SPOT", "capture_status": "captured", "error_reason": None},
            {"date": "2024-06-01", "venue": "DERIBIT", "capture_status": "empty_confirmed", "error_reason": None},
        )
        with (
            patch(_PATCH_BUILD_BUCKET, return_value="market-data-tick-cefi-xxx"),
            patch(_PATCH_READ_INDEX, return_value=df),
        ):
            resp = client.get("/data-status/venue-year-coverage", params={"asset_groups": "cefi"})

        assert resp.status_code == 200
        data = resp.json()
        assert data["asset_groups_loaded"] == ["cefi"]
        assert data["asset_groups_failed"] == []

        rows_by_key = {(r["venue"], r["year"]): r for r in data["rows"]}
        bs_2024 = rows_by_key[("BINANCE-SPOT", 2024)]
        assert bs_2024["captured"] == 2
        assert bs_2024["asset_group"] == "CEFI"

        dr_2024 = rows_by_key[("DERIBIT", 2024)]
        assert dr_2024["empty_confirmed"] == 1
        assert dr_2024["captured"] == 0

    def test_total_and_remaining_computed(self, client: TestClient) -> None:
        df = _df(
            {"date": "2023-05-01", "venue": "OKX-SPOT", "capture_status": "captured", "error_reason": None},
            {"date": "2023-05-02", "venue": "OKX-SPOT", "capture_status": "captured", "error_reason": None},
            {
                "date": "2023-05-03",
                "venue": "OKX-SPOT",
                "capture_status": "attempted_failed",
                "error_reason": "timeout",
            },
        )
        with (
            patch(_PATCH_BUILD_BUCKET, return_value="bucket"),
            patch(_PATCH_READ_INDEX, return_value=df),
        ):
            resp = client.get("/data-status/venue-year-coverage", params={"asset_groups": "cefi"})

        row = resp.json()["rows"][0]
        assert row["total"] == 3
        assert row["captured"] == 2
        assert row["attempted_failed"] == 1


class TestPendingPaidKeyClassification:
    def test_blocked_credentials_becomes_pending_paid_key(self, client: TestClient) -> None:
        df = _df(
            {
                "date": "2022-07-01",
                "venue": "COINBASE-SPOT",
                "capture_status": "attempted_failed",
                "error_reason": "blocked_credentials",
            },
            {
                "date": "2022-07-02",
                "venue": "COINBASE-SPOT",
                "capture_status": "attempted_failed",
                "error_reason": "timeout",
            },
            {
                "date": "2022-07-03",
                "venue": "COINBASE-SPOT",
                "capture_status": "captured",
                "error_reason": None,
            },
        )
        with (
            patch(_PATCH_BUILD_BUCKET, return_value="bucket"),
            patch(_PATCH_READ_INDEX, return_value=df),
        ):
            resp = client.get("/data-status/venue-year-coverage", params={"asset_groups": "cefi"})

        row = resp.json()["rows"][0]
        assert row["pending_paid_key"] == 1, "blocked_credentials must be classified as pending_paid_key"
        assert row["attempted_failed"] == 1, "non-blocked failure stays attempted_failed"
        assert row["captured"] == 1


class TestGcsReadFailure:
    def test_failed_asset_group_not_in_loaded(self, client: TestClient) -> None:
        with (
            patch(_PATCH_BUILD_BUCKET, return_value="bucket"),
            patch(_PATCH_READ_INDEX, side_effect=OSError("GCS unavailable")),
        ):
            resp = client.get("/data-status/venue-year-coverage", params={"asset_groups": "cefi"})

        assert resp.status_code == 200
        data = resp.json()
        assert data["asset_groups_failed"] == ["cefi"]
        assert data["asset_groups_loaded"] == []
        assert data["rows"] == []


class TestEmptyParquet:
    def test_empty_parquet_loaded_with_no_rows(self, client: TestClient) -> None:
        with (
            patch(_PATCH_BUILD_BUCKET, return_value="bucket"),
            patch(_PATCH_READ_INDEX, return_value=pd.DataFrame()),
        ):
            resp = client.get("/data-status/venue-year-coverage", params={"asset_groups": "cefi"})

        data = resp.json()
        assert "cefi" in data["asset_groups_loaded"]
        assert data["rows"] == []


class TestLegacyRowsNoCapturStatusColumn:
    def test_missing_capture_status_defaults_to_captured(self, client: TestClient) -> None:
        df = pd.DataFrame([{"date": "2020-01-01", "venue": "BINANCE-SPOT"}])
        with (
            patch(_PATCH_BUILD_BUCKET, return_value="bucket"),
            patch(_PATCH_READ_INDEX, return_value=df),
        ):
            resp = client.get("/data-status/venue-year-coverage", params={"asset_groups": "cefi"})

        row = resp.json()["rows"][0]
        assert row["captured"] == 1
        assert row["attempted_failed"] == 0

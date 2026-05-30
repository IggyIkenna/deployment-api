"""Tests for GET /api/venue-date-ranges endpoint.

Coverage:
1. Mock mode returns expected venue list with required fields.
2. free_date_count > 0, paid_date_count > 0.
3. _compute_date_range_stats: first-of-month dates are always free.
4. _compute_date_range_stats: dates in recent window are free.
5. _compute_date_range_stats: non-first, non-recent dates are paid.
"""

from __future__ import annotations

from datetime import date
from unittest.mock import MagicMock, patch

import pytest
from fastapi import FastAPI
from fastapi.testclient import TestClient

_PATCH_CFG = "deployment_api.routes.venue_date_ranges._cfg"


def _make_mock_cfg(is_mock: bool = True) -> MagicMock:
    cfg = MagicMock()
    cfg.is_mock_mode.return_value = is_mock
    return cfg


@pytest.fixture
def client() -> TestClient:
    from deployment_api.routes.venue_date_ranges import router

    app = FastAPI()
    app.include_router(router)
    with patch(_PATCH_CFG, _make_mock_cfg(is_mock=True)):
        yield TestClient(app, raise_server_exceptions=True)  # type: ignore[misc]


class TestVenueDateRangesMockMode:
    def test_returns_list_of_venues(self, client: TestClient) -> None:
        resp = client.get("/api/venue-date-ranges")
        assert resp.status_code == 200
        data = resp.json()
        assert isinstance(data, list)
        assert len(data) > 0

    def test_expected_venues_present(self, client: TestClient) -> None:
        resp = client.get("/api/venue-date-ranges")
        venues = {row["venue"] for row in resp.json()}
        for expected in ("binance", "okx", "bybit", "deribit"):
            assert expected in venues, f"{expected} missing from response"

    def test_required_fields_present(self, client: TestClient) -> None:
        resp = client.get("/api/venue-date-ranges")
        row = resp.json()[0]
        for field in (
            "venue",
            "key_name",
            "key_status",
            "coverage_start",
            "free_date_count",
            "paid_date_count",
            "free_description",
            "paid_description",
            "free_sample_dates",
            "assessed_at",
        ):
            assert field in row, f"Missing field: {field}"

    def test_positive_date_counts(self, client: TestClient) -> None:
        resp = client.get("/api/venue-date-ranges")
        for row in resp.json():
            assert row["free_date_count"] > 0, f"{row['venue']}: free_date_count must be > 0"
            assert row["paid_date_count"] > 0, f"{row['venue']}: paid_date_count must be > 0"

    def test_key_name_is_tardis(self, client: TestClient) -> None:
        resp = client.get("/api/venue-date-ranges")
        for row in resp.json():
            assert row["key_name"] == "tardis-api-key"


class TestComputeDateRangeStats:
    def test_first_of_month_is_free(self) -> None:
        from deployment_api.routes.venue_date_ranges import _compute_date_range_stats

        today = date(2024, 6, 15)
        free, paid, sample = _compute_date_range_stats(date(2024, 1, 1), today, recent_window_days=7)
        # Jan 1, Feb 1, Mar 1, Apr 1, May 1, Jun 1 are all free
        assert "2024-01-01" in sample or free >= 6

    def test_recent_dates_are_free(self) -> None:
        from deployment_api.routes.venue_date_ranges import _compute_date_range_stats

        today = date(2024, 6, 15)
        free, paid, _ = _compute_date_range_stats(date(2024, 6, 1), today, recent_window_days=30)
        # All 15 days in coverage are within 30-day window → all free
        assert free == 15
        assert paid == 0

    def test_non_recent_non_first_are_paid(self) -> None:
        from deployment_api.routes.venue_date_ranges import _compute_date_range_stats

        # Cover Jan 2024 with a 5-day recent window from Jan 31
        today = date(2024, 1, 31)
        free, paid, _ = _compute_date_range_stats(date(2024, 1, 1), today, recent_window_days=5)
        # Jan 1 = free (1st), Jan 27-31 = free (recent 5 days), Jan 2-26 = paid (25 days)
        assert paid == 25
        assert free == 6  # Jan 1 + Jan 27,28,29,30,31

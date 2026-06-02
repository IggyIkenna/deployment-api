"""Tests for GET /api/venue-relaunch-estimate endpoint.

Coverage:
1. Mock mode returns rows + summary with required fields.
2. est_after_renewal == pending_total for every row.
3. est_now_unlockable <= est_after_renewal (never overshoots).
4. _free_pct_for_year: full historical year ≈ 3.3% (12/365).
5. _free_pct_for_year: partial year with all dates in recent window → 100%.
6. _build_rows: rows with pending=0 are excluded.
7. Summary totals are consistent with row-level values.
"""

from __future__ import annotations

from datetime import date
from unittest.mock import MagicMock, patch

import pytest
from fastapi import FastAPI
from fastapi.testclient import TestClient

_PATCH_CFG = "deployment_api.routes.venue_relaunch_estimate._cfg"


def _make_mock_cfg(is_mock: bool = True) -> MagicMock:
    cfg = MagicMock()
    cfg.is_mock_mode.return_value = is_mock
    return cfg


@pytest.fixture
def client() -> TestClient:
    from deployment_api.routes.venue_relaunch_estimate import router

    app = FastAPI()
    app.include_router(router)
    with patch(_PATCH_CFG, _make_mock_cfg(is_mock=True)):
        yield TestClient(app, raise_server_exceptions=True)  # type: ignore[misc]


class TestVenueRelaunchEstimateMockMode:
    def test_returns_rows_and_summary(self, client: TestClient) -> None:
        resp = client.get("/api/venue-relaunch-estimate")
        assert resp.status_code == 200
        data = resp.json()
        assert "rows" in data
        assert "summary" in data
        assert len(data["rows"]) > 0

    def test_required_row_fields(self, client: TestClient) -> None:
        resp = client.get("/api/venue-relaunch-estimate")
        row = resp.json()["rows"][0]
        for field in (
            "venue",
            "asset_group",
            "year",
            "pending_total",
            "est_now_unlockable",
            "est_after_renewal",
            "free_pct",
        ):
            assert field in row, f"Missing field: {field}"

    def test_after_renewal_equals_pending_total(self, client: TestClient) -> None:
        resp = client.get("/api/venue-relaunch-estimate")
        for row in resp.json()["rows"]:
            assert row["est_after_renewal"] == row["pending_total"], (
                f"{row['venue']}/{row['year']}: after_renewal must equal pending_total"
            )

    def test_now_unlockable_not_greater_than_pending(self, client: TestClient) -> None:
        resp = client.get("/api/venue-relaunch-estimate")
        for row in resp.json()["rows"]:
            assert row["est_now_unlockable"] <= row["pending_total"], (
                f"{row['venue']}/{row['year']}: now_unlockable must not exceed pending_total"
            )

    def test_summary_totals_consistent(self, client: TestClient) -> None:
        resp = client.get("/api/venue-relaunch-estimate")
        data = resp.json()
        rows = data["rows"]
        summary = data["summary"]
        assert summary["total_pending"] == sum(r["pending_total"] for r in rows)
        assert summary["total_after_renewal"] == summary["total_pending"]
        assert summary["total_now_unlockable"] == sum(r["est_now_unlockable"] for r in rows)


class TestFreePctForYear:
    def test_full_historical_year_fraction(self) -> None:
        from deployment_api.routes.venue_relaunch_estimate import _free_pct_for_year

        pct = _free_pct_for_year(2023, date(2026, 5, 30))
        # 2023: 12 first-of-month / 365 ≈ 3.3%
        assert 3.0 <= pct <= 4.0, f"Expected ~3.3%, got {pct}%"

    def test_partial_year_all_recent_is_100(self) -> None:
        from deployment_api.routes.venue_relaunch_estimate import _free_pct_for_year

        # Year "2026" up to 2026-01-10 with a 30-day window from 2026-01-10:
        # All 10 days are within recent window → 100%
        pct = _free_pct_for_year(2026, date(2026, 1, 10), recent_window_days=30)
        assert pct == 100.0, f"Expected 100%, got {pct}%"

    def test_year_after_today_returns_zero(self) -> None:
        from deployment_api.routes.venue_relaunch_estimate import _free_pct_for_year

        pct = _free_pct_for_year(2030, date(2026, 5, 30))
        assert pct == 0.0, "Year in the future should return 0%"


class TestBuildRows:
    def test_zero_pending_excluded(self) -> None:
        from deployment_api.routes.venue_relaunch_estimate import _build_rows

        raw = [
            ("BINANCE", "cefi", 2024, 0),
            ("OKX", "cefi", 2024, 10),
        ]
        rows, total_pending, _ = _build_rows(raw, date(2026, 5, 30))  # type: ignore[arg-type]
        assert len(rows) == 1
        assert rows[0].venue == "OKX"
        assert total_pending == 10

"""Tests for routes/catalogue_lifecycle.py — the paginated New Listings /
Upcoming Expiries API (plan F10, 2026-07-18).

Both endpoints were previously unbounded (500,000+ row responses) and read
five per-asset-group catalogues serially (~35s/31s cold), exceeding the Cloud
Run / browser fetch timeout -> 500 "Unknown error". These tests verify the
route's pagination contract (``total_count``/``limit``/``offset``/``has_more``)
against a mocked service layer — the service layer's own filtering/tagging
correctness is covered by ``test_catalogue_lifecycle.py``.
"""

from __future__ import annotations

from unittest.mock import MagicMock, patch

import pytest
from fastapi import FastAPI
from fastapi.testclient import TestClient

from deployment_api.services.catalogue_lifecycle import CatalogueLifecycleBuildBusyError, CatalogueLifecycleRow

_PATCH_CFG = "deployment_api.routes.catalogue_lifecycle._cfg"
_PATCH_NEW_LISTINGS_PAGE = "deployment_api.routes.catalogue_lifecycle.list_new_listings_page"
_PATCH_EXPIRIES_PAGE = "deployment_api.routes.catalogue_lifecycle.list_upcoming_expiries_page"


def _make_mock_cfg(*, mock_mode: bool = False) -> MagicMock:
    cfg = MagicMock()
    cfg.is_mock_mode.return_value = mock_mode
    return cfg


@pytest.fixture
def client_lifecycle() -> TestClient:
    from deployment_api.routes.catalogue_lifecycle import router

    app = FastAPI()
    app.include_router(router)
    with patch(_PATCH_CFG, _make_mock_cfg()):
        yield TestClient(app, raise_server_exceptions=False)  # type: ignore[misc]


def _row(instrument_id: str) -> CatalogueLifecycleRow:
    return CatalogueLifecycleRow(
        instrument_id=instrument_id,
        instrument_type="SPOT_PAIR",
        asset_group="cefi",
        venue="BINANCE",
        chain="",
        base_asset="BTC",
        raw_symbol="BTCUSDT",
        available_from="2026-07-10",
        available_to="",
        mvp=True,
        available_from_is_venue_first_day=False,
    )


class TestNewListingsPagination:
    def test_page_shape_and_has_more(self, client_lifecycle: TestClient) -> None:
        page = [_row("A"), _row("B")]
        with patch(_PATCH_NEW_LISTINGS_PAGE, return_value=(page, 5)) as mocked:
            resp = client_lifecycle.get("/instruments/new-listings?max_age_days=30&limit=2&offset=0")
        assert resp.status_code == 200
        body = resp.json()
        assert body["new_listings"] == page
        assert body["total_count"] == 5
        assert body["limit"] == 2
        assert body["offset"] == 0
        assert body["has_more"] is True
        mocked.assert_called_once_with(max_age_days=30, asset_group=None, venue=None, limit=2, offset=0)

    def test_last_page_has_more_false(self, client_lifecycle: TestClient) -> None:
        page = [_row("C")]
        with patch(_PATCH_NEW_LISTINGS_PAGE, return_value=(page, 3)):
            resp = client_lifecycle.get("/instruments/new-listings?limit=2&offset=2")
        body = resp.json()
        assert body["has_more"] is False

    def test_asset_group_and_venue_forwarded(self, client_lifecycle: TestClient) -> None:
        with patch(_PATCH_NEW_LISTINGS_PAGE, return_value=([], 0)) as mocked:
            client_lifecycle.get("/instruments/new-listings?asset_group=defi&venue=UNISWAP")
        mocked.assert_called_once_with(max_age_days=30, asset_group="defi", venue="UNISWAP", limit=50, offset=0)

    def test_busy_guard_sheds_load_as_503(self, client_lifecycle: TestClient) -> None:
        """2026-07-31 OOM-kill investigation: the concurrent-build guard must
        surface as a 503 + Retry-After, not a bare 500, so the UI can retry."""
        with patch(_PATCH_NEW_LISTINGS_PAGE, side_effect=CatalogueLifecycleBuildBusyError("at capacity")):
            resp = client_lifecycle.get("/instruments/new-listings")
        assert resp.status_code == 503
        assert resp.headers["retry-after"] == "5"

    def test_mock_mode_returns_empty_page(self) -> None:
        from deployment_api.routes.catalogue_lifecycle import router

        app = FastAPI()
        app.include_router(router)
        with patch(_PATCH_CFG, _make_mock_cfg(mock_mode=True)):
            client = TestClient(app, raise_server_exceptions=False)
            resp = client.get("/instruments/new-listings")
        body = resp.json()
        assert body == {
            "new_listings": [],
            "total_count": 0,
            "limit": 50,
            "offset": 0,
            "has_more": False,
            "mock": True,
        }


class TestUpcomingExpiriesPagination:
    def test_page_shape_and_has_more(self, client_lifecycle: TestClient) -> None:
        page = [_row("FUT-1")]
        with patch(_PATCH_EXPIRIES_PAGE, return_value=(page, 10)) as mocked:
            resp = client_lifecycle.get("/instruments/upcoming-expiries?within_days=5&limit=1&offset=0")
        assert resp.status_code == 200
        body = resp.json()
        assert body["upcoming_expiries"] == page
        assert body["total_count"] == 10
        assert body["has_more"] is True
        mocked.assert_called_once_with(within_days=5, asset_group=None, venue=None, limit=1, offset=0)

    def test_busy_guard_sheds_load_as_503(self, client_lifecycle: TestClient) -> None:
        with patch(_PATCH_EXPIRIES_PAGE, side_effect=CatalogueLifecycleBuildBusyError("at capacity")):
            resp = client_lifecycle.get("/instruments/upcoming-expiries")
        assert resp.status_code == 503
        assert resp.headers["retry-after"] == "5"

    def test_mock_mode_returns_empty_page(self) -> None:
        from deployment_api.routes.catalogue_lifecycle import router

        app = FastAPI()
        app.include_router(router)
        with patch(_PATCH_CFG, _make_mock_cfg(mock_mode=True)):
            client = TestClient(app, raise_server_exceptions=False)
            resp = client.get("/instruments/upcoming-expiries")
        body = resp.json()
        assert body == {
            "upcoming_expiries": [],
            "total_count": 0,
            "limit": 50,
            "offset": 0,
            "has_more": False,
            "mock": True,
        }

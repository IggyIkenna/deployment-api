"""Unit tests for GET /api/data-status/recursive-borrow-coverage — Phase 11."""

from __future__ import annotations

import os

os.environ.setdefault("CLOUD_MOCK_MODE", "true")
os.environ.setdefault("CLOUD_PROVIDER", "local")
os.environ.setdefault("GCP_PROJECT_ID", "test-project")
os.environ.setdefault("DISABLE_AUTH", "true")

from unittest.mock import patch

import pytest

with (
    patch("unified_trading_library.event_sink.PubSubEventSink"),
    patch("unified_trading_library.PubSubEventSink"),
    patch("unified_trading_library.events.setup_events"),
    patch("unified_trading_library.utils.tracing.setup_tracing"),
    patch("unified_trading_library.setup_tracing"),
):
    from fastapi.testclient import TestClient

    from deployment_api.main import app
    from deployment_api.routes.recursive_borrow_coverage import _cache, _mock_cells

from unified_trading_library import setup_events

setup_events("deployment-api", "test")

_client = TestClient(app)

_ENDPOINT = "/api/data-status/recursive-borrow-coverage"


@pytest.fixture(autouse=True)
def _clear_route_cache() -> None:
    _cache.clear()


class TestRecursiveBorrowCoverageEndpoint:
    def test_returns_200(self) -> None:
        resp = _client.get(_ENDPOINT)
        assert resp.status_code == 200

    def test_response_has_17_cells(self) -> None:
        data = _client.get(_ENDPOINT).json()
        assert len(data["cells"]) == 17

    def test_7_family1_cells(self) -> None:
        cells = _client.get(_ENDPOINT).json()["cells"]
        f1 = [c for c in cells if c["family"] == "lending-only"]
        assert len(f1) == 7

    def test_10_family2_cells(self) -> None:
        cells = _client.get(_ENDPOINT).json()["cells"]
        f2 = [c for c in cells if c["family"] == "perp-hedged"]
        assert len(f2) == 10

    def test_family2_has_hyperliquid_and_bybit(self) -> None:
        cells = _client.get(_ENDPOINT).json()["cells"]
        venues = {c["perp_venue"] for c in cells if c["family"] == "perp-hedged"}
        assert "hyperliquid" in venues
        assert "bybit" in venues

    def test_summary_total_cells_17(self) -> None:
        summary = _client.get(_ENDPOINT).json()["summary"]
        assert summary["total_cells"] == 17

    def test_summary_live_ready_zero_in_mock(self) -> None:
        summary = _client.get(_ENDPOINT).json()["summary"]
        assert summary["live_ready"] == 0

    def test_cache_ttl_seconds_present(self) -> None:
        data = _client.get(_ENDPOINT).json()
        assert data["cache_ttl_seconds"] == 60

    def test_generated_at_present(self) -> None:
        data = _client.get(_ENDPOINT).json()
        assert "generated_at" in data
        assert data["generated_at"]

    def test_cell_status_is_design_ready_in_mock(self) -> None:
        cells = _client.get(_ENDPOINT).json()["cells"]
        statuses = {c["cell_status"] for c in cells}
        assert statuses == {"design-ready"}


class TestMockCells:
    def test_mock_cells_returns_17(self) -> None:
        assert len(_mock_cells()) == 17

    def test_family1_has_no_perp_venue(self) -> None:
        f1 = [c for c in _mock_cells() if c.family == "lending-only"]
        assert all(c.perp_venue is None for c in f1)

    def test_family2_has_perp_venue(self) -> None:
        f2 = [c for c in _mock_cells() if c.family == "perp-hedged"]
        assert all(c.perp_venue is not None for c in f2)

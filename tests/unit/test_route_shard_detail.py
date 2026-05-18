"""Unit tests for routes/shard_detail.py — mocked service calls + error handling."""

from __future__ import annotations

from unittest.mock import MagicMock, patch

import pytest
from fastapi import FastAPI
from fastapi.testclient import TestClient
from unified_trading_library import setup_events

setup_events("deployment-api", "test")

_PATCH_DISABLE_AUTH = "deployment_api.rbac.DISABLE_AUTH"
_PATCH_GET_SHARD_DETAIL = "deployment_api.routes.shard_detail.get_shard_detail"
_PATCH_GET_LEAF_STATS = "deployment_api.routes.shard_detail.get_leaf_parquet_stats"
_PATCH_FETCH_VENUE_DETAIL = "deployment_api.routes.shard_detail.fetch_venue_detail"


def _make_app() -> FastAPI:
    from deployment_api.routes.shard_detail import router

    app = FastAPI()
    app.include_router(router, prefix="/api/data-status")
    return app


@pytest.fixture
def client_shard() -> TestClient:
    with patch(_PATCH_DISABLE_AUTH, True):
        return TestClient(_make_app(), raise_server_exceptions=False)


# ── shard-detail route ────────────────────────────────────────────────────────


def test_shard_detail_calls_service_and_returns_result(client_shard: TestClient) -> None:
    from deployment_api.types.shard_detail import ShardDetailResponse

    mock_result = MagicMock(spec=ShardDetailResponse)
    with patch(_PATCH_GET_SHARD_DETAIL, return_value=mock_result):
        r = client_shard.get(
            "/api/data-status/shard-detail"
            "?service=instruments-service"
            "&asset_group=CEFI"
            "&instrument_type=spot"
            "&data_type=ohlcv"
            "&day=2024-01-01"
        )
    assert r.status_code == 200


def test_shard_detail_value_error_returns_400(client_shard: TestClient) -> None:
    with patch(_PATCH_GET_SHARD_DETAIL, side_effect=ValueError("bad input")):
        r = client_shard.get(
            "/api/data-status/shard-detail"
            "?service=instruments-service"
            "&asset_group=CEFI"
            "&instrument_type=spot"
            "&data_type=ohlcv"
            "&day=2024-01-01"
        )
    assert r.status_code == 400
    assert "bad input" in r.json()["detail"]


def test_shard_detail_os_error_returns_500(client_shard: TestClient) -> None:
    with patch(_PATCH_GET_SHARD_DETAIL, side_effect=OSError("disk error")):
        r = client_shard.get(
            "/api/data-status/shard-detail"
            "?service=instruments-service"
            "&asset_group=CEFI"
            "&instrument_type=spot"
            "&data_type=ohlcv"
            "&day=2024-01-01"
        )
    assert r.status_code == 500


def test_shard_detail_missing_required_param_returns_422(client_shard: TestClient) -> None:
    r = client_shard.get("/api/data-status/shard-detail?service=instruments-service")
    assert r.status_code == 422


# ── leaf-stats route ──────────────────────────────────────────────────────────


def test_leaf_stats_calls_service_and_returns_result(client_shard: TestClient) -> None:
    from deployment_api.types.shard_detail import LeafParquetStats

    mock_result = MagicMock(spec=LeafParquetStats)
    with patch(_PATCH_GET_LEAF_STATS, return_value=mock_result):
        r = client_shard.get(
            "/api/data-status/leaf-stats"
            "?service=instruments-service"
            "&asset_group=CEFI"
            "&instrument_type=spot"
            "&data_type=ohlcv"
            "&day=2024-01-01"
        )
    assert r.status_code == 200


def test_leaf_stats_value_error_returns_400(client_shard: TestClient) -> None:
    with patch(_PATCH_GET_LEAF_STATS, side_effect=ValueError("invalid day")):
        r = client_shard.get(
            "/api/data-status/leaf-stats"
            "?service=instruments-service"
            "&asset_group=CEFI"
            "&instrument_type=spot"
            "&data_type=ohlcv"
            "&day=2024-01-01"
        )
    assert r.status_code == 400


def test_leaf_stats_runtime_error_returns_500(client_shard: TestClient) -> None:
    with patch(_PATCH_GET_LEAF_STATS, side_effect=RuntimeError("read error")):
        r = client_shard.get(
            "/api/data-status/leaf-stats"
            "?service=instruments-service"
            "&asset_group=CEFI"
            "&instrument_type=spot"
            "&data_type=ohlcv"
            "&day=2024-01-01"
        )
    assert r.status_code == 500


# ── venue-detail route ────────────────────────────────────────────────────────


def test_venue_detail_calls_service_and_returns_result(client_shard: TestClient) -> None:
    from deployment_api.types.shard_detail import VenueDetailResponse

    mock_result = MagicMock(spec=VenueDetailResponse)
    with patch(_PATCH_FETCH_VENUE_DETAIL, return_value=mock_result):
        r = client_shard.get(
            "/api/data-status/venue-detail"
            "?service=instruments-service"
            "&asset_group=DEFI"
            "&venue=uniswap-ethereum"
        )
    assert r.status_code == 200


def test_venue_detail_value_error_returns_400(client_shard: TestClient) -> None:
    with patch(_PATCH_FETCH_VENUE_DETAIL, side_effect=ValueError("unknown venue")):
        r = client_shard.get(
            "/api/data-status/venue-detail"
            "?service=instruments-service"
            "&asset_group=DEFI"
            "&venue=bad-venue"
        )
    assert r.status_code == 400
    assert "unknown venue" in r.json()["detail"]


def test_venue_detail_os_error_returns_500(client_shard: TestClient) -> None:
    with patch(_PATCH_FETCH_VENUE_DETAIL, side_effect=OSError("gcs error")):
        r = client_shard.get(
            "/api/data-status/venue-detail"
            "?service=instruments-service"
            "&asset_group=DEFI"
            "&venue=uniswap-ethereum"
        )
    assert r.status_code == 500

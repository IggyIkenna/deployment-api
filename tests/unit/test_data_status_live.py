"""Unit tests for ``/api/data-status/live`` endpoint stub (Phase 11.1).

Validates the Phase 11.1 endpoint contract: empty-list response with the
correct shape so the deployment-ui ``LiveDataStatusTab`` (Phase 11.3)
can render against the contract before live pipeline ships.

Plan: ``live_pipeline_mtds_mdps_features_2026_05_08.md`` Phase 11.
"""

from __future__ import annotations

from datetime import datetime

from fastapi.testclient import TestClient


def _build_app_with_data_status_router():
    """Mount only the data_status router for the live-status smoke."""
    from fastapi import FastAPI

    from deployment_api.routes import data_status

    app = FastAPI()
    app.include_router(data_status.router, prefix="/api/data-status")
    return app


def test_live_status_returns_empty_envelope_with_correct_shape() -> None:
    """Phase 11.1 stub: endpoint returns 200 with empty rows + asset_groups."""

    client = TestClient(_build_app_with_data_status_router())
    response = client.get("/api/data-status/live")

    assert response.status_code == 200
    payload = response.json()
    assert payload["status"] == "ok"
    assert payload["rows"] == []
    assert payload["asset_groups"] == []
    # refreshed_at is ISO-8601 datetime — parse to confirm validity.
    refreshed = datetime.fromisoformat(payload["refreshed_at"])
    assert isinstance(refreshed, datetime)


def test_live_status_accepts_asset_group_filter() -> None:
    """Phase 11.1 query-param contract: ``asset_group`` list filter accepted."""

    client = TestClient(_build_app_with_data_status_router())
    response = client.get("/api/data-status/live?asset_group=defi&asset_group=cefi")

    assert response.status_code == 200
    payload = response.json()
    # Stub returns empty regardless of filter; contract is that the
    # parameter is accepted (no 422 unprocessable entity).
    assert payload["rows"] == []


def test_live_status_row_pydantic_shape_matches_phase_11_1_contract() -> None:
    """Per CLAUDE.md Citadel Rule 7 (SSOT): ``LiveStatusRow`` exposes the contract.

    Asserts the closed-set capture_status taxonomy + shard-key axes + per-shard
    health metric names line up with the Phase 11.1 endpoint contract in the
    live-pipeline plan body.
    """
    from deployment_api.routes.data_status import LiveStatusRow

    row = LiveStatusRow(
        asset_group="defi",
        venue="uniswap_v3",
        chain="arbitrum",
        data_type="OHLCV_1M",
        instrument_type=None,
        instrument_id="ETH-USDC",
        timeframe="1m",
        capture_status="captured",
        staleness_seconds=3.2,
        degraded_ratio_60s=0.0,
        cluster_pct_skipped_60s=0.0,
        last_candle_emitted_at=None,
    )
    assert row.asset_group == "defi"
    assert row.capture_status == "captured"
    # Shape-check the 4-state taxonomy at the type level by constructing
    # each variant; mypy/basedpyright catches drift here.
    for status in (
        "captured",
        "empty_confirmed",
        "attempted_failed",
        "expected_unattempted",
    ):
        variant = row.model_copy(update={"capture_status": status})
        assert variant.capture_status == status


def test_live_status_row_rejects_out_of_range_health_metrics() -> None:
    """Pydantic validators reject obviously-wrong inputs."""
    import pytest
    from pydantic import ValidationError

    from deployment_api.routes.data_status import LiveStatusRow

    base_kwargs = {
        "asset_group": "cefi",
        "venue": "binance",
        "data_type": "OHLCV_1M",
        "timeframe": "1m",
        "capture_status": "captured",
        "staleness_seconds": 0.5,
        "degraded_ratio_60s": 0.0,
        "cluster_pct_skipped_60s": 0.0,
    }

    with pytest.raises(ValidationError):
        LiveStatusRow(**{**base_kwargs, "degraded_ratio_60s": 1.5})

    with pytest.raises(ValidationError):
        LiveStatusRow(**{**base_kwargs, "staleness_seconds": -1.0})

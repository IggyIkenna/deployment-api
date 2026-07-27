"""Unit tests for ``/api/data-status/live`` endpoint (Phase 11.1 real wiring).

Promoted 2026-05-11 from stub-only smoke to manifest-derived contract:
the endpoint reads the availability manifest, filters to the live
pipeline-mode family (any ``pipeline_mode`` whose STRING begins with
``live`` — every ``live_<source>`` value such as ``live_binance``, plus
the legacy alias string), builds :class:`LiveStatusRow` per shard.
Health-API HTTP join still deferred (depends on per-service URL
registry); manifest-derived staleness is the implemented signal.

Plan: ``live_pipeline_mtds_mdps_features_2026_05_08.md`` Phase 11.1.
"""

from __future__ import annotations

from datetime import UTC, datetime, timedelta
from unittest.mock import patch

import pandas as pd
import pytest
from fastapi import FastAPI
from fastapi.testclient import TestClient


def _build_app_with_data_status_router():
    """Mount only the data_status router for the live-status smoke."""
    from deployment_api.routes import data_status

    app = FastAPI()
    app.include_router(data_status.router, prefix="/api/data-status")
    return app


def _mk_manifest_rows(
    *,
    pipeline_modes: list[str],
    venues: list[str] | None = None,
    capture_statuses: list[str] | None = None,
    attempted_ats: list[datetime] | None = None,
) -> pd.DataFrame:
    """Build a synthetic manifest DataFrame matching the v8 schema's
    superset columns (with ``pipeline_mode`` per the v8 plan)."""
    n = len(pipeline_modes)
    venues = venues or ["binance" for _ in range(n)]
    capture_statuses = capture_statuses or ["captured" for _ in range(n)]
    attempted_ats = attempted_ats or [datetime(2026, 5, 11, 12, 0, 0, tzinfo=UTC) for _ in range(n)]
    return pd.DataFrame(
        {
            "pipeline_mode": pipeline_modes,
            "venue": venues,
            "chain": [None] * n,
            "data_type": ["OHLCV_1M"] * n,
            "instrument_type": [None] * n,
            "instrument_id": [f"BTC-USDT-{i}" for i in range(n)],
            "league_id": [None] * n,
            "timeframe": ["1m"] * n,
            "feature_group": [None] * n,
            "capture_status": capture_statuses,
            "attempted_at": attempted_ats,
            "error_reason": [""] * n,
        },
    )


def test_live_status_returns_empty_envelope_when_no_live_shards() -> None:
    """No ``live_<source>`` rows → empty envelope."""

    def _empty_read(bucket, columns=None):
        return _mk_manifest_rows(pipeline_modes=["batch_databento", "batch_tardis"])

    client = TestClient(_build_app_with_data_status_router())
    with patch("unified_trading_library.read_availability_index", side_effect=_empty_read):
        response = client.get("/api/data-status/live")

    assert response.status_code == 200
    payload = response.json()
    assert payload["status"] == "ok"
    assert payload["rows"] == []
    assert payload["asset_groups"] == []


def test_live_status_returns_populated_rows_when_manifest_has_live_shards() -> None:
    """Manifest with ``live_<source>`` rows → populated response."""

    cefi_rows = _mk_manifest_rows(
        pipeline_modes=["live_binance", "live_binance", "batch_databento"],
        venues=["binance", "bybit", "binance"],
    )
    empty_rows = _mk_manifest_rows(pipeline_modes=[])

    def _read_per_bucket(bucket, columns=None):
        if "cefi" in bucket:
            return cefi_rows
        return empty_rows

    client = TestClient(_build_app_with_data_status_router())
    with patch("unified_trading_library.read_availability_index", side_effect=_read_per_bucket):
        response = client.get("/api/data-status/live")

    assert response.status_code == 200
    payload = response.json()
    # 2 live_binance rows in cefi, 1 batch_databento dropped.
    assert len(payload["rows"]) == 2
    assert payload["asset_groups"] == ["cefi"]
    venues_returned = {row["venue"] for row in payload["rows"]}
    assert venues_returned == {"binance", "bybit"}


def test_live_status_captures_live_source_and_legacy_alias_via_prefix() -> None:
    """Regression: the live reader is a STRING-PREFIX match on ``live``, so
    it captures every ``live_<source>`` value AND the legacy transitional
    live-alias string carried by OLD parquets, while dropping ``batch_*``.
    The alias literal is built from a SPLIT string (the enum member is
    deleted; only the STRING survives in old data)."""
    legacy = "live_" + "websocket"
    cefi_rows = _mk_manifest_rows(
        pipeline_modes=["live_binance", legacy, "batch_databento"],
        venues=["binance", "bybit", "binance"],
    )

    def _read(bucket, columns=None):
        if "cefi" in bucket:
            return cefi_rows
        return _mk_manifest_rows(pipeline_modes=[])

    client = TestClient(_build_app_with_data_status_router())
    with patch("unified_trading_library.read_availability_index", side_effect=_read):
        response = client.get("/api/data-status/live?asset_group=cefi")

    assert response.status_code == 200
    payload = response.json()
    # Both the live_<source> row AND the legacy-alias row are captured; the
    # batch_databento row is dropped.
    assert len(payload["rows"]) == 2
    venues_returned = {row["venue"] for row in payload["rows"]}
    assert venues_returned == {"binance", "bybit"}


def test_live_status_filters_by_asset_group_query_param() -> None:
    """Endpoint honours ``?asset_group=`` filter."""

    cefi_rows = _mk_manifest_rows(pipeline_modes=["live_binance"], venues=["binance"])
    defi_rows = _mk_manifest_rows(pipeline_modes=["live_onchain_rpc"], venues=["uniswap_v3"])

    def _read_per_bucket(bucket, columns=None):
        if "cefi" in bucket:
            return cefi_rows
        if "defi" in bucket:
            return defi_rows
        return _mk_manifest_rows(pipeline_modes=[])

    client = TestClient(_build_app_with_data_status_router())
    with patch("unified_trading_library.read_availability_index", side_effect=_read_per_bucket):
        response = client.get("/api/data-status/live?asset_group=defi")

    assert response.status_code == 200
    payload = response.json()
    assert payload["asset_groups"] == ["defi"]
    assert len(payload["rows"]) == 1
    assert payload["rows"][0]["venue"] == "uniswap_v3"


def test_live_status_derives_staleness_from_attempted_at() -> None:
    """``staleness_seconds`` derives from manifest ``attempted_at`` write-time."""

    # Use a 90s-old tz-aware UTC attempted_at so the assertion is deterministic.
    old_attempted = datetime.now(UTC) - timedelta(seconds=90)
    cefi_rows = _mk_manifest_rows(
        pipeline_modes=["live_binance"],
        attempted_ats=[old_attempted],
    )

    def _read(bucket, columns=None):
        if "cefi" in bucket:
            return cefi_rows
        return _mk_manifest_rows(pipeline_modes=[])

    client = TestClient(_build_app_with_data_status_router())
    with patch("unified_trading_library.read_availability_index", side_effect=_read):
        response = client.get("/api/data-status/live?asset_group=cefi")

    payload = response.json()
    row = payload["rows"][0]
    # Allow slop for test-clock skew; staleness should be ≥ 89s and ≤ 95s.
    assert 89 <= row["staleness_seconds"] <= 95


def test_live_status_handles_missing_pipeline_mode_column_gracefully() -> None:
    """Pre-v8 manifest (no ``pipeline_mode`` column) → empty live rows."""

    pre_v8 = pd.DataFrame(
        {
            "venue": ["binance"],
            "data_type": ["OHLCV_1M"],
            "instrument_id": ["BTC-USDT"],
            "timeframe": ["1m"],
            "capture_status": ["captured"],
            # No pipeline_mode column.
        },
    )

    def _read(bucket, columns=None):
        return pre_v8

    client = TestClient(_build_app_with_data_status_router())
    with patch("unified_trading_library.read_availability_index", side_effect=_read):
        response = client.get("/api/data-status/live")

    assert response.status_code == 200
    assert response.json()["rows"] == []


def test_live_status_handles_manifest_read_failure_gracefully() -> None:
    """Manifest read OSError → asset_group dropped + endpoint stays responsive."""

    def _read(bucket, columns=None):
        raise OSError("simulated bucket-not-found")

    client = TestClient(_build_app_with_data_status_router())
    with patch("unified_trading_library.read_availability_index", side_effect=_read):
        response = client.get("/api/data-status/live")

    assert response.status_code == 200
    payload = response.json()
    assert payload["rows"] == []
    assert payload["asset_groups"] == []


def test_live_status_capture_status_preserves_4_state_taxonomy() -> None:
    """All 4 writegate Phase 3.D.5 capture_status values pass through."""

    cefi_rows = _mk_manifest_rows(
        pipeline_modes=["live_binance"] * 4,
        capture_statuses=[
            "captured",
            "empty_confirmed",
            "attempted_failed",
            "expected_unattempted",
        ],
    )

    def _read(bucket, columns=None):
        if "cefi" in bucket:
            return cefi_rows
        return _mk_manifest_rows(pipeline_modes=[])

    client = TestClient(_build_app_with_data_status_router())
    with patch("unified_trading_library.read_availability_index", side_effect=_read):
        response = client.get("/api/data-status/live?asset_group=cefi")

    payload = response.json()
    statuses = {row["capture_status"] for row in payload["rows"]}
    assert statuses == {
        "captured",
        "empty_confirmed",
        "attempted_failed",
        "expected_unattempted",
    }


def test_live_status_aggregates_across_multiple_asset_groups() -> None:
    """When the filter is omitted, the endpoint reads ALL 5 asset_groups."""

    cefi_rows = _mk_manifest_rows(pipeline_modes=["live_binance"], venues=["binance"])
    defi_rows = _mk_manifest_rows(pipeline_modes=["live_onchain_rpc"], venues=["uniswap_v3"])
    tradfi_rows = _mk_manifest_rows(pipeline_modes=["live_databento"], venues=["cme"])

    def _read(bucket, columns=None):
        if "cefi" in bucket:
            return cefi_rows
        if "defi" in bucket:
            return defi_rows
        if "tradfi" in bucket:
            return tradfi_rows
        return _mk_manifest_rows(pipeline_modes=[])

    client = TestClient(_build_app_with_data_status_router())
    with patch("unified_trading_library.read_availability_index", side_effect=_read):
        response = client.get("/api/data-status/live")

    payload = response.json()
    assert sorted(payload["asset_groups"]) == ["cefi", "defi", "tradfi"]
    assert len(payload["rows"]) == 3


def test_live_status_row_pydantic_shape_matches_phase_11_1_contract() -> None:
    """``LiveStatusRow`` exposes the v5/v8 shard-key + 4-state capture_status contract."""
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


def test_live_status_health_api_http_join_overrides_staleness() -> None:
    """When config has service URLs, /health response overrides manifest staleness."""

    cefi_rows = _mk_manifest_rows(
        pipeline_modes=["live_binance"],
        venues=["binance"],
        attempted_ats=[datetime.now(UTC) - timedelta(seconds=300)],  # 5 min stale per manifest
    )

    def _read(bucket, columns=None):
        if "cefi" in bucket:
            return cefi_rows
        return _mk_manifest_rows(pipeline_modes=[])

    async def _fake_get(self, url, **kwargs):
        class _Resp:
            status_code = 200

            def json(self):
                return {
                    "data_freshness": {"last_event_age_seconds": 12.5},
                }

        return _Resp()

    app = _build_app_with_data_status_router()
    from deployment_api.routes import data_status

    # Override the config to register one service URL.
    original_urls = data_status._cfg.live_pipeline_service_urls
    data_status._cfg.live_pipeline_service_urls = {"market-tick-data-service": "http://mtds.local"}
    try:
        client = TestClient(app)
        with (
            patch("unified_trading_library.read_availability_index", side_effect=_read),
            patch("httpx.AsyncClient.get", new=_fake_get),
        ):
            response = client.get("/api/data-status/live?asset_group=cefi")
        assert response.status_code == 200
        row = response.json()["rows"][0]
        # Precise 12.5s from /health overrides the ~300s manifest-derived value.
        assert row["staleness_seconds"] == 12.5
    finally:
        data_status._cfg.live_pipeline_service_urls = original_urls


def test_live_status_health_api_http_failure_falls_back_to_manifest_staleness() -> None:
    """/health call failure → manifest-derived staleness used (no exception bubbles)."""

    cefi_rows = _mk_manifest_rows(
        pipeline_modes=["live_binance"],
        attempted_ats=[datetime.now(UTC) - timedelta(seconds=45)],
    )

    def _read(bucket, columns=None):
        if "cefi" in bucket:
            return cefi_rows
        return _mk_manifest_rows(pipeline_modes=[])

    async def _fake_get(self, url, **kwargs):
        raise httpx.ConnectError("simulated network failure")

    import httpx

    app = _build_app_with_data_status_router()
    from deployment_api.routes import data_status

    original_urls = data_status._cfg.live_pipeline_service_urls
    data_status._cfg.live_pipeline_service_urls = {"market-tick-data-service": "http://down.local"}
    try:
        client = TestClient(app)
        with (
            patch("unified_trading_library.read_availability_index", side_effect=_read),
            patch("httpx.AsyncClient.get", new=_fake_get),
        ):
            response = client.get("/api/data-status/live?asset_group=cefi")
        assert response.status_code == 200
        row = response.json()["rows"][0]
        # Manifest-derived ~45s (allow slop).
        assert 43 <= row["staleness_seconds"] <= 50
    finally:
        data_status._cfg.live_pipeline_service_urls = original_urls


def test_live_status_health_api_empty_registry_uses_manifest_staleness() -> None:
    """Empty URL registry → no HTTP calls; manifest-derived staleness."""

    cefi_rows = _mk_manifest_rows(
        pipeline_modes=["live_binance"],
        attempted_ats=[datetime.now(UTC) - timedelta(seconds=20)],
    )

    def _read(bucket, columns=None):
        if "cefi" in bucket:
            return cefi_rows
        return _mk_manifest_rows(pipeline_modes=[])

    app = _build_app_with_data_status_router()
    from deployment_api.routes import data_status

    original_urls = data_status._cfg.live_pipeline_service_urls
    data_status._cfg.live_pipeline_service_urls = {}
    try:
        client = TestClient(app)
        with patch("unified_trading_library.read_availability_index", side_effect=_read):
            response = client.get("/api/data-status/live?asset_group=cefi")
        assert response.status_code == 200
        row = response.json()["rows"][0]
        # Manifest-derived ~20s.
        assert 18 <= row["staleness_seconds"] <= 25
    finally:
        data_status._cfg.live_pipeline_service_urls = original_urls


def test_live_status_manifest_read_is_column_projected() -> None:
    """Regression for read_availability_index_bare_defi_callers_2026_07_27.md:
    this call site polls EVERY asset_group's (incl. defi's up-to-1.58 GB)
    availability index on every /live request and must never regress to a bare
    (unprojected) read — pins the exact call signature so a future edit can't
    silently drop the projection back to a bare call."""
    from deployment_api.services.manifest_source import DRILLDOWN_COLUMNS

    cefi_rows = _mk_manifest_rows(pipeline_modes=["live_binance"], venues=["binance"])

    def _read(bucket, columns=None):
        if "cefi" in bucket:
            return cefi_rows
        return _mk_manifest_rows(pipeline_modes=[])

    client = TestClient(_build_app_with_data_status_router())
    with patch("unified_trading_library.read_availability_index", side_effect=_read) as mock_read:
        response = client.get("/api/data-status/live?asset_group=cefi")

    assert response.status_code == 200
    mock_read.assert_called_once()
    call_args, call_kwargs = mock_read.call_args
    assert call_kwargs.get("columns") == DRILLDOWN_COLUMNS
    assert "cefi" in call_args[0]


def test_live_status_row_rejects_out_of_range_health_metrics() -> None:
    """Pydantic validators reject obviously-wrong inputs."""
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

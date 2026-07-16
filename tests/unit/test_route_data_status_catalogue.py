"""Tests for routes/data_status/_catalogue.py — the P6 phase-1 availability-derived
instrument catalogue explorer (``GET /catalogue`` + ``/download-catalogue-csv``).

Mirrors ``test_route_data_status_live.py``'s ``client_ds_live`` TestClient
pattern. ``unified_api_contracts.is_mvp`` is patched at its
``_coverage_scope`` import site (the shared ``is_mvp_for_manifest_row`` helper
lives there) so these tests stay independent of the live MVP_SCOPE registry.

Plan: ``data_status_page_ux_and_canonicalisation_2026_07_16.md`` P6.
"""

from __future__ import annotations

from unittest.mock import MagicMock, patch

import pandas as pd
import pytest
from fastapi import FastAPI
from fastapi.testclient import TestClient

_PATCH_DISABLE_AUTH = "deployment_api.rbac.DISABLE_AUTH"
_PATCH_CFG = "deployment_api.routes.data_status._cfg"
_PATCH_BUILD_BUCKET = "deployment_api.routes.data_status.build_bucket_name"
_PATCH_READ_INDEX = "deployment_api.routes.data_status._read_availability_index"
_PATCH_IS_MVP = "deployment_api.routes.data_status._coverage_scope.is_mvp"


def _make_mock_cfg() -> MagicMock:
    cfg = MagicMock()
    cfg.is_mock_mode.return_value = False
    cfg.deployment_env = "dev"
    return cfg


@pytest.fixture
def client_ds_catalogue() -> TestClient:
    from deployment_api.routes.data_status import router

    app = FastAPI()
    app.include_router(router, prefix="/data-status")
    with (
        patch(_PATCH_DISABLE_AUTH, True),
        patch(_PATCH_CFG, _make_mock_cfg()),
    ):
        yield TestClient(app, raise_server_exceptions=False)  # type: ignore[misc]


def _manifest_df() -> pd.DataFrame:
    """Two distinct instruments; BTC has TWO manifest rows (an older
    ``attempted_failed`` re-tried into a newer ``captured``) to exercise the
    written_at-latest-wins de-dup, ETH has one ``captured`` row."""
    return pd.DataFrame(
        {
            "date": ["2025-03-01", "2025-04-01", "2025-04-01"],
            "venue": ["BINANCE-FUTURES", "BINANCE-FUTURES", "BINANCE-FUTURES"],
            "instrument_type": ["PERPETUAL", "PERPETUAL", "PERPETUAL"],
            "data_type": ["trades", "trades", "trades"],
            "instrument_id": ["BTC-USDT-PERP", "BTC-USDT-PERP", "ETH-USDT-PERP"],
            "capture_status": ["attempted_failed", "captured", "captured"],
            "error_reason": ["timeout", "", ""],
            "attempted_at": ["2025-03-01T00:00:00Z", "2025-04-01T00:00:00Z", "2025-04-01T00:00:00Z"],
            "written_at": ["2025-03-01T00:05:00Z", "2025-04-01T00:05:00Z", "2025-04-01T00:05:00Z"],
            "league_id": ["", "", ""],
            "source": ["tardis", "tardis", "massive"],
        }
    )


class TestGetInstrumentCatalogue:
    def test_returns_deduped_rows_latest_written_at_wins(self, client_ds_catalogue: TestClient) -> None:
        with (
            patch(_PATCH_BUILD_BUCKET, return_value="market-data-tick-cefi-prd-fake"),
            patch(_PATCH_READ_INDEX, return_value=_manifest_df()),
            patch(_PATCH_IS_MVP, return_value=True),
        ):
            r = client_ds_catalogue.get(
                "/data-status/catalogue",
                params={"service": "market-tick-data-service", "asset_group": "cefi"},
            )
        assert r.status_code == 200
        body = r.json()
        assert body["total_count"] == 2
        by_id = {inst["instrument_id"]: inst for inst in body["instruments"]}
        assert set(by_id) == {"BTC-USDT-PERP", "ETH-USDT-PERP"}
        # BTC's latest (written_at 04-01) row is "captured", not the stale
        # 03-01 "attempted_failed" — de-dup keeps the most recent state.
        assert by_id["BTC-USDT-PERP"]["capture_status"] == "captured"
        assert body["label"] == "captured instruments (availability-derived)"

    def test_mvp_only_filters_to_mvp_true_rows(self, client_ds_catalogue: TestClient) -> None:
        def _fake_is_mvp(_asset_group, venue, _instrument_type, _data_type, **kwargs):
            return venue == "BINANCE-FUTURES" and kwargs.get("source") == "tardis"

        with (
            patch(_PATCH_BUILD_BUCKET, return_value="market-data-tick-cefi-prd-fake"),
            patch(_PATCH_READ_INDEX, return_value=_manifest_df()),
            patch(_PATCH_IS_MVP, side_effect=_fake_is_mvp),
        ):
            r = client_ds_catalogue.get(
                "/data-status/catalogue",
                params={"service": "market-tick-data-service", "asset_group": "cefi", "mvp_only": "true"},
            )
        assert r.status_code == 200
        body = r.json()
        assert body["mvp_only"] is True
        assert body["total_count"] == 1
        assert body["instruments"][0]["instrument_id"] == "BTC-USDT-PERP"
        assert body["instruments"][0]["is_mvp"] is True

    def test_search_matches_substring(self, client_ds_catalogue: TestClient) -> None:
        with (
            patch(_PATCH_BUILD_BUCKET, return_value="market-data-tick-cefi-prd-fake"),
            patch(_PATCH_READ_INDEX, return_value=_manifest_df()),
            patch(_PATCH_IS_MVP, return_value=False),
        ):
            r = client_ds_catalogue.get(
                "/data-status/catalogue",
                params={"service": "market-tick-data-service", "asset_group": "cefi", "search": "eth"},
            )
        assert r.status_code == 200
        body = r.json()
        assert body["total_count"] == 1
        assert body["instruments"][0]["instrument_id"] == "ETH-USDT-PERP"

    def test_venue_narrow_scopes_result(self, client_ds_catalogue: TestClient) -> None:
        df = pd.concat(
            [
                _manifest_df(),
                pd.DataFrame(
                    {
                        "date": ["2025-04-01"],
                        "venue": ["OKX-SWAP"],
                        "instrument_type": ["PERPETUAL"],
                        "data_type": ["trades"],
                        "instrument_id": ["SOL-USDT-SWAP"],
                        "capture_status": ["captured"],
                        "error_reason": [""],
                        "attempted_at": ["2025-04-01T00:00:00Z"],
                        "written_at": ["2025-04-01T00:05:00Z"],
                        "league_id": [""],
                        "source": ["tardis"],
                    }
                ),
            ],
            ignore_index=True,
        )
        with (
            patch(_PATCH_BUILD_BUCKET, return_value="market-data-tick-cefi-prd-fake"),
            patch(_PATCH_READ_INDEX, return_value=df),
            patch(_PATCH_IS_MVP, return_value=False),
        ):
            r = client_ds_catalogue.get(
                "/data-status/catalogue",
                params={
                    "service": "market-tick-data-service",
                    "asset_group": "cefi",
                    "venue": "OKX-SWAP",
                },
            )
        assert r.status_code == 200
        body = r.json()
        assert body["total_count"] == 1
        assert body["instruments"][0]["instrument_id"] == "SOL-USDT-SWAP"

    def test_manifest_read_failure_returns_500(self, client_ds_catalogue: TestClient) -> None:
        with (
            patch(_PATCH_BUILD_BUCKET, return_value="market-data-tick-cefi-prd-fake"),
            patch(_PATCH_READ_INDEX, side_effect=OSError("gcs unavailable")),
        ):
            r = client_ds_catalogue.get(
                "/data-status/catalogue",
                params={"service": "market-tick-data-service", "asset_group": "cefi"},
            )
        assert r.status_code == 500


class TestDownloadCatalogueCsv:
    def test_csv_matches_json_route_row_count(self, client_ds_catalogue: TestClient) -> None:
        with (
            patch(_PATCH_BUILD_BUCKET, return_value="market-data-tick-cefi-prd-fake"),
            patch(_PATCH_READ_INDEX, return_value=_manifest_df()),
            patch(_PATCH_IS_MVP, return_value=True),
        ):
            r = client_ds_catalogue.get(
                "/data-status/download-catalogue-csv",
                params={"service": "market-tick-data-service", "asset_group": "cefi"},
            )
        assert r.status_code == 200
        assert r.headers["content-type"].startswith("text/csv")
        assert r.headers["X-Row-Count"] == "2"
        assert "BTC-USDT-PERP" in r.text
        assert "ETH-USDT-PERP" in r.text

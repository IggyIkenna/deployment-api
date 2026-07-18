"""Tests for routes/data_status/_axis_census.py — the Track-6 "Axis Value
Census" backend (``GET /axis-value-census``), the non-canonical-naming /
duplication detector already consumed by deployment-ui's ``AxisValueCensus``
component (shipped ahead of this backend at ``deployment-ui@3fb6779``).

Mirrors ``test_route_data_status_catalogue.py``'s ``client_ds_catalogue``
TestClient pattern + patch surface (``build_bucket_name`` /
``_read_availability_index`` are both resolved through the shared
``deployment_api.routes.data_status`` package at call time, so both are
patched at that package path regardless of which submodule calls them).

The response shape asserted here is pinned to
``deployment-ui/src/api/client.ts``'s ``AxisValueCensus`` interface
(``{service, asset_group, row_count, axes: Record<str, {value,count}[]>,
truncated_axes: str[]}``) so a shape drift here fails loudly instead of
silently breaking the already-shipped panel.

Plan: ``tradfi_consolidated_closeout_2026_07_18.md`` Phase C /
``cefi_consolidated_closeout_2026_07_18.md`` Track-6.
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


def _make_mock_cfg() -> MagicMock:
    cfg = MagicMock()
    cfg.is_mock_mode.return_value = False
    cfg.deployment_env = "dev"
    return cfg


@pytest.fixture
def client_ds_axis_census() -> TestClient:
    from deployment_api.routes.data_status import router

    app = FastAPI()
    app.include_router(router, prefix="/data-status")
    with (
        patch(_PATCH_DISABLE_AUTH, True),
        patch(_PATCH_CFG, _make_mock_cfg()),
    ):
        yield TestClient(app, raise_server_exceptions=False)  # type: ignore[misc]


def _defi_manifest_with_known_dupes_df() -> pd.DataFrame:
    """Same real-world duplicate shape the shipped ``AxisValueCensus``
    Vitest fixture uses (``spot``/``SPOT_PAIR``/``spot_pair`` +
    ``JITO``/``JITO-SOLANA``) — so this test doubles as a consistency check
    against the contract the UI already ships against."""
    return pd.DataFrame(
        {
            "venue": ["BINANCE-SPOT", "BINANCE-SPOT", "BINANCE-SPOT", "JITO", "JITO-SOLANA"],
            "chain": ["SOLANA", "SOLANA", "", "", ""],
            "instrument_type": ["SPOT_PAIR", "spot", "spot_pair", "POOL", "POOL"],
            "data_type": ["trades", "trades", "trades", "dex_pools", "dex_pools"],
            "source": ["tardis", "tardis", "tardis", "tardis", "tardis"],
            "pipeline_mode": ["batch_tardis"] * 5,
        }
    )


class TestGetAxisValueCensus:
    def test_raw_spelling_variants_survive_as_distinct_buckets(self, client_ds_axis_census: TestClient) -> None:
        with (
            patch(_PATCH_BUILD_BUCKET, return_value="instruments-store-defi-prd-fake"),
            patch(_PATCH_READ_INDEX, return_value=_defi_manifest_with_known_dupes_df()),
        ):
            r = client_ds_axis_census.get(
                "/data-status/axis-value-census",
                params={"service": "instruments-service", "asset_group": "defi"},
            )
        assert r.status_code == 200
        body = r.json()
        assert body["service"] == "instruments-service"
        assert body["asset_group"] == "defi"
        assert body["row_count"] == 5

        instrument_types = {d["value"]: d["count"] for d in body["axes"]["instrument_type"]}
        # Every raw spelling survives as ITS OWN bucket — never merged, which
        # is precisely what the UI's "possible duplicate" flag depends on.
        assert instrument_types == {"SPOT_PAIR": 1, "spot": 1, "spot_pair": 1, "POOL": 2}

        venues = {d["value"]: d["count"] for d in body["axes"]["venue"]}
        assert venues == {"BINANCE-SPOT": 3, "JITO": 1, "JITO-SOLANA": 1}

    def test_response_matches_the_shipped_ui_contract_shape(self, client_ds_axis_census: TestClient) -> None:
        """Pins the exact top-level keys ``deployment-ui``'s ``AxisValueCensus``
        TypeScript interface expects — a rename/removal here would silently
        break the already-shipped panel."""
        with (
            patch(_PATCH_BUILD_BUCKET, return_value="instruments-store-defi-prd-fake"),
            patch(_PATCH_READ_INDEX, return_value=_defi_manifest_with_known_dupes_df()),
        ):
            r = client_ds_axis_census.get(
                "/data-status/axis-value-census",
                params={"service": "instruments-service", "asset_group": "defi"},
            )
        body = r.json()
        assert set(body.keys()) == {"service", "asset_group", "row_count", "axes", "truncated_axes"}
        for axis_rows in body["axes"].values():
            for row in axis_rows:
                assert set(row.keys()) == {"value", "count"}

    def test_sorted_by_count_descending(self, client_ds_axis_census: TestClient) -> None:
        with (
            patch(_PATCH_BUILD_BUCKET, return_value="instruments-store-defi-prd-fake"),
            patch(_PATCH_READ_INDEX, return_value=_defi_manifest_with_known_dupes_df()),
        ):
            r = client_ds_axis_census.get(
                "/data-status/axis-value-census",
                params={"service": "instruments-service", "asset_group": "defi"},
            )
        body = r.json()
        counts = [d["count"] for d in body["axes"]["venue"]]
        assert counts == sorted(counts, reverse=True)
        assert body["axes"]["venue"][0] == {"value": "BINANCE-SPOT", "count": 3}

    def test_blank_and_sentinel_values_excluded(self, client_ds_axis_census: TestClient) -> None:
        df = pd.DataFrame(
            {
                "venue": ["CME", "CME"],
                "chain": ["", "None"],
                "instrument_type": ["FUTURE", "nan"],
                "data_type": ["ohlcv_1m", "ohlcv_1m"],
                "source": ["databento", "databento"],
                "pipeline_mode": ["batch_databento", "batch_databento"],
            }
        )
        with (
            patch(_PATCH_BUILD_BUCKET, return_value="market-data-tick-tradfi-prd-fake"),
            patch(_PATCH_READ_INDEX, return_value=df),
        ):
            r = client_ds_axis_census.get(
                "/data-status/axis-value-census",
                params={"service": "market-tick-data-service", "asset_group": "tradfi"},
            )
        body = r.json()
        assert body["axes"]["chain"] == []
        assert body["axes"]["instrument_type"] == [{"value": "FUTURE", "count": 1}]

    def test_axis_present_with_empty_list_when_column_all_blank(self, client_ds_axis_census: TestClient) -> None:
        """Honest-absence: every requested axis is always a key in the
        response (matches ``_catalogue.py``'s ``_distinct_values`` precedent
        — a genuinely blank column resolves to ``[]``, never omitted)."""
        df = pd.DataFrame({"venue": ["CME", "CME"], "chain": ["", ""]})
        with (
            patch(_PATCH_BUILD_BUCKET, return_value="market-data-tick-tradfi-prd-fake"),
            patch(_PATCH_READ_INDEX, return_value=df),
        ):
            r = client_ds_axis_census.get(
                "/data-status/axis-value-census",
                params={"service": "market-tick-data-service", "asset_group": "tradfi"},
            )
        body = r.json()
        assert set(body["axes"].keys()) == {"venue", "chain", "instrument_type", "data_type", "source", "pipeline_mode"}
        assert body["axes"]["chain"] == []
        assert body["axes"]["data_type"] == []

    def test_axis_exceeding_cap_is_truncated_and_flagged(self, client_ds_axis_census: TestClient) -> None:
        # 250 distinct venues, one row each — exceeds the 200-value cap.
        df = pd.DataFrame({"venue": [f"VENUE-{i:03d}" for i in range(250)]})
        with (
            patch(_PATCH_BUILD_BUCKET, return_value="instruments-store-cefi-prd-fake"),
            patch(_PATCH_READ_INDEX, return_value=df),
        ):
            r = client_ds_axis_census.get(
                "/data-status/axis-value-census",
                params={"service": "instruments-service", "asset_group": "cefi"},
            )
        body = r.json()
        assert body["truncated_axes"] == ["venue"]
        assert len(body["axes"]["venue"]) == 200

    def test_reader_called_with_column_pruning_single_walk(self, client_ds_axis_census: TestClient) -> None:
        """Single-walk discipline: the endpoint must request ONLY the axis
        columns (column-pruned slim read), never a full-schema whole-corpus
        read."""
        with (
            patch(_PATCH_BUILD_BUCKET, return_value="instruments-store-defi-prd-fake"),
            patch(_PATCH_READ_INDEX, return_value=_defi_manifest_with_known_dupes_df()) as mock_read,
        ):
            r = client_ds_axis_census.get(
                "/data-status/axis-value-census",
                params={"service": "instruments-service", "asset_group": "defi"},
            )
        assert r.status_code == 200
        mock_read.assert_called_once()
        _, kwargs = mock_read.call_args
        assert set(kwargs["columns"]) == {"venue", "chain", "instrument_type", "data_type", "source", "pipeline_mode"}

    def test_empty_manifest_returns_zero_row_count_and_empty_axes(self, client_ds_axis_census: TestClient) -> None:
        with (
            patch(_PATCH_BUILD_BUCKET, return_value="instruments-store-defi-prd-fake"),
            patch(_PATCH_READ_INDEX, return_value=pd.DataFrame()),
        ):
            r = client_ds_axis_census.get(
                "/data-status/axis-value-census",
                params={"service": "instruments-service", "asset_group": "defi"},
            )
        assert r.status_code == 200
        body = r.json()
        assert body["row_count"] == 0
        assert all(v == [] for v in body["axes"].values())
        assert body["truncated_axes"] == []

    def test_manifest_read_failure_returns_500(self, client_ds_axis_census: TestClient) -> None:
        with (
            patch(_PATCH_BUILD_BUCKET, return_value="instruments-store-defi-prd-fake"),
            patch(_PATCH_READ_INDEX, side_effect=OSError("gcs unavailable")),
        ):
            r = client_ds_axis_census.get(
                "/data-status/axis-value-census",
                params={"service": "instruments-service", "asset_group": "defi"},
            )
        assert r.status_code == 500

    def test_unknown_service_returns_500(self, client_ds_axis_census: TestClient) -> None:
        with patch(_PATCH_BUILD_BUCKET, side_effect=ValueError("Unknown service: nope")):
            r = client_ds_axis_census.get(
                "/data-status/axis-value-census",
                params={"service": "nope", "asset_group": "defi"},
            )
        assert r.status_code == 500

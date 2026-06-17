"""Regression tests for the venue-year-coverage scope + config_versions + UNION surface.

Covers (plan ``mvp_scope_catalogue_tagging_2026_06_08.md`` §4 + §"Config versioning"
+ FLAG-1 multi-source UNION):

Item 4 — ``scope=mvp|could_exist|all`` query param:
  * the three scope values are accepted; an invalid one 422s.
  * denominator monotonicity ``mvp <= could_exist <= all`` (mvp is a subset filter).
  * ``config_versions`` triples match the UAC ``*_config_descriptor()`` SSOTs.

Item 5 — stale-tolerant read + CeFi multi-source UNION (FLAG-1):
  * the endpoint reads via the stale-tolerant ``_read_manifest_index`` (empty-live →
    consolidated ``_index`` fallback lives there — the route delegates to it).
  * a multi-source cell (A=captured, B=failed) UNIONs to captured (≥1 captured ⇒
    captured) and the per-source ``source_breakdown`` shows both sources.
"""

from __future__ import annotations

from unittest.mock import MagicMock, patch

import pandas as pd
import pytest
from fastapi import FastAPI
from fastapi.testclient import TestClient
from unified_api_contracts import (
    mvp_scope_config_descriptor,
    prediction_markets_config_descriptor,
    sports_leagues_config_descriptor,
)

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


def _defi_df() -> pd.DataFrame:
    """DeFi manifest with one MVP cell (LIDO LST lst_rates) + one non-MVP cell."""
    return pd.DataFrame(
        [
            # MVP cell — is_mvp('defi','LIDO-ETHEREUM','LST','lst_rates') is True.
            {
                "date": "2026-01-15",
                "venue": "LIDO-ETHEREUM",
                "instrument_type": "LST",
                "data_type": "lst_rates",
                "capture_status": "captured",
                "error_reason": None,
            },
            # Non-MVP cell — unknown venue → is_mvp False.
            {
                "date": "2026-01-16",
                "venue": "MYSTERY-DEX",
                "instrument_type": "DEX_POOL",
                "data_type": "dex_pool_state",
                "capture_status": "captured",
                "error_reason": None,
            },
        ]
    )


def _total_denominator(payload: dict[str, object]) -> int:
    return sum(int(r["total"]) for r in payload["rows"])  # type: ignore[index,call-overload]


class TestScopeParamAccepted:
    @pytest.mark.parametrize("scope", ["mvp", "could_exist", "all"])
    def test_each_scope_value_accepted(self, client: TestClient, scope: str) -> None:
        with (
            patch(_PATCH_BUILD_BUCKET, return_value="bucket"),
            patch(_PATCH_READ_INDEX, return_value=_defi_df()),
        ):
            resp = client.get(
                "/data-status/venue-year-coverage",
                params={"asset_groups": "defi", "scope": scope},
            )
        assert resp.status_code == 200
        assert resp.json()["scope"] == scope

    def test_invalid_scope_rejected(self, client: TestClient) -> None:
        with (
            patch(_PATCH_BUILD_BUCKET, return_value="bucket"),
            patch(_PATCH_READ_INDEX, return_value=_defi_df()),
        ):
            resp = client.get(
                "/data-status/venue-year-coverage",
                params={"asset_groups": "defi", "scope": "bogus"},
            )
        assert resp.status_code == 422

    def test_default_scope_is_could_exist(self, client: TestClient) -> None:
        with (
            patch(_PATCH_BUILD_BUCKET, return_value="bucket"),
            patch(_PATCH_READ_INDEX, return_value=_defi_df()),
        ):
            resp = client.get("/data-status/venue-year-coverage", params={"asset_groups": "defi"})
        assert resp.json()["scope"] == "could_exist"


class TestDenominatorMonotonicity:
    def test_mvp_le_could_exist_le_all(self, client: TestClient) -> None:
        """mvp denominator <= could_exist <= all (mvp is a strict subset filter)."""
        totals: dict[str, int] = {}
        for scope in ("mvp", "could_exist", "all"):
            with (
                patch(_PATCH_BUILD_BUCKET, return_value="bucket"),
                patch(_PATCH_READ_INDEX, return_value=_defi_df()),
            ):
                resp = client.get(
                    "/data-status/venue-year-coverage",
                    params={"asset_groups": "defi", "scope": scope},
                )
            totals[scope] = _total_denominator(resp.json())

        assert totals["mvp"] <= totals["could_exist"] <= totals["all"]
        # The fixture has exactly one MVP cell out of two → mvp strictly smaller.
        assert totals["mvp"] == 1
        assert totals["could_exist"] == 2
        assert totals["all"] == 2

    def test_mvp_keeps_only_mvp_cells(self, client: TestClient) -> None:
        with (
            patch(_PATCH_BUILD_BUCKET, return_value="bucket"),
            patch(_PATCH_READ_INDEX, return_value=_defi_df()),
        ):
            resp = client.get(
                "/data-status/venue-year-coverage",
                params={"asset_groups": "defi", "scope": "mvp"},
            )
        rows = resp.json()["rows"]
        venues = {r["venue"] for r in rows}
        assert venues == {"LIDO-ETHEREUM"}, "scope=mvp must drop the non-MVP MYSTERY-DEX cell"


class TestConfigVersionsTriples:
    def test_config_versions_match_uac_descriptors(self, client: TestClient) -> None:
        with (
            patch(_PATCH_BUILD_BUCKET, return_value="bucket"),
            patch(_PATCH_READ_INDEX, return_value=_defi_df()),
        ):
            resp = client.get("/data-status/venue-year-coverage", params={"asset_groups": "defi"})
        cfgv = resp.json()["config_versions"]

        mvp = mvp_scope_config_descriptor()
        leagues = sports_leagues_config_descriptor()
        markets = prediction_markets_config_descriptor()

        assert cfgv["mvp_scope"]["version"] == mvp.config_version
        assert cfgv["mvp_scope"]["content_hash"] == mvp.config_content_hash
        assert cfgv["sports_leagues"]["version"] == leagues.config_version
        assert cfgv["sports_leagues"]["content_hash"] == leagues.config_content_hash
        assert cfgv["prediction_markets"]["version"] == markets.config_version
        assert cfgv["prediction_markets"]["content_hash"] == markets.config_content_hash


class TestCefiMultiSourceUnion:
    """FLAG-1 — a CeFi cell with source A=captured, B=failed unions to captured,
    and the per-source breakdown surfaces both."""

    def _cefi_multisource_df(self) -> pd.DataFrame:
        cell = {
            "date": "2026-02-01",
            "asset_group": "cefi",
            "venue": "BINANCE-FUTURES",
            "instrument_type": "PERPETUAL",
            "data_type": "funding_rate",
            "instrument_id": "BTCUSDT",
        }
        return pd.DataFrame(
            [
                {**cell, "source": "source_a", "pipeline_mode": "batch_source_a", "capture_status": "captured"},
                {
                    **cell,
                    "source": "source_b",
                    "pipeline_mode": "batch_source_b",
                    "capture_status": "attempted_failed",
                    "error_reason": "timeout",
                },
            ]
        )

    def test_union_cell_is_captured(self, client: TestClient) -> None:
        with (
            patch(_PATCH_BUILD_BUCKET, return_value="bucket"),
            patch(_PATCH_READ_INDEX, return_value=self._cefi_multisource_df()),
        ):
            resp = client.get("/data-status/venue-year-coverage", params={"asset_groups": "cefi"})
        data = resp.json()
        rows = data["rows"]
        assert len(rows) == 1, "the two source rows must collapse to ONE cell"
        row = rows[0]
        # ≥1 source captured ⇒ the cell is captured; never double-counted (total == 1).
        assert row["captured"] == 1
        assert row["attempted_failed"] == 0
        assert row["total"] == 1

    def test_source_breakdown_shows_both_sources(self, client: TestClient) -> None:
        with (
            patch(_PATCH_BUILD_BUCKET, return_value="bucket"),
            patch(_PATCH_READ_INDEX, return_value=self._cefi_multisource_df()),
        ):
            resp = client.get("/data-status/venue-year-coverage", params={"asset_groups": "cefi"})
        breakdown = resp.json()["source_breakdown"]
        by_source = {b["source"]: b for b in breakdown}
        assert by_source["source_a"]["captured"] == 1
        assert by_source["source_b"]["attempted_failed"] == 1
        assert by_source["source_b"]["captured"] == 0
        # Tagged with the asset_group so the UI can group across AGs.
        assert all(b["asset_group"] == "CEFI" for b in breakdown)


class TestStaleTolerantReadDelegation:
    """Item 5a — the route delegates to the stale-tolerant ``_read_manifest_index``
    (whose empty-live → consolidated ``_index`` fallback is unit-tested at the
    service layer). Pinning the call here guards a revert back to the raw
    freshness-gated reader."""

    def test_route_uses_read_manifest_index(self, client: TestClient) -> None:
        reader = MagicMock(return_value=_defi_df())
        with (
            patch(_PATCH_BUILD_BUCKET, return_value="bucket"),
            patch(_PATCH_READ_INDEX, reader),
        ):
            resp = client.get("/data-status/venue-year-coverage", params={"asset_groups": "defi"})
        assert resp.status_code == 200
        reader.assert_called_once_with("bucket")

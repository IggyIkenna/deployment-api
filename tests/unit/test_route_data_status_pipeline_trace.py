"""Tests for routes/data_status/_pipeline_trace.py — the cross-service E2E pipeline
trace endpoint (``GET /pipeline-trace``), GAP G-TRACE.

The endpoint is pure orchestration over the existing per-shard
``lookup_capture_status_for_shard`` primitive (data_status_drilldown/_core.py) — no new
manifest-read logic to unit test here, just: (1) every pipeline stage is queried in the
right order with the right axes, (2) ``stuck_at`` names the first non-``captured`` hop,
(3) a per-hop lookup failure degrades that ONE hop rather than failing the whole trace.

Mirrors ``test_route_data_status_distinct_values.py``'s TestClient + patch pattern.
"""

from __future__ import annotations

from unittest.mock import MagicMock, patch

import pytest
from fastapi import FastAPI
from fastapi.testclient import TestClient

_PATCH_DISABLE_AUTH = "deployment_api.rbac.DISABLE_AUTH"
_PATCH_CFG = "deployment_api.routes.data_status._cfg"
_PATCH_LOOKUP = "deployment_api.routes.data_status.lookup_capture_status_for_shard"

_CAPTURED = {
    "status": "captured",
    "error_reason": "",
    "attempted_at": "2026-08-04T00:00:00Z",
    "written_at": "2026-08-04T00:05:00Z",
}
_NEVER_ATTEMPTED = {"status": "never_attempted", "error_reason": "", "attempted_at": "", "written_at": ""}


def _make_mock_cfg() -> MagicMock:
    cfg = MagicMock()
    cfg.is_mock_mode.return_value = False
    cfg.deployment_env = "dev"
    return cfg


@pytest.fixture
def client_pipeline_trace() -> TestClient:
    from deployment_api.routes.data_status import router

    app = FastAPI()
    app.include_router(router, prefix="/data-status")
    with (
        patch(_PATCH_DISABLE_AUTH, True),
        patch(_PATCH_CFG, _make_mock_cfg()),
    ):
        yield TestClient(app, raise_server_exceptions=False)  # type: ignore[misc]


class TestPipelineTraceEndpoint:
    def test_all_hops_captured_stuck_at_none(self, client_pipeline_trace: TestClient) -> None:
        with patch(_PATCH_LOOKUP, return_value=dict(_CAPTURED)) as mock_lookup:
            resp = client_pipeline_trace.get(
                "/data-status/pipeline-trace",
                params={"instrument": "BTC-USDT", "date": "2026-08-01", "asset_group": "cefi"},
            )
        assert resp.status_code == 200
        body = resp.json()
        assert body["instrument"] == "BTC-USDT"
        assert body["date"] == "2026-08-01"
        assert body["asset_group"] == "cefi"
        assert body["stuck_at"] is None

        # 8 hops: IS, MTDS, MDPS, 3x features, strategy, execution — in pipeline order.
        services = [hop["service"] for hop in body["hops"]]
        assert services == [
            "instruments-service",
            "market-tick-data-service",
            "market-data-processing-service",
            "features-onchain-service",
            "features-delta-one-service",
            "features-volatility-service",
            "strategy-service",
            "execution-service",
        ]
        stages = [hop["stage"] for hop in body["hops"]]
        assert stages == [1, 2, 3, 4, 4, 4, 5, 6]
        for hop in body["hops"]:
            assert hop["status"] == "captured"

        assert mock_lookup.call_count == 8
        # every call threads the same instrument/date/asset_group through as axes.
        for call in mock_lookup.call_args_list:
            assert call.kwargs["day"] == "2026-08-01"
            assert call.kwargs["asset_group"] == "cefi"
            assert call.kwargs["instrument_id"] == "BTC-USDT"

    def test_stuck_at_names_first_non_captured_hop_in_pipeline_order(self, client_pipeline_trace: TestClient) -> None:
        # MDPS (stage 3) never attempted; everything upstream captured.
        def _side_effect(*, service: str, **_kwargs: object) -> dict[str, str]:
            if service == "market-data-processing-service":
                return dict(_NEVER_ATTEMPTED)
            return dict(_CAPTURED)

        with patch(_PATCH_LOOKUP, side_effect=_side_effect):
            resp = client_pipeline_trace.get(
                "/data-status/pipeline-trace",
                params={"instrument": "ETH-USDT", "date": "2026-08-01", "asset_group": "cefi"},
            )
        assert resp.status_code == 200
        body = resp.json()
        # stuck_at names the FIRST non-captured hop in pipeline order, not just any.
        assert body["stuck_at"] == "market-data-processing-service"
        mdps_hop = next(h for h in body["hops"] if h["service"] == "market-data-processing-service")
        assert mdps_hop["status"] == "never_attempted"
        # upstream hops (IS, MTDS) still report captured — a downstream stall
        # doesn't retroactively mark upstream hops as stuck.
        is_hop = next(h for h in body["hops"] if h["service"] == "instruments-service")
        assert is_hop["status"] == "captured"

    def test_per_hop_lookup_failure_degrades_only_that_hop(self, client_pipeline_trace: TestClient) -> None:
        def _side_effect(*, service: str, **_kwargs: object) -> dict[str, str]:
            if service == "execution-service":
                raise RuntimeError("manifest bucket unreachable")
            return dict(_CAPTURED)

        with patch(_PATCH_LOOKUP, side_effect=_side_effect):
            resp = client_pipeline_trace.get(
                "/data-status/pipeline-trace",
                params={"instrument": "BTC-USDT", "date": "2026-08-01", "asset_group": "cefi"},
            )
        # the endpoint itself never 500s on a single hop's failure.
        assert resp.status_code == 200
        body = resp.json()
        exec_hop = next(h for h in body["hops"] if h["service"] == "execution-service")
        assert exec_hop["status"] == "never_attempted"
        assert body["stuck_at"] == "execution-service"
        # every other hop still reports its own (unrelated) real status.
        other_statuses = {h["status"] for h in body["hops"] if h["service"] != "execution-service"}
        assert other_statuses == {"captured"}

    def test_optional_axes_threaded_through_to_lookup(self, client_pipeline_trace: TestClient) -> None:
        with patch(_PATCH_LOOKUP, return_value=dict(_CAPTURED)) as mock_lookup:
            resp = client_pipeline_trace.get(
                "/data-status/pipeline-trace",
                params={
                    "instrument": "AAVE_V3",
                    "date": "2026-08-01",
                    "asset_group": "defi",
                    "instrument_type": "lending",
                    "chain": "ETHEREUM",
                },
            )
        assert resp.status_code == 200
        first_call = mock_lookup.call_args_list[0]
        assert first_call.kwargs["instrument_type"] == "lending"
        assert first_call.kwargs["chain"] == "ETHEREUM"
        assert first_call.kwargs["venue"] is None

    def test_asset_group_lowercased_in_response(self, client_pipeline_trace: TestClient) -> None:
        with patch(_PATCH_LOOKUP, return_value=dict(_CAPTURED)):
            resp = client_pipeline_trace.get(
                "/data-status/pipeline-trace",
                params={"instrument": "BTC-USDT", "date": "2026-08-01", "asset_group": "CEFI"},
            )
        assert resp.json()["asset_group"] == "cefi"

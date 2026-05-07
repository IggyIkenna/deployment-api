"""Phase 2 of data_status_multi_axis_shard_propagation_2026_05_06.plan.md.

deployment-api consumes the UAC ``data_status_axis_matrix`` SSOT for:

1. ``_select_coverage_group_axis`` — primary axis comes from
   ``PRIMARY_AXIS[(service, asset_group)]`` (UAC SSOT) instead of a
   hardcoded service→column map; falls back to data_type for sports +
   venue otherwise when SSOT is silent.
2. ``_build_breakdowns`` — per-(service, asset_group) breakdowns dict
   ``{axis: {value: count}}`` keyed on UAC ``BREAKDOWN_AXES`` (shard +
   display minus primary). Empty axes still emit ``{}`` so the UI can
   render an "expected, no data yet" placeholder. Empty/null values
   surface under a synthetic ``__legacy__`` key.

The two helpers together make the on-demand coverage-summary path
honour the SSOT — the rollup blob picks up breakdowns on the next
worker cron tick (its sync builder calls ``_build_coverage_for_cat``
which now returns ``breakdowns``).
"""

from __future__ import annotations

import pandas as pd

from deployment_api.services.data_status_service import DataStatusService


def _index(rows: list[dict[str, object]]) -> pd.DataFrame:
    return pd.DataFrame(rows)


class TestSelectCoverageGroupAxisFromSSOT:
    """``_select_coverage_group_axis`` reads UAC ``PRIMARY_AXIS`` first."""

    def test_strategy_service_uses_strategy_id(self) -> None:
        dss = DataStatusService()
        idx = _index(
            [
                {"strategy_id": "carry_v1", "instrument_count": 1, "venue": "DERIBIT"},
                {"strategy_id": "carry_v2", "instrument_count": 1, "venue": "DERIBIT"},
            ]
        )
        axis = dss._select_coverage_group_axis("strategy-service", "CEFI", idx)
        assert axis == "strategy_id"

    def test_execution_service_uses_instruction_type(self) -> None:
        dss = DataStatusService()
        idx = _index(
            [
                {"instruction_type": "TRADE", "instrument_count": 1},
                {"instruction_type": "SWAP", "instrument_count": 1},
            ]
        )
        axis = dss._select_coverage_group_axis("execution-service", "DEFI", idx)
        assert axis == "instruction_type"

    def test_features_sports_uses_feature_group(self) -> None:
        dss = DataStatusService()
        idx = _index(
            [
                {"feature_group": "api_football_only", "instrument_count": 5},
                {"feature_group": "footystats_only", "instrument_count": 3},
            ]
        )
        axis = dss._select_coverage_group_axis("features-sports-service", "SPORTS", idx)
        assert axis == "feature_group"

    def test_falls_back_to_venue_for_unmapped_service(self) -> None:
        dss = DataStatusService()
        idx = _index([{"venue": "DERIBIT", "instrument_count": 1}])
        axis = dss._select_coverage_group_axis("unknown-service", "CEFI", idx)
        assert axis == "venue"

    def test_falls_back_to_data_type_for_unmapped_sports(self) -> None:
        dss = DataStatusService()
        idx = _index([{"data_type": "fixtures", "instrument_count": 1}])
        # No SSOT entry for "unknown-sports-service" sports → fall through
        # to the per-asset-group default (data_type for sports).
        axis = dss._select_coverage_group_axis("unknown-sports-service", "SPORTS", idx)
        assert axis == "data_type"


class TestBuildBreakdowns:
    """``_build_breakdowns`` produces ``{axis: {value: count}}`` per UAC SSOT."""

    def test_defi_emits_chain_axis(self) -> None:
        dss = DataStatusService()
        idx = _index(
            [
                {
                    "venue": "BALANCER",
                    "chain": "ETHEREUM",
                    "instrument_type": "DEX_POOL",
                    "instrument_count": 100,
                },
                {
                    "venue": "BALANCER",
                    "chain": "ARBITRUM",
                    "instrument_type": "DEX_POOL",
                    "instrument_count": 40,
                },
                {
                    "venue": "AAVE_V3",
                    "chain": "ETHEREUM",
                    "instrument_type": "LENDING_POOL",
                    "instrument_count": 20,
                },
            ]
        )
        breakdowns = dss._build_breakdowns("instruments-service", "DEFI", idx, "venue")
        # primary axis (venue) must NOT appear
        assert "venue" not in breakdowns
        # chain is a shard axis → present + populated
        assert breakdowns["chain"] == {"ETHEREUM": 120, "ARBITRUM": 40}
        # instrument_type is a display axis → also present + populated
        assert breakdowns["instrument_type"] == {"DEX_POOL": 140, "LENDING_POOL": 20}

    def test_strategy_breakdowns_include_job_id_and_archetype(self) -> None:
        dss = DataStatusService()
        idx = _index(
            [
                {
                    "strategy_id": "carry_v1",
                    "job_id": "20260506-120000-carry",
                    "archetype": "carry_staked_basis",
                    "client_id": "internal",
                    "instrument_count": 10,
                },
                {
                    "strategy_id": "carry_v1",
                    "job_id": "20260506-130000-carry_v2",
                    "archetype": "carry_staked_basis",
                    "client_id": "internal",
                    "instrument_count": 5,
                },
            ]
        )
        breakdowns = dss._build_breakdowns("strategy-service", "CEFI", idx, "strategy_id")
        # primary axis dropped; job_id (shard axis) + archetype + client_id
        # (display axes) survive.
        assert "strategy_id" not in breakdowns
        assert breakdowns["job_id"] == {
            "20260506-120000-carry": 10,
            "20260506-130000-carry_v2": 5,
        }
        assert breakdowns["archetype"] == {"carry_staked_basis": 15}
        assert breakdowns["client_id"] == {"internal": 15}

    def test_empty_axis_column_emits_empty_dict_not_missing_key(self) -> None:
        # Phase 1 writers haven't yet populated ``job_id`` for the
        # ml-training-service manifest. The breakdowns endpoint MUST
        # still surface the axis so the UI can render the dropdown
        # (disabled / "no data yet" placeholder).
        dss = DataStatusService()
        idx = _index(
            [
                {
                    "model_family": "pregame_xg",
                    "training_period": "2024-01",
                    "client_id": "internal",
                    "asset_group": "sports",
                    "instrument_count": 1,
                }
                # NB: no ``job_id`` column at all — Phase 1 writer hasn't
                # caught up yet.
            ]
        )
        breakdowns = dss._build_breakdowns("ml-training-service", "shared", idx, "model_family")
        assert "job_id" in breakdowns
        assert breakdowns["job_id"] == {}
        # training_period populated → present
        assert breakdowns["training_period"] == {"2024-01": 1}

    def test_empty_string_value_surfaces_under_legacy_key(self) -> None:
        # Pre-Phase-1B ML rows that wrote ``job_id=""`` should not
        # pollute the breakdowns; instead they collapse under
        # ``__legacy__``.
        dss = DataStatusService()
        idx = _index(
            [
                {
                    "model_family": "pregame_xg",
                    "training_period": "2024-01",
                    "job_id": "",  # legacy
                    "client_id": "internal",
                    "asset_group": "sports",
                    "instrument_count": 3,
                },
                {
                    "model_family": "pregame_xg",
                    "training_period": "2024-02",
                    "job_id": "20260506-120000-pregame_xg",
                    "client_id": "internal",
                    "asset_group": "sports",
                    "instrument_count": 7,
                },
            ]
        )
        breakdowns = dss._build_breakdowns("ml-training-service", "shared", idx, "model_family")
        assert breakdowns["job_id"] == {
            "__legacy__": 3,
            "20260506-120000-pregame_xg": 7,
        }

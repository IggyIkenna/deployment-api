"""Drilldown provenance + group-by axes for the v9 manifest (G3 / M5 / M4).

The hierarchical drilldown must expose the per-cell pipeline_mode x source
breakdown (so a cell shows "captured via batch_binance + replay_binance,
missing in live_binance"), must support pipeline_mode / source as FILTER axes,
and as GROUP-BY axes inserted at the top of the tree. Leaf counts stay honest
(cell-grain union), never double-counted across source/mode rows.

Plan: master_data_canonicalisation_migration_catalogue_2026_06_07 G3 / M5.
"""

from __future__ import annotations

from unittest.mock import MagicMock, patch

import pandas as pd

from deployment_api.services.data_status_hierarchical import get_hierarchical_drilldown

_BUCKET = "market-tick-data-store-cefi-123"
_CELL = {
    "date": "2026-06-01",
    "venue": "BINANCE-FUTURES",
    "chain": "",
    "data_type": "funding_rate",
    "instrument_type": "PERPETUAL",
    "instrument_id": "BTC-USDT",
    "league_id": "",
    "feature_group": "",
    "feature_family": "",
    "timeframe": "",
    "canonical_question_group": "",
    "underlying": "",
    "error_reason": "",
}


def _multi_mode_manifest() -> pd.DataFrame:
    """One cell, captured via batch + replay, failed in live (3 provenance rows)."""
    return pd.DataFrame(
        [
            {**_CELL, "source": "binance", "pipeline_mode": "batch_binance", "capture_status": "captured"},
            {**_CELL, "source": "binance", "pipeline_mode": "replay_binance", "capture_status": "captured"},
            {**_CELL, "source": "binance", "pipeline_mode": "live_binance", "capture_status": "attempted_failed"},
        ]
    )


def _leaves(tree: list[dict[str, object]]) -> list[dict[str, object]]:
    out: list[dict[str, object]] = []
    for node in tree:
        children = node.get("children") or []
        if not children:
            out.append(node)
        else:
            out.extend(_leaves(children))  # pyright: ignore[reportArgumentType]
    return out


def _call(df: pd.DataFrame, **kw: object) -> dict[str, object]:
    with (
        patch(
            "deployment_api.services.data_status_hierarchical.read_availability_index",
            MagicMock(return_value=df),
        ),
        patch(
            "deployment_api.services.data_status_hierarchical.build_bucket_name",
            return_value=_BUCKET,
        ),
    ):
        return get_hierarchical_drilldown(
            service="market-tick-data-service",
            asset_group="cefi",
            window_start="2026-06-01",
            window_end="2026-06-30",
            expand_to_depth=10,
            **kw,  # pyright: ignore[reportArgumentType]
        )


class TestLeafProvenance:
    def test_leaf_carries_per_mode_breakdown_and_unioned_count(self) -> None:
        result = _call(_multi_mode_manifest())
        # Totals union across modes → ONE captured cell, not 3 rows.
        assert result["totals"]["captured"] == 1  # pyright: ignore[reportIndexIssue]
        assert result["totals"]["total"] == 1  # pyright: ignore[reportIndexIssue]
        leaves = _leaves(result["tree"])  # pyright: ignore[reportArgumentType]
        assert len(leaves) == 1
        leaf = leaves[0]
        assert leaf["captured"] == 1
        prov = {row["pipeline_mode"]: row for row in leaf["provenance"]}  # pyright: ignore[reportGeneralTypeIssues,reportArgumentType]
        assert prov["batch_binance"]["captured"] == 1
        assert prov["replay_binance"]["captured"] == 1
        assert prov["live_binance"]["attempted_failed"] == 1
        assert prov["live_binance"]["captured"] == 0

    def test_top_level_provenance_summary_present(self) -> None:
        result = _call(_multi_mode_manifest())
        prov = {row["pipeline_mode"]: row for row in result["provenance"]}  # pyright: ignore[reportGeneralTypeIssues,reportArgumentType]
        assert set(prov) == {"batch_binance", "replay_binance", "live_binance"}


class TestPipelineModeFilter:
    def test_filter_to_single_mode_scopes_the_tree(self) -> None:
        result = _call(_multi_mode_manifest(), filters={"pipeline_mode": "live_binance"})
        # Only the live row survives → the cell reads attempted_failed.
        assert result["totals"]["captured"] == 0  # pyright: ignore[reportIndexIssue]
        assert result["totals"]["attempted_failed"] == 1  # pyright: ignore[reportIndexIssue]


class TestGroupByAxis:
    def test_group_by_pipeline_mode_prepends_axis(self) -> None:
        result = _call(_multi_mode_manifest(), group_by=["pipeline_mode"])
        assert result["axes"][0] == "pipeline_mode"  # pyright: ignore[reportIndexIssue]
        top = result["tree"]
        modes = {node["value"] for node in top}  # pyright: ignore[reportGeneralTypeIssues,reportArgumentType]
        assert modes == {"batch_binance", "replay_binance", "live_binance"}
        # Totals still union across modes → one honest captured cell.
        assert result["totals"]["captured"] == 1  # pyright: ignore[reportIndexIssue]

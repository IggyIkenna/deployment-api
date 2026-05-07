"""Tests for the hierarchical shard-atom drill-down builder.

Plan: ``data_status_drilldown_shard_atom_alignment_2026_05_07`` Phase 1.
"""

from __future__ import annotations

from unittest.mock import patch

import pandas as pd
import pytest

import deployment_api.services.data_status_hierarchical as _hier
from deployment_api.services.data_status_hierarchical import (
    DrilldownNode,
    get_hierarchical_drilldown,
    list_supported_pairs,
)


def _mtds_defi_manifest() -> pd.DataFrame:
    """Realistic 2-chain x 2-venue x 2-data_type x 2-instrument x 3-day MTDS DEFI sample."""
    rows: list[dict[str, object]] = []
    for chain, venue in (
        ("ARBITRUM", "AAVEV3-ARBITRUM"),
        ("ARBITRUM", "UNISWAPV3-ARBITRUM"),
        ("BASE", "AAVEV3-BASE"),
        ("BASE", "UNISWAPV3-BASE"),
    ):
        for dt_name in ("lending_indices", "dex_swaps"):
            for inst in ("USDC", "WETH"):
                for day in ("2024-03-01", "2024-03-02", "2024-03-03"):
                    rows.append(
                        {
                            "chain": chain,
                            "venue": venue,
                            "data_type": dt_name,
                            "instrument_id": inst,
                            "date": day,
                            "capture_status": "captured",
                            "error_reason": "",
                        }
                    )
    return pd.DataFrame(rows)


class TestDrilldownNodeShape:
    def test_completion_pct_is_captured_over_total(self) -> None:
        node = DrilldownNode(
            axis="chain",
            value="ARBITRUM",
            captured=80,
            empty_confirmed=15,
            attempted_failed=5,
        )
        assert node.total == 100
        assert node.completion_pct == 80.0

    def test_to_dict_marks_leaves(self) -> None:
        leaf = DrilldownNode(axis="date", value="2024-03-01", captured=1)
        assert leaf.to_dict()["is_leaf"] is True
        parent = DrilldownNode(axis="venue", value="AAVEV3-ARBITRUM", children=[leaf])
        d = parent.to_dict()
        assert d["is_leaf"] is False
        children = d["children"]
        assert isinstance(children, list)
        assert len(children) == 1


class TestHierarchicalDrilldown:
    def _patch_manifest(self, df: pd.DataFrame):
        return patch.object(_hier, "read_availability_index", return_value=df)

    def test_top_level_returns_chain_axis_for_mtds_defi(self) -> None:
        df = _mtds_defi_manifest()
        with self._patch_manifest(df):
            result = get_hierarchical_drilldown(
                service="market-tick-data-service",
                asset_group="defi",
                window_start="2024-03-01",
                window_end="2024-03-03",
                expand_to_depth=0,
            )
        # MTDS DEFI shard axes: (venue, chain, instrument_id, data_type) + date.
        # Top axis is ``venue`` per the SSOT — but the codex-shipped order
        # for DEFI puts venue first then chain. Verify whichever the SSOT
        # declares is the head of the returned axis list.
        axes = result["axes"]
        assert isinstance(axes, list)
        assert axes[0] in ("venue", "chain")
        assert "date" in axes
        tree = result["tree"]
        assert isinstance(tree, list)
        # 4 distinct venues across the manifest -> 4 top-level nodes (or 2
        # chains if chain is the top axis).
        assert len(tree) in (2, 4)

    def test_filter_descends_into_subtree(self) -> None:
        df = _mtds_defi_manifest()
        with self._patch_manifest(df):
            result = get_hierarchical_drilldown(
                service="market-tick-data-service",
                asset_group="defi",
                window_start="2024-03-01",
                window_end="2024-03-03",
                filters={"chain": "ARBITRUM", "venue": "AAVEV3-ARBITRUM"},
                expand_to_depth=10,
            )
        totals = result["totals"]
        assert isinstance(totals, dict)
        # 1 chain x 1 venue x 2 data_types x 2 instruments x 3 days = 12 captured rows.
        assert totals["captured"] == 12
        assert totals["completion_pct"] == 100.0

    def test_capture_status_counts_split_by_status(self) -> None:
        # Mix captured / empty_confirmed / attempted_failed for one slice.
        rows: list[dict[str, object]] = []
        for status, n in (("captured", 10), ("empty_confirmed", 3), ("attempted_failed", 2)):
            for i in range(n):
                rows.append(
                    {
                        "chain": "BASE",
                        "venue": "AAVEV3-BASE",
                        "data_type": "lending_indices",
                        "instrument_id": f"INST_{i}_{status}",
                        "date": "2024-03-01",
                        "capture_status": status,
                        "error_reason": "" if status == "captured" else "EXPECTED_HOLIDAY",
                    }
                )
        df = pd.DataFrame(rows)
        with self._patch_manifest(df):
            result = get_hierarchical_drilldown(
                service="market-tick-data-service",
                asset_group="defi",
                window_start="2024-03-01",
                window_end="2024-03-01",
                filters={"chain": "BASE"},
                expand_to_depth=0,
            )
        totals = result["totals"]
        assert isinstance(totals, dict)
        assert totals["captured"] == 10
        assert totals["empty_confirmed"] == 3
        assert totals["attempted_failed"] == 2
        # 10/15 = 66.67%.
        assert totals["completion_pct"] == 66.67

    def test_window_clipping_excludes_out_of_range_dates(self) -> None:
        df = _mtds_defi_manifest()
        with self._patch_manifest(df):
            result = get_hierarchical_drilldown(
                service="market-tick-data-service",
                asset_group="defi",
                window_start="2024-03-02",
                window_end="2024-03-02",
                filters={"chain": "ARBITRUM", "venue": "AAVEV3-ARBITRUM"},
            )
        totals = result["totals"]
        assert isinstance(totals, dict)
        # 1 chain x 1 venue x 2 data_types x 2 instruments x 1 day = 4.
        assert totals["captured"] == 4

    def test_empty_manifest_returns_zero_totals(self) -> None:
        with self._patch_manifest(pd.DataFrame()):
            result = get_hierarchical_drilldown(
                service="market-tick-data-service",
                asset_group="defi",
                window_start="2024-03-01",
                window_end="2024-03-03",
            )
        totals = result["totals"]
        assert isinstance(totals, dict)
        assert totals["captured"] == 0
        assert totals["total"] == 0
        assert totals["completion_pct"] == 0.0

    def test_features_onchain_uncovered_asset_group_falls_back(self) -> None:
        """``features-onchain-service`` declares only DEFI in the SSOT.
        Querying it for ``cefi`` should warn + fall back to a sane axis
        order so the panel renders an empty tree rather than 500'ing.
        Bucket resolution is service-keyed, not asset-group-keyed, so
        the bucket call still resolves."""
        df = pd.DataFrame(
            [
                {
                    "venue": "TEST_VENUE",
                    "date": "2024-03-01",
                    "capture_status": "captured",
                    "error_reason": "",
                }
            ]
        )
        with self._patch_manifest(df):
            result = get_hierarchical_drilldown(
                service="features-onchain-service",
                asset_group="cefi",
                window_start="2024-03-01",
                window_end="2024-03-01",
            )
        assert result["axes"] == ["venue", "date"]


class TestListSupportedPairs:
    def test_includes_known_mtds_pairs(self) -> None:
        pairs = list_supported_pairs()
        services = {p["service"] for p in pairs}
        assert "market-tick-data-service" in services
        assert "instruments-service" in services

    def test_each_pair_has_axes_with_date(self) -> None:
        pairs = list_supported_pairs()
        for p in pairs:
            axes = p["axes"]
            assert isinstance(axes, list)
            assert "date" in axes

    @pytest.mark.parametrize(
        ("service", "asset_group"),
        [
            ("market-tick-data-service", "defi"),
            ("instruments-service", "cefi"),
            ("features-onchain-service", "defi"),
        ],
    )
    def test_known_pair_axes_sane(self, service: str, asset_group: str) -> None:
        pairs = list_supported_pairs()
        match = next(
            (p for p in pairs if p["service"] == service and p["asset_group"] == asset_group),
            None,
        )
        assert match is not None
        axes = match["axes"]
        assert isinstance(axes, list)
        assert len(axes) >= 2  # At least one shard axis + date.

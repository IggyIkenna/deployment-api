"""Tests for the hierarchical shard-atom drill-down builder.

Plan: ``data_status_drilldown_shard_atom_alignment_2026_05_07`` Phase 1.
"""

from __future__ import annotations

from typing import ClassVar
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
        ("ARBITRUM", "AAVE_V3-ARBITRUM"),
        ("ARBITRUM", "UNISWAP_V3-ARBITRUM"),
        ("BASE", "AAVE_V3-BASE"),
        ("BASE", "UNISWAP_V3-BASE"),
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


_DRILLDOWN_NODE_GOLDEN_KEYS: frozenset[str] = frozenset(
    {
        "axis",
        "value",
        "captured",
        "empty_confirmed",
        "attempted_failed",
        "expected_unattempted",
        "total",
        "completion_pct",
        "row_key",
        # G3/M5: per-(pipeline_mode, source) breakdown at shard-atom leaves.
        "provenance",
        "children",
        "is_leaf",
    }
)


class TestDrilldownNodeShape:
    def test_completion_pct_is_captured_plus_empty_over_total(self) -> None:
        # Drilldown-only metric (operator 2026-07-15): empty_confirmed counts as a
        # COVERED answer so it does not drag the ratio down → (captured+empty)/total.
        node = DrilldownNode(
            axis="chain",
            value="ARBITRUM",
            captured=80,
            empty_confirmed=15,
            attempted_failed=5,
        )
        assert node.total == 100
        assert node.completion_pct == 95.0  # (80+15) / (80+15+5)

    def test_to_dict_marks_leaves(self) -> None:
        leaf = DrilldownNode(axis="date", value="2024-03-01", captured=1)
        assert leaf.to_dict()["is_leaf"] is True
        parent = DrilldownNode(axis="venue", value="AAVE_V3-ARBITRUM", children=[leaf])
        d = parent.to_dict()
        assert d["is_leaf"] is False
        children = d["children"]
        assert isinstance(children, list)
        assert len(children) == 1

    def test_to_dict_golden_schema_has_exactly_twelve_keys(self) -> None:
        # B2: the schema carries the 4th capture-status bin
        # (expected_unattempted) so genuinely-missing cells are visible.
        # G3/M5: + the ``provenance`` key (per-mode/source leaf breakdown).
        node = DrilldownNode(axis="venue", value="BINANCE", captured=5)
        d = node.to_dict()
        assert len(_DRILLDOWN_NODE_GOLDEN_KEYS) == 12
        assert "expected_unattempted" in _DRILLDOWN_NODE_GOLDEN_KEYS
        assert "provenance" in _DRILLDOWN_NODE_GOLDEN_KEYS
        assert set(d.keys()) == _DRILLDOWN_NODE_GOLDEN_KEYS, (
            f"to_dict() key set drifted. Missing: {_DRILLDOWN_NODE_GOLDEN_KEYS - set(d.keys())}. "
            f"Extra: {set(d.keys()) - _DRILLDOWN_NODE_GOLDEN_KEYS}."
        )

    def test_expected_unattempted_counts_in_total_and_dilutes_completion(self) -> None:
        # B2 + B3: the 4th bin counts toward total + the completion denominator.
        node = DrilldownNode(
            axis="chain",
            value="ARBITRUM",
            captured=80,
            empty_confirmed=10,
            attempted_failed=5,
            expected_unattempted=5,
        )
        assert node.total == 100  # all four bins
        # expected_unattempted still DILUTES: (80+10)/(80+10+5+5)=90 < (80+10)/(80+10+5)=94.7.
        assert node.completion_pct == 90.0  # (80+10) / (80+10+5+5)
        d = node.to_dict()
        assert isinstance(d["expected_unattempted"], int)
        assert d["expected_unattempted"] == 5

    def test_to_dict_axis_is_str(self) -> None:
        d = DrilldownNode(axis="chain", value="ARBITRUM").to_dict()
        assert isinstance(d["axis"], str)

    def test_to_dict_value_is_str(self) -> None:
        d = DrilldownNode(axis="chain", value="ARBITRUM").to_dict()
        assert isinstance(d["value"], str)

    def test_to_dict_captured_is_int(self) -> None:
        d = DrilldownNode(axis="venue", value="X", captured=3).to_dict()
        assert isinstance(d["captured"], int)

    def test_to_dict_empty_confirmed_is_int(self) -> None:
        d = DrilldownNode(axis="venue", value="X", empty_confirmed=2).to_dict()
        assert isinstance(d["empty_confirmed"], int)

    def test_to_dict_attempted_failed_is_int(self) -> None:
        d = DrilldownNode(axis="venue", value="X", attempted_failed=1).to_dict()
        assert isinstance(d["attempted_failed"], int)

    def test_to_dict_total_is_int_and_sum(self) -> None:
        node = DrilldownNode(axis="venue", value="X", captured=4, empty_confirmed=2, attempted_failed=1)
        d = node.to_dict()
        assert isinstance(d["total"], int)
        assert d["total"] == 7

    def test_to_dict_completion_pct_is_float(self) -> None:
        node = DrilldownNode(axis="venue", value="X", captured=80, empty_confirmed=15, attempted_failed=5)
        d = node.to_dict()
        assert isinstance(d["completion_pct"], float)
        assert d["completion_pct"] == 95.0  # (80+15) / (80+15+5)

    def test_to_dict_row_key_is_dict(self) -> None:
        node = DrilldownNode(axis="venue", value="X", row_key={"venue": "X", "date": "2024-03-01"})
        d = node.to_dict()
        assert isinstance(d["row_key"], dict)

    def test_to_dict_row_key_default_is_empty_dict(self) -> None:
        d = DrilldownNode(axis="venue", value="X").to_dict()
        assert d["row_key"] == {}

    def test_to_dict_children_is_list(self) -> None:
        d = DrilldownNode(axis="venue", value="X").to_dict()
        assert isinstance(d["children"], list)

    def test_to_dict_children_are_dicts(self) -> None:
        child = DrilldownNode(axis="date", value="2024-03-01", captured=1)
        parent = DrilldownNode(axis="venue", value="X", children=[child])
        d = parent.to_dict()
        for c in d["children"]:
            assert isinstance(c, dict)

    def test_to_dict_children_have_golden_keys(self) -> None:
        child = DrilldownNode(axis="date", value="2024-03-01", captured=1)
        parent = DrilldownNode(axis="venue", value="X", children=[child])
        d = parent.to_dict()
        assert len(d["children"]) == 1
        child_dict = d["children"][0]
        assert set(child_dict.keys()) == _DRILLDOWN_NODE_GOLDEN_KEYS

    def test_to_dict_is_leaf_true_when_no_children(self) -> None:
        d = DrilldownNode(axis="date", value="2024-03-01").to_dict()
        assert d["is_leaf"] is True

    def test_to_dict_is_leaf_false_when_has_children(self) -> None:
        child = DrilldownNode(axis="date", value="2024-03-01")
        parent = DrilldownNode(axis="venue", value="X", children=[child])
        assert parent.to_dict()["is_leaf"] is False

    def test_to_dict_completion_pct_zero_when_total_zero(self) -> None:
        d = DrilldownNode(axis="venue", value="X").to_dict()
        assert d["total"] == 0
        assert d["completion_pct"] == 0.0


class TestAggregateCountsLegacyCoercion:
    """_aggregate_counts must coerce legacy non-4-state capture_status to captured.

    Regression (data-status audit 2026-06-18): a legacy v4 row whose
    capture_status is a literal ``"None"`` / NaN / blank survived
    ``.astype(str)`` and was DROPPED from every drilldown-tree count, under-
    reporting the tree denominator (sports prd carried 17,288 such rows). The
    fix coerces any non-4-state token to ``captured`` (SSOT parity with
    ``coverage_metrics.compute_capture_status_counts`` + UTL lookup)."""

    def test_literal_none_and_nan_coerce_to_captured(self) -> None:
        df = pd.DataFrame(
            {
                "capture_status": ["captured", "None", "nan", "empty_confirmed", "attempted_failed"],
            }
        )
        captured, empty, failed, exp = _hier._aggregate_counts(df)  # pyright: ignore[reportPrivateUsage]
        # 1 real captured + "None" + "nan" = 3 captured (not 1).
        assert captured == 3
        assert empty == 1
        assert failed == 1
        assert exp == 0

    def test_real_nan_value_coerces_to_captured(self) -> None:
        df = pd.DataFrame({"capture_status": ["captured", None, "empty_confirmed"]})
        captured, empty, failed, exp = _hier._aggregate_counts(df)  # pyright: ignore[reportPrivateUsage]
        assert captured == 2  # explicit captured + the None/NaN row
        assert empty == 1
        assert failed == 0
        assert exp == 0


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
                filters={"chain": "ARBITRUM", "venue": "AAVE_V3-ARBITRUM"},
                expand_to_depth=10,
            )
        totals = result["totals"]
        assert isinstance(totals, dict)
        # 1 chain x 1 venue x 2 data_types x 2 instruments x 3 days = 12 captured rows.
        assert totals["captured"] == 12
        assert totals["completion_pct"] == 100.0

    def test_capture_status_counts_split_by_status(self) -> None:
        # B2: mix all FOUR capture-status bins for one slice, incl. the 4th
        # (expected_unattempted) — genuinely-missing cells must surface in the tree.
        rows: list[dict[str, object]] = []
        for status, n in (
            ("captured", 10),
            ("empty_confirmed", 3),
            ("attempted_failed", 2),
            ("expected_unattempted", 5),
        ):
            for i in range(n):
                rows.append(
                    {
                        "chain": "BASE",
                        "venue": "AAVE_V3-BASE",
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
        assert totals["expected_unattempted"] == 5
        # Drilldown metric (operator 2026-07-15): empty_confirmed counts as covered →
        # (10+3) / (10+3+2+5) = 65.0%. Denominator still includes all 4 bins.
        assert totals["completion_pct"] == 65.0

    def test_window_clipping_excludes_out_of_range_dates(self) -> None:
        df = _mtds_defi_manifest()
        with self._patch_manifest(df):
            result = get_hierarchical_drilldown(
                service="market-tick-data-service",
                asset_group="defi",
                window_start="2024-03-02",
                window_end="2024-03-02",
                filters={"chain": "ARBITRUM", "venue": "AAVE_V3-ARBITRUM"},
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


class TestPaginationAndBundledRootVirtualisation:
    """Phase 6 (operator finding 2026-05-07): per-instrument pagination
    + bundled-data_type root virtualisation. Plan:
    ``data_status_drilldown_shard_atom_alignment_2026_05_07.md``.
    """

    def _patch_manifest(self, df: pd.DataFrame):
        return patch.object(_hier, "read_availability_index", return_value=df)

    def _per_instrument_perp_manifest(self, n_instruments: int) -> pd.DataFrame:
        """N PERPETUAL instruments under one (venue, data_type, day)."""
        rows: list[dict[str, object]] = []
        for i in range(n_instruments):
            rows.append(
                {
                    "venue": "BINANCE-FUTURES",
                    "data_type": "trades",
                    "instrument_type": "PERPETUAL",
                    "instrument_id": f"INST{i:04d}USDT",
                    "underlying": "",
                    "chain": "",
                    "date": "2024-03-01",
                    "capture_status": "captured",
                    "error_reason": "",
                }
            )
        return pd.DataFrame(rows)

    def _bundled_options_manifest(self) -> pd.DataFrame:
        """Deribit options_chain — instrument_id empty, underlying populated."""
        rows: list[dict[str, object]] = []
        for root in ("BTC", "ETH", "SOL"):
            for day in ("2024-03-01", "2024-03-02"):
                rows.append(
                    {
                        "venue": "DERIBIT",
                        "data_type": "options_chain",
                        "instrument_type": "options_chain",
                        "instrument_id": "",  # bundled — empty per writer contract
                        "underlying": root,
                        "chain": "",
                        "date": day,
                        "capture_status": "captured",
                        "error_reason": "",
                    }
                )
        return pd.DataFrame(rows)

    # MTDS CeFi shard axes after Phase 6 reorder: (venue, data_type,
    # instrument_type, instrument_id). To put ``instrument_id`` at the
    # head of the returned tree, filter through every prior axis.
    _PERP_PREFIX_FILTERS: ClassVar[dict[str, str]] = {
        "venue": "BINANCE-FUTURES",
        "data_type": "trades",
        "instrument_type": "PERPETUAL",
    }

    def test_total_top_axis_children_reports_full_count(self) -> None:
        """``total_top_axis_children`` is the unfiltered child count at the
        head axis - UI uses it to render "showing N-M of T"."""
        df = self._per_instrument_perp_manifest(n_instruments=750)
        with self._patch_manifest(df):
            result = get_hierarchical_drilldown(
                service="market-tick-data-service",
                asset_group="cefi",
                window_start="2024-03-01",
                window_end="2024-03-01",
                filters=dict(self._PERP_PREFIX_FILTERS),
                expand_to_depth=0,
                child_limit=200,
            )
        # 750 instruments under the (venue, data_type, instrument_type) filter.
        assert result["total_top_axis_children"] == 750
        tree = result["tree"]
        assert isinstance(tree, list)
        assert len(tree) == 200
        assert result["child_offset"] == 0
        assert result["child_limit"] == 200

    def test_pagination_offset_returns_next_slice(self) -> None:
        """``child_offset=200, child_limit=200`` returns instruments 200-399."""
        df = self._per_instrument_perp_manifest(n_instruments=750)
        with self._patch_manifest(df):
            page2 = get_hierarchical_drilldown(
                service="market-tick-data-service",
                asset_group="cefi",
                window_start="2024-03-01",
                window_end="2024-03-01",
                filters=dict(self._PERP_PREFIX_FILTERS),
                expand_to_depth=0,
                child_offset=200,
                child_limit=200,
            )
        tree = page2["tree"]
        assert isinstance(tree, list)
        assert len(tree) == 200
        # Sorted alphabetically — page 2 starts at INST0200USDT.
        first = tree[0]
        assert isinstance(first, dict)
        assert first["value"] == "INST0200USDT"

    def test_pagination_last_partial_page(self) -> None:
        """750 instruments, page size 200 → page 4 has 150 items."""
        df = self._per_instrument_perp_manifest(n_instruments=750)
        with self._patch_manifest(df):
            page4 = get_hierarchical_drilldown(
                service="market-tick-data-service",
                asset_group="cefi",
                window_start="2024-03-01",
                window_end="2024-03-01",
                filters=dict(self._PERP_PREFIX_FILTERS),
                expand_to_depth=0,
                child_offset=600,
                child_limit=200,
            )
        tree = page4["tree"]
        assert isinstance(tree, list)
        assert len(tree) == 150

    def test_no_limit_returns_full_list(self) -> None:
        """``child_limit=None`` (default) returns every child up to the
        per-node cap (10_000)."""
        df = self._per_instrument_perp_manifest(n_instruments=750)
        with self._patch_manifest(df):
            result = get_hierarchical_drilldown(
                service="market-tick-data-service",
                asset_group="cefi",
                window_start="2024-03-01",
                window_end="2024-03-01",
                filters=dict(self._PERP_PREFIX_FILTERS),
                expand_to_depth=0,
            )
        tree = result["tree"]
        assert isinstance(tree, list)
        assert len(tree) == 750  # No truncation.
        assert result["total_top_axis_children"] == 750

    def test_bundled_options_chain_surfaces_underlying_as_instrument_id(self) -> None:
        """Bundled ``options_chain`` rows leave ``instrument_id`` empty;
        the read-side virtualisation promotes ``underlying`` so the
        per-instrument level shows BTC / ETH / SOL roots — matches the
        codex shard atom ``(venue, data_type, options_chain, root, day)``."""
        df = self._bundled_options_manifest()
        with self._patch_manifest(df):
            result = get_hierarchical_drilldown(
                service="market-tick-data-service",
                asset_group="cefi",
                window_start="2024-03-01",
                window_end="2024-03-02",
                filters={
                    "venue": "DERIBIT",
                    "data_type": "options_chain",
                    "instrument_type": "options_chain",
                },
                expand_to_depth=10,
            )
        tree = result["tree"]
        assert isinstance(tree, list)
        # 3 distinct underlyings → 3 root nodes at the instrument_id axis.
        values = sorted(n["value"] for n in tree if isinstance(n, dict) and isinstance(n.get("value"), str))
        assert values == ["BTC", "ETH", "SOL"]

    def test_per_instrument_rows_unchanged_by_virtualisation(self) -> None:
        """Per-instrument rows (instrument_id populated) must NOT be
        rewritten — the virtualisation only fills empty instrument_id
        from underlying, leaving real values alone."""
        df = self._per_instrument_perp_manifest(n_instruments=5)
        with self._patch_manifest(df):
            result = get_hierarchical_drilldown(
                service="market-tick-data-service",
                asset_group="cefi",
                window_start="2024-03-01",
                window_end="2024-03-01",
                filters=dict(self._PERP_PREFIX_FILTERS),
                expand_to_depth=10,
            )
        tree = result["tree"]
        assert isinstance(tree, list)
        values = sorted(n["value"] for n in tree if isinstance(n, dict) and isinstance(n.get("value"), str))
        assert values == [f"INST{i:04d}USDT" for i in range(5)]


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
            # features-onchain-service consolidated into features-service in UAC
            # data_status_axis_matrix.py; use the canonical service name.
            ("features-service", "defi"),
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


# ---------------------------------------------------------------------------
# Contract regression tests — catch the bugs Playwright surfaced 2026-05-07.
# Pattern: rather than mocking the dependency to return a static df + asserting
# the response shape, these tests assert the CALL CONTRACT to the dependency.
# That's how we catch the "passed gs:// URI to a function expecting a bare
# bucket name" bug class without needing live GCS or browser exercise.
# ---------------------------------------------------------------------------


class TestReadAvailabilityIndexCallContract:
    """Regression: 2026-05-07 incident — ``read_availability_index`` was being
    called with ``"gs://{bucket}/_index/availability_index.parquet"`` instead
    of the bare bucket name. UTL silently returned ``DataFrame(0, 30)`` so the
    UI rendered "No data for cefi" despite 1M+ captured shards. The mocked-
    df-and-assert-shape tests above didn't catch this because the mock
    accepts any positional arg.

    Fix: assert the actual argument passed matches the bare-bucket-name
    signature (no scheme, no path, no slashes).
    """

    def test_passes_bare_bucket_name_not_gs_uri(self) -> None:
        captured_args: list[tuple] = []

        def fake_read_availability_index(*args, **kwargs):
            captured_args.append((args, kwargs))
            return pd.DataFrame()

        with patch.object(_hier, "read_availability_index", side_effect=fake_read_availability_index):
            get_hierarchical_drilldown(
                service="market-tick-data-service",
                asset_group="defi",
                window_start="2024-03-01",
                window_end="2024-03-03",
            )

        assert len(captured_args) >= 1, "read_availability_index was not called"
        positional, _kw = captured_args[0]
        assert len(positional) >= 1, "read_availability_index expects a positional arg"
        bucket_arg = positional[0]
        assert isinstance(bucket_arg, str), f"read_availability_index expects str, got {type(bucket_arg)}"
        # The bare bucket name has no scheme or slashes. A gs:// URI or a
        # path-suffixed bucket-and-prefix string both fail this check.
        assert "://" not in bucket_arg, (
            f"read_availability_index called with URI {bucket_arg!r}; "
            "expects bare bucket name (no scheme). 2026-05-07 incident."
        )
        assert "/" not in bucket_arg, (
            f"read_availability_index called with path {bucket_arg!r}; expects bare bucket name (no path)."
        )

    def test_passes_canonical_bucket_for_mtds_cefi(self) -> None:
        """Cross-check the bucket NAME matches the project's canonical
        template — `market-data-tick-{cat}-{pid}`. Catches both the URI
        bug AND any future drift where the bucket-name builder changes
        without the drill-down following along."""
        captured: list[str] = []

        def fake(*args, **kwargs):
            if args:
                captured.append(args[0])
            return pd.DataFrame()

        with patch.object(_hier, "read_availability_index", side_effect=fake):
            get_hierarchical_drilldown(
                service="market-tick-data-service",
                asset_group="cefi",
                window_start="2024-03-01",
                window_end="2024-03-03",
            )

        assert len(captured) >= 1
        bucket = captured[0]
        # MTDS CeFi bucket = "market-data-tick-cefi-{pid}"
        assert bucket.startswith("market-data-tick-cefi-"), f"unexpected bucket {bucket!r}"


class TestRowKeyShapeAtEachDepth:
    """Regression: 2026-05-07 incident — the UI's DeployMissingButton
    rendered on every captured=0 ``is_leaf`` node, including intermediate
    venue-level nodes whose row_key only carried ``{venue: X}``. The
    /deploy-missing-preview endpoint then 400'd on the missing data_type
    and day fields.

    Fix surface: the leaf-row's row_key carries every parent axis value
    AT THE DEPTH MATCHING THE NODE. UI consumers can then gate
    DeployMissingButton on (venue + data_type + day) presence.

    The render-gate fix lives in deployment-ui (HierarchicalShardDrilldown);
    these tests pin the BACKEND contract so the row_key shape doesn't
    silently regress and force the UI gate to drift.
    """

    def _mtds_cefi_manifest(self) -> pd.DataFrame:
        rows: list[dict[str, object]] = []
        for venue in ("BINANCE-FUTURES",):
            for dt_name in ("trades", "book_snapshot_5"):
                for inst in ("btcusdt",):
                    rows.append(
                        {
                            "venue": venue,
                            "data_type": dt_name,
                            "instrument_type": "PERPETUAL",
                            "instrument_id": inst,
                            "date": "2024-03-04",
                            "capture_status": "captured",
                            "error_reason": "",
                        }
                    )
        return pd.DataFrame(rows)

    def test_top_level_node_has_only_top_axis_in_row_key(self) -> None:
        """A venue-level node's row_key carries ONLY {venue}. The UI must
        not emit Deploy-Missing on this node because the shard atom is
        incomplete (missing data_type + day)."""
        df = self._mtds_cefi_manifest()
        with patch.object(_hier, "read_availability_index", return_value=df):
            result = get_hierarchical_drilldown(
                service="market-tick-data-service",
                asset_group="cefi",
                window_start="2024-03-04",
                window_end="2024-03-04",
                expand_to_depth=0,
            )
        tree = result["tree"]
        assert tree, "expected at least one top-level node"
        first = tree[0]
        assert first["axis"] == "venue"
        # row_key carries ONLY {venue}; no data_type, no day yet.
        rk = first["row_key"]
        assert "venue" in rk
        assert "data_type" not in rk
        assert "day" not in rk and "date" not in rk

    def test_leaf_node_carries_full_shard_atom(self) -> None:
        """A date-level leaf carries the full shard atom (venue +
        data_type + day at minimum). The UI Deploy-Missing render gate
        relies on this contract."""
        df = self._mtds_cefi_manifest()
        with patch.object(_hier, "read_availability_index", return_value=df):
            result = get_hierarchical_drilldown(
                service="market-tick-data-service",
                asset_group="cefi",
                window_start="2024-03-04",
                window_end="2024-03-04",
                expand_to_depth=10,  # materialise the full tree to the leaf.
            )

        # Walk to a date-level leaf.
        def _walk_to_leaves(nodes, leaves):
            for n in nodes:
                if not n["children"]:
                    leaves.append(n)
                else:
                    _walk_to_leaves(n["children"], leaves)

        leaves: list[dict] = []
        _walk_to_leaves(result["tree"], leaves)
        assert leaves, "expected at least one leaf"

        # Find a date/day leaf.
        date_leaves = [leaf for leaf in leaves if leaf["axis"] in ("date", "day")]
        assert date_leaves, "expected at least one date-axis leaf"
        rk = date_leaves[0]["row_key"]
        # The full shard-atom requirement that the UI render-gate checks.
        assert rk.get("venue"), f"leaf row_key missing venue: {rk}"
        assert rk.get("data_type"), f"leaf row_key missing data_type: {rk}"
        assert rk.get("day") or rk.get("date"), f"leaf row_key missing day/date: {rk}"


# ---------------------------------------------------------------------------
# feature_family axis (Phase 8B — features-repo consolidation)
# ---------------------------------------------------------------------------


def _features_onchain_manifest() -> pd.DataFrame:
    """Mixed-family manifest exercising stamping + filter behaviour.

    Two onchain rows (one pre-stamped, one blank → UAC-fill) plus two
    volatility-family rows pre-stamped on disk. Volatility rows use a
    placeholder ``feature_group`` that is NOT yet in
    ``FEATURE_GROUP_TO_FAMILY`` — this is the realistic case for a
    family the registry doesn't yet enumerate; pre-stamped values must
    survive the read-side stamper unchanged.
    """
    rows: list[dict[str, object]] = [
        # onchain rows — pre-stamped feature_family.
        {
            "venue": "LIDO",
            "chain": "ETHEREUM",
            "feature_group": "lst_staking_yields",
            "feature_family": "onchain",
            "timeframe": "1d",
            "instrument_id": "stETH",
            "date": "2024-03-01",
            "capture_status": "captured",
            "error_reason": "",
        },
        {
            "venue": "AAVE_V3-ARBITRUM",
            "chain": "ARBITRUM",
            "feature_group": "aave_lending_rates",
            "feature_family": "",  # missing — fallback should fill via UAC.
            "timeframe": "1h",
            "instrument_id": "USDC",
            "date": "2024-03-01",
            "capture_status": "captured",
            "error_reason": "",
        },
        # volatility-family rows — pre-stamped (writer-stamped). The
        # placeholder feature_group is not in FEATURE_GROUP_TO_FAMILY,
        # so the read-side stamper has nothing to fill — it must leave
        # the pre-stamped value alone.
        {
            "venue": "DERIBIT",
            "chain": "",
            "feature_group": "placeholder_vol_group",
            "feature_family": "volatility",
            "timeframe": "1m",
            "instrument_id": "BTC",
            "date": "2024-03-01",
            "capture_status": "captured",
            "error_reason": "",
        },
        {
            "venue": "DERIBIT",
            "chain": "",
            "feature_group": "placeholder_vol_group",
            "feature_family": "volatility",
            "timeframe": "1m",
            "instrument_id": "ETH",
            "date": "2024-03-02",
            "capture_status": "empty_confirmed",
            "error_reason": "EXPECTED_HOLIDAY",
        },
    ]
    return pd.DataFrame(rows)


class TestFeatureFamilyAxis:
    def test_stamper_fills_blank_feature_family_via_uac(self) -> None:
        df = _features_onchain_manifest()
        stamped = _hier._stamp_feature_family(df)  # pyright: ignore[reportPrivateUsage]
        # Pre-stamped rows preserve their write-time value (writer wins).
        lst = stamped[stamped["feature_group"] == "lst_staking_yields"].iloc[0]
        assert lst["feature_family"] == "onchain"
        # Read-side UAC fallback fills the blank aave_lending_rates row.
        aave = stamped[stamped["feature_group"] == "aave_lending_rates"].iloc[0]
        assert aave["feature_family"] == "onchain"
        # Pre-stamped volatility rows survive even though the placeholder
        # feature_group has no UAC mapping (writer is the SSOT).
        vol = stamped[stamped["instrument_id"] == "BTC"].iloc[0]
        assert vol["feature_family"] == "volatility"

    def test_stamper_handles_manifest_without_feature_group_column(self) -> None:
        # MTDS / instruments-service manifests have no feature_group →
        # stamper is a no-op.
        df = pd.DataFrame(
            {
                "venue": ["BINANCE"],
                "data_type": ["trades"],
                "instrument_id": ["BTCUSDT"],
                "date": ["2024-03-01"],
                "capture_status": ["captured"],
                "error_reason": [""],
            }
        )
        stamped = _hier._stamp_feature_family(df)  # pyright: ignore[reportPrivateUsage]
        assert "feature_family" not in stamped.columns

    def test_filter_by_feature_family_narrows_to_one_family(self) -> None:
        df = _features_onchain_manifest()
        with patch.object(_hier, "read_availability_index", return_value=df):
            result = get_hierarchical_drilldown(
                service="features-onchain-service",
                asset_group="defi",
                window_start="2024-03-01",
                window_end="2024-03-02",
                filters={"feature_family": "onchain"},
            )

        # Only the 2 onchain rows should be counted (the 2 volatility
        # rows were excluded by the feature_family filter).
        totals = result["totals"]
        assert totals["captured"] == 2
        assert totals["empty_confirmed"] == 0
        assert totals["attempted_failed"] == 0

    def test_filter_by_feature_family_volatility(self) -> None:
        df = _features_onchain_manifest()
        with patch.object(_hier, "read_availability_index", return_value=df):
            result = get_hierarchical_drilldown(
                service="features-volatility-service",
                asset_group="cefi",
                window_start="2024-03-01",
                window_end="2024-03-02",
                filters={"feature_family": "volatility"},
            )
        totals = result["totals"]
        # 1 captured (BTC iv_surface day 1) + 1 empty_confirmed
        # (ETH iv_surface day 2) — both volatility-family.
        assert totals["captured"] == 1
        assert totals["empty_confirmed"] == 1
        assert totals["attempted_failed"] == 0

    def test_no_feature_family_filter_returns_full_aggregate(self) -> None:
        df = _features_onchain_manifest()
        with patch.object(_hier, "read_availability_index", return_value=df):
            result = get_hierarchical_drilldown(
                service="features-onchain-service",
                asset_group="defi",
                window_start="2024-03-01",
                window_end="2024-03-02",
                filters={},
            )
        # All 4 rows aggregated when filter omitted.
        totals = result["totals"]
        assert totals["captured"] + totals["empty_confirmed"] == 4

    def test_filter_unknown_feature_family_yields_zero(self) -> None:
        df = _features_onchain_manifest()
        with patch.object(_hier, "read_availability_index", return_value=df):
            result = get_hierarchical_drilldown(
                service="features-onchain-service",
                asset_group="defi",
                window_start="2024-03-01",
                window_end="2024-03-02",
                filters={"feature_family": "not_a_real_family"},
            )
        totals = result["totals"]
        assert totals["captured"] == 0
        assert totals["empty_confirmed"] == 0
        assert totals["attempted_failed"] == 0


# ---------------------------------------------------------------------------
# PIECE A — data-status drilldown DISPLAY canonicalisation (2026-07-18).
# instrument_type merge (UAC InstrumentType legacy map + honest UNKNOWN
# sentinel) + registry-removed-venue exclusion + bare-vs-chain venue collapse.
# ---------------------------------------------------------------------------


class TestInstrumentTypeCanonicalisationUnit:
    """Unit coverage for the pure ``_canonicalise_instrument_type`` mapper."""

    def test_legacy_lowercase_spot_maps_to_spot_pair(self) -> None:
        assert _hier._canonicalise_instrument_type("spot") == "SPOT_PAIR"  # pyright: ignore[reportPrivateUsage]

    def test_already_canonical_spot_pair_is_unchanged(self) -> None:
        assert _hier._canonicalise_instrument_type("SPOT_PAIR") == "SPOT_PAIR"  # pyright: ignore[reportPrivateUsage]
        assert _hier._canonicalise_instrument_type("spot_pair") == "SPOT_PAIR"  # pyright: ignore[reportPrivateUsage]

    def test_perp_and_perpetual_both_map_to_perpetual(self) -> None:
        assert _hier._canonicalise_instrument_type("perp") == "PERPETUAL"  # pyright: ignore[reportPrivateUsage]
        assert _hier._canonicalise_instrument_type("perpetual") == "PERPETUAL"  # pyright: ignore[reportPrivateUsage]
        assert _hier._canonicalise_instrument_type("PERPETUAL") == "PERPETUAL"  # pyright: ignore[reportPrivateUsage]

    def test_futures_chain_and_future_both_map_to_future(self) -> None:
        assert _hier._canonicalise_instrument_type("futures_chain") == "FUTURE"  # pyright: ignore[reportPrivateUsage]
        assert _hier._canonicalise_instrument_type("FUTURE") == "FUTURE"  # pyright: ignore[reportPrivateUsage]

    def test_blank_none_and_literal_none_all_map_to_unknown(self) -> None:
        assert _hier._canonicalise_instrument_type("") == "UNKNOWN"  # pyright: ignore[reportPrivateUsage]
        assert _hier._canonicalise_instrument_type("None") == "UNKNOWN"  # pyright: ignore[reportPrivateUsage]
        assert _hier._canonicalise_instrument_type("nan") == "UNKNOWN"  # pyright: ignore[reportPrivateUsage]

    def test_lending_market_and_yield_legacy_names(self) -> None:
        assert _hier._canonicalise_instrument_type("lending_market") == "LENDING"  # pyright: ignore[reportPrivateUsage]
        assert _hier._canonicalise_instrument_type("yield") == "YIELD_BEARING"  # pyright: ignore[reportPrivateUsage]


def _instrument_type_variant_manifest() -> pd.DataFrame:
    """One venue/data_type/day with 4 raw instrument_type spellings that
    canonicalise to 2 real buckets (SPOT_PAIR merges 3; blank -> UNKNOWN)."""
    rows: list[dict[str, object]] = [
        {
            "venue": "COINBASE-SPOT",
            "data_type": "trades",
            "instrument_type": "",
            "instrument_id": "X1",
            "date": "2024-03-01",
            "capture_status": "captured",
            "error_reason": "",
        },
        {
            "venue": "COINBASE-SPOT",
            "data_type": "trades",
            "instrument_type": "SPOT_PAIR",
            "instrument_id": "X2",
            "date": "2024-03-01",
            "capture_status": "captured",
            "error_reason": "",
        },
        {
            "venue": "COINBASE-SPOT",
            "data_type": "trades",
            "instrument_type": "spot",
            "instrument_id": "X3",
            "date": "2024-03-01",
            "capture_status": "empty_confirmed",
            "error_reason": "",
        },
        {
            "venue": "COINBASE-SPOT",
            "data_type": "trades",
            "instrument_type": "spot_pair",
            "instrument_id": "X4",
            "date": "2024-03-01",
            "capture_status": "attempted_failed",
            "error_reason": "some error",
        },
    ]
    return pd.DataFrame(rows)


class TestInstrumentTypeTreeMerge:
    """Integration: the drilldown TREE merges non-canonical instrument_type
    spellings into one node, SUMS counts, and recomputes completion_pct from
    the summed totals (never averages per-raw-value percentages)."""

    def test_canonical_merge_sums_counts_and_recomputes_pct(self) -> None:
        df = _instrument_type_variant_manifest()
        with patch.object(_hier, "read_availability_index", return_value=df):
            result = get_hierarchical_drilldown(
                service="market-tick-data-service",
                asset_group="cefi",
                window_start="2024-03-01",
                window_end="2024-03-01",
                filters={"venue": "COINBASE-SPOT", "data_type": "trades"},
                expand_to_depth=0,
            )
        tree = result["tree"]
        assert isinstance(tree, list)
        by_value = {n["value"]: n for n in tree if isinstance(n, dict)}
        # 4 raw spellings collapse to exactly 2 display nodes.
        assert set(by_value) == {"SPOT_PAIR", "UNKNOWN"}
        spot_pair = by_value["SPOT_PAIR"]
        # 3 raw variants (SPOT_PAIR/spot/spot_pair) merged: 1 captured +
        # 1 empty_confirmed + 1 attempted_failed — a real SUM, not a count of 1.
        assert spot_pair["captured"] == 1
        assert spot_pair["empty_confirmed"] == 1
        assert spot_pair["attempted_failed"] == 1
        assert spot_pair["total"] == 3
        # Recomputed FROM the summed totals: (1+1)/3*100 = 66.67 — NOT an
        # average of the 3 raw rows' individual (100/0/0)% completion.
        assert spot_pair["completion_pct"] == 66.67
        unknown = by_value["UNKNOWN"]
        assert unknown["captured"] == 1
        assert unknown["total"] == 1
        assert unknown["completion_pct"] == 100.0

    def test_no_captured_rows_are_dropped_by_the_merge(self) -> None:
        """Count-preserving: total captured+empty+failed across the merged
        tree equals the raw row count (nothing silently vanishes)."""
        df = _instrument_type_variant_manifest()
        with patch.object(_hier, "read_availability_index", return_value=df):
            result = get_hierarchical_drilldown(
                service="market-tick-data-service",
                asset_group="cefi",
                window_start="2024-03-01",
                window_end="2024-03-01",
                filters={"venue": "COINBASE-SPOT", "data_type": "trades"},
                expand_to_depth=0,
            )
        totals = result["totals"]
        assert totals["captured"] + totals["empty_confirmed"] + totals["attempted_failed"] == len(df)


def _removed_and_collapsed_venue_manifest() -> pd.DataFrame:
    """DeFi manifest mixing a registry-removed venue, a legit untouched
    venue, and a bare-vs-chain duplicate pair — all in ARBITRUM/SOLANA."""
    rows: list[dict[str, object]] = [
        # Registry-removed (operator ruling 2026-07-16) — must vanish entirely.
        {
            "venue": "DRIFT",
            "chain": "SOLANA",
            "date": "2024-03-01",
            "capture_status": "captured",
            "error_reason": "",
        },
        {
            "venue": "DRIFT-SOLANA",
            "chain": "SOLANA",
            "date": "2024-03-01",
            "capture_status": "captured",
            "error_reason": "",
        },
        # Legit venue — untouched (not in any removed/collapse set).
        {
            "venue": "AAVE_V3-ARBITRUM",
            "chain": "ARBITRUM",
            "date": "2024-03-01",
            "capture_status": "captured",
            "error_reason": "",
        },
        # Bare-vs-chain duplicate — collapses into ONE JITO-SOLANA node.
        {
            "venue": "JITO",
            "chain": "SOLANA",
            "date": "2024-03-01",
            "capture_status": "captured",
            "error_reason": "",
        },
        {
            "venue": "JITO-SOLANA",
            "chain": "SOLANA",
            "date": "2024-03-01",
            "capture_status": "empty_confirmed",
            "error_reason": "",
        },
    ]
    return pd.DataFrame(rows)


class TestVenueAxisDisplayCanonicalisation:
    def test_removed_venue_is_absent_from_tree_and_totals(self) -> None:
        df = _removed_and_collapsed_venue_manifest()
        with patch.object(_hier, "read_availability_index", return_value=df):
            result = get_hierarchical_drilldown(
                service="instruments-service",
                asset_group="defi",
                window_start="2024-03-01",
                window_end="2024-03-01",
                expand_to_depth=0,
            )
        tree = result["tree"]
        assert isinstance(tree, list)
        values = {n["value"] for n in tree if isinstance(n, dict)}
        assert "DRIFT" not in values
        assert "DRIFT-SOLANA" not in values
        # Root totals must NOT count the 2 dropped DRIFT/DRIFT-SOLANA rows —
        # only the 3 remaining (AAVE_V3-ARBITRUM, JITO, JITO-SOLANA) rows.
        assert result["totals"]["captured"] + result["totals"]["empty_confirmed"] == 3

    def test_legit_venue_is_untouched(self) -> None:
        df = _removed_and_collapsed_venue_manifest()
        with patch.object(_hier, "read_availability_index", return_value=df):
            result = get_hierarchical_drilldown(
                service="instruments-service",
                asset_group="defi",
                window_start="2024-03-01",
                window_end="2024-03-01",
                expand_to_depth=0,
            )
        tree = result["tree"]
        assert isinstance(tree, list)
        by_value = {n["value"]: n for n in tree if isinstance(n, dict)}
        assert "AAVE_V3-ARBITRUM" in by_value
        assert by_value["AAVE_V3-ARBITRUM"]["captured"] == 1

    def test_bare_and_chain_venue_collapse_into_one_node_summed(self) -> None:
        df = _removed_and_collapsed_venue_manifest()
        with patch.object(_hier, "read_availability_index", return_value=df):
            result = get_hierarchical_drilldown(
                service="instruments-service",
                asset_group="defi",
                window_start="2024-03-01",
                window_end="2024-03-01",
                expand_to_depth=0,
            )
        tree = result["tree"]
        assert isinstance(tree, list)
        values = {n["value"] for n in tree if isinstance(n, dict)}
        # "JITO" (bare) must NOT survive as its own node — collapsed into
        # the canonical "JITO-SOLANA" node.
        assert "JITO" not in values
        assert "JITO-SOLANA" in values
        by_value = {n["value"]: n for n in tree if isinstance(n, dict)}
        jito = by_value["JITO-SOLANA"]
        # Both the bare-form captured row AND the chain-form empty_confirmed
        # row are SUMMED into the one collapsed node.
        assert jito["captured"] == 1
        assert jito["empty_confirmed"] == 1
        assert jito["total"] == 2
        assert jito["completion_pct"] == 100.0  # (1+1)/2 — empty_confirmed counts as covered.


class TestDropRemovedVenuesHelperUnit:
    def test_drop_removed_venues_is_a_noop_without_venue_column(self) -> None:
        df = pd.DataFrame({"chain": ["ARBITRUM"]})
        out = _hier._drop_removed_venues(df)  # pyright: ignore[reportPrivateUsage]
        assert list(out.columns) == ["chain"]

    def test_drop_removed_venues_matches_base_before_first_hyphen(self) -> None:
        df = pd.DataFrame({"venue": ["ZETA-SOLANA", "FLASH-SOLANA", "MANGO-SOLANA", "PACIFICA", "HYPERLIQUID"]})
        out = _hier._drop_removed_venues(df)  # pyright: ignore[reportPrivateUsage]
        assert list(out["venue"]) == ["HYPERLIQUID"]

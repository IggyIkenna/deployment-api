"""Cat C.3 — sports shard atom (data_type → league_id → day) preserved
through the hierarchical drill-down builder.

Plan: ``data_status_comprehensive_test_coverage_2026_05_07.plan.md``
Phase 1, Item 11 (sports-half of shard-atom alignment).

Sports shard atom for instruments-service (SSOT:
``unified_api_contracts.registry.data_status_axis_matrix.SHARD_AXIS_MATRIX``):
    ("data_type", "league_id")

So the drill-down MUST return axes = ["data_type", "league_id", "date"] and
the tree structure MUST be:
    data_type node
      └── league_id node
            └── date leaf

This test validates:
1. Axes match the UAC SSOT (data_type → league_id → date).
2. A captured FIXTURE_STATS/EPL/2024-09-15 row reaches the tree at the
   correct depth with correct counts.
3. A retired data_type (SFI_LEAGUES) that has ``empty_confirmed`` manifest
   rows still surfaces in the tree — honest coverage is not silently dropped
   after the write-path retirement.
4. Separate data_types produce separate top-level tree nodes, confirming the
   grain is data_type not just league_id.
"""

from __future__ import annotations

from unittest.mock import patch

import pandas as pd
import pytest

from deployment_api.services.data_status_hierarchical import get_hierarchical_drilldown

# ---------------------------------------------------------------------------
# Manifest fixture helpers
# ---------------------------------------------------------------------------

_MANIFEST_COLUMNS: list[str] = [
    "asset_group",
    "venue",
    "chain",
    "data_type",
    "instrument_type",
    "instrument_id",
    "league_id",
    "feature_group",
    "timeframe",
    "model_family",
    "training_period",
    "strategy_id",
    "client_id",
    "instruction_type",
    "canonical_question_group",
    "job_id",
    "archetype",
    "underlying",
    "date",
    "capture_status",
    "error_reason",
]


def _make_sports_manifest(*rows: dict[str, str]) -> pd.DataFrame:
    """Build a minimal sports manifest DataFrame from row dicts.

    Unspecified columns default to empty string so the builder never
    KeyErrors on missing axes columns.
    """
    data: dict[str, list[str]] = {col: [] for col in _MANIFEST_COLUMNS}
    for row in rows:
        for col in _MANIFEST_COLUMNS:
            data[col].append(row.get(col, ""))
    return pd.DataFrame(data)


def _sports_row(
    data_type: str,
    league_id: str,
    date: str,
    capture_status: str = "captured",
    error_reason: str = "",
) -> dict[str, str]:
    return {
        "asset_group": "sports",
        "data_type": data_type,
        "league_id": league_id,
        "date": date,
        "capture_status": capture_status,
        "error_reason": error_reason,
    }


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------


@pytest.fixture()
def sports_manifest() -> pd.DataFrame:
    """Multi-data_type sports manifest with captured + empty_confirmed rows."""
    return _make_sports_manifest(
        # Active data_type — captured
        _sports_row("FIXTURE_STATS", "EPL", "2024-09-15", "captured"),
        _sports_row("FIXTURE_STATS", "EPL", "2024-09-22", "captured"),
        _sports_row("FIXTURE_STATS", "LA_LIGA", "2024-09-15", "captured"),
        # Active data_type — attempted failure
        _sports_row("FIXTURES", "EPL", "2024-09-15", "attempted_failed"),
        # Retired data_type — empty_confirmed honest-absence rows
        # (retired from write path @ instruments-service@a0a720e, but legacy
        # manifest rows must still surface in the drilldown for honest coverage)
        _sports_row("SFI_LEAGUES", "EPL", "2024-09-15", "empty_confirmed", "SOURCE_RETIRED"),
    )


# ---------------------------------------------------------------------------
# Tests
# ---------------------------------------------------------------------------


class TestSportsAxesMatchSsot:
    """Axis shape: data_type → league_id → date matches UAC SHARD_AXIS_MATRIX."""

    def test_axes_are_data_type_league_id_date(self, sports_manifest: pd.DataFrame) -> None:
        with (
            patch(
                "deployment_api.services.data_status_hierarchical.read_availability_index",
                return_value=sports_manifest,
            ),
            patch(
                "deployment_api.services.data_status_hierarchical.build_bucket_name",
                return_value="market-data-sports-test-pid",
            ),
        ):
            result = get_hierarchical_drilldown(
                service="instruments-service",
                asset_group="sports",
                window_start="2024-09-01",
                window_end="2024-09-30",
                project_id="test-pid",
            )

        assert list(result["axes"]) == ["data_type", "league_id", "date"]


class TestSportsTreeStructure:
    """Tree shape follows data_type → league_id → date hierarchy."""

    def test_top_level_nodes_are_data_types(self, sports_manifest: pd.DataFrame) -> None:
        with (
            patch(
                "deployment_api.services.data_status_hierarchical.read_availability_index",
                return_value=sports_manifest,
            ),
            patch(
                "deployment_api.services.data_status_hierarchical.build_bucket_name",
                return_value="market-data-sports-test-pid",
            ),
        ):
            result = get_hierarchical_drilldown(
                service="instruments-service",
                asset_group="sports",
                window_start="2024-09-01",
                window_end="2024-09-30",
                project_id="test-pid",
                expand_to_depth=3,
            )

        tree = result["tree"]
        top_values = {node["value"] for node in tree}
        # Three distinct data_types in manifest → three top-level nodes
        assert top_values == {"FIXTURE_STATS", "FIXTURES", "SFI_LEAGUES"}

    def test_top_level_nodes_have_axis_data_type(self, sports_manifest: pd.DataFrame) -> None:
        with (
            patch(
                "deployment_api.services.data_status_hierarchical.read_availability_index",
                return_value=sports_manifest,
            ),
            patch(
                "deployment_api.services.data_status_hierarchical.build_bucket_name",
                return_value="market-data-sports-test-pid",
            ),
        ):
            result = get_hierarchical_drilldown(
                service="instruments-service",
                asset_group="sports",
                window_start="2024-09-01",
                window_end="2024-09-30",
                project_id="test-pid",
            )

        for node in result["tree"]:
            assert node["axis"] == "data_type", (
                f"Top-level node has wrong axis: expected 'data_type', got {node['axis']!r}"
            )

    def test_second_level_nodes_are_league_ids(self, sports_manifest: pd.DataFrame) -> None:
        with (
            patch(
                "deployment_api.services.data_status_hierarchical.read_availability_index",
                return_value=sports_manifest,
            ),
            patch(
                "deployment_api.services.data_status_hierarchical.build_bucket_name",
                return_value="market-data-sports-test-pid",
            ),
        ):
            result = get_hierarchical_drilldown(
                service="instruments-service",
                asset_group="sports",
                window_start="2024-09-01",
                window_end="2024-09-30",
                project_id="test-pid",
                expand_to_depth=3,
            )

        for top_node in result["tree"]:
            for league_node in top_node["children"]:
                assert league_node["axis"] == "league_id", (
                    f"Second-level node under {top_node['value']!r} has wrong axis: "
                    f"expected 'league_id', got {league_node['axis']!r}"
                )

    def test_leaf_nodes_are_dates(self, sports_manifest: pd.DataFrame) -> None:
        with (
            patch(
                "deployment_api.services.data_status_hierarchical.read_availability_index",
                return_value=sports_manifest,
            ),
            patch(
                "deployment_api.services.data_status_hierarchical.build_bucket_name",
                return_value="market-data-sports-test-pid",
            ),
        ):
            result = get_hierarchical_drilldown(
                service="instruments-service",
                asset_group="sports",
                window_start="2024-09-01",
                window_end="2024-09-30",
                project_id="test-pid",
                expand_to_depth=3,
            )

        leaf_axes: set[str] = set()
        for top_node in result["tree"]:
            for league_node in top_node["children"]:
                for date_node in league_node["children"]:
                    leaf_axes.add(date_node["axis"])

        assert leaf_axes == {"date"}, f"Leaf nodes MUST have axis='date', got: {leaf_axes}"


class TestSportsGrainCountsCorrect:
    """Per-(data_type, league_id) capture_status counts roll up correctly."""

    def test_fixture_stats_epl_captured_count(self, sports_manifest: pd.DataFrame) -> None:
        """FIXTURE_STATS/EPL has 2 captured rows → captured=2 at league_id node."""
        with (
            patch(
                "deployment_api.services.data_status_hierarchical.read_availability_index",
                return_value=sports_manifest,
            ),
            patch(
                "deployment_api.services.data_status_hierarchical.build_bucket_name",
                return_value="market-data-sports-test-pid",
            ),
        ):
            result = get_hierarchical_drilldown(
                service="instruments-service",
                asset_group="sports",
                window_start="2024-09-01",
                window_end="2024-09-30",
                project_id="test-pid",
                expand_to_depth=3,
            )

        fixture_stats_node = next(n for n in result["tree"] if n["value"] == "FIXTURE_STATS")
        epl_node = next(n for n in fixture_stats_node["children"] if n["value"] == "EPL")
        assert epl_node["captured"] == 2
        assert epl_node["empty_confirmed"] == 0
        assert epl_node["attempted_failed"] == 0

    def test_fixture_stats_la_liga_separate_node(self, sports_manifest: pd.DataFrame) -> None:
        """FIXTURE_STATS/LA_LIGA is a SEPARATE league_id node from EPL — confirms
        the (data_type, league_id) grain is preserved, not collapsed."""
        with (
            patch(
                "deployment_api.services.data_status_hierarchical.read_availability_index",
                return_value=sports_manifest,
            ),
            patch(
                "deployment_api.services.data_status_hierarchical.build_bucket_name",
                return_value="market-data-sports-test-pid",
            ),
        ):
            result = get_hierarchical_drilldown(
                service="instruments-service",
                asset_group="sports",
                window_start="2024-09-01",
                window_end="2024-09-30",
                project_id="test-pid",
                expand_to_depth=3,
            )

        fixture_stats_node = next(n for n in result["tree"] if n["value"] == "FIXTURE_STATS")
        league_values = {n["value"] for n in fixture_stats_node["children"]}
        assert "EPL" in league_values
        assert "LA_LIGA" in league_values

    def test_fixtures_epl_attempted_failed_count(self, sports_manifest: pd.DataFrame) -> None:
        """FIXTURES/EPL has 1 attempted_failed row → attempted_failed=1."""
        with (
            patch(
                "deployment_api.services.data_status_hierarchical.read_availability_index",
                return_value=sports_manifest,
            ),
            patch(
                "deployment_api.services.data_status_hierarchical.build_bucket_name",
                return_value="market-data-sports-test-pid",
            ),
        ):
            result = get_hierarchical_drilldown(
                service="instruments-service",
                asset_group="sports",
                window_start="2024-09-01",
                window_end="2024-09-30",
                project_id="test-pid",
                expand_to_depth=3,
            )

        fixtures_node = next(n for n in result["tree"] if n["value"] == "FIXTURES")
        epl_node = next(n for n in fixtures_node["children"] if n["value"] == "EPL")
        assert epl_node["attempted_failed"] == 1
        assert epl_node["captured"] == 0


class TestRetiredDataTypesHonestCoverage:
    """Retired data_types with empty_confirmed rows still surface in the tree.

    The write-path retirement (instruments-service@a0a720e) removes the WRITER
    but must NOT erase legacy manifest rows. The drilldown MUST surface them
    with their honest ``empty_confirmed`` status so operators can see the gap.
    """

    def test_sfi_leagues_empty_confirmed_appears_in_tree(self, sports_manifest: pd.DataFrame) -> None:
        with (
            patch(
                "deployment_api.services.data_status_hierarchical.read_availability_index",
                return_value=sports_manifest,
            ),
            patch(
                "deployment_api.services.data_status_hierarchical.build_bucket_name",
                return_value="market-data-sports-test-pid",
            ),
        ):
            result = get_hierarchical_drilldown(
                service="instruments-service",
                asset_group="sports",
                window_start="2024-09-01",
                window_end="2024-09-30",
                project_id="test-pid",
                expand_to_depth=3,
            )

        top_values = {node["value"] for node in result["tree"]}
        assert "SFI_LEAGUES" in top_values, (
            "SFI_LEAGUES empty_confirmed row must appear in drill-down tree — "
            "honest coverage requires surfacing retired data_types, not silently dropping them"
        )

    def test_sfi_leagues_epl_has_empty_confirmed_count(self, sports_manifest: pd.DataFrame) -> None:
        with (
            patch(
                "deployment_api.services.data_status_hierarchical.read_availability_index",
                return_value=sports_manifest,
            ),
            patch(
                "deployment_api.services.data_status_hierarchical.build_bucket_name",
                return_value="market-data-sports-test-pid",
            ),
        ):
            result = get_hierarchical_drilldown(
                service="instruments-service",
                asset_group="sports",
                window_start="2024-09-01",
                window_end="2024-09-30",
                project_id="test-pid",
                expand_to_depth=3,
            )

        sfi_node = next(n for n in result["tree"] if n["value"] == "SFI_LEAGUES")
        epl_node = next(n for n in sfi_node["children"] if n["value"] == "EPL")
        assert epl_node["empty_confirmed"] == 1
        assert epl_node["captured"] == 0


class TestSportsFilteredDrilldown:
    """Lazy-load drill-down via filters preserves sports grain."""

    def test_filter_by_data_type_returns_subtree(self, sports_manifest: pd.DataFrame) -> None:
        """Filtering to data_type=FIXTURE_STATS returns only EPL+LA_LIGA subtree."""
        with (
            patch(
                "deployment_api.services.data_status_hierarchical.read_availability_index",
                return_value=sports_manifest,
            ),
            patch(
                "deployment_api.services.data_status_hierarchical.build_bucket_name",
                return_value="market-data-sports-test-pid",
            ),
        ):
            result = get_hierarchical_drilldown(
                service="instruments-service",
                asset_group="sports",
                window_start="2024-09-01",
                window_end="2024-09-30",
                project_id="test-pid",
                filters={"data_type": "FIXTURE_STATS"},
                expand_to_depth=3,
            )

        # With filter applied, top-level nodes are league_ids (one level deeper)
        top_values = {node["value"] for node in result["tree"]}
        assert top_values == {"EPL", "LA_LIGA"}

    def test_filter_to_leaf_returns_date_nodes(self, sports_manifest: pd.DataFrame) -> None:
        """Filtering data_type=FIXTURE_STATS + league_id=EPL returns date leaves."""
        with (
            patch(
                "deployment_api.services.data_status_hierarchical.read_availability_index",
                return_value=sports_manifest,
            ),
            patch(
                "deployment_api.services.data_status_hierarchical.build_bucket_name",
                return_value="market-data-sports-test-pid",
            ),
        ):
            result = get_hierarchical_drilldown(
                service="instruments-service",
                asset_group="sports",
                window_start="2024-09-01",
                window_end="2024-09-30",
                project_id="test-pid",
                filters={"data_type": "FIXTURE_STATS", "league_id": "EPL"},
                expand_to_depth=3,
            )

        leaf_values = {node["value"] for node in result["tree"]}
        assert leaf_values == {"2024-09-15", "2024-09-22"}
        for node in result["tree"]:
            assert node["axis"] == "date"

"""Cat A.3 — drill-down axis depth matches the UAC SHARD_AXIS_MATRIX SSOT.

Plan: ``data_status_comprehensive_test_coverage_2026_05_07.plan.md`` Phase 1.

The hierarchical drill-down builder MUST return an ``axes`` tuple equal to
``SHARD_AXIS_MATRIX[(service, asset_group)] + ("date",)`` — anything else is
silent depth-drift. The pre-2026-05-07 panel collapsed the per-asset-group
drill-down to ``venue → instrument_type → day`` for every service, which
hid the real per-instrument fan-out for MTDS / MDPS / features-*.

This test calls the public ``get_hierarchical_drilldown`` for every
``(service, asset_group)`` pair declared in the SSOT and asserts the
returned ``axes`` equals the SSOT axes with ``"date"`` appended.

The manifest is mocked at the ``read_availability_index`` boundary so we
exercise the axis-resolution path without needing real GCS / parquet.
"""

from __future__ import annotations

from unittest.mock import patch

import pandas as pd
import pytest
from unified_api_contracts.registry.data_status_axis_matrix import SHARD_AXIS_MATRIX

from deployment_api.services.data_status_hierarchical import get_hierarchical_drilldown


def _empty_manifest() -> pd.DataFrame:
    """Return a 0-row manifest with all v5 shard-key columns present so
    the drilldown builder can resolve axes without errors."""
    columns = [
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
    return pd.DataFrame({col: [] for col in columns})


# Restrict to the canonical (service, asset_group) pairs we ship today.
# ml-training-service is excluded: its ``training_period`` axis is
# experimental and does not yet have a stable manifest layout.
# NOTE: features-* sub-family service names (features-delta-one-service,
# features-volatility-service, features-onchain-service, features-sports-service)
# were consolidated into ``features-service`` in UAC
# ``data_status_axis_matrix.py`` (2026-05-14 consolidation). Use the
# canonical ``features-service`` keys from SHARD_AXIS_MATRIX.
_PUBLIC_SERVICE_PAIRS: tuple[tuple[str, str], ...] = (
    ("execution-service", "cefi"),
    ("execution-service", "defi"),
    ("execution-service", "prediction"),
    ("execution-service", "sports"),
    ("execution-service", "tradfi"),
    ("features-service", "cefi"),
    ("features-service", "defi"),
    ("features-service", "prediction"),
    ("features-service", "shared"),
    ("features-service", "sports"),
    ("features-service", "tradfi"),
    ("instruments-service", "cefi"),
    ("instruments-service", "defi"),
    ("instruments-service", "prediction"),
    ("instruments-service", "sports"),
    ("instruments-service", "tradfi"),
    ("market-data-processing-service", "cefi"),
    ("market-data-processing-service", "defi"),
    ("market-data-processing-service", "prediction"),
    ("market-data-processing-service", "sports"),
    ("market-data-processing-service", "tradfi"),
    ("market-tick-data-service", "cefi"),
    ("market-tick-data-service", "defi"),
    ("market-tick-data-service", "prediction"),
    ("market-tick-data-service", "sports"),
    ("market-tick-data-service", "tradfi"),
    ("ml-inference-service", "shared"),
    ("strategy-service", "cefi"),
    ("strategy-service", "defi"),
    ("strategy-service", "prediction"),
    ("strategy-service", "sports"),
    ("strategy-service", "tradfi"),
)


@pytest.mark.parametrize(("service", "asset_group"), _PUBLIC_SERVICE_PAIRS)
def test_drilldown_axes_match_shard_axis_matrix_ssot(service: str, asset_group: str) -> None:
    """For every public (service, asset_group), assert the drill-down's
    returned ``axes`` equals ``SHARD_AXIS_MATRIX[(service, asset_group)]
    + ("date",)``. Catches silent depth-drift between UAC SSOT and the
    deployment-api consumer."""
    expected_shard_axes = SHARD_AXIS_MATRIX[(service, asset_group)]
    expected_full = [*expected_shard_axes, "date"]

    with (
        patch(
            "deployment_api.services.data_status_hierarchical.read_availability_index",
            return_value=_empty_manifest(),
        ),
        patch(
            "deployment_api.services.data_status_hierarchical.build_bucket_name",
            return_value=f"market-data-tick-{asset_group}-test-pid",
        ),
    ):
        result = get_hierarchical_drilldown(
            service=service,
            asset_group=asset_group,
            window_start="2024-01-01",
            window_end="2024-01-02",
            project_id="test-pid",
        )

    axes = result.get("axes")
    # Normalise both sides to list for comparison — the API may emit
    # tuple or list depending on serialisation; the SSOT comparison is
    # what matters, not the container type.
    assert list(axes) == expected_full, (  # type: ignore[arg-type]
        f"({service}, {asset_group}): drill-down axes drifted from SSOT — expected {expected_full}, got {axes}"
    )


def test_unmapped_service_falls_back_to_venue_date() -> None:
    """If a (service, asset_group) is missing from SHARD_AXIS_MATRIX,
    the builder falls back to ``("venue", "date")`` — the universal
    lowest-common-denominator. Logs a warning so operators notice."""
    with (
        patch(
            "deployment_api.services.data_status_hierarchical.read_availability_index",
            return_value=_empty_manifest(),
        ),
        patch(
            "deployment_api.services.data_status_hierarchical.build_bucket_name",
            return_value="market-data-tick-cefi-test-pid",
        ),
    ):
        result = get_hierarchical_drilldown(
            service="some-undeclared-service",
            asset_group="cefi",
            window_start="2024-01-01",
            window_end="2024-01-02",
            project_id="test-pid",
        )

    assert list(result.get("axes")) == ["venue", "date"]  # type: ignore[arg-type]

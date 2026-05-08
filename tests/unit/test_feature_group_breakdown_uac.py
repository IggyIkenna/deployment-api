"""Unit tests for the UAC-denominator feature_group breakdown (Phase 2C).

Plan: ``feature_dag_uac_ssot_and_features_coverage_2026_05_06.md``.

Covers:

* ``_clip_dates_to_feature_coverage`` returns the input window unchanged
  when no UAC ``FEATURE_COVERAGE_START`` floor is registered.
* It clips ``start_date`` to the floor when registered (e.g.
  ``aave_lending_rates`` → 2022-03-16 Aave V3 mainnet launch).
* ``_build_feature_group_breakdown_uac`` falls through to the legacy
  observed-dates inference when the service is unregistered.
* When the service IS registered, the denominator becomes
  ``len(expected_feature_groups) * clipped_dates``: declared-not-yet-written
  feature_groups render as ``missing`` instead of being silently absent.
"""

from __future__ import annotations

import pandas as pd
import pytest

from deployment_api.services.data_status_service import DataStatusService

# ---------------------------------------------------------------------------
# _clip_dates_to_feature_coverage
# ---------------------------------------------------------------------------


def test_clip_unregistered_pair_returns_input_unchanged() -> None:
    start, end = DataStatusService._clip_dates_to_feature_coverage(
        "definitely-not-a-real-service", "no-such-feature-group", "2020-01-01", "2026-05-07"
    )
    assert (start, end) == ("2020-01-01", "2026-05-07")


def test_clip_registered_aave_lending_rates_pushes_start_forward() -> None:
    """Aave V3 mainnet launched 2022-03-16; pre-launch dates drop out."""
    start, end = DataStatusService._clip_dates_to_feature_coverage(
        "features-onchain-service", "aave_lending_rates", "2020-01-01", "2026-05-07"
    )
    assert start == "2022-03-16"
    assert end == "2026-05-07"


def test_clip_does_not_push_end_forward() -> None:
    """Floor only clips start; end is preserved."""
    start, end = DataStatusService._clip_dates_to_feature_coverage(
        "features-onchain-service", "aave_lending_rates", "2023-06-01", "2024-12-31"
    )
    assert start == "2023-06-01"  # already after the floor
    assert end == "2024-12-31"


# ---------------------------------------------------------------------------
# _build_feature_group_breakdown_uac
# ---------------------------------------------------------------------------


@pytest.fixture
def service_instance() -> DataStatusService:
    """The class is instantiable without args; we only call instance methods that
    don't need the constructor's dependencies."""
    # DataStatusService.__init__ may require deps — but the methods we're testing
    # are pure functions of (venue_df, dates, service). Use object.__new__ to skip
    # the constructor where it has external deps; safe because the methods don't
    # touch self state.
    return DataStatusService.__new__(DataStatusService)


def _venue_df(rows: list[dict[str, str]]) -> pd.DataFrame:
    return pd.DataFrame(rows)


def test_uac_breakdown_unregistered_service_falls_through(
    service_instance: DataStatusService,
) -> None:
    """When the service is unregistered, fall through to observed-dates path."""
    df = _venue_df(
        [
            {"date": "2024-01-01", "feature_group": "fg_a"},
            {"date": "2024-01-02", "feature_group": "fg_a"},
        ]
    )
    out = service_instance._build_feature_group_breakdown_uac(
        df, "2024-01-01", "2024-01-03", service="not-a-features-service"
    )
    # Falls through to _build_simple_dimension_breakdown — denominator is the
    # observed dates universe (2 unique dates), so fg_a's completion_pct = 100.
    assert "fg_a" in out
    fg_a_entry = out["fg_a"]
    assert isinstance(fg_a_entry, dict)
    assert fg_a_entry["dates_found"] == 2


def test_uac_breakdown_registered_service_uses_uac_denominator(
    service_instance: DataStatusService,
) -> None:
    """When registered, the denominator is len(expected_fgs) * clipped_dates.

    features-onchain-service expects 12 feature_groups today. With 5 days
    in the window for an Aave-derived fg (clipped post-2022-03-16), one
    observed date for ``aave_lending_rates`` produces dates_found=1,
    dates_expected=5 (the window), missing=4. Other fgs (e.g.
    lst_staking_yields, defillama_tvl) get 0 found / 5 expected entries.
    """
    df = _venue_df(
        [
            {"date": "2024-01-01", "feature_group": "aave_lending_rates"},
        ]
    )
    out = service_instance._build_feature_group_breakdown_uac(
        df, "2024-01-01", "2024-01-05", service="features-onchain-service"
    )
    # All 12 onchain feature_groups are in the result.
    assert len(out) == 12
    aave_entry = out["aave_lending_rates"]
    assert isinstance(aave_entry, dict)
    assert aave_entry["dates_found"] == 1
    # 5-day window, all post-floor 2022-03-16.
    assert aave_entry["dates_expected"] == 5
    assert aave_entry["dates_missing"] == 4

    # A feature_group with 0 observed dates still appears with expected dates.
    lst_entry = out["lst_staking_yields"]
    assert isinstance(lst_entry, dict)
    assert lst_entry["dates_found"] == 0
    assert lst_entry["dates_expected"] == 5
    assert lst_entry["dates_missing"] == 5


def test_uac_breakdown_pre_floor_dates_clipped(
    service_instance: DataStatusService,
) -> None:
    """Aave V3 floor (2022-03-16) clips pre-launch dates from the denominator."""
    df = _venue_df(
        [
            {"date": "2022-03-16", "feature_group": "aave_lending_rates"},
        ]
    )
    out = service_instance._build_feature_group_breakdown_uac(
        df, "2022-01-01", "2022-03-17", service="features-onchain-service"
    )
    aave_entry = out["aave_lending_rates"]
    assert isinstance(aave_entry, dict)
    # Pre-2022-03-16 dropped → 2 days expected (2022-03-16 + 2022-03-17), not 76.
    assert aave_entry["dates_expected"] == 2
    assert aave_entry["dates_found"] == 1


def test_uac_breakdown_empty_expected_list_falls_through(
    service_instance: DataStatusService,
) -> None:
    """features-volatility-service has an empty EXPECTED list today (stub) —
    must fall through to legacy behaviour, not produce an empty result."""
    df = _venue_df(
        [
            {"date": "2024-01-01", "feature_group": "iv_surface"},
        ]
    )
    out = service_instance._build_feature_group_breakdown_uac(
        df, "2024-01-01", "2024-01-02", service="features-volatility-service"
    )
    # Falls through; observed-dates inference returns the iv_surface entry.
    assert "iv_surface" in out


def test_uac_breakdown_no_feature_group_column_returns_empty(
    service_instance: DataStatusService,
) -> None:
    df = _venue_df([{"date": "2024-01-01"}])
    out = service_instance._build_feature_group_breakdown_uac(
        df, "2024-01-01", "2024-01-02", service="features-onchain-service"
    )
    assert out == {}

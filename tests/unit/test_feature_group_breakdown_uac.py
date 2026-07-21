"""Unit tests for the UAC-denominator feature_group breakdown (Phase 2C).

Plan: ``feature_dag_uac_ssot_and_features_coverage_2026_05_06.md``.

Covers:

* ``_clip_dates_to_feature_coverage`` returns the input window unchanged
  when no UAC ``FEATURE_COVERAGE_START`` floor is registered.
* It clips ``start_date`` to the floor when registered (e.g.
  ``lending_rates`` → 2022-03-16 Aave V3 mainnet launch).
* ``_build_feature_group_breakdown_uac`` falls through to the legacy
  observed-dates inference when the service is unregistered.
* When the service IS registered, the denominator becomes
  ``len(expected_feature_groups) * clipped_dates``: declared-not-yet-written
  feature_groups render as ``missing`` instead of being silently absent.

NOTE (2026-05-14): features-* sub-family service names
(features-onchain-service, features-volatility-service, features-delta-one-service,
features-sports-service) were consolidated into ``features-service`` in UAC
``EXPECTED_FEATURE_GROUPS_BY_SERVICE``. Tests updated to use ``features-service``.

NOTE (2026-07-21): the onchain feature_group vocabulary was reconciled to the
WRITER's names (uac@e9faf32e, operator ruling 2026-07-21) — the old
Aave-specific registry names (``aave_lending_rates``, ``aave_utilization``,
``aave_risk_params``, ``lst_staking_yields``, ``eigen_rewards``,
``aave_rate_impact``) were renamed to the protocol-agnostic writer names
(``lending_rates``, ``utilization``, ``risk_params``, ``lst_yields``,
``rewards``, ``rate_impact``). Tests updated to the new vocabulary; floor
dates in ``FEATURE_COVERAGE_START`` are unchanged.
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


def test_clip_registered_lending_rates_pushes_start_forward() -> None:
    """Aave V3 mainnet launched 2022-03-16; pre-launch dates drop out."""
    start, end = DataStatusService._clip_dates_to_feature_coverage(
        "features-service", "lending_rates", "2020-01-01", "2026-05-07"
    )
    assert start == "2022-03-16"
    assert end == "2026-05-07"


def test_clip_does_not_push_end_forward() -> None:
    """Floor only clips start; end is preserved."""
    start, end = DataStatusService._clip_dates_to_feature_coverage(
        "features-service", "lending_rates", "2023-06-01", "2024-12-31"
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

    features-service (consolidated from all sub-family services 2026-05-14)
    has all onchain + delta-one + sports feature_groups. With 5 days in the
    window for an Aave-derived fg (clipped post-2022-03-16), one observed
    date for ``lending_rates`` produces dates_found=1, dates_expected=5
    (the window), missing=4. Other fgs (e.g. lst_yields) get 0 found / 5
    expected entries.
    """
    from unified_api_contracts.canonical.domain.features.registry import (
        EXPECTED_FEATURE_GROUPS_BY_SERVICE,
    )

    df = _venue_df(
        [
            {"date": "2024-01-01", "feature_group": "lending_rates"},
        ]
    )
    out = service_instance._build_feature_group_breakdown_uac(
        df, "2024-01-01", "2024-01-05", service="features-service"
    )
    # All feature_groups declared in EXPECTED_FEATURE_GROUPS_BY_SERVICE["features-service"]
    # appear in the result. Use the UAC constant directly to avoid hardcoding a stale count.
    expected_count = len(EXPECTED_FEATURE_GROUPS_BY_SERVICE["features-service"])
    assert len(out) == expected_count
    lending_entry = out["lending_rates"]
    assert isinstance(lending_entry, dict)
    assert lending_entry["dates_found"] == 1
    # 5-day window, all post-floor 2022-03-16.
    assert lending_entry["dates_expected"] == 5
    assert lending_entry["dates_missing"] == 4

    # A feature_group with 0 observed dates still appears with expected dates.
    lst_entry = out["lst_yields"]
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
            {"date": "2022-03-16", "feature_group": "lending_rates"},
        ]
    )
    out = service_instance._build_feature_group_breakdown_uac(
        df, "2022-01-01", "2022-03-17", service="features-service"
    )
    lending_entry = out["lending_rates"]
    assert isinstance(lending_entry, dict)
    # Pre-2022-03-16 dropped → 2 days expected (2022-03-16 + 2022-03-17), not 76.
    assert lending_entry["dates_expected"] == 2
    assert lending_entry["dates_found"] == 1


def test_uac_breakdown_empty_expected_list_falls_through(
    service_instance: DataStatusService,
) -> None:
    """An unregistered service falls through to legacy observed-dates path.

    features-volatility-service is used intentionally here to represent a
    service that is NOT in EXPECTED_FEATURE_GROUPS_BY_SERVICE (the sub-family
    service names were consolidated into ``features-service`` in UAC 2026-05-14;
    sub-family names are unregistered and always fall through to legacy path).
    """
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
        df, "2024-01-01", "2024-01-02", service="features-service"
    )
    assert out == {}


def test_uac_breakdown_observed_but_unexpected_group_surfaces_as_drift(
    service_instance: DataStatusService,
) -> None:
    """A feature_group present in the manifest but absent from the UAC expected
    list must surface as an explicit ``is_unexpected`` bucket, not be dropped.

    This is the pre-2026-07-21-rename legacy-name case: manifest rows written
    before the writer-name reconciliation (uac@e9faf32e) carry the OLD
    Aave-specific name (``aave_lending_rates``), which the UAC registry no
    longer expects after the rename to the protocol-agnostic ``lending_rates``.
    Silently dropping the observed legacy value would render pre-rename rows
    as a phantom coverage hole; surfacing it renders the drift AS a drift.
    Verified: the legacy writer-name appears, flagged unexpected, while the
    current registry name still appears (flagged not-unexpected).
    """
    df = _venue_df(
        [
            {"date": "2024-01-01", "feature_group": "aave_lending_rates"},
            {"date": "2024-01-02", "feature_group": "aave_lending_rates"},
        ]
    )
    out = service_instance._build_feature_group_breakdown_uac(
        df, "2024-01-01", "2024-01-05", service="features-service"
    )
    # The observed-but-unexpected legacy writer name is NOT dropped.
    assert "aave_lending_rates" in out
    drift_entry = out["aave_lending_rates"]
    assert isinstance(drift_entry, dict)
    assert drift_entry["is_unexpected"] is True
    assert drift_entry["dates_found"] == 2
    # No UAC denominator → never rendered as a coverage gap.
    assert drift_entry["dates_missing"] == 0
    assert drift_entry["dates_expected"] == 2

    # Registry-current name still appears and is flagged not-unexpected.
    lending_entry = out["lending_rates"]
    assert isinstance(lending_entry, dict)
    assert lending_entry["is_unexpected"] is False

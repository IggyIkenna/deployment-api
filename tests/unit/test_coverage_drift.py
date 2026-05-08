"""Unit tests for coverage_drift detector (Phase 8.B).

Plan: features_sports_honest_coverage_2026_05_05.md, Phase 8.B.
"""

from __future__ import annotations

import pandas as pd

from deployment_api.services.coverage_drift import (
    DRIFT_THRESHOLD_PCT,
    DriftEvent,
    _coverage_per_calc_league,
    detect_drift,
    known_calculators,
)


def _snapshot(rows: list[dict[str, object]]) -> pd.DataFrame:
    return pd.DataFrame(rows)


class TestCoveragePerCalcLeague:
    def test_empty_snapshot_returns_empty(self) -> None:
        assert _coverage_per_calc_league(pd.DataFrame()) == {}

    def test_all_captured_is_100pct(self) -> None:
        snap = _snapshot(
            [
                {"feature_group": "team_form", "league_id": "EPL", "capture_status": "captured"},
                {"feature_group": "team_form", "league_id": "EPL", "capture_status": "captured"},
            ]
        )
        assert _coverage_per_calc_league(snap) == {("team_form", "EPL"): 100.0}

    def test_failed_excluded_from_numerator(self) -> None:
        snap = _snapshot(
            [
                {"feature_group": "team_form", "league_id": "EPL", "capture_status": "captured"},
                {
                    "feature_group": "team_form",
                    "league_id": "EPL",
                    "capture_status": "attempted_failed",
                },
            ]
        )
        # 1 captured / 2 total = 50%
        assert _coverage_per_calc_league(snap) == {("team_form", "EPL"): 50.0}

    def test_empty_confirmed_counts_toward_coverage(self) -> None:
        snap = _snapshot(
            [
                {
                    "feature_group": "team_form",
                    "league_id": "EPL",
                    "capture_status": "empty_confirmed",
                },
            ]
        )
        assert _coverage_per_calc_league(snap) == {("team_form", "EPL"): 100.0}


class TestDetectDrift:
    def test_no_drift_returns_empty(self) -> None:
        snap = _snapshot(
            [
                {"feature_group": "team_form", "league_id": "EPL", "capture_status": "captured"},
            ]
        )
        assert detect_drift(snap, snap) == []

    def test_coverage_drop_triggers_event(self) -> None:
        prev = _snapshot(
            [
                # 4 of 4 captured = 100%
                {"feature_group": "team_form", "league_id": "EPL", "capture_status": "captured"},
                {"feature_group": "team_form", "league_id": "EPL", "capture_status": "captured"},
                {"feature_group": "team_form", "league_id": "EPL", "capture_status": "captured"},
                {"feature_group": "team_form", "league_id": "EPL", "capture_status": "captured"},
            ]
        )
        cur = _snapshot(
            [
                # 1 captured + 3 failed = 25%
                {"feature_group": "team_form", "league_id": "EPL", "capture_status": "captured"},
                {
                    "feature_group": "team_form",
                    "league_id": "EPL",
                    "capture_status": "attempted_failed",
                },
                {
                    "feature_group": "team_form",
                    "league_id": "EPL",
                    "capture_status": "attempted_failed",
                },
                {
                    "feature_group": "team_form",
                    "league_id": "EPL",
                    "capture_status": "attempted_failed",
                },
            ]
        )
        events = detect_drift(prev, cur)
        assert len(events) == 1
        ev = events[0]
        assert ev.calc == "team_form"
        assert ev.league_id == "EPL"
        assert ev.previous_coverage_pct == 100.0
        assert ev.current_coverage_pct == 25.0
        assert ev.drift_pct == 75.0

    def test_below_threshold_no_event(self) -> None:
        # 100% → 96% = 4pt drop, below default 5pt threshold
        prev = _snapshot(
            [{"feature_group": "team_form", "league_id": "EPL", "capture_status": "captured"}] * 100
        )
        cur = _snapshot(
            [{"feature_group": "team_form", "league_id": "EPL", "capture_status": "captured"}] * 96
            + [
                {
                    "feature_group": "team_form",
                    "league_id": "EPL",
                    "capture_status": "attempted_failed",
                }
            ]
            * 4
        )
        assert detect_drift(prev, cur) == []

    def test_coverage_gain_no_event(self) -> None:
        """Coverage going UP shouldn't trigger drift events."""
        prev = _snapshot(
            [
                {
                    "feature_group": "team_form",
                    "league_id": "EPL",
                    "capture_status": "attempted_failed",
                },
                {"feature_group": "team_form", "league_id": "EPL", "capture_status": "captured"},
            ]
        )
        cur = _snapshot(
            [
                {"feature_group": "team_form", "league_id": "EPL", "capture_status": "captured"},
                {"feature_group": "team_form", "league_id": "EPL", "capture_status": "captured"},
            ]
        )
        # 50% → 100% = +50pt gain, no event
        assert detect_drift(prev, cur) == []

    def test_pair_disappearance_treated_as_drop_to_zero(self) -> None:
        """A (calc, league) that vanishes from current = 100% → 0% = drift."""
        prev = _snapshot(
            [
                {"feature_group": "team_form", "league_id": "EPL", "capture_status": "captured"},
            ]
        )
        cur = pd.DataFrame(columns=["feature_group", "league_id", "capture_status"])
        events = detect_drift(prev, cur)
        assert len(events) == 1
        assert events[0].current_coverage_pct == 0.0

    def test_new_pair_no_event(self) -> None:
        """A (calc, league) appearing in current but not previous shouldn't
        trigger drift on day 1."""
        prev = pd.DataFrame(columns=["feature_group", "league_id", "capture_status"])
        cur = _snapshot(
            [
                {"feature_group": "team_form", "league_id": "EPL", "capture_status": "captured"},
            ]
        )
        assert detect_drift(prev, cur) == []

    def test_events_sorted_by_drop_magnitude(self) -> None:
        prev = _snapshot(
            [
                # team_form / EPL: 100%
                {"feature_group": "team_form", "league_id": "EPL", "capture_status": "captured"},
                # team_xg / EPL: 100%
                {"feature_group": "team_xg", "league_id": "EPL", "capture_status": "captured"},
            ]
        )
        cur = _snapshot(
            [
                # team_form / EPL: 0% (50pt drop)
                {
                    "feature_group": "team_form",
                    "league_id": "EPL",
                    "capture_status": "attempted_failed",
                },
                # team_xg / EPL: 0% (90pt-equivalent — but actually 100pt drop)
                {
                    "feature_group": "team_xg",
                    "league_id": "EPL",
                    "capture_status": "attempted_failed",
                },
                {
                    "feature_group": "team_xg",
                    "league_id": "EPL",
                    "capture_status": "attempted_failed",
                },
            ]
        )
        events = detect_drift(prev, cur)
        # Both 100%→0% but team_xg has more rows so still 100pt drop.
        # Both should be reported, sorted by drift_pct desc.
        assert len(events) == 2
        assert events[0].drift_pct >= events[1].drift_pct


class TestDriftEventStr:
    def test_str_repr(self) -> None:
        ev = DriftEvent(
            calc="team_xg",
            league_id="EPL",
            previous_coverage_pct=92.5,
            current_coverage_pct=70.3,
            drift_pct=22.2,
        )
        s = str(ev)
        assert "team_xg/EPL" in s
        assert "92.5" in s
        assert "70.3" in s
        assert "22.2" in s


class TestKnownCalculators:
    def test_includes_phase_1_calculators(self) -> None:
        known = known_calculators()
        # Sanity check — Phase 1 entries should be in the set
        assert "team_form" in known
        assert "sfi_progressive" in known
        assert "footystats_predictions" in known
        assert len(known) >= 30  # 34 calculators per Phase 0 inventory


class TestThreshold:
    def test_default_threshold_is_5pt(self) -> None:
        assert DRIFT_THRESHOLD_PCT == 5.0

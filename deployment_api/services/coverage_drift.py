"""Coverage drift detector for features-sports honest-coverage (Phase 8.B).

Compares two manifest snapshots and surfaces (calculator, league) pairs
where coverage has dropped beyond a threshold. Designed to power a
periodic alert so operators learn about silent regressions before they
contaminate downstream model training:

  - daily Cloud Run job runs at e.g. 02:00 UTC, snapshots the manifest's
    per-(calc, league) coverage to GCS
  - drift detector compares today's snapshot against N days back
  - if any (calc, league) drops more than DRIFT_THRESHOLD_PCT, emit a
    structured event the operator dashboard surfaces

This module exposes the core comparator. The Cloud Run job + structured
event emit lives in deployment-service (P8.B follow-up — out of scope
for this commit).

Plan: features_sports_honest_coverage_2026_05_05.plan.md, Phase 8.B.
"""

from __future__ import annotations

from dataclasses import dataclass

import pandas as pd
from unified_api_contracts.sports import FEATURE_UPSTREAM_REQUIREMENTS

# Drift threshold (percentage points). A (calc, league) reporting 80%
# coverage on snapshot N-7 and 70% on snapshot N triggers (10pt drop ≥
# threshold). Tunable; 5pt is a starting point that catches genuine
# regressions without being noisy on natural week-over-week jitter.
DRIFT_THRESHOLD_PCT = 5.0


@dataclass(frozen=True)
class DriftEvent:
    """A single coverage-drift event between two snapshots."""

    calc: str
    league_id: str
    previous_coverage_pct: float
    current_coverage_pct: float
    drift_pct: float  # previous - current (positive = drop)

    def __str__(self) -> str:
        return (
            f"{self.calc}/{self.league_id}: "
            f"{self.previous_coverage_pct:.1f}% → {self.current_coverage_pct:.1f}% "
            f"(drop {self.drift_pct:+.1f}pt)"
        )


def _coverage_per_calc_league(snapshot: pd.DataFrame) -> dict[tuple[str, str], float]:
    """Reduce a manifest snapshot to {(calc, league_id): coverage_pct}.

    Snapshot schema (subset of canonical manifest):
      - feature_group: calculator name
      - league_id: canonical UAC league_id (or "" for date-only rows)
      - capture_status: captured / empty_confirmed / attempted_failed
      - expected: optional pre-computed denominator; falls back to row count

    Coverage = (captured + empty_confirmed) / total_rows for that (calc, league).
    Caller can pre-filter the snapshot to a date window before passing in.
    """
    if snapshot.empty or "feature_group" not in snapshot.columns:
        return {}
    df = snapshot.copy()
    if "capture_status" not in df.columns:
        df["capture_status"] = "captured"
    df["capture_status"] = df["capture_status"].fillna("captured")
    df["league_id"] = df["league_id"].fillna("") if "league_id" in df.columns else ""

    out: dict[tuple[str, str], float] = {}
    for (calc, lid), group in df.groupby(["feature_group", "league_id"]):
        total = len(group)
        if total == 0:
            continue
        ok = (group["capture_status"].isin(["captured", "empty_confirmed"])).sum()
        out[(str(calc), str(lid))] = 100.0 * float(ok) / float(total)
    return out


def detect_drift(
    previous_snapshot: pd.DataFrame,
    current_snapshot: pd.DataFrame,
    threshold_pct: float = DRIFT_THRESHOLD_PCT,
) -> list[DriftEvent]:
    """Return drift events where coverage has dropped ≥ ``threshold_pct``.

    Only reports DROPS — coverage gains are silent (no operator action
    needed). Pairs that exist in current but not previous are skipped
    (new calc onboarding shouldn't trigger drift alerts on day 1).

    Args:
        previous_snapshot: Manifest DataFrame from the baseline timestamp.
        current_snapshot: Manifest DataFrame from the comparison timestamp.
        threshold_pct: Minimum percentage-point drop to emit an event.

    Returns:
        List of DriftEvent, sorted by drift magnitude (largest drop first).
    """
    prev_cov = _coverage_per_calc_league(previous_snapshot)
    cur_cov = _coverage_per_calc_league(current_snapshot)

    events: list[DriftEvent] = []
    for key, prev_pct in prev_cov.items():
        cur_pct = cur_cov.get(key, 0.0)
        drop = prev_pct - cur_pct
        if drop >= threshold_pct:
            calc, lid = key
            events.append(
                DriftEvent(
                    calc=calc,
                    league_id=lid,
                    previous_coverage_pct=prev_pct,
                    current_coverage_pct=cur_pct,
                    drift_pct=drop,
                )
            )
    events.sort(key=lambda e: e.drift_pct, reverse=True)
    return events


def known_calculators() -> set[str]:
    """Calculators that should be present in every manifest snapshot.

    Used by the drift detector's caller to flag CALC absence (not just
    coverage drop) — a calc completely missing from the current snapshot
    when it had rows previously is itself a regression.
    """
    return set(FEATURE_UPSTREAM_REQUIREMENTS.keys())

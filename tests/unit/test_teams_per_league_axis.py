"""P8 — TEAMS reclassified per-league (was ``global_trigger_date``).

Plan: ``data_status_page_ux_and_canonicalisation_2026_07_16`` P8.

TEAMS is a PER-LEAGUE reference entity: the instruments-service writer keys TEAMS
rows on ``(date, data_type='TEAMS', league_id)`` and both the UAC
``SHARD_AXIS_MATRIX`` and ``gcs_paths`` classify it per-league. The data-status
read-side axis was ``global_trigger_date`` (a 4-way drift vs writer/UAC/gcs_paths)
→ ``per_league: None`` → no league drilldown. Flipping the axis to
``per_league_trigger_date`` restores shard-atom identity, so the TEAMS
data-status entry carries a ``per_league`` map (``dt_entry["leagues"] =
honest["per_league"]``) and the UI renders the league drilldown consistently
with STANDINGS.
"""

from __future__ import annotations

from unittest.mock import MagicMock, patch

import pandas as pd

from deployment_api.services.data_status.sports_helpers import (
    SPORTS_DATA_TYPE_META,
    sports_honest_coverage,
)


def test_teams_axis_is_per_league_trigger_date() -> None:
    """The SSOT meta axis flip is the load-bearing change (routes the branch)."""
    assert SPORTS_DATA_TYPE_META["TEAMS"]["axis"] == "per_league_trigger_date"


def test_teams_response_carries_per_league_leagues() -> None:
    """TEAMS honest-coverage routes through the per_league branch and returns a
    populated ``per_league`` map → ``dt_entry["leagues"]`` → the UI's hasLeagues
    gate renders the league drilldown."""
    df = pd.DataFrame(
        {
            "data_type": ["TEAMS", "TEAMS"],
            "league_id": ["EPL", "EPL"],
            "date": ["2024-08-01", "2025-01-15"],
            "capture_status": ["captured", "captured"],
        }
    )
    epl = MagicMock()
    epl.league_id = "EPL"
    la_liga = MagicMock()
    la_liga.league_id = "LA_LIGA"

    with (
        patch(
            "deployment_api.services.data_status_service.get_expected_leagues_for_source",
            return_value=[epl, la_liga],
        ),
        patch(
            "deployment_api.services.data_status.sports_helpers.sports_trigger_dates_for_league",
            side_effect=lambda _lid, _s, _e: ["2024-08-01", "2025-01-15"],
        ),
    ):
        result = sports_honest_coverage(df, "TEAMS", "2024-07-01", "2025-06-30")

    assert result is not None
    assert result["axis"] == "per_league_trigger_date"
    per_league = result["per_league"]
    assert isinstance(per_league, dict) and per_league, (
        "TEAMS must carry a per_league map (→ leagues in the data-status response)"
    )
    assert "EPL" in per_league
    epl_stats = per_league["EPL"]
    assert isinstance(epl_stats, dict)
    # EPL had both season-boundary trigger dates captured → 2/2.
    assert epl_stats["found_shards"] == 2
    assert epl_stats["expected_shards"] == 2
    # LA_LIGA expected (2 trigger dates) but nothing captured → off-season/absent,
    # NOT a silent gap.
    assert "LA_LIGA" in per_league
    assert per_league["LA_LIGA"]["found_shards"] == 0

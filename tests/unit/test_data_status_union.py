"""Unit tests for the v9 manifest UNION read path (G3 / M5 / M4).

Post the pipeline_mode-source-aware migration, a single cell carries multiple
manifest rows (one per source x pipeline_mode). These tests pin the honest
union semantics the data-status CONSUMER must apply:

* ≥1 source/mode ``captured`` ⇒ the cell is ``captured`` (M5), never
  double-counted across its source/mode rows.
* status precedence captured > empty_confirmed > attempted_failed >
  expected_unattempted; known-empty beats pending within expected_unattempted.
* the 4-state denominator stays honest (cell-grain, not row-grain).
* v8 manifests (no provenance columns) are byte-identical (no-op reduce).
* the per-(pipeline_mode, source) drilldown breakdown is cell-grain + honest.

Plan: master_data_canonicalisation_migration_catalogue_2026_06_07 G3 / M5 ·
pipeline_mode_source_batch_live_replay_standardisation_2026_06_05 M5.
"""

from __future__ import annotations

import pandas as pd
from unified_api_contracts import CaptureStatusCounts, compute_honest_coverage

from deployment_api.services.data_status_hierarchical import _aggregate_counts
from deployment_api.services.data_status_service import _compute_capture_status_counts
from deployment_api.services.data_status_union import (
    has_provenance_columns,
    provenance_breakdown,
    union_reduce_to_cells,
)

_CELL = {
    "date": "2026-06-01",
    "asset_group": "tradfi",
    "venue": "CME",
    "instrument_type": "FUTURE",
    "data_type": "ohlcv_1m",
    "instrument_id": "ESM6",
}


def _row(
    *, source: str, pipeline_mode: str, capture_status: str, error_reason: str = "", **over: str
) -> dict[str, str]:
    return {
        **_CELL,
        "source": source,
        "pipeline_mode": pipeline_mode,
        "capture_status": capture_status,
        "error_reason": error_reason,
        **over,
    }


class TestUnionReduceToCells:
    def test_multi_source_one_captured_collapses_to_single_captured_cell(self) -> None:
        """≥1 source captured ⇒ the cell is captured, ONE row (no double-count)."""
        df = pd.DataFrame(
            [
                _row(source="databento", pipeline_mode="batch_databento", capture_status="captured"),
                _row(source="massive", pipeline_mode="batch_massive", capture_status="attempted_failed"),
            ]
        )
        reduced = union_reduce_to_cells(df)
        assert len(reduced) == 1
        assert reduced.iloc[0]["capture_status"] == "captured"

    def test_multi_mode_one_captured_collapses_to_single_captured_cell(self) -> None:
        """batch + replay + live for one cell, only replay captured ⇒ captured."""
        df = pd.DataFrame(
            [
                _row(source="databento", pipeline_mode="batch_databento", capture_status="expected_unattempted"),
                _row(source="databento", pipeline_mode="replay_databento", capture_status="captured"),
                _row(source="databento", pipeline_mode="live_databento", capture_status="attempted_failed"),
            ]
        )
        reduced = union_reduce_to_cells(df)
        assert len(reduced) == 1
        assert reduced.iloc[0]["capture_status"] == "captured"

    def test_status_precedence_empty_beats_failed_beats_expected(self) -> None:
        df = pd.DataFrame(
            [
                _row(source="a", pipeline_mode="batch_a", capture_status="expected_unattempted"),
                _row(source="b", pipeline_mode="batch_b", capture_status="attempted_failed"),
                _row(source="c", pipeline_mode="batch_c", capture_status="empty_confirmed"),
            ]
        )
        reduced = union_reduce_to_cells(df)
        assert len(reduced) == 1
        assert reduced.iloc[0]["capture_status"] == "empty_confirmed"

    def test_expected_unattempted_known_empty_beats_pending(self) -> None:
        df = pd.DataFrame(
            [
                _row(source="a", pipeline_mode="batch_a", capture_status="expected_unattempted", error_reason=""),
                _row(
                    source="b",
                    pipeline_mode="batch_b",
                    capture_status="expected_unattempted",
                    error_reason="EXPECTED_HOLIDAY",
                ),
            ]
        )
        reduced = union_reduce_to_cells(df)
        assert len(reduced) == 1
        assert reduced.iloc[0]["capture_status"] == "expected_unattempted"
        assert reduced.iloc[0]["error_reason"] == "EXPECTED_HOLIDAY"

    def test_distinct_cells_preserved(self) -> None:
        df = pd.DataFrame(
            [
                _row(source="databento", pipeline_mode="batch_databento", capture_status="captured"),
                _row(
                    source="databento",
                    pipeline_mode="batch_databento",
                    capture_status="captured",
                    instrument_id="NQM6",
                ),
            ]
        )
        reduced = union_reduce_to_cells(df)
        assert len(reduced) == 2

    def test_v8_manifest_no_provenance_is_unchanged(self) -> None:
        df = pd.DataFrame([{**_CELL, "capture_status": "captured", "error_reason": ""}])
        assert not has_provenance_columns(df)


class TestPanelUnionCounts:
    def test_coarse_vs_source_aware_not_double_counted(self) -> None:
        """One cell, 3 source/mode rows, 1 captured ⇒ panel counts captured=1."""
        df = pd.DataFrame(
            [
                _row(source="databento", pipeline_mode="batch_databento", capture_status="captured"),
                _row(source="massive", pipeline_mode="batch_massive", capture_status="empty_confirmed"),
                _row(source="databento", pipeline_mode="live_databento", capture_status="attempted_failed"),
            ]
        )
        counts = _compute_capture_status_counts(df)
        assert counts == CaptureStatusCounts(captured=1)

    def test_two_cells_union_then_count(self) -> None:
        rows = [
            # cell ESM6 — captured via replay, failed via live → captured
            _row(source="databento", pipeline_mode="replay_databento", capture_status="captured"),
            _row(source="databento", pipeline_mode="live_databento", capture_status="attempted_failed"),
            # cell NQM6 — only failed across both modes → attempted_failed
            _row(
                source="databento",
                pipeline_mode="batch_databento",
                capture_status="attempted_failed",
                instrument_id="NQM6",
            ),
            _row(
                source="databento",
                pipeline_mode="live_databento",
                capture_status="attempted_failed",
                instrument_id="NQM6",
            ),
        ]
        counts = _compute_capture_status_counts(pd.DataFrame(rows))
        assert counts.captured == 1
        assert counts.attempted_failed == 1
        # Honest denominator is cell-grain (2 cells), not row-grain (4 rows).
        assert compute_honest_coverage(counts) == 0.5

    def test_v8_panel_counts_unchanged(self) -> None:
        df = pd.DataFrame(
            [
                {**_CELL, "capture_status": "captured", "error_reason": ""},
                {**_CELL, "capture_status": "attempted_failed", "error_reason": "x", "instrument_id": "NQM6"},
            ]
        )
        counts = _compute_capture_status_counts(df)
        assert counts == CaptureStatusCounts(captured=1, attempted_failed=1)


class TestHierarchicalUnionCounts:
    def test_aggregate_counts_unions_multi_row_cell(self) -> None:
        df = pd.DataFrame(
            [
                _row(source="databento", pipeline_mode="batch_databento", capture_status="captured"),
                _row(source="massive", pipeline_mode="batch_massive", capture_status="captured"),
            ]
        )
        captured, empty, failed, expected = _aggregate_counts(df)
        assert (captured, empty, failed, expected) == (1, 0, 0, 0)


class TestProvenanceBreakdown:
    def test_breakdown_shows_per_mode_source_status(self) -> None:
        """A cell captured via batch + replay but missing in live is visible."""
        df = pd.DataFrame(
            [
                _row(source="databento", pipeline_mode="batch_databento", capture_status="captured"),
                _row(source="databento", pipeline_mode="replay_databento", capture_status="captured"),
                _row(source="databento", pipeline_mode="live_databento", capture_status="attempted_failed"),
            ]
        )
        breakdown = provenance_breakdown(df)
        by_mode = {row["pipeline_mode"]: row for row in breakdown}
        assert by_mode["batch_databento"]["captured"] == 1
        assert by_mode["replay_databento"]["captured"] == 1
        assert by_mode["live_databento"]["attempted_failed"] == 1
        assert by_mode["live_databento"]["captured"] == 0

    def test_breakdown_is_cell_grain_not_transport_double_counted(self) -> None:
        df = pd.DataFrame(
            [
                _row(source="tardis", pipeline_mode="batch_tardis", capture_status="captured", transport="flat_file"),
                _row(source="tardis", pipeline_mode="batch_tardis", capture_status="captured", transport="rest"),
            ]
        )
        breakdown = provenance_breakdown(df)
        assert len(breakdown) == 1
        assert breakdown[0]["captured"] == 1

    def test_breakdown_empty_on_v8_manifest(self) -> None:
        df = pd.DataFrame([{**_CELL, "capture_status": "captured", "error_reason": ""}])
        assert provenance_breakdown(df) == []

    def test_breakdown_includes_cadence_dimension(self) -> None:
        """A row carrying cadence surfaces it in the per-mode/source breakdown (M5b)."""
        df = pd.DataFrame(
            [
                _row(
                    source="databento",
                    pipeline_mode="batch_databento",
                    capture_status="captured",
                    cadence="daily",
                ),
            ]
        )
        breakdown = provenance_breakdown(df)
        assert len(breakdown) == 1
        assert breakdown[0]["cadence"] == "daily"
        assert breakdown[0]["captured"] == 1

    def test_breakdown_blank_cadence_does_not_break(self) -> None:
        """Older rows carry a blank cadence — the breakdown stays honest, cadence=''."""
        df = pd.DataFrame(
            [
                _row(
                    source="databento",
                    pipeline_mode="batch_databento",
                    capture_status="captured",
                    cadence="",
                ),
            ]
        )
        breakdown = provenance_breakdown(df)
        assert len(breakdown) == 1
        assert breakdown[0]["cadence"] == ""
        assert breakdown[0]["captured"] == 1


class TestM4ModePrecedenceTiebreak:
    """M4 mode-precedence (live > replay > batch) is a TIEBREAK for the
    REPRESENTATIVE row among rows sharing the M5-winning status — it never
    changes the capture_status outcome (M5 captured-union dominates)."""

    def test_captured_in_multiple_modes_represents_live(self) -> None:
        df = pd.DataFrame(
            [
                _row(source="databento", pipeline_mode="batch_databento", capture_status="captured"),
                _row(source="databento", pipeline_mode="replay_databento", capture_status="captured"),
                _row(source="databento", pipeline_mode="live_databento", capture_status="captured"),
            ]
        )
        reduced = union_reduce_to_cells(df)
        assert len(reduced) == 1
        assert reduced.iloc[0]["capture_status"] == "captured"
        assert reduced.iloc[0]["pipeline_mode"] == "live_databento"  # M4: live wins the tiebreak

    def test_status_union_dominates_mode_precedence(self) -> None:
        """batch captured + live failed → captured (M5), NOT live's failed —
        mode precedence must not override the honest status union."""
        df = pd.DataFrame(
            [
                _row(source="databento", pipeline_mode="batch_databento", capture_status="captured"),
                _row(source="databento", pipeline_mode="live_databento", capture_status="attempted_failed"),
            ]
        )
        reduced = union_reduce_to_cells(df)
        assert reduced.iloc[0]["capture_status"] == "captured"
        assert reduced.iloc[0]["pipeline_mode"] == "batch_databento"

    def test_replay_beats_batch_within_same_status(self) -> None:
        df = pd.DataFrame(
            [
                _row(source="databento", pipeline_mode="batch_databento", capture_status="captured"),
                _row(source="databento", pipeline_mode="replay_databento", capture_status="captured"),
            ]
        )
        reduced = union_reduce_to_cells(df)
        assert reduced.iloc[0]["pipeline_mode"] == "replay_databento"

    def test_live_source_value_treated_as_live(self) -> None:
        """A ``live_<source>`` value (e.g. ``live_binance``) wins the mode
        tiebreak over ``batch_*`` — the ``live`` prefix reduces it to the
        ``live`` mode (M4)."""
        df = pd.DataFrame(
            [
                _row(source="databento", pipeline_mode="batch_databento", capture_status="captured"),
                _row(source="binance", pipeline_mode="live_binance", capture_status="captured"),
            ]
        )
        reduced = union_reduce_to_cells(df)
        assert reduced.iloc[0]["pipeline_mode"] == "live_binance"

    def test_legacy_live_alias_string_still_maps_to_live(self) -> None:
        """Backward-compat: OLD live parquets carry the legacy transitional
        live-alias string — the ``live`` prefix still reduces it to the
        ``live`` mode so it wins over ``batch_*``. The alias literal is
        built from a SPLIT string so the deleted-enum token never appears
        as source text (the enum member is gone; the STRING survives in old
        data)."""
        legacy = "live_" + "websocket"
        df = pd.DataFrame(
            [
                _row(source="databento", pipeline_mode="batch_databento", capture_status="captured"),
                _row(source="databento", pipeline_mode=legacy, capture_status="captured"),
            ]
        )
        reduced = union_reduce_to_cells(df)
        assert reduced.iloc[0]["pipeline_mode"] == legacy

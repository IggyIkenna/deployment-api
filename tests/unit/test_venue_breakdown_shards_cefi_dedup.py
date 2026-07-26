"""Cefi parity for the DeFi ``pipeline_mode`` dedup regression guard.

Mirrors ``test_pipeline_mode_rows_do_not_double_count_shards``
(``tests/unit/test_chain_breakdown_shards_vs_dates.py``), which guards the
DeFi chain-breakdown builder's raw-``len()`` numerator against the
2026-05-19 ``pipeline_mode`` migration (the same shard atom can carry
multiple manifest rows — one per ``pipeline_mode=batch_*`` / ``live_*``).

For cefi/MTDS the shard atom is ``(venue, data_type, instrument_type,
instrument_id, day)`` (UAC ``SHARD_AXIS_MATRIX[("market-tick-data-service",
"cefi")]``). ``pipeline_mode`` is NOT part of that atom, so a shard captured
under both a batch and a live pipeline_mode must count ONCE. Unlike the
DeFi chain-breakdown builder (which counted a raw ``len()`` of matching
rows and needed an explicit ``drop_duplicates(subset=_shard_atom_cols)``
fix), the cefi per-instrument-shard denominator in
:func:`per_instrument_coverage` builds its found-set as a Python
``set[tuple[instrument_id, date]]`` (``found_pairs``), which already
collapses duplicate ``pipeline_mode`` rows for the same shard atom for
free. This test is the missing regression guard proving that holds, so a
future refactor that swaps the set-based numerator for a raw row count
fails loud instead of silently double-counting.
"""

from __future__ import annotations

import pandas as pd

from deployment_api.services.data_status_service import _per_instrument_coverage

_VENUE = "BINANCE-FUTURES"
_DT = "trades"  # a known per-instrument shard data_type in UAC
_INSTRUMENTS = [
    "BINANCE-FUTURES::BTCUSDT",
    "BINANCE-FUTURES::ETHUSDT",
]


def _cefi_provider(venue: str, _data_type: str) -> list[str] | None:
    """Test IS provider returning a fixed 2-instrument universe for _VENUE."""
    return _INSTRUMENTS if venue == _VENUE else None


class TestPerInstrumentCoverageDoesNotDoubleCountPipelineModeRows:
    """The cefi mirror of the DeFi ``ARBITRUM`` pipeline_mode regression."""

    def test_pipeline_mode_rows_do_not_double_count_shards(self) -> None:
        """5 days x 2 instruments, each shard atom captured under BOTH
        ``batch_binance`` AND ``live_binance`` — 20 raw rows, but only 10
        distinct ``(instrument_id, date)`` shard atoms. A raw ``len()``
        numerator would report 20 (double-counted across pipeline_mode);
        the dedup'd ``found_shards`` must be 10.
        """
        dates = [f"2026-01-{i + 1:02d}" for i in range(5)]
        rows: list[dict[str, str]] = []
        for iid in _INSTRUMENTS:
            for date in dates:
                for pipeline_mode in ("batch_binance", "live_binance"):
                    rows.append(
                        {
                            "venue": _VENUE,
                            "data_type": _DT,
                            "instrument_type": "perpetual",
                            "instrument_id": iid,
                            "date": date,
                            "capture_status": "captured",
                            "pipeline_mode": pipeline_mode,
                        }
                    )
        manifest_df = pd.DataFrame(rows)
        assert len(manifest_df) == 20, "sanity: 2 instruments x 5 dates x 2 pipeline_modes = 20 raw rows"

        result = _per_instrument_coverage(
            venue_df_ok=manifest_df,
            venue=_VENUE,
            dt=_DT,
            expected_dates=set(dates),
            cap=None,
            instruments_provider=_cefi_provider,
        )

        # expected_shards = |instruments| x |dates| = 2 x 5 = 10.
        assert result["expected_shards"] == 10
        # found_shards must be the distinct (instrument_id, date) shard-atom
        # count (10), NOT the raw row count (20) — the pipeline_mode fan-out
        # must collapse per shard atom.
        assert result["found_shards"] == 10, (
            f"pipeline_mode double-count regressed: found_shards={result['found_shards']} "
            "(expected 10 distinct shard atoms, not 20 raw batch+live rows)"
        )
        assert result["missing_shards"] == 0

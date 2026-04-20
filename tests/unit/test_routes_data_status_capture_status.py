"""Per-shard drill-down capture_status surfacing (Phase C2).

Every instrument entry returned by ``list_instruments_for_shard`` must
carry ``capture_status``, ``error_reason``, and ``attempted_at`` so the UI
can render 4-state badges + retry tooltips. Legacy manifest rows (no
capture_status column) coerce to ``captured`` to stay backward-compatible.
"""

from __future__ import annotations

from typing import cast

import pandas as pd

from deployment_api.services import data_status_drilldown as dsd


class _FakeObject:
    """Minimal stand-in for a GCS blob (mirrors the object shape used
    elsewhere — ``.name`` + ``.size`` attributes)."""

    def __init__(self, name: str, size: int = 0) -> None:
        self.name = name
        self.size = size


def _install_fakes(
    monkeypatch,
    *,
    parquet_objects: list[_FakeObject],
    availability_df: pd.DataFrame,
) -> None:
    """Patch GCS list + manifest read for the drill-down module."""

    monkeypatch.setattr(
        dsd,
        "list_objects",
        lambda bucket, prefix, max_results=10000: parquet_objects,
    )
    monkeypatch.setattr(dsd, "read_availability_index", lambda _bucket: availability_df)
    # Reset the per-module cache so each test starts clean.
    dsd.clear_drilldown_cache()


class TestDrilldownCaptureStatus:
    def test_per_symbol_rows_get_capture_status_from_manifest(self, monkeypatch) -> None:
        parquet_objects = [
            _FakeObject("raw_tick_data/by_date/day=2025-06-01/.../BTC-USDT-PERP.parquet", 100),
            _FakeObject("raw_tick_data/by_date/day=2025-06-01/.../ETH-USDT-PERP.parquet", 120),
        ]
        availability_df = pd.DataFrame(
            [
                {
                    "date": "2025-06-01",
                    "venue": "BINANCE-SWAP",
                    "instrument_id": "BTC-USDT-PERP",
                    "capture_status": "captured",
                    "error_reason": "",
                    "attempted_at": "2025-06-01T00:10:00+00:00",
                    "written_at": "2025-06-01T00:11:00+00:00",
                },
                {
                    "date": "2025-06-01",
                    "venue": "BINANCE-SWAP",
                    "instrument_id": "ETH-USDT-PERP",
                    "capture_status": "attempted_failed",
                    "error_reason": "RATE_LIMIT_HIT",
                    "attempted_at": "2025-06-01T00:12:00+00:00",
                    "written_at": "2025-06-01T00:13:00+00:00",
                },
            ]
        )
        _install_fakes(
            monkeypatch,
            parquet_objects=parquet_objects,
            availability_df=availability_df,
        )

        result = dsd.list_instruments_for_shard(
            service="market-tick-data-service",
            category="cefi",
            venue="BINANCE-SWAP",
            day="2025-06-01",
            instrument_type="perpetuals",
            data_type="trades",
        )
        instruments = cast(list[dict[str, object]], result["instruments"])
        by_id = {str(i["instrument_id"]): i for i in instruments}

        assert by_id["BTC-USDT-PERP"]["capture_status"] == "captured"
        assert by_id["BTC-USDT-PERP"]["error_reason"] == ""
        assert by_id["BTC-USDT-PERP"]["attempted_at"] == "2025-06-01T00:10:00+00:00"

        assert by_id["ETH-USDT-PERP"]["capture_status"] == "attempted_failed"
        assert by_id["ETH-USDT-PERP"]["error_reason"] == "RATE_LIMIT_HIT"
        assert by_id["ETH-USDT-PERP"]["attempted_at"] == "2025-06-01T00:12:00+00:00"

    def test_legacy_manifest_no_capture_status_column_defaults_to_captured(
        self, monkeypatch
    ) -> None:
        """Pre-v5 manifest parquet omits the capture_status column entirely."""
        parquet_objects = [
            _FakeObject("raw_tick_data/by_date/day=2025-06-01/.../BTC-USDT-PERP.parquet", 100),
        ]
        # Legacy manifest: only date / venue / instrument_id columns.
        availability_df = pd.DataFrame(
            [
                {
                    "date": "2025-06-01",
                    "venue": "BINANCE-SWAP",
                    "instrument_id": "BTC-USDT-PERP",
                }
            ]
        )
        _install_fakes(
            monkeypatch,
            parquet_objects=parquet_objects,
            availability_df=availability_df,
        )

        result = dsd.list_instruments_for_shard(
            service="market-tick-data-service",
            category="cefi",
            venue="BINANCE-SWAP",
            day="2025-06-01",
            instrument_type="perpetuals",
            data_type="trades",
        )
        instruments = cast(list[dict[str, object]], result["instruments"])
        assert instruments[0]["capture_status"] == "captured"
        assert instruments[0]["error_reason"] == ""
        assert instruments[0]["attempted_at"] == ""

    def test_multi_shard_day_latest_written_at_wins(self, monkeypatch) -> None:
        """When a (date, venue, instrument_id) has multiple manifest rows
        (re-run), the latest ``written_at`` wins — same dedup semantics as
        UTL ``_merge_dataframes``."""
        parquet_objects = [
            _FakeObject("raw_tick_data/by_date/day=2025-06-01/.../BTC-USDT-PERP.parquet", 100),
        ]
        availability_df = pd.DataFrame(
            [
                # Older failed attempt
                {
                    "date": "2025-06-01",
                    "venue": "BINANCE-SWAP",
                    "instrument_id": "BTC-USDT-PERP",
                    "capture_status": "attempted_failed",
                    "error_reason": "RATE_LIMIT_HIT",
                    "attempted_at": "2025-06-01T00:10:00+00:00",
                    "written_at": "2025-06-01T00:11:00+00:00",
                },
                # Newer successful retry
                {
                    "date": "2025-06-01",
                    "venue": "BINANCE-SWAP",
                    "instrument_id": "BTC-USDT-PERP",
                    "capture_status": "captured",
                    "error_reason": "",
                    "attempted_at": "2025-06-01T01:10:00+00:00",
                    "written_at": "2025-06-01T01:11:00+00:00",
                },
            ]
        )
        _install_fakes(
            monkeypatch,
            parquet_objects=parquet_objects,
            availability_df=availability_df,
        )

        result = dsd.list_instruments_for_shard(
            service="market-tick-data-service",
            category="cefi",
            venue="BINANCE-SWAP",
            day="2025-06-01",
            instrument_type="perpetuals",
            data_type="trades",
        )
        instruments = cast(list[dict[str, object]], result["instruments"])
        assert instruments[0]["capture_status"] == "captured"
        assert instruments[0]["error_reason"] == ""

    def test_missing_manifest_row_defaults_to_captured(self, monkeypatch) -> None:
        """Instrument present in the parquet but not in the manifest index
        (unusual but possible) defaults to captured/empty/empty."""
        parquet_objects = [
            _FakeObject("raw_tick_data/by_date/day=2025-06-01/.../BTC-USDT-PERP.parquet", 100),
        ]
        availability_df = pd.DataFrame(columns=["date", "venue", "instrument_id", "capture_status"])
        _install_fakes(
            monkeypatch,
            parquet_objects=parquet_objects,
            availability_df=availability_df,
        )

        result = dsd.list_instruments_for_shard(
            service="market-tick-data-service",
            category="cefi",
            venue="BINANCE-SWAP",
            day="2025-06-01",
            instrument_type="perpetuals",
            data_type="trades",
        )
        instruments = cast(list[dict[str, object]], result["instruments"])
        assert instruments[0]["capture_status"] == "captured"
        assert instruments[0]["error_reason"] == ""
        assert instruments[0]["attempted_at"] == ""

    def test_manifest_read_failure_is_non_fatal(self, monkeypatch) -> None:
        """A broken manifest should not break the drill-down — every
        instrument defaults to ``captured`` so the modal still renders."""
        parquet_objects = [
            _FakeObject("raw_tick_data/by_date/day=2025-06-01/.../BTC-USDT-PERP.parquet", 100),
        ]

        def _boom(_bucket):
            raise OSError("simulated manifest failure")

        monkeypatch.setattr(
            dsd,
            "list_objects",
            lambda bucket, prefix, max_results=10000: parquet_objects,
        )
        monkeypatch.setattr(dsd, "read_availability_index", _boom)
        dsd.clear_drilldown_cache()

        result = dsd.list_instruments_for_shard(
            service="market-tick-data-service",
            category="cefi",
            venue="BINANCE-SWAP",
            day="2025-06-01",
            instrument_type="perpetuals",
            data_type="trades",
        )
        instruments = cast(list[dict[str, object]], result["instruments"])
        assert instruments[0]["capture_status"] == "captured"
        assert instruments[0]["error_reason"] == ""
        assert instruments[0]["attempted_at"] == ""

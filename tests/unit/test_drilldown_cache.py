"""Unit tests for the TTL cache helpers in ``data_status_drilldown``.

Covers the cache-miss / TTL-expiry / put-then-get / clear paths. Drilldown
endpoints (per-pool breakdown, fixture detail, capture-status enrichment)
all go through this cache; without TTL expiry tests the eviction branch
silently regresses.
"""

from __future__ import annotations

from typing import cast

import pandas as pd

import deployment_api.services.data_status_drilldown as dsd


def setup_function() -> None:
    dsd.clear_drilldown_cache()


def teardown_function() -> None:
    dsd.clear_drilldown_cache()


def test_cache_miss_returns_none() -> None:
    """Empty cache → None."""
    assert dsd._cache_get("missing-key") is None


def test_cache_put_then_get_returns_value() -> None:
    """Round-trip: put → get returns the value."""
    dsd._cache_put("k", {"hello": "world"})
    value = dsd._cache_get("k")
    assert value == {"hello": "world"}


def test_cache_ttl_expiry_evicts_entry(monkeypatch: object) -> None:
    """When the TTL has elapsed, the get path evicts the entry and
    returns None — exercises the ``_cache.pop`` branch."""
    import time as time_mod

    fake_now = [1000.0]

    def fake_monotonic() -> float:
        return fake_now[0]

    # patch only the time.monotonic that the module captured at import
    monkeypatch_obj = cast(object, monkeypatch)
    monkeypatch_obj.setattr(time_mod, "monotonic", fake_monotonic)  # pyright: ignore[reportAttributeAccessIssue]

    dsd._cache_put("ephemeral", "value")
    # advance past the TTL
    fake_now[0] = 1000.0 + dsd._CACHE_TTL_SECONDS + 1.0
    out = dsd._cache_get("ephemeral")
    assert out is None
    # entry has been evicted
    assert "ephemeral" not in dsd._cache


def test_clear_drilldown_cache_empties_state() -> None:
    """``clear_drilldown_cache`` removes every entry."""
    dsd._cache_put("k1", 1)
    dsd._cache_put("k2", 2)
    assert dsd._cache_get("k1") == 1
    dsd.clear_drilldown_cache()
    assert dsd._cache_get("k1") is None
    assert dsd._cache_get("k2") is None


def test_build_capture_metadata_lookup_skips_blank_instrument_ids() -> None:
    """Instruments with empty/null ``instrument_id`` are skipped — exercises
    the ``if not iid: continue`` guard."""
    df = pd.DataFrame(
        [
            {
                "instrument_id": "BTC-USDT",
                "capture_status": "captured",
                "error_reason": "",
                "attempted_at": "2026-05-07T00:00:00Z",
            },
            {
                "instrument_id": "",  # blank → skipped
                "capture_status": "empty_confirmed",
                "error_reason": "SOURCE_RETURNED_ZERO",
                "attempted_at": "2026-05-07T00:00:00Z",
            },
            {
                "instrument_id": None,  # null → skipped
                "capture_status": "attempted_failed",
                "error_reason": "RATE_LIMIT",
                "attempted_at": "",
            },
        ]
    )
    out = dsd._build_capture_metadata_lookup(df)
    assert "BTC-USDT" in out
    assert out["BTC-USDT"]["capture_status"] == "captured"
    # blank + None entries dropped
    assert "" not in out
    assert len(out) == 1


def test_build_capture_metadata_lookup_handles_null_status_fields() -> None:
    """Null ``capture_status`` defaults to "captured"; null other columns
    coerce to empty strings."""
    df = pd.DataFrame(
        [
            {
                "instrument_id": "ETH-USDT",
                "capture_status": None,  # → default
                "error_reason": None,
                "attempted_at": None,
            },
        ]
    )
    out = dsd._build_capture_metadata_lookup(df)
    assert out["ETH-USDT"]["capture_status"] == dsd._DEFAULT_CAPTURE_STATUS
    assert out["ETH-USDT"]["error_reason"] == ""
    assert out["ETH-USDT"]["attempted_at"] == ""


# ---------------------------------------------------------------------------
# _apply_default_capture_status — defensive-default stamping
# ---------------------------------------------------------------------------


def test_apply_default_capture_status_stamps_missing_keys() -> None:
    """Instruments without capture_status/error_reason/attempted_at get
    the safe defaults set."""
    instruments: list[dict[str, object]] = [
        {"instrument_id": "BTC-USDT"},
        {"instrument_id": "ETH-USDT"},
    ]
    dsd._apply_default_capture_status(instruments)
    for inst in instruments:
        assert inst["capture_status"] == dsd._DEFAULT_CAPTURE_STATUS
        assert inst["error_reason"] == ""
        assert inst["attempted_at"] == ""


def test_apply_default_capture_status_does_not_overwrite_existing() -> None:
    """``setdefault`` preserves existing values."""
    instruments: list[dict[str, object]] = [
        {
            "instrument_id": "BTC-USDT",
            "capture_status": "empty_confirmed",
            "error_reason": "EXPECTED_HOLIDAY",
            "attempted_at": "2026-05-07T12:00:00Z",
        },
    ]
    dsd._apply_default_capture_status(instruments)
    assert instruments[0]["capture_status"] == "empty_confirmed"
    assert instruments[0]["error_reason"] == "EXPECTED_HOLIDAY"
    assert instruments[0]["attempted_at"] == "2026-05-07T12:00:00Z"


def test_apply_default_capture_status_handles_empty_list() -> None:
    """No-op on empty list."""
    instruments: list[dict[str, object]] = []
    dsd._apply_default_capture_status(instruments)  # should not raise
    assert instruments == []


# ---------------------------------------------------------------------------
# _attach_capture_status_to_instruments — empty-input guard
# ---------------------------------------------------------------------------


def test_attach_capture_status_to_instruments_empty_list_short_circuits() -> None:
    """Empty instrument list → early return, no manifest read."""
    instruments: list[dict[str, object]] = []
    dsd._attach_capture_status_to_instruments(
        instruments,
        bucket="any-bucket",
        venue="BYBIT",
        day="2026-05-07",
    )
    assert instruments == []

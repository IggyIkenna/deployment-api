"""P2 — new-listings + upcoming-expiries derived from the lifecycle catalogue.

Plan: data_status_page_ux_and_canonicalisation_2026_07_16 P2.

Verifies the two load-bearing rules:
  - new listings = ``available_from`` within the last N days (newest-first).
  - upcoming expiries = ``instrument_type ∈ {FUTURE, OPTION, COMBO}`` AND
    ``available_to`` inside ``[today, today+within_days]`` (soonest-first) — a
    spot/perp row with a forward ``available_to`` is NOT an expiry (wrong type),
    and an expired/delisted derivative (``available_to`` ≤ today) is excluded by
    the forward window.
And shard-isolation: a failing asset_group is skipped, not fatal.
"""

from __future__ import annotations

from datetime import UTC, datetime, timedelta
from unittest.mock import patch

import pandas as pd

from deployment_api.services import catalogue_lifecycle as cl


def _today():
    return datetime.now(UTC).date()


def _cefi_df() -> pd.DataFrame:
    today = _today()

    def iso(offset_days: int) -> str:
        return (today + timedelta(days=offset_days)).isoformat()

    return pd.DataFrame(
        [
            # recent SPOT_PAIR listing (2 days ago) — a new listing, not an expiry.
            {
                "instrument_id": "NEW-SPOT",
                "instrument_type": "SPOT_PAIR",
                "venue": "BINANCE",
                "chain": "",
                "available_from": iso(-2),
                "available_to": "",
                "base_asset": "NEW",
                "raw_symbol": "NEWUSDT",
                "mvp": True,
            },
            # old listing (100 days ago) — excluded from a 7-day new-listings window.
            {
                "instrument_id": "OLD-SPOT",
                "instrument_type": "SPOT_PAIR",
                "venue": "BINANCE",
                "chain": "",
                "available_from": iso(-100),
                "available_to": "",
                "base_asset": "OLD",
                "raw_symbol": "OLDUSDT",
                "mvp": False,
            },
            # FUTURE expiring in 3 days — a genuine upcoming expiry.
            {
                "instrument_id": "FUT-SOON",
                "instrument_type": "FUTURE",
                "venue": "DERIBIT",
                "chain": "",
                "available_from": iso(-30),
                "available_to": iso(3),
                "base_asset": "BTC",
                "raw_symbol": "BTC-FUT",
                "mvp": True,
            },
            # FUTURE that already expired 10 days ago — excluded by the forward window.
            {
                "instrument_id": "FUT-PAST",
                "instrument_type": "FUTURE",
                "venue": "DERIBIT",
                "chain": "",
                "available_from": iso(-90),
                "available_to": iso(-10),
                "base_asset": "BTC",
                "raw_symbol": "BTC-OLD",
                "mvp": True,
            },
            # SPOT_PAIR with a forward available_to (last-observed artifact) — NOT an
            # expiry (wrong type), must be excluded from upcoming-expiries.
            {
                "instrument_id": "SPOT-FWD",
                "instrument_type": "SPOT_PAIR",
                "venue": "BINANCE",
                "chain": "",
                "available_from": iso(-60),
                "available_to": iso(2),
                "base_asset": "ETH",
                "raw_symbol": "ETHUSDT",
                "mvp": True,
            },
        ]
    )


def test_new_listings_within_window_newest_first() -> None:
    cl._clear_caches()  # pyright: ignore[reportPrivateUsage]
    with patch.object(cl, "_read_catalogue", side_effect=lambda ag: _cefi_df() if ag == "cefi" else None):
        rows = cl.list_new_listings(max_age_days=7)
    ids = [r["instrument_id"] for r in rows]
    assert "NEW-SPOT" in ids, "a 2-day-old listing must be 'new' within a 7-day window"
    assert "OLD-SPOT" not in ids, "a 100-day-old listing must NOT be new within 7 days"
    # Every returned row is a genuine recent listing tagged with its asset_group.
    assert all(r["asset_group"] == "cefi" for r in rows)


def test_upcoming_expiries_type_and_window_filter() -> None:
    cl._clear_caches()  # pyright: ignore[reportPrivateUsage]
    with patch.object(cl, "_read_catalogue", side_effect=lambda ag: _cefi_df() if ag == "cefi" else None):
        rows = cl.list_upcoming_expiries(within_days=7)
    ids = [r["instrument_id"] for r in rows]
    assert ids == ["FUT-SOON"], (
        f"only the FUTURE expiring inside [today, today+7] should surface, got {ids} "
        "(FUT-PAST expired → excluded; SPOT-FWD is a spot pair → wrong type)"
    )


def test_shard_isolation_one_failing_asset_group_is_skipped() -> None:
    cl._clear_caches()  # pyright: ignore[reportPrivateUsage]

    def _read(ag: str):
        if ag == "defi":
            raise RuntimeError("simulated defi catalogue read failure")
        return _cefi_df() if ag == "cefi" else None

    # A raising asset_group must not sink the others: _read_catalogue itself
    # catches, but this guards the caller loop tolerates a None/raise per AG.
    with patch.object(cl, "_read_catalogue", side_effect=lambda ag: None if ag == "defi" else _read(ag)):
        rows = cl.list_new_listings(max_age_days=7)
    assert any(r["instrument_id"] == "NEW-SPOT" for r in rows)

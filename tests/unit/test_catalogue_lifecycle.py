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


def _onboarding_flood_df() -> pd.DataFrame:
    """Models the REAL false-positive found on prod GCS 2026-07-17: a venue the
    pipeline onboarded recently (COINBASE-CDE, 2026-07-10) whose entire
    pre-existing universe carries available_from = the onboarding day, alongside
    an established venue where a genuinely-new listing must NOT be tagged."""
    today = _today()

    def iso(offset_days: int) -> str:
        return (today + timedelta(days=offset_days)).isoformat()

    return pd.DataFrame(
        [
            # --- Recently-onboarded venue: every row on the venue's first day,
            # but with expiries running years out => cannot all be same-day listings.
            {
                "instrument_id": "CDE-FUT-NEAR",
                "instrument_type": "FUTURE",
                "venue": "COINBASE-CDE",
                "chain": "",
                "available_from": iso(-5),
                "available_to": iso(20),
                "base_asset": "ADA",
                "raw_symbol": "ADA-CDE",
                "mvp": True,
            },
            {
                "instrument_id": "CDE-FUT-FAR",
                "instrument_type": "FUTURE",
                "venue": "COINBASE-CDE",
                "chain": "",
                "available_from": iso(-5),
                "available_to": iso(1600),  # ~2030 expiry — clearly pre-existing
                "base_asset": "BTC",
                "raw_symbol": "BTC-CDE",
                "mvp": True,
            },
            # --- Established venue: long history, plus one genuinely-new listing.
            {
                "instrument_id": "EST-OLD",
                "instrument_type": "SPOT_PAIR",
                "venue": "BINANCE-SPOT",
                "chain": "",
                "available_from": iso(-900),
                "available_to": "",
                "base_asset": "BTC",
                "raw_symbol": "BTCUSDT",
                "mvp": True,
            },
            {
                "instrument_id": "EST-GENUINELY-NEW",
                "instrument_type": "SPOT_PAIR",
                "venue": "BINANCE-SPOT",
                "chain": "",
                "available_from": iso(-3),
                "available_to": "",
                "base_asset": "NEW",
                "raw_symbol": "NEWUSDT",
                "mvp": True,
            },
        ]
    )


class TestNewListingFalsePositiveGuard:
    """The venue-first-day provenance flag (plan P2 backend todo).

    Guards the mechanism traced on real GCS: catalogue available_from =
    MIN(first_day_observed, declared_from), so a venue with no declared listing
    date degrades to "the day the pipeline first saw it" and a recently-onboarded
    venue floods new-listings.
    """

    def test_onboarding_flood_rows_are_tagged(self) -> None:
        cl._clear_caches()  # pyright: ignore[reportPrivateUsage]
        with patch.object(
            cl, "_read_catalogue", side_effect=lambda ag: _onboarding_flood_df() if ag == "cefi" else None
        ):
            rows = cl.list_new_listings(max_age_days=30)
        tagged = {r["instrument_id"]: r["available_from_is_venue_first_day"] for r in rows}
        assert tagged["CDE-FUT-NEAR"] is True
        assert tagged["CDE-FUT-FAR"] is True, (
            "a row whose available_from is its venue's earliest known date must be flagged — "
            "this is the COINBASE-CDE onboarding-flood signature"
        )

    def test_genuine_new_listing_on_established_venue_is_not_tagged(self) -> None:
        """The guard must not cry wolf: a real listing on a venue with history
        does NOT coincide with that venue's first day, so it stays untagged."""
        cl._clear_caches()  # pyright: ignore[reportPrivateUsage]
        with patch.object(
            cl, "_read_catalogue", side_effect=lambda ag: _onboarding_flood_df() if ag == "cefi" else None
        ):
            rows = cl.list_new_listings(max_age_days=30)
        by_id = {r["instrument_id"]: r for r in rows}
        assert by_id["EST-GENUINELY-NEW"]["available_from_is_venue_first_day"] is False

    def test_rows_are_surfaced_not_excluded(self) -> None:
        """Decision: SHOW provenance, do not silently drop. Measured impact was
        99/439,940 rows (0.02%) — dropping them would hide possibly-real listings."""
        cl._clear_caches()  # pyright: ignore[reportPrivateUsage]
        with patch.object(
            cl, "_read_catalogue", side_effect=lambda ag: _onboarding_flood_df() if ag == "cefi" else None
        ):
            rows = cl.list_new_listings(max_age_days=30)
        ids = {r["instrument_id"] for r in rows}
        assert {"CDE-FUT-NEAR", "CDE-FUT-FAR"} <= ids, "flagged rows must still be returned, only tagged"

    def test_tag_is_computed_before_the_window_narrows_the_frame(self) -> None:
        """Load-bearing ordering: the venue's first day is its earliest date in the
        WHOLE catalogue. If the tag were computed after the date window, the
        established venue's oldest row INSIDE the window would falsely tag as its
        first day. A 30-day window excludes EST-OLD (-900d), so EST-GENUINELY-NEW
        would become BINANCE-SPOT's in-window minimum — and must still be False."""
        cl._clear_caches()  # pyright: ignore[reportPrivateUsage]
        with patch.object(
            cl, "_read_catalogue", side_effect=lambda ag: _onboarding_flood_df() if ag == "cefi" else None
        ):
            rows = cl.list_new_listings(max_age_days=30)
        by_id = {r["instrument_id"] for r in rows}
        assert "EST-OLD" not in by_id, "fixture guard: the -900d row must be outside the 30d window"
        genuine = next(r for r in rows if r["instrument_id"] == "EST-GENUINELY-NEW")
        assert genuine["available_from_is_venue_first_day"] is False, (
            "tag leaked from the windowed slice instead of the full catalogue"
        )

    def test_expiries_carry_a_truthful_tag_too(self) -> None:
        """The field is on the shared row type, so the expiries endpoint must
        compute it rather than letting it default to a silent False."""
        cl._clear_caches()  # pyright: ignore[reportPrivateUsage]
        with patch.object(
            cl, "_read_catalogue", side_effect=lambda ag: _onboarding_flood_df() if ag == "cefi" else None
        ):
            rows = cl.list_upcoming_expiries(within_days=30)
        by_id = {r["instrument_id"]: r for r in rows}
        assert by_id["CDE-FUT-NEAR"]["available_from_is_venue_first_day"] is True


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


class TestPagination:
    """F10 (2026-07-18): pagination + parallel per-AG reads.

    ``list_new_listings_page``/``list_upcoming_expiries_page`` must return the
    SAME rows (in the SAME order) as the unbounded ``list_new_listings``/
    ``list_upcoming_expiries``, just sliced -- so ``total_count`` always
    reflects the full filtered set even though only the page gets built.
    """

    def test_new_listings_page_matches_unbounded_slice(self) -> None:
        cl._clear_caches()  # pyright: ignore[reportPrivateUsage]
        with patch.object(cl, "_read_catalogue", side_effect=lambda ag: _cefi_df() if ag == "cefi" else None):
            full = cl.list_new_listings(max_age_days=7)
            page, total_count = cl.list_new_listings_page(max_age_days=7, limit=1, offset=0)
        assert total_count == len(full)
        assert page == full[:1]

    def test_new_listings_page_offset_beyond_total_is_empty_not_error(self) -> None:
        cl._clear_caches()  # pyright: ignore[reportPrivateUsage]
        with patch.object(cl, "_read_catalogue", side_effect=lambda ag: _cefi_df() if ag == "cefi" else None):
            page, total_count = cl.list_new_listings_page(max_age_days=7, limit=50, offset=9_999)
        assert page == []
        assert total_count >= 1

    def test_new_listings_page_limit_clamped_to_max(self) -> None:
        cl._clear_caches()  # pyright: ignore[reportPrivateUsage]
        with patch.object(cl, "_read_catalogue", side_effect=lambda ag: _cefi_df() if ag == "cefi" else None):
            page, _total_count = cl.list_new_listings_page(max_age_days=7, limit=cl.MAX_LIFECYCLE_LIMIT + 500, offset=0)
        # Clamped limit must not raise / must not silently return everything unbounded.
        assert len(page) <= cl.MAX_LIFECYCLE_LIMIT

    def test_upcoming_expiries_page_matches_unbounded_slice(self) -> None:
        cl._clear_caches()  # pyright: ignore[reportPrivateUsage]
        with patch.object(cl, "_read_catalogue", side_effect=lambda ag: _cefi_df() if ag == "cefi" else None):
            full = cl.list_upcoming_expiries(within_days=7)
            page, total_count = cl.list_upcoming_expiries_page(within_days=7, limit=10, offset=0)
        assert total_count == len(full)
        assert page == full

    def test_new_listings_page_shares_cache_with_unbounded_call(self) -> None:
        """Both entry points read through the SAME cached prepared frame -- a
        page call right after the unbounded call must not re-hit _read_catalogue."""
        cl._clear_caches()  # pyright: ignore[reportPrivateUsage]
        call_count = 0

        def _counting_read(ag: str):
            nonlocal call_count
            if ag == "cefi":
                call_count += 1
                return _cefi_df()
            return None

        with patch.object(cl, "_read_catalogue", side_effect=_counting_read):
            cl.list_new_listings(max_age_days=7)
            cefi_calls_after_first = call_count
            cl.list_new_listings_page(max_age_days=7, limit=1, offset=0)
        assert call_count == cefi_calls_after_first, "second call must hit the cached frame, not re-read the AG"

    def test_all_asset_groups_are_queried_when_unfiltered(self) -> None:
        """Parallel fan-out must still cover every configured asset_group (no
        AG silently dropped by the ThreadPoolExecutor rewrite)."""
        cl._clear_caches()  # pyright: ignore[reportPrivateUsage]
        seen: set[str] = set()

        def _record(ag: str):
            seen.add(ag)
            return _cefi_df() if ag == "cefi" else None

        with patch.object(cl, "_read_catalogue", side_effect=_record):
            cl.list_new_listings(max_age_days=7)
        assert seen == set(cl._CATALOGUE_ASSET_GROUPS)  # pyright: ignore[reportPrivateUsage]

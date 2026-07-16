"""P3 — prediction-market catalogue browser (category / cqg / search / pagination).

Plan: data_status_page_ux_and_canonicalisation_2026_07_16 P3.

Uses the REAL UAC classifiers (not mocked) — a Kalshi ticker / Polymarket slug
picked from already-proven mappings in ``test_predictions_canonical_groups.py``
(``KXBTCD-*`` -> BTC_UP_DOWN_DAILY, ``KXCPIYOY-*`` -> CPI_PRINT_PER_MONTH,
``KXMLBGAME-*`` -> SPORTS_MLB_MATCH; the equivalent Polymarket slugs classify to
the same groups), so classification is deterministic and not a test double.

Verifies:
  - facet counts (category_counts) reflect the full catalogue (bundle-grain
    ``prediction_canonical_question_group`` rows excluded) and a ``category``
    filter narrows the rows while facet counts stay unnarrowed (so the UI
    ``<select>`` can always render every bucket's count).
  - a ``canonical_question_group`` sub-filter narrows to one cqg across venues.
  - substring ``search`` (over label/instrument_id/cqg) + limit/offset pagination.
  - the honest label fallback chain + bundle-row exclusion + shard-isolated
    read failure (never raises, returns an honest empty result).
"""

from __future__ import annotations

from unittest.mock import patch

import pandas as pd

from deployment_api.services import prediction_catalogue as pc


def _fixture_df() -> pd.DataFrame:
    return pd.DataFrame(
        [
            # BTC daily up/down — Polymarket side (crypto).
            {
                "instrument_id": "COND-BTC-1",
                "instrument_type": "PREDICTION_MARKET",
                "venue": "POLYMARKET",
                "raw_symbol": "bitcoin-up-or-down-june-24-2026",
                "base_asset": "POLYMARKET:UP_DOWN:BTC:1D:2026-06-24",
                "underlying": "BTC",
                "available_from": "2026-06-01",
                "available_to": "2026-06-24",
                "data_type": "trades",
                "mvp": True,
            },
            # BTC daily up/down — Kalshi side (same cqg, same category, other venue).
            {
                "instrument_id": "KXBTCD-26JUN23",
                "instrument_type": "PREDICTION_MARKET",
                "venue": "KALSHI",
                "raw_symbol": "KXBTCD-26JUN23",
                "base_asset": "KXBTCD",
                "underlying": "BTC",
                "available_from": "2026-06-01",
                "available_to": "2026-06-23",
                "data_type": "trades",
                "mvp": True,
            },
            # CPI print (financial).
            {
                "instrument_id": "COND-CPI-1",
                "instrument_type": "PREDICTION_MARKET",
                "venue": "POLYMARKET",
                "raw_symbol": "cpi-inflation-above-3-percent-june-2026",
                "base_asset": "POLYMARKET:UP_DOWN:CPI:1M:2026-06",
                "underlying": "CPI",
                "available_from": "2026-05-01",
                "available_to": "2026-06-30",
                "data_type": "trades",
                "mvp": False,
            },
            # EPL fixture (sports).
            {
                "instrument_id": "COND-EPL-1",
                "instrument_type": "PREDICTION_MARKET",
                "venue": "POLYMARKET",
                "raw_symbol": "epl-arsenal-vs-chelsea-2026-03-22",
                "base_asset": "EPL:ARSENAL-CHELSEA:2025-26",
                "underlying": "",
                "available_from": "2026-03-01",
                "available_to": "2026-03-22",
                "data_type": "trades",
                "mvp": True,
            },
            # No raw_symbol (honest-absence case) — label falls back to base_asset
            # (truncated to 50 chars); classifies to OTHER/other via instrument_id.
            {
                "instrument_id": "COND-OTHER-1",
                "instrument_type": "PREDICTION_MARKET",
                "venue": "POLYMARKET",
                "raw_symbol": "",
                "base_asset": "a base-asset question text long enough to exercise the 50-char truncation rule",
                "underlying": "",
                "available_from": "2026-01-01",
                "available_to": "",
                "data_type": "trades",
                "mvp": False,
            },
            # cqg-bundle summary row (data_type=prediction_canonical_question_group,
            # instrument_id=cqg) — a shard-tracking row, NEVER a browsable market.
            {
                "instrument_id": "BTC_UP_DOWN_DAILY",
                "instrument_type": "",
                "venue": "POLYMARKET",
                "raw_symbol": "",
                "base_asset": "",
                "underlying": "",
                "available_from": "2026-06-01",
                "available_to": "2026-06-24",
                "data_type": "prediction_canonical_question_group",
                "mvp": False,
            },
        ]
    )


def test_category_facet_counts_and_filter() -> None:
    """category_counts covers the whole catalogue (bundle row excluded); a
    category filter narrows ``rows`` while the facet counts stay unnarrowed."""
    pc._clear_cache()  # pyright: ignore[reportPrivateUsage]
    with patch.object(pc, "_read_catalogue", return_value=_fixture_df()):
        unfiltered = pc.read_prediction_catalogue()

    assert unfiltered["total"] == 5, "the cqg-bundle row must not count as a browsable market"
    assert unfiltered["category_counts"] == {"crypto": 2, "financial": 1, "sports": 1, "other": 1}
    ids = {r["instrument_id"] for r in unfiltered["rows"]}
    assert "BTC_UP_DOWN_DAILY" not in ids, "the bundle-grain summary row must never surface as a market"

    pc._clear_cache()  # pyright: ignore[reportPrivateUsage]
    with patch.object(pc, "_read_catalogue", return_value=_fixture_df()):
        crypto_only = pc.read_prediction_catalogue(category="CRYPTO")  # case-insensitive

    assert crypto_only["total"] == 2
    assert all(r["category"] == "crypto" for r in crypto_only["rows"])
    # Facets still reflect every bucket so the UI <select> can render all of them.
    assert crypto_only["category_counts"] == {"crypto": 2, "financial": 1, "sports": 1, "other": 1}


def test_canonical_question_group_sub_filter() -> None:
    """A cqg sub-filter narrows to one canonical_question_group across BOTH venues."""
    pc._clear_cache()  # pyright: ignore[reportPrivateUsage]
    with patch.object(pc, "_read_catalogue", return_value=_fixture_df()):
        result = pc.read_prediction_catalogue(canonical_question_group="btc_up_down_daily")  # case-insensitive

    assert result["total"] == 2
    ids = {r["instrument_id"] for r in result["rows"]}
    assert ids == {"COND-BTC-1", "KXBTCD-26JUN23"}
    assert all(r["canonical_question_group"] == "BTC_UP_DOWN_DAILY" for r in result["rows"])


def test_search_and_pagination() -> None:
    """Substring search spans label/instrument_id/cqg; limit/offset paginate a
    deterministic (venue, label) order while ``total`` reflects the full filtered set."""
    pc._clear_cache()  # pyright: ignore[reportPrivateUsage]
    with patch.object(pc, "_read_catalogue", return_value=_fixture_df()):
        both = pc.read_prediction_catalogue(search="btc")

    # "btc" matches the Kalshi label directly AND the Polymarket row's cqg
    # (BTC_UP_DOWN_DAILY), even though "bitcoin" itself has no "btc" substring.
    assert both["total"] == 2
    assert {r["instrument_id"] for r in both["rows"]} == {"COND-BTC-1", "KXBTCD-26JUN23"}

    pc._clear_cache()  # pyright: ignore[reportPrivateUsage]
    with patch.object(pc, "_read_catalogue", return_value=_fixture_df()):
        page1 = pc.read_prediction_catalogue(search="btc", limit=1, offset=0)
    pc._clear_cache()  # pyright: ignore[reportPrivateUsage]
    with patch.object(pc, "_read_catalogue", return_value=_fixture_df()):
        page2 = pc.read_prediction_catalogue(search="btc", limit=1, offset=1)

    assert page1["total"] == 2 and page2["total"] == 2, "total reflects the whole filtered set, not the page"
    assert len(page1["rows"]) == 1 and len(page2["rows"]) == 1
    assert page1["rows"][0]["venue"] == "KALSHI"  # "KALSHI" < "POLYMARKET" — deterministic sort
    assert page2["rows"][0]["venue"] == "POLYMARKET"


def test_label_fallback_chain_and_read_failure_is_honest_empty() -> None:
    """Honest label fallback (raw_symbol -> base_asset, truncated -> instrument_id)
    and a catalogue-read failure returns an empty result, never raises."""
    pc._clear_cache()  # pyright: ignore[reportPrivateUsage]
    with patch.object(pc, "_read_catalogue", return_value=_fixture_df()):
        result = pc.read_prediction_catalogue(search="other-1")

    other_rows = [r for r in result["rows"] if r["instrument_id"] == "COND-OTHER-1"]
    assert len(other_rows) == 1
    label = other_rows[0]["label"]
    base_asset = "a base-asset question text long enough to exercise the 50-char truncation rule"
    assert label == base_asset[:50]
    assert len(label) == 50, "base_asset fallback must truncate at 50 chars"

    pc._clear_cache()  # pyright: ignore[reportPrivateUsage]
    with patch.object(pc, "_read_catalogue", return_value=None):
        empty = pc.read_prediction_catalogue()

    assert empty == {"rows": [], "total": 0, "category_counts": {}, "cqg_counts": {}}

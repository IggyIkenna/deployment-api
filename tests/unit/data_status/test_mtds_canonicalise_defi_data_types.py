"""Unit tests for DEFI ``data_type`` canonicalisation
(``deployment_api.services.data_status.mtds.canonicalise_defi_data_types``).

Zero test references existed for this function despite it sitting on the
LIVE honest-coverage read path for every DEFI request —
``venue_resolution.py::_apply_mtds_honest_coverage`` (line ~209) and
``manifest.py::DataStatusService`` category loop (line ~608) both call it
before the UAC ``(venue, data_type)`` honest-coverage filter runs. Pins the
three transforms the function's docstring promises so a future edit can't
silently regress the DEFI coverage numbers:

1. hyphenated sub-dim bucket names -> canonical underscore form
   (``DEFI_DATA_TYPE_ALIASES``).
2. blank ``data_type`` inferred from ``_defi_source``
   (``DEFI_SOURCE_TO_DATA_TYPE``).
3. already-canonical / no-``data_type``-column input passes through
   untouched.
"""

from __future__ import annotations

import pandas as pd

from deployment_api.services.data_status.mtds import canonicalise_defi_data_types


def test_hyphenated_data_type_renamed_to_canonical_underscore_form() -> None:
    df = pd.DataFrame(
        {
            "data_type": ["dex-swaps", "lending-indices", "perp-funding"],
            "venue": ["UNISWAP", "AAVE", "GMX"],
        }
    )
    out = canonicalise_defi_data_types(df)
    assert out["data_type"].tolist() == ["dex_pool_swaps", "lending_indices", "perp_funding"]


def test_blank_data_type_inferred_from_defi_source() -> None:
    df = pd.DataFrame(
        {
            "data_type": ["", ""],
            "_defi_source": ["dex-pools", "oracle-prices"],
            "venue": ["UNISWAP", "CHAINLINK"],
        }
    )
    out = canonicalise_defi_data_types(df)
    assert out["data_type"].tolist() == ["dex_pool_state", "oracle_prices"]


def test_non_blank_data_type_not_overridden_by_defi_source() -> None:
    """The ``_defi_source`` inference only fires on BLANK data_type rows —
    a row that already carries a data_type is left alone even if
    ``_defi_source`` disagrees."""
    df = pd.DataFrame(
        {
            "data_type": ["dex-swaps"],
            "_defi_source": ["oracle-prices"],
            "venue": ["UNISWAP"],
        }
    )
    out = canonicalise_defi_data_types(df)
    assert out["data_type"].tolist() == ["dex_pool_swaps"]


def test_already_canonical_data_type_passes_through_unchanged() -> None:
    df = pd.DataFrame(
        {
            "data_type": ["dex_pool_state", "lending_indices", "some_future_dt"],
            "venue": ["UNISWAP", "AAVE", "NEWVENUE"],
        }
    )
    out = canonicalise_defi_data_types(df)
    assert out["data_type"].tolist() == ["dex_pool_state", "lending_indices", "some_future_dt"]


def test_missing_data_type_column_returns_frame_unchanged() -> None:
    df = pd.DataFrame({"venue": ["UNISWAP"]})
    out = canonicalise_defi_data_types(df)
    assert out is df

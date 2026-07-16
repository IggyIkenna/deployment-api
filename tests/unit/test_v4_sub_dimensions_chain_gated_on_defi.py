"""P7 — CeFi chain-axis drift: the ``chains`` sub-dimension is DeFi-only.

Plan: ``data_status_page_ux_and_canonicalisation_2026_07_16.md`` P7.

Regression: the cefi CLOB-perp venues ``PACIFICA-SOLANA`` / ``LIGHTER-ZKSYNC``
carry DeFi-style ``{PROTOCOL}-{CHAIN}`` names, and the live cefi manifest holds
residual split rows (``venue=PACIFICA, chain=SOLANA`` / ``venue=LIGHTER,
chain=ZKSYNC``). ``chain`` is a shard axis ONLY for defi (UAC
``SHARD_AXIS_MATRIX``); cefi/tradfi key on ``venue`` alone. So the
``_build_v4_sub_dimensions`` ``chains`` breakdown must NOT fire for cefi even
when the ``chain`` column happens to be populated — otherwise the cefi
Instrument-Coverage-Summary manufactures ``SOLANA`` / ``ZKSYNC`` chain sub-rows
from those venue names.
"""

from __future__ import annotations

from unittest.mock import MagicMock

import pandas as pd

from deployment_api.services.data_status_service import DataStatusService


def _stub_venue_mapping() -> MagicMock:
    mapping = MagicMock()
    mapping.get_venue_start_date = MagicMock(return_value=None)
    mapping.get_expected_trading_dates = MagicMock(return_value=[])
    return mapping


def _cefi_residual_chain_frame() -> pd.DataFrame:
    """Mirror the live cefi drift: PACIFICA/LIGHTER with a populated chain."""
    rows: list[dict[str, object]] = []
    for venue, chain in (("PACIFICA", "SOLANA"), ("LIGHTER", "ZKSYNC")):
        for day in ("2024-03-04", "2024-03-05"):
            rows.append(
                {
                    "venue": venue,
                    "chain": chain,
                    "data_type": "perpetual_ohlcv",
                    "instrument_id": f"{venue}-BTC",
                    "instrument_type": "PERPETUAL",
                    "date": day,
                    "capture_status": "captured",
                    "asset_group": "cefi",
                }
            )
    return pd.DataFrame(rows)


def _defi_chain_frame() -> pd.DataFrame:
    rows: list[dict[str, object]] = []
    for day in ("2024-03-04", "2024-03-05"):
        rows.append(
            {
                "venue": "AAVE_V3-ARBITRUM",
                "chain": "ARBITRUM",
                "data_type": "lending_indices",
                "instrument_id": "USDC",
                "instrument_type": "",
                "date": day,
                "capture_status": "captured",
                "asset_group": "defi",
            }
        )
    return pd.DataFrame(rows)


def test_cefi_residual_chain_does_not_produce_chains_breakdown() -> None:
    """CeFi with a populated ``chain`` column emits NO ``chains`` sub-dimension."""
    dss = DataStatusService()
    extras = dss._build_v4_sub_dimensions(  # pyright: ignore[reportPrivateUsage]
        _cefi_residual_chain_frame(),
        "instruments-service",
        "cefi",
        "2024-03-04",
        "2024-03-05",
        _stub_venue_mapping(),
    )
    assert "chains" not in extras, (
        "cefi manufactured a chains breakdown from PACIFICA-SOLANA / "
        f"LIGHTER-ZKSYNC venue names: {extras.get('chains')!r}"
    )


def test_defi_chain_still_produces_chains_breakdown() -> None:
    """DeFi with a populated ``chain`` column still emits the ``chains`` sub-dimension."""
    dss = DataStatusService()
    extras = dss._build_v4_sub_dimensions(  # pyright: ignore[reportPrivateUsage]
        _defi_chain_frame(),
        "instruments-service",
        "defi",
        "2024-03-04",
        "2024-03-05",
        _stub_venue_mapping(),
    )
    chains = extras.get("chains")
    assert isinstance(chains, dict) and "ARBITRUM" in chains, (
        f"defi lost its chains breakdown after the P7 gate: {extras.get('chains')!r}"
    )

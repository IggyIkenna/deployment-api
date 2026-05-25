"""Synthetic availability-index generator for ``CLOUD_MOCK_MODE=true``.

When the deployment-api runs locally in mock mode there is no GCS manifest
to read, so :func:`get_hierarchical_drilldown` returns an empty tree. This
module synthesises a realistic per-shard availability DataFrame with the same
columns the real ``read_availability_index`` returns — the shard axes plus
``date`` and ``capture_status`` — so the redesigned Data Status UI has data
to render during local development.

Deterministic: ``capture_status`` for each row is a stable function of the
row's axis values + date, so repeated requests return identical trees.

The axis VALUE sets below are illustrative (NOT exhaustive) — local-dev mock
data only. The axis STRUCTURE comes from the real SSOT
(``data_status_axis_matrix``) via the caller.
"""

from __future__ import annotations

import itertools
import zlib
from datetime import date, timedelta

import pandas as pd

# Illustrative per-axis value sets — local-dev mock only.
_VENUES: dict[str, list[str]] = {
    "cefi": ["BINANCE-SPOT", "BINANCE-FUTURES", "BYBIT", "OKX", "DERIBIT", "KRAKEN"],
    "defi": ["UNISWAP_V3", "CURVE", "AAVE_V3", "BALANCER", "SUSHISWAP"],
    "tradfi": ["DATABENTO"],
    "prediction": ["POLYMARKET", "KALSHI"],
}
_CHAINS: list[str] = ["ETHEREUM", "ARBITRUM", "BASE", "POLYGON", "SOLANA"]
_DATA_TYPES: dict[str, list[str]] = {
    "cefi": ["trades", "quotes", "funding_rate", "open_interest", "liquidations"],
    "defi": ["swaps", "pool_state", "lending_rates"],
    "tradfi": ["trades", "quotes"],
    "sports": ["fixtures", "odds", "results"],
    "prediction": ["prices", "resolutions"],
}
_INSTRUMENT_TYPES: dict[str, list[str]] = {
    "cefi": ["SPOT", "PERPETUAL", "FUTURE", "OPTION"],
    "tradfi": ["FUTURE"],
    "defi": ["POOL", "LENDING", "LST"],
}
_INSTRUMENT_IDS: list[str] = [
    "BTC-USDT", "ETH-USDT", "SOL-USDT", "XRP-USDT", "ADA-USDT", "DOGE-USDT",
    "AVAX-USDT", "LINK-USDT", "MATIC-USDT", "DOT-USDT", "BNB-USDT", "LTC-USDT",
]
_LEAGUE_IDS: list[str] = ["EPL", "NBA", "NFL", "MLB", "NHL", "LALIGA"]
_FEATURE_GROUPS: list[str] = ["delta_one", "volatility", "onchain", "microstructure"]
_TIMEFRAMES: list[str] = ["1m", "5m", "15m", "1h", "1d"]
_QUESTION_GROUPS: list[str] = ["ELECTION_2026", "CRYPTO_PRICE", "SPORTS_OUTCOME"]
_MODEL_FAMILIES: list[str] = ["xgboost", "lstm", "transformer"]
_TRAINING_PERIODS: list[str] = ["2024_2025", "2025_2026"]
_JOB_IDS: list[str] = ["job_a1b2", "job_c3d4", "job_e5f6"]
_STRATEGY_IDS: list[str] = ["carry_basis_v1", "arb_dispersion_v1", "mm_v2"]
_INSTRUCTION_TYPES: list[str] = ["twap", "vwap", "passive", "aggressive"]

# (p_attempted_failed, p_empty_confirmed) per asset_group — remainder = captured.
_AG_PROFILE: dict[str, tuple[float, float]] = {
    "cefi": (0.012, 0.03),
    "defi": (0.09, 0.07),
    "tradfi": (0.005, 0.27),
    "sports": (0.02, 0.16),
    "prediction": (0.04, 0.10),
    "shared": (0.01, 0.04),
}

# Per-axis value cap to keep synthetic row counts bounded.
_AXIS_CAP: dict[str, int] = {"instrument_id": 10}
_DEFAULT_CAP: int = 8


def _axis_values(axis: str, asset_group: str) -> list[str]:
    """Illustrative value list for ``axis`` within ``asset_group``."""
    ag = asset_group.lower()
    table: dict[str, list[str]] = {
        "venue": _VENUES.get(ag, ["VENUE_A", "VENUE_B"]),
        "chain": _CHAINS,
        "data_type": _DATA_TYPES.get(ag, ["default"]),
        "instrument_type": _INSTRUMENT_TYPES.get(ag, ["SPOT"]),
        "instrument_id": _INSTRUMENT_IDS,
        "league_id": _LEAGUE_IDS,
        "feature_group": _FEATURE_GROUPS,
        "timeframe": _TIMEFRAMES,
        "canonical_question_group": _QUESTION_GROUPS,
        "model_family": _MODEL_FAMILIES,
        "training_period": _TRAINING_PERIODS,
        "job_id": _JOB_IDS,
        "strategy_id": _STRATEGY_IDS,
        "instruction_type": _INSTRUCTION_TYPES,
    }
    return table.get(axis, [f"{axis}_1", f"{axis}_2"])


def _capture_status(seed_key: str, p_fail: float, p_empty: float) -> str:
    """Deterministic 3-state capture_status from a stable hash of ``seed_key``."""
    r = (zlib.crc32(seed_key.encode()) & 0xFFFFFFFF) / 0xFFFFFFFF
    if r < p_fail:
        return "attempted_failed"
    if r < p_fail + p_empty:
        return "empty_confirmed"
    return "captured"


def build_mock_availability_index(
    service: str,
    asset_group: str,
    axes: tuple[str, ...],
    window_start: str,
    window_end: str,
) -> pd.DataFrame:
    """Synthesise a per-shard availability DataFrame for local mock mode.

    Columns = non-``date`` shard axes + ``date`` + ``capture_status``. Mirrors
    the shape ``read_availability_index`` returns so the real drill-down tree
    builder runs unchanged.
    """
    non_date_axes = [a for a in axes if a != "date"]
    value_lists: list[list[str]] = [
        _axis_values(a, asset_group)[: _AXIS_CAP.get(a, _DEFAULT_CAP)] for a in non_date_axes
    ]

    start = date.fromisoformat(window_start)
    end = date.fromisoformat(window_end)
    dates: list[str] = []
    cursor = start
    while cursor <= end:
        dates.append(cursor.isoformat())
        cursor += timedelta(days=1)

    p_fail, p_empty = _AG_PROFILE.get(asset_group.lower(), _AG_PROFILE["shared"])

    combos: list[tuple[str, ...]] = list(itertools.product(*value_lists)) if value_lists else [()]
    rows: list[dict[str, str]] = []
    for combo in combos:
        for day in dates:
            key = f"{service}|{asset_group}|{'|'.join(combo)}|{day}"
            row: dict[str, str] = dict(zip(non_date_axes, combo))
            row["date"] = day
            row["capture_status"] = _capture_status(key, p_fail, p_empty)
            rows.append(row)

    columns = [*non_date_axes, "date", "capture_status"]
    return pd.DataFrame(rows, columns=columns)

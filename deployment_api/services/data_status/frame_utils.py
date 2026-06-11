"""Small availability-index DataFrame transforms shared across groups.

Split out of the 6,663-line ``data_status_service.py`` god-module
(codex ratchet plan 2026-06-10). The facade module re-exports every
public + legacy-underscore name, so callers keep importing from
``deployment_api.services.data_status_service``.
"""

import logging

import pandas as pd

import deployment_api.services.data_status_service as _dss

logger = logging.getLogger(__name__)

TRANSFER_COUNTRIES = (
    "ENG",
    "ESP",
    "DEU",
    "ITA",
    "FRA",
    "NLD",
    "PRT",
    "BEL",
    "TUR",
    "SCO",
    "AUT",
    "CHE",
    "DNK",
    "NOR",
    "SWE",
    "POL",
    "KOR",
    "ARG",
    "BRA",
    "CHL",
    "USA",
    "MEX",
    "JPN",
    "AUS",
)

# v9 prediction bundled data_type constant (matches ManifestWriter row_key).
PREDICTION_BUNDLED_DT: str = "prediction_canonical_question_group"


def promote_prediction_cqg_from_instrument_id(df: pd.DataFrame) -> pd.DataFrame:
    """Read-side: promote ``instrument_id`` → ``canonical_question_group`` for v9 prediction rows.

    v9 canonical prediction shape (``prediction_manifest_canonicalisation_2026_06_01.md``):
    ``data_type="prediction_canonical_question_group"`` bundled rows store the cqg value in
    ``instrument_id`` (the ManifestWriter row_key field ``instrument_id=cqg_str``).  The
    turbo aggregation and ``_apply_row_filters`` both key on the ``canonical_question_group``
    column, which is absent in v9 rows.  This function fills that column read-side so both
    paths work across the migration window.

    Pre-v9 rows that already have a non-empty ``canonical_question_group`` column are
    left untouched.  Read-side only — no manifest writes.
    """
    if "data_type" not in df.columns or "instrument_id" not in df.columns:
        return df
    pred_mask = df["data_type"].astype(str) == PREDICTION_BUNDLED_DT
    if not pred_mask.any():
        return df
    out = df.copy()
    if "canonical_question_group" not in out.columns:
        out["canonical_question_group"] = ""
    cqg = out["canonical_question_group"].astype(str)
    inst = out["instrument_id"].astype(str)
    needs_fill = pred_mask & ((cqg == "") | (cqg == "nan"))
    out.loc[needs_fill, "canonical_question_group"] = inst[needs_fill]
    return out


# Cache for availability index reads — avoids repeated GCS downloads


def derive_underlying_from_instrument_id(instrument_id: str) -> str:
    """Extract the base asset (underlying) from a canonical instrument_id.

    Handles common naming conventions:
    - "BTC-USDT-PERP" -> "BTC"
    - "ETH-USDC" -> "ETH"
    - "BTC-USD-241227-C-100000" -> "BTC"
    - "ES-FUT-20260320" -> "ES"
    - "SPY" -> "SPY" (single-symbol equity)

    The first segment before the first dash is always the base asset.
    For single-symbol instruments (no dash), the full string is returned.
    """
    if not instrument_id or not instrument_id.strip():
        return ""
    parts = instrument_id.strip().split("-")
    return parts[0].upper()


def ensure_underlying_column(df: pd.DataFrame) -> pd.DataFrame:
    """Ensure the DataFrame has a populated ``underlying`` column.

    Fills blank/missing ``underlying`` rows by deriving from ``instrument_id``
    (when present) using ``derive_underlying_from_instrument_id``. Rows whose
    ``underlying`` is already non-empty are preserved as-is.
    Returns the DataFrame (modified in-place when derivation is needed).
    """
    if "instrument_id" not in df.columns:
        return df

    if "underlying" not in df.columns:
        df["underlying"] = ""
    blank_mask = df["underlying"].isna() | (df["underlying"].astype(str).str.strip() == "")
    if blank_mask.any():
        df.loc[blank_mask, "underlying"] = (
            df.loc[blank_mask, "instrument_id"].astype(str).map(derive_underlying_from_instrument_id)
        )
    return df


def clamp_to_venue_starts(filtered: pd.DataFrame, start_date: str) -> str:
    """Clamp start date forward to the latest venue launch date."""
    effective_start = start_date
    if "venue" not in filtered.columns or filtered.empty:
        return effective_start
    venue_mapping = _dss.VenueMapping()
    for v in filtered["venue"].unique():  # pyright: ignore[reportAny]
        vs = venue_mapping.get_venue_start_date(v)  # pyright: ignore[reportAny]
        if not vs and ":" in v:
            vs = venue_mapping.get_venue_start_date(v.split(":")[0])  # pyright: ignore[reportAny]
        if vs:
            effective_start = max(effective_start, vs)
    return effective_start

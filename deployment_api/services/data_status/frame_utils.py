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

    Two real shapes flow into this function (canonical_id_p0_strategy_
    reconciliation_2026_07_08 bug #5 — the original bare-``BASE-QUOTE``-only
    fallback below was proven wrong against every real venue-prefixed
    production sample):

    1. **Venue-prefixed** ``VENUE:TYPE:SYMBOL[@SUFFIX]`` (the shape real CeFi/
       TradFi manifest rows actually carry, e.g. from instruments-service /
       MTDS): the venue/type prefix is stripped (split on ``:``, keep
       everything after the *first two* colons — mirrors UAC's
       ``parse_instrument_key`` convention, so an option symbol's own
       embedded colons, if any, survive intact), then any trailing
       ``@SETTLEMENT``/``@CHAIN`` suffix is stripped before applying the
       bare-shape logic below.
       - "BINANCE-FUTURES:PERPETUAL:BTC-USDT@LIN" -> "BTC"
       - "DERIBIT:OPTION:BTC-9JUL26-56000-C" -> "BTC"
       - "DERIBIT:PERPETUAL:BTC-USD@INV" -> "BTC"
    2. **Bare** ``BASE-QUOTE[-...]`` with NO venue prefix (deployment-api's
       own DeFi row_key convention keeps ``instrument_id`` venue-free —
       ``venue`` is a separate row_key field for that asset_group — so this
       shape is legitimately still real, not just legacy):
       - "BTC-USDT-PERP" -> "BTC"
       - "ETH-USDC" -> "ETH"
       - "BTC-USD-241227-C-100000" -> "BTC"
       - "ES-FUT-20260320" -> "ES"
       - "SPY" -> "SPY" (single-symbol equity)

    In both shapes, once any venue/type prefix and ``@SUFFIX`` are stripped,
    the first segment before the first dash is the base asset; a
    single-symbol payload (no dash) returns the full payload.
    """
    if not instrument_id or not instrument_id.strip():
        return ""
    stripped = instrument_id.strip()
    payload = stripped
    if ":" in stripped:
        # Canonical VENUE:TYPE:SYMBOL shape -- split on the first two colons
        # only, so an option/combo symbol's own embedded colons (if any)
        # stay part of the payload rather than being mistaken for more
        # VENUE:TYPE segments.
        colon_parts = stripped.split(":", 2)
        payload = colon_parts[-1]
    if "@" in payload:
        payload = payload.split("@", 1)[0]
    parts = payload.split("-")
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

"""DEFI data_type canonicalisation maps + the row-level normaliser.

Split out of ``mtds.py`` (2026-07-31,
``deployment_api_qg_size_gate_debt_2026_07_30.md``) to bring that facade
module under the 900-line file-size gate. Re-exported through ``mtds.py`` so
every existing import path (``deployment_api.services.data_status.mtds.*``)
keeps working unchanged.
"""

import pandas as pd

# DEFI data_type canonicalisation maps. Sub-dim buckets write the hyphenated
# form (``lending-indices``, ``dex-swaps``) but UAC
# ``VENUE_DATA_TYPE_CAPABILITIES`` declares the underscore form
# (``lending_indices``, ``dex_pool_swaps``). The honest-coverage per-(venue, dt)
# filter needs the rows canonicalised before matching. Module-level
# constants per ruff N806 — they're configuration, not per-call state.
DEFI_DATA_TYPE_ALIASES: dict[str, str] = {
    "dex-swaps": "dex_pool_swaps",
    "dex-pools": "dex_pool_state",
    "lending-indices": "lending_indices",
    "lst-rates": "lst_rates",
    "oracle-prices": "oracle_prices",
    "perp-funding": "perp_funding",
    "gas-fees": "gas_fees",
    "eigenlayer-rewards": "eigenlayer_rewards",
    # Phase 2 event-typed handlers (defi_data_types_completeness_2026_04_24)
    "liquidation-events": "liquidation_events",
    "flash-loan-events": "flash_loan_events",
    "staking-yields": "staking_yields",
    "position-data": "position_data",
    "token-transfers": "token_transfers",
    "bridge-events": "bridge_events",
    "governance-events": "governance_events",
    "mev-events": "mev_events",
}
DEFI_SOURCE_TO_DATA_TYPE: dict[str, str] = {
    "dex-swaps": "dex_pool_swaps",
    "dex-pools": "dex_pool_state",
    "lending-indices": "lending_indices",
    "lst-rates": "lst_rates",
    "oracle-prices": "oracle_prices",
    "liquidations": "liquidations",
    "perp-funding": "perp_funding",
    "gas-fees": "gas_fees",
    "eigenlayer-rewards": "eigenlayer_rewards",
    # Phase 2 event-typed handlers
    "liquidation-events": "liquidation_events",
    "flash-loan-events": "flash_loan_events",
    "staking-yields": "staking_yields",
    "position-data": "position_data",
    "token-transfers": "token_transfers",
    "bridge-events": "bridge_events",
    "governance-events": "governance_events",
    "mev-events": "mev_events",
    "evm-defi": "",
    "solana-defi": "",
    "": "",
}


def canonicalise_defi_data_types(filtered: pd.DataFrame) -> pd.DataFrame:
    """Normalise hyphenated DEFI ``data_type`` values to underscore form.

    Sub-dim buckets (``lending-indices``, ``dex-swaps``, ``dex-pools``,
    ``lst-rates``, ``oracle-prices``, ``perp-funding``) write hyphenated
    ``data_type`` values but UAC ``VENUE_DATA_TYPE_CAPABILITIES`` uses
    canonical underscore form (``lending_indices``, ``dex_pool_swaps``, …). Two
    transforms applied here, both safe to remove once the corresponding
    one-shot manifest migration runs (Plan B follow-up — currently no
    successor plan; data_type alias migration is the natural next step):

    * Case 1: infer ``data_type`` from ``_defi_source`` for blank rows.
    * Case 2: map hyphenated forms to canonical underscore form via
      ``DEFI_DATA_TYPE_ALIASES``.

    DeFi VENUE canonicalisation is no longer done here — UTL
    ``manifest_writer._coerce_row_key`` + ``ManifestWriter.add`` apply
    ``LEGACY_DEFI_VENUE_ALIASES`` at write time, and the 2026-05-07 MTDS
    DEFI migration script rewrote 411,620 historical rows in place
    (``market_tick_data_service/scripts/migrate_mtds_defi_legacy_venue_underscore.py``).
    Live re-probe across 11 DEFI buckets confirmed 0 residual legacy-
    underscore DeFi-venue rows. Per workspace rule "Manifest migration,
    NOT fallback", the venue-side fallback is gone.
    """
    if "data_type" not in filtered.columns:
        return filtered

    out = filtered.copy()

    # Case (1): infer from _defi_source for blank rows.
    if "_defi_source" in out.columns:
        blank_dt = out["data_type"].fillna("").astype(str).str.len() == 0  # pyright: ignore[reportUnknownMemberType]
        if blank_dt.any():
            inferred = out["_defi_source"].fillna("").astype(str).map(DEFI_SOURCE_TO_DATA_TYPE).fillna("")  # pyright: ignore[reportUnknownMemberType]
            out.loc[blank_dt, "data_type"] = inferred[blank_dt]
    # Case (2): map hyphenated DEFI data_types to canonical underscore form.
    out["data_type"] = out["data_type"].fillna("").astype(str).replace(DEFI_DATA_TYPE_ALIASES)  # pyright: ignore[reportUnknownMemberType]
    return out

"""MTDS per-(venue, data_type) coverage-entry helpers.

Split out of ``mtds.py`` (2026-07-31,
``deployment_api_qg_size_gate_debt_2026_07_30.md``) to bring that facade
module under the 900-line file-size gate, and to shrink
``mtds_honest_coverage_for_venue`` under the 200-line function-size gate by
extracting its venue-row-filtering setup (``_mtds_venue_rows``) and its
per-dt loop-body dispatch (``_mtds_seeded_entry_counts`` /
``_mtds_derived_entry_counts``) here alongside the existing per-dt entry
builders. ``mtds_expected_dates_for_venue_dt`` itself stays called from
INSIDE ``mtds.py``'s own function body (not here) — see
``_mtds_derived_entry_counts``'s docstring for why. Re-exported through
``mtds.py`` so every existing import path
(``deployment_api.services.data_status.mtds.*``) keeps working unchanged.
"""

from collections.abc import Callable
from typing import cast

import pandas as pd
from unified_api_contracts import is_per_instrument_shard_data_type
from unified_api_contracts.registry import get_expected_timeframes_for_venue_dt

from deployment_api.services.data_status.instrument_coverage import (
    DEFAULT_PER_INSTRUMENT_SENTINEL_CAP,
    per_instrument_coverage,
)


def _seeded_expected_unattempted_dts(venue_df: pd.DataFrame) -> set[str]:
    """Return the data_types with ≥1 materialised ``expected_unattempted`` row.

    F4 seed-guard (codex/02-data/availability-manifest-and-data-status.md):
    ``expected_unattempted`` is MATERIALISED by the writer (MTDS pre-flight /
    IS ``enumerate_expected_universe`` v2 enumerator) and READ by consumers.
    A (venue, data_type) with seeded rows must take the 4-state READ
    denominator — re-deriving genesis/launch math on top of seeded rows
    double-counts / diverges. Cheap per-venue-frame presence check: one
    vectorised equality + one ``unique()``.
    """
    if venue_df.empty or "capture_status" not in venue_df.columns or "data_type" not in venue_df.columns:
        return set()
    seeded_mask = venue_df["capture_status"].fillna("").astype(str) == "expected_unattempted"  # pyright: ignore[reportUnknownMemberType]
    if not bool(seeded_mask.any()):  # pyright: ignore[reportUnknownArgumentType,reportUnknownMemberType]
        return set()
    return {str(d) for d in venue_df.loc[seeded_mask, "data_type"].astype(str).unique()}  # pyright: ignore[reportAny]


def _mtds_seeded_4state_dt_entry(
    venue_df: pd.DataFrame,
    venue_df_ok: pd.DataFrame,
    dt: str,
    window_start: str,
    window_end: str,
) -> dict[str, object]:
    """4-state READ entry for a (venue, dt) whose expected universe is seeded.

    F4: when the writer/enumerator has materialised ``expected_unattempted``
    rows for this (venue, data_type), the manifest rows ARE the expected
    universe. Denominator = distinct shard cells across ``captured`` +
    ``empty_confirmed`` + ``attempted_failed`` + ``expected_unattempted``
    (the M5/union rule used by ``data_status_hierarchical`` /
    ``data_status_union``); numerator keeps this module's existing
    skip-worthy convention (``venue_df_ok``: captured / empty_confirmed /
    EXPECTED_*-reasoned expected_unattempted). The genesis/launch
    re-derivation (``_mtds_expected_dates_for_venue_dt``) is NOT consulted.

    Grain dispatch mirrors the derived path: if the (venue, dt) rows carry
    non-empty ``instrument_id`` values, cells are (instrument_id, date)
    pairs (Tier-3 ``shard_instrument_days``); otherwise cells are dates
    (``shard_days``). Dates are clipped to the request window for parity
    with the derived denominator (callers pre-filter ``filtered`` by date,
    so this is a no-op on the service path but keeps direct calls honest).
    """

    def _dt_slice(df: pd.DataFrame) -> pd.DataFrame:
        if "data_type" not in df.columns or df.empty:
            return df.iloc[0:0]
        return df[df["data_type"] == dt]

    all_rows = _dt_slice(venue_df)
    ok_rows = _dt_slice(venue_df_ok)

    def _windowed(df: pd.DataFrame) -> pd.DataFrame:
        if "date" not in df.columns or df.empty:
            return df.iloc[0:0]
        date_s = df["date"].astype(str)  # pyright: ignore[reportUnknownMemberType]
        return df[(date_s >= window_start) & (date_s <= window_end)]

    all_rows = _windowed(all_rows)
    ok_rows = _windowed(ok_rows)

    has_iid = "instrument_id" in all_rows.columns
    iid_all = all_rows["instrument_id"].fillna("").astype(str).str.strip() if has_iid else pd.Series([], dtype=str)  # pyright: ignore[reportUnknownMemberType]
    per_instrument_grain = bool((iid_all.str.len() > 0).any()) if len(iid_all) else False  # pyright: ignore[reportUnknownMemberType,reportUnknownArgumentType]

    def _cells(df: pd.DataFrame) -> set[tuple[str, str]]:
        """Distinct shard cells in ``df`` at the resolved grain."""
        if df.empty:
            return set()
        date_list = [str(d) for d in df["date"].tolist()]  # pyright: ignore[reportAny]
        if per_instrument_grain:
            iid_list = [str(i).strip() for i in df["instrument_id"].fillna("").tolist()]  # pyright: ignore[reportAny,reportUnknownMemberType]
            return {(iid, d) for iid, d in zip(iid_list, date_list, strict=True) if iid}
        return {("", d) for d in date_list if d}

    expected_cells = _cells(all_rows)
    found_cells = _cells(ok_rows)
    # ok_rows is a row-subset of venue_df, but guard the intersection anyway
    # so found can never exceed expected.
    found_cells &= expected_cells

    expected_count = len(expected_cells)
    found_count = len(found_cells)
    expected_dates = {d for _, d in expected_cells}
    found_dates = {d for _, d in found_cells}

    entry: dict[str, object] = {
        "expected_shards": expected_count,
        "found_shards": found_count,
        "missing_shards": max(0, expected_count - found_count),
        "completion_pct": min(round(found_count / max(1, expected_count) * 100, 2), 100.0),
        # Collapsed across instruments at the dt level — same convention as
        # the Tier-3 derived path so the UI drill-down stays compatible.
        "missing_dates": sorted(expected_dates - found_dates)[:500],
        "dates_found_list": sorted(found_dates)[:500],
        "unit": "shard_instrument_days" if per_instrument_grain else "shard_days",
        # Provenance marker: the denominator came from materialised
        # expected_unattempted rows, NOT the genesis/launch re-derivation.
        "denominator_source": "materialised_expected_unattempted",
    }
    if per_instrument_grain:
        expected_instruments = sorted({iid for iid, _ in expected_cells})
        found_instruments = {iid for iid, _ in found_cells}
        entry["expected_instruments"] = expected_instruments
        entry["missing_instruments"] = [iid for iid in expected_instruments if iid not in found_instruments]
    return entry


def _tier2_dt_entry(
    venue_df_ok: pd.DataFrame,
    dt: str,
    expected_dates: set[str],
    timeframes: list[str] | None,
) -> dict[str, object]:
    """Venue-level (Tier-2, non-per-instrument) per-(venue, dt, date) coverage entry.

    Preserves the Phase 6d per-(venue, dt, date) denominator BYTE-FOR-BYTE
    when ``timeframes=None`` (every existing MTDS caller — the pre-2026-07-22
    behaviour of the inline branch this was extracted from). When
    ``timeframes`` is supplied (MDPS Tier-2 follow-up,
    mtds_data_status_page_parity_2026_07_21, implemented 2026-07-22), mirrors
    the Tier-3 ``per_instrument_coverage`` pattern exactly: ``expected_shards``
    multiplies by ``len(timeframes)`` and the found-set becomes distinct
    ``(date, timeframe)`` pairs read from the manifest's ``timeframe`` column,
    restricted to ``expected_dates`` AND ``timeframes``. A manifest slice with
    no ``timeframe`` column (or all-blank values) degrades to zero found
    pairs — honestly 0%, never a KeyError — matching the Tier-3 contract.
    MDPS's one current venue-level derivable dt is ``liquidations``
    (confirmed NOT in UAC's ``_PER_INSTRUMENT_SHARD_DATA_TYPES``, so it
    always dispatches here, never to the Tier-3 branch).

    FLAG-1 (TradFi v9 multi-source): the v9 manifest carries one row per
    (venue, data_type, date, source) — e.g. databento + massive both
    captured for the same date produce two rows. UNION semantics (≥1
    captured → cell captured) are enforced via distinct-date dedup before
    computing ``found_dates_set`` (the ``timeframes=None`` path). A
    per-source breakdown is surfaced in ``per_source`` for the UI drilldown
    (plan FLAG-1, operator 2026-06-02) — deliberately NOT timeframe-scoped
    even for MDPS (a known, documented residual, same convention the Tier-3
    ``per_instrument`` breakdown originally shipped with): it counts
    distinct captured dates per source across ALL timeframes, not restricted
    to ``timeframes``.
    """
    if "data_type" in venue_df_ok.columns:
        dt_rows = venue_df_ok[venue_df_ok["data_type"] == dt]
    else:
        dt_rows = venue_df_ok.iloc[0:0]

    # ``None`` (every existing MTDS caller) -> multiplier 1, i.e. a
    # complete no-op below. Only a non-empty ``timeframes`` list changes
    # the arithmetic — mirrors ``per_instrument_coverage``'s Tier-3
    # ``tf_multiplier`` exactly.
    tf_multiplier = len(timeframes) if timeframes else 1
    expected_count = len(expected_dates) * tf_multiplier

    if timeframes is not None:
        if "timeframe" in dt_rows.columns and not dt_rows.empty:
            date_s = dt_rows["date"].astype(str)  # pyright: ignore[reportUnknownMemberType]
            tf_s = dt_rows["timeframe"].fillna("").astype(str)  # pyright: ignore[reportUnknownMemberType]
            tf_mask = date_s.isin(expected_dates) & tf_s.isin(timeframes)  # pyright: ignore[reportUnknownMemberType,reportUnknownVariableType]
            found_tf_pairs: set[tuple[str, str]] = {
                (d, t)
                for d, t in zip(date_s[tf_mask], tf_s[tf_mask], strict=True)  # pyright: ignore[reportUnknownArgumentType,reportUnknownVariableType]
            }
        else:
            found_tf_pairs = set()
        found_dates_set: set[str] = {d for d, _t in found_tf_pairs}
        found_in_expected: set[str] = found_dates_set
        found_count = len(found_tf_pairs)
    else:
        # Union dedup: distinct dates regardless of how many sources
        # have a captured/empty_confirmed row for that date.
        found_dates_set = {str(d) for d in dt_rows["date"].unique()} if not dt_rows.empty else set()  # pyright: ignore[reportAny]
        # Only count dates that fall inside the expected window — a row
        # from before ``effective_start`` should not inflate ``found_shards``.
        found_in_expected = found_dates_set & expected_dates  # pyright: ignore[reportUnknownVariableType]
        found_count = len(found_in_expected)

    missing_dates = sorted(expected_dates - found_dates_set)

    # Per-source breakdown — free from the in-memory ``_index`` rows (the v9
    # manifest denormalises source to the row level so this is a single
    # groupby, no per-parquet scan). Only emitted when the ``source`` column
    # is present (v9 manifests); v8 manifests omit it.
    per_source: dict[str, object] = {}
    if "source" in dt_rows.columns and not dt_rows.empty:
        for src, src_group in dt_rows.groupby("source"):  # pyright: ignore[reportUnknownVariableType]
            src_str = str(src)
            src_dates = {str(d) for d in src_group["date"].unique()}  # pyright: ignore[reportAny]
            src_found = src_dates & expected_dates  # pyright: ignore[reportUnknownVariableType]
            per_source[src_str] = {
                "found_shards": len(src_found),  # pyright: ignore[reportUnknownArgumentType]
                "dates_found_list": sorted(src_found)[:500],  # pyright: ignore[reportUnknownVariableType]
            }

    entry: dict[str, object] = {
        "expected_shards": expected_count,
        "found_shards": found_count,
        "missing_shards": max(0, expected_count - found_count),
        "completion_pct": min(round(found_count / max(1, expected_count) * 100, 2), 100.0),
        "missing_dates": missing_dates[:500],
        "dates_found_list": sorted(found_in_expected)[:500],
        "unit": "shard_days",
    }
    if per_source:
        entry["per_source"] = per_source
    return entry


def _mtds_venue_rows(filtered: pd.DataFrame, venue: str) -> tuple[pd.DataFrame, pd.DataFrame]:
    """Pre-filter ``filtered`` to one venue and split into (all rows, ok rows).

    ``ok`` rows are ``captured`` / ``empty_confirmed`` / EXPECTED_*-reasoned
    ``expected_unattempted`` (v4 rows without ``capture_status`` are implicit
    ``captured``, same convention ``mtds_honest_coverage_for_bookmaker``
    applies inline for its own bookmaker-scoped frame). Extracted from
    :func:`mtds_honest_coverage_for_venue`'s setup (2026-07-31,
    ``deployment_api_qg_size_gate_debt_2026_07_30.md``) to bring that
    function under the 200-line function-size gate.
    """
    if "venue" not in filtered.columns or filtered.empty:
        venue_df = pd.DataFrame(columns=filtered.columns)
    else:
        venue_df = filtered[filtered["venue"] == venue]

    if "capture_status" in venue_df.columns:
        status_s = venue_df["capture_status"].fillna("captured").astype(str)
        reason_s = (
            venue_df["error_reason"].fillna("").astype(str)
            if "error_reason" in venue_df.columns
            else pd.Series("", index=venue_df.index)
        )
        ok_mask = status_s.isin(["captured", "empty_confirmed"]) | (
            (status_s == "expected_unattempted") & reason_s.str.startswith("EXPECTED_")
        )
    else:
        ok_mask = pd.Series([True] * len(venue_df), index=venue_df.index)
    venue_df_ok = venue_df[ok_mask] if not venue_df.empty else venue_df
    return venue_df, venue_df_ok


def _mtds_seeded_entry_counts(
    venue_df: pd.DataFrame,
    venue_df_ok: pd.DataFrame,
    dt: str,
    window_start: str,
    window_end: str,
) -> tuple[dict[str, object], int, int]:
    """:func:`_mtds_seeded_4state_dt_entry` + its (expected, found) counts.

    Split out of :func:`mtds_honest_coverage_for_venue`'s per-dt loop body
    (2026-07-31, ``deployment_api_qg_size_gate_debt_2026_07_30.md``, to bring
    the caller under the 200-line function-size gate) so the caller's
    running totals don't need their own ``cast`` calls.
    """
    dt_entry = _mtds_seeded_4state_dt_entry(venue_df, venue_df_ok, dt, window_start, window_end)
    expected_count = int(cast(int, dt_entry["expected_shards"]))
    found_count = int(cast(int, dt_entry["found_shards"]))
    return dt_entry, expected_count, found_count


def _mtds_derived_entry_counts(
    dt: str,
    expected_dates: set[str],
    venue: str,
    venue_df_ok: pd.DataFrame,
    category: str,
    is_mdps: bool,
    instruments_provider: Callable[[str, str], list[str] | None] | None,
    instrument_windows: dict[str, tuple[str | None, str | None]] | None,
    scope: str,
    instrument_types: dict[str, str] | None,
) -> tuple[dict[str, object], int, int]:
    """Non-seeded dt entry (Tier-3 per-instrument / Tier-2 venue-level) + its
    (expected, found) counts.

    Split out of :func:`mtds_honest_coverage_for_venue`'s per-dt loop body
    (2026-07-31, ``deployment_api_qg_size_gate_debt_2026_07_30.md``) —
    dispatch logic (:func:`per_instrument_coverage` vs. :func:`_tier2_dt_entry`)
    is unchanged. ``expected_dates`` is computed by the CALLER (via
    ``mtds_expected_dates_for_venue_dt``, kept physically inside
    ``mtds.py``'s own ``mtds_honest_coverage_for_venue`` body) rather than
    here, so ``unittest.mock.patch`` targeting that name on the ``mtds``
    module (the global-lookup convention
    ``tests/unit/test_data_status_seeded_4state_denominator.py`` relies on)
    still intercepts every derived-path call.
    """
    tf_for_dt: list[str] | None = get_expected_timeframes_for_venue_dt(venue, dt) if is_mdps else None

    if is_per_instrument_shard_data_type(dt):
        instr_cap: int | None = None if instruments_provider is not None else DEFAULT_PER_INSTRUMENT_SENTINEL_CAP
        dt_entry = per_instrument_coverage(
            venue_df_ok,
            venue,
            dt,
            expected_dates,
            instr_cap,
            instruments_provider=instruments_provider,
            instrument_windows=instrument_windows,
            asset_group=category.lower(),
            scope=scope,
            instrument_types=instrument_types,
            timeframes=tf_for_dt,
        )
    else:
        dt_entry = _tier2_dt_entry(venue_df_ok, dt, expected_dates, tf_for_dt)

    expected_count = int(cast(int, dt_entry["expected_shards"]))
    found_count = int(cast(int, dt_entry["found_shards"]))
    return dt_entry, expected_count, found_count

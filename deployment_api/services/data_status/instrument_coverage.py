"""Per-instrument (Tier-3) shard coverage + CeFi IS-provider helpers.

Split out of the 6,663-line ``data_status_service.py`` god-module
(codex ratchet plan 2026-06-10). The facade module re-exports every
public + legacy-underscore name, so callers keep importing from
``deployment_api.services.data_status_service``.
"""

import logging
from collections import Counter
from collections.abc import Callable

import pandas as pd
from unified_api_contracts import (
    get_expected_instruments_for_venue,
)

import deployment_api.services.data_status_service as _dss

logger = logging.getLogger(__name__)

# Phase 8D — MVP cap for the per-(venue, data_type, instrument_id) Tier-3
# denominator. Mirrors the MTDS orchestrator constant in
# ``market_tick_data_service/engine/orchestrator.py`` so the aggregator
# denominator matches the sentinel fan-out the orchestrator writes.
DEFAULT_PER_INSTRUMENT_SENTINEL_CAP: int = 50

# Per-instrument dt whose per_instrument drill-down panel is inlined on the
# response. Above this threshold the aggregator suppresses the dict (kept as
# totals only) to avoid ballooning the API payload size on big perp boards.
PER_INSTRUMENT_BREAKDOWN_MAX_SIZE: int = 20


def build_cefi_is_instruments_provider(
    cloud: object,
) -> Callable[[str, str], list[str] | None] | None:
    """Build an instruments_provider backed by the live IS cefi catalog.

    Reads the instruments-store-cefi-* availability index ONCE, builds a
    ``{venue: list[instrument_id]}`` map, and returns a closure that answers
    ``(venue, data_type) -> list[str]``.

    Fail-open: any GCS / parse error returns ``None`` (NOT a provider) so the
    CALLER injects no provider at all and the per-instrument denominator path
    falls back to UAC's MVP seed tables with the default cap — current
    behaviour when IS is unavailable.  Returning a ``lambda: None`` provider
    here would be WRONG: UAC only falls back to its MVP seed when the provider
    OBJECT is ``None``; a non-None provider that *returns* ``None`` yields an
    EMPTY universe (denominator collapses to 0), not the seed.  This avoids
    crashing the data-status request.

    The catalog read is performed eagerly at call-time (not lazily per
    (venue, dt) invocation) so the returned callable is cheap to call many
    times within one request: one GCS read → O(N) Python loop → per-venue
    dicts.
    """
    try:
        bucket = _dss.resolve_bucket_name(cloud=cloud, kind="instruments-store", asset_group="cefi")  # pyright: ignore[reportArgumentType]
        df: pd.DataFrame = _dss.read_availability_index(bucket)
        if df.empty or "venue" not in df.columns:
            logger.warning("cefi IS catalog empty or missing 'venue' column — falling back to MVP seed")
            return None

        # Support both column name conventions used by IS catalog parquets.
        id_col = "instrument_id" if "instrument_id" in df.columns else "instrument_key"
        if id_col not in df.columns:
            logger.warning("cefi IS catalog has neither 'instrument_id' nor 'instrument_key' — falling back")
            return None

        # Build {venue: sorted list of instrument_ids} — date-agnostic for now
        # (IS catalog available_from/to lifecycle filtering is reserved for a
        # future walk once the full universe stabilises).
        venue_map: dict[str, list[str]] = {}
        for row_venue, row_id in zip(df["venue"].astype(str), df[id_col].astype(str), strict=True):
            row_venue_s = row_venue.strip()
            row_id_s = row_id.strip()
            if not row_venue_s or not row_id_s or row_id_s.lower() in ("nan", "none", ""):
                continue
            venue_map.setdefault(row_venue_s, []).append(row_id_s)

        # Deduplicate + sort each list so the provider output is deterministic.
        deduped: dict[str, list[str]] = {v: sorted(set(ids)) for v, ids in venue_map.items()}
        instrument_count = sum(len(v) for v in deduped.values())
        logger.debug(
            "cefi IS catalog loaded: %d venues, %d instruments total",
            len(deduped),
            instrument_count,
        )
    except Exception as exc:
        logger.warning(
            "cefi IS catalog read failed (%s) — falling back to MVP seed",
            exc,
        )
        return None

    def _provider(venue: str, _data_type: str) -> list[str] | None:
        """Return IS instrument_ids for venue, or None to fall back to MVP seed."""
        result = deduped.get(venue)
        if result is None:
            # Venue not in IS catalog — fall back so UAC uses its MVP seed.
            return None
        return result

    return _provider


def per_instrument_coverage(
    venue_df_ok: pd.DataFrame,
    venue: str,
    dt: str,
    expected_dates: set[str],
    cap: int | None,
    instruments_provider: Callable[[str, str], list[str] | None] | None = None,
) -> dict[str, object]:
    """Phase 8D — compute the per-(instrument_id, date) denominator for a
    per-instrument shard ``data_type``.

    Extracted into its own helper so the parent
    :func:`mtds_honest_coverage_for_venue` stays under ruff C901 and so
    the Tier-3 branch is trivially unit-testable without re-plumbing the
    full manifest DataFrame.

    Mirrors the sentinel fan-out landed in MTDS orchestrator commit
    ``2947dd2``: expected denominator = ``|instruments| x |dates|`` where
    ``instruments`` comes from :func:`get_expected_instruments_for_venue`.
    When ``instruments_provider`` is supplied (e.g. the live IS cefi
    catalog), UAC uses it instead of its MVP seed tables so the denominator
    reflects the full real universe.  When ``instruments_provider=None``
    UAC falls back to its MVP seed tables (current behaviour for non-CEFI
    asset_groups).  The found set counts distinct ``(instrument_id, date)``
    tuples in ``venue_df_ok`` whose ``instrument_id`` is non-empty.

    **Legacy-row fallback:** if the manifest has (venue, dt) rows that
    land with an empty ``instrument_id`` (pre-Phase-8C writes), we degrade
    to the venue-level per-(venue, dt, date) denominator for that (venue,
    dt) pair so coverage % doesn't regress on already-shipped backfills.
    The degraded response is annotated with ``legacy_row_count``.

    Parameters
    ----------
    venue_df_ok:
        Pre-filtered manifest rows for this venue, gated on
        ``capture_status in {captured, empty_confirmed}``.
    venue:
        Canonical MTDS venue key.
    dt:
        Per-instrument shard data_type (caller MUST verify via
        :func:`is_per_instrument_shard_data_type`).
    expected_dates:
        Pre-computed expected date set from
        :func:`mtds_expected_dates_for_venue_dt`.
    cap:
        Hard ceiling on the returned instrument universe size. Passed to
        UAC. For CEFI pass ``None`` so the full real universe is not
        truncated by the MVP seed cap of 50.  For other asset_groups use
        :data:`DEFAULT_PER_INSTRUMENT_SENTINEL_CAP`.
    instruments_provider:
        Optional callable ``(venue, data_type) -> list[str] | None``
        injected for CEFI so the denominator uses the live IS catalog
        rather than the UAC MVP seed tables.  ``None`` preserves existing
        behaviour for non-CEFI asset_groups.

    Returns
    -------
    dict[str, object]
        ``{"expected_shards", "found_shards", "missing_shards",
        "completion_pct", "missing_dates", "dates_found_list", "unit",
        "expected_instruments", "missing_instruments", "per_instrument"?,
        "legacy_row_count"?}``. ``per_instrument`` is only emitted when
        the instrument universe size is below
        :data:`PER_INSTRUMENT_BREAKDOWN_MAX_SIZE` (keeps response bloat
        bounded on big perp boards).
    """
    expected_instruments = get_expected_instruments_for_venue(
        venue,
        dt,
        instruments_provider=instruments_provider,
        cap=cap,
    )

    # Slice to the (venue, dt) rows once.
    if "data_type" in venue_df_ok.columns and not venue_df_ok.empty:
        dt_rows = venue_df_ok[venue_df_ok["data_type"] == dt]
    else:
        dt_rows = venue_df_ok.iloc[0:0]

    has_instrument_col = "instrument_id" in dt_rows.columns
    instrument_series = (
        dt_rows["instrument_id"].fillna("").astype(str) if has_instrument_col else pd.Series([], dtype=str)  # pyright: ignore[reportUnknownMemberType]
    )
    date_series = dt_rows["date"].astype(str) if "date" in dt_rows.columns else pd.Series([], dtype=str)  # pyright: ignore[reportUnknownMemberType]

    if has_instrument_col and len(dt_rows) > 0:
        # Rows that land with empty ``instrument_id`` predate Phase-8C
        # fan-out. Count them separately so we can fall back to the
        # venue-level denominator when the (venue, dt) slice is fully
        # legacy.
        legacy_mask = instrument_series.str.strip() == ""  # pyright: ignore[reportUnknownMemberType]
        legacy_row_count = int(legacy_mask.sum())  # pyright: ignore[reportUnknownMemberType]
        non_legacy_mask = ~legacy_mask
        non_legacy_instr = instrument_series[non_legacy_mask]
        non_legacy_dates = date_series[non_legacy_mask]
    else:
        legacy_row_count = len(dt_rows)
        non_legacy_instr = pd.Series([], dtype=str)
        non_legacy_dates = pd.Series([], dtype=str)

    # Legacy-row fallback: the aggregator hasn't seen any Phase-8C rows
    # for this (venue, dt) yet -- preserve the prior per-(venue, dt, date)
    # denominator so historical backfills don't regress in the UI.
    if legacy_row_count > 0 and len(non_legacy_instr) == 0:
        found_dates_set = {str(d) for d in date_series.unique() if str(d)}  # pyright: ignore[reportAny]
        found_in_expected = found_dates_set & expected_dates
        missing_dates = sorted(expected_dates - found_dates_set)
        expected_count = len(expected_dates)
        found_count = len(found_in_expected)
        return {
            "expected_shards": expected_count,
            "found_shards": found_count,
            "missing_shards": max(0, expected_count - found_count),
            "completion_pct": min(round(found_count / max(1, expected_count) * 100, 2), 100.0),
            "missing_dates": missing_dates[:500],
            "dates_found_list": sorted(found_in_expected)[:500],
            "unit": "shard_days_legacy",
            "expected_instruments": list(expected_instruments),
            "missing_instruments": list(expected_instruments),
            "legacy_row_count": legacy_row_count,
        }

    # Phase 8D Tier-3 denominator.
    # Vectorised: build the (instrument_id, date) pair set with pandas mask
    # operations rather than a Python ``for/zip`` loop. For BINANCE-FUTURES
    # at 50 perps x ~3000 dates the prior loop iterated 150k times per
    # (venue, dt) pair; the masked path is dominated by the C-level isin().
    found_dates_in_window: set[str] = set()
    found_iid_dates_zipped: list[tuple[str, str]] = []
    if len(non_legacy_instr) > 0:
        iid_str = non_legacy_instr.astype(str).str.strip()
        rd_str = non_legacy_dates.astype(str)
        mask = (iid_str.str.len() > 0) & rd_str.isin(expected_dates)
        if bool(mask.any()):
            iid_kept = iid_str[mask].tolist()
            rd_kept = rd_str[mask].tolist()
            found_iid_dates_zipped = list(zip(iid_kept, rd_kept, strict=True))
            found_dates_in_window = set(rd_kept)
    found_pairs: set[tuple[str, str]] = set(found_iid_dates_zipped)

    n_instruments = len(expected_instruments)
    n_dates = len(expected_dates)
    expected_count = n_instruments * n_dates
    found_count = len(found_pairs)

    # Counter does both the per-instrument count AND gives us
    # ``instruments_with_shards`` as ``.keys()`` in one pass — replaces the
    # prior O(|instruments|*|pairs|) ``sum(1 for ...)`` loop below.
    iid_counts = Counter(iid for iid, _ in found_pairs)
    instruments_with_shards = set(iid_counts)
    missing_instruments = [iid for iid in expected_instruments if iid not in instruments_with_shards]

    entry: dict[str, object] = {
        "expected_shards": expected_count,
        "found_shards": found_count,
        "missing_shards": max(0, expected_count - found_count),
        "completion_pct": min(round(found_count / max(1, expected_count) * 100, 2), 100.0),
        # Missing-dates at the dt level collapses across instruments so the
        # drill-down stays backwards-compatible with the venue-level UI panel.
        "missing_dates": sorted(expected_dates - found_dates_in_window)[:500],
        "dates_found_list": sorted(found_dates_in_window)[:500],
        "unit": "shard_instrument_days",
        "expected_instruments": list(expected_instruments),
        "missing_instruments": missing_instruments,
    }

    if legacy_row_count > 0:
        # Some Phase-8C rows + some legacy rows coexist -- keep the Tier-3
        # denominator (authoritative) but surface the legacy-row count so
        # the UI can display a migration-in-progress badge.
        entry["legacy_row_count"] = legacy_row_count

    # Only inline the per-instrument breakdown on venues with a small
    # universe (<20). BINANCE-FUTURES with 50 perps would double the
    # response size per (venue, dt) pair otherwise.
    if n_instruments and n_instruments < PER_INSTRUMENT_BREAKDOWN_MAX_SIZE:
        per_instrument: dict[str, dict[str, object]] = {
            iid: {
                "found": iid_counts.get(iid, 0),
                "expected": n_dates,
                "completion_pct": min(round(iid_counts.get(iid, 0) / max(1, n_dates) * 100, 2), 100.0),
            }
            for iid in expected_instruments
        }
        entry["per_instrument"] = per_instrument

    return entry

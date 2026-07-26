"""Per-instrument (Tier-3) shard coverage + CeFi IS-provider helpers.

Split out of the 6,663-line ``data_status_service.py`` god-module
(codex ratchet plan 2026-06-10). The facade module re-exports every
public + legacy-underscore name, so callers keep importing from
``deployment_api.services.data_status_service``.
"""

import io
import logging
from collections import Counter
from collections.abc import Callable

import pandas as pd
from unified_api_contracts import (
    get_expected_instruments_for_venue,
    is_mvp,
)

import deployment_api.services.data_status_service as _dss
from deployment_api.utils.storage_client import get_storage_client

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


# bug_c_normalize_id_collision_options_futures_2026_07_22: OPTION and dated-FUTURE
# ids embed real distinguishing identity (expiry [+ strike + side]) in the
# @-suffix — measured 66,137x/135.9x colliding when blanket-stripped. Every other
# instrument_type (PERPETUAL/SPOT_PAIR/COMBO measured 1.00x collision-safe; any
# other/unrecognised type has no contrary evidence) keeps the pre-existing
# strip-on-@ behaviour — a DENY-list, not an allow-list, so an id with no
# derivable type segment at all (e.g. an older ``VENUE::SYMBOL`` shape still
# used by some call sites — an empty/absent type segment is simply not in this
# set) is unaffected.
_AT_SUFFIX_UNSAFE_TO_STRIP_TYPES: frozenset[str] = frozenset({"OPTION", "FUTURE"})


def _normalize_instrument_id_for_match(instrument_id: str) -> str:
    """Normalize an instrument_id for cross-service Tier-3 coverage matching.

    instruments-service's catalog and MTDS's manifest are independently
    written services whose ``instrument_id`` strings can diverge on
    surface-level formatting even when they name the identical real
    instrument (canonical_id_p0_strategy_reconciliation_2026_07_08 bug #4)
    — e.g. casing, incidental whitespace, or one side carrying a
    ``@SETTLEMENT``/``@CHAIN`` suffix (``@LIN``/``@INV``/``@ETHEREUM``) the
    other omits. An un-normalized exact-string match can then report a real,
    fully-captured instrument as phantom-missing.

    This is a narrow, safe normalization of those KNOWN surface-level
    divergences — NOT a full canonicalization. It deliberately does NOT
    attempt venue-token spelling normalization (e.g. ``AAVE_V3`` vs
    ``AAVEV3``) or any other semantic reconciliation; that is a much larger,
    riskier decision reserved for the sequenced ground-up canonicalization
    migration (UAC -> instruments-service -> MTDS -> strategy-service ->
    deployment-api) called out in canonical_instrument_id_audit_2026_07_08.
    A residual mismatch after this normalization is a genuine "still
    doesn't match" case, not something this function tries to paper over.

    instrument_type-aware ``@``-stripping (bug_c_normalize_id_collision_
    options_futures_2026_07_22, direction (b)): for OPTION and dated-FUTURE
    ids the ``@``-suffix encodes real distinguishing identity — expiry date
    (+ strike + side for options), e.g.
    ``DERIBIT:OPTION:BTC-USD@INV-20190405-3250-C`` — so blanket-stripping
    collapses thousands of genuinely distinct instruments onto one key
    (measured 66,137x for DERIBIT OPTION). The canonical id grammar
    (``build_instrument_id``: ``VENUE:INSTRUMENT_TYPE:SYMBOL``) puts the
    type as the SECOND colon segment, so this checks it structurally —
    working identically for both catalogue-form and manifest-form ids
    without a separate ``{instrument_id: instrument_type}`` lookup (which
    would need a dict keyed on the very divergent-form ids this function
    normalizes away). Every type OTHER than OPTION/FUTURE keeps the
    pre-existing strip-on-``@`` behaviour (a deny-list, not an allow-list),
    so an id with no derivable type segment (e.g. a legacy ``VENUE::SYMBOL``
    shape) is unaffected.
    """
    if not instrument_id:
        return ""
    normalized = "".join(instrument_id.split()).upper()  # strip/collapse ALL whitespace
    if "@" not in normalized:
        return normalized
    parts = normalized.split(":")
    instrument_type = parts[1] if len(parts) >= 2 else ""
    if instrument_type in _AT_SUFFIX_UNSAFE_TO_STRIP_TYPES:
        return normalized
    return normalized.split("@", 1)[0]


def _read_cefi_catalogue_metadata(
    cloud: object,
) -> tuple[dict[str, tuple[str | None, str | None]], dict[str, str]]:
    """Read ``prod/catalog.parquet`` for cefi ONCE, return existence windows + instrument_type.

    ``({instrument_id: (available_from, available_to)}, {instrument_id: instrument_type})``
    — the identity-level lifecycle + type source (one row per instrument;
    mirrors ``catalogue_lifecycle.py::_read_catalogue``'s bucket/path/
    projection pattern, kept local here rather than importing that module's
    private helper cross-module). Both dicts come from the SAME parquet read
    (not two round-trips) so :func:`per_instrument_coverage` can clip its
    denominator to each instrument's real existence window AND evaluate UAC
    ``is_mvp(...)`` per instrument for the ``scope=mvp`` filter. Fail-open:
    any GCS/parse error or a missing column returns ``({}, {})`` — callers
    must treat an empty dict exactly like "no data available" and fall back
    to the pre-existing unclipped/unfiltered behaviour, never raise.
    """
    try:
        bucket = _dss.resolve_bucket_name(cloud=cloud, kind="instruments-store", asset_group="cefi")  # pyright: ignore[reportArgumentType]
        raw = get_storage_client().download_bytes(bucket, "prod/catalog.parquet")
        df = pd.read_parquet(
            io.BytesIO(raw), columns=["instrument_id", "available_from", "available_to", "instrument_type"]
        )
    except Exception as exc:
        logger.warning("cefi catalogue metadata read failed (%s) — falling back to date/scope-agnostic", exc)
        return {}, {}
    if df.empty or "instrument_id" not in df.columns:
        return {}, {}

    windows: dict[str, tuple[str | None, str | None]] = {}
    instrument_types: dict[str, str] = {}
    has_from = "available_from" in df.columns
    has_to = "available_to" in df.columns
    has_itype = "instrument_type" in df.columns
    for row_id, row_from, row_to, row_itype in zip(
        df["instrument_id"].astype(str),
        df["available_from"] if has_from else pd.Series([None] * len(df)),
        df["available_to"] if has_to else pd.Series([None] * len(df)),
        df["instrument_type"] if has_itype else pd.Series([None] * len(df)),
        strict=True,
    ):
        row_id_s = row_id.strip()
        if not row_id_s or row_id_s.lower() in ("nan", "none", ""):
            continue
        af = None if pd.isna(row_from) else str(row_from)[:10]
        at = None if pd.isna(row_to) else str(row_to)[:10]
        windows[row_id_s] = (af, at)
        if row_itype is not None and not pd.isna(row_itype):
            itype_s = str(row_itype).strip()
            if itype_s:
                instrument_types[row_id_s] = itype_s
    return windows, instrument_types


def build_cefi_is_instruments_provider(
    cloud: object,
) -> tuple[
    Callable[[str, str], list[str] | None] | None,
    dict[str, tuple[str | None, str | None]],
    dict[str, str],
]:
    """Build an instruments_provider backed by the live IS cefi catalog.

    Reads the instruments-store-cefi-* availability index ONCE, builds a
    ``{venue: list[instrument_id]}`` map, and returns a closure that answers
    ``(venue, data_type) -> list[str]``, alongside a per-instrument
    existence-window dict AND a per-instrument ``instrument_type`` dict (see
    :func:`_read_cefi_catalogue_metadata`) so :func:`per_instrument_coverage`
    can (a) clip its denominator to days each instrument actually existed
    instead of a blanket ``|instruments| x |dates|`` cross-product, and (b)
    evaluate UAC ``is_mvp(...)`` per instrument for the ``scope=mvp`` filter.

    Fail-open: any GCS / parse error returns ``(None, {}, {})`` (NOT a
    provider) so the CALLER injects no provider at all and the per-instrument
    denominator path falls back to UAC's MVP seed tables with the default cap
    — current behaviour when IS is unavailable.  Returning a ``lambda: None``
    provider here would be WRONG: UAC only falls back to its MVP seed when the
    provider OBJECT is ``None``; a non-None provider that *returns* ``None``
    yields an EMPTY universe (denominator collapses to 0), not the seed.  This
    avoids crashing the data-status request.

    The catalog read is performed eagerly at call-time (not lazily per
    (venue, dt) invocation) so the returned callable is cheap to call many
    times within one request: one GCS read → O(N) Python loop → per-venue
    dicts. The existence-window + instrument_type read is a SEPARATE, small
    object (``prod/catalog.parquet`` in the SAME bucket) — not a second
    whole-corpus walk.
    """
    windows, instrument_types = _read_cefi_catalogue_metadata(cloud)
    try:
        bucket = _dss.resolve_bucket_name(cloud=cloud, kind="instruments-store", asset_group="cefi")  # pyright: ignore[reportArgumentType]
        df: pd.DataFrame = _dss.read_availability_index(bucket)
        if df.empty or "venue" not in df.columns:
            logger.warning("cefi IS catalog empty or missing 'venue' column — falling back to MVP seed")
            return None, windows, instrument_types

        # Support both column name conventions used by IS catalog parquets.
        id_col = "instrument_id" if "instrument_id" in df.columns else "instrument_key"
        if id_col not in df.columns:
            logger.warning("cefi IS catalog has neither 'instrument_id' nor 'instrument_key' — falling back")
            return None, windows, instrument_types

        # Build {venue: sorted list of instrument_ids}. Existence-window
        # clipping (available_from/to) is applied downstream in
        # per_instrument_coverage via `windows` above, not here.
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
        return None, windows, instrument_types

    def _provider(venue: str, _data_type: str) -> list[str] | None:
        """Return IS instrument_ids for venue, or None to fall back to MVP seed."""
        result = deduped.get(venue)
        if result is None:
            # Venue not in IS catalog — fall back so UAC uses its MVP seed.
            return None
        return result

    return _provider, windows, instrument_types


def _clip_dates_to_window(dates: set[str], window: tuple[str | None, str | None] | None) -> frozenset[str]:
    """Intersect ``dates`` (ISO ``YYYY-MM-DD`` strings) with an instrument's existence window.

    ``window=None`` (no catalogue entry for this instrument) returns ``dates``
    unclipped — fail-open per-instrument, matching this module's existing
    fail-open convention: we never penalize an instrument for missing
    catalogue lifecycle data we simply don't have. ISO date strings compare
    correctly lexicographically, so no ``datetime`` parsing is needed.
    """
    if window is None:
        return frozenset(dates)
    af, at = window
    return frozenset(d for d in dates if (af is None or d >= af) and (at is None or d <= at))


def per_instrument_coverage(
    venue_df_ok: pd.DataFrame,
    venue: str,
    dt: str,
    expected_dates: set[str],
    cap: int | None,
    instruments_provider: Callable[[str, str], list[str] | None] | None = None,
    instrument_windows: dict[str, tuple[str | None, str | None]] | None = None,
    asset_group: str = "cefi",
    scope: str = "could_exist",
    instrument_types: dict[str, str] | None = None,
    timeframes: list[str] | None = None,
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
    instrument_windows:
        Optional ``{instrument_id: (available_from, available_to)}`` from
        :func:`_read_cefi_catalogue_existence_windows`. When supplied, the
        denominator (and numerator) are CLIPPED per-instrument to the days
        each instrument actually existed, instead of a blanket
        ``|instruments| x |dates|`` cross-product — this is the fix for the
        "MTDS should only ask 'did we capture it' over days IS says the
        instrument existed" correctness bug. An instrument absent from this
        dict (or ``instrument_windows=None``/``{}`` entirely) falls back to
        the full, unclipped ``expected_dates`` — fail-open, never penalizes
        missing catalogue lifecycle data.
    asset_group:
        Lowercase UAC asset_group key passed to ``is_mvp(...)`` when
        ``scope="mvp"``. Defaults to ``"cefi"`` (the only asset_group this
        module's live-catalog path serves today).
    scope:
        ``"could_exist"`` (default, current behaviour) / ``"mvp"`` — when
        ``"mvp"``, ``expected_instruments`` is restricted to instruments
        where UAC ``is_mvp(asset_group, venue, instrument_type, dt,
        base_ccy=None)`` is True (``base_ccy`` is a known, pre-existing
        blank axis for cefi — see ``_coverage_scope.py``'s
        ``is_mvp_for_manifest_row`` docstring). An instrument absent from
        ``instrument_types`` (unknown type) is treated as non-MVP under
        ``scope="mvp"`` — fails CLOSED here (unlike the fail-open window
        clipping) because an unknown instrument_type cannot be proven MVP.
    instrument_types:
        Optional ``{instrument_id: instrument_type}`` from
        :func:`_read_cefi_catalogue_metadata`, required to evaluate
        ``is_mvp(...)`` per instrument under ``scope="mvp"``. Ignored when
        ``scope="could_exist"``.
    timeframes:
        ``None`` (default) preserves the pre-2026-07-21 behaviour
        BYTE-FOR-BYTE — the denominator/numerator stay per-(instrument,
        date), exactly as before this parameter existed. Every existing
        MTDS caller passes nothing here.

        When supplied (MDPS extension, mtds_data_status_page_parity_2026_07_21),
        the denominator/numerator become per-(instrument, date, timeframe):
        MDPS writes one candle parquet per (instrument, date, timeframe) — a
        strictly finer shard grain than MTDS's per-(instrument, date) raw
        ticks, since the SAME manifest ``data_type`` token (e.g. ``"trades"``)
        now fans out across N candle timeframes in a separate ``timeframe``
        column. ``expected_shards`` becomes ``|instruments| x |dates| x
        |timeframes|`` (per-instrument existence-window clipping still
        applies to the ``dates`` axis only — timeframes are not
        existence-windowed); ``found_shards`` counts distinct
        ``(instrument_id, date, timeframe)`` triples where ``timeframe`` is
        read from the manifest's ``timeframe`` column and restricted to this
        list. A manifest with no ``timeframe`` column (or all-blank values)
        degrades to zero found triples (honestly 0%, never a KeyError).

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

    if scope == "mvp":
        types = instrument_types or {}
        expected_instruments = [
            iid for iid in expected_instruments if is_mvp(asset_group, venue, types.get(iid, ""), dt, base_ccy=None)
        ]

    # ``None`` (every existing MTDS caller) -> multiplier 1, i.e. a
    # complete no-op below. Only a non-empty ``timeframes`` list changes
    # the arithmetic.
    tf_multiplier = len(timeframes) if timeframes else 1

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
        # MDPS-only (``timeframes is not None``): derive the timeframe series
        # from the SAME ``non_legacy_mask``-filtered slice as ``non_legacy_instr``
        # / ``non_legacy_dates`` (both computed one line above) -- NOT from the
        # unfiltered ``dt_rows`` -- so it shares their exact index, keeping every
        # later same-index boolean-mask operation (``iid_str``/``rd_str``/``tf_str``
        # combined via ``&``) aligned. Building it from unfiltered ``dt_rows``
        # instead is the real, review-confirmed pandas index-misalignment bug
        # (2 of 3 reviews, independently) that fires whenever legacy rows
        # (empty instrument_id) coexist with non-legacy rows in the same
        # (venue, dt) slice. ``None`` (not an empty/blank Series) when
        # ``timeframes`` isn't supplied -- skips this allocation entirely on
        # the (far more common) MTDS call path.
        non_legacy_tf: pd.Series | None = (
            dt_rows["timeframe"].fillna("").astype(str)[non_legacy_mask]
            if timeframes is not None and "timeframe" in dt_rows.columns
            else None
        )
    else:
        legacy_row_count = len(dt_rows)
        non_legacy_instr = pd.Series([], dtype=str)
        non_legacy_dates = pd.Series([], dtype=str)
        non_legacy_tf = None

    # Legacy-row fallback: the aggregator hasn't seen any Phase-8C rows
    # for this (venue, dt) yet -- preserve the prior per-(venue, dt, date)
    # denominator so historical backfills don't regress in the UI.
    if legacy_row_count > 0 and len(non_legacy_instr) == 0:
        found_dates_set = {str(d) for d in date_series.unique() if str(d)}  # pyright: ignore[reportAny]
        found_in_expected = found_dates_set & expected_dates
        missing_dates = sorted(expected_dates - found_dates_set)
        # Review finding #3 (real gap, converged with the design's own Open
        # Question 1): this fully-legacy branch has no per-instrument grain
        # to fan out timeframes across, so it can only scale the AGGREGATE
        # ``expected_count`` by ``len(timeframes)`` -- it cannot recover a
        # timeframe-aware ``found_count`` from rows that never carried a
        # timeframe axis in the first place. Without this multiply,
        # ``expected_shards`` here would under-count by a factor of
        # ``len(timeframes)`` relative to the Tier-3 branch below for the
        # exact same (venue, dt) shape.
        expected_count = len(expected_dates) * tf_multiplier
        found_count = len(found_in_expected)
        legacy_entry: dict[str, object] = {
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
        if timeframes is not None:
            # Provenance marker (the review's alternative-acceptable fix,
            # applied IN ADDITION to the multiply above rather than instead
            # of it): tells a caller this slice's numerator did NOT get the
            # timeframe-aware treatment the Tier-3 branch gives, even though
            # its denominator was scaled to match — i.e. a 100% here is
            # structurally unreachable whenever more than one timeframe is
            # expected, which is the HONEST signal that this (venue, dt)
            # needs the real per-instrument backfill, not a "this looks
            # broken" false alarm.
            legacy_entry["denominator_timeframe_aware"] = False
        return legacy_entry

    # Phase 8D Tier-3 denominator.
    # Vectorised: build the (instrument_id, date) pair set with pandas mask
    # operations rather than a Python ``for/zip`` loop. For BINANCE-FUTURES
    # at 50 perps x ~3000 dates the prior loop iterated 150k times per
    # (venue, dt) pair; the masked path is dominated by the C-level isin().
    found_dates_in_window: set[str] = set()
    found_iid_dates_zipped: list[tuple[str, str]] = []
    found_iid_date_tf_zipped: list[tuple[str, str, str]] = []
    if len(non_legacy_instr) > 0:
        iid_str = non_legacy_instr.astype(str).str.strip()
        rd_str = non_legacy_dates.astype(str)
        mask = (iid_str.str.len() > 0) & rd_str.isin(expected_dates)
        if timeframes is not None:
            # MDPS-only: AND in the timeframe restriction. ``tf_str`` is
            # ``non_legacy_tf`` (built above from the SAME masked slice as
            # ``iid_str``/``rd_str`` -- the bug #2 fix) when a ``timeframe``
            # column exists, else an all-blank same-index Series so the
            # ``.isin(timeframes)`` correctly matches nothing (0% found,
            # never a KeyError) rather than raising on a missing column.
            tf_str = non_legacy_tf if non_legacy_tf is not None else pd.Series("", index=iid_str.index)
            mask = mask & tf_str.isin(timeframes)
        if bool(mask.any()):
            iid_kept = iid_str[mask].tolist()
            rd_kept = rd_str[mask].tolist()
            found_iid_dates_zipped = list(zip(iid_kept, rd_kept, strict=True))
            found_dates_in_window = set(rd_kept)
            if timeframes is not None:
                tf_kept = tf_str[mask].tolist()  # pyright: ignore[reportPossiblyUnboundVariable]
                found_iid_date_tf_zipped = list(zip(iid_kept, rd_kept, tf_kept, strict=True))
    found_pairs: set[tuple[str, str]] = set(found_iid_dates_zipped)
    found_triples: set[tuple[str, str, str]] = set(found_iid_date_tf_zipped)

    n_instruments = len(expected_instruments)
    # Per-instrument existence-window clipping (see `_clip_dates_to_window` +
    # the `instrument_windows` docstring above) — replaces the blanket
    # `n_instruments * len(expected_dates)` cross-product, which counted
    # structurally impossible (instrument, day) pairs (before listing / after
    # delisting) as "missing shards". Keyed by NORMALIZED instrument_id (bug
    # #4 pattern below) since `instrument_windows` comes from the catalogue
    # (canonical ids) while `found_pairs`' iid component comes from the
    # manifest (which can diverge in casing/whitespace/@SUFFIX from the
    # catalogue form). The existence window clips the DATE axis only —
    # timeframes are not existence-windowed, hence the separate
    # ``tf_multiplier`` factor below (MDPS only; 1 for every MTDS caller).
    per_instrument_expected: dict[str, frozenset[str]] = {
        _normalize_instrument_id_for_match(iid): _clip_dates_to_window(
            expected_dates, (instrument_windows or {}).get(iid)
        )
        for iid in expected_instruments
    }
    expected_count = sum(len(dates) for dates in per_instrument_expected.values()) * tf_multiplier
    # Numerator clipped to the SAME per-instrument window as the denominator —
    # a found pair outside an instrument's declared existence window (stale
    # catalogue, or a genuine anomaly) is excluded from both sides rather than
    # inflating completion_pct past what the denominator represents.
    # MDPS (``timeframes is not None``): count distinct (instrument, date,
    # timeframe) triples instead of (instrument, date) pairs — the ``d in
    # per_instrument_expected[...]`` window-clip check is unchanged (it only
    # ever looked at the date component).
    if timeframes is not None:
        found_count = sum(
            1
            for iid, d, _tf in found_triples
            if d in per_instrument_expected.get(_normalize_instrument_id_for_match(iid), frozenset(expected_dates))
        )
    else:
        found_count = sum(
            1
            for iid, d in found_pairs
            if d in per_instrument_expected.get(_normalize_instrument_id_for_match(iid), frozenset(expected_dates))
        )

    # Counter does both the per-instrument count AND gives us
    # ``instruments_with_shards`` as ``.keys()`` in one pass — replaces the
    # prior O(|instruments|*|pairs|) ``sum(1 for ...)`` loop below. MDPS
    # (``timeframes is not None``): count per-(instrument, date, timeframe)
    # triples so ``per_instrument["found"]`` below reflects the true
    # timeframe-aware shard count, not a single (instrument, date) pair
    # regardless of how many timeframes it actually has.
    if timeframes is not None:
        iid_counts = Counter(iid for iid, _d, _tf in found_triples)
    else:
        iid_counts = Counter(iid for iid, _d in found_pairs)
    # bug #4: match on the NORMALIZED instrument_id (see
    # _normalize_instrument_id_for_match) rather than the raw string, so a
    # casing / whitespace / @SUFFIX divergence between instruments-service's
    # catalog and MTDS's manifest doesn't show a real, fully-captured
    # instrument as phantom-missing. iid_counts (raw-keyed) is preserved
    # as-is for any other caller; matching/display below goes through the
    # normalized counter.
    normalized_iid_counts: Counter[str] = Counter()
    for iid, count in iid_counts.items():
        normalized_iid_counts[_normalize_instrument_id_for_match(iid)] += count
    instruments_with_shards = set(normalized_iid_counts)
    missing_instruments = [
        iid for iid in expected_instruments if _normalize_instrument_id_for_match(iid) not in instruments_with_shards
    ]

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
        per_instrument: dict[str, dict[str, object]] = {}
        for iid in expected_instruments:
            found = normalized_iid_counts.get(_normalize_instrument_id_for_match(iid), 0)
            iid_expected = (
                len(per_instrument_expected.get(_normalize_instrument_id_for_match(iid), frozenset())) * tf_multiplier
            )
            per_instrument[iid] = {
                "found": found,
                "expected": iid_expected,
                "completion_pct": min(round(found / max(1, iid_expected) * 100, 2), 100.0),
            }
        entry["per_instrument"] = per_instrument

    return entry

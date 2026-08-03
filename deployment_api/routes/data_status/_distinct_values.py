"""RAW distinct-values enumeration — the SSOT-alignment / canonical-drift panel.

``GET /distinct-values/{asset_group}`` — per ``asset_group``, the DISTINCT
``venues`` / ``instrument_types`` / ``data_types`` / ``chains`` actually present
in the data, each value badged ``is_canonical`` against the UAC canonical sets.

**Why this exists (operator, 2026-07-18):** the enumeration of "what raw values
exist per asset_group in the gcs data/manifest" was the way to spot non-canonical
naming + duplicates that feed the migration worklist. Read-side canonicalisation
of the hierarchical drilldown (``deployment-api@512180be``) + the cefi chain gate
(``@47a7f67``) collapsed those raw spellings for END-USER display — the right call
for that drilldown's UX, but it removed the drift SIGNAL. This endpoint restores
the signal as a standalone RAW read that deliberately does NOT canonicalise: every
raw spelling survives so ``LENDING`` vs ``lending``, ``AAVE`` vs ``AAVE_V3``, and
``ETHEREUM`` vs ``ethereum`` all show up side-by-side, each flagged
``is_canonical: false`` when it is not an exact member of the UAC canonical set for
its axis.

**Source (single-walk discipline):** the enumeration reads ONLY the pre-computed
nightly honest-coverage rollup
(``gs://{project}-honest-coverage/{date}/coverage.json``, produced by
``instruments-service/scripts/measure_honest_coverage.py``). Its
``by_venue`` / ``by_venue_instrument_type`` / ``by_venue_data_type`` / ``by_chain``
maps ALREADY enumerate every distinct value as their keys — the endpoint just reads
those keys and badges them. There is NO fresh whole-corpus GCS walk: one bounded
``blob_exists`` probe back over the rollup's retention window + a single
``download_bytes`` of the one small ``coverage.json``, cached in-process.
(``by_chain`` is the chain-enum addition landed alongside this endpoint in
``measure_honest_coverage.py``; a rollup written before it simply enumerates zero
chains until the next nightly run.)

**Canonical sets (imported from the UAC SSOT, never hardcoded):**
  - ``venues``            → ``VENUES_BY_ASSET_GROUP[asset_group]``
  - ``instrument_types``  → ``InstrumentType`` enum member values (global universe)
  - ``data_types``        → ``DATA_TYPES_BY_ASSET_GROUP[asset_group]``
  - ``chains``            → ``MAINNET_CHAIN_IDS`` keys

The membership test is EXACT (case-sensitive) by DEFAULT — a case/plural drift
(``lending`` vs ``LENDING``) is precisely what this panel must surface, so
lower-casing both sides would defeat it.

**Accepted-exception values (operator ruling 2026-07-22).** A small number of
raw values are genuinely non-canonical AND the operator has explicitly ruled
they must never surface as a finding here — distinct from "false drift" (the
grain-aware exceptions below): these values ARE real drift, but a permanently
ACCEPTED one, not something anyone is going to fix. Today this is exactly the
20 sports ODDS_API fan-out bookmakers (BETMGM..WILLIAMHILL): the 2026-05-12
scraper-deferral decision means no per-bookmaker capture adapter will ever be
built, so flagging them every run is pure noise, not an actionable finding.
:func:`_is_accepted_exception` drops these values from the enumeration
entirely (mirrors :func:`_is_blank`'s "never enumerate this" precedent) —
see :data:`_ACCEPTED_EXCEPTIONS` and
``unified_api_contracts.registry.SPORTS_ODDS_API_ACCEPTED_NONCANONICAL_BOOKMAKERS``
(``plans/active/distinct_values_noncanonical_audit_2026_07_20.md`` §
"Operator decisions — RULED 2026-07-22"). They are NOT added to the canonical
set (`VENUES_BY_ASSET_GROUP['sports']`) — that would misrepresent them as
adapter-producible, which the operator explicitly rejected.

Same mechanism, second case (audit follow-up 2026-07-22, same plan §
"futures_chain tradfi remedy decision"): tradfi's ``data_types`` axis carries
``options_chain`` (242,210 rows, 100% captured — the Era-A mark_iv/greeks
chain-snapshot cohort, operator PRESERVE-ruled) and ``futures_chain`` (8 rows,
100% captured — the same bundle-grain shape, carved out on the same
precedent). Neither belongs in ``DATA_TYPES_BY_ASSET_GROUP['tradfi']``
(conceptually they are INSTRUMENT_TYPES — one bundle per underlying — not
data_types; the data_type for the leaf rows is ``trades``), but both are real,
protected, permanently-accepted rows, not junk and not a naming fix waiting to
happen — see
``unified_api_contracts.registry.TRADFI_CHAIN_SNAPSHOT_ACCEPTED_NONCANONICAL_DATA_TYPES``.

Same mechanism, third case (audit follow-up 2026-07-22, same plan § "D5 —
bundle-grain recognition"): tradfi's AND cefi's ``instrument_types`` axis
carries ``options_chain``/``futures_chain`` as the real, deliberate MTDS
Tardis-writer bundle-grain ``instrument_type`` stamp (one manifest row per
underlying per day, bundling every strike/expiry leg — see MTDS
``tardis_bulk_download.py::shard_it_str``). Real captured rows at this axis
(2026-07-21 live rollup): tradfi ``futures_chain`` 154,147 / ``options_chain``
121,031. Neither is an ``InstrumentType`` enum member (that enum is
per-CONTRACT grain — ``FUTURE``/``OPTION``, the individual legs the bundle
contains) and neither should be added to it — see
``unified_api_contracts.registry.CHAIN_BUNDLE_ACCEPTED_NONCANONICAL_INSTRUMENT_TYPES``.
``combo`` is deliberately NOT part of this exception (mirrors this registry's
own ``TRADFI_CHAIN_INSTRUMENT_TYPES`` exclusion — its leg-aware id format is
unsettled); tradfi's lowercase ``combo``/``equity``/``etf``/``future``/
``index`` spellings remain real, separately-owned case-drift (the in-flight
tradfi uppercase migration), untouched by this entry.

**Grain-aware exceptions (audit 2026-07-20).** For two (axis, asset_group)
pairs the canonical set and the manifest column are keyed at DELIBERATELY
different grains, so a raw exact test reports false drift rather than real
drift. :func:`_comparison_set` resolves the grain for exactly those pairs:

  - ``(defi, venues)`` — canonical is chain-qualified (``UNISWAP_V2-ETHEREUM``)
    but the operator-locked DeFi naming SSOT stores **bare venue + a separate
    ``chain=`` segment**, so the manifest carries ``UNISWAP_V2``. Bare-vs-
    composite never matched → 76/76 DeFi venues badged non-canonical. Now
    compared against the bare bases of ``ALL_DEFI_VENUES`` — the full
    registered VOCABULARY (live + ``pipeline`` phase), **not**
    ``VENUES_BY_ASSET_GROUP['defi']`` (the ``phase == "live"`` CAPABILITY
    subset a naive first pass used, D1b correction 2026-07-20). A
    ``pipeline``-phase venue is genuinely registered but not yet
    IS-producible — badging it drift conflates "no adapter yet" with "not a
    real name". Flipping such a venue to ``live`` to silence the badge would
    be WRONG (breaks ``defi_venues.py``'s own
    ``phase=="live" <=> IS-producible`` invariant; independently confirmed
    2026-07-20 when a premature ``live`` flip on ``CHAINLINK-*`` broke the
    LDR→main promotion gate and had to be reverted, ``uac@83f17c46``).
  - ``(defi, instrument_types)`` — the DeFi manifest column is canonically
    LOWERCASE (operator ruling) while the enum is UPPERCASE → case-insensitive
    for defi ONLY. cefi/tradfi keep the exact test: their canonical
    instrument_type IS uppercase, so their lowercase spellings are REAL drift
    owned by the in-flight uppercase migrations, and folding case globally
    would hide it.

Every other (axis, asset_group) keeps the exact, case-sensitive test.

Split as a new submodule (mirrors ``_axis_census.py`` / ``_catalogue.py`` — attaches
to the shared package ``router``; see ``routes/data_status/__init__.py``).

Plan: data-status distinct-values restoration (operator ask 2026-07-18).
"""

from __future__ import annotations

import gzip
import json
import logging
import time
from datetime import UTC, datetime, timedelta
from typing import cast

from fastapi import HTTPException
from unified_api_contracts import InstrumentType
from unified_api_contracts.registry import (
    ALL_DEFI_VENUES,
    CEFI_VENUE_ACCEPTED_NONCANONICAL_ALIASES,
    CHAIN_BUNDLE_ACCEPTED_NONCANONICAL_INSTRUMENT_TYPES,
    DATA_TYPES_BY_ASSET_GROUP,
    MAINNET_CHAIN_IDS,
    SPORTS_DATA_TYPE_ACCEPTED_STALE_UPPERCASE_RESIDUE,
    SPORTS_MARKET_TOKEN_ACCEPTED_NONCANONICAL_INSTRUMENT_TYPES,
    SPORTS_ODDS_API_ACCEPTED_NONCANONICAL_BOOKMAKERS,
    SPORTS_VENUE_ACCEPTED_CROSS_AG_BLEED,
    TRADFI_CHAIN_AXIS_ACCEPTED_DEAD_RESIDUE,
    TRADFI_CHAIN_SNAPSHOT_ACCEPTED_NONCANONICAL_DATA_TYPES,
    TRADFI_INSTRUMENT_TYPE_ACCEPTED_UNRESOLVED_RESIDUE,
    TRADFI_VENUE_ACCEPTED_NONCANONICAL_ALIASES,
    VENUES_BY_ASSET_GROUP,
)

from deployment_api.routes.data_status import router
from deployment_api.settings import gcp_project_id

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# UAC canonical sets (SSOT — imported, never hardcoded).
# ---------------------------------------------------------------------------
# instrument_type is a GLOBAL universe (the InstrumentType enum spans every
# asset_group; a value is canonical iff it is a valid enum member value). venues
# + data_types are per-asset_group; chains are defi-only but the canonical
# universe is the global mainnet chain registry.
_CANONICAL_INSTRUMENT_TYPES: frozenset[str] = frozenset(member.value for member in InstrumentType)
_CANONICAL_CHAINS: frozenset[str] = frozenset(MAINNET_CHAIN_IDS)

# Sentinel spellings that mean "no value recorded" — never enumerated as a real
# distinct value (honest-absence; the coverage rollup can carry a literal "" /
# "None" / "nan" chain/instrument_type key for rows that never stamped one).
_BLANK_SENTINELS: frozenset[str] = frozenset({"", "none", "nan", "<na>", "null"})

# Values that are genuinely non-canonical but the operator has explicitly ruled
# must never surface as a finding on this panel (operator ruling 2026-07-22,
# distinct_values_noncanonical_audit_2026_07_20.md § "Operator decisions —
# RULED 2026-07-22" — "do NOT add them, in fact remove them everywhere so they
# don't come up in audit"). Keyed by (axis, asset_group); never made canonical
# (see the module docstring's "Accepted-exception values" section).
#
# ("data_types", "tradfi") added 2026-07-22 (same plan, "futures_chain tradfi
# remedy decision"): options_chain (242,210 rows) + futures_chain (8 rows) are
# real, operator-preserved chain-snapshot rows — genuinely non-canonical
# data_type values (they are bundle-grain INSTRUMENT_TYPES, not data_types) but
# never going to be relabelled/purged, so they must stop badging as drift the
# same way the sports bookmakers do.
#
# ("venues", "cefi") added 2026-07-22 (same plan, "D2 — cefi venue fold"):
# OKX-SWAP/OKX-FUTURES (and the other CEFI_VENUE_FOLD dialect spellings —
# legacy raw Tardis exchange ids, writer-side aliases) are real manifest
# values instruments-service's own `_canon_venue()` already recognises and
# folds to a canonical venue; they were only ever flagged here because this
# panel had no way to see that fold — same "known, understood, not drift"
# shape as the other two entries.
#
# ("instrument_types", "tradfi") / ("instrument_types", "cefi") added
# 2026-07-22 (same plan, "D5 — bundle-grain recognition"): options_chain /
# futures_chain are the real MTDS Tardis-writer bundle-grain instrument_type
# stamp (one manifest row per underlying per day, bundling every strike/expiry
# leg) — real captured rows (tradfi futures_chain 154,147 / options_chain
# 121,031 at this axis, 2026-07-21 live rollup), never a member of the
# InstrumentType enum (that enum is per-CONTRACT grain) and never going to be
# added to it. `combo` is deliberately NOT included (its leg-aware id format
# is unsettled — same exclusion this registry's own
# TRADFI_CHAIN_INSTRUMENT_TYPES already makes); tradfi's lowercase
# `combo`/`equity`/`etf`/`future`/`index` spellings are a separate,
# already-classified finding (real case-drift owned by the in-flight tradfi
# uppercase migration) untouched by this entry.
#
# 6 entries added 2026-07-30 (distinct-values census review, tradfi_distinct_values_
# net_new_clusters_2026_07_28.md + sports_instrument_type_market_token_ssot_gap_2026_07_28.md
# + sports_venue_restamp_derived_candle_gap_2026_07_27.md family): every value below was
# GCS-and-code investigated this session, never guessed — see the citing UAC exports
# (`TRADFI_VENUE_ACCEPTED_NONCANONICAL_ALIASES`, `TRADFI_CHAIN_AXIS_ACCEPTED_DEAD_RESIDUE`,
# `TRADFI_INSTRUMENT_TYPE_ACCEPTED_UNRESOLVED_RESIDUE`, `SPORTS_VENUE_ACCEPTED_CROSS_AG_BLEED`,
# `SPORTS_DATA_TYPE_ACCEPTED_STALE_UPPERCASE_RESIDUE`,
# `SPORTS_MARKET_TOKEN_ACCEPTED_NONCANONICAL_INSTRUMENT_TYPES`) for the full per-value
# evidence. `("chains", "tradfi")` and `("data_types"/"instrument_types", "sports")` are
# NEW axis/asset_group cells in this dict (tradfi had no chains entry; sports had no
# data_types/instrument_types entry) — none change the base canonical sets.
_ACCEPTED_EXCEPTIONS: dict[tuple[str, str], frozenset[str]] = {
    ("venues", "sports"): SPORTS_ODDS_API_ACCEPTED_NONCANONICAL_BOOKMAKERS | SPORTS_VENUE_ACCEPTED_CROSS_AG_BLEED,
    ("venues", "cefi"): CEFI_VENUE_ACCEPTED_NONCANONICAL_ALIASES,
    ("venues", "tradfi"): TRADFI_VENUE_ACCEPTED_NONCANONICAL_ALIASES,
    ("chains", "tradfi"): TRADFI_CHAIN_AXIS_ACCEPTED_DEAD_RESIDUE,
    ("data_types", "tradfi"): TRADFI_CHAIN_SNAPSHOT_ACCEPTED_NONCANONICAL_DATA_TYPES,
    ("data_types", "sports"): SPORTS_DATA_TYPE_ACCEPTED_STALE_UPPERCASE_RESIDUE,
    ("instrument_types", "tradfi"): (
        CHAIN_BUNDLE_ACCEPTED_NONCANONICAL_INSTRUMENT_TYPES | TRADFI_INSTRUMENT_TYPE_ACCEPTED_UNRESOLVED_RESIDUE
    ),
    ("instrument_types", "cefi"): CHAIN_BUNDLE_ACCEPTED_NONCANONICAL_INSTRUMENT_TYPES,
    ("instrument_types", "sports"): SPORTS_MARKET_TOKEN_ACCEPTED_NONCANONICAL_INSTRUMENT_TYPES,
}

# Each output axis, the coverage.json section its distinct values live in, and
# whether the values are that section's TOP-level keys (``by_venue`` /
# ``by_chain``: ag -> {value: counts}) or the INNER keys one level down
# (``by_venue_instrument_type`` / ``by_venue_data_type``: ag -> {venue:
# {value: counts}}).
_AXIS_SOURCES: tuple[tuple[str, str, str], ...] = (
    ("venues", "by_venue", "top"),
    ("instrument_types", "by_venue_instrument_type", "inner"),
    ("data_types", "by_venue_data_type", "inner"),
    ("chains", "by_chain", "top"),
)

# ---------------------------------------------------------------------------
# Nightly honest-coverage rollup reader (bounded probe + in-process cache).
# ---------------------------------------------------------------------------
_HONEST_COVERAGE_BUCKET_TEMPLATE: str = "{pid}-honest-coverage"
# Probe back this many days from today for the newest {date}/coverage.json. The
# producing cron writes daily; the window covers a few missed runs without ever
# LISTING the bucket (bounded blob_exists probes only — no whole-corpus walk).
_COVERAGE_LOOKBACK_DAYS: int = 8
_COVERAGE_CACHE_TTL_SEC: int = 1800  # match rollup_cache — the nightly is at most a day old anyway
# bucket -> (monotonic_read_ts, payload, source_date)
_COVERAGE_CACHE: dict[str, tuple[float, dict[str, object], str]] = {}


def _read_honest_coverage_rollup() -> tuple[dict[str, object], str] | None:
    """Return ``(coverage_payload, source_date)`` from the newest nightly
    ``coverage.json``, or ``None`` when none is reachable in the lookback window.

    Reads ``gs://{project}-honest-coverage/{date}/coverage.json`` — the rollup
    ``measure_honest_coverage.py`` writes daily. Probes today backwards day-by-day
    (bounded ``blob_exists`` calls, never a bucket LIST) and downloads the single
    newest ``coverage.json``. Result is cached in-process for
    :data:`_COVERAGE_CACHE_TTL_SEC`.
    """
    bucket = _HONEST_COVERAGE_BUCKET_TEMPLATE.format(pid=gcp_project_id)
    now = time.monotonic()
    cached = _COVERAGE_CACHE.get(bucket)
    if cached is not None and (now - cached[0]) < _COVERAGE_CACHE_TTL_SEC:
        return cached[1], cached[2]

    from unified_trading_library import get_storage_client  # noqa: imports-inside-functions

    client = get_storage_client(project_id=gcp_project_id)
    today = datetime.now(UTC).date()
    for delta in range(_COVERAGE_LOOKBACK_DAYS):
        source_date = (today - timedelta(days=delta)).isoformat()
        blob_path = f"{source_date}/coverage.json"
        try:
            if not client.blob_exists(bucket, blob_path):  # pyright: ignore[reportAttributeAccessIssue]
                continue
            raw = client.download_bytes(bucket, blob_path)  # pyright: ignore[reportAttributeAccessIssue]
        except (OSError, RuntimeError, ValueError) as exc:
            logger.info("honest-coverage read failed for %s/%s (%s) — trying older", bucket, blob_path, exc)
            continue
        payload_bytes = gzip.decompress(raw) if raw[:2] == b"\x1f\x8b" else raw
        try:
            payload = json.loads(payload_bytes.decode("utf-8"))  # pyright: ignore[reportAny]
        except (ValueError, json.JSONDecodeError) as exc:
            logger.warning("honest-coverage %s/%s is not valid JSON (%s) — trying older", bucket, blob_path, exc)
            continue
        if not isinstance(payload, dict):
            logger.warning("honest-coverage %s/%s is not a dict — trying older", bucket, blob_path)
            continue
        typed = cast(dict[str, object], payload)
        _COVERAGE_CACHE[bucket] = (now, typed, source_date)
        return typed, source_date
    return None


# ---------------------------------------------------------------------------
# Pure enumeration + badging (no I/O — unit-testable + runtime-verifiable in
# isolation from a coverage.json payload).
# ---------------------------------------------------------------------------
def _is_blank(value: str) -> bool:
    return value.strip().lower() in _BLANK_SENTINELS


def _is_accepted_exception(axis: str, asset_group: str, value: str) -> bool:
    """Values deliberately excluded from the drift panel (see
    :data:`_ACCEPTED_EXCEPTIONS` + the module docstring's "Accepted-exception
    values" section). Not a canonicalisation — the value stays genuinely
    non-canonical, it is just not treated as an actionable finding."""
    return value in _ACCEPTED_EXCEPTIONS.get((axis, asset_group), frozenset())


def _top_level_keys(section: object) -> set[str]:
    """Distinct keys of a ``ag -> {value: counts}`` coverage section."""
    if not isinstance(section, dict):
        return set()
    return {str(key) for key in cast(dict[object, object], section)}


def _inner_keys(section: object) -> set[str]:
    """Union of the inner keys of a ``ag -> {venue: {value: counts}}`` section."""
    out: set[str] = set()
    if not isinstance(section, dict):
        return out
    for inner in cast(dict[object, object], section).values():
        if isinstance(inner, dict):
            out |= {str(key) for key in cast(dict[object, object], inner)}
    return out


def _canonical_set(axis: str, asset_group: str) -> frozenset[str]:
    """The UAC canonical value set for one axis + asset_group."""
    if axis == "venues":
        return frozenset(VENUES_BY_ASSET_GROUP.get(asset_group, []))
    if axis == "data_types":
        return frozenset(DATA_TYPES_BY_ASSET_GROUP.get(asset_group, []))
    if axis == "instrument_types":
        return _CANONICAL_INSTRUMENT_TYPES
    if axis == "chains":
        return _CANONICAL_CHAINS
    return frozenset()


def _defi_bare_venue_bases(composites: frozenset[str]) -> frozenset[str]:
    """Strip the trailing ``-{CHAIN}`` from each chain-qualified DeFi venue.

    ``VENUES_BY_ASSET_GROUP['defi']`` stores venues CHAIN-QUALIFIED
    (``UNISWAP_V2-ETHEREUM``, ``AAVE_V3-BASE``), but the DeFi canonical naming
    SSOT (``codex/02-data/defi-canonical-naming-ssot.md``, operator-locked) is
    **bare venue + a separate ``chain=`` path segment** — so the manifest (and
    therefore the honest-coverage rollup's ``by_venue`` keys) carries the BARE
    venue. Comparing bare-vs-composite can never match, which badged 100% of
    DeFi venues non-canonical (76/76) — a false alarm on the majority.

    The chain is the LAST ``-`` segment, but the base itself may contain ``-``
    (``SOLANA-NATIVE-SOLANA`` → base ``SOLANA-NATIVE``, chain ``SOLANA``), so a
    naive ``split("-")[0]`` is WRONG. Strip only a suffix that is a real member
    of the chain registry.
    """
    bases: set[str] = set()
    for venue in composites:
        base = venue
        for chain in _CANONICAL_CHAINS:
            suffix = f"-{chain}"
            if venue.endswith(suffix) and len(venue) > len(suffix):
                base = venue[: -len(suffix)]
                break
        bases.add(base)
    return frozenset(bases)


def _comparison_set(axis: str, asset_group: str) -> tuple[frozenset[str], bool]:
    """Return ``(set_to_compare_against, casefold)`` for one axis + asset_group.

    The canonical set and the manifest column are deliberately keyed at
    DIFFERENT grains for some (axis, asset_group) pairs; comparing them raw
    produces false drift badges. This resolves the grain so the panel flags
    only REAL drift:

    - ``(defi, venues)`` — canonical is chain-qualified, manifest is bare →
      compare against the bare bases (see :func:`_defi_bare_venue_bases`).
    - ``(defi, instrument_types)`` — the DeFi manifest column is canonically
      LOWERCASE (operator ruling) while ``InstrumentType`` is UPPERCASE →
      case-insensitive compare. Deliberately NOT applied to cefi/tradfi, whose
      canonical instrument_type IS uppercase and whose lowercase spellings are
      genuine drift owned by the in-flight uppercase migrations — folding case
      globally would HIDE that.

    Every other pair keeps the exact, case-sensitive test.
    """
    canonical = _canonical_set(axis, asset_group)
    if axis == "venues" and asset_group == "defi":
        # D1b (audit 2026-07-20 RESULT — corrects the original D1 fix): compare
        # against ALL_DEFI_VENUES (the full registered VOCABULARY, live + pipeline
        # phase), NOT VENUES_BY_ASSET_GROUP['defi'] (the phase == "live" CAPABILITY
        # subset returned by _canonical_set above). A `pipeline`-phase venue
        # (ANKR/FRAX/MAKER/STADER/STAKEWISE/SWELL/ACROSS/STARGATE/FLASHBOTS/MANTLE —
        # registered, genesis-dated, but IS has no adapter yet) is legitimate
        # vocabulary the manifest can carry; badging it as naming DRIFT confuses a
        # capability gap with a naming gap. Flipping such a venue to `live` in the
        # registry to "fix" this would be WRONG — it would assert IS can produce it
        # when it cannot, breaking `defi_venues.py`'s own
        # `phase=="live" <=> IS-producible` invariant (independently confirmed
        # 2026-07-20: uac@83f17c46 reverted CHAINLINK-* to `pipeline` after exactly
        # that class of premature `live` flip broke the LDR->main promotion gate).
        return _defi_bare_venue_bases(frozenset(ALL_DEFI_VENUES)), False
    if axis == "instrument_types" and asset_group == "defi":
        return frozenset(v.casefold() for v in canonical), True
    return canonical, False


def enumerate_distinct_values(
    coverage: dict[str, object],
    asset_group: str,
) -> tuple[dict[str, list[dict[str, object]]], dict[str, int]]:
    """Enumerate the RAW distinct values per axis for ``asset_group`` from a
    honest-coverage ``coverage.json`` payload, badging each ``is_canonical``.

    Returns ``(axes, non_canonical_count)`` where ``axes[axis]`` is a list of
    ``{"value": <raw string>, "is_canonical": <bool>}`` sorted by value and
    ``non_canonical_count[axis]`` is how many of them failed the canonical check
    (the drift headline). Values are NOT collapsed/canonicalised — every raw
    spelling variant survives, which is the entire point of the panel — EXCEPT
    blank sentinels (:func:`_is_blank`) and operator-accepted-exception values
    (:func:`_is_accepted_exception`, see :data:`_ACCEPTED_EXCEPTIONS`), both of
    which are dropped before enumeration.
    """
    ag = asset_group.lower()
    axes: dict[str, list[dict[str, object]]] = {}
    non_canonical_count: dict[str, int] = {}
    for axis, section_key, mode in _AXIS_SOURCES:
        section = coverage.get(section_key)
        ag_section = section.get(ag) if isinstance(section, dict) else None
        raw_values = _top_level_keys(ag_section) if mode == "top" else _inner_keys(ag_section)
        canonical, casefold = _comparison_set(axis, ag)
        entries: list[dict[str, object]] = []
        nc = 0
        for value in sorted(v for v in raw_values if not _is_blank(v) and not _is_accepted_exception(axis, ag, v)):
            is_canonical = (value.casefold() if casefold else value) in canonical
            if not is_canonical:
                nc += 1
            entries.append({"value": value, "is_canonical": is_canonical})
        axes[axis] = entries
        non_canonical_count[axis] = nc
    return axes, non_canonical_count


@router.get("/distinct-values/{asset_group}")
async def get_distinct_values(asset_group: str) -> dict[str, object]:
    """RAW distinct venues / instrument_types / data_types / chains present for
    ``asset_group``, each badged ``is_canonical`` against the UAC canonical sets —
    the SSOT-alignment / canonical-drift audit panel.

    Sourced from the nightly honest-coverage ``coverage.json`` rollup (its
    ``by_venue*`` / ``by_chain`` map keys ARE the enumeration); values are returned
    uncollapsed so case/plural/grain drift stays visible. ``non_canonical_count``
    is the per-axis drift headline.
    """
    result = _read_honest_coverage_rollup()
    if result is None:
        raise HTTPException(
            status_code=503,
            detail="Honest-coverage rollup unavailable (no coverage.json in the lookback window).",
        )
    coverage, source_date = result
    axes, non_canonical_count = enumerate_distinct_values(coverage, asset_group)
    return {
        "asset_group": asset_group.lower(),
        "source": "honest-coverage-rollup",
        "source_date": source_date,
        "generated_at": coverage.get("generated_at"),
        "axes": axes,
        "non_canonical_count": non_canonical_count,
    }

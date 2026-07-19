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

The membership test is EXACT (case-sensitive) on purpose — a case/plural drift
(``lending`` vs ``LENDING``) is precisely what this panel must surface, so
lower-casing both sides would defeat it.

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
    DATA_TYPES_BY_ASSET_GROUP,
    MAINNET_CHAIN_IDS,
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

    from unified_trading_library import get_storage_client

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
    spelling variant survives, which is the entire point of the panel.
    """
    ag = asset_group.lower()
    axes: dict[str, list[dict[str, object]]] = {}
    non_canonical_count: dict[str, int] = {}
    for axis, section_key, mode in _AXIS_SOURCES:
        section = coverage.get(section_key)
        ag_section = section.get(ag) if isinstance(section, dict) else None
        raw_values = _top_level_keys(ag_section) if mode == "top" else _inner_keys(ag_section)
        canonical = _canonical_set(axis, ag)
        entries: list[dict[str, object]] = []
        nc = 0
        for value in sorted(v for v in raw_values if not _is_blank(v)):
            is_canonical = value in canonical
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

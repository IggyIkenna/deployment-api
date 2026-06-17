"""Reference-data (per-venue/day bundle) scope expectations.

instruments-service / corporate-actions emit REFERENCE data: one bundled
``(venue, day)`` parquet recorded with ``data_type=""`` (the "instruments"
row). That token is in NEITHER the market-data ``EXPECTED_COVERAGE_BY_ASSET_GROUP``
registry nor ``PROCESSED_REQUIRES_RAW`` (candles), so the market-data scope
check in ``breakdowns_core._classify_data_type_for_venue`` classified EVERY
reference row as ``out_of_scope`` (`scope_in=False ∧ dt_is_processed=False`).

The proper scope source for a reference bundle service is the
instruments-service catalogue, NOT the market-data registry. A
``(asset_group, venue)`` is in-scope iff it is a CONFIGURED instruments-service
venue for that asset_group AND the row covers at least one day on/after that
venue's genesis (``start_date``). A configured venue that is simply MISSING
data stays in-scope + missing (a real, actionable gap in the denominator); an
unlisted venue or a purely pre-genesis day is genuinely out_of_scope.

Genesis source: ``data-catalogue.instruments-service.yaml``
``shard_status[ASSET_GROUP][VENUE].start_date`` (read via the bundled
``get_config_dir()``). The same file's keys ARE the configured IS venue
universe per asset_group.

Grain = venue/day. Per-instrument_type scoping is a separate follow-on tied to
a manifest-schema change and is intentionally NOT attempted here.
"""

from __future__ import annotations

import logging
from typing import cast

import yaml

from deployment_api.utils.service_utils import get_config_dir

logger = logging.getLogger(__name__)

# Services whose manifest rows are reference-data ``(venue, day)`` bundles.
# Kept in sync with ``data_status_drilldown._instruments._PER_VENUE_DAY_BUNDLE_SERVICES``.
REFERENCE_BUNDLE_SERVICES: frozenset[str] = frozenset({"instruments-service", "corporate-actions"})

_CATALOGUE_FILENAME: str = "data-catalogue.instruments-service.yaml"

# Cache: {(asset_group_upper, venue_upper): genesis_date_str}. ``None`` means
# "not yet loaded"; an empty dict means "loaded, nothing configured".
_genesis_by_venue: dict[tuple[str, str], str] | None = None


def is_reference_bundle_service(service: str) -> bool:
    """True when ``service`` records reference-data ``(venue, day)`` bundles."""
    return service.lower() in REFERENCE_BUNDLE_SERVICES


def _parse_genesis_map(raw: object) -> dict[tuple[str, str], str]:
    """Extract ``{(asset_group, venue): genesis}`` from parsed catalogue YAML.

    Keys are upper-cased so lookups are case-insensitive against the venue /
    asset_group values that reach ``_classify_data_type_for_venue``.
    """
    out: dict[tuple[str, str], str] = {}
    if not isinstance(raw, dict):
        return out
    raw_map = cast(dict[str, object], raw)
    shard_status = raw_map.get("shard_status")
    if not isinstance(shard_status, dict):
        return out
    for ag_raw, venues_raw in cast(dict[object, object], shard_status).items():
        if not isinstance(venues_raw, dict):
            continue
        ag = str(ag_raw).upper()
        for venue_raw, meta_raw in cast(dict[object, object], venues_raw).items():
            if not isinstance(meta_raw, dict):
                continue
            start = cast(dict[object, object], meta_raw).get("start_date")
            if isinstance(start, str) and start.strip():
                out[(ag, str(venue_raw).upper())] = start.strip()
    return out


def _load_genesis_map() -> dict[tuple[str, str], str]:
    """Load + cache ``{(asset_group, venue): genesis}`` from the IS catalogue."""
    global _genesis_by_venue
    if _genesis_by_venue is not None:
        return _genesis_by_venue

    try:
        path = get_config_dir() / _CATALOGUE_FILENAME
        with path.open("r", encoding="utf-8") as fh:
            raw: object = cast(object, yaml.safe_load(fh))
    except (OSError, RuntimeError, yaml.YAMLError) as exc:
        logger.warning("Could not load %s for reference scope: %s", _CATALOGUE_FILENAME, exc)
        _genesis_by_venue = {}
        return _genesis_by_venue

    _genesis_by_venue = _parse_genesis_map(raw)
    return _genesis_by_venue


def reset_genesis_cache() -> None:
    """Drop the cached genesis map (test seam / config hot-reload)."""
    global _genesis_by_venue
    _genesis_by_venue = None


# Market-role suffixes the market-data manifest appends to a base exchange
# (``COINBASE-SPOT``/``OKX-FUTURES``/``OKX-SWAP``). The IS catalogue thinks in
# BASE venues (``COINBASE``/``OKX``), so a role-qualified manifest token must be
# matched against its base before we declare the venue unlisted.
_VENUE_ROLE_SUFFIXES: tuple[str, ...] = ("-SPOT", "-FUTURES", "-SWAP", "-PERP", "-PERPETUAL", "-COMBO")


def _base_venue_token(venue_upper: str) -> str:
    """Strip a trailing market-role suffix → the base exchange token."""
    for suffix in _VENUE_ROLE_SUFFIXES:
        if venue_upper.endswith(suffix):
            return venue_upper[: -len(suffix)]
    return venue_upper


def reference_genesis(asset_group: str, venue: str) -> str | None:
    """Return the IS genesis ``start_date`` for ``(asset_group, venue)``.

    ``None`` ⟺ the venue is NOT a configured instruments-service venue for the
    asset_group (an unlisted venue → out_of_scope).

    The IS catalogue lists base exchanges (``COINBASE``/``OKX``) while the
    market-data manifest qualifies them by role (``COINBASE-SPOT``/
    ``OKX-FUTURES``); try the exact token first, then fall back to the base so a
    configured venue isn't flagged out_of_scope merely for the role suffix.
    """
    genesis_map = _load_genesis_map()
    ag = asset_group.upper()
    venue_upper = venue.upper()
    genesis = genesis_map.get((ag, venue_upper))
    if genesis is not None:
        return genesis
    base = _base_venue_token(venue_upper)
    if base != venue_upper:
        return genesis_map.get((ag, base))
    return None


def is_reference_venue_day_in_scope(asset_group: str, venue: str, days: list[str]) -> bool:
    """Venue/day reference-scope verdict for a bundle-service row.

    In-scope ⟺ ``(asset_group, venue)`` is a configured instruments-service
    venue AND ``days`` covers at least one day on/after that venue's genesis.

    - Unlisted venue (no genesis) → ``False`` (out_of_scope).
    - Configured venue, ``days`` empty → ``True`` (the venue is in-scope; an
      empty missing set just means nothing is owed on this row right now — a
      covered-but-no-gap row, never out_of_scope).
    - Configured venue, ``days`` all strictly pre-genesis → ``False``
      (genuinely out_of_scope; no day this row owns is within coverage).
    """
    genesis = reference_genesis(asset_group, venue)
    if genesis is None:
        return False
    if not days:
        return True
    return any(d >= genesis for d in days)

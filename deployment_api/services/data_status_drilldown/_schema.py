"""Schema introspection for leaf shards (contract lookup + parquet projection).

Split from ``services/data_status_drilldown.py`` (pure code motion; plan:
``codex_violations_ratchet_to_five_2026_06_10.md`` Phase-1 P2). Patched
module-level collaborators (``list_objects`` / ``read_availability_index`` /
``resolve_bucket_name`` / ``build_bucket_name`` / the parquet readers) are
resolved through the package facade (``_dd``) at call time so the existing
test patch surface ``deployment_api.services.data_status_drilldown.<name>``
keeps intercepting.
"""

from __future__ import annotations

import logging

from unified_api_contracts import SchemaContract, SchemaContractNotFoundError, lookup_contract
from unified_api_contracts.internal.schemas.contracts import (
    VENUE_CONTRACT_OVERRIDES,
)

import deployment_api.services.data_status_drilldown as _dd

logger = logging.getLogger(__name__)


def _column_dicts(contract: SchemaContract) -> list[dict[str, object]]:
    return [
        {
            "name": col.name,
            "dtype": col.dtype,
            "nullable": col.nullable,
            "description": col.description or "",
        }
        for col in contract.columns
    ]


# Known data_type / instrument_type aliases emitted by UI / manifest that
# don't match the canonical UAC contract keys 1:1. Source:
# deployment-ui-playwright-audit 2026-04-19 §5.5 — the UI can send
# ``POOL_DEFINITION`` / ``POOL_SNAPSHOT`` / ``LIQUIDITY_POOL`` / ``POOL``
# (uppercase) while UAC keys them as ``pool`` / ``dex_pool_state``.
_INSTRUMENT_TYPE_ALIASES: dict[str, str] = {
    "POOL": "pool",
    "POOL_SNAPSHOT": "pool",
    "LIQUIDITY_POOL": "pool",
    "DEX_POOL": "dex_pool",
}

_DATA_TYPE_ALIASES: dict[str, str] = {
    "POOL_DEFINITION": "dex_pool_state",
    "INSTRUMENT_DEFINITION": "dex_pool_state",
    "POOL_SNAPSHOT": "dex_pool_state",
    "POOL_STATE": "dex_pool_state",
    "POOL_SWAPS": "dex_pool_swaps",
}


def _normalise_instrument_type(raw: str) -> str:
    """Return the UAC-canonical instrument_type for a UI-supplied value."""
    if raw in _INSTRUMENT_TYPE_ALIASES:
        return _INSTRUMENT_TYPE_ALIASES[raw]
    # Registry keys are lowercase snake_case — lowercase all-caps inputs
    # so ``POOL`` resolves even without an explicit alias.
    return raw.lower() if raw.isupper() else raw


def _normalise_data_type(raw: str) -> str:
    """Return the UAC-canonical data_type for a UI-supplied value."""
    if raw in _DATA_TYPE_ALIASES:
        return _DATA_TYPE_ALIASES[raw]
    return raw.lower() if raw.isupper() else raw


# SPORTS data_type → instrument_type resolver. The UI passes
# ``instrument_type=""`` for sports (the manifest doesn't carry one), but UAC
# contract keys require an instrument_type (``("sports", "match", "fixtures")``,
# ``("sports", "reference", "leagues")``, …). Resolve at the boundary so the
# UI doesn't need to know UAC's internal partitioning. SSOT plan:
# ``sports_uac_schema_contracts_registration_2026_04_24``.
_SPORTS_DATA_TYPE_TO_INSTRUMENT_TYPE: dict[str, str] = {
    # Family A — API-Football reference
    "leagues": "reference",
    "teams": "reference",
    "venues": "reference",
    "transfermarkt_leagues": "reference",
    "sfi_leagues": "reference",
    # league-level snapshot (slightly different shape from match facts)
    "standings": "league",
    # Family B — API-Football match fact + Family D progressive + Family E
    "fixtures": "match",
    "fixture_events": "match",
    "fixture_stats": "match",
    "fixture_lineups": "match",
    "player_stats": "match",
    "injuries": "match",
    "sfi_progressive_stats": "match",
    "matches": "match",
    "predictions": "match",
    "xg": "match",
    "weather": "match",
    # Family C — Transfermarkt player values
    "player_values": "player",
    # Family E — FSS derivative
    "fixture_features": "feature",
    # Existing pre-plan registration: sports/odds/trades
    "trades": "odds",
}


_AUTO_SENTINELS: frozenset[str] = frozenset({"auto", "unknown", "auto_detect_fail", ""})


def _resolve_sports_instrument_type(asset_group: str, instrument_type: str, data_type: str) -> str:
    """If category=sports and instrument_type is empty/AUTO, infer from data_type.

    The UI passes ``"AUTO"`` (uppercase sentinel) at SPORTS click sites — the
    SPORTS manifest has no instrument_type axis, so we resolve it from
    ``data_type`` via :data:`_SPORTS_DATA_TYPE_TO_INSTRUMENT_TYPE`.
    """
    if asset_group != "sports":
        return instrument_type
    if instrument_type and instrument_type.lower() not in _AUTO_SENTINELS:
        return instrument_type
    return _SPORTS_DATA_TYPE_TO_INSTRUMENT_TYPE.get(data_type, instrument_type)


def get_schema_for_shard(
    *,
    asset_group: str,
    instrument_type: str,
    data_type: str,
    venue: str | None = None,
    service: str | None = None,
    bucket: str | None = None,
    day: str | None = None,
    chain: str | None = None,
    instrument_id: str | None = None,
    league_id: str | None = None,
    fixture_id: str | None = None,
    canonical_question_group: str | None = None,
    job_id: str | None = None,
    model_family: str | None = None,
    training_period: str | None = None,
    strategy_id: str | None = None,
    instruction_type: str | None = None,
    feature_group: str | None = None,
    timeframe: str | None = None,
) -> dict[str, object]:
    """Return the SchemaContract columns for a shard tuple.

    Falls back gracefully when no contract is registered — returns an empty
    column list with ``registered: False`` so the UI can render a
    "no schema registered — running raw projection" affordance instead of
    raising.

    When ``instrument_type`` is ``"AUTO"`` (case-insensitive) / ``"UNKNOWN"``
    / empty, the registry is searched for any matching ``(category,
    data_type)`` tuple and the resolved instrument_type is echoed in the
    response.  ``instrument_type_resolved_via`` is one of ``explicit`` /
    ``auto`` / ``none``.
    """
    # Local import to avoid an import cycle when shard_detail imports this
    # module's siblings.  The AUTO resolver lives in shard_detail.py as the
    # canonical SSOT for AUTO mode.
    from deployment_api.services.shard_detail import (
        _is_auto_instrument_type,  # pyright: ignore[reportPrivateUsage]
        _resolve_instrument_type_auto,  # pyright: ignore[reportPrivateUsage]
    )

    # Normalise UI inputs so the lookup hits the UAC registry keys
    # (lowercase snake_case). The UI passes ``POOL`` / ``POOL_DEFINITION``
    # from the manifest; UAC keys those as ``pool`` / ``dex_pool_state``.
    cat_norm = asset_group.lower()

    # instruments-service legacy v4 rows for cefi/tradfi/defi/prediction carry
    # empty instrument_type and empty data_type. Synthesise the catalogue axes
    # so the lookup resolves the registered INSTRUMENT_CATALOGUE contract.
    if (
        (service or "").lower() == "instruments-service"
        and not (instrument_type or "").strip()
        and not (data_type or "").strip()
    ):
        instrument_type = "instrument_catalogue"
        data_type = "instrument_catalogue"

    dt_norm = _normalise_data_type(data_type)
    dt_lookup = (data_type or "").lower()

    auto_requested = _is_auto_instrument_type(instrument_type)
    resolved_via = "explicit"
    if auto_requested:
        picked = _resolve_instrument_type_auto(category=cat_norm, data_type=dt_lookup, venue=venue)
        if picked is None:
            return {
                "registered": False,
                "category": cat_norm,
                "instrument_type": "",
                "data_type": dt_norm,
                "venue": venue,
                "symbol_column": None,
                "source": "none",
                "columns": [],
                "message": (f"No SchemaContract found for category={cat_norm} data_type={dt_lookup}"),
                "instrument_type_resolved_via": "none",
            }
        it_norm = picked
        resolved_via = "auto"
    else:
        it_norm = _normalise_instrument_type(instrument_type)

    # Venue override takes priority, else base registry, else fallback.
    try:
        contract = lookup_contract(
            asset_group=cat_norm,
            instrument_type=it_norm,
            data_type=dt_norm,
            venue=venue,
        )
    except SchemaContractNotFoundError:
        return {
            "registered": False,
            "category": cat_norm,
            "instrument_type": it_norm,
            "data_type": dt_norm,
            "venue": venue,
            "symbol_column": None,
            "source": "PARQUET_PROJECTION",
            "columns": [],
            "projected_from": None,
            "message": (
                "No contract registered for this shard. The UI should fall back to projecting actual parquet columns."
            ),
            "instrument_type_resolved_via": resolved_via,
        }

    # Figure out whether we resolved via override or base registry.
    source = "CONTRACT_REGISTRY"
    if venue is not None:
        override_key = (cat_norm, (venue or "").upper(), it_norm, dt_norm)
        if override_key in VENUE_CONTRACT_OVERRIDES:
            source = "VENUE_CONTRACT_OVERRIDES"

    return {
        "registered": True,
        "category": contract.asset_group,
        "instrument_type": contract.instrument_type,
        "data_type": contract.data_type,
        "venue": (venue or "").upper() if venue else None,
        "symbol_column": contract.symbol_column,
        "source": source,
        "columns": _column_dicts(contract),
        "required_row_count_min": contract.required_row_count_min,
        "instrument_type_resolved_via": resolved_via,
    }


def _project_leaf_parquet_columns(
    *,
    service: str | None,
    asset_group: str,
    bucket: str | None,
    day: str | None,
    axes: dict[str, str | None],
) -> tuple[list[str], str] | None:
    """Probe the actual parquet for a leaf shard and return its column names.

    Returns ``(columns, gs_uri)`` on success, ``None`` when no candidate
    parquet exists. The candidate paths depend on (service, asset_group);
    sports uses UAC ``candidate_parquet_paths`` (per-league subpartition
    first, then bare path), instruments-service uses the per-(venue, day)
    bundle, MTDS / MDPS / features-* probe the per-instrument or
    per-feature_group leaf, and ML / strategy / execution probe the
    per-job_id partition.

    Failure to resolve is non-fatal — caller falls through to the honest
    ``registered: False`` response.
    """
    if not (service and bucket and day):
        return None
    try:
        candidates = _build_leaf_parquet_candidates(
            service=service, asset_group=asset_group, day=day, axes=axes, bucket=bucket
        )
    except (ValueError, RuntimeError) as exc:
        logger.debug("leaf parquet candidate build failed: %s", exc)
        return None
    for gs_uri in candidates:
        # Directory prefix candidates need a list-first probe to find the
        # actual parquet file under the prefix.
        if gs_uri.endswith("/"):
            resolved = _first_parquet_under_prefix(gs_uri)
            if resolved is None:
                continue
            gs_uri = resolved
        try:
            names = _dd._parquet_schema_names(gs_uri)  # pyright: ignore[reportPrivateUsage]
        except (OSError, RuntimeError, ValueError):
            continue
        if names:
            # Stable order: alphabetic with the obvious anchors first.
            anchors = [c for c in ("date", "timestamp", "available_at", "instrument_id") if c in names]
            rest = sorted(n for n in names if n not in anchors)
            return ([*anchors, *rest], gs_uri)
    return None


def _first_parquet_under_prefix(gs_prefix: str) -> str | None:
    """List ``gs_prefix`` and return the first .parquet URI, or None.

    Used by the schema projection fallback when the candidate path is a
    hive-partition directory rather than a fully-qualified parquet file.
    Limits the listing to 10 results — we only need one parquet to
    project the schema.
    """
    if not gs_prefix.startswith("gs://"):
        return None
    rest = gs_prefix[len("gs://") :]
    bucket, _, prefix = rest.partition("/")
    if not bucket:
        return None
    try:
        blobs = _dd.list_objects(bucket_name=bucket, prefix=prefix, max_results=10)
    except (OSError, RuntimeError, ValueError):
        return None
    for blob in blobs:
        name = getattr(blob, "name", None)
        if isinstance(name, str) and name.endswith(".parquet"):
            return f"gs://{bucket}/{name}"  # noqa: gs-uri  — URI composer, bucket already resolved
    return None


def _build_leaf_parquet_candidates(
    *,
    service: str,
    asset_group: str,
    day: str,
    axes: dict[str, str | None],
    bucket: str,
) -> list[str]:
    """Return the ordered list of GCS URIs to probe for the leaf parquet.

    Per-service routing matches the writers' actual on-disk layouts.
    First-hit-wins on the projection — caller iterates the list.
    """
    svc = service.lower()
    candidates: list[str] = []

    # Sports uses the UAC SSOT.
    if asset_group == "sports":
        from unified_api_contracts.sports import (
            candidate_parquet_uris,
        )

        league = axes.get("league_id") or ""
        data_type_value = (axes.get("data_type") or "").lower()
        if data_type_value:
            try:
                uri_list = candidate_parquet_uris(data_type=data_type_value, day=day, league_id=league or None)  # pyright: ignore[reportUnknownVariableType,reportCallIssue]
                for uri in uri_list:  # pyright: ignore[reportUnknownVariableType]
                    if isinstance(uri, str):
                        candidates.append(uri)
            except (KeyError, ValueError) as exc:
                logger.debug("sports candidate_parquet_uris miss: %s", exc)
        return candidates

    # instruments-service / corporate-actions: per-(venue, day) bundle.
    venue = axes.get("venue") or ""
    if svc in ("instruments-service", "corporate-actions") and venue:
        candidates.append(f"gs://{bucket}/by_date/day={day}/venue={venue.upper()}/instruments.parquet")  # noqa: gs-uri  — URI composer, bucket already resolved
        return candidates

    # MTDS / MDPS / features-*: per-instrument (or bundle root for
    # options_chain / futures_chain) leaf parquet. We don't enumerate
    # every (chain, instrument_type, data_type) hive partition because
    # the right path depends on the writer's version; instead we
    # construct the most common variants and let the projection
    # short-circuit on first hit.
    instrument_type = (axes.get("instrument_type") or "").lower()
    data_type_value = (axes.get("data_type") or "").lower()
    instrument_id = axes.get("instrument_id") or ""
    chain = (axes.get("chain") or "").upper()
    feature_group = (axes.get("feature_group") or "").lower()
    timeframe = (axes.get("timeframe") or "").lower()

    if data_type_value and venue:
        prefix_root = f"gs://{bucket}/raw_tick_data/by_date/day={day}"  # noqa: gs-uri  — URI composer, bucket already resolved
        venue_partition = f"venue={venue.upper()}"
        if chain:
            venue_partition = f"venue={venue.upper()}-{chain}"
        if instrument_type:
            partitions = (
                f"asset_group={asset_group}/{venue_partition}/instrument_type={instrument_type}"
                f"/data_type={data_type_value}"
            )
            if instrument_id:
                candidates.append(f"{prefix_root}/{partitions}/instrument_id={instrument_id}/{instrument_id}.parquet")
                candidates.append(f"{prefix_root}/{partitions}/{instrument_id}.parquet")
            candidates.append(f"{prefix_root}/{partitions}/")

    # features-* layout: feature_group + timeframe partitions.
    if feature_group:
        partitions = f"by_date/day={day}/feature_group={feature_group}"
        if timeframe:
            partitions += f"/timeframe={timeframe}"
        if venue:
            partitions += f"/venue={venue.upper()}"
        if instrument_id:
            candidates.append(f"gs://{bucket}/{partitions}/{instrument_id}.parquet")  # noqa: gs-uri  — URI composer, bucket already resolved
        candidates.append(f"gs://{bucket}/{partitions}/")  # noqa: gs-uri  — URI composer, bucket already resolved

    # ML / strategy / execution: experiment-keyed partitions. The
    # writers may not yet emit job_id= (Phase 1B work) so we probe
    # both with and without.
    model_family = axes.get("model_family") or ""
    training_period = axes.get("training_period") or ""
    job_id = axes.get("job_id") or ""
    strategy_id = axes.get("strategy_id") or ""
    instruction_type = (axes.get("instruction_type") or "").upper()
    if svc == "ml-service" and model_family:
        base = f"gs://{bucket}/by_date/day={day}/model_family={model_family}"  # noqa: gs-uri  — URI composer, bucket already resolved
        if training_period:
            base += f"/training_period={training_period}"
        if job_id:
            candidates.append(f"{base}/job_id={job_id}/")
        candidates.append(f"{base}/")
    if svc in ("strategy-service", "execution-service") and strategy_id:
        base = f"gs://{bucket}/by_date/day={day}/strategy_id={strategy_id}"  # noqa: gs-uri  — URI composer, bucket already resolved
        if instruction_type:
            base += f"/instruction_type={instruction_type}"
        if job_id:
            candidates.append(f"{base}/job_id={job_id}/")
        candidates.append(f"{base}/")

    return candidates


# ---------------------------------------------------------------------------
# Instruments for a given (day, venue, instrument_type, data_type) shard
# ---------------------------------------------------------------------------


# Services that bundle every instrument for a (venue, day) pair into one
# parquet. The drill-down reads the parquet itself to surface the
# instrument_ids because the file layout carries no per-symbol partitioning.

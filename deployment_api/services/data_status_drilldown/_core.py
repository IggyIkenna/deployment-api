"""Bucket naming, drill-down TTL cache + capture-status lookup.

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
from typing import cast

import pandas as pd
from unified_trading_library import (
    LEGACY_REASON_ASSET_GROUPS,
    AssetGroup,
    classify_legacy_empty_row,
)

import deployment_api.services.data_status_drilldown as _dd
from deployment_api.settings import gcp_project_id as _pid
from deployment_api.utils.bounded_cache import BoundedCache

logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Bucket naming — delegates to cloud-providers.yaml via resolve_bucket_name().
# Phase 2.6.4 delegate flip: ml-models-store / ml-predictions-store per yaml SSOT.
# ---------------------------------------------------------------------------

# Service → yaml kind mapping.
SERVICE_TO_KIND: dict[str, str] = {
    "instruments-service": "instruments-store",
    "corporate-actions": "instruments-store",
    "market-tick-data-service": "market-data",
    "market-tick-data-handler": "market-data",  # API-layer alias
    "market-data-processing-service": "market-data",
    # features FOLD A (fold_a_cutover_spec, 2026-07-18): the 5 per-family kinds
    # below all collapsed into the single folded "features" key in
    # cloud-providers.yaml (per-family split is now a key PREFIX, not a
    # separate kind) — see features_service_qg_red_asset_group_parity_stale_kind_mapping_2026_07_19.md.
    "features-delta-one-service": "features",
    "features-volatility-service": "features",
    "features-onchain-service": "features",
    "features-sports-service": "features-sports",
    "features-calendar-service": "features-calendar",
    "features-multi-timeframe-service": "features",
    "features-cross-instrument-service": "features",
    # ml-training-service + ml-inference-service consolidated into ml-service (2026-05-21).
    # ml FOLD B (bucket_fold_ml_2026_07_17.md): kind="ml-models-store" folds through UTL
    # `resolve_bucket_name` `_KIND_ALIASES` to the SINGLE `ml-store-{env}-{pid}` bucket.
    # Per-kind separation is a top-level object-key PREFIX (models/); the manifest read here
    # (`read_availability_index`) is a per-bucket ROOT index (no per-prefix index), so the
    # models/ fold's rows are discriminated by the `service_name == "ml-service"` filter —
    # predictions/ + configs/ folds are empty (no manifest rows) so there is no conflation.
    "ml-service": "ml-models-store",
    "strategy-service": "strategy-store",
    "execution-service": "execution-store",
}

# CORRECT-LOCAL: features-commodity has no yaml kind; flat template retained until
# cloud-providers.yaml gains the entry (bucket_name_ssot plan pending item).
COMMODITY_BUCKET_TEMPLATE = "features-commodity-{pid}"

# Kinds whose per-AG yaml dict omits PREDICTION — route to a flat prediction kind instead.
PREDICTION_KIND_MAP: dict[str, str] = {
    "instruments-store": "instruments-store-prediction",
    "market-data": "market-data-tick-prediction",
    "strategy-store": "strategy-store-prediction",
    "execution-store": "execution-store-prediction",
}

# Services whose cloud-providers.yaml kind-dict carries exactly ONE
# asset_group entry — bucket resolution for these is SERVICE-keyed, not
# asset_group-keyed (there is only one physical bucket regardless of which
# asset_group the caller is drilling into). Mirrors
# ``DefiStatusMixin._SERVICE_CATEGORY_RESTRICTIONS`` (data_status/defi.py) —
# features-onchain-service is DEFI-only (UAC cloud-providers.yaml CEFI key
# removed 2026-07-17, asset-group parity sweep).
SINGLE_ASSET_GROUP_SERVICES: dict[str, str] = {
    "features-onchain-service": "defi",
}


# Types that bundle many underlyings / strikes into one parquet per day.
# For these the shard unit is the underlying root (ES, NQ, BTC), not the
# individual contract — a single "instrument_id" selection downloads the
# bundle parquet for that root.
_BUNDLED_INSTRUMENT_TYPES: frozenset[str] = frozenset(
    {
        "options_chain",
        "futures_chain",
        "combo",
        # Polymarket "OTHER" bucket also bundles many conditionIds into one file
        # per day, but the UI pretends those are individual conditionIds because
        # the user selects them by their in-file instrument_id.
    }
)


# Venues/shards that bundle many symbols into one parquet (Polymarket OTHER,
# options chains, etc.) — we tag these as "per_underlying" so the UI knows
# one selection = whole bundle.
def _is_bundled(instrument_type: str) -> bool:
    return (instrument_type or "").lower() in _BUNDLED_INSTRUMENT_TYPES


def build_bucket_name(service: str, asset_group: str, project_id: str | None = None) -> str:
    """Resolve the GCS bucket for a (service, asset group) pair."""
    if service == "features-commodity-service":
        return COMMODITY_BUCKET_TEMPLATE.format(pid=project_id or _pid)
    kind = SERVICE_TO_KIND.get(service)
    if kind is None:
        raise ValueError(f"Unknown service: {service}")
    fixed_ag = SINGLE_ASSET_GROUP_SERVICES.get(service)
    ag: str | None = fixed_ag or (asset_group.lower() if asset_group else None)
    if ag == "prediction":
        pred_kind = PREDICTION_KIND_MAP.get(kind)
        if pred_kind:
            return _dd.resolve_bucket_name(cloud="gcp", kind=pred_kind)
    return _dd.resolve_bucket_name(cloud="gcp", kind=kind, asset_group=cast(AssetGroup, ag))


# ---------------------------------------------------------------------------
# TTL cache for read-heavy drill-down calls (5-min). Bounded (was unbounded,
# day-keyed — grew forever across venue/day/instrument_type/data_type shards).
# ---------------------------------------------------------------------------

_CACHE_TTL_SECONDS = 300.0
_CACHE_MAX_ENTRIES = 500
_cache = BoundedCache(name="data_status_drilldown", maxsize=_CACHE_MAX_ENTRIES, ttl_seconds=_CACHE_TTL_SECONDS)


def _cache_get(key: str) -> object | None:
    return _cache.get(key)


def _cache_put(key: str, value: object) -> None:
    _cache[key] = value


def clear_drilldown_cache() -> None:
    """Reset the TTL cache (used by tests and /turbo/clear)."""
    _cache.clear()


# ---------------------------------------------------------------------------
# Manifest capture_status lookup (Phase-C honest-coverage)
# ---------------------------------------------------------------------------

# Default capture_status for rows absent from the manifest OR legacy pre-v5
# rows that carry no capture_status column. Matches the UTL legacy-read
# coercion in ``ManifestWriter.lookup``.
_DEFAULT_CAPTURE_STATUS = "captured"


def _scoped_manifest_rows(bucket: str, venue: str, day: str) -> pd.DataFrame | None:
    """Return the (date, venue) slice of the manifest or None on any miss.

    Returns ``None`` when the manifest is unreachable, empty, missing the
    ``date`` filter column, or contains no matching rows. The calling code
    then falls back to the safe ``captured`` default.
    """
    try:
        df = _dd.read_availability_index(bucket)
    except (OSError, RuntimeError, ValueError) as exc:
        logger.warning("capture-status manifest read failed for %s: %s", bucket, exc)
        return None

    if df.empty or "date" not in df.columns:
        return None

    mask = df["date"] == day
    if "venue" in df.columns:
        mask = mask & (df["venue"] == venue)
    scoped = df.loc[mask]
    if scoped.empty or "instrument_id" not in scoped.columns:
        return None

    # Dedup by instrument_id — keep the latest written_at per key. When the
    # column is missing (very old parquet) we fall back to last-seen order.
    if "written_at" in scoped.columns:
        scoped = scoped.sort_values("written_at").drop_duplicates(subset=["instrument_id"], keep="last")
    else:
        scoped = scoped.drop_duplicates(subset=["instrument_id"], keep="last")
    return scoped


def _build_capture_metadata_lookup(scoped: pd.DataFrame) -> dict[str, dict[str, str]]:
    """Build an instrument_id -> {capture_status, error_reason, attempted_at,
    league_id, source} lookup from a deduped manifest slice.

    ``league_id`` / ``source`` are genuine v9 manifest columns (confirmed
    against the live schema — ``_V8_COLUMNS`` in UTL's
    ``manifest_writer/_read_index.py``) reused here for the SAME per-instrument
    join that already powers ``capture_status`` — no new manifest read.
    They feed the per-row ``is_mvp`` tag (P6 ``mvp_only`` filter,
    ``data_status_page_ux_and_canonicalisation_2026_07_16.md``): sports
    league-drilldown shards + source-gated MVP rules need these two REAL axes.
    ``base_asset`` / ``market_group`` are NOT manifest columns (verified against
    the live parquet schema for cefi/tradfi/prediction — 42/41 columns, neither
    present) so they are deliberately NOT read here; inventing them would be a
    fabricated value.
    """
    by_iid: dict[str, dict[str, str]] = {}
    for row in scoped.to_dict(orient="records"):  # pyright: ignore[reportUnknownMemberType,reportUnknownVariableType]
        iid = str(row.get("instrument_id") or "").strip()  # pyright: ignore[reportUnknownMemberType,reportUnknownArgumentType]
        if not iid:
            continue
        by_iid[iid] = {
            "capture_status": str(
                row.get("capture_status") or _DEFAULT_CAPTURE_STATUS  # pyright: ignore[reportUnknownMemberType,reportUnknownArgumentType]
            ).lower(),
            "error_reason": str(row.get("error_reason") or ""),  # pyright: ignore[reportUnknownMemberType,reportUnknownArgumentType]
            "attempted_at": str(row.get("attempted_at") or ""),  # pyright: ignore[reportUnknownMemberType,reportUnknownArgumentType]
            "league_id": str(row.get("league_id") or ""),  # pyright: ignore[reportUnknownMemberType,reportUnknownArgumentType]
            "source": str(row.get("source") or ""),  # pyright: ignore[reportUnknownMemberType,reportUnknownArgumentType]
        }
    return by_iid


def _attach_capture_status_to_instruments(
    instruments: list[dict[str, object]],
    *,
    bucket: str,
    venue: str,
    day: str,
) -> None:
    """Mutate each instrument dict in-place to add manifest capture metadata.

    Reads ``gs://<bucket>/_index/availability_index.parquet``, filters to
    ``(date == day, venue == venue)``, then joins each instrument to its
    manifest row by ``instrument_id``. Adds five keys to every instrument:
    - ``capture_status``:  "captured" | "empty_confirmed" | "attempted_failed"
    - ``error_reason``:    classified error string (empty for captured/empty)
    - ``attempted_at``:    ISO-8601 UTC timestamp (empty for legacy rows)
    - ``league_id``:       manifest league axis (sports; "" elsewhere)
    - ``source``:          manifest data-source tag ("" single-source/legacy)

    ``league_id`` / ``source`` are genuine v9 manifest columns reused for the
    per-row ``is_mvp`` tag (P6 ``mvp_only`` filter). They use ``setdefault`` so
    a value already stamped on the instrument dict upstream (e.g. a per-row
    column read straight from an instruments-service bundle parquet) is never
    clobbered by this manifest join.

    When multiple shards match a tuple (re-runs, re-tries), the row with the
    latest ``written_at`` wins — same dedup semantics as UTL
    ``_merge_dataframes``.

    Failure to read the manifest is non-fatal: every instrument defaults to
    ``capture_status="captured"`` + empty error/attempted_at/league_id/source
    so the drill-down stays usable even when the manifest is unreachable.
    """
    if not instruments:
        return
    scoped = _scoped_manifest_rows(bucket, venue, day)
    if scoped is None:
        _apply_default_capture_status(instruments)
        return
    by_iid = _build_capture_metadata_lookup(scoped)
    for inst in instruments:
        iid = str(inst.get("instrument_id") or "").strip()
        meta = by_iid.get(iid)
        if meta is None:
            inst["capture_status"] = _DEFAULT_CAPTURE_STATUS
            inst["error_reason"] = ""
            inst["attempted_at"] = ""
            inst.setdefault("league_id", "")
            inst.setdefault("source", "")
        else:
            inst["capture_status"] = meta["capture_status"]
            inst["error_reason"] = meta["error_reason"]
            inst["attempted_at"] = meta["attempted_at"]
            inst.setdefault("league_id", meta["league_id"])
            inst.setdefault("source", meta["source"])


def _apply_default_capture_status(instruments: list[dict[str, object]]) -> None:
    """Stamp the safe default (captured / no error / no attempted_at) on every row."""
    for inst in instruments:
        inst.setdefault("capture_status", _DEFAULT_CAPTURE_STATUS)
        inst.setdefault("error_reason", "")
        inst.setdefault("attempted_at", "")
        inst.setdefault("league_id", "")
        inst.setdefault("source", "")


# Capture-status branches surfaced by the multi-axis drilldown / download
# endpoints. The download UX in deployment-ui DataStatusDrilldown branches
# on the ``X-Capture-Status`` response header into the matching empty-state
# panel:
#
# - ``captured`` — manifest claims rows; parquet read returned >0 rows;
#   today's happy path (HTTP 200 + CSV body).
# - ``empty_confirmed`` — adapter ran and the source legitimately returned
#   zero rows (paused league day, pre-source-launch, dormant prediction
#   market). HTTP 200 + header-only body explaining the honest empty.
# - ``attempted_failed`` — adapter ran and raised; ``error_reason``
#   carries the classified failure. HTTP 200 + header-only body.
# - ``never_attempted`` — manifest has no row at all for the requested
#   axis-tuple; the adapter never ran. HTTP 404.
# - ``path_drift`` — manifest says ``captured`` but the downloader probed
#   the parquet path and got 0 rows. Real bug class: writer/downloader
#   path-template drift, chain-bundle / instrument_type / hive-vocab
#   axis drift, or a per-league/per-job_id sub-partition the downloader
#   doesn't walk into. HTTP 502 — the operator needs to know the data
#   the manifest claims is in fact unreachable.
_CAPTURE_STATUS_HEADER = "X-Capture-Status"

_AXIS_KWARGS: tuple[str, ...] = (
    "venue",
    "instrument_type",
    "data_type",
    "instrument_id",
    "chain",
    "league_id",
    "fixture_id",
    "canonical_question_group",
    "job_id",
    "model_family",
    "training_period",
    "strategy_id",
    "instruction_type",
    "feature_group",
    "timeframe",
)


def lookup_capture_status_for_shard(
    *,
    service: str,
    asset_group: str,
    day: str,
    **axes: str | None,
) -> dict[str, str]:
    """Look up the manifest ``capture_status`` for an axis-tuple shard.

    Reads the (service, asset_group) availability index, filters to the
    requested ``day`` AND every populated axis kwarg, and returns the
    most recently written row's capture metadata. When no row matches,
    returns ``{"status": "never_attempted", ...}`` so the caller can
    branch — the adapter never ran for this shard, distinct from
    "ran and got nothing".

    Args:
        service: Service name (e.g. ``instruments-service``,
            ``market-tick-data-service``, ``ml-service``).
        asset_group: Asset group bucket key (cefi/tradfi/defi/sports/
            prediction/shared).
        day: ISO date (``YYYY-MM-DD``) to filter on.
        **axes: The v7 shard axes (venue, chain, league_id,
            fixture_id, canonical_question_group, job_id,
            model_family, training_period, strategy_id,
            instruction_type, feature_group, timeframe, instrument_id,
            instrument_type, data_type). Each axis kwarg is applied as
            an equality filter when the manifest carries that column;
            missing columns are silently skipped (no narrowing) so the
            lookup degrades gracefully against partially-migrated
            v4/v5/v6 manifests.

    Returns:
        Dict with keys ``status`` (one of ``captured``,
        ``empty_confirmed``, ``attempted_failed``, ``never_attempted``),
        ``error_reason`` (classified error string or ``""``),
        ``attempted_at`` (ISO-8601 UTC timestamp or ``""``),
        ``written_at`` (manifest-write time or ``""``).

    Plan: data_status_multi_axis_shard_propagation_2026_05_06.md
    Phase 3 (drilldown polish — distinguish empty_confirmed from
    path-drift zero bytes in the download UX).
    """
    try:
        bucket = _dd.build_bucket_name(service, asset_group)
    except ValueError:
        return {
            "status": "never_attempted",
            "error_reason": "",
            "attempted_at": "",
            "written_at": "",
        }
    try:
        index = _dd.read_availability_index(bucket)
    except (OSError, RuntimeError, ValueError) as exc:
        logger.warning("lookup_capture_status_for_shard manifest read failed for %s: %s", bucket, exc)
        return {
            "status": "never_attempted",
            "error_reason": "",
            "attempted_at": "",
            "written_at": "",
        }
    if index.empty or "date" not in index.columns:
        return {
            "status": "never_attempted",
            "error_reason": "",
            "attempted_at": "",
            "written_at": "",
        }
    mask = index["date"].astype(str) == day
    if "service_name" in index.columns:
        mask = mask & (index["service_name"].astype(str) == service)
    for col in _AXIS_KWARGS:
        val = axes.get(col)
        if not val or col not in index.columns:
            continue
        normalised = val.upper() if col in ("venue", "chain", "instruction_type") else val
        mask = mask & (index[col].fillna("").astype(str) == normalised)
    matched = index.loc[mask]
    if matched.empty:
        return {
            "status": "never_attempted",
            "error_reason": "",
            "attempted_at": "",
            "written_at": "",
        }
    if "written_at" in matched.columns:
        matched = matched.sort_values("written_at", ascending=False)
    row = matched.iloc[0]
    raw_status = row.get("capture_status") if "capture_status" in matched.columns else None
    status = (
        str(raw_status).lower()  # type: ignore[reportAny]
        if raw_status is not None and not pd.isna(raw_status) and str(raw_status)  # type: ignore[reportAny]
        else _DEFAULT_CAPTURE_STATUS
    )
    if status not in ("captured", "empty_confirmed", "attempted_failed", "expected_unattempted"):
        status = _DEFAULT_CAPTURE_STATUS
    error_reason = _classify_lookup_legacy_empty_reason(
        status=status,
        error_reason=str(row.get("error_reason") or ""),
        row=row,
        asset_group=asset_group,
        bucket=bucket,
    )
    return {
        "status": status,
        "error_reason": error_reason,
        "attempted_at": str(row.get("attempted_at") or ""),
        "written_at": str(row.get("written_at") or ""),
    }


def _classify_lookup_legacy_empty_reason(
    *,
    status: str,
    error_reason: str,
    row: pd.Series[object],  # type: ignore
    asset_group: str,
    bucket: str,
) -> str:
    """Reader-side fallback for legacy ``empty_confirmed`` lookup rows.

    Returns ``error_reason`` unchanged unless the row is a pre-Phase-2.E.2
    legacy row (status=empty_confirmed AND error_reason empty). In that
    case, classify on the fly via the UTL helper — same SSOT the
    Tier 3D.1 reconciler uses for batch back-fill (writegate Tier 3D.2).
    """
    if error_reason or status != "empty_confirmed":
        return error_reason
    ag_lower = asset_group.lower()
    if ag_lower not in LEGACY_REASON_ASSET_GROUPS:
        return error_reason
    # Convert the pandas Series to a plain dict — UTL's classifier
    # accepts ``Mapping[str, object]`` and pandas Series fails the
    # nominal type check even though it implements ``.get()``.
    row_dict: dict[str, object] = {str(k): v for k, v in row.items()}
    try:
        return classify_legacy_empty_row(ag_lower, row_dict)
    except (ValueError, TypeError, KeyError, AttributeError) as exc:
        logger.warning(
            "classify_legacy_empty_row failed for %s in %s: %s",
            ag_lower,
            bucket,
            exc,
        )
        return error_reason


# ---------------------------------------------------------------------------
# Schema lookup
# ---------------------------------------------------------------------------

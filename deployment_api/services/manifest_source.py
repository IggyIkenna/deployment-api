"""Manifest-index source selection — live consolidated index vs the CF-20 BETA preview.

The data-status surfaces (status tab / hierarchical / drilldowns / shard detail / CSV
export) all read the availability ``_index`` of a market-data bucket. By default that is
the LIVE consolidated index via UTL ``read_availability_index``. When
``DATA_STATUS_BETA_MANIFEST_BLOB`` is set, every read is redirected to the PROJECTED
post-migration index that the per-AG rebuild ``--dry-run --beta-manifest-out`` runs wrote
(``_index/audit/projected_index_<ag>.parquet``) — so the operator can eyeball the
post-migration goalposts in the SAME UI, with zero prod writes, before any G4 ``--apply``.

Recipe (CF-20/V5, plan ``migration_verification_orphan_safety_2026_06_10.md``)::

    DATA_STATUS_BETA_MANIFEST_BLOB="_index/audit/projected_index_{asset_group}.parquet" \
        bash unified-trading-pm/scripts/dev/restart-deployment-stack.sh --api

Unset the variable (default "") → the live index, unchanged behaviour. The blob template
is formatted with ``{asset_group}`` derived from the bucket name; it always reads from
the SAME bucket (no copying, no dev-bucket provisioning).
"""

from __future__ import annotations

import io
import logging
import time

import pandas as pd
from unified_trading_library import (
    get_storage_client,
    read_availability_index,
)

from deployment_api.settings import DATA_STATUS_BETA_MANIFEST_BLOB

logger = logging.getLogger(__name__)

# Beta-index TTL cache (bucket -> (monotonic_ts, DataFrame)). The LIVE path caches inside
# UTL ``read_availability_index``; the beta path used to re-download the projected parquet on
# EVERY per-category read (~7s of redundant GCS fetches per all-asset-group query, and it made
# the startup pre-warm useless — the warmed blob was re-fetched on the next request). Mirror
# the live 5-min TTL so a warmed index is reused (operator slowness fix, 2026-06-13).
_BETA_INDEX_CACHE: dict[str, tuple[float, pd.DataFrame]] = {}
_BETA_INDEX_TTL_SECONDS = 300.0

# The canonical consolidated availability index blob — read directly (no live freshness
# gate) as the stale-tolerant fallback for the data-status MONITORING view (see
# read_manifest_index docstring + plan proper_instrument_catalogue_lifecycle_rollup §R5).
_CONSOLIDATED_INDEX_BLOB = "_index/availability_index.parquet"

# Bucket tag → canonical asset_group (the env-tiered bucket names abbreviate prediction).
_TAG_TO_ASSET_GROUP: dict[str, str] = {
    "pred": "prediction",
    "prediction": "prediction",
    "cefi": "cefi",
    "defi": "defi",
    "tradfi": "tradfi",
    "sports": "sports",
}


def _asset_group_from_bucket(bucket: str) -> str:
    """Derive the asset_group token from an env-tiered bucket name
    (``market-data-tick-<tag>-<env>-<project>`` / ``instruments-store-<tag>-…``)."""
    for prefix in ("market-data-tick-", "instruments-store-"):
        if bucket.startswith(prefix):
            tag = bucket.removeprefix(prefix).split("-", 1)[0]
            return _TAG_TO_ASSET_GROUP.get(tag, tag)
    return ""


def read_manifest_index(bucket: str) -> pd.DataFrame:
    """The availability ``_index`` for ``bucket`` — live consolidated by default; the
    CF-20 projected (beta) index when ``DATA_STATUS_BETA_MANIFEST_BLOB`` is set.

    Beta mode FAILS LOUD on a missing projection (no silent fallback to the live index —
    a beta render quietly showing live data would defeat the whole pre-apply eyeball).

    STALE-TOLERANT MONITORING READ (2026-06-15): the data-status is a MONITORING view, not
    a live-trading read. UTL ``read_availability_index`` applies a ~120 s live staleness
    gate — when the consolidated ``_index/availability_index.parquet`` is older than that
    AND the per-VM shard dir holds only seed shards, it drops the (valid) consolidated index
    and returns ~empty. Correct for trading; but for data-status it shows 0 instruments when
    a stale-but-valid catalogue exists — the failure the operator saw while the manifest
    consolidators are paused by the held pre-migration drain (plan
    ``proper_instrument_catalogue_lifecycle_rollup_2026_06_04`` §R5). So when the live read
    yields empty (or raises) we read the consolidated blob DIRECTLY (no freshness gate) and
    surface the stale rows + their ``written_at`` age rather than 0. Live trading readers do
    not call this path — their 120 s gate is unchanged."""
    if not DATA_STATUS_BETA_MANIFEST_BLOB:
        try:
            live = read_availability_index(bucket)
        except Exception as _live_err:  # monitoring read degrades to stale, never hard-fails the UI
            logger.warning(
                "data-status: live read_availability_index(%s) raised %s — falling back to the consolidated blob",
                bucket,
                type(_live_err).__name__,
            )
            live = None
        if live is not None and not live.empty:
            return live
        # Stale-tolerant fallback: the live gate dropped a stale-but-valid consolidated
        # index (consolidator paused). Read it directly for the monitoring view.
        try:
            raw = get_storage_client().download_bytes(bucket, _CONSOLIDATED_INDEX_BLOB)
            stale = pd.read_parquet(io.BytesIO(raw))
            if not stale.empty:
                logger.warning(
                    "data-status STALE-TOLERANT read: %s live index empty; using %s directly "
                    "(%d rows — consolidator paused/stale, monitoring view)",
                    bucket,
                    _CONSOLIDATED_INDEX_BLOB,
                    len(stale),
                )
                return stale
        except Exception as _stale_err:  # genuinely-empty bucket -> empty df, not an error
            logger.debug(
                "data-status stale-tolerant read: no consolidated blob for %s (%s)",
                bucket,
                type(_stale_err).__name__,
            )
        return live if live is not None else pd.DataFrame()
    cached = _BETA_INDEX_CACHE.get(bucket)
    if cached is not None and time.monotonic() - cached[0] < _BETA_INDEX_TTL_SECONDS:
        return cached[1]
    asset_group = _asset_group_from_bucket(bucket)
    blob_name = DATA_STATUS_BETA_MANIFEST_BLOB.format(asset_group=asset_group)
    logger.info("BETA manifest mode: reading gs://%s/%s (asset_group=%s)", bucket, blob_name, asset_group)
    # Fail LOUD on a missing projected index (honest absence — see
    # test_beta_mode_fails_loud_on_missing_projection): if a beta query targets a
    # bucket with no v9 projection, surface it rather than silently rendering empty.
    # The rollup worker only sweeps beta-eligible services in beta mode (see
    # ``BETA_ELIGIBLE_SERVICES`` in data_status_rollup_worker), so it never hits this.
    raw = get_storage_client().download_bytes(bucket, blob_name)
    df = pd.read_parquet(io.BytesIO(raw))
    _BETA_INDEX_CACHE[bucket] = (time.monotonic(), df)
    return df


def is_beta_mode() -> bool:
    """True when the CF-20 beta-manifest preview is active (every data-status read
    redirected to the projected indexes) — precomputed LIVE rollup fast-paths must
    be bypassed in this mode or the beta render quietly shows live data."""
    return bool(DATA_STATUS_BETA_MANIFEST_BLOB)


# Services with a CF-20 v9 PROJECTED index (so a beta rollup is meaningful). Both the
# instruments-store AGs AND the market-data-tick AGs are now projected — every asset_group
# has ``_index/audit/projected_index_<ag>.parquet`` (defi/cefi/tradfi/sports/prediction) —
# so both ``instruments-service`` and ``market-tick-data-service`` read the projected v9
# index in beta mode. The beta index read FAILS LOUD on a missing projection (honest
# absence), so a service is added here ONLY once EVERY asset_group it serves has a
# projection; a still-unprojected service (features/strategy/…) has NO beta rollup and
# keeps its LIVE rollup. Beta is therefore a PER-SERVICE concept, NOT a global one (see
# ``is_service_beta``). The two-phase rollup worker writes each eligible service's
# ``.beta`` rollup in phase 2, so an eligible service's beta read always finds its blob
# (no 503). SSOT lives here (the beta module); the rollup worker + the rollup blob-path
# resolver + the in-service endpoint all import it.
BETA_ELIGIBLE_SERVICES: frozenset[str] = frozenset({"instruments-service", "market-tick-data-service"})


def beta_eligible(services: list[str]) -> list[str]:
    """Keep only services with a v9 projected index (``BETA_ELIGIBLE_SERVICES``).

    Used when computing the beta rollup so the worker never sweeps a non-projected
    service whose loud-failing beta read would crash the run.
    """
    return [s for s in services if s in BETA_ELIGIBLE_SERVICES]


def is_service_beta(service: str) -> bool:
    """True when THIS specific service should read/write the beta-namespaced rollup.

    Beta is PER-SERVICE, not global: only a beta-eligible service has a v9 projection.
    A non-eligible service (mtds/features/…) keeps its LIVE rollup (``{service}/full.json.gz``)
    even while the deployment is in beta-preview mode — otherwise its beta blob never
    exists, the manifest read finds nothing, and it falls through to a multi-minute all-
    asset-group live compute on the 8 GiB service → HTTP 503 (the failure the operator hit
    on the market-tick-data-service data-status page, 2026-06-16). Only the eligible
    service(s) flip to the projected-v9 view in beta mode."""
    return is_beta_mode() and service in BETA_ELIGIBLE_SERVICES


_UNIQUE_COUNT_CACHE: dict[str, int] = {}


def read_unique_instrument_count(asset_group: str, *, cloud: str = "gcp") -> int | None:
    """Deduplicated instrument-identity count for an asset_group, from the lifecycle
    catalogue (``prod/catalog.parquet`` — one row per instrument with
    available_from/to; the ONLY identity-level source: the availability ``_index``
    carries per-shard COUNTS, so summing it over days multi-counts).

    Cached per-process (the catalogue changes daily at most). ``None`` when the
    catalogue is missing/unreadable — callers surface ``null``, never a fake 0."""
    from unified_trading_library import resolve_bucket_name  # noqa: qg-inside-import

    ag = asset_group.lower()
    if ag in _UNIQUE_COUNT_CACHE:
        return _UNIQUE_COUNT_CACHE[ag]
    try:
        # Prediction's bucket is its own KIND in cloud-providers.yaml (no asset_group
        # entry under instruments-store) — same mapping as build_instrument_catalogue.
        if ag == "prediction":
            bucket = resolve_bucket_name(cloud=cloud, kind="instruments-store-prediction")
        else:
            bucket = resolve_bucket_name(cloud=cloud, kind="instruments-store", asset_group=ag)
        raw = get_storage_client().download_bytes(bucket, "prod/catalog.parquet")
        df = pd.read_parquet(io.BytesIO(raw), columns=["instrument_id"])
        count = int(df["instrument_id"].nunique())
    except Exception as exc:
        logger.warning("unique-instrument catalogue read failed for %s (%s) — returning None", ag, exc)
        return None
    _UNIQUE_COUNT_CACHE[ag] = count
    return count

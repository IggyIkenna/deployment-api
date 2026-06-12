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

import pandas as pd
from unified_trading_library import (
    get_storage_client,
    read_availability_index,
)

from deployment_api.settings import DATA_STATUS_BETA_MANIFEST_BLOB

logger = logging.getLogger(__name__)

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
    a beta render quietly showing live data would defeat the whole pre-apply eyeball)."""
    if not DATA_STATUS_BETA_MANIFEST_BLOB:
        return read_availability_index(bucket)
    asset_group = _asset_group_from_bucket(bucket)
    blob_name = DATA_STATUS_BETA_MANIFEST_BLOB.format(asset_group=asset_group)
    logger.info("BETA manifest mode: reading gs://%s/%s (asset_group=%s)", bucket, blob_name, asset_group)
    raw = get_storage_client().download_bytes(bucket, blob_name)
    return pd.read_parquet(io.BytesIO(raw))

def is_beta_mode() -> bool:
    """True when the CF-20 beta-manifest preview is active (every data-status read
    redirected to the projected indexes) — precomputed LIVE rollup fast-paths must
    be bypassed in this mode or the beta render quietly shows live data."""
    return bool(DATA_STATUS_BETA_MANIFEST_BLOB)


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

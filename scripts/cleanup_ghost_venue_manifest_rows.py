"""One-shot script: strip ghost DeFi venue rows from all MTDS DeFi bucket manifest indexes.

Ghost venues are Era-2 capabilities-era names (no underscore, no chain suffix) that have been
superseded by canonical underscore names (e.g. AAVE_V3 → AAVE_V3). They appear in the manifest
index as both bare form (AAVE_V3) and hyphenated-chain form (AAVE_V3-ETHEREUM).

This script cleans TWO layers:
  1. Per-VM shard parquets under _index/per_vm/ — these are the SOURCE; the consolidator
     merges them every minute, so leaving ghost rows here means they come back immediately.
  2. Consolidated _index/availability_index.parquet — current merged view.
  3. Rollup blob — so /manifest API regenerates clean on next call.

Run with workspace .venv-workspace (which has UTL + UAC installed):
    source ${WORKSPACE_ROOT}/.venv-workspace/bin/activate
    CLOUD_PROVIDER=gcp python3 deployment-api/scripts/cleanup_ghost_venue_manifest_rows.py
"""

from __future__ import annotations

import io
import logging
import sys

import pandas as pd

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
logger = logging.getLogger(__name__)

PROJECT_ID = "central-element-323112"
INDEX_BLOB = "_index/availability_index.parquet"
PER_VM_PREFIX = "_index/per_vm/"
ROLLUP_BUCKET = f"{PROJECT_ID}-data-status-rollups"
ROLLUP_BLOB = "market-tick-data-service/full.json.gz"

DEFI_MTDS_BUCKETS: list[str] = [
    f"gas-fees-{PROJECT_ID}",
    f"evm-defi-{PROJECT_ID}",
    f"solana-defi-{PROJECT_ID}",
    f"dex-pools-{PROJECT_ID}",
    f"dex-swaps-{PROJECT_ID}",
    f"lending-indices-{PROJECT_ID}",
    f"liquidations-{PROJECT_ID}",
    f"lst-rates-{PROJECT_ID}",
    f"oracle-prices-{PROJECT_ID}",
    f"perp-funding-{PROJECT_ID}",
]


def _clean_parquet_blob(
    client: object,
    bucket: str,
    blob_name: str,
    ghost_names: frozenset[str],
    dry_run: bool,
) -> int:
    """Download, filter, re-upload a single parquet blob. Returns rows dropped."""
    from unified_trading_library import upload_to_storage

    raw = client.download_bytes(bucket, blob_name)  # type: ignore[attr-defined]
    df = pd.read_parquet(io.BytesIO(raw))
    if "venue" not in df.columns:
        return 0
    venue_prefix = df["venue"].str.split("-", n=1).str[0]
    ghost_mask = venue_prefix.isin(ghost_names)
    n_ghost = int(ghost_mask.sum())
    if n_ghost == 0:
        return 0
    ghost_venues = sorted(df.loc[ghost_mask, "venue"].unique().tolist())
    logger.info(
        "  DROP %s — %d ghost rows (%s) from %d total",
        blob_name,
        n_ghost,
        ghost_venues,
        len(df),
    )
    if not dry_run:
        clean_df = df[~ghost_mask].reset_index(drop=True)
        buf = io.BytesIO()
        clean_df.to_parquet(buf, index=False)
        upload_to_storage(bucket, blob_name, buf.getvalue(), "application/octet-stream")
        logger.info("  WROTE %s — %d rows remaining", blob_name, len(clean_df))
    return n_ghost


def main(dry_run: bool = False) -> None:
    from unified_api_contracts import DEPRECATED_DEFI_GHOST_VENUE_NAMES
    from unified_trading_library import get_storage_client, storage_exists
    from unified_trading_library.cloud_interface.gcs_blob_ops import gcs_delete_object  # noqa: qg-deep-import

    client = get_storage_client()
    total_dropped = 0

    for bucket in DEFI_MTDS_BUCKETS:
        logger.info("=== %s ===", bucket)

        # Step 1: clean per-VM shard parquets (the SOURCE — consolidator reads these every minute)
        per_vm_blobs = list(client.list_blobs(bucket, prefix=PER_VM_PREFIX))
        shard_dropped = 0
        for blob in per_vm_blobs:
            try:
                dropped = _clean_parquet_blob(
                    client, bucket, blob.name, DEPRECATED_DEFI_GHOST_VENUE_NAMES, dry_run
                )
                shard_dropped += dropped
            except Exception as exc:
                logger.warning("  ERROR %s: %s", blob.name, exc)
        if per_vm_blobs:
            logger.info(
                "  per-vm: %d shards checked, %d ghost rows %s",
                len(per_vm_blobs),
                shard_dropped,
                "would be dropped" if dry_run else "dropped",
            )
        total_dropped += shard_dropped

        # Step 2: clean consolidated index
        if not storage_exists(bucket, INDEX_BLOB):
            logger.info("  SKIP consolidated index — blob not found")
            continue
        try:
            dropped = _clean_parquet_blob(
                client, bucket, INDEX_BLOB, DEPRECATED_DEFI_GHOST_VENUE_NAMES, dry_run
            )
            if dropped == 0:
                logger.info("  consolidated index — already clean")
            total_dropped += dropped
        except Exception as exc:
            logger.warning("  ERROR consolidated index: %s", exc)

    # Step 3: invalidate the rollup blob so /manifest API regenerates clean
    rollup_uri = f"gs://{ROLLUP_BUCKET}/{ROLLUP_BLOB}"
    if storage_exists(ROLLUP_BUCKET, ROLLUP_BLOB):
        if not dry_run:
            gcs_delete_object(rollup_uri)
            logger.info("DELETED rollup blob %s", rollup_uri)
        else:
            logger.info("DRY-RUN: would delete rollup blob %s", rollup_uri)
    else:
        logger.info("rollup blob %s — already gone", rollup_uri)

    logger.info(
        "Done. Total ghost rows %s: %d across %d buckets.",
        "that would be dropped" if dry_run else "dropped",
        total_dropped,
        len(DEFI_MTDS_BUCKETS),
    )


if __name__ == "__main__":
    dry_run = "--dry-run" in sys.argv
    if dry_run:
        logger.info("DRY-RUN mode — no writes will happen")
    main(dry_run=dry_run)

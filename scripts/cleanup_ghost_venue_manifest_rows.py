"""One-shot script: strip ghost DeFi venue rows from all MTDS DeFi bucket manifest indexes.

Ghost venues are Era-2 capabilities-era names (no underscore, no chain suffix) that have been
superseded by canonical underscore names (e.g. AAVEV3 → AAVE_V3). They appear in the manifest
index as both bare form (AAVEV3) and hyphenated-chain form (AAVEV3-ETHEREUM).

After running this script:
  - All 10 DeFi MTDS bucket availability_index.parquet files are free of ghost rows.
  - The rollup blob is deleted so the /manifest API regenerates clean on next call.

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


def main(dry_run: bool = False) -> None:
    from unified_api_contracts import DEPRECATED_DEFI_GHOST_VENUE_NAMES
    from unified_trading_library.cloud_interface.factory import (
        download_from_storage,
        storage_exists,
        upload_to_storage,
    )
    from unified_trading_library.cloud_interface.gcs_blob_ops import gcs_delete_object

    total_dropped = 0

    for bucket in DEFI_MTDS_BUCKETS:
        if not storage_exists(bucket, INDEX_BLOB):
            logger.info("SKIP %s — index blob not found", bucket)
            continue

        raw = download_from_storage(bucket, INDEX_BLOB)
        df = pd.read_parquet(io.BytesIO(raw))
        original_len = len(df)

        if "venue" not in df.columns:
            logger.info("SKIP %s — no venue column (%d rows)", bucket, original_len)
            continue

        # Match both bare form (AAVEV3) and hyphenated-chain form (AAVEV3-ETHEREUM)
        venue_prefix = df["venue"].str.split("-", n=1).str[0]
        ghost_mask = venue_prefix.isin(DEPRECATED_DEFI_GHOST_VENUE_NAMES)
        n_ghost = int(ghost_mask.sum())

        if n_ghost == 0:
            logger.info("OK   %s — 0 ghost rows (%d total)", bucket, original_len)
            continue

        ghost_venues = sorted(df.loc[ghost_mask, "venue"].unique().tolist())
        logger.info(
            "DROP %s — removing %d ghost rows (venues: %s) from %d total",
            bucket,
            n_ghost,
            ghost_venues,
            original_len,
        )

        if not dry_run:
            clean_df = df[~ghost_mask].reset_index(drop=True)
            buf = io.BytesIO()
            clean_df.to_parquet(buf, index=False)
            upload_to_storage(bucket, INDEX_BLOB, buf.getvalue(), "application/octet-stream")
            logger.info("WROTE %s — %d rows remaining", bucket, len(clean_df))

        total_dropped += n_ghost

    # Invalidate the rollup blob so the /manifest API regenerates clean
    rollup_uri = f"gs://{ROLLUP_BUCKET}/{ROLLUP_BLOB}"
    if storage_exists(ROLLUP_BUCKET, ROLLUP_BLOB):
        if not dry_run:
            gcs_delete_object(rollup_uri)
            logger.info("DELETED rollup blob %s", rollup_uri)
        else:
            logger.info("DRY-RUN: would delete rollup blob %s", rollup_uri)
    else:
        logger.info("rollup blob %s not found (already gone or never written)", rollup_uri)

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

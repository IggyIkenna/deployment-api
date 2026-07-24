"""Read-only manifest data_type census — sanctioned axis-value-census logic invoked
directly as a Python function call (no HTTP server, no writes).

# Lifecycle: reusable verification tool, not a one-off.
# Delete-when: after fixtures_manifest_legacy_backfill_2026_07_24.md's backfill todo
#   and its >=2-consolidator-cycle verification both close (issue doc + plan todo in
#   unified-trading-pm/plans/active/issues/fixtures_manifest_legacy_backfill_2026_07_24.md).

Does a single bounded, column-pruned read of the consolidated availability_index.parquet
via the same `read_availability_index()` reader every data-status endpoint uses — this is
the single-walk-exempt "G1 in-session census" pattern documented in
codex/02-data/reconciliation-census-and-compute-tiers.md §1.1. Mirrors
deployment_api/routes/data_status/_axis_census.py's `get_axis_value_census` without needing
a running HTTP server.

Usage (from this repo's venv, with GCP_PROJECT_ID set):
    GCP_PROJECT_ID=central-element-323112 uv run --frozen python3 \
        scripts/census_manifest_data_type_2026_07_24.py \
        --service instruments-service --asset-group sports \
        --filter-prefix odds_horizon_bucket --filter-prefix FIXTURES

Origin: written 2026-07-24 while verifying two sports_closeout_batch1_ao_ready_2026_07_24.md
todos (the odds_horizon_bucket legacy-atom purge, already-done; the FIXTURES manifest atom
backfill, still open) — promoted from a scratchpad one-off since the FIXTURES backfill's
own Done-when clause requires re-running this exact census after the restamp AND after >=2
manifest-consolidator cycles.
"""

from __future__ import annotations

import argparse

from unified_trading_library import read_availability_index

from deployment_api.services.data_status_drilldown import build_bucket_name

_BLANK_SENTINELS = frozenset({"", "none", "nan", "<na>"})


def _axis_value_counts(df, column: str) -> list[dict[str, object]]:
    if df.empty or column not in df.columns:
        return []
    series = df[column].astype(str).str.strip()
    series = series[~series.str.lower().isin(_BLANK_SENTINELS)]
    if series.empty:
        return []
    counts = series.value_counts()
    return [{"value": str(v), "count": int(c)} for v, c in counts.items()]


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--service", required=True, help='e.g. "instruments-service"')
    parser.add_argument("--asset-group", required=True, help='e.g. "sports"')
    parser.add_argument(
        "--filter-prefix",
        action="append",
        default=[],
        help="Only print data_type values starting with this prefix (repeatable). Omit to print all.",
    )
    args = parser.parse_args()

    bucket = build_bucket_name(args.service, args.asset_group)
    print(f"bucket = {bucket}")
    df = read_availability_index(bucket, columns=["data_type"])
    print(f"row_count = {len(df)}")

    values = _axis_value_counts(df, "data_type")
    if args.filter_prefix:
        values = [v for v in values if any(str(v["value"]).startswith(p) for p in args.filter_prefix)]
    print(f"\n=== axis: data_type ({len(values)} matching distinct values) ===")
    for entry in values:
        print(f"  {entry['value']!r}: {entry['count']}")


if __name__ == "__main__":
    main()

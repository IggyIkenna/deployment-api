"""SQL builders for the two billing exports.

Dates are computed server-side from an int day-count and `datetime.now(UTC).date()`, then inlined
as literals — there is no user-supplied string in these queries, so no injection surface.
Keeping them as literals (not bind params) also keeps the UTL `execute_query` call uniform
across BigQuery and Athena, which take parameters differently.
"""

from __future__ import annotations

from datetime import date, timedelta

# GCP: the resource-level export is ingestion-time partitioned on _PARTITIONTIME, NOT on
# usage_start_time — so we MUST filter _PARTITIONTIME too or BigQuery scans every partition
# (~4x bytes; verified). Rows can land a couple days after usage, so pad the partition floor.
_PARTITION_PAD_DAYS = 3

# `system_labels` keys carrying the VM machine spec — present on the instance's Core/Ram SKU
# rows only; disk, IP, and other per-resource SKU rows for the same VM have no machine-spec
# labels (verified live), hence ANY_VALUE picks whichever of a group's rows has it. No Compute
# API call.
_MACHINE_SPEC_LABELS = (
    ("machine_spec", "compute.googleapis.com/machine_spec"),
    ("machine_cores", "compute.googleapis.com/cores"),
    ("machine_memory_mib", "compute.googleapis.com/memory"),
)


def _system_label_col(alias: str, key: str) -> str:
    # nosec B608 — alias/key come only from the hardcoded _MACHINE_SPEC_LABELS tuple, no user input
    return f"  ANY_VALUE((SELECT value FROM UNNEST(system_labels)\n    WHERE key = '{key}' LIMIT 1)) AS {alias}"  # nosec B608


def gcp_facts_sql(table: str, start: date, end: date) -> str:
    """Per (day, service, resource, region, sku, zone) cost + credit + usage for the GCP export.

    `cost`/`credit` are converted to USD AT QUERY TIME. The export bills in the account currency
    (this account = GBP, verified via the export's `currency` column) and carries
    `currency_conversion_rate` — the USD→account-currency rate GCP billed at — so `amount / rate`
    is the USD-equivalent list price. Dividing PER SOURCE ROW (inside SUM) applies each day's exact
    rate. A USD account has rate=1.0 (no-op); a missing/zero rate falls back to 1.0 (leave the row
    unconverted rather than drop it via a NULL divisor). This makes the whole page single-currency
    USD, matching the AWS CUR (native USD). Native-GBP figures for the GCP invoice tally are a
    separate concern (a GBP view option), not this conversion. Usage amounts are not costs — untouched.
    """
    part_floor = start - timedelta(days=_PARTITION_PAD_DAYS)
    machine_spec_cols = ",\n".join(_system_label_col(alias, key) for alias, key in _MACHINE_SPEC_LABELS)
    return f"""
SELECT
  FORMAT_DATE('%Y-%m-%d', DATE(usage_start_time)) AS day,
  COALESCE(service.description, 'Unknown') AS service,
  COALESCE(resource.name, '') AS resource_id,
  COALESCE(location.region, location.location, 'global') AS region,
  COALESCE(sku.description, 'Unknown') AS sku,
  COALESCE(usage.pricing_unit, '') AS usage_unit,
  COALESCE(location.zone, '') AS zone,
  ROUND(SUM(cost / IFNULL(NULLIF(currency_conversion_rate, 0), 1)), 6) AS cost,
  ROUND(SUM(cost), 6) AS cost_native,
  ROUND(SUM((SELECT IFNULL(SUM(c.amount), 0) FROM UNNEST(credits) AS c)
    / IFNULL(NULLIF(currency_conversion_rate, 0), 1)), 6) AS credit,
  ROUND(SUM((SELECT IFNULL(SUM(c.amount), 0) FROM UNNEST(credits) AS c)), 6) AS credit_native,
  ANY_VALUE(currency) AS currency,
  ROUND(SUM(usage.amount_in_pricing_units), 6) AS usage_amount,
{machine_spec_cols}
FROM `{table}`
WHERE _PARTITIONTIME >= TIMESTAMP('{part_floor.isoformat()}')
  AND usage_start_time >= TIMESTAMP('{start.isoformat()}')
  AND usage_start_time < TIMESTAMP('{end.isoformat()}')
  AND cost <> 0
GROUP BY day, service, resource_id, region, sku, usage_unit, zone
""".strip()  # nosec B608 — dates via date.isoformat(); table from config; no user input


def aws_facts_sql(database: str, table: str, start: date, end: date) -> str:
    """Per (day, service, resource, region, usage_type, zone) GROSS cost + credits + usage for the
    AWS CUR (Athena) — mirrors the GCP query's cost/credit split so the AWS tab reports NET spend.

    `cost` (gross) = `line_item_unblended_cost` over Usage/DiscountedUsage/Tax/Fee. This CUR's
    crawler schema has **no** `line_item_net_unblended_cost` column (verified via SHOW COLUMNS
    2026-07-09 — an earlier switch to it silently zeroed the whole AWS tab, a failed per-cloud query
    being isolated). `credit` = the same column over `Credit` line-item rows (negative); net =
    cost + credit, so a fully-credited account (verified 2026-07-09: ~$752 usage, ~-$752 credit →
    **net $0**) reports $0 net with the gross/credit split visible, not a bare or overstated number.
    Refund / RIFee / SavingsPlanRecurringFee stay excluded. `usage_type` is the AWS analog of a GCP
    SKU. `HAVING sum(abs(...))` keeps groups with any activity so a fully-credited (net-0) group is
    not dropped.
    """
    return f"""
SELECT
  date_format(line_item_usage_start_date, '%Y-%m-%d') AS day,
  product_product_name AS service,
  product_servicecode AS service_code,
  line_item_resource_id AS resource_id,
  COALESCE(NULLIF(product_region_code, ''), 'global') AS region,
  COALESCE(NULLIF(line_item_usage_type, ''), 'Unknown') AS usage_type,
  COALESCE(NULLIF(line_item_availability_zone, ''), '') AS zone,
  round(sum(CASE WHEN line_item_line_item_type = 'Credit' THEN 0 ELSE line_item_unblended_cost END), 6) AS cost,
  round(sum(CASE WHEN line_item_line_item_type = 'Credit' THEN line_item_unblended_cost ELSE 0 END), 6) AS credit,
  round(sum(line_item_usage_amount), 6) AS usage_amount
FROM {database}.{table}
WHERE line_item_line_item_type IN ('Usage', 'DiscountedUsage', 'Tax', 'Fee', 'Credit')
  AND date(line_item_usage_start_date) >= date('{start.isoformat()}')
  AND date(line_item_usage_start_date) < date('{end.isoformat()}')
GROUP BY 1, 2, 3, 4, 5, 6, 7
HAVING sum(abs(line_item_unblended_cost)) <> 0
""".strip()  # nosec B608 — dates via date.isoformat(); db/table from config; no user input

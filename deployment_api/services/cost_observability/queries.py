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


def gcp_facts_sql(table: str, start: date, end: date) -> str:
    """Per (day, service, resource, region) cost + credit for the GCP export."""
    part_floor = start - timedelta(days=_PARTITION_PAD_DAYS)
    return f"""
SELECT
  FORMAT_DATE('%Y-%m-%d', DATE(usage_start_time)) AS day,
  COALESCE(service.description, 'Unknown') AS service,
  COALESCE(resource.name, '') AS resource_id,
  COALESCE(location.region, location.location, 'global') AS region,
  ROUND(SUM(cost), 6) AS cost,
  ROUND(SUM((SELECT IFNULL(SUM(c.amount), 0) FROM UNNEST(credits) AS c)), 6) AS credit
FROM `{table}`
WHERE _PARTITIONTIME >= TIMESTAMP('{part_floor.isoformat()}')
  AND usage_start_time >= TIMESTAMP('{start.isoformat()}')
  AND usage_start_time < TIMESTAMP('{end.isoformat()}')
  AND cost <> 0
GROUP BY day, service, resource_id, region
""".strip()  # nosec B608 — dates via date.isoformat(); table from config; no user input


def aws_facts_sql(database: str, table: str, start: date, end: date) -> str:
    """Per (day, service, resource, region) cost for the AWS CUR (Athena).

    `line_item_line_item_type IN ('Usage','DiscountedUsage')` excludes taxes, refunds, and
    Savings-Plan/RI recurring-fee rows so the total matches "usage spend".
    """
    return f"""
SELECT
  date_format(line_item_usage_start_date, '%Y-%m-%d') AS day,
  product_product_name AS service,
  product_servicecode AS service_code,
  line_item_resource_id AS resource_id,
  COALESCE(NULLIF(product_region_code, ''), 'global') AS region,
  round(sum(line_item_unblended_cost), 6) AS cost
FROM {database}.{table}
WHERE line_item_line_item_type IN ('Usage', 'DiscountedUsage')
  AND date(line_item_usage_start_date) >= date('{start.isoformat()}')
  AND date(line_item_usage_start_date) < date('{end.isoformat()}')
GROUP BY 1, 2, 3, 4, 5
HAVING sum(line_item_unblended_cost) <> 0
""".strip()  # nosec B608 — dates via date.isoformat(); db/table from config; no user input

"""SQL builders + BigQuery access for the durable operational-data tables.

deployment_durable_operational_data_bigquery_2026_07_21.md — reads from the
BQ dataset `bootstrap_operational_data_bq.py` (deployment-service) creates,
fed by the dedicated Pub/Sub topics + native BQ subscriptions
`setup-pubsub.sh` provisions. All access via the UTL `get_analytics_client`
wrapper — never raw `google.cloud.bigquery`, per this workspace's QG-enforced
coding standard.

Every table here is partitioned (`require_partition_filter=True` on the
underlying UTL `create_table` wrapper — PR-4, still open), so every query MUST
carry a `DATE(<partition_field>) >=` filter or BigQuery rejects it outright.

No user-supplied string is ever interpolated into a query verbatim: `vm_name`
is validated against `_IDENTIFIER_RE` before use (same shape GCE/systemd
instance names take), and `window` only ever selects a value out of the
hardcoded `WINDOWS` mapping below — never a raw client string.
"""

from __future__ import annotations

import re
from typing import Literal

from unified_trading_library import get_analytics_client

DATASET = "deployment_operational_data"

_IDENTIFIER_RE = re.compile(r"^[A-Za-z0-9_-]{1,63}$")

WindowName = Literal["1h", "4h", "24h", "1wk"]

# window -> (BigQuery INTERVAL expression, partition-day lookback with a safety buffer
# for UTC/partition-boundary edges).
WINDOWS: dict[str, tuple[str, int]] = {
    "1h": ("INTERVAL 1 HOUR", 2),
    "4h": ("INTERVAL 4 HOUR", 2),
    "24h": ("INTERVAL 24 HOUR", 3),
    "1wk": ("INTERVAL 7 DAY", 9),
}


class InvalidIdentifierError(ValueError):
    """Raised when a caller-supplied identifier (vm_name/service/...) fails validation."""


def _validate_identifier(value: str, *, field: str) -> str:
    if not _IDENTIFIER_RE.match(value):
        raise InvalidIdentifierError(f"invalid {field}: {value!r}")
    return value


def resource_samples_rolling_sql(project_id: str, window: WindowName, vm_name: str | None) -> str:
    """Per-(vm_name, service) avg/min/max/p95 of cpu/mem/disk over `window`."""
    interval_sql, lookback_days = WINDOWS[window]
    vm_filter = ""
    if vm_name is not None:
        safe_vm = _validate_identifier(vm_name, field="vm_name")
        # nosec B608 — safe_vm is regex-validated above ([A-Za-z0-9_-]{1,63}), not raw user SQL.
        vm_filter = f"  AND vm_name = '{safe_vm}'\n"
    return f"""
SELECT
  vm_name,
  service,
  AVG(cpu_pct) AS avg_cpu_pct, MIN(cpu_pct) AS min_cpu_pct, MAX(cpu_pct) AS max_cpu_pct,
  APPROX_QUANTILES(cpu_pct, 100)[OFFSET(95)] AS p95_cpu_pct,
  AVG(mem_pct) AS avg_mem_pct, MIN(mem_pct) AS min_mem_pct, MAX(mem_pct) AS max_mem_pct,
  APPROX_QUANTILES(mem_pct, 100)[OFFSET(95)] AS p95_mem_pct,
  AVG(disk_pct) AS avg_disk_pct, MIN(disk_pct) AS min_disk_pct, MAX(disk_pct) AS max_disk_pct,
  APPROX_QUANTILES(disk_pct, 100)[OFFSET(95)] AS p95_disk_pct,
  COUNT(*) AS sample_count
FROM `{project_id}.{DATASET}.resource_samples`
WHERE DATE(ts) >= DATE_SUB(CURRENT_DATE(), INTERVAL {lookback_days} DAY)
  AND ts >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), {interval_sql})
{vm_filter}GROUP BY vm_name, service
ORDER BY vm_name, service
"""  # nosec B608 — interval_sql/lookback_days come only from the hardcoded WINDOWS dict, keyed by a Literal


def process_category_breakdown_sql(project_id: str, window: WindowName, vm_name: str) -> str:
    """Per-category (worker_agent/orchestrator/ci/ao_plan_work/other) CPU/mem rollup for one
    multi-tenant VM over `window` — the 4th signal, scoped to hosts that actually need it."""
    interval_sql, lookback_days = WINDOWS[window]
    safe_vm = _validate_identifier(vm_name, field="vm_name")
    return f"""
SELECT
  category,
  AVG(cpu_pct) AS avg_cpu_pct, MAX(cpu_pct) AS max_cpu_pct,
  AVG(mem_pct) AS avg_mem_pct, MAX(mem_pct) AS max_mem_pct,
  COUNT(DISTINCT pid) AS distinct_pids,
  COUNT(*) AS sample_count
FROM `{project_id}.{DATASET}.process_samples`
WHERE DATE(ts) >= DATE_SUB(CURRENT_DATE(), INTERVAL {lookback_days} DAY)
  AND ts >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), {interval_sql})
  AND vm_name = '{safe_vm}'
GROUP BY category
ORDER BY avg_cpu_pct DESC
"""  # nosec B608 — safe_vm is regex-validated; interval_sql/lookback_days from the hardcoded WINDOWS dict


def watchdog_kill_events_sql(project_id: str, *, hours: int, vm_name: str | None) -> str:
    """Recent resource-watchdog kill/violation rows over a rolling `hours` window (max 7 days).

    Reads the durable ``watchdog_kill_events`` table written by
    ``operational_data_writer.write_watchdog_kill_event`` (the deployment-api side of the
    resource-watchdog dual-write). Partition-aware like the other tables here
    (``require_partition_filter``), so the ``DATE(ts) >=`` day-buffer filter is mandatory.
    """
    if not isinstance(hours, int) or not 1 <= hours <= 168:
        raise ValueError(f"invalid hours: {hours!r} (expected int in 1..168)")
    lookback_days = 2 + hours // 24  # partition-day buffer — matches WINDOWS' 2/3/9 for 1h/24h/1wk
    vm_filter = ""
    if vm_name is not None:
        safe_vm = _validate_identifier(vm_name, field="vm_name")
        # nosec B608 — safe_vm is regex-validated above ([A-Za-z0-9_-]{1,63}), not raw user SQL.
        vm_filter = f"  AND vm_name = '{safe_vm}'\n"
    return f"""
SELECT
  ts, vm_name, pid, slot_id, command, reason, rss_mb, limit_mb, pressure_level, killed
FROM `{project_id}.{DATASET}.watchdog_kill_events`
WHERE DATE(ts) >= DATE_SUB(CURRENT_DATE(), INTERVAL {lookback_days} DAY)
  AND ts >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL {hours} HOUR)
{vm_filter}ORDER BY ts DESC
"""  # nosec B608 — hours validated above (int 1..168), lookback_days derived from it, safe_vm regex-validated


def run_query(project_id: str, sql: str) -> list[dict[str, object]]:
    client = get_analytics_client(provider="gcp", project_id=project_id)
    return client.execute_query(sql)

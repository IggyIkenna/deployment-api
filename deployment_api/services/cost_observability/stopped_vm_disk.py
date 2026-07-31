"""The ``stopped_vm_disk`` waste kind — a VM's disk billed after its own compute usage stopped.

Split out of ``cost_observability/service.py`` (1055L, over the 900L file-size gate) +
``CostObservabilityService._stopped_vm_disk_waste_rows`` (79L, over the 50L method-size gate)
per ``plans/active/issues/deployment_api_qg_size_gate_debt_2026_07_30.md``. The class keeps a
thin same-named wrapper method that delegates here (see that method's docstring) — both so
``waste.py``'s cross-reference to ``cost_observability.service._stopped_vm_disk_waste_rows``
stays accurate and so any future caller can keep calling it as a bound method.
"""

from __future__ import annotations

from collections.abc import Sequence

import pyarrow as pa

from deployment_api.services.cost_observability.models import CLOUD_GCP, KIND_VM, BreakdownRow
from deployment_api.services.cost_observability.row_builders import (
    _f,  # pyright: ignore[reportPrivateUsage]
    _s,  # pyright: ignore[reportPrivateUsage]
)
from deployment_api.services.cost_observability.snapshot import aggregate_arrow
from deployment_api.services.cost_observability.waste import WASTE_STOPPED_VM_DISK


def _stopped_vm_disk_rows(table: pa.Table, cwhere: str, cparams: Sequence[object]) -> list[BreakdownRow]:
    """Disk (`PD Capacity`) spend billed AFTER a VM's own compute usage stopped appearing.

    Compute-usage rows key off `projects/<num>/instances/<name>`; the SAME VM's disk rows key
    off the bare disk name (`<name>` for the boot disk) — two different `resource_id`s for one
    logical VM (verified live: `E2 Instance Core running in Japan` vs `Storage PD Capacity in
    Japan` for `cefi-binance-futures-2020-heavy-...`). Stripping the `projects/.../instances/`
    prefix joins them under one `vm_key`.

    For each `vm_key` with BOTH a compute day and a LATER disk day inside the query window: the
    row's `cost` is the disk cost strictly after the last compute day — i.e. only the idle
    portion, not the resource's whole-window cost (unlike the SKU-classified waste kinds in
    ``resource_rows``, where the whole flagged row IS the waste). This is real billed $, not a
    list-rate estimate, and needs no live GCP call — a VM already reaped shows up exactly the
    same way a still-stopped one does, since both are pure billing history.

    A VM whose compute usage started before the query window (so `MIN(day)` of the window
    already finds it mid-run, with no compute row at all if it fully finished earlier) can't be
    distinguished here from a disk with no VM component — both show zero compute days — so this
    under-counts left-truncated cases rather than risk a false positive. Widen `days` to recover
    them.

    ONE query, not one-per-`vm_key`: `aggregate_arrow` opens a fresh in-memory DuckDB connection
    and re-registers the whole (~168K-row) window table on EVERY call (thread-safety, see
    `snapshot.aggregate_arrow`) — a per-`vm_key` follow-up loop paid that setup cost dozens of
    times, measured live at ~12s for a 30-day window (vs. ~1s for this single-query form). The
    `vm_keys` CTE finds the compute/disk day pair; `disk_rows` re-derives the same `vm_key` per
    disk row (SQL has no CTE-result reuse across the join otherwise) and joins on
    `day > last_compute_day` to sum only the post-compute-stop portion.
    """
    rows = aggregate_arrow(
        table,
        "WITH vm_keys AS ("  # nosec B608 — cwhere is code-internal ('cloud = ?' or 'TRUE'); user params bound
        "  SELECT COALESCE(NULLIF(regexp_extract(resource_id, 'instances/(.*)$', 1), ''), resource_id) AS vm_key, "
        "    MAX(day) FILTER (WHERE sku LIKE '%Instance Core%' OR sku LIKE '%Instance Ram%') AS last_compute_day, "
        "    MAX(day) FILTER (WHERE sku LIKE '%PD Capacity%') AS last_disk_day "
        f"  FROM cost_records WHERE {cwhere} AND cloud = 'gcp' AND resource_id <> '' "
        "  GROUP BY vm_key "
        "  HAVING last_compute_day IS NOT NULL AND last_disk_day IS NOT NULL AND last_disk_day > last_compute_day"
        "), disk_rows AS ("
        "  SELECT COALESCE(NULLIF(regexp_extract(resource_id, 'instances/(.*)$', 1), ''), resource_id) AS vm_key, "
        "    day, cost, credit, cost_native, credit_native, currency "
        f"  FROM cost_records WHERE {cwhere} AND cloud = 'gcp' AND sku LIKE '%PD Capacity%'"
        ") "
        "SELECT vk.vm_key, vk.last_compute_day, vk.last_disk_day, "
        "  SUM(dr.cost + dr.credit), SUM(dr.cost), SUM(dr.credit), "
        "  SUM(dr.cost_native + dr.credit_native), SUM(dr.cost_native), SUM(dr.credit_native), "
        "  ANY_VALUE(dr.currency) "
        "FROM vm_keys vk JOIN disk_rows dr ON dr.vm_key = vk.vm_key AND dr.day > vk.last_compute_day "
        "GROUP BY vk.vm_key, vk.last_compute_day, vk.last_disk_day",
        [*cparams, *cparams],
    )
    out: list[BreakdownRow] = []
    for vm_key, last_compute_day, last_disk_day, net, gross, credit, net_n, gross_n, credit_n, currency in rows:
        cost = round(_f(net), 2)
        if abs(cost) < 0.01:
            continue
        vm_key_s, compute_day_s = _s(vm_key), _s(last_compute_day)
        out.append(
            BreakdownRow(
                label=f"{vm_key_s} (idle since {compute_day_s})",
                cloud=CLOUD_GCP,
                cost=cost,
                gross=round(_f(gross), 2),
                credit=round(_f(credit), 2),
                currency=_s(currency) or "USD",
                cost_native=round(_f(net_n), 2),
                gross_native=round(_f(gross_n), 2),
                credit_native=round(_f(credit_n), 2),
                detail=f"disk billed {compute_day_s} → {_s(last_disk_day)} after compute usage stopped",
                resource_kind=KIND_VM,
                is_idle=True,
                waste_kind=WASTE_STOPPED_VM_DISK,
            )
        )
    out.sort(key=lambda r: r.cost, reverse=True)
    return out

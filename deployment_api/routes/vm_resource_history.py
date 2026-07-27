# Epic: observability_master
"""Rolling-window resource-history endpoints, alongside the existing D.1-metrics
live-snapshot path (`_vm_health.py`).

deployment_durable_operational_data_bigquery_2026_07_21.md — the durable
BigQuery-backed answer to "what did this VM's CPU/RAM/disk look like over the
last 1h/4h/24h/1wk", and the cross-VM comparison the operator originally asked
for ("ten VMs running instruments-service — what were their resources?").
The live-snapshot Host Resources panel (Firestore-backed, `_vm_health.py`)
is unchanged — this is the rolling-window complement, not a replacement.
"""

from __future__ import annotations

import logging

from fastapi import APIRouter, HTTPException, Query
from pydantic import BaseModel

from deployment_api.deployment_api_config import DeploymentApiConfig
from deployment_api.services.operational_data_queries import (
    InvalidIdentifierError,
    WindowName,
    process_category_breakdown_sql,
    resource_samples_rolling_sql,
    run_query,
)

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/api/vm-resources", tags=["VM Resources"])

_cfg = DeploymentApiConfig()


class ResourceRollingWindowRow(BaseModel):
    vm_name: str
    service: str
    avg_cpu_pct: float | None
    min_cpu_pct: float | None
    max_cpu_pct: float | None
    p95_cpu_pct: float | None
    avg_mem_pct: float | None
    min_mem_pct: float | None
    max_mem_pct: float | None
    p95_mem_pct: float | None
    avg_disk_pct: float | None
    min_disk_pct: float | None
    max_disk_pct: float | None
    p95_disk_pct: float | None
    sample_count: int


class ResourceRollingWindowResponse(BaseModel):
    window: WindowName
    rows: list[ResourceRollingWindowRow]


class ProcessCategoryRow(BaseModel):
    category: str
    avg_cpu_pct: float | None
    max_cpu_pct: float | None
    avg_mem_pct: float | None
    max_mem_pct: float | None
    distinct_pids: int
    sample_count: int


class ProcessCategoryBreakdownResponse(BaseModel):
    vm_name: str
    window: WindowName
    rows: list[ProcessCategoryRow]


def _as_float_or_none(v: object) -> float | None:
    return None if v is None else float(v)  # pyright: ignore[reportArgumentType]


@router.get("/rolling", response_model=ResourceRollingWindowResponse)
def get_resource_rolling_window(
    window: WindowName = Query("1h"),
    vm_name: str | None = Query(None, description="Filter to one VM; omit for the cross-VM comparison view"),
) -> ResourceRollingWindowResponse:
    """avg/min/max/p95 cpu/mem/disk per (vm_name, service) over the requested window.

    No `vm_name` filter -> every VM's rollup for the window (the cross-VM comparison
    the operator asked for). Degrades honestly to an empty row list in mock mode or
    with no GCP project configured — never a 5xx for an unconfigured/mock deployment.
    """
    if _cfg.is_mock_mode() or not _cfg.effective_project_id:
        return ResourceRollingWindowResponse(window=window, rows=[])
    try:
        sql = resource_samples_rolling_sql(_cfg.require_gcp_project_id(), window, vm_name)
    except InvalidIdentifierError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc
    try:
        raw_rows = run_query(_cfg.require_gcp_project_id(), sql)
    except Exception as exc:
        logger.warning("resource_samples rolling-window query failed: %s", exc)
        return ResourceRollingWindowResponse(window=window, rows=[])
    rows = [
        ResourceRollingWindowRow(
            vm_name=str(r.get("vm_name") or ""),
            service=str(r.get("service") or ""),
            avg_cpu_pct=_as_float_or_none(r.get("avg_cpu_pct")),
            min_cpu_pct=_as_float_or_none(r.get("min_cpu_pct")),
            max_cpu_pct=_as_float_or_none(r.get("max_cpu_pct")),
            p95_cpu_pct=_as_float_or_none(r.get("p95_cpu_pct")),
            avg_mem_pct=_as_float_or_none(r.get("avg_mem_pct")),
            min_mem_pct=_as_float_or_none(r.get("min_mem_pct")),
            max_mem_pct=_as_float_or_none(r.get("max_mem_pct")),
            p95_mem_pct=_as_float_or_none(r.get("p95_mem_pct")),
            avg_disk_pct=_as_float_or_none(r.get("avg_disk_pct")),
            min_disk_pct=_as_float_or_none(r.get("min_disk_pct")),
            max_disk_pct=_as_float_or_none(r.get("max_disk_pct")),
            p95_disk_pct=_as_float_or_none(r.get("p95_disk_pct")),
            sample_count=int(r.get("sample_count") or 0),  # pyright: ignore[reportArgumentType]
        )
        for r in raw_rows
    ]
    return ResourceRollingWindowResponse(window=window, rows=rows)


@router.get("/process-category", response_model=ProcessCategoryBreakdownResponse)
def get_process_category_breakdown(
    vm_name: str = Query(..., description="Required — process-category breakdown is per multi-tenant VM"),
    window: WindowName = Query("1h"),
) -> ProcessCategoryBreakdownResponse:
    """Per-category (worker_agent/orchestrator/ci/ao_plan_work/other) CPU/mem rollup.

    Only meaningful for genuinely multi-tenant hosts (the orchestrator VM today) —
    returns an empty row list rather than an error for a VM with no process_samples
    rows (single-tenant VMs never emit this signal by design).
    """
    if _cfg.is_mock_mode() or not _cfg.effective_project_id:
        return ProcessCategoryBreakdownResponse(vm_name=vm_name, window=window, rows=[])
    try:
        sql = process_category_breakdown_sql(_cfg.require_gcp_project_id(), window, vm_name)
    except InvalidIdentifierError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc
    try:
        raw_rows = run_query(_cfg.require_gcp_project_id(), sql)
    except Exception as exc:
        logger.warning("process_samples category-breakdown query failed: %s", exc)
        return ProcessCategoryBreakdownResponse(vm_name=vm_name, window=window, rows=[])
    rows = [
        ProcessCategoryRow(
            category=str(r.get("category") or "other"),
            avg_cpu_pct=_as_float_or_none(r.get("avg_cpu_pct")),
            max_cpu_pct=_as_float_or_none(r.get("max_cpu_pct")),
            avg_mem_pct=_as_float_or_none(r.get("avg_mem_pct")),
            max_mem_pct=_as_float_or_none(r.get("max_mem_pct")),
            distinct_pids=int(r.get("distinct_pids") or 0),  # pyright: ignore[reportArgumentType]
            sample_count=int(r.get("sample_count") or 0),  # pyright: ignore[reportArgumentType]
        )
        for r in raw_rows
    ]
    return ProcessCategoryBreakdownResponse(vm_name=vm_name, window=window, rows=rows)

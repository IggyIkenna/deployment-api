"""GET /api/costs/daily?date=YYYY-MM-DD — per-asset_group cost aggregation.

Reads cost_summary blobs from `gs://{pid}-deployment-events/cost_summary/{date}/`
where each blob is a JSONL record with shape:
  {
    "vm_name": "cefi-backfill-20260515",
    "asset_group": "cefi",
    "archetype": "carry_staked_basis",
    "machine_type": "n1-standard-4",
    "runtime_hours": 2.5,
    "compute_usd": 0.475,
    "disk_usd": 0.008,
    "total_usd": 0.483
  }

In mock mode: returns synthetic data without GCS reads.
In prod mode: streams blobs, parses JSONL, aggregates.
"""

from __future__ import annotations

import json
import logging
from datetime import UTC, datetime
from typing import cast

from fastapi import APIRouter, HTTPException, Query
from pydantic import BaseModel
from unified_trading_library import get_storage_client, log_event

from deployment_api.deployment_api_config import DeploymentApiConfig

router = APIRouter()
logger = logging.getLogger(__name__)

_cfg = DeploymentApiConfig()


class VmCostRow(BaseModel):  # CORRECT-LOCAL: FastAPI API contract model
    vm_name: str
    asset_group: str
    archetype: str
    machine_type: str
    runtime_hours: float
    compute_usd: float
    disk_usd: float
    total_usd: float


class AssetGroupCostRow(BaseModel):  # CORRECT-LOCAL: FastAPI API contract model
    asset_group: str
    total_usd: float
    vm_count: int


class ArchetypeCostRow(BaseModel):  # CORRECT-LOCAL: FastAPI API contract model
    archetype: str
    total_usd: float
    vm_count: int


class DailyCostResponse(BaseModel):  # CORRECT-LOCAL: FastAPI API contract model
    date: str
    total_usd: float
    by_asset_group: list[AssetGroupCostRow]
    by_archetype: list[ArchetypeCostRow]
    by_vm: list[VmCostRow]


def _normalise_date(date: str | None) -> str:
    if date is None:
        return datetime.now(UTC).strftime("%Y-%m-%d")
    try:
        datetime.strptime(date, "%Y-%m-%d")
    except ValueError as exc:
        raise HTTPException(
            status_code=400,
            detail={"code": "BAD_DATE", "message": f"date '{date}' must be YYYY-MM-DD"},
        ) from exc
    return date


def _cost_bucket() -> str:
    pid = _cfg.gcp_project_id or "central-element-323112"
    return f"{pid}-deployment-events"


def _parse_blob(blob_bytes: bytes, blob_name: str) -> VmCostRow | None:
    try:
        text = blob_bytes.decode().strip()
        if not text:
            return None
        row_obj: object = json.loads(text.split("\n", 1)[0])
    except (json.JSONDecodeError, UnicodeDecodeError) as exc:
        log_event(
            "COST_PARSE_FAILED",
            severity="WARNING",
            details={"blob": blob_name, "error": str(exc)},
        )
        return None
    if not isinstance(row_obj, dict):
        return None
    row = cast(dict[str, object], row_obj)
    try:
        return VmCostRow(
            vm_name=str(row.get("vm_name", "")),
            asset_group=str(row.get("asset_group", "unknown")),
            archetype=str(row.get("archetype", "unknown")),
            machine_type=str(row.get("machine_type", "unknown")),
            runtime_hours=float(str(row.get("runtime_hours", 0))),
            compute_usd=float(str(row.get("compute_usd", 0))),
            disk_usd=float(str(row.get("disk_usd", 0))),
            total_usd=float(str(row.get("total_usd", 0))),
        )
    except (ValueError, TypeError) as exc:
        log_event(
            "COST_PARSE_FAILED",
            severity="WARNING",
            details={"blob": blob_name, "error": str(exc)},
        )
        return None


def _aggregate(vm_rows: list[VmCostRow], date: str) -> DailyCostResponse:
    total = sum(r.total_usd for r in vm_rows)

    ag_totals: dict[str, tuple[float, int]] = {}
    arch_totals: dict[str, tuple[float, int]] = {}
    for r in vm_rows:
        prev_ag = ag_totals.get(r.asset_group, (0.0, 0))
        ag_totals[r.asset_group] = (prev_ag[0] + r.total_usd, prev_ag[1] + 1)
        prev_ar = arch_totals.get(r.archetype, (0.0, 0))
        arch_totals[r.archetype] = (prev_ar[0] + r.total_usd, prev_ar[1] + 1)

    by_ag = [
        AssetGroupCostRow(asset_group=ag, total_usd=round(v, 4), vm_count=c)
        for ag, (v, c) in sorted(ag_totals.items(), key=lambda kv: -kv[1][0])
    ]
    by_arch = [
        ArchetypeCostRow(archetype=arch, total_usd=round(v, 4), vm_count=c)
        for arch, (v, c) in sorted(arch_totals.items(), key=lambda kv: -kv[1][0])
    ]

    return DailyCostResponse(
        date=date,
        total_usd=round(total, 4),
        by_asset_group=by_ag,
        by_archetype=by_arch,
        by_vm=sorted(vm_rows, key=lambda r: -r.total_usd),
    )


def _mock_response(date: str) -> DailyCostResponse:
    rows = [
        VmCostRow(
            vm_name=f"cefi-backfill-{date.replace('-', '')}",
            asset_group="cefi",
            archetype="carry_staked_basis",
            machine_type="n1-standard-4",
            runtime_hours=2.0,
            compute_usd=0.38,
            disk_usd=0.01,
            total_usd=0.39,
        ),
        VmCostRow(
            vm_name=f"defi-backfill-{date.replace('-', '')}",
            asset_group="defi",
            archetype="arbitrage_price_dispersion",
            machine_type="n1-standard-8",
            runtime_hours=4.0,
            compute_usd=1.52,
            disk_usd=0.03,
            total_usd=1.55,
        ),
    ]
    return _aggregate(rows, date)


@router.get("/costs/daily", response_model=DailyCostResponse)
def get_daily_costs(
    date: str | None = Query(None, description="YYYY-MM-DD (default: today UTC)"),
) -> DailyCostResponse:
    """Return per-asset_group / per-archetype / per-vm cost totals for a given date.

    Reads from `gs://{pid}-deployment-events/cost_summary/{date}/` JSONL blobs.
    """
    resolved_date = _normalise_date(date)

    if _cfg.is_mock_mode():
        return _mock_response(resolved_date)

    bucket = _cost_bucket()
    prefix = f"cost_summary/{resolved_date}/"
    storage = get_storage_client(project_id=_cfg.gcp_project_id)

    try:
        blobs = list(storage.list_blobs(bucket=bucket, prefix=prefix))  # pyright: ignore[reportUnknownVariableType,reportUnknownMemberType,reportUnknownArgumentType]
    except (OSError, ValueError) as exc:
        logger.warning("Failed to list cost blobs for %s: %s", resolved_date, exc)
        raise HTTPException(
            status_code=502,
            detail={"code": "GCS_UNAVAILABLE", "message": "Cost data storage unavailable"},
        ) from exc

    vm_rows: list[VmCostRow] = []
    for blob in blobs:
        blob_name: str = blob.name  # pyright: ignore[reportUnknownVariableType,reportUnknownMemberType]
        if not blob_name.endswith(".jsonl"):
            continue
        try:
            blob_bytes: bytes = storage.download_bytes(bucket=bucket, blob_path=blob_name)  # pyright: ignore[reportAttributeAccessIssue,reportUnknownVariableType,reportUnknownMemberType]
        except (OSError, ValueError) as exc:
            log_event(
                "COST_FETCH_FAILED",
                severity="WARNING",
                details={"blob": blob_name, "error": str(exc)},
            )
            continue
        parsed = _parse_blob(blob_bytes, blob_name)
        if parsed is not None:
            vm_rows.append(parsed)

    return _aggregate(vm_rows, resolved_date)

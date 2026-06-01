"""POST /api/vm/cost-estimate — GCP Compute Engine pre-launch cost projection.

Accepts a machine_type and runtime_hours and returns an estimated cost in USD
using published GCP on-demand pricing for the asia-northeast1 region.
In mock mode the same formula runs but is labelled dry_run=True.
"""

from __future__ import annotations

import logging
from datetime import UTC, datetime

from fastapi import APIRouter
from pydantic import BaseModel, Field

from deployment_api.deployment_api_config import DeploymentApiConfig

router = APIRouter()
logger = logging.getLogger(__name__)

_cfg = DeploymentApiConfig()

# GCP on-demand hourly prices for asia-northeast1 (USD, as of 2026-05).
# Source: cloud.google.com/compute/vm-instance-pricing
_MACHINE_HOURLY_USD: dict[str, float] = {
    "n1-standard-1": 0.0475,
    "n1-standard-2": 0.0950,
    "n1-standard-4": 0.1900,
    "n1-standard-8": 0.3800,
    "n1-standard-16": 0.7600,
    "n1-standard-32": 1.5200,
    "n1-highmem-2": 0.1184,
    "n1-highmem-4": 0.2368,
    "n1-highmem-8": 0.4736,
    "n1-highmem-16": 0.9472,
    "n2-standard-2": 0.0971,
    "n2-standard-4": 0.1942,
    "n2-standard-8": 0.3884,
    "n2-standard-16": 0.7768,
    "n2-highcpu-4": 0.1528,
    "n2-highcpu-8": 0.3056,
}

_DISK_HOURLY_USD_PER_GB = 0.000_054  # pd-ssd in asia-northeast1


class VmCostEstimateRequest(BaseModel):
    """Request payload for POST /api/vm/cost-estimate."""

    machine_type: str = Field(
        ...,
        description="GCP machine type (e.g. n1-standard-4).",
        examples=["n1-standard-4"],
    )
    runtime_hours: float = Field(
        ...,
        gt=0,
        le=720,
        description="Expected VM lifetime in hours (1-720).",
    )
    disk_gb: int = Field(
        default=50,
        ge=10,
        le=2000,
        description="Boot disk size in GB (default 50).",
    )
    count: int = Field(
        default=1,
        ge=1,
        le=100,
        description="Number of VM instances (default 1).",
    )


class VmCostEstimateResponse(BaseModel):
    """Estimated GCP cost breakdown for a VM launch."""

    machine_type: str
    runtime_hours: float
    disk_gb: int
    count: int
    compute_cost_usd: float
    disk_cost_usd: float
    total_cost_usd: float
    hourly_rate_usd: float
    currency: str
    region: str
    dry_run: bool
    estimated_at: str
    unknown_machine_type: bool


@router.post("/api/vm/cost-estimate", response_model=VmCostEstimateResponse, tags=["VM Cost"])
def estimate_vm_cost(req: VmCostEstimateRequest) -> VmCostEstimateResponse:
    """Return a pre-launch GCP cost projection for a VM type + runtime."""
    hourly = _MACHINE_HOURLY_USD.get(req.machine_type)
    unknown = hourly is None
    if unknown:
        # Fall back to n1-standard-4 rate with a flag so callers know it's approximate.
        hourly = _MACHINE_HOURLY_USD["n1-standard-4"]
        logger.warning("Unknown machine type %r — using n1-standard-4 rate as proxy", req.machine_type)

    compute_cost = round(hourly * req.runtime_hours * req.count, 4)
    disk_cost = round(_DISK_HOURLY_USD_PER_GB * req.disk_gb * req.runtime_hours * req.count, 4)
    total = round(compute_cost + disk_cost, 4)

    return VmCostEstimateResponse(
        machine_type=req.machine_type,
        runtime_hours=req.runtime_hours,
        disk_gb=req.disk_gb,
        count=req.count,
        compute_cost_usd=compute_cost,
        disk_cost_usd=disk_cost,
        total_cost_usd=total,
        hourly_rate_usd=hourly,
        currency="USD",
        region="asia-northeast1",
        dry_run=_cfg.is_mock_mode(),
        estimated_at=datetime.now(UTC).isoformat(),
        unknown_machine_type=unknown,
    )

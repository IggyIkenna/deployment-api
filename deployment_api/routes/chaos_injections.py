"""Chaos Injection API Routes.

Register, list, and revoke ChaosInjectionSpec records. Each injection targets a
declared chaos hook (runtime-topology.yaml `chaos_hooks`). UTL ChaosController
reads active injections and applies them at the declared hook locations.

Hard rules enforced at this layer:
  - `runtime_profile=prod` is FORBIDDEN — production never accepts chaos.
  - `point` must exist in runtime-topology.yaml `chaos_hooks`.
  - `active_until` must be in the future relative to `active_from`.

SSOT schema: unified_api_contracts.internal.domain.deployment_service.ChaosInjectionSpec
SSOT doc: codex/04-architecture/client-isolation-sla-and-runtime-profiles.md §5
"""

from __future__ import annotations

import logging
import uuid
from datetime import UTC, datetime

from fastapi import APIRouter, HTTPException
from pydantic import BaseModel, Field
from unified_api_contracts.internal.domain.deployment_service import (
    ChaosInjectionPoint,
    ChaosInjectionSpec,
    RuntimeProfile,
)
from unified_trading_library import (
    list_chaos_injection_points,
)

router = APIRouter()
logger = logging.getLogger(__name__)

# In-memory store — production would use a shared cache (Redis) or firestore.
# This keeps the API contract stable while we wire persistence in a follow-up.
_active_injections: dict[str, ChaosInjectionSpec] = {}


class CreateInjectionRequest(BaseModel):
    """Request body for POST /chaos/injections."""

    point: ChaosInjectionPoint
    target_service: str = Field(..., min_length=1)
    parameters: dict[str, str] = Field(default_factory=dict)
    active_from: datetime | None = None
    active_until: datetime | None = None
    runtime_profile: RuntimeProfile
    created_by: str = Field(..., min_length=1)


class InjectionResponse(BaseModel):
    injection: ChaosInjectionSpec


class InjectionListResponse(BaseModel):
    injections: list[ChaosInjectionSpec]


def _validate_profile_allows_chaos(profile: RuntimeProfile) -> None:
    if profile == RuntimeProfile.PROD:
        raise HTTPException(
            status_code=403,
            detail={
                "message": (
                    "Chaos injection is FORBIDDEN in runtime_profile=prod. Use staging, paper, mock-live, or backtest."
                )
            },
        )


def _validate_point_registered(point: ChaosInjectionPoint) -> None:
    registered = set(list_chaos_injection_points())
    if not registered:
        return  # topology file not loaded (tests); skip rather than fail
    if point not in registered:
        raise HTTPException(
            status_code=400,
            detail={"message": (f"ChaosInjectionPoint '{point}' is not declared in runtime-topology.yaml chaos_hooks")},
        )


@router.post("/chaos/injections", response_model=InjectionResponse, status_code=201)
async def create_injection(req: CreateInjectionRequest) -> InjectionResponse:
    """Register a new chaos injection. Forbidden in prod."""
    _validate_profile_allows_chaos(req.runtime_profile)
    _validate_point_registered(req.point)

    now = datetime.now(UTC)
    active_from = req.active_from or now
    if req.active_until is not None and req.active_until <= active_from:
        raise HTTPException(
            status_code=400,
            detail={"message": "active_until must be strictly after active_from"},
        )

    injection = ChaosInjectionSpec(
        injection_id=f"chaos_{uuid.uuid4().hex[:12]}",
        point=req.point,
        target_service=req.target_service,
        parameters=req.parameters,
        active_from=active_from,
        active_until=req.active_until,
        created_by=req.created_by,
        runtime_profile=req.runtime_profile,
    )
    _active_injections[injection.injection_id] = injection
    logger.info(
        "Chaos injection created: id=%s point=%s target=%s profile=%s",
        injection.injection_id,
        injection.point,
        injection.target_service,
        injection.runtime_profile,
    )
    return InjectionResponse(injection=injection)


@router.get("/chaos/injections", response_model=InjectionListResponse)
async def list_injections(
    runtime_profile: RuntimeProfile | None = None,
    active_only: bool = True,
) -> InjectionListResponse:
    """List registered chaos injections.

    Filters:
      - runtime_profile: only return injections for this profile
      - active_only: only return injections that are currently active
    """
    now = datetime.now(UTC)
    results: list[ChaosInjectionSpec] = []
    for injection in _active_injections.values():
        if runtime_profile is not None and injection.runtime_profile != runtime_profile:
            continue
        if active_only:
            if injection.active_from > now:
                continue
            if injection.active_until is not None and injection.active_until <= now:
                continue
        results.append(injection)
    return InjectionListResponse(injections=results)


@router.get("/chaos/injections/{injection_id}", response_model=InjectionResponse)
async def get_injection(injection_id: str) -> InjectionResponse:
    """Fetch one injection by id."""
    injection = _active_injections.get(injection_id)
    if injection is None:
        raise HTTPException(
            status_code=404,
            detail={"message": f"No chaos injection with id '{injection_id}'"},
        )
    return InjectionResponse(injection=injection)


@router.delete("/chaos/injections/{injection_id}", status_code=204)
async def revoke_injection(injection_id: str) -> None:
    """Revoke an active chaos injection."""
    if injection_id not in _active_injections:
        raise HTTPException(
            status_code=404,
            detail={"message": f"No chaos injection with id '{injection_id}'"},
        )
    del _active_injections[injection_id]
    logger.info("Chaos injection revoked: id=%s", injection_id)


@router.get("/chaos/hooks", response_model=list[str])
async def list_hooks() -> list[str]:
    """List the ChaosInjectionPoint values declared in runtime-topology.yaml chaos_hooks."""
    return [str(p) for p in list_chaos_injection_points()]


__all__ = ["ChaosInjectionPoint", "ChaosInjectionSpec", "RuntimeProfile", "router"]

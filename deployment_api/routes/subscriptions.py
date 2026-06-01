"""Client Subscriptions API Routes.

CRUD for ClientSubscription records — drives per-service isolation materialisation.
Subscriptions are stored as YAML files under
`deployment-service/configs/client_subscriptions/<client_id>.yaml` (created by
deployment-service at materialisation time) and mirrored in-memory here for API
reads. Writes through to disk via deployment-service.

SSOT schema: unified_api_contracts.internal.domain.deployment_service.ClientSubscription
SSOT doc: codex/04-architecture/client-isolation-sla-and-runtime-profiles.md
"""

from __future__ import annotations

import logging
from datetime import UTC, datetime
from pathlib import Path

import yaml
from fastapi import APIRouter, HTTPException
from pydantic import BaseModel, Field
from unified_api_contracts.internal.domain.deployment_service import (
    ClientServiceOverride,
    ClientSubscription,
    IsolationPolicy,
    SLATier,
)

from deployment_api.deployment_api_config import DeploymentApiConfig

router = APIRouter()
logger = logging.getLogger(__name__)

_cfg = DeploymentApiConfig()


def _subscriptions_dir() -> Path:
    """Return the directory holding client subscription YAMLs.

    Resolution order:
      1. deployment-service/configs/client_subscriptions/ (mounted in prod)
      2. A temp fallback for local dev — the dir is created on first write.
    """
    workspace_root = Path(_cfg.workspace_root or "")
    path = workspace_root / "deployment-service" / "configs" / "client_subscriptions"
    path.mkdir(parents=True, exist_ok=True)
    return path


class CreateSubscriptionRequest(BaseModel):
    """Request body for POST /subscriptions."""

    client_id: str = Field(..., min_length=1)
    sla_tier: SLATier
    service_overrides: list[ClientServiceOverride] = Field(default_factory=list)
    active_from: datetime | None = None
    active_until: datetime | None = None
    note: str = ""


class SubscriptionResponse(BaseModel):
    """Standard response envelope for subscription endpoints."""

    subscription: ClientSubscription


class SubscriptionListResponse(BaseModel):
    """List response."""

    subscriptions: list[ClientSubscription]


def _subscription_path(client_id: str) -> Path:
    safe_client_id = "".join(c for c in client_id if c.isalnum() or c in "_-")
    if safe_client_id != client_id or not safe_client_id:
        raise HTTPException(
            status_code=400,
            detail={"message": f"Invalid client_id '{client_id}'"},
        )
    return _subscriptions_dir() / f"{safe_client_id}.yaml"


def _load_subscription(path: Path) -> ClientSubscription:
    raw = yaml.safe_load(path.read_text())  # type: ignore[reportAny]
    return ClientSubscription.model_validate(raw)


def _write_subscription(sub: ClientSubscription) -> None:
    path = _subscription_path(sub.client_id)
    path.write_text(yaml.safe_dump(sub.model_dump(mode="json"), sort_keys=False))


@router.get("/subscriptions", response_model=SubscriptionListResponse)
async def list_subscriptions() -> SubscriptionListResponse:
    """List all known client subscriptions."""
    subs: list[ClientSubscription] = []
    for path in sorted(_subscriptions_dir().glob("*.yaml")):
        try:
            subs.append(_load_subscription(path))
        except (yaml.YAMLError, ValueError) as e:
            logger.warning("Failed to load subscription %s: %s", path.name, e)
    return SubscriptionListResponse(subscriptions=subs)


@router.get("/subscriptions/{client_id}", response_model=SubscriptionResponse)
async def get_subscription(client_id: str) -> SubscriptionResponse:
    """Fetch one client's subscription."""
    path = _subscription_path(client_id)
    if not path.exists():
        raise HTTPException(
            status_code=404,
            detail={"message": f"No subscription for client '{client_id}'"},
        )
    return SubscriptionResponse(subscription=_load_subscription(path))


@router.post("/subscriptions", response_model=SubscriptionResponse, status_code=201)
async def create_subscription(req: CreateSubscriptionRequest) -> SubscriptionResponse:
    """Create a new client subscription."""
    path = _subscription_path(req.client_id)
    if path.exists():
        raise HTTPException(
            status_code=409,
            detail={"message": f"Subscription already exists for '{req.client_id}'"},
        )
    sub = ClientSubscription(
        client_id=req.client_id,
        sla_tier=req.sla_tier,
        service_overrides=req.service_overrides,
        active_from=req.active_from or datetime.now(UTC),
        active_until=req.active_until,
        note=req.note,
    )
    _write_subscription(sub)
    logger.info("Created subscription: client_id=%s tier=%s", sub.client_id, sub.sla_tier)
    return SubscriptionResponse(subscription=sub)


@router.patch("/subscriptions/{client_id}/overrides", response_model=SubscriptionResponse)
async def set_override(client_id: str, override: ClientServiceOverride) -> SubscriptionResponse:
    """Add or replace a per-service isolation override on an existing subscription."""
    path = _subscription_path(client_id)
    if not path.exists():
        raise HTTPException(
            status_code=404,
            detail={"message": f"No subscription for client '{client_id}'"},
        )
    sub = _load_subscription(path)
    remaining = [o for o in sub.service_overrides if o.service_name != override.service_name]
    sub.service_overrides = [*remaining, override]
    _write_subscription(sub)
    logger.info(
        "Set override for %s: %s -> %s",
        client_id,
        override.service_name,
        override.isolation,
    )
    return SubscriptionResponse(subscription=sub)


@router.delete(
    "/subscriptions/{client_id}/overrides/{service_name}",
    response_model=SubscriptionResponse,
)
async def clear_override(client_id: str, service_name: str) -> SubscriptionResponse:
    """Remove a per-service override, reverting to the SLA tier's default."""
    path = _subscription_path(client_id)
    if not path.exists():
        raise HTTPException(
            status_code=404,
            detail={"message": f"No subscription for client '{client_id}'"},
        )
    sub = _load_subscription(path)
    sub.service_overrides = [o for o in sub.service_overrides if o.service_name != service_name]
    _write_subscription(sub)
    return SubscriptionResponse(subscription=sub)


@router.delete("/subscriptions/{client_id}", status_code=204)
async def delete_subscription(client_id: str) -> None:
    """Delete a client subscription entirely."""
    path = _subscription_path(client_id)
    if path.exists():
        path.unlink()


__all__ = [
    "ClientServiceOverride",
    "ClientSubscription",
    "IsolationPolicy",
    "SLATier",
    "router",
]

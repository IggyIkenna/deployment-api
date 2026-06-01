"""Unit tests for routes/subscriptions.py — client subscription CRUD."""

from __future__ import annotations

from datetime import UTC, datetime
from pathlib import Path

import pytest
from fastapi import HTTPException
from unified_api_contracts.internal.domain.deployment_service import (
    ClientServiceOverride,
    IsolationPolicy,
    SLATier,
)

import deployment_api.routes.subscriptions as subs_routes
from deployment_api.routes.subscriptions import (
    CreateSubscriptionRequest,
    clear_override,
    create_subscription,
    delete_subscription,
    get_subscription,
    list_subscriptions,
    set_override,
)


@pytest.fixture(autouse=True)
def _tmp_subscriptions_dir(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> Path:
    """Redirect the YAML store to a temp directory for isolation."""

    def _tmp_dir() -> Path:
        d = tmp_path / "client_subscriptions"
        d.mkdir(parents=True, exist_ok=True)
        return d

    monkeypatch.setattr(subs_routes, "_subscriptions_dir", _tmp_dir)
    return tmp_path


def _basic_request() -> CreateSubscriptionRequest:
    return CreateSubscriptionRequest(
        client_id="client_alpha",
        sla_tier=SLATier.PREMIUM,
        active_from=datetime(2026, 1, 1, tzinfo=UTC),
    )


class TestCreate:
    @pytest.mark.asyncio
    async def test_create_persists_yaml(self) -> None:
        resp = await create_subscription(_basic_request())
        assert resp.subscription.client_id == "client_alpha"
        assert resp.subscription.sla_tier == SLATier.PREMIUM

    @pytest.mark.asyncio
    async def test_create_conflict_on_duplicate(self) -> None:
        await create_subscription(_basic_request())
        with pytest.raises(HTTPException) as exc:
            await create_subscription(_basic_request())
        assert exc.value.status_code == 409

    @pytest.mark.asyncio
    async def test_create_rejects_unsafe_client_id(self) -> None:
        req = CreateSubscriptionRequest(
            client_id="../evil",
            sla_tier=SLATier.BASIC,
            active_from=datetime(2026, 1, 1, tzinfo=UTC),
        )
        with pytest.raises(HTTPException) as exc:
            await create_subscription(req)
        assert exc.value.status_code == 400


class TestGet:
    @pytest.mark.asyncio
    async def test_round_trip(self) -> None:
        await create_subscription(_basic_request())
        got = await get_subscription("client_alpha")
        assert got.subscription.client_id == "client_alpha"
        assert got.subscription.sla_tier == SLATier.PREMIUM

    @pytest.mark.asyncio
    async def test_missing_returns_404(self) -> None:
        with pytest.raises(HTTPException) as exc:
            await get_subscription("ghost")
        assert exc.value.status_code == 404


class TestList:
    @pytest.mark.asyncio
    async def test_empty_list(self) -> None:
        result = await list_subscriptions()
        assert result.subscriptions == []

    @pytest.mark.asyncio
    async def test_returns_multiple(self) -> None:
        await create_subscription(_basic_request())
        await create_subscription(
            CreateSubscriptionRequest(
                client_id="client_beta",
                sla_tier=SLATier.STANDARD,
                active_from=datetime(2026, 1, 1, tzinfo=UTC),
            )
        )
        result = await list_subscriptions()
        ids = sorted(s.client_id for s in result.subscriptions)
        assert ids == ["client_alpha", "client_beta"]


class TestOverrides:
    @pytest.mark.asyncio
    async def test_set_override_persists(self) -> None:
        await create_subscription(_basic_request())
        resp = await set_override(
            "client_alpha",
            ClientServiceOverride(
                service_name="strategy-service",
                isolation=IsolationPolicy.ISOLATED,
            ),
        )
        assert len(resp.subscription.service_overrides) == 1
        assert resp.subscription.service_overrides[0].service_name == "strategy-service"

    @pytest.mark.asyncio
    async def test_set_override_replaces_existing_for_same_service(self) -> None:
        await create_subscription(_basic_request())
        await set_override(
            "client_alpha",
            ClientServiceOverride(
                service_name="strategy-service",
                isolation=IsolationPolicy.ISOLATED,
            ),
        )
        resp = await set_override(
            "client_alpha",
            ClientServiceOverride(
                service_name="strategy-service",
                isolation=IsolationPolicy.SHARED,
            ),
        )
        assert len(resp.subscription.service_overrides) == 1
        assert resp.subscription.service_overrides[0].isolation == IsolationPolicy.SHARED

    @pytest.mark.asyncio
    async def test_clear_override_removes_entry(self) -> None:
        await create_subscription(_basic_request())
        await set_override(
            "client_alpha",
            ClientServiceOverride(
                service_name="strategy-service",
                isolation=IsolationPolicy.ISOLATED,
            ),
        )
        resp = await clear_override("client_alpha", "strategy-service")
        assert resp.subscription.service_overrides == []

    @pytest.mark.asyncio
    async def test_set_override_on_missing_subscription_404(self) -> None:
        with pytest.raises(HTTPException) as exc:
            await set_override(
                "ghost",
                ClientServiceOverride(
                    service_name="x",
                    isolation=IsolationPolicy.ISOLATED,
                ),
            )
        assert exc.value.status_code == 404


class TestDelete:
    @pytest.mark.asyncio
    async def test_delete_is_idempotent(self) -> None:
        await create_subscription(_basic_request())
        await delete_subscription("client_alpha")
        # Second delete must not raise
        await delete_subscription("client_alpha")
        with pytest.raises(HTTPException):
            await get_subscription("client_alpha")

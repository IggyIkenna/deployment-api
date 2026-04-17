"""Unit tests for routes/chaos_injections.py — chaos registration / list / revoke."""

from __future__ import annotations

from datetime import UTC, datetime, timedelta

import pytest
from fastapi import HTTPException
from unified_api_contracts.internal.domain.deployment_service import (
    ChaosInjectionPoint,
    RuntimeProfile,
)

import deployment_api.routes.chaos_injections as chaos_routes
from deployment_api.routes.chaos_injections import (
    CreateInjectionRequest,
    create_injection,
    get_injection,
    list_hooks,
    list_injections,
    revoke_injection,
)


@pytest.fixture(autouse=True)
def _clear_store(monkeypatch: pytest.MonkeyPatch) -> None:
    """Clear the in-memory injection store between tests."""
    chaos_routes._active_injections.clear()
    # Bypass runtime-topology.yaml check — tests assert route behaviour, not
    # topology shape (covered by UTL tests).
    monkeypatch.setattr(chaos_routes, "list_chaos_injection_points", list)


def _basic_request(
    profile: RuntimeProfile = RuntimeProfile.STAGING,
    active_until: datetime | None = None,
) -> CreateInjectionRequest:
    return CreateInjectionRequest(
        point=ChaosInjectionPoint.VENUE_LATENCY,
        target_service="binance",
        parameters={"delay_ms": "100"},
        active_from=datetime.now(UTC),
        active_until=active_until,
        runtime_profile=profile,
        created_by="qa@example.com",
    )


class TestCreate:
    @pytest.mark.asyncio
    async def test_create_returns_injection(self) -> None:
        resp = await create_injection(_basic_request())
        assert resp.injection.point == ChaosInjectionPoint.VENUE_LATENCY
        assert resp.injection.runtime_profile == RuntimeProfile.STAGING
        assert resp.injection.injection_id.startswith("chaos_")

    @pytest.mark.asyncio
    async def test_prod_profile_is_forbidden(self) -> None:
        with pytest.raises(HTTPException) as exc:
            await create_injection(_basic_request(profile=RuntimeProfile.PROD))
        assert exc.value.status_code == 403

    @pytest.mark.asyncio
    async def test_active_until_before_active_from_rejected(self) -> None:
        req = CreateInjectionRequest(
            point=ChaosInjectionPoint.VENUE_LATENCY,
            target_service="binance",
            parameters={},
            active_from=datetime.now(UTC),
            active_until=datetime.now(UTC) - timedelta(hours=1),
            runtime_profile=RuntimeProfile.STAGING,
            created_by="qa",
        )
        with pytest.raises(HTTPException) as exc:
            await create_injection(req)
        assert exc.value.status_code == 400


class TestList:
    @pytest.mark.asyncio
    async def test_empty_store(self) -> None:
        result = await list_injections()
        assert result.injections == []

    @pytest.mark.asyncio
    async def test_active_only_filter(self) -> None:
        # Future-start (not yet active)
        future = CreateInjectionRequest(
            point=ChaosInjectionPoint.VENUE_LATENCY,
            target_service="future",
            parameters={},
            active_from=datetime.now(UTC) + timedelta(hours=1),
            runtime_profile=RuntimeProfile.STAGING,
            created_by="qa",
        )
        await create_injection(future)
        await create_injection(_basic_request())
        active = await list_injections(active_only=True)
        assert len(active.injections) == 1
        assert active.injections[0].target_service == "binance"
        all_ = await list_injections(active_only=False)
        assert len(all_.injections) == 2

    @pytest.mark.asyncio
    async def test_filter_by_runtime_profile(self) -> None:
        await create_injection(_basic_request(profile=RuntimeProfile.STAGING))
        await create_injection(_basic_request(profile=RuntimeProfile.BACKTEST))
        result = await list_injections(runtime_profile=RuntimeProfile.STAGING)
        assert len(result.injections) == 1
        assert result.injections[0].runtime_profile == RuntimeProfile.STAGING


class TestGet:
    @pytest.mark.asyncio
    async def test_round_trip(self) -> None:
        created = await create_injection(_basic_request())
        got = await get_injection(created.injection.injection_id)
        assert got.injection.injection_id == created.injection.injection_id

    @pytest.mark.asyncio
    async def test_missing_404(self) -> None:
        with pytest.raises(HTTPException) as exc:
            await get_injection("chaos_does_not_exist")
        assert exc.value.status_code == 404


class TestRevoke:
    @pytest.mark.asyncio
    async def test_revoke_removes(self) -> None:
        created = await create_injection(_basic_request())
        await revoke_injection(created.injection.injection_id)
        with pytest.raises(HTTPException):
            await get_injection(created.injection.injection_id)

    @pytest.mark.asyncio
    async def test_revoke_missing_404(self) -> None:
        with pytest.raises(HTTPException) as exc:
            await revoke_injection("chaos_ghost")
        assert exc.value.status_code == 404


class TestHooks:
    @pytest.mark.asyncio
    async def test_list_hooks_returns_points(self, monkeypatch: pytest.MonkeyPatch) -> None:
        monkeypatch.setattr(
            chaos_routes,
            "list_chaos_injection_points",
            lambda: [ChaosInjectionPoint.VENUE_LATENCY, ChaosInjectionPoint.RPC_TIMEOUT],
        )
        result = await list_hooks()
        assert "venue_latency" in result
        assert "rpc_timeout" in result

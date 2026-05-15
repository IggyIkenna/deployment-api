"""Unit tests for manual_pending.py — pvl-p23c execution-service unhold path.

Tests:
- Mock mode: approve/reject work on in-process queue
- Production mode: approve/reject forward to execution-service via httpx
- 404 on missing instruction in mock mode
- 502 on execution-service unreachable in production mode
"""

from __future__ import annotations

from collections.abc import Generator
from unittest.mock import AsyncMock, MagicMock, patch

import pytest
from fastapi import FastAPI
from fastapi.testclient import TestClient
from unified_trading_library import setup_events

import deployment_api.routes.manual_pending as _mod
from deployment_api.routes.manual_pending import router

setup_events("deployment-api", "test")

_PATCH_MOCK = "deployment_api.deployment_api_config.DeploymentApiConfig.is_mock_mode"
_PATCH_CFG_URL = "deployment_api.routes.manual_pending._cfg.execution_service_url"


@pytest.fixture
def mock_client() -> Generator[TestClient]:
    _mod._PENDING.clear()
    with patch(_PATCH_MOCK, return_value=True):
        app = FastAPI()
        app.include_router(router)
        yield TestClient(app)
    _mod._PENDING.clear()


@pytest.fixture
def prod_client() -> Generator[TestClient]:
    _mod._PENDING.clear()
    with patch(_PATCH_MOCK, return_value=False):
        app = FastAPI()
        app.include_router(router)
        yield TestClient(app)
    _mod._PENDING.clear()


class TestMockModeApprove:
    def test_approve_removes_from_queue(self, mock_client: TestClient) -> None:
        r = mock_client.get("/manual/pending")
        assert r.status_code == 200
        instructions = r.json()
        assert len(instructions) >= 1
        instr_id = instructions[0]["instruction_id"]

        r = mock_client.post(f"/manual/pending/{instr_id}/approve")
        assert r.status_code == 200
        body = r.json()
        assert body["action"] == "approved"
        assert body["instruction_id"] == instr_id

    def test_approve_unknown_id_returns_404(self, mock_client: TestClient) -> None:
        r = mock_client.post("/manual/pending/does-not-exist/approve")
        assert r.status_code == 404

    def test_reject_removes_from_queue(self, mock_client: TestClient) -> None:
        r = mock_client.get("/manual/pending")
        assert r.status_code == 200
        instructions = r.json()
        assert len(instructions) >= 1
        instr_id = instructions[0]["instruction_id"]

        r = mock_client.post(f"/manual/pending/{instr_id}/reject", json={"reason": "stale signal"})
        assert r.status_code == 200
        body = r.json()
        assert body["action"] == "rejected"


class TestProductionApprove:
    def test_approve_forwards_to_execution_service(self, prod_client: TestClient) -> None:
        mock_resp = MagicMock()
        mock_resp.status_code = 200

        with (
            patch(_PATCH_CFG_URL, new="http://localhost:8018"),
            patch("deployment_api.routes.manual_pending.httpx.AsyncClient") as mock_ac,
        ):
            mock_ac.return_value.__aenter__ = AsyncMock(
                return_value=MagicMock(post=AsyncMock(return_value=mock_resp))
            )
            mock_ac.return_value.__aexit__ = AsyncMock(return_value=False)
            r = prod_client.post("/manual/pending/instr-001/approve")

        assert r.status_code == 200
        assert r.json()["action"] == "approved"

    def test_approve_502_on_unreachable(self, prod_client: TestClient) -> None:
        import httpx as _httpx

        with (
            patch(_PATCH_CFG_URL, new="http://localhost:8018"),
            patch("deployment_api.routes.manual_pending.httpx.AsyncClient") as mock_ac,
        ):
            mock_ac.return_value.__aenter__ = AsyncMock(
                return_value=MagicMock(post=AsyncMock(side_effect=_httpx.ConnectError("refused")))
            )
            mock_ac.return_value.__aexit__ = AsyncMock(return_value=False)
            r = prod_client.post("/manual/pending/instr-001/approve")

        assert r.status_code == 502

    def test_reject_forwards_to_execution_service(self, prod_client: TestClient) -> None:
        mock_resp = MagicMock()
        mock_resp.status_code = 200

        with (
            patch(_PATCH_CFG_URL, new="http://localhost:8018"),
            patch("deployment_api.routes.manual_pending.httpx.AsyncClient") as mock_ac,
        ):
            mock_ac.return_value.__aenter__ = AsyncMock(
                return_value=MagicMock(post=AsyncMock(return_value=mock_resp))
            )
            mock_ac.return_value.__aexit__ = AsyncMock(return_value=False)
            r = prod_client.post("/manual/pending/instr-001/reject", json={"reason": "stale"})

        assert r.status_code == 200
        assert r.json()["action"] == "rejected"

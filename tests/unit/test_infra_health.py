"""Unit tests for infra_health() — covers real-mode (non-mock) code paths.

Tests call the route function directly to avoid the health_router catch-all
(/{full_path:path}) which intercepts /infra/health at the HTTP layer.
"""

from __future__ import annotations

from unittest.mock import MagicMock, patch

import pytest
from unified_trading_library import setup_events

setup_events("deployment-api", "test")

_PATCH_IS_MOCK = "deployment_api.deployment_api_config.DeploymentApiConfig.is_mock_mode"
_PATCH_STORAGE = "unified_trading_library.get_storage_client"
_PATCH_SECRET = "unified_trading_library.get_secret_client"
_PATCH_AUTH_CFG = "deployment_api.auth.auth_cfg"
_PATCH_STATE_BUCKET = "deployment_api.settings.STATE_BUCKET"


class TestInfraHealthMockMode:
    @pytest.mark.asyncio
    async def test_mock_mode_returns_ok(self) -> None:
        with patch(_PATCH_IS_MOCK, return_value=True):
            from deployment_api.routes.infra_health import infra_health

            body = await infra_health()

        assert body["status"] == "ok"
        assert body["project_id"] == "mock-project"
        assert len(body["checks"]) == 2  # type: ignore[arg-type]


class TestInfraHealthRealMode:
    """Cover lines 54-127: real-mode GCS + secret manager checks."""

    @pytest.mark.asyncio
    async def test_all_checks_ok(self) -> None:
        mock_storage = MagicMock()
        mock_bucket = MagicMock()
        mock_bucket.exists.return_value = True
        mock_storage.bucket.return_value = mock_bucket

        mock_auth = MagicMock()
        mock_auth.gcp_project_id = "test-project"

        with (
            patch(_PATCH_IS_MOCK, return_value=False),
            patch(_PATCH_STORAGE, return_value=mock_storage),
            patch(_PATCH_SECRET, return_value=MagicMock()),
            patch(_PATCH_AUTH_CFG, mock_auth),
            patch(_PATCH_STATE_BUCKET, "test-state-bucket"),
        ):
            from deployment_api.routes.infra_health import infra_health

            body = await infra_health()

        assert body["status"] == "ok"
        checks = {c["name"]: c["status"] for c in body["checks"]}  # type: ignore[union-attr]
        assert checks["gcs_state_bucket"] == "ok"
        assert checks["secret_manager"] == "ok"

    @pytest.mark.asyncio
    async def test_no_state_bucket_gives_skip(self) -> None:
        mock_auth = MagicMock()
        mock_auth.gcp_project_id = "test-project"

        with (
            patch(_PATCH_IS_MOCK, return_value=False),
            patch(_PATCH_STORAGE, return_value=MagicMock()),
            patch(_PATCH_SECRET, return_value=MagicMock()),
            patch(_PATCH_AUTH_CFG, mock_auth),
            patch(_PATCH_STATE_BUCKET, None),
        ):
            from deployment_api.routes.infra_health import infra_health

            body = await infra_health()

        checks = {c["name"]: c["status"] for c in body["checks"]}  # type: ignore[union-attr]
        assert checks["gcs_state_bucket"] == "skip"
        assert body["status"] == "ok"

    @pytest.mark.asyncio
    async def test_storage_raises_gives_degraded(self) -> None:
        mock_auth = MagicMock()
        mock_auth.gcp_project_id = "test-project"

        with (
            patch(_PATCH_IS_MOCK, return_value=False),
            patch(_PATCH_STORAGE, side_effect=OSError("no bucket")),
            patch(_PATCH_SECRET, return_value=MagicMock()),
            patch(_PATCH_AUTH_CFG, mock_auth),
            patch(_PATCH_STATE_BUCKET, "test-bucket"),
        ):
            from deployment_api.routes.infra_health import infra_health

            body = await infra_health()

        assert body["status"] == "degraded"
        checks = {c["name"]: c["status"] for c in body["checks"]}  # type: ignore[union-attr]
        assert checks["gcs_state_bucket"] == "error"

    @pytest.mark.asyncio
    async def test_secret_raises_gives_degraded(self) -> None:
        mock_storage = MagicMock()
        mock_bucket = MagicMock()
        mock_bucket.exists.return_value = True
        mock_storage.bucket.return_value = mock_bucket

        mock_auth = MagicMock()
        mock_auth.gcp_project_id = "test-project"

        with (
            patch(_PATCH_IS_MOCK, return_value=False),
            patch(_PATCH_STORAGE, return_value=mock_storage),
            patch(_PATCH_SECRET, side_effect=RuntimeError("secret down")),
            patch(_PATCH_AUTH_CFG, mock_auth),
            patch(_PATCH_STATE_BUCKET, "test-bucket"),
        ):
            from deployment_api.routes.infra_health import infra_health

            body = await infra_health()

        assert body["status"] == "degraded"
        checks = {c["name"]: c["status"] for c in body["checks"]}  # type: ignore[union-attr]
        assert checks["secret_manager"] == "error"

    @pytest.mark.asyncio
    async def test_top_level_exception_returns_error(self) -> None:
        """Top-level except catches unexpected exceptions in real mode."""
        # AttributeError is NOT in the inner GCS handler's (OSError/ValueError/RuntimeError),
        # so it propagates past the inner except to the outer except Exception handler.
        mock_auth = MagicMock()
        mock_auth.gcp_project_id = "test-project"

        with (
            patch(_PATCH_IS_MOCK, return_value=False),
            patch(_PATCH_STORAGE, side_effect=AttributeError("catastrophic")),
            patch(_PATCH_SECRET, return_value=MagicMock()),
            patch(_PATCH_AUTH_CFG, mock_auth),
            patch(_PATCH_STATE_BUCKET, "test-bucket"),
        ):
            from deployment_api.routes import infra_health as _mod

            body = await _mod.infra_health()

        assert body["status"] == "error"
        assert "catastrophic" in str(body.get("error", ""))

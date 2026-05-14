"""Unit tests for tarball-from-local env-locking guard (B-001).

Plan: deployment_and_qg_strategy_implementation_2026_05_13.md Phase 1.

Covers assert_tarball_not_blocked helper + the six env x override cells:
  dev   / no-override  → allowed (no error, no audit log)
  dev   / override     → allowed (no error, no audit log — not a blocked env)
  staging / no-override → DeployMissingError
  staging / override    → allowed + AUDIT warning emitted
  production / no-override → DeployMissingError
  production / override    → allowed + AUDIT warning emitted
  prod (alias) / no-override → DeployMissingError

Also covers structured log_event audit events (Phase 1 audit log wire-in):
  TARBALL_DEPLOY_ATTEMPTED  — emitted when allowed (non-blocked env)
  TARBALL_DEPLOY_BLOCKED    — emitted when rejected (blocked env, no override)
  TARBALL_DEPLOY_OVERRIDE   — emitted when override bypasses block
"""

from __future__ import annotations

import logging
from unittest.mock import patch

import pytest

from deployment_api.services.deploy_missing import DeployMissingError, assert_tarball_not_blocked


class TestAssertTarballNotBlocked:
    def test_development_allows_tarball_no_override(self) -> None:
        assert_tarball_not_blocked("development")  # must not raise

    def test_development_allows_tarball_with_override(
        self, caplog: pytest.LogCaptureFixture
    ) -> None:
        with caplog.at_level(logging.WARNING, logger="deployment_api.services.deploy_missing"):
            assert_tarball_not_blocked("development", override=True)
        assert "AUDIT" not in caplog.text

    def test_staging_blocks_tarball_without_override(self) -> None:
        with pytest.raises(DeployMissingError, match="staging"):
            assert_tarball_not_blocked("staging")

    def test_staging_override_succeeds_and_emits_audit(
        self, caplog: pytest.LogCaptureFixture
    ) -> None:
        with caplog.at_level(logging.WARNING, logger="deployment_api.services.deploy_missing"):
            assert_tarball_not_blocked("staging", override=True)  # must not raise
        assert "AUDIT" in caplog.text
        assert "staging" in caplog.text

    def test_production_blocks_tarball_without_override(self) -> None:
        with pytest.raises(DeployMissingError, match="production"):
            assert_tarball_not_blocked("production")

    def test_production_override_succeeds_and_emits_audit(
        self, caplog: pytest.LogCaptureFixture
    ) -> None:
        with caplog.at_level(logging.WARNING, logger="deployment_api.services.deploy_missing"):
            assert_tarball_not_blocked("production", override=True)  # must not raise
        assert "AUDIT" in caplog.text
        assert "production" in caplog.text

    def test_prod_alias_blocks_tarball_without_override(self) -> None:
        with pytest.raises(DeployMissingError):
            assert_tarball_not_blocked("prod")

    def test_error_message_references_ssot(self) -> None:
        with pytest.raises(DeployMissingError, match=r"vm-tarball-deployment\.md"):
            assert_tarball_not_blocked("staging")


class TestAuditLogWireIn:
    """Verify structured log_event calls are emitted for each tarball-deploy outcome."""

    def test_allowed_env_emits_tarball_deploy_attempted(self) -> None:
        with patch("deployment_api.services.deploy_missing.log_event") as mock_log:
            assert_tarball_not_blocked("development")
        mock_log.assert_called_once()
        call_kwargs = mock_log.call_args
        assert call_kwargs.args[0] == "TARBALL_DEPLOY_ATTEMPTED"
        assert call_kwargs.kwargs.get("details", {}).get("outcome") == "allowed"

    def test_blocked_env_emits_tarball_deploy_blocked(self) -> None:
        with patch("deployment_api.services.deploy_missing.log_event") as mock_log:
            with pytest.raises(DeployMissingError):
                assert_tarball_not_blocked("staging")
        mock_log.assert_called_once()
        call_kwargs = mock_log.call_args
        assert call_kwargs.args[0] == "TARBALL_DEPLOY_BLOCKED"
        assert call_kwargs.kwargs.get("details", {}).get("outcome") == "rejected"

    def test_override_emits_tarball_deploy_override(self) -> None:
        with patch("deployment_api.services.deploy_missing.log_event") as mock_log:
            assert_tarball_not_blocked("production", override=True)
        mock_log.assert_called_once()
        call_kwargs = mock_log.call_args
        assert call_kwargs.args[0] == "TARBALL_DEPLOY_OVERRIDE"
        assert call_kwargs.kwargs.get("details", {}).get("outcome") == "override_allowed"

    def test_log_event_failure_does_not_propagate(self) -> None:
        with patch(
            "deployment_api.services.deploy_missing.log_event",
            side_effect=RuntimeError("events not initialized"),
        ):
            assert_tarball_not_blocked("development")  # must not raise

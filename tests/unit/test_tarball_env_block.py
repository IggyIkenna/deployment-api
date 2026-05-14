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
"""

from __future__ import annotations

import logging

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

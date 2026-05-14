"""Compliance tests for treasury withdrawal + Cloud Audit Log wiring.

Phase 3 of wallet_treasury_post_cutover_custody_signing_2026_06_01.md.

Coverage (PB-1 / PB-3 compliance):
1. Cloud Audit Log emitted on withdrawal request (log_struct called w/ correct payload)
2. Cloud Audit Log failure does NOT block withdrawal (returns 200)
3. Withdrawal endpoint returns pending_approval status on happy path
4. Two sequential calls produce distinct request_id values (UUID uniqueness)
"""

from __future__ import annotations

from datetime import UTC, datetime
from decimal import Decimal
from unittest.mock import MagicMock, patch

import pytest
from fastapi import HTTPException
from unified_api_contracts.internal.domain.client_lifecycle import (
    ClientShareClassSubscription,
)

from deployment_api.routes.client_treasury import (
    WithdrawalRequest,
    _emit_cloud_audit_log,
    get_subscription_store,
    post_client_treasury_withdraw,
    set_subscription_store,
)

# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------


def _make_subscription(client_id: str = "client-alpha") -> ClientShareClassSubscription:
    return ClientShareClassSubscription(
        client_id=client_id,
        share_class_id="USDC_CUTOVER_V1",
        archetype_id="carry_staked_basis",
        allocation_pct=Decimal("100"),
        subscribed_at=datetime(2026, 5, 10, tzinfo=UTC),
    )


def _make_withdrawal_request() -> WithdrawalRequest:
    return WithdrawalRequest(
        amount=10_000.0,
        currency="USDC",
        destination="0xDeadBeef0000000000000000000000000000BEEF",
    )


@pytest.fixture(autouse=True)
def _isolated_store() -> None:  # type: ignore[return]
    """Restore the global subscription store after each test."""
    original = dict(get_subscription_store())
    yield
    set_subscription_store(original)


# ---------------------------------------------------------------------------
# Test 1 — Cloud Audit Log emitted on withdrawal request
# ---------------------------------------------------------------------------


class TestCloudAuditLogEmittedOnWithdrawalRequest:
    @pytest.mark.asyncio
    async def test_cloud_audit_log_emitted_on_withdrawal_request(self) -> None:
        """log_struct called with correct payload shape when withdrawal is requested."""
        set_subscription_store({"client-alpha": [_make_subscription("client-alpha")]})

        mock_logger = MagicMock()
        mock_gcp_logger_instance = MagicMock()
        mock_gcp_logger_instance.logger.return_value = mock_logger

        with patch("google.cloud.logging.Client", return_value=mock_gcp_logger_instance):
            resp = await post_client_treasury_withdraw("client-alpha", _make_withdrawal_request())

        # The mock_gcp_logger_instance.logger() call returns mock_logger;
        # log_struct should have been called on it.
        mock_gcp_logger_instance.logger.assert_called_once()
        log_name_arg = mock_gcp_logger_instance.logger.call_args[0][0]
        assert "cloudaudit.googleapis.com" in log_name_arg

        mock_logger.log_struct.assert_called_once()
        payload = mock_logger.log_struct.call_args[0][0]

        assert payload["operation"] == "withdrawal_request"
        assert payload["client_id"] == "client-alpha"
        assert payload["service"] == "deployment-api"
        assert "timestamp" in payload
        assert payload["request_id"] == resp.request_id
        assert payload["currency"] == "USDC"


# ---------------------------------------------------------------------------
# Test 2 — Cloud Audit Log failure does NOT block the withdrawal
# ---------------------------------------------------------------------------


class TestCloudAuditLogFailureDoesNotBlockWithdrawal:
    @pytest.mark.asyncio
    async def test_cloud_audit_log_failure_does_not_block_withdrawal(self) -> None:
        """When google.cloud.logging.Client raises, withdrawal still returns 200."""
        set_subscription_store({"client-alpha": [_make_subscription("client-alpha")]})

        with patch(
            "google.cloud.logging.Client",
            side_effect=Exception("GCP credentials unavailable"),
        ):
            resp = await post_client_treasury_withdraw("client-alpha", _make_withdrawal_request())

        # Withdrawal proceeds despite audit log failure
        assert resp.status == "pending_approval"
        assert resp.request_id  # non-empty UUID

    @pytest.mark.asyncio
    async def test_emit_helper_swallows_exception(self) -> None:
        """_emit_cloud_audit_log never raises regardless of upstream exception."""
        # Call the helper directly — must not propagate any exception
        with patch(
            "google.cloud.logging.Client",
            side_effect=RuntimeError("simulated failure"),
        ):
            # This must complete without raising
            _emit_cloud_audit_log(
                operation="test_op",
                client_id="test-client",
                details={"foo": "bar"},
            )


# ---------------------------------------------------------------------------
# Test 3 — Withdrawal endpoint returns pending_approval status on happy path
# ---------------------------------------------------------------------------


class TestWithdrawalEndpointHappyPath:
    @pytest.mark.asyncio
    async def test_withdrawal_endpoint_returns_pending_approval_status(self) -> None:
        """Happy path: known client + valid body → pending_approval with audit_logged=True."""
        set_subscription_store({"client-alpha": [_make_subscription("client-alpha")]})

        with patch("google.cloud.logging.Client"):
            resp = await post_client_treasury_withdraw("client-alpha", _make_withdrawal_request())

        assert resp.status == "pending_approval"
        assert resp.audit_logged is True
        assert resp.request_id  # non-empty string (UUID4)

    @pytest.mark.asyncio
    async def test_withdrawal_unknown_client_returns_404(self) -> None:
        """Unknown client_id → 404 HTTPException; no audit log attempted."""
        set_subscription_store({})

        with pytest.raises(HTTPException) as exc:
            await post_client_treasury_withdraw("ghost-client", _make_withdrawal_request())

        assert exc.value.status_code == 404


# ---------------------------------------------------------------------------
# Test 4 — Two sequential calls produce distinct request_id values
# ---------------------------------------------------------------------------


class TestWithdrawalRequestIdIsUnique:
    @pytest.mark.asyncio
    async def test_withdrawal_request_id_is_unique_per_call(self) -> None:
        """Two sequential withdrawal calls return distinct request_id values."""
        set_subscription_store({"client-alpha": [_make_subscription("client-alpha")]})

        with patch("google.cloud.logging.Client"):
            resp1 = await post_client_treasury_withdraw(
                "client-alpha", _make_withdrawal_request()
            )
            resp2 = await post_client_treasury_withdraw(
                "client-alpha", _make_withdrawal_request()
            )

        assert resp1.request_id != resp2.request_id, (
            f"Expected unique request_ids, got {resp1.request_id!r} twice"
        )

"""Compliance tests for treasury withdrawal approval + Cloud Audit Log wiring.

Phase 1 of wallet_treasury_post_cutover_custody_signing_2026_06_01.md.

Coverage (PB-1 / PB-3 compliance):
1. approve_withdrawal happy path — quorum reached after single SMALL (<=$10k) approval
2. approve_withdrawal unknown client → 404
3. approve_withdrawal invalid treasury_source → 400
4. approve_withdrawal invalid amount_usd → 400
5. approve_withdrawal invalid signed_at → 400
6. approve_withdrawal approver not in pool → 400
7. Cloud Audit Log emitted on withdrawal approval
8. Cloud Audit Log failure does NOT block the approval (returns 200)
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
    _WITHDRAWAL_CHAINS,
    WithdrawalApproveRequest,
    _emit_cloud_audit_log,
    approve_withdrawal,
    get_subscription_store,
    set_subscription_store,
)

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

_TEST_HMAC = "aabbccdd" * 8  # 64 hex chars, valid hex — not cryptographically correct
_TEST_SIGNED_AT = datetime(2026, 5, 14, 10, 0, 0, tzinfo=UTC).isoformat()


def _make_subscription(client_id: str = "client-alpha") -> ClientShareClassSubscription:
    return ClientShareClassSubscription(
        client_id=client_id,
        share_class_id="USDC_CUTOVER_V1",
        archetype_id="carry_staked_basis",
        allocation_pct=Decimal("100"),
        subscribed_at=datetime(2026, 5, 10, tzinfo=UTC),
    )


def _small_approval_body(approver_id: str = "operator:ikenna") -> WithdrawalApproveRequest:
    return WithdrawalApproveRequest(
        approver_id=approver_id,
        amount_usd="1000.00",
        treasury_source="DEFI_HOT_WALLET",
        hmac_signature=_TEST_HMAC,
        signed_at=_TEST_SIGNED_AT,
    )


@pytest.fixture(autouse=True)
def _isolated_store() -> None:  # type: ignore[return]
    """Restore the global subscription store and withdrawal chains after each test."""
    original = dict(get_subscription_store())
    original_chains = dict(_WITHDRAWAL_CHAINS)
    _WITHDRAWAL_CHAINS.clear()
    yield
    set_subscription_store(original)
    _WITHDRAWAL_CHAINS.clear()
    _WITHDRAWAL_CHAINS.update(original_chains)


# ---------------------------------------------------------------------------
# Test 1 — Happy path: small withdrawal reaches quorum with single approval
# ---------------------------------------------------------------------------


class TestApproveWithdrawalHappyPath:
    @pytest.mark.asyncio
    async def test_small_withdrawal_reaches_quorum_in_one_approval(self) -> None:
        """SMALL withdrawal (<=10k) requires 1 approver → quorum after first call."""
        set_subscription_store({"client-alpha": [_make_subscription("client-alpha")]})

        with patch("google.cloud.logging.Client"):
            resp = await approve_withdrawal("client-alpha", "wid-001", _small_approval_body())

        assert resp.withdrawal_id == "wid-001"
        assert resp.client_id == "client-alpha"
        assert resp.approver_id == "operator:ikenna"
        assert resp.quorum_reached is True
        assert resp.approver_count == 1
        assert resp.approvers_remaining == 0
        assert resp.quorum_reached_at is not None

    @pytest.mark.asyncio
    async def test_second_approval_raises_400_when_quorum_already_reached(self) -> None:
        """Once quorum is reached, further approvals → 400."""
        set_subscription_store({"client-alpha": [_make_subscription("client-alpha")]})

        with patch("google.cloud.logging.Client"):
            await approve_withdrawal("client-alpha", "wid-001", _small_approval_body())

        with pytest.raises(HTTPException) as exc:
            with patch("google.cloud.logging.Client"):
                await approve_withdrawal(
                    "client-alpha", "wid-001", _small_approval_body("operator:harsh")
                )

        assert exc.value.status_code == 400
        assert "quorum already reached" in exc.value.detail["message"]


# ---------------------------------------------------------------------------
# Test 2 — Unknown client → 404
# ---------------------------------------------------------------------------


class TestApproveWithdrawalUnknownClient:
    @pytest.mark.asyncio
    async def test_unknown_client_returns_404(self) -> None:
        set_subscription_store({})

        with pytest.raises(HTTPException) as exc:
            await approve_withdrawal("ghost-client", "wid-002", _small_approval_body())

        assert exc.value.status_code == 404


# ---------------------------------------------------------------------------
# Test 3 — Validation errors
# ---------------------------------------------------------------------------


class TestApproveWithdrawalValidation:
    @pytest.mark.asyncio
    async def test_invalid_treasury_source_returns_400(self) -> None:
        set_subscription_store({"client-alpha": [_make_subscription("client-alpha")]})

        body = WithdrawalApproveRequest(
            approver_id="operator:ikenna",
            amount_usd="1000",
            treasury_source="NOT_A_REAL_SOURCE",
            hmac_signature=_TEST_HMAC,
            signed_at=_TEST_SIGNED_AT,
        )
        with pytest.raises(HTTPException) as exc:
            await approve_withdrawal("client-alpha", "wid-003", body)

        assert exc.value.status_code == 400
        assert "treasury_source" in exc.value.detail["message"]

    @pytest.mark.asyncio
    async def test_invalid_amount_usd_returns_400(self) -> None:
        set_subscription_store({"client-alpha": [_make_subscription("client-alpha")]})

        body = WithdrawalApproveRequest(
            approver_id="operator:ikenna",
            amount_usd="not-a-number",
            treasury_source="DEFI_HOT_WALLET",
            hmac_signature=_TEST_HMAC,
            signed_at=_TEST_SIGNED_AT,
        )
        with pytest.raises(HTTPException) as exc:
            await approve_withdrawal("client-alpha", "wid-004", body)

        assert exc.value.status_code == 400
        assert "amount_usd" in exc.value.detail["message"]

    @pytest.mark.asyncio
    async def test_invalid_signed_at_returns_400(self) -> None:
        set_subscription_store({"client-alpha": [_make_subscription("client-alpha")]})

        body = WithdrawalApproveRequest(
            approver_id="operator:ikenna",
            amount_usd="1000",
            treasury_source="DEFI_HOT_WALLET",
            hmac_signature=_TEST_HMAC,
            signed_at="not-a-timestamp",
        )
        with pytest.raises(HTTPException) as exc:
            await approve_withdrawal("client-alpha", "wid-005", body)

        assert exc.value.status_code == 400
        assert "signed_at" in exc.value.detail["message"]

    @pytest.mark.asyncio
    async def test_approver_not_in_pool_returns_400(self) -> None:
        set_subscription_store({"client-alpha": [_make_subscription("client-alpha")]})

        body = WithdrawalApproveRequest(
            approver_id="operator:unknown",
            amount_usd="1000",
            treasury_source="DEFI_HOT_WALLET",
            hmac_signature=_TEST_HMAC,
            signed_at=_TEST_SIGNED_AT,
        )
        with pytest.raises(HTTPException) as exc:
            with patch("google.cloud.logging.Client"):
                await approve_withdrawal("client-alpha", "wid-006", body)

        assert exc.value.status_code == 400
        assert "not in pool" in exc.value.detail["message"]


# ---------------------------------------------------------------------------
# Test 4 — Cloud Audit Log emitted on approval
# ---------------------------------------------------------------------------


class TestCloudAuditLogEmittedOnApproval:
    @pytest.mark.asyncio
    async def test_cloud_audit_log_emitted_on_approval(self) -> None:
        """log_struct called with correct payload shape on approval."""
        set_subscription_store({"client-alpha": [_make_subscription("client-alpha")]})

        mock_logger = MagicMock()
        mock_gcp_logger_instance = MagicMock()
        mock_gcp_logger_instance.logger.return_value = mock_logger

        with patch("google.cloud.logging.Client", return_value=mock_gcp_logger_instance):
            await approve_withdrawal("client-alpha", "wid-007", _small_approval_body())

        mock_gcp_logger_instance.logger.assert_called_once()
        log_name_arg = mock_gcp_logger_instance.logger.call_args[0][0]
        assert "cloudaudit.googleapis.com" in log_name_arg

        mock_logger.log_struct.assert_called_once()
        payload = mock_logger.log_struct.call_args[0][0]
        assert payload["operation"] == "withdrawal_approval"
        assert payload["client_id"] == "client-alpha"
        assert payload["service"] == "deployment-api"
        assert "timestamp" in payload
        assert payload["withdrawal_id"] == "wid-007"


# ---------------------------------------------------------------------------
# Test 5 — Cloud Audit Log failure does NOT block approval
# ---------------------------------------------------------------------------


class TestCloudAuditLogFailureDoesNotBlockApproval:
    @pytest.mark.asyncio
    async def test_cloud_audit_log_failure_does_not_block_approval(self) -> None:
        """When google.cloud.logging.Client raises, approval still succeeds."""
        set_subscription_store({"client-alpha": [_make_subscription("client-alpha")]})

        with patch(
            "google.cloud.logging.Client",
            side_effect=Exception("GCP credentials unavailable"),
        ):
            resp = await approve_withdrawal("client-alpha", "wid-008", _small_approval_body())

        assert resp.quorum_reached is True

    @pytest.mark.asyncio
    async def test_emit_helper_swallows_exception(self) -> None:
        """_emit_cloud_audit_log never raises regardless of upstream exception."""
        with patch(
            "google.cloud.logging.Client",
            side_effect=RuntimeError("simulated failure"),
        ):
            _emit_cloud_audit_log(
                operation="test_op",
                client_id="test-client",
                details={"foo": "bar"},
            )

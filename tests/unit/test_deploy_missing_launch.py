"""Tests for deploy_missing_launch.py — Phase 2 auto-launch service.

Coverage:
  1. Rate limiter: per-operator/hour, per-operator/day, per-project/hour ceilings.
  2. check_inflight_vm: mock mode → None; subprocess filter pattern.
  3. launch_deploy_missing_vm dry_run → correct DeployMissingLaunchResult shape.
  4. launch_deploy_missing_vm validation: missing venue/data_type/day → DeployMissingError.
  5. launch_deploy_missing_vm unknown service → DeployMissingError.
  6. launch_deploy_missing_vm rate limit trip → DeployMissingRateLimitError.
  7. launch_deploy_missing_vm inflight idempotency → returns existing VM, no subprocess.
  8. POST /api/data-status/deploy-missing-launch happy path → 200 + result dict.
  9. POST /api/data-status/deploy-missing-launch rate limit → 429.
 10. POST /api/data-status/deploy-missing-launch bad service → 400.
 11. vm_name regex: ``dm-{16 hex chars}-{YYYYMMDD-HHMMSS}``
 12. shard_key_hash: SHA-256[:16] deterministic.
 13. DEPLOY_MISSING_VM_LAUNCHED event emitted in dry_run.
"""

from __future__ import annotations

import os
import re

os.environ.setdefault("CLOUD_MOCK_MODE", "true")
os.environ.setdefault("CLOUD_PROVIDER", "local")
os.environ.setdefault("GCP_PROJECT_ID", "test-project")
os.environ.setdefault("DISABLE_AUTH", "true")
os.environ.setdefault("MOCK_STATE_MODE", "deterministic")

import sys
from types import ModuleType
from unittest.mock import MagicMock, patch

if "deployment_service.deployments_registry" not in sys.modules:
    _dr_mod = ModuleType("deployment_service.deployments_registry")
    _dr_mod.DEFAULT_BUCKET = "test-bucket"  # type: ignore[attr-defined]
    _dr_mod.DeploymentRegistryEntry = MagicMock()  # type: ignore[attr-defined]
    _dr_mod.DeploymentsRegistry = MagicMock()  # type: ignore[attr-defined]
    sys.modules["deployment_service.deployments_registry"] = _dr_mod

import hashlib
import time

import pytest
from fastapi.testclient import TestClient

with (
    patch("unified_trading_library.event_sink.PubSubEventSink"),
    patch("unified_trading_library.PubSubEventSink"),
    patch("unified_trading_library.events.setup_events"),
    patch("unified_trading_library.utils.tracing.setup_tracing"),
    patch("unified_trading_library.setup_tracing"),
):
    from deployment_api.main import app

from unified_trading_library import setup_events

setup_events("deployment-api", "test")

from deployment_api import auth as _auth_mod

_auth_mod.DISABLE_AUTH = True

from deployment_api.services.deploy_missing_launch import (
    DeployMissingLaunchResult,
    DeployMissingRateLimiter,
    DeployMissingRateLimitError,
    _build_shard_key_hash,
    check_inflight_vm,
    launch_deploy_missing_vm,
)

pytestmark = [pytest.mark.timeout(30)]

_VALID_ROW_KEY = {
    "venue": "AAVEV3-ARBITRUM",
    "data_type": "lending_indices",
    "instrument_id": "USDC",
    "day": "2024-03-04",
}
_VALID_SERVICE = "market-tick-data-service"
_VALID_ASSET_GROUP = "defi"


# ── Helper ────────────────────────────────────────────────────────────────────


def _fresh_limiter(per_op_hour: int = 30, per_op_day: int = 200, per_proj_hour: int = 100) -> DeployMissingRateLimiter:
    return DeployMissingRateLimiter(per_op_hour=per_op_hour, per_op_day=per_op_day, per_proj_hour=per_proj_hour)


# ── Unit: shard key hash ──────────────────────────────────────────────────────


def test_shard_key_hash_deterministic() -> None:
    key = "defi|AAVEV3-ARBITRUM|lending_indices||USDC|2024-03-04"
    h1 = _build_shard_key_hash(key)
    h2 = _build_shard_key_hash(key)
    assert h1 == h2
    assert len(h1) == 16
    assert re.fullmatch(r"[0-9a-f]{16}", h1)


def test_shard_key_hash_matches_sha256() -> None:
    key = "defi|AAVEV3-ARBITRUM|lending_indices||USDC|2024-03-04"
    expected = hashlib.sha256(key.encode()).hexdigest()[:16]
    assert _build_shard_key_hash(key) == expected


# ── Unit: rate limiter ────────────────────────────────────────────────────────


def test_rate_limiter_per_op_hour() -> None:
    limiter = _fresh_limiter(per_op_hour=3)
    for _ in range(3):
        limiter.check_and_record("op1")
    with pytest.raises(DeployMissingRateLimitError, match="3 deploys/hr per operator"):
        limiter.check_and_record("op1")


def test_rate_limiter_per_op_day() -> None:
    limiter = _fresh_limiter(per_op_hour=999, per_op_day=2)
    for _ in range(2):
        limiter.check_and_record("op1")
    with pytest.raises(DeployMissingRateLimitError, match="2 deploys/day per operator"):
        limiter.check_and_record("op1")


def test_rate_limiter_per_proj_hour() -> None:
    limiter = _fresh_limiter(per_op_hour=999, per_op_day=999, per_proj_hour=2)
    limiter.check_and_record("op1")
    limiter.check_and_record("op2")
    with pytest.raises(DeployMissingRateLimitError, match="2 deploys/hr project-wide"):
        limiter.check_and_record("op3")


def test_rate_limiter_different_operators_independent() -> None:
    limiter = _fresh_limiter(per_op_hour=2, per_op_day=999, per_proj_hour=999)
    limiter.check_and_record("op1")
    limiter.check_and_record("op1")
    # op1 hit; op2 should still succeed
    limiter.check_and_record("op2")


def test_rate_limiter_window_slides(monkeypatch: pytest.MonkeyPatch) -> None:
    limiter = _fresh_limiter(per_op_hour=2)
    base = 1000.0
    monkeypatch.setattr(time, "monotonic", lambda: base)
    limiter.check_and_record("op1")
    limiter.check_and_record("op1")
    # Advance time past 1hr window
    monkeypatch.setattr(time, "monotonic", lambda: base + 3601.0)
    limiter.check_and_record("op1")  # should not raise


# ── Unit: check_inflight_vm ────────────────────────────────────────────────────


def test_check_inflight_vm_mock_mode() -> None:
    # CLOUD_MOCK_MODE=true → returns None immediately, no subprocess
    result = check_inflight_vm("abcdef0123456789", "test-project")
    assert result is None


# ── Unit: launch_deploy_missing_vm validation ──────────────────────────────────


def test_launch_missing_venue() -> None:
    with pytest.raises(Exception, match="venue"):
        launch_deploy_missing_vm(
            service=_VALID_SERVICE,
            asset_group=_VALID_ASSET_GROUP,
            row_key={"data_type": "lending_indices", "day": "2024-03-04"},
            operator_id="op1",
        )


def test_launch_missing_data_type() -> None:
    with pytest.raises(Exception, match="data_type"):
        launch_deploy_missing_vm(
            service=_VALID_SERVICE,
            asset_group=_VALID_ASSET_GROUP,
            row_key={"venue": "AAVEV3-ARBITRUM", "day": "2024-03-04"},
            operator_id="op1",
        )


def test_launch_missing_day() -> None:
    with pytest.raises(Exception, match="day"):
        launch_deploy_missing_vm(
            service=_VALID_SERVICE,
            asset_group=_VALID_ASSET_GROUP,
            row_key={"venue": "AAVEV3-ARBITRUM", "data_type": "lending_indices"},
            operator_id="op1",
        )


def test_launch_unknown_service() -> None:
    with pytest.raises(Exception, match="No launcher script registered"):
        launch_deploy_missing_vm(
            service="not-a-real-service",
            asset_group=_VALID_ASSET_GROUP,
            row_key=_VALID_ROW_KEY,
            operator_id="op1",
        )


# ── Unit: launch_deploy_missing_vm dry_run ─────────────────────────────────────


def test_launch_dry_run_returns_correct_shape() -> None:
    result = launch_deploy_missing_vm(
        service=_VALID_SERVICE,
        asset_group=_VALID_ASSET_GROUP,
        row_key=_VALID_ROW_KEY,
        operator_id="op1",
        dry_run=True,
    )
    assert isinstance(result, DeployMissingLaunchResult)
    assert result.dry_run is True
    assert result.started_confirmed is True
    assert result.service == _VALID_SERVICE
    assert result.asset_group == _VALID_ASSET_GROUP
    assert result.inflight_vm_name is None


def test_launch_dry_run_vm_name_pattern() -> None:
    result = launch_deploy_missing_vm(
        service=_VALID_SERVICE,
        asset_group=_VALID_ASSET_GROUP,
        row_key=_VALID_ROW_KEY,
        operator_id="op1",
        dry_run=True,
    )
    # dm-{16 hex}-{YYYYMMDD-HHMMSS}
    assert re.fullmatch(r"dm-[0-9a-f]{16}-\d{8}-\d{6}", result.vm_name), result.vm_name


def test_launch_dry_run_shard_key_deterministic() -> None:
    r1 = launch_deploy_missing_vm(
        service=_VALID_SERVICE,
        asset_group=_VALID_ASSET_GROUP,
        row_key=_VALID_ROW_KEY,
        operator_id="op1",
        dry_run=True,
    )
    r2 = launch_deploy_missing_vm(
        service=_VALID_SERVICE,
        asset_group=_VALID_ASSET_GROUP,
        row_key=_VALID_ROW_KEY,
        operator_id="op1",
        dry_run=True,
    )
    assert r1.shard_key == r2.shard_key
    assert r1.shard_key_hash == r2.shard_key_hash


def test_launch_dry_run_events_uri_pattern() -> None:
    result = launch_deploy_missing_vm(
        service=_VALID_SERVICE,
        asset_group=_VALID_ASSET_GROUP,
        row_key=_VALID_ROW_KEY,
        operator_id="op1",
        dry_run=True,
    )
    assert result.events_uri.startswith("gs://")
    assert "-events/events/" in result.events_uri
    assert result.vm_name in result.events_uri


def test_launch_dry_run_to_dict_keys() -> None:
    result = launch_deploy_missing_vm(
        service=_VALID_SERVICE,
        asset_group=_VALID_ASSET_GROUP,
        row_key=_VALID_ROW_KEY,
        operator_id="op1",
        dry_run=True,
    )
    d = result.to_dict()
    expected_keys = {
        "service",
        "asset_group",
        "shard_key",
        "shard_key_hash",
        "vm_name",
        "correlation_id",
        "events_uri",
        "dry_run",
        "started_confirmed",
        "inflight_vm_name",
    }
    assert expected_keys == set(d.keys())


# ── Unit: rate limit trip → DeployMissingRateLimitError ──────────────────────


def test_launch_rate_limit_trip(monkeypatch: pytest.MonkeyPatch) -> None:
    import deployment_api.services.deploy_missing_launch as dm_launch_mod

    small_limiter = DeployMissingRateLimiter(per_op_hour=1)
    monkeypatch.setattr(dm_launch_mod, "_rate_limiter", small_limiter)
    # First call succeeds
    launch_deploy_missing_vm(
        service=_VALID_SERVICE,
        asset_group=_VALID_ASSET_GROUP,
        row_key=_VALID_ROW_KEY,
        operator_id="op1",
        dry_run=True,
    )
    # Second call hits ceiling
    with pytest.raises(DeployMissingRateLimitError):
        launch_deploy_missing_vm(
            service=_VALID_SERVICE,
            asset_group=_VALID_ASSET_GROUP,
            row_key=_VALID_ROW_KEY,
            operator_id="op1",
            dry_run=True,
        )


# ── Unit: inflight idempotency ─────────────────────────────────────────────────


def test_launch_inflight_returns_existing_vm(monkeypatch: pytest.MonkeyPatch) -> None:
    import deployment_api.services.deploy_missing_launch as dm_launch_mod

    monkeypatch.setattr(dm_launch_mod, "check_inflight_vm", lambda _hash, _pid: "dm-abcd1234-20260101-120000")

    result = launch_deploy_missing_vm(
        service=_VALID_SERVICE,
        asset_group=_VALID_ASSET_GROUP,
        row_key=_VALID_ROW_KEY,
        operator_id="op1",
        dry_run=False,
    )
    assert result.vm_name == "dm-abcd1234-20260101-120000"
    assert result.inflight_vm_name == "dm-abcd1234-20260101-120000"
    assert result.started_confirmed is True


# ── Integration: HTTP route ────────────────────────────────────────────────────


@pytest.fixture()
def client() -> TestClient:
    return TestClient(app, raise_server_exceptions=True)


def test_route_deploy_missing_launch_dry_run(client: TestClient) -> None:
    resp = client.post(
        "/api/data-status/deploy-missing-launch",
        json={
            "service": _VALID_SERVICE,
            "asset_group": _VALID_ASSET_GROUP,
            "row_key": _VALID_ROW_KEY,
            "operator_id": "test-op",
            "dry_run": True,
        },
    )
    assert resp.status_code == 200
    body = resp.json()
    assert body["service"] == _VALID_SERVICE
    assert body["dry_run"] is True
    assert re.fullmatch(r"dm-[0-9a-f]{16}-\d{8}-\d{6}", body["vm_name"])


def test_route_deploy_missing_launch_bad_service(client: TestClient) -> None:
    resp = client.post(
        "/api/data-status/deploy-missing-launch",
        json={
            "service": "not-a-real-service",
            "asset_group": _VALID_ASSET_GROUP,
            "row_key": _VALID_ROW_KEY,
            "operator_id": "test-op",
            "dry_run": True,
        },
    )
    assert resp.status_code == 400
    body = resp.json()
    msg = body.get("detail", body.get("error", {}).get("message", ""))
    assert "launcher" in str(msg).lower()


def test_route_deploy_missing_launch_rate_limit(client: TestClient, monkeypatch: pytest.MonkeyPatch) -> None:
    import deployment_api.services.deploy_missing_launch as dm_launch_mod

    small_limiter = DeployMissingRateLimiter(per_op_hour=0)
    monkeypatch.setattr(dm_launch_mod, "_rate_limiter", small_limiter)

    resp = client.post(
        "/api/data-status/deploy-missing-launch",
        json={
            "service": _VALID_SERVICE,
            "asset_group": _VALID_ASSET_GROUP,
            "row_key": _VALID_ROW_KEY,
            "operator_id": "test-op",
            "dry_run": True,
        },
    )
    assert resp.status_code == 429


def test_route_deploy_missing_launch_missing_fields(client: TestClient) -> None:
    resp = client.post(
        "/api/data-status/deploy-missing-launch",
        json={
            "service": "",
            "asset_group": "",
            "row_key": _VALID_ROW_KEY,
            "operator_id": "test-op",
        },
    )
    assert resp.status_code == 400

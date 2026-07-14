"""Unit tests for the stale-while-revalidate cache in routes/vm_deployments.py.

`GET /api/vm-deployments` backs a fully synchronous, uncached full GCS registry walk
(active + N-day archive) plus a per-VM Compute API census — measured live in prod at
avg 93.75s / max 99.27s, occasionally 503/500ing outright under load. This SWR cache
mirrors the already-proven `_load_inventory` pattern in routes/deployments_inventory.py:
instant fresh/stale serve, single-flight background refresh, never poisons the cache
on a failed refresh.
"""

from __future__ import annotations

import os
from dataclasses import dataclass
from unittest.mock import MagicMock, patch

os.environ.setdefault("CLOUD_MOCK_MODE", "false")
os.environ.setdefault("CLOUD_PROVIDER", "local")
os.environ.setdefault("GCP_PROJECT_ID", "test-project")
os.environ.setdefault("DISABLE_AUTH", "true")
os.environ.setdefault("MOCK_STATE_MODE", "deterministic")

with (
    patch("unified_trading_library.event_sink.PubSubEventSink"),
    patch("unified_trading_library.PubSubEventSink"),
    patch("unified_trading_library.events.setup_events"),
    patch("unified_trading_library.utils.tracing.setup_tracing"),
    patch("unified_trading_library.setup_tracing"),
):
    from deployment_api.main import app
    from deployment_api.routes import vm_deployments as _vm_mod

from deployment_api import auth as _auth_mod

_auth_mod.DISABLE_AUTH = True

import pytest
from fastapi.testclient import TestClient

from deployment_api.deployment_api_config import DeploymentApiConfig

pytestmark = [pytest.mark.timeout(60)]


@dataclass
class _FakeEntry:
    """Minimal stand-in for DeploymentRegistryEntry -- the fields `_to_model` touches
    when `vm_details` is empty (the health-status-enrichment branch is then skipped)."""

    deployment_id: str
    vm_name: str
    asset_group: str = "cefi"
    task: str = "backfill"
    mode: str = "live"
    start_date: str = "2026-01-01"
    end_date: str = "2026-01-01"
    status: str = "running"
    started_at: str = "2026-01-01T00:00:00Z"
    last_heartbeat_at: str = "2026-01-01T00:00:00Z"
    completed_at: str | None = None


def setup_function() -> None:
    with _vm_mod._vm_deployments_lock:  # pyright: ignore[reportPrivateUsage]
        _vm_mod._vm_deployments_cache.clear()  # pyright: ignore[reportPrivateUsage]
        _vm_mod._vm_deployments_refreshing.clear()  # pyright: ignore[reportPrivateUsage]


teardown_function = setup_function


@pytest.fixture(scope="module")
def client() -> TestClient:
    return TestClient(app)


# ---------------------------------------------------------------------------
# 1. `_load_vm_deployments` cache mechanics (fresh / stale / cold), isolated
#    from the real registry walk.
# ---------------------------------------------------------------------------


def test_cold_path_computes_synchronously_and_populates_cache() -> None:
    """No snapshot yet -> computed synchronously, under the lock, and cached."""
    cache_key = "7|False"
    computed = _vm_mod.VmDeploymentsListModel(active=[], recent=[], archive_days=7)
    with patch.object(_vm_mod, "_compute_vm_deployments", return_value=computed) as mock_compute:
        result = _vm_mod._load_vm_deployments(7, False)
    mock_compute.assert_called_once_with(7, False)
    assert result is computed
    assert _vm_mod._vm_deployments_cache[cache_key][1] is computed


def test_fresh_cache_hit_never_recomputes() -> None:
    """A snapshot inside the TTL is served instantly -- no registry walk at all."""
    cache_key = "7|True"
    cached_result = _vm_mod.VmDeploymentsListModel(active=[], recent=[], archive_days=7)
    _vm_mod._store_vm_deployments(cache_key, cached_result)
    with patch.object(_vm_mod, "_compute_vm_deployments") as mock_compute:
        result = _vm_mod._load_vm_deployments(7, True)
    mock_compute.assert_not_called()
    assert result is cached_result


def test_stale_cache_serves_snapshot_and_kicks_single_background_refresh(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Past TTL: the OLD snapshot is served instantly (never blocks on a synchronous
    recompute) AND exactly one background refresh is queued (single-flight)."""
    cache_key = "7|True"
    stale_result = _vm_mod.VmDeploymentsListModel(active=[], recent=[], archive_days=7)
    fake_now = [1000.0]
    monkeypatch.setattr(_vm_mod.time, "monotonic", lambda: fake_now[0])
    _vm_mod._vm_deployments_cache[cache_key] = (fake_now[0], stale_result)
    fake_now[0] += _vm_mod._VM_DEPLOYMENTS_TTL_SEC + 1.0

    with (
        patch.object(_vm_mod, "_compute_vm_deployments") as mock_compute,
        patch.object(_vm_mod._vm_deployments_refresh_pool, "submit") as mock_submit,
    ):
        first = _vm_mod._load_vm_deployments(7, True)
        second = _vm_mod._load_vm_deployments(7, True)  # concurrent-ish second stale hit

    assert first is stale_result
    assert second is stale_result
    mock_compute.assert_not_called()  # served path never recomputes synchronously
    # exactly one background refresh queued across both stale hits
    mock_submit.assert_called_once_with(_vm_mod._refresh_vm_deployments, cache_key, 7, True)


def test_kick_background_refresh_is_single_flight_per_cache_key() -> None:
    """Two overlapping kicks for the SAME cache key collapse to one submit."""
    cache_key = "7|True"
    with patch.object(_vm_mod._vm_deployments_refresh_pool, "submit") as mock_submit:
        _vm_mod._kick_background_vm_refresh(cache_key, 7, True)
        _vm_mod._kick_background_vm_refresh(cache_key, 7, True)
    mock_submit.assert_called_once_with(_vm_mod._refresh_vm_deployments, cache_key, 7, True)
    assert cache_key in _vm_mod._vm_deployments_refreshing


def test_refresh_updates_cache_and_clears_in_flight_flag() -> None:
    """A successful background refresh stores the new snapshot and clears the flag."""
    cache_key = "7|False"
    _vm_mod._vm_deployments_refreshing.add(cache_key)
    fresh_result = _vm_mod.VmDeploymentsListModel(active=[], recent=[], archive_days=7)
    with patch.object(_vm_mod, "_compute_vm_deployments", return_value=fresh_result):
        _vm_mod._refresh_vm_deployments(cache_key, 7, False)
    assert cache_key not in _vm_mod._vm_deployments_refreshing
    assert _vm_mod._vm_deployments_cache[cache_key][1] is fresh_result


def test_refresh_failure_keeps_stale_snapshot_never_poisons_cache() -> None:
    """A failed background refresh leaves the last-known-good snapshot in place."""
    cache_key = "7|False"
    stale_result = _vm_mod.VmDeploymentsListModel(active=[], recent=[], archive_days=7)
    _vm_mod._store_vm_deployments(cache_key, stale_result)
    _vm_mod._vm_deployments_refreshing.add(cache_key)
    with patch.object(_vm_mod, "_compute_vm_deployments", side_effect=RuntimeError("registry down")):
        _vm_mod._refresh_vm_deployments(cache_key, 7, False)
    assert _vm_mod._vm_deployments_cache[cache_key][1] is stale_result
    assert cache_key not in _vm_mod._vm_deployments_refreshing


# ---------------------------------------------------------------------------
# 2. End-to-end via the route -- registry mocked, single-flight verified through
#    real HTTP calls (mirrors test_vm_reconcile.py's client-level conventions).
# ---------------------------------------------------------------------------


def test_route_serves_cached_snapshot_without_rewalking_registry_within_ttl(
    client: TestClient,
) -> None:
    """Two GETs inside the TTL window walk the registry exactly once."""
    entries = [_FakeEntry(deployment_id="dep-1", vm_name="vm-1")]
    mock_registry = MagicMock()
    mock_registry.list_active.return_value = entries
    mock_registry.list_recent_archive.return_value = []

    with (
        patch.object(DeploymentApiConfig, "is_mock_mode", return_value=False),
        patch("deployment_api.routes.vm_deployments.DeploymentsRegistry", return_value=mock_registry),
        patch("deployment_api.routes.vm_deployments.get_vm_instance_details", return_value={}),
    ):
        first = client.get("/api/vm-deployments", params={"filter_stale": "false"})
        second = client.get("/api/vm-deployments", params={"filter_stale": "false"})

    assert first.status_code == 200
    assert second.status_code == 200
    assert first.json() == second.json()
    assert first.json()["active"][0]["vm_name"] == "vm-1"
    # SWR: the second request is served from cache, not a second registry walk.
    assert mock_registry.list_active.call_count == 1
    assert mock_registry.list_recent_archive.call_count == 1


def test_route_cold_path_failure_returns_502_and_does_not_poison_cache(
    client: TestClient,
) -> None:
    """A cold-path registry/GCP failure surfaces as 502 and leaves nothing cached."""
    mock_registry = MagicMock()
    mock_registry.list_active.return_value = []

    with (
        patch.object(DeploymentApiConfig, "is_mock_mode", return_value=False),
        patch("deployment_api.routes.vm_deployments.DeploymentsRegistry", return_value=mock_registry),
        patch(
            "deployment_api.routes.vm_deployments.get_vm_instance_details",
            side_effect=RuntimeError("GCP unavailable"),
        ),
    ):
        resp = client.get("/api/vm-deployments", params={"filter_stale": "true"})

    assert resp.status_code == 502
    assert "7|True" not in _vm_mod._vm_deployments_cache


def test_route_different_query_params_use_independent_cache_keys(
    client: TestClient,
) -> None:
    """`days`/`filter_stale` are part of the cache key -- distinct params walk the
    registry independently rather than sharing (or clobbering) one snapshot."""
    entries = [_FakeEntry(deployment_id="dep-1", vm_name="vm-1")]
    mock_registry = MagicMock()
    mock_registry.list_active.return_value = entries
    mock_registry.list_recent_archive.return_value = []

    with (
        patch.object(DeploymentApiConfig, "is_mock_mode", return_value=False),
        patch("deployment_api.routes.vm_deployments.DeploymentsRegistry", return_value=mock_registry),
        patch("deployment_api.routes.vm_deployments.get_vm_instance_details", return_value={}),
    ):
        client.get("/api/vm-deployments", params={"days": 7, "filter_stale": "false"})
        client.get("/api/vm-deployments", params={"days": 14, "filter_stale": "false"})

    assert mock_registry.list_recent_archive.call_count == 2
    assert "7|False" in _vm_mod._vm_deployments_cache
    assert "14|False" in _vm_mod._vm_deployments_cache

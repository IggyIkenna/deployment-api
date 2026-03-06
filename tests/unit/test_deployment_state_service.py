"""
Unit tests for services/deployment_state module (DeploymentStateManager).

Tests list_deployments, get_deployment_status, cancel_deployment,
resume_deployment, and _enrich_deployment_summary.
"""

import sys
from pathlib import Path
from types import ModuleType
from unittest.mock import MagicMock, patch

import pytest

# Remove pre-mocked entries so we import the real DeploymentStateManager.
for _key in list(sys.modules.keys()):
    if _key in (
        "deployment_api.services.deployment_state",
        "deployment_api.services",
    ):
        del sys.modules[_key]

# Re-build minimal package pointing to the real services directory.
_svc_pkg = ModuleType("deployment_api.services")
_svc_pkg.__package__ = "deployment_api.services"
_services_dir = str(Path(__file__).parent.parent.parent / "deployment_api" / "services")
_svc_pkg.__path__ = [_services_dir]  # type: ignore[attr-defined]
sys.modules["deployment_api.services"] = _svc_pkg

# DeploymentStateManager uses lazy imports inside methods (from ..routes.X import Y)
# We will patch those at the module level where they eventually resolve to:
#   deployment_api.routes.deployment_caching
#   deployment_api.routes.deployment_state
#   deployment_api.routes.shard_management
# Patching will be done inside individual test methods to avoid polluting sys.modules.

from deployment_api.services.deployment_state import DeploymentStateManager  # noqa: E402


def _make_mgr() -> DeploymentStateManager:
    return DeploymentStateManager()


class TestDeploymentStateManagerInit:
    """Tests for DeploymentStateManager.__init__."""

    def test_can_be_instantiated(self):
        mgr = _make_mgr()
        assert isinstance(mgr, DeploymentStateManager)


def _with_mock_routes(deps=None, state=None, classified=None, counts=None, date_range=None):
    """Context manager factory that injects mock route sub-modules for lazy imports."""
    import contextlib

    mock_caching = MagicMock()
    mock_caching.get_cached_deployments = MagicMock(return_value=deps or [])
    mock_caching.get_cached_deployment_state = MagicMock(return_value=state)
    mock_caching.invalidate_deployment_state_cache = MagicMock()

    mock_dep_state = MagicMock()
    mock_dep_state._cancel_deployment_sync = MagicMock()
    mock_dep_state._refresh_deployment_status_sync = MagicMock()
    mock_dep_state._resume_deployment_sync = MagicMock()

    mock_shard = MagicMock()
    mock_shard._classify_all_shards = MagicMock(return_value=classified or [])
    mock_shard._compute_classification_counts = MagicMock(return_value=counts or {})
    mock_shard._get_state_date_range = MagicMock(return_value=date_range or {})

    return patch.dict(
        sys.modules,
        {
            "deployment_api.routes.deployment_caching": mock_caching,
            "deployment_api.routes.deployment_state": mock_dep_state,
            "deployment_api.routes.shard_management": mock_shard,
        },
    )


class TestListDeployments:
    """Tests for DeploymentStateManager.list_deployments."""

    def test_returns_deployment_dict_with_expected_keys(self):
        mgr = _make_mgr()
        mock_deps = [
            {"deployment_id": "d1", "status": "running", "service": "svc-a", "created_at": "2024-01-10"},
            {"deployment_id": "d2", "status": "completed", "service": "svc-b", "created_at": "2024-01-09"},
        ]

        with _with_mock_routes(deps=mock_deps):
            result = mgr.list_deployments()

        assert "deployments" in result
        assert "total_count" in result
        assert "limit" in result
        assert "offset" in result
        assert "has_more" in result

    def test_returns_all_deployments_by_default(self):
        mgr = _make_mgr()
        deps = [
            {"deployment_id": f"d{i}", "status": "running", "created_at": "2024-01-01"}
            for i in range(5)
        ]

        with _with_mock_routes(deps=deps):
            result = mgr.list_deployments()

        assert result["total_count"] == 5

    def test_filters_by_status(self):
        mgr = _make_mgr()
        deps = [
            {"deployment_id": "d1", "status": "running", "created_at": "2024-01-10"},
            {"deployment_id": "d2", "status": "completed", "created_at": "2024-01-09"},
        ]

        with _with_mock_routes(deps=deps):
            result = mgr.list_deployments(status_filter="running")

        assert result["total_count"] == 1
        assert result["deployments"][0]["status"] == "running"

    def test_filters_by_service(self):
        mgr = _make_mgr()
        deps = [
            {"deployment_id": "d1", "service": "svc-a", "status": "running", "created_at": "2024-01-10"},
            {"deployment_id": "d2", "service": "svc-b", "status": "running", "created_at": "2024-01-09"},
        ]

        with _with_mock_routes(deps=deps):
            result = mgr.list_deployments(service_filter="svc-a")

        assert result["total_count"] == 1
        assert result["deployments"][0]["service"] == "svc-a"

    def test_pagination_limit_and_offset(self):
        mgr = _make_mgr()
        deps = [
            {"deployment_id": f"d{i}", "status": "running", "created_at": "2024-01-01"}
            for i in range(10)
        ]

        with _with_mock_routes(deps=deps):
            result = mgr.list_deployments(limit=3, offset=2)

        assert len(result["deployments"]) == 3
        assert result["offset"] == 2
        assert result["limit"] == 3
        assert result["has_more"] is True

    def test_has_more_false_at_end(self):
        mgr = _make_mgr()
        deps = [
            {"deployment_id": f"d{i}", "status": "running", "created_at": "2024-01-01"}
            for i in range(3)
        ]

        with _with_mock_routes(deps=deps):
            result = mgr.list_deployments(limit=10, offset=0)

        assert result["has_more"] is False

    def test_sorts_by_created_at_descending(self):
        mgr = _make_mgr()
        deps = [
            {"deployment_id": "d1", "status": "running", "created_at": "2024-01-01"},
            {"deployment_id": "d2", "status": "running", "created_at": "2024-01-10"},
            {"deployment_id": "d3", "status": "running", "created_at": "2024-01-05"},
        ]

        with _with_mock_routes(deps=deps):
            result = mgr.list_deployments()

        first = result["deployments"][0]
        assert first["deployment_id"] == "d2"  # most recent first


class TestGetDeploymentStatus:
    """Tests for DeploymentStateManager.get_deployment_status."""

    def test_raises_when_not_found(self):
        mgr = _make_mgr()

        with (
            _with_mock_routes(state=None),
            pytest.raises(ValueError, match="not found"),
        ):
            mgr.get_deployment_status("dep-missing")

    def test_returns_status_dict(self):
        mgr = _make_mgr()
        state = {
            "deployment_id": "dep-1",
            "status": "running",
            "service": "my-svc",
            "created_at": "2024-01-01T00:00:00",
            "shards": [{"shard_id": "s1", "status": "running"}],
        }
        mock_shards = [{"shard_id": "s1", "status": "running", "class": "active"}]
        mock_counts = {"active": 1}
        mock_date_range = {"start_date": "2024-01-01", "end_date": "2024-01-31"}

        with _with_mock_routes(state=state, classified=mock_shards, counts=mock_counts, date_range=mock_date_range):
            result = mgr.get_deployment_status("dep-1")

        assert result["deployment_id"] == "dep-1"
        assert result["status"] == "running"
        assert result["service"] == "my-svc"

    def test_detailed_false_omits_shards(self):
        mgr = _make_mgr()
        state = {
            "status": "completed",
            "service": "svc-a",
            "shards": [],
        }

        with _with_mock_routes(state=state):
            result = mgr.get_deployment_status("dep-1", detailed=False)

        assert "shards" not in result


class TestCancelDeployment:
    """Tests for DeploymentStateManager.cancel_deployment."""

    def test_returns_cancelled_status(self):
        mgr = _make_mgr()

        with _with_mock_routes():
            result = mgr.cancel_deployment("dep-1")

        assert result["status"] == "cancelled"
        assert result["deployment_id"] == "dep-1"

    def test_raises_on_cancel_error(self):
        mgr = _make_mgr()
        mock_dep_state = MagicMock()
        mock_dep_state._cancel_deployment_sync = MagicMock(side_effect=RuntimeError("cancel failed"))

        with (
            patch.dict(sys.modules, {
                "deployment_api.routes.deployment_state": mock_dep_state,
                "deployment_api.routes.deployment_caching": MagicMock(),
            }),
            pytest.raises(ValueError, match="Failed to cancel deployment"),
        ):
            mgr.cancel_deployment("dep-fail")

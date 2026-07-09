"""Unit tests for routes/_gcp_cloud_functions.py — GCP Cloud Functions (gen2) census.

Credential-free: ``tests/unit/conftest.py`` pre-stubs ``deployment_service`` as an
EMPTY package (``__path__ = []``, no real ``backends`` subpackage) for the whole
unit-test session — the same reason ``test_backfill_launch.py`` /
``test_vm_deployment_bom.py`` etc. pre-register their own
``deployment_service.deployments_registry`` stub before use. We do the same here
for ``deployment_service.backends`` + ``deployment_service.backends._gcp_sdk``:
register both directly in ``sys.modules`` (scoped per-test via ``patch.dict``) so
``from deployment_service.backends import _gcp_sdk`` resolves without needing the
real deployment-service package tree.

Verifies: state → wire-status mapping, runtime/service_name extraction, and honest
degradation to ``{}`` on any GCP error (never raises, never crashes the inventory).
"""

from __future__ import annotations

import sys
from dataclasses import dataclass, field
from datetime import UTC, datetime
from types import ModuleType
from unittest.mock import patch

import pytest

pytestmark = [pytest.mark.timeout(60)]

_FIXED_NOW = datetime(2026, 7, 9, 6, 0, 0, tzinfo=UTC)


@dataclass
class _FakeState:
    name: str


@dataclass
class _FakeBuildConfig:
    runtime: str = ""


@dataclass
class _FakeServiceConfig:
    service: str = ""


@dataclass
class _FakeFunction:
    name: str
    state: _FakeState
    build_config: _FakeBuildConfig = field(default_factory=_FakeBuildConfig)
    service_config: _FakeServiceConfig = field(default_factory=_FakeServiceConfig)
    update_time: datetime | None = None


class _FakeRequest:
    def __init__(self, parent: str) -> None:
        self.parent = parent


def _fake_gcp_sdk_modules(functions_v2_mod: ModuleType) -> dict[str, ModuleType]:
    """The ``sys.modules`` entries needed for ``from deployment_service.backends
    import _gcp_sdk`` to resolve to ``functions_v2_mod`` under the conftest stub.
    """
    backends_stub = ModuleType("deployment_service.backends")
    backends_stub.__package__ = "deployment_service.backends"
    backends_stub.__path__ = []  # type: ignore[attr-defined]

    gcp_sdk_stub = ModuleType("deployment_service.backends._gcp_sdk")
    gcp_sdk_stub.functions_v2 = functions_v2_mod  # type: ignore[attr-defined]

    return {
        "deployment_service.backends": backends_stub,
        "deployment_service.backends._gcp_sdk": gcp_sdk_stub,
    }


def _fake_functions_v2(functions: list[_FakeFunction]) -> ModuleType:
    """A fake ``google.cloud.functions_v2`` module whose client returns ``functions``."""

    class _FakeClient:
        def list_functions(self, request: object) -> list[_FakeFunction]:
            assert isinstance(request, _FakeRequest)
            return functions

    mod = ModuleType("google.cloud.functions_v2")
    mod.FunctionServiceClient = _FakeClient  # type: ignore[attr-defined]
    mod.ListFunctionsRequest = _FakeRequest  # type: ignore[attr-defined]
    return mod


def test_list_cloud_functions_maps_active_state_and_config() -> None:
    from deployment_api.routes import _gcp_cloud_functions

    fn = _FakeFunction(
        name="projects/test-project/locations/asia-northeast1/functions/trigger-ingest",
        state=_FakeState(name="ACTIVE"),
        build_config=_FakeBuildConfig(runtime="python313"),
        service_config=_FakeServiceConfig(
            service="projects/test-project/locations/asia-northeast1/services/trigger-ingest"
        ),
        update_time=_FIXED_NOW,
    )
    with patch.dict(sys.modules, _fake_gcp_sdk_modules(_fake_functions_v2([fn]))):
        result = _gcp_cloud_functions.list_cloud_functions("test-project")

    assert set(result) == {"trigger-ingest"}
    status = result["trigger-ingest"]
    assert status.status == "running"
    assert status.runtime == "python313"
    assert status.service_name == "trigger-ingest"
    assert status.last_updated_at == _FIXED_NOW.isoformat()


@pytest.mark.parametrize(
    ("state_name", "expected_status"),
    [
        ("ACTIVE", "running"),
        ("FAILED", "failed"),
        ("DEPLOYING", "pending"),
        ("DELETING", "pending"),
        ("UNKNOWN", "unknown"),
        ("STATE_UNSPECIFIED", "unknown"),
    ],
)
def test_list_cloud_functions_state_mapping(state_name: str, expected_status: str) -> None:
    from deployment_api.routes import _gcp_cloud_functions

    fn = _FakeFunction(
        name="projects/test-project/locations/asia-northeast1/functions/some-fn",
        state=_FakeState(name=state_name),
    )
    with patch.dict(sys.modules, _fake_gcp_sdk_modules(_fake_functions_v2([fn]))):
        result = _gcp_cloud_functions.list_cloud_functions("test-project")

    assert result["some-fn"].status == expected_status
    # No build_config/service_config supplied → honest empty defaults, never crashes.
    assert result["some-fn"].runtime == ""
    assert result["some-fn"].service_name == ""


def test_list_cloud_functions_degrades_on_gcp_error() -> None:
    """A GCP client-construction/list failure degrades to an empty map, never raises."""
    from deployment_api.routes import _gcp_cloud_functions

    class _BoomClient:
        def __init__(self) -> None:
            raise RuntimeError("no creds")

    boom_mod = ModuleType("google.cloud.functions_v2")
    boom_mod.FunctionServiceClient = _BoomClient  # type: ignore[attr-defined]
    boom_mod.ListFunctionsRequest = _FakeRequest  # type: ignore[attr-defined]

    with patch.dict(sys.modules, _fake_gcp_sdk_modules(boom_mod)):
        result = _gcp_cloud_functions.list_cloud_functions("test-project")
    assert result == {}

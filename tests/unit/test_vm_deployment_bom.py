"""Regression: BoM (bill-of-materials) fields surface through the VM-deployments API.

The launch path stamps ``image_digest`` / ``git_commit`` / ``dep_versions`` onto the
``DeploymentRegistryEntry`` (deployment_service.bom). ``_to_model`` builds the API
response from ``asdict(entry)`` and pydantic silently DROPS unknown keys — so without
explicit field declarations on ``VmDeploymentEntryModel`` the BoM reaches the GCS rows
but never the API response. These tests guard the declarations + the asdict path so a
future edit can't silently re-drop them.

SSOT: dependency_promotion_range_pins_and_major_bump_sit_2026_06_09.md § Phase 6 (BoM follow-up).
"""

from __future__ import annotations

import os
import sys
from dataclasses import dataclass, field
from types import ModuleType
from unittest.mock import MagicMock, patch

os.environ.setdefault("CLOUD_MOCK_MODE", "false")
os.environ.setdefault("CLOUD_PROVIDER", "local")
os.environ.setdefault("GCP_PROJECT_ID", "test-project")
os.environ.setdefault("DISABLE_AUTH", "true")
os.environ.setdefault("MOCK_STATE_MODE", "deterministic")

if "deployment_service.deployments_registry" not in sys.modules:
    _dr_mod = ModuleType("deployment_service.deployments_registry")
    _dr_mod.DEFAULT_BUCKET = "test-bucket"  # type: ignore[attr-defined]
    _dr_mod.DeploymentRegistryEntry = MagicMock()  # type: ignore[attr-defined]
    _dr_mod.DeploymentsRegistry = MagicMock()  # type: ignore[attr-defined]
    _dr_mod.vm_run_log_rolling_uri = MagicMock()  # type: ignore[attr-defined]
    _dr_mod.vm_serial_rolling_uri = MagicMock()  # type: ignore[attr-defined]
    sys.modules["deployment_service.deployments_registry"] = _dr_mod

with (
    patch("unified_trading_library.event_sink.PubSubEventSink"),
    patch("unified_trading_library.PubSubEventSink"),
    patch("unified_trading_library.events.setup_events"),
    patch("unified_trading_library.utils.tracing.setup_tracing"),
    patch("unified_trading_library.setup_tracing"),
):
    from deployment_api.routes.vm_deployments import VmDeploymentEntryModel, _to_model

import pytest

pytestmark = [pytest.mark.timeout(60)]


@dataclass
class _FakeEntry:
    """Minimal stand-in for DeploymentRegistryEntry carrying the BoM fields."""

    deployment_id: str = "dep-bom-1"
    vm_name: str = "mtds-deploy-20260612-010101"
    asset_group: str = "cefi"
    task: str = "service-deploy"
    mode: str = "live"
    start_date: str = "2026-06-12"
    end_date: str = "2026-06-12"
    status: str = "completed"
    started_at: str = "2026-06-12T01:01:01Z"
    last_heartbeat_at: str = "2026-06-12T01:05:01Z"
    image_digest: str = "sha256:abc123"
    git_commit: str = "deadbeef"
    dep_versions: dict[str, str] = field(
        default_factory=lambda: {"unified-trading-library": "0.5.0", "base_image_digest": "sha256:base99"}
    )


def test_bom_fields_declared_on_model() -> None:
    """The model retains BoM kwargs (pydantic would drop them if undeclared)."""
    model = VmDeploymentEntryModel(
        deployment_id="d",
        vm_name="v",
        asset_group="cefi",
        task="t",
        mode="live",
        start_date="2026-06-12",
        end_date="2026-06-12",
        status="completed",
        started_at="2026-06-12T01:01:01Z",
        last_heartbeat_at="2026-06-12T01:05:01Z",
        image_digest="sha256:abc123",
        git_commit="deadbeef",
        dep_versions={"unified-trading-library": "0.5.0"},
    )
    assert model.image_digest == "sha256:abc123"
    assert model.git_commit == "deadbeef"
    assert model.dep_versions == {"unified-trading-library": "0.5.0"}


def test_bom_fields_surface_through_to_model() -> None:
    """`_to_model` (asdict path) carries BoM from the registry entry to the response."""
    model = _to_model(_FakeEntry())  # type: ignore[arg-type]
    assert model.image_digest == "sha256:abc123"
    assert model.git_commit == "deadbeef"
    assert model.dep_versions == {"unified-trading-library": "0.5.0", "base_image_digest": "sha256:base99"}


def test_bom_fields_default_empty_when_absent() -> None:
    """Honest-unknown defaults: a registry entry without BoM yields empty values, not errors."""
    model = VmDeploymentEntryModel(
        deployment_id="d",
        vm_name="v",
        asset_group="cefi",
        task="t",
        mode="live",
        start_date="2026-06-12",
        end_date="2026-06-12",
        status="running",
        started_at="2026-06-12T01:01:01Z",
        last_heartbeat_at="2026-06-12T01:05:01Z",
    )
    assert model.image_digest == ""
    assert model.git_commit == ""
    assert model.dep_versions == {}

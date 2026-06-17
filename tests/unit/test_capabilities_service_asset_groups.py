"""Regression tests for the capabilities ``service-asset-groups`` route.

Guards the live 404 incident where ``/api/capabilities/service-asset-groups/<service>``
returned ``404 "Sharding config not found for service: <svc>"`` for EVERY service:
the deployed image's ``pm-configs/`` (where ``get_config_dir()`` reads
``sharding.<service>.yaml``) was emptied by the cloudbuild / AWS-buildspec
``vendor-deps`` step instead of being populated from the
``unified-trading-pm/configs/`` SSOT.

Two layers of protection:
1. Route-level — the handler resolves a known bundled sharding config and returns its
   ``asset_group`` values (not a 404). Passes wherever ``pm-configs`` resolves to the
   config SSOT (repo symlink locally; ``COPY pm-configs/`` mirror in the image).
2. Build-config guard — the cloudbuild + AWS buildspec must POPULATE ``pm-configs``,
   never empty it; catches a re-introduction of the original bundling bug.
"""

from __future__ import annotations

from pathlib import Path

import pytest

from deployment_api.routes.capabilities import _service_asset_groups_from_sharding
from deployment_api.utils.service_utils import get_config_dir

_REPO_ROOT = Path(__file__).resolve().parents[2]


@pytest.mark.parametrize("service", ["market-tick-data-service", "instruments-service"])
def test_service_asset_groups_resolves_known_sharding_config(service: str) -> None:
    """A known service's sharding config resolves to a non-empty asset_group list."""
    # Precondition: the config the route reads is actually bundled where it looks.
    assert (get_config_dir() / f"sharding.{service}.yaml").exists(), (
        f"sharding.{service}.yaml missing from {get_config_dir()} — the route would 404"
    )

    result = _service_asset_groups_from_sharding(service)

    assert result["service"] == service
    asset_groups = result["asset_groups"]
    assert isinstance(asset_groups, list)
    assert asset_groups, f"expected non-empty asset_groups for {service}"


@pytest.mark.parametrize(
    "build_file",
    ["cloudbuild.yaml", "buildspec.aws.yaml"],
)
def test_build_config_populates_pm_configs_not_emptied(build_file: str) -> None:
    """The build must populate ``pm-configs`` from the PM SSOT, never leave it empty.

    Regression guard for the original incident: ``pm-configs`` emptied to a bare
    ``.gitkeep`` in the image → every ``service-asset-groups`` lookup 404'd.
    """
    text = (_REPO_ROOT / build_file).read_text()

    # It must copy the configs SSOT mirror INTO pm-configs. The original 404 bug
    # never did this — it only emptied pm-configs to a bare .gitkeep placeholder.
    assert "cp -a" in text and "configs pm-configs" in text, (
        f"{build_file} must populate pm-configs from unified-trading-pm/configs "
        f"(else /api/capabilities/service-asset-groups/<service> 404s)"
    )

"""
Unit test configuration - mocks broken UTL imports before any module imports.

unified_trading_library has a broken CloudTarget/StandardizedDomainCloudService
import chain that prevents importing deployment_api.settings. We mock these
at the sys.modules level before any test module is collected.

We also pre-mock deployment_api.services as a proper package-like mock so that
both flat imports (from deployment_api.services import SyncService) and
sub-module imports (deployment_api.services.deployment_manager) work correctly,
regardless of the order tests are collected.
"""

import sys
from types import ModuleType
from unittest.mock import MagicMock


def _ensure_utl_mocked() -> None:
    """Pre-mock unified_trading_library so deployment_api.settings can be imported."""
    broken_modules = [
        "unified_trading_library",
        "unified_trading_library.core",
        "unified_trading_library.core.cloud_config",
        "unified_trading_library.core.cloud_base_service",
        "unified_trading_library.core.cloud_pubsub_service",
        "unified_trading_library.core.cloud_data_provider",
        "unified_trading_library.core.gcsfuse_helper",
        "unified_trading_library.domain",
        "unified_trading_library.domain.standardized_service",
        "unified_trading_library.domain.execution_client",
    ]
    for mod in broken_modules:
        if mod not in sys.modules:
            mock_mod = MagicMock()
            mock_mod.__version__ = "0.0.0-test"
            sys.modules[mod] = mock_mod


def _ensure_services_mocked() -> None:
    """Pre-mock deployment_api.services as a package-compatible module.

    Both flat imports (from deployment_api.services import SyncService) and
    dotted sub-module imports (from deployment_api.services.deployment_manager
    import DeploymentManager) must work. We achieve this by creating real
    ModuleType objects with MagicMock attributes and registering all sub-modules
    in sys.modules before any test file is collected.
    """
    if "deployment_api.services" in sys.modules:
        return

    # Build the top-level services package module
    services_mod = ModuleType("deployment_api.services")
    services_mod.__package__ = "deployment_api.services"
    services_mod.__path__ = []  # type: ignore[attr-defined]  # marks it as a package

    # Expose common service classes at the package level
    services_mod.SyncService = MagicMock()  # type: ignore[attr-defined]
    services_mod.DataAnalyticsService = MagicMock()  # type: ignore[attr-defined]
    services_mod.DataQueryService = MagicMock()  # type: ignore[attr-defined]
    services_mod.DataStatusService = MagicMock()  # type: ignore[attr-defined]

    sys.modules["deployment_api.services"] = services_mod

    # Sub-module list mirrors deployment_api/services/
    sub_modules = [
        "data_analytics_service",
        "data_query_service",
        "data_status_service",
        "deployment_manager",
        "deployment_state",
        "event_processor",
        "state_manager",
        "sync_service",
    ]

    for sub in sub_modules:
        full_name = f"deployment_api.services.{sub}"
        if full_name not in sys.modules:
            sub_mod = ModuleType(full_name)
            sub_mod.__package__ = "deployment_api.services"
            # Expose plausible class names on each sub-module via MagicMock
            sub_mod.DeploymentManager = MagicMock()  # type: ignore[attr-defined]
            sub_mod.DeploymentStateManager = MagicMock()  # type: ignore[attr-defined]
            sub_mod.SyncService = MagicMock()  # type: ignore[attr-defined]
            sub_mod.DataAnalyticsService = MagicMock()  # type: ignore[attr-defined]
            sub_mod.DataQueryService = MagicMock()  # type: ignore[attr-defined]
            sub_mod.DataStatusService = MagicMock()  # type: ignore[attr-defined]
            sub_mod.StateManager = MagicMock()  # type: ignore[attr-defined]
            sub_mod.EventProcessor = MagicMock()  # type: ignore[attr-defined]
            sys.modules[full_name] = sub_mod
            # Also set as attribute on the package module
            setattr(services_mod, sub, sub_mod)


# Run immediately at import time (before pytest collects tests)
_ensure_utl_mocked()
_ensure_services_mocked()

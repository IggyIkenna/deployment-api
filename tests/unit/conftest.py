"""
Unit test configuration - mocks broken UTL imports before any module imports.

unified_trading_library has a broken CloudTarget/StandardizedDomainCloudService
import chain that prevents importing deployment_api.settings. We mock these
at the sys.modules level before any test module is collected.
"""

import sys
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


# Run immediately at import time (before pytest collects tests)
_ensure_utl_mocked()

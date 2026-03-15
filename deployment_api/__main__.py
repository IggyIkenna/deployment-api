"""Dev entry point: python -m deployment_api."""

from __future__ import annotations

import uvicorn
from unified_config_interface import UnifiedCloudConfig

_cfg = UnifiedCloudConfig()
_port = _cfg.port if hasattr(_cfg, "port") else 8004
_reload = _cfg.runtime_mode == "local"

uvicorn.run(
    "deployment_api.main:app",
    host="0.0.0.0",
    port=_port,
    reload=_reload,
)

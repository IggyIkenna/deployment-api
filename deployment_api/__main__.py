"""Dev entry point: python -m deployment_api."""

from __future__ import annotations

import os

import uvicorn
from unified_config_interface import UnifiedCloudConfig

_cfg = UnifiedCloudConfig()
_port = int(os.environ.get("PORT", "8004"))
_reload = _cfg.runtime_mode == "local"

uvicorn.run(
    "deployment_api.main:app",
    host="0.0.0.0",
    port=_port,
    reload=_reload,
)

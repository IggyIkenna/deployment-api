"""
Application configuration helpers.

Provides config-directory resolution and UI-dist detection for the
deployment-api service.  The FastAPI app is created and wired in
``deployment_api/lifespan.py`` (not here).
"""

import logging
from pathlib import Path

logger = logging.getLogger(__name__)


def get_config_dir() -> Path:
    """Get the operational configs directory path.

    Search order:
    1. repo_root/pm-configs/  -- symlink to ../unified-trading-pm/configs (local dev)
                              -- real dir populated by cloudbuild before docker build (prod)
    2. workspace sibling      -- ../unified-trading-pm/configs

    SSOT: unified-trading-pm/configs/ (PM is the canonical source for operational configs)
    """
    api_dir = Path(__file__).parent  # deployment_api/
    repo_root = api_dir.parent  # deployment-api/

    bundled = repo_root / "pm-configs"
    if bundled.exists():
        return bundled

    sibling = repo_root.parent / "unified-trading-pm" / "configs"
    if sibling.exists():
        return sibling

    raise RuntimeError(
        "Could not find operational configs directory. "
        "Expected pm-configs/ (bundled) or ../unified-trading-pm/configs (sibling)."
    )


def get_ui_dist_dir() -> Path | None:
    """Get the UI dist directory if it exists (for production serving)."""
    api_dir = Path(__file__).parent
    repo_root = api_dir.parent
    ui_dist = repo_root / "ui" / "dist"

    if ui_dist.exists() and (ui_dist / "index.html").exists():
        return ui_dist
    return None

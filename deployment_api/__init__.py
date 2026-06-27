"""
Deployment Monitoring API

FastAPI backend for the deployment monitoring UI.
Wraps the existing CLI functionality.
"""

from importlib.metadata import PackageNotFoundError
from importlib.metadata import version as _dist_version

# Resolve the version from the INSTALLED distribution metadata (Phase-2 / WS-L D13 API-2:
# version-out-of-source). This was a hardcoded literal that had drifted from pyproject (it read
# "0.1.1" while the package was at 0.50.0), so the /health + /version surfaces showed a stale value.
# importlib.metadata reads the installed dist, so it stays correct whether the version is a static
# pyproject line OR resolved dynamically from the git tag (hatch-vcs). Mirrors the API-1 pattern in
# routes/cloud_builds.py.
try:
    __version__ = _dist_version("deployment-api")
except PackageNotFoundError:  # source checkout without an editable install
    __version__ = "0.0.0"

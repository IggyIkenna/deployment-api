"""GET /api/deployments/diff — compare deployed service versions at two git SHAs.

In mock mode (CLOUD_MOCK_MODE=true) returns synthetic diff data.
In prod mode: reads deployed_versions.production from workspace-manifest.json at
both SHAs via `git show` and computes added/removed/changed entries.
"""

from __future__ import annotations

import json
import logging
import subprocess
from os import environ as _process_env
from pathlib import Path

from fastapi import APIRouter, Query
from pydantic import BaseModel, Field
from unified_trading_library import log_event

from deployment_api.deployment_api_config import DeploymentApiConfig

router = APIRouter()
logger = logging.getLogger(__name__)

_cfg = DeploymentApiConfig()

_MANIFEST_REPO_PATH = "unified-trading-pm/workspace-manifest.json"


class DiffEntry(BaseModel):
    """A single service that appeared in the diff."""

    service: str = Field(..., description="Service name.")
    from_version: str | None = Field(
        default=None, description="Version at from_sha; None for newly added services."
    )
    to_version: str | None = Field(
        default=None, description="Version at to_sha; None for removed services."
    )


class DeploymentDiffResponse(BaseModel):
    """Response for GET /api/deployments/diff."""

    from_sha: str
    to_sha: str
    added: list[DiffEntry] = Field(default_factory=list)
    removed: list[DiffEntry] = Field(default_factory=list)
    changed: list[DiffEntry] = Field(default_factory=list)
    total_changes: int
    dry_run: bool


def _workspace_root() -> Path:
    ws = _process_env.get("WORKSPACE_ROOT") or str(Path(__file__).resolve().parents[4])
    return Path(ws)


def _deployed_versions_at_sha(sha: str) -> dict[str, str]:
    """Read deployed_versions.production from workspace manifest at a git SHA."""
    try:
        result = subprocess.run(
            ["git", "show", f"{sha}:{_MANIFEST_REPO_PATH}"],
            capture_output=True,
            text=True,
            cwd=_workspace_root(),
            timeout=10,
        )
        if result.returncode != 0:
            return {}
        parsed: object = json.loads(result.stdout)
        if not isinstance(parsed, dict):
            return {}
        raw: dict[str, object] = parsed
        dv: object = raw.get("deployed_versions")
        if not isinstance(dv, dict):
            return {}
        dv_typed: dict[str, object] = dv
        prod: object = dv_typed.get("production")
        return dict(prod) if isinstance(prod, dict) else {}
    except Exception:
        logger.warning("could not read manifest at sha %s", sha)
        return {}


def _compute_diff(
    from_versions: dict[str, str],
    to_versions: dict[str, str],
) -> tuple[list[DiffEntry], list[DiffEntry], list[DiffEntry]]:
    all_services = set(from_versions) | set(to_versions)
    added: list[DiffEntry] = []
    removed: list[DiffEntry] = []
    changed: list[DiffEntry] = []
    for svc in sorted(all_services):
        fv = from_versions.get(svc)
        tv = to_versions.get(svc)
        if fv is None:
            added.append(DiffEntry(service=svc, from_version=None, to_version=tv))
        elif tv is None:
            removed.append(DiffEntry(service=svc, from_version=fv, to_version=None))
        elif fv != tv:
            changed.append(DiffEntry(service=svc, from_version=fv, to_version=tv))
    return added, removed, changed


_MOCK_FROM_VERSIONS = {
    "execution-service": "0.14.2",
    "features-service": "0.21.0",
    "market-tick-data-service": "0.8.1",
    "strategy-service": "0.9.3",
}

_MOCK_TO_VERSIONS = {
    "execution-service": "0.15.0",  # changed
    "features-service": "0.21.0",  # unchanged
    "market-tick-data-service": "0.8.1",  # unchanged
    "risk-and-exposure-service": "0.4.0",  # added
    # strategy-service removed
}


def _mock_diff(from_sha: str, to_sha: str) -> DeploymentDiffResponse:
    if from_sha == to_sha:
        return DeploymentDiffResponse(
            from_sha=from_sha,
            to_sha=to_sha,
            added=[],
            removed=[],
            changed=[],
            total_changes=0,
            dry_run=True,
        )
    added, removed, changed = _compute_diff(_MOCK_FROM_VERSIONS, _MOCK_TO_VERSIONS)
    total = len(added) + len(removed) + len(changed)
    return DeploymentDiffResponse(
        from_sha=from_sha,
        to_sha=to_sha,
        added=added,
        removed=removed,
        changed=changed,
        total_changes=total,
        dry_run=True,
    )


@router.get("/api/deployments/diff", response_model=DeploymentDiffResponse, tags=["Deployments"])
def get_deployment_diff(
    from_sha: str = Query(..., description="Git SHA of the base snapshot."),
    to_sha: str = Query(..., description="Git SHA of the target snapshot."),
) -> DeploymentDiffResponse:
    """Compare deployed service versions at two workspace-manifest commit SHAs."""
    log_event(
        "ADAPTER_FETCH_STARTED",
        details={"endpoint": "GET /api/deployments/diff", "from_sha": from_sha, "to_sha": to_sha},
    )

    if _cfg.is_mock_mode():
        result = _mock_diff(from_sha, to_sha)
        log_event(
            "ADAPTER_FETCH_COMPLETED",
            details={
                "endpoint": "GET /api/deployments/diff",
                "total_changes": result.total_changes,
                "dry_run": True,
            },
        )
        return result

    from_versions = _deployed_versions_at_sha(from_sha)
    to_versions = _deployed_versions_at_sha(to_sha)
    added, removed, changed = _compute_diff(from_versions, to_versions)
    total = len(added) + len(removed) + len(changed)

    result = DeploymentDiffResponse(
        from_sha=from_sha,
        to_sha=to_sha,
        added=added,
        removed=removed,
        changed=changed,
        total_changes=total,
        dry_run=False,
    )
    log_event(
        "ADAPTER_FETCH_COMPLETED",
        details={"endpoint": "GET /api/deployments/diff", "total_changes": total, "dry_run": False},
    )
    return result

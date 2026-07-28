"""
Version-coherence panel — GET /api/version-coherence/overview.

Reads the Firestore verdict-store (``version_coherence_verdicts``, one document per repo) written
by unified-trading-pm's ``.github/workflows/version-coherence-check.yml`` (scheduled, every 30 min)
via ``scripts/cicd/assert_version_coherence.py --json`` + ``write_version_coherence_verdicts.py``.

**Read-only proxy — this route NEVER re-derives VERSION_SPLIT / VESTIGIAL_SCALAR_DRIFT /
DEP_FLOOR_UNSATISFIABLE itself.** That was the explicit "do NOT reimplement" trap flagged when this
panel was first scoped (CLAUDE.md § "Manifest version-surface semantics" — VESTIGIAL_SCALAR_DRIFT is
harmless-when-stale, dep-floors are intentional range pins, not bugs to "fix" by syncing): the panel
surfaces the checker's VERDICT, not a second implementation.

Plan: unified-trading-pm/plans/active/monitoring_control_plane_master_2026_06_10.md
(version-coherence panel item). Operator decision 2026-07-27: build the real Firestore
verdict-store generalisation, not a faithful-port workaround.
"""

from __future__ import annotations

from typing import TypedDict, cast

from fastapi import APIRouter

from deployment_api.deployment_api_config import DeploymentApiConfig
from deployment_api.settings import gcp_project_id as default_project_id

from ._repo_ci_types import _now_iso  # pyright: ignore[reportPrivateUsage]
from ._verdict_store_reader import VERSION_COHERENCE_COLLECTION, get_all_verdicts_with_status

router = APIRouter(prefix="/api/version-coherence", tags=["Version Coherence"])

# The four verdict classes assert_version_coherence.py's --json mode emits (build_repo_verdicts'
# worst-of priority order). Mirrored here as a display-order constant only — the panel does not
# re-derive membership, it just orders what Firestore already resolved.
VERDICT_SEVERITY_ORDER: tuple[str, ...] = (
    "VERSION_SPLIT",
    "DEP_FLOOR_UNSATISFIABLE",
    "VESTIGIAL_SCALAR_DRIFT",
    "OK",
)


class RepoVersionVerdictDict(TypedDict):  # CORRECT-LOCAL: deployment-api response shape, not a domain contract
    """One repo's stored verdict doc, shaped for the UI."""

    verdict: str
    reasons: list[str]
    checked_at: str | None


class VersionCoherenceOverviewDict(TypedDict):  # CORRECT-LOCAL: deployment-api response shape, not a domain contract
    generated_at: str
    source: str  # "firestore" | "unavailable" | "mock"
    repos: dict[str, RepoVersionVerdictDict]


def _shape_doc(doc: dict[str, object]) -> RepoVersionVerdictDict:
    verdict = doc.get("verdict")
    reasons = doc.get("reasons")
    checked_at = doc.get("checked_at")
    reasons_list: list[str] = []
    if isinstance(reasons, list):
        reasons_list = [str(cast("object", r)) for r in reasons]  # pyright: ignore[reportUnknownVariableType]
    return RepoVersionVerdictDict(
        verdict=str(verdict) if isinstance(verdict, str) and verdict else "UNKNOWN",
        reasons=reasons_list,
        checked_at=str(checked_at) if isinstance(checked_at, str) else None,
    )


def _mock_overview() -> VersionCoherenceOverviewDict:
    """Deterministic fixture for mock mode / UI dev / the playwright regression spec — one repo per
    verdict class so every chip tone is exercised."""
    return VersionCoherenceOverviewDict(
        generated_at="2026-07-27T12:00:00+00:00",
        source="mock",
        repos={
            "unified-trading-library": RepoVersionVerdictDict(
                verdict="OK", reasons=[], checked_at="2026-07-27T11:30:00Z"
            ),
            "instruments-service": RepoVersionVerdictDict(
                verdict="VERSION_SPLIT",
                reasons=["instruments-service: version split (source pyproject.version != manifest SSOT)"],
                checked_at="2026-07-27T11:30:00Z",
            ),
            "deployment-api": RepoVersionVerdictDict(
                verdict="VESTIGIAL_SCALAR_DRIFT",
                reasons=["deployment-api: repositories{}.version=0.42.0 != versions{}=0.57.0"],
                checked_at="2026-07-27T11:30:00Z",
            ),
            "strategy-service": RepoVersionVerdictDict(
                verdict="DEP_FLOOR_UNSATISFIABLE",
                reasons=[
                    "strategy-service -> unified-trading-library: versions{}=0.48.0 "
                    "does not satisfy declared range '>=0.60.0,<1.0.0'"
                ],
                checked_at="2026-07-27T11:30:00Z",
            ),
        },
    )


@router.get("/overview")
async def get_version_coherence_overview() -> VersionCoherenceOverviewDict:
    """Fleet matrix: every repo's stored version-coherence verdict + why."""
    cfg = DeploymentApiConfig()
    if cfg.is_mock_mode():
        return _mock_overview()

    project_id = default_project_id or ""
    docs, available = get_all_verdicts_with_status(VERSION_COHERENCE_COLLECTION, project_id=project_id)
    source = "firestore" if available else "unavailable"
    repos = {repo: _shape_doc(doc) for repo, doc in docs.items()}
    return VersionCoherenceOverviewDict(generated_at=_now_iso(), source=source, repos=repos)

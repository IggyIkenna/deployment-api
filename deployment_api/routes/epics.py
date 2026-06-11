"""
Epic Readiness API Routes
=========================
Endpoints for asset-class epic readiness aggregation.

SSOT:
  Definitions:  unified-trading-codex/11-project-management/epics/{epic}-epic.yaml
  Per-repo data: unified-trading-codex/10-audit/repos/{repo}.yaml (asset_group_readiness)
  Schema:        unified-trading-codex/11-project-management/epics/epic-schema.yaml
  Computed by:   unified-trading-pm/scripts/compute-epic-readiness.py

Endpoints:
  GET /api/epics            — list all 4 epics with % complete
  GET /api/epics/{epic_id}  — detailed epic status with per-repo breakdown
"""

from __future__ import annotations

import logging
from pathlib import Path
from typing import cast

import aiohttp
import yaml
from fastapi import APIRouter, HTTPException, Request

from deployment_api.deployment_api_config import DeploymentApiConfig
from deployment_api.routes._epics_plans import load_epics_plans
from deployment_api.routes._epics_plans_types import (
    EpicCardDict,
    EpicPlanDict,
    EpicsPlansResponseDict,
)
from deployment_api.routes._repo_ci_github import resolve_gh_token
from deployment_api.settings import gcp_project_id as default_project_id

logger = logging.getLogger(__name__)

router = APIRouter()


def _mock_plan(slug: str, parent: str, done: int, open_: int, open_p01: int) -> EpicPlanDict:
    total = done + open_
    return EpicPlanDict(
        slug=slug,
        parent_epic=parent,
        status="active",
        estimate_class="brand-new",
        done=done,
        open=open_,
        open_p0p1=open_p01,
        pct=round(100.0 * done / total, 1) if total > 0 else 0.0,
        github_url=f"https://github.com/IggyIkenna/unified-trading-pm/blob/main/plans/active/{slug}.md",
    )


def _mock_epics_plans() -> EpicsPlansResponseDict:
    """Mock fixture mirroring _epics_plans.load_epics_plans — two epics (one with plans,
    one empty) + one orphan, so the UI/playwright can assert cards, drilldown, orphans."""
    obs_plans = [
        _mock_plan("monitoring_control_plane_master_2026_06_10", "observability_master", 18, 5, 2),
        _mock_plan("ci_dashboard_deployment_ui_2026_06_10", "observability_master", 30, 6, 1),
    ]
    epics: list[EpicCardDict] = [
        EpicCardDict(
            name="observability_master",
            slug="observability_master",
            title="Observability Master",
            tier="L4",
            priority="P0",
            assigned_vm="vm-cross-cutting",
            status="active",
            github_url="https://github.com/IggyIkenna/unified-trading-pm/blob/main/plans/epics/observability_master.md",
            plans=obs_plans,
            plan_count=len(obs_plans),
            done_total=48,
            open_total=11,
        ),
        EpicCardDict(
            name="orchestrator_master",
            slug="orchestrator_master",
            title="Orchestrator Master",
            tier="L4",
            priority="P1",
            assigned_vm="vm-orchestrator",
            status="active",
            github_url="https://github.com/IggyIkenna/unified-trading-pm/blob/main/plans/epics/orchestrator_master.md",
            plans=[],
            plan_count=0,
            done_total=0,
            open_total=0,
        ),
    ]
    orphans = [_mock_plan("some_orphan_plan_2026_06_10", "", 2, 4, 1)]
    return EpicsPlansResponseDict(
        generated_at="2026-06-10T13:00:00Z",
        source="mock",
        epics=epics,
        orphans=orphans,
        orphan_count=len(orphans),
    )


# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------

_CR_ORDINALS: dict[str, int] = {
    "cr0": 0,
    "cr1": 1,
    "cr2": 2,
    "cr3": 3,
    "cr4": 4,
    "cr5": 5,
}

_BR_ORDINALS: dict[str, int] = {
    "br0": 0,
    "br1": 1,
    "br2": 2,
    "br3": 3,
    "br4": 4,
    "br5": 5,
    "br6": 6,
    "br7": 7,
    "br8": 8,
    "na": -1,
}

_EPIC_DISPLAY_ORDER = ["defi", "cefi", "tradfi", "sports"]


def _extract_data_fields(ac_data: dict[str, object]) -> dict[str, object]:
    """Extract nested data fields from ac_data safely, avoiding union-attr on .get() chains."""
    data_raw = ac_data.get("data")
    if not isinstance(data_raw, dict):
        return {
            "historical_available": False,
            "live_available": False,
            "mock_available": False,
            "testnet_available": False,
            "historical_start_date": None,
        }
    data_dict = cast(dict[str, object], data_raw)
    return {
        "historical_available": data_dict.get("historical_available", False),
        "live_available": data_dict.get("live_available", False),
        "mock_available": data_dict.get("mock_available", False),
        "testnet_available": data_dict.get("testnet_available", False),
        "historical_start_date": data_dict.get("historical_start_date"),
    }


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _load_yaml(path: Path) -> dict[str, object]:
    with path.open() as f:
        data = cast(dict[str, object] | None, yaml.safe_load(f))
    return data if data is not None else {}


def _cr_ordinal(stage: str | None) -> int:
    return _CR_ORDINALS.get((stage or "cr0").lower(), 0)


def _br_ordinal(stage: str | None) -> int:
    return _BR_ORDINALS.get((stage or "br0").lower(), 0)


def _get_branch_status(
    repo_data: dict[str, object],
    asset_group: str,
) -> dict[str, object]:
    """Return branch_status dict for the given asset class (or core fallback)."""
    acr = repo_data.get("asset_group_readiness")
    if not isinstance(acr, dict):
        return {}
    acr_dict = cast(dict[str, object], acr)
    class_data_val: object = acr_dict.get(asset_group) or acr_dict.get("core") or {}
    if not isinstance(class_data_val, dict):
        return {}
    class_dict = cast(dict[str, object], class_data_val)
    branch_status: object = class_dict.get("branch_status")
    return cast(dict[str, object], branch_status) if isinstance(branch_status, dict) else {}


def _get_asset_group_data(
    repo_data: dict[str, object],
    asset_group: str,
) -> dict[str, object]:
    """Return asset class block for data availability, feature groups, etc."""
    acr = repo_data.get("asset_group_readiness")
    if not isinstance(acr, dict):
        return {}
    acr_dict2 = cast(dict[str, object], acr)
    result_val: object = acr_dict2.get(asset_group) or acr_dict2.get("core") or {}
    return cast(dict[str, object], result_val) if isinstance(result_val, dict) else {}


def _is_repo_complete(
    repo_data: dict[str, object],
    min_cr: str,
    min_br: str,
    asset_group: str,
) -> tuple[bool, str]:
    """Return (complete, reason) for a single required repo."""
    cr_section = repo_data.get("code_readiness")
    cr_current = (
        cast(dict[str, object], cr_section).get("current_stage", "CR0") if isinstance(cr_section, dict) else "CR0"
    )
    if _cr_ordinal(str(cr_current)) < _cr_ordinal(min_cr):
        return False, f"CR stage: {cr_current} (need {min_cr.upper()})"

    if min_br.lower() != "na":
        br_section = repo_data.get("business_readiness")
        br_current = (
            cast(dict[str, object], br_section).get("current_stage", "BR0") if isinstance(br_section, dict) else "BR0"
        )
        if _br_ordinal(str(br_current)) < _br_ordinal(min_br):
            return False, f"BR stage: {br_current} (need {min_br.upper()})"

    branch_status = _get_branch_status(repo_data, asset_group)
    main_status = branch_status.get("main")
    main_qm = cast(dict[str, object], main_status).get("quickmerged", False) if isinstance(main_status, dict) else False
    if not main_qm:
        return False, "branch_status.main.quickmerged = false"

    return True, ""


# ---------------------------------------------------------------------------
# Core computation (inline — no subprocess call)
# ---------------------------------------------------------------------------


def _compute_epic(
    epic: dict[str, object],
    all_repos: dict[str, dict[str, object]],
) -> dict[str, object]:
    """Compute full epic status dict from definition + per-repo data."""
    epic_id = str(epic.get("epic_id", "unknown"))
    required: list[dict[str, object]] = cast(list[dict[str, object]], epic.get("required_repos") or [])
    optional: list[dict[str, object]] = cast(list[dict[str, object]], epic.get("optional_repos") or [])

    total_required = len(required)
    completed = 0
    blocking_repos: list[dict[str, object]] = []
    completed_repos: list[str] = []

    for req in required:
        repo_name = str(req.get("repo", ""))  # noqa: qg-empty-fallback — YAML epic deserialization
        min_cr = str(req.get("min_stage", "cr5"))
        min_br = str(req.get("min_br_stage", "na"))
        asset_group = str(req.get("asset_group", "core"))

        repo_data = all_repos.get(repo_name)
        if repo_data is None:
            blocking_repos.append(
                {
                    "repo": repo_name,
                    "arch_tier": req.get("arch_tier"),
                    "asset_group": asset_group,
                    "blocking_reason": "No per-repo YAML found in 10-audit/repos/",
                    "cr_current": None,
                    "cr_required": min_cr.upper(),
                    "br_current": None,
                    "br_required": min_br.upper() if min_br.lower() != "na" else "na",
                    "main_quickmerged": False,
                    "data": {},
                }
            )
            continue

        ok, reason = _is_repo_complete(repo_data, min_cr, min_br, asset_group)

        cr_section = repo_data.get("code_readiness")
        cr_current = (
            cast(dict[str, object], cr_section).get("current_stage", "CR0") if isinstance(cr_section, dict) else "CR0"
        )
        br_section = repo_data.get("business_readiness")
        br_current = (
            cast(dict[str, object], br_section).get("current_stage", "BR0") if isinstance(br_section, dict) else "BR0"
        )
        branch_status = _get_branch_status(repo_data, asset_group)
        main_status = branch_status.get("main")
        main_qm = (
            cast(dict[str, object], main_status).get("quickmerged", False) if isinstance(main_status, dict) else False
        )
        ac_data = _get_asset_group_data(repo_data, asset_group)

        repo_entry: dict[str, object] = {
            "repo": repo_name,
            "arch_tier": req.get("arch_tier"),
            "asset_group": asset_group,
            "cr_current": str(cr_current),
            "cr_required": min_cr.upper(),
            "br_current": str(br_current),
            "br_required": min_br.upper() if min_br.lower() != "na" else "na",
            "main_quickmerged": bool(main_qm),
            "branch_status": branch_status,
            "data": _extract_data_fields(ac_data),
            "feature_groups": ac_data.get("feature_groups") or [],
            "ml_models": ac_data.get("ml_models") or [],
            "venue_deps": ac_data.get("venue_deps") or [],
        }

        if ok:
            completed += 1
            completed_repos.append(repo_name)
        else:
            repo_entry["blocking_reason"] = reason
            blocking_repos.append(repo_entry)

    epic_pct = round(completed / total_required * 100) if total_required > 0 else 0

    optional_status = [
        {
            "repo": str(opt.get("repo", "")),  # noqa: qg-empty-fallback — YAML epic deserialization
            "asset_group": str(opt.get("asset_group", "")),  # noqa: qg-empty-fallback — YAML epic deserialization
            "note": str(opt.get("note", "")),  # noqa: qg-empty-fallback — YAML epic deserialization
            "yaml_present": str(opt.get("repo", "")) in all_repos,  # noqa: qg-empty-fallback — YAML epic deserialization
        }
        for opt in optional
    ]

    return {
        "epic_id": epic_id,
        "display_name": str(epic.get("display_name", epic_id)),
        "mvp_priority": int(cast(int, epic.get("mvp_priority", 99))),
        "business_requirement_minimum": str(epic.get("business_requirement_minimum", "br5_pnl_backtest")),
        "epic_pct": epic_pct,
        "total_required": total_required,
        "completed": completed,
        "epic_complete": epic_pct == 100,
        "completion_criteria": epic.get("completion_criteria") or {},
        "blocking_repos": blocking_repos,
        "completed_repos": completed_repos,
        "optional_repos_status": optional_status,
    }


def _load_all_repos(codex_dir: Path) -> dict[str, dict[str, object]]:
    repos: dict[str, dict[str, object]] = {}
    for yaml_file in codex_dir.glob("*.yaml"):
        data = _load_yaml(yaml_file)
        repo_name = str(data.get("repo") or yaml_file.stem)
        repos[repo_name] = data
    return repos


def _load_all_epics(epics_dir: Path) -> list[dict[str, object]]:
    epics: list[dict[str, object]] = []
    for yaml_file in sorted(epics_dir.glob("*-epic.yaml")):
        epics.append(_load_yaml(yaml_file))
    return epics


def _sort_key(epic: dict[str, object]) -> int:
    epic_id = str(epic.get("epic_id", ""))  # noqa: qg-empty-fallback — YAML epic deserialization
    try:
        return _EPIC_DISPLAY_ORDER.index(epic_id)
    except ValueError:
        return 99


# ---------------------------------------------------------------------------
# Routes
# ---------------------------------------------------------------------------


@router.get("")
async def list_epics(request: Request) -> list[dict[str, object]]:
    """
    List all epics with summary readiness percentages.

    Returns compact summaries: epic_id, display_name, mvp_priority,
    epic_pct, total_required, completed, epic_complete, blocking_count.

    Reads from:
      - codex_dir (10-audit/repos/) for per-repo data
      - epics_dir (11-project-management/epics/) for epic definitions
    """
    _state: object = getattr(getattr(request, "app", None), "state", None)
    epics_dir = cast(Path | None, getattr(_state, "epics_dir", None))
    codex_dir = cast(Path | None, getattr(_state, "codex_dir", None))

    if epics_dir is None:
        raise HTTPException(
            status_code=503,
            detail="Epics directory not configured. "
            "Set codex-data-epics symlink or ensure "
            "unified-trading-codex/11-project-management/epics/ is accessible.",
        )
    if codex_dir is None:
        raise HTTPException(
            status_code=503,
            detail="Codex directory not configured. unified-trading-codex/10-audit/repos/ must be accessible.",
        )

    epics = _load_all_epics(epics_dir)
    if not epics:
        return []

    all_repos = _load_all_repos(codex_dir)
    epics.sort(key=_sort_key)

    summaries: list[dict[str, object]] = []
    for epic in epics:
        result = _compute_epic(epic, all_repos)
        summaries.append(
            {
                "epic_id": result["epic_id"],
                "display_name": result["display_name"],
                "mvp_priority": result["mvp_priority"],
                "epic_pct": result["epic_pct"],
                "total_required": result["total_required"],
                "completed": result["completed"],
                "epic_complete": result["epic_complete"],
                "blocking_count": len(cast(list[object], result["blocking_repos"])),
                "business_requirement_minimum": result["business_requirement_minimum"],
            }
        )

    return summaries


@router.get("/plans")
async def list_epic_plans() -> EpicsPlansResponseDict:
    """Epics tab v2 — LIVE PM epics + active-plan drilldown (operator add 2026-06-10).

    Replaces the stale archived-codex asset-class view: reads `plans/epics/*.md` +
    `plans/active/*.md` frontmatter from PM `main` (GitHub contents API, 300 s TTL),
    rolls active plans under their `parent_epic`, counts checkboxes for completion %,
    and surfaces orphans (no parent_epic) as a review-blocking strip. Declared BEFORE
    `/{epic_id}` so `/plans` is not captured by the path param. Honest 503 on no token.
    """
    cfg = DeploymentApiConfig()
    if cfg.is_mock_mode():
        return _mock_epics_plans()
    project_id = default_project_id or ""
    token = await resolve_gh_token(project_id)
    async with aiohttp.ClientSession() as session:
        return await load_epics_plans(session, token)


@router.get("/{epic_id}")
async def get_epic_detail(epic_id: str, request: Request) -> dict[str, object]:
    """
    Get detailed epic status: per-repo CR/BR stages, branch status,
    data availability, feature groups, ML models, and blocking reasons.
    """
    _state: object = getattr(getattr(request, "app", None), "state", None)
    epics_dir = cast(Path | None, getattr(_state, "epics_dir", None))
    codex_dir = cast(Path | None, getattr(_state, "codex_dir", None))

    if epics_dir is None:
        raise HTTPException(status_code=503, detail="Epics directory not configured.")
    if codex_dir is None:
        raise HTTPException(status_code=503, detail="Codex directory not configured.")

    epic_path = epics_dir / f"{epic_id}-epic.yaml"
    if not epic_path.exists():
        raise HTTPException(
            status_code=404,
            detail=f"Epic '{epic_id}' not found. "
            f"Expected: epics/{epic_id}-epic.yaml. "
            f"Valid epic IDs: defi, cefi, tradfi, sports.",
        )

    epic = _load_yaml(epic_path)
    all_repos = _load_all_repos(codex_dir)
    return _compute_epic(epic, all_repos)

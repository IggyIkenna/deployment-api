"""TypedDict shapes for the Epics-tab-v2 live PM epics + plan drilldown.

Plan: ci_dashboard_deployment_ui_2026_06_10.md Phase 2 "Epics tab v2".
"""

from __future__ import annotations

from typing import TypedDict


class EpicPlanDict(TypedDict):  # CORRECT-LOCAL: deployment-api response shape, not a domain contract
    """One active plan under an epic (or an orphan)."""

    slug: str
    parent_epic: str
    status: str
    estimate_class: str
    done: int
    open: int
    open_p0p1: int
    pct: float
    github_url: str


class EpicCardDict(TypedDict):  # CORRECT-LOCAL: deployment-api response shape, not a domain contract
    """One epic card with its rolled-up active plans."""

    name: str
    title: str
    tier: str
    priority: str
    assigned_vm: str
    status: str
    github_url: str
    plans: list[EpicPlanDict]
    plan_count: int
    done_total: int
    open_total: int


class EpicsPlansResponseDict(TypedDict):  # CORRECT-LOCAL: deployment-api response shape, not a domain contract
    """GET /api/epics/plans response — live PM epics + plan drilldown + orphans."""

    generated_at: str
    source: str  # "live" | "mock"
    epics: list[EpicCardDict]
    orphans: list[EpicPlanDict]  # active plans with no parent_epic — review-blocking
    orphan_count: int

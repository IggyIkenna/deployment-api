"""Live PM epics + active-plan drilldown for the Epics tab v2 (operator add 2026-06-10).

The legacy `/api/epics` reads the ARCHIVED `unified-trading-codex` epic yamls (4 stale
asset-class epics — dead source per CLAUDE.md). This reads the LIVE PM repo via the GitHub
contents API behind a 300 s TTL cache (mirrors `_repo_ci_manifest.py`):

  - `plans/epics/*.md` frontmatter → epic cards (name, title, tier, priority, assigned_vm, status).
  - `plans/active/*.md` frontmatter `parent_epic:` + `- [x]`/`- [ ]` checkbox counts → per-epic
    drilldown (each active plan with completion % + open count + estimate fields); plans with no
    `parent_epic` surface as a review-blocking orphans strip (per the active-plan-inventory rule).

Plan: ci_dashboard_deployment_ui_2026_06_10.md Phase 2 "Epics tab v2".
"""

from __future__ import annotations

import asyncio
import datetime as dt
import logging
import re
import time
from typing import cast

import aiohttp
import yaml

from deployment_api.routes._epics_plans_types import (
    EpicCardDict,
    EpicPlanDict,
    EpicsPlansResponseDict,
)
from deployment_api.routes._repo_ci_github import gh_get_json, gh_raw_file
from deployment_api.settings import GITHUB_ORG

logger = logging.getLogger(__name__)

_PM_REPO = "unified-trading-pm"
_TTL_SECONDS = 300.0
_FETCH_CONCURRENCY = 8
_EPICS_DIR = "plans/epics"
_ACTIVE_DIR = "plans/active"

# Tier sort: L0 (foundation) → L5; unknown last. Priority sort P0 → P3.
_TIER_ORDER = {"l0": 0, "l1": 1, "l2": 2, "l3": 3, "l4": 4, "l5": 5}
_PRIORITY_ORDER = {"p0": 0, "p1": 1, "p2": 2, "p3": 3}

_cache: tuple[float, EpicsPlansResponseDict] | None = None

# A top-level plan checkbox per PLAN_FORMAT: "- [x] " / "- [ ] " at any indent.
_DONE_RE = re.compile(r"^\s*- \[[xX]\] ")
_OPEN_RE = re.compile(r"^\s*- \[ \] ")
# Open P0/P1 todos (priority tag right after the role tag, e.g. "[CODE] P1.").
_OPEN_P01_RE = re.compile(r"^\s*- \[ \].*\bP[01]\b")


def _now_iso() -> str:
    return dt.datetime.now(dt.UTC).isoformat()


def _parse_frontmatter(text: str) -> dict[str, object]:
    """Parse the leading `--- ... ---` YAML frontmatter; {} if absent/malformed."""
    if not text.startswith("---"):
        return {}
    end = text.find("\n---", 3)
    if end == -1:
        return {}
    block = text[3:end]
    try:
        parsed = cast(object, yaml.safe_load(block))
    except yaml.YAMLError:
        return {}
    return cast(dict[str, object], parsed) if isinstance(parsed, dict) else {}


def _str_field(fm: dict[str, object], key: str) -> str:
    value = fm.get(key)
    return str(value) if value is not None and not isinstance(value, (dict, list)) else ""


def _count_checkboxes(text: str) -> tuple[int, int, int]:
    """(done, open, open_p0p1) checkbox counts for an active plan body."""
    done = open_ = open_p01 = 0
    for line in text.splitlines():
        if _DONE_RE.match(line):
            done += 1
        elif _OPEN_RE.match(line):
            open_ += 1
            if _OPEN_P01_RE.match(line):
                open_p01 += 1
    return done, open_, open_p01


def _normalize_epic_ref(ref: str) -> str:
    """Reduce any epic reference to its bare slug for matching.

    Plan `parent_epic:` is declared inconsistently across the repo — `mtds_mdps_master`,
    `epics/mtds_mdps_master.md`, `plans/epics/infrastructure_master.md` all mean the same epic.
    Strip the directory prefix + `.md` suffix and lowercase so they all collapse to one key.
    """
    bare = ref.strip().rsplit("/", 1)[-1]
    if bare.endswith(".md"):
        bare = bare[: -len(".md")]
    return bare.lower()


async def _list_md(session: aiohttp.ClientSession, token: str, path: str) -> list[str]:
    """List `*.md` file paths directly under a PM directory (top-level only)."""
    listing = await gh_get_json(session, token, f"/repos/{GITHUB_ORG}/{_PM_REPO}/contents/{path}?ref=main")
    if not isinstance(listing, list):
        return []
    out: list[str] = []
    for entry in cast(list[object], listing):
        if not isinstance(entry, dict):
            continue
        e = cast(dict[str, object], entry)
        name = e.get("name")
        if e.get("type") == "file" and isinstance(name, str) and name.endswith(".md"):
            out.append(f"{path}/{name}")
    return out


async def _fetch_epic(
    session: aiohttp.ClientSession, token: str, path: str, sem: asyncio.Semaphore
) -> EpicCardDict | None:
    async with sem:
        try:
            text = await gh_raw_file(session, token, GITHUB_ORG, _PM_REPO, path, ref="main")
        except (TimeoutError, aiohttp.ClientError):
            return None
    fm = _parse_frontmatter(text)
    slug = path.rsplit("/", 1)[-1].removesuffix(".md")
    name = _str_field(fm, "name") or slug
    return EpicCardDict(
        name=name,
        slug=slug,
        title=_str_field(fm, "title") or name,
        tier=_str_field(fm, "tier"),
        priority=_str_field(fm, "priority"),
        assigned_vm=_str_field(fm, "assigned_vm"),
        status=_str_field(fm, "status"),
        github_url=f"https://github.com/{GITHUB_ORG}/{_PM_REPO}/blob/main/{path}",
        plans=[],
        plan_count=0,
        done_total=0,
        open_total=0,
    )


async def _fetch_plan(
    session: aiohttp.ClientSession, token: str, path: str, sem: asyncio.Semaphore
) -> EpicPlanDict | None:
    async with sem:
        try:
            text = await gh_raw_file(session, token, GITHUB_ORG, _PM_REPO, path, ref="main")
        except (TimeoutError, aiohttp.ClientError):
            return None
    fm = _parse_frontmatter(text)
    done, open_, open_p01 = _count_checkboxes(text)
    total = done + open_
    slug = path.rsplit("/", 1)[-1].removesuffix(".md")
    return EpicPlanDict(
        slug=slug,
        parent_epic=_str_field(fm, "parent_epic"),
        status=_str_field(fm, "status"),
        estimate_class=_str_field(fm, "estimate_class"),
        done=done,
        open=open_,
        open_p0p1=open_p01,
        pct=round(100.0 * done / total, 1) if total > 0 else 0.0,
        github_url=f"https://github.com/{GITHUB_ORG}/{_PM_REPO}/blob/main/{path}",
    )


async def load_epics_plans(session: aiohttp.ClientSession, token: str) -> EpicsPlansResponseDict:
    """Live epics + active-plan drilldown from PM `main` (300 s TTL cache)."""
    global _cache
    now = time.monotonic()
    if _cache is not None and now - _cache[0] < _TTL_SECONDS:
        return _cache[1]

    epic_paths = await _list_md(session, token, _EPICS_DIR)
    plan_paths = await _list_md(session, token, _ACTIVE_DIR)
    sem = asyncio.Semaphore(_FETCH_CONCURRENCY)

    epics_raw = await asyncio.gather(*[_fetch_epic(session, token, p, sem) for p in epic_paths])
    plans_raw = await asyncio.gather(*[_fetch_plan(session, token, p, sem) for p in plan_paths])
    epics = [e for e in epics_raw if e is not None]
    plans = [p for p in plans_raw if p is not None]

    # Match on the NORMALIZED epic slug, not the raw string — plan `parent_epic:` is declared in
    # three inconsistent forms across the repo (`mtds_mdps_master`, `epics/mtds_mdps_master.md`,
    # `plans/epics/infrastructure_master.md`). An exact-string match wrongly orphans the path-forms
    # (e.g. every asset-group `*_manifest_canonicalisation` plan declares `epics/mtds_mdps_master.md`).
    by_key: dict[str, EpicCardDict] = {}
    for e in epics:
        by_key[_normalize_epic_ref(e["slug"])] = e
        by_key.setdefault(_normalize_epic_ref(e["name"]), e)
    orphans: list[EpicPlanDict] = []
    for plan in plans:
        parent = plan["parent_epic"]
        epic = by_key.get(_normalize_epic_ref(parent)) if parent else None
        if epic is None:
            orphans.append(plan)
            continue
        epic["plans"].append(plan)
        epic["plan_count"] += 1
        epic["done_total"] += plan["done"]
        epic["open_total"] += plan["open"]

    for epic in epics:
        epic["plans"].sort(key=lambda p: (-p["open_p0p1"], -p["open"], p["slug"]))
    # Sort epics by tier then priority then name.
    epics.sort(
        key=lambda e: (
            _TIER_ORDER.get(e["tier"].lower(), 99),
            _PRIORITY_ORDER.get(e["priority"].lower(), 99),
            e["name"],
        )
    )

    result = EpicsPlansResponseDict(
        generated_at=_now_iso(),
        source="live",
        epics=epics,
        orphans=sorted(orphans, key=lambda p: p["slug"]),
        orphan_count=len(orphans),
    )
    _cache = (now, result)
    return result

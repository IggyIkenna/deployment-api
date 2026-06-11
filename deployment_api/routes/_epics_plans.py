"""Live PM epics + active-plan drilldown for the Epics tab v2 (operator add 2026-06-10).

The legacy `/api/epics` reads the ARCHIVED `unified-trading-codex` epic yamls (4 stale
asset-class epics — dead source per CLAUDE.md). This reads the LIVE PM repo in **ONE
GraphQL query** (both plan trees with inline blob text) behind a 300 s TTL cache:

  - `plans/epics/*.md` frontmatter → epic cards (name, title, tier, priority, assigned_vm, status).
  - `plans/active/*.md` frontmatter `parent_epic:` + `- [x]`/`- [ ]` checkbox counts → per-epic
    drilldown (each active plan with completion % + open count + estimate fields); plans with no
    `parent_epic` surface as a review-blocking orphans strip (per the active-plan-inventory rule).

Quota budget (2026-06-11 fix — live 503 reproduced): the original per-file contents-API
loader cost ~92 REST requests per cache miss and exhausted the GH_PAT 5,000/hr core budget
under normal browsing. Now: one ~1-point GraphQL query (its OWN 5,000-pt budget — zero core
spend), rare per-file REST fallback only for truncated blobs, and on rate-limit the LAST
cached payload is served with `stale: true` instead of a 503 (alert-parity: degraded ≠ blank).

Plan: ci_dashboard_deployment_ui_2026_06_10.md Phase 2 "Epics tab v2".
"""

from __future__ import annotations

import datetime as dt
import logging
import re
import time
from typing import cast

import aiohttp
import yaml
from fastapi import HTTPException

from deployment_api.routes._epics_plans_types import (
    EpicCardDict,
    EpicPlanDict,
    EpicsPlansResponseDict,
)
from deployment_api.routes._repo_ci_github import gh_graphql, gh_raw_file
from deployment_api.settings import GITHUB_ORG

logger = logging.getLogger(__name__)

_PM_REPO = "unified-trading-pm"
_TTL_SECONDS = 300.0
_EPICS_DIR = "plans/epics"
_ACTIVE_DIR = "plans/active"
# Read plans from LDR, not main: LDR is the plan SSOT (plans are authored there and drain to
# main on a ~15-min promote) — reading main false-orphans any plan whose parent_epic landed on
# LDR inside the promotion-lag window (operator-reported 2026-06-11: 2 of 25 "orphans" were lag).
_REF = "live-defi-rollout"
# Housekeeping files that are not plans/epics — never orphan-strip or epic-card material
# (per the inventory-tracker rule, the orphan check applies to ACTIVE PLANS only; README is
# the epic registry doc). `_`-prefixed files (_agent_pings.md) are skipped by prefix below.
_NON_PLAN_FILES = frozenset({"INDEX.md", "task_template.md", "README.md"})

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


_SIMPLE_FM_LINE = re.compile(r"^([a-z][a-z0-9_]*):[ \t]+(.+?)[ \t]*$", re.IGNORECASE)


def _line_based_frontmatter(block: str) -> dict[str, object]:
    """Best-effort extraction of top-level scalar `key: value` pairs from a frontmatter block.

    Fallback for when `yaml.safe_load` chokes (e.g. a `title:` prettier-wrapped across lines with a
    `\\` continuation, or a `source:` list whose plain scalars embed `:`/quotes — both seen in live
    PM plans). The fields the epics tab needs (parent_epic / status / tier / priority / assigned_vm /
    name / title) are always single-line `key: value`, so a regex over top-level (column-0) lines
    recovers them even when the document as a whole is invalid YAML. Indented (list/continuation)
    lines are skipped so a malformed `source:` block can't corrupt a later key.
    """
    out: dict[str, object] = {}
    for line in block.splitlines():
        if not line or line[0] in " \t#-":  # skip indented / comment / list-item / blank lines
            continue
        m = _SIMPLE_FM_LINE.match(line)
        if m:
            out.setdefault(m.group(1), m.group(2).strip().strip("\"'"))
    return out


def _parse_frontmatter(text: str) -> dict[str, object]:
    """Parse the leading `--- ... ---` frontmatter; {} if absent. Robust to invalid YAML — falls
    back to a line-based scalar extraction so a plan is never silently dropped over a wrapped title
    or a funky `source:` list (regression: 2 live plans orphaned 2026-06-11 despite valid parent_epic)."""
    if not text.startswith("---"):
        return {}
    end = text.find("\n---", 3)
    if end == -1:
        return {}
    block = text[3:end]
    try:
        parsed = cast(object, yaml.safe_load(block))
    except yaml.YAMLError:
        return _line_based_frontmatter(block)
    if isinstance(parsed, dict):
        return cast(dict[str, object], parsed)
    return _line_based_frontmatter(block)


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


def _is_plan_md(entry_type: object, name: object) -> bool:
    """True for a real plan/epic markdown file; excludes housekeeping + `_`-prefixed files.

    `entry_type` is "file" from the REST contents API or "blob" from a GraphQL tree entry —
    both mean a regular file."""
    if entry_type not in ("file", "blob") or not isinstance(name, str) or not name.endswith(".md"):
        return False
    return name not in _NON_PLAN_FILES and not name.startswith("_")  # _agent_pings.md etc.


# ONE query fetches BOTH plan directories' entries WITH inline blob text — the whole
# cold load in a single ~1-point request (vs ~92 REST calls; see module docstring).
_GQL_PLAN_TREES = """
query($owner: String!, $name: String!, $epicsExpr: String!, $activeExpr: String!) {
  repository(owner: $owner, name: $name) {
    epics: object(expression: $epicsExpr) {
      ... on Tree { entries { name type object { ... on Blob { text isTruncated } } } }
    }
    active: object(expression: $activeExpr) {
      ... on Tree { entries { name type object { ... on Blob { text isTruncated } } } }
    }
  }
}
"""


def _as_dict(value: object) -> dict[str, object]:
    return cast(dict[str, object], value) if isinstance(value, dict) else {}


def _tree_entries(data: dict[str, object], alias: str) -> list[dict[str, object]]:
    """The entries list of one aliased tree object in the GraphQL response ([] if absent)."""
    repo = _as_dict(data.get("repository"))
    tree = _as_dict(repo.get(alias))
    entries = tree.get("entries")
    if not isinstance(entries, list):
        return []
    return [_as_dict(e) for e in cast(list[object], entries)]


async def _entry_text(
    session: aiohttp.ClientSession, token: str, dir_path: str, entry: dict[str, object]
) -> str | None:
    """Blob text of one tree entry — inline from the GraphQL payload, or (rarely) a per-file
    REST fallback when the blob is truncated/binary. None when unreadable."""
    blob = _as_dict(entry.get("object"))
    text = blob.get("text")
    if isinstance(text, str) and not bool(blob.get("isTruncated")):
        return text
    name = str(entry.get("name") or "")
    try:
        return await gh_raw_file(session, token, GITHUB_ORG, _PM_REPO, f"{dir_path}/{name}", ref=_REF)
    except (TimeoutError, aiohttp.ClientError, HTTPException):
        logger.warning("epics: unreadable plan blob %s/%s (skipped)", dir_path, name)
        return None


def _parse_epic(name: str, text: str) -> EpicCardDict:
    fm = _parse_frontmatter(text)
    slug = name.removesuffix(".md")
    display = _str_field(fm, "name") or slug
    return EpicCardDict(
        name=display,
        slug=slug,
        title=_str_field(fm, "title") or display,
        tier=_str_field(fm, "tier"),
        priority=_str_field(fm, "priority"),
        assigned_vm=_str_field(fm, "assigned_vm"),
        status=_str_field(fm, "status"),
        github_url=f"https://github.com/{GITHUB_ORG}/{_PM_REPO}/blob/{_REF}/{_EPICS_DIR}/{name}",
        plans=[],
        plan_count=0,
        done_total=0,
        open_total=0,
    )


def _parse_plan(name: str, text: str) -> EpicPlanDict:
    fm = _parse_frontmatter(text)
    done, open_, open_p01 = _count_checkboxes(text)
    total = done + open_
    slug = name.removesuffix(".md")
    return EpicPlanDict(
        slug=slug,
        parent_epic=_str_field(fm, "parent_epic"),
        status=_str_field(fm, "status"),
        estimate_class=_str_field(fm, "estimate_class"),
        done=done,
        open=open_,
        open_p0p1=open_p01,
        pct=round(100.0 * done / total, 1) if total > 0 else 0.0,
        github_url=f"https://github.com/{GITHUB_ORG}/{_PM_REPO}/blob/{_REF}/{_ACTIVE_DIR}/{name}",
    )


async def load_epics_plans(session: aiohttp.ClientSession, token: str) -> EpicsPlansResponseDict:
    """Live epics + active-plan drilldown from PM LDR — the plan SSOT (300 s TTL cache).

    One GraphQL query per cache miss. On GitHub failure (rate-limit/5xx) the last cached
    payload is served with ``stale=True`` — degraded beats blank; only a cold start with
    no cache at all propagates the error."""
    global _cache
    now = time.monotonic()
    if _cache is not None and now - _cache[0] < _TTL_SECONDS:
        return _cache[1]

    variables: dict[str, object] = {
        "owner": GITHUB_ORG,
        "name": _PM_REPO,
        "epicsExpr": f"{_REF}:{_EPICS_DIR}",
        "activeExpr": f"{_REF}:{_ACTIVE_DIR}",
    }
    try:
        data = await gh_graphql(session, token, _GQL_PLAN_TREES, variables)
    except (HTTPException, TimeoutError, aiohttp.ClientError) as exc:
        if _cache is not None:
            logger.warning("epics: GitHub fetch failed (%s) — serving stale cached payload", exc)
            return EpicsPlansResponseDict(**{**_cache[1], "stale": True})  # pyright: ignore[reportArgumentType]
        raise

    epics: list[EpicCardDict] = []
    for entry in _tree_entries(data, "epics"):
        if not _is_plan_md(entry.get("type"), entry.get("name")):
            continue
        text = await _entry_text(session, token, _EPICS_DIR, entry)
        if text is not None:
            epics.append(_parse_epic(str(entry.get("name")), text))
    plans: list[EpicPlanDict] = []
    for entry in _tree_entries(data, "active"):
        if not _is_plan_md(entry.get("type"), entry.get("name")):
            continue
        text = await _entry_text(session, token, _ACTIVE_DIR, entry)
        if text is not None:
            plans.append(_parse_plan(str(entry.get("name")), text))

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
        stale=False,
        epics=epics,
        orphans=sorted(orphans, key=lambda p: p["slug"]),
        orphan_count=len(orphans),
    )
    _cache = (now, result)
    return result

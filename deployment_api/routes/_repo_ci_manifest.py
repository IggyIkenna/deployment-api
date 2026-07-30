"""
workspace-manifest.json accessor for the repo-CI dashboard — the SINGLE manifest reader.

All manifest-sourced state (repo registry, ci_status, staging_status, deployed_versions)
flows through ManifestView so the Firestore ci_status side-store (Phase 2 of
ci_status_firestore_side_store_2026_06_10.md) is a one-function swap: replace
`ManifestView.ci_status_for` with a store read, nothing else moves.

Fetches the manifest from PM `main` via the GitHub contents API (raw accept header — the
repo is private, raw.githubusercontent needs the same token anyway) behind a short TTL
cache mirroring the _cloud_builds TTL pattern.
"""

from __future__ import annotations

import json
import logging
import time
from dataclasses import dataclass
from typing import cast

import aiohttp

from deployment_api.settings import GITHUB_ORG

from ._ci_status_firestore_store import (  # pyright: ignore[reportPrivateUsage]
    _CodebaseHealthDict,
    resolve_ci_status_map,
    resolve_codebase_health_map,
    resolve_release_version_map,
)
from ._repo_ci_github import gh_raw_file
from ._repo_ci_types import DepBlockerDict, PromotionHeldDict, RepoOverviewDict, RootBlockerDict

logger = logging.getLogger(__name__)

_PM_REPO = "unified-trading-pm"
_MANIFEST_PATH = "workspace-manifest.json"
_MANIFEST_TTL_SECONDS = 120.0

# Cache stores (timestamp, raw_manifest, firestore_ci_status_overlay, codebase_health_overlay,
# released_version_overlay) so the Firestore reads are amortised over the same 120 s window as the
# manifest fetch.
_manifest_cache: (
    tuple[float, dict[str, object], dict[str, str], dict[str, _CodebaseHealthDict], dict[str, str]] | None
) = None


def _semver_tuple(version: str) -> tuple[int, ...]:
    """Parse a dotted version (e.g. "1.2.86") to an int tuple for ordering. Non-numeric or
    pre-release segments degrade to 0 so a malformed value sorts low rather than raising."""
    parts: list[int] = []
    for segment in version.split("."):
        digits = ""
        for ch in segment:
            if ch.isdigit():
                digits += ch
            else:
                break
        parts.append(int(digits) if digits else 0)
    return tuple(parts)


@dataclass(frozen=True)
class RepoMeta:
    """Registry metadata for one repo (from workspace-manifest.json.repositories)."""

    name: str
    repo_type: str
    github_url: str


class ManifestView:
    """Typed read surface over the raw manifest dict."""

    def __init__(
        self,
        raw: dict[str, object],
        *,
        ci_status_override: dict[str, str] | None = None,
        codebase_health_override: dict[str, _CodebaseHealthDict] | None = None,
        versions_override: dict[str, str] | None = None,
    ) -> None:
        self._raw = raw
        # When provided, ci_status_override is the Firestore-authoritative map
        # (manifest as fallback) produced by resolve_ci_status_map — checked first in
        # ci_status_for so the dashboard reflects the same source as the promoter gate.
        self._ci_status_override = ci_status_override
        # When provided, codebase_health_override is the Firestore-authoritative map
        # (manifest as fallback) produced by resolve_codebase_health_map.
        self._codebase_health_override = codebase_health_override
        # When provided, versions_override is the Firestore-authoritative released-version map
        # (manifest versions{} as fallback) produced by resolve_release_version_map — the LIVE git-tag
        # registry, so release_version_for + the pending-bump comparison reflect Firestore truth
        # without waiting for the hourly manifest consolidation. (staging_versions / deployed_versions
        # have no Firestore writer — see _ci_status_firestore_store scope note — so they stay manifest.)
        self._versions_override = versions_override

    @property
    def repos(self) -> list[RepoMeta]:
        """The 25-repo registry, sorted by name."""
        repositories = self._raw.get("repositories")
        if not isinstance(repositories, dict):
            return []
        out: list[RepoMeta] = []
        for name, meta_obj in cast(dict[str, object], repositories).items():
            meta_typed: dict[str, object] = cast(dict[str, object], meta_obj) if isinstance(meta_obj, dict) else {}
            out.append(
                RepoMeta(
                    name=name,
                    repo_type=str(meta_typed.get("type") or "unknown"),
                    github_url=str(meta_typed.get("github_url") or ""),
                )
            )
        return sorted(out, key=lambda r: r.name)

    def ci_status_for(self, repo: str) -> str:
        """The 9-state ci_status lifecycle value for one repo.

        Phase-2 Firestore swap: reads the Firestore-authoritative map (manifest as
        fallback) when load_manifest_view has resolved it; falls back to the manifest
        dict directly (test seam / no-Firestore path via manifest_view_from_raw).
        """
        if self._ci_status_override is not None:
            return self._ci_status_override.get(repo, "UNKNOWN")
        repositories = self._raw.get("repositories")
        if not isinstance(repositories, dict):
            return "UNKNOWN"
        meta_obj = cast(dict[str, object], repositories).get(repo)
        if not isinstance(meta_obj, dict):
            return "UNKNOWN"
        return str(cast(dict[str, object], meta_obj).get("ci_status") or "NOT_CONFIGURED")

    def codebase_health_for(self, repo: str) -> _CodebaseHealthDict | None:
        """Per-repo QG health snapshot (coverage_pct / qg_red_reason / large_file_count / warn_file_count).

        Firestore-authoritative when load_manifest_view has resolved the overlay;
        falls back to the manifest ``repositories[repo].codebase_health`` sub-dict.
        Returns None when no data is available for the repo in either source.
        """
        if self._codebase_health_override is not None:
            return self._codebase_health_override.get(repo)
        repositories = self._raw.get("repositories")
        if not isinstance(repositories, dict):
            return None
        meta_obj = cast(dict[str, object], repositories).get(repo)
        if not isinstance(meta_obj, dict):
            return None
        health = cast(dict[str, object], meta_obj).get("codebase_health")
        if not isinstance(health, dict):
            return None
        return cast(_CodebaseHealthDict, health)

    @property
    def breaking_pending(self) -> list[str]:
        """Repos queued for SIT (staging_status.breaking_pending)."""
        staging_status = self._staging_status
        pending = staging_status.get("breaking_pending")
        if not isinstance(pending, list):
            return []
        return [str(item) for item in cast(list[object], pending)]

    @property
    def staging_locked(self) -> bool:
        return bool(self._staging_status.get("locked"))

    @property
    def staging_locked_reason(self) -> str | None:
        reason = self._staging_status.get("locked_reason")
        return str(reason) if reason else None

    def resolve_repo(self, name: str) -> str | None:
        """Resolve a DEPLOYMENT-SERVICE name to its hosting REPO (or the repo itself).

        The canonical mapping is `repositories[repo].consolidates[]` (e.g. features-service
        consolidates features-delta-one-service et al; ml-service consolidates ml-training/
        ml-inference) — so the per-service CI tab works for consolidated service names too.
        """
        repositories = self._raw.get("repositories")
        if not isinstance(repositories, dict):
            return None
        repos = cast(dict[str, object], repositories)
        if name in repos:
            return name
        for repo_name, meta_obj in repos.items():
            if not isinstance(meta_obj, dict):
                continue
            consolidates = cast(dict[str, object], meta_obj).get("consolidates")
            if isinstance(consolidates, list) and name in cast(list[object], consolidates):
                return repo_name
        return None

    def dependencies_for(self, repo: str) -> list[str]:
        """The dep repo NAMES declared by one repo (repositories[repo].dependencies[].name).

        Mirrors the STAGE 1.8 dep-order gate's dep extraction: each dependency entry is a dict
        `{"name", "version", "required"}` (a bare string is tolerated too). Returns [] when the
        repo has no manifest entry or no declared deps (fail-open: a repo with no deps is never
        held). Self-references and blank names are dropped."""
        repositories = self._raw.get("repositories")
        if not isinstance(repositories, dict):
            return []
        meta_obj = cast(dict[str, object], repositories).get(repo)
        if not isinstance(meta_obj, dict):
            return []
        deps_obj = cast(dict[str, object], meta_obj).get("dependencies")
        if not isinstance(deps_obj, list):
            return []
        out: list[str] = []
        for dep in cast(list[object], deps_obj):
            if isinstance(dep, dict):
                name = cast(dict[str, object], dep).get("name")
                dep_name = str(name) if name else ""
            elif isinstance(dep, str):
                dep_name = dep
            else:
                dep_name = ""
            if dep_name and dep_name != repo:
                out.append(dep_name)
        return out

    def tier_for(self, repo: str) -> str:
        """workspace-manifest.json.repositories[repo].tier, stringified.

        The manifest stores tier as a value like `0`/`1`/`3`/`"service"`; the response shape
        carries it as a string (stringify whatever is present). Empty string when absent."""
        repositories = self._raw.get("repositories")
        if not isinstance(repositories, dict):
            return ""
        meta_obj = cast(dict[str, object], repositories).get(repo)
        if not isinstance(meta_obj, dict):
            return ""
        tier = cast(dict[str, object], meta_obj).get("tier")
        return str(tier) if tier is not None else ""

    def promotion_model_for(self, repo: str) -> str | None:
        """workspace-manifest.json.repositories[repo].promotion_model (e.g. "ldr_main").

        Returns None when the field is absent (the default promotion path, staging→main).
        "ldr_main" indicates the repo promotes live-defi-rollout→main directly, so staging
        being ahead of main is the EXPECTED steady state — not a stall."""
        repositories = self._raw.get("repositories")
        if not isinstance(repositories, dict):
            return None
        meta_obj = cast(dict[str, object], repositories).get(repo)
        if not isinstance(meta_obj, dict):
            return None
        value = cast(dict[str, object], meta_obj).get("promotion_model")
        return str(value) if value else None

    def staging_dormant_mode(self) -> bool:
        """workspace-manifest.json top-level `staging_dormant_mode` (WS-L operator toggle).

        When true the fleet promotes LDR→main directly and STAGING is dormant, so the dashboard +
        alerts suppress every staging-direction signal (LDR→staging drain behind / staging→main not
        promoting / the stg→main hop / drain-stalled) for ALL repos — only the LDR→main signal stays
        actionable. Absent/false = the normal staging-in-the-flow display. Reversible (flip the flag)."""
        return bool(self._raw.get("staging_dormant_mode"))

    def version_source_for(self, repo: str) -> str:
        """workspace-manifest.json.repositories[repo].version_source (e.g. "git-tag").

        Mirrors `promotion_model_for`. Returns the repo's declared version source: "git-tag" (the
        Phase-2/D13 dynamic model — version resolved from the git tag + Firestore registry, no
        committed version line and no staging→main bump path) or a static line source
        ("pyproject.toml"/"package.json"/"version-file"). Defaults to "pyproject.toml" (the
        fleet-typical static case) when absent — matching the PM `assert_version_coherence._version_source`
        default so the two readers agree on what "static" means."""
        repositories = self._raw.get("repositories")
        if not isinstance(repositories, dict):
            return "pyproject.toml"
        meta_obj = cast(dict[str, object], repositories).get(repo)
        if not isinstance(meta_obj, dict):
            return "pyproject.toml"
        value = cast(dict[str, object], meta_obj).get("version_source")
        return str(value) if value else "pyproject.toml"

    def deployed_version_for(self, repo: str) -> str | None:
        """workspace-manifest.json.deployed_versions["prod"][repo]["version"] (image-level deploy signal).

        Stays manifest-sourced: deployed_versions is per-env image-deploy state committed to the
        manifest by the cloudbuild post-build step — it has no Firestore writer (the version registry
        tracks the RELEASED version, see release_version_for / the _ci_status_firestore_store scope note).

        SHAPE FIX (post-cutover sweep F5, 2026-07-29): the writer (`cloud-build-router.yml`'s
        populate-deployed-versions step) nests by env FIRST, then repo, with a dict value —
        `deployed_versions[env][repo] = {version, image_tag, deployed_at, build_id}` — never a
        flat `deployed_versions[repo]` string. A flat top-level lookup by repo name always misses
        (repo names never collide with env names like "prod"), so this column read permanently
        blank regardless of real deploy state. "prod" mirrors the dashboard's own jq query
        (`cloud-build-router.yml`'s deployed_versions read-back, `.deployed_versions.prod[$dep]`) —
        the only env this repo-CI dashboard surfaces.
        """
        deployed = self._raw.get("deployed_versions")
        if not isinstance(deployed, dict):
            return None
        prod = cast(dict[str, object], deployed).get("prod")
        if not isinstance(prod, dict):
            return None
        entry = cast(dict[str, object], prod).get(repo)
        if not isinstance(entry, dict):
            return None
        value = cast(dict[str, object], entry).get("version")
        return str(value) if value else None

    def release_version_for(self, repo: str) -> str | None:
        """The repo's RELEASED version (git tag = SSOT), Firestore-authoritative with manifest fallback.

        Phase-2 (D13): reads the resolve_release_version_map overlay (manifest versions{} as fallback)
        when load_manifest_view has resolved it — the LIVE git-tag registry, not the up-to-an-hour
        consolidated manifest cache; falls back to the manifest versions{} dict directly (test seam /
        no-Firestore path via manifest_view_from_raw). None when the repo has no released version."""
        if self._versions_override is not None:
            return self._versions_override.get(repo)
        versions = self._raw.get("versions")
        if not isinstance(versions, dict):
            return None
        value = cast(dict[str, object], versions).get(repo)
        return str(value) if value else None

    def _versions_main_map(self) -> dict[str, str]:
        """Repo → released (main) version: the Firestore overlay when resolved, else manifest versions{}.

        The single source the pending-bump comparison reads, so it reflects the same LIVE released
        version as release_version_for / the promoter gate."""
        if self._versions_override is not None:
            return self._versions_override
        versions = self._raw.get("versions")
        out: dict[str, str] = {}
        if isinstance(versions, dict):
            for k, v in cast(dict[str, object], versions).items():
                if isinstance(v, str) and v:
                    out[str(k)] = v
        return out

    def promotion_failures(self) -> dict[str, int]:
        """workspace-manifest.json.promotion_failures (`{repo: consecutive-fail count}`)."""
        raw = self._raw.get("promotion_failures")
        if not isinstance(raw, dict):
            return {}
        out: dict[str, int] = {}
        for repo, count in cast(dict[str, object], raw).items():
            if isinstance(count, bool):
                continue
            if isinstance(count, int):
                out[repo] = count
            elif isinstance(count, str) and count.isdigit():
                out[repo] = int(count)
        return out

    def promotion_quarantine(self) -> dict[str, dict[str, object]]:
        """workspace-manifest.json.promotion_quarantine (`{repo: {since, attempts, escalated}}`)."""
        raw = self._raw.get("promotion_quarantine")
        if not isinstance(raw, dict):
            return {}
        out: dict[str, dict[str, object]] = {}
        for repo, detail in cast(dict[str, object], raw).items():
            out[repo] = cast(dict[str, object], detail) if isinstance(detail, dict) else {}
        return out

    def pending_version_bumps(self) -> list[str]:
        """Repos whose staging version is AHEAD of main (a bump promoted to staging but not
        yet to main — the "pending staging bump" the semver-agent circuit-breaker counts; G2).

        Compares `staging_versions[repo]` vs the main version by semver tuple; only a staging
        version strictly greater than main counts (a staging version BEHIND main — e.g. PM's
        vestigial no-staging entry — is not a pending bump). `_note` keys are skipped.

        The main version is read from `_versions_main_map` (the Firestore release overlay when
        resolved, manifest `versions{}` otherwise) so the comparison uses the same LIVE released
        version as the promoter gate; `staging_versions` stays manifest-sourced (no Firestore writer).

        **git-tag repos are skipped** (F5): a `version_source=git-tag` repo (Phase-2/D13) has NO
        staging→main bump path — its version SSOT is the git tag / Firestore registry and its
        `staging_versions` entry is vestigial (no live writer keeps it current; staging_versions is
        retiring), so a stale staging value above main is never a real pending bump and must not arm
        the semver circuit-breaker. Mirrors the PM `assert_version_coherence` git-tag branch ("staging
        is not the source"). The whole-map-absent / per-repo-absent fail-open paths stay intact, so a
        fully-retired staging_versions block yields `[]`, not a crash.
        """
        staging = self._raw.get("staging_versions")
        if not isinstance(staging, dict):
            return []
        versions_d = self._versions_main_map()
        staging_d = cast(dict[str, object], staging)
        out: list[str] = []
        for repo, staging_v in staging_d.items():
            if repo.startswith("_"):
                continue
            if self.version_source_for(repo) == "git-tag":
                continue  # no staging→main bump path; staging entry is vestigial (F5)
            main_v = versions_d.get(repo)
            if not isinstance(staging_v, str) or not isinstance(main_v, str):
                continue
            if _semver_tuple(staging_v) > _semver_tuple(main_v):
                out.append(repo)
        return sorted(out)

    @property
    def _staging_status(self) -> dict[str, object]:
        staging_status = self._raw.get("staging_status")
        if not isinstance(staging_status, dict):
            return {}
        return cast(dict[str, object], staging_status)


async def load_manifest_view(session: aiohttp.ClientSession, token: str) -> ManifestView:
    """Fetch (or serve cached) workspace-manifest.json from PM `main` and wrap it.

    On each cache miss also resolves the Firestore ci_status, codebase_health and released-version
    overlays (Firestore-authoritative per-repo, manifest as fallback cache) so the dashboard reflects
    the same source as the promoter gate. All overlays are cached for the same 120 s window.
    """
    global _manifest_cache
    now = time.monotonic()
    if _manifest_cache is not None and now - _manifest_cache[0] < _MANIFEST_TTL_SECONDS:
        return ManifestView(
            _manifest_cache[1],
            ci_status_override=_manifest_cache[2],
            codebase_health_override=_manifest_cache[3],
            versions_override=_manifest_cache[4],
        )
    raw_text = await gh_raw_file(session, token, GITHUB_ORG, _PM_REPO, _MANIFEST_PATH, ref="main")
    parsed = cast(object, json.loads(raw_text))
    if not isinstance(parsed, dict):
        raise ValueError("workspace-manifest.json did not parse to an object")
    manifest = cast(dict[str, object], parsed)
    ci_override = resolve_ci_status_map(manifest)
    health_override = resolve_codebase_health_map(manifest)
    versions_override = resolve_release_version_map(manifest)
    _manifest_cache = (now, manifest, ci_override, health_override, versions_override)
    return ManifestView(
        manifest,
        ci_status_override=ci_override,
        codebase_health_override=health_override,
        versions_override=versions_override,
    )


def manifest_view_from_raw(raw: dict[str, object]) -> ManifestView:
    """Test seam: build a ManifestView from an in-memory manifest dict."""
    return ManifestView(raw)


# ---------------------------------------------------------------------------
# Dep-order HOLD helpers (STAGE 1.8 mirror) — live here because they take
# ManifestView as their primary input.
# ---------------------------------------------------------------------------

# A dep is "on main" iff its ci_status is one of these — the exact ON_MAIN_STATUSES set the
# staging-to-main.yml dep-order gate uses. Fail OPEN on missing data (no manifest entry / no
# deps / blank ci_status → never held). Tier sorts ascending (lowest/most-foundational first).
_ON_MAIN_STATUSES = frozenset({"MAIN_GREEN", "SIT_VALIDATED"})
# Absence sentinels: ci_status values that carry NO real on-main signal. STAGE 1.8 does
# `if not dep_status: continue` (blank → safe-default PASS); ManifestView.ci_status_for renders
# a missing/blank ci_status as "NOT_CONFIGURED"/"UNKNOWN" — treated as on-main, never held on.
_CI_STATUS_ABSENCE_SENTINELS = frozenset({"", "NOT_CONFIGURED", "UNKNOWN"})


def _tier_rank(tier: str) -> tuple[int, str]:
    """Order key for a (stringified) tier: numeric tiers ascending and BEFORE non-numeric ones.

    `0`/`1`/`3` sort by their int value (lowest = most foundational = first); a non-numeric tier
    (`"service"`, `""`) sorts after all numeric tiers, then lexically — deterministic, never raises.
    """
    if tier.isdigit():
        return (int(tier), "")
    return (1 << 30, tier)


def _staging_main_files_behind(rows: list[RepoOverviewDict], repo: str) -> int:
    """A repo's staging→main delta files_changed (base=main, head=staging), 0 when absent.

    The "content stuck behind main" signal for a root blocker: how much real file content sits on
    staging not yet on main (squash-skew-aware — files_changed, not ahead_by)."""
    row = next((r for r in rows if r["repo"] == repo), None)
    if row is None:
        return 0
    delta = next((d for d in row["deltas"] if d["base"] == "main" and d["head"] == "staging"), None)
    return delta["files_changed"] if delta is not None else 0


def _compute_dep_order(
    rows: list[RepoOverviewDict],
    view: ManifestView,
) -> tuple[dict[str, list[DepBlockerDict]], dict[str, list[str]], PromotionHeldDict]:
    """Cross-repo dep-order HOLD computation (mirrors staging-to-main.yml STAGE 1.8).

    Pure function — depends only on the already-built rows (each carrying ci_status + the
    staging→main delta) and the manifest view (dep names + tiers). Returns:
      - blocked_by: repo -> the DepBlockerDicts (deps NOT on main) holding its promotion. A repo
        on main (no pending promotion) maps to []; blocked_by non-empty ⟺ that repo is HELD.
      - blocking: repo -> sorted repo names held because THIS repo isn't on main (the inverse map).
      - promotion_held: held_repos (sorted) + root_blockers (the bottom-of-stack not-on-main repos
        causing the holds, lowest tier first then blocking_count desc).

    Fail-OPEN on missing data exactly like STAGE 1.8: a repo with no manifest deps is never held;
    a dep whose ci_status is unknown/blank is treated as on-main (does not block). Never raises.
    """
    # ci_status of every repo: prefer the row's value (the live/Firestore-overlaid status the
    # overview already resolved), fall back to the manifest for any dep that isn't a rendered row.
    status_by_repo: dict[str, str] = {row["repo"]: row["ci_status"] for row in rows}

    def status_of(repo: str) -> str:
        return status_by_repo.get(repo) or view.ci_status_for(repo)

    def on_main(repo: str) -> bool:
        # On main (= NOT a blocker) when the status is a real on-main signal OR an absence sentinel
        # (blank / NOT_CONFIGURED / UNKNOWN) — the latter is STAGE 1.8's fail-open "ci_status not set
        # → safe-default pass". Only a real non-on-main status (STAGING_GREEN / FAILING / …) blocks.
        status = status_of(repo)
        return status in _ON_MAIN_STATUSES or status in _CI_STATUS_ABSENCE_SENTINELS

    def deps_of(repo: str) -> list[str]:
        return view.dependencies_for(repo)

    def deps_not_on_main(repo: str) -> list[str]:
        """The dep names of `repo` that are themselves NOT on main (the blockers of `repo`)."""
        return [dep for dep in deps_of(repo) if not on_main(dep)]

    # blocked_by(R): a repo with a PENDING promotion (its own ci_status not on main) is held by each
    # dep that is itself not on main. A repo already on main has no pending promotion → blocked_by=[].
    blocked_by: dict[str, list[DepBlockerDict]] = {}
    for row in rows:
        repo = row["repo"]
        if on_main(repo):
            blocked_by[repo] = []
            continue
        blockers = [
            DepBlockerDict(name=dep, tier=view.tier_for(dep), ci_status=status_of(dep))
            for dep in deps_not_on_main(repo)
        ]
        blocked_by[repo] = blockers

    # blocking(X) = inverse of blocked_by: every R such that X appears in blocked_by(R).
    blocking: dict[str, list[str]] = {row["repo"]: [] for row in rows}
    for held_repo, blockers in blocked_by.items():
        for blocker in blockers:
            blocking.setdefault(blocker["name"], []).append(held_repo)
    blocking = {repo: sorted(held) for repo, held in blocking.items()}

    held_repos = sorted(repo for repo, blockers in blocked_by.items() if blockers)

    # The candidate root blockers = every dep name that appears in ANY blocked_by AND is itself not
    # on main. The TRUE roots are the bottom of the stack: those whose OWN blocked_by is empty (they
    # are not themselves held by a dep). Degenerate fallback (a dep cycle / all blockers themselves
    # held): keep the lowest-tier blockers so the card never renders empty while a hold exists.
    candidate_blockers = {b["name"] for blockers in blocked_by.values() for b in blockers if not on_main(b["name"])}

    def own_blocked_by_empty(repo: str) -> bool:
        # A blocker that is itself a rendered row uses its computed blocked_by; one that isn't a row
        # (an external/untracked dep) is re-derived from the manifest (same fail-open rules).
        if repo in blocked_by:
            return not blocked_by[repo]
        return not deps_not_on_main(repo)

    root_names = {b for b in candidate_blockers if own_blocked_by_empty(b)}
    if not root_names and candidate_blockers:
        lowest = min(_tier_rank(view.tier_for(b)) for b in candidate_blockers)
        root_names = {b for b in candidate_blockers if _tier_rank(view.tier_for(b)) == lowest}

    root_blockers = [
        RootBlockerDict(
            repo=name,
            tier=view.tier_for(name),
            ci_status=status_of(name),
            blocking_count=len(blocking.get(name, [])),
            main_files_behind=_staging_main_files_behind(rows, name),
        )
        for name in root_names
    ]
    # Lowest/most-foundational tier first, then most-blocking first (blocking_count desc).
    root_blockers.sort(key=lambda rb: (_tier_rank(rb["tier"]), -rb["blocking_count"], rb["repo"]))

    return blocked_by, blocking, PromotionHeldDict(held_repos=held_repos, root_blockers=root_blockers)

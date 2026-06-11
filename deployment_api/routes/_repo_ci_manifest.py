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

from ._repo_ci_github import gh_raw_file

logger = logging.getLogger(__name__)

_PM_REPO = "unified-trading-pm"
_MANIFEST_PATH = "workspace-manifest.json"
_MANIFEST_TTL_SECONDS = 120.0

_manifest_cache: tuple[float, dict[str, object]] | None = None


@dataclass(frozen=True)
class RepoMeta:
    """Registry metadata for one repo (from workspace-manifest.json.repositories)."""

    name: str
    repo_type: str
    github_url: str


class ManifestView:
    """Typed read surface over the raw manifest dict."""

    def __init__(self, raw: dict[str, object]) -> None:
        self._raw = raw

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

        THE Firestore swap point: when the ci_status side-store cuts over, this method
        reads the store instead of the manifest — no other consumer changes.
        """
        repositories = self._raw.get("repositories")
        if not isinstance(repositories, dict):
            return "UNKNOWN"
        meta_obj = cast(dict[str, object], repositories).get(repo)
        if not isinstance(meta_obj, dict):
            return "UNKNOWN"
        return str(cast(dict[str, object], meta_obj).get("ci_status") or "NOT_CONFIGURED")

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

    def deployed_version_for(self, repo: str) -> str | None:
        """workspace-manifest.json.deployed_versions[repo] (image-level deploy signal)."""
        deployed = self._raw.get("deployed_versions")
        if not isinstance(deployed, dict):
            return None
        value = cast(dict[str, object], deployed).get(repo)
        return str(value) if value else None

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

    @property
    def _staging_status(self) -> dict[str, object]:
        staging_status = self._raw.get("staging_status")
        if not isinstance(staging_status, dict):
            return {}
        return cast(dict[str, object], staging_status)


async def load_manifest_view(session: aiohttp.ClientSession, token: str) -> ManifestView:
    """Fetch (or serve cached) workspace-manifest.json from PM `main` and wrap it."""
    global _manifest_cache
    now = time.monotonic()
    if _manifest_cache is not None and now - _manifest_cache[0] < _MANIFEST_TTL_SECONDS:
        return ManifestView(_manifest_cache[1])
    raw_text = await gh_raw_file(session, token, GITHUB_ORG, _PM_REPO, _MANIFEST_PATH, ref="main")
    parsed = cast(object, json.loads(raw_text))
    if not isinstance(parsed, dict):
        raise ValueError("workspace-manifest.json did not parse to an object")
    manifest = cast(dict[str, object], parsed)
    _manifest_cache = (now, manifest)
    return ManifestView(manifest)


def manifest_view_from_raw(raw: dict[str, object]) -> ManifestView:
    """Test seam: build a ManifestView from an in-memory manifest dict."""
    return ManifestView(raw)

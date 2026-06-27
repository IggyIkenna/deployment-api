"""CI-status Firestore side-store reader — deployment-api Phase-2 cut-over.

Ports the read half of unified-trading-pm/scripts/cicd/ci_status_store.py into
deployment-api so the dashboard's ci_status reflects the AUTHORITATIVE Firestore
side-store rather than the committed workspace-manifest.json cache (120 s TTL).

Design (matches the PM store's resolve_ci_status_map contract):
  * Start from the manifest ci_status map (fallback / seed).
  * OVERLAY Firestore ``ci_status/{repo}`` per-repo where a document exists.
  * On ANY Firestore unavailability: warn loudly, return the manifest-only map.
    Never a silent swallow — the manifest is the designed offline fallback.

The cloud SDK is imported lazily inside ``_default_firestore_module`` so importing
this module in unit tests with an injected fake never requires the SDK to be present.

SSOT: unified-trading-pm plans/archive/2026_06/ci_status_firestore_side_store_2026_06_10.md
"""

from __future__ import annotations

import logging
from collections.abc import Callable
from typing import Protocol, cast

COLLECTION: str = "ci_status"

# Version registry (Phase-2 / D13) — the SAME Firestore document the PM-side
# unified-trading-pm/scripts/cicd/version_registry_store.py writes (CAS + monotonic). The released
# version SSOT is the git tag; this registry is its Firestore mirror, and the manifest ``versions{}``
# is the hourly-consolidated fallback cache. deployment-api reads it here so the dashboard reflects the
# LIVE released version (no up-to-an-hour consolidation lag), manifest as the offline fallback.
RELEASE_COLLECTION: str = "repo_state"
RELEASE_TAG_FIELD: str = "release_tag"


# ── Structural protocols (slice of google.cloud.firestore we need) ─────────────


class _DocProto(Protocol):
    id: str

    def to_dict(self) -> dict[str, object] | None: ...


class _CollectionProto(Protocol):
    def stream(self) -> list[_DocProto]: ...


class _ClientProto(Protocol):
    def collection(self, path: str) -> _CollectionProto: ...


class _FirestoreModuleProto(Protocol):
    def Client(self, project: str | None = ...) -> _ClientProto:  # noqa: N802 — mirrors SDK API
        ...


FirestoreModuleFactory = Callable[[], _FirestoreModuleProto]
"""Returns the firestore MODULE (not just a client); tests inject a fake module."""


def _default_firestore_module() -> _FirestoreModuleProto:
    """Lazily import ``google.cloud.firestore`` — lazy so unit tests never need the SDK."""
    from google.cloud import (  # noqa: TID251, imports-inside-functions — lazy Firestore SDK import; cloud-sdk-direct
        firestore,  # pyright: ignore[reportMissingImports, reportAttributeAccessIssue, reportUnknownVariableType]
    )

    return cast(_FirestoreModuleProto, cast(object, firestore))


# ── Internal helpers ────────────────────────────────────────────────────────────


def _get_all(
    *,
    project_id: str | None = None,
    firestore_module_factory: FirestoreModuleFactory = _default_firestore_module,
) -> dict[str, dict[str, object]]:
    """Read the whole-fleet ci_status aggregate — one collection query."""
    fs = firestore_module_factory()
    client = fs.Client(project=project_id)
    out: dict[str, dict[str, object]] = {}
    for doc in client.collection(COLLECTION).stream():
        out[doc.id] = doc.to_dict() or {}
    return out


def _manifest_ci_status_map(manifest: dict[str, object]) -> dict[str, str]:
    """Map repo-name → ci_status from the manifest dict (the fallback cache).

    Tolerant of dict-keyed (``{repo: {...}}``) or list (``[{name, ci_status}, ...]``)
    ``repositories`` shapes. Repos with no/blank ci_status are omitted.
    """
    repos = manifest.get("repositories")
    items: list[tuple[str, object]] = []
    if isinstance(repos, dict):
        items = [(str(k), v) for k, v in cast("dict[str, object]", repos).items()]
    elif isinstance(repos, list):
        for r in cast("list[object]", repos):
            if isinstance(r, dict):
                r_d = cast("dict[str, object]", r)
                name = r_d.get("name")
                if isinstance(name, str) and name:
                    items.append((name, r_d))
    out: dict[str, str] = {}
    for name, value in items:
        if isinstance(value, dict):
            status = cast("dict[str, object]", value).get("ci_status")
            if isinstance(status, str) and status:
                out[name] = status
    return out


# ── Public API ──────────────────────────────────────────────────────────────────


def resolve_ci_status_map(
    manifest: dict[str, object],
    *,
    project_id: str | None = None,
    firestore_module_factory: FirestoreModuleFactory = _default_firestore_module,
) -> dict[str, str]:
    """Repo → ci_status: **Firestore-authoritative per-repo, manifest as fallback cache**.

    Starts from the manifest map and OVERLAYS Firestore per-repo where a document is
    present, so a reader behaves identically to pre-Firestore while the side-store is
    empty and shifts to Firestore-truth repo-by-repo as docs appear.

    On any Firestore unavailability (SDK absent, transient API error) degrades LOUDLY
    to the manifest cache — a warning, not a silent swallow.
    """
    base = _manifest_ci_status_map(manifest)
    try:
        fs_data = _get_all(project_id=project_id, firestore_module_factory=firestore_module_factory)
    except Exception as err:
        logging.getLogger(__name__).warning(
            "ci_status Firestore read unavailable (%s: %s) — using manifest fallback cache",
            type(err).__name__,
            err,
        )
        return base
    for repo, doc in fs_data.items():
        status = doc.get("status")
        if isinstance(status, str) and status:
            base[repo] = status
    return base


# ── codebase_health helpers ─────────────────────────────────────────────────────

_CodebaseHealthDict = dict[str, object]
"""The raw codebase_health sub-dict from a Firestore ci_status doc or manifest entry.

Keys (all optional): coverage_pct (float), qg_red_reason (str), large_file_count (int),
warn_file_count (int).  None / missing keys stay absent; consumers validate per-field.
"""


def _manifest_codebase_health_map(manifest: dict[str, object]) -> dict[str, _CodebaseHealthDict]:
    """Map repo-name → codebase_health from the manifest (fallback cache).

    Mirrors ``_manifest_ci_status_map`` — tolerates dict-keyed or list ``repositories``.
    Repos with no ``codebase_health`` key are omitted.
    """
    repos = manifest.get("repositories")
    items: list[tuple[str, object]] = []
    if isinstance(repos, dict):
        items = [(str(k), v) for k, v in cast("dict[str, object]", repos).items()]
    elif isinstance(repos, list):
        for r in cast("list[object]", repos):
            if isinstance(r, dict):
                r_d = cast("dict[str, object]", r)
                name = r_d.get("name")
                if isinstance(name, str) and name:
                    items.append((name, r_d))
    out: dict[str, _CodebaseHealthDict] = {}
    for name, value in items:
        if isinstance(value, dict):
            health = cast("dict[str, object]", value).get("codebase_health")
            if isinstance(health, dict):
                out[name] = cast("_CodebaseHealthDict", health)
    return out


def resolve_codebase_health_map(
    manifest: dict[str, object],
    *,
    project_id: str | None = None,
    firestore_module_factory: FirestoreModuleFactory = _default_firestore_module,
) -> dict[str, _CodebaseHealthDict]:
    """Repo → codebase_health dict: **Firestore-authoritative per-repo, manifest as fallback**.

    Mirrors ``resolve_ci_status_map`` exactly:
      * Start from the manifest ``codebase_health`` sub-field per repo.
      * OVERLAY the Firestore ``ci_status/{repo}`` doc's ``codebase_health`` sub-field.
      * On ANY Firestore unavailability: warn loudly + return the manifest-only map.

    Reuses the same ``_get_all`` call (same Firestore collection, same SDK machinery) — no
    separate collection query.  Returns an empty dict when a repo has no health data in
    either source.
    """
    base = _manifest_codebase_health_map(manifest)
    try:
        fs_data = _get_all(project_id=project_id, firestore_module_factory=firestore_module_factory)
    except Exception as err:
        logging.getLogger(__name__).warning(
            "codebase_health Firestore read unavailable (%s: %s) — using manifest fallback cache",
            type(err).__name__,
            err,
        )
        return base
    for repo, doc in fs_data.items():
        health = doc.get("codebase_health")
        if isinstance(health, dict):
            base[repo] = cast("_CodebaseHealthDict", health)
    return base


# ── Version registry (Phase-2) — released version, Firestore-authoritative, manifest fallback ─────
#
# Scope note (honest, verified 2026-06-27): of the three manifest version surfaces only ``versions{}``
# (the released/main version) has a Firestore writer — ``repo_state/{repo}.release_tag`` minted by the
# PM version_registry_store on every ``v*`` tag push. ``staging_versions{}`` (being retired with the
# staging branch) and ``deployed_versions{}`` (per-env image-deploy state, committed to the manifest by
# the cloudbuild post-build step) have NO Firestore source, so they stay manifest-sourced — overlaying
# them from a non-existent registry would be a lie. When a Firestore deployed-version source ever lands,
# this is the one-function seam to add ``resolve_deployed_version_map`` alongside.


def _get_all_release_tags(
    *,
    project_id: str | None = None,
    firestore_module_factory: FirestoreModuleFactory = _default_firestore_module,
) -> dict[str, dict[str, object]]:
    """Read the whole-fleet ``release_tag`` aggregate from ``repo_state`` — one collection query.

    Mirrors the PM-side ``version_registry_store.get_all`` (the writer's own reader): returns
    ``{repo: release_tag_map}`` for every ``repo_state`` doc that carries a ``release_tag`` field
    (docs with only ci_failure/promotion_lag siblings are omitted).
    """
    fs = firestore_module_factory()
    client = fs.Client(project=project_id)
    out: dict[str, dict[str, object]] = {}
    for doc in client.collection(RELEASE_COLLECTION).stream():
        full = doc.to_dict() or {}
        rt = full.get(RELEASE_TAG_FIELD)
        if isinstance(rt, dict):
            out[doc.id] = cast("dict[str, object]", rt)
    return out


def _manifest_versions_map(manifest: dict[str, object]) -> dict[str, str]:
    """Map repo-name → released version from the manifest top-level ``versions{}`` (fallback cache).

    The manifest is the offline-fallback cache (the hourly consolidator projects Firestore into it).
    Repos with no/blank version are omitted.
    """
    versions = manifest.get("versions")
    out: dict[str, str] = {}
    if isinstance(versions, dict):
        for k, v in cast("dict[str, object]", versions).items():
            if isinstance(v, str) and v:
                out[str(k)] = v
    return out


def resolve_release_version_map(
    manifest: dict[str, object],
    *,
    project_id: str | None = None,
    firestore_module_factory: FirestoreModuleFactory = _default_firestore_module,
) -> dict[str, str]:
    """Repo → released version: **Firestore-authoritative per-repo, manifest as fallback cache**.

    The same contract as ``resolve_ci_status_map`` / the PM-side
    ``version_registry_store.resolve_version_map`` (the SAME registry, read from deployment-api):
    start from the manifest ``versions{}`` map and OVERLAY the Firestore
    ``repo_state/{repo}.release_tag.version`` per-repo where present. While the registry is still
    ramping (Firestore empty → pure manifest) a reader behaves identically to today, and shifts to
    Firestore-truth repo-by-repo as docs appear. On any Firestore unavailability degrades LOUDLY to
    the manifest cache (a warning, never a silent swallow).
    """
    base = _manifest_versions_map(manifest)
    try:
        fs_data = _get_all_release_tags(project_id=project_id, firestore_module_factory=firestore_module_factory)
    except Exception as err:
        logging.getLogger(__name__).warning(
            "version_registry Firestore read unavailable (%s: %s) — using manifest fallback cache",
            type(err).__name__,
            err,
        )
        return base
    for repo, rt in fs_data.items():
        version = rt.get("version")
        if isinstance(version, str) and version:
            base[repo] = version
    return base

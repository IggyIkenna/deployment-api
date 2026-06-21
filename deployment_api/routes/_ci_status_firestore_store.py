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

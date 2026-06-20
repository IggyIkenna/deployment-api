"""CI-status Firestore side-store — read side only (Phase 2 reader, deployment-api).

Ported read surface from ``unified-trading-pm/scripts/cicd/ci_status_store.py`` (the
design SSOT; write side lives there). This module is the Phase-2 reader for
deployment-api: Firestore-authoritative per-repo, manifest as the designed fallback.

Design: ``ci_status/{repo}`` documents in Firestore carry ``status``, ``rank``,
``branch``, ``sha``, ``updated_at``. The dual-write ramp means Firestore may be empty
or partial — absent repos fall back to the manifest, so the reader is safe during any
stage of the cutover (never a flag-day, never a repo blanked by a partial Firestore).

The cloud SDK is imported lazily (mirrors UTL ``firestore_lifecycle``) so importing this
module in a unit test with an injected fake never requires the SDK installed.
"""

from __future__ import annotations

import logging
from collections.abc import Callable
from typing import Protocol, cast

COLLECTION: str = "ci_status"
"""Canonical Firestore collection name — import this constant, never hardcode the string."""


# ── Minimal structural protocols for the google.cloud.firestore slice we use ────────────


class _SnapProto(Protocol):
    id: str

    def to_dict(self) -> dict[str, object] | None: ...


class _CollectionProto(Protocol):
    def stream(self) -> list[_SnapProto]: ...


class _ClientProto(Protocol):
    def collection(self, collection_path: str) -> _CollectionProto: ...


class _FirestoreModuleProto(Protocol):
    def Client(self, project: str | None = ...) -> _ClientProto:  # noqa: N802 — mirrors SDK
        ...


FirestoreModuleFactory = Callable[[], _FirestoreModuleProto]
"""Returns the firestore MODULE. Production uses :func:`_default_firestore_module`;
tests inject a fake module."""


def _default_firestore_module() -> _FirestoreModuleProto:
    """Lazily import ``google.cloud.firestore``.

    Lazy so importing this module for ``resolve_ci_status_map`` or a fake-injected test
    never needs the SDK (same rationale as UTL firestore_lifecycle).
    """
    # Deliberate lazy in-function import — sanctioned markers on the `from` line:
    from google.cloud import (  # noqa: imports-inside-functions # noqa: cloud-sdk-direct
        firestore,  # pyright: ignore[reportMissingImports, reportAttributeAccessIssue, reportUnknownVariableType]
    )

    return cast(_FirestoreModuleProto, cast(object, firestore))


def get_all(
    *,
    project_id: str | None = None,
    firestore_module_factory: FirestoreModuleFactory = _default_firestore_module,
) -> dict[str, dict[str, object]]:
    """Read the whole-fleet ci_status aggregate — a single collection query (Phase 2 readers)."""
    fs = firestore_module_factory()
    client = fs.Client(project=project_id)
    out: dict[str, dict[str, object]] = {}
    for doc in client.collection(COLLECTION).stream():
        out[doc.id] = doc.to_dict() or {}
    return out


def manifest_ci_status_map(manifest: dict[str, object]) -> dict[str, str]:
    """Map repo-name → ci_status from ``workspace-manifest.json`` (the fallback cache).

    Tolerant of dict-keyed (``{repo: {...}}``) or list (``[{name, ci_status}, ...]``)
    ``repositories``. Repos with no/blank ci_status are omitted.
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


def resolve_ci_status_map(
    manifest: dict[str, object],
    *,
    project_id: str | None = None,
    firestore_module_factory: FirestoreModuleFactory = _default_firestore_module,
) -> dict[str, str]:
    """Repo → ci_status: **Firestore-authoritative per-repo, manifest as fallback cache**.

    The migration-safe read primitive (Phase 2). Starts from the manifest map and overlays
    Firestore per-repo where present — absent repos stay on the manifest. This means the
    reader behaves identically to today while the Firestore collection is empty, and shifts
    to Firestore-truth repo-by-repo as docs appear (never a flag-day).

    On any Firestore unavailability (SDK absent, transient API error) degrades **loudly**
    to the manifest cache (a WARNING, not a silent swallow) — the manifest is the designed
    offline fallback.
    """
    base = manifest_ci_status_map(manifest)
    try:
        fs = get_all(project_id=project_id, firestore_module_factory=firestore_module_factory)
    except Exception as err:
        logging.getLogger(__name__).warning(
            "ci_status Firestore read unavailable (%s: %s) — using manifest fallback cache",
            type(err).__name__,
            err,
        )
        return base
    for repo, doc in fs.items():
        status = doc.get("status")
        if isinstance(status, str) and status:
            base[repo] = status
    return base

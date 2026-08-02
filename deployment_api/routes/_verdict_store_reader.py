"""Generic verdict-store reader — deployment-api's read half of the Firestore verdict-store.

``unified-trading-pm``'s ``scripts/cicd/verdict_store.py`` is the writer/PM-side twin (one document
per ``(collection, key)``, latest-wins CAS via a ``checked_at`` ordering guard); this module ports
just the READ half so deployment-api never needs a PM import — mirrors ``_ci_status_firestore_store.py``'s
structure exactly (same lazy-SDK-import / Protocol / degrade-loudly shape), generalized to an
arbitrary collection name instead of the hardcoded ``ci_status`` one.

Unlike ``_ci_status_firestore_store.py``, there is **no manifest fallback cache** here — neither
verdict type (version-coherence, change-freeze) has a pre-existing ``workspace-manifest.json``
field, so a manifest fallback would be fabricated data, not a real cache. On any Firestore
unavailability this degrades LOUDLY (a logged warning) to an EMPTY dict — the panel then renders
"not yet checked" (honest absence), never a fabricated OK/CLEAR.

SSOT: plans/active/monitoring_control_plane_master_2026_06_10.md (version-coherence + G5
change-freeze panels), operator decision 2026-07-27 (the real Firestore verdict-store
generalisation).
"""

from __future__ import annotations

import logging
from collections.abc import Callable
from typing import Protocol, cast

VERSION_COHERENCE_COLLECTION: str = "version_coherence_verdicts"
"""Mirrors unified-trading-pm's scripts/cicd/verdict_store.VERSION_COHERENCE_COLLECTION."""

CHANGE_FREEZE_COLLECTION: str = "change_freeze_verdicts"
"""Mirrors unified-trading-pm's scripts/cicd/verdict_store.CHANGE_FREEZE_COLLECTION."""


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
    from google.cloud import (  # noqa: TID251  # noqa: cloud-sdk-direct, imports-inside-functions — lazy Firestore SDK import, mirrors _ci_status_firestore_store.py
        firestore,  # pyright: ignore[reportMissingImports, reportAttributeAccessIssue, reportUnknownVariableType]
    )

    return cast(_FirestoreModuleProto, cast(object, firestore))


def _get_all(
    collection: str,
    *,
    project_id: str | None = None,
    firestore_module_factory: FirestoreModuleFactory = _default_firestore_module,
) -> dict[str, dict[str, object]]:
    """Read every doc in a collection — one collection query, no degrade handling (raw primitive)."""
    fs = firestore_module_factory()
    client = fs.Client(project=project_id)
    out: dict[str, dict[str, object]] = {}
    for doc in client.collection(collection).stream():
        out[doc.id] = doc.to_dict() or {}
    return out


def get_all_verdicts_with_status(
    collection: str,
    *,
    project_id: str | None = None,
    firestore_module_factory: FirestoreModuleFactory = _default_firestore_module,
) -> tuple[dict[str, dict[str, object]], bool]:
    """``({key: doc}, available)`` for every document in ``collection``.

    ``available`` distinguishes "Firestore reachable but the collection is genuinely empty" (e.g.
    before the first scheduled writer run ever completes) from "Firestore unreachable" — a plain
    ``dict == {}`` check on the read alone cannot tell those apart, and conflating them would mislabel
    a real outage as "nothing checked yet" (or vice versa). Degrades LOUDLY on any Firestore
    unavailability (a logged warning, never a silent swallow) to ``({}, False)``.
    """
    try:
        return _get_all(collection, project_id=project_id, firestore_module_factory=firestore_module_factory), True
    except Exception as err:
        logging.getLogger(__name__).warning(
            "verdict-store Firestore read unavailable (collection=%s, %s: %s)",
            collection,
            type(err).__name__,
            err,
        )
        return {}, False


def get_all_verdicts(
    collection: str,
    *,
    project_id: str | None = None,
    firestore_module_factory: FirestoreModuleFactory = _default_firestore_module,
) -> dict[str, dict[str, object]]:
    """``{key: doc}`` for every document in ``collection`` — the plain convenience wrapper (drops
    the availability flag; use :func:`get_all_verdicts_with_status` when a caller must distinguish
    "empty but reachable" from "unreachable", e.g. to set a panel's honest ``source`` label)."""
    docs, _available = get_all_verdicts_with_status(
        collection, project_id=project_id, firestore_module_factory=firestore_module_factory
    )
    return docs


def get_verdict(
    collection: str,
    key: str,
    *,
    project_id: str | None = None,
    firestore_module_factory: FirestoreModuleFactory = _default_firestore_module,
) -> dict[str, object]:
    """One ``{collection}/{key}`` doc, or ``{}`` if absent/unavailable. Reuses ``get_all_verdicts``
    (these collections are small — a couple dozen repos / two check_types)."""
    return get_all_verdicts(collection, project_id=project_id, firestore_module_factory=firestore_module_factory).get(
        key, {}
    )

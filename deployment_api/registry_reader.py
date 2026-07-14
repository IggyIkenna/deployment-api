"""Firestore-first deployment-registry reads with a loud GCS fallback (Phase-2 migration).

Every reader of the RUNNING deployment set routes through :func:`resolve_active_registry`
so the GCS→Firestore migration is one switch. When the migration flag
(``deployment_registry_firestore_dualwrite``) is on, reads go to Firestore's indexed
``query_by_status("running")`` — a single indexed query instead of downloading N GCS blobs,
which is the whole point (the census used to time out downloading ~3k ``active/`` blobs).

The fallback is LOUD and conservative: on empty (Firestore not yet filled) or any error, log a
warning and fall back to the GCS ``list_active()`` — verbatim today's behaviour — so the fleet
view is never worse than today while Firestore fills, then shifts to Firestore-truth as docs
appear (same pattern as ``resolve_ci_status_map`` in the CI-status migration). When the flag is
off, reads stay pure-GCS (Firestore is not being written, so querying it would be pointless).

Phase 3 (cutover) simplifies this to Firestore-only once every reader is migrated and the
dual-write soak confirms parity.
"""

from __future__ import annotations

import logging

from unified_trading_library import (
    DEFAULT_BUCKET,
    DeploymentRegistryEntry,
    DeploymentsRegistry,
)
from unified_trading_library.cloud_interface import (  # noqa: qg-deep-import — P1 store not re-exported from UTL top-level yet
    DeploymentRegistryStore,
    FirestoreDeploymentRegistryStore,
)

from deployment_api.deployment_api_config import DeploymentApiConfig

logger = logging.getLogger(__name__)


def _dualwrite_enabled() -> bool:
    enabled: bool = DeploymentApiConfig().deployment_registry_firestore_dualwrite
    return enabled


def _project_id() -> str:
    return DeploymentApiConfig().require_gcp_project_id()


def resolve_active_registry(
    *,
    store: DeploymentRegistryStore | None = None,
    gcs: DeploymentsRegistry | None = None,
    firestore_enabled: bool | None = None,
) -> list[DeploymentRegistryEntry]:
    """Return the RUNNING deployment entries, Firestore-first with a loud GCS fallback.

    Args:
        store: injected Firestore store (tests); defaults to a real
            ``FirestoreDeploymentRegistryStore`` when the flag is on.
        gcs: injected GCS registry (tests); defaults to a real ``DeploymentsRegistry``.
        firestore_enabled: override the flag (tests); defaults to the config flag.

    Behaviour:
        * flag on → Firestore ``query_by_status("running")``. Non-empty → return it (the
          indexed scale path). Empty or raises → **log a warning and fall back to GCS**.
        * flag off → GCS ``list_active()`` (Firestore is not being written).
    """
    enabled = firestore_enabled if firestore_enabled is not None else _dualwrite_enabled()
    if enabled:
        fs = store if store is not None else FirestoreDeploymentRegistryStore(project_id=_project_id())
        try:
            entries = fs.query_by_status("running")
            if entries:
                return entries
            logger.warning(
                "resolve_active_registry: Firestore returned 0 running deployments — "
                "falling back to the GCS active/ list (Firestore may still be filling)"
            )
        except Exception as exc:  # loud best-effort read migration — never worse than GCS
            logger.warning(
                "resolve_active_registry: Firestore read failed (%s) — falling back to the GCS active/ list",
                exc,
            )
    registry = gcs if gcs is not None else DeploymentsRegistry(bucket=DEFAULT_BUCKET)
    return registry.list_active()

# Epic: observability_master
# Lifecycle: permanent
"""GCP Cloud Functions (gen2) census — existence + config for the deployment inventory.

The unified deployment inventory (``GET /api/deployments/inventory``) needs a
read-only list of the live GCP Cloud Functions gen2 estate. Gen2 functions run on
Cloud Run underneath (WS-B), so this census is existence + config ONLY — invocation
counts / latency / error-rate belong to the ``CLOUD_RUN_SERVICE`` census (a separate
WS-B task), not duplicated here.

Cloud-agnostic boundary: the GCP SDK is reached ONLY through deployment-service's
``backends._gcp_sdk`` lazy-import boundary (``functions_v2.FunctionServiceClient``),
the same pattern ``_cloud_run_executions.py`` uses for ``run_v2`` — never an inline
``google.cloud``-direct import of ``functions_v2`` here (CLAUDE.md cloud-SDK-direct ban).

Honest degradation: a Cloud Functions list failure (creds / API down / region) is
logged and yields an empty map so the inventory degrades to the other kinds — never
a crash, never a fabricated status.

SSOT: ``plans/active/deployment_obs_backend_kinds_health_2026_07_09.md`` WS-B.
"""

from __future__ import annotations

import logging
from dataclasses import dataclass
from datetime import UTC, datetime

logger = logging.getLogger(__name__)

# Cloud Functions gen2 live alongside the rest of the GCP estate in asia-northeast1
# (CLAUDE.md § VM launchers — all GCS data is in asia-northeast1).
DEFAULT_CLOUD_FUNCTIONS_REGION = "asia-northeast1"

# Per-RPC deadline (< the inventory census wall-clock of 45 s) so a wedged functions-list RPC
# unwinds its census worker on its own — DeadlineExceeded is caught below and degrades to an
# empty census. Keeps the inventory census pool from starving under a persistent hang.
_RPC_TIMEOUT_SEC = 30.0

# functions_v2.Function.State enum name → inventory wire status.
_STATE_TO_STATUS: dict[str, str] = {
    "ACTIVE": "running",
    "FAILED": "failed",
    "DEPLOYING": "pending",
    "DELETING": "pending",
    "UNKNOWN": "unknown",
    "STATE_UNSPECIFIED": "unknown",
}


@dataclass(frozen=True)
class CloudFunctionStatus:
    """One GCP Cloud Function (gen2) in the read-only census.

    Attributes:
        name: The short function name (last path segment of ``fn.name``).
        status: ``running``/``failed``/``pending``/``unknown`` — the inventory wire
            status, mapped from the function's ``state``.
        runtime: The ``build_config.runtime`` string (e.g. ``python313``), or ``""``.
        service_name: The short name of the underlying Cloud Run service gen2 deploys
            onto (``service_config.service``), or ``""`` when absent.
        last_updated_at: ISO-8601 ``update_time``, or ``None`` when unparseable.
    """

    name: str
    status: str
    runtime: str
    service_name: str
    last_updated_at: str | None


def _iso(value: object) -> str | None:
    """Best-effort ISO-8601 from a functions_v2 timestamp (proto datetime), else None."""
    if isinstance(value, datetime):
        dt = value if value.tzinfo is not None else value.replace(tzinfo=UTC)
        return dt.isoformat()
    return None


def list_cloud_functions(
    project_id: str,
    region: str = DEFAULT_CLOUD_FUNCTIONS_REGION,
) -> dict[str, CloudFunctionStatus]:
    """List every GCP Cloud Function (gen2), keyed by short function name.

    Existence + config only (per WS-B scope): gen2 functions run on Cloud Run
    underneath, so deeper health (invocations/latency/error-rate) is the
    ``CLOUD_RUN_SERVICE`` census's job, not this one's.

    Honest degradation: any GCP error (creds / API down / region) is logged and
    yields ``{}`` so the inventory falls back to the other censused kinds.
    """
    try:
        # GCP SDK reached ONLY via the deployment-service _gcp_sdk boundary.
        from deployment_service.backends import _gcp_sdk  # noqa: imports-inside-functions

        functions_v2 = _gcp_sdk.functions_v2
        client = functions_v2.FunctionServiceClient()
        parent = f"projects/{project_id}/locations/{region}"
        result: dict[str, CloudFunctionStatus] = {}
        # functions_v2 is the untyped GCP-SDK boundary (_gcp_sdk); its pager element
        # type is partially unknown — fields are read defensively via getattr() below.
        for fn in client.list_functions(  # pyright: ignore[reportUnknownMemberType, reportUnknownVariableType]
            request=functions_v2.ListFunctionsRequest(parent=parent),
            timeout=_RPC_TIMEOUT_SEC,  # pyright: ignore[reportUnknownMemberType, reportUnknownArgumentType]
        ):
            short_name = str(fn.name).rsplit("/", 1)[-1]  # pyright: ignore[reportUnknownMemberType, reportUnknownArgumentType]
            state = getattr(fn, "state", None)
            status = _STATE_TO_STATUS.get(getattr(state, "name", "UNKNOWN"), "unknown")
            build_config = getattr(fn, "build_config", None)
            runtime = str(getattr(build_config, "runtime", "") or "")
            fn_service_cfg = getattr(fn, "service_config", None)
            raw_service = str(getattr(fn_service_cfg, "service", "") or "")
            service_name = raw_service.rsplit("/", 1)[-1] if raw_service else ""
            result[short_name] = CloudFunctionStatus(
                name=short_name,
                status=status,
                runtime=runtime,
                service_name=service_name,
                last_updated_at=_iso(getattr(fn, "update_time", None)),
            )
        return result
    except Exception as exc:
        logger.warning("GCP Cloud Functions census failed (degrading to empty list): %s", exc)
        return {}


__all__ = [
    "DEFAULT_CLOUD_FUNCTIONS_REGION",
    "CloudFunctionStatus",
    "list_cloud_functions",
]

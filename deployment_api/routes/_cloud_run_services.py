# Epic: observability_master
# Lifecycle: permanent
"""GCP Cloud Run **services** census — the always-on service estate.

The unified deployment inventory (``GET /api/deployments/inventory``) censuses VMs
and Cloud Run **jobs** today but silently ignores Cloud Run **services**
(deployment-api, market-data-query, dashboards, alerting, quota-broker,
data-status-rollup, ...) — always-on prod deployables invisible on the
deployment-observability surface. This module lists the live service estate via the
GCP Cloud Run Admin API (``run_v2.ServicesClient``), the twin of
``_cloud_run_executions.latest_execution_by_job`` for jobs.

A service has no live/batch/paper phase (WS-B Open-Q1, resolved 2026-07-09): it never
rides ``classify_deployment_target`` / :class:`DeploymentTarget` — the inventory route
stamps its wire-level ``umbrella`` as ``"—"`` directly; the ``Kind`` column/filter is
how an operator finds it.

Cloud-agnostic boundary: the GCP SDK is reached ONLY through deployment-service's
``backends._gcp_sdk`` lazy-import boundary (``run_v2.ServicesClient``), the same seam
``_cloud_run_executions`` uses for ``run_v2.JobsClient`` / ``ExecutionsClient`` — never
an inline ``from google.cloud import run_v2`` here.

Honest degradation: a Cloud Run list failure (creds / API down / region) is logged and
yields an empty list — never a crash, never a fabricated service.

SSOT: ``plans/active/deployment_obs_backend_kinds_health_2026_07_09.md`` WS-B (kinds
census).
"""

from __future__ import annotations

import logging
from dataclasses import dataclass

logger = logging.getLogger(__name__)

# Cloud Run services live alongside the rest of the GCP estate in asia-northeast1
# (CLAUDE.md § VM launchers — all GCS data is in asia-northeast1), same default the
# Cloud Run job census (``_cloud_run_executions.DEFAULT_CLOUD_RUN_REGION``) uses.
DEFAULT_CLOUD_RUN_SERVICE_REGION = "asia-northeast1"


@dataclass(frozen=True)
class CloudRunServiceCensus:
    """One live Cloud Run service (ready-state + latest revision + region).

    Attributes:
        name: The short Cloud Run service name (last path segment), e.g.
            ``deployment-api``.
        region: The GCP region the service runs in.
        ready: Whether the service's terminal condition reports success (latest
            revision serving traffic).
        revision: The short name of the latest READY revision, or ``""`` when the
            service has never successfully reconciled.
        uri: The service's public URI, or ``""`` when absent.
    """

    name: str
    region: str
    ready: bool
    revision: str
    uri: str


def _ready_state(service: object) -> bool:
    """True if the service's terminal condition reports success (serving traffic).

    ``run_v2.Service.terminal_condition`` is the SDK's own readiness rollup — read
    defensively via ``getattr`` since the underlying condition/state enum types live
    behind the untyped ``_gcp_sdk`` boundary. Any missing/unexpected shape reads as
    NOT ready (honest default — never fabricate a healthy service).
    """
    terminal = getattr(service, "terminal_condition", None)
    if terminal is None:
        return False
    state = getattr(terminal, "state", None)
    state_name = getattr(state, "name", str(state))
    return state_name == "CONDITION_SUCCEEDED"


def _latest_revision(service: object) -> str:
    """Short name of the latest READY revision (last path segment), or ``""``."""
    raw = getattr(service, "latest_ready_revision", "") or ""
    return str(raw).rsplit("/", 1)[-1]


def list_cloud_run_services(
    project_id: str,
    region: str = DEFAULT_CLOUD_RUN_SERVICE_REGION,
) -> list[CloudRunServiceCensus]:
    """List every live Cloud Run SERVICE (not job) — the always-on estate census.

    Lists services (``run_v2.ServicesClient.list_services``) and maps each to its
    ready-state + latest revision + region. Honest degradation: any GCP error (creds /
    API down / region) is logged and yields ``[]`` so the inventory falls back to the
    VM + Cloud Run job estate — never a crash.
    """
    try:
        # GCP SDK reached ONLY via the deployment-service _gcp_sdk boundary.
        from deployment_service.backends import _gcp_sdk

        run_v2 = _gcp_sdk.run_v2
        client = run_v2.ServicesClient()
        parent = f"projects/{project_id}/locations/{region}"
        result: list[CloudRunServiceCensus] = []
        # run_v2 is the untyped GCP-SDK boundary (_gcp_sdk); its pager element type is
        # partially unknown — fields are read defensively via getattr() below, so the
        # unknown pager element type is safe here.
        for svc in client.list_services(request=run_v2.ListServicesRequest(parent=parent)):  # pyright: ignore[reportUnknownMemberType, reportUnknownVariableType]
            name = str(svc.name).rsplit("/", 1)[-1]  # pyright: ignore[reportUnknownMemberType, reportUnknownArgumentType]
            result.append(
                CloudRunServiceCensus(
                    name=name,
                    region=region,
                    ready=_ready_state(svc),
                    revision=_latest_revision(svc),
                    uri=str(getattr(svc, "uri", "") or ""),
                )
            )
        return result
    except Exception as exc:
        logger.warning("Cloud Run services list failed (degrading to empty list): %s", exc)
        return []


__all__ = [
    "DEFAULT_CLOUD_RUN_SERVICE_REGION",
    "CloudRunServiceCensus",
    "list_cloud_run_services",
]

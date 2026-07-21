# Epic: observability_master
# Lifecycle: permanent
"""Live census of GCP Cloud Run **services** (always-on, no live/batch/paper phase).

Unlike a Cloud Run **job** (a bounded execution the unified inventory already
censuses via ``_cloud_run_executions.py``), a Cloud Run **service** is a
continuously-serving deployment (``deployment-api``, ``market-data-query``,
``alerting``, ...) with a ready-state + revision instead of exit-code
executions. This helper lists every live service in one region through the
GCP Cloud Run Admin API and maps it to the inventory wire shape.

Cloud-agnostic boundary: the GCP SDK is reached ONLY through deployment-service's
``backends._gcp_sdk`` lazy-import boundary (``run_v2.ServicesClient``), the same
client ``backends/gcp.py`` + ``_cloud_run_executions.py`` already use — never an
inline ``from google.cloud import run_v2`` here (CLAUDE.md cloud-SDK-direct ban).

Honest degradation: a Cloud Run services-list failure (creds / API down / region)
is logged and yields an empty list, so the inventory simply shows zero
``CLOUD_RUN_SERVICE`` rows rather than crashing — the census for every other kind
(VMs, Cloud Run jobs) proceeds unaffected (shard-level failure isolation).

SSOT: ``plans/active/deployment_obs_backend_kinds_health_2026_07_09.md``.
"""

from __future__ import annotations

import logging
from dataclasses import dataclass
from datetime import UTC, datetime

logger = logging.getLogger(__name__)

# Cloud Run services live alongside the rest of the GCP estate in asia-northeast1
# (CLAUDE.md § VM launchers — all GCS data is in asia-northeast1), same default the
# Cloud Run jobs census uses.
DEFAULT_CLOUD_RUN_REGION = "asia-northeast1"

# Per-RPC deadline (< the inventory census wall-clock of 45 s) so a wedged services-list RPC
# unwinds its census worker on its own — DeadlineExceeded is caught below and degrades to an
# empty census. Keeps the inventory census pool from starving under a persistent hang.
_RPC_TIMEOUT_SEC = 30.0

# run_v2 Condition.State enum name for a fully-ready, traffic-serving revision.
_CONDITION_SUCCEEDED = "CONDITION_SUCCEEDED"


@dataclass(frozen=True)
class CloudRunServiceStatus:
    """Live status for one Cloud Run **service** (the inventory enrichment).

    Attributes:
        name: The short Cloud Run service name (last path segment), e.g.
            ``deployment-api``.
        ready: ``True`` when the service's ``terminal_condition`` reports
            ``CONDITION_SUCCEEDED`` (traffic is being served by a ready revision).
        state: The raw ``terminal_condition.state`` enum name (e.g.
            ``CONDITION_SUCCEEDED`` / ``CONDITION_RECONCILING`` /
            ``CONDITION_FAILED``), for callers that want the exact reconciliation
            state rather than the collapsed ``ready`` bool.
        revision: The short name of ``latest_ready_revision`` (falls back to
            ``latest_created_revision`` when nothing has gone ready yet), or
            ``""`` when neither is set.
        region: The location segment parsed from the service's full resource
            name (``projects/{p}/locations/{region}/services/{name}``).
        uri: The service's main serving URI, or ``""`` when absent.
        min_instance_count: The service's configured minimum instances
            (``template.scaling.min_instance_count``), ``0`` when scale-to-zero
            or unreadable — lets the health classifier tell an always-on service
            (min > 0 → ``serving``) from an idle scale-to-zero one.
        last_deployed_at: ISO-8601 ``update_time`` (falls back to ``create_time``) off the
            SAME ``Service`` resource this call already fetched — a Tier-0 free win, no extra
            RPC. Bumped on every new-revision deploy, so it approximates "when the latest
            revision was created" without the per-service ``GetRevision`` call that would cost.
            Closes the audit-found asymmetry vs its AWS twin ``ECS_SERVICE``
            (``last_run_at = updated_at or created_at``, ``_aws_deployments.py``) — Cloud Run
            services previously carried NO timestamp at all. ``None`` on a read/parse failure.
    """

    name: str
    ready: bool
    state: str
    revision: str
    region: str
    uri: str
    min_instance_count: int = 0
    last_deployed_at: str | None = None


def _iso(value: object) -> str | None:
    """Best-effort ISO-8601 from a Cloud Run timestamp (proto-plus datetime), else None."""
    if not isinstance(value, datetime):
        return None
    dt = value if value.tzinfo is not None else value.replace(tzinfo=UTC)
    return dt.isoformat()


def _region_from_resource_name(full_name: str, fallback: str) -> str:
    """Parse the ``{region}`` segment out of ``projects/{p}/locations/{region}/services/{s}``.

    Falls back to the region the list call was made against when the resource
    name doesn't match the expected shape (defensive — never raises).
    """
    parts = full_name.split("/")
    if len(parts) >= 4 and parts[2] == "locations":
        return parts[3]
    return fallback


def list_cloud_run_services(
    project_id: str,
    region: str = DEFAULT_CLOUD_RUN_REGION,
) -> list[CloudRunServiceStatus]:
    """List every Cloud Run **service** in one region with ready-state + revision.

    Lists services (``run_v2.ServicesClient.list_services``) and maps each to its
    ready-state (from ``terminal_condition``), latest revision, region, and URI.

    Honest degradation: any GCP error (creds / API down / import failure) is
    logged and yields ``[]`` so the census for every other kind proceeds
    unaffected — never a crash, never a fabricated service row.
    """
    try:
        # GCP SDK reached ONLY via the deployment-service _gcp_sdk boundary.
        from deployment_service.backends import _gcp_sdk

        run_v2 = _gcp_sdk.run_v2
        services_client = run_v2.ServicesClient()
        parent = f"projects/{project_id}/locations/{region}"
        result: list[CloudRunServiceStatus] = []
        # run_v2 is the untyped GCP-SDK boundary (_gcp_sdk); its pager member types
        # are partially unknown — per-service fields are read defensively via
        # getattr() below, so the unknown pager element type is safe here.
        for service in services_client.list_services(  # pyright: ignore[reportUnknownMemberType, reportUnknownVariableType]
            request=run_v2.ListServicesRequest(parent=parent),
            timeout=_RPC_TIMEOUT_SEC,  # pyright: ignore[reportUnknownMemberType, reportUnknownArgumentType]
        ):
            full_name = str(service.name)  # pyright: ignore[reportUnknownMemberType, reportUnknownArgumentType]
            short_name = full_name.rsplit("/", 1)[-1]
            terminal = getattr(service, "terminal_condition", None)
            state_enum = getattr(terminal, "state", None)
            state_name = str(state_enum.name) if state_enum is not None else "STATE_UNSPECIFIED"
            revision = str(
                getattr(service, "latest_ready_revision", "") or getattr(service, "latest_created_revision", "") or ""
            )
            # template.scaling.min_instance_count — 0 (scale-to-zero) when absent/unreadable.
            template = getattr(service, "template", None)
            scaling = getattr(template, "scaling", None)
            try:
                min_instances = int(getattr(scaling, "min_instance_count", 0) or 0)
            except (TypeError, ValueError):
                min_instances = 0
            last_deployed_at = _iso(getattr(service, "update_time", None)) or _iso(
                getattr(service, "create_time", None)
            )
            result.append(
                CloudRunServiceStatus(
                    name=short_name,
                    ready=state_name == _CONDITION_SUCCEEDED,
                    state=state_name,
                    revision=revision.rsplit("/", 1)[-1],
                    region=_region_from_resource_name(full_name, region),
                    uri=str(getattr(service, "uri", "") or ""),
                    min_instance_count=min_instances,
                    last_deployed_at=last_deployed_at,
                )
            )
        return result
    except Exception as exc:
        logger.warning("Cloud Run services list failed (degrading to empty census): %s", exc)
        return []


__all__ = [
    "DEFAULT_CLOUD_RUN_REGION",
    "CloudRunServiceStatus",
    "list_cloud_run_services",
]

# Epic: observability_master
# Lifecycle: permanent
"""Service-health sub-taxonomy classifiers (parent plan D.3).

Pure, I/O-free classifiers for the `serving` / `scaled-to-zero` / `dead` / `degraded`
service states, shared by BOTH the GCP inventory row builder (`deployments_inventory`,
Cloud Run services) and the AWS one (`_aws_deployments`, ECS services). They live in
their own module — rather than on `deployments_inventory` — because `_aws_deployments`
is imported BY `deployments_inventory`; a reverse import for the ECS wiring would cycle.

The states mean:

* `serving`        — the service is up and healthy (running == desired, or a ready Cloud
                     Run revision with instances) and any error-rate is in band.
* `scaled-to-zero` — an intentional off switch (ECS desired == 0, or a Cloud Run service
                     configured min-instances 0 with none active). Neutral, never red.
* `dead`           — it should be up and isn't (ECS desired > 0 but running == 0, or a
                     Cloud Run revision that failed to become ready). "should be up, isn't"
                     beats "configured off".
* `degraded`       — partial capacity, unknown ready-state, or an over-threshold error-rate.
"""

from __future__ import annotations

# error-rate threshold above which a service reads "degraded" even while fully
# scaled. v1 default (undocumented SLO in the plan) — revisit once the ECS/Cloud
# Run census is wired to a real error-rate signal (parent plan Open-Q7).
_SERVICE_ERROR_RATE_THRESHOLD = 0.05

SERVICE_STATUS_SERVING = "serving"
SERVICE_STATUS_SCALED_TO_ZERO = "scaled-to-zero"
SERVICE_STATUS_DEAD = "dead"
SERVICE_STATUS_DEGRADED = "degraded"


def ecs_service_health_status(
    desired_count: int,
    running_count: int,
    error_rate: float | None = None,
) -> str:
    """ECS service composite status from desired-vs-running (parent D.3).

    ``desired_count == 0`` is an intentional scale-to-zero (neutral, not an
    error) — never hidden, never flagged red. ``running_count == 0`` while
    something is desired is ``dead`` (should be up, isn't). Any capacity
    shortfall short of fully dead, or an error-rate over threshold, is
    ``degraded`` (amber) rather than a false ``serving`` green.
    """
    if desired_count <= 0:
        return SERVICE_STATUS_SCALED_TO_ZERO
    if running_count <= 0:
        return SERVICE_STATUS_DEAD
    if running_count < desired_count:
        return SERVICE_STATUS_DEGRADED
    if error_rate is not None and error_rate > _SERVICE_ERROR_RATE_THRESHOLD:
        return SERVICE_STATUS_DEGRADED
    return SERVICE_STATUS_SERVING


def cloud_run_service_health_status(
    ready: bool | None,
    min_instance_count: int = 0,
    active_instance_count: int | None = None,
    error_rate: float | None = None,
) -> str:
    """Cloud Run service composite status from ready-state + revision health
    (parent D.3) — the Cloud Run analog of ``ecs_service_health_status``, using
    the terminal-condition ready-state + traffic-serving revision in place of
    ECS's desired/running counts.

    ``ready is False`` means the latest revision failed to become ready — the
    service should be serving and isn't, so ``dead``. A service configured with
    ``min_instance_count == 0`` and observed with zero active instances is an
    intentional scale-to-zero. ``ready is None`` (state unknown / not yet
    resolved) degrades honest rather than claiming a green it can't back up.
    """
    if ready is False:
        return SERVICE_STATUS_DEAD
    if min_instance_count <= 0 and (active_instance_count is None or active_instance_count <= 0):
        return SERVICE_STATUS_SCALED_TO_ZERO
    if ready is None:
        return SERVICE_STATUS_DEGRADED
    if error_rate is not None and error_rate > _SERVICE_ERROR_RATE_THRESHOLD:
        return SERVICE_STATUS_DEGRADED
    return SERVICE_STATUS_SERVING


__all__ = [
    "SERVICE_STATUS_DEAD",
    "SERVICE_STATUS_DEGRADED",
    "SERVICE_STATUS_SCALED_TO_ZERO",
    "SERVICE_STATUS_SERVING",
    "cloud_run_service_health_status",
    "ecs_service_health_status",
]

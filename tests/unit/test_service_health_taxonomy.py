# Epic: observability_master
# Lifecycle: permanent
"""Unit tests for the service-health sub-taxonomy (parent plan D.3).

Pure-function tests — no fixtures, no network, no mocks needed. Pins the
ECS (desired-vs-running) and Cloud Run (ready-state/revision) composite-status
classifiers against every state in the D.3 taxonomy: serving / scaled-to-zero /
dead / degraded.
"""

from __future__ import annotations

import pytest

from deployment_api.routes.deployments_inventory import (
    SERVICE_STATUS_DEAD,
    SERVICE_STATUS_DEGRADED,
    SERVICE_STATUS_SCALED_TO_ZERO,
    SERVICE_STATUS_SERVING,
    cloud_run_service_health_status,
    ecs_service_health_status,
)

pytestmark = [pytest.mark.timeout(10)]


class TestEcsServiceHealthStatus:
    def test_scaled_to_zero_when_desired_is_zero(self) -> None:
        assert ecs_service_health_status(desired_count=0, running_count=0) == SERVICE_STATUS_SCALED_TO_ZERO

    def test_scaled_to_zero_never_flagged_dead_even_with_stray_running_count(self) -> None:
        # desired=0 is an intentional off switch — a stray running count doesn't
        # override the neutral verdict.
        assert ecs_service_health_status(desired_count=0, running_count=1) == SERVICE_STATUS_SCALED_TO_ZERO

    def test_dead_when_desired_positive_but_nothing_running(self) -> None:
        assert ecs_service_health_status(desired_count=3, running_count=0) == SERVICE_STATUS_DEAD

    def test_serving_when_running_matches_desired_and_no_error_rate(self) -> None:
        assert ecs_service_health_status(desired_count=3, running_count=3) == SERVICE_STATUS_SERVING

    def test_degraded_on_partial_capacity(self) -> None:
        assert ecs_service_health_status(desired_count=3, running_count=1) == SERVICE_STATUS_DEGRADED

    def test_degraded_on_error_rate_over_threshold_even_at_full_capacity(self) -> None:
        assert ecs_service_health_status(desired_count=3, running_count=3, error_rate=0.10) == SERVICE_STATUS_DEGRADED

    def test_serving_when_error_rate_in_band(self) -> None:
        assert ecs_service_health_status(desired_count=3, running_count=3, error_rate=0.01) == SERVICE_STATUS_SERVING

    def test_serving_at_exactly_the_error_rate_threshold(self) -> None:
        # Threshold is exclusive (> not >=) — right at the line still reads healthy.
        assert ecs_service_health_status(desired_count=1, running_count=1, error_rate=0.05) == SERVICE_STATUS_SERVING


class TestCloudRunServiceHealthStatus:
    def test_dead_when_not_ready(self) -> None:
        assert cloud_run_service_health_status(ready=False) == SERVICE_STATUS_DEAD

    def test_dead_takes_priority_over_scale_to_zero_config(self) -> None:
        # A revision that failed to become ready is dead regardless of its
        # min-instance config — "should be up, isn't" beats "configured off".
        assert (
            cloud_run_service_health_status(ready=False, min_instance_count=0, active_instance_count=0)
            == SERVICE_STATUS_DEAD
        )

    def test_scaled_to_zero_when_min_instances_zero_and_none_active(self) -> None:
        assert (
            cloud_run_service_health_status(ready=True, min_instance_count=0, active_instance_count=0)
            == SERVICE_STATUS_SCALED_TO_ZERO
        )

    def test_scaled_to_zero_when_active_instance_count_unknown(self) -> None:
        assert (
            cloud_run_service_health_status(ready=True, min_instance_count=0, active_instance_count=None)
            == SERVICE_STATUS_SCALED_TO_ZERO
        )

    def test_serving_when_ready_and_actively_instanced(self) -> None:
        assert (
            cloud_run_service_health_status(ready=True, min_instance_count=1, active_instance_count=1)
            == SERVICE_STATUS_SERVING
        )

    def test_degraded_when_ready_state_unknown(self) -> None:
        assert (
            cloud_run_service_health_status(ready=None, min_instance_count=1, active_instance_count=1)
            == SERVICE_STATUS_DEGRADED
        )

    def test_degraded_on_error_rate_over_threshold(self) -> None:
        assert (
            cloud_run_service_health_status(ready=True, min_instance_count=1, active_instance_count=1, error_rate=0.25)
            == SERVICE_STATUS_DEGRADED
        )

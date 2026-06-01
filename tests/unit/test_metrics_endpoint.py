"""Unit tests for GET /metrics Prometheus telemetry endpoint."""

from __future__ import annotations

import re

import pytest
from fastapi.testclient import TestClient

_EXPECTED_METRICS = [
    "deployment_api_records_processed_total",
    "deployment_api_processing_latency_seconds",
    "deployment_api_vms_in_flight",
    "deployment_api_last_snapshot_age_seconds",
]


@pytest.fixture(scope="module")
def client() -> TestClient:
    from deployment_api.main import app

    return TestClient(app, raise_server_exceptions=False)


def test_metrics_returns_200(client: TestClient) -> None:
    resp = client.get("/metrics")
    assert resp.status_code == 200


def test_metrics_content_type_is_prometheus(client: TestClient) -> None:
    resp = client.get("/metrics")
    assert "text/plain" in resp.headers.get("content-type", "")


def test_metrics_exposes_five_or_more_named_metrics(client: TestClient) -> None:
    """Verify at least 5 distinct metric names appear in the Prometheus output."""
    resp = client.get("/metrics")
    body = resp.text
    # Extract distinct metric names from HELP lines (# HELP <name> ...)
    names = re.findall(r"^# HELP (\S+)", body, re.MULTILINE)
    assert len(names) >= 5, f"Expected ≥5 metrics, found {len(names)}: {names}"


def test_metrics_includes_custom_deployment_metrics(client: TestClient) -> None:
    """All four custom deployment-api metrics are present."""
    resp = client.get("/metrics")
    body = resp.text
    for metric in _EXPECTED_METRICS:
        assert metric in body, f"Missing metric: {metric}"


def test_metrics_vms_in_flight_is_gauge(client: TestClient) -> None:
    resp = client.get("/metrics")
    body = resp.text
    assert "# TYPE deployment_api_vms_in_flight gauge" in body


def test_metrics_snapshot_age_is_gauge(client: TestClient) -> None:
    resp = client.get("/metrics")
    body = resp.text
    assert "# TYPE deployment_api_last_snapshot_age_seconds gauge" in body

"""Pytest configuration and fixtures for deployment-api tests."""

import os

import pytest

os.environ.setdefault("CLOUD_MOCK_MODE", "true")
os.environ.setdefault("GCP_PROJECT_ID", "test-project")

from unified_events_interface import MockEventSink, setup_events

setup_events(
    mode="batch",
    service_name="deployment-api",
    sink=MockEventSink(),
)


@pytest.fixture
def client():
    """FastAPI test client."""
    from fastapi.testclient import TestClient

    from deployment_api.main import app

    return TestClient(app)

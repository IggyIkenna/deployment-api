"""Health endpoint tests."""

import pytest


def test_root(client):
    """Root returns service info."""
    r = client.get("/")
    assert r.status_code == 200
    data = r.json()
    assert data["service"] == "deployment-api"
    assert data["status"] == "ok"


def test_health(client):
    """Health endpoint returns healthy."""
    r = client.get("/api/health")
    assert r.status_code == 200
    assert r.json()["status"] == "healthy"

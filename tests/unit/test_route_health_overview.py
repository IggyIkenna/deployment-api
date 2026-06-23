"""Unit tests for routes/health_overview.py + routes/health_consolidator.py.

Credential-free / --block-network safe: every data source (fleet census, consolidator
GCS reads, coverage rollup, alert ledger, gh rate-limit, cost blobs) is mocked or routed
through mock-mode. Covers the worst-tile rollup logic (ok/degraded/critical/unknown) +
the per-asset_group consolidator classification explicitly.

Plan: unified_deployment_health_cockpit_2026_06_23.md Phase 1 [TEST].
"""

from __future__ import annotations

from datetime import UTC, datetime
from unittest.mock import patch

import pytest
from fastapi import FastAPI
from fastapi.testclient import TestClient

_OVERVIEW_MOCK = "deployment_api.routes.health_overview.DeploymentApiConfig.is_mock_mode"
_CONSOLIDATOR_MOCK = "deployment_api.routes.health_consolidator.DeploymentApiConfig.is_mock_mode"
_FIXED_NOW = datetime(2026, 6, 23, 12, 0, 0, tzinfo=UTC)


# ---------------------------------------------------------------------------
# health_overview — pure rollup logic (no I/O)
# ---------------------------------------------------------------------------


def _tile(status: str, tile_id: str = "t") -> object:
    from deployment_api.routes.health_overview import HealthTile

    return HealthTile(id=tile_id, label=tile_id, status=status, value="v", detail_href="/x")


def test_rollup_all_ok_is_ok() -> None:
    from deployment_api.routes.health_overview import build_overview_rollup

    resp = build_overview_rollup([_tile("ok"), _tile("ok")], _FIXED_NOW)  # type: ignore[list-item]
    assert resp.overall == "ok"


def test_rollup_any_degraded_is_degraded() -> None:
    from deployment_api.routes.health_overview import build_overview_rollup

    resp = build_overview_rollup([_tile("ok"), _tile("degraded"), _tile("ok")], _FIXED_NOW)  # type: ignore[list-item]
    assert resp.overall == "degraded"


def test_rollup_any_critical_is_critical_dominates_degraded() -> None:
    from deployment_api.routes.health_overview import build_overview_rollup

    resp = build_overview_rollup([_tile("degraded"), _tile("critical"), _tile("ok")], _FIXED_NOW)  # type: ignore[list-item]
    assert resp.overall == "critical"


def test_rollup_unknown_escalates_to_degraded_not_critical() -> None:
    """An unreadable source (unknown) is a degraded posture — never silently ok, never critical."""
    from deployment_api.routes.health_overview import build_overview_rollup

    resp = build_overview_rollup([_tile("ok"), _tile("unknown")], _FIXED_NOW)  # type: ignore[list-item]
    assert resp.overall == "degraded"


def test_rollup_generated_at_is_iso() -> None:
    from deployment_api.routes.health_overview import build_overview_rollup

    resp = build_overview_rollup([_tile("ok")], _FIXED_NOW)  # type: ignore[list-item]
    assert resp.generated_at == _FIXED_NOW.isoformat()


# ---------------------------------------------------------------------------
# health_overview — route in mock mode (all tiles present + shaped)
# ---------------------------------------------------------------------------


@pytest.fixture
def client_overview() -> TestClient:
    from deployment_api.routes.health_overview import router

    app = FastAPI()
    app.include_router(router, prefix="/api")
    return TestClient(app, raise_server_exceptions=False)


def test_overview_route_mock_shape(client_overview: TestClient) -> None:
    with (
        patch(_OVERVIEW_MOCK, return_value=True),
        patch(_CONSOLIDATOR_MOCK, return_value=True),
    ):
        resp = client_overview.get("/api/health/overview")
    assert resp.status_code == 200
    body = resp.json()
    assert body["overall"] in ("ok", "degraded", "critical")
    tile_ids = {t["id"] for t in body["tiles"]}
    assert tile_ids == {"fleet", "consolidator", "coverage", "alerts", "gh_budget", "cost"}
    for tile in body["tiles"]:
        assert tile["status"] in ("ok", "degraded", "critical", "unknown")
        assert tile["detail_href"].startswith("/api/")
    # The mock consolidator has a DEFI critical → consolidator tile critical → overall critical.
    consolidator_tile = next(t for t in body["tiles"] if t["id"] == "consolidator")
    assert consolidator_tile["status"] == "critical"
    assert body["overall"] == "critical"


def test_fleet_tile_unknown_on_failure() -> None:
    from deployment_api.routes import health_overview

    with (
        patch(_OVERVIEW_MOCK, return_value=False),
        patch.object(health_overview, "_cfg") as mock_cfg,
        patch("deployment_api.routes.fleet.get_vm_instance_details", side_effect=OSError("api down")),
    ):
        mock_cfg.is_mock_mode.return_value = False
        mock_cfg.gcp_project_id = "test-project"
        tile = health_overview._fleet_tile(_FIXED_NOW)  # pyright: ignore[reportPrivateUsage]
    assert tile.id == "fleet"
    assert tile.status == "unknown"


def test_fleet_tile_critical_on_oom() -> None:
    from deployment_api.routes import health_overview
    from deployment_api.routes._fleet_types import VmCensusResponse

    census = VmCensusResponse(
        generated_at=_FIXED_NOW.isoformat(), running=5, expected=5, zombie=0, oom=2, stopped=1, vms=[]
    )
    with (
        patch(_OVERVIEW_MOCK, return_value=False),
        patch.object(health_overview, "_cfg") as mock_cfg,
        patch("deployment_api.routes._fleet_census.build_vm_census", return_value=census),
        patch("deployment_api.routes._fleet_census.load_watchdog_census", return_value=None),
        patch("deployment_api.routes.fleet.get_vm_instance_details", return_value={}),
    ):
        mock_cfg.is_mock_mode.return_value = False
        mock_cfg.gcp_project_id = "test-project"
        tile = health_overview._fleet_tile(_FIXED_NOW)  # pyright: ignore[reportPrivateUsage]
    assert tile.status == "critical"
    assert "2 OOM" in tile.value


# ---------------------------------------------------------------------------
# health_consolidator — per-AG classification + overall
# ---------------------------------------------------------------------------


def test_classify_ag_fresh_is_ok() -> None:
    from deployment_api.routes.health_consolidator import _classify_ag

    status, fallback, _ = _classify_ag(age=30.0, budget=86400, shards_exist=False)
    assert status == "ok"
    assert fallback is False


def test_classify_ag_stale_with_shards_is_critical() -> None:
    from deployment_api.routes.health_consolidator import _classify_ag

    status, fallback, detail = _classify_ag(age=90000.0, budget=86400, shards_exist=True)
    assert status == "critical"
    assert fallback is True
    assert "DOWN" in detail


def test_classify_ag_stale_no_shards_is_degraded() -> None:
    """Stale/missing index but no shards = genuinely empty bucket, not an outage."""
    from deployment_api.routes.health_consolidator import _classify_ag

    status, fallback, _ = _classify_ag(age=None, budget=86400, shards_exist=False)
    assert status == "degraded"
    assert fallback is False


def test_build_consolidator_overall_is_worst() -> None:
    from deployment_api.routes.health_consolidator import (
        ConsolidatorAgHealth,
        build_consolidator_health,
    )

    entries = [
        ConsolidatorAgHealth(
            asset_group="cefi",
            bucket="b1",
            status="ok",
            staleness_budget_seconds=86400,
            per_vm_shard_fallback_active=False,
            detail="ok",
        ),
        ConsolidatorAgHealth(
            asset_group="defi",
            bucket="b2",
            status="critical",
            staleness_budget_seconds=86400,
            per_vm_shard_fallback_active=True,
            detail="down",
        ),
    ]
    resp = build_consolidator_health(entries, _FIXED_NOW)
    assert resp.overall == "critical"
    assert resp.generated_at == _FIXED_NOW.isoformat()


@pytest.fixture
def client_consolidator() -> TestClient:
    from deployment_api.routes.health_consolidator import router

    app = FastAPI()
    app.include_router(router, prefix="/api")
    return TestClient(app, raise_server_exceptions=False)


def test_consolidator_route_mock_shape(client_consolidator: TestClient) -> None:
    with patch(_CONSOLIDATOR_MOCK, return_value=True):
        resp = client_consolidator.get("/api/health/consolidator")
    assert resp.status_code == 200
    body = resp.json()
    assert body["overall"] == "critical"  # mock defi entry is critical
    ags = {e["asset_group"]: e for e in body["asset_groups"]}
    assert ags["cefi"]["status"] == "ok"
    assert ags["defi"]["status"] == "critical"
    assert ags["defi"]["per_vm_shard_fallback_active"] is True

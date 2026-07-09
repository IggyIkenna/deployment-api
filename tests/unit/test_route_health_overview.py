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


# ---------------------------------------------------------------------------
# Regression — real-cloud bucket-kind resolution (caught 2026-06-24 verifying the
# live deployment-api: the shipped endpoint passed kind="raw_tick_data" — not a valid
# bucket kind — so EVERY AG 500'd; and prediction's market-data store is the dedicated
# ``market-data-tick-prediction`` key, NOT the shared ``market-data`` kind which has no
# prediction entry). The per-AG kind map below is the fix; the guard test proves the map
# is COMPLETE so an unmapped AG can never reach production as a 5xx.
# ---------------------------------------------------------------------------


def test_market_data_kind_prediction_has_dedicated_key() -> None:
    from deployment_api.routes.health_consolidator import _market_data_kind

    # cefi/defi/tradfi/sports share the market-data kind; prediction has its own flat key.
    assert _market_data_kind("cefi") == "market-data"
    assert _market_data_kind("tradfi") == "market-data"
    assert _market_data_kind("prediction") == "market-data-tick-prediction"


def test_every_asset_group_resolves_a_market_data_bucket() -> None:
    """Guard: every consolidator-tracked AG resolves a non-empty bucket via its kind map.

    ``resolve_bucket_name`` is pure string templating (no network), so this is a fast
    completeness check — adding an AG to ``_ASSET_GROUPS`` without a kind entry (the
    real-cloud bug class where prediction had no ``market-data`` entry) fails HERE at
    test time instead of 5xx-ing the live endpoint.
    """
    from unified_trading_library import resolve_bucket_name

    from deployment_api.routes.health_consolidator import _ASSET_GROUPS, _market_data_kind

    for ag in _ASSET_GROUPS:
        bucket = resolve_bucket_name(cloud="gcp", kind=_market_data_kind(ag), asset_group=ag)
        assert bucket, f"{ag} resolved an empty bucket via kind {_market_data_kind(ag)!r}"


_HC = "deployment_api.routes.health_consolidator"


def test_ag_health_include_backlog_populates_pending_and_total() -> None:
    """include_backlog=True → the per-VM shard backlog counts land on the posture."""
    from deployment_api.routes.health_consolidator import _ag_health

    with (
        patch(f"{_HC}.resolve_bucket_name", return_value="market-data-tick-cefi"),
        patch(f"{_HC}.get_storage_client", return_value=object()),
        patch(f"{_HC}.consolidated_blob_age_sec", return_value=30.0),
        patch(f"{_HC}.per_vm_shard_backlog", return_value=(2, 6)) as backlog,
    ):
        posture = _ag_health("cefi", budget=120, now=_FIXED_NOW, include_backlog=True)

    assert posture.status == "ok"  # fresh index (30s <= 120s)
    assert posture.pending_shard_count == 2
    assert posture.total_shard_count == 6
    backlog.assert_called_once()


def test_ag_health_default_omits_backlog_and_never_lists_when_fresh() -> None:
    """Default (freshness-route path) leaves backlog None and pays no shard-list when fresh."""
    from deployment_api.routes.health_consolidator import _ag_health

    with (
        patch(f"{_HC}.resolve_bucket_name", return_value="market-data-tick-cefi"),
        patch(f"{_HC}.get_storage_client", return_value=object()),
        patch(f"{_HC}.consolidated_blob_age_sec", return_value=30.0),
        patch(f"{_HC}.per_vm_shard_backlog") as backlog,
        patch(f"{_HC}.per_vm_shards_exist") as exists,
    ):
        posture = _ag_health("cefi", budget=120, now=_FIXED_NOW)

    assert posture.pending_shard_count is None
    assert posture.total_shard_count is None
    backlog.assert_not_called()
    exists.assert_not_called()  # fresh index → no shard-list at all


def test_budget_for_cefi_overrides_default_others_pass_through() -> None:
    """cefi (daily-batch market-tick, ~5-min consolidator) gets its 86400s tolerance; others default."""
    from deployment_api.routes.health_consolidator import _budget_for

    assert _budget_for("cefi", 120) == 86400  # cadence-matched override
    assert _budget_for("defi", 120) == 120  # ~per-minute consolidator → global default
    assert _budget_for("tradfi", 999) == 999  # default flows through unchanged

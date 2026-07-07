"""Unit tests for routes/fleet.py + its census builder and infra-VM-health proxy.

Credential-free / --block-network safe: the compute API, the agent-orchestrator HTTP
call, and Secret Manager are all mocked. Pins the API side of the deployment-ui FleetInfra
tile contract (the frontend + its playwright spec are already shipped).
"""

from __future__ import annotations

import asyncio
from datetime import UTC, datetime
from unittest.mock import AsyncMock, patch

import pytest
from fastapi import FastAPI
from fastapi.testclient import TestClient

_PATCH_MOCK_MODE = "deployment_api.routes.fleet.DeploymentApiConfig.is_mock_mode"
_FIXED_NOW = datetime(2026, 6, 22, 12, 0, 0, tzinfo=UTC)


@pytest.fixture
def client_fleet() -> TestClient:
    from deployment_api.routes.fleet import router

    app = FastAPI()
    app.include_router(router)
    return TestClient(app, raise_server_exceptions=False)


# ---------------------------------------------------------------------------
# Census builder — counts, classification, age, honest oom/zombie defaults
# ---------------------------------------------------------------------------


def _details() -> dict[str, dict[str, object]]:
    """A representative GCE instance-details map (the shape get_vm_instance_details returns)."""
    return {
        # LONG_LIVED_LIVE, running → counts toward running + expected.
        "planning-vm-1": {
            "status": "RUNNING",
            "zone": "asia-northeast1-c",
            "creation_timestamp": "2026-06-22T11:00:00+00:00",  # 60 min old
            "machine_type": "m7i-2xlarge",
        },
        "human-planning-vm-1": {
            "status": "RUNNING",
            "zone": "asia-northeast1-c",
            "creation_timestamp": "2026-06-21T12:00:00+00:00",  # 1440 min old
            "machine_type": "m7i-2xlarge",
        },
        # EPHEMERAL_BATCH (unknown prefix → default), terminated → counts toward stopped only.
        "cefi-binance-spot-20260622-014158": {
            "status": "TERMINATED",
            "zone": "asia-northeast1-b",
            "creation_timestamp": "2026-06-22T11:30:00+00:00",  # 30 min old
            "machine_type": "n2-standard-8",
        },
        # SCHEDULED_RECURRING, running → running + expected.
        "vm-zombie-watchdog-20260622": {
            "status": "RUNNING",
            "zone": "asia-northeast1-c",
            "creation_timestamp": "",  # unparseable → age_min None
            "machine_type": "e2-small",
        },
        # Stopped batch → stopped (not expected).
        "tradfi-databento-20260620": {
            "status": "STOPPED",
            "zone": "asia-northeast1-c",
            "creation_timestamp": "2026-06-20T00:00:00+00:00",
            "machine_type": "n2-standard-4",
        },
    }


def test_build_census_counts() -> None:
    from deployment_api.routes._fleet_census import build_vm_census

    census = build_vm_census(_details(), _FIXED_NOW)
    # running: planning, human-planning, watchdog = 3.
    assert census.running == 3
    # stopped: cefi (TERMINATED) + tradfi (STOPPED) = 2.
    assert census.stopped == 2
    # expected (LONG_LIVED_LIVE + SCHEDULED_RECURRING): planning, human-planning, watchdog = 3.
    assert census.expected == 3
    # No readable watchdog snapshot → honest zero.
    assert census.zombie == 0
    assert census.oom == 0
    assert len(census.vms) == 5
    assert census.generated_at == _FIXED_NOW.isoformat()


def test_build_census_classification() -> None:
    from deployment_api.routes._fleet_census import build_vm_census

    by_name = {v.name: v for v in build_vm_census(_details(), _FIXED_NOW).vms}
    assert by_name["planning-vm-1"].lifecycle_class == "LONG_LIVED_LIVE"
    assert by_name["human-planning-vm-1"].lifecycle_class == "LONG_LIVED_LIVE"
    assert by_name["vm-zombie-watchdog-20260622"].lifecycle_class == "SCHEDULED_RECURRING"
    # Unknown prefix degrades to EPHEMERAL_BATCH (the honest default).
    assert by_name["cefi-binance-spot-20260622-014158"].lifecycle_class == "EPHEMERAL_BATCH"
    # Prefix label is the first dash-segment.
    assert by_name["cefi-binance-spot-20260622-014158"].prefix == "cefi"


def test_build_census_age_and_status_mapping() -> None:
    from deployment_api.routes._fleet_census import build_vm_census

    by_name = {v.name: v for v in build_vm_census(_details(), _FIXED_NOW).vms}
    assert by_name["planning-vm-1"].age_min == 60
    assert by_name["human-planning-vm-1"].age_min == 1440
    # Unparseable creation_timestamp → None, never a crash.
    assert by_name["vm-zombie-watchdog-20260622"].age_min is None
    # Per-VM honest defaults.
    assert by_name["cefi-binance-spot-20260622-014158"].oom is False
    assert by_name["cefi-binance-spot-20260622-014158"].zombie is False
    assert by_name["cefi-binance-spot-20260622-014158"].status == "TERMINATED"


def test_build_census_empty_map_is_honest_zero() -> None:
    """A compute-API failure → empty map → all-zero census, never a crash."""
    from deployment_api.routes._fleet_census import build_vm_census

    census = build_vm_census({}, _FIXED_NOW)
    assert census.running == 0
    assert census.expected == 0
    assert census.stopped == 0
    assert census.vms == []


# ---------------------------------------------------------------------------
# Census route — mock mode + live path delegates to the builder
# ---------------------------------------------------------------------------


def test_census_route_mock_shape(client_fleet: TestClient) -> None:
    with patch(_PATCH_MOCK_MODE, return_value=True):
        resp = client_fleet.get("/api/fleet/vm-census")
    assert resp.status_code == 200
    body = resp.json()
    assert body["running"] == 2
    assert body["expected"] == 3
    assert body["oom"] == 1
    assert body["zombie"] == 0
    # The OOM-killed defi backfill entry is present (TERMINATED + oom=true).
    oom_entries = [v for v in body["vms"] if v["oom"]]
    assert any(v["name"].startswith("vm-defi-backfill-") and v["status"] == "TERMINATED" for v in oom_entries)
    # The LONG_LIVED_LIVE planning pair is running.
    live = [v for v in body["vms"] if v["lifecycle_class"] == "LONG_LIVED_LIVE"]
    assert {v["prefix"] for v in live} == {"planning", "human-planning"}
    assert all(v["status"] == "RUNNING" for v in live)


def test_census_route_live_delegates_to_builder(client_fleet: TestClient) -> None:
    """Non-mock path reads get_vm_instance_details + rolls it through the builder.

    ``gcp_project_id`` is a UnifiedCloudConfig (pydantic-settings) field, not a class
    attribute, so patch the module-level ``_cfg`` instance (same pattern as
    test_route_monitor_experiments.py)."""
    with (
        patch("deployment_api.routes.fleet._cfg") as mock_cfg,
        patch("deployment_api.routes.fleet.get_vm_instance_details", return_value=_details()) as mock_get,
    ):
        mock_cfg.is_mock_mode.return_value = False
        mock_cfg.gcp_project_id = "test-project"
        resp = client_fleet.get("/api/fleet/vm-census")
    assert resp.status_code == 200
    mock_get.assert_called_once_with("test-project")
    body = resp.json()
    assert body["running"] == 3
    assert body["stopped"] == 2


# ---------------------------------------------------------------------------
# Infra-VM-health proxy — mapping + honest degradation
# ---------------------------------------------------------------------------


def _ao_fleet_payload() -> dict[str, object]:
    """A realistic agent-orchestrator /api/fleet/summary envelope (two VMs)."""
    return {
        "vms": [
            {
                "id": "planning",
                "label": "Central",
                "url": "https://api.agent-orchestrator.odum-research.com",
                "stale": False,
                "last_heartbeat_seconds_ago": 12,
                "error": None,
                "summary": {
                    "vm_id": "planning",
                    "role": "planning",
                    "label": "Central VM",
                    "slots_total": 8,
                    "slots_working": 3,
                    "slots_idle": 5,
                    "slots_stale": 0,
                    "slots_paused": 0,
                    "slots_blocked": 0,
                    "backlog_total": 14,
                    "backlog_queued": 6,
                    "alerts": [{"severity": "warn", "kind": "slots_stale", "detail": "1 slot(s) stale"}],
                    "watchdog": {
                        "enabled": True,
                        "kills_today": 2,
                        "daily_cap": 20,
                        "dormant": False,
                        "flapping": False,
                    },
                },
            },
            {
                # An unreachable VM: error set, summary None → available derived False.
                "id": "human-planning",
                "label": "Human Planning",
                "url": "http://10.0.0.2:8765",
                "stale": True,
                "last_heartbeat_seconds_ago": None,
                "error": "HTTP 502",
                "summary": None,
            },
        ]
    }


def test_infra_health_mapping() -> None:
    from deployment_api.routes._fleet_infra_health import _map_fleet_summary  # pyright: ignore[reportPrivateUsage]

    slots = _map_fleet_summary(_ao_fleet_payload())
    assert len(slots) == 2
    central, human = slots[0], slots[1]

    assert central.id == "planning"
    assert central.available is True
    assert central.last_heartbeat_seconds_ago == 12
    assert central.summary is not None
    assert central.summary.role == "planning"
    assert central.summary.slots_working == 3
    assert central.summary.backlog_queued == 6
    assert central.summary.alerts[0].kind == "slots_stale"
    assert central.summary.watchdog is not None
    assert central.summary.watchdog.kills_today == 2
    assert central.summary.watchdog.daily_cap == 20

    # Unreachable VM: summary None → available derived False, error surfaced.
    assert human.available is False
    assert human.summary is None
    assert human.error == "HTTP 502"
    assert human.last_heartbeat_seconds_ago is None


def test_infra_health_degrades_without_token() -> None:
    """No orchestrator token → available=False + empty vms + the deep-link URL, never raises."""
    from deployment_api.routes import _fleet_infra_health

    with patch.object(_fleet_infra_health, "_resolve_orchestrator_token", new=AsyncMock(return_value=None)):
        result = asyncio.run(_fleet_infra_health.fetch_infra_vm_health("proj"))
    assert result.available is False
    assert result.vms == []
    assert result.orchestrator_url.startswith("http")


def test_infra_health_degrades_on_http_error() -> None:
    """Orchestrator returns non-200 → honest degradation (available=False, empty vms)."""
    from deployment_api.routes import _fleet_infra_health

    class _Resp:
        status = 503

        async def __aenter__(self) -> _Resp:
            return self

        async def __aexit__(self, *_: object) -> None:
            return None

        async def json(self) -> dict[str, object]:
            return {}

    class _Session:
        async def __aenter__(self) -> _Session:
            return self

        async def __aexit__(self, *_: object) -> None:
            return None

        def get(self, *_a: object, **_k: object) -> _Resp:
            return _Resp()

    with (
        patch.object(_fleet_infra_health, "_resolve_orchestrator_token", new=AsyncMock(return_value="tok")),
        patch.object(_fleet_infra_health.aiohttp, "ClientSession", return_value=_Session()),
    ):
        result = asyncio.run(_fleet_infra_health.fetch_infra_vm_health("proj"))
    assert result.available is False
    assert result.vms == []
    assert result.orchestrator_url.startswith("http")


def test_infra_health_route_mock_shape(client_fleet: TestClient) -> None:
    with patch(_PATCH_MOCK_MODE, return_value=True):
        resp = client_fleet.get("/api/fleet/infra-vm-health")
    assert resp.status_code == 200
    body = resp.json()
    assert body["available"] is True
    assert body["orchestrator_url"].startswith("http")
    vm_ids = {v["id"] for v in body["vms"]}
    assert vm_ids == {"planning", "human-planning"}
    # Each mock VM carries a summary with a watchdog posture.
    for vm in body["vms"]:
        assert vm["summary"] is not None
        assert vm["summary"]["watchdog"]["daily_cap"] == 20


# ---------------------------------------------------------------------------
# Orphan inventory builder — stopped-VM enrichment, verdicts, disk cost rollup
# ---------------------------------------------------------------------------

# Fixed "now" 5 days after the representative stop timestamps so the old ones are past a 24h grace.
_ORPHAN_NOW = datetime(2026, 6, 30, 12, 0, 0, tzinfo=UTC)


def _orphan_details() -> dict[str, dict[str, object]]:
    """GCE instance-details map with a mix of stopped + running VMs across lifecycle classes."""
    return {
        # EPHEMERAL_BATCH (unknown prefix), stopped 5 days ago → REAP.
        "cefi-binance-spot-20260601": {
            "status": "TERMINATED",
            "zone": "asia-northeast1-c",
            "creation_timestamp": "2026-05-01T00:00:00+00:00",
            "last_stop_timestamp": "2026-06-25T12:00:00+00:00",
            "boot_disk_name": "cefi-binance-spot-20260601",
            "labels": {},
        },
        # EPHEMERAL_BATCH stopped only 2h ago → within 24h grace → KEEP.
        "tradfi-databento-recent": {
            "status": "STOPPED",
            "zone": "asia-northeast1-c",
            "creation_timestamp": "",
            "last_stop_timestamp": "2026-06-30T10:00:00+00:00",
            "boot_disk_name": "tradfi-databento-recent",
            "labels": {},
        },
        # LONG_LIVED_LIVE stopped for days → never reapable (paused live VM).
        "strategy-live-eth-20260601": {
            "status": "TERMINATED",
            "zone": "asia-northeast1-c",
            "creation_timestamp": "",
            "last_stop_timestamp": "2026-06-20T00:00:00+00:00",
            "boot_disk_name": "strategy-live-eth-20260601",
            "labels": {},
        },
        # Ephemeral + keep=true label → retained for forensics.
        "cefi-keepme-20260601": {
            "status": "TERMINATED",
            "zone": "asia-northeast1-c",
            "creation_timestamp": "",
            "last_stop_timestamp": "2026-06-20T00:00:00+00:00",
            "boot_disk_name": "cefi-keepme-20260601",
            "labels": {"keep": "true"},
        },
        # RUNNING → excluded from the orphan inventory entirely.
        "cefi-running-20260630": {
            "status": "RUNNING",
            "zone": "asia-northeast1-c",
            "creation_timestamp": "2026-06-30T11:00:00+00:00",
            "last_stop_timestamp": "",
            "boot_disk_name": "cefi-running-20260630",
            "labels": {},
        },
    }


def _disks() -> dict[str, dict[str, object]]:
    return {
        "cefi-binance-spot-20260601": {"size_gb": 50, "type": "pd-standard"},
        "tradfi-databento-recent": {"size_gb": 50, "type": "pd-standard"},
        "strategy-live-eth-20260601": {"size_gb": 50, "type": "pd-ssd"},
        "cefi-keepme-20260601": {"size_gb": 50, "type": "pd-standard"},
        "cefi-running-20260630": {"size_gb": 50, "type": "pd-standard"},
    }


def test_orphan_inventory_counts_and_verdicts() -> None:
    from deployment_api.routes._fleet_inventory import build_orphan_inventory

    inv = build_orphan_inventory(_orphan_details(), _disks(), _ORPHAN_NOW, 24.0)
    # 4 stopped (RUNNING excluded).
    assert inv.stopped_total == 4
    # Only the old ephemeral one is reapable.
    assert inv.reapable_total == 1
    by_name = {o.name: o for o in inv.orphans}
    assert "cefi-running-20260630" not in by_name  # RUNNING excluded
    assert by_name["cefi-binance-spot-20260601"].verdict == "reap"
    assert by_name["cefi-binance-spot-20260601"].reapable is True
    assert by_name["tradfi-databento-recent"].verdict == "keep_within_grace"
    assert by_name["strategy-live-eth-20260601"].verdict == "keep_not_ephemeral"
    assert by_name["cefi-keepme-20260601"].verdict == "keep_retained"


def test_orphan_inventory_disk_cost_rollup() -> None:
    from deployment_api.routes._fleet_inventory import build_orphan_inventory

    inv = build_orphan_inventory(_orphan_details(), _disks(), _ORPHAN_NOW, 24.0)
    # pd-standard 50GB = 50*0.052 = 2.6; pd-ssd 50GB = 50*0.221 = 11.05.
    by_name = {o.name: o for o in inv.orphans}
    assert by_name["cefi-binance-spot-20260601"].monthly_disk_usd == 2.6
    assert by_name["strategy-live-eth-20260601"].monthly_disk_usd == 11.05
    # idle = 3x 2.6 (standard) + 11.05 (ssd) = 18.85; reapable = just the one 2.6.
    assert inv.monthly_idle_usd == 18.85
    assert inv.monthly_reapable_usd == 2.6


def test_orphan_inventory_grace_widens_reapable() -> None:
    """A 0h grace makes the 'recent' stop reapable too (age-gate is the only blocker)."""
    from deployment_api.routes._fleet_inventory import build_orphan_inventory

    inv = build_orphan_inventory(_orphan_details(), _disks(), _ORPHAN_NOW, 0.0)
    assert inv.reapable_total == 2  # both ephemeral, neither keep-labelled


def test_orphan_inventory_empty_is_honest_zero() -> None:
    from deployment_api.routes._fleet_inventory import build_orphan_inventory

    inv = build_orphan_inventory({}, {}, _ORPHAN_NOW, 24.0)
    assert inv.stopped_total == 0
    assert inv.reapable_total == 0
    assert inv.monthly_idle_usd == 0.0
    assert inv.orphans == []


# ---------------------------------------------------------------------------
# Orphan / reap / delete routes
# ---------------------------------------------------------------------------


def test_orphans_route_mock_is_empty(client_fleet: TestClient) -> None:
    with patch(_PATCH_MOCK_MODE, return_value=True):
        resp = client_fleet.get("/api/fleet/orphans")
    assert resp.status_code == 200
    body = resp.json()
    assert body["stopped_total"] == 0
    assert body["orphans"] == []


def test_orphans_route_live_delegates(client_fleet: TestClient) -> None:
    # Route reads ``datetime.now(UTC)`` for its grace window (fleet.py:101); pin it
    # to the fixture's ``_ORPHAN_NOW`` so ``tradfi-databento-recent`` stays inside
    # the 24h grace (it's `stop_time` is 12h before _ORPHAN_NOW). Without this the
    # test-suite time-drifts as the wall clock moves past the fixture stop-times,
    # flipping formerly-within-grace VMs to reap.
    with (
        patch("deployment_api.routes.fleet._cfg") as mock_cfg,
        patch("deployment_api.routes.fleet.get_vm_instance_details", return_value=_orphan_details()),
        patch("deployment_api.routes.fleet.get_disk_details", return_value=_disks()),
        patch("deployment_api.routes.fleet.datetime") as mock_dt,
    ):
        mock_dt.now.return_value = _ORPHAN_NOW
        mock_cfg.is_mock_mode.return_value = False
        mock_cfg.gcp_project_id = "test-project"
        resp = client_fleet.get("/api/fleet/orphans?grace_hours=24")
    assert resp.status_code == 200
    body = resp.json()
    assert body["stopped_total"] == 4
    assert body["reapable_total"] == 1
    assert body["monthly_reapable_usd"] == 2.6


def test_reap_dry_run_lists_candidates_without_deleting(client_fleet: TestClient) -> None:
    # Pin ``datetime.now`` to _ORPHAN_NOW so the grace window matches the fixture
    # stop-times (see test_orphans_route_live_delegates rationale).
    with (
        patch("deployment_api.rbac.DISABLE_AUTH", True),
        patch("deployment_api.routes.fleet._cfg") as mock_cfg,
        patch("deployment_api.routes.fleet.get_vm_instance_details", return_value=_orphan_details()),
        patch("deployment_api.routes.fleet.get_disk_details", return_value=_disks()),
        patch("deployment_api.routes.fleet.delete_vm_instance") as mock_del,
        patch("deployment_api.routes.fleet.datetime") as mock_dt,
    ):
        mock_dt.now.return_value = _ORPHAN_NOW
        mock_cfg.is_mock_mode.return_value = False
        mock_cfg.gcp_project_id = "test-project"
        resp = client_fleet.post("/api/fleet/reap", json={"dry_run": True, "grace_hours": 24.0})
    assert resp.status_code == 200
    body = resp.json()
    assert body["dry_run"] is True
    assert body["candidate_total"] == 1
    assert body["reaped_total"] == 0
    mock_del.assert_not_called()


def test_reap_execute_deletes_only_reapable(client_fleet: TestClient) -> None:
    # Pin ``datetime.now`` to _ORPHAN_NOW so the grace window matches the fixture
    # stop-times (see test_orphans_route_live_delegates rationale).
    with (
        patch("deployment_api.rbac.DISABLE_AUTH", True),
        patch("deployment_api.routes.fleet._cfg") as mock_cfg,
        patch("deployment_api.routes.fleet.get_vm_instance_details", return_value=_orphan_details()),
        patch("deployment_api.routes.fleet.get_disk_details", return_value=_disks()),
        patch("deployment_api.routes.fleet.delete_vm_instance", return_value=True) as mock_del,
        patch("deployment_api.routes.fleet.datetime") as mock_dt,
    ):
        mock_dt.now.return_value = _ORPHAN_NOW
        mock_cfg.is_mock_mode.return_value = False
        mock_cfg.gcp_project_id = "test-project"
        resp = client_fleet.post("/api/fleet/reap", json={"dry_run": False, "grace_hours": 24.0})
    assert resp.status_code == 200
    body = resp.json()
    assert body["reaped_total"] == 1
    assert body["monthly_reclaimed_usd"] == 2.6
    # Only the single reapable candidate is deleted (not the running/live/keep/recent VMs).
    mock_del.assert_called_once_with("test-project", "cefi-binance-spot-20260601", "asia-northeast1-c")


def test_delete_instance_refuses_running(client_fleet: TestClient) -> None:
    with (
        patch("deployment_api.rbac.DISABLE_AUTH", True),
        patch("deployment_api.routes.fleet._cfg") as mock_cfg,
        patch("deployment_api.routes.fleet.get_vm_instance_details", return_value=_orphan_details()),
        patch("deployment_api.routes.fleet.delete_vm_instance") as mock_del,
    ):
        mock_cfg.is_mock_mode.return_value = False
        mock_cfg.gcp_project_id = "test-project"
        resp = client_fleet.delete("/api/fleet/instances/cefi-running-20260630?zone=asia-northeast1-c")
    assert resp.status_code == 409
    mock_del.assert_not_called()


def test_delete_instance_404_when_missing(client_fleet: TestClient) -> None:
    with (
        patch("deployment_api.rbac.DISABLE_AUTH", True),
        patch("deployment_api.routes.fleet._cfg") as mock_cfg,
        patch("deployment_api.routes.fleet.get_vm_instance_details", return_value=_orphan_details()),
    ):
        mock_cfg.is_mock_mode.return_value = False
        mock_cfg.gcp_project_id = "test-project"
        resp = client_fleet.delete("/api/fleet/instances/does-not-exist?zone=asia-northeast1-c")
    assert resp.status_code == 404


def test_delete_instance_deletes_stopped(client_fleet: TestClient) -> None:
    with (
        patch("deployment_api.rbac.DISABLE_AUTH", True),
        patch("deployment_api.routes.fleet._cfg") as mock_cfg,
        patch("deployment_api.routes.fleet.get_vm_instance_details", return_value=_orphan_details()),
        patch("deployment_api.routes.fleet.delete_vm_instance", return_value=True) as mock_del,
    ):
        mock_cfg.is_mock_mode.return_value = False
        mock_cfg.gcp_project_id = "test-project"
        resp = client_fleet.delete("/api/fleet/instances/cefi-binance-spot-20260601?zone=asia-northeast1-c")
    assert resp.status_code == 200
    assert resp.json()["deleted"] is True
    mock_del.assert_called_once_with("test-project", "cefi-binance-spot-20260601", "asia-northeast1-c")

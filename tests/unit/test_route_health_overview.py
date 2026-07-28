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


def test_verdict_derived_without_execution() -> None:
    """No execution info → verdict falls back to the freshness+backlog derivation."""
    from deployment_api.routes.health_consolidator import _verdict

    assert _verdict("ok", 0) == "produced"
    assert _verdict("ok", 3) == "producing"
    assert _verdict("critical", 5) == "stale_output"
    assert _verdict("degraded", 0) == "empty"
    assert _verdict("unknown", None) == "unknown"


def test_verdict_fired_but_empty_takes_precedence() -> None:
    from deployment_api.routes.health_consolidator import _verdict

    # A stale index that WOULD read as stale_output is reclassified when the run fired green.
    assert _verdict("critical", 5, fired_but_empty=True) == "fired_but_empty"


def test_is_fired_but_empty_recent_success_stale_index() -> None:
    """Execution succeeded within budget but the index is stale → fired-but-empty."""
    from datetime import timedelta

    from deployment_api.routes._cloud_run_executions import CloudRunExecutionStatus
    from deployment_api.routes.health_consolidator import _is_fired_but_empty

    exec_status = CloudRunExecutionStatus(
        job_name="j",
        status="succeeded",
        last_run_at=(_FIXED_NOW - timedelta(seconds=30)).isoformat(),
        exit_code=0,
        log_uri="",
        region="asia-northeast1",
    )
    assert _is_fired_but_empty(exec_status, index_age=90000.0, budget=86400, now=_FIXED_NOW) is True


def test_is_fired_but_empty_false_when_index_fresh() -> None:
    from datetime import timedelta

    from deployment_api.routes._cloud_run_executions import CloudRunExecutionStatus
    from deployment_api.routes.health_consolidator import _is_fired_but_empty

    exec_status = CloudRunExecutionStatus(
        job_name="j",
        status="succeeded",
        last_run_at=(_FIXED_NOW - timedelta(seconds=30)).isoformat(),
        exit_code=0,
        log_uri="",
        region="asia-northeast1",
    )
    # Index fresh → the run DID write; not empty.
    assert _is_fired_but_empty(exec_status, index_age=42.0, budget=86400, now=_FIXED_NOW) is False


def test_is_fired_but_empty_false_when_success_is_also_old() -> None:
    """A stale index whose last SUCCESS is also old = down/behind, not fired-but-empty."""
    from datetime import timedelta

    from deployment_api.routes._cloud_run_executions import CloudRunExecutionStatus
    from deployment_api.routes.health_consolidator import _is_fired_but_empty

    exec_status = CloudRunExecutionStatus(
        job_name="j",
        status="succeeded",
        last_run_at=(_FIXED_NOW - timedelta(seconds=200000)).isoformat(),
        exit_code=0,
        log_uri="",
        region="asia-northeast1",
    )
    assert _is_fired_but_empty(exec_status, index_age=90000.0, budget=86400, now=_FIXED_NOW) is False


def test_is_fired_but_empty_false_when_execution_failed_or_absent() -> None:
    from datetime import timedelta

    from deployment_api.routes._cloud_run_executions import CloudRunExecutionStatus
    from deployment_api.routes.health_consolidator import _is_fired_but_empty

    failed = CloudRunExecutionStatus(
        job_name="j",
        status="failed",
        last_run_at=(_FIXED_NOW - timedelta(seconds=30)).isoformat(),
        exit_code=1,
        log_uri="",
        region="asia-northeast1",
    )
    assert _is_fired_but_empty(failed, index_age=90000.0, budget=86400, now=_FIXED_NOW) is False
    assert _is_fired_but_empty(None, index_age=90000.0, budget=86400, now=_FIXED_NOW) is False


def test_authoritative_verdict_maps_self_reported_run() -> None:
    from deployment_api.routes.health_consolidator import _authoritative_verdict

    # produced → produced/producing per backlog.
    assert _authoritative_verdict("produced", "unknown", 0, 100) == "produced"
    assert _authoritative_verdict("produced", "unknown", 5, 100) == "producing"
    # failed → stale_output.
    assert _authoritative_verdict("failed", "producing", 0, 100) == "stale_output"
    # 'empty' on a GENUINELY EMPTY index (0 rows): backlog waiting → fired_but_empty; else idle → empty.
    assert _authoritative_verdict("empty", "producing", 5, 0) == "fired_but_empty"
    assert _authoritative_verdict("empty", "producing", 0, 0) == "empty"
    assert _authoritative_verdict("empty", "producing", 0, None) == "empty"
    # 'empty' but the index actually HOLDS ROWS = a no-op cycle on real data → defer to freshness.
    assert _authoritative_verdict("empty", "produced", 0, 5_000_000) == "produced"
    assert _authoritative_verdict("empty", "stale_output", 8, 27_000_000) == "stale_output"
    # Unknown / absent self-report → fall back to the freshness-derived verdict.
    assert _authoritative_verdict(None, "stale_output", 0, 0) == "stale_output"


def test_read_latest_run_parses_and_degrades() -> None:
    from unittest.mock import MagicMock

    from deployment_api.routes.health_consolidator import _read_latest_run

    client = MagicMock()
    client.download_bytes.return_value = b'{"verdict": "produced", "rows_added": 5}'
    parsed = _read_latest_run(client, "bkt")
    assert parsed is not None and parsed["verdict"] == "produced"
    # Missing blob → None (honest "not reporting"), never a fabricated summary.
    client.download_bytes.side_effect = FileNotFoundError("no latest.json")
    assert _read_latest_run(client, "bkt") is None
    # Malformed JSON → None.
    client.download_bytes.side_effect = None
    client.download_bytes.return_value = b"not json{"
    assert _read_latest_run(client, "bkt") is None


def test_audit_fields_reads_phantom_and_reprobe() -> None:
    """A market-data/instruments bucket surfaces its last phantom audit + empty re-probe summaries."""
    from unittest.mock import MagicMock

    from deployment_api.routes.health_consolidator import _audit_fields

    client = MagicMock()
    client.get_blob_metadata.return_value = object()  # both summaries present

    def _dl(_bucket: str, blob: str) -> bytes:
        if blob.endswith("phantom_audit_latest.json"):
            return (
                b'{"generated_at": "2026-07-13T08:00:00+00:00", "phantom_count": 3, "triage_jsonl": "gs://b/t.jsonl"}'
            )
        if blob.endswith("reprobe_audit_latest.json"):
            return b'{"generated_at": "2026-07-13T09:00:00+00:00", "new_empties": 12, "disagreements": 2, "reclassified": 1}'
        raise FileNotFoundError(blob)

    client.download_bytes.side_effect = _dl
    fields = _audit_fields(client, "market-data-tick-cefi-prd", "market-data")
    assert fields["phantom_count"] == 3
    assert fields["phantom_audit_at"] == "2026-07-13T08:00:00+00:00"
    assert (fields["phantom_triage_link"] or "").endswith("t.jsonl")
    assert fields["reprobe_new_empties"] == 12
    assert fields["reprobe_disagreements"] == 2
    assert fields["reprobe_reclassified"] == 1


def test_audit_fields_gated_to_audit_bearing_kinds() -> None:
    """features/execution/flat consolidators pay no read — every field None, no GCS call."""
    from unittest.mock import MagicMock

    from deployment_api.routes.health_consolidator import _audit_fields

    client = MagicMock()
    fields = _audit_fields(client, "bkt", "features-volatility")
    assert all(v is None for v in fields.values())
    client.get_blob_metadata.assert_not_called()


def test_audit_fields_absent_summaries_are_none() -> None:
    """Missing summary objects → all None (honest 'no audit yet'), never a fabricated clean state."""
    from unittest.mock import MagicMock

    from deployment_api.routes.health_consolidator import _audit_fields

    client = MagicMock()
    client.get_blob_metadata.return_value = None  # both summaries absent
    fields = _audit_fields(client, "bkt", "instruments")
    assert all(v is None for v in fields.values())


def test_mock_estate_carries_reporting_and_dead_consolidators() -> None:
    """The mock estate distinguishes live (reporting latest.json) from dead (not reporting)."""
    from deployment_api.routes.health_consolidator import _mock_response

    resp = _mock_response(_FIXED_NOW)
    by_cat = {c.category: c for c in resp.consolidators}
    # A live consolidator self-reports.
    assert by_cat["market-data-cefi"].run_reporting is True
    assert by_cat["market-data-cefi"].run_verdict is not None
    # The dead one has no latest.json → not reporting, honest empty state.
    assert by_cat["strategy"].run_reporting is False
    assert by_cat["strategy"].run_verdict is None


def test_entry_budget_reads_catalog_then_falls_back() -> None:
    from deployment_api.routes.health_consolidator import _entry_budget

    # Catalog carries the cadence-matched budget → used verbatim.
    assert _entry_budget({"asset_group": "defi", "staleness_budget_seconds": "120"}, 999) == 120
    assert _entry_budget({"asset_group": "cefi", "staleness_budget_seconds": "86400"}, 999) == 86400
    # Missing/blank → legacy per-AG override (cefi=86400, sports=1800) then the passed default.
    assert _entry_budget({"asset_group": "cefi"}, 120) == 86400
    assert _entry_budget({"asset_group": "sports"}, 120) == 1800
    assert _entry_budget({"asset_group": "tradfi"}, 120) == 120
    # Garbage budget → falls back, never raises.
    assert _entry_budget({"asset_group": "sports", "staleness_budget_seconds": "oops"}, 120) == 1800


def test_catalog_live_market_data_is_120_everything_else_86400() -> None:
    """The generated catalog must budget live market-data ticks at 120s and all else at 86400s.

    market-data-defi is the one documented exception: its date-range-chunked incremental merge
    (the OOM fix) runs ~24-30 min against the ~28M-row DeFi canonical, so it gets a 3600s
    merge-cadence budget instead of the 120s live tick (see _SLOW_MERGE_MARKET_DATA_BUDGET_SEC in
    scripts/gen_consolidator_catalog.py — verified 2026-07-16, index age ~1579s falsely tripped
    the 120s budget every normal merge cycle).
    """
    from deployment_api.routes.health_consolidator import _CATALOG

    if not _CATALOG:  # catalog is a generated artifact; skip if absent in this checkout
        pytest.skip("consolidator catalog not present")
    live = {"defi", "tradfi", "sports", "prediction"}
    slow_merge_exceptions = {("market-data", "defi"): 3600}
    for entry in _CATALOG:
        budget = int(entry["staleness_budget_seconds"] or 0)
        key = (entry["kind"], entry["asset_group"])
        if key in slow_merge_exceptions:
            assert budget == slow_merge_exceptions[key], (
                f"{entry['category']} should be a {slow_merge_exceptions[key]}s merge-cadence budget"
            )
        elif entry["kind"] == "market-data" and entry["asset_group"] in live:
            assert budget == 120, f"{entry['category']} should be a 120s live tick"
        else:
            assert budget == 86400, f"{entry['category']} should be an 86400s batch budget"


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
    # The full estate carries a fired-but-empty consolidator (ran green, index stale) with its
    # execution truth attached — the data-correctness signal a liveness-only view would miss.
    verdicts = {c["category"]: c for c in body["consolidators"]}
    fired = verdicts["features-onchain-defi"]
    assert fired["verdict"] == "fired_but_empty"
    assert fired["execution_status"] == "succeeded"
    assert fired["execution_exit_code"] == 0
    # A consolidator with a backlog carries the oldest-unmerged-shard age (merge-stuck-for signal).
    assert fired["oldest_pending_shard_age_seconds"] is not None
    # A caught-up consolidator (no backlog) has no oldest-pending age.
    assert verdicts["instruments-cefi"]["oldest_pending_shard_age_seconds"] is None


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
    from unified_trading_library import PerVmShardBacklog

    from deployment_api.routes.health_consolidator import _ag_health

    with (
        patch(f"{_HC}.resolve_bucket_name", return_value="market-data-tick-cefi"),
        patch(f"{_HC}.get_storage_client", return_value=object()),
        patch(f"{_HC}.consolidated_blob_age_sec", return_value=30.0),
        patch(f"{_HC}.per_vm_shard_backlog", return_value=PerVmShardBacklog(2, 6, None)) as backlog,
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
    assert _budget_for("sports", 120) == 1800  # ~11-min consolidator cadence → cadence-matched override
    assert _budget_for("defi", 120) == 120  # ~per-minute consolidator → global default
    assert _budget_for("tradfi", 999) == 999  # default flows through unchanged


# ---------------------------------------------------------------------------
# health_consolidator — stale-while-revalidate snapshot cache
# ---------------------------------------------------------------------------


@pytest.fixture()
def _reset_consolidator_cache():
    """Isolate each cache test — clear the module-global snapshot before + after."""
    from deployment_api.routes import health_consolidator as hc

    hc._consolidator_health_cache = None
    hc._consolidator_health_refreshing = False
    yield
    hc._consolidator_health_cache = None
    hc._consolidator_health_refreshing = False


def _fake_response():
    from deployment_api.routes.health_consolidator import build_consolidator_health

    return build_consolidator_health([], _FIXED_NOW, consolidators=[])


@pytest.mark.usefixtures("_reset_consolidator_cache")
def test_consolidator_health_cold_call_computes_once_and_caches() -> None:
    """Cold cache → ONE synchronous walk; an immediate second call is served from cache."""
    from deployment_api.routes import health_consolidator as hc

    with (
        patch(_CONSOLIDATOR_MOCK, return_value=False),
        patch(f"{_HC}._compute_consolidator_health", return_value=_fake_response()) as compute,
    ):
        first = hc.get_consolidator_health()
        second = hc.get_consolidator_health()

    compute.assert_called_once()
    assert first is second  # same snapshot object — no recompute inside TTL


@pytest.mark.usefixtures("_reset_consolidator_cache")
def test_consolidator_health_stale_serves_snapshot_and_kicks_one_refresh() -> None:
    """Stale cache → the old snapshot returns instantly + exactly ONE background refresh."""
    from deployment_api.routes import health_consolidator as hc

    stale_snapshot = _fake_response()
    hc._consolidator_health_cache = (-999_999.0, stale_snapshot)  # far past → stale

    with (
        patch(_CONSOLIDATOR_MOCK, return_value=False),
        patch.object(hc._consolidator_health_refresh_pool, "submit") as submit,
    ):
        first = hc.get_consolidator_health()
        second = hc.get_consolidator_health()  # refresh already in flight → no second submit

    assert first is stale_snapshot
    assert second is stale_snapshot
    submit.assert_called_once_with(hc._refresh_consolidator_health)


@pytest.mark.usefixtures("_reset_consolidator_cache")
def test_consolidator_health_failed_refresh_keeps_stale_snapshot() -> None:
    """A failing background refresh never poisons the cache and clears the in-flight flag."""
    from deployment_api.routes import health_consolidator as hc

    stale_snapshot = _fake_response()
    hc._consolidator_health_cache = (-999_999.0, stale_snapshot)
    hc._consolidator_health_refreshing = True

    with patch(f"{_HC}._compute_consolidator_health", side_effect=OSError("gcs down")):
        hc._refresh_consolidator_health()

    assert hc._consolidator_health_cache == (-999_999.0, stale_snapshot)
    assert hc._consolidator_health_refreshing is False


@pytest.mark.usefixtures("_reset_consolidator_cache")
def test_consolidator_health_mock_mode_bypasses_cache() -> None:
    """Mock mode stays deterministic — never reads or writes the snapshot cache."""
    from deployment_api.routes import health_consolidator as hc

    with patch(_CONSOLIDATOR_MOCK, return_value=True):
        resp = hc.get_consolidator_health()

    assert hc._consolidator_health_cache is None
    assert resp.asset_groups  # the mock estate renders

"""Regression guards for two data-status fixes (2026-06-15):

1. **Beta-aware rollup blob path** (503 fix): in CF-20 beta-manifest mode the
   rollup is computed from the projected v9 index, so it must live in a distinct
   ``{service}/{kind}.beta.json.gz`` blob — never mixing with the live rollup.
   The worker writes it and the service reads it, so the all-asset-group beta
   view is served from cache instead of live-computing every request (HTTP 503).

2. **CLI ``--config-dir`` resolution** (P2 / 500 fix): the in-image
   ``python -m deployment_service data-status`` subprocess could not find its
   ``configs/`` dir (dropped by ``pip install --no-deps``), so it 500'd with
   "Could not find configs directory". ``_build_cli_cmd`` now points it at the
   bundled ``pm-configs`` mirror via ``--config-dir`` (a GROUP-level option that
   must precede the ``data-status`` subcommand).
"""

from __future__ import annotations

from pathlib import Path

import deployment_api.services.data_status.cli as cli_mod
import deployment_api.services.data_status.rollup_cache as rollup_cache
import deployment_api.services.data_status_service as dss_mod
import deployment_api.services.manifest_source as manifest_source


def test_rollup_blob_path_live_vs_beta(monkeypatch) -> None:
    monkeypatch.setattr(manifest_source, "is_beta_mode", lambda: False)
    assert rollup_cache.rollup_blob_path("instruments-service", "full") == "instruments-service/full.json.gz"
    assert rollup_cache.rollup_blob_path("instruments-service", "coverage") == "instruments-service/coverage.json.gz"

    monkeypatch.setattr(manifest_source, "is_beta_mode", lambda: True)
    assert rollup_cache.rollup_blob_path("instruments-service", "full") == "instruments-service/full.beta.json.gz"
    assert (
        rollup_cache.rollup_blob_path("instruments-service", "coverage") == "instruments-service/coverage.beta.json.gz"
    )


def test_resolve_cli_config_dir_finds_pm_configs() -> None:
    # Resolves the bundled pm-configs mirror — which exists in this repo (and in
    # the deployment-api image at /app/pm-configs, COPY'd by the Dockerfile).
    resolved = cli_mod._resolve_cli_config_dir()
    assert resolved is not None
    assert Path(resolved).name == "pm-configs"
    assert Path(resolved).is_dir()


def test_build_cli_cmd_passes_config_dir_before_subcommand() -> None:
    svc = dss_mod.DataStatusService()
    cmd = svc._build_cli_cmd(  # pyright: ignore[reportPrivateUsage]
        "instruments-service", "2018-01-01", "2026-06-15", None, None, False, False, False, False, False, "batch"
    )
    # --config-dir is a GROUP option → must appear BEFORE the "data-status" subcommand.
    assert "--config-dir" in cmd
    assert cmd.index("--config-dir") < cmd.index("data-status")
    # The value is the resolved pm-configs mirror, and precedes the subcommand args.
    assert Path(cmd[cmd.index("--config-dir") + 1]).name == "pm-configs"
    assert cmd.index("--config-dir") < cmd.index("-s")


def test_slice_rollup_emits_r7_overall_capture_attempt() -> None:
    """The rollup-served path must carry the R7 capture/attempt split, not just
    overall_completion_pct — else the headline loses the split when a fresh rollup
    exists (regression guard for slice_rollup_to_window dropping the new fields)."""
    rollup: dict = {
        "service": "instruments-service",
        "asset_groups": {
            "PREDICTION": {
                "attempt_coverage_pct": 100.0,
                "venues": {
                    "POLYMARKET": {
                        "dates_found_list": ["2025-03-14", "2025-03-15"],
                        "missing_dates": ["2025-03-16"],
                        "dates_expected_list": ["2025-03-14", "2025-03-15", "2025-03-16"],
                    }
                },
            }
        },
    }
    out = rollup_cache.slice_rollup_to_window(rollup, "2025-03-14", "2025-03-16", None)
    assert "overall_capture_coverage_pct" in out
    assert "overall_attempt_coverage_pct" in out
    # capture = 2 found / 3 expected shards == overall_completion_pct; attempt == 100 (only cat).
    assert out["overall_capture_coverage_pct"] == out["overall_completion_pct"]
    assert out["overall_attempt_coverage_pct"] == 100.0


def test_beta_eligible_filters_to_projected_services() -> None:
    """In beta mode the rollup worker must sweep ONLY services with a v9 projected
    index (instruments-service) — else the loud-failing beta read on a non-projected
    service (mtds/features) crashes the whole job (regression guard for the cloud
    beta job exit-1 incident, 2026-06-15)."""
    assert set(manifest_source.BETA_ELIGIBLE_SERVICES) == {"instruments-service"}
    full = [
        "instruments-service",
        "market-tick-data-service",
        "features-delta-one-service",
        "strategy-service",
    ]
    assert manifest_source.beta_eligible(full) == ["instruments-service"]
    assert manifest_source.beta_eligible(["market-tick-data-service"]) == []


def test_beta_rollup_served_despite_staleness() -> None:
    """The beta rollup (static projected-v9 index) must serve regardless of age — the
    30-min staleness gate must NOT force a fall-through (which would 503 the all-AG
    beta view). Regression guard for the beta staleness exemption (2026-06-15)."""
    import gzip
    import json as _json
    from unittest.mock import MagicMock, patch

    import pandas as pd

    import deployment_api.services.data_status_service as dss_mod

    client = MagicMock()
    client.blob_exists.return_value = True
    meta = MagicMock()
    meta.updated = pd.Timestamp("2020-01-01", tz="UTC")  # ~years stale
    client.get_blob_metadata.return_value = meta
    payload = {"asset_groups": {}, "beta": True}
    client.download_bytes.return_value = gzip.compress(_json.dumps(payload).encode())
    dss_mod._ROLLUP_CACHE.clear()  # pyright: ignore[reportPrivateUsage]
    with (
        patch.object(dss_mod, "get_storage_client", return_value=client),
        patch.object(dss_mod, "_is_service_beta", return_value=True),
    ):
        out = dss_mod._read_rollup_if_fresh("instruments-service")  # pyright: ignore[reportPrivateUsage]
    assert out == payload  # served despite staleness because beta-eligible (is_service_beta)


def test_live_rollup_respects_staleness() -> None:
    """A service reading its LIVE blob keeps the staleness gate (regression guard — the beta
    exemption must not leak to live blobs). Covers BOTH live mode AND a non-beta-eligible
    service in beta mode: both resolve ``is_service_beta``→False, so both must fall through
    on a stale blob (the market-tick-data-service path)."""
    from unittest.mock import MagicMock, patch

    import pandas as pd

    import deployment_api.services.data_status_service as dss_mod

    client = MagicMock()
    client.blob_exists.return_value = True
    meta = MagicMock()
    meta.updated = pd.Timestamp("2020-01-01", tz="UTC")  # stale
    client.get_blob_metadata.return_value = meta
    dss_mod._ROLLUP_CACHE.clear()  # pyright: ignore[reportPrivateUsage]
    with (
        patch.object(dss_mod, "get_storage_client", return_value=client),
        patch.object(dss_mod, "_is_service_beta", return_value=False),
    ):
        out = dss_mod._read_rollup_if_fresh("market-tick-data-service")  # pyright: ignore[reportPrivateUsage]
    assert out is None  # stale + live blob → fall through


def test_rollup_endpoint_runs_worker_in_service(monkeypatch) -> None:
    """POST /api/data-status/rollup-run runs the worker's run_rollup IN the gen1 service
    (the gen2 Cloud Run Job crashes natively — R7 follow-up #4). Asserts the handler
    dispatches to run_rollup with the default services + restores _PROCESS_POOL_DISABLED."""
    import asyncio

    import deployment_api.scripts.data_status_rollup_worker as worker
    import deployment_api.services.data_status_service as dss_mod
    from deployment_api.routes.data_status import _rollup

    captured: dict[str, object] = {}

    def _fake_run_rollup(project: str, bucket: str, services: list[str]) -> int:
        captured["project"] = project
        captured["bucket"] = bucket
        captured["services"] = list(services)
        return 0

    monkeypatch.setattr(worker, "run_rollup", _fake_run_rollup)
    # Non-beta deployment: only the LIVE phase runs, over ALL default services.
    monkeypatch.setattr(manifest_source, "DATA_STATUS_BETA_MANIFEST_BLOB", "")
    _before = dss_mod._PROCESS_POOL_DISABLED  # pyright: ignore[reportPrivateUsage]
    out = asyncio.run(_rollup.run_data_status_rollup(services=None))

    assert out["status"] == "ok"
    assert out["exit_code_live"] == 0
    assert out["live_services"] == list(worker.DEFAULT_SERVICES)
    assert out["beta_services"] == []  # no beta phase when not in beta mode
    assert captured["services"] == list(worker.DEFAULT_SERVICES)
    assert "data-status-rollups" in str(captured["bucket"])
    # the service-wide pool flag must be restored after the call
    assert _before == dss_mod._PROCESS_POOL_DISABLED  # pyright: ignore[reportPrivateUsage]


def test_rollup_endpoint_two_phase_live_then_beta(monkeypatch) -> None:
    """In beta mode the endpoint runs BOTH phases (2026-06-16 503 fix): phase 1 = LIVE for
    EVERY service with beta forced OFF (→ full.json.gz, so mtds/features stay fresh), phase
    2 = BETA for the beta-eligible service(s) with beta forced ON (→ full.beta.json.gz).
    Captures is_beta_mode() at each run_rollup call to prove the per-phase toggle, and that
    DATA_STATUS_BETA_MANIFEST_BLOB is restored afterwards."""
    import asyncio

    import deployment_api.scripts.data_status_rollup_worker as worker
    from deployment_api.routes.data_status import _rollup

    calls: list[tuple[list[str], bool]] = []

    def _fake_run_rollup(project: str, bucket: str, services: list[str]) -> int:
        calls.append((list(services), manifest_source.is_beta_mode()))
        return 0

    monkeypatch.setattr(worker, "run_rollup", _fake_run_rollup)
    monkeypatch.setattr(
        manifest_source, "DATA_STATUS_BETA_MANIFEST_BLOB", "_index/audit/projected_index_{asset_group}.parquet"
    )

    out = asyncio.run(_rollup.run_data_status_rollup(services=None))

    assert out["status"] == "ok"
    # Phase 1: LIVE, all services, beta OFF.
    assert calls[0][0] == list(worker.DEFAULT_SERVICES)
    assert calls[0][1] is False
    # Phase 2: BETA, beta-eligible only, beta ON.
    assert calls[1][0] == ["instruments-service"]
    assert calls[1][1] is True
    assert out["beta_services"] == ["instruments-service"]
    # The global beta flag is restored to the configured value after the run.
    assert manifest_source.DATA_STATUS_BETA_MANIFEST_BLOB == "_index/audit/projected_index_{asset_group}.parquet"


def test_rollup_blob_path_non_beta_eligible_stays_live_in_beta_mode(monkeypatch) -> None:
    """Beta is PER-SERVICE: a non-beta-eligible service (market-tick-data-service) keeps its
    LIVE blob even in beta mode — namespacing it to a non-existent .beta blob is what 503'd
    the mtds data-status page (2026-06-16). The eligible service still flips to .beta."""
    monkeypatch.setattr(manifest_source, "is_beta_mode", lambda: True)
    # Eligible → beta-namespaced.
    assert rollup_cache.rollup_blob_path("instruments-service", "full") == "instruments-service/full.beta.json.gz"
    # NON-eligible → stays live even in beta mode.
    assert rollup_cache.rollup_blob_path("market-tick-data-service", "full") == "market-tick-data-service/full.json.gz"
    assert (
        rollup_cache.rollup_blob_path("features-delta-one-service", "coverage")
        == "features-delta-one-service/coverage.json.gz"
    )


def test_is_service_beta_semantics(monkeypatch) -> None:
    """is_service_beta = is_beta_mode AND beta-eligible (the per-service beta gate)."""
    monkeypatch.setattr(manifest_source, "is_beta_mode", lambda: False)
    assert manifest_source.is_service_beta("instruments-service") is False  # not in beta mode
    monkeypatch.setattr(manifest_source, "is_beta_mode", lambda: True)
    assert manifest_source.is_service_beta("instruments-service") is True  # eligible + beta
    assert manifest_source.is_service_beta("market-tick-data-service") is False  # beta but not eligible


def test_thread_pool_disabled_forces_serial(monkeypatch) -> None:
    """_THREAD_POOL_DISABLED=True bypasses the ThreadPoolExecutor (fully serial per-AG
    compute) — running all AGs in parallel threads OOMs the 8 GiB service (R7 follow-up
    #4); the in-service rollup endpoint sets this so the compute fits. The API path keeps
    it False (fast responses)."""
    from unittest.mock import MagicMock, patch

    import deployment_api.services.data_status_service as dss_mod
    from deployment_api.services.data_status import manifest as manifest_mod

    assert dss_mod._THREAD_POOL_DISABLED is False  # pyright: ignore[reportPrivateUsage]
    svc = dss_mod.DataStatusService(project_id="test")
    monkeypatch.setattr(dss_mod, "_PROCESS_POOL_DISABLED", True)
    monkeypatch.setattr(dss_mod, "_THREAD_POOL_DISABLED", True)
    tpe = MagicMock(side_effect=AssertionError("ThreadPoolExecutor used despite _THREAD_POOL_DISABLED"))
    with (
        patch.object(manifest_mod, "ThreadPoolExecutor", tpe),
        patch.object(type(svc), "_build_manifest_category", return_value={"ok": True}),
    ):
        out = svc._dispatch_category_builds(  # pyright: ignore[reportPrivateUsage]
            ["CEFI", "DEFI"],
            "instruments-service",
            "2025-01-01",
            "2025-01-02",
            ["2025-01-01", "2025-01-02"],
            2,
            dss_mod.VenueMapping(),
            row_filters=None,
            cloud="gcp",
            pipeline_modes=None,
        )
    assert set(out.keys()) == {"CEFI", "DEFI"}
    tpe.assert_not_called()

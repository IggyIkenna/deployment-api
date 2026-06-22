"""Regression guards for data-status rollup + CLI fixes:

1. **Rollup blob path** (503 fix): the all-asset-group view is served from the
   precomputed ``{service}/{kind}.json.gz`` rollup blob instead of live-computing
   every request (HTTP 503).

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


def test_rollup_blob_path_live() -> None:
    assert rollup_cache.rollup_blob_path("instruments-service", "full") == "instruments-service/full.json.gz"
    assert rollup_cache.rollup_blob_path("instruments-service", "coverage") == "instruments-service/coverage.json.gz"


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


def test_live_rollup_respects_staleness() -> None:
    """A service reading its rollup blob respects the staleness gate — a frozen blob
    (e.g. the worker stalled) must not be served indefinitely; it falls through to the
    on-demand compute."""
    from unittest.mock import MagicMock, patch

    import pandas as pd

    import deployment_api.services.data_status_service as dss_mod

    client = MagicMock()
    client.blob_exists.return_value = True
    meta = MagicMock()
    meta.updated = pd.Timestamp("2020-01-01", tz="UTC")  # stale
    client.get_blob_metadata.return_value = meta
    dss_mod._ROLLUP_CACHE.clear()  # pyright: ignore[reportPrivateUsage]
    with patch.object(dss_mod, "get_storage_client", return_value=client):
        out = dss_mod._read_rollup_if_fresh("market-tick-data-service")  # pyright: ignore[reportPrivateUsage]
    assert out is None  # stale blob → fall through


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
    _before = dss_mod._PROCESS_POOL_DISABLED  # pyright: ignore[reportPrivateUsage]
    out = asyncio.run(_rollup.run_data_status_rollup(services=None))

    assert out["status"] == "ok"
    assert out["exit_code_live"] == 0
    assert out["live_services"] == list(worker.DEFAULT_SERVICES)
    assert captured["services"] == list(worker.DEFAULT_SERVICES)
    assert "data-status-rollups" in str(captured["bucket"])
    # the service-wide pool flag must be restored after the call
    assert _before == dss_mod._PROCESS_POOL_DISABLED  # pyright: ignore[reportPrivateUsage]


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

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

"""Cat D.3 — tarball-from-local mode chains with ``&&``, not ``;``.

Plan: ``data_status_comprehensive_test_coverage_2026_05_07.plan.md`` Phase 3.

Reference contract (deploy_missing.py § ``tarball-from-local``): when an
operator picks the tarball-from-local mode in the UI, the API emits a
combo command:

    bash deployment-service/scripts/vm/create-code-tarballs.sh --all \\
      && bash deployment-service/scripts/vm/launch-mtds-backfill-vm.sh ...

The ``&&`` is load-bearing: if the tarball build fails, the launcher
MUST NOT run — otherwise the new VM boots stale code while the operator
thinks they're testing fresh local edits. A regression to ``;`` (or no
chain) would let the launcher run regardless, silently rolling back to
the prior tarball.

This test pins the chain operator. Catches both the obvious
``&& → ;`` swap and subtler regressions like a missing ``&&`` between
the two bash calls.
"""

from __future__ import annotations

import re

import pytest

from deployment_api.services.deploy_missing import (
    DeployMissingError,
    build_deploy_missing_preview,
)

_BASE_ROW_KEY: dict[str, str] = {
    "venue": "BINANCE-FUTURES",
    "data_type": "trades",
    "instrument_type": "PERPETUAL",
    "instrument_id": "btcusdt",
    "day": "2024-03-04",
}


class TestTarballFromLocalModeChainsCorrectly:
    def test_tarball_from_local_command_chains_with_double_ampersand(self) -> None:
        preview = build_deploy_missing_preview(
            service="market-tick-data-service",
            asset_group="cefi",
            row_key=_BASE_ROW_KEY,
            mode="tarball-from-local",
        )
        # Required structure: refresh && launcher
        assert "create-code-tarballs.sh" in preview.command
        assert "launch-mtds-backfill-vm.sh" in preview.command
        # CRITICAL: chained with ``&&`` so a refresh failure aborts the launch.
        assert " && " in preview.command, f"tarball-from-local mode lost its && chain — command={preview.command!r}"
        # Negative pin — never the unsafe ``;`` separator (which would
        # run the launcher even after a tarball build failure).
        assert ";" not in preview.command, (
            f"tarball-from-local must NOT use ';' — would run launcher on refresh failure. command={preview.command!r}"
        )

    def test_refresh_step_runs_before_launcher(self) -> None:
        """The refresh script must appear BEFORE the launcher — not after,
        not interleaved. ``create-code-tarballs.sh`` returns first, then
        ``launch-*.sh`` is invoked.
        """
        preview = build_deploy_missing_preview(
            service="market-tick-data-service",
            asset_group="cefi",
            row_key=_BASE_ROW_KEY,
            mode="tarball-from-local",
        )
        refresh_idx = preview.command.find("create-code-tarballs.sh")
        launcher_idx = preview.command.find("launch-mtds-backfill-vm.sh")
        assert refresh_idx >= 0
        assert launcher_idx >= 0
        assert refresh_idx < launcher_idx, (
            f"refresh must run before launcher; got refresh@{refresh_idx} > "
            f"launcher@{launcher_idx} in {preview.command!r}"
        )

    def test_tarball_from_local_emits_local_only_warning(self) -> None:
        """The mode emits a strong warning the UI can render so operators
        don't paste this command into a remote shell."""
        preview = build_deploy_missing_preview(
            service="market-tick-data-service",
            asset_group="cefi",
            row_key=_BASE_ROW_KEY,
            mode="tarball-from-local",
        )
        joined = " ".join(preview.warnings)
        assert "LOCAL-ONLY" in joined, (
            "tarball-from-local mode must emit a LOCAL-ONLY warning so operators "
            "don't run the command from a remote shell / Cloud Run pod."
        )
        assert "UNCOMMITTED" in joined, "tarball-from-local must warn about uncommitted-changes capture."

    def test_preview_mode_does_not_emit_chain(self) -> None:
        """Default ``preview`` mode is a single launcher invocation — no
        ``&&`` chain. The operator runs ``create-code-tarballs.sh``
        manually if needed (a note hints at this)."""
        preview = build_deploy_missing_preview(
            service="market-tick-data-service",
            asset_group="cefi",
            row_key=_BASE_ROW_KEY,
            mode="preview",
        )
        assert "&&" not in preview.command
        assert preview.warnings == []
        # The preview-mode notes do mention the manual refresh option as a hint,
        # but the *command* itself is just the launcher.
        assert preview.command.startswith("bash ")
        assert "launch-mtds-backfill-vm.sh" in preview.command

    def test_unsupported_mode_raises(self) -> None:
        with pytest.raises(DeployMissingError, match="Unsupported deploy-missing mode"):
            build_deploy_missing_preview(
                service="market-tick-data-service",
                asset_group="cefi",
                row_key=_BASE_ROW_KEY,
                mode="auto-launch",  # not yet supported per Phase 3
            )


class TestShardKeyShapeAcrossModes:
    """The ``shard_key`` payload is identical across modes — only the
    surrounding command shape differs. This pins that the 6-field
    pipe form survives a mode toggle in the UI."""

    def test_shard_key_identical_for_preview_and_tarball_modes(self) -> None:
        preview_mode = build_deploy_missing_preview(
            service="market-tick-data-service",
            asset_group="cefi",
            row_key=_BASE_ROW_KEY,
            mode="preview",
        )
        local_mode = build_deploy_missing_preview(
            service="market-tick-data-service",
            asset_group="cefi",
            row_key=_BASE_ROW_KEY,
            mode="tarball-from-local",
        )
        assert preview_mode.shard_key == local_mode.shard_key

    def test_shard_key_six_pipe_fields(self) -> None:
        """Pin the 6-field shape: asset_group | venue | data_type |
        instrument_type | instrument_or_root | day."""
        preview = build_deploy_missing_preview(
            service="market-tick-data-service",
            asset_group="cefi",
            row_key=_BASE_ROW_KEY,
            mode="preview",
        )
        # 6 fields → 5 pipes
        assert preview.shard_key.count("|") == 5, (
            f"shard_key shape regressed to {preview.shard_key.count('|') + 1} fields; "
            f"expected 6 (asset_group|venue|data_type|instrument_type|inst|day). "
            f"shard_key={preview.shard_key!r}"
        )
        # Round-trip extract per the canonical decomposer's regex shape.
        assert re.match(r"^[a-z]+\|[^|]+\|[^|]+\|[^|]*\|[^|]+\|\d{4}-\d{2}-\d{2}$", preview.shard_key)

"""Tests for the Deploy-Missing surgical-recovery preview helper.

Plan: ``data_status_drilldown_shard_atom_alignment_2026_05_07.md`` Phase 3.
"""

from __future__ import annotations

import pytest

from deployment_api.services.deploy_missing import (
    DeployMissingError,
    build_deploy_missing_preview,
    list_supported_services,
)


class TestBuildDeployMissingPreview:
    def test_per_instrument_cefi_shard(self) -> None:
        preview = build_deploy_missing_preview(
            service="market-tick-data-service",
            asset_group="cefi",
            row_key={
                "venue": "BINANCE-FUTURES",
                "data_type": "trades",
                "instrument_type": "PERPETUAL",
                "instrument_id": "btcusdt",
                "day": "2024-03-04",
            },
        )
        assert preview.shard_key == "cefi|BINANCE-FUTURES|trades|PERPETUAL|btcusdt|2024-03-04"
        # The launcher script + --shard-key combine into the operator-runnable command.
        assert "launch-mtds-backfill-vm.sh" in preview.command
        assert "--shard-key=" in preview.command
        # 5th field is the per-instrument id (not bundled), so the note
        # explains that.
        assert any("Per-instrument" in n for n in preview.notes)

    def test_bundled_options_chain_routes_via_root(self) -> None:
        preview = build_deploy_missing_preview(
            service="market-tick-data-service",
            asset_group="tradfi",
            row_key={
                "venue": "CME",
                "data_type": "options_chain",
                "instrument_type": "options_chain",
                "root": "ES.OPT",
                "day": "2024-01-15",
            },
        )
        assert preview.shard_key == "tradfi|CME|options_chain|options_chain|ES.OPT|2024-01-15"
        # Bundled-data_type note explains the 5th field is a cluster root.
        assert any("Bundled" in n and "ROOT" in n for n in preview.notes)

    def test_defi_shard_with_empty_instrument_type(self) -> None:
        """DeFi protocol shards leave instrument_type empty -- the
        decomposer on the MTDS side skips empty fields, so the preview
        emits the empty position correctly."""
        preview = build_deploy_missing_preview(
            service="market-tick-data-service",
            asset_group="defi",
            row_key={
                "venue": "AAVEV3-ARBITRUM",
                "data_type": "lending_indices",
                "instrument_type": "",
                "instrument_id": "USDC",
                "day": "2024-03-04",
            },
        )
        assert preview.shard_key == "defi|AAVEV3-ARBITRUM|lending_indices||USDC|2024-03-04"

    def test_date_alias_accepted_for_day(self) -> None:
        preview = build_deploy_missing_preview(
            service="market-tick-data-service",
            asset_group="cefi",
            row_key={
                "venue": "BYBIT",
                "data_type": "trades",
                "instrument_id": "btcusdt",
                "date": "2024-03-04",  # ``date`` instead of ``day``.
            },
        )
        assert preview.shard_key.endswith("|2024-03-04")

    def test_unknown_service_raises(self) -> None:
        with pytest.raises(DeployMissingError, match="No launcher script"):
            build_deploy_missing_preview(
                service="some-unknown-service",
                asset_group="cefi",
                row_key={"venue": "BYBIT", "data_type": "trades", "day": "2024-03-04"},
            )

    def test_missing_venue_raises(self) -> None:
        with pytest.raises(DeployMissingError, match="venue"):
            build_deploy_missing_preview(
                service="market-tick-data-service",
                asset_group="cefi",
                row_key={"data_type": "trades", "day": "2024-03-04"},
            )

    def test_missing_day_raises(self) -> None:
        with pytest.raises(DeployMissingError, match="day"):
            build_deploy_missing_preview(
                service="market-tick-data-service",
                asset_group="cefi",
                row_key={"venue": "BYBIT", "data_type": "trades"},
            )

    def test_command_quotes_shard_key(self) -> None:
        """The pipe character is a shell metacharacter; the command MUST
        wrap the shard-key value in shell-safe quoting."""
        preview = build_deploy_missing_preview(
            service="market-tick-data-service",
            asset_group="cefi",
            row_key={
                "venue": "BINANCE-FUTURES",
                "data_type": "trades",
                "instrument_type": "PERPETUAL",
                "instrument_id": "btcusdt",
                "day": "2024-03-04",
            },
        )
        # shlex.quote wraps in single quotes when the value contains
        # shell-meta chars; the pipe ensures it does.
        assert "'" in preview.command


class TestListSupportedServices:
    def test_includes_mtds(self) -> None:
        services = list_supported_services()
        assert "market-tick-data-service" in services

    def test_returns_sorted_list(self) -> None:
        services = list_supported_services()
        assert services == sorted(services)


# ---------------------------------------------------------------------------
# Mode = tarball-from-local — pairs the launcher with a refresh step that
# bundles the operator's local working tree before the VM launches.
# ---------------------------------------------------------------------------


class TestTarballFromLocalMode:
    def test_default_mode_is_preview(self) -> None:
        preview = build_deploy_missing_preview(
            service="market-tick-data-service",
            asset_group="cefi",
            row_key={"venue": "BYBIT", "data_type": "trades", "day": "2024-03-04"},
        )
        assert preview.mode == "preview"
        assert preview.warnings == []

    def test_tarball_mode_chains_refresh_then_launcher(self) -> None:
        preview = build_deploy_missing_preview(
            service="market-tick-data-service",
            asset_group="cefi",
            row_key={
                "venue": "BINANCE-FUTURES",
                "data_type": "trades",
                "instrument_type": "PERPETUAL",
                "instrument_id": "btcusdt",
                "day": "2024-03-04",
            },
            mode="tarball-from-local",
        )
        assert preview.mode == "tarball-from-local"
        # Combined command: refresh + launcher chained via && so a
        # tarball-build failure aborts before the VM launches.
        assert "create-code-tarballs.sh --all" in preview.command
        assert " && " in preview.command
        assert "launch-mtds-backfill-vm.sh" in preview.command

    def test_tarball_mode_emits_local_only_warning(self) -> None:
        preview = build_deploy_missing_preview(
            service="market-tick-data-service",
            asset_group="cefi",
            row_key={"venue": "BYBIT", "data_type": "trades", "day": "2024-03-04"},
            mode="tarball-from-local",
        )
        assert any("LOCAL-ONLY" in w for w in preview.warnings)
        assert any("UNCOMMITTED" in w for w in preview.warnings)

    def test_preview_mode_does_not_emit_warnings(self) -> None:
        preview = build_deploy_missing_preview(
            service="market-tick-data-service",
            asset_group="cefi",
            row_key={"venue": "BYBIT", "data_type": "trades", "day": "2024-03-04"},
            mode="preview",
        )
        assert preview.warnings == []
        # The preview-mode notes still mention the tarball-from-local
        # alternative so operators can discover the mode.
        assert any("tarball-from-local" in n for n in preview.notes)

    def test_unknown_mode_raises(self) -> None:
        with pytest.raises(DeployMissingError, match="Unsupported deploy-missing mode"):
            build_deploy_missing_preview(
                service="market-tick-data-service",
                asset_group="cefi",
                row_key={"venue": "BYBIT", "data_type": "trades", "day": "2024-03-04"},
                mode="auto-launch",  # not in SUPPORTED_LAUNCH_MODES yet.
            )

"""Phase 11.4 — Deploy-live-cluster preview unit tests.

Plan: live_pipeline_mtds_mdps_features_2026_05_08.md Phase 11.4.
Covers the closed-set role taxonomy + validation branches +
window-parameterised replay-cascade shape.
"""

import pytest

from deployment_api.services.deploy_missing import (
    DeployMissingError,
    build_live_cluster_launch_preview,
    list_supported_live_cluster_roles,
)


def test_list_roles_returns_closed_set() -> None:
    roles = list_supported_live_cluster_roles()
    assert roles == [
        "features-cross-cutting",
        "mdps-features-live",
        "mtds-live",
        "replay-cascade",
    ]


def test_mtds_live_cefi_prod_command_shape() -> None:
    preview = build_live_cluster_launch_preview(
        role="mtds-live",
        asset_group="cefi",
        deployment_env="prod",
    )
    assert preview.role == "mtds-live"
    assert preview.asset_group == "cefi"
    assert preview.deployment_env == "prod"
    assert "launch-mtds-live.sh" in preview.launcher_script
    assert "--asset-group cefi" in preview.command
    assert "--env prod" in preview.command
    assert preview.warnings == []


def test_features_cross_cutting_singleton_staging_warns() -> None:
    preview = build_live_cluster_launch_preview(
        role="features-cross-cutting",
        asset_group=None,
        deployment_env="staging",
    )
    assert preview.role == "features-cross-cutting"
    assert preview.asset_group is None
    assert "--asset-group" not in preview.command
    assert "--env staging" in preview.command
    assert preview.warnings
    assert "staging" in preview.warnings[0]


def test_replay_cascade_window_parameterised() -> None:
    preview = build_live_cluster_launch_preview(
        role="replay-cascade",
        asset_group="defi",
        deployment_env="prod",
        replay_start="2026-05-11T12:00:00Z",
        replay_end="2026-05-11T13:00:00Z",
        replay_shard_key="ETH-USDC",
    )
    assert "--asset-group defi" in preview.command
    assert "--start 2026-05-11T12:00:00Z" in preview.command
    assert "--end 2026-05-11T13:00:00Z" in preview.command
    assert "--shard-key ETH-USDC" in preview.command


def test_unknown_role_raises() -> None:
    with pytest.raises(DeployMissingError, match="Unknown live-cluster role"):
        build_live_cluster_launch_preview(
            role="bogus",
            asset_group=None,
            deployment_env="prod",
        )


def test_invalid_env_raises() -> None:
    with pytest.raises(DeployMissingError, match="Unknown deployment_env"):
        build_live_cluster_launch_preview(
            role="mtds-live",
            asset_group="cefi",
            deployment_env="bogus",
        )


def test_per_asset_group_role_missing_asset_group_raises() -> None:
    with pytest.raises(DeployMissingError, match="requires --asset-group"):
        build_live_cluster_launch_preview(
            role="mtds-live",
            asset_group=None,
            deployment_env="prod",
        )


def test_singleton_role_with_asset_group_raises() -> None:
    with pytest.raises(DeployMissingError, match="is singleton"):
        build_live_cluster_launch_preview(
            role="features-cross-cutting",
            asset_group="cefi",
            deployment_env="prod",
        )


def test_replay_missing_window_args_raises() -> None:
    with pytest.raises(DeployMissingError, match="--start, --end, --shard-key"):
        build_live_cluster_launch_preview(
            role="replay-cascade",
            asset_group="cefi",
            deployment_env="prod",
        )


def test_replay_partial_window_args_raises() -> None:
    with pytest.raises(DeployMissingError, match="--start, --end, --shard-key"):
        build_live_cluster_launch_preview(
            role="replay-cascade",
            asset_group="cefi",
            deployment_env="prod",
            replay_start="2026-05-11T12:00:00Z",
        )


def test_invalid_asset_group_raises() -> None:
    with pytest.raises(DeployMissingError, match="requires --asset-group"):
        build_live_cluster_launch_preview(
            role="mtds-live",
            asset_group="bogus_ag",
            deployment_env="prod",
        )


def test_preview_to_dict_serialises_all_fields() -> None:
    preview = build_live_cluster_launch_preview(
        role="mdps-features-live",
        asset_group="sports",
        deployment_env="dev",
    )
    d = preview.to_dict()
    assert d["role"] == "mdps-features-live"
    assert d["asset_group"] == "sports"
    assert d["deployment_env"] == "dev"
    assert isinstance(d["notes"], list)
    assert isinstance(d["warnings"], list)
    assert "launcher_script" in d
    assert "command" in d

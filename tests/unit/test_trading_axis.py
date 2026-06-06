"""Unit tests for deployment_api.utils.trading_axis.

v9 canonical: ``asset_group`` is the ONLY accepted key for shard dims and
config mappings.  Legacy ``category`` keys in mapping dicts are intentionally
NOT read (would confuse post-migration counts).  CLI reading still accepts
``--category`` since old VM launch strings persisted in state.json may carry it.
"""

from deployment_api.utils.trading_axis import trading_axis_from_deployment_state


def test_trading_axis_from_top_level_asset_group() -> None:
    assert trading_axis_from_deployment_state({"asset_group": "cefi"}) == "CEFI"


def test_trading_axis_top_level_category_returns_none() -> None:
    """v9: top-level ``category`` key is NOT read; unmigrated state surfaces as None."""
    assert trading_axis_from_deployment_state({"category": "defi"}) is None


def test_trading_axis_from_config() -> None:
    assert trading_axis_from_deployment_state({"config": {"asset_group": "TRADFI"}}) == "TRADFI"


def test_trading_axis_config_category_returns_none() -> None:
    """v9: config-level ``category`` key is NOT read."""
    assert trading_axis_from_deployment_state({"config": {"category": "SPORTS"}}) is None


def test_trading_axis_from_first_shard_dimensions() -> None:
    data = {
        "shards": [
            {"dimensions": {"asset_group": "CEFI", "venue": "X"}},
        ]
    }
    assert trading_axis_from_deployment_state(data) == "CEFI"


def test_trading_axis_shard_category_returns_none() -> None:
    """v9: shard dims with only ``category`` (no ``asset_group``) → None (unmigrated)."""
    data2 = {"shards": [{"dimensions": {"category": "DEFI"}}]}
    assert trading_axis_from_deployment_state(data2) is None


def test_trading_axis_from_cli_command() -> None:
    assert trading_axis_from_deployment_state({"cli_command": "python run.py --asset-group=CEFI --foo 1"}) == "CEFI"


def test_trading_axis_cli_category_flag_still_accepted() -> None:
    """CLI reading still accepts ``--category`` (old VM launch strings persisted in state.json)."""
    assert trading_axis_from_deployment_state({"cli_command": "python run.py --category DEFI"}) == "DEFI"


def test_trading_axis_from_cli_args_list() -> None:
    assert trading_axis_from_deployment_state({"cli_args": ["run.py", "--asset-group", "SPORTS"]}) == "SPORTS"


def test_trading_axis_none_when_missing() -> None:
    assert trading_axis_from_deployment_state({}) is None

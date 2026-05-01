"""Unit tests for deployment_api.utils.trading_axis."""

from deployment_api.utils.trading_axis import trading_axis_from_deployment_state


def test_trading_axis_from_top_level_keys() -> None:
    assert trading_axis_from_deployment_state({"asset_group": "cefi"}) == "CEFI"
    assert trading_axis_from_deployment_state({"category": "defi"}) == "DEFI"


def test_trading_axis_from_config() -> None:
    assert trading_axis_from_deployment_state({"config": {"asset_group": "TRADFI"}}) == "TRADFI"
    assert trading_axis_from_deployment_state({"config": {"category": "SPORTS"}}) == "SPORTS"


def test_trading_axis_from_first_shard_dimensions() -> None:
    data = {
        "shards": [
            {"dimensions": {"asset_group": "CEFI", "venue": "X"}},
        ]
    }
    assert trading_axis_from_deployment_state(data) == "CEFI"
    data2 = {"shards": [{"dimensions": {"category": "DEFI"}}]}
    assert trading_axis_from_deployment_state(data2) == "DEFI"


def test_trading_axis_from_cli_command() -> None:
    assert (
        trading_axis_from_deployment_state(
            {"cli_command": "python run.py --asset-group=CEFI --foo 1"}
        )
        == "CEFI"
    )
    assert (
        trading_axis_from_deployment_state({"cli_command": "python run.py --category DEFI"})
        == "DEFI"
    )


def test_trading_axis_from_cli_args_list() -> None:
    assert (
        trading_axis_from_deployment_state({"cli_args": ["run.py", "--asset-group", "SPORTS"]})
        == "SPORTS"
    )


def test_trading_axis_none_when_missing() -> None:
    assert trading_axis_from_deployment_state({}) is None

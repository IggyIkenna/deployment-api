"""Cat D.1 — every registered service has a routable Deploy-Missing preview.

Plan: ``data_status_comprehensive_test_coverage_2026_05_07.plan.md`` Phase 3.

The Deploy-Missing UI button reads ``list_supported_services()`` to
decide which leaves render the button. For every service in that list,
the preview composer (``build_deploy_missing_preview``) must
successfully produce a launcher invocation given a representative leaf
``row_key`` for that service's primary asset_group.

If a service is added to ``_SERVICE_LAUNCHER_SCRIPTS`` without a working
row_key contract, the UI button renders but the preview API 400s — this
test catches that drift before the button reaches a user.
"""

from __future__ import annotations

import pytest

from deployment_api.services.deploy_missing import (
    build_deploy_missing_preview,
    list_supported_services,
)

# Per-service representative row_key — chosen to hit the service's primary
# asset_group + a populated shard atom. If a new service registers a
# launcher and the test fails because of an unmapped service slug, add
# the row_key here in the same PR.
_SERVICE_REPRESENTATIVE_ROW_KEYS: dict[str, tuple[str, dict[str, str]]] = {
    "market-tick-data-service": (
        "cefi",
        {
            "venue": "BINANCE-FUTURES",
            "data_type": "trades",
            "instrument_type": "PERPETUAL",
            "instrument_id": "btcusdt",
            "day": "2024-03-04",
        },
    ),
    "market-data-processing-service": (
        "cefi",
        {
            "venue": "BYBIT",
            "data_type": "ohlcv_1m",
            "instrument_type": "PERPETUAL",
            "instrument_id": "btcusdt",
            "day": "2024-03-04",
        },
    ),
    "instruments-service": (
        "cefi",
        {
            "venue": "BINANCE",
            "data_type": "instruments_metadata",
            "instrument_type": "SPOT",
            "instrument_id": "btcusdt",
            "day": "2024-03-04",
        },
    ),
    "features-onchain-service": (
        "defi",
        {
            "venue": "AAVEV3-ARBITRUM",
            "chain": "ARBITRUM",
            "data_type": "lending_indices",
            "feature_group": "lst_yields",
            "timeframe": "1h",
            "instrument_id": "USDC",
            "day": "2024-03-04",
        },
    ),
    "features-delta-one-service": (
        "cefi",
        {
            "venue": "BYBIT",
            "data_type": "carry",
            "feature_group": "carry_basic",
            "timeframe": "1h",
            "instrument_id": "btcusdt",
            "day": "2024-03-04",
        },
    ),
    "features-volatility-service": (
        "cefi",
        {
            "venue": "DERIBIT",
            "data_type": "implied_vol",
            "feature_group": "iv_surface",
            "timeframe": "1d",
            "instrument_id": "BTC-OPT",
            "day": "2024-03-04",
        },
    ),
    "features-cross-instrument-service": (
        "cefi",
        {
            "venue": "BYBIT",
            "data_type": "spread",
            "feature_group": "cross_venue_basis",
            "timeframe": "1h",
            "instrument_id": "btcusdt",
            "day": "2024-03-04",
        },
    ),
    "features-sports-service": (
        "sports",
        {
            "venue": "FOOTYSTATS",
            "data_type": "fixture_features",
            "feature_group": "footystats_only",
            "league_id": "39",
            "day": "2024-03-04",
        },
    ),
    "features-calendar-service": (
        "shared",
        {
            "venue": "OPEN_METEO",
            "data_type": "weather_forecast",
            "feature_group": "weather",
            "timeframe": "1h",
            "day": "2024-03-04",
        },
    ),
}


def test_every_registered_service_has_a_representative_row_key() -> None:
    """Coverage gate: every service in ``list_supported_services()`` MUST
    appear in ``_SERVICE_REPRESENTATIVE_ROW_KEYS``. Failing means a new
    launcher was registered without a matching test fixture — add the
    row_key in the same PR."""
    registered = set(list_supported_services())
    fixtured = set(_SERVICE_REPRESENTATIVE_ROW_KEYS.keys())
    missing = registered - fixtured
    assert not missing, (
        f"Services registered in _SERVICE_LAUNCHER_SCRIPTS but missing a representative "
        f"row_key in this test: {sorted(missing)}. Add a row_key to "
        f"_SERVICE_REPRESENTATIVE_ROW_KEYS in test_deploy_missing_preview_routing_per_service.py."
    )


@pytest.mark.parametrize("service", sorted(list_supported_services()))
def test_each_service_routes_to_a_launcher_script(service: str) -> None:
    """For every registered service, the preview composer produces a
    valid launcher invocation that:
    1. Includes ``bash <launch-*.sh>`` in the command;
    2. Includes the canonical ``--shard-key=...`` flag;
    3. Returns a non-empty ``shard_key`` field.
    """
    asset_group, row_key = _SERVICE_REPRESENTATIVE_ROW_KEYS[service]
    preview = build_deploy_missing_preview(
        service=service,
        asset_group=asset_group,
        row_key=row_key,
        mode="preview",
    )
    assert preview.launcher_script.endswith(".sh"), (
        f"{service} launcher_script must end in .sh; got {preview.launcher_script!r}"
    )
    assert "launch-" in preview.launcher_script, (
        f"{service} launcher_script must follow the launch-*.sh naming convention; "
        f"got {preview.launcher_script!r}"
    )
    assert preview.command.startswith("bash "), (
        f"{service} command must start with 'bash '; got {preview.command!r}"
    )
    assert "--shard-key=" in preview.command, (
        f"{service} command missing --shard-key flag; got {preview.command!r}"
    )
    # shard_key has the canonical 6-field shape.
    assert preview.shard_key.count("|") == 5, (
        f"{service} shard_key has {preview.shard_key.count('|') + 1} fields; "
        f"expected 6. shard_key={preview.shard_key!r}"
    )
    # asset_group is the first field.
    assert preview.shard_key.split("|", 1)[0] == asset_group


def test_default_mode_is_preview() -> None:
    """When mode is omitted, default is ``preview`` (no tarball refresh)."""
    preview = build_deploy_missing_preview(
        service="market-tick-data-service",
        asset_group="cefi",
        row_key=_SERVICE_REPRESENTATIVE_ROW_KEYS["market-tick-data-service"][1],
    )
    assert preview.mode == "preview"
    assert "create-code-tarballs.sh" not in preview.command

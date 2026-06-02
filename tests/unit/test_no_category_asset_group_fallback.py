"""QG ratchet: assert that the data-status surface does NOT read ``category``
as an asset-group key (v9 canonical: only ``asset_group`` is accepted).

Operator directive 2026-06-02: "category shouldn't be allowed anywhere even as
a fallback in data-status — it will confuse counts; we want v9 post-migration
canonical only."

This file is intentionally small — it is a regression guard, not an exhaustive
test suite.  Failures here mean a ``category``-as-asset-group fallback was
re-introduced; fix the production code, do NOT relax the assertion.
"""

from __future__ import annotations

from types import SimpleNamespace

import pytest

# ---------------------------------------------------------------------------
# trading_axis: asset_group only — no category fallback in mapping reads
# ---------------------------------------------------------------------------


class TestTradingAxisNoCategoryFallback:
    """Ratchet: trading_axis.py mapping reads must NOT fall back to category."""

    def test_shard_dims_category_only_returns_none(self) -> None:
        """Shard dim with only ``category`` (no ``asset_group``) → None (unmigrated)."""
        from deployment_api.utils.trading_axis import trading_axis_value_from_shard_dimensions

        result = trading_axis_value_from_shard_dimensions({"category": "cefi"})
        assert result is None, (
            "trading_axis_value_from_shard_dimensions MUST NOT fall back to 'category'. "
            "A shard with only 'category' is unmigrated — surface as None, not 'CEFI'. "
            "This is a v9-canonical regression guard."
        )

    def test_top_level_category_key_returns_none(self) -> None:
        """Top-level ``{category: ...}`` state → None (no asset_group present)."""
        from deployment_api.utils.trading_axis import trading_axis_from_deployment_state

        result = trading_axis_from_deployment_state({"category": "defi"})
        assert result is None, (
            "trading_axis_from_deployment_state MUST NOT fall back to top-level 'category'. v9 regression guard."
        )

    def test_config_category_key_returns_none(self) -> None:
        """Config ``{category: ...}`` mapping → None (no asset_group present)."""
        from deployment_api.utils.trading_axis import trading_axis_from_deployment_state

        result = trading_axis_from_deployment_state({"config": {"category": "tradfi"}})
        assert result is None, (
            "trading_axis_from_deployment_state config block MUST NOT fall back to 'category'. v9 regression guard."
        )

    def test_asset_group_still_works(self) -> None:
        """Sanity: canonical ``asset_group`` key is correctly read."""
        from deployment_api.utils.trading_axis import trading_axis_from_deployment_state

        assert trading_axis_from_deployment_state({"asset_group": "cefi"}) == "CEFI"
        assert trading_axis_from_deployment_state({"asset_group": "defi"}) == "DEFI"
        assert trading_axis_from_deployment_state({"asset_group": "sports"}) == "SPORTS"


# ---------------------------------------------------------------------------
# shard_management: _asset_group_from_shard_dims must not fall back to category
# ---------------------------------------------------------------------------


class TestShardManagementNoCategoryFallback:
    """Ratchet: shard_management.py dim reads must NOT fall back to category."""

    def test_asset_group_from_shard_dims_category_only_returns_empty(self) -> None:
        from deployment_api.routes.shard_management import (
            _asset_group_from_shard_dims,  # type: ignore[reportPrivateUsage]
        )

        result = _asset_group_from_shard_dims({"category": "cefi"})
        assert result == "", (
            "_asset_group_from_shard_dims MUST NOT fall back to 'category'. "
            "Returns '' for unmigrated shards. v9 regression guard."
        )

    def test_asset_group_from_shard_dims_canonical(self) -> None:
        from deployment_api.routes.shard_management import (
            _asset_group_from_shard_dims,  # type: ignore[reportPrivateUsage]
        )

        assert _asset_group_from_shard_dims({"asset_group": "CEFI"}) == "CEFI"

    def test_asset_groups_from_state_skips_category_only_shards(self) -> None:
        """Shards with only ``category`` (no ``asset_group``) are unmigrated → skipped."""
        from deployment_api.routes.shard_management import asset_groups_from_state

        shards = [
            SimpleNamespace(status="succeeded", dimensions={"category": "CEFI"}),
        ]
        state = SimpleNamespace(shards=shards)
        result = asset_groups_from_state(state)
        assert result is None, (
            "asset_groups_from_state MUST skip shards with only 'category' (no asset_group). "
            "Returns None when all shards are unmigrated. v9 regression guard."
        )

    def test_verified_shard_ids_uses_asset_group_not_category(self) -> None:
        """_compute_verified_succeeded_shard_ids reads ``asset_group`` dim, not ``category``."""
        from deployment_api.routes.shard_management import compute_completed_breakdown

        # Shard with only category: should NOT be verified (asset_group dim absent).
        shards = [
            SimpleNamespace(
                shard_id="s1",
                status="succeeded",
                dimensions={"category": "CEFI", "venue": "BINANCE", "date": "2024-01-01"},
            ),
        ]
        state = SimpleNamespace(shards=shards)
        existing_cat_dates = {"CEFI": {"2024-01-01"}}
        existing_venue_dates = {"CEFI": {"BINANCE": {"2024-01-01"}}}
        result = compute_completed_breakdown(state, None, existing_cat_dates, existing_venue_dates)
        # With category-only dim: asset_group lookup returns "" → shard is NOT verified.
        assert result["verified_clean"] == 0, (
            "compute_completed_breakdown MUST NOT use 'category' dim for verification. "
            "A shard with only 'category' (no 'asset_group') must not be counted as verified. "
            "v9 regression guard."
        )


# ---------------------------------------------------------------------------
# GCS path building: _shard_prefix must use asset_group=, not category=
# ---------------------------------------------------------------------------


class TestShardPrefixUsesAssetGroup:
    """Ratchet: _shard_prefix must emit ``asset_group=`` hive key, not ``category=``."""

    @pytest.mark.parametrize("svc", ["market-tick-data-service", "market-data-processing-service"])
    def test_shard_prefix_uses_asset_group_not_category(self, svc: str) -> None:
        from deployment_api.services.data_status_drilldown import _shard_prefix  # type: ignore[reportPrivateUsage]

        prefix = _shard_prefix(
            service=svc,
            asset_group="cefi",
            venue="BINANCE",
            day="2026-06-01",
            instrument_type="spot",
            data_type="ohlcv_1m",
        )
        assert "asset_group=" in prefix, (
            f"_shard_prefix for {svc} must use 'asset_group=' hive key. Got: {prefix!r}. v9 regression guard."
        )
        assert "category=" not in prefix, (
            f"_shard_prefix for {svc} must NOT use 'category=' hive key. Got: {prefix!r}. v9 regression guard."
        )

    def test_shard_prefix_non_mtds_uses_asset_group(self) -> None:
        """Non-MTDS services (fallback branch) also use ``asset_group=``."""
        from deployment_api.services.data_status_drilldown import _shard_prefix  # type: ignore[reportPrivateUsage]

        prefix = _shard_prefix(
            service="some-other-service",
            asset_group="defi",
            venue="UNISWAP_V3",
            day="2026-06-01",
            instrument_type="pool",
            data_type="dex_pool_state",
        )
        assert "asset_group=" in prefix, (
            f"_shard_prefix fallback branch must use 'asset_group='. Got: {prefix!r}. v9 regression guard."
        )
        assert "category=" not in prefix, (
            f"_shard_prefix fallback branch must NOT use 'category='. Got: {prefix!r}. v9 regression guard."
        )

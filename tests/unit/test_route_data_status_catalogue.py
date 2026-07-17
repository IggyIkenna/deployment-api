"""Tests for routes/data_status/_catalogue.py — the P6 phase-1 availability-derived
instrument catalogue explorer (``GET /catalogue`` + ``/download-catalogue-csv``).

Mirrors ``test_route_data_status_live.py``'s ``client_ds_live`` TestClient
pattern. ``unified_api_contracts.is_mvp`` is patched at its
``_coverage_scope`` import site (the shared ``is_mvp_for_manifest_row`` helper
lives there) so these tests stay independent of the live MVP_SCOPE registry.

**Two-source coverage (2026-07-17 real-data bugfix):** prediction/sports keep
reading ``_read_availability_index`` (``TestGetInstrumentCatalogue`` below,
unchanged). cefi/defi/tradfi now read ``prod/catalog.parquet`` instead (their
``_index`` is VENUE-level — no ``instrument_id`` column at all, which is
exactly why ``GET /data-status/catalogue`` returned ``total_count=0`` for
these three on real data even though this file's original tests were green —
they only ever exercised ``asset_group="cefi"`` against a manifest-shaped
fixture, which is not what production actually reads for that asset_group).
``TestIdentityCatalogueSource`` below fixtures a frame shaped like the REAL
``prod/catalog.parquet`` schema (``instruments-service/scripts/
build_instrument_catalogue.py::CATALOG_COLUMNS`` — ``data_type`` blank for
these single-grain asset groups, a precomputed ``mvp`` bool, no
``capture_status``/``error_reason``/``attempted_at``/``written_at``) so a
regression back to the venue-level index would fail loudly again.

Plan: ``data_status_page_ux_and_canonicalisation_2026_07_16.md`` P6.
"""

from __future__ import annotations

import io
from unittest.mock import MagicMock, patch

import pandas as pd
import pytest
from fastapi import FastAPI
from fastapi.testclient import TestClient

_PATCH_DISABLE_AUTH = "deployment_api.rbac.DISABLE_AUTH"
_PATCH_CFG = "deployment_api.routes.data_status._cfg"
_PATCH_BUILD_BUCKET = "deployment_api.routes.data_status.build_bucket_name"
_PATCH_READ_INDEX = "deployment_api.routes.data_status._read_availability_index"
_PATCH_IS_MVP = "deployment_api.routes.data_status._coverage_scope.is_mvp"
_PATCH_READ_IDENTITY_CATALOGUE = "deployment_api.routes.data_status._catalogue._read_identity_catalogue"
_PATCH_STORAGE_CLIENT = "deployment_api.routes.data_status._catalogue.get_storage_client"
_PATCH_RESOLVE_BUCKET = "unified_trading_library.resolve_bucket_name"


def _make_mock_cfg() -> MagicMock:
    cfg = MagicMock()
    cfg.is_mock_mode.return_value = False
    cfg.deployment_env = "dev"
    return cfg


@pytest.fixture
def client_ds_catalogue() -> TestClient:
    from deployment_api.routes.data_status import router

    app = FastAPI()
    app.include_router(router, prefix="/data-status")
    with (
        patch(_PATCH_DISABLE_AUTH, True),
        patch(_PATCH_CFG, _make_mock_cfg()),
    ):
        yield TestClient(app, raise_server_exceptions=False)  # type: ignore[misc]


def _parquet_bytes(df: pd.DataFrame) -> bytes:
    buf = io.BytesIO()
    df.to_parquet(buf, index=False)
    return buf.getvalue()


def _real_schema_catalog_df() -> pd.DataFrame:
    """A frame shaped like the REAL ``prod/catalog.parquet`` for cefi/defi/tradfi:
    ``data_type`` blank (single-grain AGs — only prediction's catalogue binds a
    real data_type per row), a precomputed ``mvp`` bool, NO
    ``capture_status``/``error_reason``/``attempted_at``/``written_at`` (those
    are manifest-only columns this identity catalogue never carries)."""
    return pd.DataFrame(
        [
            {
                "instrument_id": "BTC-USDT-PERP",
                "venue": "BINANCE-FUTURES",
                "instrument_type": "PERPETUAL",
                "data_type": "",
                "base_asset": "BTC",
                "mvp": True,
            },
            {
                "instrument_id": "ETH-USDT-PERP",
                "venue": "BINANCE-FUTURES",
                "instrument_type": "PERPETUAL",
                "data_type": "",
                "base_asset": "ETH",
                "mvp": False,
            },
            {
                "instrument_id": "SOL-USDT-SWAP",
                "venue": "OKX-SWAP",
                "instrument_type": "PERPETUAL",
                "data_type": "",
                "base_asset": "SOL",
                "mvp": True,
            },
        ]
    )


def _manifest_df() -> pd.DataFrame:
    """Two distinct instruments; BTC has TWO manifest rows (an older
    ``attempted_failed`` re-tried into a newer ``captured``) to exercise the
    written_at-latest-wins de-dup, ETH has one ``captured`` row."""
    return pd.DataFrame(
        {
            "date": ["2025-03-01", "2025-04-01", "2025-04-01"],
            "venue": ["BINANCE-FUTURES", "BINANCE-FUTURES", "BINANCE-FUTURES"],
            "instrument_type": ["PERPETUAL", "PERPETUAL", "PERPETUAL"],
            "data_type": ["trades", "trades", "trades"],
            "instrument_id": ["BTC-USDT-PERP", "BTC-USDT-PERP", "ETH-USDT-PERP"],
            "capture_status": ["attempted_failed", "captured", "captured"],
            "error_reason": ["timeout", "", ""],
            "attempted_at": ["2025-03-01T00:00:00Z", "2025-04-01T00:00:00Z", "2025-04-01T00:00:00Z"],
            "written_at": ["2025-03-01T00:05:00Z", "2025-04-01T00:05:00Z", "2025-04-01T00:05:00Z"],
            "league_id": ["", "", ""],
            "source": ["tardis", "tardis", "massive"],
        }
    )


class TestGetInstrumentCatalogue:
    def test_returns_deduped_rows_latest_written_at_wins(self, client_ds_catalogue: TestClient) -> None:
        with (
            patch(_PATCH_BUILD_BUCKET, return_value="market-data-tick-cefi-prd-fake"),
            patch(_PATCH_READ_INDEX, return_value=_manifest_df()),
            patch(_PATCH_IS_MVP, return_value=True),
        ):
            r = client_ds_catalogue.get(
                "/data-status/catalogue",
                params={"service": "market-tick-data-service", "asset_group": "prediction"},
            )
        assert r.status_code == 200
        body = r.json()
        assert body["total_count"] == 2
        by_id = {inst["instrument_id"]: inst for inst in body["instruments"]}
        assert set(by_id) == {"BTC-USDT-PERP", "ETH-USDT-PERP"}
        # BTC's latest (written_at 04-01) row is "captured", not the stale
        # 03-01 "attempted_failed" — de-dup keeps the most recent state.
        assert by_id["BTC-USDT-PERP"]["capture_status"] == "captured"
        assert body["label"] == "captured instruments (availability-derived)"

    def test_mvp_only_filters_to_mvp_true_rows(self, client_ds_catalogue: TestClient) -> None:
        def _fake_is_mvp(_asset_group, venue, _instrument_type, _data_type, **kwargs):
            return venue == "BINANCE-FUTURES" and kwargs.get("source") == "tardis"

        with (
            patch(_PATCH_BUILD_BUCKET, return_value="market-data-tick-cefi-prd-fake"),
            patch(_PATCH_READ_INDEX, return_value=_manifest_df()),
            patch(_PATCH_IS_MVP, side_effect=_fake_is_mvp),
        ):
            r = client_ds_catalogue.get(
                "/data-status/catalogue",
                params={"service": "market-tick-data-service", "asset_group": "prediction", "mvp_only": "true"},
            )
        assert r.status_code == 200
        body = r.json()
        assert body["mvp_only"] is True
        assert body["total_count"] == 1
        assert body["instruments"][0]["instrument_id"] == "BTC-USDT-PERP"
        assert body["instruments"][0]["is_mvp"] is True

    def test_search_matches_substring(self, client_ds_catalogue: TestClient) -> None:
        with (
            patch(_PATCH_BUILD_BUCKET, return_value="market-data-tick-cefi-prd-fake"),
            patch(_PATCH_READ_INDEX, return_value=_manifest_df()),
            patch(_PATCH_IS_MVP, return_value=False),
        ):
            r = client_ds_catalogue.get(
                "/data-status/catalogue",
                params={"service": "market-tick-data-service", "asset_group": "prediction", "search": "eth"},
            )
        assert r.status_code == 200
        body = r.json()
        assert body["total_count"] == 1
        assert body["instruments"][0]["instrument_id"] == "ETH-USDT-PERP"

    def test_venue_narrow_scopes_result(self, client_ds_catalogue: TestClient) -> None:
        df = pd.concat(
            [
                _manifest_df(),
                pd.DataFrame(
                    {
                        "date": ["2025-04-01"],
                        "venue": ["OKX-SWAP"],
                        "instrument_type": ["PERPETUAL"],
                        "data_type": ["trades"],
                        "instrument_id": ["SOL-USDT-SWAP"],
                        "capture_status": ["captured"],
                        "error_reason": [""],
                        "attempted_at": ["2025-04-01T00:00:00Z"],
                        "written_at": ["2025-04-01T00:05:00Z"],
                        "league_id": [""],
                        "source": ["tardis"],
                    }
                ),
            ],
            ignore_index=True,
        )
        with (
            patch(_PATCH_BUILD_BUCKET, return_value="market-data-tick-cefi-prd-fake"),
            patch(_PATCH_READ_INDEX, return_value=df),
            patch(_PATCH_IS_MVP, return_value=False),
        ):
            r = client_ds_catalogue.get(
                "/data-status/catalogue",
                params={
                    "service": "market-tick-data-service",
                    "asset_group": "prediction",
                    "venue": "OKX-SWAP",
                },
            )
        assert r.status_code == 200
        body = r.json()
        assert body["total_count"] == 1
        assert body["instruments"][0]["instrument_id"] == "SOL-USDT-SWAP"

    def test_manifest_read_failure_returns_500(self, client_ds_catalogue: TestClient) -> None:
        with (
            patch(_PATCH_BUILD_BUCKET, return_value="market-data-tick-cefi-prd-fake"),
            patch(_PATCH_READ_INDEX, side_effect=OSError("gcs unavailable")),
        ):
            r = client_ds_catalogue.get(
                "/data-status/catalogue",
                params={"service": "market-tick-data-service", "asset_group": "prediction"},
            )
        assert r.status_code == 500


class TestDownloadCatalogueCsv:
    def test_csv_matches_json_route_row_count(self, client_ds_catalogue: TestClient) -> None:
        with (
            patch(_PATCH_BUILD_BUCKET, return_value="market-data-tick-cefi-prd-fake"),
            patch(_PATCH_READ_INDEX, return_value=_manifest_df()),
            patch(_PATCH_IS_MVP, return_value=True),
        ):
            r = client_ds_catalogue.get(
                "/data-status/download-catalogue-csv",
                params={"service": "market-tick-data-service", "asset_group": "prediction"},
            )
        assert r.status_code == 200
        assert r.headers["content-type"].startswith("text/csv")
        assert r.headers["X-Row-Count"] == "2"
        assert "BTC-USDT-PERP" in r.text
        assert "ETH-USDT-PERP" in r.text


class TestIdentityCatalogueSource:
    """cefi/defi/tradfi read ``prod/catalog.parquet`` (real-data bugfix
    2026-07-17), NOT ``_read_availability_index`` — these three asset groups'
    ``_index`` is VENUE-level (no ``instrument_id`` per row), which is why the
    endpoint returned ``total_count=0`` in production despite this file's
    pre-existing tests (all of which — before this fix — exercised
    ``asset_group="cefi"`` against a manifest-SHAPED fixture that production
    never actually has for cefi). ``_PATCH_IS_MVP`` is deliberately made to
    raise in most tests below to PROVE ``is_mvp`` is never called on this
    path — the identity catalogue's own ``mvp`` column is used directly."""

    @pytest.mark.parametrize("asset_group", ["cefi", "defi", "tradfi"])
    def test_returns_nonempty_rows_for_every_affected_asset_group(
        self, client_ds_catalogue: TestClient, asset_group: str
    ) -> None:
        with (
            patch(_PATCH_READ_IDENTITY_CATALOGUE, return_value=_real_schema_catalog_df()) as mock_read,
            patch(_PATCH_IS_MVP, side_effect=AssertionError("is_mvp must not be called for identity-catalogue rows")),
        ):
            r = client_ds_catalogue.get(
                "/data-status/catalogue",
                params={"service": "instruments-service", "asset_group": asset_group},
            )
        assert r.status_code == 200
        body = r.json()
        # This is the exact regression this fix closes: total_count was 0 for
        # all three of cefi/defi/tradfi on real data before this fix.
        assert body["total_count"] == 3
        by_id = {inst["instrument_id"]: inst for inst in body["instruments"]}
        assert set(by_id) == {"BTC-USDT-PERP", "ETH-USDT-PERP", "SOL-USDT-SWAP"}
        # Honest defaults for the fields the identity catalogue doesn't carry —
        # never fabricated (see module docstring).
        assert by_id["BTC-USDT-PERP"]["capture_status"] == "captured"
        assert by_id["BTC-USDT-PERP"]["error_reason"] == ""
        assert by_id["BTC-USDT-PERP"]["attempted_at"] == ""
        mock_read.assert_called_once_with(asset_group)

    def test_bucket_resolution_is_asset_group_scoped_not_service_scoped(self, client_ds_catalogue: TestClient) -> None:
        """The identity catalogue lives in the instruments-service bucket
        regardless of the caller's ``service`` param (unlike the manifest path,
        which resolves per-service) — a real production call always passes
        ``service=instruments-service`` (CatalogueExplorer is IS-tab-only), but
        the bucket resolution must not accidentally depend on it."""
        with (
            patch(_PATCH_RESOLVE_BUCKET, return_value="instruments-store-cefi-prd-fake") as mock_resolve,
            patch(_PATCH_STORAGE_CLIENT) as mock_get_client,
        ):
            mock_get_client.return_value.download_bytes.return_value = _parquet_bytes(_real_schema_catalog_df())
            r = client_ds_catalogue.get(
                "/data-status/catalogue",
                params={"service": "instruments-service", "asset_group": "cefi"},
            )
        assert r.status_code == 200
        assert r.json()["total_count"] == 3
        mock_resolve.assert_called_once_with(cloud="gcp", kind="instruments-store", asset_group="cefi")
        mock_get_client.return_value.download_bytes.assert_called_once_with(
            "instruments-store-cefi-prd-fake", "prod/catalog.parquet"
        )

    def test_mvp_only_uses_the_catalogues_own_mvp_column(self, client_ds_catalogue: TestClient) -> None:
        with (
            patch(_PATCH_READ_IDENTITY_CATALOGUE, return_value=_real_schema_catalog_df()),
            patch(_PATCH_IS_MVP, side_effect=AssertionError("is_mvp must not be called for identity-catalogue rows")),
        ):
            r = client_ds_catalogue.get(
                "/data-status/catalogue",
                params={"service": "instruments-service", "asset_group": "cefi", "mvp_only": "true"},
            )
        assert r.status_code == 200
        body = r.json()
        assert body["total_count"] == 2
        by_id = {inst["instrument_id"] for inst in body["instruments"]}
        # BTC + SOL have mvp=True in the fixture; ETH (mvp=False) is excluded.
        assert by_id == {"BTC-USDT-PERP", "SOL-USDT-SWAP"}

    def test_search_matches_substring(self, client_ds_catalogue: TestClient) -> None:
        with (
            patch(_PATCH_READ_IDENTITY_CATALOGUE, return_value=_real_schema_catalog_df()),
            patch(_PATCH_IS_MVP, side_effect=AssertionError("is_mvp must not be called for identity-catalogue rows")),
        ):
            r = client_ds_catalogue.get(
                "/data-status/catalogue",
                params={"service": "instruments-service", "asset_group": "defi", "search": "sol"},
            )
        assert r.status_code == 200
        body = r.json()
        assert body["total_count"] == 1
        assert body["instruments"][0]["instrument_id"] == "SOL-USDT-SWAP"

    def test_venue_narrow_scopes_result(self, client_ds_catalogue: TestClient) -> None:
        with (
            patch(_PATCH_READ_IDENTITY_CATALOGUE, return_value=_real_schema_catalog_df()),
            patch(_PATCH_IS_MVP, side_effect=AssertionError("is_mvp must not be called for identity-catalogue rows")),
        ):
            r = client_ds_catalogue.get(
                "/data-status/catalogue",
                params={"service": "instruments-service", "asset_group": "tradfi", "venue": "OKX-SWAP"},
            )
        assert r.status_code == 200
        body = r.json()
        assert body["total_count"] == 1
        assert body["instruments"][0]["instrument_id"] == "SOL-USDT-SWAP"

    def test_catalogue_read_failure_degrades_to_empty_not_500(self, client_ds_catalogue: TestClient) -> None:
        """Shard-isolated: unlike the manifest path (500 on read failure), a
        missing/unreadable ``prod/catalog.parquet`` degrades to an honest
        empty catalogue — same "never raise" contract as
        ``catalogue_lifecycle.py``/``prediction_catalogue.py`` reading the
        SAME file."""
        with (
            patch(_PATCH_STORAGE_CLIENT) as mock_get_client,
            patch(_PATCH_RESOLVE_BUCKET, return_value="instruments-store-cefi-prd-fake"),
        ):
            mock_get_client.return_value.download_bytes.side_effect = OSError("gcs unavailable")
            r = client_ds_catalogue.get(
                "/data-status/catalogue",
                params={"service": "instruments-service", "asset_group": "cefi"},
            )
        assert r.status_code == 200
        assert r.json()["total_count"] == 0

    def test_csv_export_matches_json_row_count(self, client_ds_catalogue: TestClient) -> None:
        with (
            patch(_PATCH_READ_IDENTITY_CATALOGUE, return_value=_real_schema_catalog_df()),
            patch(_PATCH_IS_MVP, side_effect=AssertionError("is_mvp must not be called for identity-catalogue rows")),
        ):
            r = client_ds_catalogue.get(
                "/data-status/download-catalogue-csv",
                params={"service": "instruments-service", "asset_group": "cefi"},
            )
        assert r.status_code == 200
        assert r.headers["X-Row-Count"] == "3"
        assert "BTC-USDT-PERP" in r.text
        assert "SOL-USDT-SWAP" in r.text

    def test_real_parquet_bytes_schema_aware_projection(self, client_ds_catalogue: TestClient) -> None:
        """End-to-end through the real parquet read path (not a mocked
        DataFrame): a raw parquet payload shaped like the actual
        ``CATALOG_COLUMNS`` schema (extra columns this endpoint doesn't
        project — ``chain``/``available_from``/``available_to``/``raw_symbol``
        — must be silently dropped, not error)."""
        real_shaped_df = pd.DataFrame(
            [
                {
                    "instrument_id": "BTC-USDT-PERP",
                    "instrument_type": "PERPETUAL",
                    "venue": "BINANCE-FUTURES",
                    "chain": "",
                    "available_from": "2024-01-01",
                    "available_to": "",
                    "base_asset": "BTC",
                    "raw_symbol": "BTCUSDT",
                    "data_type": "",
                    "mvp": True,
                }
            ]
        )
        with (
            patch(_PATCH_STORAGE_CLIENT) as mock_get_client,
            patch(_PATCH_RESOLVE_BUCKET, return_value="instruments-store-cefi-prd-fake"),
        ):
            mock_get_client.return_value.download_bytes.return_value = _parquet_bytes(real_shaped_df)
            r = client_ds_catalogue.get(
                "/data-status/catalogue",
                params={"service": "instruments-service", "asset_group": "cefi"},
            )
        assert r.status_code == 200
        body = r.json()
        assert body["total_count"] == 1
        assert body["instruments"][0]["instrument_id"] == "BTC-USDT-PERP"
        assert body["instruments"][0]["is_mvp"] is True


class TestIdentityCatalogueAssetGroupsGuard:
    """Regression guard: sports/prediction must NEVER join ``_IDENTITY_CATALOGUE_ASSET_GROUPS``.

    Adding them reads like a consistency win (one source for every asset_group) and
    has been prototyped + reverted more than once. It is a real regression — see the
    dense comment above the frozenset in ``_catalogue.py`` for the measured numbers.
    This test is the enforcement; the comment alone has not been enough.
    """

    def test_sports_and_prediction_not_in_identity_catalogue_asset_groups(self) -> None:
        from deployment_api.routes.data_status._catalogue import _IDENTITY_CATALOGUE_ASSET_GROUPS

        for ag in ("sports", "prediction"):
            assert ag not in _IDENTITY_CATALOGUE_ASSET_GROUPS, (
                f"{ag!r} must NOT read the identity catalogue (prod/catalog.parquet). Measured on the live "
                "sports catalogue 2026-07-17 (27,250 rows): venue is BLANK on 100.0% of rows (the explorer's "
                "venue narrow would silently return nothing), and there is NO capture_status column — the "
                "identity path defaults capture_status to 'captured', which would FABRICATE a captured status "
                "for ~27k rows carrying no capture evidence (honest-absence violation, "
                "codex/02-data/honest-absence-downstream-handling.md). Its instrument_type is also lowercase "
                "legacy (fixture/player/team/league), not the UPPERCASE canonical vocabulary this endpoint's "
                "type narrow matches. sports/prediction read _read_availability_index, whose _index carries a "
                "genuine per-row instrument_id/league_id. Do not 'unify' these paths."
            )

    def test_identity_catalogue_asset_groups_is_exactly_the_venue_level_index_three(self) -> None:
        """The set is exactly cefi/defi/tradfi — the three whose ``_index`` is VENUE-level."""
        from deployment_api.routes.data_status._catalogue import _IDENTITY_CATALOGUE_ASSET_GROUPS

        assert sorted(_IDENTITY_CATALOGUE_ASSET_GROUPS) == ["cefi", "defi", "tradfi"]


class TestCatalogueFilterOptions:
    """F3 (live UI review round 3, 2026-07-17): the Catalogue Explorer's
    venue/instrument_type/data_type filters were free-text; this endpoint feeds
    the real distinct values so the UI can render dropdowns. Distinct values are
    honest-absence: an axis with no/blank data returns ``[]``."""

    def test_identity_catalogue_distinct_values(self, client_ds_catalogue: TestClient) -> None:
        # cefi/defi/tradfi read prod/catalog.parquet (identity source). data_type is
        # blank for these single-grain AGs -> honest empty list.
        with patch(_PATCH_READ_IDENTITY_CATALOGUE, return_value=_real_schema_catalog_df()):
            r = client_ds_catalogue.get(
                "/data-status/catalogue-filter-options",
                params={"service": "market-tick-data-service", "asset_group": "cefi"},
            )
        assert r.status_code == 200
        body = r.json()
        assert body["asset_group"] == "cefi"
        assert body["venues"] == ["BINANCE-FUTURES", "OKX-SWAP"]  # sorted, distinct
        assert body["instrument_types"] == ["PERPETUAL"]
        assert body["data_types"] == []  # all blank -> honest-absence, not [""]

    def test_manifest_catalogue_distinct_values(self, client_ds_catalogue: TestClient) -> None:
        # prediction/sports read the availability index; _manifest_df carries a real
        # data_type ("trades") per row.
        with patch(_PATCH_READ_INDEX, return_value=_manifest_df()):
            r = client_ds_catalogue.get(
                "/data-status/catalogue-filter-options",
                params={"service": "market-tick-data-service", "asset_group": "prediction"},
            )
        assert r.status_code == 200
        body = r.json()
        assert body["venues"] == ["BINANCE-FUTURES"]
        assert body["instrument_types"] == ["PERPETUAL"]
        assert body["data_types"] == ["trades"]

    def test_all_blank_axis_is_honest_empty(self, client_ds_catalogue: TestClient) -> None:
        # Mirrors the sports catalogue: venue 100% blank -> [] (the UI shows only
        # the "any" default, never a fabricated venue option).
        blank_venue = _manifest_df()
        blank_venue["venue"] = ""
        with patch(_PATCH_READ_INDEX, return_value=blank_venue):
            r = client_ds_catalogue.get(
                "/data-status/catalogue-filter-options",
                params={"service": "market-tick-data-service", "asset_group": "sports"},
            )
        assert r.status_code == 200
        assert r.json()["venues"] == []

    def test_read_failure_is_500(self, client_ds_catalogue: TestClient) -> None:
        with patch(_PATCH_READ_INDEX, side_effect=OSError("gcs unavailable")):
            r = client_ds_catalogue.get(
                "/data-status/catalogue-filter-options",
                params={"service": "market-tick-data-service", "asset_group": "prediction"},
            )
        assert r.status_code == 500

    def test_distinct_values_helper_strips_and_sorts(self) -> None:
        from deployment_api.routes.data_status._catalogue import _distinct_values

        df = pd.DataFrame({"venue": ["  OKX  ", "BINANCE", "OKX", "", None, "binance"]})
        # Distinct, stripped, blank/None dropped; sorted (case-sensitive so
        # "BINANCE" < "OKX" < "binance").
        assert _distinct_values(df, "venue") == ["BINANCE", "OKX", "binance"]
        assert _distinct_values(df, "absent_column") == []
        assert _distinct_values(pd.DataFrame(), "venue") == []

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


class TestDownloadCatalogueCsvPerAssetGroupSmoke:
    """Regression for data_status_catalogue_csv_download_500_sports_tradfi_2026_07_18:
    ``GET /download-catalogue-csv`` returned HTTP 500 for asset_group=tradfi and
    asset_group=sports in prod (2026-07-18) while cefi/defi/prediction returned 200.

    Root-caused to TWO distinct, unrelated failure modes sharing one symptom:

    * **tradfi** — a real, deterministic code bug. The endpoint built the FULL CSV
      via ``DataFrame.to_csv()`` and returned it as one buffered ``Response``. For a
      large asset_group (tradfi: 1,060,790 rows -> a 67 MiB CSV, measured live
      2026-07-20 against the real tradfi identity-catalogue bucket's
      ``prod/catalog.parquet``) that exceeds Cloud Run's ~32 MiB buffered-response cap
      — the PLATFORM rejects the oversized response before it reaches the client.
      Confirmed via Cloud Logging: the tradfi request's response carried a
      "Response size was too large" WARNING with NO Python traceback anywhere near
      it (row-building itself never raised). Fixed by streaming the CSV in bounded
      chunks (``_iter_catalogue_csv_chunks`` + ``StreamingResponse`` — chunked
      transfer encoding is not subject to the buffered-response cap). See
      ``TestDownloadCatalogueCsvStreamingBoundaries`` for the streaming-correctness
      proof.
    * **sports** — NOT a ``_catalogue.py`` bug. Cloud Logging showed a genuine
      ``RuntimeError`` (``ManifestConsolidatorStaleError``, raised by
      ``unified_trading_library.manifest_writer.read_availability_index``'s
      loud-fail-by-design guard) when the sports manifest consolidator fell behind
      mid-session — the SAME endpoint had returned 200 minutes earlier in the same
      testing session, and a live re-check 2026-07-20 succeeds cleanly. This is the
      SAME honest-absence contract ``/catalogue`` already enforces by design (see
      ``TestGetInstrumentCatalogue.test_manifest_read_failure_returns_500`` above) —
      swallowing it would fabricate data the manifest genuinely couldn't confirm.
      The tests below prove the CSV export still works end-to-end for sports (and
      every other asset_group) when the underlying read succeeds, which is the
      actual, permanent contract.
    """

    @pytest.mark.parametrize(
        ("asset_group", "expect_row_count"),
        [("cefi", 3), ("defi", 3), ("tradfi", 3)],
    )
    def test_identity_catalogue_asset_groups_download_200(
        self, client_ds_catalogue: TestClient, asset_group: str, expect_row_count: int
    ) -> None:
        from deployment_api.routes.data_status._catalogue import _CATALOGUE_CSV_COLUMNS

        with (
            patch(_PATCH_READ_IDENTITY_CATALOGUE, return_value=_real_schema_catalog_df()),
            patch(_PATCH_IS_MVP, side_effect=AssertionError("is_mvp must not be called for identity-catalogue rows")),
        ):
            r = client_ds_catalogue.get(
                "/data-status/download-catalogue-csv",
                params={"service": "instruments-service", "asset_group": asset_group},
            )
        assert r.status_code == 200
        assert r.headers["content-type"].startswith("text/csv")
        assert r.headers["X-Row-Count"] == str(expect_row_count)
        assert "BTC-USDT-PERP" in r.text
        assert "SOL-USDT-SWAP" in r.text
        # Header row present + exactly expect_row_count data rows (LF-terminated,
        # matching the pre-fix DataFrame.to_csv() format — see
        # _iter_catalogue_csv_chunks's lineterminator note).
        lines = r.text.splitlines()
        assert lines[0] == ",".join(_CATALOGUE_CSV_COLUMNS)
        assert len(lines) == expect_row_count + 1

    @pytest.mark.parametrize("asset_group", ["prediction", "sports"])
    def test_manifest_backed_asset_groups_download_200(self, client_ds_catalogue: TestClient, asset_group: str) -> None:
        with (
            patch(_PATCH_BUILD_BUCKET, return_value=f"market-data-tick-{asset_group}-prd-fake"),
            patch(_PATCH_READ_INDEX, return_value=_manifest_df()),
            patch(_PATCH_IS_MVP, return_value=True),
        ):
            r = client_ds_catalogue.get(
                "/data-status/download-catalogue-csv",
                params={"service": "market-tick-data-service", "asset_group": asset_group},
            )
        assert r.status_code == 200
        assert r.headers["content-type"].startswith("text/csv")
        assert r.headers["X-Row-Count"] == "2"
        assert "BTC-USDT-PERP" in r.text
        assert "ETH-USDT-PERP" in r.text

    def test_sports_manifest_read_failure_still_surfaces_as_500_not_swallowed(
        self, client_ds_catalogue: TestClient
    ) -> None:
        """The honest-absence contract this endpoint deliberately preserves
        (module docstring + TestGetInstrumentCatalogue.test_manifest_read_failure_
        returns_500): a genuine manifest read failure for a manifest-backed
        asset_group (prediction/sports) must still 500, never silently degrade to
        an empty/fabricated CSV. This is what actually happened in prod for
        sports on 2026-07-18 (a real, transient ManifestConsolidatorStaleError) —
        proving it is NOT a regression to "fix away" with a blanket except."""
        with (
            patch(_PATCH_BUILD_BUCKET, return_value="instruments-store-sports-prd-fake"),
            patch(_PATCH_READ_INDEX, side_effect=RuntimeError("Consolidated availability_index is stale")),
        ):
            r = client_ds_catalogue.get(
                "/data-status/download-catalogue-csv",
                params={"service": "instruments-service", "asset_group": "sports"},
            )
        assert r.status_code == 500


class TestDownloadCatalogueCsvStreamingBoundaries:
    """Streaming-correctness proof for the tradfi fix: the rewritten
    ``_iter_catalogue_csv_chunks`` (``StreamingResponse``, ``_CSV_STREAM_BATCH_ROWS``
    -row chunks) must produce BYTE-IDENTICAL output to the old single-buffer
    ``DataFrame.to_csv()`` approach — no row loss/duplication/reordering at chunk
    boundaries. Parametrized around ``_CSV_STREAM_BATCH_ROWS`` itself so a future
    change to the batch size keeps exercising the real boundary."""

    @staticmethod
    def _synthetic_identity_df(n: int) -> pd.DataFrame:
        return pd.DataFrame(
            [
                {
                    "instrument_id": f"SYN:INSTR:{i:07d}",
                    "venue": "CBOE" if i % 2 == 0 else "CME",
                    "instrument_type": "FUTURE",
                    "data_type": "",
                    "base_asset": "SYN",
                    "mvp": i % 3 == 0,
                }
                for i in range(n)
            ]
        )

    @pytest.mark.parametrize(
        "n_rows",
        [
            0,
            1,
            "batch_minus_1",
            "batch",
            "batch_plus_1",
            "two_batches_plus_7",
        ],
    )
    def test_streamed_csv_matches_pre_fix_buffered_reference(
        self, client_ds_catalogue: TestClient, n_rows: int | str
    ) -> None:
        from deployment_api.routes.data_status._catalogue import (
            _CATALOGUE_CSV_COLUMNS,
            _CSV_STREAM_BATCH_ROWS,
            _build_catalogue_rows,
        )

        resolved_n = {
            "batch_minus_1": _CSV_STREAM_BATCH_ROWS - 1,
            "batch": _CSV_STREAM_BATCH_ROWS,
            "batch_plus_1": _CSV_STREAM_BATCH_ROWS + 1,
            "two_batches_plus_7": 2 * _CSV_STREAM_BATCH_ROWS + 7,
        }.get(n_rows, n_rows)
        assert isinstance(resolved_n, int)
        df = self._synthetic_identity_df(resolved_n)

        with patch(_PATCH_READ_IDENTITY_CATALOGUE, return_value=df):
            r = client_ds_catalogue.get(
                "/data-status/download-catalogue-csv",
                params={"service": "instruments-service", "asset_group": "tradfi"},
            )
        assert r.status_code == 200
        assert r.headers["X-Row-Count"] == str(resolved_n)

        # Reference: the row-builder's OWN output run through the pre-fix
        # single-buffer pandas serialization — must match byte-for-byte.
        with patch(_PATCH_READ_IDENTITY_CATALOGUE, return_value=df):
            rows = _build_catalogue_rows(
                service="instruments-service",
                asset_group="tradfi",
                venue=None,
                instrument_type=None,
                data_type=None,
                search=None,
                mvp_only=False,
            )
        assert len(rows) == resolved_n
        expected_csv = pd.DataFrame(data=rows or None, columns=_CATALOGUE_CSV_COLUMNS).to_csv(index=False)
        assert r.text == expected_csv

    def test_response_is_chunked_streaming_not_a_single_buffer(self, client_ds_catalogue: TestClient) -> None:
        """Asserts the response class is actually ``StreamingResponse`` (chunked
        transfer, no upfront ``Content-Length``) — the architectural fix, not
        just a byte-content coincidence. A regression back to a single buffered
        ``Response`` would reintroduce the Cloud Run buffered-response-cap bug
        even if the CSV content stayed byte-identical for small fixtures."""
        import inspect

        from fastapi.responses import StreamingResponse

        from deployment_api.routes.data_status._catalogue import download_catalogue_csv

        # ``from __future__ import annotations`` stringifies annotations at
        # def-time; eval_str=True resolves it back through the function's own
        # module globals (where StreamingResponse is imported) for a real
        # class-identity check rather than a string comparison.
        assert inspect.signature(download_catalogue_csv, eval_str=True).return_annotation is StreamingResponse

        with patch(_PATCH_READ_IDENTITY_CATALOGUE, return_value=_real_schema_catalog_df()):
            r = client_ds_catalogue.get(
                "/data-status/download-catalogue-csv",
                params={"service": "instruments-service", "asset_group": "tradfi"},
            )
        assert r.status_code == 200
        # httpx/starlette TestClient surfaces a chunked StreamingResponse without
        # a Content-Length header (content-length is only set for fully-buffered
        # bodies); Transfer-Encoding is stripped by the test transport, so the
        # absence of Content-Length is the observable signal here.
        assert "content-length" not in {k.lower() for k in r.headers}


def _krx_identity_catalog_df() -> pd.DataFrame:
    """A tradfi identity-catalogue frame carrying the ``name`` display column the
    roll-up's ``_add_instrument_name`` stamps for KRX single-stock equities — the
    opaque 6-digit KRX code as ``instrument_id`` + a human-readable issuer ``name``.
    A non-KRX row with a blank name proves the column is honest-blank when absent."""
    return pd.DataFrame(
        [
            {
                "instrument_id": "KRX:EQUITY:005930",
                "name": "Samsung Electronics",
                "venue": "KRX",
                "instrument_type": "EQUITY",
                "data_type": "",
                "base_asset": "005930",
                "mvp": True,
            },
            {
                "instrument_id": "KRX:EQUITY:000660",
                "name": "SK Hynix",
                "venue": "KRX",
                "instrument_type": "EQUITY",
                "data_type": "",
                "base_asset": "000660",
                "mvp": True,
            },
            {
                "instrument_id": "CME:FUTURE:ESZ5",
                "name": "",
                "venue": "CME",
                "instrument_type": "FUTURE",
                "data_type": "",
                "base_asset": "SP500",
                "mvp": False,
            },
        ]
    )


class TestCatalogueNameColumn:
    """Regression for the KRX human-readable-name deliverable (2026-07-20): the
    catalogue ``name`` column (roll-up ``_add_instrument_name`` from the UAC
    ``KRX_EQUITY_NAMES`` SSOT) must surface on BOTH the JSON ``/catalogue`` route
    and the ``/download-catalogue-csv`` twin, next to the opaque coded
    ``instrument_id`` — so a KRX equity reads "Samsung Electronics" next to
    ``KRX:EQUITY:005930`` instead of a bare 6-digit code."""

    def test_json_catalogue_row_carries_name(self, client_ds_catalogue: TestClient) -> None:
        with (
            patch(_PATCH_READ_IDENTITY_CATALOGUE, return_value=_krx_identity_catalog_df()),
            patch(_PATCH_IS_MVP, side_effect=AssertionError("is_mvp must not be called for identity-catalogue rows")),
        ):
            r = client_ds_catalogue.get(
                "/data-status/catalogue",
                params={"service": "instruments-service", "asset_group": "tradfi"},
            )
        assert r.status_code == 200
        by_id = {inst["instrument_id"]: inst for inst in r.json()["instruments"]}
        assert by_id["KRX:EQUITY:005930"]["name"] == "Samsung Electronics"
        assert by_id["KRX:EQUITY:000660"]["name"] == "SK Hynix"
        # Honest-blank for an instrument with no display name (readable id already).
        assert by_id["CME:FUTURE:ESZ5"]["name"] == ""

    def test_csv_download_carries_name_column(self, client_ds_catalogue: TestClient) -> None:
        from deployment_api.routes.data_status._catalogue import _CATALOGUE_CSV_COLUMNS

        with (
            patch(_PATCH_READ_IDENTITY_CATALOGUE, return_value=_krx_identity_catalog_df()),
            patch(_PATCH_IS_MVP, side_effect=AssertionError("is_mvp must not be called for identity-catalogue rows")),
        ):
            r = client_ds_catalogue.get(
                "/data-status/download-catalogue-csv",
                params={"service": "instruments-service", "asset_group": "tradfi"},
            )
        assert r.status_code == 200
        assert r.headers["content-type"].startswith("text/csv")
        lines = r.text.splitlines()
        header = lines[0].split(",")
        # ``name`` sits immediately after ``instrument_id`` in the export.
        assert header == _CATALOGUE_CSV_COLUMNS
        assert header[0] == "instrument_id"
        assert header[1] == "name"
        name_idx = header.index("name")
        rows_by_id = {ln.split(",")[0]: ln.split(",") for ln in lines[1:]}
        assert rows_by_id["KRX:EQUITY:005930"][name_idx] == "Samsung Electronics"
        assert rows_by_id["KRX:EQUITY:000660"][name_idx] == "SK Hynix"
        assert rows_by_id["CME:FUTURE:ESZ5"][name_idx] == ""

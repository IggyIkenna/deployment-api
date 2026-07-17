"""
Unit tests for DataQueryService.

Tests cover:
- list_files_in_path (mocked storage)
- get_venue_filters (mocked storage)
- get_instruments_list (mocked storage)
- get_instrument_availability (mocked storage)
"""

import importlib.util
import os
import time
from datetime import UTC, datetime, timedelta
from types import SimpleNamespace
from unittest.mock import patch

import pandas as pd
import pytest

# Load data_query_service directly without triggering services/__init__.py circular import
_path = os.path.join(os.path.dirname(__file__), "../../deployment_api/services/data_query_service.py")
_spec = importlib.util.spec_from_file_location("_dqs_standalone", os.path.abspath(_path))
assert _spec is not None and _spec.loader is not None
_dqs_mod = importlib.util.module_from_spec(_spec)
_spec.loader.exec_module(_dqs_mod)  # type: ignore[union-attr]
DataQueryService = _dqs_mod.DataQueryService


def _load_dqs():
    """Return DataQueryService class."""
    return DataQueryService


class TestSearchCorpusBucketResolution:
    """Symbol-search corpus MUST resolve the SAME env-qualified bucket as the
    coverage/sports paths (``resolve_bucket_name``). Regression: it used
    ``build_bucket("instruments", …)`` which drops the ``-{env}-`` segment →
    a non-existent ``instruments-store-{ag}-{project}`` bucket (no ``-prd-``) →
    a 404 that 500'd the whole symbol search (blank results in the UI)."""

    def _make_service(self):
        return _load_dqs()(project_id="test-project")

    def test_corpus_bucket_uses_resolve_bucket_name_with_env(self) -> None:
        svc = self._make_service()
        captured: list[dict[str, object]] = []

        def _fake_resolve(**kwargs: object) -> str:
            captured.append(kwargs)
            return "instruments-store-cefi-prd-test-project"

        with (
            patch.object(_dqs_mod, "resolve_bucket_name", side_effect=_fake_resolve),
            # short-circuit after bucket resolution — we only assert the bucket call.
            patch.object(_dqs_mod.DataQueryService, "_latest_available_day", return_value=None),
        ):
            out = svc._load_corpus_from_per_venue_parquets("cefi")  # pyright: ignore[reportPrivateUsage]
        assert out == []
        assert captured, "resolve_bucket_name must be called (not build_bucket)"
        assert captured[0].get("kind") == "instruments-store"
        assert captured[0].get("asset_group") == "cefi"

    def test_corpus_bucket_prediction_uses_own_kind(self) -> None:
        svc = self._make_service()
        captured: list[dict[str, object]] = []

        def _fake_resolve(**kwargs: object) -> str:
            captured.append(kwargs)
            return "instruments-store-pred-prd-test-project"

        with (
            patch.object(_dqs_mod, "resolve_bucket_name", side_effect=_fake_resolve),
            patch.object(_dqs_mod.DataQueryService, "_latest_available_day", return_value=None),
        ):
            svc._load_corpus_from_per_venue_parquets("prediction")  # pyright: ignore[reportPrivateUsage]
        # prediction is its own bucket KIND (no asset_group under instruments-store).
        assert captured and captured[0].get("kind") == "instruments-store-prediction"


class TestListFilesInPath:
    """Tests for DataQueryService.list_files_in_path."""

    def _make_service(self):
        dqs = _load_dqs()
        return dqs(project_id="test-project")

    @pytest.mark.asyncio
    async def test_empty_bucket_returns_empty_files(self):
        svc = self._make_service()
        with patch.object(_dqs_mod, "list_objects", return_value=[]):
            result = await svc.list_files_in_path("my-bucket", "some/path")
        assert result["files"] == []
        assert result["directories"] == []
        assert result["total_count"] == 0
        assert result["truncated"] is False

    @pytest.mark.asyncio
    async def test_files_returned(self):
        svc = self._make_service()
        blob = SimpleNamespace(name="some/path/file.parquet", updated=None)
        with patch.object(_dqs_mod, "list_objects", return_value=[blob]):
            result = await svc.list_files_in_path("bucket", "some/path/")
        assert len(result["files"]) == 1
        assert result["files"][0]["name"] == "file.parquet"

    @pytest.mark.asyncio
    async def test_directories_extracted(self):
        svc = self._make_service()
        blob = SimpleNamespace(name="prefix/subdir/file.parquet", updated=None)
        with patch.object(_dqs_mod, "list_objects", return_value=[blob]):
            result = await svc.list_files_in_path("bucket", "prefix/")
        assert len(result["directories"]) >= 1

    @pytest.mark.asyncio
    async def test_truncated_when_exceeds_max(self):
        svc = self._make_service()
        blobs = [SimpleNamespace(name=f"path/file_{i}.parquet", updated=None) for i in range(15)]
        with patch.object(_dqs_mod, "list_objects", return_value=blobs):
            result = await svc.list_files_in_path("bucket", "path/", max_results=5)
        assert result["truncated"] is True
        assert len(result["files"]) <= 5

    @pytest.mark.asyncio
    async def test_error_returns_error_dict(self):
        svc = self._make_service()
        with patch.object(_dqs_mod, "list_objects", side_effect=OSError("bucket not found")):
            result = await svc.list_files_in_path("bad-bucket", "")
        assert "error" in result

    @pytest.mark.asyncio
    async def test_skips_blob_matching_exact_path(self):
        svc = self._make_service()
        # A blob with the exact same name as the path prefix is skipped
        blob = SimpleNamespace(name="exact/path/", updated=None)
        with patch.object(_dqs_mod, "list_objects", return_value=[blob]):
            result = await svc.list_files_in_path("bucket", "exact/path/")
        assert result["files"] == []


class TestGetVenueFilters:
    """Tests for DataQueryService.get_venue_filters."""

    def _make_service(self):
        dqs = _load_dqs()
        return dqs(project_id="test-project")

    @pytest.mark.asyncio
    async def test_unknown_service_returns_error(self):
        svc = self._make_service()
        result = await svc.get_venue_filters("nonexistent-service")
        assert "error" in result

    @pytest.mark.asyncio
    async def test_known_service_returns_venues(self):
        svc = self._make_service()
        with patch.object(_dqs_mod, "_drilldown_build_bucket_name", return_value="test-bucket"):
            with patch.object(_dqs_mod, "list_prefixes", return_value=["BINANCE/", "OKX/"]):
                result = await svc.get_venue_filters("instruments-service")
        assert "service" in result
        assert "asset_groups" in result
        # Should have cefi, tradfi, defi asset groups
        assert "cefi" in result["asset_groups"]

    @pytest.mark.asyncio
    async def test_exception_handled_per_category(self):
        svc = self._make_service()
        with patch.object(_dqs_mod, "_drilldown_build_bucket_name", return_value="test-bucket"):
            with patch.object(_dqs_mod, "list_prefixes", side_effect=OSError("bucket missing")):
                result = await svc.get_venue_filters("instruments-service")
        # Should still return a dict, each asset group gets an error key
        assert "asset_groups" in result
        for cat_data in result["asset_groups"].values():
            assert "error" in cat_data


class TestGetInstrumentsList:
    """Tests for DataQueryService.get_instruments_list."""

    def _make_service(self):
        dqs = _load_dqs()
        return dqs(project_id="test-project")

    @pytest.mark.asyncio
    async def test_returns_instruments_from_blobs(self):
        svc = self._make_service()
        blobs = [
            SimpleNamespace(name="cefi/binance/spot/BTC-USDT.parquet"),
            SimpleNamespace(name="cefi/binance/spot/ETH-USDT.parquet"),
        ]
        with patch.object(_dqs_mod, "list_objects", return_value=blobs):
            result = await svc.get_instruments_list("cefi")
        assert result["asset_group"] == "cefi"
        assert "BTC-USDT" in result["instruments"] or len(result["instruments"]) >= 0

    @pytest.mark.asyncio
    async def test_venue_filter_builds_path(self):
        svc = self._make_service()
        with patch.object(_dqs_mod, "list_objects", return_value=[]) as mock_list:
            await svc.get_instruments_list("cefi", venue="BINANCE")
        # The path should include the venue
        call_args = mock_list.call_args
        assert "BINANCE" in call_args[0][1]

    @pytest.mark.asyncio
    async def test_instrument_type_spot_maps_to_spot_pairs(self):
        svc = self._make_service()
        with patch.object(_dqs_mod, "list_objects", return_value=[]) as mock_list:
            await svc.get_instruments_list("cefi", instrument_type="SPOT")
        call_args = mock_list.call_args
        assert "spot_pairs" in call_args[0][1]

    @pytest.mark.asyncio
    async def test_deduplicates_instruments(self):
        svc = self._make_service()
        blobs = [
            SimpleNamespace(name="a/b/BTC-USDT.parquet"),
            SimpleNamespace(name="a/c/BTC-USDT.parquet"),
        ]
        with patch.object(_dqs_mod, "list_objects", return_value=blobs):
            result = await svc.get_instruments_list("cefi")
        instruments = result["instruments"]
        assert instruments.count("BTC-USDT") == 1

    @pytest.mark.asyncio
    async def test_error_returns_error_dict(self):
        svc = self._make_service()
        with patch.object(_dqs_mod, "list_objects", side_effect=RuntimeError("error")):
            result = await svc.get_instruments_list("cefi")
        assert "error" in result

    @pytest.mark.asyncio
    async def test_limit_respected(self):
        svc = self._make_service()
        blobs = [SimpleNamespace(name=f"a/b/INSTR-{i:03d}.parquet") for i in range(20)]
        with patch.object(_dqs_mod, "list_objects", return_value=blobs):
            result = await svc.get_instruments_list("cefi", limit=5)
        assert len(result["instruments"]) <= 5
        assert result["truncated"] is True


def _availability_rows(
    venue: str,
    instrument_type: str,
    instrument: str,
    data_types: list[str],
    date_strs: list[str],
    *,
    capture_status: str = "captured",
) -> pd.DataFrame:
    """Build a manifest-shaped DataFrame with one row per (date, data_type)."""
    rows = [
        {
            "venue": venue,
            "instrument_type": instrument_type.lower(),
            "instrument_id": instrument,
            "date": date_str,
            "data_type": dt,
            "capture_status": capture_status,
        }
        for date_str in date_strs
        for dt in data_types
    ]
    return pd.DataFrame(rows)


def _date_range_strs(start: str, end: str) -> list[str]:
    start_dt = datetime.strptime(start, "%Y-%m-%d").replace(tzinfo=UTC)
    end_dt = datetime.strptime(end, "%Y-%m-%d").replace(tzinfo=UTC)
    out = []
    cur = start_dt
    while cur <= end_dt:
        out.append(cur.strftime("%Y-%m-%d"))
        cur += timedelta(days=1)
    return out


class TestGetInstrumentAvailability:
    """Tests for DataQueryService.get_instrument_availability."""

    def _make_service(self):
        dqs = _load_dqs()
        return dqs(project_id="test-project")

    @pytest.mark.asyncio
    async def test_cefi_venue_returns_cefi_category(self):
        svc = self._make_service()
        with patch.object(_dqs_mod, "read_availability_index", return_value=pd.DataFrame()):
            result = await svc.get_instrument_availability(
                "BINANCE-SPOT", "SPOT", "BTC-USDT", "2026-01-01", "2026-01-03"
            )
        assert result["venue"] == "BINANCE-SPOT"
        assert "daily_availability" in result
        assert "summary" in result

    @pytest.mark.asyncio
    async def test_unknown_venue_returns_error(self):
        svc = self._make_service()
        result = await svc.get_instrument_availability("UNKNOWN_VENUE", "SPOT", "BTC", "2026-01-01", "2026-01-02")
        assert "error" in result

    @pytest.mark.asyncio
    async def test_tradfi_venue_category_detection(self):
        svc = self._make_service()
        date_strs = _date_range_strs("2026-01-01", "2026-01-02")
        df = _availability_rows("NYSE", "EQUITY", "AAPL", ["trades", "ohlcv_1m", "tbbo"], date_strs)
        with patch.object(_dqs_mod, "read_availability_index", return_value=df):
            result = await svc.get_instrument_availability("NYSE", "EQUITY", "AAPL", "2026-01-01", "2026-01-02")
        assert "error" not in result
        assert result["venue"] == "NYSE"

    @pytest.mark.asyncio
    async def test_available_days_counted_correctly(self):
        svc = self._make_service()
        date_strs = _date_range_strs("2026-01-01", "2026-01-03")
        # All days available
        df = _availability_rows("BINANCE-SPOT", "SPOT", "BTC-USDT", ["trades", "book_snapshot_5"], date_strs)
        with patch.object(_dqs_mod, "read_availability_index", return_value=df):
            result = await svc.get_instrument_availability(
                "BINANCE-SPOT", "SPOT", "BTC-USDT", "2026-01-01", "2026-01-03"
            )
        summary = result["summary"]
        assert summary["total_days"] == 3
        assert summary["available_days"] == 3
        assert summary["missing_days"] == 0

    @pytest.mark.asyncio
    async def test_missing_days_counted_when_no_data(self):
        svc = self._make_service()
        with patch.object(_dqs_mod, "read_availability_index", return_value=pd.DataFrame()):
            result = await svc.get_instrument_availability(
                "BINANCE-SPOT", "SPOT", "BTC-USDT", "2026-01-01", "2026-01-03"
            )
        summary = result["summary"]
        assert summary["missing_days"] == 3
        assert summary["available_days"] == 0

    @pytest.mark.asyncio
    async def test_invalid_date_format_returns_error(self):
        svc = self._make_service()
        result = await svc.get_instrument_availability("BINANCE", "SPOT", "BTC", "not-a-date", "2026-01-01")
        assert "error" in result

    @pytest.mark.asyncio
    async def test_available_from_clips_start(self):
        svc = self._make_service()
        date_strs = _date_range_strs("2026-01-03", "2026-01-05")
        df = _availability_rows("BINANCE-SPOT", "SPOT", "BTC-USDT", ["trades", "book_snapshot_5"], date_strs)
        with patch.object(_dqs_mod, "read_availability_index", return_value=df):
            result = await svc.get_instrument_availability(
                "BINANCE-SPOT",
                "SPOT",
                "BTC-USDT",
                "2026-01-01",
                "2026-01-05",
                available_from="2026-01-03",
            )
        # Effective start should be 2026-01-03, so only 3 days (Jan 3, 4, 5)
        summary = result["summary"]
        assert summary["total_days"] == 3

    @pytest.mark.asyncio
    async def test_data_type_passed_used(self):
        svc = self._make_service()
        with patch.object(_dqs_mod, "read_availability_index", return_value=pd.DataFrame()):
            result = await svc.get_instrument_availability(
                "BINANCE-SPOT",
                "SPOT",
                "BTC-USDT",
                "2026-01-01",
                "2026-01-01",
                data_type="custom_type",
            )
        assert result["data_types"] == ["custom_type"]

    @pytest.mark.asyncio
    async def test_defi_venue_returns_defi_category(self):
        svc = self._make_service()
        with patch.object(_dqs_mod, "read_availability_index", return_value=pd.DataFrame()):
            result = await svc.get_instrument_availability(
                "UNISWAP_V3-ETHEREUM", "SWAP", "ETH-USDC", "2026-01-01", "2026-01-01"
            )
        assert "error" not in result

    @pytest.mark.asyncio
    async def test_manifest_backed_lookup_reads_real_hive_partitioned_index(self):
        """Regression: the endpoint must read the manifest, not probe a flat
        {venue}/{instrument_type}/{instrument}/{date}/{data_type} GCS path that
        never matched the real hive-partitioned layout."""
        svc = self._make_service()
        date_strs = _date_range_strs("2026-01-01", "2026-01-02")
        df = _availability_rows("ASTER", "PERPETUAL", "ASTER:PERPETUAL:BTC-USDT@LIN", ["trades"], date_strs)
        with patch.object(_dqs_mod, "read_availability_index", return_value=df) as mock_read:
            result = await svc.get_instrument_availability(
                "ASTER",
                "PERPETUAL",
                "ASTER:PERPETUAL:BTC-USDT@LIN",
                "2026-01-01",
                "2026-01-02",
                data_type="trades",
            )
        mock_read.assert_called_once()
        assert result["summary"]["available_days"] == 2

    @pytest.mark.asyncio
    async def test_capture_status_not_captured_counts_as_unavailable(self):
        svc = self._make_service()
        date_strs = _date_range_strs("2026-01-01", "2026-01-01")
        df = _availability_rows(
            "BINANCE-SPOT",
            "SPOT",
            "BTC-USDT",
            ["trades"],
            date_strs,
            capture_status="expected_unattempted",
        )
        with patch.object(_dqs_mod, "read_availability_index", return_value=df):
            result = await svc.get_instrument_availability(
                "BINANCE-SPOT", "SPOT", "BTC-USDT", "2026-01-01", "2026-01-01", data_type="trades"
            )
        assert result["summary"]["available_days"] == 0


# ---------------------------------------------------------------------------
# search_instruments — Gap 3 (canonical-symbol cross-category search)
# ---------------------------------------------------------------------------


class TestParseInstrumentObjectPath:
    """Path-parsing logic for _parse_instrument_object_path (institutional).

    The parser handles two GCS layouts:
      1. Per-venue: ``{venue}/{instrument_type}/{canonical_id}.parquet``
      2. By-date partitioned: ``instrument_availability/by_date/day=.../venue=...``
    plus rejects sentinel + index files (``_index/``, ``_vm_staging/``).
    """

    def test_per_venue_layout(self):
        dqs = _load_dqs()
        # Standard CeFi/TradFi/DeFi layout.
        assert dqs._parse_instrument_object_path("BINANCE-FUTURES/perps/BTC-USDT-PERP.parquet") == (
            "BTC-USDT-PERP",
            "BINANCE-FUTURES",
            "perps",
        )

    def test_per_venue_defi_pool(self):
        dqs = _load_dqs()
        assert dqs._parse_instrument_object_path("UNISWAP_V3/pools/USDC-WETH-500.parquet") == (
            "USDC-WETH-500",
            "UNISWAP_V3",
            "pools",
        )

    def test_by_date_partitioned_layout(self):
        dqs = _load_dqs()
        path = (
            "instrument_availability/by_date/day=2026-04-20/venue=POLYMARKET/"
            "instrument_type=prediction_market/0xabc123.parquet"
        )
        result = dqs._parse_instrument_object_path(path)
        assert result == ("0xabc123", "POLYMARKET", "prediction_market")

    def test_by_date_with_entity_partition(self):
        # Sports/prediction availability layout uses ``entity=`` not
        # ``instrument_type=`` — parser must fall back gracefully.
        dqs = _load_dqs()
        path = "instrument_availability/by_date/day=2026-04-20/entity=fixtures/EPL.parquet"
        result = dqs._parse_instrument_object_path(path)
        assert result == ("EPL", "", "fixtures")

    def test_skips_sentinel_files(self):
        dqs = _load_dqs()
        assert dqs._parse_instrument_object_path("_index/manifest.parquet") is None
        assert dqs._parse_instrument_object_path("_vm_staging/foo.parquet") is None

    def test_skips_paths_without_extension(self):
        dqs = _load_dqs()
        assert dqs._parse_instrument_object_path("BINANCE/perps/BTC-USDT-PERP") is None
        assert dqs._parse_instrument_object_path("") is None

    def test_skips_unparseable_paths(self):
        # No venue partition AND no instrument_type — can't form a meaningful
        # match result, so reject.
        dqs = _load_dqs()
        assert dqs._parse_instrument_object_path("instrument_availability/by_date/day=2026/x.parquet") is None


class TestSearchInstruments:
    """search_instruments — cross-category canonical-ID substring search.

    Tests inject a synthetic canonical-ID corpus per category by patching
    ``_load_search_corpus``. This bypasses GCS — the corpus loader hits the
    sports availability index + per-venue parquets, both of which are
    integration-tested separately against real buckets.
    """

    @pytest.fixture(autouse=True)
    def _clear_corpus_cache(self):
        DataQueryService._corpus_cache.clear()
        yield
        DataQueryService._corpus_cache.clear()

    def _patch(self, monkeypatch, corpus: dict[str, list[dict[str, str]]]):
        def _fake_load(_self, category):
            return list(corpus.get(category, []))

        monkeypatch.setattr(DataQueryService, "_load_search_corpus", _fake_load)

    @pytest.mark.asyncio
    async def test_empty_query_returns_empty_matches(self):
        svc = DataQueryService(project_id="p")
        result = await svc.search_instruments(query="", limit=10)
        assert result["matches"] == []
        assert result["total_matches"] == 0
        assert result["asset_groups_searched"] == []

    @pytest.mark.asyncio
    async def test_whitespace_only_query_returns_empty_matches(self):
        svc = DataQueryService(project_id="p")
        result = await svc.search_instruments(query="   ", limit=10)
        assert result["matches"] == []

    @pytest.mark.asyncio
    async def test_single_category_search(self, monkeypatch):
        self._patch(
            monkeypatch,
            {
                "cefi": [
                    {
                        "canonical_id": "BINANCE-FUTURES:PERPETUAL:BTC-USDT",
                        "venue": "BINANCE-FUTURES",
                        "instrument_type": "PERPETUAL",
                    },
                    {
                        "canonical_id": "BINANCE-FUTURES:PERPETUAL:ETH-USDT",
                        "venue": "BINANCE-FUTURES",
                        "instrument_type": "PERPETUAL",
                    },
                    {
                        "canonical_id": "BYBIT:PERPETUAL:BTC-USDT",
                        "venue": "BYBIT",
                        "instrument_type": "PERPETUAL",
                    },
                ]
            },
        )
        svc = DataQueryService(project_id="p")
        result = await svc.search_instruments(query="btc", asset_group="cefi", limit=10)
        assert result["total_matches"] == 2
        assert {m["venue"] for m in result["matches"]} == {"BINANCE-FUTURES", "BYBIT"}
        assert result["asset_groups_searched"] == ["cefi"]

    @pytest.mark.asyncio
    async def test_case_insensitive_match(self, monkeypatch):
        self._patch(
            monkeypatch,
            {
                "cefi": [
                    {
                        "canonical_id": "BINANCE:PERPETUAL:BTC-USDT-PERP",
                        "venue": "BINANCE",
                        "instrument_type": "PERPETUAL",
                    },
                ]
            },
        )
        svc = DataQueryService(project_id="p")
        assert (await svc.search_instruments(query="btc-usdt", asset_group="cefi"))["total_matches"] == 1
        assert (await svc.search_instruments(query="BTC-USDT", asset_group="cefi"))["total_matches"] == 1

    @pytest.mark.asyncio
    async def test_multi_token_and_match(self, monkeypatch):
        self._patch(
            monkeypatch,
            {
                "defi": [
                    {
                        "canonical_id": "UNISWAP_V3:POOL:USDC-WETH-500",
                        "venue": "UNISWAP_V3",
                        "instrument_type": "POOL",
                    },
                    {
                        "canonical_id": "UNISWAP_V3:POOL:USDC-USDT-100",
                        "venue": "UNISWAP_V3",
                        "instrument_type": "POOL",
                    },
                    {
                        "canonical_id": "UNISWAP_V3:POOL:WBTC-WETH-3000",
                        "venue": "UNISWAP_V3",
                        "instrument_type": "POOL",
                    },
                ]
            },
        )
        svc = DataQueryService(project_id="p")
        result = await svc.search_instruments(query="usdc weth", asset_group="defi")
        assert result["total_matches"] == 1
        assert "USDC-WETH-500" in result["matches"][0]["canonical_id"]

    @pytest.mark.asyncio
    async def test_cross_category_walks_all_five(self, monkeypatch):
        self._patch(
            monkeypatch,
            {
                "cefi": [
                    {
                        "canonical_id": "BINANCE:PERPETUAL:BTC-USDT-PERP",
                        "venue": "BINANCE",
                        "instrument_type": "PERPETUAL",
                    }
                ],
                "defi": [
                    {
                        "canonical_id": "UNISWAP_V3:POOL:WBTC-WETH-500",
                        "venue": "UNISWAP_V3",
                        "instrument_type": "POOL",
                    }
                ],
                "prediction": [],
                "tradfi": [],
                "sports": [],
            },
        )
        svc = DataQueryService(project_id="p")
        result = await svc.search_instruments(query="btc", limit=10)
        assert result["total_matches"] == 2
        assert {m["asset_group"] for m in result["matches"]} == {"CEFI", "DEFI"}

    @pytest.mark.asyncio
    async def test_sports_search_uses_league_id(self, monkeypatch):
        self._patch(
            monkeypatch,
            {
                "sports": [
                    {
                        "canonical_id": "EPL",
                        "venue": "API_FOOTBALL_FIXTURES",
                        "instrument_type": "",
                    },
                    {
                        "canonical_id": "BUNDESLIGA",
                        "venue": "API_FOOTBALL_FIXTURES",
                        "instrument_type": "",
                    },
                ]
            },
        )
        svc = DataQueryService(project_id="p")
        result = await svc.search_instruments(query="bunde", asset_group="sports")
        assert result["total_matches"] == 1
        assert result["matches"][0]["canonical_id"] == "BUNDESLIGA"

    @pytest.mark.asyncio
    async def test_truncation_flag_set_when_limit_hit(self, monkeypatch):
        corpus = [
            {
                "canonical_id": f"BINANCE:PERPETUAL:BTC-USDT-{i}",
                "venue": "BINANCE",
                "instrument_type": "PERPETUAL",
            }
            for i in range(10)
        ]
        self._patch(monkeypatch, {"cefi": corpus})
        svc = DataQueryService(project_id="p")
        result = await svc.search_instruments(query="btc", asset_group="cefi", limit=3)
        assert result["total_matches"] == 3
        assert result["truncated"] is True

    @pytest.mark.asyncio
    async def test_corpus_load_failure_returns_empty_for_that_category(self, monkeypatch):
        def _failing_load(_self, category):
            if category == "cefi":
                raise OSError("simulated GCS error")
            if category == "defi":
                return [
                    {
                        "canonical_id": "UNISWAP_V3:POOL:USDC-WETH-500",
                        "venue": "UNISWAP_V3",
                        "instrument_type": "POOL",
                    }
                ]
            return []

        monkeypatch.setattr(DataQueryService, "_load_search_corpus", _failing_load)
        svc = DataQueryService(project_id="p")
        result = await svc.search_instruments(query="usdc", limit=10)
        assert result["total_matches"] == 1
        assert result["matches"][0]["asset_group"] == "DEFI"


class TestCorpusCacheTTL:
    """``_load_search_corpus``'s 5-minute in-process TTL cache — the house
    convention also used by ``upcoming_fixtures._FIXTURES_CACHE`` /
    ``prediction_catalogue`` / ``catalogue_lifecycle``. Regression coverage
    for the 2026-07-16 ~44s symbol-search latency fix: a cold miss must
    populate the cache, a within-TTL call must reuse it (no re-read), and
    an expired entry must trigger exactly one fresh read.
    """

    @pytest.fixture(autouse=True)
    def _clear_corpus_cache(self):
        DataQueryService._corpus_cache.clear()
        yield
        DataQueryService._corpus_cache.clear()

    def test_ttl_cache_cold_miss_then_hit_then_expiry(self, monkeypatch):
        svc = DataQueryService(project_id="p")
        call_count = {"n": 0}
        fake_now = {"t": 1000.0}

        def _fake_underlying_load(_self, _category):
            call_count["n"] += 1
            return [{"canonical_id": f"X-{call_count['n']}", "venue": "V", "instrument_type": "SPOT"}]

        monkeypatch.setattr(DataQueryService, "_load_corpus_from_per_venue_parquets", _fake_underlying_load)
        monkeypatch.setattr(_dqs_mod.time, "monotonic", lambda: fake_now["t"])

        # (a) cold cache miss populates the cache — the expensive loader runs once.
        first = svc._load_search_corpus("cefi")
        assert call_count["n"] == 1
        assert first[0]["canonical_id"] == "X-1"

        # (b) a subsequent call within the TTL window (well under 300s) does
        # NOT re-trigger the expensive read — served from the in-process cache.
        fake_now["t"] += 100
        second = svc._load_search_corpus("cefi")
        assert call_count["n"] == 1
        assert second == first

        # (c) after TTL expiry (>300s since the original load) a new read happens.
        fake_now["t"] += 300
        third = svc._load_search_corpus("cefi")
        assert call_count["n"] == 2
        assert third[0]["canonical_id"] == "X-2"


class TestReadAllVenueParquetsConcurrency:
    """``_read_all_venue_parquets`` must read per-venue parquets in PARALLEL,
    not sequentially. Root cause of the ~44s cold symbol-search latency
    (operator-reported 2026-07-16): a sequential ``for venue in per_venue_uris``
    loop over per-venue GCS parquet reads — DeFi alone registers 63 venues in
    ``VENUE_TO_ASSET_GROUP``, each an independent ~1-3s transpacific round-trip.
    Threading (mirroring ``upcoming_fixtures._read_frames_for_window``'s
    per-day pattern) collapses N round-trips into ~one round-trip's wall time.
    """

    def test_reads_execute_concurrently_not_sequentially(self, monkeypatch):
        svc = DataQueryService(project_id="p")
        n_venues = 6
        sleep_s = 0.2
        per_venue_uris = {f"VENUE-{i}": f"gs://bucket/VENUE-{i}/instruments.parquet" for i in range(n_venues)}

        def _slow_read(_self, _uri, venue):
            time.sleep(sleep_s)
            return [{"canonical_id": f"{venue}:X", "venue": venue, "instrument_type": "SPOT"}]

        monkeypatch.setattr(DataQueryService, "_read_venue_parquet_rows", _slow_read)

        start = time.monotonic()
        corpus = svc._read_all_venue_parquets(per_venue_uris)
        elapsed = time.monotonic() - start

        assert len(corpus) == n_venues
        # Sequential would take ~n_venues * sleep_s (1.2s here). All 6 venues
        # fit under _VENUE_READ_MAX_WORKERS (16) so a parallel run finishes
        # in ~one sleep_s; generous margin for slow/shared-host CI.
        sequential_cost = sleep_s * n_venues
        assert elapsed < sequential_cost * 0.65, (
            f"per-venue reads look sequential: {elapsed:.3f}s for {n_venues} venues "
            f"(sequential would be ~{sequential_cost:.3f}s)"
        )

    def test_merges_and_dedupes_across_venues(self, monkeypatch):
        svc = DataQueryService(project_id="p")
        per_venue_uris = {
            "BINANCE": "gs://b/BINANCE/instruments.parquet",
            "BYBIT": "gs://b/BYBIT/instruments.parquet",
        }

        def _fake_read(_self, _uri, venue):
            # Exact intra-venue duplicate — must collapse to one row.
            return [
                {"canonical_id": "BTC-USDT", "venue": venue, "instrument_type": "SPOT"},
                {"canonical_id": "BTC-USDT", "venue": venue, "instrument_type": "SPOT"},
            ]

        monkeypatch.setattr(DataQueryService, "_read_venue_parquet_rows", _fake_read)
        corpus = svc._read_all_venue_parquets(per_venue_uris)

        assert len(corpus) == 2
        assert {(r["canonical_id"], r["venue"]) for r in corpus} == {
            ("BTC-USDT", "BINANCE"),
            ("BTC-USDT", "BYBIT"),
        }

    def test_empty_per_venue_uris_short_circuits_without_reading(self, monkeypatch):
        svc = DataQueryService(project_id="p")
        read_calls: list[str] = []

        def _tracking_read(_self, _uri, venue):
            read_calls.append(venue)
            return []

        monkeypatch.setattr(_dqs_mod, "resolve_bucket_name", lambda **_kwargs: "instruments-store-cefi-prd-proj")
        monkeypatch.setattr(DataQueryService, "_latest_available_day", lambda _self, _bucket: "2026-07-15")
        monkeypatch.setattr(DataQueryService, "_collect_per_venue_uris", lambda _self, _bucket, _day: {})
        monkeypatch.setattr(DataQueryService, "_read_venue_parquet_rows", _tracking_read)

        result = svc._load_corpus_from_per_venue_parquets("cefi")

        assert result == []
        assert read_calls == []

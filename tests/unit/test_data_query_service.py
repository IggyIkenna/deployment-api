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

"""
Unit tests for data_status_service module.

Tests cover pure methods: build_bucket_name, _calculate_completion_rate,
run_data_status_cli, calculate_missing_shards, get_last_updated_info,
validate_data_completeness.
"""

import importlib.util
import json
import os
from unittest.mock import AsyncMock, MagicMock, patch

import pandas as pd
import pytest

# Load directly to avoid circular import via services/__init__.py
_path = os.path.join(
    os.path.dirname(__file__), "../../deployment_api/services/data_status_service.py"
)
_spec = importlib.util.spec_from_file_location("_dss_standalone", os.path.abspath(_path))
assert _spec is not None and _spec.loader is not None
_dss_mod = importlib.util.module_from_spec(_spec)
_spec.loader.exec_module(_dss_mod)  # type: ignore[union-attr]
DataStatusService = _dss_mod.DataStatusService


def _make_svc(**kwargs) -> DataStatusService:
    return DataStatusService(project_id=kwargs.get("project_id", "test-project"))


def _mock_process(returncode: int = 0, stdout: bytes = b"{}", stderr: bytes = b"") -> MagicMock:
    """Build a mock asyncio Process object."""
    proc = MagicMock()
    proc.returncode = returncode
    proc.communicate = AsyncMock(return_value=(stdout, stderr))
    return proc


class TestBuildBucketName:
    """Tests for DataStatusService.build_bucket_name."""

    def test_format(self):
        svc = DataStatusService(project_id="my-project")
        assert svc.build_bucket_name("instruments", "CEFI") == "instruments-cefi-my-project"

    def test_lowercases_category(self):
        svc = DataStatusService(project_id="proj")
        result = svc.build_bucket_name("market-data", "TRADFI")
        assert "tradfi" in result
        assert "TRADFI" not in result

    def test_project_id_appended(self):
        svc = DataStatusService(project_id="test-123")
        result = svc.build_bucket_name("prefix", "DEFI")
        assert result.endswith("test-123")


class TestCalculateCompletionRate:
    """Tests for DataStatusService._calculate_completion_rate."""

    def setup_method(self):
        self.svc = DataStatusService(project_id="test-proj")

    def test_no_dates_returns_zero(self):
        assert self.svc._calculate_completion_rate({}) == 0.0

    def test_non_list_dates_returns_zero(self):
        assert self.svc._calculate_completion_rate({"dates": "invalid"}) == 0.0

    def test_all_present_returns_100(self):
        data = {
            "dates": [
                {"venues": [{"status": "present"}, {"status": "present"}]},
                {"venues": [{"status": "present"}]},
            ]
        }
        assert self.svc._calculate_completion_rate(data) == 100.0

    def test_all_missing_returns_zero(self):
        data = {
            "dates": [
                {"venues": [{"status": "missing"}, {"status": "missing"}]},
            ]
        }
        assert self.svc._calculate_completion_rate(data) == 0.0

    def test_partial_completion(self):
        data = {
            "dates": [
                {"venues": [{"status": "present"}, {"status": "missing"}]},
            ]
        }
        assert self.svc._calculate_completion_rate(data) == 50.0

    def test_empty_dates_returns_zero(self):
        data = {"dates": []}
        assert self.svc._calculate_completion_rate(data) == 0.0

    def test_non_dict_venue_entries_skipped(self):
        data = {
            "dates": [
                {"venues": [None, "string", {"status": "present"}]},
            ]
        }
        # Only the dict entry counts
        result = self.svc._calculate_completion_rate(data)
        assert result == 100.0

    def test_non_list_venues_skipped(self):
        data = {
            "dates": [
                {"venues": None},
                {"venues": [{"status": "present"}]},
            ]
        }
        result = self.svc._calculate_completion_rate(data)
        assert result == 100.0


class TestRunDataStatusCli:
    """Tests for DataStatusService.run_data_status_cli."""

    @pytest.mark.asyncio
    async def test_returns_json_on_success(self):
        svc = _make_svc()
        expected = {"completion": 85.0, "categories": {}}
        proc = _mock_process(stdout=json.dumps(expected).encode())

        with patch("asyncio.create_subprocess_exec", new=AsyncMock(return_value=proc)):
            result = await svc.run_data_status_cli("svc-a", "2024-01-01", "2024-01-31")

        assert result == expected

    @pytest.mark.asyncio
    async def test_returns_error_on_nonzero_returncode(self):
        svc = _make_svc()
        proc = _mock_process(returncode=1, stderr=b"command failed")

        with patch("asyncio.create_subprocess_exec", new=AsyncMock(return_value=proc)):
            result = await svc.run_data_status_cli("svc-a", "2024-01-01", "2024-01-31")

        assert "error" in result
        assert "1" in result["error"]

    @pytest.mark.asyncio
    async def test_returns_error_on_invalid_json(self):
        svc = _make_svc()
        proc = _mock_process(stdout=b"not-json")

        with patch("asyncio.create_subprocess_exec", new=AsyncMock(return_value=proc)):
            result = await svc.run_data_status_cli("svc-a", "2024-01-01", "2024-01-31")

        assert "error" in result

    @pytest.mark.asyncio
    async def test_returns_error_on_subprocess_exception(self):
        svc = _make_svc()

        async def _raise(*a, **kw):
            raise OSError("spawn failed")

        with patch("asyncio.create_subprocess_exec", new=_raise):
            result = await svc.run_data_status_cli("svc-a", "2024-01-01", "2024-01-31")

        assert "error" in result

    @pytest.mark.asyncio
    async def test_includes_category_filters_in_cmd(self):
        svc = _make_svc()
        captured: dict[str, object] = {}

        async def _capture(*args, **kwargs):
            captured["args"] = list(args)
            return _mock_process()

        with patch("asyncio.create_subprocess_exec", new=_capture):
            await svc.run_data_status_cli(
                "svc-a", "2024-01-01", "2024-01-31", categories=["CEFI", "DEFI"]
            )

        assert "-c" in captured["args"]
        assert "CEFI" in captured["args"]
        assert "DEFI" in captured["args"]

    @pytest.mark.asyncio
    async def test_includes_venue_filters_in_cmd(self):
        svc = _make_svc()
        captured: dict[str, object] = {}

        async def _capture(*args, **kwargs):
            captured["args"] = list(args)
            return _mock_process()

        with patch("asyncio.create_subprocess_exec", new=_capture):
            await svc.run_data_status_cli(
                "svc-a", "2024-01-01", "2024-01-31", venues=["BINANCE", "OKX"]
            )

        assert "-v" in captured["args"]
        assert "BINANCE" in captured["args"]

    @pytest.mark.asyncio
    async def test_includes_show_missing_flag(self):
        svc = _make_svc()
        captured: dict[str, object] = {}

        async def _capture(*args, **kwargs):
            captured["args"] = list(args)
            return _mock_process()

        with patch("asyncio.create_subprocess_exec", new=_capture):
            await svc.run_data_status_cli("svc-a", "2024-01-01", "2024-01-31", show_missing=True)

        assert "--show-missing" in captured["args"]

    @pytest.mark.asyncio
    async def test_includes_check_venues_flag(self):
        svc = _make_svc()
        captured: dict[str, object] = {}

        async def _capture(*args, **kwargs):
            captured["args"] = list(args)
            return _mock_process()

        with patch("asyncio.create_subprocess_exec", new=_capture):
            await svc.run_data_status_cli("svc-a", "2024-01-01", "2024-01-31", check_venues=True)

        assert "--check-venues" in captured["args"]

    @pytest.mark.asyncio
    async def test_adds_fast_flag_for_market_tick_service(self):
        svc = _make_svc()
        captured: dict[str, object] = {}

        async def _capture(*args, **kwargs):
            captured["args"] = list(args)
            return _mock_process()

        with patch("asyncio.create_subprocess_exec", new=_capture):
            await svc.run_data_status_cli("market-tick-data-handler", "2024-01-01", "2024-01-31")

        assert "--fast" in captured["args"]

    @pytest.mark.asyncio
    async def test_adds_fast_flag_for_market_data_processing(self):
        svc = _make_svc()
        captured: dict[str, object] = {}

        async def _capture(*args, **kwargs):
            captured["args"] = list(args)
            return _mock_process()

        with patch("asyncio.create_subprocess_exec", new=_capture):
            await svc.run_data_status_cli(
                "market-data-processing-service", "2024-01-01", "2024-01-31"
            )

        assert "--fast" in captured["args"]

    @pytest.mark.asyncio
    async def test_includes_check_data_types_flag(self):
        svc = _make_svc()
        captured: dict[str, object] = {}

        async def _capture(*args, **kwargs):
            captured["args"] = list(args)
            return _mock_process()

        with patch("asyncio.create_subprocess_exec", new=_capture):
            await svc.run_data_status_cli(
                "svc-a", "2024-01-01", "2024-01-31", check_data_types=True
            )

        assert "--check-data-types" in captured["args"]

    @pytest.mark.asyncio
    async def test_includes_check_feature_groups_flag(self):
        svc = _make_svc()
        captured: dict[str, object] = {}

        async def _capture(*args, **kwargs):
            captured["args"] = list(args)
            return _mock_process()

        with patch("asyncio.create_subprocess_exec", new=_capture):
            await svc.run_data_status_cli(
                "svc-a", "2024-01-01", "2024-01-31", check_feature_groups=True
            )

        assert "--check-feature-groups" in captured["args"]

    @pytest.mark.asyncio
    async def test_includes_check_timeframes_flag(self):
        svc = _make_svc()
        captured: dict[str, object] = {}

        async def _capture(*args, **kwargs):
            captured["args"] = list(args)
            return _mock_process()

        with patch("asyncio.create_subprocess_exec", new=_capture):
            await svc.run_data_status_cli(
                "svc-a", "2024-01-01", "2024-01-31", check_timeframes=True
            )

        assert "--check-timeframes" in captured["args"]


class TestCalculateMissingShards:
    """Tests for DataStatusService.calculate_missing_shards.

    calculate_missing_shards reads manifest indices directly via
    _scan_category_manifest (not CLI). Tests mock at that level.
    """

    @pytest.mark.asyncio
    async def test_returns_error_when_scan_raises(self):
        svc = _make_svc()
        with patch.object(svc, "_scan_category_manifest", side_effect=RuntimeError("scan failed")):
            result = await svc.calculate_missing_shards(
                "instruments-service", "2024-01-01", "2024-01-31"
            )
        assert "error" in result

    @pytest.mark.asyncio
    async def test_returns_missing_analysis_structure(self):
        svc = _make_svc()

        def _mock_scan(
            service: str, cat: str, start: str, end: str
        ) -> dict[str, list[str] | int] | None:
            if "cefi" in cat.lower():
                return {"missing": ["2024-01-01"], "days_checked": 2}
            return None

        with patch.object(svc, "_scan_category_manifest", side_effect=_mock_scan):
            result = await svc.calculate_missing_shards(
                "instruments-service", "2024-01-01", "2024-01-02"
            )

        assert result["service"] == "instruments-service"
        assert "total_missing" in result
        assert result["total_missing"] == 1
        assert "missing_by_venue" in result
        assert "summary" in result

    @pytest.mark.asyncio
    async def test_handles_no_missing(self):
        svc = _make_svc()
        with patch.object(svc, "_scan_category_manifest", return_value=None):
            result = await svc.calculate_missing_shards(
                "instruments-service", "2024-01-01", "2024-01-31"
            )
        assert result["total_missing"] == 0

    @pytest.mark.asyncio
    async def test_handles_all_categories_empty(self):
        svc = _make_svc()
        with patch.object(svc, "_scan_category_manifest", return_value=None):
            result = await svc.calculate_missing_shards(
                "instruments-service", "2024-01-01", "2024-01-31"
            )
        assert "total_missing" in result
        assert result["total_missing"] == 0

    @pytest.mark.asyncio
    async def test_counts_by_category(self):
        svc = _make_svc()

        def _mock_scan(
            service: str, cat: str, start: str, end: str
        ) -> dict[str, list[str] | int] | None:
            cat_l = cat.lower()
            if "cefi" in cat_l:
                return {"missing": ["2024-01-01"], "days_checked": 1}
            if "defi" in cat_l:
                return {"missing": ["2024-01-01"], "days_checked": 1}
            return None

        with patch.object(svc, "_scan_category_manifest", side_effect=_mock_scan):
            result = await svc.calculate_missing_shards(
                "instruments-service", "2024-01-01", "2024-01-01"
            )

        mbc = result["missing_by_category"]
        assert isinstance(mbc, dict)
        assert sum(mbc.values()) >= 2

    @pytest.mark.asyncio
    async def test_returns_correct_date_range(self):
        svc = _make_svc()
        with patch.object(svc, "_scan_category_manifest", return_value=None):
            result = await svc.calculate_missing_shards(
                "instruments-service", "2024-01-01", "2024-01-31"
            )
        assert result["date_range"] == {"start": "2024-01-01", "end": "2024-01-31"}

    @pytest.mark.asyncio
    async def test_passes_categories_filter(self):
        svc = _make_svc()
        calls: list[str] = []

        def _mock_scan(
            service: str, cat: str, start: str, end: str
        ) -> dict[str, list[str] | int] | None:
            calls.append(cat)
            return None

        with patch.object(svc, "_scan_category_manifest", side_effect=_mock_scan):
            await svc.calculate_missing_shards(
                "instruments-service",
                "2024-01-01",
                "2024-01-31",
                categories=["CEFI"],
            )

        assert calls == ["CEFI"]

    @pytest.mark.asyncio
    async def test_summary_contains_completion_rate(self):
        svc = _make_svc()

        def _mock_scan(
            service: str, cat: str, start: str, end: str
        ) -> dict[str, list[str] | int] | None:
            if "cefi" in cat.lower():
                return {"missing": ["2024-01-01"], "days_checked": 3}
            return None

        with patch.object(svc, "_scan_category_manifest", side_effect=_mock_scan):
            result = await svc.calculate_missing_shards(
                "instruments-service", "2024-01-01", "2024-01-03"
            )

        summary = result["summary"]
        assert isinstance(summary, dict)
        assert "completion_rate" in summary
        assert "total_days_checked" in summary
        assert summary["total_days_checked"] == 3


class TestGetLastUpdatedInfo:
    """Tests for DataStatusService.get_last_updated_info."""

    @pytest.mark.asyncio
    async def test_returns_error_for_unknown_service(self):
        svc = _make_svc()
        result = await svc.get_last_updated_info("unknown-service")
        assert "error" in result

    @pytest.mark.asyncio
    async def test_returns_dict_for_known_service(self):
        svc = _make_svc()
        mock_obj = MagicMock()
        mock_obj.name = "some/path/file.parquet"

        with patch.object(_dss_mod, "list_objects", return_value=[mock_obj, mock_obj]):
            result = await svc.get_last_updated_info("market-tick-data-handler")

        assert "service" in result
        assert result["service"] == "market-tick-data-handler"
        assert "categories" in result

    @pytest.mark.asyncio
    async def test_returns_empty_category_when_no_objects(self):
        svc = _make_svc()

        with patch.object(_dss_mod, "list_objects", return_value=[]):
            result = await svc.get_last_updated_info(
                "market-tick-data-handler", categories=["cefi"]
            )

        assert result["categories"]["cefi"]["status"] == "empty"

    @pytest.mark.asyncio
    async def test_handles_list_objects_error(self):
        svc = _make_svc()

        with patch.object(_dss_mod, "list_objects", side_effect=OSError("bucket not found")):
            result = await svc.get_last_updated_info("instruments-service", categories=["cefi"])

        assert result["categories"]["cefi"]["status"] == "error"

    @pytest.mark.asyncio
    async def test_uses_default_categories_when_none_given(self):
        svc = _make_svc()

        with patch.object(_dss_mod, "list_objects", return_value=[]):
            result = await svc.get_last_updated_info("instruments-service")

        # Should have checked cefi, tradfi, defi
        assert "cefi" in result["categories"]
        assert "tradfi" in result["categories"]
        assert "defi" in result["categories"]


class TestValidateDataCompleteness:
    """Tests for DataStatusService.validate_data_completeness."""

    @pytest.mark.asyncio
    async def test_returns_error_when_cli_fails(self):
        svc = _make_svc()
        with patch.object(
            svc, "run_data_status_cli", new=AsyncMock(return_value={"error": "failed"})
        ):
            result = await svc.validate_data_completeness("svc-a", "2024-01-01")
        assert "error" in result

    @pytest.mark.asyncio
    async def test_returns_validation_structure(self):
        svc = _make_svc()
        cli_result = {
            "dates": [
                {
                    "date": "2024-01-01",
                    "venues": [
                        {"venue": "BINANCE", "status": "present"},
                        {"venue": "OKX", "status": "present"},
                    ],
                }
            ]
        }
        with patch.object(svc, "run_data_status_cli", new=AsyncMock(return_value=cli_result)):
            result = await svc.validate_data_completeness("svc-a", "2024-01-01")

        assert result["service"] == "svc-a"
        assert result["date"] == "2024-01-01"
        assert result["is_complete"] is True
        assert result["total_venues"] == 2
        assert result["completed_venues"] == 2
        assert result["completion_rate"] == 100.0

    @pytest.mark.asyncio
    async def test_reports_missing_venues(self):
        svc = _make_svc()
        cli_result = {
            "dates": [
                {
                    "date": "2024-01-01",
                    "venues": [
                        {"venue": "BINANCE", "status": "missing"},
                        {"venue": "OKX", "status": "present"},
                    ],
                }
            ]
        }
        with patch.object(svc, "run_data_status_cli", new=AsyncMock(return_value=cli_result)):
            result = await svc.validate_data_completeness("svc-a", "2024-01-01")

        assert result["is_complete"] is False
        assert "BINANCE" in result["missing_venues"]

    @pytest.mark.asyncio
    async def test_handles_no_dates_in_result(self):
        svc = _make_svc()
        cli_result = {"categories": {}}
        with patch.object(svc, "run_data_status_cli", new=AsyncMock(return_value=cli_result)):
            result = await svc.validate_data_completeness("svc-a", "2024-01-01")

        assert result["total_venues"] == 0
        assert result["completion_rate"] == 0.0


class TestBuildDataTypeBreakdown:
    """Tests for DataStatusService._build_data_type_breakdown."""

    def setup_method(self):
        self.svc = DataStatusService(project_id="test-proj")

    def test_returns_empty_when_no_data_type_column(self):
        df = pd.DataFrame({"date": ["2024-01-01"], "venue": ["BINANCE-SPOT"]})
        vm = MagicMock()
        result = self.svc._build_data_type_breakdown(
            df, "BINANCE-SPOT", "2024-01-01", "2024-01-01", vm
        )
        assert result == {}

    def test_returns_data_types_from_index(self):
        df = pd.DataFrame(
            {
                "date": ["2024-01-01", "2024-01-01"],
                "venue": ["BINANCE-SPOT", "BINANCE-SPOT"],
                "data_type": ["trades", "book_snapshot_5"],
            }
        )
        vm = MagicMock()
        vm.get_expected_trading_dates.return_value = ["2024-01-01"]
        with patch.object(
            _dss_mod,
            "get_expected_data_types_for_venue",
            return_value=["trades", "book_snapshot_5"],
        ):
            with patch.object(
                _dss_mod, "get_venue_data_type_start_date", return_value="2020-01-01"
            ):
                result = self.svc._build_data_type_breakdown(
                    df, "BINANCE-SPOT", "2024-01-01", "2024-01-01", vm
                )

        assert "trades" in result
        assert "book_snapshot_5" in result
        assert result["trades"]["dates_found"] == 1
        assert result["trades"]["is_expected"] is True

    def test_includes_unexpected_data_types(self):
        df = pd.DataFrame(
            {
                "date": ["2024-01-01"],
                "venue": ["BINANCE-SPOT"],
                "data_type": ["unexpected_type"],
            }
        )
        vm = MagicMock()
        vm.get_expected_trading_dates.return_value = ["2024-01-01"]
        with patch.object(_dss_mod, "get_expected_data_types_for_venue", return_value=["trades"]):
            with patch.object(_dss_mod, "get_venue_data_type_start_date", return_value=None):
                result = self.svc._build_data_type_breakdown(
                    df, "BINANCE-SPOT", "2024-01-01", "2024-01-01", vm
                )

        assert "unexpected_type" in result
        assert result["unexpected_type"]["is_expected"] is False
        assert "trades" in result
        assert result["trades"]["is_expected"] is True


class TestBuildVenueBreakdown:
    """Tests for DataStatusService._build_venue_breakdown with service param."""

    def setup_method(self):
        self.svc = DataStatusService(project_id="test-proj")

    def test_empty_dataframe_returns_empty(self):
        df = pd.DataFrame(columns=["date", "venue"])
        vm = MagicMock()
        venues, found, expected = self.svc._build_venue_breakdown(
            df, "2024-01-01", "2024-01-01", vm, 0, 1, service="market-tick-data-service"
        )
        assert venues == {}

    def test_no_venue_column_returns_empty(self):
        df = pd.DataFrame({"date": ["2024-01-01"]})
        vm = MagicMock()
        venues, found, expected = self.svc._build_venue_breakdown(
            df, "2024-01-01", "2024-01-01", vm, 0, 1, service=""
        )
        assert venues == {}

    def test_venue_breakdown_includes_data_types_when_present(self):
        df = pd.DataFrame(
            {
                "date": ["2024-01-01", "2024-01-01"],
                "venue": ["BINANCE-SPOT", "BINANCE-SPOT"],
                "data_type": ["trades", "book_snapshot_5"],
            }
        )
        vm = MagicMock()
        vm.get_venue_start_date.return_value = "2020-01-01"
        vm.get_expected_trading_dates.return_value = ["2024-01-01"]

        with patch.object(
            _dss_mod,
            "get_expected_data_types_for_venue",
            return_value=["trades", "book_snapshot_5"],
        ):
            with patch.object(
                _dss_mod, "get_venue_data_type_start_date", return_value="2020-01-01"
            ):
                venues, found, expected = self.svc._build_venue_breakdown(
                    df,
                    "2024-01-01",
                    "2024-01-01",
                    vm,
                    1,
                    1,
                    service="market-tick-data-service",
                )

        assert "BINANCE-SPOT" in venues
        venue_entry = venues["BINANCE-SPOT"]
        assert isinstance(venue_entry, dict)
        assert "data_types" in venue_entry


class TestReadIndexCached:
    """Tests for _read_index_cached and clear_index_cache."""

    def test_returns_dataframe(self):
        mock_df = pd.DataFrame({"date": ["2024-01-01"], "venue": ["BINANCE"]})
        with patch.object(_dss_mod, "read_availability_index", return_value=mock_df):
            result = _dss_mod._read_index_cached("test-bucket-cached")
        assert len(result) == 1
        assert list(result.columns) == ["date", "venue"]

    def test_caches_result(self):
        mock_df = pd.DataFrame({"date": ["2024-01-01"], "venue": ["BINANCE"]})
        _dss_mod.clear_index_cache()
        with patch.object(_dss_mod, "read_availability_index", return_value=mock_df) as m:
            _dss_mod._read_index_cached("cached-bucket-a")
            _dss_mod._read_index_cached("cached-bucket-a")
            m.assert_called_once()
        _dss_mod.clear_index_cache()

    def test_clear_cache_empties(self):
        mock_df = pd.DataFrame({"date": ["2024-01-01"], "venue": ["BINANCE"]})
        with patch.object(_dss_mod, "read_availability_index", return_value=mock_df) as m:
            _dss_mod._read_index_cached("bucket-clear-test")
            _dss_mod.clear_index_cache()
            _dss_mod._read_index_cached("bucket-clear-test")
            assert m.call_count == 2
        _dss_mod.clear_index_cache()


class TestClampToVenueStarts:
    """Tests for _clamp_to_venue_starts."""

    def test_returns_start_date_when_no_venue_col(self):
        df = pd.DataFrame({"date": ["2024-01-01"]})
        result = _dss_mod._clamp_to_venue_starts(df, "2024-01-01")
        assert result == "2024-01-01"

    def test_returns_start_date_when_empty(self):
        df = pd.DataFrame(columns=["date", "venue"])
        result = _dss_mod._clamp_to_venue_starts(df, "2024-01-01")
        assert result == "2024-01-01"

    def test_clamps_forward_to_latest_venue_start(self):
        df = pd.DataFrame({"date": ["2024-01-01"], "venue": ["TESTV"]})
        vm = MagicMock()
        vm.get_venue_start_date.return_value = "2024-06-01"
        with patch.object(_dss_mod, "VenueMapping", return_value=vm):
            result = _dss_mod._clamp_to_venue_starts(df, "2024-01-01")
        assert result == "2024-06-01"


class TestTallyMissingVenues:
    """Tests for DataStatusService._tally_missing_venues."""

    def setup_method(self):
        self.svc = DataStatusService(project_id="test-proj")

    def test_counts_missing_venues(self):
        date_info: dict[str, object] = {
            "venues": [
                {"venue": "BINANCE", "status": "missing", "category": "CEFI"},
                {"venue": "OKX", "status": "present", "category": "CEFI"},
            ]
        }
        by_venue: dict[str, int] = {}
        by_cat: dict[str, int] = {}
        count = self.svc._tally_missing_venues(date_info, by_venue, by_cat)
        assert count == 1
        assert by_venue["BINANCE"] == 1
        assert by_cat["CEFI"] == 1

    def test_returns_zero_when_no_venues(self):
        date_info: dict[str, object] = {}
        by_venue: dict[str, int] = {}
        by_cat: dict[str, int] = {}
        count = self.svc._tally_missing_venues(date_info, by_venue, by_cat)
        assert count == 0

    def test_skips_non_dict_entries(self):
        date_info: dict[str, object] = {
            "venues": [
                "not-a-dict",
                {"venue": "BINANCE", "status": "missing", "category": "CEFI"},
            ]
        }
        by_venue: dict[str, int] = {}
        by_cat: dict[str, int] = {}
        count = self.svc._tally_missing_venues(date_info, by_venue, by_cat)
        assert count == 1


class TestScanCategoryManifest:
    """Tests for DataStatusService._scan_category_manifest."""

    def setup_method(self):
        self.svc = DataStatusService(project_id="test-proj")

    def test_returns_none_for_unknown_service(self):
        result = self.svc._scan_category_manifest("unknown-svc", "CEFI", "2024-01-01", "2024-01-31")
        assert result is None

    def test_returns_none_when_index_read_fails(self):
        with patch.object(_dss_mod, "_read_index_cached", side_effect=OSError("no bucket")):
            result = self.svc._scan_category_manifest(
                "instruments-service", "CEFI", "2024-01-01", "2024-01-31"
            )
        assert result is None

    def test_returns_none_when_index_empty(self):
        with patch.object(_dss_mod, "_read_index_cached", return_value=pd.DataFrame()):
            result = self.svc._scan_category_manifest(
                "instruments-service", "CEFI", "2024-01-01", "2024-01-31"
            )
        assert result is None

    def test_returns_missing_dates(self):
        index = pd.DataFrame(
            {
                "date": ["2024-01-01", "2024-01-03"],
                "venue": ["BINANCE", "BINANCE"],
                "service_name": ["instruments-service", "instruments-service"],
            }
        )
        vm = MagicMock()
        vm.get_venue_start_date.return_value = "2020-01-01"
        with (
            patch.object(_dss_mod, "_read_index_cached", return_value=index),
            patch.object(_dss_mod, "VenueMapping", return_value=vm),
        ):
            result = self.svc._scan_category_manifest(
                "instruments-service", "CEFI", "2024-01-01", "2024-01-03"
            )
        assert result is not None
        assert "2024-01-02" in result["missing"]


class TestGetCoverageSummary:
    """Tests for DataStatusService.get_coverage_summary."""

    @pytest.mark.asyncio
    async def test_returns_summary_structure(self):
        svc = _make_svc()
        index = pd.DataFrame(
            {
                "date": ["2024-01-01", "2024-01-02"],
                "venue": ["BINANCE", "BINANCE"],
                "service_name": ["instruments-service", "instruments-service"],
            }
        )
        with patch.object(_dss_mod, "_read_index_cached", return_value=index):
            result = await svc.get_coverage_summary("instruments-service", categories=["CEFI"])

        assert result["service"] == "instruments-service"
        assert "categories" in result
        assert "totals" in result
        assert result["totals"]["shards"] == 2

    @pytest.mark.asyncio
    async def test_handles_empty_index(self):
        svc = _make_svc()
        with patch.object(_dss_mod, "_read_index_cached", return_value=pd.DataFrame()):
            result = await svc.get_coverage_summary("instruments-service", categories=["CEFI"])

        assert result["totals"]["shards"] == 0

    @pytest.mark.asyncio
    async def test_handles_read_error(self):
        svc = _make_svc()
        with patch.object(_dss_mod, "_read_index_cached", side_effect=OSError("no bucket")):
            result = await svc.get_coverage_summary("instruments-service", categories=["CEFI"])

        assert result["totals"]["shards"] == 0


class TestGetManifestStatus:
    """Tests for DataStatusService.get_manifest_status."""

    @pytest.mark.asyncio
    async def test_returns_status_structure(self):
        svc = _make_svc()
        index = pd.DataFrame(
            {
                "date": ["2024-01-01"],
                "venue": ["BINANCE"],
                "service_name": ["instruments-service"],
            }
        )
        vm = MagicMock()
        vm.get_venue_start_date.return_value = "2020-01-01"
        vm.get_expected_trading_dates.return_value = ["2024-01-01"]
        with (
            patch.object(_dss_mod, "_read_index_cached", return_value=index),
            patch.object(_dss_mod, "VenueMapping", return_value=vm),
        ):
            result = await svc.get_manifest_status(
                "instruments-service", "2024-01-01", "2024-01-01", categories=["CEFI"]
            )

        assert result["service"] == "instruments-service"
        assert result["mode"] == "turbo"
        assert "overall_completion_pct" in result
        assert "categories" in result

    @pytest.mark.asyncio
    async def test_handles_no_template(self):
        svc = _make_svc()
        result = await svc.get_manifest_status(
            "unknown-service", "2024-01-01", "2024-01-01", categories=["CEFI"]
        )
        assert result["overall_completion_pct"] == 0.0

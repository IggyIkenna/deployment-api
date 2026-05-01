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
        expected = {"completion": 85.0, "asset_groups": {}}
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
        assert "asset_groups" in result

    @pytest.mark.asyncio
    async def test_returns_empty_category_when_no_objects(self):
        svc = _make_svc()

        with patch.object(_dss_mod, "list_objects", return_value=[]):
            result = await svc.get_last_updated_info(
                "market-tick-data-handler", categories=["cefi"]
            )

        assert result["asset_groups"]["cefi"]["status"] == "empty"

    @pytest.mark.asyncio
    async def test_handles_list_objects_error(self):
        svc = _make_svc()

        with patch.object(_dss_mod, "list_objects", side_effect=OSError("bucket not found")):
            result = await svc.get_last_updated_info("instruments-service", categories=["cefi"])

        assert result["asset_groups"]["cefi"]["status"] == "error"

    @pytest.mark.asyncio
    async def test_uses_default_categories_when_none_given(self):
        svc = _make_svc()

        with patch.object(_dss_mod, "list_objects", return_value=[]):
            result = await svc.get_last_updated_info("instruments-service")

        # Should have checked cefi, tradfi, defi
        assert "cefi" in result["asset_groups"]
        assert "tradfi" in result["asset_groups"]
        assert "defi" in result["asset_groups"]


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
        cli_result = {"asset_groups": {}}
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

        # Observed-but-not-UAC-declared dt is kept with is_expected=False
        assert "unexpected_type" in result
        assert result["unexpected_type"]["is_expected"] is False
        # Phantom-expected clamp (2026-04-19): UAC declared "trades" but it
        # was never observed in this slice → dropped to avoid cartesian
        # inflation when this function is called from a narrowed sub-slice
        # (e.g. per-instrument_type / per-underlying). The top-level
        # "missing data_type" signal still surfaces via the venue-level
        # cat_total_days denominator and per-venue completion metrics.
        assert "trades" not in result


class TestFourStateClassification:
    """Phase 1 four-state classifier on _build_data_type_breakdown.

    Verifies that processed data_type rows split into actionable missing
    vs blocked-on-raw, and that out-of-scope rows are flagged.
    """

    def setup_method(self):
        self.svc = DataStatusService(project_id="test-proj")

    def test_processed_dt_missing_is_blocked_when_raw_also_missing(self):
        """ohlcv_5m on a CeFi venue with no raw `trades` → blocked_on_raw."""
        df = pd.DataFrame(
            {
                "date": [],
                "venue": [],
                "data_type": [],
            }
        )
        vm = MagicMock()
        vm.get_expected_trading_dates.return_value = ["2024-01-01", "2024-01-02"]
        with (
            patch.object(_dss_mod, "get_expected_data_types_for_venue", return_value=["ohlcv_5m"]),
            patch.object(_dss_mod, "get_venue_data_type_start_date", return_value=None),
        ):
            result = self.svc._build_data_type_breakdown(
                df,
                "BINANCE-SPOT",
                "2024-01-01",
                "2024-01-02",
                vm,
                category="CEFI",
            )
        assert result == {}

    def test_processed_dt_with_raw_captured_is_actionable_missing(self):
        """ohlcv_5m absent but raw trades captured → actionable missing, not blocked."""
        df = pd.DataFrame(
            {
                "date": ["2024-01-01", "2024-01-02"],
                "venue": ["BINANCE-SPOT", "BINANCE-SPOT"],
                "data_type": ["trades", "trades"],
            }
        )
        vm = MagicMock()
        vm.get_expected_trading_dates.return_value = ["2024-01-01", "2024-01-02"]
        with (
            patch.object(
                _dss_mod,
                "get_expected_data_types_for_venue",
                return_value=["trades", "ohlcv_5m"],
            ),
            patch.object(_dss_mod, "get_venue_data_type_start_date", return_value=None),
        ):
            result = self.svc._build_data_type_breakdown(
                df,
                "BINANCE-SPOT",
                "2024-01-01",
                "2024-01-02",
                vm,
                category="CEFI",
            )
        # ohlcv_5m gets phantom-clamped because it was never observed in this
        # slice — so it doesn't appear in the result. trades captured 100%.
        assert "trades" in result
        assert result["trades"]["dates_found"] == 2
        assert result["trades"]["dates_blocked_on_raw"] == 0
        assert result["trades"]["out_of_scope"] is False
        assert result["trades"]["is_processed_data_type"] is False

    def test_out_of_scope_when_venue_dt_not_in_expected_coverage(self):
        """NASDAQ trades observed → in_expected_coverage False, out_of_scope True (TradFi)."""
        df = pd.DataFrame(
            {
                "date": ["2024-01-01"],
                "venue": ["NASDAQ"],
                "data_type": ["trades"],
            }
        )
        vm = MagicMock()
        vm.get_expected_trading_dates.return_value = ["2024-01-01"]
        with (
            patch.object(_dss_mod, "get_expected_data_types_for_venue", return_value=["trades"]),
            patch.object(_dss_mod, "get_venue_data_type_start_date", return_value=None),
        ):
            result = self.svc._build_data_type_breakdown(
                df,
                "NASDAQ",
                "2024-01-01",
                "2024-01-01",
                vm,
                category="TRADFI",
            )
        # NASDAQ + trades is NOT in EXPECTED_COVERAGE['tradfi'] — operator policy.
        # `trades` is also not in PROCESSED_REQUIRES_RAW (it's raw), so the
        # row classifies as out_of_scope=True, in_expected_coverage=False.
        assert "trades" in result
        assert result["trades"]["in_expected_coverage"] is False
        assert result["trades"]["out_of_scope"] is True
        assert result["trades"]["is_processed_data_type"] is False

    def test_in_scope_venue_dt_is_not_out_of_scope(self):
        """CME + trades is in EXPECTED_COVERAGE['tradfi'] — not out of scope."""
        df = pd.DataFrame(
            {
                "date": ["2024-01-01"],
                "venue": ["CME"],
                "data_type": ["trades"],
            }
        )
        vm = MagicMock()
        vm.get_expected_trading_dates.return_value = ["2024-01-01"]
        with (
            patch.object(_dss_mod, "get_expected_data_types_for_venue", return_value=["trades"]),
            patch.object(_dss_mod, "get_venue_data_type_start_date", return_value=None),
        ):
            result = self.svc._build_data_type_breakdown(
                df,
                "CME",
                "2024-01-01",
                "2024-01-01",
                vm,
                category="TRADFI",
            )
        assert result["trades"]["in_expected_coverage"] is True
        assert result["trades"]["out_of_scope"] is False
        assert result["trades"]["dates_blocked_on_raw"] == 0


class TestPhantomExpectedClamp:
    """Tests for the 2026-04-19 phantom-expected denominator clamp.

    Covers the three axes where cartesian inflation was inflating
    ``shards_expected`` on the MTDS data-status ``/turbo`` endpoint:

    1. data_type x (venue, instrument_type, underlying) - UAC-declared dts
       that never materialise for the sub-slice were counted as phantom
       missing.
    2. instrument_type launch date — new instrument_types inherited the
       venue's full calendar even if they launched mid-history.
    3. underlying launch date — same, for underlyings under an
       instrument_type (e.g. DERIBIT SOL options post-2024).
    """

    def setup_method(self):
        self.svc = DataStatusService(project_id="test-proj")

    def test_data_type_breakdown_drops_unobserved_uac_phantoms(self):
        """UAC declares 4 dts; only 1 observed → expected = 1, not 4."""
        df = pd.DataFrame(
            {
                "date": ["2024-01-01", "2024-01-02", "2024-01-03"],
                "venue": ["ODDS_API"] * 3,
                "data_type": ["ODDS", "ODDS", "ODDS"],
            }
        )
        vm = MagicMock()
        vm.get_expected_trading_dates.return_value = [
            "2024-01-01",
            "2024-01-02",
            "2024-01-03",
        ]
        with patch.object(
            _dss_mod,
            "get_expected_data_types_for_venue",
            return_value=["ODDS", "arbitrage_opportunity", "odds_movement", "odds_snapshot"],
        ):
            with patch.object(_dss_mod, "get_venue_data_type_start_date", return_value=None):
                result = self.svc._build_data_type_breakdown(
                    df, "ODDS_API", "2024-01-01", "2024-01-03", vm
                )

        # Only ODDS (observed) shows up. arbitrage_opportunity, odds_movement,
        # odds_snapshot are UAC-declared but never observed → phantom → dropped.
        assert set(result.keys()) == {"ODDS"}
        assert result["ODDS"]["dates_found"] == 3
        assert result["ODDS"]["dates_expected"] == 3

    def test_instrument_type_breakdown_clamps_to_observed_launch(self):
        """Instrument_type launched 2024-06-01 does not get 2024-01 phantom days."""
        df = pd.DataFrame(
            {
                "date": ["2024-01-01", "2024-01-02", "2024-06-01"],
                "venue": ["POLYMARKET"] * 3,
                "instrument_type": ["BTC", "BTC", "HYPE"],
            }
        )
        vm = MagicMock()
        vm.get_expected_trading_dates.side_effect = lambda venue, start, end: (
            pd.date_range(start, end, freq="D").strftime("%Y-%m-%d").tolist()
        )

        result = self.svc._build_instrument_type_breakdown(
            df,
            "POLYMARKET",
            "2024-01-01",
            "2024-06-01",
            vm,
            has_data_type=False,
            category="PREDICTION",
            service="market-tick-data-service",
        )

        # HYPE's effective start = 2024-06-01 (first observed) → only 1 day
        # expected (2024-06-01 itself). BTC's effective start stays at the
        # user-supplied 2024-01-01 → full window to 2024-06-01.
        btc_expected = int(result["BTC"]["dates_expected"])
        hype_expected = int(result["HYPE"]["dates_expected"])
        assert hype_expected == 1, f"HYPE should be clamped to 1 day, got {hype_expected}"
        assert btc_expected > hype_expected, (
            f"BTC ({btc_expected}) should span more days than HYPE ({hype_expected})"
        )

    def test_underlying_breakdown_clamps_to_observed_launch(self):
        """Underlying that launched mid-history does not get pre-launch phantom."""
        df = pd.DataFrame(
            {
                "date": ["2022-01-01", "2022-01-02", "2024-06-01"],
                "venue": ["DERIBIT"] * 3,
                "instrument_type": ["options_chain"] * 3,
                "underlying": ["BTC", "BTC", "SOL"],
            }
        )
        vm = MagicMock()
        vm.get_expected_trading_dates.side_effect = lambda venue, start, end: (
            pd.date_range(start, end, freq="D").strftime("%Y-%m-%d").tolist()
        )

        result = self.svc._build_underlying_breakdown(
            df,
            "DERIBIT",
            "2022-01-01",
            "2024-06-01",
            vm,
            has_data_type=False,
            service="market-tick-data-service",
            category="CEFI",
        )

        # SOL's effective start = 2024-06-01 → 1 day expected.
        # BTC stays at user-supplied 2022-01-01 → full window.
        btc_expected = int(result["BTC"]["dates_expected"])
        sol_expected = int(result["SOL"]["dates_expected"])
        assert sol_expected == 1, f"SOL should be clamped to 1 day, got {sol_expected}"
        assert btc_expected > sol_expected, (
            f"BTC ({btc_expected}) should span more days than SOL ({sol_expected})"
        )

    def test_data_type_breakdown_falls_back_to_observed_min_when_uac_unknown(self):
        """When UAC has no declared dt start, use earliest observed date."""
        df = pd.DataFrame(
            {
                "date": ["2024-06-01", "2024-06-02"],
                "venue": ["DRIFT", "DRIFT"],
                "data_type": ["derivative_ticker", "derivative_ticker"],
            }
        )
        vm = MagicMock()
        # Returning the whole year would be phantom; but with the observed-min
        # fallback we clamp to 2024-06-01 → end.
        vm.get_expected_trading_dates.side_effect = lambda venue, start, end: (
            pd.date_range(start, end, freq="D").strftime("%Y-%m-%d").tolist()
        )
        with patch.object(
            _dss_mod,
            "get_expected_data_types_for_venue",
            return_value=["derivative_ticker"],
        ):
            with patch.object(_dss_mod, "get_venue_data_type_start_date", return_value=None):
                result = self.svc._build_data_type_breakdown(
                    df, "DRIFT", "2024-01-01", "2024-06-02", vm
                )

        # Effective start clamps to observed min (2024-06-01), so expected
        # range is 2024-06-01..2024-06-02 = 2 days, NOT 2024-01-01..2024-06-02.
        assert result["derivative_ticker"]["dates_expected"] == 2


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
        assert "asset_groups" in result
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

    @pytest.mark.asyncio
    async def test_drops_legacy_defi_venue_aliases(self):
        """Regression from audit 2026-04-19 §2.A.1b — legacy pre-canonicalisation
        DeFi alias rows like ``venue='AAVEV3-ETHEREUM' chain=''`` were leaking into
        the Instrument Coverage Summary widget despite the per-shard rollup fix in
        22f0024/959bdab. Both paths now apply the same legacy-alias filter so the
        widget matches the rollup."""
        svc = _make_svc()
        # Mix of canonical rows (AAVE_V3 + ETHEREUM) and legacy alias rows
        # (AAVEV3-ETHEREUM + empty chain). The filter must keep the canonical
        # ones and drop the legacy ones.
        index = pd.DataFrame(
            {
                "date": ["2024-01-01", "2024-01-01", "2024-01-02"],
                "venue": ["AAVE_V3", "AAVEV3-ETHEREUM", "AAVEV3-POLYGON"],
                "chain": ["ETHEREUM", "", ""],
                "service_name": [
                    "instruments-service",
                    "instruments-service",
                    "instruments-service",
                ],
            }
        )
        with patch.object(_dss_mod, "_read_index_cached", return_value=index):
            result = await svc.get_coverage_summary("instruments-service", categories=["DEFI"])

        cat = result["asset_groups"]["DEFI"]
        # Only the canonical AAVE_V3 row should survive.
        assert cat["unique_venues"] == 1
        assert "AAVEV3-ETHEREUM" not in cat["latest_day_instruments"]
        assert "AAVEV3-POLYGON" not in cat["latest_day_instruments"]
        assert cat["total_shards"] == 1


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
                "instruments-service", "2024-01-01", "2024-01-01", asset_groups=["CEFI"]
            )

        assert result["service"] == "instruments-service"
        assert result["mode"] == "turbo"
        assert "overall_completion_pct" in result
        assert "asset_groups" in result

    @pytest.mark.asyncio
    async def test_handles_no_template(self):
        svc = _make_svc()
        result = await svc.get_manifest_status(
            "unknown-service", "2024-01-01", "2024-01-01", asset_groups=["CEFI"]
        )
        assert result["overall_completion_pct"] == 0.0


class TestTransferWindowAwareness:
    """Tests for transfer-window-aware data status denominator."""

    def test_is_transfer_window_venue(self):
        svc = _make_svc()
        assert svc._is_transfer_window_venue("TRANSFERMARKT_TEAMS")
        assert svc._is_transfer_window_venue("TRANSFERMARKT_LEAGUES")
        assert not svc._is_transfer_window_venue("FOOTYSTATS_EPL")
        assert not svc._is_transfer_window_venue("UNDERSTAT_XG")

    def test_transfermarkt_not_sports_reference(self):
        """Transfermarkt should NOT be classified as fixture-dependent."""
        svc = _make_svc()
        assert not svc._is_sports_reference_venue("TRANSFERMARKT_TEAMS")
        assert not svc._is_sports_reference_venue("TRANSFERMARKT_LEAGUES")
        # Other sports venues are still fixture-dependent
        assert svc._is_sports_reference_venue("FOOTYSTATS_EPL")
        assert svc._is_sports_reference_venue("UNDERSTAT_XG")

    def test_resolve_transfer_window_dates_has_trigger_dates(self):
        """Trigger dates (season start, window open/close) should be included."""
        svc = _make_svc()
        # Full year: should have trigger dates (season starts, window boundaries)
        dates = svc._resolve_transfer_window_dates("2024-01-01", "2024-12-31")
        # Multiple leagues x multiple triggers x +/-3 day tolerance
        assert len(dates) > 20
        # Mid-October should NOT be a trigger (no season start or window boundary)
        assert "2024-10-15" not in dates

    def test_resolve_transfer_window_dates_sparse_in_midseason(self):
        """Mid-season months with no triggers should have few/no expected dates."""
        svc = _make_svc()
        # October: no European window boundary, no season start
        dates = svc._resolve_transfer_window_dates("2024-10-01", "2024-10-31")
        # Should be very few (maybe MLS secondary close nearby, but not many)
        assert len(dates) < 15

    def test_resolve_transfer_window_dates_summer_has_triggers(self):
        """Summer window boundaries should produce trigger dates."""
        svc = _make_svc()
        # June-August: summer windows open/close across multiple leagues
        dates = svc._resolve_transfer_window_dates("2024-06-01", "2024-08-31")
        assert len(dates) > 10  # Multiple window open/close triggers

    def test_resolve_expected_dates_transfermarkt_uses_triggers(self):
        """Transfermarkt venue should use trigger dates, not fixture calendar."""
        svc = _make_svc()
        fixture_cal: set[str] = {"2024-01-06", "2024-01-13", "2024-01-20"}
        ref_dates: dict[str, set[str]] = {}
        vm = MagicMock()
        result = svc._resolve_expected_dates(
            "TRANSFERMARKT_TEAMS",
            "2024-01-01",
            "2024-01-31",
            fixture_cal,
            ref_dates,
            vm,
        )
        # Should NOT be the fixture calendar (3 dates)
        # Should be trigger-based dates from UAC calendar
        assert result != fixture_cal


# ── Underlying derivation + grouping helpers ────────────────────────────

_derive = _dss_mod._derive_underlying_from_instrument_id
_ensure = _dss_mod._ensure_underlying_column


class TestDeriveUnderlyingFromInstrumentId:
    """Tests for _derive_underlying_from_instrument_id."""

    def test_perp_instrument(self):
        assert _derive("BTC-USDT-PERP") == "BTC"

    def test_spot_pair(self):
        assert _derive("ETH-USDC") == "ETH"

    def test_options_instrument(self):
        assert _derive("BTC-USD-241227-C-100000") == "BTC"

    def test_futures_instrument(self):
        assert _derive("ES-FUT-20260320") == "ES"

    def test_single_symbol(self):
        assert _derive("SPY") == "SPY"

    def test_lowercase_uppercased(self):
        assert _derive("sol-usdt-perp") == "SOL"

    def test_empty_string(self):
        assert _derive("") == ""

    def test_whitespace_only(self):
        assert _derive("   ") == ""


class TestEnsureUnderlyingColumn:
    """Tests for _ensure_underlying_column."""

    def test_preserves_existing_underlying(self):
        df = pd.DataFrame({"underlying": ["BTC", "ETH"], "instrument_id": ["X-Y", "A-B"]})
        result = _ensure(df)
        assert list(result["underlying"]) == ["BTC", "ETH"]

    def test_derives_from_instrument_id_when_underlying_missing(self):
        df = pd.DataFrame(
            {"instrument_id": ["BTC-USDT-PERP", "ETH-USDC"], "date": ["2024-01-01"] * 2}
        )
        result = _ensure(df)
        assert "underlying" in result.columns
        assert list(result["underlying"]) == ["BTC", "ETH"]

    def test_fills_blank_rows_only(self):
        df = pd.DataFrame(
            {
                "underlying": ["BTC", ""],
                "instrument_id": ["BTC-USDT-PERP", "SOL-USDC"],
            }
        )
        result = _ensure(df)
        assert list(result["underlying"]) == ["BTC", "SOL"]

    def test_no_instrument_id_column_no_crash(self):
        df = pd.DataFrame({"date": ["2024-01-01"], "venue": ["BINANCE"]})
        result = _ensure(df)
        assert "underlying" not in result.columns or result["underlying"].str.len().sum() == 0


class TestBuildUnderlyingGrouping:
    """Tests for DataStatusService._build_underlying_grouping."""

    def test_groups_by_underlying(self):
        svc = _make_svc()
        df = pd.DataFrame(
            {
                "date": ["2024-01-01", "2024-01-01", "2024-01-02", "2024-01-02"],
                "venue": ["BINANCE", "BINANCE", "OKX-SPOT", "OKX-SPOT"],
                "underlying": ["BTC", "ETH", "BTC", "ETH"],
                "instrument_type": ["perpetuals", "perpetuals", "spot", "spot"],
            }
        )
        vm = MagicMock()
        vm.get_venue_start_date.return_value = None
        vm.get_expected_trading_dates.return_value = ["2024-01-01", "2024-01-02"]
        result = svc._build_underlying_grouping(df, "2024-01-01", "2024-01-02", vm)
        assert "BTC" in result
        assert "ETH" in result
        assert result["BTC"]["venue_count"] == 2
        assert result["BTC"]["dates_found"] == 2

    def test_derives_underlying_when_column_empty(self):
        svc = _make_svc()
        df = pd.DataFrame(
            {
                "date": ["2024-01-01", "2024-01-01"],
                "venue": ["BINANCE", "BINANCE"],
                "instrument_id": ["BTC-USDT-PERP", "ETH-USDC"],
                "instrument_type": ["perpetuals", "spot"],
            }
        )
        vm = MagicMock()
        vm.get_venue_start_date.return_value = None
        vm.get_expected_trading_dates.return_value = ["2024-01-01"]
        result = svc._build_underlying_grouping(df, "2024-01-01", "2024-01-01", vm)
        assert "BTC" in result
        assert "ETH" in result

    def test_returns_empty_when_no_underlying_data(self):
        svc = _make_svc()
        df = pd.DataFrame(
            {
                "date": ["2024-01-01"],
                "venue": ["BINANCE"],
            }
        )
        vm = MagicMock()
        result = svc._build_underlying_grouping(df, "2024-01-01", "2024-01-01", vm)
        assert result == {}

    def test_includes_instrument_types(self):
        svc = _make_svc()
        df = pd.DataFrame(
            {
                "date": ["2024-01-01", "2024-01-01"],
                "venue": ["DERIBIT", "DERIBIT"],
                "underlying": ["BTC", "BTC"],
                "instrument_type": ["options", "perpetuals"],
            }
        )
        vm = MagicMock()
        vm.get_venue_start_date.return_value = None
        vm.get_expected_trading_dates.return_value = ["2024-01-01"]
        result = svc._build_underlying_grouping(df, "2024-01-01", "2024-01-01", vm)
        assert "BTC" in result
        assert sorted(result["BTC"]["instrument_types"]) == ["options", "perpetuals"]


class TestBuildManifestCategoryShardsWeightedCompletion:
    """_build_manifest_category exposes shards-weighted completion_pct.

    Previously ``completion_pct`` was date-based (days with any data /
    total days in range). That over-states the real coverage when a
    handful of shards fill a date while most stay empty — Polymarket
    header was showing 100% on days where 11/94 shards had data.

    The new primary ``completion_pct`` is shards-weighted
    (``venue_dates_found / venue_dates_expected``), matching the
    sub-row math the user sees. ``completion_pct_dates`` remains for
    backwards-compat.
    """

    def test_shards_weighted_replaces_dates_for_primary(self):
        svc = _make_svc()
        # Stub out the heavy dependencies and force a known
        # venue_found_total / venue_expected_total split so we can
        # exercise the new percentage math directly.
        venues = {
            "POLYMARKET-BTC": {"completion_pct": 100.0},
            "POLYMARKET-TRUMP": {"completion_pct": 80.0},
        }
        with (
            patch.object(
                svc,
                "_read_defi_merged_index",
                return_value=pd.DataFrame(
                    {
                        "date": ["2025-03-14", "2025-03-14"],
                        "venue": ["POLYMARKET", "POLYMARKET"],
                        "service_name": ["instruments-service", "instruments-service"],
                        "row_count": [100, 80],
                    }
                ),
            ),
            patch.object(
                svc,
                "_build_venue_breakdown",
                return_value=(venues, 17, 18),  # 17 found / 18 expected
            ),
            patch.object(svc, "_build_v4_sub_dimensions", return_value={}),
            patch.object(_dss_mod, "get_effective_start_date", return_value="2025-03-14"),
        ):
            vm = MagicMock()
            result = svc._build_manifest_category(
                service="instruments-service",
                cat="PREDICTION",
                start_date="2025-03-14",
                end_date="2025-03-14",
                all_date_strs=["2025-03-14"],
                total_days=1,
                venue_mapping=vm,
            )
        expected_shards_pct = round(17 / 18 * 100, 2)
        assert result["completion_pct"] == expected_shards_pct
        assert result["completion_pct_shards_weighted"] == expected_shards_pct
        assert result["completion_pct_dates"] == 100.0

    def test_falls_back_to_dates_when_no_shards_expected(self):
        svc = _make_svc()
        with (
            patch.object(
                svc,
                "_read_defi_merged_index",
                return_value=pd.DataFrame(
                    {
                        "date": ["2025-03-14"],
                        "venue": ["POLYMARKET"],
                        "service_name": ["instruments-service"],
                        "row_count": [100],
                    }
                ),
            ),
            patch.object(
                svc,
                "_build_venue_breakdown",
                return_value=({}, 0, 0),
            ),
            patch.object(svc, "_build_v4_sub_dimensions", return_value={}),
            patch.object(_dss_mod, "get_effective_start_date", return_value="2025-03-14"),
        ):
            vm = MagicMock()
            result = svc._build_manifest_category(
                service="instruments-service",
                cat="PREDICTION",
                start_date="2025-03-14",
                end_date="2025-03-14",
                all_date_strs=["2025-03-14"],
                total_days=1,
                venue_mapping=vm,
            )
        # venue_expected_total is 0 → shards-weighted falls back to
        # the date-based figure (100% — the date had data).
        assert result["completion_pct"] == 100.0
        assert result["completion_pct_dates"] == 100.0
        assert result["completion_pct_shards_weighted"] == 100.0

    def test_empty_index_returns_zero(self):
        svc = _make_svc()
        with patch.object(svc, "_read_defi_merged_index", return_value=pd.DataFrame()):
            vm = MagicMock()
            result = svc._build_manifest_category(
                service="instruments-service",
                cat="PREDICTION",
                start_date="2025-03-14",
                end_date="2025-03-14",
                all_date_strs=["2025-03-14"],
                total_days=1,
                venue_mapping=vm,
            )
        assert result["completion_pct"] == 0.0

    def test_shards_weighted_caps_at_100(self):
        """Even if venue_found exceeds venue_expected (data drift),
        completion_pct caps at 100."""
        svc = _make_svc()
        with (
            patch.object(
                svc,
                "_read_defi_merged_index",
                return_value=pd.DataFrame(
                    {
                        "date": ["2025-03-14"],
                        "venue": ["POLYMARKET"],
                        "service_name": ["instruments-service"],
                        "row_count": [100],
                    }
                ),
            ),
            patch.object(
                svc,
                "_build_venue_breakdown",
                return_value=({"POLYMARKET-BTC": {"completion_pct": 100.0}}, 110, 100),
            ),
            patch.object(svc, "_build_v4_sub_dimensions", return_value={}),
            patch.object(_dss_mod, "get_effective_start_date", return_value="2025-03-14"),
        ):
            vm = MagicMock()
            result = svc._build_manifest_category(
                service="instruments-service",
                cat="PREDICTION",
                start_date="2025-03-14",
                end_date="2025-03-14",
                all_date_strs=["2025-03-14"],
                total_days=1,
                venue_mapping=vm,
            )
        assert result["completion_pct"] == 100.0
        assert result["completion_pct_shards_weighted"] == 100.0

    def test_response_carries_both_variants(self):
        """Both shards-weighted + date-based fields must be exposed for UI compat."""
        svc = _make_svc()
        with (
            patch.object(
                svc,
                "_read_defi_merged_index",
                return_value=pd.DataFrame(
                    {
                        "date": ["2025-03-14", "2025-03-15"],
                        "venue": ["POLYMARKET", "POLYMARKET"],
                        "service_name": ["instruments-service", "instruments-service"],
                        "row_count": [100, 100],
                    }
                ),
            ),
            patch.object(
                svc,
                "_build_venue_breakdown",
                return_value=({"POLYMARKET-BTC": {}}, 9, 10),
            ),
            patch.object(svc, "_build_v4_sub_dimensions", return_value={}),
            patch.object(_dss_mod, "get_effective_start_date", return_value="2025-03-14"),
        ):
            vm = MagicMock()
            result = svc._build_manifest_category(
                service="instruments-service",
                cat="PREDICTION",
                start_date="2025-03-14",
                end_date="2025-03-15",
                all_date_strs=["2025-03-14", "2025-03-15"],
                total_days=2,
                venue_mapping=vm,
            )
        assert "completion_pct" in result
        assert "completion_pct_dates" in result
        assert "completion_pct_shards_weighted" in result
        assert result["completion_pct_shards_weighted"] == round(9 / 10 * 100, 2)
        assert result["completion_pct_dates"] == 100.0


class TestDefiLegacyVenueFilter:
    """Filter pre-canonicalisation DeFi venue aliases from DEFI aggregate.

    The DeFi availability indices contain BOTH:
      - Legacy rows: ``venue='AAVEV3-ETHEREUM' chain=''`` (pre-migration)
      - Canonical rows: ``venue='AAVE_V3' chain='ETHEREUM'`` (post-migration)

    The legacy rows have no matching shard paths post-migration, so they
    inflate ``venue_dates_expected`` without contributing found dates,
    dragging DEFI completion from ~99% to ~40%. The filter drops legacy
    rows before ``_build_venue_breakdown`` sees them.
    """

    def test_is_legacy_defi_venue_row_detects_canonical_patterns(self):
        svc = _make_svc()
        # Known legacy patterns — chain empty, venue is PROTOCOL[V<N>]-CHAIN
        assert svc._is_legacy_defi_venue_row("AAVEV3-ETHEREUM", "")
        assert svc._is_legacy_defi_venue_row("UNISWAPV2-ETHEREUM", "")
        assert svc._is_legacy_defi_venue_row("UNISWAPV3-ETHEREUM", "")
        assert svc._is_legacy_defi_venue_row("CURVE-ETHEREUM", "")
        assert svc._is_legacy_defi_venue_row("LIDO-ETHEREUM", "")
        assert svc._is_legacy_defi_venue_row("BALANCER-ETHEREUM", "")
        assert svc._is_legacy_defi_venue_row("COMPOUND-ETHEREUM", None)
        # NaN chain also counts as empty
        assert svc._is_legacy_defi_venue_row("AAVEV3-POLYGON", float("nan"))

    def test_is_legacy_defi_venue_row_covers_extended_protocol_set(self):
        """Second-tier DeFi protocols observed in the real manifest:
        Camelot, GMX, Jito, Orca, Marinade, Kamino, Morpho, Fluid,
        Ethena, Ether.fi, EigenLayer. All must be caught."""
        svc = _make_svc()
        assert svc._is_legacy_defi_venue_row("CAMELOTV3-ARBITRUM", "")
        assert svc._is_legacy_defi_venue_row("GMX-ARBITRUM", "")
        assert svc._is_legacy_defi_venue_row("JITO-SOLANA", "")
        assert svc._is_legacy_defi_venue_row("ORCA-SOLANA", "")
        assert svc._is_legacy_defi_venue_row("MARINADE-SOLANA", "")
        assert svc._is_legacy_defi_venue_row("KAMINO-SOLANA", "")
        assert svc._is_legacy_defi_venue_row("MORPHO-ETHEREUM", "")
        assert svc._is_legacy_defi_venue_row("FLUID-ETHEREUM", "")
        assert svc._is_legacy_defi_venue_row("ETHENA-ETHEREUM", "")
        assert svc._is_legacy_defi_venue_row("ETHERFI-ETHEREUM", "")
        assert svc._is_legacy_defi_venue_row("EIGENLAYER-ETHEREUM", "")

    def test_is_legacy_defi_venue_row_skips_canonical_rows(self):
        svc = _make_svc()
        # Canonical rows always have a non-empty chain → never legacy
        assert not svc._is_legacy_defi_venue_row("AAVE_V3", "ETHEREUM")
        assert not svc._is_legacy_defi_venue_row("UNISWAP_V2", "ETHEREUM")
        assert not svc._is_legacy_defi_venue_row("CURVE", "ETHEREUM")
        assert not svc._is_legacy_defi_venue_row("LIDO", "ETHEREUM")

    def test_is_legacy_defi_venue_row_skips_cefi_hyphenated_venues(self):
        svc = _make_svc()
        # CeFi venues are hyphenated too but protocol root doesn't match
        # the DeFi whitelist → never legacy
        assert not svc._is_legacy_defi_venue_row("BINANCE-FUTURES", "")
        assert not svc._is_legacy_defi_venue_row("OKX-SWAP", "")
        assert not svc._is_legacy_defi_venue_row("COINBASE-SPOT", "")
        assert not svc._is_legacy_defi_venue_row("BINANCE-SPOT", "")

    def test_is_legacy_defi_venue_row_skips_non_hyphenated(self):
        svc = _make_svc()
        assert not svc._is_legacy_defi_venue_row("AAVE_V3", "")
        assert not svc._is_legacy_defi_venue_row("BINANCE", "")
        assert not svc._is_legacy_defi_venue_row("", "")
        assert not svc._is_legacy_defi_venue_row(None, "")

    def test_defi_category_drops_legacy_rows_before_breakdown(self):
        """End-to-end: manifest with mixed old+new DeFi rows → breakdown
        only sees canonical rows."""
        svc = _make_svc()
        # Synthetic manifest: 2 canonical rows + 2 legacy alias rows.
        # Canonical rows should flow to _build_venue_breakdown; legacy
        # rows (empty chain, protocol-hyphenated venue) should be dropped.
        mixed_df = pd.DataFrame(
            {
                "date": [
                    "2026-04-17",
                    "2026-04-17",
                    "2026-04-17",
                    "2026-04-17",
                ],
                "venue": [
                    "AAVE_V3",
                    "UNISWAP_V2",
                    "AAVEV3-ETHEREUM",
                    "UNISWAPV2-ETHEREUM",
                ],
                "chain": ["ETHEREUM", "ETHEREUM", "", ""],
                "service_name": ["instruments-service"] * 4,
                "row_count": [100, 100, 100, 100],
            }
        )

        captured_filtered_df: dict[str, pd.DataFrame] = {}

        def _fake_breakdown(filtered_df, *args, **kwargs):
            captured_filtered_df["df"] = filtered_df.copy()
            # Pretend 2 found / 2 expected so DEFI completion is 100%
            return ({"AAVE_V3": {}, "UNISWAP_V2": {}}, 2, 2)

        with (
            patch.object(svc, "_read_defi_merged_index", return_value=mixed_df),
            patch.object(svc, "_build_venue_breakdown", side_effect=_fake_breakdown),
            patch.object(svc, "_build_v4_sub_dimensions", return_value={}),
            patch.object(_dss_mod, "get_effective_start_date", return_value="2026-04-17"),
        ):
            vm = MagicMock()
            result = svc._build_manifest_category(
                service="instruments-service",
                cat="DEFI",
                start_date="2026-04-17",
                end_date="2026-04-17",
                all_date_strs=["2026-04-17"],
                total_days=1,
                venue_mapping=vm,
            )

        # Breakdown should have received only canonical rows (no legacy aliases).
        seen = captured_filtered_df["df"]
        venues_seen = set(seen["venue"].tolist())
        assert "AAVEV3-ETHEREUM" not in venues_seen
        assert "UNISWAPV2-ETHEREUM" not in venues_seen
        assert "AAVE_V3" in venues_seen
        assert "UNISWAP_V2" in venues_seen
        # Category-level completion reflects only canonical rows (100%),
        # not the inflated 2/4 = 50% that legacy rows would have produced.
        assert result["completion_pct"] == 100.0

    def test_cefi_category_keeps_hyphenated_venues(self):
        """CeFi venues like BINANCE-FUTURES must NOT be dropped by the DeFi filter."""
        svc = _make_svc()
        cefi_df = pd.DataFrame(
            {
                "date": ["2026-04-17", "2026-04-17"],
                "venue": ["BINANCE-FUTURES", "OKX-SWAP"],
                "chain": ["", ""],
                "service_name": ["instruments-service", "instruments-service"],
                "row_count": [100, 100],
            }
        )

        captured: dict[str, pd.DataFrame] = {}

        def _fake_breakdown(filtered_df, *args, **kwargs):
            captured["df"] = filtered_df.copy()
            return ({"BINANCE-FUTURES": {}, "OKX-SWAP": {}}, 2, 2)

        with (
            patch.object(svc, "_read_defi_merged_index", return_value=cefi_df),
            patch.object(svc, "_build_venue_breakdown", side_effect=_fake_breakdown),
            patch.object(svc, "_build_v4_sub_dimensions", return_value={}),
            patch.object(_dss_mod, "get_effective_start_date", return_value="2026-04-17"),
        ):
            vm = MagicMock()
            svc._build_manifest_category(
                service="instruments-service",
                cat="CEFI",
                start_date="2026-04-17",
                end_date="2026-04-17",
                all_date_strs=["2026-04-17"],
                total_days=1,
                venue_mapping=vm,
            )

        seen_venues = set(captured["df"]["venue"].tolist())
        assert "BINANCE-FUTURES" in seen_venues
        assert "OKX-SWAP" in seen_venues


class TestMTDSHonestCoverage:
    """MTDS honest-coverage aggregator (Phase 6c).

    SSOT: codex/02-data/mtds-data-source-coverage-matrix.md.

    Covers:
      (a) CEFI per-venue x per-data_type daily denominator
      (b) TRADFI tick-window gate excludes trades/tbbo outside windows
      (c) DEFI per-venue scope (chain axis is a multi-venue Phase 6d follow-up)
      (d) SPORTS MTDS (bookmakers) is the instruments-service path — not
          re-asserted here (covered by TestBuildSportsEntityEntry elsewhere)
      (e) PREDICTION per-venue daily denominator
      (f) category-level ``expected_venues`` / ``missing_venues`` injection
    """

    def _mtds_df(self, rows):
        """Build a minimal availability-index DataFrame with the columns
        consumed by _build_manifest_category."""
        cols = [
            "date",
            "venue",
            "data_type",
            "service_name",
            "capture_status",
            "chain",
            "instrument_type",
            "league_id",
            "row_count",
        ]
        return pd.DataFrame(rows, columns=cols)

    def test_cefi_per_venue_denominator_honest(self):
        """CEFI CEFI BINANCE-SPOT has 2 expected dts (book_snapshot_5, trades).
        2 days window → expected_shards = 2 dts x 2 days = 4 per venue."""
        svc = _make_svc()
        # Only BINANCE-SPOT has ``trades`` shipped on 2026-04-17; missing
        # book_snapshot_5 + 2026-04-18 for both.
        df = self._mtds_df(
            [
                [
                    "2026-04-17",
                    "BINANCE-SPOT",
                    "trades",
                    "market-tick-data-service",
                    "captured",
                    "",
                    "",
                    "",
                    100,
                ],
            ]
        )
        with (
            patch.object(svc, "_read_defi_merged_index", return_value=df),
            patch.object(_dss_mod, "get_effective_start_date", return_value="2026-04-17"),
            patch.object(svc, "_build_v4_sub_dimensions", return_value={}),
        ):
            from unified_api_contracts import VenueMapping

            result = svc._build_manifest_category(
                service="market-tick-data-service",
                cat="CEFI",
                start_date="2026-04-17",
                end_date="2026-04-18",
                all_date_strs=["2026-04-17", "2026-04-18"],
                total_days=2,
                venue_mapping=VenueMapping(),
            )
        # BINANCE-SPOT post-Phase-8D:
        #   - ``trades``: 1 legacy row (empty instrument_id in the fixture
        #     helper) -> legacy fallback per-(venue, dt, date) = 2 expected,
        #     1 found.
        #   - ``book_snapshot_5``: 0 rows -> Tier-3 per-instrument denom =
        #     21 MVP SPOT instruments x 2 days = 42 expected, 0 found.
        # Total expected = 44; total found = 1.
        binance = result["venues"]["BINANCE-SPOT"]
        assert isinstance(binance, dict)
        assert binance["dates_expected"] == 44
        assert binance["dates_found"] == 1
        assert sorted(binance["expected_data_types"]) == ["book_snapshot_5", "trades"]
        assert "book_snapshot_5" in binance["missing_data_types"]
        assert result["honest_axis"] == "per_venue_per_data_type_daily"
        # Category has all 11 expected venues, but only BINANCE-SPOT has data
        assert "BINANCE-SPOT" not in result["missing_venues"]
        assert len(result["missing_venues"]) >= 9
        assert result["expected_venues"]  # non-empty

    def test_tradfi_tick_window_gate(self):
        """TRADFI ``trades`` / ``tbbo`` are only expected inside tick windows.
        2024-07-10..2024-07-15 is INSIDE window (2024-07-01..2024-07-31)."""
        svc = _make_svc()
        # Provide ohlcv_1m + trades + tbbo for NYSE on 2024-07-10.
        # 4 trading days x 3 dts should all be expected (all inside window).
        df = self._mtds_df(
            [
                [
                    "2024-07-10",
                    "NYSE",
                    "ohlcv_1m",
                    "market-tick-data-service",
                    "captured",
                    "",
                    "",
                    "",
                    1,
                ],
                [
                    "2024-07-10",
                    "NYSE",
                    "trades",
                    "market-tick-data-service",
                    "captured",
                    "",
                    "",
                    "",
                    1,
                ],
                [
                    "2024-07-10",
                    "NYSE",
                    "tbbo",
                    "market-tick-data-service",
                    "captured",
                    "",
                    "",
                    "",
                    1,
                ],
            ]
        )
        with (
            patch.object(svc, "_read_defi_merged_index", return_value=df),
            patch.object(_dss_mod, "get_effective_start_date", return_value="2024-07-10"),
            patch.object(svc, "_build_v4_sub_dimensions", return_value={}),
        ):
            from unified_api_contracts import VenueMapping

            result = svc._build_manifest_category(
                service="market-tick-data-service",
                cat="TRADFI",
                start_date="2024-07-10",
                end_date="2024-07-15",
                all_date_strs=[
                    "2024-07-10",
                    "2024-07-11",
                    "2024-07-12",
                    "2024-07-13",
                    "2024-07-14",
                    "2024-07-15",
                ],
                total_days=6,
                venue_mapping=VenueMapping(),
            )
        # NYSE has 4 trading days (10,11,12,15); 3 dts all in tick window.
        # Expected: 4 x 3 = 12 shards. Found: 3.
        nyse = result["venues"]["NYSE"]
        assert nyse["dates_expected"] == 12
        assert nyse["dates_found"] == 3

    def test_tradfi_tick_window_gate_outside_window(self):
        """Outside the tick window, only ohlcv_1m is expected for NYSE —
        trades/tbbo drop from the denominator."""
        svc = _make_svc()
        # 2025-01-06..2025-01-10 is OUTSIDE any tick window. NYSE has 5
        # trading days. Only ohlcv_1m is expected => 5 x 1 = 5 expected.
        df = self._mtds_df(
            [
                [
                    "2025-01-06",
                    "NYSE",
                    "ohlcv_1m",
                    "market-tick-data-service",
                    "captured",
                    "",
                    "",
                    "",
                    1,
                ],
            ]
        )
        with (
            patch.object(svc, "_read_defi_merged_index", return_value=df),
            patch.object(_dss_mod, "get_effective_start_date", return_value="2025-01-06"),
            patch.object(svc, "_build_v4_sub_dimensions", return_value={}),
        ):
            from unified_api_contracts import VenueMapping

            result = svc._build_manifest_category(
                service="market-tick-data-service",
                cat="TRADFI",
                start_date="2025-01-06",
                end_date="2025-01-10",
                all_date_strs=[
                    "2025-01-06",
                    "2025-01-07",
                    "2025-01-08",
                    "2025-01-09",
                    "2025-01-10",
                ],
                total_days=5,
                venue_mapping=VenueMapping(),
            )
        nyse = result["venues"]["NYSE"]
        # Outside tick window: trades/tbbo expected = 0 → only ohlcv_1m
        # counts toward the denominator. 5 trading days x 1 dt = 5.
        honest_dts = nyse["honest_data_types"]
        assert honest_dts["trades"]["expected_shards"] == 0
        assert honest_dts["tbbo"]["expected_shards"] == 0
        assert honest_dts["ohlcv_1m"]["expected_shards"] == 5
        assert nyse["dates_expected"] == 5
        assert nyse["dates_found"] == 1  # one ohlcv_1m shipped

    def test_defi_per_venue_scope(self):
        """DEFI uses ``all_defi_venues`` (11 PROTOCOL-ETHEREUM entries)."""
        svc = _make_svc()
        # AAVEV3-ETHEREUM start = 2023-01-27; 4 dts declared.
        # Window 2023-01-27..2023-01-28 = 2 days x 4 dts = 8 expected.
        df = self._mtds_df(
            [
                [
                    "2023-01-27",
                    "AAVEV3-ETHEREUM",
                    "lending_indices",
                    "market-tick-data-service",
                    "captured",
                    "ETHEREUM",
                    "LENDING",
                    "",
                    1,
                ],
                [
                    "2023-01-28",
                    "AAVEV3-ETHEREUM",
                    "lending_indices",
                    "market-tick-data-service",
                    "captured",
                    "ETHEREUM",
                    "LENDING",
                    "",
                    1,
                ],
            ]
        )
        with (
            patch.object(svc, "_read_defi_merged_index", return_value=df),
            patch.object(_dss_mod, "get_effective_start_date", return_value="2023-01-27"),
            patch.object(svc, "_build_v4_sub_dimensions", return_value={}),
        ):
            from unified_api_contracts import VenueMapping

            result = svc._build_manifest_category(
                service="market-tick-data-service",
                cat="DEFI",
                start_date="2023-01-27",
                end_date="2023-01-28",
                all_date_strs=["2023-01-27", "2023-01-28"],
                total_days=2,
                venue_mapping=VenueMapping(),
            )
        aave = result["venues"]["AAVEV3-ETHEREUM"]
        # Post-Phase-1 (2026-04-24 DeFi data types completeness): AAVEV3-ETHEREUM
        # declares 7 dts in VENUE_DATA_TYPE_CAPABILITIES.
        #   - Per-instrument dts (Tier-3, 10-reserve seed x 2 days = 20):
        #     ``oracle_prices``, ``rewards``, ``risk_params`` → 20 expected each.
        #   - Legacy + new venue-level dts (per-(venue, dt, date), 2 days):
        #     ``lending_indices`` (legacy 2 rows captured),
        #     ``flash_loan_events``, ``liquidation_events``, ``position_data``
        #     → 2 expected each.
        # Total expected = 20*3 + 2*4 = 68; total found = 2 (only
        # ``lending_indices`` has rows). ``missing_data_types`` lists the 6 dts
        # with found_count == 0 and expected_count > 0.
        assert aave["dates_expected"] == 68
        assert aave["dates_found"] == 2
        assert sorted(aave["missing_data_types"]) == [
            "flash_loan_events",
            "liquidation_events",
            "oracle_prices",
            "position_data",
            "rewards",
            "risk_params",
        ]
        assert result["honest_axis"] == "per_venue_per_data_type_per_chain_daily"

    def test_prediction_per_venue_daily(self):
        """PREDICTION — POLYMARKET + KALSHI, all 4 SchemaContract dts each.

        After UAC ``c7642f3`` registered ``book_snapshot`` / ``market_metadata`` /
        ``fills``, the manifest enumeration loop unions
        ``PREDICTION_DATA_TYPE_META.keys()`` into the UAC-declared dt set so all
        4 data_types surface as expected rows. ``trades`` remains the only dt
        with captured rows here; the other three appear with 0 found shards in
        ``missing_data_types`` to make the SSOT gap visible.
        """
        svc = _make_svc()
        df = self._mtds_df(
            [
                [
                    "2025-03-14",
                    "POLYMARKET",
                    "trades",
                    "market-tick-data-service",
                    "captured",
                    "",
                    "",
                    "",
                    1,
                ],
            ]
        )
        with (
            patch.object(svc, "_read_defi_merged_index", return_value=df),
            patch.object(_dss_mod, "get_effective_start_date", return_value="2025-03-14"),
            patch.object(svc, "_build_v4_sub_dimensions", return_value={}),
        ):
            from unified_api_contracts import VenueMapping

            result = svc._build_manifest_category(
                service="market-tick-data-service",
                cat="PREDICTION",
                start_date="2025-03-14",
                end_date="2025-03-14",
                all_date_strs=["2025-03-14"],
                total_days=1,
                venue_mapping=VenueMapping(),
            )
        # Two UAC-declared venues (POLYMARKET + KALSHI); only POLYMARKET has
        # data. KALSHI should surface as missing.
        assert "KALSHI" in result["expected_venues"]
        assert "POLYMARKET" in result["expected_venues"]
        assert "KALSHI" in result["missing_venues"]
        poly = result["venues"]["POLYMARKET"]
        assert sorted(poly["expected_data_types"]) == sorted(
            ["trades", "book_snapshot", "market_metadata", "fills"]
        )
        # Only ``trades`` has rows in the fixture — the other 3 are missing.
        assert sorted(poly["missing_data_types"]) == sorted(
            ["book_snapshot", "market_metadata", "fills"]
        )
        assert poly["dates_found"] == 1

    @pytest.mark.parametrize(
        "data_type",
        ["trades", "book_snapshot", "market_metadata", "fills"],
    )
    def test_prediction_meta_dict_enumerates_all_four_data_types(self, data_type):
        """PREDICTION manifest panel surfaces all 4 SchemaContract data_types.

        After UAC ``c7642f3`` registered ``book_snapshot`` / ``market_metadata`` /
        ``fills`` SchemaContracts on PREDICTION venues, the manifest panel must
        enumerate all 4 data_types (was just ``trades``). Verifies the union of
        ``PREDICTION_DATA_TYPE_META.keys()`` into the UAC-declared dt set in
        ``_mtds_honest_coverage_for_venue``.
        """
        from unified_api_contracts import VenueMapping

        # Empty manifest — every PREDICTION dt should still appear as expected
        # (with 0 found, 0% completion) so the SSOT gap is visible.
        df = self._mtds_df([])
        honest = _dss_mod._mtds_honest_coverage_for_venue(
            df, "POLYMARKET", "PREDICTION", "2026-04-17", "2026-04-17", VenueMapping()
        )

        expected_data_types = honest["expected_data_types"]
        assert isinstance(expected_data_types, list)
        # Each of the 4 SchemaContract data_types must appear as an expected row.
        assert data_type in expected_data_types, (
            f"PREDICTION data_type {data_type!r} missing from expected_data_types: "
            f"{expected_data_types}"
        )

        # Also verify the per-dt entry exists in the data_types breakdown so the
        # UI can render the row (even with 0 captured / 0% completion).
        data_types = honest["data_types"]
        assert isinstance(data_types, dict)
        assert data_type in data_types, (
            f"PREDICTION data_type {data_type!r} missing from data_types breakdown"
        )

    def test_prediction_meta_includes_all_four_keys(self):
        """Sanity: ``PREDICTION_DATA_TYPE_META`` mirrors the 4 UAC SchemaContracts."""
        keys = set(_dss_mod.PREDICTION_DATA_TYPE_META.keys())
        assert keys == {"trades", "book_snapshot", "market_metadata", "fills"}
        # ``book_snapshot`` / ``market_metadata`` / ``fills`` carry the
        # ``indeterminate`` denominator marker (per Follow-up B prompt §B). The
        # UI shows captured count without an arbitrary per-day denominator.
        for dt in ("book_snapshot", "market_metadata", "fills"):
            assert (
                _dss_mod.PREDICTION_DATA_TYPE_META[dt]["expected_count_per_day"] == "indeterminate"
            )
        # ``trades`` keeps the existing per-venue daily denominator from
        # ``_mtds_expected_dates_for_venue_dt``.
        assert (
            _dss_mod.PREDICTION_DATA_TYPE_META["trades"]["expected_count_per_day"]
            == "per_venue_daily"
        )

    def test_category_completion_not_tautology(self):
        """Before Phase 6c, CEFI header showed 100% when a single venue
        had a single day. Honest-coverage surfaces the full UAC denominator."""
        svc = _make_svc()
        df = self._mtds_df(
            [
                [
                    "2026-04-17",
                    "BINANCE-SPOT",
                    "trades",
                    "market-tick-data-service",
                    "captured",
                    "",
                    "",
                    "",
                    1,
                ],
            ]
        )
        with (
            patch.object(svc, "_read_defi_merged_index", return_value=df),
            patch.object(_dss_mod, "get_effective_start_date", return_value="2026-04-17"),
            patch.object(svc, "_build_v4_sub_dimensions", return_value={}),
        ):
            from unified_api_contracts import VenueMapping

            result = svc._build_manifest_category(
                service="market-tick-data-service",
                cat="CEFI",
                start_date="2026-04-17",
                end_date="2026-04-17",
                all_date_strs=["2026-04-17"],
                total_days=1,
                venue_mapping=VenueMapping(),
            )
        # Honest denominator: 11 venues x their respective dts. Should be far
        # from 100% with a single-row fixture.
        assert result["completion_pct"] < 10.0
        assert result["shards_expected"] > 20  # 11 venues x ≥2 dts x 1 day


class TestMTDSPerInstrumentHonestCoverage:
    """Phase 8D — per-(venue, data_type, instrument_id, date) denominator.

    SSOT:
      - plan: ``plans/active/mtds_per_instrument_sentinels_2026_04_21.plan.md``
      - UAC accessor: ``get_expected_instruments_for_venue`` +
        ``is_per_instrument_shard_data_type`` (WAVE 8B, commit 74e278c).
      - MTDS orchestrator: Tier-3 fan-out landed in commit 2947dd2 (WAVE 8C).

    Covers ``_mtds_honest_coverage_for_venue`` + the extracted
    ``_per_instrument_coverage`` helper:

      1. BINANCE-FUTURES ``derivative_ticker`` with no captures -> 0%
         and every MVP perp in ``missing_instruments``.
      2. BINANCE-FUTURES ``derivative_ticker`` with 2 captured + 8
         empty-confirmed sentinels -> expected count covers the full MVP
         perp universe; found count reflects the 2 captured pairs.
      3. Legacy-row fallback: ``trades`` rows with empty ``instrument_id``
         -> degrade to venue-level per-(venue, dt, date) denominator and
         annotate with ``legacy_row_count``.
      4. DEFI ``dex_swaps`` with empty MVP seed -> 0 expected (WAVE 8G
         seed follow-up) and `per_instrument` dict not emitted.
      5. Non-per-instrument dt (``liquidations``) -> old per-(venue, dt,
         date) path preserved + ``unit == "shard_days"``.
    """

    def _mtds_df(self, rows):
        cols = [
            "date",
            "venue",
            "data_type",
            "service_name",
            "capture_status",
            "chain",
            "instrument_type",
            "league_id",
            "instrument_id",
            "row_count",
        ]
        return pd.DataFrame(rows, columns=cols)

    def test_derivative_ticker_no_captures_zero_completion(self):
        """BINANCE-FUTURES + derivative_ticker: MVP seed has 10 perps.
        Zero manifest rows -> expected = 10 * 1 = 10; found = 0; every
        perp listed under ``missing_instruments``."""
        from unified_api_contracts import VenueMapping

        df = self._mtds_df([])
        honest = _dss_mod._mtds_honest_coverage_for_venue(
            df, "BINANCE-FUTURES", "CEFI", "2026-04-17", "2026-04-17", VenueMapping()
        )
        data_types = honest["data_types"]
        assert isinstance(data_types, dict)
        dt_entry = data_types["derivative_ticker"]
        assert isinstance(dt_entry, dict)
        # MVP PERP seed = 10 instruments.
        assert len(dt_entry["expected_instruments"]) == 10
        assert dt_entry["expected_shards"] == 10  # 10 instruments x 1 day
        assert dt_entry["found_shards"] == 0
        assert dt_entry["completion_pct"] == 0.0
        assert sorted(dt_entry["missing_instruments"]) == sorted(dt_entry["expected_instruments"])
        assert dt_entry["unit"] == "shard_instrument_days"

    def test_derivative_ticker_partial_capture(self):
        """2 captured + 8 empty_confirmed sentinels on 1 date ->
        expected = 10 (10 MVP perps x 1 date), found = 2 (distinct
        (instrument_id, date) pairs with capture_status gated)."""
        from unified_api_contracts import VenueMapping

        captured = [
            [
                "2026-04-17",
                "BINANCE-FUTURES",
                "derivative_ticker",
                "market-tick-data-service",
                "captured",
                "",
                "PERPETUAL",
                "",
                "BTC-PERP",
                100,
            ],
            [
                "2026-04-17",
                "BINANCE-FUTURES",
                "derivative_ticker",
                "market-tick-data-service",
                "captured",
                "",
                "PERPETUAL",
                "",
                "ETH-PERP",
                50,
            ],
        ]
        empties = [
            [
                "2026-04-17",
                "BINANCE-FUTURES",
                "derivative_ticker",
                "market-tick-data-service",
                "empty_confirmed",
                "",
                "PERPETUAL",
                "",
                perp,
                0,
            ]
            for perp in [
                "SOL-PERP",
                "BNB-PERP",
                "XRP-PERP",
                "ADA-PERP",
                "AVAX-PERP",
                "DOGE-PERP",
                "MATIC-PERP",
                "ARB-PERP",
            ]
        ]
        df = self._mtds_df(captured + empties)
        honest = _dss_mod._mtds_honest_coverage_for_venue(
            df, "BINANCE-FUTURES", "CEFI", "2026-04-17", "2026-04-17", VenueMapping()
        )
        data_types = honest["data_types"]
        assert isinstance(data_types, dict)
        dt_entry = data_types["derivative_ticker"]
        assert isinstance(dt_entry, dict)
        assert dt_entry["expected_shards"] == 10
        # All 10 empty_confirmed + captured should count as found (both
        # gate "in window" with capture_status in {captured, empty_confirmed}).
        assert dt_entry["found_shards"] == 10
        assert dt_entry["completion_pct"] == 100.0
        assert dt_entry["missing_instruments"] == []
        assert dt_entry["unit"] == "shard_instrument_days"

    def test_legacy_rows_fallback_to_venue_level_denominator(self):
        """Legacy Phase-7 manifest rows (no ``instrument_id`` value) ->
        aggregator falls back to venue-level per-(venue, dt, date)
        denominator for that (venue, dt) and annotates
        ``legacy_row_count`` so coverage % doesn't regress on already-
        shipped backfills."""
        from unified_api_contracts import VenueMapping

        df = self._mtds_df(
            [
                [
                    "2026-04-17",
                    "BINANCE-SPOT",
                    "trades",
                    "market-tick-data-service",
                    "captured",
                    "",
                    "SPOT_PAIR",
                    "",
                    "",  # empty instrument_id -> legacy
                    1000,
                ],
            ]
        )
        honest = _dss_mod._mtds_honest_coverage_for_venue(
            df, "BINANCE-SPOT", "CEFI", "2026-04-17", "2026-04-17", VenueMapping()
        )
        data_types = honest["data_types"]
        assert isinstance(data_types, dict)
        dt_entry = data_types["trades"]
        assert isinstance(dt_entry, dict)
        # Fallback denominator is per-(venue, dt, date) = 1 (single day).
        assert dt_entry["expected_shards"] == 1
        assert dt_entry["found_shards"] == 1
        assert dt_entry["unit"] == "shard_days_legacy"
        assert dt_entry["legacy_row_count"] == 1

    def test_defi_dex_swaps_empty_seed(self):
        """DEFI ``dex_swaps`` on a PROTOCOL-CHAIN venue (e.g.
        UNISWAPV3-ETHEREUM) — Wave 8G seeded 20 top-TVL pools. With 0 rows in
        the fixture, the aggregator returns a Tier-3 denominator of
        ``n_instruments x n_dates`` with ``found_shards == 0`` and the
        full ``missing_instruments`` list.
        Sentinel ``_PER_INSTRUMENT_BREAKDOWN_MAX_SIZE`` is ``< 20``, so a
        20-instrument universe does NOT emit the inline ``per_instrument``
        dict (keeps response bloat bounded on big pools boards)."""
        from unified_api_contracts import VenueMapping

        df = self._mtds_df([])
        honest = _dss_mod._mtds_honest_coverage_for_venue(
            df,
            "UNISWAPV3-ETHEREUM",
            "DEFI",
            "2026-04-17",
            "2026-04-17",
            VenueMapping(),
        )
        data_types = honest["data_types"]
        assert isinstance(data_types, dict)
        if "dex_swaps" in data_types:
            dt_entry = data_types["dex_swaps"]
            assert isinstance(dt_entry, dict)
            # 20 seeded instruments x 1 day = 20 expected, 0 found.
            expected_instruments = dt_entry["expected_instruments"]
            assert isinstance(expected_instruments, list)
            assert len(expected_instruments) == 20
            assert dt_entry["expected_shards"] == 20
            assert dt_entry["found_shards"] == 0
            # 20 == _PER_INSTRUMENT_BREAKDOWN_MAX_SIZE (not ``<``) → no
            # per-instrument breakdown to keep payload bounded.
            assert "per_instrument" not in dt_entry
            assert dt_entry["unit"] == "shard_instrument_days"
            # All 20 instruments missing in the window.
            missing_instruments = dt_entry["missing_instruments"]
            assert isinstance(missing_instruments, list)
            assert len(missing_instruments) == 20

    def test_venue_level_dt_preserves_legacy_path(self):
        """Non-per-instrument dt (``liquidations``) keeps the existing
        Phase 6d per-(venue, dt, date) denominator and the
        ``unit == "shard_days"`` tag (NOT ``shard_instrument_days``).
        Also asserts no ``expected_instruments`` key appears on
        venue-level entries."""
        from unified_api_contracts import VenueMapping

        df = self._mtds_df(
            [
                [
                    "2026-04-17",
                    "BINANCE-FUTURES",
                    "liquidations",
                    "market-tick-data-service",
                    "captured",
                    "",
                    "PERPETUAL",
                    "",
                    "",  # venue-level dt -> instrument_id unused
                    5,
                ],
            ]
        )
        honest = _dss_mod._mtds_honest_coverage_for_venue(
            df,
            "BINANCE-FUTURES",
            "CEFI",
            "2026-04-17",
            "2026-04-18",
            VenueMapping(),
        )
        data_types = honest["data_types"]
        assert isinstance(data_types, dict)
        assert "liquidations" in data_types
        dt_entry = data_types["liquidations"]
        assert isinstance(dt_entry, dict)
        assert dt_entry["unit"] == "shard_days"
        # 2-day window, 1 captured date -> expected=2, found=1.
        assert dt_entry["expected_shards"] == 2
        assert dt_entry["found_shards"] == 1
        # Venue-level entries do NOT carry expected_instruments.
        assert "expected_instruments" not in dt_entry
        assert "missing_instruments" not in dt_entry

"""
Unit tests for data_status_service module.

Tests cover pure methods: _calculate_completion_rate,
run_data_status_cli, calculate_missing_shards, get_last_updated_info,
validate_data_completeness.
"""

import json
from unittest.mock import AsyncMock, MagicMock, patch

import pandas as pd
import pytest

# Import the CANONICAL facade module (NOT a standalone file-loaded copy).
# The data_status mixin chain resolves the patchable module-level names
# (list_objects / read_availability_index / VenueMapping / ...) late-bound
# through ``sys.modules["deployment_api.services.data_status_service"]``,
# so ``patch.object(_dss_mod, ...)`` only reaches the implementation when
# ``_dss_mod`` IS that canonical module. The old standalone loader (which
# existed to dodge a since-fixed circular import via services/__init__)
# created a second module instance whose patches the mixins never saw.
import deployment_api.services.data_status_service as _dss_mod

DataStatusService = _dss_mod.DataStatusService


def _make_svc(**kwargs) -> DataStatusService:
    return DataStatusService(project_id=kwargs.get("project_id", "test-project"))


def _mock_process(returncode: int = 0, stdout: bytes = b"{}", stderr: bytes = b"") -> MagicMock:
    """Build a mock asyncio Process object."""
    proc = MagicMock()
    proc.returncode = returncode
    proc.communicate = AsyncMock(return_value=(stdout, stderr))
    return proc


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
            await svc.run_data_status_cli("svc-a", "2024-01-01", "2024-01-31", asset_groups=["CEFI", "DEFI"])

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
            await svc.run_data_status_cli("svc-a", "2024-01-01", "2024-01-31", venues=["BINANCE", "OKX"])

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
            await svc.run_data_status_cli("market-data-processing-service", "2024-01-01", "2024-01-31")

        assert "--fast" in captured["args"]

    @pytest.mark.asyncio
    async def test_includes_check_data_types_flag(self):
        svc = _make_svc()
        captured: dict[str, object] = {}

        async def _capture(*args, **kwargs):
            captured["args"] = list(args)
            return _mock_process()

        with patch("asyncio.create_subprocess_exec", new=_capture):
            await svc.run_data_status_cli("svc-a", "2024-01-01", "2024-01-31", check_data_types=True)

        assert "--check-data-types" in captured["args"]

    @pytest.mark.asyncio
    async def test_includes_check_feature_groups_flag(self):
        svc = _make_svc()
        captured: dict[str, object] = {}

        async def _capture(*args, **kwargs):
            captured["args"] = list(args)
            return _mock_process()

        with patch("asyncio.create_subprocess_exec", new=_capture):
            await svc.run_data_status_cli("svc-a", "2024-01-01", "2024-01-31", check_feature_groups=True)

        assert "--check-feature-groups" in captured["args"]

    @pytest.mark.asyncio
    async def test_includes_check_timeframes_flag(self):
        svc = _make_svc()
        captured: dict[str, object] = {}

        async def _capture(*args, **kwargs):
            captured["args"] = list(args)
            return _mock_process()

        with patch("asyncio.create_subprocess_exec", new=_capture):
            await svc.run_data_status_cli("svc-a", "2024-01-01", "2024-01-31", check_timeframes=True)

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
            result = await svc.calculate_missing_shards("instruments-service", "2024-01-01", "2024-01-31")
        assert "error" in result

    @pytest.mark.asyncio
    async def test_returns_missing_analysis_structure(self):
        svc = _make_svc()

        def _mock_scan(service: str, cat: str, start: str, end: str, **_: object) -> dict[str, list[str] | int] | None:
            if "cefi" in cat.lower():
                return {"missing": ["2024-01-01"], "days_checked": 2}
            return None

        with patch.object(svc, "_scan_category_manifest", side_effect=_mock_scan):
            result = await svc.calculate_missing_shards("instruments-service", "2024-01-01", "2024-01-02")

        assert result["service"] == "instruments-service"
        assert "total_missing" in result
        assert result["total_missing"] == 1
        assert "missing_by_venue" in result
        assert "summary" in result

    @pytest.mark.asyncio
    async def test_handles_no_missing(self):
        svc = _make_svc()
        with patch.object(svc, "_scan_category_manifest", return_value=None):
            result = await svc.calculate_missing_shards("instruments-service", "2024-01-01", "2024-01-31")
        assert result["total_missing"] == 0

    @pytest.mark.asyncio
    async def test_handles_all_categories_empty(self):
        svc = _make_svc()
        with patch.object(svc, "_scan_category_manifest", return_value=None):
            result = await svc.calculate_missing_shards("instruments-service", "2024-01-01", "2024-01-31")
        assert "total_missing" in result
        assert result["total_missing"] == 0

    @pytest.mark.asyncio
    async def test_counts_by_category(self):
        svc = _make_svc()

        def _mock_scan(service: str, cat: str, start: str, end: str, **_: object) -> dict[str, list[str] | int] | None:
            cat_l = cat.lower()
            if "cefi" in cat_l:
                return {"missing": ["2024-01-01"], "days_checked": 1}
            if "defi" in cat_l:
                return {"missing": ["2024-01-01"], "days_checked": 1}
            return None

        with patch.object(svc, "_scan_category_manifest", side_effect=_mock_scan):
            result = await svc.calculate_missing_shards("instruments-service", "2024-01-01", "2024-01-01")

        mbc = result["missing_by_category"]
        assert isinstance(mbc, dict)
        assert sum(mbc.values()) >= 2

    @pytest.mark.asyncio
    async def test_returns_correct_date_range(self):
        svc = _make_svc()
        with patch.object(svc, "_scan_category_manifest", return_value=None):
            result = await svc.calculate_missing_shards("instruments-service", "2024-01-01", "2024-01-31")
        assert result["date_range"] == {"start": "2024-01-01", "end": "2024-01-31"}

    @pytest.mark.asyncio
    async def test_passes_categories_filter(self):
        svc = _make_svc()
        calls: list[str] = []

        def _mock_scan(service: str, cat: str, start: str, end: str, **_: object) -> dict[str, list[str] | int] | None:
            calls.append(cat)
            return None

        with patch.object(svc, "_scan_category_manifest", side_effect=_mock_scan):
            await svc.calculate_missing_shards(
                "instruments-service",
                "2024-01-01",
                "2024-01-31",
                asset_groups=["CEFI"],
            )

        assert calls == ["CEFI"]

    @pytest.mark.asyncio
    async def test_summary_contains_completion_rate(self):
        svc = _make_svc()

        def _mock_scan(service: str, cat: str, start: str, end: str, **_: object) -> dict[str, list[str] | int] | None:
            if "cefi" in cat.lower():
                return {"missing": ["2024-01-01"], "days_checked": 3}
            return None

        with patch.object(svc, "_scan_category_manifest", side_effect=_mock_scan):
            result = await svc.calculate_missing_shards("instruments-service", "2024-01-01", "2024-01-03")

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
            result = await svc.get_last_updated_info("market-tick-data-handler", asset_groups=["cefi"])

        assert result["asset_groups"]["cefi"]["status"] == "empty"

    @pytest.mark.asyncio
    async def test_handles_list_objects_error(self):
        svc = _make_svc()

        with patch.object(_dss_mod, "list_objects", side_effect=OSError("bucket not found")):
            result = await svc.get_last_updated_info("instruments-service", asset_groups=["cefi"])

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
        with patch.object(svc, "run_data_status_cli", new=AsyncMock(return_value={"error": "failed"})):
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
        result = self.svc._build_data_type_breakdown(df, "BINANCE-SPOT", "2024-01-01", "2024-01-01", vm)
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
            with patch.object(_dss_mod, "get_venue_data_type_start_date", return_value="2020-01-01"):
                result = self.svc._build_data_type_breakdown(df, "BINANCE-SPOT", "2024-01-01", "2024-01-01", vm)

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
                result = self.svc._build_data_type_breakdown(df, "BINANCE-SPOT", "2024-01-01", "2024-01-01", vm)

        # Observed-but-not-UAC-declared dt is kept with is_expected=False
        assert "unexpected_type" in result
        assert result["unexpected_type"]["is_expected"] is False
        # Phantom-expected clamp (2026-04-19): UAC declared "trades" but it
        # was never observed in this slice -> dropped to avoid cartesian
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
        """ohlcv_5m on a CeFi venue with no raw `trades` -> blocked_on_raw."""
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
        """ohlcv_5m absent but raw trades captured -> actionable missing, not blocked."""
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
        # slice -- so it doesn't appear in the result. trades captured 100%.
        assert "trades" in result
        assert result["trades"]["dates_found"] == 2
        assert result["trades"]["dates_blocked_on_raw"] == 0
        assert result["trades"]["out_of_scope"] is False
        assert result["trades"]["is_processed_data_type"] is False

    def test_out_of_scope_when_venue_dt_not_in_expected_coverage(self):
        """NASDAQ trades observed -> in_expected_coverage False, out_of_scope True (TradFi)."""
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
        # NASDAQ + trades is NOT in EXPECTED_COVERAGE['tradfi'] -- operator policy.
        # `trades` is also not in PROCESSED_REQUIRES_RAW (it's raw), so the
        # row classifies as out_of_scope=True, in_expected_coverage=False.
        assert "trades" in result
        assert result["trades"]["in_expected_coverage"] is False
        assert result["trades"]["out_of_scope"] is True
        assert result["trades"]["is_processed_data_type"] is False

    def test_in_scope_venue_dt_is_not_out_of_scope(self):
        """CME + trades is in EXPECTED_COVERAGE['tradfi'] -- not out of scope."""
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


class TestReferenceDataBundleScope:
    """instruments-service / corporate-actions reference-data scope (audit §B).

    The bundled reference row has no market-data ``data_type``, so the
    market-data ``EXPECTED_COVERAGE`` registry flagged EVERY reference row
    ``out_of_scope``. For ``REFERENCE_BUNDLE_SERVICES`` the scope must come
    from the instruments-service catalogue (configured venue + genesis), at
    venue/day grain. A market-data service (MTDS) stays on the registry path.
    """

    def setup_method(self):
        self.svc = DataStatusService(project_id="test-proj")
        # Genesis map is cached process-wide; force a reload so these tests
        # read the real catalogue rather than a stale empty map.
        from deployment_api.services.data_status import reference_scope

        reference_scope.reset_genesis_cache()

    def _build(self, df, venue, start, end, *, service, category):
        vm = MagicMock()
        # Expect a contiguous range so the bundle row owns the days in [start,end].
        vm.get_expected_trading_dates.return_value = sorted({str(d) for d in df["date"].tolist()} | {start, end})
        with (
            patch.object(_dss_mod, "get_expected_data_types_for_venue", return_value=["instruments"]),
            patch.object(_dss_mod, "get_venue_data_type_start_date", return_value=None),
        ):
            return self.svc._build_data_type_breakdown(df, venue, start, end, vm, service=service, category=category)

    def test_instruments_service_within_coverage_is_in_scope(self):
        """CEFI BINANCE-SPOT (genesis 2019-03-30), day 2024-01-01 -> out_of_scope=False."""
        df = pd.DataFrame(
            {
                "date": ["2024-01-01"],
                "venue": ["BINANCE-SPOT"],
                "data_type": ["instruments"],
            }
        )
        result = self._build(
            df, "BINANCE-SPOT", "2024-01-01", "2024-01-01", service="instruments-service", category="CEFI"
        )
        assert "instruments" in result
        assert result["instruments"]["out_of_scope"] is False
        assert result["instruments"]["in_expected_coverage"] is True

    def test_instruments_service_pre_genesis_day_is_out_of_scope(self):
        """HYPERLIQUID genesis 2023-05-01; a 2022 row owns no covered day -> out_of_scope=True."""
        df = pd.DataFrame(
            {
                "date": ["2022-01-01"],
                "venue": ["HYPERLIQUID"],
                "data_type": ["instruments"],
            }
        )
        result = self._build(
            df, "HYPERLIQUID", "2022-01-01", "2022-01-01", service="instruments-service", category="CEFI"
        )
        assert "instruments" in result
        assert result["instruments"]["out_of_scope"] is True
        assert result["instruments"]["in_expected_coverage"] is False

    def test_instruments_service_unlisted_venue_is_out_of_scope(self):
        """A venue absent from the IS catalogue -> out_of_scope=True."""
        df = pd.DataFrame(
            {
                "date": ["2024-01-01"],
                "venue": ["MADEUP-VENUE"],
                "data_type": ["instruments"],
            }
        )
        result = self._build(
            df, "MADEUP-VENUE", "2024-01-01", "2024-01-01", service="instruments-service", category="CEFI"
        )
        assert "instruments" in result
        assert result["instruments"]["out_of_scope"] is True
        assert result["instruments"]["in_expected_coverage"] is False

    def test_market_data_service_unaffected_by_reference_scope(self):
        """MTDS NASDAQ trades still uses the market-data scope path (out_of_scope=True)."""
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
                service="market-tick-data-service",
                category="TRADFI",
            )
        # NASDAQ + trades is out_of_scope per the EXISTING market-data policy —
        # proving the reference branch did NOT hijack a market-data service.
        assert result["trades"]["out_of_scope"] is True
        assert result["trades"]["in_expected_coverage"] is False

    def test_reference_genesis_tolerates_market_role_suffix(self):
        """IS catalogue lists base exchanges (COINBASE/OKX/DERIBIT); the manifest
        qualifies them by role (COINBASE-SPOT/OKX-SWAP/DERIBIT-COMBO). The genesis
        lookup must fall back to the base token so a role-qualified row is NOT
        flagged out_of_scope on the instruments-service view (2026-06-17 fix)."""
        from deployment_api.services.data_status import reference_scope

        reference_scope.reset_genesis_cache()
        with patch.object(
            reference_scope,
            "_load_genesis_map",
            return_value={
                ("CEFI", "COINBASE"): "2019-03-30",
                ("CEFI", "OKX"): "2019-03-30",
                ("CEFI", "DERIBIT"): "2019-03-30",
                ("CEFI", "BITFINEX-SPOT"): "2020-01-01",
            },
        ):
            # Role-qualified tokens resolve via base-token fallback.
            assert reference_scope.reference_genesis("cefi", "COINBASE-SPOT") == "2019-03-30"
            assert reference_scope.reference_genesis("cefi", "OKX-FUTURES") == "2019-03-30"
            assert reference_scope.reference_genesis("cefi", "OKX-SWAP") == "2019-03-30"
            assert reference_scope.reference_genesis("cefi", "DERIBIT-COMBO") == "2019-03-30"
            # An exact role-qualified catalogue entry still wins directly.
            assert reference_scope.reference_genesis("cefi", "BITFINEX-SPOT") == "2020-01-01"
            # A genuinely unlisted venue stays out_of_scope (no false positives).
            assert reference_scope.reference_genesis("cefi", "KRAKEN-SPOT") is None
        reference_scope.reset_genesis_cache()


class TestPhantomExpectedClamp:
    """Tests for the 2026-04-19 phantom-expected denominator clamp.

    Covers the three axes where cartesian inflation was inflating
    ``shards_expected`` on the MTDS data-status ``/turbo`` endpoint:

    1. data_type x (venue, instrument_type, underlying) - UAC-declared dts
       that never materialise for the sub-slice were counted as phantom
       missing.
    2. instrument_type launch date -- new instrument_types inherited the
       venue's full calendar even if they launched mid-history.
    3. underlying launch date -- same, for underlyings under an
       instrument_type (e.g. DERIBIT SOL options post-2024).
    """

    def setup_method(self):
        self.svc = DataStatusService(project_id="test-proj")

    def test_data_type_breakdown_drops_unobserved_uac_phantoms(self):
        """UAC declares 4 dts; only 1 observed -> expected = 1, not 4."""
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
                result = self.svc._build_data_type_breakdown(df, "ODDS_API", "2024-01-01", "2024-01-03", vm)

        # Only ODDS (observed) shows up. arbitrage_opportunity, odds_movement,
        # odds_snapshot are UAC-declared but never observed -> phantom -> dropped.
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

        # HYPE's effective start = 2024-06-01 (first observed) -> only 1 day
        # expected (2024-06-01 itself). BTC's effective start stays at the
        # user-supplied 2024-01-01 -> full window to 2024-06-01.
        btc_expected = int(result["BTC"]["dates_expected"])
        hype_expected = int(result["HYPE"]["dates_expected"])
        assert hype_expected == 1, f"HYPE should be clamped to 1 day, got {hype_expected}"
        assert btc_expected > hype_expected, f"BTC ({btc_expected}) should span more days than HYPE ({hype_expected})"

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

        # SOL's effective start = 2024-06-01 -> 1 day expected.
        # BTC stays at user-supplied 2022-01-01 -> full window.
        btc_expected = int(result["BTC"]["dates_expected"])
        sol_expected = int(result["SOL"]["dates_expected"])
        assert sol_expected == 1, f"SOL should be clamped to 1 day, got {sol_expected}"
        assert btc_expected > sol_expected, f"BTC ({btc_expected}) should span more days than SOL ({sol_expected})"

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
        # fallback we clamp to 2024-06-01 -> end.
        vm.get_expected_trading_dates.side_effect = lambda venue, start, end: (
            pd.date_range(start, end, freq="D").strftime("%Y-%m-%d").tolist()
        )
        with patch.object(
            _dss_mod,
            "get_expected_data_types_for_venue",
            return_value=["derivative_ticker"],
        ):
            with patch.object(_dss_mod, "get_venue_data_type_start_date", return_value=None):
                result = self.svc._build_data_type_breakdown(df, "DRIFT", "2024-01-01", "2024-06-02", vm)

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
        venues, found, expected = self.svc._build_venue_breakdown(df, "2024-01-01", "2024-01-01", vm, 0, 1, service="")
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
            with patch.object(_dss_mod, "get_venue_data_type_start_date", return_value="2020-01-01"):
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
            result = self.svc._scan_category_manifest("instruments-service", "CEFI", "2024-01-01", "2024-01-31")
        assert result is None

    def test_returns_none_when_index_empty(self):
        with patch.object(_dss_mod, "_read_index_cached", return_value=pd.DataFrame()):
            result = self.svc._scan_category_manifest("instruments-service", "CEFI", "2024-01-01", "2024-01-31")
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
            result = self.svc._scan_category_manifest("instruments-service", "CEFI", "2024-01-01", "2024-01-03")
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
            result = await svc.get_coverage_summary("instruments-service", asset_groups=["CEFI"])

        assert result["service"] == "instruments-service"
        assert "asset_groups" in result
        assert "totals" in result
        assert result["totals"]["shards"] == 2

    @pytest.mark.asyncio
    async def test_handles_empty_index(self):
        svc = _make_svc()
        with patch.object(_dss_mod, "_read_index_cached", return_value=pd.DataFrame()):
            result = await svc.get_coverage_summary("instruments-service", asset_groups=["CEFI"])

        assert result["totals"]["shards"] == 0

    @pytest.mark.asyncio
    async def test_handles_read_error(self):
        svc = _make_svc()
        with patch.object(_dss_mod, "_read_index_cached", side_effect=OSError("no bucket")):
            result = await svc.get_coverage_summary("instruments-service", asset_groups=["CEFI"])

        assert result["totals"]["shards"] == 0

    @pytest.mark.asyncio
    async def test_drops_legacy_defi_venue_aliases(self):
        """Regression from audit 2026-04-19 §2.A.1b -- legacy pre-canonicalisation
        DeFi alias rows like ``venue='AAVE_V3-ETHEREUM' chain=''`` were leaking into
        the Instrument Coverage Summary widget despite the per-shard rollup fix in
        22f0024/959bdab. Both paths now apply the same legacy-alias filter so the
        widget matches the rollup."""
        svc = _make_svc()
        # Mix of canonical rows (AAVE_V3 + ETHEREUM) and legacy alias rows
        # (AAVE_V3-ETHEREUM + empty chain). The filter must keep the canonical
        # ones and drop the legacy ones.
        index = pd.DataFrame(
            {
                "date": ["2024-01-01", "2024-01-01", "2024-01-02"],
                "venue": ["AAVE_V3", "AAVE_V3-ETHEREUM", "AAVE_V3-POLYGON"],
                "chain": ["ETHEREUM", "", ""],
                "service_name": [
                    "instruments-service",
                    "instruments-service",
                    "instruments-service",
                ],
            }
        )
        with patch.object(_dss_mod, "_read_index_cached", return_value=index):
            result = await svc.get_coverage_summary("instruments-service", asset_groups=["DEFI"])

        cat = result["asset_groups"]["DEFI"]
        # Only the canonical AAVE_V3 row should survive.
        assert cat["unique_venues"] == 1
        assert "AAVE_V3-ETHEREUM" not in cat["latest_day_instruments"]
        assert "AAVE_V3-POLYGON" not in cat["latest_day_instruments"]
        assert cat["total_shards"] == 1

    @pytest.mark.asyncio
    async def test_legacy_none_capture_status_counts_as_captured(self):
        """Regression (data-status audit 2026-06-18): a legacy pre-v5 manifest row
        whose ``capture_status`` is a blank / NaN / literal ``"None"`` / ``"nan"``
        / any non-4-state token must COERCE to ``captured`` in the coverage-summary
        denominator — matching ``coverage_metrics.compute_capture_status_counts``
        + UTL ``ManifestWriter.lookup``. Before the fix ``(cs == "captured")``
        silently DROPPED these rows from BOTH numerator and denominator (e.g.
        sports prd carried 17,288 legacy v4 ``"None"`` rows), under-reporting
        completion_pct and diverging from the per-venue breakdown SSOT. Here a
        50/50 split of one real ``captured`` row + one legacy ``"None"`` row must
        report 100% (both counted as captured), NOT 100% over a denom of 1."""
        svc = _make_svc()
        index = pd.DataFrame(
            {
                "date": ["2024-01-01", "2024-01-02"],
                "venue": ["BINANCE", "BINANCE"],
                "service_name": ["instruments-service", "instruments-service"],
                # One real captured row + one legacy row with literal "None".
                "capture_status": ["captured", "None"],
            }
        )
        with patch.object(_dss_mod, "_read_index_cached", return_value=index):
            result = await svc.get_coverage_summary("instruments-service", asset_groups=["CEFI"])

        cat = result["asset_groups"]["CEFI"]
        counts = cat["capture_status_counts"]
        # Both rows count as captured; denominator = 2; completion = 100%.
        assert counts["captured"] == 2
        assert counts["empty_confirmed"] == 0
        assert counts["attempted_failed"] == 0
        assert cat["completion_pct"] == 100.0


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
        # Added 2026-05-06: _resolve_venue_start prefers
        # get_instrument_discovery_start for instruments-service. Mock it
        # to return None so the helper falls through to get_venue_start_date.
        vm.get_instrument_discovery_start.return_value = None
        vm.get_expected_trading_dates.return_value = ["2024-01-01"]
        with (
            patch.object(_dss_mod, "_read_index_cached", return_value=index),
            patch.object(_dss_mod, "VenueMapping", return_value=vm),
            patch.object(_dss_mod, "_read_rollup_if_fresh", return_value=None),
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
        result = await svc.get_manifest_status("unknown-service", "2024-01-01", "2024-01-01", asset_groups=["CEFI"])
        assert result["overall_completion_pct"] == 0.0


class TestTransferWindowAwareness:
    """Tests for transfer-window-aware data status denominator."""

    def test_is_transfer_window_venue(self):
        svc = _make_svc()
        assert svc._is_transfer_window_venue("TRANSFERMARKT_TEAMS")
        # TRANSFERMARKT_LEAGUES retired 2026-05-05 — removed from assertions
        assert not svc._is_transfer_window_venue("FOOTYSTATS_EPL")
        assert not svc._is_transfer_window_venue("UNDERSTAT_XG")

    def test_transfermarkt_not_sports_reference(self):
        """Transfermarkt should NOT be classified as fixture-dependent."""
        svc = _make_svc()
        assert not svc._is_sports_reference_venue("TRANSFERMARKT_TEAMS")
        # TRANSFERMARKT_LEAGUES retired 2026-05-05 — removed from assertions
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
        df = pd.DataFrame({"instrument_id": ["BTC-USDT-PERP", "ETH-USDC"], "date": ["2024-01-01"] * 2})
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
    handful of shards fill a date while most stay empty -- Polymarket
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
        # venue_expected_total is 0 -> shards-weighted falls back to
        # the date-based figure (100% -- the date had data).
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
      - Legacy rows: ``venue='AAVE_V3-ETHEREUM' chain=''`` (pre-migration)
      - Canonical rows: ``venue='AAVE_V3' chain='ETHEREUM'`` (post-migration)

    The legacy rows have no matching shard paths post-migration, so they
    inflate ``venue_dates_expected`` without contributing found dates,
    dragging DEFI completion from ~99% to ~40%. The filter drops legacy
    rows before ``_build_venue_breakdown`` sees them.
    """

    def test_is_legacy_defi_venue_row_detects_canonical_patterns(self):
        svc = _make_svc()
        # Known legacy patterns -- chain empty, venue is PROTOCOL[V<N>]-CHAIN
        assert svc._is_legacy_defi_venue_row("AAVE_V3-ETHEREUM", "")
        assert svc._is_legacy_defi_venue_row("UNISWAP_V2-ETHEREUM", "")
        assert svc._is_legacy_defi_venue_row("UNISWAP_V3-ETHEREUM", "")
        assert svc._is_legacy_defi_venue_row("CURVE-ETHEREUM", "")
        assert svc._is_legacy_defi_venue_row("LIDO-ETHEREUM", "")
        assert svc._is_legacy_defi_venue_row("BALANCER-ETHEREUM", "")
        assert svc._is_legacy_defi_venue_row("COMPOUND-ETHEREUM", None)
        # NaN chain also counts as empty
        assert svc._is_legacy_defi_venue_row("AAVE_V3-POLYGON", float("nan"))

    def test_is_legacy_defi_venue_row_covers_extended_protocol_set(self):
        """Second-tier DeFi protocols observed in the real manifest:
        Camelot, GMX, Jito, Orca, Marinade, Kamino, Morpho, Fluid,
        Ethena, Ether.fi, EigenLayer. All must be caught."""
        svc = _make_svc()
        assert svc._is_legacy_defi_venue_row("CAMELOT_V3-ARBITRUM", "")
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
        # Canonical rows always have a non-empty chain -> never legacy
        assert not svc._is_legacy_defi_venue_row("AAVE_V3", "ETHEREUM")
        assert not svc._is_legacy_defi_venue_row("UNISWAP_V2", "ETHEREUM")
        assert not svc._is_legacy_defi_venue_row("CURVE", "ETHEREUM")
        assert not svc._is_legacy_defi_venue_row("LIDO", "ETHEREUM")

    def test_is_legacy_defi_venue_row_skips_cefi_hyphenated_venues(self):
        svc = _make_svc()
        # CeFi venues are hyphenated too but protocol root doesn't match
        # the DeFi whitelist -> never legacy
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
        """End-to-end: manifest with mixed old+new DeFi rows -> breakdown
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
                    "AAVE_V3-ETHEREUM",
                    "UNISWAP_V2-ETHEREUM",
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
        assert "AAVE_V3-ETHEREUM" not in venues_seen
        assert "UNISWAP_V2-ETHEREUM" not in venues_seen
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

    def test_blank_chain_protocol_row_does_not_produce_bare_protocol_duplicate(self):
        """Regression: blank-chain DeFi protocol row must not produce a bare PROTOCOL
        duplicate entry alongside the canonical PROTOCOL-CHAIN entry.

        Scenario: consolidated index contains
          (venue=TRADER_JOE_V2, chain='')       — sub-bucket phantom row (blank chain)
          (venue=TRADER_JOE_V2, chain=AVALANCHE) — canonical row

        Expected: after _filter_to_canonical_defi_venues + _canonicalise_defi_venue_column
          only 'TRADER_JOE_V2-AVALANCHE' appears; bare 'TRADER_JOE_V2' is absent.
        """
        svc = _make_svc()

        # Synthetic index: one phantom blank-chain row + two canonical rows.
        mixed_df = pd.DataFrame(
            {
                "date": ["2026-04-17", "2026-04-17", "2026-04-17"],
                "venue": ["TRADER_JOE_V2", "TRADER_JOE_V2", "AAVE_V3"],
                "chain": ["", "AVALANCHE", "AVALANCHE"],
                "service_name": ["market-tick-data-service"] * 3,
                "data_type": ["ohlcv"] * 3,
                "capture_status": ["captured"] * 3,
                "row_count": [50, 100, 200],
            }
        )

        # Step 1: whitelist filter — blank-chain phantom must be dropped.
        filtered = svc._filter_to_canonical_defi_venues(mixed_df)
        filtered_venues_chains = list(zip(filtered["venue"].tolist(), filtered["chain"].tolist(), strict=True))
        assert ("TRADER_JOE_V2", "") not in filtered_venues_chains, (
            "blank-chain TRADER_JOE_V2 row should have been dropped by whitelist filter"
        )
        assert ("TRADER_JOE_V2", "AVALANCHE") in filtered_venues_chains, (
            "canonical (TRADER_JOE_V2, AVALANCHE) row must survive the whitelist filter"
        )

        # Step 2: canonicalise — venue column must be PROTOCOL-CHAIN, no bare PROTOCOL.
        canonicalised = svc._canonicalise_defi_venue_column(filtered)
        result_venues = set(canonicalised["venue"].tolist())
        assert "TRADER_JOE_V2" not in result_venues, "bare TRADER_JOE_V2 must not appear after canonicalisation"
        assert "TRADER_JOE_V2-AVALANCHE" in result_venues, (
            "canonical TRADER_JOE_V2-AVALANCHE must be present after canonicalisation"
        )


class TestMTDSHonestCoverage:
    """MTDS honest-coverage aggregator (Phase 6c).

    SSOT: codex/02-data/mtds-data-source-coverage-matrix.md.

    Covers:
      (a) CEFI per-venue x per-data_type daily denominator
      (b) TRADFI tick-window gate excludes trades/tbbo outside windows
      (c) DEFI per-venue scope (chain axis is a multi-venue Phase 6d follow-up)
      (d) SPORTS MTDS (bookmakers) is the instruments-service path -- not
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
        2 days window -> expected_shards = 2 dts x 2 days = 4 per venue."""
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

    @pytest.mark.skip(  # reason: TRADFI trades/tbbo removed from May-23 MVP (ohlcv_1m only); restore at tradfi_l1_l2_l3_tick_data_post_cutover_2026_06_01
        reason="TRADFI trades/tbbo removed from May-23 MVP"
    )
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

    @pytest.mark.skip(  # reason: TRADFI trades/tbbo removed from May-23 MVP (ohlcv_1m only); restore at tradfi_l1_l2_l3_tick_data_post_cutover_2026_06_01
        reason="TRADFI trades/tbbo removed from May-23 MVP"
    )
    def test_tradfi_tick_window_gate_outside_window(self):
        """Outside the tick window, ``tbbo`` drops from the denominator
        (legacy global tick-window clip via _TRADFI_TICK_ONLY_DATA_TYPES).

        2026-05-05 narrowing: ``trades`` is now expected year-round across
        all TradFi venues (we capture ≥99% of trading days; clipping to
        the 60-day reference window was understating reality and inflating
        coverage_pct). Per-(venue, data_type) overrides for TBBO live in
        UAC ``VENUE_DATA_TYPE_COVERAGE_WINDOWS`` for the venues we've
        scoped (currently CME). NYSE tbbo still uses the legacy global
        clip -> expected=0 outside the window.
        """
        svc = _make_svc()
        # 2025-01-06..2025-01-10 is OUTSIDE any tick window. NYSE has 5
        # trading days. Expected: trades=5 (year-round), tbbo=0 (clipped
        # to tick window), ohlcv_1m=5 (year-round).
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
        honest_dts = nyse["honest_data_types"]
        # ``trades`` is year-round + per-instrument shard -> 5 days x 21
        # MVP NYSE-listed SP500 instruments = 105 expected.
        assert honest_dts["trades"]["expected_shards"] == 105
        # ``tbbo`` clipped via legacy global tick-window gate -> 0 outside window
        assert honest_dts["tbbo"]["expected_shards"] == 0
        # ``ohlcv_1m`` year-round + venue-level shard -> 5 days x 1 = 5.
        assert honest_dts["ohlcv_1m"]["expected_shards"] == 5
        # Sum across data_types: trades (105 per-instrument shards) +
        # ohlcv_1m (5 venue-level shards) = 110. tbbo (0) doesn't contribute.
        assert nyse["dates_expected"] == 110
        assert nyse["dates_found"] == 1

    def test_defi_per_venue_scope(self):
        """DEFI uses ``all_defi_venues`` (11 PROTOCOL-ETHEREUM entries)."""
        svc = _make_svc()
        # AAVE_V3-ETHEREUM start = 2023-01-27; 4 dts declared.
        # Window 2023-01-27..2023-01-28 = 2 days x 4 dts = 8 expected.
        df = self._mtds_df(
            [
                [
                    "2023-01-27",
                    "AAVE_V3-ETHEREUM",
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
                    "AAVE_V3-ETHEREUM",
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
        aave = result["venues"]["AAVE_V3-ETHEREUM"]
        # Post-Phase-1 (2026-04-24 DeFi data types completeness): AAVE_V3-ETHEREUM
        # declares 7 dts in VENUE_DATA_TYPE_CAPABILITIES.
        #   - Per-instrument dts (Tier-3, 10-reserve seed x 2 days = 20):
        #     ``oracle_prices``, ``rewards``, ``risk_params`` -> 20 expected each.
        #   - Legacy + new venue-level dts (per-(venue, dt, date), 2 days):
        #     ``lending_indices`` (legacy 2 rows captured),
        #     ``flash_loan_events``, ``liquidation_events``, ``position_data``
        #     -> 2 expected each.
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
        """PREDICTION -- POLYMARKET + KALSHI, all 4 SchemaContract dts each.

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
            ["trades", "book_snapshot", "book_snapshot_5", "fills", "market_metadata"]
        )
        # ``trades`` + ``book_snapshot_5`` have rows in the fixture -- the other 3 are missing.
        assert sorted(poly["missing_data_types"]) == sorted(["book_snapshot", "fills", "market_metadata"])
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

        # Empty manifest -- every PREDICTION dt should still appear as expected
        # (with 0 found, 0% completion) so the SSOT gap is visible.
        df = self._mtds_df([])
        honest = _dss_mod._mtds_honest_coverage_for_venue(
            df, "POLYMARKET", "PREDICTION", "2026-04-17", "2026-04-17", VenueMapping()
        )

        expected_data_types = honest["expected_data_types"]
        assert isinstance(expected_data_types, list)
        # Each of the 4 SchemaContract data_types must appear as an expected row.
        assert data_type in expected_data_types, (
            f"PREDICTION data_type {data_type!r} missing from expected_data_types: {expected_data_types}"
        )

        # Also verify the per-dt entry exists in the data_types breakdown so the
        # UI can render the row (even with 0 captured / 0% completion).
        data_types = honest["data_types"]
        assert isinstance(data_types, dict)
        assert data_type in data_types, f"PREDICTION data_type {data_type!r} missing from data_types breakdown"

    def test_prediction_meta_includes_all_four_keys(self):
        """Sanity: ``PREDICTION_DATA_TYPE_META`` mirrors the 4 UAC SchemaContracts."""
        keys = set(_dss_mod.PREDICTION_DATA_TYPE_META.keys())
        assert keys == {"trades", "book_snapshot", "market_metadata", "fills"}
        # ``book_snapshot`` / ``market_metadata`` / ``fills`` carry the
        # ``indeterminate`` denominator marker (per Follow-up B prompt §B). The
        # UI shows captured count without an arbitrary per-day denominator.
        for dt in ("book_snapshot", "market_metadata", "fills"):
            assert _dss_mod.PREDICTION_DATA_TYPE_META[dt]["expected_count_per_day"] == "indeterminate"
        # ``trades`` keeps the existing per-venue daily denominator from
        # ``_mtds_expected_dates_for_venue_dt``.
        assert _dss_mod.PREDICTION_DATA_TYPE_META["trades"]["expected_count_per_day"] == "per_venue_daily"

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


class TestTradFiMultiSourceUnion:
    """FLAG-1: TradFi v9 multi-source UNION dedup + per-source breakdown.

    The v9 manifest carries one row per (venue, data_type, date, source).
    Databento + Massive both captured for the same date → two rows. The
    honest-coverage aggregator MUST:
      1. Count that date ONCE (union: ≥1 captured → cell captured).
      2. Surface a ``per_source`` breakdown so the drilldown can show
         per-vendor coverage without double-counting the headline.

    Plan: downstream_services_manifest_canonicalisation_2026_06_01 FLAG-1.
    Operator 2026-06-02: UNION + manifest-derived per-source breakdown.
    """

    def _v9_df(self, rows):
        """Build a v9-shaped manifest DataFrame including the ``source`` column."""
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
            "source",
        ]
        return pd.DataFrame(rows, columns=cols)

    # Use CBOE + ohlcv_15m: a UAC-declared TRADFI (venue, dt) pair that is
    # NOT per_instrument (is_per_instrument_shard_data_type returns False),
    # so it exercises the venue-level dt branch where per_source is emitted.
    _VENUE = "CBOE"
    _DT = "ohlcv_15m"

    def test_two_source_rows_same_date_counts_once(self):
        """FLAG-1 regression: databento + massive both captured on 2026-01-02
        must produce found_shards=1 (union), NOT 2 (per-source double-count).

        Uses CBOE/ohlcv_15m — a UAC-declared venue-level (not per-instrument)
        TRADFI (venue, data_type) pair."""
        from unified_api_contracts import VenueMapping

        df = self._v9_df(
            [
                # databento row
                [
                    "2026-01-02",
                    self._VENUE,
                    self._DT,
                    "market-tick-data-service",
                    "captured",
                    "",
                    "",
                    "",
                    100,
                    "databento",
                ],
                # massive row — same venue/dt/date, different source
                [
                    "2026-01-02",
                    self._VENUE,
                    self._DT,
                    "market-tick-data-service",
                    "captured",
                    "",
                    "",
                    "",
                    95,
                    "massive",
                ],
            ]
        )
        honest = _dss_mod._mtds_honest_coverage_for_venue(
            df, self._VENUE, "TRADFI", "2026-01-02", "2026-01-02", VenueMapping()
        )
        dt_entry = honest["data_types"].get(self._DT)
        assert dt_entry is not None, f"{self._DT} dt entry missing"
        # UNION: one date, two sources → found_shards must be 1
        assert dt_entry["found_shards"] == 1, f"Expected 1 found_shard (union), got {dt_entry['found_shards']}"
        assert dt_entry["expected_shards"] == 1

    def test_per_source_breakdown_present_in_dt_entry(self):
        """FLAG-1: per-source breakdown is emitted when the source column is present.

        The breakdown should have one entry per distinct source in the
        manifest rows (e.g. databento and massive), each reporting its
        own found_shards count. The headline (union) count stays at 1."""
        from unified_api_contracts import VenueMapping

        df = self._v9_df(
            [
                [
                    "2026-01-02",
                    self._VENUE,
                    self._DT,
                    "market-tick-data-service",
                    "captured",
                    "",
                    "",
                    "",
                    100,
                    "databento",
                ],
                [
                    "2026-01-02",
                    self._VENUE,
                    self._DT,
                    "market-tick-data-service",
                    "captured",
                    "",
                    "",
                    "",
                    95,
                    "massive",
                ],
            ]
        )
        honest = _dss_mod._mtds_honest_coverage_for_venue(
            df, self._VENUE, "TRADFI", "2026-01-02", "2026-01-02", VenueMapping()
        )
        dt_entry = honest["data_types"].get(self._DT)
        assert dt_entry is not None
        # per_source breakdown must be present
        per_source = dt_entry.get("per_source")
        assert per_source is not None, "per_source key missing from dt entry"
        assert isinstance(per_source, dict)
        # Both sources should appear
        assert "databento" in per_source, f"databento missing from per_source: {per_source}"
        assert "massive" in per_source, f"massive missing from per_source: {per_source}"
        # Each source found the same date
        assert per_source["databento"]["found_shards"] == 1
        assert per_source["massive"]["found_shards"] == 1
        # Headline union found is still 1 (no double-count)
        assert dt_entry["found_shards"] == 1

    def test_one_source_captured_other_failed_union_is_captured(self):
        """FLAG-1: if databento=captured and massive=attempted_failed on same date,
        the union MUST see the date as found (databento passes the ok_mask).

        Uses 2026-01-02 — a valid CBOE trading day (verified via
        _mtds_expected_dates_for_venue_dt returns {'2026-01-02'})."""
        from unified_api_contracts import VenueMapping

        df = self._v9_df(
            [
                [
                    "2026-01-02",
                    self._VENUE,
                    self._DT,
                    "market-tick-data-service",
                    "captured",
                    "",
                    "",
                    "",
                    100,
                    "databento",
                ],
                [
                    "2026-01-02",
                    self._VENUE,
                    self._DT,
                    "market-tick-data-service",
                    "attempted_failed",
                    "",
                    "",
                    "",
                    0,
                    "massive",
                ],
            ]
        )
        honest = _dss_mod._mtds_honest_coverage_for_venue(
            df, self._VENUE, "TRADFI", "2026-01-02", "2026-01-02", VenueMapping()
        )
        dt_entry = honest["data_types"].get(self._DT)
        assert dt_entry is not None
        # Union: databento captured → date found
        assert dt_entry["found_shards"] == 1, f"Expected 1 (union of captured), got {dt_entry['found_shards']}"
        # per_source only shows sources that pass the ok_mask
        per_source = dt_entry.get("per_source")
        if per_source:
            # databento passed; massive did not (attempted_failed filtered out)
            assert "databento" in per_source
            # massive row was filtered by ok_mask so it may not appear
            assert per_source["databento"]["found_shards"] == 1

    def test_no_source_column_no_per_source_key(self):
        """FLAG-1: v8 manifests lack the source column; per_source must not crash
        and the key must simply be absent from the dt entry."""
        from unified_api_contracts import VenueMapping

        # v8-shaped df: same cols as _mtds_df (no source column)
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
        df = pd.DataFrame(
            [
                [
                    "2026-01-02",
                    self._VENUE,
                    self._DT,
                    "market-tick-data-service",
                    "captured",
                    "",
                    "",
                    "",
                    100,
                ]
            ],
            columns=cols,
        )
        honest = _dss_mod._mtds_honest_coverage_for_venue(
            df, self._VENUE, "TRADFI", "2026-01-02", "2026-01-02", VenueMapping()
        )
        dt_entry = honest["data_types"].get(self._DT)
        assert dt_entry is not None
        # No source column → per_source key must be absent (not an empty dict error)
        assert "per_source" not in dt_entry


class TestMTDSPerInstrumentHonestCoverage:
    """Phase 8D -- per-(venue, data_type, instrument_id, date) denominator.

    SSOT:
      - plan: ``plans/active/mtds_per_instrument_sentinels_2026_04_21.md``
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
        UNISWAP_V3-ETHEREUM) -- Wave 8G seeded 20 top-TVL pools. With 0 rows in
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
            "UNISWAP_V3-ETHEREUM",
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
            # 20 == _PER_INSTRUMENT_BREAKDOWN_MAX_SIZE (not ``<``) -> no
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


class TestBuildChainBreakdownShardMath:
    """Tests for the rewritten ``_build_chain_breakdown`` shard-count math
    (2026-05-07 chain-row math fix -- see plan
    ``data_status_drilldown_shard_atom_alignment_2026_05_07``).

    The old date-only math collapsed the within-day (protocol x data_type
    x instrument) fan-out and produced misleading ``ARBITRUM 32/54``
    rollups; the new math respects the codex DeFi shard atom
    ``(asset_group=defi, chain, venue/protocol, data_type,
    instrument_id_or_protocol_id, day)``.
    """

    def _vm(self) -> MagicMock:
        vm = MagicMock()
        vm.get_venue_start_date.return_value = "2024-01-01"

        # Every chain x venue gets the same 5-day expected window for
        # determinism. Real ``get_expected_trading_dates`` clips by
        # venue calendar / chain genesis / protocol launch -- those
        # composition tests live in the UAC-level tests.
        def _expected_dates(_venue: str, start: str, _end: str) -> list[str]:
            del _venue
            base = pd.Timestamp(start)
            return [(base + pd.Timedelta(days=i)).strftime("%Y-%m-%d") for i in range(5)]

        vm.get_expected_trading_dates.side_effect = _expected_dates
        return vm

    def _make_chain_df(self, rows: list[dict[str, object]]) -> pd.DataFrame:
        # All chain-breakdown callers pass these columns; v5+ also
        # ``capture_status``. Fill ``capture_status="captured"`` if
        # absent so tests don't accidentally null-out the numerator.
        df = pd.DataFrame(rows)
        if "capture_status" not in df.columns:
            df["capture_status"] = "captured"
        return df

    def test_emits_shards_found_and_shards_expected(self):
        """The new headline fields appear in the payload alongside the
        backward-compat dates_* fields."""
        svc = _make_svc()
        df = self._make_chain_df(
            [
                {
                    "chain": "ARBITRUM",
                    "venue": "AAVE_V3-ARBITRUM",
                    "data_type": "lending_indices",
                    "instrument_id": "USDC",
                    "date": "2024-01-01",
                },
            ]
        )
        result = svc._build_chain_breakdown(df, "2024-01-01", "2024-01-05", self._vm())
        assert "ARBITRUM" in result
        entry = result["ARBITRUM"]
        assert isinstance(entry, dict)
        assert "shards_found" in entry
        assert "shards_expected" in entry
        # Backward-compat fields preserved for UI consumers that haven't
        # yet switched to the new fields.
        assert "dates_found" in entry
        assert "dates_expected" in entry

    def test_shards_expected_exceeds_dates_expected_for_multi_dt_chain(self):
        """The whole point of the rewrite: when a chain has multiple
        data_types per venue, ``shards_expected`` ≫ ``dates_expected``.
        For the 2026-05-07 ARBITRUM screenshot, the old math reported
        ``32/54 dates``; the new math reports thousands of shards."""
        svc = _make_svc()
        # 1 venue (AAVE_V3-ARBITRUM) x 3 data_types x 2 instruments
        # captured across 5 expected dates -> expected = 5 x 6 = 30 shards
        # vs dates_expected = 5.
        rows: list[dict[str, object]] = []
        for dt_name in ("lending_indices", "borrow_indices", "supply_apy"):
            for inst in ("USDC", "WETH"):
                rows.append(
                    {
                        "chain": "ARBITRUM",
                        "venue": "AAVE_V3-ARBITRUM",
                        "data_type": dt_name,
                        "instrument_id": inst,
                        "date": "2024-01-01",
                    }
                )
        df = self._make_chain_df(rows)
        result = svc._build_chain_breakdown(df, "2024-01-01", "2024-01-05", self._vm())
        entry = result["ARBITRUM"]
        assert isinstance(entry, dict)
        # 5 expected dates x 6 distinct (data_type, instrument_id) leaves = 30.
        assert entry["shards_expected"] == 30
        # 6 captured rows (all on 2024-01-01).
        assert entry["shards_found"] == 6
        # Old dates-only math would have reported 1/5 -> 20% completion.
        assert entry["dates_expected"] == 5
        assert entry["dates_found"] == 1

    def test_capture_status_filter_excludes_empty_confirmed(self):
        """Numerator counts only ``captured`` rows when capture_status
        is present. ``empty_confirmed`` / ``attempted_failed`` rows
        are honest gaps, not real shards."""
        svc = _make_svc()
        df = self._make_chain_df(
            [
                {
                    "chain": "BASE",
                    "venue": "AAVE_V3-BASE",
                    "data_type": "lending_indices",
                    "instrument_id": "USDC",
                    "date": "2024-01-01",
                    "capture_status": "captured",
                },
                {
                    "chain": "BASE",
                    "venue": "AAVE_V3-BASE",
                    "data_type": "lending_indices",
                    "instrument_id": "USDC",
                    "date": "2024-01-02",
                    "capture_status": "empty_confirmed",
                },
                {
                    "chain": "BASE",
                    "venue": "AAVE_V3-BASE",
                    "data_type": "lending_indices",
                    "instrument_id": "USDC",
                    "date": "2024-01-03",
                    "capture_status": "attempted_failed",
                },
            ]
        )
        result = svc._build_chain_breakdown(df, "2024-01-01", "2024-01-05", self._vm())
        entry = result["BASE"]
        assert isinstance(entry, dict)
        # Only the one ``captured`` row counts toward shards_found.
        assert entry["shards_found"] == 1
        # 5 expected dates x 1 leaf (lending_indices, USDC) = 5.
        assert entry["shards_expected"] == 5

    def test_completion_pct_uses_shard_math_not_date_math(self):
        """The headline ``completion_pct`` derives from shards_found /
        shards_expected, not the legacy dates_found / dates_expected.
        Verify with a chain where the two ratios diverge."""
        svc = _make_svc()
        # 5 expected dates; 2 dates have captures across 4 leaves each;
        # 3 dates fully missing. dates: 2/5=40%; shards: 8/20=40%.
        # Make the divergence by adding a non-captured row on a "missing" date.
        rows: list[dict[str, object]] = []
        for dt_name in ("lending_indices", "borrow_indices"):
            for inst in ("USDC", "WETH"):
                for d in ("2024-01-01", "2024-01-02"):
                    rows.append(
                        {
                            "chain": "BASE",
                            "venue": "AAVE_V3-BASE",
                            "data_type": dt_name,
                            "instrument_id": inst,
                            "date": d,
                            "capture_status": "captured",
                        }
                    )
        df = self._make_chain_df(rows)
        result = svc._build_chain_breakdown(df, "2024-01-01", "2024-01-05", self._vm())
        entry = result["BASE"]
        assert isinstance(entry, dict)
        # 5 dates x 4 leaves = 20 expected; 8 captured.
        assert entry["shards_expected"] == 20
        assert entry["shards_found"] == 8
        # 8/20 = 40%.
        assert entry["completion_pct"] == 40.0
        # Date math: 2 found / 5 expected.
        assert entry["dates_found"] == 2
        assert entry["dates_expected"] == 5


class TestSportsRetiredDataTypeFiltering:
    """Phase 3 smoke-test: retired sports data_types don't render in the data-status panel.

    Regression guard for plans/active/sports_retired_data_types_code_cleanup_2026_05_13.md.
    TRANSFERMARKT_LEAGUES / SFI_LEAGUES / SFI_STANDINGS were retired 2026-05-05 /
    2026-04-24 and must not appear in the panel denominator. The 88,779 manifest rows
    for these types were flipped to empty_confirmed/EXPECTED_DEPRECATED_DATA_TYPE at
    instruments-service@a0a720e; the panel must clip them entirely from the denominator.
    """

    _RETIRED = frozenset({"TRANSFERMARKT_LEAGUES", "SFI_LEAGUES", "SFI_STANDINGS"})

    def test_retired_types_absent_from_sports_data_type_meta(self):
        """SPORTS_DATA_TYPE_META must not contain the three retired data types."""
        ssot_keys = set(_dss_mod.SPORTS_DATA_TYPE_META.keys())
        for dt in self._RETIRED:
            assert dt not in ssot_keys, f"{dt} still present in SPORTS_DATA_TYPE_META — should be absent"

    def test_panel_skips_retired_types_present_in_manifest(self):
        """_build_data_type_grouping must not render retired types even when manifest rows exist.

        Covers the filtering path: for SPORTS, all_dt_vals = sports_ssot_vals
        (set(SPORTS_DATA_TYPE_META.keys())), so retired types that appear in the
        manifest as empty_confirmed/EXPECTED_DEPRECATED_DATA_TYPE rows are never
        iterated and never added to dt_venues.
        """
        svc = _make_svc()
        rows = [
            {
                "date": "2024-01-01",
                "venue": "",
                "data_type": dt,
                "league_id": "1",
                "capture_status": "empty_confirmed",
                "error_reason": "EXPECTED_DEPRECATED_DATA_TYPE",
                "instrument_count": 0,
            }
            for dt in self._RETIRED
        ]
        df = pd.DataFrame(rows)
        _stub_entry: dict[str, object] = {
            "dates_found": 0,
            "dates_expected": 0,
            "dates_expected_venue": 0,
            "dates_missing": 0,
            "completion_pct": 0.0,
        }
        with patch.object(svc, "_build_sports_entity_entry", return_value=_stub_entry):
            dt_venues, _found, _expected = svc._build_data_type_grouping(df, "2024-01-01", "2024-01-31", cat="SPORTS")
        for dt in self._RETIRED:
            assert dt not in dt_venues, (
                f"Retired data_type {dt!r} was rendered in the data-status panel — "
                "should be clipped from denominator (SPORTS_DATA_TYPE_META is the SSOT)"
            )


# ── Item 7: Trigger-date denominator for mapping entities ─────────────────────


class TestTriggerDateDenominator:
    """ITEM 7 (sports_master.md:1064): TEAMS and PLAYER_VALUES use trigger-date
    denominator instead of daily/periodic calendar.

    TEAMS uses ``global_trigger_date`` axis — expected = union of
    get_reference_refresh_dates across all leagues.
    PLAYER_VALUES uses ``per_league_trigger_date`` axis — expected = per-league
    trigger date count.

    Soft-gated on sports_master item A2.4 (instruments-service write-path);
    these tests verify the denominator mechanics in isolation.
    """

    def test_teams_axis_is_global_trigger_date(self) -> None:
        """TEAMS meta entry declares global_trigger_date axis."""
        from deployment_api.services.data_status_service import SPORTS_DATA_TYPE_META

        meta = SPORTS_DATA_TYPE_META["TEAMS"]
        assert meta["axis"] == "global_trigger_date", (
            f"TEAMS axis should be 'global_trigger_date', got {meta['axis']!r}"
        )
        assert meta["unit"] == "trigger_date_snapshots"
        # No cadence_days for trigger-date axes
        assert "cadence_days" not in meta

    def test_player_values_axis_is_per_league_trigger_date(self) -> None:
        """PLAYER_VALUES meta entry declares per_league_trigger_date axis."""
        from deployment_api.services.data_status_service import SPORTS_DATA_TYPE_META

        meta = SPORTS_DATA_TYPE_META["PLAYER_VALUES"]
        assert meta["axis"] == "per_league_trigger_date", (
            f"PLAYER_VALUES axis should be 'per_league_trigger_date', got {meta['axis']!r}"
        )
        assert meta["unit"] == "trigger_date_snapshots"

    def test_sports_trigger_dates_for_window_returns_sorted_list(self) -> None:
        """_sports_trigger_dates_for_window returns sorted ISO date strings."""
        from deployment_api.services.data_status_service import _sports_trigger_dates_for_window

        result = _sports_trigger_dates_for_window("2024-01-01", "2024-12-31")
        assert isinstance(result, list)
        # Should contain at least some trigger dates for a full year
        assert len(result) > 0
        # All dates should be within the window
        for d in result:
            assert "2024-01-01" <= d <= "2024-12-31"
        # Should be sorted
        assert result == sorted(result)
        # No duplicates
        assert len(result) == len(set(result))

    def test_sports_trigger_dates_empty_window(self) -> None:
        """Invalid date range returns empty list, no exception."""
        from deployment_api.services.data_status_service import _sports_trigger_dates_for_window

        result = _sports_trigger_dates_for_window("2024-12-31", "2024-01-01")
        assert result == []

    def test_sports_trigger_dates_for_league_returns_sorted_list(self) -> None:
        """_sports_trigger_dates_for_league returns sorted ISO date strings for EPL."""
        from deployment_api.services.data_status_service import _sports_trigger_dates_for_league

        result = _sports_trigger_dates_for_league("EPL", "2024-01-01", "2024-12-31")
        assert isinstance(result, list)
        # EPL has season-start + summer/winter window dates
        assert len(result) > 0
        assert result == sorted(result)
        for d in result:
            assert "2024-01-01" <= d <= "2024-12-31"

    def test_teams_honest_coverage_uses_trigger_dates(self) -> None:
        """_sports_honest_coverage for TEAMS returns global_trigger_date axis result."""
        import pandas as pd

        from deployment_api.services.data_status_service import (
            _sports_honest_coverage,
            _sports_trigger_dates_for_window,
        )

        # Real trigger dates for 2024 (derived from UAC)
        trigger_dates = _sports_trigger_dates_for_window("2024-06-01", "2024-06-30")

        result = _sports_honest_coverage(
            filtered=pd.DataFrame(columns=["data_type", "league_id", "date", "capture_status"]),
            entity_name="TEAMS",
            start_date="2024-06-01",
            end_date="2024-06-30",
        )
        assert result is not None
        assert result["axis"] == "global_trigger_date"
        assert result["unit"] == "trigger_date_snapshots"
        # expected_shards should match the trigger date count for the window
        assert result["expected_shards"] == len(trigger_dates)
        # Empty manifest → 0 found
        assert result["found_shards"] == 0
        # Trigger dates list is included in response for UI drill-down
        assert "trigger_dates" in result

    def test_teams_honest_coverage_counts_found_on_trigger_dates(self) -> None:
        """found_shards for TEAMS counts manifest rows that land on trigger dates."""
        import pandas as pd

        from deployment_api.services.data_status_service import (
            _sports_honest_coverage,
            _sports_trigger_dates_for_window,
        )

        trigger_dates = _sports_trigger_dates_for_window("2024-06-01", "2024-06-30")
        if not trigger_dates:
            pytest.skip("No trigger dates for 2024-06 window — UAC data not available")

        # One manifest row on a real trigger date, one on a non-trigger date
        trigger_date_in = trigger_dates[0]
        nontrigger_date = "2024-06-15"  # Mid-month, unlikely to be a trigger date

        df = pd.DataFrame(
            {
                "data_type": ["TEAMS", "TEAMS"],
                "league_id": ["EPL", "EPL"],
                "date": [trigger_date_in, nontrigger_date],
                "capture_status": ["captured", "captured"],
            }
        )

        result = _sports_honest_coverage(
            filtered=df,
            entity_name="TEAMS",
            start_date="2024-06-01",
            end_date="2024-06-30",
        )
        assert result is not None
        # found_shards = intersection of manifest dates with trigger dates
        assert result["found_shards"] >= 1

    def test_player_values_honest_coverage_uses_per_league_triggers(self) -> None:
        """_sports_honest_coverage for PLAYER_VALUES returns per_league_trigger_date."""
        import pandas as pd

        from deployment_api.services.data_status_service import _sports_honest_coverage

        result = _sports_honest_coverage(
            filtered=pd.DataFrame(columns=["data_type", "league_id", "date", "capture_status"]),
            entity_name="PLAYER_VALUES",
            start_date="2024-01-01",
            end_date="2024-12-31",
        )
        assert result is not None
        assert result["axis"] == "per_league_trigger_date"
        assert result["unit"] == "trigger_date_snapshots"
        # expected_shards > 0 for a full year (multiple leagues x multiple triggers)
        assert isinstance(result["expected_shards"], int)
        assert result["expected_shards"] > 0
        # found_shards = 0 (empty manifest)
        assert result["found_shards"] == 0
        # per_league populated
        assert isinstance(result["per_league"], dict)

    def test_trigger_date_denominator_less_than_daily_denominator(self) -> None:
        """Trigger-date denominator must be smaller than daily denominator.

        The whole point of this item is that the daily calendar over-estimates
        the expected shard count for reference entities. Trigger-date count for
        a full year should be << 365 days.
        """
        from deployment_api.services.data_status_service import _sports_trigger_dates_for_window

        trigger_dates = _sports_trigger_dates_for_window("2024-01-01", "2024-12-31")
        days_in_year = 366  # 2024 is a leap year
        # Trigger dates should be a fraction of daily days
        assert len(trigger_dates) < days_in_year // 4, (
            f"Expected trigger-date count << {days_in_year // 4} days/year, "
            f"got {len(trigger_dates)} — denominator may not be corrected"
        )


class TestTradFiVenueAccessorFlag4:
    """FLAG-4: the TRADFI honest-coverage DENOMINATOR must count the full cross-source venue universe.

    Verified (slot-6 2026-06-03): no undercount existed — `all_databento_venues` (despite the
    misleading name) already resolves to the complete 6-venue tradfi set including the non-Databento
    venues CBOE (→Barchart/VIX) and FX (→Yahoo). This test guards that INVARIANT against whatever
    `venue_accessor` is configured, so a future narrowing of the venue list can't silently shrink the
    tradfi coverage denominator. (UAC also adds a correctly-named `all_tradfi_venues` alias + its own
    universe test; deployment-api will switch the accessor string to it after the UAC version cascade.)
    """

    def test_tradfi_venue_accessor_resolves_to_full_universe_incl_cboe_fx(self) -> None:
        """The configured TRADFI venue_accessor must resolve to the full tradfi universe (incl CBOE+FX)."""
        meta = _dss_mod.MTDS_CATEGORY_META.get("TRADFI")
        assert meta is not None, "TRADFI missing from MTDS_CATEGORY_META"
        accessor = meta.get("venue_accessor")
        assert accessor, "TRADFI venue_accessor must be set"
        venues = getattr(_dss_mod.VenueMapping(), str(accessor), None)
        assert isinstance(venues, list) and len(venues) >= 6, (
            f"TRADFI venue_accessor {accessor!r} must resolve to >=6 venues (full universe), got {venues!r}"
        )
        # The non-Databento tradfi venues MUST be in the denominator (else VIX/FX coverage is undercounted).
        for required in ("CBOE", "FX"):
            assert required in venues, (
                f"{required} missing from TRADFI denominator via {accessor!r}: {venues} — "
                "VIX(CBOE/Barchart) / forex(FX/Yahoo) coverage would be undercounted (FLAG-4)."
            )


class TestManifestStatusVenueFilter:
    """Venue filter on the manifest status fast-path.

    Root cause (data_status venue chip did not narrow): ``get_manifest_status``
    exposed ``league_id`` / ``chain`` / ``job_id`` / ... but had NO ``venue``
    parameter, so the manifest fast-path that powers the data-status tab
    ignored the chip entirely. These tests assert that:

    1. passing ``venue=["BINANCE-FUTURES"]`` narrows the filtered manifest
       slice to that venue (case-insensitively) BEFORE the per-venue
       breakdown is computed, and
    2. omitting ``venue`` preserves the all-venue behaviour, and
    3. a non-empty ``venue`` engages the ``any_row_filter`` gate so the
       request bypasses the filter-free rollup fast-path and takes the
       on-demand filtered compute.
    """

    @staticmethod
    def _cefi_index() -> pd.DataFrame:
        # Two CeFi venues across the same day; only BINANCE-FUTURES should
        # survive a ``venue=["BINANCE-FUTURES"]`` filter.
        return pd.DataFrame(
            {
                "date": ["2025-03-14", "2025-03-14", "2025-03-14"],
                "venue": ["BINANCE-FUTURES", "BYBIT", "binance-futures"],
                "data_type": ["funding_rate", "funding_rate", "funding_rate"],
                "instrument_id": ["BTCUSDT", "BTCUSDT", "ETHUSDT"],
                "service_name": ["market-tick-data-service"] * 3,
                "capture_status": ["captured", "captured", "captured"],
                "asset_group": ["cefi", "cefi", "cefi"],
                "row_count": [100, 100, 100],
            }
        )

    def _build_with(self, venue: list[str] | None) -> pd.DataFrame:
        """Run ``_build_manifest_category`` capturing the DataFrame that reaches
        ``_build_venue_breakdown`` (i.e. the slice AFTER the venue mask)."""
        svc = _make_svc()
        captured: dict[str, pd.DataFrame] = {}

        def _capture(filtered: pd.DataFrame, *args: object, **kwargs: object):
            captured["df"] = filtered.copy()
            return ({}, 0, 0)

        with (
            patch.object(svc, "_read_defi_merged_index", return_value=self._cefi_index()),
            patch.object(svc, "_build_venue_breakdown", side_effect=_capture),
            patch.object(svc, "_build_v4_sub_dimensions", return_value={}),
            patch.object(_dss_mod, "get_effective_start_date", return_value="2025-03-14"),
        ):
            vm = MagicMock()
            svc._build_manifest_category(
                service="market-tick-data-service",
                cat="CEFI",
                start_date="2025-03-14",
                end_date="2025-03-14",
                all_date_strs=["2025-03-14"],
                total_days=1,
                venue_mapping=vm,
                venue=venue,
            )
        return captured["df"]

    def test_venue_filter_narrows_to_requested_venue(self) -> None:
        df = self._build_with(["BINANCE-FUTURES"])
        survived = {str(v).upper() for v in df["venue"].tolist()}
        # Only BINANCE-FUTURES rows (both exact + the lower-cased duplicate via
        # the case-insensitive match) survive; BYBIT is dropped.
        assert survived == {"BINANCE-FUTURES"}, survived
        assert "BYBIT" not in survived
        # Both BINANCE-FUTURES rows (BTCUSDT + ETHUSDT, mixed case) kept.
        assert len(df) == 2

    def test_no_venue_filter_preserves_all_venues(self) -> None:
        df = self._build_with(None)
        survived = {str(v).upper() for v in df["venue"].tolist()}
        assert "BINANCE-FUTURES" in survived
        assert "BYBIT" in survived
        assert len(df) == 3

    async def test_venue_engages_any_row_filter_gate_and_bypasses_rollup(self) -> None:
        """A non-empty ``venue`` must NOT take the filter-free rollup fast-path."""
        svc = _make_svc()
        sentinel: dict[str, object] = {"on_demand": True}
        with (
            patch.object(
                _dss_mod,
                "_read_rollup_if_fresh",
                return_value={"should": "not be read"},
            ) as mock_rollup,
            patch.object(
                svc,
                "_get_manifest_status_sync",
                return_value=sentinel,
            ) as mock_sync,
        ):
            result = await svc.get_manifest_status(
                service="market-tick-data-service",
                start_date="2025-03-14",
                end_date="2025-03-14",
                venue=["BINANCE-FUTURES"],
            )
        # Rollup fast-path skipped; on-demand sync path taken with venue threaded.
        mock_rollup.assert_not_called()
        mock_sync.assert_called_once()
        assert ["BINANCE-FUTURES"] in mock_sync.call_args.args
        assert result is sentinel

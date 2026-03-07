"""
Extended unit tests for services/data_status_service module.

Tests run_data_status_cli, calculate_missing_shards.
"""

import json
import sys
from pathlib import Path
from types import ModuleType
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

# Remove pre-mocked entries so we import the real DataStatusService.
for _key in list(sys.modules.keys()):
    if _key in (
        "deployment_api.services.data_status_service",
        "deployment_api.services",
    ):
        del sys.modules[_key]

# Re-build minimal package pointing to the real services directory.
_svc_pkg = ModuleType("deployment_api.services")
_svc_pkg.__package__ = "deployment_api.services"
_services_dir = str(Path(__file__).parent.parent.parent / "deployment_api" / "services")
_svc_pkg.__path__ = [_services_dir]  # type: ignore[attr-defined]
sys.modules["deployment_api.services"] = _svc_pkg

from deployment_api.services.data_status_service import DataStatusService


def _make_svc(**kwargs) -> DataStatusService:
    return DataStatusService(project_id=kwargs.get("project_id", "test-project"))


def _mock_process(returncode: int = 0, stdout: bytes = b"{}", stderr: bytes = b"") -> MagicMock:
    """Build a mock asyncio Process object."""
    proc = MagicMock()
    proc.returncode = returncode
    proc.communicate = AsyncMock(return_value=(stdout, stderr))
    return proc


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
    """Tests for DataStatusService.calculate_missing_shards."""

    @pytest.mark.asyncio
    async def test_returns_error_when_cli_fails(self):
        svc = _make_svc()
        with patch.object(
            svc, "run_data_status_cli", new=AsyncMock(return_value={"error": "cli failed"})
        ):
            result = await svc.calculate_missing_shards("svc-a", "2024-01-01", "2024-01-31")
        assert "error" in result

    @pytest.mark.asyncio
    async def test_returns_missing_analysis_structure(self):
        svc = _make_svc()
        cli_result = {
            "dates": [
                {
                    "date": "2024-01-01",
                    "venues": [
                        {"venue": "BINANCE", "status": "missing", "category": "CEFI"},
                        {"venue": "OKX", "status": "present", "category": "CEFI"},
                    ],
                },
                {
                    "date": "2024-01-02",
                    "venues": [
                        {"venue": "BINANCE", "status": "present", "category": "CEFI"},
                    ],
                },
            ]
        }

        with patch.object(svc, "run_data_status_cli", new=AsyncMock(return_value=cli_result)):
            result = await svc.calculate_missing_shards("svc-a", "2024-01-01", "2024-01-02")

        assert result["service"] == "svc-a"
        assert "total_missing" in result
        assert result["total_missing"] == 1
        assert "BINANCE" in result["missing_by_venue"]
        assert "summary" in result

    @pytest.mark.asyncio
    async def test_handles_empty_dates(self):
        svc = _make_svc()
        cli_result = {"dates": []}

        with patch.object(svc, "run_data_status_cli", new=AsyncMock(return_value=cli_result)):
            result = await svc.calculate_missing_shards("svc-a", "2024-01-01", "2024-01-31")

        assert result["total_missing"] == 0

    @pytest.mark.asyncio
    async def test_handles_no_dates_key(self):
        svc = _make_svc()
        cli_result = {"categories": {}}

        with patch.object(svc, "run_data_status_cli", new=AsyncMock(return_value=cli_result)):
            result = await svc.calculate_missing_shards("svc-a", "2024-01-01", "2024-01-31")

        assert "total_missing" in result
        assert result["total_missing"] == 0

    @pytest.mark.asyncio
    async def test_counts_by_category(self):
        svc = _make_svc()
        cli_result = {
            "dates": [
                {
                    "date": "2024-01-01",
                    "venues": [
                        {"venue": "BINANCE", "status": "missing", "category": "CEFI"},
                        {"venue": "UNI", "status": "missing", "category": "DEFI"},
                    ],
                }
            ]
        }

        with patch.object(svc, "run_data_status_cli", new=AsyncMock(return_value=cli_result)):
            result = await svc.calculate_missing_shards("svc-a", "2024-01-01", "2024-01-01")

        assert result["missing_by_category"]["CEFI"] == 1
        assert result["missing_by_category"]["DEFI"] == 1

    @pytest.mark.asyncio
    async def test_skips_non_dict_venue_entries(self):
        svc = _make_svc()
        cli_result = {
            "dates": [
                {
                    "date": "2024-01-01",
                    "venues": [
                        "not-a-dict",
                        {"venue": "BINANCE", "status": "missing", "category": "CEFI"},
                    ],
                }
            ]
        }

        with patch.object(svc, "run_data_status_cli", new=AsyncMock(return_value=cli_result)):
            result = await svc.calculate_missing_shards("svc-a", "2024-01-01", "2024-01-01")

        assert result["total_missing"] == 1

    @pytest.mark.asyncio
    async def test_passes_mode_to_cli(self):
        svc = _make_svc()
        cli_mock = AsyncMock(return_value={"dates": []})

        with patch.object(svc, "run_data_status_cli", new=cli_mock):
            await svc.calculate_missing_shards("svc-a", "2024-01-01", "2024-01-31", mode="live")

        cli_mock.assert_called_once()
        _, kwargs = cli_mock.call_args
        assert kwargs.get("mode") == "live" or "live" in str(cli_mock.call_args)


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

        with patch(
            "deployment_api.services.data_status_service.list_objects",
            return_value=[mock_obj, mock_obj],
        ):
            result = await svc.get_last_updated_info("market-tick-data-handler")

        assert "service" in result
        assert result["service"] == "market-tick-data-handler"
        assert "categories" in result

    @pytest.mark.asyncio
    async def test_returns_empty_category_when_no_objects(self):
        svc = _make_svc()

        with patch(
            "deployment_api.services.data_status_service.list_objects",
            return_value=[],
        ):
            result = await svc.get_last_updated_info(
                "market-tick-data-handler", categories=["cefi"]
            )

        assert result["categories"]["cefi"]["status"] == "empty"

    @pytest.mark.asyncio
    async def test_handles_list_objects_error(self):
        svc = _make_svc()

        with patch(
            "deployment_api.services.data_status_service.list_objects",
            side_effect=OSError("bucket not found"),
        ):
            result = await svc.get_last_updated_info("instruments-service", categories=["cefi"])

        assert result["categories"]["cefi"]["status"] == "error"

    @pytest.mark.asyncio
    async def test_uses_default_categories_when_none_given(self):
        svc = _make_svc()

        with patch(
            "deployment_api.services.data_status_service.list_objects",
            return_value=[],
        ):
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

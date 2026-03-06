"""
Unit tests for data_status_helpers module.

Tests cover _bucket helper and verify the CLI wrapper structure.
"""

from unittest.mock import AsyncMock, patch

import pytest


class TestBucketHelper:
    """Tests for _bucket function."""

    def test_bucket_name_format(self):
        from deployment_api.routes import data_status_helpers
        with patch("deployment_api.routes.data_status_helpers._PID", "my-project-123"), patch.object(data_status_helpers, "_PID", "my-project-123"):
            bucket_name = data_status_helpers._bucket("instruments-store", "CEFI")
            assert bucket_name == "instruments-store-cefi-my-project-123"

    def test_bucket_lowercases_category(self):
        with patch("deployment_api.routes.data_status_helpers._PID", "proj"):
            from deployment_api.routes import data_status_helpers
            with patch.object(data_status_helpers, "_PID", "proj"):
                bucket_name = data_status_helpers._bucket("market-data", "TRADFI")
                assert "tradfi" in bucket_name
                assert "TRADFI" not in bucket_name

    def test_bucket_includes_prefix_and_project(self):
        from deployment_api.routes import data_status_helpers
        with patch.object(data_status_helpers, "_PID", "test-project"):
            bucket_name = data_status_helpers._bucket("my-prefix", "DEFI")
            assert bucket_name.startswith("my-prefix-defi-")
            assert bucket_name.endswith("test-project")


class TestRunDataStatusCli:
    """Tests for _run_data_status_cli (via mocked subprocess)."""

    @pytest.mark.asyncio
    async def test_successful_cli_returns_dict(self):
        import json

        from deployment_api.routes.data_status_helpers import _run_data_status_cli

        fake_output = json.dumps({"completion_pct": 95.0, "service": "instruments-service"}).encode()
        mock_proc = AsyncMock()
        mock_proc.returncode = 0
        mock_proc.communicate.return_value = (fake_output, b"")

        with patch("asyncio.create_subprocess_exec", return_value=mock_proc), patch("asyncio.wait_for", return_value=(fake_output, b"")):
            result = await _run_data_status_cli(
                service="instruments-service",
                start_date="2024-01-01",
                end_date="2024-01-31",
            )
        assert result["completion_pct"] == 95.0

    @pytest.mark.asyncio
    async def test_failed_cli_raises_http_exception(self):
        from fastapi import HTTPException

        from deployment_api.routes.data_status_helpers import _run_data_status_cli

        mock_proc = AsyncMock()
        mock_proc.returncode = 1
        mock_proc.communicate.return_value = (b"", b"Error: something went wrong")

        with (
            patch("asyncio.create_subprocess_exec", return_value=mock_proc),
            patch("asyncio.wait_for", return_value=(b"", b"Error: something went wrong")),
            pytest.raises(HTTPException) as exc_info,
        ):
            await _run_data_status_cli(
                service="instruments-service",
                start_date="2024-01-01",
                end_date="2024-01-31",
            )
        assert exc_info.value.status_code == 500

    @pytest.mark.asyncio
    async def test_invalid_json_returns_raw(self):
        from deployment_api.routes.data_status_helpers import _run_data_status_cli

        fake_output = b"not valid json output"
        mock_proc = AsyncMock()
        mock_proc.returncode = 0
        mock_proc.communicate.return_value = (fake_output, b"")

        with patch("asyncio.create_subprocess_exec", return_value=mock_proc), patch("asyncio.wait_for", return_value=(fake_output, b"")):
            result = await _run_data_status_cli(
                service="instruments-service",
                start_date="2024-01-01",
                end_date="2024-01-31",
            )
        assert "error" in result
        assert "raw_output" in result

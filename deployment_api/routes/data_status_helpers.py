"""
Helper functions for data status routes.

Contains utility functions for data status checking and processing.
"""

import asyncio
import json
import logging
import os
import sys
from typing import cast

from fastapi import HTTPException

from deployment_api.settings import GCP_PROJECT_ID as _PID

logger = logging.getLogger(__name__)


# Helper: build bucket name from prefix, category, and project ID
def _bucket(prefix: str, category: str) -> str:
    """Build a GCS bucket name: {prefix}-{category_lower}-{project_id}."""
    return f"{prefix}-{category.lower()}-{_PID}"


async def _run_data_status_cli(
    service: str,
    start_date: str,
    end_date: str,
    categories: list[str] | None = None,
    venues: list[str] | None = None,
    show_missing: bool = False,
    check_venues: bool = False,
    check_data_types: bool = False,
    check_feature_groups: bool = False,
    check_timeframes: bool = False,
    mode: str = "batch",
) -> dict:
    """
    Run data-status CLI command and parse JSON output.

    This wraps the CLI for API access with proper error handling.
    """
    try:
        # Build CLI command
        cmd = [
            sys.executable,
            "-m",
            "deployment_service.cli",
            "data-status",
            "--service",
            service,
            "--start-date",
            start_date,
            "--end-date",
            end_date,
            "--output",
            "json",
            "--mode",
            mode,
        ]

        # Add optional parameters
        if categories:
            for cat in categories:
                cmd.extend(["--category", cat])

        if venues:
            for venue in venues:
                cmd.extend(["--venue", venue])

        if show_missing:
            cmd.append("--show-missing")

        if check_venues:
            cmd.append("--check-venues")

        if check_data_types:
            cmd.append("--check-data-types")

        if check_feature_groups:
            cmd.append("--check-feature-groups")

        if check_timeframes:
            cmd.append("--check-timeframes")

        # Run command with timeout
        proc = await asyncio.create_subprocess_exec(
            *cmd,
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.PIPE,
            cwd=os.getcwd(),
        )

        try:
            stdout, stderr = await asyncio.wait_for(proc.communicate(), timeout=300)  # 5 min timeout
        except TimeoutError:
            proc.kill()
            await proc.communicate()
            raise HTTPException(status_code=500, detail="Data status check timed out (>5 minutes)") from None

        if proc.returncode != 0:
            error_msg = stderr.decode() if stderr else "Unknown error"
            logger.error("data-status CLI failed: %s", error_msg)
            raise HTTPException(status_code=500, detail=f"Data status check failed: {error_msg}")

        # Parse JSON output
        try:
            result = cast(dict[str, object], json.loads(stdout.decode()))
            return result
        except json.JSONDecodeError as e:
            logger.error("Failed to parse CLI JSON output: %s", e)
            # Return raw output for debugging
            return {
                "error": "Failed to parse JSON output",
                "raw_output": stdout.decode(),
                "stderr": stderr.decode() if stderr else None,
            }

    except (OSError, ValueError, RuntimeError) as e:
        logger.error("Error running data-status CLI: %s", e)
        raise HTTPException(status_code=500, detail=f"Internal error: {e!s}") from e

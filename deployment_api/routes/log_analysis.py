"""
Log analysis utilities for deployment routes.

Contains functions for analyzing deployment logs for errors, warnings, and status details.
"""

import asyncio
import logging
import re
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from typing import cast

logger = logging.getLogger(__name__)

# Cache for log analysis results
_log_analysis_cache: dict[str, dict[str, object]] = {}
_log_analysis_cache_ttl = 60  # Cache log analysis for 60 seconds


def analyze_deployment_logs_sync(
    state_manager: object, deployment_id: str, state: object
) -> dict[str, object]:
    """
    Analyze deployment logs for errors and warnings.

    CRITICAL: A "completed" deployment is NOT necessarily "successful".
    This function scans logs for errors/warnings even on completed deployments.

    Returns:
        dict: Log analysis results with errors, warnings, and status_detail
    """
    # Check cache first
    now = time.time()
    cache_entry = _log_analysis_cache.get(deployment_id)
    if cache_entry:
        ts_raw = cache_entry.get("timestamp")
        ts = float(ts_raw) if isinstance(ts_raw, (int, float)) else 0.0
        if now - ts < _log_analysis_cache_ttl:
            data_raw = cache_entry.get("data")
            if isinstance(data_raw, dict):
                return cast(dict[str, object], data_raw)

    # Only analyze completed/failed deployments (don't slow down running ones)
    status_attr = getattr(state, "status", "")
    status_val = str(getattr(status_attr, "value", None) or status_attr)
    if status_val not in ("completed", "succeeded", "failed"):
        return {
            "status_detail": status_val,  # Keep original status for running/pending
            "log_analysis": None,
        }

    # Patterns to search for
    error_patterns = [
        (r"\bERROR\b", "ERROR"),
        (r"\bCRITICAL\b", "CRITICAL"),
        (r"\bFATAL\b", "FATAL"),
        (r"\bException\b", "EXCEPTION"),
        (r"\bFailed\b.*:", "FAILED"),
        (r"Error:", "ERROR"),
    ]

    warning_patterns = [
        (r"\bWARNING\b", "WARNING"),
        (r"\bWARN\b", "WARN"),
        (r"\bdeprecated\b", "DEPRECATED"),
        (r"\bretry\b", "RETRY"),
        (r"\btimeout\b", "TIMEOUT"),
    ]

    # Stack trace patterns
    stack_trace_patterns = [
        r"Traceback \(most recent call last\)",
        r"^\s+at .+\(.+:\d+\)",  # JavaScript stack trace
        r"^\s+File .+line \d+",  # Python stack trace
    ]

    # Success patterns (strong indicators of successful completion)
    success_patterns = [
        r"Processing complete",
        r"All shards completed successfully",
        r"Deployment completed successfully",
        r"Task completed successfully",
        r"✓.*complete",
    ]

    try:
        get_shards_fn = getattr(state_manager, "get_deployment_shards", None)
        raw_shards: object = get_shards_fn(deployment_id) if callable(get_shards_fn) else []
        shards: list[dict[str, object]] = (
            cast(list[dict[str, object]], raw_shards) if isinstance(raw_shards, list) else []
        )

        if not shards:
            return {
                "status_detail": "no_shards",
                "log_analysis": {
                    "errors": [],
                    "warnings": [],
                    "has_stack_traces": False,
                    "success_indicators": [],
                    "shards_analyzed": 0,
                },
            }

        # Filter to completed/failed shards for log analysis
        _terminal = ("completed", "succeeded", "failed")
        shards_to_check: list[dict[str, object]] = [
            s for s in shards if str(s.get("status") or "").lower() in _terminal
        ]

        if not shards_to_check:
            return {
                "status_detail": "no_completed_shards",
                "log_analysis": None,
            }

        all_errors: list[dict[str, object]] = []
        all_warnings: list[dict[str, object]] = []
        stack_traces_found = False
        success_indicators: list[dict[str, object]] = []

        def _analyze_shard_vm(
            shard: dict[str, object],
        ) -> tuple[list[dict[str, object]], list[dict[str, object]], bool, list[dict[str, object]]]:
            """Analyze logs for a single shard (VM-based deployment)."""
            shard_errors: list[dict[str, object]] = []
            shard_warnings: list[dict[str, object]] = []
            shard_stack_traces = False
            shard_success: list[dict[str, object]] = []

            try:
                # Get logs from shard serial console (VM deployment)
                _compute_raw: object = shard.get("compute_info")
                compute_info: dict[str, object] = (
                    cast(dict[str, object], _compute_raw) if isinstance(_compute_raw, dict) else {}
                )
                if not compute_info:
                    return shard_errors, shard_warnings, shard_stack_traces, shard_success

                vm_name_raw = compute_info.get("vm_name")
                vm_name = str(vm_name_raw) if isinstance(vm_name_raw, str) else ""
                if not vm_name:
                    return shard_errors, shard_warnings, shard_stack_traces, shard_success

                # Get serial console output
                get_console_fn = getattr(state_manager, "get_vm_serial_console", None)
                logs: object = get_console_fn(vm_name) if callable(get_console_fn) else None
                if not logs:
                    return shard_errors, shard_warnings, shard_stack_traces, shard_success

                log_lines = logs.split("\n") if isinstance(logs, str) else []

                for line_num, line in enumerate(log_lines, 1):
                    line = line.strip()
                    if not line:
                        continue

                    # Check for errors
                    for pattern, error_type in error_patterns:
                        if re.search(pattern, line, re.IGNORECASE):
                            shard_errors.append(
                                {
                                    "line": line,
                                    "type": error_type,
                                    "shard_id": shard.get("shard_id"),
                                    "vm_name": vm_name,
                                    "line_number": line_num,
                                }
                            )
                            break

                    # Check for warnings
                    for pattern, warn_type in warning_patterns:
                        if re.search(pattern, line, re.IGNORECASE):
                            shard_warnings.append(
                                {
                                    "line": line,
                                    "type": warn_type,
                                    "shard_id": shard.get("shard_id"),
                                    "vm_name": vm_name,
                                    "line_number": line_num,
                                }
                            )
                            break

                    # Check for stack traces
                    for pattern in stack_trace_patterns:
                        if re.search(pattern, line):
                            shard_stack_traces = True
                            break

                    # Check for success indicators
                    for pattern in success_patterns:
                        if re.search(pattern, line, re.IGNORECASE):
                            shard_success.append(
                                {
                                    "line": line,
                                    "shard_id": shard.get("shard_id"),
                                    "vm_name": vm_name,
                                    "line_number": line_num,
                                }
                            )
                            break

            except (OSError, ValueError, RuntimeError) as e:
                logger.warning("Error analyzing shard %s logs: %s", shard.get("shard_id"), e)

            return shard_errors, shard_warnings, shard_stack_traces, shard_success

        # Parallel analysis of shard logs (limit concurrency to avoid overwhelming)
        max_workers = min(10, len(shards_to_check))

        with ThreadPoolExecutor(max_workers=max_workers) as executor:
            # Submit all shard analysis tasks
            futures = {
                executor.submit(_analyze_shard_vm, shard): shard for shard in shards_to_check
            }

            # Collect results as they complete
            for future in as_completed(futures, timeout=30):  # 30s timeout per shard
                try:
                    shard_errors, shard_warnings, shard_stack_traces, shard_success = (
                        future.result()
                    )

                    all_errors.extend(shard_errors)
                    all_warnings.extend(shard_warnings)
                    if shard_stack_traces:
                        stack_traces_found = True
                    success_indicators.extend(shard_success)

                except (OSError, ValueError, RuntimeError) as e:
                    shard = futures[future]
                    logger.warning("Timeout/error analyzing shard %s: %s", shard.get("shard_id"), e)

        # Determine overall status based on log analysis
        has_errors = len(all_errors) > 0
        has_warnings = len(all_warnings) > 0
        has_success_indicators = len(success_indicators) > 0

        # Status logic for "completed" deployments:
        # - If has strong success indicators and no errors -> "succeeded"
        # - If has errors -> "failed_with_errors"
        # - If has stack traces -> "failed_with_errors"
        # - If has only warnings -> "succeeded_with_warnings"
        # - Otherwise -> keep original status

        if status_val == "completed":
            if has_errors or stack_traces_found:
                status_detail = "failed_with_errors"
            elif has_success_indicators and not has_warnings:
                status_detail = "succeeded"
            elif has_success_indicators and has_warnings:
                status_detail = "succeeded_with_warnings"
            elif has_warnings:
                status_detail = "completed_with_warnings"
            else:
                status_detail = "completed"  # No strong indicators either way
        else:
            status_detail = status_val  # Keep failed/succeeded as-is

        log_analysis_data: dict[str, object] = {
            "errors": all_errors[:50],  # Limit to first 50 errors to avoid huge responses
            "warnings": all_warnings[:50],  # Limit to first 50 warnings
            "has_stack_traces": stack_traces_found,
            "success_indicators": success_indicators[:10],  # Sample of success indicators
            "shards_analyzed": len(shards_to_check),
            "total_errors": len(all_errors),
            "total_warnings": len(all_warnings),
        }
        result: dict[str, object] = {
            "status_detail": status_detail,
            "log_analysis": log_analysis_data,
        }

        # Cache the result
        _log_analysis_cache[deployment_id] = {"data": result, "timestamp": now}

        return result

    except (OSError, ValueError, RuntimeError) as e:
        logger.error("Error during log analysis for %s: %s", deployment_id, e)
        return {
            "status_detail": "analysis_error",
            "log_analysis": None,
            "error": str(e),
        }


async def analyze_deployment_logs(
    state_manager: object, deployment_id: str, state: object
) -> dict[str, object]:
    """Async wrapper for log analysis."""
    return await asyncio.to_thread(
        analyze_deployment_logs_sync, state_manager, deployment_id, state
    )


def invalidate_log_analysis_cache(deployment_id: str | None = None):
    """Invalidate log analysis cache for specific deployment or all deployments."""
    if deployment_id:
        _log_analysis_cache.pop(deployment_id, None)
    else:
        _log_analysis_cache.clear()

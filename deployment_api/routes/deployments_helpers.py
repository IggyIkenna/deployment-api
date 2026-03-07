"""
Helper functions for deployments routes.

Contains utility functions for log analysis, state management, and other common operations.
"""

import json
import logging
import re
import time
from datetime import UTC, datetime, timedelta
from typing import cast

from deployment_api.deployment_api_config import DeploymentApiConfig

logger = logging.getLogger(__name__)

# Deployment configuration
_deployment_config = DeploymentApiConfig()

# Cache for verification results
_verification_cache = {}


def _get_verification_cache(deployment_id: str) -> dict | None:
    """
    Get verification cache for a deployment.

    Returns cached verification data or None if not found/expired.
    """
    if deployment_id not in _verification_cache:
        return None

    cache_entry = _verification_cache[deployment_id]
    # Cache expires after 5 minutes
    if time.time() - cache_entry.get("timestamp", 0) > 300:
        del _verification_cache[deployment_id]
        return None

    return cache_entry.get("data")


def _set_verification_cache(deployment_id: str, data: dict) -> None:
    """
    Set verification cache for a deployment.
    """
    _verification_cache[deployment_id] = {"data": data, "timestamp": time.time()}


def _build_deploy_env_vars(
    service: str,
    project_id: str,
    deployment_id: str,
    max_concurrent: int,
    deployment_mode: str = "vm",  # or "cloud-run"
    enable_direct_gcs: bool = False,
    shard_id: str | None = None,
) -> dict[str, str]:
    """
    Build standardized environment variables for deployment.

    These are passed to the actual deployment containers and must match
    what the services expect. See: deployment env var standardization.
    """
    env_vars = {
        "SERVICE_NAME": service,
        "PROJECT_ID": project_id,
        "DEPLOYMENT_ID": deployment_id,
        "MAX_CONCURRENT": str(max_concurrent),
        "DEPLOYMENT_MODE": deployment_mode,
    }

    # Optional shard-specific environment
    if shard_id:
        env_vars["SHARD_ID"] = shard_id

    # GCS direct access for performance-critical services
    if enable_direct_gcs:
        env_vars["ENABLE_DIRECT_GCS"] = "true"

    return env_vars


def _maybe_add_direct_gcs(
    service: str, env_vars: dict[str, str], deployment_config: dict | None = None
) -> dict[str, str]:
    """
    Conditionally add direct GCS environment variables for high-throughput services.

    Services like market-tick-data-handler benefit from direct GCS access
    instead of going through the API server.
    """
    # Services that benefit from direct GCS access
    direct_gcs_services = {
        "market-tick-data-handler",
        "market-data-processing-service",
        "features-delta-one-service",
    }

    if service in direct_gcs_services:
        env_vars["ENABLE_DIRECT_GCS"] = "true"
        if deployment_config:
            # Add any service-specific GCS configuration
            gcs_config = deployment_config.get("gcs_config") or {}
            if gcs_config:
                env_vars.update({f"GCS_{k.upper()}": str(v) for k, v in gcs_config.items()})

    return env_vars


def find_duplicate_running_shards(
    state_manager, service: str, deployment_id: str, shard_args_list: list[list[str]]
) -> list[dict]:
    """
    Find any currently running deployments that would conflict with the new shards.

    Returns list of conflicts with details about the overlapping deployments.
    This is critical for preventing data corruption in services that process
    by date ranges or other dimensions.
    """
    conflicts = []

    try:
        # Get all active deployments for this service
        active_deployments = state_manager.list_deployments(
            service=service, status=["running", "pending", "resuming"]
        )

        # Convert new shard args to comparable format
        new_shard_signatures = set()
        for shard_args in shard_args_list:
            # Extract key identifying parameters (service-specific logic)
            signature = _extract_shard_signature(service, shard_args)
            if signature:
                new_shard_signatures.add(signature)

        # Check each active deployment for overlaps
        for active_deployment in active_deployments:
            if active_deployment["deployment_id"] == deployment_id:
                continue  # Skip self

            active_shards = state_manager.get_deployment_shards(active_deployment["deployment_id"])

            for active_shard in active_shards:
                active_signature = _extract_shard_signature(service, active_shard.get("args") or [])

                if active_signature and active_signature in new_shard_signatures:
                    conflicts.append(
                        {
                            "deployment_id": active_deployment["deployment_id"],
                            "shard_id": active_shard.get("shard_id"),
                            "signature": active_signature,
                            "status": active_deployment.get("status"),
                            "started_at": active_deployment.get("started_at"),
                        }
                    )

    except (OSError, ValueError, RuntimeError) as e:
        logger.warning("Error checking for duplicate shards: %s", e)
        # Don't fail deployment due to conflict check failure

    return conflicts


def _extract_shard_signature(service: str, shard_args: list[str]) -> str | None:
    """
    Extract a signature from shard arguments that uniquely identifies the work.

    This is used to detect overlapping shards across deployments.
    Service-specific logic for what constitutes "overlapping work".
    """
    if not shard_args:
        return None

    signature_parts = [service]

    # Extract date range and category for most services
    for i, arg in enumerate(shard_args):
        if arg == "--start-date" and i + 1 < len(shard_args):
            signature_parts.append(f"start:{shard_args[i + 1]}")
        elif arg == "--end-date" and i + 1 < len(shard_args):
            signature_parts.append(f"end:{shard_args[i + 1]}")
        elif arg == "--category" and i + 1 < len(shard_args):
            signature_parts.append(f"cat:{shard_args[i + 1]}")
        elif arg == "--venue" and i + 1 < len(shard_args):
            signature_parts.append(f"venue:{shard_args[i + 1]}")

    return "|".join(signature_parts) if len(signature_parts) > 1 else None


def _status_str(val: object) -> str:
    """Convert various status representations to string."""
    if isinstance(val, str):
        return val
    elif isinstance(val, dict):
        return val.get("status", "unknown")
    elif hasattr(val, "status"):
        return str(val.status)
    else:
        return str(val)


def _extract_severity_and_logger(line: str) -> tuple[str, str | None]:
    """
    Extract severity level and logger name from log line.

    Handles various log formats:
    - Python logging: "ERROR:service_name:Message"
    - Structured JSON: {"level": "error", "logger": "service_name"}
    - Cloud Logging: severity field
    """
    severity = "INFO"  # default
    logger_name = None

    # Try Python logging format first
    if ":" in line:
        parts = line.split(":", 2)
        if len(parts) >= 2:
            potential_level = parts[0].strip().upper()
            if potential_level in {"DEBUG", "INFO", "WARNING", "ERROR", "CRITICAL"}:
                severity = potential_level
                logger_name = parts[1].strip() if len(parts) > 1 else None

    # Try to extract from JSON if it looks like structured logging
    if line.strip().startswith("{"):
        try:
            log_obj = cast(dict[str, object], json.loads(line.strip()))
            if "level" in log_obj:
                severity = str(log_obj["level"]).upper()
            if "logger" in log_obj:
                logger_name = str(log_obj["logger"])
            elif "name" in log_obj:
                logger_name = str(log_obj["name"])
        except (json.JSONDecodeError, AttributeError) as e:
            logger.debug("Suppressed %s during operation: %s", type(e).__name__, e)
            pass

    # Map severity levels to standard format
    severity_map = {
        "WARN": "WARNING",
        "ERR": "ERROR",
        "CRIT": "CRITICAL",
        "FATAL": "CRITICAL",
        "TRACE": "DEBUG",
    }
    severity = severity_map.get(severity, severity)

    return severity, logger_name


def _extract_date_range(date_val: object) -> tuple[str | None, str | None]:
    """
    Extract start and end date from various date specifications.

    Handles:
    - Single date: "2024-01-01" -> ("2024-01-01", "2024-01-01")
    - Date range: "2024-01-01,2024-01-31" -> ("2024-01-01", "2024-01-31")
    - Date range: "2024-01-01 to 2024-01-31" -> ("2024-01-01", "2024-01-31")
    - Relative: "last-7-days" -> computed range
    """
    if not date_val:
        return None, None

    date_str = str(date_val).strip()

    # Handle comma-separated range
    if "," in date_str:
        parts = [p.strip() for p in date_str.split(",", 1)]
        return parts[0] if parts[0] else None, parts[1] if len(parts) > 1 and parts[1] else None

    # Handle "to" separated range
    if " to " in date_str:
        parts = [p.strip() for p in date_str.split(" to ", 1)]
        return parts[0] if parts[0] else None, parts[1] if len(parts) > 1 and parts[1] else None

    # Handle relative dates
    if date_str.startswith("last-") and date_str.endswith("-days"):
        try:
            days = int(date_str.replace("last-", "").replace("-days", ""))
            end_date = datetime.now(UTC).strftime("%Y-%m-%d")
            start_date = (datetime.now(UTC) - timedelta(days=days - 1)).strftime("%Y-%m-%d")
            return start_date, end_date
        except ValueError as e:
            logger.debug("Suppressed %s during operation: %s", type(e).__name__, e)
            pass

    # Single date - treat as single day range
    if re.match(r"\d{4}-\d{2}-\d{2}", date_str):
        return date_str, date_str

    return None, None

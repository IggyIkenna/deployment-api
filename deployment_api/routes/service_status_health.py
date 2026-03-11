"""
Service Status Health Check Utilities

Functions for anomaly detection and health assessment.
"""

import logging
from datetime import UTC, datetime, timedelta

logger = logging.getLogger(__name__)


def detect_anomalies(
    data_timestamp: str | None,
    deployment_timestamp: str | None,
    build_timestamp: str | None,
    code_timestamp: str | None,
) -> list[dict[str, str]]:
    """
    Detect temporal anomalies in service status.

    Returns list of detected issues.
    """
    anomalies: list[dict[str, str]] = []
    now = datetime.now(UTC)

    try:
        # Parse timestamps
        data_time = datetime.fromisoformat(data_timestamp) if data_timestamp else None
        deploy_time = datetime.fromisoformat(deployment_timestamp) if deployment_timestamp else None
        build_time = datetime.fromisoformat(build_timestamp) if build_timestamp else None
        code_time = datetime.fromisoformat(code_timestamp) if code_timestamp else None

        # Anomaly 1: Stale data (no update in 24 hours)
        if data_time and (now - data_time) > timedelta(hours=24):
            age_hours = (now - data_time).total_seconds() / 3600
            anomalies.append(
                {
                    "type": "stale_data",
                    "severity": "warning" if age_hours < 48 else "error",
                    "message": f"Data not updated in {age_hours:.1f} hours",
                }
            )

        # Anomaly 2: Deployment ran but data not updated (should update within 1 hour)
        if deploy_time and data_time and (deploy_time - data_time) > timedelta(hours=1):
            anomalies.append(
                {
                    "type": "deployment_without_data",
                    "severity": "warning",
                    "message": (
                        f"Deployment ran"
                        f" {(deploy_time - data_time).total_seconds() / 3600:.1f}h"
                        " after data update"
                    ),
                }
            )

        # Anomaly 3: Code pushed but not built (should build within 30 min)
        if code_time and build_time and (code_time - build_time) > timedelta(minutes=30):
            anomalies.append(
                {
                    "type": "code_not_built",
                    "severity": "warning",
                    "message": (
                        f"Code pushed"
                        f" {(code_time - build_time).total_seconds() / 60:.0f}m"
                        " ago but not built"
                    ),
                }
            )

        # Anomaly 4: Old deployment (no deployment in 7 days)
        if deploy_time and (now - deploy_time) > timedelta(days=7):
            age_days = (now - deploy_time).days
            anomalies.append(
                {
                    "type": "no_recent_deployment",
                    "severity": "info",
                    "message": f"No deployment in {age_days} days",
                }
            )

    except (OSError, ValueError, RuntimeError) as e:
        logger.error("Error detecting anomalies: %s", e)

    return anomalies


def determine_service_health(
    data_ts: str | None,
    deploy_ts: str | None,
    deploy_status: str | None,
    build_status: str | None,
    anomalies: list[dict[str, str]],
) -> str:
    """
    Determine overall health status for a service.

    Returns: "healthy", "warning", "error", "stale", "build_failed", or "unknown"
    """

    def parse_timestamp(ts: str) -> datetime:
        """Parse timestamp string to timezone-aware datetime."""
        ts = ts.replace("Z", "+00:00")
        dt = datetime.fromisoformat(ts)
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=UTC)
        return dt

    health = "healthy"
    now = datetime.now(UTC)

    # First priority: Check deployment status
    if deploy_ts and deploy_status:
        try:
            deploy_time = parse_timestamp(deploy_ts)
            deploy_age = now - deploy_time

            # If there's a recent successful deployment (within 7 days), prioritize it
            if deploy_age < timedelta(days=7):
                # Normalize status to lowercase string for comparison
                deploy_status_lower = str(deploy_status).lower()
                if deploy_status_lower == "completed":
                    health = "healthy"  # Recent successful deployment = healthy
                elif deploy_status_lower == "running":
                    health = "warning"  # Deployment in progress
                elif deploy_status_lower == "failed":
                    health = "error"  # Failed deployment
                elif deploy_status_lower in ["cancelled", "partial"]:
                    health = "warning"  # Partial/cancelled deployment
                # If status is unknown/missing but deployment exists, check data freshness
                elif deploy_status_lower == "unknown":
                    pass  # Fall through to data freshness check
        except (OSError, ValueError, RuntimeError) as e:
            logger.warning("Error parsing deployment timestamp: %s", e)

    # Second priority: Build failures override deployment status
    if build_status == "FAILURE":
        health = "build_failed"

    # Third priority: Anomalies (only downgrade health if no recent deployment)
    if deploy_ts:
        try:
            deploy_time = parse_timestamp(deploy_ts)
            deploy_age = now - deploy_time
            # Only apply anomaly downgrades if deployment is old (>7 days)
            if deploy_age >= timedelta(days=7):
                if any(a["severity"] == "error" for a in anomalies):
                    health = "error"
                elif any(a["severity"] == "warning" for a in anomalies):
                    health = "warning"
        except (ValueError, TypeError) as e:
            logger.debug("Error computing health from deploy/anomalies: %s", e)
    else:
        # No deployment info - use anomalies
        if any(a["severity"] == "error" for a in anomalies):
            health = "error"
        elif any(a["severity"] == "warning" for a in anomalies):
            health = "warning"

    # Fourth priority: Data freshness (fallback)
    if health == "healthy" and data_ts and not deploy_ts:
        try:
            data_time = parse_timestamp(data_ts)
            data_age = now - data_time
            if data_age < timedelta(hours=24):
                health = "healthy"
            elif data_age < timedelta(hours=48):
                health = "warning"
            else:
                health = "stale"
        except (OSError, ValueError, RuntimeError) as e:
            logger.warning("Error parsing data timestamp: %s", e)
            health = "unknown"

    return health


def determine_overview_health(
    data_ts: str | None,
    deploy_ts: str | None,
    deploy_status: str | None,
    build_status: str | None,
) -> str:
    """
    Determine health for overview display (simplified logic).

    Returns: "healthy", "warning", "error", "stale", "build_failed", or "unknown"
    """

    def parse_timestamp(ts: str) -> datetime:
        """Parse timestamp string to timezone-aware datetime."""
        # Handle "Z" suffix
        ts = ts.replace("Z", "+00:00")
        dt = datetime.fromisoformat(ts)
        # If parsed datetime is naive, assume UTC
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=UTC)
        return dt

    health = "unknown"
    now = datetime.now(UTC)

    # First check: If there's a recent successful deployment (within 7 days), mark as healthy
    if deploy_ts and deploy_status:
        try:
            deploy_time = parse_timestamp(deploy_ts)
            deploy_age = now - deploy_time
            if deploy_age < timedelta(days=7):
                # Recent deployment exists - check if it completed successfully
                deploy_status_lower = str(deploy_status).lower()
                if deploy_status_lower == "completed":
                    health = "healthy"
                elif deploy_status_lower == "running":
                    health = "warning"  # Deployment in progress
                elif deploy_status_lower == "failed":
                    health = "error"  # Failed deployment
                elif deploy_status_lower in ["cancelled", "partial"]:
                    health = "warning"  # Partial/cancelled deployment
                # If status is unknown/missing but deployment exists, check data freshness
                elif deploy_status_lower == "unknown":
                    pass  # Fall through to data freshness check
            else:
                # Old deployment - check data freshness instead
                if data_ts:
                    try:
                        data_time = parse_timestamp(data_ts)
                        data_age = now - data_time
                        if data_age < timedelta(hours=24):
                            health = "healthy"
                        elif data_age < timedelta(hours=48):
                            health = "warning"
                        else:
                            health = "stale"
                    except (ValueError, TypeError) as e:
                        logger.debug("Error parsing data timestamp: %s", e)
                        health = "unknown"
        except (OSError, ValueError, RuntimeError) as e:
            logger.warning("Error parsing deployment timestamp: %s", e)

    # Second check: If no deployment info, check data freshness
    elif data_ts:
        try:
            data_time = parse_timestamp(data_ts)
            data_age = now - data_time
            if data_age < timedelta(hours=24):
                health = "healthy"
            elif data_age < timedelta(hours=48):
                health = "warning"
            else:
                health = "stale"
        except (OSError, ValueError, RuntimeError) as e:
            logger.warning("Error parsing data timestamp: %s", e)
            health = "unknown"

    # Third check: Build failures override everything
    if build_status == "FAILURE":
        health = "build_failed"

    return health
